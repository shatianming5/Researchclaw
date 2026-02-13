import fs from "node:fs/promises";
import path from "node:path";
import type { OpenClawConfig } from "../../config/config.js";
import type { CommandOptions, SpawnResult } from "../../process/exec.js";
import { resolveSandboxConfigForAgent } from "../../agents/sandbox/config.js";
import { ensureSandboxContainer, execDocker } from "../../agents/sandbox/docker.js";
import { normalizePathForContainer } from "./utils.js";

export function resolveContainerWorkdir(params: {
  planDir: string;
  hostCwd?: string | null;
  containerRoot: string;
}) {
  const cwd = params.hostCwd ? path.resolve(params.hostCwd) : params.planDir;
  const rel = path.relative(params.planDir, cwd);
  if (!rel || rel === ".") {
    return params.containerRoot;
  }
  if (rel.startsWith("..") || path.isAbsolute(rel)) {
    return params.containerRoot;
  }
  const normalized = normalizePathForContainer(rel);
  return path.posix.join(params.containerRoot, normalized);
}

export async function ensureSandboxForPlan(params: {
  planDir: string;
  cfg: OpenClawConfig;
  agentId: string;
  planId?: string;
  imageOverride?: string;
  networkOverride?: string;
}): Promise<{ containerName: string; containerRoot: string }> {
  const base = resolveSandboxConfigForAgent(params.cfg, params.agentId);
  const docker = {
    ...base.docker,
    image: params.imageOverride?.trim() || base.docker.image,
    network: params.networkOverride?.trim() || "bridge",
  };
  const sandboxCfg = {
    ...base,
    mode: "all" as const,
    scope: "session" as const,
    workspaceAccess: "rw" as const,
    docker,
  };

  // Best-effort: if image is missing and Dockerfile.sandbox exists in cwd, build it.
  const image = docker.image;
  const inspect = await execDocker(["image", "inspect", image], { allowFailure: true });
  if (inspect.code !== 0) {
    try {
      await fs.stat(path.resolve("Dockerfile.sandbox"));
      await execDocker(["build", "-t", image, "-f", "Dockerfile.sandbox", "."], {
        allowFailure: false,
      });
    } catch {
      // Fall back to whatever ensureSandboxContainer does; it may tag a minimal image.
    }
  }

  const sessionKey = `proposal:${params.planId ?? path.basename(params.planDir)}`;
  const containerName = await ensureSandboxContainer({
    sessionKey,
    workspaceDir: params.planDir,
    agentWorkspaceDir: params.planDir,
    cfg: sandboxCfg,
  });
  return { containerName, containerRoot: docker.workdir };
}

export function createSandboxRunCommand(params: {
  planDir: string;
  containerName: string;
  containerRoot: string;
  baseEnv?: Record<string, string>;
  runHostCommand: (argv: string[], options: CommandOptions) => Promise<SpawnResult>;
}) {
  const baseEnv = params.baseEnv ?? {};
  return async (argv: string[], options: CommandOptions): Promise<SpawnResult> => {
    const workdir = resolveContainerWorkdir({
      planDir: params.planDir,
      hostCwd: options.cwd ?? params.planDir,
      containerRoot: params.containerRoot,
    });
    const envMerged: Record<string, string> = { ...baseEnv };
    for (const [key, value] of Object.entries(options.env ?? {})) {
      if (typeof value === "string") {
        envMerged[key] = value;
      }
    }
    const dockerArgv: string[] = ["docker", "exec", "-i", "-w", workdir];
    for (const [key, value] of Object.entries(envMerged)) {
      dockerArgv.push("-e", `${key}=${value}`);
    }
    dockerArgv.push(params.containerName, ...argv);
    return await params.runHostCommand(dockerArgv, {
      cwd: params.planDir,
      timeoutMs: options.timeoutMs,
      env: undefined,
    });
  };
}
