import { execFile } from "node:child_process";
import os from "node:os";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export type NodeHostResources = {
  gpuCount?: number;
  gpuType?: string;
  gpuMemGB?: number;
  cpuCores?: number;
  ramGB?: number;
};

function sanitizeResources(raw: unknown): NodeHostResources | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const obj = raw as Record<string, unknown>;

  const int0 = (value: unknown): number | undefined => {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return undefined;
    }
    return Number.isInteger(value) && value >= 0 ? value : undefined;
  };

  const numPos = (value: unknown): number | undefined => {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return undefined;
    }
    return value > 0 ? value : undefined;
  };

  const text = (value: unknown): string | undefined => {
    if (typeof value !== "string") {
      return undefined;
    }
    const trimmed = value.trim();
    return trimmed ? trimmed : undefined;
  };

  const resources: NodeHostResources = {
    gpuCount: int0(obj.gpuCount),
    gpuType: text(obj.gpuType),
    gpuMemGB: numPos(obj.gpuMemGB),
    cpuCores: int0(obj.cpuCores),
    ramGB: numPos(obj.ramGB),
  };

  const hasAny = Object.values(resources).some((value) => value !== undefined);
  return hasAny ? resources : null;
}

async function tryExec(
  cmd: string,
  args: string[],
  env: NodeJS.ProcessEnv,
): Promise<string | null> {
  try {
    const res = await execFileAsync(cmd, args, {
      env,
      timeout: 5000,
      maxBuffer: 1024 * 1024,
    });
    return typeof res.stdout === "string" ? res.stdout : "";
  } catch {
    return null;
  }
}

function parseNvidiaSmiList(output: string): number {
  const lines = output
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.filter((line) => line.startsWith("GPU ")).length;
}

function parseNvidiaQuery(output: string): { name?: string; memGB?: number } {
  const first = output
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)[0];
  if (!first) {
    return {};
  }
  const parts = first.split(",").map((p) => p.trim());
  const name = parts[0] || undefined;
  const memMb = parts[1] ? Number(parts[1]) : NaN;
  const memGB =
    Number.isFinite(memMb) && memMb > 0 ? Math.round((memMb / 1024) * 10) / 10 : undefined;
  return { name, memGB };
}

export async function detectNodeResources(params?: {
  env?: NodeJS.ProcessEnv;
  pathEnv?: string;
}): Promise<NodeHostResources | undefined> {
  const env = { ...(params?.env ?? process.env) };
  if (params?.pathEnv) {
    env.PATH = params.pathEnv;
  }

  const override = env.OPENCLAW_NODE_RESOURCES_JSON?.trim();
  if (override) {
    try {
      const parsed = JSON.parse(override) as unknown;
      const sanitized = sanitizeResources(parsed);
      return sanitized ?? undefined;
    } catch {
      // ignore invalid JSON override
    }
  }

  const cpuCores = os.cpus().length;
  const ramGB = Math.round((os.totalmem() / 1024 ** 3) * 10) / 10;

  let gpuCount: number | undefined;
  let gpuType: string | undefined;
  let gpuMemGB: number | undefined;

  const list = await tryExec("nvidia-smi", ["-L"], env);
  if (typeof list === "string") {
    const count = parseNvidiaSmiList(list);
    if (count > 0) {
      gpuCount = count;
      const query = await tryExec(
        "nvidia-smi",
        ["--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
        env,
      );
      if (typeof query === "string") {
        const parsed = parseNvidiaQuery(query);
        gpuType = parsed.name;
        gpuMemGB = parsed.memGB;
      }
    }
  }

  const resources: NodeHostResources = {
    cpuCores: Number.isFinite(cpuCores) && cpuCores > 0 ? cpuCores : undefined,
    ramGB: Number.isFinite(ramGB) && ramGB > 0 ? ramGB : undefined,
    gpuCount,
    gpuType,
    gpuMemGB,
  };
  return Object.values(resources).some((value) => value !== undefined) ? resources : undefined;
}
