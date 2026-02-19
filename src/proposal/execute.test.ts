import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { OpenClawConfig } from "../config/config.js";
import type { ProposalLlmClient } from "./llm.js";
import { runCommandWithTimeout } from "../process/exec.js";
import { executeProposalPlan } from "./execute.js";

async function writePlanPackage(params: {
  dag: unknown;
  discovery?: unknown;
  report: unknown;
  acceptance?: unknown;
  retry?: unknown;
}): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-exec-"));
  await fs.mkdir(path.join(root, "plan"), { recursive: true });
  await fs.mkdir(path.join(root, "ir"), { recursive: true });
  await fs.mkdir(path.join(root, "report"), { recursive: true });
  await fs.mkdir(path.join(root, "input"), { recursive: true });

  await fs.writeFile(path.join(root, "plan", "plan.dag.json"), JSON.stringify(params.dag, null, 2));
  await fs.writeFile(
    path.join(root, "plan", "acceptance.json"),
    JSON.stringify(params.acceptance ?? { checks: [] }, null, 2),
  );
  await fs.writeFile(
    path.join(root, "plan", "retry.json"),
    JSON.stringify(params.retry ?? { policies: [], defaultPolicyId: "retry.unknown" }, null, 2),
  );
  await fs.writeFile(
    path.join(root, "ir", "discovery.json"),
    JSON.stringify(params.discovery ?? { repos: [], datasets: [] }, null, 2),
  );
  await fs.writeFile(
    path.join(root, "report", "compile_report.json"),
    JSON.stringify(params.report, null, 2),
  );
  await fs.writeFile(path.join(root, "input", "context.json"), JSON.stringify({ agentId: "main" }));

  return root;
}

describe("proposal/execute", () => {
  it("executes CPU shell nodes via exec runner", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "build",
            type: "build",
            tool: "shell",
            commands: ["echo hello"],
            outputs: ["report/build.log"],
          },
        ],
        edges: [],
      },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "plan",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
      retry: {
        policies: [
          { id: "retry.unknown", category: "unknown", maxAttempts: 1, retryablePatterns: [] },
        ],
        defaultPolicyId: "retry.unknown",
      },
    });

    const runHostCommand = vi.fn(async (_argv: string[]) => ({
      code: 0,
      stdout: "ok",
      stderr: "",
      signal: null,
      killed: false,
    }));

    const res = await executeProposalPlan({
      planDir,
      cfg: {} as OpenClawConfig,
      opts: { sandbox: false, repair: false },
      deps: { runHostCommand },
    });

    expect(res.ok).toBe(true);
    expect(runHostCommand).toHaveBeenCalledTimes(1);
    const callArgv = runHostCommand.mock.calls[0]?.[0] ?? [];
    expect(callArgv.slice(0, 2)).toEqual(["sh", "-lc"]);
    expect(res.results[0]?.status).toBe("ok");
    expect(res.results[0]?.executor).toBe("exec");
  });

  it("executes GPU nodes via node.invoke when --node is set", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "train.run",
            type: "train",
            tool: "shell",
            commands: ["python train.py"],
            resources: { gpuCount: 1 },
            outputs: ["report/train.jsonl"],
          },
        ],
        edges: [],
      },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "plan",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
      retry: {
        policies: [
          { id: "retry.unknown", category: "unknown", maxAttempts: 1, retryablePatterns: [] },
        ],
        defaultPolicyId: "retry.unknown",
      },
    });

    const callGateway = vi.fn(async (opts: { method: string }) => {
      if (opts.method === "node.list") {
        return {
          nodes: [
            {
              nodeId: "node-123",
              connected: true,
              commands: ["system.run"],
              resources: { gpuCount: 1 },
            },
          ],
        };
      }
      if (opts.method === "node.invoke") {
        return { payload: { exitCode: 0, success: true, timedOut: false, stdout: "", stderr: "" } };
      }
      throw new Error(`unexpected method: ${opts.method}`);
    });

    const res = await executeProposalPlan({
      planDir,
      cfg: {} as OpenClawConfig,
      opts: { sandbox: false, repair: false, node: "node-123" },
      deps: { callGateway: callGateway as unknown as typeof callGateway },
    });

    expect(res.ok).toBe(true);
    expect(callGateway).toHaveBeenCalledTimes(2);
    expect(callGateway.mock.calls.map((call) => call[0]?.method)).toEqual([
      "node.list",
      "node.invoke",
    ]);
    expect(res.results[0]?.executor).toBe("node.invoke");
    expect(res.results[0]?.status).toBe("ok");
  });

  it("applies an LLM patch and reruns on failure", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "repo.build",
            type: "build",
            tool: "shell",
            inputs: ["cache/git/repo"],
            commands: ["node build.js"],
          },
        ],
        edges: [],
      },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "plan",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
      retry: {
        policies: [
          { id: "retry.unknown", category: "unknown", maxAttempts: 2, retryablePatterns: [] },
        ],
        defaultPolicyId: "retry.unknown",
      },
    });

    const repoRoot = path.join(planDir, "cache", "git", "repo");
    await fs.mkdir(path.join(repoRoot, "src"), { recursive: true });
    await fs.writeFile(path.join(repoRoot, "src", "app.ts"), "export const value = 1;\n", "utf-8");
    await runCommandWithTimeout(["git", "init"], { cwd: repoRoot, timeoutMs: 10_000 });
    await runCommandWithTimeout(["git", "config", "user.email", "test@example.com"], {
      cwd: repoRoot,
      timeoutMs: 10_000,
    });
    await runCommandWithTimeout(["git", "config", "user.name", "OpenClaw Test"], {
      cwd: repoRoot,
      timeoutMs: 10_000,
    });
    await runCommandWithTimeout(["git", "add", "-A"], { cwd: repoRoot, timeoutMs: 10_000 });
    await runCommandWithTimeout(["git", "commit", "-m", "init"], {
      cwd: repoRoot,
      timeoutMs: 10_000,
    });

    let callCount = 0;
    const runHostCommand = vi.fn(async (_argv: string[], _opts?: { cwd?: string }) => {
      callCount += 1;
      if (callCount === 1) {
        return {
          code: 1,
          stdout: "",
          stderr: "src/app.ts:1:1 build failed\n",
          signal: null,
          killed: false,
        };
      }
      return { code: 0, stdout: "ok\n", stderr: "", signal: null, killed: false };
    });

    const llmClient = {
      modelKey: "test",
      completeText: vi.fn(async () => {
        return [
          "*** Begin Patch",
          "*** Update File: src/app.ts",
          "@@",
          "-export const value = 1;",
          "+export const value = 2;",
          "*** End Patch",
          "",
        ].join("\n");
      }),
    } satisfies ProposalLlmClient;

    const res = await executeProposalPlan({
      planDir,
      cfg: {} as OpenClawConfig,
      opts: { sandbox: false, repair: true, repairAttempts: 1, maxAttempts: 2 },
      deps: { runHostCommand, llmClient },
    });

    expect(res.ok).toBe(true);
    expect(runHostCommand).toHaveBeenCalledTimes(2);
    const worktreeRoot = path.join(planDir, "cache", "worktrees", "repo", "test-plan");
    expect(runHostCommand.mock.calls[0]?.[1]?.cwd).toBe(worktreeRoot);
    expect(llmClient.completeText).toHaveBeenCalledTimes(1);

    const base = await fs.readFile(path.join(repoRoot, "src", "app.ts"), "utf-8");
    expect(base).toContain("value = 1");

    const patched = await fs.readFile(path.join(worktreeRoot, "src", "app.ts"), "utf-8");
    expect(patched).toContain("value = 2");

    const repairDir = path.join(planDir, "report", "repairs", "repo.build");
    await expect(fs.stat(path.join(repairDir, "attempt-1.patch"))).resolves.toBeTruthy();
    await expect(
      fs.stat(path.join(repairDir, "attempt-1", "repair_evidence.json")),
    ).resolves.toBeTruthy();
    await expect(
      fs.stat(path.join(repairDir, "attempt-1", "repair_evidence.md")),
    ).resolves.toBeTruthy();

    const nodeRes = res.results.find((r) => r.nodeId === "repo.build");
    expect(nodeRes?.attempts.length).toBe(2);
    expect(nodeRes?.attempts[0]?.patch).toBeTruthy();
  });

  it("applies an LLM patch for GPU nodes when repo worktrees are enabled", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "train.run",
            type: "train",
            tool: "shell",
            inputs: ["cache/git/repo"],
            commands: ["node build.js"],
          },
        ],
        edges: [],
      },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "plan",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
      retry: {
        policies: [
          { id: "retry.unknown", category: "unknown", maxAttempts: 2, retryablePatterns: [] },
        ],
        defaultPolicyId: "retry.unknown",
      },
    });

    const repoRoot = path.join(planDir, "cache", "git", "repo");
    await fs.mkdir(path.join(repoRoot, "src"), { recursive: true });
    await fs.writeFile(path.join(repoRoot, "src", "app.ts"), "export const value = 1;\n", "utf-8");
    await runCommandWithTimeout(["git", "init"], { cwd: repoRoot, timeoutMs: 10_000 });
    await runCommandWithTimeout(["git", "config", "user.email", "test@example.com"], {
      cwd: repoRoot,
      timeoutMs: 10_000,
    });
    await runCommandWithTimeout(["git", "config", "user.name", "OpenClaw Test"], {
      cwd: repoRoot,
      timeoutMs: 10_000,
    });
    await runCommandWithTimeout(["git", "add", "-A"], { cwd: repoRoot, timeoutMs: 10_000 });
    await runCommandWithTimeout(["git", "commit", "-m", "init"], {
      cwd: repoRoot,
      timeoutMs: 10_000,
    });

    const llmClient = {
      modelKey: "test",
      completeText: vi.fn(async () => {
        return [
          "*** Begin Patch",
          "*** Update File: src/app.ts",
          "@@",
          "-export const value = 1;",
          "+export const value = 2;",
          "*** End Patch",
          "",
        ].join("\n");
      }),
    } satisfies ProposalLlmClient;

    const worktreeRoot = path.join(planDir, "cache", "worktrees", "repo", "test-plan");

    let invokeCount = 0;
    const callGateway = vi.fn(async (opts: { method: string; params?: unknown }) => {
      if (opts.method === "node.list") {
        return {
          nodes: [
            {
              nodeId: "node-123",
              connected: true,
              commands: ["system.run"],
              resources: { gpuCount: 1 },
            },
          ],
        };
      }
      expect(opts.method).toBe("node.invoke");
      const paramsObj =
        opts.params && typeof opts.params === "object"
          ? (opts.params as Record<string, unknown>)
          : {};
      const inner =
        paramsObj.params && typeof paramsObj.params === "object"
          ? (paramsObj.params as Record<string, unknown>)
          : {};
      expect(inner.cwd).toBe(worktreeRoot);

      invokeCount += 1;
      if (invokeCount === 1) {
        return {
          payload: {
            exitCode: 1,
            success: false,
            timedOut: false,
            stdout: "",
            stderr: "src/app.ts:1:1 build failed\n",
          },
        };
      }

      const patched = await fs.readFile(path.join(worktreeRoot, "src", "app.ts"), "utf-8");
      expect(patched).toContain("value = 2");
      return { payload: { exitCode: 0, success: true, timedOut: false, stdout: "", stderr: "" } };
    });

    const res = await executeProposalPlan({
      planDir,
      cfg: {} as OpenClawConfig,
      opts: { sandbox: false, repair: true, repairAttempts: 1, maxAttempts: 2, node: "node-123" },
      deps: { callGateway: callGateway as unknown as typeof callGateway, llmClient },
    });

    expect(res.ok).toBe(true);
    expect(callGateway).toHaveBeenCalledTimes(4);
    expect(llmClient.completeText).toHaveBeenCalledTimes(1);

    const base = await fs.readFile(path.join(repoRoot, "src", "app.ts"), "utf-8");
    expect(base).toContain("value = 1");

    const patched = await fs.readFile(path.join(worktreeRoot, "src", "app.ts"), "utf-8");
    expect(patched).toContain("value = 2");

    const repairDir = path.join(planDir, "report", "repairs", "train.run");
    await expect(fs.stat(path.join(repairDir, "attempt-1.patch"))).resolves.toBeTruthy();
    await expect(
      fs.stat(path.join(repairDir, "attempt-1", "repair_evidence.json")),
    ).resolves.toBeTruthy();

    const nodeRes = res.results.find((r) => r.nodeId === "train.run");
    expect(nodeRes?.attempts.length).toBe(2);
    expect(nodeRes?.attempts[0]?.patch).toBeTruthy();
  });

  it("applies an LLM patch for GPU nodes via gpu scheduler when repo worktrees are enabled", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "train.run",
            type: "train",
            tool: "shell",
            inputs: ["cache/git/repo"],
            commands: ["node build.js"],
            resources: { gpuCount: 1 },
          },
        ],
        edges: [],
      },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "plan",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
      retry: {
        policies: [
          { id: "retry.unknown", category: "unknown", maxAttempts: 2, retryablePatterns: [] },
        ],
        defaultPolicyId: "retry.unknown",
      },
    });

    const repoRoot = path.join(planDir, "cache", "git", "repo");
    await fs.mkdir(path.join(repoRoot, "src"), { recursive: true });
    await fs.writeFile(path.join(repoRoot, "src", "app.ts"), "export const value = 1;\n", "utf-8");
    await runCommandWithTimeout(["git", "init"], { cwd: repoRoot, timeoutMs: 10_000 });
    await runCommandWithTimeout(["git", "config", "user.email", "test@example.com"], {
      cwd: repoRoot,
      timeoutMs: 10_000,
    });
    await runCommandWithTimeout(["git", "config", "user.name", "OpenClaw Test"], {
      cwd: repoRoot,
      timeoutMs: 10_000,
    });
    await runCommandWithTimeout(["git", "add", "-A"], { cwd: repoRoot, timeoutMs: 10_000 });
    await runCommandWithTimeout(["git", "commit", "-m", "init"], {
      cwd: repoRoot,
      timeoutMs: 10_000,
    });

    const llmClient = {
      modelKey: "test",
      completeText: vi.fn(async () => {
        return [
          "*** Begin Patch",
          "*** Update File: src/app.ts",
          "@@",
          "-export const value = 1;",
          "+export const value = 2;",
          "*** End Patch",
          "",
        ].join("\n");
      }),
    } satisfies ProposalLlmClient;

    const worktreeRoot = path.join(planDir, "cache", "worktrees", "repo", "test-plan");

    let submitCount = 0;
    const callGateway = vi.fn(async (opts: { method: string; params?: unknown }) => {
      if (opts.method === "node.list") {
        return {
          nodes: [
            {
              connected: true,
              commands: ["system.run"],
              resources: { gpuCount: 1 },
            },
          ],
        };
      }

      if (opts.method === "gpu.job.submit") {
        const paramsObj =
          opts.params && typeof opts.params === "object"
            ? (opts.params as Record<string, unknown>)
            : {};
        const exec =
          paramsObj.exec && typeof paramsObj.exec === "object"
            ? (paramsObj.exec as Record<string, unknown>)
            : {};
        expect(exec.cwd).toBe(worktreeRoot);

        submitCount += 1;
        if (submitCount === 2) {
          const patched = await fs.readFile(path.join(worktreeRoot, "src", "app.ts"), "utf-8");
          expect(patched).toContain("value = 2");
        }

        return { job: { jobId: `job-${submitCount}` } };
      }

      if (opts.method === "gpu.job.wait") {
        const paramsObj =
          opts.params && typeof opts.params === "object"
            ? (opts.params as Record<string, unknown>)
            : {};
        const jobId = typeof paramsObj.jobId === "string" ? paramsObj.jobId : "";
        if (jobId === "job-1") {
          return {
            done: true,
            job: {
              state: "failed",
              attempts: [
                {
                  ok: false,
                  exitCode: 1,
                  timedOut: false,
                  stdoutTail: "",
                  stderrTail: "src/app.ts:1:1 build failed\n",
                  error: "build failed",
                  startedAtMs: Date.now() - 10,
                  finishedAtMs: Date.now(),
                },
              ],
            },
          };
        }

        if (jobId === "job-2") {
          return {
            done: true,
            job: {
              state: "succeeded",
              attempts: [
                {
                  ok: true,
                  exitCode: 0,
                  timedOut: false,
                  stdoutTail: "ok\n",
                  stderrTail: "",
                  error: "",
                  startedAtMs: Date.now() - 10,
                  finishedAtMs: Date.now(),
                },
              ],
            },
          };
        }

        throw new Error(`Unexpected gpu.job.wait jobId: ${jobId || "<missing>"}`);
      }

      throw new Error(`Unexpected gateway method: ${opts.method}`);
    });

    const res = await executeProposalPlan({
      planDir,
      cfg: {} as OpenClawConfig,
      opts: { sandbox: false, repair: true, repairAttempts: 1, maxAttempts: 2 },
      deps: { callGateway: callGateway as unknown as typeof callGateway, llmClient },
    });

    expect(res.ok).toBe(true);
    expect(callGateway.mock.calls.map((call) => call[0].method)).toEqual([
      "gpu.job.submit",
      "gpu.job.wait",
      "gpu.job.submit",
      "gpu.job.wait",
    ]);
    expect(llmClient.completeText).toHaveBeenCalledTimes(1);

    const base = await fs.readFile(path.join(repoRoot, "src", "app.ts"), "utf-8");
    expect(base).toContain("value = 1");

    const patched = await fs.readFile(path.join(worktreeRoot, "src", "app.ts"), "utf-8");
    expect(patched).toContain("value = 2");

    const repairDir = path.join(planDir, "report", "repairs", "train.run");
    await expect(fs.stat(path.join(repairDir, "attempt-1.patch"))).resolves.toBeTruthy();
    await expect(
      fs.stat(path.join(repairDir, "attempt-1", "repair_evidence.json")),
    ).resolves.toBeTruthy();

    const nodeRes = res.results.find((r) => r.nodeId === "train.run");
    expect(nodeRes?.attempts.length).toBe(2);
    expect(nodeRes?.attempts[0]?.patch).toBeTruthy();
  });
});
