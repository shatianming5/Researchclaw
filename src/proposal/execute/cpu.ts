import type { CommandOptions, SpawnResult } from "../../process/exec.js";
import type { PlanNode, RetrySpec } from "../schema.js";
import type { ExecuteAttempt, ExecuteNodeResult } from "./types.js";
import { inferHostWorkdir } from "./node-utils.js";
import { classifyFailure, computeBackoffMs, resolveRetryPolicy } from "./retry.js";
import { sleepMs, tail } from "./utils.js";

export type CpuRepairHook = (params: {
  planDir: string;
  node: PlanNode;
  hostWorkdir: string;
  attempt: ExecuteAttempt;
  stdout: string;
  stderr: string;
}) => Promise<{ applied: boolean; patch?: ExecuteAttempt["patch"] }>;

export async function runCpuShellNode(params: {
  planDir: string;
  node: PlanNode;
  dryRun: boolean;
  commandTimeoutMs: number;
  maxAttempts: number;
  retryDelayMs: number;
  retrySpec: RetrySpec;
  hostWorkdirOverride?: string;
  runInSandbox: (argv: string[], options: CommandOptions) => Promise<SpawnResult>;
  maybeRepair?: CpuRepairHook;
}): Promise<ExecuteNodeResult> {
  const attempts: ExecuteAttempt[] = [];
  const nodeId = params.node.id;
  const commands = params.node.commands ?? [];
  if (commands.length === 0) {
    return {
      nodeId,
      type: params.node.type,
      tool: params.node.tool,
      status: "skipped",
      executor: "manual",
      attempts,
      outputs: params.node.outputs,
    };
  }

  const policy = resolveRetryPolicy({ retry: params.retrySpec, node: params.node });
  const policyAttempts = policy?.maxAttempts ?? params.maxAttempts;
  const maxAttempts = Math.max(1, Math.min(params.maxAttempts, policyAttempts));

  const hostWorkdir = params.hostWorkdirOverride ?? inferHostWorkdir(params.planDir, params.node);
  const raw = `set -e\n${commands.join("\n")}\n`;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const startedAt = new Date().toISOString();
    const started = Date.now();

    if (params.dryRun) {
      const finishedAt = new Date().toISOString();
      attempts.push({
        attempt,
        executor: "exec",
        startedAt,
        finishedAt,
        durationMs: Date.now() - started,
        ok: true,
      });
      return {
        nodeId,
        type: params.node.type,
        tool: params.node.tool,
        status: "dry_run",
        executor: "exec",
        attempts,
        outputs: params.node.outputs,
      };
    }

    const result = await params.runInSandbox(["sh", "-lc", raw], {
      cwd: hostWorkdir,
      timeoutMs: params.commandTimeoutMs,
    });

    const stdoutTail = tail(result.stdout);
    const stderrTail = tail(result.stderr);
    const ok = result.code === 0;
    const outputCombined = [result.stderr, result.stdout].filter(Boolean).join("\n");
    const failureCategory = ok
      ? undefined
      : classifyFailure({ retry: params.retrySpec, node: params.node, output: outputCombined });

    const attemptRecord: ExecuteAttempt = {
      attempt,
      executor: "exec",
      startedAt,
      finishedAt: new Date().toISOString(),
      durationMs: Date.now() - started,
      ok,
      exitCode: result.code,
      timedOut: result.killed,
      stdoutTail: stdoutTail || undefined,
      stderrTail: stderrTail || undefined,
      failureCategory,
    };

    if (ok) {
      attempts.push(attemptRecord);
      return {
        nodeId,
        type: params.node.type,
        tool: params.node.tool,
        status: "ok",
        executor: "exec",
        attempts,
        outputs: params.node.outputs,
      };
    }

    // Record the failed attempt before any repair action.
    attempts.push(attemptRecord);

    if (params.maybeRepair) {
      try {
        const repaired = await params.maybeRepair({
          planDir: params.planDir,
          node: params.node,
          hostWorkdir,
          attempt: attemptRecord,
          stdout: result.stdout,
          stderr: result.stderr,
        });
        if (repaired.applied && repaired.patch) {
          attemptRecord.patch = repaired.patch;
        }
      } catch {
        // Ignore repair failures; retry policy still applies.
      }
    }

    if (attempt < maxAttempts) {
      const backoffMs = computeBackoffMs(policy, attempt, params.retryDelayMs);
      await sleepMs(backoffMs);
    }
  }

  return {
    nodeId,
    type: params.node.type,
    tool: params.node.tool,
    status: "failed",
    executor: "exec",
    attempts,
    outputs: params.node.outputs,
  };
}
