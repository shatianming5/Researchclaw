import path from "node:path";
import type { PlanNode, RetrySpec } from "../schema.js";
import type { CpuRepairHook } from "./cpu.js";
import type { ExecuteAttempt, ExecuteNodeResult, GatewayCallLike } from "./types.js";
import { randomIdempotencyKey } from "../../gateway/call.js";
import { inferHostWorkdir } from "./node-utils.js";
import {
  collectMetricsSnapshot,
  finalizeRepairEvidence,
  recordPendingRepair,
  writeAppliedOnlyRepairEvidence,
  type PendingRepair,
} from "./repair-evidence.js";
import { classifyFailure, computeBackoffMs, resolveRetryPolicy } from "./retry.js";
import { sleepMs, tail } from "./utils.js";

function hasEligibleConnectedGpuNode(params: {
  listPayload: unknown;
  required: { gpuCount: number; gpuType?: string; gpuMemGB?: number };
  nodeId?: string;
}): boolean {
  const obj =
    params.listPayload && typeof params.listPayload === "object"
      ? (params.listPayload as Record<string, unknown>)
      : {};
  const nodes = Array.isArray(obj.nodes) ? obj.nodes : [];
  const targetNodeId = params.nodeId?.trim();
  const reqType = params.required.gpuType?.trim().toLowerCase();
  const reqMem = params.required.gpuMemGB;
  const reqCount = Math.max(1, Math.floor(params.required.gpuCount));

  for (const entry of nodes) {
    if (!entry || typeof entry !== "object") {
      continue;
    }
    const node = entry as Record<string, unknown>;
    if (targetNodeId) {
      const nodeId = typeof node.nodeId === "string" ? node.nodeId : "";
      if (nodeId.trim() !== targetNodeId) {
        continue;
      }
    }
    if (node.connected !== true) {
      continue;
    }
    const commands = Array.isArray(node.commands) ? node.commands : [];
    if (!commands.includes("system.run")) {
      continue;
    }
    const resources =
      node.resources && typeof node.resources === "object"
        ? (node.resources as Record<string, unknown>)
        : {};
    const gpuCount = typeof resources.gpuCount === "number" ? resources.gpuCount : 0;
    if (!Number.isFinite(gpuCount) || gpuCount < reqCount) {
      continue;
    }
    if (reqType) {
      const gpuType =
        typeof resources.gpuType === "string" ? resources.gpuType.trim().toLowerCase() : "";
      if (!gpuType || gpuType !== reqType) {
        continue;
      }
    }
    if (typeof reqMem === "number" && Number.isFinite(reqMem)) {
      const mem = typeof resources.gpuMemGB === "number" ? resources.gpuMemGB : NaN;
      if (!Number.isFinite(mem) || mem < reqMem) {
        continue;
      }
    }
    return true;
  }
  return false;
}

async function waitForEligibleConnectedGpuNode(params: {
  callGateway: GatewayCallLike;
  gatewayUrl?: string;
  gatewayToken?: string;
  gatewayTimeoutMs: number;
  required: { gpuCount: number; gpuType?: string; gpuMemGB?: number };
  timeoutMs: number;
  nodeId?: string;
}): Promise<
  | { ok: true; polls: number; waitedMs: number }
  | { ok: false; polls: number; waitedMs: number; error: string }
> {
  const timeoutMs = Math.max(0, Math.floor(params.timeoutMs));
  const pollIntervalMs = 1_000;
  const started = Date.now();
  let polls = 0;

  while (true) {
    polls += 1;
    const listPayload = await params.callGateway({
      url: params.gatewayUrl,
      token: params.gatewayToken,
      method: "node.list",
      params: {},
      timeoutMs: params.gatewayTimeoutMs,
    });

    if (
      hasEligibleConnectedGpuNode({
        listPayload,
        required: params.required,
        nodeId: params.nodeId,
      })
    ) {
      return { ok: true, polls, waitedMs: Date.now() - started };
    }

    const elapsed = Date.now() - started;
    if (timeoutMs <= 0) {
      return {
        ok: false,
        polls,
        waitedMs: elapsed,
        error: "no eligible GPU nodes connected (need system.run + resources.gpuCount)",
      };
    }
    if (elapsed >= timeoutMs) {
      return {
        ok: false,
        polls,
        waitedMs: elapsed,
        error: `no eligible GPU nodes connected after waiting ${Math.ceil(elapsed / 1000)}s (need system.run + resources.gpuCount)`,
      };
    }

    const remaining = Math.max(0, timeoutMs - elapsed);
    await sleepMs(Math.min(pollIntervalMs, remaining));
  }
}

export async function runGpuNodeViaGateway(params: {
  planDir: string;
  node: PlanNode;
  nodeId: string;
  dryRun: boolean;
  commandTimeoutMs: number;
  maxAttempts: number;
  retryDelayMs: number;
  retrySpec: RetrySpec;
  hostWorkdirOverride?: string;
  gatewayUrl?: string;
  gatewayToken?: string;
  gatewayTimeoutMs: number;
  invokeTimeoutMs: number;
  gpuWaitTimeoutMs?: number;
  nodeApprove: "off" | "allow-once" | "allow-always";
  callGateway: GatewayCallLike;
  maybeRepair?: CpuRepairHook;
}): Promise<ExecuteNodeResult> {
  const attempts: ExecuteAttempt[] = [];
  const nodeKey = params.node.id;
  const commands = params.node.commands ?? [];
  if (commands.length === 0) {
    return {
      nodeId: nodeKey,
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

  const required = {
    gpuCount: params.node.resources?.gpuCount ?? 1,
    gpuType: params.node.resources?.gpuType,
    gpuMemGB: params.node.resources?.gpuMemGB,
  };

  const hostWorkdir = params.hostWorkdirOverride ?? inferHostWorkdir(params.planDir, params.node);
  const raw = `set -e\n${commands.join("\n")}\n`;
  const workdirRel = path.relative(params.planDir, hostWorkdir).replaceAll("\\", "/") || ".";
  let pendingRepair: PendingRepair | null = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const startedAt = new Date().toISOString();
    const started = Date.now();

    if (params.dryRun) {
      attempts.push({
        attempt,
        executor: "node.invoke",
        startedAt,
        finishedAt: new Date().toISOString(),
        durationMs: Date.now() - started,
        ok: true,
      });
      return {
        nodeId: nodeKey,
        type: params.node.type,
        tool: params.node.tool,
        status: "dry_run",
        executor: "node.invoke",
        attempts,
        outputs: params.node.outputs,
      };
    }

    const nodeWait = await waitForEligibleConnectedGpuNode({
      callGateway: params.callGateway,
      gatewayUrl: params.gatewayUrl,
      gatewayToken: params.gatewayToken,
      gatewayTimeoutMs: params.gatewayTimeoutMs,
      required,
      timeoutMs: params.gpuWaitTimeoutMs ?? 0,
      nodeId: params.nodeId,
    });
    if (!nodeWait.ok) {
      attempts.push({
        attempt,
        executor: "node.invoke",
        startedAt,
        finishedAt: new Date().toISOString(),
        durationMs: Date.now() - started,
        ok: false,
        error: nodeWait.error,
        failureCategory: "unknown",
      });
      break;
    }

    const approvalDecision =
      params.nodeApprove === "allow-once" || params.nodeApprove === "allow-always"
        ? params.nodeApprove
        : null;
    const approved = approvalDecision !== null;

    const invokeParams: Record<string, unknown> = {
      nodeId: params.nodeId,
      command: "system.run",
      params: {
        command: ["sh", "-lc", raw],
        cwd: hostWorkdir,
        env: params.node.env,
        timeoutMs: params.commandTimeoutMs,
        approved,
        approvalDecision,
      },
      idempotencyKey: randomIdempotencyKey(),
      timeoutMs: params.invokeTimeoutMs,
    };

    const res = await params.callGateway({
      url: params.gatewayUrl,
      token: params.gatewayToken,
      method: "node.invoke",
      params: invokeParams,
      timeoutMs: params.gatewayTimeoutMs,
    });

    const payload =
      typeof res === "object" && res !== null ? (res as { payload?: unknown }).payload : undefined;
    const obj = payload && typeof payload === "object" ? (payload as Record<string, unknown>) : {};
    const stdout = typeof obj.stdout === "string" ? obj.stdout : "";
    const stderr = typeof obj.stderr === "string" ? obj.stderr : "";
    const exitCode = typeof obj.exitCode === "number" ? obj.exitCode : null;
    const timedOut = obj.timedOut === true;
    const success = obj.success === true;
    const ok = success && !timedOut && (exitCode === null || exitCode === 0);

    const outputCombined = [stderr, stdout].filter(Boolean).join("\n");
    const failureCategory = ok
      ? undefined
      : classifyFailure({ retry: params.retrySpec, node: params.node, output: outputCombined });

    const attemptRecord: ExecuteAttempt = {
      attempt,
      executor: "node.invoke",
      startedAt,
      finishedAt: new Date().toISOString(),
      durationMs: Date.now() - started,
      ok,
      exitCode,
      timedOut,
      stdoutTail: tail(stdout) || undefined,
      stderrTail: tail(stderr) || undefined,
      failureCategory,
      error:
        ok || (!stderr.trim() && !stdout.trim())
          ? undefined
          : `node.invoke failed (exit=${exitCode ?? "null"}, timedOut=${timedOut})`,
    };
    attempts.push(attemptRecord);

    if (ok) {
      if (pendingRepair && pendingRepair.rerunAttempt === attempt) {
        try {
          await finalizeRepairEvidence({
            planDir: params.planDir,
            workdirRel,
            node: {
              id: params.node.id,
              type: params.node.type,
              tool: params.node.tool,
              commands: params.node.commands,
            },
            pending: pendingRepair,
            after: {
              ok: true,
              exitCode,
              timedOut,
              failureCategory: undefined,
              stdout,
              stderr,
              stdoutTail: attemptRecord.stdoutTail,
              stderrTail: attemptRecord.stderrTail,
            },
            metricsAfter: await collectMetricsSnapshot(params.planDir),
          });
        } catch {
          // Ignore evidence failures.
        }
        pendingRepair = null;
      }
      return {
        nodeId: nodeKey,
        type: params.node.type,
        tool: params.node.tool,
        status: "ok",
        executor: "node.invoke",
        attempts,
        outputs: params.node.outputs,
      };
    }

    if (pendingRepair && pendingRepair.rerunAttempt === attempt) {
      try {
        await finalizeRepairEvidence({
          planDir: params.planDir,
          workdirRel,
          node: {
            id: params.node.id,
            type: params.node.type,
            tool: params.node.tool,
            commands: params.node.commands,
          },
          pending: pendingRepair,
          after: {
            ok: false,
            exitCode,
            timedOut,
            failureCategory: failureCategory ?? "unknown",
            stdout,
            stderr,
            stdoutTail: attemptRecord.stdoutTail,
            stderrTail: attemptRecord.stderrTail,
          },
          metricsAfter: await collectMetricsSnapshot(params.planDir),
        });
      } catch {
        // Ignore evidence failures.
      }
      pendingRepair = null;
    }

    if (params.maybeRepair) {
      try {
        const repaired = await params.maybeRepair({
          planDir: params.planDir,
          node: params.node,
          hostWorkdir,
          attempt: attemptRecord,
          stdout,
          stderr,
        });
        if (repaired.applied && repaired.patch) {
          attemptRecord.patch = repaired.patch;
          const pending: PendingRepair = {
            nodeId: nodeKey,
            patchAttempt: attempt,
            rerunAttempt: attempt + 1,
            patchPath: repaired.patch.patchPath,
            patchSummary: repaired.patch.summary,
            before: {
              ok: false,
              exitCode,
              timedOut,
              failureCategory: failureCategory ?? "unknown",
              stdout,
              stderr,
              stdoutTail: attemptRecord.stdoutTail,
              stderrTail: attemptRecord.stderrTail,
            },
            metricsBefore: await collectMetricsSnapshot(params.planDir),
            warnings: [],
          };
          pendingRepair = await recordPendingRepair({
            planDir: params.planDir,
            workdirRel,
            node: {
              id: params.node.id,
              type: params.node.type,
              tool: params.node.tool,
              commands: params.node.commands,
            },
            pending,
          });
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

  if (pendingRepair) {
    try {
      await writeAppliedOnlyRepairEvidence({
        planDir: params.planDir,
        workdirRel,
        node: {
          id: params.node.id,
          type: params.node.type,
          tool: params.node.tool,
          commands: params.node.commands,
        },
        pending: pendingRepair,
      });
    } catch {
      // Ignore evidence failures.
    }
  }

  return {
    nodeId: nodeKey,
    type: params.node.type,
    tool: params.node.tool,
    status: "failed",
    executor: "node.invoke",
    attempts,
    outputs: params.node.outputs,
  };
}

export async function runGpuNodeViaScheduler(params: {
  planDir: string;
  node: PlanNode;
  dryRun: boolean;
  commandTimeoutMs: number;
  maxAttempts: number;
  retryDelayMs: number;
  retrySpec: RetrySpec;
  hostWorkdirOverride?: string;
  gatewayUrl?: string;
  gatewayToken?: string;
  gatewayTimeoutMs: number;
  invokeTimeoutMs: number;
  gpuWaitTimeoutMs?: number;
  nodeApprove: "off" | "allow-once" | "allow-always";
  callGateway: GatewayCallLike;
  maybeRepair?: CpuRepairHook;
}): Promise<ExecuteNodeResult> {
  const attempts: ExecuteAttempt[] = [];
  const nodeKey = params.node.id;
  const commands = params.node.commands ?? [];
  if (commands.length === 0) {
    return {
      nodeId: nodeKey,
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

  const required = {
    gpuCount: params.node.resources?.gpuCount ?? 1,
    gpuType: params.node.resources?.gpuType,
    gpuMemGB: params.node.resources?.gpuMemGB,
  };

  if (params.dryRun) {
    attempts.push({
      attempt: 1,
      executor: "node.invoke",
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
      durationMs: 0,
      ok: true,
    });
    return {
      nodeId: nodeKey,
      type: params.node.type,
      tool: params.node.tool,
      status: "dry_run",
      executor: "node.invoke",
      attempts,
      outputs: params.node.outputs,
    };
  }

  const approvalDecision =
    params.nodeApprove === "allow-once" || params.nodeApprove === "allow-always"
      ? params.nodeApprove
      : null;
  const approved = approvalDecision !== null;

  const hostWorkdir = params.hostWorkdirOverride ?? inferHostWorkdir(params.planDir, params.node);
  const submitEnv = params.node.env
    ? { ...params.node.env, OPENCLAW_PLAN_DIR: path.resolve(params.planDir) }
    : { OPENCLAW_PLAN_DIR: path.resolve(params.planDir) };
  const raw = `set -e\n${commands.join("\n")}\n`;
  const workdirRel = path.relative(params.planDir, hostWorkdir).replaceAll("\\", "/") || ".";
  let pendingRepair: PendingRepair | null = null;

  const overallTimeoutMs = Math.max(
    params.invokeTimeoutMs,
    params.commandTimeoutMs * maxAttempts + 60_000,
  );

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const startedAt = new Date().toISOString();
    const started = Date.now();

    const submitRes = await params.callGateway({
      url: params.gatewayUrl,
      token: params.gatewayToken,
      method: "gpu.job.submit",
      params: {
        resources: {
          gpuCount: required.gpuCount ?? 1,
          gpuType: required.gpuType,
          gpuMemGB: required.gpuMemGB,
          cpuCores: params.node.resources?.cpuCores,
          ramGB: params.node.resources?.ramGB,
        },
        exec: {
          command: ["sh", "-lc", raw],
          cwd: hostWorkdir,
          env: submitEnv,
          commandTimeoutMs: params.commandTimeoutMs,
          invokeTimeoutMs: params.invokeTimeoutMs,
          approved,
          approvalDecision,
        },
        maxAttempts: 1,
      },
      timeoutMs: params.gatewayTimeoutMs,
    });

    const submitObj = submitRes && typeof submitRes === "object" ? submitRes : {};
    const job =
      submitObj.job && typeof submitObj.job === "object"
        ? (submitObj.job as Record<string, unknown>)
        : null;
    const jobId = job && typeof job.jobId === "string" ? job.jobId : null;
    if (!jobId) {
      const finishedAt = new Date().toISOString();
      const attemptRecord: ExecuteAttempt = {
        attempt,
        executor: "node.invoke",
        startedAt,
        finishedAt,
        durationMs: Date.now() - started,
        ok: false,
        error: "gpu.job.submit returned no jobId",
        failureCategory: "unknown",
      };
      attempts.push(attemptRecord);
      if (attempt < maxAttempts) {
        const backoffMs = computeBackoffMs(policy, attempt, params.retryDelayMs);
        await sleepMs(backoffMs);
        continue;
      }
      break;
    }

    const waitForStartMs = Math.max(0, Math.floor(params.gpuWaitTimeoutMs ?? 0));
    const startDeadline = waitForStartMs > 0 ? started + waitForStartMs : null;
    const deadline = started + overallTimeoutMs + waitForStartMs;
    let finalJob: Record<string, unknown> | null = null;
    let timedOutReason: string | null = null;
    while (true) {
      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) {
        timedOutReason = "timed out waiting for GPU job completion";
        break;
      }
      const waitTimeoutMs = Math.max(
        1000,
        Math.min(15_000, params.gatewayTimeoutMs - 1000, remainingMs),
      );
      const waitRes = await params.callGateway({
        url: params.gatewayUrl,
        token: params.gatewayToken,
        method: "gpu.job.wait",
        params: {
          jobId,
          timeoutMs: waitTimeoutMs,
        },
        timeoutMs: params.gatewayTimeoutMs,
      });
      const waitObj = waitRes && typeof waitRes === "object" ? waitRes : {};
      const done = waitObj.done === true;
      const maybeJob =
        waitObj.job && typeof waitObj.job === "object"
          ? (waitObj.job as Record<string, unknown>)
          : null;
      if (maybeJob) {
        finalJob = maybeJob;
      }
      if (done) {
        break;
      }

      if (startDeadline !== null && Date.now() >= startDeadline) {
        const stateNow = typeof finalJob?.state === "string" ? finalJob.state : "";
        const attemptsNow = Array.isArray(finalJob?.attempts)
          ? (finalJob?.attempts as unknown[])
          : [];
        const startedRemote = stateNow === "running" || attemptsNow.length > 0;
        if (!startedRemote) {
          timedOutReason = "timed out waiting for eligible GPU nodes to connect";
          break;
        }
      }
    }

    if (timedOutReason) {
      try {
        await params.callGateway({
          url: params.gatewayUrl,
          token: params.gatewayToken,
          method: "gpu.job.cancel",
          params: { jobId },
          timeoutMs: params.gatewayTimeoutMs,
        });
      } catch {
        // best-effort
      }
    }

    const state = typeof finalJob?.state === "string" ? finalJob.state : "failed";
    const jobAttempts = Array.isArray(finalJob?.attempts) ? (finalJob?.attempts as unknown[]) : [];
    const latest =
      jobAttempts.length > 0 && typeof jobAttempts.at(-1) === "object"
        ? (jobAttempts.at(-1) as Record<string, unknown>)
        : null;

    const startedAtMs = typeof latest?.startedAtMs === "number" ? latest.startedAtMs : started;
    const finishedAtMs =
      typeof latest?.finishedAtMs === "number" ? latest.finishedAtMs : Date.now();
    const stdoutTail = typeof latest?.stdoutTail === "string" ? latest.stdoutTail : "";
    const stderrTail = typeof latest?.stderrTail === "string" ? latest.stderrTail : "";
    const outputCombined = [stderrTail, stdoutTail].filter(Boolean).join("\n");
    const ok = timedOutReason ? false : latest?.ok === true || state === "succeeded";
    const failureCategory = ok
      ? undefined
      : classifyFailure({ retry: params.retrySpec, node: params.node, output: outputCombined });
    const errorText = typeof latest?.error === "string" ? latest.error.trim() : "";

    const attemptRecord: ExecuteAttempt = {
      attempt,
      executor: "node.invoke",
      startedAt: new Date(startedAtMs).toISOString(),
      finishedAt: new Date(finishedAtMs).toISOString(),
      durationMs: Math.max(0, finishedAtMs - startedAtMs),
      ok,
      exitCode:
        typeof latest?.exitCode === "number"
          ? latest.exitCode
          : latest?.exitCode === null
            ? null
            : undefined,
      timedOut: timedOutReason ? true : latest?.timedOut === true,
      stdoutTail: stdoutTail || undefined,
      stderrTail: stderrTail || undefined,
      failureCategory,
      error: timedOutReason ?? (errorText || undefined),
    };
    attempts.push(attemptRecord);

    if (state === "canceled") {
      return {
        nodeId: nodeKey,
        type: params.node.type,
        tool: params.node.tool,
        status: "skipped",
        executor: "node.invoke",
        attempts,
        outputs: params.node.outputs,
      };
    }

    if (ok) {
      if (pendingRepair && pendingRepair.rerunAttempt === attempt) {
        try {
          await finalizeRepairEvidence({
            planDir: params.planDir,
            workdirRel,
            node: {
              id: params.node.id,
              type: params.node.type,
              tool: params.node.tool,
              commands: params.node.commands,
            },
            pending: pendingRepair,
            after: {
              ok: true,
              exitCode: attemptRecord.exitCode ?? null,
              timedOut: attemptRecord.timedOut,
              failureCategory: undefined,
              stdout: stdoutTail,
              stderr: stderrTail,
              stdoutTail: attemptRecord.stdoutTail,
              stderrTail: attemptRecord.stderrTail,
            },
            metricsAfter: await collectMetricsSnapshot(params.planDir),
          });
        } catch {
          // Ignore evidence failures.
        }
        pendingRepair = null;
      }
      return {
        nodeId: nodeKey,
        type: params.node.type,
        tool: params.node.tool,
        status: "ok",
        executor: "node.invoke",
        attempts,
        outputs: params.node.outputs,
      };
    }

    if (pendingRepair && pendingRepair.rerunAttempt === attempt) {
      try {
        await finalizeRepairEvidence({
          planDir: params.planDir,
          workdirRel,
          node: {
            id: params.node.id,
            type: params.node.type,
            tool: params.node.tool,
            commands: params.node.commands,
          },
          pending: pendingRepair,
          after: {
            ok: false,
            exitCode: attemptRecord.exitCode ?? null,
            timedOut: attemptRecord.timedOut,
            failureCategory: attemptRecord.failureCategory ?? "unknown",
            stdout: stdoutTail,
            stderr: stderrTail,
            stdoutTail: attemptRecord.stdoutTail,
            stderrTail: attemptRecord.stderrTail,
          },
          metricsAfter: await collectMetricsSnapshot(params.planDir),
        });
      } catch {
        // Ignore evidence failures.
      }
      pendingRepair = null;
    }

    if (params.maybeRepair) {
      try {
        const repaired = await params.maybeRepair({
          planDir: params.planDir,
          node: params.node,
          hostWorkdir,
          attempt: attemptRecord,
          stdout: stdoutTail,
          stderr: stderrTail,
        });
        if (repaired.applied && repaired.patch) {
          attemptRecord.patch = repaired.patch;
          const pending: PendingRepair = {
            nodeId: nodeKey,
            patchAttempt: attempt,
            rerunAttempt: attempt + 1,
            patchPath: repaired.patch.patchPath,
            patchSummary: repaired.patch.summary,
            before: {
              ok: false,
              exitCode: attemptRecord.exitCode ?? null,
              timedOut: attemptRecord.timedOut,
              failureCategory: attemptRecord.failureCategory,
              stdout: stdoutTail,
              stderr: stderrTail,
              stdoutTail: attemptRecord.stdoutTail,
              stderrTail: attemptRecord.stderrTail,
            },
            metricsBefore: await collectMetricsSnapshot(params.planDir),
            warnings: ["GPU scheduler only provides stdout/stderr tails; full logs unavailable."],
          };
          pendingRepair = await recordPendingRepair({
            planDir: params.planDir,
            workdirRel,
            node: {
              id: params.node.id,
              type: params.node.type,
              tool: params.node.tool,
              commands: params.node.commands,
            },
            pending,
          });
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

  if (pendingRepair) {
    try {
      await writeAppliedOnlyRepairEvidence({
        planDir: params.planDir,
        workdirRel,
        node: {
          id: params.node.id,
          type: params.node.type,
          tool: params.node.tool,
          commands: params.node.commands,
        },
        pending: pendingRepair,
      });
    } catch {
      // Ignore evidence failures.
    }
  }

  return {
    nodeId: nodeKey,
    type: params.node.type,
    tool: params.node.tool,
    status: "failed",
    executor: "node.invoke",
    attempts,
    outputs: params.node.outputs,
  };
}
