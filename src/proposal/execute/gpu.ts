import type { PlanNode, RetrySpec } from "../schema.js";
import type { ExecuteAttempt, ExecuteNodeResult, GatewayCallLike } from "./types.js";
import { randomIdempotencyKey } from "../../gateway/call.js";
import { inferHostWorkdir } from "./node-utils.js";
import { classifyFailure, computeBackoffMs, resolveRetryPolicy } from "./retry.js";
import { sleepMs, tail } from "./utils.js";

function hasEligibleConnectedGpuNode(params: {
  listPayload: unknown;
  required: { gpuCount: number; gpuType?: string; gpuMemGB?: number };
}): boolean {
  const obj =
    params.listPayload && typeof params.listPayload === "object"
      ? (params.listPayload as Record<string, unknown>)
      : {};
  const nodes = Array.isArray(obj.nodes) ? obj.nodes : [];
  const reqType = params.required.gpuType?.trim().toLowerCase();
  const reqMem = params.required.gpuMemGB;
  const reqCount = Math.max(1, Math.floor(params.required.gpuCount));

  for (const entry of nodes) {
    if (!entry || typeof entry !== "object") {
      continue;
    }
    const node = entry as Record<string, unknown>;
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

export async function runGpuNodeViaGateway(params: {
  planDir: string;
  node: PlanNode;
  nodeId: string;
  dryRun: boolean;
  commandTimeoutMs: number;
  maxAttempts: number;
  retryDelayMs: number;
  retrySpec: RetrySpec;
  gatewayUrl?: string;
  gatewayToken?: string;
  gatewayTimeoutMs: number;
  invokeTimeoutMs: number;
  nodeApprove: "off" | "allow-once" | "allow-always";
  callGateway: GatewayCallLike;
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

  const hostWorkdir = inferHostWorkdir(params.planDir, params.node);
  const raw = `set -e\n${commands.join("\n")}\n`;

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

    attempts.push({
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
    });

    if (ok) {
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

    if (attempt < maxAttempts) {
      const backoffMs = computeBackoffMs(policy, attempt, params.retryDelayMs);
      await sleepMs(backoffMs);
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
  gatewayUrl?: string;
  gatewayToken?: string;
  gatewayTimeoutMs: number;
  invokeTimeoutMs: number;
  nodeApprove: "off" | "allow-once" | "allow-always";
  callGateway: GatewayCallLike;
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

  const listPayload = await params.callGateway({
    url: params.gatewayUrl,
    token: params.gatewayToken,
    method: "node.list",
    params: {},
    timeoutMs: params.gatewayTimeoutMs,
  });
  if (!hasEligibleConnectedGpuNode({ listPayload, required })) {
    const startedAt = new Date().toISOString();
    attempts.push({
      attempt: 1,
      executor: "node.invoke",
      startedAt,
      finishedAt: new Date().toISOString(),
      durationMs: 0,
      ok: false,
      error: "no eligible GPU nodes connected (need system.run + resources.gpuCount)",
      failureCategory: "unknown",
    });
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

  const approvalDecision =
    params.nodeApprove === "allow-once" || params.nodeApprove === "allow-always"
      ? params.nodeApprove
      : null;
  const approved = approvalDecision !== null;

  const hostWorkdir = inferHostWorkdir(params.planDir, params.node);
  const raw = `set -e\n${commands.join("\n")}\n`;

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
        env: params.node.env,
        commandTimeoutMs: params.commandTimeoutMs,
        invokeTimeoutMs: params.invokeTimeoutMs,
        approved,
        approvalDecision,
      },
      maxAttempts,
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
    const startedAt = new Date().toISOString();
    attempts.push({
      attempt: 1,
      executor: "node.invoke",
      startedAt,
      finishedAt: new Date().toISOString(),
      durationMs: 0,
      ok: false,
      error: "gpu.job.submit returned no jobId",
      failureCategory: "unknown",
    });
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

  const overallTimeoutMs = Math.max(
    params.invokeTimeoutMs,
    params.commandTimeoutMs * maxAttempts + 60_000,
  );
  const deadline = Date.now() + overallTimeoutMs;

  let finalJob: Record<string, unknown> | null = null;
  while (true) {
    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) {
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
  }

  const state = typeof finalJob?.state === "string" ? finalJob.state : "failed";
  const jobAttempts = Array.isArray(finalJob?.attempts) ? (finalJob?.attempts as unknown[]) : [];
  for (const entry of jobAttempts) {
    if (!entry || typeof entry !== "object") {
      continue;
    }
    const a = entry as Record<string, unknown>;
    const startedAtMs = typeof a.startedAtMs === "number" ? a.startedAtMs : Date.now();
    const finishedAtMs = typeof a.finishedAtMs === "number" ? a.finishedAtMs : startedAtMs;
    const stdoutTail = typeof a.stdoutTail === "string" ? a.stdoutTail : "";
    const stderrTail = typeof a.stderrTail === "string" ? a.stderrTail : "";
    const outputCombined = [stderrTail, stdoutTail].filter(Boolean).join("\n");
    const ok = a.ok === true;
    attempts.push({
      attempt: typeof a.attempt === "number" ? a.attempt : attempts.length + 1,
      executor: "node.invoke",
      startedAt: new Date(startedAtMs).toISOString(),
      finishedAt: new Date(finishedAtMs).toISOString(),
      durationMs: Math.max(0, finishedAtMs - startedAtMs),
      ok,
      exitCode:
        typeof a.exitCode === "number" ? a.exitCode : a.exitCode === null ? null : undefined,
      timedOut: a.timedOut === true,
      stdoutTail: stdoutTail || undefined,
      stderrTail: stderrTail || undefined,
      failureCategory: ok
        ? undefined
        : classifyFailure({ retry: params.retrySpec, node: params.node, output: outputCombined }),
      error: typeof a.error === "string" && a.error.trim() ? a.error : undefined,
    });
  }

  const ok =
    state === "succeeded" ||
    (state === "running" && attempts.length > 0 && attempts.at(-1)?.ok === true);

  const status = ok
    ? ("ok" as const)
    : state === "canceled"
      ? ("skipped" as const)
      : ("failed" as const);
  return {
    nodeId: nodeKey,
    type: params.node.type,
    tool: params.node.tool,
    status,
    executor: "node.invoke",
    attempts,
    outputs: params.node.outputs,
  };
}
