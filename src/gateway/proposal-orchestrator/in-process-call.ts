import type { OpenClawConfig } from "../../config/config.js";
import type { GatewayCallLike } from "../../proposal/execute/types.js";
import type { GpuScheduler } from "../gpu-scheduler/scheduler.js";
import type { GpuApprovalDecision, GpuJobExecSpec } from "../gpu-scheduler/types.js";
import type { NodeRegistry } from "../node-registry.js";
import { isNodeCommandAllowed, resolveNodeCommandAllowlist } from "../node-command-policy.js";

function safeParseJson(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    return null;
  }
}

export function createInProcessGatewayCall(params: {
  nodeRegistry: NodeRegistry;
  gpuScheduler: GpuScheduler;
  loadConfig: () => OpenClawConfig;
}): GatewayCallLike {
  return async <T = Record<string, unknown>>(opts: {
    url?: string;
    token?: string;
    method: string;
    params?: unknown;
    timeoutMs?: number;
  }): Promise<T> => {
    if (opts.method === "node.list") {
      const nodes = params.nodeRegistry.listConnected().map((node) => ({
        nodeId: node.nodeId,
        displayName: node.displayName,
        platform: node.platform,
        version: node.version,
        coreVersion: node.coreVersion,
        uiVersion: node.uiVersion,
        deviceFamily: node.deviceFamily,
        modelIdentifier: node.modelIdentifier,
        remoteIp: node.remoteIp,
        caps: node.caps,
        commands: node.commands,
        pathEnv: node.pathEnv,
        permissions: node.permissions,
        resources: node.resources,
        connectedAtMs: node.connectedAtMs,
        paired: false,
        connected: true,
      }));
      return { ts: Date.now(), nodes } as T;
    }

    if (opts.method === "node.invoke") {
      const p =
        opts.params && typeof opts.params === "object"
          ? (opts.params as Record<string, unknown>)
          : {};
      const nodeId = typeof p.nodeId === "string" ? p.nodeId.trim() : "";
      const command = typeof p.command === "string" ? p.command.trim() : "";
      if (!nodeId || !command) {
        throw new Error("node.invoke requires nodeId and command");
      }

      const nodeSession = params.nodeRegistry.get(nodeId);
      if (!nodeSession) {
        throw new Error("node not connected");
      }

      const cfg = params.loadConfig();
      const allowlist = resolveNodeCommandAllowlist(cfg, nodeSession);
      const allowed = isNodeCommandAllowed({
        command,
        declaredCommands: nodeSession.commands,
        allowlist,
      });
      if (!allowed.ok) {
        throw new Error(`node command not allowed: ${allowed.reason}`);
      }

      const res = await params.nodeRegistry.invoke({
        nodeId,
        command,
        params: p.params,
        timeoutMs: typeof p.timeoutMs === "number" ? p.timeoutMs : undefined,
        idempotencyKey: typeof p.idempotencyKey === "string" ? p.idempotencyKey : undefined,
      });
      if (!res.ok) {
        throw new Error(res.error?.message ?? "node invoke failed");
      }

      const payload = res.payloadJSON ? safeParseJson(res.payloadJSON) : res.payload;
      return {
        ok: true,
        nodeId,
        command,
        payload,
        payloadJSON: res.payloadJSON ?? null,
      } as T;
    }

    if (opts.method === "gpu.job.submit") {
      const p =
        opts.params && typeof opts.params === "object"
          ? (opts.params as Record<string, unknown>)
          : {};
      const execRaw =
        p.exec && typeof p.exec === "object" ? (p.exec as Record<string, unknown>) : {};
      const approvalDecisionRaw =
        typeof execRaw.approvalDecision === "string" ? execRaw.approvalDecision.trim() : "";
      const approvalDecision: GpuApprovalDecision | undefined =
        approvalDecisionRaw === "allow-once" || approvalDecisionRaw === "allow-always"
          ? approvalDecisionRaw
          : undefined;
      const exec: GpuJobExecSpec = {
        command: Array.isArray(execRaw.command) ? execRaw.command.map((part) => String(part)) : [],
        rawCommand: typeof execRaw.rawCommand === "string" ? execRaw.rawCommand : undefined,
        cwd: typeof execRaw.cwd === "string" ? execRaw.cwd : undefined,
        env:
          execRaw.env && typeof execRaw.env === "object"
            ? (execRaw.env as Record<string, string>)
            : undefined,
        commandTimeoutMs:
          typeof execRaw.commandTimeoutMs === "number" ? execRaw.commandTimeoutMs : undefined,
        invokeTimeoutMs:
          typeof execRaw.invokeTimeoutMs === "number" ? execRaw.invokeTimeoutMs : undefined,
        approved: execRaw.approved === true,
        approvalDecision,
      };
      const job = await params.gpuScheduler.submit({
        resources: (p.resources ?? {}) as { gpuCount: number; gpuType?: string; gpuMemGB?: number },
        exec,
        maxAttempts: typeof p.maxAttempts === "number" ? p.maxAttempts : undefined,
      });
      return { job } as T;
    }

    if (opts.method === "gpu.job.get") {
      const p =
        opts.params && typeof opts.params === "object"
          ? (opts.params as Record<string, unknown>)
          : {};
      const jobId = typeof p.jobId === "string" ? p.jobId.trim() : "";
      if (!jobId) {
        throw new Error("gpu.job.get requires jobId");
      }
      const job = params.gpuScheduler.get(jobId);
      if (!job) {
        throw new Error("unknown jobId");
      }
      return { job } as T;
    }

    if (opts.method === "gpu.job.list") {
      const p =
        opts.params && typeof opts.params === "object"
          ? (opts.params as Record<string, unknown>)
          : {};
      const state = typeof p.state === "string" ? p.state : undefined;
      const jobs = params.gpuScheduler.list({
        state:
          state === "queued" ||
          state === "running" ||
          state === "succeeded" ||
          state === "failed" ||
          state === "canceled"
            ? state
            : undefined,
      });
      return { jobs } as T;
    }

    if (opts.method === "gpu.job.cancel") {
      const p =
        opts.params && typeof opts.params === "object"
          ? (opts.params as Record<string, unknown>)
          : {};
      const jobId = typeof p.jobId === "string" ? p.jobId.trim() : "";
      if (!jobId) {
        throw new Error("gpu.job.cancel requires jobId");
      }
      const res = await params.gpuScheduler.cancel(jobId);
      if (!res.ok) {
        throw new Error(res.reason ?? "cancel failed");
      }
      return { ok: true } as T;
    }

    if (opts.method === "gpu.job.pause") {
      const p =
        opts.params && typeof opts.params === "object"
          ? (opts.params as Record<string, unknown>)
          : {};
      const jobId = typeof p.jobId === "string" ? p.jobId.trim() : "";
      if (!jobId) {
        throw new Error("gpu.job.pause requires jobId");
      }
      const res = await params.gpuScheduler.pause(jobId);
      if (!res.ok) {
        throw new Error(res.reason ?? "pause failed");
      }
      return { ok: true } as T;
    }

    if (opts.method === "gpu.job.resume") {
      const p =
        opts.params && typeof opts.params === "object"
          ? (opts.params as Record<string, unknown>)
          : {};
      const jobId = typeof p.jobId === "string" ? p.jobId.trim() : "";
      if (!jobId) {
        throw new Error("gpu.job.resume requires jobId");
      }
      const res = await params.gpuScheduler.resume(jobId);
      if (!res.ok) {
        throw new Error(res.reason ?? "resume failed");
      }
      return { ok: true } as T;
    }

    if (opts.method === "gpu.job.wait") {
      const p =
        opts.params && typeof opts.params === "object"
          ? (opts.params as Record<string, unknown>)
          : {};
      const jobId = typeof p.jobId === "string" ? p.jobId.trim() : "";
      if (!jobId) {
        throw new Error("gpu.job.wait requires jobId");
      }
      const timeoutMs =
        typeof p.timeoutMs === "number" && Number.isFinite(p.timeoutMs)
          ? Math.max(0, p.timeoutMs)
          : 30_000;
      const job = await params.gpuScheduler.wait(jobId, timeoutMs);
      if (!job) {
        throw new Error("unknown jobId");
      }
      const done = job.state === "succeeded" || job.state === "failed" || job.state === "canceled";
      return { done, job } as T;
    }

    throw new Error(`Unsupported in-process gateway method: ${opts.method}`);
  };
}
