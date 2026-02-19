import type { NodeResources } from "../node-registry.js";

export type GpuApprovalDecision = "allow-once" | "allow-always";

export type GpuJobState = "queued" | "running" | "succeeded" | "failed" | "canceled";

export type GpuJobResourceRequest = {
  gpuCount: number;
  gpuType?: string;
  gpuMemGB?: number;
  cpuCores?: number;
  ramGB?: number;
};

export type GpuJobExecSpec = {
  /**
   * argv for system.run
   * Example: ["sh", "-lc", "python train.py"]
   */
  command: string[];
  rawCommand?: string;
  cwd?: string;
  env?: Record<string, string>;
  /**
   * Timeout for the remote command itself (system.run timeoutMs).
   */
  commandTimeoutMs?: number;
  /**
   * Timeout for node.invoke round trip.
   * When omitted, scheduler will add slack on top of commandTimeoutMs.
   */
  invokeTimeoutMs?: number;
  approved?: boolean;
  approvalDecision?: GpuApprovalDecision;
};

export type GpuJobPolicyWindow = {
  /**
   * Days of week for this window (e.g. ["mon","tue"]).
   * When omitted or empty, applies to all days.
   */
  days?: string[];
  /**
   * Start time (HH:MM, 24h).
   */
  start: string;
  /**
   * End time (HH:MM, 24h). If end < start, the window wraps past midnight.
   */
  end: string;
  /**
   * Optional IANA timezone for evaluating this window (e.g. "America/Los_Angeles").
   * When omitted, scheduler uses the gateway host timezone.
   */
  tz?: string;
};

export type GpuJobPolicy = {
  autoPause?: boolean;
  autoResume?: boolean;
  windows?: GpuJobPolicyWindow[];
};

export type GpuJobSubmitRequest = {
  resources: GpuJobResourceRequest;
  exec: GpuJobExecSpec;
  maxAttempts?: number;
  policy?: GpuJobPolicy;
};

export type GpuJobAttempt = {
  attempt: number;
  nodeId: string;
  startedAtMs: number;
  finishedAtMs?: number;
  ok?: boolean;
  exitCode?: number | null;
  timedOut?: boolean;
  stdoutTail?: string;
  stderrTail?: string;
  error?: string;
};

export type GpuJobResult = {
  exitCode: number | null;
  timedOut: boolean;
  success: boolean;
  stdoutTail?: string;
  stderrTail?: string;
};

export type GpuJob = {
  jobId: string;
  createdAtMs: number;
  updatedAtMs: number;
  state: GpuJobState;
  /**
   * When true, this job remains queued but won't be dispatched.
   * Only applies to queued jobs, but may be set while running when a pause is requested.
   */
  paused?: boolean;
  pausedReason?: "manual" | "policy";
  /**
   * When true, the scheduler will preempt a running job and requeue it as paused.
   */
  pauseRequested?: boolean;
  notBeforeMs?: number;
  policy?: GpuJobPolicy;
  resources: GpuJobResourceRequest;
  exec: GpuJobExecSpec;
  maxAttempts: number;
  assignedNodeId?: string;
  attempts: GpuJobAttempt[];
  result?: GpuJobResult;
  cancelRequested?: boolean;
};

export type GpuNodeCandidate = {
  nodeId: string;
  resources: NodeResources;
  allocatedGpu: number;
};
