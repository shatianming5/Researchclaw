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

export type GpuJobSubmitRequest = {
  resources: GpuJobResourceRequest;
  exec: GpuJobExecSpec;
  maxAttempts?: number;
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
  notBeforeMs?: number;
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
