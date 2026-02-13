import type { CommandOptions, SpawnResult } from "../../process/exec.js";
import type { ProposalLlmClient } from "../llm.js";
import type { FailureCategory } from "../schema.js";

export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export type GatewayCallLike = <T = Record<string, unknown>>(opts: {
  url?: string;
  token?: string;
  method: string;
  params?: unknown;
  timeoutMs?: number;
}) => Promise<T>;

export type ProposalExecuteOpts = {
  dryRun?: boolean;
  json?: boolean;
  failOnNeedsConfirm?: boolean;
  sandbox?: boolean;
  sandboxImage?: string;
  sandboxNetwork?: string;
  maxAttempts?: number;
  commandTimeoutMs?: number;
  retryDelayMs?: number;
  repair?: boolean;
  repairAttempts?: number;
  modelOverride?: string;
  agentId?: string;
  gatewayUrl?: string;
  gatewayToken?: string;
  gatewayTimeoutMs?: number;
  node?: string;
  nodeApprove?: "off" | "allow-once" | "allow-always";
  invokeTimeoutMs?: number;
};

export type ExecuteNodeStatus = "ok" | "failed" | "skipped" | "dry_run";

export type ExecuteNodeExecutor = "exec" | "node.invoke" | "manual";

export type ExecuteAttempt = {
  attempt: number;
  executor: ExecuteNodeExecutor;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
  ok: boolean;
  exitCode?: number | null;
  timedOut?: boolean;
  stdoutTail?: string;
  stderrTail?: string;
  error?: string;
  failureCategory?: FailureCategory;
  patch?: {
    summary: { added: string[]; modified: string[]; deleted: string[] };
    patchPath: string;
  } | null;
};

export type ExecuteNodeResult = {
  nodeId: string;
  type: string;
  tool: string;
  status: ExecuteNodeStatus;
  executor: ExecuteNodeExecutor;
  attempts: ExecuteAttempt[];
  outputs?: string[];
};

export type ProposalExecuteResult = {
  ok: boolean;
  planDir: string;
  planId?: string;
  startedAt: string;
  finishedAt: string;
  warnings: string[];
  errors: string[];
  results: ExecuteNodeResult[];
  skipped: Array<{ nodeId: string; type: string; reason: string }>;
  paths: {
    executeLog: string;
    executeSummary: string;
  };
};

export type ProposalExecuteDeps = {
  fetchFn?: FetchLike;
  runHostCommand?: (argv: string[], options: CommandOptions) => Promise<SpawnResult>;
  callGateway?: GatewayCallLike;
  llmClient?: ProposalLlmClient;
};

export const SAFE_NODE_TYPES = new Set<string>([
  "fetch_repo",
  "fetch_dataset_sample",
  "static_checks",
]);
