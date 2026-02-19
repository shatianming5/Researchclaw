import type { ProposalExecuteOpts } from "../../proposal/execute.js";
import type { ProposalFinalizeOpts } from "../../proposal/finalize.js";
import type { ProposalRefineOpts } from "../../proposal/refine.js";
import type { ProposalRunOpts } from "../../proposal/run.js";

export type ProposalJobState = "queued" | "running" | "succeeded" | "failed" | "canceled";
export type ProposalJobStepId = "compile" | "run" | "refine" | "execute" | "finalize" | "accept";
export type ProposalJobStepStatus = "pending" | "running" | "succeeded" | "failed" | "skipped";

export type ProposalJobEvent = {
  ts: number;
  level: "info" | "warn" | "error";
  message: string;
};

export type ProposalJobStepSnapshot = {
  id: ProposalJobStepId;
  status: ProposalJobStepStatus;
  startedAtMs?: number;
  finishedAtMs?: number;
  ok?: boolean;
  warningsCount?: number;
  errorsCount?: number;
  summary?: string;
};

export type ProposalJobRequest = {
  proposalMarkdown?: string;
  planDir?: string;
  compile?: {
    discovery?: "off" | "plan" | "sample";
    useLlm?: boolean;
    modelOverride?: string;
    agentId?: string;
    workspaceDir?: string;
    outDir?: string;
  };
  steps?: Partial<Record<ProposalJobStepId, boolean>>;
  run?: ProposalRunOpts;
  refine?: ProposalRefineOpts;
  execute?: ProposalExecuteOpts;
  finalize?: ProposalFinalizeOpts;
  accept?: { baselinePath?: string };
};

export type ProposalJob = {
  version: 1;
  jobId: string;
  createdAtMs: number;
  updatedAtMs: number;
  state: ProposalJobState;
  cancelRequested?: boolean;
  request: ProposalJobRequest;
  planId?: string;
  planDir?: string;
  steps: Record<ProposalJobStepId, ProposalJobStepSnapshot>;
  events: ProposalJobEvent[];
};
