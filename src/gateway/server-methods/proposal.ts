import fs from "node:fs/promises";
import type { ProposalJobRequest } from "../proposal-orchestrator/types.js";
import type { GatewayRequestHandlers } from "./types.js";
import { resolveAgentWorkspaceDir, resolveDefaultAgentId } from "../../agents/agent-scope.js";
import { loadConfig } from "../../config/config.js";
import { compileProposal } from "../../proposal/compiler.js";
import { executeProposalPlan } from "../../proposal/execute.js";
import { finalizeProposalPlan } from "../../proposal/finalize.js";
import { refineProposalPlan } from "../../proposal/refine.js";
import { acceptProposalResults } from "../../proposal/results/index.js";
import { runProposalPlanSafeNodes } from "../../proposal/run.js";
import { DiscoveryModeSchema } from "../../proposal/schema.js";
import { resolveUserPath } from "../../utils.js";
import { createInProcessGatewayCall } from "../proposal-orchestrator/in-process-call.js";
import { ErrorCodes, errorShape } from "../protocol/index.js";

function requireNonEmptyString(
  value: unknown,
  field: string,
): { ok: true; value: string } | { ok: false; error: string } {
  if (typeof value !== "string") {
    return { ok: false, error: `${field} (string) required` };
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return { ok: false, error: `${field} (non-empty string) required` };
  }
  return { ok: true, value: trimmed };
}

function optionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function optionalBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function optionalNumber(value: unknown): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return undefined;
  }
  return value;
}

export const proposalHandlers: GatewayRequestHandlers = {
  "proposal.compile": async ({ params, respond }) => {
    const proposalMarkdownRes = requireNonEmptyString(params.proposalMarkdown, "proposalMarkdown");
    if (!proposalMarkdownRes.ok) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid proposal.compile params: ${proposalMarkdownRes.error}`,
        ),
      );
      return;
    }

    const discoveryRaw = optionalString(params.discovery) ?? "plan";
    const discoveryParsed = DiscoveryModeSchema.safeParse(discoveryRaw);
    if (!discoveryParsed.success) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          'invalid proposal.compile params: discovery must be one of "off" | "plan" | "sample"',
        ),
      );
      return;
    }

    const cfg = loadConfig();
    const agentId = optionalString(params.agentId) ?? resolveDefaultAgentId(cfg);
    const workspaceDir =
      optionalString(params.workspaceDir) ?? resolveAgentWorkspaceDir(cfg, agentId);
    const resolvedWorkspaceDir = resolveUserPath(workspaceDir);
    await fs.mkdir(resolvedWorkspaceDir, { recursive: true });

    const outDirRaw = optionalString(params.outDir);
    const outDir = outDirRaw ? resolveUserPath(outDirRaw) : undefined;
    const modelOverride = optionalString(params.modelOverride);

    const useLlmRaw = params.useLlm;
    const useLlm = typeof useLlmRaw === "boolean" ? useLlmRaw : true;

    const result = await compileProposal({
      proposalMarkdown: proposalMarkdownRes.value,
      proposalSource: "gateway:proposal.compile",
      cfg,
      agentId,
      workspaceDir: resolvedWorkspaceDir,
      outDir,
      discovery: discoveryParsed.data,
      modelOverride,
      useLlm,
    });

    respond(
      true,
      {
        ok: result.ok,
        planId: result.planId,
        rootDir: result.rootDir,
        report: result.report,
        paths: result.paths,
      },
      undefined,
    );
  },

  "proposal.run": async ({ params, respond }) => {
    const planDirRes = requireNonEmptyString(params.planDir, "planDir");
    if (!planDirRes.ok) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, `invalid proposal.run params: ${planDirRes.error}`),
      );
      return;
    }

    const planDir = resolveUserPath(planDirRes.value);
    const result = await runProposalPlanSafeNodes({
      planDir,
      opts: {
        dryRun: optionalBoolean(params.dryRun),
        failOnNeedsConfirm: optionalBoolean(params.failOnNeedsConfirm),
        maxAttempts: optionalNumber(params.maxAttempts),
        retryDelayMs: optionalNumber(params.retryDelayMs),
        commandTimeoutMs: optionalNumber(params.commandTimeoutMs),
        json: optionalBoolean(params.json),
      },
    });
    respond(true, result, undefined);
  },

  "proposal.refine": async ({ params, respond }) => {
    const planDirRes = requireNonEmptyString(params.planDir, "planDir");
    if (!planDirRes.ok) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid proposal.refine params: ${planDirRes.error}`,
        ),
      );
      return;
    }

    const planDir = resolveUserPath(planDirRes.value);
    const result = await refineProposalPlan({
      planDir,
      opts: {
        model: optionalString(params.model),
        agent: optionalString(params.agent),
        timeoutMs: optionalNumber(params.timeoutMs),
        dryRun: optionalBoolean(params.dryRun),
        writeAcceptance: optionalBoolean(params.writeAcceptance),
      },
    });
    respond(true, result, undefined);
  },

  "proposal.execute": async ({ params, respond, context }) => {
    const planDirRes = requireNonEmptyString(params.planDir, "planDir");
    if (!planDirRes.ok) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid proposal.execute params: ${planDirRes.error}`,
        ),
      );
      return;
    }

    const planDir = resolveUserPath(planDirRes.value);
    const cfg = loadConfig();
    const callGateway = createInProcessGatewayCall({
      nodeRegistry: context.nodeRegistry,
      gpuScheduler: context.gpuScheduler,
      loadConfig,
    });

    const nodeApproveRaw = optionalString(params.nodeApprove);
    const nodeApprove =
      nodeApproveRaw === "allow-once" || nodeApproveRaw === "allow-always" ? nodeApproveRaw : "off";

    const result = await executeProposalPlan({
      planDir,
      cfg,
      opts: {
        dryRun: optionalBoolean(params.dryRun),
        json: optionalBoolean(params.json),
        failOnNeedsConfirm: optionalBoolean(params.failOnNeedsConfirm),
        sandbox: optionalBoolean(params.sandbox),
        sandboxImage: optionalString(params.sandboxImage),
        sandboxNetwork: optionalString(params.sandboxNetwork),
        maxAttempts: optionalNumber(params.maxAttempts),
        commandTimeoutMs: optionalNumber(params.commandTimeoutMs),
        retryDelayMs: optionalNumber(params.retryDelayMs),
        repair: optionalBoolean(params.repair),
        repairAttempts: optionalNumber(params.repairAttempts),
        modelOverride: optionalString(params.modelOverride),
        agentId: optionalString(params.agentId),
        gatewayUrl: optionalString(params.gatewayUrl),
        gatewayToken: optionalString(params.gatewayToken),
        gatewayTimeoutMs: optionalNumber(params.gatewayTimeoutMs),
        node: optionalString(params.node),
        nodeApprove,
        invokeTimeoutMs: optionalNumber(params.invokeTimeoutMs),
      },
      deps: { callGateway },
    });
    respond(true, result, undefined);
  },

  "proposal.finalize": async ({ params, respond }) => {
    const planDirRes = requireNonEmptyString(params.planDir, "planDir");
    if (!planDirRes.ok) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid proposal.finalize params: ${planDirRes.error}`,
        ),
      );
      return;
    }
    const planDir = resolveUserPath(planDirRes.value);
    const result = await finalizeProposalPlan({
      planDir,
      opts: {
        force: optionalBoolean(params.force),
      },
    });
    respond(true, result, undefined);
  },

  "proposal.accept": async ({ params, respond }) => {
    const planDirRes = requireNonEmptyString(params.planDir, "planDir");
    if (!planDirRes.ok) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid proposal.accept params: ${planDirRes.error}`,
        ),
      );
      return;
    }
    const planDir = resolveUserPath(planDirRes.value);
    const baselinePath = optionalString(params.baselinePath);
    const report = await acceptProposalResults({ planDir, baselinePath });
    respond(true, report, undefined);
  },

  "proposal.job.submit": async ({ params, respond, context }) => {
    try {
      const job = await context.proposalOrchestrator.submit(
        params as unknown as ProposalJobRequest,
      );
      respond(true, { job }, undefined);
    } catch (err) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, String(err)));
    }
  },

  "proposal.job.get": async ({ params, respond, context }) => {
    const jobIdRes = requireNonEmptyString(params.jobId, "jobId");
    if (!jobIdRes.ok) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid proposal.job.get params: ${jobIdRes.error}`,
        ),
      );
      return;
    }
    const job = context.proposalOrchestrator.get(jobIdRes.value);
    if (!job) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "unknown jobId"));
      return;
    }
    respond(true, { job }, undefined);
  },

  "proposal.job.list": async ({ params, respond, context }) => {
    const stateRaw = optionalString(params.state);
    const state =
      stateRaw === "queued" ||
      stateRaw === "running" ||
      stateRaw === "succeeded" ||
      stateRaw === "failed" ||
      stateRaw === "canceled"
        ? stateRaw
        : undefined;
    const jobs = context.proposalOrchestrator.list(state ? { state } : undefined);
    respond(true, { jobs }, undefined);
  },

  "proposal.job.cancel": async ({ params, respond, context }) => {
    const jobIdRes = requireNonEmptyString(params.jobId, "jobId");
    if (!jobIdRes.ok) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid proposal.job.cancel params: ${jobIdRes.error}`,
        ),
      );
      return;
    }
    const res = await context.proposalOrchestrator.cancel(jobIdRes.value);
    if (!res.ok) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, res.reason ?? "cancel failed"),
      );
      return;
    }
    respond(true, { ok: true }, undefined);
  },

  "proposal.job.wait": async ({ params, respond, context }) => {
    const jobIdRes = requireNonEmptyString(params.jobId, "jobId");
    if (!jobIdRes.ok) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid proposal.job.wait params: ${jobIdRes.error}`,
        ),
      );
      return;
    }
    const timeoutMs = optionalNumber(params.timeoutMs) ?? 30_000;
    const job = await context.proposalOrchestrator.wait(jobIdRes.value, timeoutMs);
    if (!job) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "unknown jobId"));
      return;
    }
    const done = job.state === "succeeded" || job.state === "failed" || job.state === "canceled";
    respond(true, { done, job }, undefined);
  },
};
