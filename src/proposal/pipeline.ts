import path from "node:path";
import type { OpenClawConfig } from "../config/config.js";
import type { AcceptanceReport } from "./results/schema.js";
import type { DiscoveryMode } from "./schema.js";
import {
  bootstrapProposalPlan,
  type ProposalBootstrapOpts,
  type ProposalBootstrapResult,
} from "./bootstrap.js";
import { compileProposal, type CompileProposalResult } from "./compiler.js";
import {
  executeProposalPlan,
  type ProposalExecuteOpts,
  type ProposalExecuteResult,
} from "./execute.js";
import {
  finalizeProposalPlan,
  type ProposalFinalizeOpts,
  type ProposalFinalizeResult,
} from "./finalize.js";
import {
  refineProposalPlan,
  type ProposalRefineOpts,
  type ProposalRefineResult,
} from "./refine.js";
import { acceptProposalResults } from "./results/index.js";
import { runProposalPlanSafeNodes, type ProposalRunOpts, type ProposalRunResult } from "./run.js";
import { validatePlanDir } from "./validate.js";

export type ExperimentPipelineAction = "plan" | "execute" | "pipeline";

export type ExperimentPipelineInput =
  | { kind: "planDir"; planDir: string }
  | { kind: "proposalPath"; proposalPath: string }
  | { kind: "proposalMarkdown"; proposalMarkdown: string; proposalSource: string };

export type ExperimentPipelineCompileOpts = {
  agentId: string;
  workspaceDir: string;
  outDir?: string;
  discovery: DiscoveryMode;
  modelOverride?: string;
  useLlm?: boolean;
};

export type ExperimentPipelineStages = {
  compile?: ExperimentPipelineCompileOpts;
  run?: ProposalRunOpts;
  refine?: (ProposalRefineOpts & { enabled?: boolean }) | undefined;
  bootstrap?: (ProposalBootstrapOpts & { enabled?: boolean }) | undefined;
  execute?: ProposalExecuteOpts;
  finalize?: ProposalFinalizeOpts;
  accept?: { baselinePath?: string };
};

export type ExperimentPipelineResult = {
  ok: boolean;
  action: ExperimentPipelineAction;
  planDir: string;
  planId?: string;
  compile?: CompileProposalResult;
  validate?: Awaited<ReturnType<typeof validatePlanDir>>;
  safe?: ProposalRunResult;
  refine?: ProposalRefineResult | { skipped: true };
  bootstrap?: ProposalBootstrapResult | { skipped: true };
  execute?: ProposalExecuteResult;
  finalize?: ProposalFinalizeResult;
  accept?: AcceptanceReport;
};

export async function runExperimentPipeline(params: {
  action: ExperimentPipelineAction;
  input: ExperimentPipelineInput;
  cfg: OpenClawConfig;
  stages?: ExperimentPipelineStages;
}): Promise<ExperimentPipelineResult> {
  const stages = params.stages ?? {};
  let compile: CompileProposalResult | undefined;

  let planDir =
    params.input.kind === "planDir"
      ? path.resolve(params.input.planDir)
      : path.resolve(stages.compile?.outDir ?? stages.compile?.workspaceDir ?? process.cwd());
  let planId: string | undefined;

  if (params.input.kind !== "planDir") {
    const compileOpts = stages.compile;
    if (!compileOpts) {
      throw new Error("compile options required for non-planDir inputs");
    }
    compile = await compileProposal({
      ...(params.input.kind === "proposalPath"
        ? { proposalPath: params.input.proposalPath }
        : {
            proposalMarkdown: params.input.proposalMarkdown,
            proposalSource: params.input.proposalSource,
          }),
      cfg: params.cfg,
      agentId: compileOpts.agentId,
      workspaceDir: compileOpts.workspaceDir,
      outDir: compileOpts.outDir,
      discovery: compileOpts.discovery,
      modelOverride: compileOpts.modelOverride,
      useLlm: compileOpts.useLlm,
    });
    planDir = compile.rootDir;
    planId = compile.planId;
    if (!compile.ok) {
      return { ok: false, action: params.action, planDir, planId, compile };
    }
  }

  let validate = await validatePlanDir(planDir);
  if (!validate.ok || !validate.data) {
    return { ok: false, action: params.action, planDir, planId, compile, validate };
  }
  planId ||= validate.data.report.planId;

  const safe = await runProposalPlanSafeNodes({ planDir, opts: stages.run });
  if (!safe.ok && params.action !== "plan") {
    return { ok: false, action: params.action, planDir, planId, compile, validate, safe };
  }

  let refine: ProposalRefineResult | { skipped: true } | undefined;
  const refineStage = stages.refine;
  const shouldRefine =
    (params.action === "plan" || params.action === "pipeline") &&
    refineStage !== undefined &&
    refineStage.enabled !== false;
  if (shouldRefine) {
    refine = await refineProposalPlan({
      planDir,
      opts: refineStage,
    });
    if ("ok" in refine && !refine.ok && params.action !== "plan") {
      return {
        ok: false,
        action: params.action,
        planDir,
        planId,
        compile,
        validate,
        safe,
        refine,
      };
    }

    // Enforce strict resume contract after refine, since refine is responsible for generating scripts.
    validate = await validatePlanDir(planDir, { strictResume: true });
    if (!validate.ok || !validate.data) {
      return {
        ok: false,
        action: params.action,
        planDir,
        planId,
        compile,
        validate,
        safe,
        refine,
      };
    }
  } else if (params.action === "plan" || params.action === "pipeline") {
    refine = { skipped: true };
  }

  if (params.action === "plan") {
    return {
      ok: compile?.ok ?? true,
      action: params.action,
      planDir,
      planId,
      compile,
      validate,
      safe,
      ...(refine ? { refine } : {}),
    };
  }

  let bootstrap: ProposalBootstrapResult | { skipped: true } | undefined;
  const bootstrapStage = stages.bootstrap;
  const shouldBootstrap =
    (params.action === "execute" || params.action === "pipeline") &&
    bootstrapStage !== undefined &&
    bootstrapStage.enabled !== false;
  if (shouldBootstrap) {
    const { enabled: _enabled, ...bootstrapOpts } = bootstrapStage;
    bootstrap = await bootstrapProposalPlan({
      planDir,
      opts: bootstrapOpts,
    });
    if (!bootstrap.ok) {
      return {
        ok: false,
        action: params.action,
        planDir,
        planId,
        compile,
        validate,
        safe,
        ...(refine ? { refine } : {}),
        bootstrap,
      };
    }
  } else if ((params.action === "execute" || params.action === "pipeline") && bootstrapStage) {
    bootstrap = { skipped: true };
  }

  // Final pre-exec validation: require checkpoint/resume contract (pause/resume relies on it).
  validate = await validatePlanDir(planDir, { strictResume: true });
  if (!validate.ok || !validate.data) {
    return {
      ok: false,
      action: params.action,
      planDir,
      planId,
      compile,
      validate,
      safe,
      ...(refine ? { refine } : {}),
      ...(bootstrap ? { bootstrap } : {}),
    };
  }

  const execute = await executeProposalPlan({
    planDir,
    cfg: params.cfg,
    opts: stages.execute,
  });
  if (!execute.ok) {
    return {
      ok: false,
      action: params.action,
      planDir,
      planId,
      compile,
      validate,
      safe,
      ...(refine ? { refine } : {}),
      ...(bootstrap ? { bootstrap } : {}),
      execute,
    };
  }

  const finalize = await finalizeProposalPlan({ planDir, opts: stages.finalize });
  if (!finalize.ok) {
    return {
      ok: false,
      action: params.action,
      planDir,
      planId,
      compile,
      validate,
      safe,
      ...(refine ? { refine } : {}),
      ...(bootstrap ? { bootstrap } : {}),
      execute,
      finalize,
    };
  }

  const accept = await acceptProposalResults({
    planDir,
    baselinePath: stages.accept?.baselinePath,
  });

  return {
    ok: accept.ok,
    action: params.action,
    planDir,
    planId,
    compile,
    validate,
    safe,
    ...(refine ? { refine } : {}),
    ...(bootstrap ? { bootstrap } : {}),
    execute,
    finalize,
    accept,
  };
}
