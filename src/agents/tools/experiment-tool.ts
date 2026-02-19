import { Type } from "@sinclair/typebox";
import fs from "node:fs/promises";
import path from "node:path";
import type { OpenClawConfig } from "../../config/config.js";
import { loadConfig } from "../../config/config.js";
import {
  runExperimentPipeline,
  type ExperimentPipelineCompileOpts,
  type ExperimentPipelineInput,
} from "../../proposal/pipeline.js";
import { loadExecuteLog } from "../../proposal/results/collect.js";
import { AcceptanceReportSchema } from "../../proposal/results/schema.js";
import { DiscoveryModeSchema } from "../../proposal/schema.js";
import { runExperimentSuite } from "../../proposal/suite.js";
import { validatePlanDir } from "../../proposal/validate.js";
import { resolveUserPath } from "../../utils.js";
import { resolveSessionAgentId, resolveAgentWorkspaceDir } from "../agent-scope.js";
import { optionalStringEnum, stringEnum } from "../schema/typebox.js";
import { type AnyAgentTool, jsonResult, readNumberParam, readStringParam } from "./common.js";

const EXPERIMENT_ACTIONS = ["plan", "execute", "pipeline", "suite", "status"] as const;
const EXPERIMENT_DISCOVERY_MODES = ["off", "plan", "sample"] as const;
const EXPERIMENT_NODE_APPROVE = ["off", "allow-once", "allow-always"] as const;
const EXPERIMENT_BOOTSTRAP_MODES = ["worktree", "plan", "both"] as const;

const ExperimentToolSchema = Type.Object({
  action: stringEnum(EXPERIMENT_ACTIONS),

  // Inputs
  inputPath: Type.Optional(Type.String()),
  proposalMarkdown: Type.Optional(Type.String()),
  planDir: Type.Optional(Type.String()),

  // Compile
  workspaceDir: Type.Optional(Type.String()),
  outDir: Type.Optional(Type.String()),
  discovery: optionalStringEnum(EXPERIMENT_DISCOVERY_MODES),
  agentId: Type.Optional(Type.String()),
  compileModel: Type.Optional(Type.String()),
  useLlm: Type.Optional(Type.Boolean()),

  // Suite
  suiteCount: Type.Optional(Type.Number()),
  suiteOutDir: Type.Optional(Type.String()),

  // Run/Execute
  dryRun: Type.Optional(Type.Boolean()),
  maxAttempts: Type.Optional(Type.Number()),
  retryDelayMs: Type.Optional(Type.Number()),
  commandTimeoutMs: Type.Optional(Type.Number()),
  failOnNeedsConfirm: Type.Optional(Type.Boolean()),

  // Refine
  refine: Type.Optional(Type.Boolean()),
  opencodeModel: Type.Optional(Type.String()),
  opencodeAgent: Type.Optional(Type.String()),
  opencodeTimeoutMs: Type.Optional(Type.Number()),
  writeAcceptance: Type.Optional(Type.Boolean()),

  // Bootstrap
  bootstrap: Type.Optional(Type.Boolean()),
  bootstrapMode: optionalStringEnum(EXPERIMENT_BOOTSTRAP_MODES),
  bootstrapModel: Type.Optional(Type.String()),
  bootstrapAgent: Type.Optional(Type.String()),
  bootstrapTimeoutMs: Type.Optional(Type.Number()),
  bootstrapMaxAttempts: Type.Optional(Type.Number()),
  bootstrapInstructions: Type.Optional(Type.String()),

  // Execute
  sandbox: Type.Optional(Type.Boolean()),
  sandboxImage: Type.Optional(Type.String()),
  sandboxNetwork: Type.Optional(Type.String()),
  repair: Type.Optional(Type.Boolean()),
  repairAttempts: Type.Optional(Type.Number()),
  repairModel: Type.Optional(Type.String()),

  // Gateway/node
  gatewayUrl: Type.Optional(Type.String()),
  gatewayToken: Type.Optional(Type.String()),
  gatewayTimeoutMs: Type.Optional(Type.Number()),
  invokeTimeoutMs: Type.Optional(Type.Number()),
  gpuWaitTimeoutMs: Type.Optional(Type.Number()),
  node: Type.Optional(Type.String()),
  nodeApprove: optionalStringEnum(EXPERIMENT_NODE_APPROVE),

  // Finalize/accept
  finalizeForce: Type.Optional(Type.Boolean()),
  baselinePath: Type.Optional(Type.String()),

  // Status
  includePaths: Type.Optional(Type.Boolean()),
  readAcceptance: Type.Optional(Type.Boolean()),
  readExecute: Type.Optional(Type.Boolean()),
});

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.stat(filePath);
    return true;
  } catch {
    return false;
  }
}

async function resolvePipelineInput(params: {
  inputPath?: string;
  planDir?: string;
  proposalMarkdown?: string;
}): Promise<ExperimentPipelineInput> {
  const explicitPlanDir = params.planDir?.trim();
  if (explicitPlanDir) {
    return { kind: "planDir", planDir: resolveUserPath(explicitPlanDir) };
  }

  const inputPathRaw = params.inputPath?.trim();
  if (inputPathRaw) {
    const resolved = resolveUserPath(inputPathRaw);
    const stat = await fs.stat(resolved);
    if (stat.isDirectory()) {
      return { kind: "planDir", planDir: resolved };
    }
    return { kind: "proposalPath", proposalPath: resolved };
  }

  const proposalMarkdown =
    typeof params.proposalMarkdown === "string" ? params.proposalMarkdown.trim() : "";
  if (!proposalMarkdown) {
    throw new Error("Missing input: provide planDir, inputPath, or proposalMarkdown.");
  }

  return {
    kind: "proposalMarkdown",
    proposalMarkdown,
    proposalSource: "agent-tool:experiment",
  };
}

function summarizeExecute(results: Array<{ status?: string }>) {
  let ok = 0;
  let failed = 0;
  let skipped = 0;
  for (const entry of results) {
    if (entry.status === "ok") {
      ok += 1;
      continue;
    }
    if (entry.status === "failed") {
      failed += 1;
      continue;
    }
    if (entry.status === "skipped") {
      skipped += 1;
    }
  }
  return { ok, failed, skipped, total: results.length };
}

async function readAcceptanceReport(planDir: string) {
  const reportPath = path.join(planDir, "report", "acceptance_report.json");
  if (!(await fileExists(reportPath))) {
    return { path: reportPath, report: null as unknown, warnings: [] as string[] };
  }
  try {
    const raw = await fs.readFile(reportPath, "utf-8");
    const parsed = AcceptanceReportSchema.safeParse(JSON.parse(raw) as unknown);
    if (!parsed.success) {
      return {
        path: reportPath,
        report: null,
        warnings: [`Invalid acceptance_report.json: ${parsed.error.message}`],
      };
    }
    return { path: reportPath, report: parsed.data, warnings: [] };
  } catch (err) {
    return {
      path: reportPath,
      report: null,
      warnings: [`Failed to read acceptance_report.json: ${String(err)}`],
    };
  }
}

async function runStatus(params: {
  planDir: string;
  includePaths: boolean;
  readAcceptance: boolean;
  readExecute: boolean;
}) {
  const warnings: string[] = [];
  const errors: string[] = [];

  const validation = await validatePlanDir(params.planDir);
  warnings.push(...validation.warnings);
  if (!validation.ok) {
    errors.push(...validation.errors);
  }
  const planId = validation.ok ? validation.data?.report.planId : undefined;

  const artifactPaths = {
    compileReport: path.join(params.planDir, "report", "compile_report.json"),
    runLog: path.join(params.planDir, "report", "run_log.json"),
    refineReport: path.join(params.planDir, "report", "refine_report.json"),
    executeLog: path.join(params.planDir, "report", "execute_log.json"),
    executeSummary: path.join(params.planDir, "report", "execute_summary.md"),
    finalMetrics: path.join(params.planDir, "report", "final_metrics.json"),
    finalReport: path.join(params.planDir, "report", "final_report.md"),
    acceptanceReport: path.join(params.planDir, "report", "acceptance_report.json"),
  };

  const artifacts: Record<string, boolean> = {};
  await Promise.all(
    Object.entries(artifactPaths).map(async ([key, filePath]) => {
      artifacts[key] = await fileExists(filePath);
    }),
  );

  let acceptance: unknown = null;
  if (params.readAcceptance) {
    const accepted = await readAcceptanceReport(params.planDir);
    warnings.push(...accepted.warnings);
    acceptance = accepted.report;
  }

  let executeSummary: unknown = null;
  if (params.readExecute) {
    const executeLog = await loadExecuteLog(params.planDir);
    warnings.push(...executeLog.warnings);
    const results = executeLog.log?.results ?? [];
    executeSummary = summarizeExecute(results);
  }

  return {
    ok: errors.length === 0,
    planDir: params.planDir,
    planId,
    warnings,
    errors,
    artifacts,
    acceptance,
    executeSummary,
    ...(params.includePaths ? { paths: artifactPaths } : {}),
  };
}

export function createExperimentTool(opts?: {
  agentSessionKey?: string;
  config?: OpenClawConfig;
  workspaceDir?: string;
}): AnyAgentTool {
  return {
    label: "Experiment",
    name: "experiment",
    description:
      "Run the proposal experiment pipeline (compile/run/refine/execute/finalize/accept) and report status for a planDir.",
    parameters: ExperimentToolSchema,
    execute: async (_toolCallId, args) => {
      const params = args as Record<string, unknown>;
      const action = readStringParam(params, "action", { required: true });
      const cfg = opts?.config ?? loadConfig();

      if (action === "status") {
        const planDir = readStringParam(params, "planDir") ?? readStringParam(params, "inputPath");
        if (!planDir) {
          throw new Error("planDir required for status");
        }
        const includePaths = typeof params.includePaths === "boolean" ? params.includePaths : true;
        const readAcceptance =
          typeof params.readAcceptance === "boolean" ? params.readAcceptance : true;
        const readExecute = typeof params.readExecute === "boolean" ? params.readExecute : true;
        const res = await runStatus({
          planDir: resolveUserPath(planDir),
          includePaths,
          readAcceptance,
          readExecute,
        });
        return jsonResult(res);
      }

      if (action === "suite") {
        const input = await resolvePipelineInput({
          inputPath: readStringParam(params, "inputPath"),
          planDir: readStringParam(params, "planDir"),
          proposalMarkdown: readStringParam(params, "proposalMarkdown", { allowEmpty: true }),
        });

        let compile: ExperimentPipelineCompileOpts | undefined;
        if (input.kind !== "planDir") {
          const agentId =
            readStringParam(params, "agentId")?.trim() ||
            resolveSessionAgentId({ sessionKey: opts?.agentSessionKey, config: cfg });
          const workspaceDir = readStringParam(params, "workspaceDir")?.trim()
            ? resolveUserPath(readStringParam(params, "workspaceDir")!.trim())
            : opts?.workspaceDir?.trim()
              ? resolveUserPath(opts.workspaceDir.trim())
              : resolveAgentWorkspaceDir(cfg, agentId);
          await fs.mkdir(workspaceDir, { recursive: true });

          const discovery = DiscoveryModeSchema.parse(
            readStringParam(params, "discovery") ?? "plan",
          );
          compile = {
            agentId,
            workspaceDir,
            outDir: readStringParam(params, "outDir")?.trim()
              ? resolveUserPath(readStringParam(params, "outDir")!.trim())
              : undefined,
            discovery,
            modelOverride: readStringParam(params, "compileModel")?.trim() || undefined,
            useLlm: typeof params.useLlm === "boolean" ? params.useLlm : true,
          };
        }

        const dryRun = typeof params.dryRun === "boolean" ? params.dryRun : false;
        const maxAttempts = readNumberParam(params, "maxAttempts", { integer: true }) ?? 3;
        const retryDelayMs = readNumberParam(params, "retryDelayMs", { integer: true }) ?? 1500;
        const commandTimeoutMs =
          readNumberParam(params, "commandTimeoutMs", { integer: true }) ?? 600_000;
        const failOnNeedsConfirm =
          typeof params.failOnNeedsConfirm === "boolean" ? params.failOnNeedsConfirm : false;

        const shouldRefine = typeof params.refine === "boolean" ? params.refine : true;
        const opencodeTimeoutMs =
          readNumberParam(params, "opencodeTimeoutMs", { integer: true }) ?? 180_000;

        const shouldBootstrap = typeof params.bootstrap === "boolean" ? params.bootstrap : false;
        const bootstrapModeRaw = readStringParam(params, "bootstrapMode") ?? "worktree";
        const bootstrapMode =
          bootstrapModeRaw === "plan" || bootstrapModeRaw === "both"
            ? bootstrapModeRaw
            : "worktree";
        const bootstrapTimeoutMs =
          readNumberParam(params, "bootstrapTimeoutMs", { integer: true }) ?? 180_000;
        const bootstrapMaxAttempts =
          readNumberParam(params, "bootstrapMaxAttempts", { integer: true }) ?? 2;
        const bootstrapModel =
          readStringParam(params, "bootstrapModel") ??
          readStringParam(params, "opencodeModel") ??
          "opencode/kimi-k2.5-free";
        const bootstrapAgent = readStringParam(params, "bootstrapAgent");
        const bootstrapInstructions = readStringParam(params, "bootstrapInstructions");

        const sandbox = typeof params.sandbox === "boolean" ? params.sandbox : true;
        const repair = typeof params.repair === "boolean" ? params.repair : true;
        const repairAttempts = readNumberParam(params, "repairAttempts", { integer: true }) ?? 1;
        const gatewayTimeoutMs =
          readNumberParam(params, "gatewayTimeoutMs", { integer: true }) ?? 30_000;
        const invokeTimeoutMs =
          readNumberParam(params, "invokeTimeoutMs", { integer: true }) ?? 1_200_000;
        const gpuWaitTimeoutMs =
          readNumberParam(params, "gpuWaitTimeoutMs", { integer: true }) ?? 1_800_000;

        const baselinePathRaw = readStringParam(params, "baselinePath");

        const suiteCount = readNumberParam(params, "suiteCount", { integer: true }) ?? 4;
        const suiteOutDirRaw = readStringParam(params, "suiteOutDir");
        const suiteOutDir = suiteOutDirRaw ? resolveUserPath(suiteOutDirRaw) : undefined;

        const suite = await runExperimentSuite({
          input,
          cfg,
          opts: {
            variantCount: Math.max(0, Math.floor(suiteCount)),
            suiteOutDir,
            designModel: readStringParam(params, "opencodeModel") ?? "opencode/kimi-k2.5-free",
            designAgent: readStringParam(params, "opencodeAgent"),
            designTimeoutMs: opencodeTimeoutMs,
          },
          stages: {
            compile,
            run: {
              dryRun,
              maxAttempts,
              retryDelayMs,
              commandTimeoutMs,
              failOnNeedsConfirm,
            },
            refine: {
              enabled: shouldRefine,
              dryRun: false,
              model: readStringParam(params, "opencodeModel") ?? "opencode/kimi-k2.5-free",
              agent: readStringParam(params, "opencodeAgent"),
              timeoutMs: opencodeTimeoutMs,
              writeAcceptance:
                typeof params.writeAcceptance === "boolean" ? params.writeAcceptance : false,
            },
            bootstrap: shouldBootstrap
              ? {
                  enabled: true,
                  mode: bootstrapMode,
                  dryRun,
                  model: bootstrapModel,
                  agent: bootstrapAgent ?? undefined,
                  timeoutMs: bootstrapTimeoutMs,
                  maxAttempts: bootstrapMaxAttempts,
                  instructions: bootstrapInstructions ?? undefined,
                }
              : undefined,
            execute: {
              dryRun,
              maxAttempts,
              retryDelayMs,
              commandTimeoutMs,
              failOnNeedsConfirm,
              sandbox,
              sandboxImage: readStringParam(params, "sandboxImage"),
              sandboxNetwork: readStringParam(params, "sandboxNetwork"),
              repair,
              repairAttempts,
              modelOverride: readStringParam(params, "repairModel"),
              agentId: readStringParam(params, "agentId"),
              gatewayUrl: readStringParam(params, "gatewayUrl"),
              gatewayToken: readStringParam(params, "gatewayToken"),
              gatewayTimeoutMs,
              invokeTimeoutMs,
              gpuWaitTimeoutMs,
              node: readStringParam(params, "node"),
              nodeApprove:
                params.nodeApprove === "allow-once" || params.nodeApprove === "allow-always"
                  ? params.nodeApprove
                  : "off",
            },
            finalize: {
              force: typeof params.finalizeForce === "boolean" ? params.finalizeForce : false,
            },
            accept: {
              baselinePath: baselinePathRaw ? resolveUserPath(baselinePathRaw) : undefined,
            },
          },
        });

        return jsonResult(suite);
      }

      const input = await resolvePipelineInput({
        inputPath: readStringParam(params, "inputPath"),
        planDir: readStringParam(params, "planDir"),
        proposalMarkdown: readStringParam(params, "proposalMarkdown", { allowEmpty: true }),
      });

      const pipelineAction =
        action === "plan" || action === "execute" || action === "pipeline" ? action : "pipeline";

      let compile: ExperimentPipelineCompileOpts | undefined;
      if (input.kind !== "planDir") {
        const agentId =
          readStringParam(params, "agentId")?.trim() ||
          resolveSessionAgentId({ sessionKey: opts?.agentSessionKey, config: cfg });
        const workspaceDir = readStringParam(params, "workspaceDir")?.trim()
          ? resolveUserPath(readStringParam(params, "workspaceDir")!.trim())
          : opts?.workspaceDir?.trim()
            ? resolveUserPath(opts.workspaceDir.trim())
            : resolveAgentWorkspaceDir(cfg, agentId);
        await fs.mkdir(workspaceDir, { recursive: true });

        const discovery = DiscoveryModeSchema.parse(readStringParam(params, "discovery") ?? "plan");
        compile = {
          agentId,
          workspaceDir,
          outDir: readStringParam(params, "outDir")?.trim()
            ? resolveUserPath(readStringParam(params, "outDir")!.trim())
            : undefined,
          discovery,
          modelOverride: readStringParam(params, "compileModel")?.trim() || undefined,
          useLlm: typeof params.useLlm === "boolean" ? params.useLlm : true,
        };
      }

      const dryRun = typeof params.dryRun === "boolean" ? params.dryRun : false;
      const maxAttempts = readNumberParam(params, "maxAttempts", { integer: true }) ?? 3;
      const retryDelayMs = readNumberParam(params, "retryDelayMs", { integer: true }) ?? 1500;
      const commandTimeoutMs =
        readNumberParam(params, "commandTimeoutMs", { integer: true }) ?? 600_000;
      const failOnNeedsConfirm =
        typeof params.failOnNeedsConfirm === "boolean" ? params.failOnNeedsConfirm : false;

      const shouldRefine = typeof params.refine === "boolean" ? params.refine : true;
      const opencodeTimeoutMs =
        readNumberParam(params, "opencodeTimeoutMs", { integer: true }) ?? 180_000;

      const shouldBootstrap = typeof params.bootstrap === "boolean" ? params.bootstrap : false;
      const bootstrapModeRaw = readStringParam(params, "bootstrapMode") ?? "worktree";
      const bootstrapMode =
        bootstrapModeRaw === "plan" || bootstrapModeRaw === "both" ? bootstrapModeRaw : "worktree";
      const bootstrapTimeoutMs =
        readNumberParam(params, "bootstrapTimeoutMs", { integer: true }) ?? 180_000;
      const bootstrapMaxAttempts =
        readNumberParam(params, "bootstrapMaxAttempts", { integer: true }) ?? 2;
      const bootstrapModel =
        readStringParam(params, "bootstrapModel") ??
        readStringParam(params, "opencodeModel") ??
        "opencode/kimi-k2.5-free";
      const bootstrapAgent = readStringParam(params, "bootstrapAgent");
      const bootstrapInstructions = readStringParam(params, "bootstrapInstructions");

      const sandbox = typeof params.sandbox === "boolean" ? params.sandbox : true;
      const repair = typeof params.repair === "boolean" ? params.repair : true;
      const repairAttempts = readNumberParam(params, "repairAttempts", { integer: true }) ?? 1;
      const gatewayTimeoutMs =
        readNumberParam(params, "gatewayTimeoutMs", { integer: true }) ?? 30_000;
      const invokeTimeoutMs =
        readNumberParam(params, "invokeTimeoutMs", { integer: true }) ?? 1_200_000;
      const gpuWaitTimeoutMs =
        readNumberParam(params, "gpuWaitTimeoutMs", { integer: true }) ?? 1_800_000;

      const baselinePathRaw = readStringParam(params, "baselinePath");

      const pipeline = await runExperimentPipeline({
        action: pipelineAction,
        input,
        cfg,
        stages: {
          compile,
          run: {
            dryRun,
            maxAttempts,
            retryDelayMs,
            commandTimeoutMs,
            failOnNeedsConfirm,
          },
          refine:
            pipelineAction === "plan" || pipelineAction === "pipeline"
              ? {
                  enabled: shouldRefine,
                  dryRun: false,
                  model: readStringParam(params, "opencodeModel") ?? "opencode/kimi-k2.5-free",
                  agent: readStringParam(params, "opencodeAgent"),
                  timeoutMs: opencodeTimeoutMs,
                  writeAcceptance:
                    typeof params.writeAcceptance === "boolean" ? params.writeAcceptance : false,
                }
              : undefined,
          bootstrap: shouldBootstrap
            ? {
                enabled: true,
                mode: bootstrapMode,
                dryRun,
                model: bootstrapModel,
                agent: bootstrapAgent ?? undefined,
                timeoutMs: bootstrapTimeoutMs,
                maxAttempts: bootstrapMaxAttempts,
                instructions: bootstrapInstructions ?? undefined,
              }
            : undefined,
          execute: {
            dryRun,
            maxAttempts,
            retryDelayMs,
            commandTimeoutMs,
            failOnNeedsConfirm,
            sandbox,
            sandboxImage: readStringParam(params, "sandboxImage"),
            sandboxNetwork: readStringParam(params, "sandboxNetwork"),
            repair,
            repairAttempts,
            modelOverride: readStringParam(params, "repairModel"),
            agentId: readStringParam(params, "agentId"),
            gatewayUrl: readStringParam(params, "gatewayUrl"),
            gatewayToken: readStringParam(params, "gatewayToken"),
            gatewayTimeoutMs,
            invokeTimeoutMs,
            gpuWaitTimeoutMs,
            node: readStringParam(params, "node"),
            nodeApprove:
              params.nodeApprove === "allow-once" || params.nodeApprove === "allow-always"
                ? params.nodeApprove
                : "off",
          },
          finalize: {
            force: typeof params.finalizeForce === "boolean" ? params.finalizeForce : false,
          },
          accept: { baselinePath: baselinePathRaw ? resolveUserPath(baselinePathRaw) : undefined },
        },
      });

      return jsonResult({
        ok: pipeline.ok,
        action,
        planDir: pipeline.planDir,
        planId: pipeline.planId,
        ...(pipeline.compile ? { compile: pipeline.compile } : {}),
        ...(pipeline.validate ? { validate: pipeline.validate } : {}),
        ...(pipeline.safe ? { safe: pipeline.safe } : {}),
        ...(pipeline.refine ? { refine: pipeline.refine } : {}),
        ...(pipeline.bootstrap ? { bootstrap: pipeline.bootstrap } : {}),
        ...(pipeline.execute ? { execute: pipeline.execute } : {}),
        ...(pipeline.finalize ? { finalize: pipeline.finalize } : {}),
        ...(pipeline.accept ? { accept: pipeline.accept } : {}),
      });
    },
  };
}
