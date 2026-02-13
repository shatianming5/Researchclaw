import fs from "node:fs/promises";
import path from "node:path";
import type { OpenClawConfig } from "../config/config.js";
import type { CommandOptions, SpawnResult } from "../process/exec.js";
import { resolveDefaultAgentId } from "../agents/agent-scope.js";
import { callGateway } from "../gateway/call.js";
import { runCommandWithTimeout } from "../process/exec.js";
import { validateDag } from "./dag.js";
import { runCpuShellNode } from "./execute/cpu.js";
import { runGpuNodeViaGateway, runGpuNodeViaScheduler } from "./execute/gpu.js";
import { isGpuNode } from "./execute/node-utils.js";
import { renderExecuteSummary } from "./execute/render.js";
import { createCpuRepairHook } from "./execute/repair.js";
import { createSandboxRunCommand, ensureSandboxForPlan } from "./execute/sandbox.js";
import {
  SAFE_NODE_TYPES,
  type ExecuteNodeResult,
  type ProposalExecuteDeps,
  type ProposalExecuteOpts,
  type ProposalExecuteResult,
} from "./execute/types.js";
import { writeJsonFile, writeTextFile } from "./files.js";
import { resolveProposalLlmClient } from "./llm.js";
import { writeRepoWorkflowEvidence } from "./repo-workflow/evidence.js";
import { RepoWorkflowManager, shouldUseRepoWorktreeForNode } from "./repo-workflow/manager.js";
import { runProposalPlanSafeNodes } from "./run.js";
import { RetrySpecSchema, type PlanNode } from "./schema.js";
import { validatePlanDir } from "./validate.js";

export type {
  ProposalExecuteDeps,
  ProposalExecuteOpts,
  ProposalExecuteResult,
} from "./execute/types.js";

async function readContextAgentId(planDir: string): Promise<string | null> {
  try {
    const raw = await fs.readFile(path.join(planDir, "input", "context.json"), "utf-8");
    const parsed = JSON.parse(raw) as { agentId?: unknown };
    const agentId = typeof parsed.agentId === "string" ? parsed.agentId.trim() : "";
    return agentId || null;
  } catch {
    return null;
  }
}

function resolveNodeById(dag: { nodes: PlanNode[] }, nodeId: string): PlanNode | null {
  return dag.nodes.find((n) => n.id === nodeId) ?? null;
}

function mapSafeNodeResult(params: {
  node: PlanNode;
  startedAt: string;
  safe: {
    status: "ok" | "failed" | "skipped" | "dry_run";
    durationMs: number;
    error?: string;
    stderrTail?: string;
    outputs?: string[];
  };
}): ExecuteNodeResult {
  return {
    nodeId: params.node.id,
    type: params.node.type,
    tool: params.node.tool,
    status: params.safe.status,
    executor: "exec",
    attempts: [
      {
        attempt: 1,
        executor: "exec",
        startedAt: params.startedAt,
        finishedAt: new Date().toISOString(),
        durationMs: params.safe.durationMs,
        ok:
          params.safe.status === "ok" ||
          params.safe.status === "skipped" ||
          params.safe.status === "dry_run",
        exitCode: params.safe.status === "failed" ? 1 : 0,
        stderrTail: params.safe.stderrTail,
        error: params.safe.error,
      },
    ],
    outputs: params.safe.outputs ?? params.node.outputs,
  };
}

export async function executeProposalPlan(params: {
  planDir: string;
  cfg: OpenClawConfig;
  opts?: ProposalExecuteOpts;
  deps?: ProposalExecuteDeps;
}): Promise<ProposalExecuteResult> {
  const planDir = path.resolve(params.planDir);
  const startedAt = new Date().toISOString();

  const warnings: string[] = [];
  const errors: string[] = [];
  const results: ExecuteNodeResult[] = [];
  const skipped: Array<{ nodeId: string; type: string; reason: string }> = [];

  const opts: Required<
    Pick<
      ProposalExecuteOpts,
      | "dryRun"
      | "failOnNeedsConfirm"
      | "sandbox"
      | "maxAttempts"
      | "commandTimeoutMs"
      | "retryDelayMs"
      | "repair"
      | "repairAttempts"
      | "gatewayTimeoutMs"
      | "invokeTimeoutMs"
      | "nodeApprove"
    >
  > & {
    sandboxImage?: string;
    sandboxNetwork?: string;
    modelOverride?: string;
    agentId?: string;
    gatewayUrl?: string;
    gatewayToken?: string;
    node?: string;
  } = {
    dryRun: Boolean(params.opts?.dryRun),
    failOnNeedsConfirm: Boolean(params.opts?.failOnNeedsConfirm),
    sandbox: params.opts?.sandbox !== false,
    maxAttempts: Math.max(1, Math.floor(params.opts?.maxAttempts ?? 3)),
    commandTimeoutMs: Math.max(5_000, Math.floor(params.opts?.commandTimeoutMs ?? 10 * 60_000)),
    retryDelayMs: Math.max(0, Math.floor(params.opts?.retryDelayMs ?? 1500)),
    repair: Boolean(params.opts?.repair),
    repairAttempts: Math.max(0, Math.floor(params.opts?.repairAttempts ?? 1)),
    sandboxImage: params.opts?.sandboxImage,
    sandboxNetwork: params.opts?.sandboxNetwork,
    modelOverride: params.opts?.modelOverride,
    agentId: params.opts?.agentId,
    gatewayUrl: params.opts?.gatewayUrl,
    gatewayToken: params.opts?.gatewayToken,
    gatewayTimeoutMs: Math.max(5_000, Math.floor(params.opts?.gatewayTimeoutMs ?? 30_000)),
    node: params.opts?.node,
    nodeApprove: params.opts?.nodeApprove ?? "off",
    invokeTimeoutMs: Math.max(5_000, Math.floor(params.opts?.invokeTimeoutMs ?? 20 * 60_000)),
  };

  const validation = await validatePlanDir(planDir);
  if (!validation.ok || !validation.data) {
    errors.push(...validation.errors);
    const finishedAt = new Date().toISOString();
    const out = finalizeExecuteResult({
      planDir,
      planId: undefined,
      startedAt,
      finishedAt,
      warnings,
      errors,
      results,
      skipped,
    });
    await writeExecuteArtifacts(out);
    return out;
  }

  const planId = validation.data.report.planId;
  if (validation.warnings.length > 0) {
    warnings.push(...validation.warnings);
  }

  const needsConfirmCount =
    validation.data.report.needsConfirm.length + validation.data.needsConfirmCount;
  if (needsConfirmCount > 0) {
    const msg = `Plan has ${needsConfirmCount} needs_confirm item(s).`;
    if (opts.failOnNeedsConfirm) {
      errors.push(msg);
    } else {
      warnings.push(msg);
    }
  }

  const topo = validateDag(validation.data.dag);
  if (!topo.ok) {
    errors.push(...topo.errors);
    const finishedAt = new Date().toISOString();
    const out = finalizeExecuteResult({
      planDir,
      planId,
      startedAt,
      finishedAt,
      warnings,
      errors,
      results,
      skipped,
    });
    await writeExecuteArtifacts(out);
    return out;
  }

  const retryRaw = await fs.readFile(path.join(planDir, "plan", "retry.json"), "utf-8");
  const retrySpec = RetrySpecSchema.parse(JSON.parse(retryRaw) as unknown);

  const agentId =
    opts.agentId?.trim() ||
    (await readContextAgentId(planDir)) ||
    resolveDefaultAgentId(params.cfg);

  const llmClient =
    params.deps?.llmClient ??
    (await (async () => {
      if (!opts.repair) {
        return undefined;
      }
      const resolved = await resolveProposalLlmClient({
        cfg: params.cfg,
        modelOverride: opts.modelOverride,
        agentId,
      });
      if (!resolved.ok) {
        warnings.push(`Repair LLM unavailable: ${resolved.error}`);
        return undefined;
      }
      return resolved.client;
    })());

  const runHostCommand = params.deps?.runHostCommand ?? runCommandWithTimeout;
  const repoWorkflow = new RepoWorkflowManager({
    planDir,
    planId,
    runHostCommand: runCommandWithTimeout,
  });
  const executedNodeIdsByRepoKey = new Map<string, string[]>();

  const sandbox =
    opts.sandbox && !opts.dryRun
      ? await ensureSandboxForPlan({
          planDir,
          cfg: params.cfg,
          agentId,
          planId,
          imageOverride: opts.sandboxImage,
          networkOverride: opts.sandboxNetwork,
        })
      : null;

  const runInSandbox =
    sandbox && opts.sandbox
      ? createSandboxRunCommand({
          planDir,
          containerName: sandbox.containerName,
          containerRoot: sandbox.containerRoot,
          baseEnv: { HOME: sandbox.containerRoot },
          runHostCommand,
        })
      : async (argv: string[], options: CommandOptions): Promise<SpawnResult> =>
          await runHostCommand(argv, {
            ...options,
            cwd: options.cwd ?? planDir,
          });

  const repairHook =
    opts.repair && llmClient && opts.repairAttempts > 0
      ? createCpuRepairHook({
          planDir,
          llmClient,
          maxRepairAttempts: opts.repairAttempts,
        })
      : undefined;

  // 1) Run safe nodes via the sandbox exec runner when available.
  const safe = await runProposalPlanSafeNodes({
    planDir,
    opts: {
      dryRun: opts.dryRun,
      failOnNeedsConfirm: opts.failOnNeedsConfirm,
      maxAttempts: opts.maxAttempts,
      retryDelayMs: opts.retryDelayMs,
      commandTimeoutMs: opts.commandTimeoutMs,
    },
    deps: {
      fetchFn: params.deps?.fetchFn ?? fetch,
      runCommand: runInSandbox,
    },
  });

  warnings.push(...safe.warnings);

  const safeByNodeId = new Map(
    safe.results.filter((r) => SAFE_NODE_TYPES.has(r.type)).map((r) => [r.nodeId, r]),
  );
  if (safe.errors.length > 0) {
    errors.push(...safe.errors);
  }

  // 2) Execute remaining nodes with scheduler.
  const callGatewayImpl = params.deps?.callGateway ?? callGateway;

  for (const nodeId of topo.order) {
    const node = resolveNodeById(validation.data.dag, nodeId);
    if (!node) {
      errors.push(`Missing node in DAG: ${nodeId}`);
      break;
    }

    const safeRes = safeByNodeId.get(nodeId);
    if (safeRes) {
      results.push(mapSafeNodeResult({ node, startedAt, safe: safeRes }));
      if (safeRes.status === "failed") {
        errors.push(`${nodeId}: safe node failed`);
        break;
      }
      continue;
    }

    if (node.type === "manual_review") {
      skipped.push({ nodeId, type: node.type, reason: "Manual review required" });
      results.push({
        nodeId,
        type: node.type,
        tool: node.tool,
        status: "skipped",
        executor: "manual",
        attempts: [],
        outputs: node.outputs,
      });
      continue;
    }

    const hasCommands = (node.commands?.length ?? 0) > 0;
    if (!hasCommands) {
      skipped.push({ nodeId, type: node.type, reason: "No commands provided" });
      results.push({
        nodeId,
        type: node.type,
        tool: node.tool,
        status: "skipped",
        executor: "manual",
        attempts: [],
        outputs: node.outputs,
      });
      continue;
    }

    let nodeResult: ExecuteNodeResult;
    try {
      let hostWorkdirOverride: string | undefined;
      const repoRef = repoWorkflow.resolveRepoRef(node);
      if (repoRef && shouldUseRepoWorktreeForNode(node)) {
        const record = await repoWorkflow.prepareWorktree(repoRef);
        hostWorkdirOverride = record.worktreeAbs;
        const list = executedNodeIdsByRepoKey.get(repoRef.repoKey) ?? [];
        list.push(node.id);
        executedNodeIdsByRepoKey.set(repoRef.repoKey, list);
      }

      const maybeRepair = repoRef && hostWorkdirOverride && repairHook ? repairHook : undefined;

      nodeResult = isGpuNode(node)
        ? await (async () => {
            const nodeKey = (opts.node ?? "").trim();
            if (!nodeKey) {
              return await runGpuNodeViaScheduler({
                planDir,
                node,
                dryRun: opts.dryRun,
                commandTimeoutMs: opts.commandTimeoutMs,
                maxAttempts: opts.maxAttempts,
                retryDelayMs: opts.retryDelayMs,
                retrySpec,
                hostWorkdirOverride,
                gatewayUrl: opts.gatewayUrl,
                gatewayToken: opts.gatewayToken,
                gatewayTimeoutMs: opts.gatewayTimeoutMs,
                invokeTimeoutMs: opts.invokeTimeoutMs,
                nodeApprove: opts.nodeApprove,
                callGateway: callGatewayImpl,
                maybeRepair,
              });
            }
            return await runGpuNodeViaGateway({
              planDir,
              node,
              nodeId: nodeKey,
              dryRun: opts.dryRun,
              commandTimeoutMs: opts.commandTimeoutMs,
              maxAttempts: opts.maxAttempts,
              retryDelayMs: opts.retryDelayMs,
              retrySpec,
              hostWorkdirOverride,
              gatewayUrl: opts.gatewayUrl,
              gatewayToken: opts.gatewayToken,
              gatewayTimeoutMs: opts.gatewayTimeoutMs,
              invokeTimeoutMs: opts.invokeTimeoutMs,
              nodeApprove: opts.nodeApprove,
              callGateway: callGatewayImpl,
              maybeRepair,
            });
          })()
        : await runCpuShellNode({
            planDir,
            node,
            dryRun: opts.dryRun,
            commandTimeoutMs: opts.commandTimeoutMs,
            maxAttempts: opts.maxAttempts,
            retryDelayMs: opts.retryDelayMs,
            retrySpec,
            hostWorkdirOverride,
            runInSandbox,
            maybeRepair,
          });

      if (repoRef) {
        repoWorkflow.recordNode(repoRef.repoKey, {
          nodeId: node.id,
          status: nodeResult.status,
        });
      }
    } catch (err) {
      errors.push(`${nodeId}: ${String(err)}`);
      break;
    }

    results.push(nodeResult);
    if (nodeResult.status === "failed") {
      errors.push(`${nodeId}: execution failed`);
      break;
    }
  }

  const finishedAt = new Date().toISOString();
  const out = finalizeExecuteResult({
    planDir,
    planId,
    startedAt,
    finishedAt,
    warnings,
    errors,
    results,
    skipped,
  });
  await writeExecuteArtifacts(out);

  const records = repoWorkflow.listWorktrees();
  if (records.length > 0) {
    try {
      await writeRepoWorkflowEvidence({
        planDir,
        planId,
        dag: validation.data.dag,
        execute: out,
        records,
        executedNodeIdsByRepoKey,
        runHostCommand: runCommandWithTimeout,
      });
    } catch (err) {
      warnings.push(`Repo workflow evidence failed: ${String(err)}`);
      const outWithWarning = finalizeExecuteResult({
        planDir,
        planId,
        startedAt,
        finishedAt,
        warnings,
        errors,
        results,
        skipped,
      });
      await writeExecuteArtifacts(outWithWarning);
      return outWithWarning;
    }
  }
  return out;
}

function finalizeExecuteResult(params: {
  planDir: string;
  planId?: string;
  startedAt: string;
  finishedAt: string;
  warnings: string[];
  errors: string[];
  results: ExecuteNodeResult[];
  skipped: Array<{ nodeId: string; type: string; reason: string }>;
}): ProposalExecuteResult {
  const executeLog = path.join(params.planDir, "report", "execute_log.json");
  const executeSummary = path.join(params.planDir, "report", "execute_summary.md");
  return {
    ok: params.errors.length === 0,
    planDir: params.planDir,
    planId: params.planId,
    startedAt: params.startedAt,
    finishedAt: params.finishedAt,
    warnings: params.warnings,
    errors: params.errors,
    results: params.results,
    skipped: params.skipped,
    paths: { executeLog, executeSummary },
  };
}

async function writeExecuteArtifacts(result: ProposalExecuteResult): Promise<void> {
  await writeJsonFile(result.paths.executeLog, {
    ok: result.ok,
    planId: result.planId,
    planDir: result.planDir,
    startedAt: result.startedAt,
    finishedAt: result.finishedAt,
    warnings: result.warnings,
    errors: result.errors,
    results: result.results,
    skipped: result.skipped,
  });
  await writeTextFile(result.paths.executeSummary, renderExecuteSummary(result));
}
