import fs from "node:fs/promises";
import path from "node:path";
import type { OpenClawConfig } from "../config/config.js";
import type { ProposalLlmClient } from "./llm.js";
import type {
  AcceptanceSpec,
  CompileReport,
  DiscoveryMode,
  NeedsConfirmItem,
  PlanDag,
  ProposalEntities,
} from "./schema.js";
import { buildAcceptance } from "./acceptance.js";
import { validateDag } from "./dag.js";
import { discoverAll } from "./discovery.js";
import { extractEntities } from "./extract.js";
import { writeJsonFile, writeTextFile, copyFile } from "./files.js";
import { resolveProposalLlmClient } from "./llm.js";
import { buildSkeletonPlan } from "./plan.js";
import { renderNeedsConfirmMd, renderRunbookMd } from "./render.js";
import { buildDefaultRetrySpec } from "./retry.js";
import { CompileReportSchema, DiscoveryModeSchema } from "./schema.js";
import { createPlanLayout, generatePlanId } from "./workdir.js";

export type CompileProposalParams = {
  proposalPath?: string;
  proposalMarkdown?: string;
  proposalSource?: string;
  cfg: OpenClawConfig;
  agentId?: string;
  workspaceDir: string;
  outDir?: string;
  discovery: DiscoveryMode;
  modelOverride?: string;
  fetchFn?: (input: string, init?: RequestInit) => Promise<Response>;
  llmClient?: ProposalLlmClient;
  useLlm?: boolean;
};

export type CompileProposalResult = {
  ok: boolean;
  planId: string;
  rootDir: string;
  report: CompileReport;
  paths: Record<string, string>;
};

function buildNeedsConfirm(params: {
  entities: ProposalEntities;
  acceptance: AcceptanceSpec;
  discoveryWarnings: string[];
  discovery: Awaited<ReturnType<typeof discoverAll>>;
  dag: PlanDag;
}): NeedsConfirmItem[] {
  const items: NeedsConfirmItem[] = [];

  for (const repo of params.discovery.repos) {
    if (repo.exists) {
      continue;
    }
    const id = `repo:${repo.input.name ?? repo.resolvedUrl ?? "unknown"}`;
    items.push({
      id,
      area: "repo",
      message: `Repo could not be verified: ${repo.input.name ?? repo.resolvedUrl ?? "<unknown>"}`,
      suggested: repo.resolvedUrl,
      evidence: repo.evidence ?? [],
    });
  }

  for (const ds of params.discovery.datasets) {
    if (ds.platform === "kaggle") {
      items.push({
        id: `dataset:kaggle:${ds.resolvedId ?? ds.input.name ?? "unknown"}`,
        area: "dataset",
        message: "Kaggle dataset requires credentials and manual download/config.",
        suggested: ds.resolvedUrl,
        evidence: ds.evidence ?? [],
      });
      continue;
    }
    if (ds.exists === false) {
      items.push({
        id: `dataset:${ds.platform}:${ds.resolvedId ?? ds.input.name ?? "unknown"}`,
        area: "dataset",
        message: `Dataset could not be verified (${ds.platform}).`,
        suggested: ds.resolvedUrl ?? ds.resolvedId,
        evidence: ds.evidence ?? [],
      });
    }
  }

  for (const check of params.acceptance.checks) {
    if (!check.needs_confirm) {
      continue;
    }
    const id = `accept:${check.type}:${check.selector}`;
    const area = check.type === "metric_threshold" ? "metric" : "other";
    items.push({
      id,
      area,
      message: `Acceptance check requires confirmation: ${check.type} ${check.selector}`,
      suggested:
        check.type === "metric_threshold"
          ? `Set threshold (op=${check.op ?? ">="}, value=<number>)`
          : undefined,
      evidence: check.evidence ?? [],
    });
  }

  if (!params.entities.constraints?.gpu && params.dag.nodes.some((n) => n.type === "train")) {
    items.push({
      id: "resource:gpu",
      area: "resource",
      message:
        "GPU requirements are not specified in the proposal; confirm resources before running training.",
      suggested: "Set constraints.gpu (e.g. 1) and optionally gpuMemGB/gpuType.",
      evidence: [],
    });
  }

  return items;
}

export async function compileProposal(
  params: CompileProposalParams,
): Promise<CompileProposalResult> {
  const discoveryParsed = DiscoveryModeSchema.safeParse(params.discovery);
  if (!discoveryParsed.success) {
    throw new Error(`Invalid discovery mode: ${params.discovery}`);
  }

  const proposalPathRaw = typeof params.proposalPath === "string" ? params.proposalPath.trim() : "";
  const resolvedProposalPath = proposalPathRaw ? path.resolve(proposalPathRaw) : null;
  const proposalMarkdown =
    typeof params.proposalMarkdown === "string"
      ? params.proposalMarkdown
      : resolvedProposalPath
        ? await fs.readFile(resolvedProposalPath, "utf-8")
        : null;
  if (!proposalMarkdown || !proposalMarkdown.trim()) {
    throw new Error("Missing proposal input: provide proposalMarkdown or proposalPath");
  }

  const llmClient = await (async () => {
    if (params.useLlm === false) {
      return undefined;
    }
    if (params.llmClient) {
      return params.llmClient;
    }
    const resolved = await resolveProposalLlmClient({
      cfg: params.cfg,
      modelOverride: params.modelOverride,
      agentId: params.agentId,
    });
    return resolved.ok ? resolved.client : undefined;
  })();

  const modelKey = llmClient?.modelKey;
  const idInfo = generatePlanId({
    proposalMarkdown,
    discovery: params.discovery,
    modelKey,
  });

  const layout = await createPlanLayout({
    planId: idInfo.planId,
    workspaceDir: params.workspaceDir,
    outDir: params.outDir,
  });

  const warnings: string[] = [];
  const errors: string[] = [];
  if (params.discovery === "off") {
    warnings.push("Discovery mode is off; repos/datasets are not verified or sampled.");
  }

  const extracted = await extractEntities({ proposalMarkdown, llmClient });
  warnings.push(...extracted.warnings);

  const discovery = await discoverAll({
    repos: extracted.entities.repos,
    datasets: extracted.entities.datasets,
    mode: params.discovery,
    fetchFn: params.fetchFn,
  });

  const acceptanceRes = await buildAcceptance({
    entities: extracted.entities,
    discovery,
    llmClient,
  });
  warnings.push(...acceptanceRes.warnings);

  const retry = buildDefaultRetrySpec();
  const dag = buildSkeletonPlan({ entities: extracted.entities, discovery });
  const dagValidation = validateDag(dag);
  if (!dagValidation.ok) {
    errors.push(...dagValidation.errors);
  }

  const needsConfirm = buildNeedsConfirm({
    entities: extracted.entities,
    acceptance: acceptanceRes.spec,
    discoveryWarnings: [],
    discovery,
    dag,
  });

  const report: CompileReport = CompileReportSchema.parse({
    planId: layout.planId,
    createdAt: idInfo.createdAt,
    model: modelKey,
    discovery: params.discovery,
    warnings,
    errors,
    needsConfirm,
  });

  // Write input
  if (resolvedProposalPath) {
    await copyFile(resolvedProposalPath, path.join(layout.inputDir, "proposal.md"));
  } else {
    await writeTextFile(path.join(layout.inputDir, "proposal.md"), proposalMarkdown);
  }
  await writeJsonFile(path.join(layout.inputDir, "context.json"), {
    planId: layout.planId,
    createdAt: report.createdAt,
    proposalPath: resolvedProposalPath,
    proposalSource: params.proposalSource,
    discovery: params.discovery,
    model: modelKey,
    agentId: params.agentId,
    workspaceDir: params.workspaceDir,
  });

  // Write IR + discovery
  await writeJsonFile(path.join(layout.irDir, "extracted.entities.json"), extracted.entities);
  await writeJsonFile(path.join(layout.irDir, "discovery.json"), discovery);
  if (extracted.raw) {
    await writeTextFile(path.join(layout.irDir, "extracted.entities.raw.txt"), extracted.raw);
  }

  // Write plan
  await writeJsonFile(path.join(layout.planDir, "plan.dag.json"), dag);
  await writeJsonFile(path.join(layout.planDir, "acceptance.json"), acceptanceRes.spec);
  await writeJsonFile(path.join(layout.planDir, "retry.json"), retry);
  if (acceptanceRes.raw) {
    await writeTextFile(path.join(layout.planDir, "acceptance.raw.txt"), acceptanceRes.raw);
  }

  // Write reports
  await writeJsonFile(path.join(layout.reportDir, "compile_report.json"), report);
  await writeTextFile(
    path.join(layout.reportDir, "needs_confirm.md"),
    renderNeedsConfirmMd(report),
  );
  await writeTextFile(
    path.join(layout.reportDir, "runbook.md"),
    renderRunbookMd({ layout, report }),
  );

  const resultPaths = {
    rootDir: layout.rootDir,
    proposal: path.join(layout.inputDir, "proposal.md"),
    entities: path.join(layout.irDir, "extracted.entities.json"),
    discovery: path.join(layout.irDir, "discovery.json"),
    dag: path.join(layout.planDir, "plan.dag.json"),
    acceptance: path.join(layout.planDir, "acceptance.json"),
    retry: path.join(layout.planDir, "retry.json"),
    compileReport: path.join(layout.reportDir, "compile_report.json"),
    needsConfirm: path.join(layout.reportDir, "needs_confirm.md"),
    runbook: path.join(layout.reportDir, "runbook.md"),
  };

  return {
    ok: report.errors.length === 0,
    planId: layout.planId,
    rootDir: layout.rootDir,
    report,
    paths: resultPaths,
  };
}
