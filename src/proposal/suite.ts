import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { OpenClawConfig } from "../config/config.js";
import type { DagPatchOp, ExperimentOverrides, ExperimentSuiteDesign } from "./design.schema.js";
import type { PlanLayout } from "./workdir.js";
import { validateDag } from "./dag.js";
import { designExperimentSuite, type ExperimentSuiteDesignResult } from "./design.js";
import { writeJsonFile, writeTextFile } from "./files.js";
import { expandExperimentAxes } from "./matrix.js";
import {
  runExperimentPipeline,
  type ExperimentPipelineInput,
  type ExperimentPipelineResult,
  type ExperimentPipelineStages,
} from "./pipeline.js";
import { refineProposalPlan } from "./refine.js";
import { renderNeedsConfirmMd, renderPlanRunbookMd, renderRunbookMd } from "./render.js";
import {
  AcceptanceSpecSchema,
  CompileReportSchema,
  DiscoveryModeSchema,
  PlanDagSchema,
  ResourceSpecSchema,
  type CompileReport,
  type DiscoveryMode,
  type PlanDag,
  type ResourceSpec,
} from "./schema.js";
import { validatePlanDir } from "./validate.js";
import { formatPlanTimestamp, generatePlanId } from "./workdir.js";

function normalizeRel(p: string): string {
  return p.replaceAll("\\", "/");
}

function sanitizeExperimentId(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return "";
  }
  const safe = trimmed.replaceAll(/[^a-zA-Z0-9._-]+/g, "-").replaceAll(/-+/g, "-");
  return safe.replaceAll(/^-|-$/g, "").toLowerCase();
}

function randomSuffix(bytes = 3): string {
  return crypto.randomBytes(bytes).toString("hex");
}

function generateSuiteId(now = new Date()): string {
  return `suite-${formatPlanTimestamp(now)}-${randomSuffix(3)}`;
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.stat(filePath);
    return true;
  } catch {
    return false;
  }
}

type ReadJsonResult = { ok: true; value: unknown } | { ok: false };

async function readJsonIfExists(filePath: string): Promise<ReadJsonResult> {
  try {
    const raw = await fs.readFile(filePath, "utf-8");
    return { ok: true, value: JSON.parse(raw) as unknown };
  } catch {
    return { ok: false };
  }
}

async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const out = new Array<R>(items.length);
  const concurrency = Math.max(1, Math.floor(limit));
  let next = 0;

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (true) {
      const idx = next;
      next += 1;
      if (idx >= items.length) {
        return;
      }
      out[idx] = await fn(items[idx] as T, idx);
    }
  });

  await Promise.all(workers);
  return out;
}

function inferWorkspaceDirFromPlanDir(planDir: string): string | null {
  const normalized = normalizeRel(path.resolve(planDir));
  const parts = normalized.split("/");
  const idx = parts.lastIndexOf("workdir");
  if (idx >= 1 && parts[idx - 1] === "experiments") {
    return parts.slice(0, idx - 1).join("/") || null;
  }
  return null;
}

async function resolveWorkspaceDir(params: {
  input: ExperimentPipelineInput;
  compile?: ExperimentPipelineStages["compile"];
}): Promise<string> {
  if (params.compile?.workspaceDir?.trim()) {
    return path.resolve(params.compile.workspaceDir.trim());
  }
  if (params.input.kind === "planDir") {
    const ctxPath = path.join(path.resolve(params.input.planDir), "input", "context.json");
    const raw = await readJsonIfExists(ctxPath);
    const obj =
      raw.ok && raw.value && typeof raw.value === "object"
        ? (raw.value as Record<string, unknown>)
        : {};
    const workspaceDir = typeof obj.workspaceDir === "string" ? obj.workspaceDir.trim() : "";
    if (workspaceDir) {
      return path.resolve(workspaceDir);
    }
    const inferred = inferWorkspaceDirFromPlanDir(params.input.planDir);
    if (inferred) {
      return inferred;
    }
  }
  return path.resolve(process.cwd());
}

async function copyPlanPackage(params: { srcPlanDir: string; destPlanDir: string }): Promise<void> {
  const src = path.resolve(params.srcPlanDir);
  const dest = path.resolve(params.destPlanDir);

  if (await fileExists(dest)) {
    throw new Error(`Destination already exists: ${dest}`);
  }

  await fs.mkdir(path.dirname(dest), { recursive: true });

  const shouldSkip = (rel: string) => {
    const normalized = normalizeRel(rel);
    if (!normalized) {
      return false;
    }
    if (normalized === "cache" || normalized.startsWith("cache/")) {
      return true;
    }
    if (normalized === "artifacts" || normalized.startsWith("artifacts/")) {
      return true;
    }
    if (normalized === "report/runs" || normalized.startsWith("report/runs/")) {
      return true;
    }
    if (normalized === "report/repairs" || normalized.startsWith("report/repairs/")) {
      return true;
    }
    if (normalized === "plan/scripts" || normalized.startsWith("plan/scripts/")) {
      return true;
    }
    return false;
  };

  await fs.cp(src, dest, {
    recursive: true,
    filter: (srcPath) => {
      const rel = path.relative(src, srcPath);
      return !shouldSkip(rel);
    },
  });
}

async function ensureDir(dirPath: string): Promise<void> {
  await fs.mkdir(dirPath, { recursive: true });
}

async function trySymlinkDir(params: {
  targetAbs: string;
  linkAbs: string;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  try {
    await fs.symlink(
      params.targetAbs,
      params.linkAbs,
      process.platform === "win32" ? "junction" : "dir",
    );
    return { ok: true };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function setupSharedCache(params: {
  planDir: string;
  sharedFromPlanDir: string;
  share: string[];
  ensure: string[];
}): Promise<{ warnings: string[] }> {
  const warnings: string[] = [];
  const planDir = path.resolve(params.planDir);
  const targetRoot = path.join(planDir, "cache");
  await ensureDir(targetRoot);

  const sourceRoot = path.join(path.resolve(params.sharedFromPlanDir), "cache");

  for (const name of params.share) {
    const destAbs = path.join(targetRoot, name);
    const srcAbs = path.join(sourceRoot, name);
    try {
      await fs.rm(destAbs, { recursive: true, force: true });
    } catch {
      // ignore
    }
    if (!(await fileExists(srcAbs))) {
      await ensureDir(destAbs);
      continue;
    }
    const linked = await trySymlinkDir({ targetAbs: srcAbs, linkAbs: destAbs });
    if (!linked.ok) {
      warnings.push(`Failed to symlink cache/${name}: ${linked.error}`);
      await ensureDir(destAbs);
    }
  }

  for (const name of params.ensure) {
    await ensureDir(path.join(targetRoot, name));
  }

  return { warnings };
}

async function rewritePlanMetadata(params: {
  planDir: string;
  newPlanId: string;
  createdAt: string;
}): Promise<{ ok: boolean; warnings: string[]; errors: string[] }> {
  const warnings: string[] = [];
  const errors: string[] = [];
  const planDir = path.resolve(params.planDir);

  const compileReportPath = path.join(planDir, "report", "compile_report.json");
  const contextPath = path.join(planDir, "input", "context.json");
  const needsConfirmPath = path.join(planDir, "report", "needs_confirm.md");
  const runbookPath = path.join(planDir, "report", "runbook.md");
  const planRunbookPath = path.join(planDir, "plan", "runbook.md");

  const reportRaw = await readJsonIfExists(compileReportPath);
  if (!reportRaw.ok) {
    errors.push("Missing report/compile_report.json");
    return { ok: false, warnings, errors };
  }
  const parsedReport = CompileReportSchema.safeParse(reportRaw.value);
  if (!parsedReport.success) {
    errors.push(`Invalid compile_report.json: ${parsedReport.error.message}`);
    return { ok: false, warnings, errors };
  }
  const report: CompileReport = {
    ...parsedReport.data,
    planId: params.newPlanId,
    createdAt: params.createdAt,
  };
  await writeJsonFile(compileReportPath, report);

  const ctxRaw = await readJsonIfExists(contextPath);
  if (ctxRaw.ok && ctxRaw.value && typeof ctxRaw.value === "object") {
    const obj = ctxRaw.value as Record<string, unknown>;
    await writeJsonFile(contextPath, {
      ...obj,
      planId: params.newPlanId,
      createdAt: params.createdAt,
    });
  } else {
    warnings.push("Missing input/context.json; planId metadata not updated there.");
  }

  const dagRaw = await readJsonIfExists(path.join(planDir, "plan", "plan.dag.json"));
  const dagParsed = PlanDagSchema.safeParse(dagRaw.ok ? dagRaw.value : null);
  if (!dagParsed.success) {
    errors.push(`Invalid plan DAG schema: ${dagParsed.error.message}`);
    return { ok: false, warnings, errors };
  }
  const dag = dagParsed.data;

  const layout: PlanLayout = {
    planId: params.newPlanId,
    rootDir: planDir,
    inputDir: path.join(planDir, "input"),
    irDir: path.join(planDir, "ir"),
    planDir: path.join(planDir, "plan"),
    reportDir: path.join(planDir, "report"),
    cacheDir: path.join(planDir, "cache"),
  };

  await writeTextFile(needsConfirmPath, renderNeedsConfirmMd(report));
  await writeTextFile(runbookPath, renderRunbookMd({ layout, report }));
  await writeTextFile(planRunbookPath, renderPlanRunbookMd({ layout, report, dag }));

  return { ok: true, warnings, errors };
}

function mergeResources(
  base: ResourceSpec | undefined,
  override: ResourceSpec | undefined,
): ResourceSpec | undefined {
  if (!override) {
    return base;
  }
  const parsed = ResourceSpecSchema.safeParse(override);
  if (!parsed.success) {
    return base;
  }
  if (!base) {
    return parsed.data;
  }
  return { ...base, ...parsed.data };
}

export async function applyExperimentOverridesToPlan(params: {
  planDir: string;
  overrides: ExperimentOverrides;
}): Promise<{ ok: boolean; warnings: string[]; errors: string[] }> {
  const warnings: string[] = [];
  const errors: string[] = [];
  const planDir = path.resolve(params.planDir);

  const overrides = params.overrides ?? {};
  const envOverrides = overrides.env ?? {};
  const resourcesOverride = overrides.resources;
  const acceptanceOverride = overrides.acceptance;

  const dagPath = path.join(planDir, "plan", "plan.dag.json");
  const dagRaw = await readJsonIfExists(dagPath);
  const dagParsed = PlanDagSchema.safeParse(dagRaw.ok ? dagRaw.value : null);
  if (!dagParsed.success) {
    errors.push(`Invalid plan DAG schema: ${dagParsed.error.message}`);
    return { ok: false, warnings, errors };
  }
  const dag: PlanDag = dagParsed.data;

  const envTargets = new Set([
    "setup.venv",
    "install.deps",
    "train.run",
    "eval.run",
    "report.write",
  ]);
  const resourceTargets = new Set(["train.run", "eval.run"]);

  const nextNodes = dag.nodes.map((node) => {
    const next = { ...node };
    if (Object.keys(envOverrides).length > 0 && envTargets.has(node.id)) {
      next.env = node.env ? { ...node.env, ...envOverrides } : { ...envOverrides };
    }
    if (resourcesOverride && resourceTargets.has(node.id)) {
      next.resources = mergeResources(node.resources, resourcesOverride);
    }
    return next;
  });

  const nextDag: PlanDag = { ...dag, nodes: nextNodes };
  await writeJsonFile(dagPath, nextDag);

  if (acceptanceOverride) {
    const parsed = AcceptanceSpecSchema.safeParse(acceptanceOverride);
    if (!parsed.success) {
      warnings.push(`Ignoring invalid acceptance override: ${parsed.error.message}`);
    } else {
      await writeJsonFile(path.join(planDir, "plan", "acceptance.json"), parsed.data);
    }
  }

  return { ok: errors.length === 0, warnings, errors };
}

function normalizeDagPatchPath(rawPath: string): string {
  return rawPath.trim().replaceAll("\\", "/");
}

function isSafePatchRelPath(relPath: string): boolean {
  const p = normalizeDagPatchPath(relPath);
  if (!p) {
    return false;
  }
  if (p.startsWith("/") || p.startsWith("../") || p.includes("/../")) {
    return false;
  }
  return true;
}

export async function applyExperimentDagPatchOpsToPlan(params: {
  planDir: string;
  ops: DagPatchOp[];
}): Promise<{ ok: boolean; warnings: string[]; errors: string[] }> {
  const warnings: string[] = [];
  const errors: string[] = [];
  const planDir = path.resolve(params.planDir);
  const ops = params.ops ?? [];
  if (ops.length === 0) {
    return { ok: true, warnings, errors };
  }

  const dagPath = path.join(planDir, "plan", "plan.dag.json");
  const dagRaw = await readJsonIfExists(dagPath);
  const dagParsed = PlanDagSchema.safeParse(dagRaw.ok ? dagRaw.value : null);
  if (!dagParsed.success) {
    errors.push(`Invalid plan DAG schema: ${dagParsed.error.message}`);
    return { ok: false, warnings, errors };
  }

  const dag = dagParsed.data;
  let nextNodes = [...dag.nodes];
  let nextEdges = [...(dag.edges ?? [])];

  const nodeById = new Map(nextNodes.map((node) => [node.id, node]));
  const edgeKey = (from: string, to: string) => `${from}→${to}`;
  let edgeKeys = new Set(nextEdges.map((edge) => edgeKey(edge.from, edge.to)));

  for (const op of ops) {
    if (op.op === "addNode") {
      if (nodeById.has(op.node.id)) {
        errors.push(`dagPatchOps addNode: node already exists: ${op.node.id}`);
        continue;
      }
      nextNodes.push(op.node);
      nodeById.set(op.node.id, op.node);
      continue;
    }

    if (op.op === "removeNode") {
      const nodeId = op.nodeId.trim();
      if (!nodeId) {
        warnings.push("dagPatchOps removeNode: empty nodeId");
        continue;
      }
      if (!nodeById.has(nodeId)) {
        warnings.push(`dagPatchOps removeNode: unknown nodeId: ${nodeId}`);
        continue;
      }
      nextNodes = nextNodes.filter((node) => node.id !== nodeId);
      nodeById.delete(nodeId);
      nextEdges = nextEdges.filter((edge) => edge.from !== nodeId && edge.to !== nodeId);
      edgeKeys = new Set(nextEdges.map((edge) => edgeKey(edge.from, edge.to)));
      continue;
    }

    if (op.op === "replaceNode") {
      if (!nodeById.has(op.node.id)) {
        errors.push(`dagPatchOps replaceNode: unknown nodeId: ${op.node.id}`);
        continue;
      }
      nextNodes = nextNodes.map((node) => (node.id === op.node.id ? op.node : node));
      nodeById.set(op.node.id, op.node);
      continue;
    }

    if (op.op === "addEdge") {
      const from = op.edge.from.trim();
      const to = op.edge.to.trim();
      if (!from || !to) {
        warnings.push("dagPatchOps addEdge: empty from/to");
        continue;
      }
      if (!nodeById.has(from)) {
        errors.push(`dagPatchOps addEdge: unknown from node: ${from}`);
        continue;
      }
      if (!nodeById.has(to)) {
        errors.push(`dagPatchOps addEdge: unknown to node: ${to}`);
        continue;
      }
      const key = edgeKey(from, to);
      if (edgeKeys.has(key)) {
        continue;
      }
      nextEdges.push(op.edge);
      edgeKeys.add(key);
      continue;
    }

    if (op.op === "removeEdge") {
      const from = op.from.trim();
      const to = op.to.trim();
      if (!from || !to) {
        warnings.push("dagPatchOps removeEdge: empty from/to");
        continue;
      }
      const key = edgeKey(from, to);
      if (!edgeKeys.has(key)) {
        warnings.push(`dagPatchOps removeEdge: unknown edge: ${from} -> ${to}`);
        continue;
      }
      nextEdges = nextEdges.filter((edge) => !(edge.from === from && edge.to === to));
      edgeKeys = new Set(nextEdges.map((edge) => edgeKey(edge.from, edge.to)));
      continue;
    }
  }

  // Sanity-check file path fields for newly added nodes.
  for (const node of nextNodes) {
    for (const rel of [...(node.inputs ?? []), ...(node.outputs ?? [])]) {
      if (rel && !isSafePatchRelPath(rel)) {
        errors.push(`dagPatchOps: unsafe path in node ${node.id}: ${rel}`);
      }
    }
  }

  // Ensure pipeline-critical nodes remain present.
  for (const required of ["setup.venv", "install.deps", "train.run", "eval.run", "report.write"]) {
    if (!nodeById.has(required)) {
      errors.push(`dagPatchOps: missing required node: ${required}`);
    }
  }

  const nextDag: PlanDag = { nodes: nextNodes, edges: nextEdges };
  const topo = validateDag(nextDag);
  if (!topo.ok) {
    errors.push(...topo.errors);
  }

  if (errors.length > 0) {
    return { ok: false, warnings, errors };
  }

  await writeJsonFile(dagPath, nextDag);
  return { ok: true, warnings, errors };
}

export type ExperimentSuiteRunOpts = {
  variantCount: number;
  suiteOutDir?: string;
  concurrency?: number;
  designModel?: string;
  designAgent?: string;
  designTimeoutMs?: number;
};

export type ExperimentSuiteExperimentResult = {
  id: string;
  name: string;
  planDir: string;
  pipeline: ExperimentPipelineResult;
};

export type ExperimentSuiteRunResult = {
  ok: boolean;
  suiteId: string;
  suiteDir: string;
  startedAt: string;
  finishedAt: string;
  warnings: string[];
  errors: string[];
  design?: ExperimentSuiteDesignResult;
  experiments: ExperimentSuiteExperimentResult[];
  paths: {
    suiteJson: string;
    suiteSummary: string;
  };
};

function renderSuiteSummary(params: {
  suiteId: string;
  suiteDir: string;
  warnings: string[];
  errors: string[];
  experiments: ExperimentSuiteExperimentResult[];
}): string {
  const lines: string[] = [];
  lines.push("# Experiment Suite Summary");
  lines.push("");
  lines.push(`Suite: \`${params.suiteId}\``);
  lines.push(`Dir: \`${params.suiteDir}\``);
  lines.push("");

  if (params.errors.length > 0) {
    lines.push("## Errors");
    lines.push("");
    for (const e of params.errors) {
      lines.push(`- ${e}`);
    }
    lines.push("");
  }

  if (params.warnings.length > 0) {
    lines.push("## Warnings");
    lines.push("");
    for (const w of params.warnings) {
      lines.push(`- ${w}`);
    }
    lines.push("");
  }

  lines.push("## Experiments");
  lines.push("");
  for (const exp of params.experiments) {
    const accepted = exp.pipeline.accept;
    const status = accepted ? accepted.status : exp.pipeline.ok ? "ok" : "failed";
    lines.push(`- \`${exp.id}\`: ${status} (${exp.name})`);
    lines.push(`  - planDir: \`${exp.planDir}\``);
    if (accepted?.runDir) {
      lines.push(`  - runDir: \`${accepted.runDir}\``);
    }
  }
  lines.push("");

  const estimates = params.experiments
    .map((exp) => {
      const dag = exp.pipeline.validate?.data?.dag;
      if (!dag) {
        return null;
      }
      const train =
        dag.nodes.find((n) => n.id === "train.run") ?? dag.nodes.find((n) => n.type === "train");
      const gpuCountRaw = train?.resources?.gpuCount;
      const minutesRaw = train?.resources?.estimatedMinutes;
      const gpuCount =
        typeof gpuCountRaw === "number" && Number.isFinite(gpuCountRaw)
          ? Math.max(0, Math.floor(gpuCountRaw))
          : null;
      const minutes =
        typeof minutesRaw === "number" && Number.isFinite(minutesRaw)
          ? Math.max(0, Math.floor(minutesRaw))
          : null;
      if (!gpuCount || !minutes) {
        return null;
      }
      return { id: exp.id, name: exp.name, gpuCount, minutes, gpuMinutes: gpuCount * minutes };
    })
    .filter((v): v is NonNullable<typeof v> => Boolean(v));

  if (estimates.length > 0) {
    const totalGpuMinutes = estimates.reduce((acc, est) => acc + est.gpuMinutes, 0);
    lines.push("## Estimates");
    lines.push("");
    for (const est of estimates) {
      const gpuHours = (est.gpuMinutes / 60).toFixed(2);
      lines.push(
        `- \`${est.id}\`: gpuCount=${est.gpuCount} estimatedMinutes=${est.minutes} (gpuHours=${gpuHours})`,
      );
    }
    lines.push(`- total: gpuHours=${(totalGpuMinutes / 60).toFixed(2)}`);
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

function buildVariantRefineInstructions(params: {
  variant: { id: string; name: string; rationale?: string };
  overrides: ExperimentOverrides;
}): string {
  const lines: string[] = [];
  lines.push(`Experiment variant: ${params.variant.id}`);
  lines.push(`Name: ${params.variant.name}`);
  if ((params.variant.rationale ?? "").trim()) {
    lines.push(`Rationale: ${params.variant.rationale?.trim()}`);
  }
  const env = params.overrides.env ?? {};
  if (Object.keys(env).length > 0) {
    lines.push("");
    lines.push("Env overrides (already applied to input DAG):");
    for (const [k, v] of Object.entries(env)) {
      lines.push(`- ${k}=${JSON.stringify(v)}`);
    }
  }
  if (params.overrides.resources) {
    lines.push("");
    lines.push("Resource overrides (already applied to input DAG):");
    lines.push(JSON.stringify(params.overrides.resources, null, 2));
  }
  lines.push("");
  lines.push("Adjust the train/eval commands to reflect this variant when applicable.");
  return lines.join("\n");
}

export async function materializeExperimentPlanDir(params: {
  suiteId: string;
  experimentId: string;
  experimentName: string;
  baselinePlanDir: string;
  outPlanDir: string;
  proposalMarkdown: string;
  discovery: DiscoveryMode;
  modelKey?: string;
}): Promise<{
  ok: boolean;
  planDir: string;
  planId?: string;
  warnings: string[];
  errors: string[];
}> {
  const warnings: string[] = [];
  const errors: string[] = [];
  const baselinePlanDir = path.resolve(params.baselinePlanDir);
  const planDir = path.resolve(params.outPlanDir);

  await copyPlanPackage({ srcPlanDir: baselinePlanDir, destPlanDir: planDir });

  // Shared caches: reuse expensive downloads but keep per-experiment worktrees/venvs isolated.
  const cacheSetup = await setupSharedCache({
    planDir,
    sharedFromPlanDir: baselinePlanDir,
    share: ["git", "data", "hf", "pip", "kaggle"],
    ensure: ["worktrees", "venv"],
  });
  warnings.push(...cacheSetup.warnings);

  const id = generatePlanId({
    proposalMarkdown: params.proposalMarkdown,
    discovery: params.discovery,
    modelKey: `${params.modelKey ?? ""}|suite:${params.suiteId}|exp:${params.experimentId}`,
  });

  const rewrite = await rewritePlanMetadata({
    planDir,
    newPlanId: id.planId,
    createdAt: id.createdAt,
  });
  warnings.push(...rewrite.warnings);
  errors.push(...rewrite.errors);

  await writeJsonFile(path.join(planDir, "input", "suite.json"), {
    schemaVersion: 1,
    suiteId: params.suiteId,
    experimentId: params.experimentId,
    experimentName: params.experimentName,
    createdAt: id.createdAt,
    baselinePlanDir: baselinePlanDir,
  });

  const validation = await validatePlanDir(planDir);
  warnings.push(...validation.warnings);
  if (!validation.ok) {
    errors.push(...validation.errors);
  }

  return { ok: errors.length === 0, planDir, planId: id.planId, warnings, errors };
}

export async function runExperimentSuite(params: {
  input: ExperimentPipelineInput;
  cfg: OpenClawConfig;
  opts: ExperimentSuiteRunOpts;
  stages: ExperimentPipelineStages;
}): Promise<ExperimentSuiteRunResult> {
  const startedAt = new Date().toISOString();
  const warnings: string[] = [];
  const errors: string[] = [];

  const suiteId = generateSuiteId();
  const workspaceDir = await resolveWorkspaceDir({
    input: params.input,
    compile: params.stages.compile,
  });
  const suiteDir = params.opts.suiteOutDir?.trim()
    ? path.resolve(params.opts.suiteOutDir.trim())
    : path.join(workspaceDir, "experiments", "suites", suiteId);

  const paths = {
    suiteJson: path.join(suiteDir, "suite_results.json"),
    suiteSummary: path.join(suiteDir, "suite_summary.md"),
  };

  await ensureDir(path.join(suiteDir, "experiments"));

  const baselinePlanDir = path.join(suiteDir, "experiments", "baseline");
  let baselineInput: ExperimentPipelineInput = params.input;
  let baselineStages: ExperimentPipelineStages = { ...params.stages };

  if (params.input.kind === "planDir") {
    // Copy the provided plan package into the suite so suite artifacts stay contained.
    try {
      const proposalMarkdown = await fs.readFile(
        path.join(params.input.planDir, "input", "proposal.md"),
        "utf-8",
      );
      const ctxRaw = await readJsonIfExists(
        path.join(params.input.planDir, "input", "context.json"),
      );
      const ctx =
        ctxRaw.ok && ctxRaw.value && typeof ctxRaw.value === "object"
          ? (ctxRaw.value as Record<string, unknown>)
          : {};
      const discoveryRaw = typeof ctx.discovery === "string" ? ctx.discovery : "plan";
      const discovery = DiscoveryModeSchema.safeParse(discoveryRaw);
      const modelKey = typeof ctx.model === "string" ? ctx.model : "";

      const copied = await materializeExperimentPlanDir({
        suiteId,
        experimentId: "baseline",
        experimentName: "Baseline",
        baselinePlanDir: path.resolve(params.input.planDir),
        outPlanDir: baselinePlanDir,
        proposalMarkdown,
        discovery: discovery.success ? discovery.data : "plan",
        modelKey,
      });
      warnings.push(...copied.warnings);
      errors.push(...copied.errors);
    } catch (err) {
      errors.push(`Failed to copy baseline planDir: ${String(err)}`);
    }
    baselineInput = { kind: "planDir", planDir: baselinePlanDir };
  } else {
    const compile = params.stages.compile;
    if (!compile) {
      errors.push("compile options required for non-planDir suite inputs");
    } else {
      baselineStages = {
        ...baselineStages,
        compile: { ...compile, outDir: baselinePlanDir },
      };
    }
  }

  // Baseline plan stage (safe + refine).
  const baselineRefineStage =
    baselineStages.refine ??
    ({
      enabled: true,
      model: "opencode/kimi-k2.5-free",
      timeoutMs: 180_000,
      writeAcceptance: false,
    } satisfies ExperimentPipelineStages["refine"]);

  const baselinePlan = await runExperimentPipeline({
    action: "plan",
    input: baselineInput,
    cfg: params.cfg,
    stages: {
      ...baselineStages,
      refine: baselineRefineStage,
    },
  });
  const baselinePlanOk =
    Boolean(baselinePlan.validate?.ok && baselinePlan.validate.data) &&
    Boolean(baselinePlan.safe?.ok) &&
    (baselinePlan.refine ? ("ok" in baselinePlan.refine ? baselinePlan.refine.ok : true) : true);
  if (!baselinePlanOk) {
    errors.push("Baseline plan stage failed.");
  }

  if (!baselinePlanOk) {
    const finishedAt = new Date().toISOString();
    const out: ExperimentSuiteRunResult = {
      ok: false,
      suiteId,
      suiteDir,
      startedAt,
      finishedAt,
      warnings,
      errors,
      experiments: [],
      paths,
    };
    await writeJsonFile(paths.suiteJson, out);
    await writeTextFile(
      paths.suiteSummary,
      renderSuiteSummary({ suiteId, suiteDir, warnings, errors, experiments: [] }),
    );
    return out;
  }

  const design = await designExperimentSuite({
    planDir: baselinePlan.planDir,
    outDir: suiteDir,
    opts: {
      suiteId,
      variantCount: Math.max(0, Math.floor(params.opts.variantCount)),
      model: params.opts.designModel ?? baselineRefineStage?.model,
      agent: params.opts.designAgent ?? baselineRefineStage?.agent,
      timeoutMs: params.opts.designTimeoutMs ?? baselineRefineStage?.timeoutMs,
    },
  });
  warnings.push(...design.warnings);
  if (!design.ok || !design.design) {
    errors.push(...design.errors);
    errors.push("Suite design failed.");
  }

  const experiments: ExperimentSuiteExperimentResult[] = [];

  const finishedAtError = new Date().toISOString();
  if (errors.length > 0 || !design.design) {
    const out: ExperimentSuiteRunResult = {
      ok: false,
      suiteId,
      suiteDir,
      startedAt,
      finishedAt: finishedAtError,
      warnings,
      errors,
      design,
      experiments,
      paths,
    };
    await writeJsonFile(paths.suiteJson, out);
    await writeTextFile(
      paths.suiteSummary,
      renderSuiteSummary({ suiteId, suiteDir, warnings, errors, experiments }),
    );
    return out;
  }

  const suiteDesign: ExperimentSuiteDesign = design.design;
  const usedIds = new Set<string>();
  const addUniqueId = (rawId: string) => {
    const base = sanitizeExperimentId(rawId) || `variant-${usedIds.size + 1}`;
    let id = base;
    let i = 2;
    while (usedIds.has(id)) {
      id = `${base}-${i}`;
      i += 1;
    }
    usedIds.add(id);
    return id;
  };

  const baselineMeta = await validatePlanDir(baselinePlan.planDir);
  const baselineReport = baselineMeta.ok ? baselineMeta.data?.report : null;
  const baselineProposal = (
    await fs.readFile(path.join(baselinePlan.planDir, "input", "proposal.md"), "utf-8")
  ).toString();
  const baselineModelKey = baselineReport?.model;
  const baselineDiscovery = baselineReport?.discovery ?? "plan";

  // Ensure baseline has suite metadata and baseline overrides.
  await writeJsonFile(path.join(baselinePlan.planDir, "input", "suite.json"), {
    schemaVersion: 1,
    suiteId,
    experimentId: "baseline",
    experimentName: suiteDesign.baseline.name,
    createdAt: new Date().toISOString(),
    baselinePlanDir: baselinePlan.planDir,
  });

  const baselineOverridesRes = await applyExperimentOverridesToPlan({
    planDir: baselinePlan.planDir,
    overrides: suiteDesign.baseline.overrides ?? {},
  });
  warnings.push(...baselineOverridesRes.warnings);
  errors.push(...baselineOverridesRes.errors);

  const baselineDagPatchOps =
    suiteDesign.schemaVersion === 2 || suiteDesign.schemaVersion === 3
      ? (suiteDesign.baseline.dagPatchOps ?? [])
      : [];
  if (baselineDagPatchOps.length > 0) {
    const patched = await applyExperimentDagPatchOpsToPlan({
      planDir: baselinePlan.planDir,
      ops: baselineDagPatchOps,
    });
    warnings.push(...patched.warnings);
    errors.push(...patched.errors);

    if (patched.ok && baselineRefineStage.enabled !== false) {
      const { enabled: _enabled, ...refineOpts } = baselineRefineStage;
      const rerun = await refineProposalPlan({
        planDir: baselinePlan.planDir,
        opts: refineOpts,
      });
      warnings.push(...rerun.warnings);
      errors.push(...rerun.errors);
      if (!rerun.ok) {
        errors.push("Baseline re-refine after dagPatchOps failed.");
      }
    }
  }

  // Materialize variants.
  const desiredVariantCount = Math.max(0, Math.floor(params.opts.variantCount));
  const variantPlanDirs: Array<{
    id: string;
    name: string;
    planDir: string;
    overrides: ExperimentOverrides;
    dagPatchOps: DagPatchOp[];
    rationale?: string;
  }> = [];
  const variantSpecs: Array<{
    id: string;
    name: string;
    overrides: ExperimentOverrides;
    dagPatchOps: DagPatchOp[];
    rationale?: string;
  }> =
    suiteDesign.schemaVersion === 3
      ? (() => {
          const expanded = expandExperimentAxes({
            axes: suiteDesign.axes,
            maxVariants: desiredVariantCount,
          });
          warnings.push(...expanded.warnings);
          return expanded.variants.map((variant) => ({
            id: variant.id,
            name: variant.name,
            overrides: variant.overrides ?? {},
            dagPatchOps: variant.dagPatchOps ?? [],
            rationale: variant.rationale,
          }));
        })()
      : suiteDesign.schemaVersion === 2
        ? suiteDesign.variants.slice(0, desiredVariantCount).map((variant) => ({
            id: variant.id,
            name: variant.name,
            overrides: variant.overrides ?? {},
            dagPatchOps: variant.dagPatchOps ?? [],
            rationale: variant.rationale,
          }))
        : suiteDesign.variants.slice(0, desiredVariantCount).map((variant) => ({
            id: variant.id,
            name: variant.name,
            overrides: variant.overrides ?? {},
            dagPatchOps: [],
            rationale: variant.rationale,
          }));

  for (const variant of variantSpecs) {
    const id = addUniqueId(variant.id);
    const planDir = path.join(suiteDir, "experiments", id);
    const created = await materializeExperimentPlanDir({
      suiteId,
      experimentId: id,
      experimentName: variant.name,
      baselinePlanDir: baselinePlan.planDir,
      outPlanDir: planDir,
      proposalMarkdown: baselineProposal,
      discovery: baselineDiscovery,
      modelKey: baselineModelKey,
    });
    warnings.push(...created.warnings);
    errors.push(...created.errors);
    if (!created.ok) {
      errors.push(`Failed to materialize variant ${id}`);
      continue;
    }
    const overrides = variant.overrides ?? {};
    const applied = await applyExperimentOverridesToPlan({ planDir, overrides });
    warnings.push(...applied.warnings);
    errors.push(...applied.errors);
    const dagPatchOps = variant.dagPatchOps ?? [];
    if (dagPatchOps.length > 0) {
      const patched = await applyExperimentDagPatchOpsToPlan({ planDir, ops: dagPatchOps });
      warnings.push(...patched.warnings);
      errors.push(...patched.errors);
    }
    variantPlanDirs.push({
      id,
      name: variant.name,
      planDir,
      overrides,
      dagPatchOps,
      rationale: variant.rationale,
    });
  }

  // Execute baseline first to produce baseline metrics for deltas.
  const baselineExec = await runExperimentPipeline({
    action: "execute",
    input: { kind: "planDir", planDir: baselinePlan.planDir },
    cfg: params.cfg,
    stages: {
      run: baselineStages.run,
      bootstrap: baselineStages.bootstrap,
      execute: baselineStages.execute,
      finalize: baselineStages.finalize,
      accept: baselineStages.accept,
    },
  });
  experiments.push({
    id: "baseline",
    name: suiteDesign.baseline.name,
    planDir: baselineExec.planDir,
    pipeline: baselineExec,
  });

  if (!baselineExec.ok) {
    errors.push("Baseline execution failed; skipping variants.");
  } else {
    const baselineMetricsPath = path.join(baselineExec.planDir, "report", "final_metrics.json");
    const baselinePath = (await fileExists(baselineMetricsPath)) ? baselineMetricsPath : undefined;

    const concurrency = Math.max(1, Math.min(16, Math.floor(params.opts.concurrency ?? 1)));
    const variantResults = await mapWithConcurrency(
      variantPlanDirs,
      concurrency,
      async (variant) => {
        const refineInstructions = buildVariantRefineInstructions({
          variant: { id: variant.id, name: variant.name, rationale: variant.rationale },
          overrides: variant.overrides,
        });
        const res = await runExperimentPipeline({
          action: "pipeline",
          input: { kind: "planDir", planDir: variant.planDir },
          cfg: params.cfg,
          stages: {
            run: baselineStages.run,
            refine: {
              ...baselineRefineStage,
              enabled: true,
              instructions: refineInstructions,
            },
            bootstrap: baselineStages.bootstrap,
            execute: baselineStages.execute,
            finalize: baselineStages.finalize,
            accept: { baselinePath },
          },
        });
        return { variant, res };
      },
    );

    for (const entry of variantResults) {
      experiments.push({
        id: entry.variant.id,
        name: entry.variant.name,
        planDir: entry.res.planDir,
        pipeline: entry.res,
      });
      if (!entry.res.ok) {
        errors.push(`Variant ${entry.variant.id} failed.`);
      }
    }
  }

  const finishedAt = new Date().toISOString();
  const out: ExperimentSuiteRunResult = {
    ok: errors.length === 0,
    suiteId,
    suiteDir,
    startedAt,
    finishedAt,
    warnings,
    errors,
    design,
    experiments,
    paths,
  };
  await writeJsonFile(paths.suiteJson, out);
  await writeTextFile(
    paths.suiteSummary,
    renderSuiteSummary({ suiteId, suiteDir, warnings, errors, experiments }),
  );
  return out;
}
