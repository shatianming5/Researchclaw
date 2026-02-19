import fs from "node:fs/promises";
import path from "node:path";
import type { CommandOptions, SpawnResult } from "../process/exec.js";
import { retryAsync } from "../infra/retry.js";
import { runCommandWithTimeout } from "../process/exec.js";
import { validateDag } from "./dag.js";
import { discoverDataset } from "./discovery.js";
import { writeJsonFile, writeTextFile } from "./files.js";
import {
  CompileReportSchema,
  DiscoveryReportSchema,
  PlanDagSchema,
  type CompileReport,
  type DiscoveryReport,
  type PlanDag,
  type PlanNode,
} from "./schema.js";
import { resolveExecutionSecretsEnv, resolveKaggleCredentials } from "./secrets.js";
import { validatePlanDir } from "./validate.js";

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export type ProposalRunOpts = {
  dryRun?: boolean;
  failOnNeedsConfirm?: boolean;
  maxAttempts?: number;
  retryDelayMs?: number;
  commandTimeoutMs?: number;
  json?: boolean;
};

export type RunNodeStatus = "ok" | "failed" | "skipped" | "dry_run";

export type RunNodeResult = {
  nodeId: string;
  type: string;
  status: RunNodeStatus;
  attempts: number;
  durationMs: number;
  error?: string;
  stderrTail?: string;
  outputs?: string[];
};

export type ProposalRunResult = {
  ok: boolean;
  planDir: string;
  planId?: string;
  startedAt: string;
  finishedAt: string;
  warnings: string[];
  errors: string[];
  results: RunNodeResult[];
  skipped: Array<{ nodeId: string; type: string; reason: string }>;
  paths: {
    runLog: string;
    suggestions: string;
  };
};

export type ProposalRunDeps = {
  fetchFn?: FetchLike;
  runCommand?: (argv: string[], options: CommandOptions) => Promise<SpawnResult>;
};

const SAFE_NODE_TYPES = new Set<string>(["fetch_repo", "fetch_dataset_sample", "static_checks"]);

class NonRetryableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NonRetryableError";
  }
}

function sanitizeIdPart(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replaceAll(/[^a-z0-9._-]+/g, "-")
    .replaceAll(/-+/g, "-")
    .replaceAll(/^-|-$/g, "");
}

function ensureWithinBase(base: string, targetAbs: string, label: string) {
  const rel = path.relative(base, targetAbs);
  if (rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel))) {
    return;
  }
  throw new NonRetryableError(`${label} must be under ${base} (got ${targetAbs})`);
}

function parseSafeGitClone(cmd: string): { url: string; branch?: string; target: string } {
  const trimmed = cmd.trim();
  const re = /^git\s+clone\s+--depth\s+1(?:\s+--branch\s+([^\s]+))?\s+([^\s]+)\s+([^\s]+)\s*$/i;
  const m = trimmed.match(re);
  if (!m) {
    throw new NonRetryableError(`Unsupported fetch_repo command: "${trimmed}"`);
  }
  const branch = m[1]?.trim();
  const url = (m[2] ?? "").trim();
  const target = (m[3] ?? "").trim();
  if (!url || !target) {
    throw new NonRetryableError(`Invalid fetch_repo command: "${trimmed}"`);
  }
  if (!isAllowedGitUrl(url)) {
    throw new NonRetryableError(`Disallowed git URL: "${url}"`);
  }
  return { url, branch, target };
}

function parseSafeDatasetSampleCommand(cmd: string): {
  platform?: "hf" | "kaggle";
  dataset: string;
  out: string;
} {
  const trimmed = cmd.trim();
  const tokens = trimmed.split(/\s+/).filter(Boolean);
  if (tokens.length < 8) {
    throw new NonRetryableError(`Unsupported fetch_dataset_sample command: "${trimmed}"`);
  }
  if (tokens[0] !== "openclaw" || tokens[1] !== "proposal" || tokens[2] !== "dataset") {
    throw new NonRetryableError(`Unsupported fetch_dataset_sample command: "${trimmed}"`);
  }
  if (tokens[3] !== "sample") {
    throw new NonRetryableError(`Unsupported fetch_dataset_sample command: "${trimmed}"`);
  }

  let platform: "hf" | "kaggle" | undefined;
  let dataset = "";
  let out = "";

  for (let i = 4; i < tokens.length; i += 1) {
    const flag = tokens[i] ?? "";
    if (!flag.startsWith("--")) {
      throw new NonRetryableError(`Unsupported fetch_dataset_sample command: "${trimmed}"`);
    }
    const value = tokens[i + 1] ?? "";
    if (!value || value.startsWith("--")) {
      throw new NonRetryableError(`Unsupported fetch_dataset_sample command: "${trimmed}"`);
    }
    i += 1;

    if (flag === "--platform") {
      if (value !== "hf" && value !== "kaggle") {
        throw new NonRetryableError(`Unsupported dataset platform: "${value}"`);
      }
      platform = value;
      continue;
    }
    if (flag === "--dataset") {
      dataset = value;
      continue;
    }
    if (flag === "--out") {
      out = value;
      continue;
    }
    throw new NonRetryableError(`Unsupported fetch_dataset_sample flag: "${flag}"`);
  }

  if (!dataset || !out) {
    throw new NonRetryableError(`Invalid fetch_dataset_sample command: "${trimmed}"`);
  }
  if (!/^[\w.-]+(?:\/[\w.-]+)?$/.test(dataset)) {
    throw new NonRetryableError(`Invalid dataset id: "${dataset}"`);
  }
  return { platform, dataset, out };
}

function isAllowedGitUrl(url: string): boolean {
  const trimmed = url.trim();
  if (!trimmed) {
    return false;
  }
  if (
    trimmed.startsWith("http://") ||
    trimmed.startsWith("https://") ||
    trimmed.startsWith("ssh://") ||
    trimmed.startsWith("git://") ||
    trimmed.startsWith("git@")
  ) {
    return true;
  }
  return false;
}

async function pathExists(p: string): Promise<boolean> {
  try {
    await fs.stat(p);
    return true;
  } catch {
    return false;
  }
}

async function isGitRepo(dir: string): Promise<boolean> {
  try {
    await fs.stat(path.join(dir, ".git"));
    return true;
  } catch {
    return false;
  }
}

function tail(text: string, maxChars = 1200): string {
  const trimmed = text.trim();
  if (trimmed.length <= maxChars) {
    return trimmed;
  }
  return trimmed.slice(-maxChars);
}

function buildSuggestionMarkdown(params: {
  planId?: string;
  skipped: Array<{ nodeId: string; type: string; reason: string }>;
  warnings: string[];
}): string {
  const lines: string[] = [];
  lines.push("# Execution Suggestions");
  lines.push("");
  if (params.planId) {
    lines.push(`Plan: \`${params.planId}\``);
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
  lines.push("## Skipped nodes (manual / unsafe)");
  lines.push("");
  if (params.skipped.length === 0) {
    lines.push("No nodes were skipped.");
    lines.push("");
    return `${lines.join("\n")}\n`;
  }
  for (const entry of params.skipped) {
    lines.push(`- \`${entry.nodeId}\` (${entry.type}): ${entry.reason}`);
  }
  lines.push("");
  lines.push(
    "Tip: use `report/runbook.md` plus cached repos/data to proceed, or try `openclaw proposal execute <planDir>` for automated execution.",
  );
  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function loadJsonFile(filePath: string): Promise<unknown> {
  const raw = await fs.readFile(filePath, "utf-8");
  return JSON.parse(raw) as unknown;
}

async function loadPlanPackage(planDir: string): Promise<{
  dag: PlanDag;
  discovery: DiscoveryReport;
  report: CompileReport;
}> {
  const root = path.resolve(planDir);
  const dagRaw = await loadJsonFile(path.join(root, "plan", "plan.dag.json"));
  const discoveryRaw = await loadJsonFile(path.join(root, "ir", "discovery.json"));
  const reportRaw = await loadJsonFile(path.join(root, "report", "compile_report.json"));
  return {
    dag: PlanDagSchema.parse(dagRaw),
    discovery: DiscoveryReportSchema.parse(discoveryRaw),
    report: CompileReportSchema.parse(reportRaw),
  };
}

function resolveNodeById(dag: PlanDag, nodeId: string): PlanNode {
  const node = dag.nodes.find((n) => n.id === nodeId);
  if (!node) {
    throw new NonRetryableError(`Missing node in DAG: ${nodeId}`);
  }
  return node;
}

function ensureSafeRelativePath(params: { planDir: string; baseRel: string; relPath: string }) {
  const normalized = params.relPath.replaceAll("\\", "/");
  if (normalized.startsWith("/") || normalized.startsWith("../") || normalized.includes("/../")) {
    throw new NonRetryableError(`Unsafe path: ${params.relPath}`);
  }
  const baseAbs = path.resolve(params.planDir, params.baseRel);
  const targetAbs = path.resolve(params.planDir, normalized);
  ensureWithinBase(baseAbs, targetAbs, "output path");
  return { baseAbs, targetAbs, rel: normalized };
}

async function runFetchRepoNode(params: {
  planDir: string;
  node: PlanNode;
  opts: Required<Pick<ProposalRunOpts, "dryRun" | "commandTimeoutMs">>;
  retry: Required<Pick<ProposalRunOpts, "maxAttempts" | "retryDelayMs">>;
  deps: Required<Pick<ProposalRunDeps, "runCommand">>;
}): Promise<RunNodeResult> {
  const started = Date.now();
  const nodeId = params.node.id;
  const cmd = params.node.commands?.[0]?.trim() ?? "";
  if (!cmd) {
    throw new NonRetryableError(`fetch_repo node has no commands: ${nodeId}`);
  }

  const parsed = parseSafeGitClone(cmd);
  const targetRel = parsed.target.replaceAll("\\", "/");
  const safeTarget = ensureSafeRelativePath({
    planDir: params.planDir,
    baseRel: "cache/git",
    relPath: targetRel,
  });
  if (!safeTarget.rel.startsWith("cache/git/")) {
    throw new NonRetryableError(`fetch_repo target must be under cache/git (got ${targetRel})`);
  }

  if (await pathExists(safeTarget.targetAbs)) {
    const durationMs = Date.now() - started;
    const alreadyGit = await isGitRepo(safeTarget.targetAbs);
    return {
      nodeId,
      type: params.node.type,
      status: "skipped",
      attempts: 0,
      durationMs,
      outputs: params.node.outputs,
      error: alreadyGit ? undefined : "Target exists but is not a git repo; skipping.",
    };
  }

  if (params.opts.dryRun) {
    const durationMs = Date.now() - started;
    return {
      nodeId,
      type: params.node.type,
      status: "dry_run",
      attempts: 0,
      durationMs,
      outputs: params.node.outputs,
    };
  }

  const argv: string[] = ["git", "clone", "--depth", "1"];
  if (parsed.branch) {
    argv.push("--branch", parsed.branch);
  }
  argv.push(parsed.url, safeTarget.rel);

  let attemptsUsed = 0;
  let stderrTail: string | undefined;
  await retryAsync(
    async () => {
      attemptsUsed += 1;
      const res = await params.deps.runCommand(argv, {
        cwd: params.planDir,
        timeoutMs: params.opts.commandTimeoutMs,
      });
      stderrTail = tail(res.stderr);
      if (res.code !== 0) {
        throw new Error(`git clone failed (code=${res.code ?? "null"}): ${stderrTail}`);
      }
    },
    {
      attempts: params.retry.maxAttempts,
      minDelayMs: params.retry.retryDelayMs,
      maxDelayMs: Math.max(params.retry.retryDelayMs, params.retry.retryDelayMs * 16),
      jitter: 0,
      shouldRetry: (err) => !(err instanceof NonRetryableError),
    },
  );

  return {
    nodeId,
    type: params.node.type,
    status: "ok",
    attempts: attemptsUsed,
    durationMs: Date.now() - started,
    stderrTail,
    outputs: params.node.outputs,
  };
}

async function runStaticChecksNode(params: {
  planDir: string;
  node: PlanNode;
  opts: Required<Pick<ProposalRunOpts, "dryRun" | "commandTimeoutMs">>;
  deps: Required<Pick<ProposalRunDeps, "runCommand">>;
}): Promise<RunNodeResult> {
  const started = Date.now();
  const nodeId = params.node.id;

  const repoRel = params.node.inputs?.[0]?.trim() ?? "";
  if (!repoRel) {
    throw new NonRetryableError(
      `static_checks node must have inputs[0] repo path (node=${nodeId})`,
    );
  }
  const safeRepo = ensureSafeRelativePath({
    planDir: params.planDir,
    baseRel: "cache/git",
    relPath: repoRel,
  });
  if (!safeRepo.rel.startsWith("cache/git/")) {
    throw new NonRetryableError(`static_checks repo must be under cache/git (got ${repoRel})`);
  }

  const outRel = params.node.outputs?.[0]?.trim() ?? "";
  if (!outRel) {
    throw new NonRetryableError(
      `static_checks node must have outputs[0] under report/static_checks (node=${nodeId})`,
    );
  }
  const safeOut = ensureSafeRelativePath({
    planDir: params.planDir,
    baseRel: "report",
    relPath: outRel,
  });
  if (!safeOut.rel.startsWith("report/static_checks/")) {
    throw new NonRetryableError(
      `static_checks output must be under report/static_checks (got ${outRel})`,
    );
  }

  if (params.opts.dryRun) {
    return {
      nodeId,
      type: params.node.type,
      status: "dry_run",
      attempts: 0,
      durationMs: Date.now() - started,
      outputs: params.node.outputs,
    };
  }

  if (!(await pathExists(safeRepo.targetAbs))) {
    throw new NonRetryableError(`Repo directory missing: ${safeRepo.rel}`);
  }
  if (!(await isGitRepo(safeRepo.targetAbs))) {
    throw new NonRetryableError(`Repo is not a git repo: ${safeRepo.rel}`);
  }

  const warnings: string[] = [];
  let headCommit: string | undefined;
  let dirty: boolean | undefined;
  const headRes = await params.deps.runCommand(["git", "-C", safeRepo.rel, "rev-parse", "HEAD"], {
    cwd: params.planDir,
    timeoutMs: params.opts.commandTimeoutMs,
  });
  if (headRes.code !== 0) {
    warnings.push(`git rev-parse failed (code=${headRes.code ?? "null"})`);
  } else {
    headCommit = headRes.stdout.trim().split("\n")[0]?.trim() || undefined;
  }

  const statusRes = await params.deps.runCommand(
    ["git", "-C", safeRepo.rel, "status", "--porcelain=v1"],
    {
      cwd: params.planDir,
      timeoutMs: params.opts.commandTimeoutMs,
    },
  );
  if (statusRes.code !== 0) {
    warnings.push(`git status failed (code=${statusRes.code ?? "null"})`);
  } else {
    dirty = statusRes.stdout.trim().length > 0;
  }

  const packageJson = await pathExists(path.join(safeRepo.targetAbs, "package.json"));
  const pnpmLock = await pathExists(path.join(safeRepo.targetAbs, "pnpm-lock.yaml"));
  const pyprojectToml = await pathExists(path.join(safeRepo.targetAbs, "pyproject.toml"));
  const requirementsTxt = await pathExists(path.join(safeRepo.targetAbs, "requirements.txt"));
  const setupPy = await pathExists(path.join(safeRepo.targetAbs, "setup.py"));

  const report = {
    createdAt: new Date().toISOString(),
    nodeId,
    repo: {
      path: safeRepo.rel,
      headCommit,
      dirty,
    },
    detected: {
      node: packageJson,
      python: pyprojectToml || requirementsTxt || setupPy,
    },
    files: {
      packageJson,
      pnpmLock,
      pyprojectToml,
      requirementsTxt,
      setupPy,
    },
    warnings,
  };

  await writeJsonFile(safeOut.targetAbs, report);

  return {
    nodeId,
    type: params.node.type,
    status: "ok",
    attempts: 1,
    durationMs: Date.now() - started,
    outputs: params.node.outputs,
  };
}

function mapDatasetNodeToDiscovery(params: { node: PlanNode; discovery: DiscoveryReport }): {
  label: string;
  entry: DiscoveryReport["datasets"][number];
} {
  const output = params.node.outputs?.[0]?.replaceAll("\\", "/") ?? "";
  const prefix = "cache/data/";
  const label = output.startsWith(prefix) ? (output.slice(prefix.length).split("/")[0] ?? "") : "";
  const normalizedLabel = label.trim();
  if (!normalizedLabel) {
    throw new NonRetryableError(
      `fetch_dataset_sample node must output to ${prefix}<label> (node=${params.node.id})`,
    );
  }

  const candidates = params.discovery.datasets.filter((ds) => {
    const derived = sanitizeIdPart(ds.resolvedId ?? ds.input.name ?? ds.resolvedUrl ?? "dataset");
    return derived === normalizedLabel;
  });
  if (candidates.length === 1) {
    return { label: normalizedLabel, entry: candidates[0] };
  }
  if (params.discovery.datasets.length === 1) {
    return { label: normalizedLabel, entry: params.discovery.datasets[0] };
  }
  throw new NonRetryableError(
    `Cannot map dataset sample node ${params.node.id} to discovery entry (label=${normalizedLabel}).`,
  );
}

async function runFetchDatasetSampleNode(params: {
  planDir: string;
  node: PlanNode;
  discovery: DiscoveryReport;
  opts: Required<Pick<ProposalRunOpts, "dryRun" | "commandTimeoutMs">>;
  retry: Required<Pick<ProposalRunOpts, "maxAttempts" | "retryDelayMs">>;
  deps: Required<Pick<ProposalRunDeps, "fetchFn" | "runCommand">>;
}): Promise<RunNodeResult> {
  const started = Date.now();
  const nodeId = params.node.id;

  const mapped = mapDatasetNodeToDiscovery({ node: params.node, discovery: params.discovery });

  const safeOut = ensureSafeRelativePath({
    planDir: params.planDir,
    baseRel: "cache/data",
    relPath: `cache/data/${mapped.label}`,
  });

  if (params.opts.dryRun) {
    return {
      nodeId,
      type: params.node.type,
      status: "dry_run",
      attempts: 0,
      durationMs: Date.now() - started,
      outputs: params.node.outputs,
    };
  }

  const command = params.node.commands?.[0]?.trim() ?? "";
  if (command) {
    const parsed = parseSafeDatasetSampleCommand(command);
    const expectedDataset = (mapped.entry.resolvedId ?? mapped.entry.input.name ?? "").trim();
    if (expectedDataset && parsed.dataset !== expectedDataset) {
      throw new NonRetryableError(
        `fetch_dataset_sample dataset mismatch (expected ${expectedDataset}, got ${parsed.dataset}).`,
      );
    }

    const outRel = parsed.out.replaceAll("\\\\", "/");
    if (outRel !== safeOut.rel) {
      throw new NonRetryableError(
        `fetch_dataset_sample output mismatch (expected ${safeOut.rel}, got ${outRel}).`,
      );
    }

    const effectivePlatform = parsed.platform ?? mapped.entry.platform;
    if (
      parsed.platform &&
      mapped.entry.platform &&
      parsed.platform !== mapped.entry.platform &&
      mapped.entry.platform !== "unknown"
    ) {
      throw new NonRetryableError(
        `fetch_dataset_sample platform mismatch (expected ${mapped.entry.platform}, got ${parsed.platform}).`,
      );
    }

    if (effectivePlatform === "kaggle") {
      const creds = await resolveKaggleCredentials();
      if (!creds) {
        return {
          nodeId,
          type: params.node.type,
          status: "skipped",
          attempts: 0,
          durationMs: Date.now() - started,
          outputs: params.node.outputs,
          error:
            "Kaggle credentials missing (set KAGGLE_USERNAME/KAGGLE_KEY or `openclaw proposal secrets set`).",
        };
      }
    }

    const argv = ["openclaw", "proposal", "dataset", "sample"];
    if (effectivePlatform !== "hf") {
      argv.push("--platform", effectivePlatform);
    }
    argv.push("--dataset", parsed.dataset, "--out", safeOut.rel);
    let attemptsUsed = 0;
    let stderrTail: string | undefined;
    await retryAsync(
      async () => {
        attemptsUsed += 1;
        const res = await params.deps.runCommand(argv, {
          cwd: params.planDir,
          timeoutMs: params.opts.commandTimeoutMs,
        });
        stderrTail = tail(res.stderr);
        if (res.code !== 0) {
          throw new Error(
            `dataset sample command failed (code=${res.code ?? "null"}): ${stderrTail}`,
          );
        }
      },
      {
        attempts: params.retry.maxAttempts,
        minDelayMs: params.retry.retryDelayMs,
        maxDelayMs: Math.max(params.retry.retryDelayMs, params.retry.retryDelayMs * 16),
        jitter: 0,
        shouldRetry: (err) => !(err instanceof NonRetryableError),
      },
    );

    return {
      nodeId,
      type: params.node.type,
      status: "ok",
      attempts: attemptsUsed,
      durationMs: Date.now() - started,
      stderrTail,
      outputs: params.node.outputs,
    };
  }

  if (mapped.entry.platform === "kaggle") {
    const creds = await resolveKaggleCredentials();
    if (!creds) {
      return {
        nodeId,
        type: params.node.type,
        status: "skipped",
        attempts: 0,
        durationMs: Date.now() - started,
        outputs: params.node.outputs,
        error:
          "Kaggle credentials missing (set KAGGLE_USERNAME/KAGGLE_KEY or `openclaw proposal secrets set`).",
      };
    }
  }

  if (mapped.entry.platform !== "hf" && mapped.entry.platform !== "kaggle") {
    return {
      nodeId,
      type: params.node.type,
      status: "skipped",
      attempts: 0,
      durationMs: Date.now() - started,
      outputs: params.node.outputs,
      error: `Dataset platform "${mapped.entry.platform}" requires manual handling.`,
    };
  }

  let attemptsUsed = 0;
  let discovered: unknown;
  await retryAsync(
    async () => {
      attemptsUsed += 1;
      const res = await discoverDataset({
        input: {
          ...mapped.entry.input,
          platform: mapped.entry.platform,
          name: mapped.entry.resolvedId ?? mapped.entry.input.name,
          url: mapped.entry.resolvedUrl ?? mapped.entry.input.url,
        },
        mode: "sample",
        fetchFn: params.deps.fetchFn,
      });
      if (res.exists === false) {
        throw new Error(`Dataset sample fetch failed: ${res.warnings.join("; ")}`);
      }
      discovered = res;
    },
    {
      attempts: params.retry.maxAttempts,
      minDelayMs: params.retry.retryDelayMs,
      maxDelayMs: Math.max(params.retry.retryDelayMs, params.retry.retryDelayMs * 16),
      jitter: 0,
      shouldRetry: (err) => !(err instanceof NonRetryableError),
    },
  );

  await fs.mkdir(safeOut.targetAbs, { recursive: true });
  await writeJsonFile(path.join(safeOut.targetAbs, "discovered.json"), discovered);
  const sample = (discovered as { sample?: unknown } | undefined)?.sample;
  if (sample !== undefined) {
    await writeJsonFile(path.join(safeOut.targetAbs, "sample.json"), sample);
  }

  return {
    nodeId,
    type: params.node.type,
    status: "ok",
    attempts: attemptsUsed,
    durationMs: Date.now() - started,
    outputs: params.node.outputs,
  };
}

export async function runProposalPlanSafeNodes(params: {
  planDir: string;
  opts?: ProposalRunOpts;
  deps?: ProposalRunDeps;
}): Promise<ProposalRunResult> {
  const planDir = path.resolve(params.planDir);
  const startedAt = new Date().toISOString();

  const warnings: string[] = [];
  const errors: string[] = [];
  const results: RunNodeResult[] = [];
  const skipped: Array<{ nodeId: string; type: string; reason: string }> = [];

  const opts: Required<Pick<ProposalRunOpts, "dryRun" | "failOnNeedsConfirm">> &
    Required<Pick<ProposalRunOpts, "maxAttempts" | "retryDelayMs" | "commandTimeoutMs">> = {
    dryRun: Boolean(params.opts?.dryRun),
    failOnNeedsConfirm: Boolean(params.opts?.failOnNeedsConfirm),
    maxAttempts: Math.max(1, Math.floor(params.opts?.maxAttempts ?? 3)),
    retryDelayMs: Math.max(0, Math.floor(params.opts?.retryDelayMs ?? 1500)),
    commandTimeoutMs: Math.max(5_000, Math.floor(params.opts?.commandTimeoutMs ?? 120_000)),
  };

  const deps: Required<Pick<ProposalRunDeps, "fetchFn" | "runCommand">> = {
    fetchFn: params.deps?.fetchFn ?? fetch,
    runCommand: params.deps?.runCommand ?? runCommandWithTimeout,
  };

  const secretsEnv = await (async () => {
    try {
      return await resolveExecutionSecretsEnv();
    } catch (err) {
      warnings.push(`Failed to load secrets env: ${String(err)}`);
      return {};
    }
  })();

  const runCommandWithSecrets = async (
    argv: string[],
    options: CommandOptions,
  ): Promise<SpawnResult> => {
    if (Object.keys(secretsEnv).length === 0) {
      return await deps.runCommand(argv, options);
    }
    const envMerged: NodeJS.ProcessEnv = {};
    if (options.env) {
      Object.assign(envMerged, options.env);
    }
    for (const [key, value] of Object.entries(secretsEnv)) {
      if (!(key in envMerged)) {
        envMerged[key] = value;
      }
    }
    return await deps.runCommand(argv, { ...options, env: envMerged });
  };

  const validation = await validatePlanDir(planDir);
  if (!validation.ok || !validation.data) {
    errors.push(...validation.errors);
    const finishedAt = new Date().toISOString();
    const out = finalizeRunResult({
      planDir,
      planId: undefined,
      startedAt,
      finishedAt,
      warnings,
      errors,
      results,
      skipped,
    });
    await writeRunArtifacts(out);
    return out;
  }

  const pkg = await loadPlanPackage(planDir);
  const planId = pkg.report.planId;

  if (validation.warnings.length > 0) {
    warnings.push(...validation.warnings);
  }

  const needsConfirmCount = pkg.report.needsConfirm.length + validation.data.needsConfirmCount;
  if (needsConfirmCount > 0) {
    const msg = `Plan has ${needsConfirmCount} needs_confirm item(s).`;
    if (opts.failOnNeedsConfirm) {
      errors.push(msg);
    } else {
      warnings.push(msg);
    }
  }

  if (errors.length > 0) {
    const finishedAt = new Date().toISOString();
    const out = finalizeRunResult({
      planDir,
      planId,
      startedAt,
      finishedAt,
      warnings,
      errors,
      results,
      skipped,
    });
    await writeRunArtifacts(out);
    return out;
  }

  const topo = validateDag(pkg.dag);
  if (!topo.ok) {
    errors.push(...topo.errors);
    const finishedAt = new Date().toISOString();
    const out = finalizeRunResult({
      planDir,
      planId,
      startedAt,
      finishedAt,
      warnings,
      errors,
      results,
      skipped,
    });
    await writeRunArtifacts(out);
    return out;
  }

  for (const nodeId of topo.order) {
    const node = resolveNodeById(pkg.dag, nodeId);
    if (!SAFE_NODE_TYPES.has(node.type)) {
      skipped.push({
        nodeId,
        type: node.type,
        reason: node.type === "manual_review" ? "Manual review required" : "Not a safe node type",
      });
      results.push({
        nodeId,
        type: node.type,
        status: "skipped",
        attempts: 0,
        durationMs: 0,
        outputs: node.outputs,
      });
      continue;
    }

    try {
      if (node.type === "fetch_repo") {
        const res = await runFetchRepoNode({
          planDir,
          node,
          opts,
          retry: { maxAttempts: opts.maxAttempts, retryDelayMs: opts.retryDelayMs },
          deps: { runCommand: runCommandWithSecrets },
        });
        results.push(res);
        if (res.status === "skipped" && res.error) {
          skipped.push({ nodeId, type: node.type, reason: res.error });
        }
      } else if (node.type === "fetch_dataset_sample") {
        const res = await runFetchDatasetSampleNode({
          planDir,
          node,
          discovery: pkg.discovery,
          opts,
          retry: { maxAttempts: opts.maxAttempts, retryDelayMs: opts.retryDelayMs },
          deps: { fetchFn: deps.fetchFn, runCommand: runCommandWithSecrets },
        });
        results.push(res);
        if (res.status === "skipped" && res.error) {
          skipped.push({ nodeId, type: node.type, reason: res.error });
        }
      } else if (node.type === "static_checks") {
        const res = await runStaticChecksNode({
          planDir,
          node,
          opts,
          deps: { runCommand: runCommandWithSecrets },
        });
        results.push(res);
      }
    } catch (err) {
      const msg = String(err instanceof Error ? err.message : err);
      results.push({
        nodeId,
        type: node.type,
        status: "failed",
        attempts: 1,
        durationMs: 0,
        error: msg,
        outputs: node.outputs,
      });
      errors.push(`${nodeId}: ${msg}`);
      break;
    }
  }

  const finishedAt = new Date().toISOString();
  const out = finalizeRunResult({
    planDir,
    planId,
    startedAt,
    finishedAt,
    warnings,
    errors,
    results,
    skipped,
  });
  await writeRunArtifacts(out);
  return out;
}

function finalizeRunResult(params: {
  planDir: string;
  planId?: string;
  startedAt: string;
  finishedAt: string;
  warnings: string[];
  errors: string[];
  results: RunNodeResult[];
  skipped: Array<{ nodeId: string; type: string; reason: string }>;
}): ProposalRunResult {
  const runLog = path.join(params.planDir, "report", "run_log.json");
  const suggestions = path.join(params.planDir, "report", "execution_suggestions.md");
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
    paths: { runLog, suggestions },
  };
}

async function writeRunArtifacts(result: ProposalRunResult): Promise<void> {
  await writeJsonFile(result.paths.runLog, {
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
  await writeTextFile(
    result.paths.suggestions,
    buildSuggestionMarkdown({
      planId: result.planId,
      skipped: result.skipped,
      warnings: result.warnings,
    }),
  );
}
