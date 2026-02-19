import fs from "node:fs/promises";
import path from "node:path";
import type { CommandOptions, SpawnResult } from "../process/exec.js";
import type { PlanDag, PlanNode } from "./schema.js";
import { applyPatch, type ApplyPatchSummary } from "../agents/apply-patch.js";
import { runCommandWithTimeout } from "../process/exec.js";
import { writeJsonFile, writeTextFile } from "./files.js";
import { runOpencodeJson } from "./opencode/run.js";
import { validatePlanDir } from "./validate.js";

type RunCommandLike = (argv: string[], options: CommandOptions) => Promise<SpawnResult>;

export type ProposalBootstrapMode = "worktree" | "plan" | "both";

export type ProposalBootstrapOpts = {
  mode?: ProposalBootstrapMode;
  model?: string;
  agent?: string;
  timeoutMs?: number;
  dryRun?: boolean;
  instructions?: string;
  maxAttempts?: number;
};

export type ProposalBootstrapDeps = {
  runCommand?: RunCommandLike;
  opencodeConfigDir?: string;
};

export type ProposalBootstrapResult = {
  ok: boolean;
  planDir: string;
  planId?: string;
  startedAt: string;
  finishedAt: string;
  warnings: string[];
  errors: string[];
  notes: string[];
  planPatch?: {
    applied: boolean;
    summary?: ApplyPatchSummary;
  };
  worktreePatches: Array<{
    repoKey: string;
    patchPath: string;
    sessionId?: string;
  }>;
  paths: {
    bootstrapInput: string;
    bootstrapOutput: string;
    bootstrapReport: string;
    bootstrapSummary: string;
    worktreePatchesDir: string;
  };
};

function normalizeRel(p: string): string {
  return p.trim().replaceAll("\\", "/");
}

function extractRepoKeys(dag: PlanDag): string[] {
  const keys = new Set<string>();
  for (const node of dag.nodes) {
    if (node.type !== "fetch_repo") {
      continue;
    }
    for (const out of node.outputs ?? []) {
      const rel = normalizeRel(out);
      if (!rel.startsWith("cache/git/")) {
        continue;
      }
      const repoKey = rel.slice("cache/git/".length).replace(/^\/+/, "").trim();
      if (repoKey) {
        keys.add(repoKey);
      }
    }
  }
  return [...keys].toSorted((a, b) => a.localeCompare(b));
}

function findNodeById(dag: PlanDag, nodeId: string): PlanNode | null {
  return dag.nodes.find((node) => node.id === nodeId) ?? null;
}

function extractPatchBlock(text: string): string | null {
  const start = text.indexOf("*** Begin Patch");
  if (start === -1) {
    return null;
  }
  const end = text.indexOf("*** End Patch", start);
  if (end === -1) {
    return null;
  }
  const block = text.slice(start, end + "*** End Patch".length);
  return `${block.trim()}\n`;
}

function listPatchPaths(patchText: string): string[] {
  const out: string[] = [];
  const patterns = ["*** Add File: ", "*** Update File: ", "*** Delete File: ", "*** Move to: "];
  for (const line of patchText.split(/\r?\n/)) {
    for (const prefix of patterns) {
      if (!line.startsWith(prefix)) {
        continue;
      }
      const rel = line.slice(prefix.length).trim();
      if (rel) {
        out.push(rel);
      }
    }
  }
  return out;
}

function isSafeRelPath(rel: string): boolean {
  const p = normalizeRel(rel);
  if (!p) {
    return false;
  }
  if (p.startsWith("/") || p.startsWith("../") || p.includes("/../")) {
    return false;
  }
  return true;
}

function isAllowedPlanPatchPath(rel: string): boolean {
  const p = normalizeRel(rel);
  if (!isSafeRelPath(p)) {
    return false;
  }
  if (p === ".git" || p.startsWith(".git/")) {
    return false;
  }
  return (
    p.startsWith("plan/") ||
    p.startsWith("report/") ||
    p.startsWith("input/") ||
    p.startsWith("ir/")
  );
}

function isAllowedRepoPatchPath(rel: string): boolean {
  const p = normalizeRel(rel);
  if (!isSafeRelPath(p)) {
    return false;
  }
  if (p === ".git" || p.startsWith(".git/")) {
    return false;
  }
  return true;
}

function clip(text: string, maxChars: number): string {
  const max = Math.max(0, Math.floor(maxChars));
  if (max === 0) {
    return "";
  }
  if (text.length <= max) {
    return text;
  }
  return `${text.slice(0, max)}\n...<clipped>`;
}

function renderPlanPatchPrompt(params: {
  planDir: string;
  planId?: string;
  repoKeys: string[];
  dag: PlanDag;
  instructions?: string;
}): string {
  const train = findNodeById(params.dag, "train.run");
  const evalNode = findNodeById(params.dag, "eval.run");
  const report = findNodeById(params.dag, "report.write");

  return (
    `You are an automated experiment bootstrap agent for OpenClaw.\n` +
    `Your goal: make the plan package runnable and reproducible BEFORE execution.\n\n` +
    `Output rules:\n` +
    `- Output ONLY either the single token NO_CHANGES or a single apply_patch patch.\n` +
    `- If outputting a patch, it must include "*** Begin Patch" and "*** End Patch".\n` +
    `- Patch may ONLY touch files under plan/, report/, input/, or ir/.\n` +
    `- Do NOT touch cache/.\n` +
    `- Do NOT include markdown fences or commentary.\n\n` +
    `Context:\n` +
    `- planDir: ${params.planDir}\n` +
    (params.planId ? `- planId: ${params.planId}\n` : "") +
    `- repoKeys: ${params.repoKeys.join(", ") || "(none)"}\n\n` +
    `Relevant nodes (for reference):\n` +
    `- train.run commands:\n${clip((train?.commands ?? []).join("\n"), 4000)}\n\n` +
    `- eval.run commands:\n${clip((evalNode?.commands ?? []).join("\n"), 4000)}\n\n` +
    `- report.write commands:\n${clip((report?.commands ?? []).join("\n"), 4000)}\n\n` +
    (params.instructions?.trim() ? `Extra instructions:\n${params.instructions.trim()}\n\n` : "") +
    `If you decide changes are needed, prefer adding plan-local wrapper scripts under plan/scripts/ and adjusting plan/plan.dag.json to call them.\n`
  );
}

function renderRepoPatchPrompt(params: {
  planDir: string;
  planId?: string;
  repoKey: string;
  dag: PlanDag;
  instructions?: string;
}): string {
  const train = findNodeById(params.dag, "train.run");
  const evalNode = findNodeById(params.dag, "eval.run");

  return (
    `You are an automated code patch agent for OpenClaw experiments.\n` +
    `Your goal: ensure the repo works with OpenClaw's execution harness.\n\n` +
    `Execution harness notes:\n` +
    `- Commands run with cwd set to the repo workdir.\n` +
    `- The environment variable OPENCLAW_PLAN_DIR points to the plan root.\n` +
    `- Training must write checkpoints under $OPENCLAW_CHECKPOINT_DIR (same as $OPENCLAW_OUTPUT_DIR).\n` +
    `- Training must be resumable: if a checkpoint exists in $OPENCLAW_CHECKPOINT_DIR, resume rather than starting over.\n` +
    `- Evaluation must write metrics to $OPENCLAW_PLAN_DIR/report/eval_metrics.json (or ensure the plan's eval command does).\n\n` +
    `Output rules:\n` +
    `- Output ONLY either the single token NO_CHANGES or a single apply_patch patch.\n` +
    `- Patch paths must be repo-root relative.\n` +
    `- Do NOT touch .git/.\n` +
    `- Do NOT include markdown fences or commentary.\n\n` +
    `Context:\n` +
    `- planDir: ${params.planDir}\n` +
    (params.planId ? `- planId: ${params.planId}\n` : "") +
    `- repoKey: ${params.repoKey}\n\n` +
    `Relevant plan nodes (for reference):\n` +
    `- train.run commands:\n${clip((train?.commands ?? []).join("\n"), 4000)}\n\n` +
    `- eval.run commands:\n${clip((evalNode?.commands ?? []).join("\n"), 4000)}\n\n` +
    (params.instructions?.trim() ? `Extra instructions:\n${params.instructions.trim()}\n\n` : "") +
    `If changes are needed, keep the patch minimal and focused (instrument metrics output, fix obvious broken paths/imports).\n`
  );
}

function renderBootstrapSummary(result: ProposalBootstrapResult): string {
  const lines: string[] = [];
  lines.push("# Bootstrap Summary");
  lines.push("");
  if (result.planId) {
    lines.push(`- planId: \`${result.planId}\``);
  }
  lines.push(`- planDir: \`${result.planDir}\``);
  lines.push("");

  if (result.errors.length > 0) {
    lines.push("## Errors");
    lines.push("");
    for (const e of result.errors) {
      lines.push(`- ${e}`);
    }
    lines.push("");
  }

  if (result.warnings.length > 0) {
    lines.push("## Warnings");
    lines.push("");
    for (const w of result.warnings) {
      lines.push(`- ${w}`);
    }
    lines.push("");
  }

  if (result.planPatch) {
    lines.push("## Plan Patch");
    lines.push("");
    lines.push(`- applied: ${String(result.planPatch.applied)}`);
    if (result.planPatch.summary) {
      const summary = result.planPatch.summary;
      for (const rel of summary.added) {
        lines.push(`- A ${rel}`);
      }
      for (const rel of summary.modified) {
        lines.push(`- M ${rel}`);
      }
      for (const rel of summary.deleted) {
        lines.push(`- D ${rel}`);
      }
    }
    lines.push("");
  }

  if (result.worktreePatches.length > 0) {
    lines.push("## Worktree Patches");
    lines.push("");
    for (const patch of result.worktreePatches) {
      lines.push(`- ${patch.repoKey}: \`${patch.patchPath}\``);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

export async function bootstrapProposalPlan(params: {
  planDir: string;
  opts?: ProposalBootstrapOpts;
  deps?: ProposalBootstrapDeps;
}): Promise<ProposalBootstrapResult> {
  const startedAt = new Date().toISOString();
  const planDir = path.resolve(params.planDir);
  const warnings: string[] = [];
  const errors: string[] = [];
  const notes: string[] = [];

  const bootstrapDir = path.join(planDir, "report", "bootstrap");
  const worktreePatchesDir = path.join(bootstrapDir, "worktree_patches");

  const paths = {
    bootstrapInput: path.join(bootstrapDir, "bootstrap_input.json"),
    bootstrapOutput: path.join(bootstrapDir, "bootstrap_output.json"),
    bootstrapReport: path.join(bootstrapDir, "bootstrap_report.json"),
    bootstrapSummary: path.join(bootstrapDir, "bootstrap_summary.md"),
    worktreePatchesDir,
  };

  await fs.mkdir(worktreePatchesDir, { recursive: true });

  const validation = await validatePlanDir(planDir);
  const planId = validation.ok ? validation.data?.report.planId : undefined;
  if (!validation.ok || !validation.data) {
    errors.push(...validation.errors);
    warnings.push(...validation.warnings);
    const finishedAt = new Date().toISOString();
    const out: ProposalBootstrapResult = {
      ok: false,
      planDir,
      planId,
      startedAt,
      finishedAt,
      warnings,
      errors,
      notes,
      worktreePatches: [],
      paths,
    };
    await writeJsonFile(paths.bootstrapReport, out);
    await writeTextFile(paths.bootstrapSummary, renderBootstrapSummary(out));
    return out;
  }

  warnings.push(...validation.warnings);
  const dag = validation.data.dag;
  const repoKeys = extractRepoKeys(dag);

  const mode: ProposalBootstrapMode = params.opts?.mode ?? "worktree";
  const dryRun = params.opts?.dryRun === true;
  const model = params.opts?.model?.trim() || "opencode/kimi-k2.5-free";
  const agent = params.opts?.agent?.trim() || "openclaw-bootstrap";
  const timeoutMs = Math.max(10_000, Math.floor(params.opts?.timeoutMs ?? 180_000));
  const maxAttempts = Math.max(1, Math.floor(params.opts?.maxAttempts ?? 2));

  await writeJsonFile(paths.bootstrapInput, {
    schemaVersion: 1,
    createdAt: new Date().toISOString(),
    planDir,
    planId,
    mode,
    model,
    agent,
    repoKeys,
  });

  const runCommand = params.deps?.runCommand ?? runCommandWithTimeout;
  const worktreePatches: ProposalBootstrapResult["worktreePatches"] = [];

  let planPatchApplied = false;
  let planPatchSummary: ApplyPatchSummary | undefined;

  if ((mode === "plan" || mode === "both") && !dryRun) {
    const prompt = renderPlanPatchPrompt({
      planDir,
      planId,
      repoKeys,
      dag,
      instructions: params.opts?.instructions,
    });

    let lastErr = "";
    let lastRaw = "";
    let sessionId: string | undefined;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      const message =
        attempt === 1
          ? prompt
          : `${prompt}\n\nYour previous output was invalid.\nError: ${lastErr}\n\nPrevious output:\n${clip(lastRaw, 8000)}\n\nFix it.`;

      const res = await runOpencodeJson({
        message,
        model,
        agent,
        agentKind: "text",
        cwd: planDir,
        timeoutMs,
        opencodeConfigDir: params.deps?.opencodeConfigDir,
        deps: params.deps?.runCommand ? { runCommand } : undefined,
      });
      warnings.push(...res.warnings);
      sessionId = res.sessionId;
      lastRaw = res.text;
      if (!res.ok) {
        lastErr = res.error ?? "opencode failed";
        continue;
      }

      const raw = res.text.trim();
      if (raw === "NO_CHANGES") {
        notes.push("bootstrap: planPatch NO_CHANGES");
        break;
      }

      const patchText = extractPatchBlock(res.text);
      if (!patchText) {
        lastErr = "missing apply_patch block";
        continue;
      }

      const patchedPaths = listPatchPaths(patchText);
      const illegal = patchedPaths.filter((p) => !isAllowedPlanPatchPath(p));
      if (illegal.length > 0) {
        lastErr = `plan patch touched disallowed path(s): ${illegal.join(", ")}`;
        continue;
      }

      const applied = await applyPatch(patchText, { cwd: planDir, sandboxRoot: planDir });
      planPatchApplied = true;
      planPatchSummary = applied.summary;

      const post = await validatePlanDir(planDir);
      warnings.push(...post.warnings);
      if (!post.ok) {
        errors.push(...post.errors);
        errors.push("bootstrap: plan patch produced an invalid planDir");
      }
      await writeJsonFile(path.join(bootstrapDir, "plan_patch.json"), {
        ok: post.ok,
        sessionId,
        summary: applied.summary,
      });
      break;
    }
  }

  if (mode === "worktree" || mode === "both") {
    for (const repoKey of repoKeys) {
      if (dryRun) {
        notes.push(`bootstrap: dryRun skipping repo patch for ${repoKey}`);
        continue;
      }

      const repoRoot = path.join(planDir, "cache", "git", repoKey);
      const prompt = renderRepoPatchPrompt({
        planDir,
        planId,
        repoKey,
        dag,
        instructions: params.opts?.instructions,
      });

      let lastErr = "";
      let lastRaw = "";
      let sessionId: string | undefined;
      let patchPath: string | null = null;

      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        const message =
          attempt === 1
            ? prompt
            : `${prompt}\n\nYour previous output was invalid.\nError: ${lastErr}\n\nPrevious output:\n${clip(lastRaw, 8000)}\n\nFix it.`;

        const res = await runOpencodeJson({
          message,
          model,
          agent,
          agentKind: "text",
          cwd: repoRoot,
          timeoutMs,
          opencodeConfigDir: params.deps?.opencodeConfigDir,
          deps: params.deps?.runCommand ? { runCommand } : undefined,
        });
        warnings.push(...res.warnings);
        sessionId = res.sessionId;
        lastRaw = res.text;
        if (!res.ok) {
          lastErr = res.error ?? "opencode failed";
          continue;
        }

        const raw = res.text.trim();
        if (raw === "NO_CHANGES") {
          notes.push(`bootstrap: repo ${repoKey} NO_CHANGES`);
          break;
        }

        const patchText = extractPatchBlock(res.text);
        if (!patchText) {
          lastErr = "missing apply_patch block";
          continue;
        }

        const patchedPaths = listPatchPaths(patchText);
        const illegal = patchedPaths.filter((p) => !isAllowedRepoPatchPath(p));
        if (illegal.length > 0) {
          lastErr = `repo patch touched disallowed path(s): ${illegal.join(", ")}`;
          continue;
        }

        patchPath = path.join(worktreePatchesDir, `${repoKey}.patch`);
        await fs.writeFile(patchPath, patchText, "utf-8");
        await writeJsonFile(path.join(worktreePatchesDir, `${repoKey}.json`), {
          schemaVersion: 1,
          repoKey,
          createdAt: new Date().toISOString(),
          sessionId,
          patchPath: path.relative(planDir, patchPath).replaceAll("\\", "/"),
          paths: patchedPaths,
        });
        break;
      }

      if (patchPath) {
        worktreePatches.push({ repoKey, patchPath, sessionId });
      }
    }
  }

  const finishedAt = new Date().toISOString();
  const out: ProposalBootstrapResult = {
    ok: errors.length === 0,
    planDir,
    planId,
    startedAt,
    finishedAt,
    warnings,
    errors,
    notes,
    planPatch:
      mode === "plan" || mode === "both"
        ? { applied: planPatchApplied, ...(planPatchSummary ? { summary: planPatchSummary } : {}) }
        : undefined,
    worktreePatches,
    paths,
  };

  await writeJsonFile(paths.bootstrapOutput, {
    ok: out.ok,
    planDir: out.planDir,
    planId: out.planId,
    startedAt: out.startedAt,
    finishedAt: out.finishedAt,
    warnings: out.warnings,
    errors: out.errors,
    notes: out.notes,
    planPatch: out.planPatch,
    worktreePatches: out.worktreePatches.map((p) => ({
      repoKey: p.repoKey,
      patchPath: path.relative(planDir, p.patchPath).replaceAll("\\", "/"),
      sessionId: p.sessionId,
    })),
  });

  await writeJsonFile(paths.bootstrapReport, out);
  await writeTextFile(paths.bootstrapSummary, renderBootstrapSummary(out));
  return out;
}
