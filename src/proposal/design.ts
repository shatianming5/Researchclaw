import fs from "node:fs/promises";
import path from "node:path";
import type { CommandOptions, SpawnResult } from "../process/exec.js";
import { runCommandWithTimeout } from "../process/exec.js";
import { ExperimentSuiteDesignSchema, type ExperimentSuiteDesign } from "./design.schema.js";
import { writeJsonFile, writeTextFile } from "./files.js";
import { completeJsonWithSchemaViaOpencode } from "./opencode/run.js";
import { validatePlanDir } from "./validate.js";

type RunCommandLike = (argv: string[], options: CommandOptions) => Promise<SpawnResult>;

export type ExperimentSuiteDesignOpts = {
  suiteId: string;
  variantCount: number;
  model?: string;
  agent?: string;
  timeoutMs?: number;
  dryRun?: boolean;
};

export type ExperimentSuiteDesignDeps = {
  runCommand?: RunCommandLike;
  opencodeConfigDir?: string;
};

export type ExperimentSuiteDesignResult = {
  ok: boolean;
  suiteId: string;
  planDir: string;
  planId?: string;
  startedAt: string;
  finishedAt: string;
  warnings: string[];
  errors: string[];
  design?: ExperimentSuiteDesign;
  opencodeSessionId?: string;
  paths: {
    designInput: string;
    designOutput: string;
    designReport: string;
    designSummary: string;
  };
};

function renderDesignSummary(params: {
  suiteId: string;
  planId?: string;
  variantCount: number;
  warnings: string[];
  errors: string[];
  design?: ExperimentSuiteDesign;
}): string {
  const lines: string[] = [];
  lines.push("# Experiment Suite Design Summary");
  lines.push("");
  lines.push(`Suite: \`${params.suiteId}\``);
  if (params.planId) {
    lines.push(`Plan: \`${params.planId}\``);
  }
  lines.push(`Variant count: \`${params.variantCount}\``);
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

  if (params.design) {
    lines.push("## Experiments");
    lines.push("");
    lines.push(`- baseline: ${params.design.baseline.name}`);
    if (params.design.schemaVersion === 3) {
      for (const axis of params.design.axes) {
        lines.push(`- axis ${axis.id}: ${axis.name} (${axis.levels.length} levels)`);
        for (const level of axis.levels) {
          lines.push(`  - ${level.id}: ${level.name}`);
        }
      }
    } else {
      for (const variant of params.design.variants) {
        lines.push(`- ${variant.id}: ${variant.name}`);
      }
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function readTextIfExists(filePath: string): Promise<string | null> {
  try {
    return await fs.readFile(filePath, "utf-8");
  } catch {
    return null;
  }
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.stat(filePath);
    return true;
  } catch {
    return false;
  }
}

function sanitizeVariantCount(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.min(32, Math.floor(value)));
}

function buildDesignPrompt(params: {
  suiteId: string;
  variantCount: number;
  planId: string;
  planDir: string;
  proposalExcerpt: string;
  dag: unknown;
  acceptance: unknown;
  repoProfilesIndexRel?: string;
}): string {
  const basePayload = {
    suiteId: params.suiteId,
    variantCount: params.variantCount,
    planId: params.planId,
    planDir: params.planDir,
    proposalExcerpt: params.proposalExcerpt,
    repoProfilesFile: params.repoProfilesIndexRel,
    dag: params.dag,
    acceptance: params.acceptance,
    outputSchema: {
      schemaVersion: 3,
      suiteId: params.suiteId,
      selectedRepoKey: "<optional repoKey>",
      baseline: {
        id: "baseline",
        name: "Baseline",
        rationale: "As-is plan",
        overrides: {
          env: { OPENCLAW_TRAIN_EXTRA_ARGS: "", OPENCLAW_EVAL_EXTRA_ARGS: "" },
          resources: { gpuCount: 1, estimatedMinutes: 30 },
        },
        dagPatchOps: [],
      },
      axes: [
        {
          id: "lr",
          name: "Learning rate",
          kind: "grid",
          levels: [
            {
              id: "lr-3e-5",
              name: "lr=3e-5",
              rationale: "Common stable default",
              overrides: {
                env: { OPENCLAW_TRAIN_EXTRA_ARGS: "--learning_rate 3e-5" },
                resources: { gpuCount: 1, estimatedMinutes: 30 },
              },
              dagPatchOps: [],
            },
            {
              id: "lr-1e-4",
              name: "lr=1e-4",
              rationale: "Faster learning, may be unstable",
              overrides: {
                env: { OPENCLAW_TRAIN_EXTRA_ARGS: "--learning_rate 1e-4" },
                resources: { gpuCount: 1, estimatedMinutes: 30 },
              },
            },
          ],
        },
      ],
      notes: ["..."],
      warnings: ["..."],
    },
  };

  return (
    `You are designing an experiment suite for OpenClaw.\n` +
    `Return ONLY valid JSON matching this TypeScript-like schema:\n` +
    `{\n` +
    `  "schemaVersion": 3,\n` +
    `  "suiteId": string,\n` +
    `  "selectedRepoKey"?: string,\n` +
    `  "baseline": { "id": "baseline", "name": string, "rationale"?: string, "overrides": { "env"?: Record<string,string>, "resources"?: unknown, "acceptance"?: unknown }, "dagPatchOps"?: Array<unknown> },\n` +
    `  "axes": Array<{ "id": string, "name": string, "kind": "grid", "levels": Array<{ "id": string, "name": string, "rationale"?: string, "overrides": { "env"?: Record<string,string>, "resources"?: unknown, "acceptance"?: unknown }, "dagPatchOps"?: Array<unknown> }> }>,\n` +
    `  "needsConfirm"?: Array<{ "id": string, "message": string, "severity"?: "warn"|"error" }>,\n` +
    `  "notes"?: string[],\n` +
    `  "warnings"?: string[]\n` +
    `}\n\n` +
    `Rules:\n` +
    `- Set suiteId to exactly "${params.suiteId}".\n` +
    `- baseline.id must be "baseline".\n` +
    `- Design axes/levels to produce an informative grid; OpenClaw will expand the cartesian product and then truncate to at most ${params.variantCount} variants.\n` +
    `- Axis ids and level ids must be filesystem-safe (use only letters, numbers, ".", "_", "-").\n` +
    `- You may use dagPatchOps on levels to create DAG-level ablations (add/remove/replace nodes/edges). If you don't need DAG changes, omit dagPatchOps.\n` +
    `- Do NOT remove core nodes: setup.venv, install.deps, train.run, eval.run, report.write.\n` +
    `- When adding nodes, use unique ids (e.g. abl.*) and keep them cheap.\n` +
    `- Use OPENCLAW_TRAIN_EXTRA_ARGS / OPENCLAW_EVAL_EXTRA_ARGS when you want to vary CLI args.\n` +
    `- Prefer small axes first; keep total compute under control (use estimatedMinutes if possible).\n` +
    `- Do not include secrets.\n\n` +
    `Context:\n` +
    JSON.stringify(basePayload, null, 2)
  );
}

export async function designExperimentSuite(params: {
  planDir: string;
  outDir: string;
  opts: ExperimentSuiteDesignOpts;
  deps?: ExperimentSuiteDesignDeps;
}): Promise<ExperimentSuiteDesignResult> {
  const startedAt = new Date().toISOString();
  const planDir = path.resolve(params.planDir);
  const outDir = path.resolve(params.outDir);

  const warnings: string[] = [];
  const errors: string[] = [];

  const designDir = path.join(outDir, "design");
  const paths = {
    designInput: path.join(designDir, "suite_design_input.json"),
    designOutput: path.join(designDir, "suite_design_output.json"),
    designReport: path.join(designDir, "suite_design_report.json"),
    designSummary: path.join(designDir, "suite_design_summary.md"),
  };

  await fs.mkdir(designDir, { recursive: true });

  const suiteId = params.opts.suiteId.trim();
  const variantCount = sanitizeVariantCount(params.opts.variantCount);
  if (!suiteId) {
    errors.push("suiteId is required.");
  }

  const validation = await validatePlanDir(planDir);
  const planId = validation.ok ? validation.data?.report.planId : undefined;
  if (!validation.ok || !validation.data || !planId) {
    errors.push(...validation.errors);
    warnings.push(...validation.warnings);
  }

  const proposalPath = path.join(planDir, "input", "proposal.md");
  const proposalRaw = (await readTextIfExists(proposalPath)) ?? "";
  const proposalExcerpt = proposalRaw.trim().slice(0, 4000);

  const repoProfilesIndexAbs = path.join(planDir, "ir", "repo_profiles", "index.json");
  const hasRepoProfiles = await fileExists(repoProfilesIndexAbs);
  const repoProfilesIndexRel = hasRepoProfiles
    ? path.relative(planDir, repoProfilesIndexAbs).replaceAll("\\", "/")
    : undefined;

  const prompt =
    validation.ok && validation.data && planId && errors.length === 0
      ? buildDesignPrompt({
          suiteId,
          variantCount,
          planId,
          planDir,
          proposalExcerpt,
          dag: validation.data.dag,
          acceptance: validation.data.acceptance,
          repoProfilesIndexRel,
        })
      : "";

  await writeJsonFile(paths.designInput, {
    schemaVersion: 1,
    createdAt: new Date().toISOString(),
    suiteId,
    variantCount,
    planId,
    planDir,
    repoProfilesIndex: repoProfilesIndexRel,
    promptHint: "Use opencode run --format json with this prompt.",
  });

  if (errors.length > 0) {
    const finishedAt = new Date().toISOString();
    const out: ExperimentSuiteDesignResult = {
      ok: false,
      suiteId,
      planDir,
      planId,
      startedAt,
      finishedAt,
      warnings,
      errors,
      paths,
    };
    await writeJsonFile(paths.designReport, out);
    await writeTextFile(
      paths.designSummary,
      renderDesignSummary({ suiteId, planId, variantCount, warnings, errors }),
    );
    return out;
  }

  if (params.opts.dryRun) {
    const finishedAt = new Date().toISOString();
    const out: ExperimentSuiteDesignResult = {
      ok: true,
      suiteId,
      planDir,
      planId,
      startedAt,
      finishedAt,
      warnings,
      errors,
      design: ExperimentSuiteDesignSchema.parse({
        schemaVersion: 3,
        suiteId,
        baseline: { id: "baseline", name: "Baseline", overrides: {}, dagPatchOps: [] },
        axes: [],
      }),
      paths,
    };
    await writeJsonFile(paths.designOutput, { ok: true, output: out.design });
    await writeJsonFile(paths.designReport, out);
    await writeTextFile(
      paths.designSummary,
      renderDesignSummary({ suiteId, planId, variantCount, warnings, errors, design: out.design }),
    );
    return out;
  }

  const model = params.opts.model?.trim() || "opencode/kimi-k2.5-free";
  const timeoutMs = Math.max(10_000, Math.floor(params.opts.timeoutMs ?? 180_000));
  const agent = (params.opts.agent ?? "").trim() || "openclaw-refine";

  const runCommand = params.deps?.runCommand ?? runCommandWithTimeout;

  const res = await completeJsonWithSchemaViaOpencode({
    schema: ExperimentSuiteDesignSchema,
    prompt,
    model,
    agent,
    files: repoProfilesIndexRel ? [repoProfilesIndexRel] : undefined,
    cwd: planDir,
    timeoutMs,
    opencodeConfigDir: params.deps?.opencodeConfigDir,
    attempts: 2,
    deps: params.deps?.runCommand ? { runCommand } : undefined,
  });
  warnings.push(...res.warnings);

  let design: ExperimentSuiteDesign | undefined;
  let sessionId: string | undefined;
  if (!res.ok) {
    errors.push(res.error);
    sessionId = res.sessionId;
    await writeJsonFile(paths.designOutput, {
      ok: false,
      error: res.error,
      raw: res.raw,
      warnings: res.warnings,
      sessionId: res.sessionId,
    });
  } else {
    design = res.value;
    sessionId = res.sessionId;
    await writeJsonFile(paths.designOutput, {
      ok: true,
      output: design,
      raw: res.raw,
      warnings: res.warnings,
      sessionId: res.sessionId,
    });
  }

  const finishedAt = new Date().toISOString();
  const out: ExperimentSuiteDesignResult = {
    ok: errors.length === 0 && Boolean(design),
    suiteId,
    planDir,
    planId,
    startedAt,
    finishedAt,
    warnings,
    errors,
    design,
    opencodeSessionId: sessionId,
    paths,
  };

  await writeJsonFile(paths.designReport, out);
  await writeTextFile(
    paths.designSummary,
    renderDesignSummary({ suiteId, planId, variantCount, warnings, errors, design }),
  );

  return out;
}
