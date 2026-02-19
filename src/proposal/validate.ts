import fs from "node:fs/promises";
import path from "node:path";
import { validateDag } from "./dag.js";
import {
  AcceptanceSpecSchema,
  CompileReportSchema,
  PlanDagSchema,
  RetrySpecSchema,
  type AcceptanceSpec,
  type CompileReport,
  type PlanDag,
  type RetrySpec,
} from "./schema.js";

function normalizeRel(p: string): string {
  return p.trim().replaceAll("\\", "/");
}

export type ValidatePlanDirOpts = {
  /**
   * Enforce checkpoint/resume contract (required for reliable GPU pause/resume).
   * Default: false for backwards compatibility with older plan packages.
   */
  strictResume?: boolean;
};

function validateDagConventions(
  dag: PlanDag,
  opts?: ValidatePlanDirOpts,
): { errors: string[]; warnings: string[] } {
  const errors: string[] = [];
  const warnings: string[] = [];
  const strictResume = opts?.strictResume === true;

  const setup =
    dag.nodes.find((n) => n.id === "setup.venv") ?? dag.nodes.find((n) => n.type === "setup_venv");
  const train =
    dag.nodes.find((n) => n.id === "train.run") ?? dag.nodes.find((n) => n.type === "train");

  if (!setup && !train) {
    return { errors, warnings };
  }

  const setupOutputs = new Set((setup?.outputs ?? []).map((p) => normalizeRel(p)));
  const venvOutput = [...setupOutputs].find((p) => p.startsWith("cache/venv/")) ?? "";
  let repoKey = venvOutput ? venvOutput.slice("cache/venv/".length).split("/")[0]?.trim() : "";
  if (!repoKey && train) {
    const trainOutputs = (train.outputs ?? []).map((p) => normalizeRel(p));
    const artifact = trainOutputs.find((p) => p.startsWith("artifacts/model/")) ?? "";
    repoKey = artifact ? artifact.slice("artifacts/model/".length).split("/")[0]?.trim() : "";
  }

  if (setup) {
    if (!venvOutput || !repoKey) {
      errors.push("setup.venv: outputs must include cache/venv/<repoKey>.");
    }
    for (const required of ["cache/hf", "cache/pip"]) {
      if (!setupOutputs.has(required)) {
        errors.push(`setup.venv: outputs must include ${required}.`);
      }
    }
  }

  if (train && repoKey) {
    const trainOutputs = new Set((train.outputs ?? []).map((p) => normalizeRel(p)));
    const checkpointDir = `artifacts/model/${repoKey}`;
    if (!trainOutputs.has(checkpointDir)) {
      errors.push(`train.run: outputs must include ${checkpointDir}.`);
    }

    if (strictResume) {
      if (!trainOutputs.has("report/checkpoint_manifest.json")) {
        errors.push(
          "train.run: outputs must include report/checkpoint_manifest.json (strictResume).",
        );
      }
    }
  }

  if (strictResume && train) {
    const cmdText = (train.commands ?? []).join("\n");
    if (!cmdText.includes("plan/scripts/train.run.sh")) {
      errors.push("train.run: commands must invoke plan/scripts/train.run.sh (strictResume).");
    }
    const env = train.env ?? {};
    if (!env.OPENCLAW_PLAN_DIR) {
      errors.push("train.run: env.OPENCLAW_PLAN_DIR is required (strictResume).");
    }
    if (!env.OPENCLAW_CHECKPOINT_DIR) {
      errors.push("train.run: env.OPENCLAW_CHECKPOINT_DIR is required (strictResume).");
    }
  }

  return { errors, warnings };
}

async function readJson(filePath: string): Promise<unknown> {
  const raw = await fs.readFile(filePath, "utf-8");
  return JSON.parse(raw) as unknown;
}

export type ValidatePlanResult = {
  ok: boolean;
  errors: string[];
  warnings: string[];
  paths: {
    planDag: string;
    acceptance: string;
    retry: string;
    compileReport: string;
  };
  data?: {
    dag: PlanDag;
    acceptance: AcceptanceSpec;
    retry: RetrySpec;
    report: CompileReport;
    needsConfirmCount: number;
  };
};

export async function validatePlanDir(
  planDir: string,
  opts?: ValidatePlanDirOpts,
): Promise<ValidatePlanResult> {
  const rootDir = path.resolve(planDir);
  const paths = {
    planDag: path.join(rootDir, "plan", "plan.dag.json"),
    acceptance: path.join(rootDir, "plan", "acceptance.json"),
    retry: path.join(rootDir, "plan", "retry.json"),
    compileReport: path.join(rootDir, "report", "compile_report.json"),
  };

  const errors: string[] = [];
  const warnings: string[] = [];

  const [dagRaw, acceptanceRaw, retryRaw, reportRaw] = await Promise.all(
    Object.values(paths).map(async (p) => {
      try {
        return await readJson(p);
      } catch (err) {
        errors.push(`Failed to read ${p}: ${String(err)}`);
        return null;
      }
    }),
  );

  if (errors.length > 0) {
    return { ok: false, errors, warnings, paths };
  }

  const dagParsed = PlanDagSchema.safeParse(dagRaw);
  if (!dagParsed.success) {
    errors.push(`Invalid plan DAG schema: ${dagParsed.error.message}`);
  }
  const acceptanceParsed = AcceptanceSpecSchema.safeParse(acceptanceRaw);
  if (!acceptanceParsed.success) {
    errors.push(`Invalid acceptance schema: ${acceptanceParsed.error.message}`);
  }
  const retryParsed = RetrySpecSchema.safeParse(retryRaw);
  if (!retryParsed.success) {
    errors.push(`Invalid retry schema: ${retryParsed.error.message}`);
  }
  const reportParsed = CompileReportSchema.safeParse(reportRaw);
  if (!reportParsed.success) {
    errors.push(`Invalid compile report schema: ${reportParsed.error.message}`);
  }

  if (errors.length > 0) {
    return { ok: false, errors, warnings, paths };
  }

  if (
    !dagParsed.success ||
    !acceptanceParsed.success ||
    !retryParsed.success ||
    !reportParsed.success
  ) {
    return { ok: false, errors, warnings, paths };
  }

  const dag = dagParsed.data;
  const topo = validateDag(dag);
  if (!topo.ok) {
    errors.push(...topo.errors);
  }

  const conventions = validateDagConventions(dag, opts);
  warnings.push(...conventions.warnings);
  errors.push(...conventions.errors);

  if (opts?.strictResume === true) {
    const script = path.join(rootDir, "plan", "scripts", "train.run.sh");
    const inner = path.join(rootDir, "plan", "scripts", "train.run.inner.sh");
    for (const p of [script, inner]) {
      try {
        await fs.stat(p);
      } catch {
        errors.push(`Missing required script: ${p}`);
      }
    }
  }

  const acceptance = acceptanceParsed.data;
  const needsConfirmCount = acceptance.checks.filter((c) => c.needs_confirm).length;
  if (needsConfirmCount > 0) {
    warnings.push(`Acceptance has ${needsConfirmCount} checks requiring confirmation.`);
  }

  const report = reportParsed.data;
  if (report.needsConfirm.length > 0) {
    warnings.push(
      `Compile report lists ${report.needsConfirm.length} items requiring confirmation.`,
    );
  }

  const ok = errors.length === 0;
  return {
    ok,
    errors,
    warnings,
    paths,
    data: {
      dag,
      acceptance,
      retry: retryParsed.data,
      report,
      needsConfirmCount,
    },
  };
}
