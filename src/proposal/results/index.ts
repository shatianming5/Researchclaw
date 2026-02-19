import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { writeJsonFile, writeTextFile } from "../files.js";
import { validatePlanDir } from "../validate.js";
import { formatPlanTimestamp } from "../workdir.js";
import { archivePlanArtifacts, buildArtifactManifest, mergeArchiveResults } from "./archive.js";
import {
  loadExecuteLog,
  loadManualApprovals,
  readMetricsFile,
  resolveBaselineMetricsPath,
  resolveCurrentMetrics,
} from "./collect.js";
import { evaluateAcceptanceSpec } from "./evaluate.js";
import { computeMetricDeltas } from "./metrics.js";
import { renderAcceptanceReportMd } from "./render.js";
import { collectRepairEvidenceIndex } from "./repairs.js";
import { AcceptanceReportSchema, type AcceptanceReport, type MetricValue } from "./schema.js";

export const DEFAULT_ARCHIVE_PATHS: string[] = [
  "input/proposal.md",
  "input/context.json",
  "plan/plan.dag.json",
  "plan/acceptance.json",
  "plan/retry.json",
  "report/compile_report.json",
  "report/run_log.json",
  "report/execution_suggestions.md",
  "report/execute_log.json",
  "report/execute_summary.md",
  "report/static_checks",
  "report/checkpoint_manifest.json",
  "report/final_metrics.json",
  "report/final_report.md",
  "report/repo_workflow",
  "report/repairs",
];

function statusToExitCode(status: AcceptanceReport["status"]): number {
  if (status === "pass") {
    return 0;
  }
  if (status === "needs_confirm") {
    return 2;
  }
  return 1;
}

function buildRunId(now: Date): string {
  const suffix = crypto.randomBytes(3).toString("hex");
  return `${formatPlanTimestamp(now)}-${suffix}`;
}

export async function acceptProposalResults(params: {
  planDir: string;
  baselinePath?: string;
}): Promise<AcceptanceReport> {
  const planDir = path.resolve(params.planDir);
  const createdAt = new Date().toISOString();

  const reportJson = path.join(planDir, "report", "acceptance_report.json");
  const reportMd = path.join(planDir, "report", "acceptance_report.md");

  const baselineResolved = await resolveBaselineMetricsPath({
    planDir,
    baselinePath: params.baselinePath,
  });

  const validation = await validatePlanDir(planDir);
  const planId = validation.ok ? validation.data?.report.planId : undefined;

  const runId = buildRunId(new Date());
  const runsRoot = path.join(planDir, "report", "runs");
  const runDir = path.join(runsRoot, runId);
  const manifestJson = path.join(runDir, "manifest.json");

  await fs.mkdir(path.join(planDir, "report"), { recursive: true });
  await fs.mkdir(runDir, { recursive: true });

  const archiveBase = await archivePlanArtifacts({
    planDir,
    runDir,
    include: DEFAULT_ARCHIVE_PATHS,
  });

  let manifest = buildArtifactManifest({
    runId,
    createdAt,
    planId,
    planDir,
    archive: archiveBase,
  });
  await writeJsonFile(manifestJson, manifest);

  const warnings: string[] = [];
  const errors: string[] = [];
  warnings.push(...baselineResolved.warnings);
  warnings.push(...archiveBase.warnings);

  const repairsIndex = await collectRepairEvidenceIndex(planDir);

  if (!validation.ok || !validation.data) {
    errors.push(...validation.errors);
    warnings.push(...validation.warnings);

    const status: AcceptanceReport["status"] = "fail";
    const exitCode = statusToExitCode(status);
    const report: AcceptanceReport = AcceptanceReportSchema.parse({
      schemaVersion: 1,
      ok: false,
      exitCode,
      createdAt,
      planId,
      planDir,
      runId,
      runDir,
      status,
      summary: { pass: 0, fail: 0, needs_confirm: 0, total: 0 },
      checks: [],
      artifacts: {
        manifestPath: manifestJson,
        archived: archiveBase.archived,
        missing: archiveBase.missing,
      },
      repairs: {
        entries: repairsIndex.entries,
        warnings: repairsIndex.warnings,
      },
      warnings,
      errors,
      paths: { reportJson, reportMd, manifestJson },
    });

    await writeJsonFile(reportJson, report);
    await writeTextFile(reportMd, renderAcceptanceReportMd(report));

    const archiveReport = await archivePlanArtifacts({
      planDir,
      runDir,
      include: ["report/acceptance_report.json", "report/acceptance_report.md"],
    });
    const merged = mergeArchiveResults({ base: archiveBase, extra: archiveReport });
    manifest = buildArtifactManifest({ runId, createdAt, planId, planDir, archive: merged });
    await writeJsonFile(manifestJson, manifest);

    return report;
  }

  warnings.push(...validation.warnings);

  const manualApprovals = await loadManualApprovals(planDir);
  warnings.push(...manualApprovals.warnings);

  const executeLogRes = await loadExecuteLog(planDir);
  warnings.push(...executeLogRes.warnings);

  const currentMetrics = await resolveCurrentMetrics(planDir);
  const metricsWarnings = currentMetrics?.warnings ?? [];
  warnings.push(...metricsWarnings);
  const currentValues = currentMetrics?.values ?? {};

  const baselinePath = baselineResolved.path;
  let baselineValues: Record<string, MetricValue> | undefined;
  if (baselinePath) {
    const baselineRes = await readMetricsFile(baselinePath);
    if (!baselineRes.ok) {
      warnings.push(`Failed to parse baseline metrics: ${baselineRes.error}`);
    } else {
      baselineValues = baselineRes.values;
      warnings.push(...baselineRes.warnings.map((w) => `Baseline: ${w}`));
    }
  }

  const evaluated = await evaluateAcceptanceSpec({
    planDir,
    spec: validation.data.acceptance,
    metrics: currentValues,
    executeLog: executeLogRes.log,
    approved: manualApprovals.approved,
  });

  const status = evaluated.status;
  const exitCode = statusToExitCode(status);
  const ok = status === "pass";

  const deltas = computeMetricDeltas({ current: currentValues, baseline: baselineValues });
  const report: AcceptanceReport = AcceptanceReportSchema.parse({
    schemaVersion: 1,
    ok,
    exitCode,
    createdAt,
    planId,
    planDir,
    runId,
    runDir,
    status,
    summary: evaluated.summary,
    checks: evaluated.checks,
    metrics: {
      currentPath: currentMetrics?.path,
      baselinePath,
      values: currentValues,
      deltas,
    },
    artifacts: {
      manifestPath: manifestJson,
      archived: archiveBase.archived,
      missing: archiveBase.missing,
    },
    repairs: {
      entries: repairsIndex.entries,
      warnings: repairsIndex.warnings,
    },
    warnings,
    errors,
    paths: { reportJson, reportMd, manifestJson },
  });

  await writeJsonFile(reportJson, report);
  await writeTextFile(reportMd, renderAcceptanceReportMd(report));

  const archiveReport = await archivePlanArtifacts({
    planDir,
    runDir,
    include: ["report/acceptance_report.json", "report/acceptance_report.md"],
  });
  const merged = mergeArchiveResults({ base: archiveBase, extra: archiveReport });
  manifest = buildArtifactManifest({ runId, createdAt, planId, planDir, archive: merged });
  await writeJsonFile(manifestJson, manifest);

  return report;
}
