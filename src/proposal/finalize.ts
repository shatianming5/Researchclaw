import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { MetricValue } from "./results/schema.js";
import { readMetricsFile } from "./results/collect.js";
import { validatePlanDir } from "./validate.js";

export type ProposalFinalizeOpts = {
  force?: boolean;
};

export type ProposalFinalizeResult = {
  ok: boolean;
  planDir: string;
  planId?: string;
  startedAt: string;
  finishedAt: string;
  warnings: string[];
  errors: string[];
  wrote: {
    finalMetrics: boolean;
    finalReport: boolean;
  };
  paths: {
    evalMetrics: string;
    finalMetrics: string;
    finalReport: string;
  };
};

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.stat(filePath);
    return true;
  } catch {
    return false;
  }
}

async function writeTextFileAtomic(params: {
  filePath: string;
  content: string;
  overwrite: boolean;
}): Promise<void> {
  const dir = path.dirname(params.filePath);
  await fs.mkdir(dir, { recursive: true });

  const suffix = crypto.randomBytes(4).toString("hex");
  const tempPath = `${params.filePath}.tmp-${suffix}`;
  await fs.writeFile(tempPath, params.content, "utf-8");

  try {
    await fs.rename(tempPath, params.filePath);
  } catch (err) {
    if (params.overwrite) {
      try {
        await fs.rm(params.filePath, { force: true });
      } catch {
        // ignore; rename will throw if still present.
      }
      await fs.rename(tempPath, params.filePath);
      return;
    }
    throw err;
  }
}

async function writeJsonFileAtomic(params: {
  filePath: string;
  value: unknown;
  overwrite: boolean;
}): Promise<void> {
  const json = `${JSON.stringify(params.value, null, 2)}\n`;
  await writeTextFileAtomic({
    filePath: params.filePath,
    content: json,
    overwrite: params.overwrite,
  });
}

function renderFinalReportMd(params: {
  createdAt: string;
  metrics: Record<string, MetricValue>;
  notes: string[];
}): string {
  const lines: string[] = [];
  lines.push("# Final Report");
  lines.push("");
  lines.push(`- createdAt: ${params.createdAt}`);
  lines.push("");

  const metricKeys = Object.keys(params.metrics).toSorted();
  if (metricKeys.length > 0) {
    lines.push("## Metrics");
    lines.push("");
    for (const key of metricKeys) {
      lines.push(`- ${key}: ${String(params.metrics[key])}`);
    }
    lines.push("");
  }

  const notes = params.notes.map((note) => note.trim()).filter(Boolean);
  if (notes.length > 0) {
    lines.push("## Notes");
    lines.push("");
    for (const note of notes) {
      lines.push(`- ${note}`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

export async function finalizeProposalPlan(params: {
  planDir: string;
  opts?: ProposalFinalizeOpts;
}): Promise<ProposalFinalizeResult> {
  const startedAt = new Date().toISOString();
  const planDir = path.resolve(params.planDir);
  const warnings: string[] = [];
  const errors: string[] = [];

  const validation = await validatePlanDir(planDir);
  const planId = validation.ok ? validation.data?.report.planId : undefined;
  if (!validation.ok || !validation.data) {
    errors.push(...validation.errors);
    warnings.push(...validation.warnings);
    const finishedAt = new Date().toISOString();
    return {
      ok: false,
      planDir,
      planId,
      startedAt,
      finishedAt,
      warnings,
      errors,
      wrote: { finalMetrics: false, finalReport: false },
      paths: {
        evalMetrics: path.join(planDir, "report", "eval_metrics.json"),
        finalMetrics: path.join(planDir, "report", "final_metrics.json"),
        finalReport: path.join(planDir, "report", "final_report.md"),
      },
    };
  }

  warnings.push(...validation.warnings);

  const force = params.opts?.force === true;
  const evalMetricsPath = path.join(planDir, "report", "eval_metrics.json");
  const finalMetricsPath = path.join(planDir, "report", "final_metrics.json");
  const finalReportPath = path.join(planDir, "report", "final_report.md");

  const finalMetricsExists = await fileExists(finalMetricsPath);
  const finalReportExists = await fileExists(finalReportPath);

  const createdAt = new Date().toISOString();
  const notes: string[] = [];
  let metrics: Record<string, MetricValue> = {};

  if (await fileExists(evalMetricsPath)) {
    const res = await readMetricsFile(evalMetricsPath);
    if (res.ok) {
      metrics = res.values;
      notes.push(...res.warnings);
    } else {
      notes.push(`Failed to parse eval metrics: ${res.error}`);
    }
  } else {
    notes.push("Missing report/eval_metrics.json");
  }

  const payload = {
    schemaVersion: 1,
    createdAt,
    metrics,
    notes,
  };

  let wroteFinalMetrics = false;
  if (!finalMetricsExists || force) {
    await writeJsonFileAtomic({ filePath: finalMetricsPath, value: payload, overwrite: force });
    wroteFinalMetrics = true;
  } else {
    warnings.push("final_metrics.json exists; skipping (use --force to overwrite).");
  }

  let wroteFinalReport = false;
  if (!finalReportExists || force) {
    const md = renderFinalReportMd({ createdAt, metrics, notes });
    await writeTextFileAtomic({ filePath: finalReportPath, content: md, overwrite: force });
    wroteFinalReport = true;
  } else {
    warnings.push("final_report.md exists; skipping (use --force to overwrite).");
  }

  const finishedAt = new Date().toISOString();
  return {
    ok: errors.length === 0,
    planDir,
    planId,
    startedAt,
    finishedAt,
    warnings,
    errors,
    wrote: { finalMetrics: wroteFinalMetrics, finalReport: wroteFinalReport },
    paths: {
      evalMetrics: evalMetricsPath,
      finalMetrics: finalMetricsPath,
      finalReport: finalReportPath,
    },
  };
}
