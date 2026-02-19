import fs from "node:fs/promises";
import path from "node:path";
import type { MetricDelta, MetricValue } from "../results/schema.js";
import { redactSensitiveText } from "../../logging/redact.js";
import { writeJsonFile, writeTextFile } from "../files.js";
import { readMetricsFile } from "../results/collect.js";
import { computeMetricDeltas } from "../results/metrics.js";
import {
  RepairEvidenceSchema,
  type RepairEvidence,
  type RepairEvidenceStatus,
} from "./repair-evidence.schema.js";
import { sanitizeIdPart } from "./utils.js";

const MAX_LOG_BYTES = 1_000_000;

function capText(text: string, maxBytes = MAX_LOG_BYTES): { text: string; capped: boolean } {
  const buf = Buffer.from(text ?? "", "utf-8");
  if (buf.byteLength <= maxBytes) {
    return { text, capped: false };
  }
  const sliced = buf.subarray(buf.byteLength - maxBytes);
  return { text: sliced.toString("utf-8"), capped: true };
}

export function resolveRepairsDir(planDir: string, nodeId: string): string {
  return path.join(planDir, "report", "repairs", sanitizeIdPart(nodeId));
}

export function resolveRepairAttemptDir(
  planDir: string,
  nodeId: string,
  patchAttempt: number,
): string {
  return path.join(resolveRepairsDir(planDir, nodeId), `attempt-${patchAttempt}`);
}

async function writeCappedLogFile(params: {
  filePath: string;
  content: string;
  warnings: string[];
}): Promise<void> {
  const redacted = redactSensitiveText(params.content ?? "");
  const capped = capText(redacted);
  if (capped.capped) {
    params.warnings.push(`Log capped to ${MAX_LOG_BYTES} bytes: ${path.basename(params.filePath)}`);
  }
  await fs.mkdir(path.dirname(params.filePath), { recursive: true });
  await fs.writeFile(params.filePath, capped.text, "utf-8");
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.stat(filePath);
    return true;
  } catch {
    return false;
  }
}

async function findMetricsFile(planDir: string): Promise<{ path?: string; warnings: string[] }> {
  const warnings: string[] = [];
  const evalPath = path.join(planDir, "report", "eval_metrics.json");
  if (await fileExists(evalPath)) {
    return { path: evalPath, warnings };
  }
  const finalPath = path.join(planDir, "report", "final_metrics.json");
  if (await fileExists(finalPath)) {
    return { path: finalPath, warnings };
  }
  return { warnings };
}

export async function collectMetricsSnapshot(planDir: string): Promise<{
  path?: string;
  values: Record<string, MetricValue>;
  warnings: string[];
}> {
  const found = await findMetricsFile(planDir);
  const warnings = [...found.warnings];
  if (!found.path) {
    return {
      values: {},
      warnings: ["No metrics file found (expected report/eval_metrics.json).", ...warnings],
    };
  }
  const res = await readMetricsFile(found.path);
  if (!res.ok) {
    return {
      path: found.path,
      values: {},
      warnings: [`Failed to parse metrics: ${res.error}`, ...warnings],
    };
  }
  return { path: res.path, values: res.values, warnings: [...warnings, ...res.warnings] };
}

export function renderRepairEvidenceMd(evidence: RepairEvidence): string {
  const lines: string[] = [];
  lines.push("# Repair Evidence");
  lines.push("");
  lines.push(`- createdAt: ${evidence.createdAt}`);
  if (evidence.planId) {
    lines.push(`- planId: \`${evidence.planId}\``);
  }
  lines.push(`- node: \`${evidence.node.id}\``);
  lines.push(`- patchAttempt: ${evidence.attempts.patchAttempt}`);
  if (evidence.attempts.rerunAttempt) {
    lines.push(`- rerunAttempt: ${evidence.attempts.rerunAttempt}`);
  }
  lines.push(`- status: **${evidence.status}**`);
  lines.push("");

  lines.push("## Before");
  lines.push(`- ok: ${String(evidence.before.ok)}`);
  if (evidence.before.failureCategory) {
    lines.push(`- failureCategory: ${evidence.before.failureCategory}`);
  }
  if (evidence.before.stdoutPath) {
    lines.push(`- stdout: \`${evidence.before.stdoutPath}\``);
  }
  if (evidence.before.stderrPath) {
    lines.push(`- stderr: \`${evidence.before.stderrPath}\``);
  }
  lines.push("");

  if (evidence.patch) {
    lines.push("## Patch");
    lines.push(`- patchPath: \`${evidence.patch.patchPath}\``);
    const summary = evidence.patch.summary;
    const changed = summary.added.length + summary.modified.length + summary.deleted.length;
    lines.push(`- filesChanged: ${changed}`);
    lines.push("");
  }

  if (evidence.after) {
    lines.push("## After");
    lines.push(`- ok: ${String(evidence.after.ok)}`);
    if (evidence.after.failureCategory) {
      lines.push(`- failureCategory: ${evidence.after.failureCategory}`);
    }
    if (evidence.after.stdoutPath) {
      lines.push(`- stdout: \`${evidence.after.stdoutPath}\``);
    }
    if (evidence.after.stderrPath) {
      lines.push(`- stderr: \`${evidence.after.stderrPath}\``);
    }
    lines.push("");
  }

  if (evidence.metrics) {
    lines.push("## Metrics (After - Before)");
    if (evidence.metrics.beforePath) {
      lines.push(`- before: \`${evidence.metrics.beforePath}\``);
    }
    if (evidence.metrics.afterPath) {
      lines.push(`- after: \`${evidence.metrics.afterPath}\``);
    }
    lines.push("");
    lines.push("| metric | after | before | delta |");
    lines.push("| --- | --- | --- | --- |");
    for (const delta of evidence.metrics.deltas) {
      lines.push(
        `| ${delta.name} | ${delta.current ?? ""} | ${delta.baseline ?? ""} | ${delta.delta ?? ""} |`,
      );
    }
    if (evidence.metrics.deltas.length === 0) {
      lines.push("| (none) | | | |");
    }
    lines.push("");
  }

  if (evidence.warnings.length > 0) {
    lines.push("## Warnings");
    for (const warning of evidence.warnings) {
      lines.push(`- ${warning}`);
    }
    lines.push("");
  }

  if (evidence.errors.length > 0) {
    lines.push("## Errors");
    for (const error of evidence.errors) {
      lines.push(`- ${error}`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

export type PendingRepair = {
  nodeId: string;
  patchAttempt: number;
  rerunAttempt: number;
  patchPath?: string;
  patchSummary?: { added: string[]; modified: string[]; deleted: string[] };
  before: {
    ok: boolean;
    exitCode?: number | null;
    timedOut?: boolean;
    failureCategory?: string;
    stdout: string;
    stderr: string;
    stdoutTail?: string;
    stderrTail?: string;
    stdoutPath?: string;
    stderrPath?: string;
  };
  metricsBefore?: { path?: string; values: Record<string, MetricValue>; warnings: string[] };
  warnings: string[];
};

export async function recordPendingRepair(params: {
  planDir: string;
  workdirRel?: string;
  node: { id: string; type?: string; tool?: string; commands?: string[] };
  pending: PendingRepair;
}): Promise<PendingRepair> {
  const attemptDir = resolveRepairAttemptDir(
    params.planDir,
    params.node.id,
    params.pending.patchAttempt,
  );
  const warnings = params.pending.warnings;

  const beforeStdoutPath = path.join(attemptDir, "before.stdout.txt");
  const beforeStderrPath = path.join(attemptDir, "before.stderr.txt");
  await writeCappedLogFile({
    filePath: beforeStdoutPath,
    content: params.pending.before.stdout,
    warnings,
  });
  await writeCappedLogFile({
    filePath: beforeStderrPath,
    content: params.pending.before.stderr,
    warnings,
  });

  const metricsBefore = params.pending.metricsBefore;
  if (metricsBefore) {
    const beforeMetricsPath = path.join(attemptDir, "metrics_before.json");
    await writeJsonFile(beforeMetricsPath, metricsBefore);
  }

  return {
    ...params.pending,
    before: {
      ...params.pending.before,
      stdoutPath: path.relative(params.planDir, beforeStdoutPath).replaceAll("\\", "/"),
      stderrPath: path.relative(params.planDir, beforeStderrPath).replaceAll("\\", "/"),
    },
  };
}

export async function finalizeRepairEvidence(params: {
  planDir: string;
  planId?: string;
  workdirRel?: string;
  node: { id: string; type?: string; tool?: string; commands?: string[] };
  pending: PendingRepair;
  after: {
    ok: boolean;
    exitCode?: number | null;
    timedOut?: boolean;
    failureCategory?: string;
    stdout: string;
    stderr: string;
    stdoutTail?: string;
    stderrTail?: string;
  };
  metricsAfter?: { path?: string; values: Record<string, MetricValue>; warnings: string[] };
}): Promise<RepairEvidence> {
  const createdAt = new Date().toISOString();
  const attemptDir = resolveRepairAttemptDir(
    params.planDir,
    params.node.id,
    params.pending.patchAttempt,
  );
  const warnings = [...params.pending.warnings];
  const errors: string[] = [];

  const afterStdoutPath = path.join(attemptDir, "after.stdout.txt");
  const afterStderrPath = path.join(attemptDir, "after.stderr.txt");
  await writeCappedLogFile({ filePath: afterStdoutPath, content: params.after.stdout, warnings });
  await writeCappedLogFile({ filePath: afterStderrPath, content: params.after.stderr, warnings });

  const metricsBefore = params.pending.metricsBefore ?? {
    values: {},
    warnings: ["No metrics snapshot captured before patch."],
  };
  const metricsAfter = params.metricsAfter ?? {
    values: {},
    warnings: ["No metrics snapshot captured after rerun."],
  };
  const metricWarnings = [...metricsBefore.warnings, ...metricsAfter.warnings];
  const deltas: MetricDelta[] = computeMetricDeltas({
    current: metricsAfter.values,
    baseline: metricsBefore.values,
  });

  const beforeFile = path.join(attemptDir, "metrics_before.json");
  const afterFile = path.join(attemptDir, "metrics_after.json");
  const deltasFile = path.join(attemptDir, "metrics_deltas.json");
  await writeJsonFile(beforeFile, metricsBefore);
  await writeJsonFile(afterFile, metricsAfter);
  await writeJsonFile(deltasFile, deltas);

  const status: RepairEvidenceStatus = params.after.ok ? "rerun_ok" : "rerun_failed";

  const evidenceJson = path.join(attemptDir, "repair_evidence.json");
  const evidenceMd = path.join(attemptDir, "repair_evidence.md");

  const evidence: RepairEvidence = RepairEvidenceSchema.parse({
    schemaVersion: 1,
    createdAt,
    planId: params.planId,
    planDir: params.planDir,
    node: {
      id: params.node.id,
      type: params.node.type,
      tool: params.node.tool,
      commands: params.node.commands,
      workdirRel: params.workdirRel,
    },
    attempts: {
      patchAttempt: params.pending.patchAttempt,
      rerunAttempt: params.pending.rerunAttempt,
    },
    before: {
      ok: params.pending.before.ok,
      exitCode: params.pending.before.exitCode,
      timedOut: params.pending.before.timedOut,
      failureCategory: params.pending.before.failureCategory as never,
      stdoutTail: params.pending.before.stdoutTail,
      stderrTail: params.pending.before.stderrTail,
      stdoutPath: path
        .relative(params.planDir, path.join(attemptDir, "before.stdout.txt"))
        .replaceAll("\\", "/"),
      stderrPath: path
        .relative(params.planDir, path.join(attemptDir, "before.stderr.txt"))
        .replaceAll("\\", "/"),
    },
    patch:
      params.pending.patchPath && params.pending.patchSummary
        ? {
            patchPath: params.pending.patchPath,
            summary: params.pending.patchSummary,
          }
        : undefined,
    after: {
      ok: params.after.ok,
      exitCode: params.after.exitCode,
      timedOut: params.after.timedOut,
      failureCategory: params.after.failureCategory as never,
      stdoutTail: params.after.stdoutTail,
      stderrTail: params.after.stderrTail,
      stdoutPath: path.relative(params.planDir, afterStdoutPath).replaceAll("\\", "/"),
      stderrPath: path.relative(params.planDir, afterStderrPath).replaceAll("\\", "/"),
    },
    metrics: {
      beforePath: metricsBefore.path,
      afterPath: metricsAfter.path,
      valuesBefore: metricsBefore.values,
      valuesAfter: metricsAfter.values,
      deltas,
      warnings: metricWarnings,
      files: {
        before: path.relative(params.planDir, beforeFile).replaceAll("\\", "/"),
        after: path.relative(params.planDir, afterFile).replaceAll("\\", "/"),
        deltas: path.relative(params.planDir, deltasFile).replaceAll("\\", "/"),
      },
    },
    status,
    warnings,
    errors,
    paths: {
      evidenceJson: path.relative(params.planDir, evidenceJson).replaceAll("\\", "/"),
      evidenceMd: path.relative(params.planDir, evidenceMd).replaceAll("\\", "/"),
    },
  });

  await writeJsonFile(evidenceJson, evidence);
  await writeTextFile(evidenceMd, renderRepairEvidenceMd(evidence));
  return evidence;
}

export async function writeAppliedOnlyRepairEvidence(params: {
  planDir: string;
  planId?: string;
  workdirRel?: string;
  node: { id: string; type?: string; tool?: string; commands?: string[] };
  pending: PendingRepair;
}): Promise<RepairEvidence> {
  const createdAt = new Date().toISOString();
  const attemptDir = resolveRepairAttemptDir(
    params.planDir,
    params.node.id,
    params.pending.patchAttempt,
  );
  const warnings = [...params.pending.warnings];
  const errors: string[] = [];

  const metricsBefore = params.pending.metricsBefore ?? {
    values: {},
    warnings: ["No metrics snapshot captured before patch."],
  };
  const beforeFile = path.join(attemptDir, "metrics_before.json");
  await writeJsonFile(beforeFile, metricsBefore);

  const evidenceJson = path.join(attemptDir, "repair_evidence.json");
  const evidenceMd = path.join(attemptDir, "repair_evidence.md");

  const evidence: RepairEvidence = RepairEvidenceSchema.parse({
    schemaVersion: 1,
    createdAt,
    planId: params.planId,
    planDir: params.planDir,
    node: {
      id: params.node.id,
      type: params.node.type,
      tool: params.node.tool,
      commands: params.node.commands,
      workdirRel: params.workdirRel,
    },
    attempts: {
      patchAttempt: params.pending.patchAttempt,
    },
    before: {
      ok: params.pending.before.ok,
      exitCode: params.pending.before.exitCode,
      timedOut: params.pending.before.timedOut,
      failureCategory: params.pending.before.failureCategory as never,
      stdoutTail: params.pending.before.stdoutTail,
      stderrTail: params.pending.before.stderrTail,
      stdoutPath:
        params.pending.before.stdoutPath ??
        path
          .relative(params.planDir, path.join(attemptDir, "before.stdout.txt"))
          .replaceAll("\\", "/"),
      stderrPath:
        params.pending.before.stderrPath ??
        path
          .relative(params.planDir, path.join(attemptDir, "before.stderr.txt"))
          .replaceAll("\\", "/"),
    },
    patch:
      params.pending.patchPath && params.pending.patchSummary
        ? { patchPath: params.pending.patchPath, summary: params.pending.patchSummary }
        : undefined,
    metrics: {
      beforePath: metricsBefore.path,
      valuesBefore: metricsBefore.values,
      warnings: metricsBefore.warnings,
      files: {
        before: path.relative(params.planDir, beforeFile).replaceAll("\\", "/"),
      },
    },
    status: "applied_only",
    warnings,
    errors,
    paths: {
      evidenceJson: path.relative(params.planDir, evidenceJson).replaceAll("\\", "/"),
      evidenceMd: path.relative(params.planDir, evidenceMd).replaceAll("\\", "/"),
    },
  });

  await writeJsonFile(evidenceJson, evidence);
  await writeTextFile(evidenceMd, renderRepairEvidenceMd(evidence));
  return evidence;
}

export function pickTopNumericDeltas(
  deltas: MetricDelta[],
  max = 3,
): Array<{ name: string; delta: number | null }> {
  const numeric = deltas
    .map((d) => ({ name: d.name, delta: typeof d.delta === "number" ? d.delta : null }))
    .filter((d) => d.delta !== null);
  numeric.sort((a, b) => Math.abs(b.delta ?? 0) - Math.abs(a.delta ?? 0));
  return numeric.slice(0, Math.max(0, Math.floor(max)));
}
