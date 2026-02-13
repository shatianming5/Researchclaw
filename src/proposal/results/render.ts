import type { AcceptanceReport } from "./schema.js";

function fmtStatus(status: string): string {
  if (status === "pass") {
    return "PASS";
  }
  if (status === "fail") {
    return "FAIL";
  }
  return "NEEDS_CONFIRM";
}

export function renderAcceptanceReportMd(report: AcceptanceReport): string {
  const lines: string[] = [];
  lines.push("# Acceptance Report");
  lines.push("");
  if (report.planId) {
    lines.push(`- Plan: \`${report.planId}\``);
  }
  lines.push(`- Created: ${report.createdAt}`);
  lines.push(`- Status: **${fmtStatus(report.status)}**`);
  lines.push(`- Run: \`${report.runId}\``);
  lines.push(`- Archive: \`${report.runDir}\``);
  lines.push(`- Manifest: \`${report.paths.manifestJson}\``);
  lines.push("");

  lines.push("## Summary");
  lines.push(`- pass: ${report.summary.pass}`);
  lines.push(`- fail: ${report.summary.fail}`);
  lines.push(`- needs_confirm: ${report.summary.needs_confirm}`);
  lines.push(`- total: ${report.summary.total}`);
  lines.push("");

  lines.push("## Checks");
  for (const entry of report.checks) {
    const label = fmtStatus(entry.status);
    const detail = entry.message ? ` — ${entry.message}` : "";
    lines.push(`- [${label}] ${entry.check.type} ${entry.check.selector}${detail}`);
  }
  if (report.checks.length === 0) {
    lines.push("- (none)");
  }
  lines.push("");

  if (report.metrics) {
    lines.push("## Metrics");
    if (report.metrics.currentPath) {
      lines.push(`- current: \`${report.metrics.currentPath}\``);
    }
    if (report.metrics.baselinePath) {
      lines.push(`- baseline: \`${report.metrics.baselinePath}\``);
    }
    lines.push("");
    lines.push("| metric | current | baseline | delta |");
    lines.push("| --- | --- | --- | --- |");
    for (const delta of report.metrics.deltas) {
      lines.push(
        `| ${delta.name} | ${delta.current ?? ""} | ${delta.baseline ?? ""} | ${
          delta.delta ?? ""
        } |`,
      );
    }
    if (report.metrics.deltas.length === 0) {
      lines.push("| (none) | | | |");
    }
    lines.push("");
  }

  if (report.warnings.length > 0) {
    lines.push("## Warnings");
    for (const warning of report.warnings) {
      lines.push(`- ${warning}`);
    }
    lines.push("");
  }

  if (report.errors.length > 0) {
    lines.push("## Errors");
    for (const error of report.errors) {
      lines.push(`- ${error}`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}
