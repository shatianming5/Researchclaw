import type { ProposalExecuteResult } from "./types.js";

export function renderExecuteSummary(result: ProposalExecuteResult): string {
  const lines: string[] = [];
  lines.push("# Proposal Execute Summary");
  lines.push("");
  if (result.planId) {
    lines.push(`Plan: \`${result.planId}\``);
    lines.push("");
  }
  lines.push(`Dir: \`${result.planDir}\``);
  lines.push("");
  const okCount = result.results.filter((r) => r.status === "ok").length;
  const skippedCount = result.results.filter((r) => r.status === "skipped").length;
  const failedCount = result.results.filter((r) => r.status === "failed").length;
  lines.push(`Results: ok=${okCount} skipped=${skippedCount} failed=${failedCount}`);
  lines.push("");
  if (result.warnings.length > 0) {
    lines.push("## Warnings");
    lines.push("");
    for (const w of result.warnings) {
      lines.push(`- ${w}`);
    }
    lines.push("");
  }
  if (result.errors.length > 0) {
    lines.push("## Errors");
    lines.push("");
    for (const e of result.errors) {
      lines.push(`- ${e}`);
    }
    lines.push("");
  }
  lines.push("## Node results");
  lines.push("");
  for (const r of result.results) {
    lines.push(`- \`${r.nodeId}\` (${r.type}) via ${r.executor}: ${r.status}`);
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}
