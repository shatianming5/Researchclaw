import path from "node:path";
import type { CompileReport, NeedsConfirmItem } from "./schema.js";
import type { PlanLayout } from "./workdir.js";

function renderItem(item: NeedsConfirmItem): string {
  const lines: string[] = [];
  lines.push(`- [${item.area}] ${item.message}`);
  if (item.suggested?.trim()) {
    lines.push(`  - Suggested: ${item.suggested.trim()}`);
  }
  if (item.evidence?.length) {
    lines.push(`  - Evidence: ${item.evidence.join(", ")}`);
  }
  return lines.join("\n");
}

export function renderNeedsConfirmMd(report: CompileReport): string {
  const lines: string[] = [];
  lines.push(`# Needs Confirm`);
  lines.push("");
  lines.push(`Plan: \`${report.planId}\``);
  lines.push(`Created: \`${report.createdAt}\``);
  if (report.model) {
    lines.push(`Model: \`${report.model}\``);
  }
  lines.push(`Discovery: \`${report.discovery}\``);
  lines.push("");

  if (report.needsConfirm.length === 0) {
    lines.push("No confirmation needed.");
    lines.push("");
    return `${lines.join("\n")}\n`;
  }

  lines.push("## Items");
  lines.push("");
  for (const item of report.needsConfirm) {
    lines.push(renderItem(item));
  }
  lines.push("");
  lines.push("## How to confirm");
  lines.push("");
  lines.push(
    `- Edit \`plan/acceptance.json\` and \`plan/plan.dag.json\` under this plan directory, or create \`plan/overrides.json\` if your execution layer supports it.`,
  );
  lines.push("");
  return `${lines.join("\n")}\n`;
}

export function renderRunbookMd(params: { layout: PlanLayout; report: CompileReport }): string {
  const { layout, report } = params;
  const rel = (p: string) => path.relative(layout.rootDir, p) || ".";

  const lines: string[] = [];
  lines.push(`# Runbook`);
  lines.push("");
  lines.push(`Plan: \`${report.planId}\``);
  lines.push("");
  lines.push("## Files");
  lines.push("");
  lines.push(`- Proposal: \`${rel(path.join(layout.inputDir, "proposal.md"))}\``);
  lines.push(`- Entities: \`${rel(path.join(layout.irDir, "extracted.entities.json"))}\``);
  lines.push(`- Discovery: \`${rel(path.join(layout.irDir, "discovery.json"))}\``);
  lines.push(`- DAG: \`${rel(path.join(layout.planDir, "plan.dag.json"))}\``);
  lines.push(`- Acceptance: \`${rel(path.join(layout.planDir, "acceptance.json"))}\``);
  lines.push(`- Retry: \`${rel(path.join(layout.planDir, "retry.json"))}\``);
  lines.push(`- Compile report: \`${rel(path.join(layout.reportDir, "compile_report.json"))}\``);
  lines.push(`- Needs confirm: \`${rel(path.join(layout.reportDir, "needs_confirm.md"))}\``);
  lines.push("");

  lines.push("## Next commands");
  lines.push("");
  lines.push(`- Validate: \`openclaw proposal validate "${layout.rootDir}"\``);
  lines.push(`- Review: \`openclaw proposal review "${layout.rootDir}"\``);
  lines.push(`- Run (safe nodes only): \`openclaw proposal run "${layout.rootDir}"\``);
  lines.push("");

  lines.push("## Notes");
  lines.push("");
  lines.push(
    `- This is a compile-time plan package. It does not execute training or install dependencies.`,
  );
  lines.push(
    `- Any acceptance checks marked \`needs_confirm=true\` should be finalized before execution.`,
  );
  lines.push("");

  return `${lines.join("\n")}\n`;
}
