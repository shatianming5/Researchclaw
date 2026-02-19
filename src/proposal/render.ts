import path from "node:path";
import type { CompileReport, NeedsConfirmItem, PlanDag } from "./schema.js";
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

function guessRepoKey(dag: PlanDag): string | null {
  const candidates = ["setup.venv", "install.deps", "train.run", "eval.run"];
  for (const id of candidates) {
    const node = dag.nodes.find((n) => n.id === id);
    const input = (node?.inputs?.[0] ?? "").trim().replaceAll("\\", "/");
    if (input.startsWith("cache/git/")) {
      const key = input.slice("cache/git/".length).split("/")[0]?.trim();
      if (key) {
        return key;
      }
    }
  }
  const fetch = dag.nodes.find((n) => n.type === "fetch_repo");
  const output = (fetch?.outputs?.[0] ?? "").trim().replaceAll("\\", "/");
  if (output.startsWith("cache/git/")) {
    const key = output.slice("cache/git/".length).split("/")[0]?.trim();
    if (key) {
      return key;
    }
  }
  return null;
}

export function renderPlanRunbookMd(params: {
  layout: PlanLayout;
  report: CompileReport;
  dag: PlanDag;
}): string {
  const { layout, report, dag } = params;
  const rel = (p: string) => path.relative(layout.rootDir, p) || ".";

  const repoKey = guessRepoKey(dag);
  const repoRel = repoKey ? `cache/git/${repoKey}` : "cache/git/<repoKey>";

  const lines: string[] = [];
  lines.push(`# Plan Runbook`);
  lines.push("");
  lines.push(`Plan: \`${report.planId}\``);
  lines.push(`Created: \`${report.createdAt}\``);
  if (report.model) {
    lines.push(`Model: \`${report.model}\``);
  }
  lines.push("");

  lines.push("## Conventions");
  lines.push("");
  lines.push(`- Repo: \`${repoRel}\``);
  lines.push(`- venv: \`${repoKey ? `cache/venv/${repoKey}` : "cache/venv/<repoKey>"}\``);
  lines.push(
    `- Dependency lock (pip freeze): \`${repoKey ? `plan/locks/${repoKey}/pip-freeze.txt` : "plan/locks/<repoKey>/pip-freeze.txt"}\``,
  );
  lines.push(
    `- Training output dir: \`${repoKey ? `artifacts/model/${repoKey}` : "artifacts/model/<repoKey>"}\` (also exported as \`OPENCLAW_OUTPUT_DIR\`)`,
  );
  lines.push(`- Eval metrics: \`report/eval_metrics.json\``);
  lines.push(`- Final metrics: \`report/final_metrics.json\``);
  lines.push(`- Final report: \`report/final_report.md\``);
  lines.push("");

  lines.push("## Pipeline");
  lines.push("");
  lines.push(`- Validate: \`openclaw proposal validate "${layout.rootDir}"\``);
  lines.push(`- Run (safe nodes only): \`openclaw proposal run "${layout.rootDir}"\``);
  lines.push(
    `- Refine (generate commands + scripts): \`openclaw proposal refine "${layout.rootDir}"\``,
  );
  lines.push(`- Execute (sandbox + GPU nodes): \`openclaw proposal execute "${layout.rootDir}"\``);
  lines.push(`- Finalize: \`openclaw proposal finalize "${layout.rootDir}"\``);
  lines.push(`- Accept: \`openclaw proposal accept "${layout.rootDir}"\``);
  lines.push("");

  lines.push("## Scripts");
  lines.push("");
  lines.push("Scripts are written after refine under:");
  lines.push("");

  const scriptNodeIds = ["setup.venv", "install.deps", "train.run", "eval.run", "report.write"];
  const scriptPaths = scriptNodeIds
    .filter((id) => dag.nodes.some((n) => n.id === id))
    .map((id) => rel(path.join(layout.planDir, "scripts", `${id}.sh`)));
  if (scriptPaths.length === 0) {
    lines.push("- (No standard nodes detected in this plan.)");
  } else {
    for (const scriptPath of scriptPaths) {
      lines.push(`- \`${scriptPath}\``);
    }
  }
  lines.push("");

  lines.push("## Manual reproduction");
  lines.push("");
  lines.push("Run the scripts from the repo root:");
  lines.push("");
  lines.push(`- \`cd ${repoRel}\``);
  lines.push(`- \`sh "../../../plan/scripts/setup.venv.sh"\``);
  lines.push(`- \`sh "../../../plan/scripts/install.deps.sh"\``);
  lines.push(`- \`sh "../../../plan/scripts/train.run.sh"\``);
  lines.push(`- \`sh "../../../plan/scripts/eval.run.sh"\``);
  lines.push(`- \`sh "../../../plan/scripts/report.write.sh"\``);
  lines.push("");

  lines.push("## Files");
  lines.push("");
  lines.push(`- DAG: \`${rel(path.join(layout.planDir, "plan.dag.json"))}\``);
  lines.push(`- Acceptance: \`${rel(path.join(layout.planDir, "acceptance.json"))}\``);
  lines.push(`- Retry: \`${rel(path.join(layout.planDir, "retry.json"))}\``);
  lines.push(`- Compile report: \`${rel(path.join(layout.reportDir, "compile_report.json"))}\``);
  lines.push("");

  return `${lines.join("\n")}\n`;
}
