import fs from "node:fs/promises";
import path from "node:path";
import type { CommandOptions, SpawnResult } from "../process/exec.js";
import type { RepoProfile } from "./adapters/schema.js";
import type { PlanLayout } from "./workdir.js";
import { profileRepo } from "./adapters/profile.js";
import { getAdapter, pickBestAdapter } from "./adapters/registry.js";
import { validateDag } from "./dag.js";
import { writeJsonFile, writeTextFile } from "./files.js";
import { completeJsonWithSchemaViaOpencode } from "./opencode/run.js";
import { ProposalRefineOutputSchemaV1, type ProposalRefineOutputV1 } from "./refine.schema.js";
import { renderPlanRunbookMd } from "./render.js";
import { type PlanDag, PlanDagSchema, type PlanNode } from "./schema.js";
import { validatePlanDir } from "./validate.js";

type RunCommandLike = (argv: string[], options: CommandOptions) => Promise<SpawnResult>;

export type ProposalRefineOpts = {
  model?: string;
  agent?: string;
  timeoutMs?: number;
  dryRun?: boolean;
  writeAcceptance?: boolean;
  /**
   * Optional extra instructions to steer the refine output (e.g. experiment variant details).
   * Must not change node/edge structure (refine enforces parity).
   */
  instructions?: string;
};

export type ProposalRefineDeps = {
  runCommand?: RunCommandLike;
  opencodeConfigDir?: string;
};

export type ProposalRefineResult = {
  ok: boolean;
  planDir: string;
  planId?: string;
  startedAt: string;
  finishedAt: string;
  warnings: string[];
  errors: string[];
  notes: string[];
  paths: {
    refineInput: string;
    refineOutput: string;
    refineReport: string;
    refineSummary: string;
    repoProfilesDir: string;
    repoProfilesIndex: string;
    dagBackup?: string;
    dag: string;
    acceptance: string;
  };
};

type RepoRef = { repoKey: string; repoRel: string; nodeId: string };
type RepoAdapterInfo = {
  repoKey: string;
  repoRel: string;
  adapter: { id: string; confidence: number; evidence: string[] };
  outputDirRel: string;
  templates: {
    env: Record<string, string>;
    setup: string[];
    install: string[];
    train: string[];
    eval: string[];
    notes: string[];
    warnings: string[];
  };
};

function normalizeRel(p: string): string {
  return p.replaceAll("\\", "/");
}

function isTrainWrapperCommand(cmd: string): boolean {
  return cmd.includes("plan/scripts/train.run.sh");
}

function extractCommandsFromGeneratedScript(scriptText: string): string[] {
  const lines = scriptText.split(/\r?\n/);
  let i = 0;
  if (lines[i]?.startsWith("#!")) {
    i += 1;
  }
  if (lines[i]?.trim() === "set -e" || lines[i]?.trim() === "set +e") {
    i += 1;
  }
  while (i < lines.length && !lines[i]?.trim()) {
    i += 1;
  }
  while (i < lines.length && (lines[i]?.trim().startsWith("#") || !lines[i]?.trim())) {
    i += 1;
  }
  const body = lines.slice(i).join("\n").trim();
  return body ? [body] : [];
}

function extractRepoRefs(dag: PlanDag): RepoRef[] {
  const out = new Map<string, RepoRef>();
  for (const node of dag.nodes) {
    if (node.type !== "fetch_repo") {
      continue;
    }
    const repoRel = normalizeRel(node.outputs?.[0]?.trim() ?? "");
    if (!repoRel.startsWith("cache/git/")) {
      continue;
    }
    const repoKey = repoRel.slice("cache/git/".length).replace(/^\/+/, "");
    if (!repoKey) {
      continue;
    }
    out.set(repoKey, { repoKey, repoRel, nodeId: node.id });
  }
  return [...out.values()].toSorted((a, b) => a.repoKey.localeCompare(b.repoKey));
}

async function listDatasetSamples(planDir: string): Promise<
  Array<{
    label: string;
    cacheDir: string;
    discoveredJson?: string;
    sampleJson?: string;
  }>
> {
  const baseAbs = path.join(planDir, "cache", "data");
  try {
    const entries = await fs.readdir(baseAbs, { withFileTypes: true });
    const out: Array<{
      label: string;
      cacheDir: string;
      discoveredJson?: string;
      sampleJson?: string;
    }> = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) {
        continue;
      }
      const label = entry.name.trim();
      if (!label) {
        continue;
      }
      const cacheDir = `cache/data/${label}`;
      const discoveredAbs = path.join(baseAbs, label, "discovered.json");
      const sampleAbs = path.join(baseAbs, label, "sample.json");
      let discoveredJson: string | undefined;
      let sampleJson: string | undefined;
      try {
        await fs.stat(discoveredAbs);
        discoveredJson = `${cacheDir}/discovered.json`;
      } catch {
        // ignore
      }
      try {
        await fs.stat(sampleAbs);
        sampleJson = `${cacheDir}/sample.json`;
      } catch {
        // ignore
      }
      out.push({ label, cacheDir, discoveredJson, sampleJson });
    }
    out.sort((a, b) => a.label.localeCompare(b.label));
    return out;
  } catch {
    return [];
  }
}

function mergeEnv(node: PlanNode, defaults: Record<string, string>): PlanNode {
  const current = node.env ?? {};
  const merged: Record<string, string> = { ...current };
  for (const [key, value] of Object.entries(defaults)) {
    if (!(key in merged)) {
      merged[key] = value;
    }
  }
  return { ...node, env: merged };
}

function buildStandardEnvAbs(params: {
  planDir: string;
  outputDirAbs?: string;
  repoKey?: string;
}): Record<string, string> {
  const env: Record<string, string> = {
    OPENCLAW_PLAN_DIR: params.planDir,
    OPENCLAW_CACHE_DIR: path.join(params.planDir, "cache"),
    HF_HOME: path.join(params.planDir, "cache", "hf"),
    TRANSFORMERS_CACHE: path.join(params.planDir, "cache", "hf", "transformers"),
    HF_DATASETS_CACHE: path.join(params.planDir, "cache", "hf", "datasets"),
    PIP_CACHE_DIR: path.join(params.planDir, "cache", "pip"),
  };
  if (params.repoKey?.trim()) {
    const repoKey = params.repoKey.trim();
    const outputDir =
      params.outputDirAbs ?? path.join(params.planDir, "artifacts", "model", repoKey);
    env.OPENCLAW_REPO_KEY = repoKey;
    env.OPENCLAW_REPO_DIR = path.join(params.planDir, "cache", "git", repoKey);
    env.OPENCLAW_VENV_DIR = path.join(params.planDir, "cache", "venv", repoKey);
    env.OPENCLAW_LOCK_DIR = path.join(params.planDir, "plan", "locks", repoKey);
    // Treat output_dir and checkpoint_dir as the same root for v1.
    env.OPENCLAW_OUTPUT_DIR = outputDir;
    env.OPENCLAW_CHECKPOINT_DIR = outputDir;
  }
  return env;
}

function isUnsafeCommand(cmd: string): boolean {
  const lowered = cmd.toLowerCase();
  const banned = [
    "sudo ",
    "rm -rf /",
    "rm -rf /*",
    "mkfs",
    "dd if=",
    "shutdown",
    "reboot",
    "poweroff",
    "halt",
    ":(){:|:&};:",
  ];
  return banned.some((needle) => lowered.includes(needle));
}

function filterUnsafeCommands(commands: string[] | undefined): {
  ok: boolean;
  commands: string[];
  warnings: string[];
} {
  const warnings: string[] = [];
  const list = (commands ?? []).map((c) => c.trim()).filter(Boolean);
  for (const cmd of list) {
    if (isUnsafeCommand(cmd)) {
      warnings.push(`Blocked unsafe command: ${cmd.slice(0, 120)}`);
      return { ok: false, commands: [], warnings };
    }
  }
  return { ok: true, commands: list, warnings };
}

function renderRefineSummary(params: {
  planId?: string;
  notes: string[];
  warnings: string[];
  errors: string[];
  updatedNodes: Array<{ id: string; tool: string; commandCount: number }>;
  needsConfirm?: Array<{ id: string; message: string; severity?: string }>;
}): string {
  const lines: string[] = [];
  lines.push("# Proposal Refine Summary");
  lines.push("");
  if (params.planId) {
    lines.push(`Plan: \`${params.planId}\``);
    lines.push("");
  }
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
  const needsConfirm = params.needsConfirm ?? [];
  if (needsConfirm.length > 0) {
    lines.push("## Needs confirm");
    lines.push("");
    for (const item of needsConfirm) {
      const severity = item.severity ? ` (${item.severity})` : "";
      lines.push(`- \`${item.id}\`${severity}: ${item.message}`);
    }
    lines.push("");
  }
  if (params.notes.length > 0) {
    lines.push("## Notes");
    lines.push("");
    for (const n of params.notes) {
      lines.push(`- ${n}`);
    }
    lines.push("");
  }
  lines.push("## Updated nodes");
  lines.push("");
  if (params.updatedNodes.length === 0) {
    lines.push("No nodes were updated.");
    lines.push("");
    return `${lines.join("\n")}\n`;
  }
  for (const node of params.updatedNodes) {
    lines.push(`- \`${node.id}\`: tool=${node.tool} commands=${node.commandCount}`);
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

function buildRefinePrompt(params: {
  dag: PlanDag;
  planId: string;
  planDir: string;
  repoRefs: RepoRef[];
  repoProfilesIndexRel: string;
  repoAdapters: RepoAdapterInfo[];
  datasetSamples: Array<{
    label: string;
    cacheDir: string;
    discoveredJson?: string;
    sampleJson?: string;
  }>;
  instructions?: string;
}): string {
  const hasReproPack =
    params.dag.nodes.some((n) => n.id === "setup.venv") &&
    params.dag.nodes.some((n) => n.id === "install.deps");
  const executableNodes = hasReproPack
    ? "setup.venv, install.deps, train.run, eval.run, and report.write"
    : "train.run, eval.run, and report.write";

  const promptPayload = {
    planId: params.planId,
    planDir: params.planDir,
    repos: params.repoRefs.map((r) => ({ repoKey: r.repoKey, repoRel: r.repoRel })),
    repoProfilesFile: params.repoProfilesIndexRel,
    conventions: {
      venvDir: "cache/venv/<repoKey>",
      pipFreezeLock: "plan/locks/<repoKey>/pip-freeze.txt",
      outputDir: "artifacts/model/<repoKey>",
      checkpointManifest: "report/checkpoint_manifest.json",
      evalMetrics: "report/eval_metrics.json",
      finalMetrics: "report/final_metrics.json",
      finalReport: "report/final_report.md",
      scriptsDir: "plan/scripts/<nodeId>.sh",
    },
    adaptersByRepoKey: Object.fromEntries(
      params.repoAdapters.map((r) => [
        r.repoKey,
        {
          repoRel: r.repoRel,
          adapter: r.adapter,
          outputDirRel: r.outputDirRel,
          templates: r.templates,
        },
      ]),
    ),
    datasetSamples: params.datasetSamples,
    inputDag: params.dag,
    outputSchema: {
      schemaVersion: 1,
      selectedRepoKey: "<repoKey from repos[]>",
      sessionId: "<optional: opencode session id>",
      dag: params.dag,
      acceptance: {
        checks: [
          {
            id: "acc.1",
            type: "artifact_exists",
            selector: "report/final_metrics.json",
            needs_confirm: false,
          },
        ],
      },
      needsConfirm: [{ id: "confirm.1", message: "<what to confirm>", severity: "warn" }],
      notes: ["..."],
      warnings: ["..."],
    },
  };

  const instructions = (params.instructions ?? "").trim();
  return (
    `You are generating an executable experiment plan for OpenClaw.\n` +
    `Return ONLY valid JSON matching this TypeScript-like schema:\n` +
    `{\n` +
    `  "schemaVersion": 1,\n` +
    `  "selectedRepoKey"?: string,\n` +
    `  "sessionId"?: string,\n` +
    `  "dag": {\n` +
    `    "nodes": Array<{ id: string; type: string; tool: "shell"|"manual"|"gateway_rpc"; inputs: string[]; outputs: string[]; commands?: string[]; env?: Record<string,string>; resources?: unknown; retryPolicyId?: string; title?: string; description?: string; }>,\n` +
    `    "edges": Array<{ from: string; to: string; reason?: string }>\n` +
    `  },\n` +
    `  "acceptance"?: { checks: Array<{ type: string; selector: string; op?: string; value?: number|string; needs_confirm?: boolean; description?: string; evidence?: string[]; }> },\n` +
    `  "needsConfirm"?: Array<{ id: string; message: string; severity?: "warn"|"error" }>,\n` +
    `  "notes"?: string[],\n` +
    `  "warnings"?: string[]\n` +
    `}\n\n` +
    `Rules:\n` +
    `- Do NOT add or remove nodes. Output dag.nodes with the exact same node ids as inputDag.\n` +
    `- Do NOT add or remove edges. Output dag.edges with the exact same (from,to) pairs as inputDag.\n` +
    (instructions ? `- Follow these additional experiment instructions:\n${instructions}\n` : "") +
    `- Make ${executableNodes} executable.\n` +
    `  - tool="shell"\n` +
    `  - commands is a non-empty string[]\n` +
    `- If multiple repos exist, set selectedRepoKey and ensure train.run.inputs[0] and eval.run.inputs[0] equal cache/git/<selectedRepoKey>.\n` +
    (hasReproPack
      ? `- Ensure setup.venv and install.deps also use cache/git/<selectedRepoKey> as inputs[0].\n` +
        `- Use these standard outputs for reproducibility:\n` +
        `  - setup.venv.outputs includes cache/venv/<selectedRepoKey>, cache/hf, and cache/pip\n` +
        `  - install.deps.outputs includes plan/locks/<selectedRepoKey>/pip-freeze.txt\n` +
        `  - train.run.outputs includes artifacts/model/<selectedRepoKey>, report/train_metrics.jsonl, and report/checkpoint_manifest.json\n` +
        `  - eval.run.outputs includes report/eval_metrics.json\n` +
        `  - report.write.outputs includes report/final_metrics.json and report/final_report.md\n`
      : "") +
    `- Train/eval nodes run with cwd set to the repo workdir. If you write to plan package paths (report/, artifacts/), prefer using $OPENCLAW_PLAN_DIR (see adapter templates) to target the correct plan root.\n` +
    `- Training must save checkpoints under $OPENCLAW_OUTPUT_DIR (or $OPENCLAW_CHECKPOINT_DIR) and support resume when checkpoints exist.\n` +
    `- Commands run via "sh -lc" and MUST be non-interactive.\n` +
    `- Use plan-relative paths. Use the outputs specified on each node.\n` +
    `- Do not output destructive commands (no sudo, mkfs, rm -rf /, etc).\n` +
    `- If you cannot confidently generate commands, include a needsConfirm item explaining what is missing.\n\n` +
    `Context (input dag + attached repo profiles):\n` +
    JSON.stringify(promptPayload, null, 2)
  );
}

function ensureRepoInput(node: PlanNode, repoRel: string): PlanNode {
  const normalized = repoRel.replaceAll("\\", "/");
  const existing = node.inputs ?? [];
  if (existing.length > 0 && existing[0]?.replaceAll("\\", "/") === normalized) {
    return node;
  }
  return {
    ...node,
    inputs: [
      normalized,
      ...existing.filter((p) => p.trim() && p.replaceAll("\\", "/") !== normalized),
    ],
  };
}

function renderGenericReportCommands(): string[] {
  return [
    "python3 - <<'PY'",
    "import datetime, json, pathlib",
    "plan_dir = pathlib.Path('.').resolve()",
    "eval_path = plan_dir / 'report' / 'eval_metrics.json'",
    "final_metrics_path = plan_dir / 'report' / 'final_metrics.json'",
    "final_report_path = plan_dir / 'report' / 'final_report.md'",
    "metrics = {}",
    "notes = []",
    "if eval_path.exists():",
    "  try:",
    "    raw = json.loads(eval_path.read_text(encoding='utf-8'))",
    "    candidate = raw.get('metrics') if isinstance(raw, dict) and isinstance(raw.get('metrics'), dict) else raw",
    "    if isinstance(candidate, dict):",
    "      for k, v in candidate.items():",
    "        if isinstance(v, (int, float, str)):",
    "          metrics[str(k)] = v",
    "        else:",
    "          notes.append(f'ignored non-scalar metric: {k}')",
    "    else:",
    "      notes.append('eval_metrics.json is not an object')",
    "  except Exception as e:",
    "    notes.append(f'failed to parse eval_metrics.json: {e}')",
    "else:",
    "  notes.append('missing report/eval_metrics.json')",
    "payload = {",
    "  'schemaVersion': 1,",
    "  'createdAt': datetime.datetime.utcnow().isoformat() + 'Z',",
    "  'metrics': metrics,",
    "  'notes': notes,",
    "}",
    "final_metrics_path.parent.mkdir(parents=True, exist_ok=True)",
    "final_metrics_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\\n', encoding='utf-8')",
    "lines = ['# Final Report', '', f'- createdAt: {payload[\"createdAt\"]}', '']",
    "if metrics:",
    "  lines.append('## Metrics')",
    "  lines.append('')",
    "  for k in sorted(metrics.keys()):",
    "    lines.append(f'- {k}: {metrics[k]}')",
    "  lines.append('')",
    "if notes:",
    "  lines.append('## Notes')",
    "  lines.append('')",
    "  for n in notes:",
    "    lines.append(f'- {n}')",
    "  lines.append('')",
    "final_report_path.parent.mkdir(parents=True, exist_ok=True)",
    "final_report_path.write_text('\\n'.join(lines) + '\\n', encoding='utf-8')",
    "print(str(final_metrics_path))",
    "print(str(final_report_path))",
    "PY",
  ];
}

function renderTrainWrapperScript(params: { planId: string }): string {
  const lines: string[] = [];
  lines.push("#!/usr/bin/env sh");
  // Avoid set -e so we can always emit the manifest even on failure.
  lines.push("set +e");
  lines.push("");
  lines.push("# Generated by OpenClaw proposal refine.");
  lines.push(`# Plan: ${params.planId}`);
  lines.push("# Node: train.run");
  lines.push("# Purpose: ensure checkpoint manifest is emitted (required for GPU pause/resume).");
  lines.push("");
  lines.push('PLAN_DIR="${OPENCLAW_PLAN_DIR:-}"');
  lines.push('if [ -z "$PLAN_DIR" ]; then');
  lines.push('  echo "OPENCLAW_PLAN_DIR is missing" >&2');
  lines.push("  exit 2");
  lines.push("fi");
  lines.push("");
  lines.push('INNER="$PLAN_DIR/plan/scripts/train.run.inner.sh"');
  lines.push('MANIFEST="$PLAN_DIR/report/checkpoint_manifest.json"');
  lines.push('mkdir -p "$PLAN_DIR/report" 2>/dev/null || true');
  lines.push("");
  lines.push("write_manifest() {");
  lines.push('  REASON="${1:-exit}"');
  lines.push('  EXIT_CODE="${2:-0}"');
  lines.push('  export OPENCLAW_MANIFEST_REASON="$REASON"');
  lines.push('  export OPENCLAW_MANIFEST_EXIT_CODE="$EXIT_CODE"');
  lines.push("  PY=python3");
  lines.push('  command -v "$PY" >/dev/null 2>&1 || PY=python');
  lines.push("  \"$PY\" - <<'PY'");
  lines.push("import datetime, json, os, pathlib");
  lines.push("plan_dir_raw = os.environ.get('OPENCLAW_PLAN_DIR', '').strip()");
  lines.push(
    "plan_dir = pathlib.Path(plan_dir_raw).resolve() if plan_dir_raw else pathlib.Path('.').resolve()",
  );
  lines.push("manifest_path = plan_dir / 'report' / 'checkpoint_manifest.json'");
  lines.push(
    "checkpoint_dir_raw = (os.environ.get('OPENCLAW_CHECKPOINT_DIR') or os.environ.get('OPENCLAW_OUTPUT_DIR') or '').strip()",
  );
  lines.push(
    "checkpoint_dir = pathlib.Path(checkpoint_dir_raw).resolve() if checkpoint_dir_raw else None",
  );
  lines.push("repo_key = (os.environ.get('OPENCLAW_REPO_KEY') or '').strip() or None");
  lines.push("reason = (os.environ.get('OPENCLAW_MANIFEST_REASON') or 'exit').strip() or 'exit'");
  lines.push("exit_code_raw = os.environ.get('OPENCLAW_MANIFEST_EXIT_CODE')");
  lines.push("try:");
  lines.push("  exit_code = int(exit_code_raw) if exit_code_raw is not None else None");
  lines.push("except Exception:");
  lines.push("  exit_code = None");
  lines.push("notes = []");
  lines.push("checkpoint_found = False");
  lines.push("latest_hint = None");
  lines.push("try:");
  lines.push("  if checkpoint_dir and checkpoint_dir.exists():");
  lines.push("    try:");
  lines.push("      items = list(checkpoint_dir.iterdir())");
  lines.push("      checkpoint_found = len(items) > 0");
  lines.push("    except Exception as e:");
  lines.push("      notes.append('failed to list checkpoint dir: ' + str(e))");
  lines.push("    candidates = []");
  lines.push("    try:");
  lines.push("      for p in checkpoint_dir.glob('checkpoint-*'):");
  lines.push("        try:");
  lines.push("          candidates.append((p.stat().st_mtime, p))");
  lines.push("        except Exception:");
  lines.push("          pass");
  lines.push("    except Exception:");
  lines.push("      pass");
  lines.push("    if not candidates:");
  lines.push("      try:");
  lines.push("        for p in checkpoint_dir.iterdir():");
  lines.push("          try:");
  lines.push("            candidates.append((p.stat().st_mtime, p))");
  lines.push("          except Exception:");
  lines.push("            pass");
  lines.push("      except Exception:");
  lines.push("        pass");
  lines.push("    if candidates:");
  lines.push("      candidates.sort(key=lambda t: t[0])");
  lines.push("      latest = candidates[-1][1]");
  lines.push("      try:");
  lines.push("        latest_hint = str(latest.relative_to(plan_dir))");
  lines.push("      except Exception:");
  lines.push("        latest_hint = str(latest)");
  lines.push("except Exception as e:");
  lines.push("  notes.append('checkpoint detection failed: ' + str(e))");
  lines.push("payload = {");
  lines.push("  'schemaVersion': 1,");
  lines.push("  'createdAt': datetime.datetime.utcnow().isoformat() + 'Z',");
  lines.push("  'repoKey': repo_key,");
  lines.push("  'checkpointDir': str(checkpoint_dir) if checkpoint_dir else checkpoint_dir_raw,");
  lines.push("  'checkpointFound': bool(checkpoint_found),");
  lines.push("  'latestHint': latest_hint,");
  lines.push("  'reason': reason,");
  lines.push("  'exitCode': exit_code,");
  lines.push("  'notes': notes,");
  lines.push("}");
  lines.push("manifest_path.parent.mkdir(parents=True, exist_ok=True)");
  lines.push("manifest_path.write_text(json.dumps(payload, indent=2) + '\\n', encoding='utf-8')");
  lines.push("PY");
  lines.push("}");
  lines.push("");
  lines.push('CHILD_PID=""');
  lines.push("on_term() {");
  lines.push('  if [ -n "$CHILD_PID" ]; then');
  lines.push('    kill -TERM "$CHILD_PID" 2>/dev/null || true');
  lines.push("    i=0");
  lines.push('    while kill -0 "$CHILD_PID" 2>/dev/null && [ "$i" -lt 5 ]; do');
  lines.push("      sleep 1");
  lines.push("      i=$((i+1))");
  lines.push("    done");
  lines.push('    kill -KILL "$CHILD_PID" 2>/dev/null || true');
  lines.push("  fi");
  lines.push('  write_manifest "signal" "143"');
  lines.push("  exit 143");
  lines.push("}");
  lines.push("trap 'on_term' TERM INT");
  lines.push("");
  lines.push('if [ ! -f "$INNER" ]; then');
  lines.push('  echo "Missing inner train script: $INNER" >&2');
  lines.push('  write_manifest "error:missing_inner" "2"');
  lines.push("  exit 2");
  lines.push("fi");
  lines.push("");
  lines.push('sh "$INNER" &');
  lines.push("CHILD_PID=$!");
  lines.push('wait "$CHILD_PID"');
  lines.push("CODE=$?");
  lines.push('write_manifest "exit" "$CODE"');
  lines.push('exit "$CODE"');
  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function writeBackupDagIfMissing(planDir: string): Promise<string | undefined> {
  const dag = path.join(planDir, "plan", "plan.dag.json");
  const backup = path.join(planDir, "plan", "plan.dag.skeleton.json");
  try {
    await fs.stat(backup);
    return undefined;
  } catch {
    // continue
  }
  const raw = await fs.readFile(dag, "utf-8");
  await fs.writeFile(backup, raw, "utf-8");
  return backup;
}

export async function refineProposalPlan(params: {
  planDir: string;
  opts: ProposalRefineOpts;
  deps?: ProposalRefineDeps;
}): Promise<ProposalRefineResult> {
  const startedAt = new Date().toISOString();
  const planDir = path.resolve(params.planDir);
  const warnings: string[] = [];
  const errors: string[] = [];
  const notes: string[] = [];

  const paths = {
    refineInput: path.join(planDir, "ir", "refine_input.json"),
    refineOutput: path.join(planDir, "ir", "refine_output.json"),
    refineReport: path.join(planDir, "report", "refine_report.json"),
    refineSummary: path.join(planDir, "report", "refine_summary.md"),
    repoProfilesDir: path.join(planDir, "ir", "repo_profiles"),
    repoProfilesIndex: path.join(planDir, "ir", "repo_profiles", "index.json"),
    dag: path.join(planDir, "plan", "plan.dag.json"),
    acceptance: path.join(planDir, "plan", "acceptance.json"),
    dagBackup: undefined as string | undefined,
  };

  const validation = await validatePlanDir(planDir);
  const planId = validation.ok ? validation.data?.report.planId : undefined;
  if (!validation.ok || !validation.data || !planId) {
    errors.push(...validation.errors);
    warnings.push(...validation.warnings);
    const finishedAt = new Date().toISOString();
    const out: ProposalRefineResult = {
      ok: false,
      planDir,
      planId,
      startedAt,
      finishedAt,
      warnings,
      errors,
      notes,
      paths,
    };
    await fs.mkdir(path.join(planDir, "report"), { recursive: true });
    await writeJsonFile(paths.refineReport, out);
    await writeTextFile(
      paths.refineSummary,
      renderRefineSummary({ planId, notes, warnings, errors, updatedNodes: [], needsConfirm: [] }),
    );
    return out;
  }

  const dag = validation.data.dag;
  const dagForPrompt: PlanDag = JSON.parse(JSON.stringify(dag)) as PlanDag;
  let previousTrainInnerCommands: string[] | null = null;
  {
    const train = dagForPrompt.nodes.find((n) => n.id === "train.run");
    const commands = (train?.commands ?? []).filter(Boolean);
    if (train && commands.some(isTrainWrapperCommand)) {
      const innerPath = path.join(planDir, "plan", "scripts", "train.run.inner.sh");
      try {
        const innerRaw = await fs.readFile(innerPath, "utf-8");
        const extracted = extractCommandsFromGeneratedScript(innerRaw);
        if (extracted.length > 0) {
          previousTrainInnerCommands = extracted;
          train.commands = extracted;
        } else {
          warnings.push(
            "train.run appears wrapped but plan/scripts/train.run.inner.sh is empty; refine may be degraded.",
          );
        }
      } catch {
        warnings.push(
          "train.run appears wrapped but plan/scripts/train.run.inner.sh is missing; refine may be degraded.",
        );
      }
    }
  }

  const repoRefs = extractRepoRefs(dag);
  if (repoRefs.length === 0) {
    warnings.push("No fetch_repo outputs found; repo profiling will be skipped.");
  }

  await fs.mkdir(paths.repoProfilesDir, { recursive: true });
  const repoProfiles: RepoProfile[] = [];
  for (const ref of repoRefs) {
    try {
      const profile = await profileRepo({
        planDir,
        repoRel: ref.repoRel,
        repoKey: ref.repoKey,
        runHostCommand: params.deps?.runCommand,
      });
      repoProfiles.push(profile);
      await writeJsonFile(path.join(paths.repoProfilesDir, `${ref.repoKey}.json`), profile);
    } catch (err) {
      warnings.push(`Repo profiling failed for ${ref.repoKey}: ${String(err)}`);
    }
  }
  await writeJsonFile(paths.repoProfilesIndex, {
    schemaVersion: 1,
    createdAt: new Date().toISOString(),
    repos: repoProfiles,
  });

  const trainNode = dag.nodes.find((n) => n.id === "train.run");
  const gpuCount =
    typeof trainNode?.resources?.gpuCount === "number" &&
    Number.isFinite(trainNode.resources.gpuCount)
      ? Math.max(1, Math.floor(trainNode.resources.gpuCount))
      : undefined;

  const repoAdapters: RepoAdapterInfo[] = [];
  for (const ref of repoRefs) {
    const profile = repoProfiles.find((p) => p.repoKey === ref.repoKey);
    if (!profile) {
      continue;
    }
    const adapter = pickBestAdapter(profile);
    const outputDirRel = `artifacts/model/${ref.repoKey}`;
    const templates = getAdapter(adapter.id).buildTemplates({
      planDir,
      repoKey: ref.repoKey,
      repoRel: ref.repoRel,
      profile,
      outputDirRel,
      gpuCount,
    });
    repoAdapters.push({
      repoKey: ref.repoKey,
      repoRel: ref.repoRel,
      adapter,
      outputDirRel,
      templates,
    });
  }

  await fs.mkdir(path.join(planDir, "ir"), { recursive: true });
  const repoProfilesIndexRel = normalizeRel(path.relative(planDir, paths.repoProfilesIndex));
  const datasetSamples = await listDatasetSamples(planDir);

  const prompt = buildRefinePrompt({
    dag: dagForPrompt,
    planId,
    planDir,
    repoRefs,
    repoProfilesIndexRel,
    repoAdapters,
    datasetSamples,
    instructions: params.opts.instructions,
  });

  await writeJsonFile(paths.refineInput, {
    schemaVersion: 1,
    createdAt: new Date().toISOString(),
    planId,
    planDir,
    repoRefs: repoRefs.map((r) => ({ repoKey: r.repoKey, repoRel: r.repoRel })),
    repoProfilesIndex: repoProfilesIndexRel,
    instructions: (params.opts.instructions ?? "").trim() || undefined,
    promptHint: "Use opencode run --format json with this prompt.",
  });

  const model = params.opts.model?.trim() || "opencode/kimi-k2.5-free";
  const timeoutMs = Math.max(10_000, params.opts.timeoutMs ?? 180_000);

  const requestedAgent = (params.opts.agent ?? "").trim();
  const agent = "openclaw-refine";
  if (requestedAgent && requestedAgent !== agent) {
    warnings.push(`Ignoring opencode agent "${requestedAgent}"; using "${agent}" (read-only).`);
  }

  const refineRes = await completeJsonWithSchemaViaOpencode({
    schema: ProposalRefineOutputSchemaV1,
    prompt,
    model,
    agent,
    files: [paths.repoProfilesIndex],
    cwd: planDir,
    timeoutMs,
    opencodeConfigDir: params.deps?.opencodeConfigDir,
    attempts: 2,
    deps: params.deps?.runCommand ? { runCommand: params.deps.runCommand } : undefined,
  });
  warnings.push(...refineRes.warnings);

  let refined: ProposalRefineOutputV1 | null = null;
  let opencodeSessionId: string | undefined = refineRes.sessionId;
  let needsConfirm: Array<{ id: string; message: string; severity?: string }> = [];
  if (!refineRes.ok) {
    errors.push(`OpenCode refine failed: ${refineRes.error}`);
    await writeJsonFile(paths.refineOutput, {
      ok: false,
      error: refineRes.error,
      raw: refineRes.raw,
      warnings: refineRes.warnings,
      sessionId: refineRes.sessionId,
    });
  } else {
    refined = refineRes.value;
    notes.push(...(refined.notes ?? []));
    warnings.push(...(refined.warnings ?? []));
    needsConfirm = refined.needsConfirm ?? [];
    if (needsConfirm.length > 0) {
      warnings.push(`Refine output includes ${needsConfirm.length} needsConfirm item(s).`);
    }
    opencodeSessionId ||= refined.sessionId;
    await writeJsonFile(paths.refineOutput, {
      ok: true,
      output: refined,
      raw: refineRes.raw,
      warnings: refineRes.warnings,
      sessionId: refineRes.sessionId,
    });
  }

  const updatedNodes: Array<{ id: string; tool: string; commandCount: number }> = [];
  let refinedDag: PlanDag | null = null;
  let selectedRepoKey: string | undefined;
  let trainInnerCommands: string[] | null = null;

  if (refined) {
    const refinedDagOut = refined.dag;
    refinedDag = refinedDagOut;

    // Enforce node id parity (no adds/removes).
    const inputIds = dag.nodes.map((n) => n.id).toSorted();
    const outputIds = refinedDagOut.nodes.map((n) => n.id).toSorted();
    if (inputIds.length !== outputIds.length || inputIds.some((id, idx) => id !== outputIds[idx])) {
      errors.push("Refine output must preserve the exact set of node ids (no adds/removes).");
    }

    // Enforce edge parity on (from,to) pairs.
    const inputEdges = new Set(dag.edges.map((e) => `${e.from}→${e.to}`));
    const outputEdges = new Set(refinedDagOut.edges.map((e) => `${e.from}→${e.to}`));
    for (const edge of inputEdges) {
      if (!outputEdges.has(edge)) {
        errors.push(`Refine output is missing edge ${edge}.`);
      }
    }
    for (const edge of outputEdges) {
      if (!inputEdges.has(edge)) {
        errors.push(`Refine output added unexpected edge ${edge}.`);
      }
    }

    const repoRelByKey = new Map<string, string>();
    for (const ref of repoRefs) {
      repoRelByKey.set(ref.repoKey, ref.repoRel);
    }

    selectedRepoKey = refined.selectedRepoKey?.trim() || undefined;
    if (!selectedRepoKey) {
      const train = refinedDagOut.nodes.find((n) => n.id === "train.run");
      const repoRel = normalizeRel(train?.inputs?.[0]?.trim() ?? "");
      if (repoRel.startsWith("cache/git/")) {
        selectedRepoKey = repoRel.slice("cache/git/".length).replace(/^\/+/, "") || undefined;
      } else if (repoRefs.length === 1) {
        selectedRepoKey = repoRefs[0]?.repoKey;
      }
    }

    if (selectedRepoKey && !repoRelByKey.has(selectedRepoKey)) {
      errors.push(`selectedRepoKey "${selectedRepoKey}" was not found in fetch_repo outputs.`);
    }

    const adapterInfo = selectedRepoKey
      ? repoAdapters.find((r) => r.repoKey === selectedRepoKey)
      : repoRefs.length === 1
        ? repoAdapters.find((r) => r.repoKey === repoRefs[0]?.repoKey)
        : undefined;
    if (adapterInfo?.templates?.warnings?.length) {
      warnings.push(...adapterInfo.templates.warnings);
    }
    if (adapterInfo?.templates?.notes?.length) {
      notes.push(...adapterInfo.templates.notes);
    }

    const enforceNode = (id: string): PlanNode | undefined =>
      refinedDagOut.nodes.find((n) => n.id === id);

    const hasSetup = dag.nodes.some((n) => n.id === "setup.venv");
    const hasInstall = dag.nodes.some((n) => n.id === "install.deps");
    const requireReproPack = hasSetup && hasInstall;
    if (hasSetup !== hasInstall) {
      errors.push("Plan must include both setup.venv and install.deps nodes (or neither).");
    }

    const setup = requireReproPack ? enforceNode("setup.venv") : undefined;
    const install = requireReproPack ? enforceNode("install.deps") : undefined;
    const train = enforceNode("train.run");
    const evalNode = enforceNode("eval.run");
    const report = enforceNode("report.write");

    if (requireReproPack && !setup) {
      errors.push("Refine output missing node setup.venv");
    }
    if (requireReproPack && !install) {
      errors.push("Refine output missing node install.deps");
    }
    if (!train) {
      errors.push("Refine output missing node train.run");
    }
    if (!evalNode) {
      errors.push("Refine output missing node eval.run");
    }
    if (!report) {
      errors.push("Refine output missing node report.write");
    }

    const requiredEvalOutputs = new Set(["report/eval_metrics.json"]);
    const requiredReportOutputs = new Set(["report/final_metrics.json", "report/final_report.md"]);

    const ensureOutputs = (node: PlanNode, required: Set<string>) => {
      const outs = new Set((node.outputs ?? []).map((p) => normalizeRel(p)));
      for (const req of required) {
        if (!outs.has(req)) {
          errors.push(`${node.id}: missing required output ${req}`);
        }
      }
    };

    if (requireReproPack && selectedRepoKey) {
      const requiredSetupOutputs = new Set([
        `cache/venv/${selectedRepoKey}`,
        "cache/hf",
        "cache/pip",
      ]);
      const requiredInstallOutputs = new Set([`plan/locks/${selectedRepoKey}/pip-freeze.txt`]);
      const requiredTrainOutputs = new Set([
        `artifacts/model/${selectedRepoKey}`,
        "report/train_metrics.jsonl",
        "report/checkpoint_manifest.json",
      ]);

      if (setup) {
        ensureOutputs(setup, requiredSetupOutputs);
      }
      if (install) {
        ensureOutputs(install, requiredInstallOutputs);
      }
      if (train) {
        ensureOutputs(train, requiredTrainOutputs);
      }
    } else if (requireReproPack) {
      errors.push("selectedRepoKey is required to build a reproducible script pack.");
    }

    if (evalNode) {
      ensureOutputs(evalNode, requiredEvalOutputs);
    }
    if (report) {
      ensureOutputs(report, requiredReportOutputs);
    }

    const outputDirAbs = adapterInfo ? path.join(planDir, adapterInfo.outputDirRel) : undefined;
    const defaultEnv = buildStandardEnvAbs({ planDir, outputDirAbs, repoKey: selectedRepoKey });

    const fillMissingCommands = (node: PlanNode | undefined, template: string[], label: string) => {
      if (!node || node.tool !== "shell") {
        return;
      }
      if ((node.commands ?? []).length > 0) {
        return;
      }
      if (template.length === 0) {
        return;
      }
      node.commands = template;
      warnings.push(`${node.id}: commands were missing; filled from ${label} adapter template.`);
    };

    if (adapterInfo) {
      fillMissingCommands(setup, adapterInfo.templates.setup, adapterInfo.adapter.id);
      fillMissingCommands(install, adapterInfo.templates.install, adapterInfo.adapter.id);
      fillMissingCommands(train, adapterInfo.templates.train, adapterInfo.adapter.id);
      fillMissingCommands(evalNode, adapterInfo.templates.eval, adapterInfo.adapter.id);
    }

    if (setup) {
      Object.assign(setup, mergeEnv(setup, defaultEnv));
    }
    if (install) {
      Object.assign(install, mergeEnv(install, defaultEnv));
    }
    if (train) {
      Object.assign(train, mergeEnv(train, defaultEnv));
    }
    if (evalNode) {
      Object.assign(evalNode, mergeEnv(evalNode, defaultEnv));
    }
    if (report) {
      Object.assign(
        report,
        mergeEnv(report, buildStandardEnvAbs({ planDir, repoKey: selectedRepoKey })),
      );
    }

    const ensureShellCommands = (node: PlanNode, allowFallbackReport: boolean) => {
      if (node.tool !== "shell") {
        errors.push(`${node.id}: tool must be shell`);
        return;
      }
      const cmdFilter = filterUnsafeCommands(node.commands);
      warnings.push(...cmdFilter.warnings);
      if (!cmdFilter.ok) {
        errors.push(`${node.id}: blocked unsafe command(s)`);
        return;
      }
      if (cmdFilter.commands.length === 0) {
        if (allowFallbackReport && node.id === "report.write") {
          warnings.push("report.write: commands missing; injecting generic report script.");
          node.commands = renderGenericReportCommands();
        } else {
          errors.push(`${node.id}: commands missing`);
        }
        return;
      }
      node.commands = cmdFilter.commands;
    };

    // Patch nodes in-place (safe, already parsed).
    if (setup) {
      ensureShellCommands(setup, false);
    }
    if (install) {
      ensureShellCommands(install, false);
    }
    if (train) {
      ensureShellCommands(train, false);
    }
    if (evalNode) {
      ensureShellCommands(evalNode, false);
    }
    if (report) {
      ensureShellCommands(report, true);
    }

    if (train && train.tool === "shell") {
      const current = (train.commands ?? []).map((c) => c.trim()).filter(Boolean);
      const looksWrapped = current.some(isTrainWrapperCommand);
      if (looksWrapped) {
        if (previousTrainInnerCommands && previousTrainInnerCommands.length > 0) {
          trainInnerCommands = previousTrainInnerCommands;
        } else {
          errors.push(
            "train.run: commands appear to be wrapped, but inner training commands could not be recovered.",
          );
        }
      } else {
        trainInnerCommands = current;
      }

      // Always run through a plan-local wrapper so we can reliably emit checkpoint/restore evidence.
      train.commands = ['sh "$OPENCLAW_PLAN_DIR/plan/scripts/train.run.sh"'];
    }

    if (selectedRepoKey) {
      const repoRel = repoRelByKey.get(selectedRepoKey);
      if (repoRel) {
        if (setup && setup.tool === "shell") {
          Object.assign(setup, ensureRepoInput(setup, repoRel));
        }
        if (install && install.tool === "shell") {
          Object.assign(install, ensureRepoInput(install, repoRel));
        }
        if (train && train.tool === "shell") {
          Object.assign(train, ensureRepoInput(train, repoRel));
        }
        if (evalNode && evalNode.tool === "shell") {
          Object.assign(evalNode, ensureRepoInput(evalNode, repoRel));
        }
      }
    }

    // Validate patched DAG shape.
    const dagParsed = PlanDagSchema.safeParse(refinedDag);
    if (!dagParsed.success) {
      errors.push(`Invalid refined DAG schema: ${dagParsed.error.message}`);
    } else {
      const topo = validateDag(dagParsed.data);
      if (!topo.ok) {
        errors.push(...topo.errors);
      }
    }

    // Summarize updated nodes for the report.
    const inputById = new Map<string, PlanNode>();
    for (const node of dag.nodes) {
      inputById.set(node.id, node);
    }
    for (const node of refinedDagOut.nodes) {
      const before = inputById.get(node.id);
      if (!before) {
        continue;
      }
      const beforeCmd = JSON.stringify(before.commands ?? []);
      const afterCmd = JSON.stringify(node.commands ?? []);
      if (before.tool !== node.tool || beforeCmd !== afterCmd) {
        updatedNodes.push({
          id: node.id,
          tool: node.tool,
          commandCount: (node.commands ?? []).length,
        });
      }
    }
  }

  const shouldWriteDag = !params.opts.dryRun && errors.length === 0 && refinedDag;
  if (shouldWriteDag && refinedDag) {
    paths.dagBackup = await writeBackupDagIfMissing(planDir);
    await writeJsonFile(paths.dag, refinedDag);

    if (params.opts.writeAcceptance && refined?.acceptance) {
      await writeJsonFile(paths.acceptance, refined.acceptance);
    }

    const scriptsDir = path.join(planDir, "plan", "scripts");
    await fs.mkdir(scriptsDir, { recursive: true });

    const headerFor = (nodeId: string, setE: boolean) =>
      [
        "#!/usr/bin/env sh",
        setE ? "set -e" : "set +e",
        "",
        "# Generated by OpenClaw proposal refine.",
        `# Plan: ${planId}`,
        `# Node: ${nodeId}`,
        "",
      ].join("\n");

    const trainInner = (trainInnerCommands ?? []).filter(Boolean);
    if (trainInner.length > 0) {
      const innerPath = path.join(scriptsDir, "train.run.inner.sh");
      const innerBody = `${headerFor("train.run.inner", true)}${trainInner.join("\n")}\n`;
      await fs.writeFile(innerPath, innerBody, { encoding: "utf-8", mode: 0o755 });
      await fs.chmod(innerPath, 0o755);

      const wrapperPath = path.join(scriptsDir, "train.run.sh");
      await fs.writeFile(wrapperPath, renderTrainWrapperScript({ planId }), {
        encoding: "utf-8",
        mode: 0o755,
      });
      await fs.chmod(wrapperPath, 0o755);
    } else {
      warnings.push("train.run: inner commands unavailable; wrapper scripts were not generated.");
    }

    const scriptNodeIds = ["setup.venv", "install.deps", "eval.run", "report.write"];
    for (const nodeId of scriptNodeIds) {
      const node = refinedDag.nodes.find((n) => n.id === nodeId);
      const commands = node?.tool === "shell" ? (node.commands ?? []).filter(Boolean) : [];
      if (!node || commands.length === 0) {
        continue;
      }

      const scriptPath = path.join(scriptsDir, `${nodeId}.sh`);
      const body = `${headerFor(nodeId, true)}${commands.join("\n")}\n`;
      await fs.writeFile(scriptPath, body, { encoding: "utf-8", mode: 0o755 });
      await fs.chmod(scriptPath, 0o755);
    }

    const layout: PlanLayout = {
      planId,
      rootDir: planDir,
      inputDir: path.join(planDir, "input"),
      irDir: path.join(planDir, "ir"),
      planDir: path.join(planDir, "plan"),
      reportDir: path.join(planDir, "report"),
      cacheDir: path.join(planDir, "cache"),
    };
    await writeTextFile(
      path.join(planDir, "plan", "runbook.md"),
      renderPlanRunbookMd({ layout, report: validation.data.report, dag: refinedDag }),
    );
  }

  const finishedAt = new Date().toISOString();
  const ok = errors.length === 0;
  const out: ProposalRefineResult = {
    ok,
    planDir,
    planId,
    startedAt,
    finishedAt,
    warnings,
    errors,
    notes,
    paths: { ...paths, dagBackup: paths.dagBackup },
  };

  await fs.mkdir(path.join(planDir, "report"), { recursive: true });
  await writeJsonFile(paths.refineReport, {
    ok: out.ok,
    planId: out.planId,
    planDir: out.planDir,
    startedAt: out.startedAt,
    finishedAt: out.finishedAt,
    sessionId: opencodeSessionId,
    selectedRepoKey,
    warnings: out.warnings,
    errors: out.errors,
    notes: out.notes,
    needsConfirm,
    updatedNodes,
    paths: out.paths,
  });
  await writeTextFile(
    paths.refineSummary,
    renderRefineSummary({ planId, notes, warnings, errors, updatedNodes, needsConfirm }),
  );

  return out;
}
