import fs from "node:fs/promises";
import path from "node:path";
import { z } from "zod";
import type { CommandOptions, SpawnResult } from "../process/exec.js";
import type { RepoProfile } from "./adapters/schema.js";
import { profileRepo } from "./adapters/profile.js";
import { writeJsonFile, writeTextFile } from "./files.js";
import { completeJsonWithSchemaViaOpencode } from "./opencode/run.js";
import { PlanToolSchema, ResourceSpecSchema, type PlanDag, type PlanNode } from "./schema.js";
import { validatePlanDir } from "./validate.js";

type RunCommandLike = (argv: string[], options: CommandOptions) => Promise<SpawnResult>;

export const ProposalRefineNodeUpdateSchema = z
  .object({
    id: z.string().min(1),
    tool: PlanToolSchema.optional(),
    commands: z.array(z.string()).optional(),
    env: z.record(z.string(), z.string()).optional(),
    inputs: z.array(z.string()).optional(),
    outputs: z.array(z.string()).optional(),
    resources: ResourceSpecSchema.optional(),
    description: z.string().optional(),
  })
  .strip();
export type ProposalRefineNodeUpdate = z.infer<typeof ProposalRefineNodeUpdateSchema>;

export const ProposalRefineOutputSchema = z
  .object({
    schemaVersion: z.literal(1),
    nodeUpdates: z.array(ProposalRefineNodeUpdateSchema).default([]),
    notes: z.array(z.string()).default([]),
  })
  .strip();
export type ProposalRefineOutput = z.infer<typeof ProposalRefineOutputSchema>;

export type ProposalRefineOpts = {
  model?: string;
  agent?: string;
  timeoutMs?: number;
  dryRun?: boolean;
};

export type ProposalRefineDeps = {
  runCommand?: RunCommandLike;
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
    repoProfiles: string;
    dagBackup?: string;
    dag: string;
  };
};

function resolvePrimaryRepo(dag: PlanDag): { repoRel: string; repoKey: string } | null {
  const fetchRepo = dag.nodes.find((n) => n.type === "fetch_repo");
  const repoRel = fetchRepo?.outputs?.[0]?.trim().replaceAll("\\", "/") ?? "";
  if (!repoRel.startsWith("cache/git/")) {
    return null;
  }
  const repoKey = repoRel.slice("cache/git/".length).replace(/^\/+/, "");
  if (!repoKey) {
    return null;
  }
  return { repoRel, repoKey };
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
  repoProfiles: RepoProfile[];
  planDir: string;
}): string {
  const nodesOfInterest = params.dag.nodes
    .filter((n) => n.id === "train.run" || n.id === "eval.run" || n.id === "report.write")
    .map((n) => ({
      id: n.id,
      type: n.type,
      tool: n.tool,
      inputs: n.inputs,
      outputs: n.outputs,
      resources: n.resources,
      description: n.description,
    }));

  const promptPayload = {
    planId: params.planId,
    planDir: params.planDir,
    nodes: nodesOfInterest,
    repos: params.repoProfiles,
    outputSchema: {
      schemaVersion: 1,
      nodeUpdates: [
        {
          id: "train.run|eval.run|report.write",
          tool: "shell",
          commands: ["<shell command line>", "..."],
          env: { KEY: "VALUE" },
          inputs: ["<plan-relative paths>"],
          outputs: ["<plan-relative paths>"],
          resources: { gpuCount: 1 },
        },
      ],
    },
  };

  return (
    `You are generating an executable experiment plan for OpenClaw.\n` +
    `Return ONLY valid JSON matching this TypeScript-like schema:\n` +
    `{\n` +
    `  "schemaVersion": 1,\n` +
    `  "nodeUpdates": Array<{\n` +
    `    "id": string,\n` +
    `    "tool"?: "shell" | "manual",\n` +
    `    "commands"?: string[],\n` +
    `    "env"?: Record<string,string>,\n` +
    `    "inputs"?: string[],\n` +
    `    "outputs"?: string[],\n` +
    `    "resources"?: { gpuCount?: number; cpuCores?: number; ramGB?: number; gpuType?: string; gpuMemGB?: number; },\n` +
    `    "description"?: string\n` +
    `  }>,\n` +
    `  "notes"?: string[]\n` +
    `}\n\n` +
    `Rules:\n` +
    `- Only update existing nodes by id. Do not add new nodes.\n` +
    `- Prefer tool="shell" with explicit commands for train.run and eval.run.\n` +
    `- Do not output destructive commands (no sudo, mkfs, rm -rf /, etc).\n` +
    `- Commands run via "sh -lc" and MUST be non-interactive.\n` +
    `- Use the plan output paths EXACTLY as specified on each node.\n` +
    `- If you cannot confidently generate commands, set tool="manual" and explain in notes.\n\n` +
    `Context (repo profiles + nodes):\n` +
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
    repoProfiles: path.join(planDir, "ir", "repo_profiles.json"),
    dag: path.join(planDir, "plan", "plan.dag.json"),
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
      renderRefineSummary({ planId, notes, warnings, errors, updatedNodes: [] }),
    );
    return out;
  }

  const dag = validation.data.dag;
  const repo = resolvePrimaryRepo(dag);
  if (!repo) {
    warnings.push("No fetch_repo output found; cannot infer primary repo.");
  }

  const repoProfiles: RepoProfile[] = [];
  if (repo) {
    try {
      repoProfiles.push(
        await profileRepo({
          planDir,
          repoRel: repo.repoRel,
          repoKey: repo.repoKey,
          runHostCommand: params.deps?.runCommand,
        }),
      );
    } catch (err) {
      warnings.push(`Repo profiling failed: ${String(err)}`);
    }
  }

  await fs.mkdir(path.join(planDir, "ir"), { recursive: true });
  await writeJsonFile(paths.repoProfiles, repoProfiles);

  const prompt = buildRefinePrompt({
    dag,
    planId,
    repoProfiles,
    planDir,
  });

  const refineInput = {
    schemaVersion: 1,
    createdAt: new Date().toISOString(),
    planId,
    planDir,
    repoKey: repo?.repoKey,
    promptHint: "Use opencode run --format json with this prompt.",
  };
  await writeJsonFile(paths.refineInput, refineInput);

  const model = params.opts.model?.trim() || "opencode/kimi-k2.5-free";
  const timeoutMs = Math.max(10_000, params.opts.timeoutMs ?? 180_000);

  const refineRes = await completeJsonWithSchemaViaOpencode({
    schema: ProposalRefineOutputSchema,
    prompt,
    model,
    agent: params.opts.agent,
    files: [paths.repoProfiles],
    cwd: planDir,
    timeoutMs,
    attempts: 2,
    deps: params.deps?.runCommand ? { runCommand: params.deps.runCommand } : undefined,
  });
  warnings.push(...refineRes.warnings);

  let refined: ProposalRefineOutput | null = null;
  if (!refineRes.ok) {
    errors.push(`OpenCode refine failed: ${refineRes.error}`);
    await writeJsonFile(paths.refineOutput, {
      ok: false,
      error: refineRes.error,
      raw: refineRes.raw,
      warnings: refineRes.warnings,
    });
  } else {
    refined = refineRes.value;
    notes.push(...(refined.notes ?? []));
    await writeJsonFile(paths.refineOutput, {
      ok: true,
      output: refined,
      raw: refineRes.raw,
      warnings: refineRes.warnings,
    });
  }

  const nodeById = new Map<string, PlanNode>();
  for (const node of dag.nodes) {
    nodeById.set(node.id, node);
  }

  const updatedNodes: Array<{ id: string; tool: string; commandCount: number }> = [];
  const applyUpdate = (update: ProposalRefineNodeUpdate) => {
    const existing = nodeById.get(update.id);
    if (!existing) {
      warnings.push(`Refine output referenced unknown node: ${update.id}`);
      return;
    }

    const cmdFilter = filterUnsafeCommands(update.commands);
    warnings.push(...cmdFilter.warnings);
    if (!cmdFilter.ok) {
      nodeById.set(update.id, { ...existing, tool: "manual", commands: undefined });
      updatedNodes.push({ id: update.id, tool: "manual", commandCount: 0 });
      return;
    }

    const next: PlanNode = {
      ...existing,
      tool: update.tool ?? existing.tool,
      commands: update.commands ? cmdFilter.commands : existing.commands,
      env: update.env ?? existing.env,
      inputs: update.inputs ?? existing.inputs,
      outputs: update.outputs ?? existing.outputs,
      resources: update.resources ?? existing.resources,
      description: update.description ?? existing.description,
    };
    nodeById.set(update.id, next);
    updatedNodes.push({
      id: update.id,
      tool: next.tool,
      commandCount: (next.commands ?? []).length,
    });
  };

  for (const update of refined?.nodeUpdates ?? []) {
    applyUpdate(update);
  }

  // Enforce: report.write produces final artifacts even if other nodes stay manual.
  const reportNode = nodeById.get("report.write");
  if (reportNode) {
    const commands = reportNode.commands ?? [];
    const hasCommands = commands.length > 0;
    if (!hasCommands || reportNode.tool !== "shell") {
      nodeById.set("report.write", {
        ...reportNode,
        tool: "shell",
        commands: renderGenericReportCommands(),
      });
      updatedNodes.push({
        id: "report.write",
        tool: "shell",
        commandCount: renderGenericReportCommands().length,
      });
    }
  }

  if (repo) {
    for (const id of ["train.run", "eval.run"]) {
      const node = nodeById.get(id);
      if (!node) {
        continue;
      }
      const next = node.tool === "shell" ? ensureRepoInput(node, repo.repoRel) : node;
      if (next !== node) {
        nodeById.set(id, next);
      }
    }
  }

  // If train/eval are shell nodes but lack commands, downgrade to manual to avoid silent skips.
  for (const id of ["train.run", "eval.run"]) {
    const node = nodeById.get(id);
    if (!node) {
      continue;
    }
    const hasCommands = (node.commands?.length ?? 0) > 0;
    if (node.tool === "shell" && !hasCommands) {
      warnings.push(`${id}: tool=shell but commands missing; keeping manual.`);
      nodeById.set(id, { ...node, tool: "manual", commands: undefined });
    }
  }

  const refinedDag: PlanDag = {
    nodes: dag.nodes.map((n) => nodeById.get(n.id) ?? n),
    edges: dag.edges,
  };

  if (!params.opts.dryRun) {
    paths.dagBackup = await writeBackupDagIfMissing(planDir);
    await writeJsonFile(paths.dag, refinedDag);
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
    warnings: out.warnings,
    errors: out.errors,
    notes: out.notes,
    updatedNodes,
    paths: out.paths,
  });
  await writeTextFile(
    paths.refineSummary,
    renderRefineSummary({ planId, notes, warnings, errors, updatedNodes }),
  );

  return out;
}
