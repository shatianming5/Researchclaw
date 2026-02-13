import fs from "node:fs/promises";
import path from "node:path";
import type { ProposalLlmClient } from "../llm.js";
import type { PlanNode } from "../schema.js";
import type { CpuRepairHook } from "./cpu.js";
import type { ExecuteAttempt } from "./types.js";
import { applyPatch, type ApplyPatchSummary } from "../../agents/apply-patch.js";
import { sanitizeIdPart, tail } from "./utils.js";

type RepairResult = { applied: boolean; patch?: ExecuteAttempt["patch"] };

function isWithinRoot(root: string, filePath: string) {
  const rel = path.relative(root, filePath);
  return rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel));
}

function extractPatchBlock(text: string): string | null {
  const start = text.indexOf("*** Begin Patch");
  if (start === -1) {
    return null;
  }
  const end = text.indexOf("*** End Patch", start);
  if (end === -1) {
    return null;
  }
  const block = text.slice(start, end + "*** End Patch".length);
  return `${block.trim()}\n`;
}

function extractFileHint(params: { sandboxRoot: string; combinedOutput: string }): string | null {
  const re =
    /([A-Za-z0-9_./-]+\.(?:ts|tsx|js|jsx|py|rs|go|java|cpp|c|h|json|yaml|yml|md)):(\d+)(?::(\d+))?/;
  const m = params.combinedOutput.match(re);
  if (!m) {
    return null;
  }
  const relPath = (m[1] ?? "").trim();
  const lineRaw = (m[2] ?? "").trim();
  const lineNum = Number.parseInt(lineRaw, 10);
  if (!relPath || !Number.isFinite(lineNum) || lineNum <= 0) {
    return null;
  }
  const abs = path.resolve(params.sandboxRoot, relPath);
  if (!isWithinRoot(params.sandboxRoot, abs)) {
    return null;
  }
  return `${relPath}:${lineNum}`;
}

async function readSnippet(params: {
  sandboxRoot: string;
  fileWithLine: string;
  radius?: number;
}): Promise<string | null> {
  const [fileRel, lineRaw] = params.fileWithLine.split(":");
  if (!fileRel || !lineRaw) {
    return null;
  }
  const lineNum = Number.parseInt(lineRaw, 10);
  if (!Number.isFinite(lineNum) || lineNum <= 0) {
    return null;
  }
  const abs = path.resolve(params.sandboxRoot, fileRel);
  if (!isWithinRoot(params.sandboxRoot, abs)) {
    return null;
  }
  try {
    const raw = await fs.readFile(abs, "utf-8");
    const lines = raw.split(/\r?\n/);
    const radius = Math.max(1, Math.floor(params.radius ?? 20));
    const start = Math.max(1, lineNum - radius);
    const end = Math.min(lines.length, lineNum + radius);
    const excerpt: string[] = [];
    for (let i = start; i <= end; i += 1) {
      const text = lines[i - 1] ?? "";
      excerpt.push(`${String(i).padStart(5, " ")} | ${text}`);
    }
    return [`<file:${fileRel}:${lineNum}>`, ...excerpt, `</file:${fileRel}:${lineNum}>`].join("\n");
  } catch {
    return null;
  }
}

function buildRepairPrompt(params: {
  node: PlanNode;
  workdir: string;
  combinedOutput: string;
  snippet?: string | null;
}): string {
  const commands = params.node.commands?.join("\n") ?? "";
  const clippedOutput =
    params.combinedOutput.length > 12_000
      ? tail(params.combinedOutput, 12_000)
      : params.combinedOutput;
  return (
    `You are an automated code repair agent.\n` +
    `Your task: produce a minimal patch to fix the failing step.\n\n` +
    `Output format rules:\n` +
    `- Output ONLY an apply_patch patch.\n` +
    `- Must include "*** Begin Patch" and "*** End Patch".\n` +
    `- Do not include markdown fences or commentary.\n` +
    `- Only touch files inside the repo workspace.\n` +
    `- Keep the patch minimal; do not refactor.\n\n` +
    `Failing node:\n` +
    `- id: ${params.node.id}\n` +
    `- type: ${params.node.type}\n` +
    `- workdir: ${params.workdir}\n\n` +
    `Command(s):\n${commands}\n\n` +
    `Failure output:\n${clippedOutput}\n\n` +
    (params.snippet ? `Context snippet:\n${params.snippet}\n\n` : "") +
    `Now output the patch.\n`
  );
}

function shouldAttemptRepair(category?: string) {
  if (!category) {
    return true;
  }
  return category !== "network" && category !== "rate_limit";
}

export function createCpuRepairHook(params: {
  planDir: string;
  llmClient: ProposalLlmClient;
  maxRepairAttempts: number;
}): CpuRepairHook {
  const remainingByNodeId = new Map<string, number>();
  return async ({ planDir, node, hostWorkdir, attempt, stdout, stderr }): Promise<RepairResult> => {
    const category = attempt.failureCategory;
    if (!shouldAttemptRepair(category)) {
      return { applied: false };
    }

    const key = node.id;
    const remaining = remainingByNodeId.get(key) ?? params.maxRepairAttempts;
    if (remaining <= 0) {
      return { applied: false };
    }
    remainingByNodeId.set(key, remaining - 1);

    const sandboxRoot = hostWorkdir && isWithinRoot(planDir, hostWorkdir) ? hostWorkdir : planDir;
    const combinedOutput = [stderr, stdout].filter(Boolean).join("\n");
    const fileHint = extractFileHint({ sandboxRoot, combinedOutput });
    const snippet = fileHint ? await readSnippet({ sandboxRoot, fileWithLine: fileHint }) : null;
    const prompt = buildRepairPrompt({
      node,
      workdir: path.relative(planDir, sandboxRoot) || ".",
      combinedOutput,
      snippet,
    });

    const response = await params.llmClient.completeText({
      prompt,
      temperature: 0,
      timeoutMs: 60_000,
      maxTokens: 2_000,
    });
    const patchText = extractPatchBlock(response);
    if (!patchText) {
      return { applied: false };
    }

    const repairsDir = path.join(planDir, "report", "repairs", sanitizeIdPart(node.id));
    await fs.mkdir(repairsDir, { recursive: true });
    const patchPath = path.join(repairsDir, `attempt-${attempt.attempt}.patch`);
    await fs.writeFile(patchPath, patchText, "utf-8");

    const applied = await applyPatch(patchText, {
      cwd: sandboxRoot,
      sandboxRoot,
    });

    const summary: ApplyPatchSummary = applied.summary;
    return {
      applied: summary.added.length + summary.modified.length + summary.deleted.length > 0,
      patch: {
        summary,
        patchPath,
      },
    };
  };
}
