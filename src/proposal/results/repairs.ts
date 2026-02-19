import type { Dirent } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import type { MetricDelta } from "./schema.js";
import { RepairEvidenceSchema } from "../execute/repair-evidence.schema.js";

function normalizeRel(p: string): string {
  return p.replaceAll("\\", "/");
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.stat(filePath);
    return true;
  } catch {
    return false;
  }
}

function pickTopNumericDeltas(
  deltas: MetricDelta[],
  max = 3,
): Array<{ name: string; delta: number | null }> {
  const numeric = deltas
    .map((d) => ({ name: d.name, delta: typeof d.delta === "number" ? d.delta : null }))
    .filter((d) => d.delta !== null);
  numeric.sort((a, b) => Math.abs(b.delta ?? 0) - Math.abs(a.delta ?? 0));
  return numeric.slice(0, Math.max(0, Math.floor(max)));
}

async function readJson(filePath: string): Promise<unknown> {
  const raw = await fs.readFile(filePath, "utf-8");
  return JSON.parse(raw) as unknown;
}

export type RepairEvidenceIndexEntry = {
  nodeId: string;
  patchAttempt: number;
  status: "applied_only" | "rerun_ok" | "rerun_failed";
  evidenceJson: string;
  evidenceMd: string;
  keyDeltas: Array<{ name: string; delta: number | null }>;
};

export async function collectRepairEvidenceIndex(planDir: string): Promise<{
  entries: RepairEvidenceIndexEntry[];
  warnings: string[];
}> {
  const root = path.join(planDir, "report", "repairs");
  const warnings: string[] = [];
  if (!(await fileExists(root))) {
    return { entries: [], warnings };
  }

  const out: RepairEvidenceIndexEntry[] = [];
  let nodeEntries: Dirent[] = [];
  try {
    nodeEntries = await fs.readdir(root, { withFileTypes: true });
  } catch (err) {
    warnings.push(`Failed to read repairs dir: ${String(err)}`);
    return { entries: [], warnings };
  }

  for (const nodeEntry of nodeEntries) {
    if (!nodeEntry.isDirectory()) {
      continue;
    }
    const nodeDir = path.join(root, nodeEntry.name);
    let attemptDirs: Dirent[] = [];
    try {
      // eslint-disable-next-line no-await-in-loop
      attemptDirs = await fs.readdir(nodeDir, { withFileTypes: true });
    } catch (err) {
      warnings.push(`Failed to read repairs node dir ${nodeEntry.name}: ${String(err)}`);
      continue;
    }

    for (const attemptEntry of attemptDirs) {
      if (!attemptEntry.isDirectory() || !attemptEntry.name.startsWith("attempt-")) {
        continue;
      }
      const evidenceJsonAbs = path.join(nodeDir, attemptEntry.name, "repair_evidence.json");
      if (!(await fileExists(evidenceJsonAbs))) {
        continue;
      }
      try {
        // eslint-disable-next-line no-await-in-loop
        const raw = await readJson(evidenceJsonAbs);
        const parsed = RepairEvidenceSchema.parse(raw);
        const evidenceJsonRel =
          normalizeRel(path.relative(planDir, evidenceJsonAbs)) || parsed.paths.evidenceJson;
        const evidenceMdRel = parsed.paths.evidenceMd;
        out.push({
          nodeId: parsed.node.id,
          patchAttempt: parsed.attempts.patchAttempt,
          status: parsed.status,
          evidenceJson: evidenceJsonRel,
          evidenceMd: evidenceMdRel,
          keyDeltas: pickTopNumericDeltas(parsed.metrics?.deltas ?? []),
        });
      } catch (err) {
        warnings.push(
          `Failed to parse ${normalizeRel(path.relative(planDir, evidenceJsonAbs))}: ${String(err)}`,
        );
      }
    }
  }

  out.sort((a, b) => {
    if (a.nodeId !== b.nodeId) {
      return a.nodeId.localeCompare(b.nodeId);
    }
    return a.patchAttempt - b.patchAttempt;
  });

  return { entries: out, warnings };
}
