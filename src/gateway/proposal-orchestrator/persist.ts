import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { ProposalJob } from "./types.js";
import { resolveStateDir } from "../../config/paths.js";

export type PersistedProposalOrchestratorState = {
  version: 1;
  jobs: ProposalJob[];
};

function defaultStatePath(): string {
  return path.join(resolveStateDir(), "proposal-orchestrator", "jobs.json");
}

export async function readProposalOrchestratorState(opts?: {
  statePath?: string;
}): Promise<PersistedProposalOrchestratorState | null> {
  const statePath = opts?.statePath ?? defaultStatePath();
  try {
    const raw = await fs.readFile(statePath, "utf8");
    const parsed = JSON.parse(raw) as PersistedProposalOrchestratorState;
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    if (parsed.version !== 1) {
      return null;
    }
    if (!Array.isArray(parsed.jobs)) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

export async function writeProposalOrchestratorState(
  state: PersistedProposalOrchestratorState,
  opts?: { statePath?: string },
): Promise<void> {
  const statePath = opts?.statePath ?? defaultStatePath();
  const dir = path.dirname(statePath);
  await fs.mkdir(dir, { recursive: true });
  const tmp = `${statePath}.${randomUUID()}.tmp`;
  await fs.writeFile(tmp, JSON.stringify(state, null, 2), "utf8");
  try {
    await fs.chmod(tmp, 0o600);
  } catch {
    // best-effort
  }
  await fs.rename(tmp, statePath);
  try {
    await fs.chmod(statePath, 0o600);
  } catch {
    // best-effort
  }
}
