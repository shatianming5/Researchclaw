import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { DiscoveryMode } from "./schema.js";

export type PlanLayout = {
  planId: string;
  rootDir: string;
  inputDir: string;
  irDir: string;
  planDir: string;
  reportDir: string;
  cacheDir: string;
};

function pad2(n: number): string {
  return String(n).padStart(2, "0");
}

export function formatPlanTimestamp(now: Date): string {
  // UTC timestamp to avoid machine-local ambiguity
  const yyyy = now.getUTCFullYear();
  const mm = pad2(now.getUTCMonth() + 1);
  const dd = pad2(now.getUTCDate());
  const hh = pad2(now.getUTCHours());
  const mi = pad2(now.getUTCMinutes());
  const ss = pad2(now.getUTCSeconds());
  return `${yyyy}${mm}${dd}-${hh}${mi}${ss}`;
}

export function computeProposalDigest(params: {
  proposalMarkdown: string;
  discovery: DiscoveryMode;
  modelKey?: string;
}): string {
  const clipped =
    params.proposalMarkdown.length > 80_000
      ? params.proposalMarkdown.slice(0, 80_000)
      : params.proposalMarkdown;
  const seed = JSON.stringify({
    discovery: params.discovery,
    modelKey: params.modelKey ?? "",
    proposal: clipped,
  });
  return crypto.createHash("sha256").update(seed).digest("hex");
}

export function generatePlanId(params: {
  proposalMarkdown: string;
  discovery: DiscoveryMode;
  modelKey?: string;
  now?: Date;
}): { planId: string; digest: string; createdAt: string } {
  const now = params.now ?? new Date();
  const createdAt = now.toISOString();
  const digest = computeProposalDigest({
    proposalMarkdown: params.proposalMarkdown,
    discovery: params.discovery,
    modelKey: params.modelKey,
  });
  const ts = formatPlanTimestamp(now);
  const short = digest.slice(0, 12);
  return { planId: `${ts}-${short}`, digest, createdAt };
}

export function buildDefaultPlanRoot(workspaceDir: string, planId: string): string {
  return path.join(workspaceDir, "experiments", "workdir", planId);
}

export async function createPlanLayout(params: {
  planId: string;
  workspaceDir: string;
  outDir?: string;
}): Promise<PlanLayout> {
  const rootDir = params.outDir?.trim()
    ? path.resolve(params.outDir)
    : buildDefaultPlanRoot(params.workspaceDir, params.planId);

  const inputDir = path.join(rootDir, "input");
  const irDir = path.join(rootDir, "ir");
  const planDir = path.join(rootDir, "plan");
  const reportDir = path.join(rootDir, "report");
  const cacheDir = path.join(rootDir, "cache");

  await fs.mkdir(inputDir, { recursive: true });
  await fs.mkdir(irDir, { recursive: true });
  await fs.mkdir(planDir, { recursive: true });
  await fs.mkdir(reportDir, { recursive: true });
  await fs.mkdir(cacheDir, { recursive: true });

  return {
    planId: params.planId,
    rootDir,
    inputDir,
    irDir,
    planDir,
    reportDir,
    cacheDir,
  };
}
