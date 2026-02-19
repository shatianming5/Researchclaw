import fs from "node:fs/promises";
import path from "node:path";
import type { PlanNode } from "../schema.js";
import type { HostCommandRunner } from "./git.js";
import type { RepoExecutedNode, RepoRef, RepoWorktreeRecord } from "./types.js";
import { applyPatch } from "../../agents/apply-patch.js";
import { SAFE_NODE_TYPES } from "../execute/types.js";
import {
  gitHeadSha,
  gitWorktreeAdd,
  gitWorktreePrune,
  gitWorktreeRemove,
  isGitRepo,
} from "./git.js";

function normalizeRelPath(relPath: string): string {
  return relPath.trim().replaceAll("\\", "/");
}

function isWithinRoot(root: string, targetAbs: string): boolean {
  const rel = path.relative(root, targetAbs);
  return rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel));
}

function ensureSafeRelPath(relPath: string): string {
  const normalized = normalizeRelPath(relPath);
  if (!normalized) {
    throw new Error("Missing path");
  }
  if (normalized.startsWith("/") || normalized.startsWith("../") || normalized.includes("/../")) {
    throw new Error(`Unsafe path: ${relPath}`);
  }
  return normalized;
}

function requireRepoRefFromNode(node: PlanNode): RepoRef | null {
  const input = normalizeRelPath(node.inputs?.[0] ?? "");
  if (!input.startsWith("cache/git/")) {
    return null;
  }
  const repoKey = input.slice("cache/git/".length).replace(/^\/+/, "");
  if (!repoKey) {
    return null;
  }
  return { repoRel: input, repoKey };
}

export function shouldUseRepoWorktreeForNode(node: PlanNode): boolean {
  if (!node || node.tool !== "shell") {
    return false;
  }
  const ref = requireRepoRefFromNode(node);
  if (!ref) {
    return false;
  }
  if (SAFE_NODE_TYPES.has(node.type)) {
    return false;
  }
  return true;
}

export type RepoWorkflowManagerOpts = {
  planDir: string;
  planId: string;
  runHostCommand: HostCommandRunner;
};

export class RepoWorkflowManager {
  private readonly planDir: string;
  readonly planId: string;
  private readonly runHostCommand: HostCommandRunner;
  private readonly worktrees = new Map<string, RepoWorktreeRecord>();
  private readonly executedNodes = new Map<string, RepoExecutedNode[]>();

  constructor(opts: RepoWorkflowManagerOpts) {
    this.planDir = path.resolve(opts.planDir);
    this.planId = opts.planId;
    this.runHostCommand = opts.runHostCommand;
  }

  resolveRepoRef(node: PlanNode): RepoRef | null {
    return requireRepoRefFromNode(node);
  }

  recordNode(repoKey: string, node: RepoExecutedNode): void {
    const list = this.executedNodes.get(repoKey) ?? [];
    list.push(node);
    this.executedNodes.set(repoKey, list);
  }

  getExecutedNodes(repoKey: string): RepoExecutedNode[] {
    return this.executedNodes.get(repoKey) ?? [];
  }

  listWorktrees(): RepoWorktreeRecord[] {
    return [...this.worktrees.values()];
  }

  async prepareWorktree(repoRef: RepoRef): Promise<RepoWorktreeRecord> {
    const existing = this.worktrees.get(repoRef.repoKey);
    if (existing) {
      return existing;
    }

    const repoRel = ensureSafeRelPath(repoRef.repoRel);
    const repoKey = repoRef.repoKey;
    const baseRepoAbs = path.resolve(this.planDir, repoRel);
    if (!isWithinRoot(this.planDir, baseRepoAbs)) {
      throw new Error(`Repo path must be under planDir (got ${baseRepoAbs})`);
    }
    if (!(await isGitRepo(baseRepoAbs))) {
      throw new Error(`Repo is not a git checkout: ${repoRel}`);
    }

    const baseSha = await gitHeadSha(this.runHostCommand, baseRepoAbs);
    if (!baseSha) {
      throw new Error(`Failed to resolve base SHA for repo: ${repoRel}`);
    }

    const branchName = `proposal/${this.planId}`;
    const worktreeRel = path.join("cache", "worktrees", repoKey, this.planId);
    const safeWorktreeRel = ensureSafeRelPath(worktreeRel);
    const worktreeAbs = path.resolve(this.planDir, safeWorktreeRel);
    if (!isWithinRoot(this.planDir, worktreeAbs)) {
      throw new Error(`Worktree must be under planDir (got ${worktreeAbs})`);
    }

    // Ensure a clean, isolated worktree.
    await this.resetWorktree({
      baseRepoAbs,
      worktreeAbs,
      worktreeRel: safeWorktreeRel,
      branchName,
      baseSha,
    });
    await this.applyBootstrapPatchIfAny({ repoKey, worktreeAbs });

    const record: RepoWorktreeRecord = {
      createdAt: new Date().toISOString(),
      repoRel,
      repoKey,
      baseRepoAbs,
      worktreeRel: safeWorktreeRel,
      worktreeAbs,
      branchName,
      baseSha,
    };
    this.worktrees.set(repoKey, record);
    return record;
  }

  private async applyBootstrapPatchIfAny(params: {
    repoKey: string;
    worktreeAbs: string;
  }): Promise<void> {
    const patchPath = path.join(
      this.planDir,
      "report",
      "bootstrap",
      "worktree_patches",
      `${params.repoKey}.patch`,
    );
    let patchText: string;
    try {
      patchText = await fs.readFile(patchPath, "utf-8");
    } catch {
      return;
    }

    const applied = await applyPatch(patchText, {
      cwd: params.worktreeAbs,
      sandboxRoot: params.worktreeAbs,
    });

    const appliedPath = path.join(
      this.planDir,
      "report",
      "bootstrap",
      "worktree_patches",
      `${params.repoKey}.applied.json`,
    );
    await fs.mkdir(path.dirname(appliedPath), { recursive: true });
    await fs.writeFile(
      appliedPath,
      `${JSON.stringify(
        {
          schemaVersion: 1,
          repoKey: params.repoKey,
          appliedAt: new Date().toISOString(),
          worktreeRel: path.relative(this.planDir, params.worktreeAbs).replaceAll("\\", "/"),
          summary: applied.summary,
        },
        null,
        2,
      )}\n`,
      "utf-8",
    );
  }

  async resetWorktree(params: {
    baseRepoAbs: string;
    worktreeAbs: string;
    worktreeRel: string;
    branchName: string;
    baseSha: string;
  }): Promise<void> {
    // Best-effort remove an existing worktree registration, then wipe the directory.
    await gitWorktreeRemove(this.runHostCommand, params.baseRepoAbs, params.worktreeAbs);
    await fs.rm(params.worktreeAbs, { recursive: true, force: true });
    await fs.mkdir(path.dirname(params.worktreeAbs), { recursive: true });
    await gitWorktreePrune(this.runHostCommand, params.baseRepoAbs);

    // Create the worktree (branch is local; no push).
    await gitWorktreeAdd(
      this.runHostCommand,
      params.baseRepoAbs,
      params.branchName,
      params.worktreeAbs,
      params.baseSha,
    );
  }
}
