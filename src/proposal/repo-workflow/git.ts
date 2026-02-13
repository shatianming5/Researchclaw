import fs from "node:fs/promises";
import path from "node:path";
import type { CommandOptions, SpawnResult } from "../../process/exec.js";

export type HostCommandRunner = (argv: string[], options: CommandOptions) => Promise<SpawnResult>;

function tail(text: string, maxChars = 1200): string {
  const trimmed = text.trim();
  if (trimmed.length <= maxChars) {
    return trimmed;
  }
  return trimmed.slice(-maxChars);
}

async function pathExists(p: string): Promise<boolean> {
  try {
    await fs.stat(p);
    return true;
  } catch {
    return false;
  }
}

export async function isGitRepo(dir: string): Promise<boolean> {
  return await pathExists(path.join(dir, ".git"));
}

export async function runGit(
  runHostCommand: HostCommandRunner,
  cwd: string,
  args: string[],
  opts?: { timeoutMs?: number },
): Promise<SpawnResult> {
  const argv = ["git", ...args];
  const res = await runHostCommand(argv, {
    cwd,
    timeoutMs: opts?.timeoutMs ?? 30_000,
  });
  if (res.code !== 0) {
    throw new Error(
      `git ${args.join(" ")} failed (code=${res.code ?? "null"}): ${tail(res.stderr)}`,
    );
  }
  return res;
}

export async function tryGit(
  runHostCommand: HostCommandRunner,
  cwd: string,
  args: string[],
  opts?: { timeoutMs?: number },
): Promise<{ ok: true; res: SpawnResult } | { ok: false; error: string }> {
  try {
    const res = await runHostCommand(["git", ...args], {
      cwd,
      timeoutMs: opts?.timeoutMs ?? 30_000,
    });
    if (res.code !== 0) {
      return {
        ok: false,
        error: `git ${args.join(" ")} failed (code=${res.code ?? "null"}): ${tail(res.stderr)}`,
      };
    }
    return { ok: true, res };
  } catch (err) {
    return { ok: false, error: `git ${args.join(" ")} error: ${String(err)}` };
  }
}

export async function gitHeadSha(
  runHostCommand: HostCommandRunner,
  cwd: string,
): Promise<string | null> {
  const res = await tryGit(runHostCommand, cwd, ["rev-parse", "HEAD"]);
  if (!res.ok) {
    return null;
  }
  const sha = res.res.stdout.trim().split("\n")[0]?.trim() ?? "";
  return sha ? sha : null;
}

export async function gitRemoteOriginUrl(
  runHostCommand: HostCommandRunner,
  cwd: string,
): Promise<string | null> {
  const res = await tryGit(runHostCommand, cwd, ["remote", "get-url", "origin"]);
  if (!res.ok) {
    return null;
  }
  const url = res.res.stdout.trim().split("\n")[0]?.trim() ?? "";
  return url ? url : null;
}

export async function gitIsDirty(
  runHostCommand: HostCommandRunner,
  cwd: string,
): Promise<boolean | null> {
  const res = await tryGit(runHostCommand, cwd, ["status", "--porcelain=v1"]);
  if (!res.ok) {
    return null;
  }
  return res.res.stdout.trim().length > 0;
}

export async function gitDiffPatch(
  runHostCommand: HostCommandRunner,
  cwd: string,
  baseSha: string,
): Promise<string> {
  const res = await runGit(runHostCommand, cwd, ["diff", "--binary", baseSha], {
    timeoutMs: 60_000,
  });
  return res.stdout;
}

export async function gitDiffStat(
  runHostCommand: HostCommandRunner,
  cwd: string,
  baseSha: string,
): Promise<string> {
  const res = await runGit(runHostCommand, cwd, ["diff", "--stat", baseSha], {
    timeoutMs: 60_000,
  });
  return res.stdout;
}

export async function gitWorktreePrune(
  runHostCommand: HostCommandRunner,
  baseRepoAbs: string,
): Promise<void> {
  await tryGit(runHostCommand, baseRepoAbs, ["worktree", "prune"]);
}

export async function gitWorktreeRemove(
  runHostCommand: HostCommandRunner,
  baseRepoAbs: string,
  worktreeAbs: string,
): Promise<void> {
  await tryGit(runHostCommand, baseRepoAbs, ["worktree", "remove", "--force", worktreeAbs], {
    timeoutMs: 60_000,
  });
}

export async function gitWorktreeAdd(
  runHostCommand: HostCommandRunner,
  baseRepoAbs: string,
  branchName: string,
  worktreeAbs: string,
  baseSha: string,
): Promise<void> {
  await runGit(
    runHostCommand,
    baseRepoAbs,
    ["worktree", "add", "--force", "-B", branchName, worktreeAbs, baseSha],
    { timeoutMs: 60_000 },
  );
}
