import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { runCommandWithTimeout } from "../../process/exec.js";
import { writeRepoWorkflowEvidence } from "./evidence.js";
import { RepoWorkflowManager } from "./manager.js";

async function initGitRepo(repoRoot: string): Promise<void> {
  await runCommandWithTimeout(["git", "init"], { cwd: repoRoot, timeoutMs: 10_000 });
  await runCommandWithTimeout(["git", "config", "user.email", "test@example.com"], {
    cwd: repoRoot,
    timeoutMs: 10_000,
  });
  await runCommandWithTimeout(["git", "config", "user.name", "OpenClaw Test"], {
    cwd: repoRoot,
    timeoutMs: 10_000,
  });
  await runCommandWithTimeout(["git", "remote", "add", "origin", "https://example.com/repo.git"], {
    cwd: repoRoot,
    timeoutMs: 10_000,
  });
  await runCommandWithTimeout(["git", "add", "-A"], { cwd: repoRoot, timeoutMs: 10_000 });
  await runCommandWithTimeout(["git", "commit", "-m", "init"], {
    cwd: repoRoot,
    timeoutMs: 10_000,
  });
}

describe("proposal/repo-workflow", () => {
  it("creates an isolated worktree and writes PR evidence", async () => {
    const planDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-repo-workflow-"));
    const repoRoot = path.join(planDir, "cache", "git", "repo");
    await fs.mkdir(path.join(repoRoot, "src"), { recursive: true });
    await fs.writeFile(path.join(repoRoot, "src", "app.ts"), "export const value = 1;\n", "utf-8");
    await initGitRepo(repoRoot);

    const manager = new RepoWorkflowManager({
      planDir,
      planId: "test-plan",
      runHostCommand: runCommandWithTimeout,
    });
    const record = await manager.prepareWorktree({ repoRel: "cache/git/repo", repoKey: "repo" });

    const worktreeFile = path.join(record.worktreeAbs, "src", "app.ts");
    await fs.writeFile(worktreeFile, "export const value = 2;\n", "utf-8");

    const base = await fs.readFile(path.join(repoRoot, "src", "app.ts"), "utf-8");
    expect(base).toContain("value = 1");

    const execute = {
      ok: true,
      planDir,
      planId: "test-plan",
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
      warnings: [],
      errors: [],
      results: [],
      skipped: [],
      paths: {
        executeLog: path.join(planDir, "report", "execute_log.json"),
        executeSummary: path.join(planDir, "report", "execute_summary.md"),
      },
    };

    const { manifestPath } = await writeRepoWorkflowEvidence({
      planDir,
      planId: "test-plan",
      dag: {
        nodes: [
          {
            id: "repo.build",
            type: "build",
            tool: "shell",
            inputs: ["cache/git/repo"],
            commands: ["echo build"],
            outputs: [],
          },
        ],
        edges: [],
      },
      execute,
      records: [record],
      executedNodeIdsByRepoKey: new Map([["repo", ["repo.build"]]]),
      runHostCommand: runCommandWithTimeout,
    });

    await expect(fs.stat(manifestPath)).resolves.toBeTruthy();
    await expect(
      fs.stat(path.join(planDir, "report", "repo_workflow", "repo", "diff.patch")),
    ).resolves.toBeTruthy();
    const prBody = await fs.readFile(
      path.join(planDir, "report", "repo_workflow", "repo", "pr_body.md"),
      "utf-8",
    );
    expect(prBody).toContain("test-plan");
    expect(prBody).toContain("https://example.com/repo.git");
  });
});
