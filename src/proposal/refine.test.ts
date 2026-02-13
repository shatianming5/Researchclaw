import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { runCommandWithTimeout } from "../process/exec.js";
import { compileProposal } from "./compiler.js";
import { refineProposalPlan } from "./refine.js";

describe("proposal/refine", () => {
  it("updates train/eval/report nodes with generated commands", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-refine-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const proposalPath = path.join(tmp, "proposal.md");
    await fs.writeFile(
      proposalPath,
      [
        "# Proposal",
        "",
        "Repo: https://github.com/example/example",
        "",
        "Goal: run training.",
      ].join("\n"),
      "utf-8",
    );

    const compiled = await compileProposal({
      proposalPath,
      cfg: {},
      workspaceDir,
      discovery: "off",
      useLlm: false,
    });

    const planDir = compiled.rootDir;
    const repoRel = "cache/git/example-example";
    const repoAbs = path.join(planDir, repoRel);
    await fs.mkdir(repoAbs, { recursive: true });
    await fs.writeFile(path.join(repoAbs, "README.md"), "# Example Repo\n", "utf-8");
    await fs.writeFile(path.join(repoAbs, "train.py"), "print('train')\n", "utf-8");

    await runCommandWithTimeout(["git", "init"], { cwd: repoAbs, timeoutMs: 30_000 });
    await runCommandWithTimeout(["git", "add", "."], { cwd: repoAbs, timeoutMs: 30_000 });
    await runCommandWithTimeout(["git", "config", "user.email", "test@example.com"], {
      cwd: repoAbs,
      timeoutMs: 30_000,
    });
    await runCommandWithTimeout(["git", "config", "user.name", "Test"], {
      cwd: repoAbs,
      timeoutMs: 30_000,
    });
    await runCommandWithTimeout(["git", "commit", "-m", "init"], {
      cwd: repoAbs,
      timeoutMs: 30_000,
    });

    const opencodeOutput = {
      schemaVersion: 1,
      nodeUpdates: [
        {
          id: "train.run",
          tool: "shell",
          commands: ['echo train > "$OPENCLAW_PLAN_DIR/report/train_metrics.jsonl"'],
          env: { OPENCLAW_PLAN_DIR: planDir },
        },
        {
          id: "eval.run",
          tool: "shell",
          commands: [
            "python3 - <<'PY'\nimport json, pathlib\np=pathlib.Path('report')\np.mkdir(exist_ok=True)\n(p/'eval_metrics.json').write_text(json.dumps({'acc': 1.0})+'\\n')\nPY",
          ],
          env: { OPENCLAW_PLAN_DIR: planDir },
        },
      ],
      notes: ["ok"],
    };

    const res = await refineProposalPlan({
      planDir,
      opts: { model: "opencode/kimi-k2.5-free", timeoutMs: 30_000 },
      deps: {
        runCommand: async (argv, options) => {
          if (argv[0] === "opencode") {
            const stdout = JSON.stringify({
              type: "text",
              part: { text: JSON.stringify(opencodeOutput) },
            });
            return { stdout, stderr: "", code: 0, signal: null, killed: false };
          }
          return await runCommandWithTimeout(argv, options);
        },
      },
    });

    expect(res.ok).toBe(true);

    const dagRaw = await fs.readFile(path.join(planDir, "plan", "plan.dag.json"), "utf-8");
    const dag = JSON.parse(dagRaw) as {
      nodes?: Array<{ id?: string; tool?: string; commands?: string[]; inputs?: string[] }>;
    };
    const train = dag.nodes?.find((n) => n.id === "train.run");
    const evalNode = dag.nodes?.find((n) => n.id === "eval.run");
    const report = dag.nodes?.find((n) => n.id === "report.write");

    expect(train?.tool).toBe("shell");
    expect(train?.commands?.length).toBeGreaterThan(0);
    expect(train?.inputs?.[0]).toBe(repoRel);

    expect(evalNode?.tool).toBe("shell");
    expect(evalNode?.commands?.length).toBeGreaterThan(0);
    expect(evalNode?.inputs?.[0]).toBe(repoRel);

    expect(report?.tool).toBe("shell");
    expect(report?.commands?.length).toBeGreaterThan(0);
  });
});
