import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import type { PlanDag } from "./schema.js";
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

    const dagRawBefore = await fs.readFile(path.join(planDir, "plan", "plan.dag.json"), "utf-8");
    const dagBefore = JSON.parse(dagRawBefore) as PlanDag;
    const dagAfter: PlanDag = {
      nodes: dagBefore.nodes.map((node) => {
        if (node.id === "setup.venv") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel],
            commands: ['echo "setup ok"'],
          };
        }
        if (node.id === "install.deps") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel],
            commands: ['echo "install ok"'],
          };
        }
        if (node.id === "train.run") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel],
            commands: ["echo train > report/train_metrics.jsonl"],
          };
        }
        if (node.id === "eval.run") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel, ...node.inputs.filter((p) => p !== repoRel)],
            commands: [
              "python3 - <<'PY'\nimport json, pathlib\np=pathlib.Path('report')\np.mkdir(exist_ok=True)\n(p/'eval_metrics.json').write_text(json.dumps({'acc': 1.0})+'\\n')\nPY",
            ],
          };
        }
        if (node.id === "report.write") {
          return {
            ...node,
            tool: "shell",
            inputs: ["report/eval_metrics.json"],
            commands: [
              "python3 - <<'PY'\nimport json, pathlib\np=pathlib.Path('report')\np.mkdir(exist_ok=True)\n(p/'final_metrics.json').write_text(json.dumps({'schemaVersion': 1, 'metrics': {'acc': 1.0}})+'\\n')\n(p/'final_report.md').write_text('# ok\\n')\nPY",
            ],
          };
        }
        return node;
      }),
      edges: dagBefore.edges,
    };

    const opencodeOutput = {
      schemaVersion: 1,
      selectedRepoKey: "example-example",
      dag: dagAfter,
      notes: ["ok"],
    };

    const res = await refineProposalPlan({
      planDir,
      opts: { model: "opencode/kimi-k2.5-free", timeoutMs: 30_000 },
      deps: {
        opencodeConfigDir: path.join(tmp, "opencode-config"),
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

    const scriptsDir = path.join(planDir, "plan", "scripts");
    await expect(fs.stat(path.join(scriptsDir, "setup.venv.sh"))).resolves.toBeTruthy();
    await expect(fs.stat(path.join(scriptsDir, "install.deps.sh"))).resolves.toBeTruthy();
    await expect(fs.stat(path.join(scriptsDir, "train.run.sh"))).resolves.toBeTruthy();
    await expect(fs.stat(path.join(scriptsDir, "eval.run.sh"))).resolves.toBeTruthy();
    await expect(fs.stat(path.join(scriptsDir, "report.write.sh"))).resolves.toBeTruthy();
  });

  it("blocks unsafe commands and does not write the refined DAG", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-refine-unsafe-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const proposalPath = path.join(tmp, "proposal.md");
    await fs.writeFile(
      proposalPath,
      ["# Proposal", "", "Repo: https://github.com/example/example", ""].join("\n"),
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

    const dagRawBefore = await fs.readFile(path.join(planDir, "plan", "plan.dag.json"), "utf-8");
    const dagBefore = JSON.parse(dagRawBefore) as PlanDag;

    const dagAfter: PlanDag = {
      nodes: dagBefore.nodes.map((node) => {
        if (node.id === "setup.venv") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel],
            commands: ['echo "setup ok"'],
          };
        }
        if (node.id === "install.deps") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel],
            commands: ['echo "install ok"'],
          };
        }
        if (node.id === "train.run") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel],
            commands: ["sudo echo nope > report/train_metrics.jsonl"],
          };
        }
        if (node.id === "eval.run") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel],
            commands: ["echo '{\"acc\": 1.0}' > report/eval_metrics.json"],
          };
        }
        if (node.id === "report.write") {
          return {
            ...node,
            tool: "shell",
            inputs: ["report/eval_metrics.json"],
            commands: ["echo ok > report/final_report.md && echo '{}' > report/final_metrics.json"],
          };
        }
        return node;
      }),
      edges: dagBefore.edges,
    };

    const opencodeOutput = {
      schemaVersion: 1,
      selectedRepoKey: "example-example",
      dag: dagAfter,
      notes: ["unsafe"],
    };

    const res = await refineProposalPlan({
      planDir,
      opts: { model: "opencode/kimi-k2.5-free", timeoutMs: 30_000 },
      deps: {
        opencodeConfigDir: path.join(tmp, "opencode-config"),
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

    expect(res.ok).toBe(false);

    const dagRawAfter = await fs.readFile(path.join(planDir, "plan", "plan.dag.json"), "utf-8");
    expect(dagRawAfter).toBe(dagRawBefore);
  });
});
