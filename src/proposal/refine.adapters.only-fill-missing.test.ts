import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { describe, expect, it } from "vitest";
import type { PlanDag } from "./schema.js";
import { runCommandWithTimeout } from "../process/exec.js";
import { compileProposal } from "./compiler.js";
import { refineProposalPlan } from "./refine.js";

describe("proposal/refine adapters", () => {
  it("fills missing train/eval commands from adapter templates", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-refine-adapter-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const proposalPath = path.join(tmp, "proposal.md");
    await fs.writeFile(
      proposalPath,
      ["# Proposal", "", "Repo: https://github.com/example/mmengine-minimal", ""].join("\n"),
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
    const repoRel = "cache/git/example-mmengine-minimal";
    const repoAbs = path.join(planDir, repoRel);
    await fs.mkdir(repoAbs, { recursive: true });
    await fs.cp(path.resolve(process.cwd(), "test/fixtures/repos/mmengine-minimal"), repoAbs, {
      recursive: true,
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
          };
        }
        if (node.id === "install.deps") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel],
          };
        }
        if (node.id === "train.run") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel],
          };
        }
        if (node.id === "eval.run") {
          return {
            ...node,
            tool: "shell",
            inputs: [repoRel],
          };
        }
        if (node.id === "report.write") {
          return {
            ...node,
            tool: "shell",
            inputs: ["report/eval_metrics.json"],
          };
        }
        return node;
      }),
      edges: dagBefore.edges,
    };

    const opencodeOutput = {
      schemaVersion: 1,
      selectedRepoKey: "example-mmengine-minimal",
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
    const dag = JSON.parse(dagRaw) as PlanDag;

    const train = dag.nodes.find((n) => n.id === "train.run");
    const evalNode = dag.nodes.find((n) => n.id === "eval.run");
    const setup = dag.nodes.find((n) => n.id === "setup.venv");
    const install = dag.nodes.find((n) => n.id === "install.deps");
    expect(train?.tool).toBe("shell");
    expect(evalNode?.tool).toBe("shell");
    expect(setup?.commands?.length).toBeGreaterThan(0);
    expect(install?.commands?.length).toBeGreaterThan(0);
    expect(train?.commands?.join("\n")).toContain("tools/train.py");
    expect(evalNode?.commands?.join("\n")).toContain("tools/test.py");

    expect(train?.env?.OPENCLAW_PLAN_DIR).toBeDefined();
    expect(train?.env?.PIP_CACHE_DIR).toBeDefined();
    expect(evalNode?.env?.OPENCLAW_PLAN_DIR).toBeDefined();
    expect(evalNode?.env?.HF_HOME).toBeDefined();
  });
});
