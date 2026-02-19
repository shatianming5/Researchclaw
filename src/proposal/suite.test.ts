import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { describe, expect, it } from "vitest";
import { compileProposal } from "./compiler.js";
import { designExperimentSuite } from "./design.js";
import { applyExperimentDagPatchOpsToPlan, materializeExperimentPlanDir } from "./suite.js";
import { validatePlanDir } from "./validate.js";

describe("proposal/suite", () => {
  it("designExperimentSuite parses OpenCode JSON output and writes artifacts", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-suite-design-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const planDir = path.join(tmp, "baseline-plan");
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
      outDir: planDir,
      discovery: "off",
      useLlm: false,
    });
    expect(compiled.ok).toBe(true);

    const suiteId = "suite-test";
    const outDir = path.join(tmp, "suite-out");
    const opencodeOutput = {
      schemaVersion: 2,
      suiteId,
      baseline: { id: "baseline", name: "Baseline", overrides: {} },
      variants: [
        {
          id: "lr-high",
          name: "LR high",
          rationale: "smoke",
          overrides: { env: { OPENCLAW_TRAIN_EXTRA_ARGS: "--learning_rate 5e-5" } },
          dagPatchOps: [
            { op: "addNode", node: { id: "abl.smoke", type: "train", tool: "manual" } },
            { op: "addEdge", edge: { from: "install.deps", to: "abl.smoke" } },
            { op: "addEdge", edge: { from: "abl.smoke", to: "train.run" } },
          ],
        },
        {
          id: "lr-low",
          name: "LR low",
          rationale: "smoke",
          overrides: { env: { OPENCLAW_TRAIN_EXTRA_ARGS: "--learning_rate 1e-5" } },
        },
      ],
      notes: ["ok"],
    };

    const res = await designExperimentSuite({
      planDir,
      outDir,
      opts: { suiteId, variantCount: 2, model: "opencode/kimi-k2.5-free", timeoutMs: 30_000 },
      deps: {
        opencodeConfigDir: path.join(tmp, "opencode-config"),
        runCommand: async (argv) => {
          if (argv[0] === "opencode" && argv[1] === "agent" && argv[2] === "list") {
            return {
              stdout: "openclaw-refine (primary)\n",
              stderr: "",
              code: 0,
              signal: null,
              killed: false,
            };
          }
          if (argv[0] === "opencode" && argv[1] === "run") {
            const stdout = JSON.stringify({
              type: "text",
              part: { text: JSON.stringify(opencodeOutput) },
            });
            return { stdout, stderr: "", code: 0, signal: null, killed: false };
          }
          throw new Error(`unexpected command: ${argv.join(" ")}`);
        },
      },
    });

    expect(res.ok).toBe(true);
    expect(res.design?.suiteId).toBe(suiteId);
    expect(res.design?.variants.length).toBe(2);
    await expect(fs.stat(res.paths.designReport)).resolves.toBeTruthy();
    await expect(fs.stat(res.paths.designSummary)).resolves.toBeTruthy();
  });

  it("applyExperimentDagPatchOpsToPlan applies DAG-level ops and keeps the plan valid", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-suite-dagpatch-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const planDir = path.join(tmp, "plan");
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
      outDir: planDir,
      discovery: "off",
      useLlm: false,
    });
    expect(compiled.ok).toBe(true);

    const before = await validatePlanDir(planDir);
    expect(before.ok).toBe(true);
    if (!before.ok || !before.data) {
      throw new Error("validate before failed");
    }

    const res = await applyExperimentDagPatchOpsToPlan({
      planDir,
      ops: [
        { op: "addNode", node: { id: "abl.smoke", type: "train", tool: "manual" } },
        { op: "addEdge", edge: { from: "install.deps", to: "abl.smoke" } },
        { op: "addEdge", edge: { from: "abl.smoke", to: "train.run" } },
      ],
    });
    expect(res.ok).toBe(true);

    const after = await validatePlanDir(planDir);
    expect(after.ok).toBe(true);
    if (!after.ok || !after.data) {
      throw new Error("validate after failed");
    }
    expect(after.data.dag.nodes.some((node) => node.id === "abl.smoke")).toBe(true);
    expect(
      after.data.dag.edges.some((edge) => edge.from === "install.deps" && edge.to === "abl.smoke"),
    ).toBe(true);
  });

  it("materializeExperimentPlanDir copies a plan package and shares cache subdirs safely", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-suite-mat-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const baselineDir = path.join(tmp, "baseline-plan");
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
      outDir: baselineDir,
      discovery: "off",
      useLlm: false,
    });
    expect(compiled.ok).toBe(true);

    const repoRel = "cache/git/example-mmengine-minimal";
    const repoAbs = path.join(baselineDir, repoRel);
    await fs.mkdir(repoAbs, { recursive: true });
    await fs.cp(path.resolve(process.cwd(), "test/fixtures/repos/mmengine-minimal"), repoAbs, {
      recursive: true,
    });

    const baselineValidated = await validatePlanDir(baselineDir);
    expect(baselineValidated.ok).toBe(true);
    if (!baselineValidated.ok || !baselineValidated.data) {
      throw new Error("baseline validate failed");
    }

    const proposalMarkdown = await fs.readFile(
      path.join(baselineDir, "input", "proposal.md"),
      "utf-8",
    );
    const suiteId = "suite-test";
    const variantDir = path.join(tmp, "suite", "experiments", "lr-high");
    const created = await materializeExperimentPlanDir({
      suiteId,
      experimentId: "lr-high",
      experimentName: "LR high",
      baselinePlanDir: baselineDir,
      outPlanDir: variantDir,
      proposalMarkdown,
      discovery: baselineValidated.data.report.discovery,
      modelKey: baselineValidated.data.report.model,
    });

    expect(created.ok).toBe(true);
    const variantValidated = await validatePlanDir(variantDir);
    expect(variantValidated.ok).toBe(true);
    if (!variantValidated.ok || !variantValidated.data) {
      throw new Error("variant validate failed");
    }

    expect(variantValidated.data.report.planId).not.toBe(baselineValidated.data.report.planId);

    const baseGitReal = await fs.realpath(path.join(baselineDir, "cache", "git"));
    const variantGitReal = await fs.realpath(path.join(variantDir, "cache", "git"));
    expect(variantGitReal).toBe(baseGitReal);

    const worktreesStat = await fs.lstat(path.join(variantDir, "cache", "worktrees"));
    expect(worktreesStat.isDirectory()).toBe(true);
    expect(worktreesStat.isSymbolicLink()).toBe(false);
  });
});
