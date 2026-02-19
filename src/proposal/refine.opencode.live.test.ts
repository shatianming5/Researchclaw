import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { isTruthyEnvValue } from "../infra/env.js";
import { runCommandWithTimeout } from "../process/exec.js";
import { compileProposal } from "./compiler.js";
import { validateDag } from "./dag.js";
import { refineProposalPlan } from "./refine.js";
import { type PlanDag, PlanDagSchema } from "./schema.js";

const LIVE = isTruthyEnvValue(process.env.LIVE) || isTruthyEnvValue(process.env.OPENCLAW_LIVE_TEST);
const describeLive = LIVE ? describe : describe.skip;

async function hasOpencode(): Promise<boolean> {
  try {
    const res = await runCommandWithTimeout(["opencode", "--version"], { timeoutMs: 8_000 });
    return res.code === 0;
  } catch {
    return false;
  }
}

describeLive("proposal/refine (opencode live)", () => {
  it("produces shell commands for setup/install/train/eval/report", async () => {
    if (!(await hasOpencode())) {
      return;
    }

    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-refine-live-"));
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
    expect(compiled.ok).toBe(true);

    const planDir = compiled.rootDir;
    const repoRel = "cache/git/example-example";
    const repoAbs = path.join(planDir, repoRel);
    await fs.mkdir(repoAbs, { recursive: true });
    await fs.cp(path.resolve(process.cwd(), "test/fixtures/repos/hf-minimal"), repoAbs, {
      recursive: true,
    });

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

    const model =
      (process.env.OPENCLAW_LIVE_OPENCODE_MODEL ?? "").trim() || "opencode/kimi-k2.5-free";
    const res = await refineProposalPlan({
      planDir,
      opts: {
        model,
        agent: (process.env.OPENCLAW_LIVE_OPENCODE_AGENT ?? "").trim() || "openclaw-refine",
        timeoutMs: 180_000,
      },
    });
    expect(res.ok).toBe(true);

    const dagRaw = await fs.readFile(path.join(planDir, "plan", "plan.dag.json"), "utf-8");
    const dag = PlanDagSchema.parse(JSON.parse(dagRaw) as PlanDag);

    const setup = dag.nodes.find((node) => node.id === "setup.venv");
    const install = dag.nodes.find((node) => node.id === "install.deps");
    const train = dag.nodes.find((node) => node.id === "train.run");
    const evalNode = dag.nodes.find((node) => node.id === "eval.run");
    const report = dag.nodes.find((node) => node.id === "report.write");

    expect(setup?.tool).toBe("shell");
    expect(setup?.commands?.length ?? 0).toBeGreaterThan(0);
    expect(install?.tool).toBe("shell");
    expect(install?.commands?.length ?? 0).toBeGreaterThan(0);
    expect(train?.tool).toBe("shell");
    expect(train?.commands?.length ?? 0).toBeGreaterThan(0);
    expect(evalNode?.tool).toBe("shell");
    expect(evalNode?.commands?.length ?? 0).toBeGreaterThan(0);
    expect(report?.tool).toBe("shell");
    expect(report?.commands?.length ?? 0).toBeGreaterThan(0);

    const dagCheck = validateDag(dag);
    expect(dagCheck.ok).toBe(true);
  });
});
