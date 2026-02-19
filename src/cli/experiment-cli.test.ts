import { Command } from "commander";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";

const events: string[] = [];

const { loadConfig } = vi.hoisted(() => ({
  loadConfig: vi.fn(() => ({})),
}));

const { resolveAgentWorkspaceDir, resolveDefaultAgentId } = vi.hoisted(() => ({
  resolveAgentWorkspaceDir: vi.fn((_cfg: unknown, _agentId: string) => "/tmp/workspace"),
  resolveDefaultAgentId: vi.fn((_cfg: unknown) => "agent-1"),
}));

const { compileProposal } = vi.hoisted(() => ({
  compileProposal: vi.fn(async () => {
    events.push("compile");
    return {
      ok: true,
      planId: "compiled-plan",
      rootDir: "/tmp/compiled-plan",
      report: { warnings: [], errors: [] },
      paths: {},
    };
  }),
}));

const { validatePlanDir } = vi.hoisted(() => ({
  validatePlanDir: vi.fn(async () => {
    events.push("validate");
    return {
      ok: true,
      warnings: [],
      errors: [],
      paths: { planDag: "", acceptance: "", retry: "", compileReport: "" },
      data: {
        dag: { nodes: [], edges: [] },
        acceptance: { checks: [] },
        retry: { policies: [], defaultPolicyId: "retry.unknown" },
        report: {
          planId: "test-plan",
          createdAt: new Date().toISOString(),
          discovery: "plan",
          warnings: [],
          errors: [],
          needsConfirm: [],
        },
        needsConfirmCount: 0,
      },
    };
  }),
}));

const { runProposalPlanSafeNodes } = vi.hoisted(() => ({
  runProposalPlanSafeNodes: vi.fn(async () => {
    events.push("run-safe");
    return { ok: true, planDir: "/tmp", warnings: [], errors: [], results: [], paths: {} };
  }),
}));

const { refineProposalPlan } = vi.hoisted(() => ({
  refineProposalPlan: vi.fn(async () => {
    events.push("refine");
    return { ok: true, planDir: "/tmp", warnings: [], errors: [], notes: [], paths: {} };
  }),
}));

const { executeProposalPlan } = vi.hoisted(() => ({
  executeProposalPlan: vi.fn(async () => {
    events.push("execute");
    return {
      ok: true,
      planDir: "/tmp",
      warnings: [],
      errors: [],
      results: [],
      skipped: [],
      paths: {},
    };
  }),
}));

const { finalizeProposalPlan } = vi.hoisted(() => ({
  finalizeProposalPlan: vi.fn(async () => {
    events.push("finalize");
    return {
      ok: true,
      planDir: "/tmp",
      warnings: [],
      errors: [],
      wrote: { finalMetrics: true, finalReport: true },
      paths: { evalMetrics: "", finalMetrics: "", finalReport: "" },
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
    };
  }),
}));

const { acceptProposalResults } = vi.hoisted(() => ({
  acceptProposalResults: vi.fn(async () => {
    events.push("accept");
    return {
      schemaVersion: 1,
      ok: true,
      exitCode: 0,
      createdAt: new Date().toISOString(),
      planDir: "/tmp",
      runId: "run-1",
      runDir: "/tmp/run-1",
      status: "pass",
      summary: { pass: 1, fail: 0, needs_confirm: 0, total: 1 },
      checks: [],
      warnings: [],
      errors: [],
      artifacts: { manifestPath: "", archived: [], missing: [] },
      paths: { reportJson: "", reportMd: "", manifestJson: "" },
    };
  }),
}));

const runtime = {
  log: vi.fn(),
  error: vi.fn(),
  exit: vi.fn((code: number) => {
    events.push(`exit:${code}`);
  }),
};

vi.mock("../config/config.js", () => ({ loadConfig }));
vi.mock("../agents/agent-scope.js", () => ({ resolveAgentWorkspaceDir, resolveDefaultAgentId }));
vi.mock("../proposal/compiler.js", () => ({ compileProposal }));
vi.mock("../proposal/validate.js", () => ({ validatePlanDir }));
vi.mock("../proposal/run.js", () => ({ runProposalPlanSafeNodes }));
vi.mock("../proposal/refine.js", () => ({ refineProposalPlan }));
vi.mock("../proposal/execute.js", () => ({ executeProposalPlan }));
vi.mock("../proposal/finalize.js", () => ({ finalizeProposalPlan }));
vi.mock("../proposal/results/index.js", () => ({ acceptProposalResults }));
vi.mock("../runtime.js", () => ({ defaultRuntime: runtime }));

const { registerExperimentCli } = await import("./experiment-cli.js");

describe("cli/experiment", () => {
  it("orchestrates stages for an existing planDir", async () => {
    events.length = 0;
    const planDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-exp-plan-"));

    const program = new Command();
    registerExperimentCli(program);

    await program.parseAsync(["experiment", "run", planDir], { from: "user" });

    expect(events).toEqual([
      "validate",
      "run-safe",
      "refine",
      "validate",
      "validate",
      "execute",
      "finalize",
      "accept",
      "exit:0",
    ]);
    expect(compileProposal).not.toHaveBeenCalled();
  });

  it("compiles a proposal file before running stages", async () => {
    events.length = 0;
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-exp-proposal-"));
    const proposalPath = path.join(root, "proposal.md");
    await fs.writeFile(proposalPath, "# test\n", "utf-8");

    const compiledPlanDir = path.join(root, "workdir");
    await fs.mkdir(compiledPlanDir, { recursive: true });
    compileProposal.mockImplementationOnce(async () => {
      events.push("compile");
      return {
        ok: true,
        planId: "compiled-plan",
        rootDir: compiledPlanDir,
        report: { warnings: [], errors: [] },
        paths: {},
      };
    });

    const program = new Command();
    registerExperimentCli(program);

    await program.parseAsync(
      ["experiment", "run", proposalPath, "--workspace", root, "--compile-model", "test/model"],
      { from: "user" },
    );

    expect(events).toEqual([
      "compile",
      "validate",
      "run-safe",
      "refine",
      "validate",
      "validate",
      "execute",
      "finalize",
      "accept",
      "exit:0",
    ]);
  });

  it("compiles free-form --message input before running stages", async () => {
    events.length = 0;

    const program = new Command();
    registerExperimentCli(program);

    await program.parseAsync(
      ["experiment", "run", "--message", "Repo: https://github.com/example/example"],
      { from: "user" },
    );

    expect(events).toEqual([
      "compile",
      "validate",
      "run-safe",
      "refine",
      "validate",
      "validate",
      "execute",
      "finalize",
      "accept",
      "exit:0",
    ]);
  });
});
