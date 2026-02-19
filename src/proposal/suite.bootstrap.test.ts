import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { compileProposal } from "./compiler.js";

const { pipelineCalls, runExperimentPipeline } = vi.hoisted(() => {
  const calls: Array<{ action: string; stages?: unknown }> = [];
  const fn = vi.fn(async (params: { action: string; input: unknown }) => {
    calls.push({ action: params.action, stages: (params as { stages?: unknown }).stages });
    if (params.action === "plan") {
      return {
        ok: true,
        action: "plan",
        planDir: (params.input as { planDir: string }).planDir,
        validate: { ok: true, data: {} },
        safe: { ok: true },
        refine: { ok: true, warnings: [], errors: [] },
      };
    }
    return {
      ok: true,
      action: params.action,
      planDir: (params.input as { planDir: string }).planDir,
    };
  });
  return { pipelineCalls: calls, runExperimentPipeline: fn };
});

const { designExperimentSuite } = vi.hoisted(() => ({
  designExperimentSuite: vi.fn(async () => {
    return {
      ok: true,
      warnings: [],
      errors: [],
      design: {
        schemaVersion: 2,
        suiteId: "suite-test",
        baseline: { id: "baseline", name: "Baseline", overrides: {}, dagPatchOps: [] },
        variants: [{ id: "v1", name: "Variant 1", overrides: {}, dagPatchOps: [] }],
      },
    };
  }),
}));

vi.mock("./pipeline.js", () => ({ runExperimentPipeline }));
vi.mock("./design.js", () => ({ designExperimentSuite }));

const { runExperimentSuite } = await import("./suite.js");

describe("proposal/suite bootstrap stage wiring", () => {
  it("threads bootstrap stage through baseline + variants pipelines", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-suite-bootstrap-"));
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

    pipelineCalls.length = 0;
    const res = await runExperimentSuite({
      input: { kind: "planDir", planDir },
      cfg: {},
      opts: { variantCount: 1 },
      stages: {
        bootstrap: {
          enabled: true,
          mode: "worktree",
          dryRun: true,
          model: "opencode/kimi-k2.5-free",
          timeoutMs: 10_000,
          maxAttempts: 1,
        },
      },
    });
    expect(res.ok).toBe(true);

    const stagesByAction = new Map(pipelineCalls.map((call) => [call.action, call.stages]));
    expect(stagesByAction.get("execute")).toMatchObject({
      bootstrap: expect.any(Object),
    });
    expect(stagesByAction.get("pipeline")).toMatchObject({
      bootstrap: expect.any(Object),
    });
  });
});
