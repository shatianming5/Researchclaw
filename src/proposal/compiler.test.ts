import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { compileProposal } from "./compiler.js";
import { CompileReportSchema } from "./schema.js";
import { validatePlanDir } from "./validate.js";

describe("proposal/compiler", () => {
  it("writes a plan package with heuristics and discovery off", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const proposalPath = path.join(tmp, "proposal.md");
    await fs.writeFile(
      proposalPath,
      [
        "# My Proposal",
        "",
        "Repo: https://github.com/openclaw/openclaw",
        "Dataset: https://huggingface.co/datasets/squad",
        "",
        "Goal: train a model and report results.",
      ].join("\n"),
      "utf-8",
    );

    const result = await compileProposal({
      proposalPath,
      cfg: {},
      workspaceDir,
      discovery: "off",
      useLlm: false,
    });

    expect(result.planId).toMatch(/^\d{8}-\d{6}-[0-9a-f]{12}$/);
    expect(result.rootDir).toContain(path.join(workspaceDir, "experiments", "workdir"));

    const reportRaw = await fs.readFile(result.paths.compileReport, "utf-8");
    const report = CompileReportSchema.parse(JSON.parse(reportRaw) as unknown);
    expect(report.planId).toBe(result.planId);
    expect(report.discovery).toBe("off");
    expect(report.warnings.length).toBeGreaterThan(0);
    expect(report.needsConfirm.some((i) => i.id === "resource:gpu")).toBe(true);

    await expect(fs.stat(result.paths.dag)).resolves.toBeTruthy();
    await expect(fs.stat(result.paths.acceptance)).resolves.toBeTruthy();
    await expect(fs.stat(result.paths.retry)).resolves.toBeTruthy();
    await expect(fs.stat(result.paths.needsConfirm)).resolves.toBeTruthy();
    await expect(fs.stat(result.paths.runbook)).resolves.toBeTruthy();
    await expect(fs.stat(path.join(result.rootDir, "plan", "runbook.md"))).resolves.toBeTruthy();

    const validation = await validatePlanDir(result.rootDir);
    expect(validation.ok).toBe(true);

    const discovery = JSON.parse(await fs.readFile(result.paths.discovery, "utf-8")) as {
      datasets?: Array<{ platform?: string; resolvedId?: string }>;
    };
    expect(discovery.datasets?.[0]?.platform).toBe("hf");
    expect(discovery.datasets?.[0]?.resolvedId).toBe("squad");
  });

  it("extracts GitHub owner/repo shorthand and emits fetch_repo + static_checks nodes", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const proposalPath = path.join(tmp, "proposal.md");
    await fs.writeFile(
      proposalPath,
      ["# My Proposal", "", "Repo: openclaw/openclaw", "", "Goal: run checks."].join("\n"),
      "utf-8",
    );

    const result = await compileProposal({
      proposalPath,
      cfg: {},
      workspaceDir,
      discovery: "off",
      useLlm: false,
    });

    const dagRaw = await fs.readFile(result.paths.dag, "utf-8");
    const dag = JSON.parse(dagRaw) as {
      nodes?: Array<{ id?: string; type?: string; commands?: string[] }>;
    };
    const nodeIds = new Set((dag.nodes ?? []).map((n) => n.id ?? ""));

    expect(nodeIds.has("repo.fetch.openclaw-openclaw")).toBe(true);
    expect(nodeIds.has("repo.check.openclaw-openclaw")).toBe(true);

    const fetchNode = (dag.nodes ?? []).find((n) => n.id === "repo.fetch.openclaw-openclaw");
    expect(fetchNode?.type).toBe("fetch_repo");
    expect(fetchNode?.commands?.[0]).toContain("https://github.com/openclaw/openclaw.git");
  });

  it("emits a Kaggle full download node for Kaggle datasets", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const proposalPath = path.join(tmp, "proposal.md");
    await fs.writeFile(
      proposalPath,
      [
        "# My Proposal",
        "",
        "Dataset: https://www.kaggle.com/datasets/owner/ds",
        "",
        "Goal: download data and run training.",
      ].join("\n"),
      "utf-8",
    );

    const result = await compileProposal({
      proposalPath,
      cfg: {},
      workspaceDir,
      discovery: "off",
      useLlm: false,
    });

    const dagRaw = await fs.readFile(result.paths.dag, "utf-8");
    const dag = JSON.parse(dagRaw) as {
      nodes?: Array<{ id?: string; type?: string; tool?: string; commands?: string[] }>;
      edges?: Array<{ from?: string; to?: string }>;
    };

    const fetchNode = (dag.nodes ?? []).find((n) => n.id === "data.fetch.owner-ds");
    expect(fetchNode?.type).toBe("fetch_dataset_kaggle");
    expect(fetchNode?.tool).toBe("shell");
    expect(fetchNode?.commands?.join("\n")).toContain("kaggle datasets download -d owner/ds");

    const edges = (dag.edges ?? []).map((e) => `${e.from}→${e.to}`);
    expect(edges).toContain("review.needs_confirm→data.fetch.owner-ds");
    expect(edges).toContain("data.fetch.owner-ds→train.run");
  });
});
