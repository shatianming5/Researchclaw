import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { runProposalPlanSafeNodes } from "./run.js";

async function writePlanPackage(params: {
  dag: unknown;
  discovery: unknown;
  report: unknown;
  acceptance?: unknown;
  retry?: unknown;
}): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-run-"));
  await fs.mkdir(path.join(root, "plan"), { recursive: true });
  await fs.mkdir(path.join(root, "ir"), { recursive: true });
  await fs.mkdir(path.join(root, "report"), { recursive: true });

  await fs.writeFile(path.join(root, "plan", "plan.dag.json"), JSON.stringify(params.dag, null, 2));
  await fs.writeFile(
    path.join(root, "plan", "acceptance.json"),
    JSON.stringify(params.acceptance ?? { checks: [] }, null, 2),
  );
  await fs.writeFile(
    path.join(root, "plan", "retry.json"),
    JSON.stringify(params.retry ?? { policies: [] }, null, 2),
  );
  await fs.writeFile(
    path.join(root, "ir", "discovery.json"),
    JSON.stringify(params.discovery, null, 2),
  );
  await fs.writeFile(
    path.join(root, "report", "compile_report.json"),
    JSON.stringify(params.report, null, 2),
  );

  return root;
}

describe("proposal/run", () => {
  it("dry-run does not execute commands", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "repo.fetch.repo",
            type: "fetch_repo",
            tool: "shell",
            outputs: ["cache/git/repo"],
            commands: ["git clone --depth 1 https://example.com/repo.git cache/git/repo"],
          },
          {
            id: "repo.check.repo",
            type: "static_checks",
            tool: "shell",
            inputs: ["cache/git/repo"],
            outputs: ["report/static_checks/repo.json"],
          },
        ],
        edges: [{ from: "repo.fetch.repo", to: "repo.check.repo" }],
      },
      discovery: { repos: [], datasets: [] },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "plan",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
    });

    const runCommand = vi.fn(async () => ({ code: 0, stdout: "", stderr: "" }));
    const res = await runProposalPlanSafeNodes({
      planDir,
      opts: { dryRun: true },
      deps: { runCommand },
    });

    expect(res.ok).toBe(true);
    expect(runCommand).not.toHaveBeenCalled();
    expect(res.results.map((r) => r.status)).toEqual(["dry_run", "dry_run"]);

    await expect(fs.stat(path.join(planDir, "report", "run_log.json"))).resolves.toBeTruthy();
    await expect(
      fs.stat(path.join(planDir, "report", "execution_suggestions.md")),
    ).resolves.toBeTruthy();
  });

  it("rejects unsafe git clone target paths", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "repo.fetch.evil",
            type: "fetch_repo",
            tool: "shell",
            outputs: ["../evil"],
            commands: ["git clone --depth 1 https://example.com/repo.git ../evil"],
          },
        ],
        edges: [],
      },
      discovery: { repos: [], datasets: [] },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "plan",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
    });

    const runCommand = vi.fn(async () => ({ code: 0, stdout: "", stderr: "" }));
    const res = await runProposalPlanSafeNodes({
      planDir,
      deps: { runCommand },
    });

    expect(res.ok).toBe(false);
    expect(runCommand).not.toHaveBeenCalled();
    expect(res.results[0]?.status).toBe("failed");
  });

  it("runs static checks and writes a report under report/static_checks", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "repo.check.repo",
            type: "static_checks",
            tool: "shell",
            inputs: ["cache/git/repo"],
            outputs: ["report/static_checks/repo.json"],
          },
        ],
        edges: [],
      },
      discovery: { repos: [], datasets: [] },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "plan",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
    });

    const repoDir = path.join(planDir, "cache", "git", "repo");
    await fs.mkdir(path.join(repoDir, ".git"), { recursive: true });
    await fs.writeFile(
      path.join(repoDir, "package.json"),
      JSON.stringify({ name: "repo" }),
      "utf-8",
    );

    const runCommand = vi.fn(async (argv: string[]) => {
      const cmd = argv.join(" ");
      if (cmd.includes("rev-parse HEAD")) {
        return { code: 0, stdout: "deadbeef\n", stderr: "" };
      }
      if (cmd.includes("status --porcelain")) {
        return { code: 0, stdout: "", stderr: "" };
      }
      return { code: 0, stdout: "", stderr: "" };
    });

    const res = await runProposalPlanSafeNodes({
      planDir,
      deps: { runCommand },
    });

    expect(res.ok).toBe(true);
    expect(runCommand).toHaveBeenCalled();

    const reportPath = path.join(planDir, "report", "static_checks", "repo.json");
    const raw = await fs.readFile(reportPath, "utf-8");
    const parsed = JSON.parse(raw) as {
      repo?: { headCommit?: string };
      detected?: { node?: boolean };
    };
    expect(parsed.repo?.headCommit).toBe("deadbeef");
    expect(parsed.detected?.node).toBe(true);
  });

  it("fetches a HF dataset sample and writes it under cache/data/<label>", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "data.sample.squad",
            type: "fetch_dataset_sample",
            tool: "shell",
            outputs: ["cache/data/squad"],
          },
        ],
        edges: [],
      },
      discovery: {
        repos: [],
        datasets: [
          {
            platform: "hf",
            resolvedId: "squad",
            resolvedUrl: "https://huggingface.co/datasets/squad",
            input: { name: "squad" },
            exists: true,
            evidence: [],
            warnings: [],
          },
        ],
      },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "sample",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
    });

    const fetchFn = vi.fn(async (input: string) => {
      if (input.startsWith("https://huggingface.co/api/datasets/squad")) {
        return new Response(JSON.stringify({ id: "squad" }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      if (input.startsWith("https://datasets-server.huggingface.co/splits?dataset=squad")) {
        return new Response(JSON.stringify({ splits: [{ config: "default", split: "train" }] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      if (input.startsWith("https://datasets-server.huggingface.co/rows?dataset=squad")) {
        return new Response(JSON.stringify({ rows: [{ row: { id: 1 } }] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      return new Response("not found", { status: 404 });
    });

    const res = await runProposalPlanSafeNodes({
      planDir,
      deps: { fetchFn },
    });

    expect(res.ok).toBe(true);

    const outDir = path.join(planDir, "cache", "data", "squad");
    await expect(fs.stat(path.join(outDir, "discovered.json"))).resolves.toBeTruthy();
    await expect(fs.stat(path.join(outDir, "sample.json"))).resolves.toBeTruthy();
  });

  it("runs fetch_dataset_sample node commands via openclaw proposal dataset sample", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "data.sample.squad",
            type: "fetch_dataset_sample",
            tool: "shell",
            outputs: ["cache/data/squad"],
            commands: ["openclaw proposal dataset sample --dataset squad --out cache/data/squad"],
          },
        ],
        edges: [],
      },
      discovery: {
        repos: [],
        datasets: [
          {
            platform: "hf",
            resolvedId: "squad",
            resolvedUrl: "https://huggingface.co/datasets/squad",
            input: { name: "squad" },
            exists: true,
            evidence: [],
            warnings: [],
          },
        ],
      },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "sample",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
    });

    const fetchFn = vi.fn(async () => new Response("not expected", { status: 500 }));

    const runCommand = vi.fn(async (argv: string[], opts?: { cwd?: string }) => {
      expect(argv).toEqual([
        "openclaw",
        "proposal",
        "dataset",
        "sample",
        "--dataset",
        "squad",
        "--out",
        "cache/data/squad",
      ]);
      expect(opts?.cwd).toBe(planDir);

      const outDir = path.join(planDir, "cache", "data", "squad");
      await fs.mkdir(outDir, { recursive: true });
      await fs.writeFile(path.join(outDir, "sample.json"), "{}", "utf-8");
      await fs.writeFile(path.join(outDir, "discovered.json"), "{}", "utf-8");

      return { code: 0, stdout: "", stderr: "" };
    });

    const res = await runProposalPlanSafeNodes({
      planDir,
      deps: { fetchFn, runCommand },
    });

    expect(res.ok).toBe(true);
    expect(fetchFn).not.toHaveBeenCalled();
    expect(runCommand).toHaveBeenCalledTimes(1);

    const outDir = path.join(planDir, "cache", "data", "squad");
    await expect(fs.stat(path.join(outDir, "sample.json"))).resolves.toBeTruthy();
  });

  it("skips Kaggle dataset sampling commands when credentials are missing", async () => {
    const prevState = process.env.OPENCLAW_STATE_DIR;
    const tmpState = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-kaggle-"));
    process.env.OPENCLAW_STATE_DIR = tmpState;

    try {
      const planDir = await writePlanPackage({
        dag: {
          nodes: [
            {
              id: "data.sample.owner-ds",
              type: "fetch_dataset_sample",
              tool: "shell",
              outputs: ["cache/data/owner-ds"],
              commands: [
                "openclaw proposal dataset sample --platform kaggle --dataset owner/ds --out cache/data/owner-ds",
              ],
            },
          ],
          edges: [],
        },
        discovery: {
          repos: [],
          datasets: [
            {
              platform: "kaggle",
              resolvedId: "owner/ds",
              resolvedUrl: "https://www.kaggle.com/datasets/owner/ds",
              input: { name: "owner/ds" },
              exists: undefined,
              evidence: [],
              warnings: [],
            },
          ],
        },
        report: {
          planId: "test-plan",
          createdAt: new Date().toISOString(),
          discovery: "sample",
          warnings: [],
          errors: [],
          needsConfirm: [],
        },
      });

      const runCommand = vi.fn(async () => ({ code: 1, stdout: "", stderr: "not expected" }));
      const res = await runProposalPlanSafeNodes({
        planDir,
        deps: { runCommand },
      });

      expect(res.ok).toBe(true);
      expect(runCommand).not.toHaveBeenCalled();
      expect(res.results[0]?.status).toBe("skipped");
      expect(res.results[0]?.error).toContain("Kaggle credentials");
    } finally {
      if (prevState === undefined) {
        delete process.env.OPENCLAW_STATE_DIR;
      } else {
        process.env.OPENCLAW_STATE_DIR = prevState;
      }
    }
  });

  it("runs Kaggle dataset sampling commands when credentials are present", async () => {
    const prevState = process.env.OPENCLAW_STATE_DIR;
    const prevUser = process.env.KAGGLE_USERNAME;
    const prevKey = process.env.KAGGLE_KEY;
    const tmpState = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-kaggle-"));
    process.env.OPENCLAW_STATE_DIR = tmpState;
    process.env.KAGGLE_USERNAME = "user";
    process.env.KAGGLE_KEY = "key";

    try {
      const planDir = await writePlanPackage({
        dag: {
          nodes: [
            {
              id: "data.sample.owner-ds",
              type: "fetch_dataset_sample",
              tool: "shell",
              outputs: ["cache/data/owner-ds"],
              commands: [
                "openclaw proposal dataset sample --platform kaggle --dataset owner/ds --out cache/data/owner-ds",
              ],
            },
          ],
          edges: [],
        },
        discovery: {
          repos: [],
          datasets: [
            {
              platform: "kaggle",
              resolvedId: "owner/ds",
              resolvedUrl: "https://www.kaggle.com/datasets/owner/ds",
              input: { name: "owner/ds" },
              exists: undefined,
              evidence: [],
              warnings: [],
            },
          ],
        },
        report: {
          planId: "test-plan",
          createdAt: new Date().toISOString(),
          discovery: "sample",
          warnings: [],
          errors: [],
          needsConfirm: [],
        },
      });

      const runCommand = vi.fn(async (argv: string[], opts?: { cwd?: string }) => {
        expect(argv).toEqual([
          "openclaw",
          "proposal",
          "dataset",
          "sample",
          "--platform",
          "kaggle",
          "--dataset",
          "owner/ds",
          "--out",
          "cache/data/owner-ds",
        ]);
        expect(opts?.cwd).toBe(planDir);

        const outDir = path.join(planDir, "cache", "data", "owner-ds");
        await fs.mkdir(outDir, { recursive: true });
        await fs.writeFile(path.join(outDir, "sample.json"), "{}", "utf-8");
        await fs.writeFile(path.join(outDir, "discovered.json"), "{}", "utf-8");

        return { code: 0, stdout: "", stderr: "" };
      });

      const res = await runProposalPlanSafeNodes({
        planDir,
        deps: { runCommand },
      });

      expect(res.ok).toBe(true);
      expect(runCommand).toHaveBeenCalledTimes(1);
      expect(res.results[0]?.status).toBe("ok");
    } finally {
      if (prevState === undefined) {
        delete process.env.OPENCLAW_STATE_DIR;
      } else {
        process.env.OPENCLAW_STATE_DIR = prevState;
      }
      if (prevUser === undefined) {
        delete process.env.KAGGLE_USERNAME;
      } else {
        process.env.KAGGLE_USERNAME = prevUser;
      }
      if (prevKey === undefined) {
        delete process.env.KAGGLE_KEY;
      } else {
        process.env.KAGGLE_KEY = prevKey;
      }
    }
  });

  it("fetches a HF dataset sample even if metadata fetch fails", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "data.sample.squad",
            type: "fetch_dataset_sample",
            tool: "shell",
            outputs: ["cache/data/squad"],
          },
        ],
        edges: [],
      },
      discovery: {
        repos: [],
        datasets: [
          {
            platform: "hf",
            resolvedId: "squad",
            resolvedUrl: "https://huggingface.co/datasets/squad",
            input: { name: "squad" },
            exists: undefined,
            evidence: [],
            warnings: [],
          },
        ],
      },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "sample",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
    });

    const fetchFn = vi.fn(async (input: string) => {
      if (input.startsWith("https://huggingface.co/api/datasets/squad")) {
        return new Response("bad gateway", { status: 502 });
      }
      if (input.startsWith("https://datasets-server.huggingface.co/splits?dataset=squad")) {
        return new Response(JSON.stringify({ splits: [{ config: "default", split: "train" }] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      if (input.startsWith("https://datasets-server.huggingface.co/rows?dataset=squad")) {
        return new Response(JSON.stringify({ rows: [{ row: { id: 1 } }] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      return new Response("not found", { status: 404 });
    });

    const res = await runProposalPlanSafeNodes({
      planDir,
      deps: { fetchFn },
    });

    expect(res.ok).toBe(true);
    expect(res.warnings.some((w) => w.includes("needs_confirm"))).toBe(false);

    const outDir = path.join(planDir, "cache", "data", "squad");
    await expect(fs.stat(path.join(outDir, "sample.json"))).resolves.toBeTruthy();
  });
});
