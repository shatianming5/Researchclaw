import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { finalizeProposalPlan } from "./finalize.js";

async function writePlanPackage(): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-finalize-"));
  await fs.mkdir(path.join(root, "plan"), { recursive: true });
  await fs.mkdir(path.join(root, "report"), { recursive: true });

  await fs.writeFile(
    path.join(root, "plan", "plan.dag.json"),
    JSON.stringify({ nodes: [], edges: [] }, null, 2),
  );
  await fs.writeFile(
    path.join(root, "plan", "acceptance.json"),
    JSON.stringify({ checks: [] }, null, 2),
  );
  await fs.writeFile(
    path.join(root, "plan", "retry.json"),
    JSON.stringify({ policies: [], defaultPolicyId: "retry.unknown" }, null, 2),
  );
  await fs.writeFile(
    path.join(root, "report", "compile_report.json"),
    JSON.stringify(
      {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "plan",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
      null,
      2,
    ),
  );

  return root;
}

describe("proposal/finalize", () => {
  it("writes final metrics and report from eval metrics", async () => {
    const planDir = await writePlanPackage();
    await fs.mkdir(path.join(planDir, "report"), { recursive: true });
    await fs.writeFile(
      path.join(planDir, "report", "eval_metrics.json"),
      JSON.stringify({ metrics: { acc: 1.0, loss: 0.2 } }, null, 2),
      "utf-8",
    );

    const res = await finalizeProposalPlan({ planDir });
    expect(res.ok).toBe(true);
    expect(res.wrote.finalMetrics).toBe(true);
    expect(res.wrote.finalReport).toBe(true);

    const metricsRaw = await fs.readFile(
      path.join(planDir, "report", "final_metrics.json"),
      "utf-8",
    );
    const metrics = JSON.parse(metricsRaw) as {
      schemaVersion: number;
      metrics?: Record<string, unknown>;
    };
    expect(metrics.schemaVersion).toBe(1);
    expect(metrics.metrics?.acc).toBe(1);

    const reportMd = await fs.readFile(path.join(planDir, "report", "final_report.md"), "utf-8");
    expect(reportMd).toContain("## Metrics");
    expect(reportMd).toContain("acc:");
  });

  it("writes placeholder artifacts when eval metrics are missing", async () => {
    const planDir = await writePlanPackage();

    const res = await finalizeProposalPlan({ planDir });
    expect(res.ok).toBe(true);
    expect(res.wrote.finalMetrics).toBe(true);
    expect(res.wrote.finalReport).toBe(true);

    const metricsRaw = await fs.readFile(
      path.join(planDir, "report", "final_metrics.json"),
      "utf-8",
    );
    const metrics = JSON.parse(metricsRaw) as {
      metrics?: Record<string, unknown>;
      notes?: string[];
    };
    expect(metrics.metrics && Object.keys(metrics.metrics).length).toBe(0);
    expect(metrics.notes?.join("\n")).toContain("Missing report/eval_metrics.json");

    const reportMd = await fs.readFile(path.join(planDir, "report", "final_report.md"), "utf-8");
    expect(reportMd).toContain("## Notes");
    expect(reportMd).toContain("Missing report/eval_metrics.json");
  });

  it("does not overwrite existing artifacts without --force", async () => {
    const planDir = await writePlanPackage();
    await fs.mkdir(path.join(planDir, "report"), { recursive: true });
    await fs.writeFile(
      path.join(planDir, "report", "final_metrics.json"),
      JSON.stringify({ schemaVersion: 1, metrics: { old: 1 }, notes: [] }, null, 2),
      "utf-8",
    );
    await fs.writeFile(path.join(planDir, "report", "final_report.md"), "old\n", "utf-8");

    const res = await finalizeProposalPlan({ planDir, opts: { force: false } });
    expect(res.ok).toBe(true);
    expect(res.wrote.finalMetrics).toBe(false);
    expect(res.wrote.finalReport).toBe(false);

    const metricsRaw = await fs.readFile(
      path.join(planDir, "report", "final_metrics.json"),
      "utf-8",
    );
    expect(metricsRaw).toContain('"old": 1');

    const reportMd = await fs.readFile(path.join(planDir, "report", "final_report.md"), "utf-8");
    expect(reportMd).toBe("old\n");
  });
});
