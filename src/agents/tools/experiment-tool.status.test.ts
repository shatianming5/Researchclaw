import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { createExperimentTool } from "./experiment-tool.js";

async function writePlanDir(): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-experiment-tool-"));
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

describe("experiment tool status", () => {
  it("summarizes artifacts and acceptance for a planDir", async () => {
    const planDir = await writePlanDir();
    const acceptanceReportPath = path.join(planDir, "report", "acceptance_report.json");
    await fs.writeFile(
      acceptanceReportPath,
      JSON.stringify(
        {
          schemaVersion: 1,
          ok: true,
          exitCode: 0,
          createdAt: new Date().toISOString(),
          planId: "test-plan",
          planDir,
          runId: "run-1",
          runDir: path.join(planDir, "report", "runs", "run-1"),
          status: "pass",
          summary: { pass: 1, fail: 0, needs_confirm: 0, total: 1 },
          checks: [],
          artifacts: { manifestPath: "manifest.json", archived: [], missing: [] },
          warnings: [],
          errors: [],
          paths: {
            reportJson: "report/acceptance_report.json",
            reportMd: "report/acceptance_report.md",
            manifestJson: "manifest.json",
          },
        },
        null,
        2,
      ),
      "utf-8",
    );
    await fs.writeFile(
      path.join(planDir, "report", "execute_log.json"),
      JSON.stringify({ results: [{ nodeId: "n1", status: "ok", attempts: [] }] }, null, 2),
      "utf-8",
    );

    const tool = createExperimentTool({ config: {} });
    const result = await tool.execute("call1", { action: "status", planDir });
    const details = result.details as {
      ok?: boolean;
      artifacts?: Record<string, boolean>;
      acceptance?: { status?: string };
      executeSummary?: { ok?: number; total?: number };
    };

    expect(details.ok).toBe(true);
    expect(details.artifacts?.acceptanceReport).toBe(true);
    expect(details.acceptance?.status).toBe("pass");
    expect(details.executeSummary?.ok).toBe(1);
    expect(details.executeSummary?.total).toBe(1);
  });
});
