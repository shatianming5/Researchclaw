import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { acceptProposalResults } from "./index.js";

async function writePlanPackage(params: {
  dag?: unknown;
  acceptance: unknown;
  retry?: unknown;
  report?: unknown;
  proposal?: string;
  metrics?: unknown;
  finalReport?: string;
  executeLog?: unknown;
  manualApprovals?: unknown;
}): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-accept-"));
  await fs.mkdir(path.join(root, "plan"), { recursive: true });
  await fs.mkdir(path.join(root, "ir"), { recursive: true });
  await fs.mkdir(path.join(root, "report"), { recursive: true });
  await fs.mkdir(path.join(root, "input"), { recursive: true });

  await fs.writeFile(
    path.join(root, "plan", "plan.dag.json"),
    JSON.stringify(
      params.dag ?? {
        nodes: [{ id: "train.run", type: "train", tool: "manual", inputs: [], outputs: [] }],
        edges: [],
      },
      null,
      2,
    ),
  );
  await fs.writeFile(
    path.join(root, "plan", "acceptance.json"),
    JSON.stringify(params.acceptance, null, 2),
  );
  await fs.writeFile(
    path.join(root, "plan", "retry.json"),
    JSON.stringify(params.retry ?? { policies: [], defaultPolicyId: "retry.unknown" }, null, 2),
  );
  await fs.writeFile(
    path.join(root, "report", "compile_report.json"),
    JSON.stringify(
      params.report ?? {
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
  await fs.writeFile(
    path.join(root, "input", "proposal.md"),
    params.proposal ?? "# Proposal\n\nTest.\n",
  );
  await fs.writeFile(path.join(root, "input", "context.json"), JSON.stringify({ agentId: "main" }));

  if (params.metrics !== undefined) {
    await fs.writeFile(
      path.join(root, "report", "final_metrics.json"),
      `${JSON.stringify(params.metrics, null, 2)}\n`,
    );
  }
  if (params.finalReport !== undefined) {
    await fs.writeFile(path.join(root, "report", "final_report.md"), params.finalReport);
  }
  if (params.executeLog !== undefined) {
    await fs.writeFile(
      path.join(root, "report", "execute_log.json"),
      `${JSON.stringify(params.executeLog, null, 2)}\n`,
    );
  }
  if (params.manualApprovals !== undefined) {
    await fs.writeFile(
      path.join(root, "report", "manual_approvals.json"),
      `${JSON.stringify(params.manualApprovals, null, 2)}\n`,
    );
  }

  return root;
}

describe("proposal/results accept", () => {
  it("passes when all checks are satisfied", async () => {
    const planDir = await writePlanPackage({
      acceptance: {
        checks: [
          { type: "artifact_exists", selector: "report/final_metrics.json" },
          { type: "artifact_exists", selector: "report/final_report.md" },
          { type: "metric_threshold", selector: "accuracy", op: ">=", value: 0.8 },
          { type: "command_exit_code", selector: "train.run", op: "==", value: 0 },
        ],
      },
      metrics: { metrics: { accuracy: 0.9 } },
      finalReport: "# Final\n\nok\n",
      executeLog: {
        results: [{ nodeId: "train.run", type: "train", attempts: [{ ok: true, exitCode: 0 }] }],
      },
    });

    const res = await acceptProposalResults({ planDir });

    expect(res.ok).toBe(true);
    expect(res.status).toBe("pass");
    expect(res.exitCode).toBe(0);

    await expect(fs.stat(res.paths.reportJson)).resolves.toBeTruthy();
    await expect(fs.stat(res.paths.reportMd)).resolves.toBeTruthy();
    await expect(fs.stat(res.paths.manifestJson)).resolves.toBeTruthy();

    const manifestRaw = await fs.readFile(res.paths.manifestJson, "utf-8");
    const manifest = JSON.parse(manifestRaw) as { entries?: Array<{ path?: string }> };
    const paths = new Set((manifest.entries ?? []).map((e) => e.path ?? ""));
    expect(paths.has("report/final_metrics.json")).toBe(true);
    expect(paths.has("report/final_report.md")).toBe(true);
    expect(paths.has("report/acceptance_report.json")).toBe(true);
  });

  it("returns needs_confirm when manual approval is missing", async () => {
    const planDir = await writePlanPackage({
      acceptance: {
        checks: [{ type: "manual_approval", selector: "approve:budget" }],
      },
    });

    const res = await acceptProposalResults({ planDir });
    expect(res.ok).toBe(false);
    expect(res.status).toBe("needs_confirm");
    expect(res.exitCode).toBe(2);
  });

  it("fails when required artifacts are missing", async () => {
    const planDir = await writePlanPackage({
      acceptance: {
        checks: [
          { type: "artifact_exists", selector: "report/final_report.md" },
          { type: "artifact_exists", selector: "report/final_metrics.json" },
        ],
      },
      metrics: { accuracy: 0.9 },
    });

    const res = await acceptProposalResults({ planDir });
    expect(res.ok).toBe(false);
    expect(res.status).toBe("fail");
    expect(res.exitCode).toBe(1);
    expect(res.errors.length).toBe(0);
  });
});
