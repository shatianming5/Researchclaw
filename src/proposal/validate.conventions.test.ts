import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { validatePlanDir } from "./validate.js";

async function writePlanPackage(params: {
  dag: unknown;
  acceptance?: unknown;
  retry?: unknown;
  report: unknown;
}): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-validate-"));
  await fs.mkdir(path.join(root, "plan"), { recursive: true });
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
    path.join(root, "report", "compile_report.json"),
    JSON.stringify(params.report, null, 2),
  );

  return root;
}

describe("proposal/validate conventions", () => {
  it("fails when setup caches are not declared as outputs", async () => {
    const planDir = await writePlanPackage({
      dag: {
        nodes: [
          {
            id: "setup.venv",
            type: "setup_venv",
            tool: "shell",
            outputs: ["cache/venv/repo"],
            commands: ["echo ok"],
          },
          {
            id: "train.run",
            type: "train",
            tool: "shell",
            outputs: ["artifacts/model/repo"],
            commands: ["echo ok"],
          },
        ],
        edges: [{ from: "setup.venv", to: "train.run" }],
      },
      report: {
        planId: "test-plan",
        createdAt: new Date().toISOString(),
        discovery: "plan",
        warnings: [],
        errors: [],
        needsConfirm: [],
      },
    });

    const res = await validatePlanDir(planDir);
    expect(res.ok).toBe(false);
    expect(res.errors.join("\n")).toContain("setup.venv: outputs must include cache/hf");
    expect(res.errors.join("\n")).toContain("setup.venv: outputs must include cache/pip");
  });
});
