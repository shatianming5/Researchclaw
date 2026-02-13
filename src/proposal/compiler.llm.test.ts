import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { compileProposal } from "./compiler.js";
import { CompileReportSchema } from "./schema.js";
import { validatePlanDir } from "./validate.js";

describe("proposal/compiler (mock LLM)", () => {
  it("uses an injected LLM client to extract entities and suggest acceptance", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-llm-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const proposalPath = path.join(tmp, "proposal.md");
    await fs.writeFile(
      proposalPath,
      ["# Proposal", "", "Train and evaluate.", ""].join("\n"),
      "utf-8",
    );

    const llmClient = {
      modelKey: "mock/mock-model",
      completeText: async ({ prompt }: { prompt: string }) => {
        if (prompt.includes("<proposal_markdown>")) {
          return JSON.stringify({
            repos: [{ name: "openclaw/openclaw" }],
            datasets: [
              { name: "squad", url: "https://huggingface.co/datasets/squad", platform: "hf" },
            ],
            metrics: [
              { name: "f1", goal: "max", target: 0.8, unit: "score", sourceText: "F1 >= 0.8" },
              { name: "em", goal: "max" },
            ],
            deliverables: ["report/final_report.md", "report/final_metrics.json"],
          });
        }
        if (prompt.includes('"checks"') || prompt.includes("acceptance checks")) {
          // Intentionally omit the value for "em" to ensure compiler forces needs_confirm=true.
          return JSON.stringify({
            checks: [
              {
                type: "metric_threshold",
                selector: "f1",
                op: ">=",
                value: 0.8,
                unit: "score",
                needs_confirm: false,
                suggested_by: "llm",
                evidence: [],
              },
              {
                type: "metric_threshold",
                selector: "em",
                op: ">=",
                needs_confirm: false,
                suggested_by: "llm",
                evidence: [],
              },
            ],
          });
        }
        return JSON.stringify({ repos: [], datasets: [], metrics: [], deliverables: [] });
      },
    };

    const result = await compileProposal({
      proposalPath,
      cfg: {},
      workspaceDir,
      discovery: "off",
      useLlm: true,
      llmClient,
    });

    const reportRaw = await fs.readFile(result.paths.compileReport, "utf-8");
    const report = CompileReportSchema.parse(JSON.parse(reportRaw) as unknown);
    expect(report.model).toBe("mock/mock-model");
    expect(report.needsConfirm.some((i) => i.id === "accept:metric_threshold:em")).toBe(true);

    const acceptance = JSON.parse(await fs.readFile(result.paths.acceptance, "utf-8")) as {
      checks: Array<{ type: string; selector: string; needs_confirm?: boolean }>;
    };
    const emCheck = acceptance.checks.find(
      (c) => c.type === "metric_threshold" && c.selector === "em",
    );
    expect(emCheck?.needs_confirm).toBe(true);

    await expect(
      fs.stat(path.join(result.rootDir, "ir", "extracted.entities.raw.txt")),
    ).resolves.toBeTruthy();
    await expect(
      fs.stat(path.join(result.rootDir, "plan", "acceptance.raw.txt")),
    ).resolves.toBeTruthy();

    const validation = await validatePlanDir(result.rootDir);
    expect(validation.ok).toBe(true);
  });
});
