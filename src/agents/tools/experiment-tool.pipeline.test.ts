import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { createExperimentTool } from "./experiment-tool.js";

describe("experiment tool pipeline", () => {
  it("runs a dry-run pipeline without refine", async () => {
    const workspaceDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-exp-workspace-"));

    const tool = createExperimentTool({ config: {}, workspaceDir });
    const result = await tool.execute("call1", {
      action: "pipeline",
      proposalMarkdown: "# Test Proposal\n",
      discovery: "off",
      useLlm: false,
      dryRun: true,
      refine: false,
      workspaceDir,
      maxAttempts: 1,
      commandTimeoutMs: 5_000,
    });

    const details = result.details as {
      ok?: boolean;
      planDir?: string;
      compile?: { ok?: boolean; rootDir?: string };
      refine?: { skipped?: boolean };
      accept?: { status?: string; exitCode?: number };
    };

    expect(details.ok).toBe(true);
    expect(details.compile?.ok).toBe(true);
    expect(details.planDir?.startsWith(workspaceDir)).toBe(true);
    expect(details.refine?.skipped).toBe(true);
    expect(details.accept?.status).toBe("pass");
    expect(details.accept?.exitCode).toBe(0);
  });
});
