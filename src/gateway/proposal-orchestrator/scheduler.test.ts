import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { loadConfig } from "../../config/config.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { GpuScheduler } from "../gpu-scheduler/scheduler.js";
import { NodeRegistry } from "../node-registry.js";
import { ProposalOrchestrator } from "./scheduler.js";

describe("ProposalOrchestrator", () => {
  it("runs compile + run (dryRun) without opencode", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-proposal-orch-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const nodeRegistry = new NodeRegistry();
    const gpuScheduler = new GpuScheduler({
      nodeRegistry,
      loadConfig,
      log: createSubsystemLogger("test/gpu-scheduler"),
      config: { persist: false, enabled: false },
    });

    const orchestrator = new ProposalOrchestrator({
      nodeRegistry,
      gpuScheduler,
      loadConfig,
      log: createSubsystemLogger("test/proposal-orchestrator"),
      config: { persist: false, pollIntervalMs: 10 },
    });
    await orchestrator.start();

    const job = await orchestrator.submit({
      proposalMarkdown: ["# Proposal", "", "Repo: openclaw/openclaw", ""].join("\n"),
      compile: { discovery: "off", useLlm: false, workspaceDir },
      steps: { refine: false, execute: false, finalize: false, accept: false },
      run: { dryRun: true },
    });

    const done = await orchestrator.wait(job.jobId, 60_000);
    expect(done).toBeTruthy();
    expect(done?.state).toBe("succeeded");
    expect(done?.planDir).toBeTruthy();
    expect(done?.steps.compile.status).toBe("succeeded");
    expect(done?.steps.run.status).toBe("succeeded");
    expect(done?.steps.refine.status).toBe("skipped");

    orchestrator.stop();
  });
});
