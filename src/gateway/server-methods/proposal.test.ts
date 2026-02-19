import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { loadConfig } from "../../config/config.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { GpuScheduler } from "../gpu-scheduler/scheduler.js";
import { NodeRegistry } from "../node-registry.js";
import { ProposalOrchestrator } from "../proposal-orchestrator/scheduler.js";
import { ErrorCodes } from "../protocol/index.js";
import { proposalHandlers } from "./proposal.js";

const noop = () => false;

describe("gateway proposal.compile", () => {
  it("compiles a plan package from markdown", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gateway-proposal-"));
    const workspaceDir = path.join(tmp, "ws");

    const respond = vi.fn();
    await proposalHandlers["proposal.compile"]({
      params: {
        proposalMarkdown: ["# Proposal", "", "Repo: openclaw/openclaw", ""].join("\n"),
        discovery: "off",
        workspaceDir,
        useLlm: false,
      },
      respond,
      context: {} as unknown as Parameters<
        (typeof proposalHandlers)["proposal.compile"]
      >[0]["context"],
      client: null,
      req: { id: "req-1", type: "req", method: "proposal.compile" },
      isWebchatConnect: noop,
    });

    expect(respond).toHaveBeenCalledTimes(1);
    const [ok, payload, error] = respond.mock.calls[0] ?? [];
    expect(ok).toBe(true);
    expect(error).toBeUndefined();
    expect(payload).toEqual(
      expect.objectContaining({
        ok: expect.any(Boolean),
        planId: expect.any(String),
        rootDir: expect.any(String),
        report: expect.any(Object),
        paths: expect.any(Object),
      }),
    );

    const rootDir = payload.rootDir;
    expect(rootDir).toContain(path.join(workspaceDir, "experiments", "workdir"));

    await expect(fs.stat(path.join(rootDir, "plan", "plan.dag.json"))).resolves.toBeTruthy();
    await expect(
      fs.stat(path.join(rootDir, "report", "compile_report.json")),
    ).resolves.toBeTruthy();
  });

  it("rejects missing proposalMarkdown", async () => {
    const respond = vi.fn();
    await proposalHandlers["proposal.compile"]({
      params: { discovery: "off", useLlm: false },
      respond,
      context: {} as unknown as Parameters<
        (typeof proposalHandlers)["proposal.compile"]
      >[0]["context"],
      client: null,
      req: { id: "req-2", type: "req", method: "proposal.compile" },
      isWebchatConnect: noop,
    });

    expect(respond).toHaveBeenCalledTimes(1);
    const [ok, _payload, error] = respond.mock.calls[0] ?? [];
    expect(ok).toBe(false);
    expect(error).toEqual(
      expect.objectContaining({
        code: ErrorCodes.INVALID_REQUEST,
      }),
    );
  });

  it("rejects invalid discovery mode", async () => {
    const respond = vi.fn();
    await proposalHandlers["proposal.compile"]({
      params: {
        proposalMarkdown: "# Proposal",
        discovery: "nope",
        useLlm: false,
      },
      respond,
      context: {} as unknown as Parameters<
        (typeof proposalHandlers)["proposal.compile"]
      >[0]["context"],
      client: null,
      req: { id: "req-3", type: "req", method: "proposal.compile" },
      isWebchatConnect: noop,
    });

    expect(respond).toHaveBeenCalledTimes(1);
    const [ok, _payload, error] = respond.mock.calls[0] ?? [];
    expect(ok).toBe(false);
    expect(error).toEqual(
      expect.objectContaining({
        code: ErrorCodes.INVALID_REQUEST,
      }),
    );
  });
});

describe("gateway proposal pipeline methods", () => {
  async function compilePlan(workspaceDir: string): Promise<string> {
    const respond = vi.fn();
    await proposalHandlers["proposal.compile"]({
      params: {
        proposalMarkdown: ["# Proposal", "", "Repo: openclaw/openclaw", ""].join("\n"),
        discovery: "off",
        workspaceDir,
        useLlm: false,
      },
      respond,
      context: {} as unknown as Parameters<
        (typeof proposalHandlers)["proposal.compile"]
      >[0]["context"],
      client: null,
      req: { id: "compile", type: "req", method: "proposal.compile" },
      isWebchatConnect: noop,
    });
    const [ok, payload, error] = respond.mock.calls[0] ?? [];
    expect(ok).toBe(true);
    expect(error).toBeUndefined();
    return (payload as { rootDir: string }).rootDir;
  }

  function createContext() {
    const nodeRegistry = new NodeRegistry();
    const gpuScheduler = new GpuScheduler({
      nodeRegistry,
      loadConfig,
      log: createSubsystemLogger("test/gateway-proposal/gpu"),
      config: { enabled: false, persist: false },
    });
    const proposalOrchestrator = new ProposalOrchestrator({
      nodeRegistry,
      gpuScheduler,
      loadConfig,
      log: createSubsystemLogger("test/gateway-proposal/orchestrator"),
      config: { enabled: true, persist: false, pollIntervalMs: 10 },
    });
    return { nodeRegistry, gpuScheduler, proposalOrchestrator };
  }

  it("runs proposal.run (dryRun)", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gateway-proposal-run-"));
    const workspaceDir = path.join(tmp, "ws");
    const rootDir = await compilePlan(workspaceDir);

    const respond = vi.fn();
    await proposalHandlers["proposal.run"]({
      params: {
        planDir: rootDir,
        dryRun: true,
      },
      respond,
      context: {} as unknown as Parameters<(typeof proposalHandlers)["proposal.run"]>[0]["context"],
      client: null,
      req: { id: "run", type: "req", method: "proposal.run" },
      isWebchatConnect: noop,
    });

    const [ok, payload, error] = respond.mock.calls[0] ?? [];
    expect(ok).toBe(true);
    expect(error).toBeUndefined();
    expect(payload).toEqual(expect.objectContaining({ ok: true, planDir: rootDir }));
    await expect(fs.stat(path.join(rootDir, "report", "run_log.json"))).resolves.toBeTruthy();
  });

  it("runs proposal.execute (dryRun)", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gateway-proposal-exec-"));
    const workspaceDir = path.join(tmp, "ws");
    const rootDir = await compilePlan(workspaceDir);

    const ctx = createContext();
    const respond = vi.fn();
    await proposalHandlers["proposal.execute"]({
      params: {
        planDir: rootDir,
        dryRun: true,
        sandbox: false,
        repair: false,
      },
      respond,
      context: ctx as unknown as Parameters<
        (typeof proposalHandlers)["proposal.execute"]
      >[0]["context"],
      client: null,
      req: { id: "execute", type: "req", method: "proposal.execute" },
      isWebchatConnect: noop,
    });

    const [ok, payload, error] = respond.mock.calls[0] ?? [];
    expect(ok).toBe(true);
    expect(error).toBeUndefined();
    expect(payload).toEqual(expect.objectContaining({ ok: true, planDir: rootDir }));
    await expect(fs.stat(path.join(rootDir, "report", "execute_log.json"))).resolves.toBeTruthy();
  });

  it("submits and waits for proposal.job.*", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gateway-proposal-job-"));
    const workspaceDir = path.join(tmp, "ws");
    await fs.mkdir(workspaceDir, { recursive: true });

    const ctx = createContext();
    await ctx.proposalOrchestrator.start();

    const submitRespond = vi.fn();
    await proposalHandlers["proposal.job.submit"]({
      params: {
        proposalMarkdown: ["# Proposal", "", "Repo: openclaw/openclaw", ""].join("\n"),
        compile: { discovery: "off", useLlm: false, workspaceDir },
        steps: { refine: false, execute: false, finalize: false, accept: false },
        run: { dryRun: true },
      },
      respond: submitRespond,
      context: ctx as unknown as Parameters<
        (typeof proposalHandlers)["proposal.job.submit"]
      >[0]["context"],
      client: null,
      req: { id: "submit", type: "req", method: "proposal.job.submit" },
      isWebchatConnect: noop,
    });
    const [submitOk, submitPayload, submitError] = submitRespond.mock.calls[0] ?? [];
    expect(submitOk).toBe(true);
    expect(submitError).toBeUndefined();

    const jobId = (submitPayload as { job: { jobId: string } }).job.jobId;
    expect(jobId).toBeTruthy();

    const waitRespond = vi.fn();
    await proposalHandlers["proposal.job.wait"]({
      params: { jobId, timeoutMs: 60_000 },
      respond: waitRespond,
      context: ctx as unknown as Parameters<
        (typeof proposalHandlers)["proposal.job.wait"]
      >[0]["context"],
      client: null,
      req: { id: "wait", type: "req", method: "proposal.job.wait" },
      isWebchatConnect: noop,
    });
    const [waitOk, waitPayload, waitError] = waitRespond.mock.calls[0] ?? [];
    expect(waitOk).toBe(true);
    expect(waitError).toBeUndefined();
    expect(waitPayload).toEqual(
      expect.objectContaining({
        done: true,
        job: expect.objectContaining({ jobId, state: "succeeded" }),
      }),
    );

    ctx.proposalOrchestrator.stop();
  });
});
