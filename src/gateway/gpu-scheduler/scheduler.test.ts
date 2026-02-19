import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { GatewayWsClient } from "../server/ws-types.js";
import { NodeRegistry, type NodeInvokeResult, type NodeSession } from "../node-registry.js";
import { writeGpuSchedulerState } from "./persist.js";
import { GpuScheduler } from "./scheduler.js";

function makeStubClient(): GatewayWsClient {
  return {
    socket: { send: vi.fn() } as unknown as GatewayWsClient["socket"],
    connect: {
      minProtocol: 1,
      maxProtocol: 1,
      client: { id: "node", version: "dev", platform: "linux", mode: "node" },
      role: "node",
      scopes: [],
      caps: [],
      commands: ["system.run"],
    } as unknown as GatewayWsClient["connect"],
    connId: "c1",
  };
}

function makeSession(params: { nodeId: string; gpuCount: number }): NodeSession {
  return {
    nodeId: params.nodeId,
    connId: `conn:${params.nodeId}`,
    client: makeStubClient(),
    caps: [],
    commands: ["system.run"],
    resources: { gpuCount: params.gpuCount, gpuType: "nvidia", gpuMemGB: 24 },
    connectedAtMs: Date.now(),
  };
}

class TestNodeRegistry extends NodeRegistry {
  nodes: NodeSession[] = [];
  invokes: Array<{ nodeId: string; command: string; params?: unknown }> = [];

  override listConnected(): NodeSession[] {
    return this.nodes;
  }

  override async invoke(params: {
    nodeId: string;
    command: string;
    params?: unknown;
    timeoutMs?: number;
    idempotencyKey?: string;
  }): Promise<NodeInvokeResult> {
    this.invokes.push({ nodeId: params.nodeId, command: params.command, params: params.params });
    const payload = {
      success: true,
      stdout: `ok:${params.nodeId}`,
      stderr: "",
      exitCode: 0,
      timedOut: false,
    };
    return { ok: true, payloadJSON: JSON.stringify(payload) };
  }
}

describe("GpuScheduler", () => {
  it("schedules on best-fit GPU node", async () => {
    const nodeRegistry = new TestNodeRegistry();
    nodeRegistry.nodes = [
      makeSession({ nodeId: "gpu-1", gpuCount: 1 }),
      makeSession({ nodeId: "gpu-4", gpuCount: 4 }),
    ];

    const scheduler = new GpuScheduler({
      nodeRegistry,
      loadConfig: () => ({}) as unknown as import("../../config/config.js").OpenClawConfig,
      log: {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        debug: vi.fn(),
      } as unknown as ReturnType<typeof import("../../logging/subsystem.js").createSubsystemLogger>,
      config: { persist: false, pollIntervalMs: 0, maxConcurrentJobs: 10 },
    });

    const job = await scheduler.submit({
      resources: { gpuCount: 1 },
      exec: { command: ["sh", "-lc", "echo hi"] },
    });

    const done = await scheduler.wait(job.jobId, 5000);
    expect(done?.state).toBe("succeeded");
    expect(nodeRegistry.invokes[0]).toMatchObject({ nodeId: "gpu-1", command: "system.run" });
    scheduler.stop();
  });

  it("does not oversubscribe GPUs on a node", async () => {
    const nodeRegistry = new TestNodeRegistry();
    nodeRegistry.nodes = [
      makeSession({ nodeId: "gpu-1", gpuCount: 1 }),
      makeSession({ nodeId: "gpu-2", gpuCount: 2 }),
    ];

    const scheduler = new GpuScheduler({
      nodeRegistry,
      loadConfig: () => ({}) as unknown as import("../../config/config.js").OpenClawConfig,
      log: {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        debug: vi.fn(),
      } as unknown as ReturnType<typeof import("../../logging/subsystem.js").createSubsystemLogger>,
      config: { persist: false, pollIntervalMs: 0, maxConcurrentJobs: 10 },
    });

    const job1 = await scheduler.submit({
      resources: { gpuCount: 1 },
      exec: { command: ["sh", "-lc", "echo 1"] },
    });
    const job2 = await scheduler.submit({
      resources: { gpuCount: 1 },
      exec: { command: ["sh", "-lc", "echo 2"] },
    });

    const [done1, done2] = await Promise.all([
      scheduler.wait(job1.jobId, 5000),
      scheduler.wait(job2.jobId, 5000),
    ]);
    expect(done1?.state).toBe("succeeded");
    expect(done2?.state).toBe("succeeded");

    const invocations = nodeRegistry.invokes.map((i) => i.nodeId);
    expect(invocations.filter((id) => id === "gpu-1").length).toBe(1);
    scheduler.stop();
  });

  it("supports pause/resume for queued jobs", async () => {
    const nodeRegistry = new TestNodeRegistry();
    nodeRegistry.nodes = [makeSession({ nodeId: "gpu-1", gpuCount: 1 })];

    const scheduler = new GpuScheduler({
      nodeRegistry,
      loadConfig: () => ({}) as unknown as import("../../config/config.js").OpenClawConfig,
      log: {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        debug: vi.fn(),
      } as unknown as ReturnType<typeof import("../../logging/subsystem.js").createSubsystemLogger>,
      config: { persist: false, pollIntervalMs: 0, maxConcurrentJobs: 1 },
    });

    const job1 = await scheduler.submit({
      resources: { gpuCount: 1 },
      exec: { command: ["sh", "-lc", "echo job1"] },
    });
    const paused = await scheduler.pause(job1.jobId);
    expect(paused.ok).toBe(true);

    const job2 = await scheduler.submit({
      resources: { gpuCount: 1 },
      exec: { command: ["sh", "-lc", "echo job2"] },
    });

    const done2 = await scheduler.wait(job2.jobId, 5000);
    expect(done2?.state).toBe("succeeded");
    expect(nodeRegistry.invokes[0]?.params).toMatchObject({
      command: ["sh", "-lc", "echo job2"],
    });

    expect(scheduler.get(job1.jobId)?.state).toBe("queued");
    expect(scheduler.get(job1.jobId)?.paused).toBe(true);

    const resumed = await scheduler.resume(job1.jobId);
    expect(resumed.ok).toBe(true);

    const done1 = await scheduler.wait(job1.jobId, 5000);
    expect(done1?.state).toBe("succeeded");
    expect(nodeRegistry.invokes[1]?.params).toMatchObject({
      command: ["sh", "-lc", "echo job1"],
    });
    scheduler.stop();
  });

  it("supports pause/resume for running jobs by preempting and requeuing", async () => {
    class BlockingNodeRegistry extends NodeRegistry {
      nodes: NodeSession[] = [];
      invokes: Array<{ nodeId: string; command: string; params?: unknown }> = [];
      private resolveQueue: Array<(value: NodeInvokeResult) => void> = [];

      override listConnected(): NodeSession[] {
        return this.nodes;
      }

      override async invoke(params: {
        nodeId: string;
        command: string;
        params?: unknown;
        timeoutMs?: number;
        idempotencyKey?: string;
      }): Promise<NodeInvokeResult> {
        this.invokes.push({
          nodeId: params.nodeId,
          command: params.command,
          params: params.params,
        });
        return await new Promise<NodeInvokeResult>((resolve) => {
          this.resolveQueue.push(resolve);
        });
      }

      resolveNext(value: NodeInvokeResult): void {
        const resolve = this.resolveQueue.shift();
        if (resolve) {
          resolve(value);
        }
      }
    }

    const nodeRegistry = new BlockingNodeRegistry();
    nodeRegistry.nodes = [makeSession({ nodeId: "gpu-1", gpuCount: 1 })];

    const planDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gpu-sched-pause-run-"));

    const scheduler = new GpuScheduler({
      nodeRegistry,
      loadConfig: () => ({}) as unknown as import("../../config/config.js").OpenClawConfig,
      log: {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        debug: vi.fn(),
      } as unknown as ReturnType<typeof import("../../logging/subsystem.js").createSubsystemLogger>,
      config: { persist: false, pollIntervalMs: 0, maxConcurrentJobs: 1 },
    });

    const job = await scheduler.submit({
      resources: { gpuCount: 1 },
      exec: { command: ["sh", "-lc", "echo hi"], env: { OPENCLAW_PLAN_DIR: planDir } },
    });

    // Allow the scheduler to dispatch the job.
    await new Promise((resolve) => setTimeout(resolve, 75));
    expect(scheduler.get(job.jobId)?.state).toBe("running");

    const paused = await scheduler.pause(job.jobId);
    expect(paused.ok).toBe(true);

    const pauseMarkerPath = path.join(
      planDir,
      "report",
      "gpu_scheduler",
      "jobs",
      job.jobId,
      "attempt-1",
      "pause.requested",
    );
    const markerText = await fs.readFile(pauseMarkerPath, "utf8");
    expect(markerText).toContain("pauseRequestedAtMs=");

    nodeRegistry.resolveNext({
      ok: true,
      payloadJSON: JSON.stringify({
        success: false,
        stdout: "",
        stderr: "",
        exitCode: 143,
        timedOut: false,
      }),
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(scheduler.get(job.jobId)?.state).toBe("queued");
    expect(scheduler.get(job.jobId)?.paused).toBe(true);

    const resumed = await scheduler.resume(job.jobId);
    expect(resumed.ok).toBe(true);

    await new Promise((resolve) => setTimeout(resolve, 75));
    expect(scheduler.get(job.jobId)?.state).toBe("running");

    nodeRegistry.resolveNext({
      ok: true,
      payloadJSON: JSON.stringify({
        success: true,
        stdout: "ok",
        stderr: "",
        exitCode: 0,
        timedOut: false,
      }),
    });

    const done = await scheduler.wait(job.jobId, 5000);
    expect(done?.state).toBe("succeeded");
    scheduler.stop();
  });

  it("wraps sh -lc commands when OPENCLAW_PLAN_DIR is set (for restart recovery)", async () => {
    const nodeRegistry = new TestNodeRegistry();
    nodeRegistry.nodes = [makeSession({ nodeId: "gpu-1", gpuCount: 1 })];

    const planDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gpu-sched-plan-"));

    const scheduler = new GpuScheduler({
      nodeRegistry,
      loadConfig: () => ({}) as unknown as import("../../config/config.js").OpenClawConfig,
      log: {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        debug: vi.fn(),
      } as unknown as ReturnType<typeof import("../../logging/subsystem.js").createSubsystemLogger>,
      config: { persist: false, pollIntervalMs: 0, maxConcurrentJobs: 1 },
    });

    const job = await scheduler.submit({
      resources: { gpuCount: 1 },
      exec: {
        command: ["sh", "-lc", "echo hi"],
        env: { OPENCLAW_PLAN_DIR: planDir },
      },
    });
    const done = await scheduler.wait(job.jobId, 5000);
    expect(done?.state).toBe("succeeded");

    const invoke = nodeRegistry.invokes[0];
    expect(invoke?.command).toBe("system.run");
    const invokeParams =
      invoke?.params && typeof invoke.params === "object"
        ? (invoke.params as Record<string, unknown>)
        : {};
    expect(invokeParams.command).toBeDefined();
    const argv = Array.isArray(invokeParams.command) ? invokeParams.command : [];
    expect(argv[0]).toBe("sh");
    expect(argv[1]).toBe("-lc");
    expect(typeof argv[2]).toBe("string");
    const script = String(argv[2] ?? "");
    expect(script).toContain("gpu_scheduler/jobs");
    expect(script).toContain(job.jobId);
    expect(script).toContain("pause.requested");

    const env = invokeParams.env && typeof invokeParams.env === "object" ? invokeParams.env : {};
    expect((env as Record<string, unknown>).OPENCLAW_GPU_SCHEDULER_WRAPPED).toBe("1");
    scheduler.stop();
  });

  it("recovers running jobs on restart via exit.json", async () => {
    const nodeRegistry = new TestNodeRegistry();
    nodeRegistry.nodes = [];

    const root = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gpu-sched-recover-"));
    const planDir = path.join(root, "plan");
    const statePath = path.join(root, "state.json");

    const jobId = "job-recover-exit";
    const monitorDir = path.join(planDir, "report", "gpu_scheduler", "jobs", jobId, "attempt-1");
    await fs.mkdir(monitorDir, { recursive: true });
    await fs.writeFile(path.join(monitorDir, "stdout.txt"), "hello\n", "utf8");
    await fs.writeFile(path.join(monitorDir, "stderr.txt"), "", "utf8");
    await fs.writeFile(
      path.join(monitorDir, "exit.json"),
      JSON.stringify(
        {
          schemaVersion: 1,
          jobId,
          attempt: 1,
          startedAtMs: Date.now() - 50,
          finishedAtMs: Date.now(),
          exitCode: 0,
          timedOut: false,
          success: true,
        },
        null,
        2,
      ),
      "utf8",
    );

    await writeGpuSchedulerState(
      {
        version: 1,
        jobs: [
          {
            jobId,
            createdAtMs: Date.now() - 1000,
            updatedAtMs: Date.now() - 1000,
            state: "running",
            resources: { gpuCount: 1 },
            exec: {
              command: ["sh", "-lc", "echo hi"],
              env: { OPENCLAW_PLAN_DIR: planDir },
            },
            maxAttempts: 1,
            assignedNodeId: "gpu-1",
            attempts: [{ attempt: 1, nodeId: "gpu-1", startedAtMs: Date.now() - 500 }],
          },
        ],
      },
      { statePath },
    );

    const scheduler = new GpuScheduler({
      nodeRegistry,
      loadConfig: () => ({}) as unknown as import("../../config/config.js").OpenClawConfig,
      log: {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        debug: vi.fn(),
      } as unknown as ReturnType<typeof import("../../logging/subsystem.js").createSubsystemLogger>,
      config: { persist: true, persistPath: statePath, pollIntervalMs: 0, maxConcurrentJobs: 1 },
    });

    await scheduler.start();
    const done = await scheduler.wait(jobId, 5000);
    expect(done?.state).toBe("succeeded");
    expect(done?.result?.success).toBe(true);
    expect(done?.result?.stdoutTail).toContain("hello");
    expect(nodeRegistry.invokes.length).toBe(0);
    scheduler.stop();
  });

  it("requeues recovered jobs when heartbeat is stale or missing", async () => {
    const nodeRegistry = new TestNodeRegistry();
    nodeRegistry.nodes = [];

    const root = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gpu-sched-stale-"));
    const planDir = path.join(root, "plan");
    const statePath = path.join(root, "state.json");

    const jobId = "job-recover-stale";
    await writeGpuSchedulerState(
      {
        version: 1,
        jobs: [
          {
            jobId,
            createdAtMs: Date.now() - 1000,
            updatedAtMs: Date.now() - 1000,
            state: "running",
            resources: { gpuCount: 1 },
            exec: {
              command: ["sh", "-lc", "echo hi"],
              env: { OPENCLAW_PLAN_DIR: planDir },
            },
            maxAttempts: 2,
            assignedNodeId: "gpu-1",
            attempts: [{ attempt: 1, nodeId: "gpu-1", startedAtMs: Date.now() - 500 }],
          },
        ],
      },
      { statePath },
    );

    const scheduler = new GpuScheduler({
      nodeRegistry,
      loadConfig: () => ({}) as unknown as import("../../config/config.js").OpenClawConfig,
      log: {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        debug: vi.fn(),
      } as unknown as ReturnType<typeof import("../../logging/subsystem.js").createSubsystemLogger>,
      config: { persist: true, persistPath: statePath, pollIntervalMs: 0, maxConcurrentJobs: 1 },
    });

    await scheduler.start();
    await new Promise((resolve) => setTimeout(resolve, 100));
    const job = scheduler.get(jobId);
    expect(job?.state).toBe("queued");
    expect(job?.assignedNodeId).toBeUndefined();
    expect(job?.attempts[0]?.error).toContain("heartbeat stale");
    scheduler.stop();
  });
});
