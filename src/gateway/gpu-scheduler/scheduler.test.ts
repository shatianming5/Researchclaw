import { describe, expect, it, vi } from "vitest";
import type { GatewayWsClient } from "../server/ws-types.js";
import { NodeRegistry, type NodeInvokeResult, type NodeSession } from "../node-registry.js";
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
  invokes: Array<{ nodeId: string; command: string }> = [];

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
    this.invokes.push({ nodeId: params.nodeId, command: params.command });
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
    expect(nodeRegistry.invokes[0]).toEqual({ nodeId: "gpu-1", command: "system.run" });
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
  });
});
