import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { PlanNode, RetrySpec } from "../schema.js";
import type { GatewayCallLike } from "./types.js";
import { runGpuNodeViaScheduler } from "./gpu.js";

function buildNode(overrides?: Partial<PlanNode>): PlanNode {
  return {
    id: "train.run",
    type: "train",
    tool: "shell",
    inputs: ["cache/git/repo"],
    outputs: ["artifacts/model/repo", "report/train_metrics.jsonl"],
    commands: ["echo hello"],
    resources: { gpuCount: 1 },
    ...overrides,
  };
}

const emptyRetry: RetrySpec = { policies: [] };

describe("proposal/execute gpu wait", () => {
  it("submits a scheduler job even when no GPU nodes are connected", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gpu-wait-"));

    const calls: Array<Parameters<GatewayCallLike>[0]> = [];
    const callGateway: GatewayCallLike = async <T = Record<string, unknown>>(
      req: Parameters<GatewayCallLike>[0],
    ) => {
      calls.push(req);
      if (req.method === "node.list") {
        throw new Error("should not call node.list for scheduler jobs");
      }
      if (req.method === "gpu.job.submit") {
        return { job: { jobId: "job-1" } } as T;
      }
      if (req.method === "gpu.job.wait") {
        const now = Date.now();
        return {
          done: true,
          job: {
            jobId: "job-1",
            state: "succeeded",
            attempts: [
              {
                startedAtMs: now,
                finishedAtMs: now + 1,
                ok: true,
                exitCode: 0,
                stdoutTail: "ok",
                stderrTail: "",
              },
            ],
          },
        } as T;
      }
      throw new Error(`unexpected gateway method: ${req.method}`);
    };

    const res = await runGpuNodeViaScheduler({
      planDir: tmp,
      node: buildNode(),
      dryRun: false,
      commandTimeoutMs: 10_000,
      maxAttempts: 1,
      retryDelayMs: 1,
      retrySpec: emptyRetry,
      gatewayTimeoutMs: 10_000,
      invokeTimeoutMs: 10_000,
      gpuWaitTimeoutMs: 5_000,
      nodeApprove: "off",
      callGateway,
    });

    expect(res.status).toBe("ok");
    const submitCall = calls.find((call) => call.method === "gpu.job.submit");
    expect(submitCall).toBeDefined();
    const submitParams =
      submitCall?.params && typeof submitCall.params === "object"
        ? (submitCall.params as Record<string, unknown>)
        : {};
    const exec =
      submitParams.exec && typeof submitParams.exec === "object" ? submitParams.exec : {};
    const env = (exec as Record<string, unknown>).env as Record<string, unknown>;
    expect(env.OPENCLAW_PLAN_DIR).toBe(tmp);
  });

  it("cancels the job when it stays queued past gpuWaitTimeoutMs", async () => {
    vi.useFakeTimers();
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gpu-wait-timeout-"));

    const calls: Array<Parameters<GatewayCallLike>[0]> = [];
    const callGateway: GatewayCallLike = async <T = Record<string, unknown>>(
      req: Parameters<GatewayCallLike>[0],
    ) => {
      calls.push(req);
      if (req.method === "gpu.job.submit") {
        return { job: { jobId: "job-1" } } as T;
      }
      if (req.method === "gpu.job.wait") {
        const timeoutMs =
          req.params && typeof req.params === "object"
            ? Number((req.params as Record<string, unknown>).timeoutMs ?? 0)
            : 0;
        if (timeoutMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, timeoutMs));
        }
        return { done: false, job: { jobId: "job-1", state: "queued", attempts: [] } } as T;
      }
      if (req.method === "gpu.job.cancel") {
        return { ok: true } as T;
      }
      throw new Error(`unexpected gateway method: ${req.method}`);
    };

    const promise = runGpuNodeViaScheduler({
      planDir: tmp,
      node: buildNode(),
      dryRun: false,
      commandTimeoutMs: 10_000,
      maxAttempts: 1,
      retryDelayMs: 1,
      retrySpec: emptyRetry,
      gatewayTimeoutMs: 1_500,
      invokeTimeoutMs: 10_000,
      gpuWaitTimeoutMs: 2_500,
      nodeApprove: "off",
      callGateway,
    });

    await vi.advanceTimersByTimeAsync(4_000);
    const res = await promise;

    expect(res.status).toBe("failed");
    const attemptError = res.attempts[0]?.error ?? "";
    expect(attemptError).toContain("timed out waiting for eligible GPU nodes");
    expect(calls.some((call) => call.method === "gpu.job.submit")).toBe(true);
    expect(calls.some((call) => call.method === "gpu.job.cancel")).toBe(true);
    vi.useRealTimers();
  });
});
