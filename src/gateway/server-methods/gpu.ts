import type { GatewayRequestHandlers } from "./types.js";
import {
  ErrorCodes,
  errorShape,
  validateGpuJobCancelParams,
  validateGpuJobGetParams,
  validateGpuJobListParams,
  validateGpuJobPauseParams,
  validateGpuJobResumeParams,
  validateGpuJobSubmitParams,
  validateGpuJobWaitParams,
} from "../protocol/index.js";
import { respondInvalidParams, respondUnavailableOnThrow } from "./nodes.helpers.js";

export const gpuHandlers: GatewayRequestHandlers = {
  "gpu.job.submit": async ({ params, respond, context }) => {
    if (!validateGpuJobSubmitParams(params)) {
      respondInvalidParams({
        respond,
        method: "gpu.job.submit",
        validator: validateGpuJobSubmitParams,
      });
      return;
    }
    const p = params;
    await respondUnavailableOnThrow(respond, async () => {
      const job = await context.gpuScheduler.submit({
        resources: p.resources,
        exec: p.exec,
        maxAttempts: p.maxAttempts,
        policy: p.policy,
      });
      respond(true, { job }, undefined);
    });
  },

  "gpu.job.get": async ({ params, respond, context }) => {
    if (!validateGpuJobGetParams(params)) {
      respondInvalidParams({ respond, method: "gpu.job.get", validator: validateGpuJobGetParams });
      return;
    }
    const { jobId } = params;
    const id = String(jobId ?? "").trim();
    if (!id) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "jobId required"));
      return;
    }
    const job = context.gpuScheduler.get(id);
    if (!job) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "unknown jobId"));
      return;
    }
    respond(true, { job }, undefined);
  },

  "gpu.job.list": async ({ params, respond, context }) => {
    if (!validateGpuJobListParams(params)) {
      respondInvalidParams({
        respond,
        method: "gpu.job.list",
        validator: validateGpuJobListParams,
      });
      return;
    }
    const p = params;
    const state = typeof p.state === "string" ? p.state : undefined;
    const jobs = context.gpuScheduler.list({
      state:
        state === "queued" ||
        state === "running" ||
        state === "succeeded" ||
        state === "failed" ||
        state === "canceled"
          ? state
          : undefined,
    });
    respond(true, { jobs }, undefined);
  },

  "gpu.job.cancel": async ({ params, respond, context }) => {
    if (!validateGpuJobCancelParams(params)) {
      respondInvalidParams({
        respond,
        method: "gpu.job.cancel",
        validator: validateGpuJobCancelParams,
      });
      return;
    }
    const { jobId } = params;
    const id = String(jobId ?? "").trim();
    if (!id) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "jobId required"));
      return;
    }
    await respondUnavailableOnThrow(respond, async () => {
      const res = await context.gpuScheduler.cancel(id);
      if (!res.ok) {
        respond(
          false,
          undefined,
          errorShape(ErrorCodes.INVALID_REQUEST, res.reason ?? "cancel failed"),
        );
        return;
      }
      respond(true, { ok: true }, undefined);
    });
  },

  "gpu.job.pause": async ({ params, respond, context }) => {
    if (!validateGpuJobPauseParams(params)) {
      respondInvalidParams({
        respond,
        method: "gpu.job.pause",
        validator: validateGpuJobPauseParams,
      });
      return;
    }
    const { jobId } = params;
    const id = String(jobId ?? "").trim();
    if (!id) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "jobId required"));
      return;
    }
    await respondUnavailableOnThrow(respond, async () => {
      const res = await context.gpuScheduler.pause(id);
      if (!res.ok) {
        respond(
          false,
          undefined,
          errorShape(ErrorCodes.INVALID_REQUEST, res.reason ?? "pause failed"),
        );
        return;
      }
      respond(true, { ok: true }, undefined);
    });
  },

  "gpu.job.resume": async ({ params, respond, context }) => {
    if (!validateGpuJobResumeParams(params)) {
      respondInvalidParams({
        respond,
        method: "gpu.job.resume",
        validator: validateGpuJobResumeParams,
      });
      return;
    }
    const { jobId } = params;
    const id = String(jobId ?? "").trim();
    if (!id) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "jobId required"));
      return;
    }
    await respondUnavailableOnThrow(respond, async () => {
      const res = await context.gpuScheduler.resume(id);
      if (!res.ok) {
        respond(
          false,
          undefined,
          errorShape(ErrorCodes.INVALID_REQUEST, res.reason ?? "resume failed"),
        );
        return;
      }
      respond(true, { ok: true }, undefined);
    });
  },

  "gpu.job.wait": async ({ params, respond, context }) => {
    if (!validateGpuJobWaitParams(params)) {
      respondInvalidParams({
        respond,
        method: "gpu.job.wait",
        validator: validateGpuJobWaitParams,
      });
      return;
    }
    const p = params;
    const id = String(p.jobId ?? "").trim();
    if (!id) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "jobId required"));
      return;
    }
    const timeoutMs =
      typeof p.timeoutMs === "number" && Number.isFinite(p.timeoutMs)
        ? Math.max(0, p.timeoutMs)
        : 30_000;
    await respondUnavailableOnThrow(respond, async () => {
      const job = await context.gpuScheduler.wait(id, timeoutMs);
      if (!job) {
        respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "unknown jobId"));
        return;
      }
      const done = job.state === "succeeded" || job.state === "failed" || job.state === "canceled";
      respond(true, { done, job }, undefined);
    });
  },
};
