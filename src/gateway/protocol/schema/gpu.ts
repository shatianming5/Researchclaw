import { Type } from "@sinclair/typebox";
import { NonEmptyString } from "./primitives.js";

export const GpuJobStateSchema = Type.Union([
  Type.Literal("queued"),
  Type.Literal("running"),
  Type.Literal("succeeded"),
  Type.Literal("failed"),
  Type.Literal("canceled"),
]);

export const GpuJobResourceRequestSchema = Type.Object(
  {
    gpuCount: Type.Integer({ minimum: 1 }),
    gpuType: Type.Optional(NonEmptyString),
    gpuMemGB: Type.Optional(Type.Number({ exclusiveMinimum: 0 })),
    cpuCores: Type.Optional(Type.Integer({ minimum: 0 })),
    ramGB: Type.Optional(Type.Number({ exclusiveMinimum: 0 })),
  },
  { additionalProperties: false },
);

export const GpuJobExecSpecSchema = Type.Object(
  {
    command: Type.Array(NonEmptyString, { minItems: 1 }),
    rawCommand: Type.Optional(Type.String()),
    cwd: Type.Optional(Type.String()),
    env: Type.Optional(Type.Record(NonEmptyString, Type.String())),
    commandTimeoutMs: Type.Optional(Type.Integer({ minimum: 0 })),
    invokeTimeoutMs: Type.Optional(Type.Integer({ minimum: 0 })),
    approved: Type.Optional(Type.Boolean()),
    approvalDecision: Type.Optional(
      Type.Union([Type.Literal("allow-once"), Type.Literal("allow-always")]),
    ),
  },
  { additionalProperties: false },
);

export const GpuJobAttemptSchema = Type.Object(
  {
    attempt: Type.Integer({ minimum: 1 }),
    nodeId: NonEmptyString,
    startedAtMs: Type.Integer({ minimum: 0 }),
    finishedAtMs: Type.Optional(Type.Integer({ minimum: 0 })),
    ok: Type.Optional(Type.Boolean()),
    exitCode: Type.Optional(Type.Union([Type.Integer(), Type.Null()])),
    timedOut: Type.Optional(Type.Boolean()),
    stdoutTail: Type.Optional(Type.String()),
    stderrTail: Type.Optional(Type.String()),
    error: Type.Optional(Type.String()),
  },
  { additionalProperties: false },
);

export const GpuJobResultSchema = Type.Object(
  {
    exitCode: Type.Union([Type.Integer(), Type.Null()]),
    timedOut: Type.Boolean(),
    success: Type.Boolean(),
    stdoutTail: Type.Optional(Type.String()),
    stderrTail: Type.Optional(Type.String()),
  },
  { additionalProperties: false },
);

export const GpuJobSchema = Type.Object(
  {
    jobId: NonEmptyString,
    createdAtMs: Type.Integer({ minimum: 0 }),
    updatedAtMs: Type.Integer({ minimum: 0 }),
    state: GpuJobStateSchema,
    notBeforeMs: Type.Optional(Type.Integer({ minimum: 0 })),
    resources: GpuJobResourceRequestSchema,
    exec: GpuJobExecSpecSchema,
    maxAttempts: Type.Integer({ minimum: 1 }),
    assignedNodeId: Type.Optional(NonEmptyString),
    attempts: Type.Array(GpuJobAttemptSchema),
    result: Type.Optional(GpuJobResultSchema),
    cancelRequested: Type.Optional(Type.Boolean()),
  },
  { additionalProperties: false },
);

export const GpuJobSubmitParamsSchema = Type.Object(
  {
    resources: GpuJobResourceRequestSchema,
    exec: GpuJobExecSpecSchema,
    maxAttempts: Type.Optional(Type.Integer({ minimum: 1 })),
  },
  { additionalProperties: false },
);

export const GpuJobSubmitResultSchema = Type.Object(
  { job: GpuJobSchema },
  { additionalProperties: false },
);

export const GpuJobGetParamsSchema = Type.Object(
  { jobId: NonEmptyString },
  { additionalProperties: false },
);

export const GpuJobGetResultSchema = Type.Object(
  { job: GpuJobSchema },
  { additionalProperties: false },
);

export const GpuJobListParamsSchema = Type.Object(
  {
    state: Type.Optional(GpuJobStateSchema),
  },
  { additionalProperties: false },
);

export const GpuJobListResultSchema = Type.Object(
  { jobs: Type.Array(GpuJobSchema) },
  { additionalProperties: false },
);

export const GpuJobCancelParamsSchema = Type.Object(
  { jobId: NonEmptyString },
  { additionalProperties: false },
);

export const GpuJobCancelResultSchema = Type.Object(
  { ok: Type.Boolean() },
  { additionalProperties: false },
);

export const GpuJobWaitParamsSchema = Type.Object(
  {
    jobId: NonEmptyString,
    timeoutMs: Type.Optional(Type.Integer({ minimum: 0 })),
  },
  { additionalProperties: false },
);

export const GpuJobWaitResultSchema = Type.Object(
  {
    done: Type.Boolean(),
    job: GpuJobSchema,
  },
  { additionalProperties: false },
);
