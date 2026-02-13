import { z } from "zod";
import { AcceptanceCheckSchema } from "../schema.js";

export const MetricValueSchema = z.union([z.number(), z.string()]);
export type MetricValue = z.infer<typeof MetricValueSchema>;

export const MetricDeltaSchema = z
  .object({
    name: z.string().min(1),
    current: MetricValueSchema.optional(),
    baseline: MetricValueSchema.optional(),
    delta: z.number().nullable().optional(),
  })
  .strip();
export type MetricDelta = z.infer<typeof MetricDeltaSchema>;

export const AcceptanceCheckStatusSchema = z.enum(["pass", "fail", "needs_confirm"]);
export type AcceptanceCheckStatus = z.infer<typeof AcceptanceCheckStatusSchema>;

export const AcceptanceCheckResultSchema = z
  .object({
    check: AcceptanceCheckSchema,
    status: AcceptanceCheckStatusSchema,
    message: z.string().optional(),
    actual: z.unknown().optional(),
    expected: z.unknown().optional(),
  })
  .strip();
export type AcceptanceCheckResult = z.infer<typeof AcceptanceCheckResultSchema>;

export const AcceptanceSummarySchema = z
  .object({
    pass: z.number().int().min(0),
    fail: z.number().int().min(0),
    needs_confirm: z.number().int().min(0),
    total: z.number().int().min(0),
  })
  .strip();
export type AcceptanceSummary = z.infer<typeof AcceptanceSummarySchema>;

export const ManualApprovalsSchema = z.union([
  z.array(z.string()),
  z
    .object({
      approved: z.array(z.string()).default([]),
      notes: z.record(z.string(), z.string()).optional(),
    })
    .strip(),
  z.record(z.string(), z.boolean()),
]);
export type ManualApprovals = z.infer<typeof ManualApprovalsSchema>;

export const ExecuteAttemptSchema = z
  .object({
    ok: z.boolean().optional(),
    exitCode: z.number().int().nullable().optional(),
  })
  .strip();

export const ExecuteNodeResultSchema = z
  .object({
    nodeId: z.string().min(1),
    type: z.string().optional(),
    status: z.string().optional(),
    attempts: z.array(ExecuteAttemptSchema).default([]),
  })
  .strip();

export const ExecuteLogSchema = z
  .object({
    results: z.array(ExecuteNodeResultSchema).default([]),
  })
  .strip();
export type ExecuteLog = z.infer<typeof ExecuteLogSchema>;

export const ArtifactManifestEntrySchema = z
  .object({
    path: z.string().min(1),
    sourcePath: z.string().min(1),
    size: z.number().int().min(0),
    sha256: z.string().min(1),
  })
  .strip();
export type ArtifactManifestEntry = z.infer<typeof ArtifactManifestEntrySchema>;

export const ArtifactManifestSchema = z
  .object({
    schemaVersion: z.literal(1),
    runId: z.string().min(1),
    createdAt: z.string().min(1),
    planId: z.string().optional(),
    planDir: z.string().min(1),
    entries: z.array(ArtifactManifestEntrySchema).default([]),
    missing: z.array(z.string()).default([]),
    warnings: z.array(z.string()).default([]),
  })
  .strip();
export type ArtifactManifest = z.infer<typeof ArtifactManifestSchema>;

export const AcceptanceReportSchema = z
  .object({
    schemaVersion: z.literal(1),
    ok: z.boolean(),
    exitCode: z.number().int().min(0),
    createdAt: z.string().min(1),
    planId: z.string().optional(),
    planDir: z.string().min(1),
    runId: z.string().min(1),
    runDir: z.string().min(1),
    status: AcceptanceCheckStatusSchema,
    summary: AcceptanceSummarySchema,
    checks: z.array(AcceptanceCheckResultSchema).default([]),
    metrics: z
      .object({
        currentPath: z.string().optional(),
        baselinePath: z.string().optional(),
        values: z.record(z.string(), MetricValueSchema).default({}),
        deltas: z.array(MetricDeltaSchema).default([]),
      })
      .strip()
      .optional(),
    artifacts: z
      .object({
        manifestPath: z.string().min(1),
        archived: z.array(z.string()).default([]),
        missing: z.array(z.string()).default([]),
      })
      .strip(),
    warnings: z.array(z.string()).default([]),
    errors: z.array(z.string()).default([]),
    paths: z
      .object({
        reportJson: z.string().min(1),
        reportMd: z.string().min(1),
        manifestJson: z.string().min(1),
      })
      .strip(),
  })
  .strip();
export type AcceptanceReport = z.infer<typeof AcceptanceReportSchema>;
