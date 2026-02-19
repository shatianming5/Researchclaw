import { z } from "zod";
import { MetricDeltaSchema, MetricValueSchema } from "../results/schema.js";
import { FailureCategorySchema } from "../schema.js";

export const RepairEvidenceStatusSchema = z.enum(["applied_only", "rerun_ok", "rerun_failed"]);
export type RepairEvidenceStatus = z.infer<typeof RepairEvidenceStatusSchema>;

const AttemptOutcomeSchema = z
  .object({
    ok: z.boolean(),
    exitCode: z.number().int().nullable().optional(),
    timedOut: z.boolean().optional(),
    failureCategory: FailureCategorySchema.optional(),
    stdoutTail: z.string().optional(),
    stderrTail: z.string().optional(),
    stdoutPath: z.string().optional(),
    stderrPath: z.string().optional(),
  })
  .strip();

export const RepairEvidenceSchema = z
  .object({
    schemaVersion: z.literal(1),
    createdAt: z.string().min(1),
    planId: z.string().optional(),
    planDir: z.string().min(1),
    node: z
      .object({
        id: z.string().min(1),
        type: z.string().optional(),
        tool: z.string().optional(),
        commands: z.array(z.string()).optional(),
        workdirRel: z.string().optional(),
      })
      .strip(),
    attempts: z
      .object({
        patchAttempt: z.number().int().min(1),
        rerunAttempt: z.number().int().min(1).optional(),
      })
      .strip(),
    before: AttemptOutcomeSchema,
    patch: z
      .object({
        patchPath: z.string().min(1),
        summary: z
          .object({
            added: z.array(z.string()).default([]),
            modified: z.array(z.string()).default([]),
            deleted: z.array(z.string()).default([]),
          })
          .strip(),
      })
      .strip()
      .optional(),
    after: AttemptOutcomeSchema.optional(),
    metrics: z
      .object({
        beforePath: z.string().optional(),
        afterPath: z.string().optional(),
        valuesBefore: z.record(z.string(), MetricValueSchema).default({}),
        valuesAfter: z.record(z.string(), MetricValueSchema).default({}),
        deltas: z.array(MetricDeltaSchema).default([]),
        warnings: z.array(z.string()).default([]),
        files: z
          .object({
            before: z.string().optional(),
            after: z.string().optional(),
            deltas: z.string().optional(),
          })
          .strip()
          .optional(),
      })
      .strip()
      .optional(),
    status: RepairEvidenceStatusSchema,
    warnings: z.array(z.string()).default([]),
    errors: z.array(z.string()).default([]),
    paths: z
      .object({
        evidenceJson: z.string().min(1),
        evidenceMd: z.string().min(1),
      })
      .strip(),
  })
  .strip();

export type RepairEvidence = z.infer<typeof RepairEvidenceSchema>;
