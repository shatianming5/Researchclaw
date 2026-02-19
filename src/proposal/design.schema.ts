import { z } from "zod";
import {
  AcceptanceSpecSchema,
  PlanEdgeSchema,
  PlanNodeSchema,
  ResourceSpecSchema,
} from "./schema.js";

export const DagPatchOpSchema = z.discriminatedUnion("op", [
  z
    .object({
      op: z.literal("addNode"),
      node: PlanNodeSchema,
    })
    .strip(),
  z
    .object({
      op: z.literal("removeNode"),
      nodeId: z.string().min(1),
    })
    .strip(),
  z
    .object({
      op: z.literal("replaceNode"),
      node: PlanNodeSchema,
    })
    .strip(),
  z
    .object({
      op: z.literal("addEdge"),
      edge: PlanEdgeSchema,
    })
    .strip(),
  z
    .object({
      op: z.literal("removeEdge"),
      from: z.string().min(1),
      to: z.string().min(1),
    })
    .strip(),
]);
export type DagPatchOp = z.infer<typeof DagPatchOpSchema>;

const NeedsConfirmItemSchema = z
  .object({
    id: z.string().min(1),
    message: z.string().min(1),
    severity: z.enum(["warn", "error"]).optional(),
  })
  .strip();

export const ExperimentOverridesSchema = z
  .object({
    env: z.record(z.string(), z.string()).optional(),
    resources: ResourceSpecSchema.optional(),
    acceptance: AcceptanceSpecSchema.optional(),
  })
  .strip();
export type ExperimentOverrides = z.infer<typeof ExperimentOverridesSchema>;

const ExperimentDesignBaseSchema = z
  .object({
    id: z.string().min(1),
    name: z.string().min(1),
    rationale: z.string().optional(),
    overrides: ExperimentOverridesSchema.default({}),
  })
  .strip();

export const ExperimentBaselineDesignSchema = ExperimentDesignBaseSchema.extend({
  id: z.literal("baseline"),
}).strip();
export type ExperimentBaselineDesign = z.infer<typeof ExperimentBaselineDesignSchema>;

export const ExperimentVariantDesignSchema = ExperimentDesignBaseSchema.refine(
  (value) => value.id !== "baseline",
  {
    message: "variant id must not be baseline",
    path: ["id"],
  },
);
export type ExperimentVariantDesign = z.infer<typeof ExperimentVariantDesignSchema>;

export const ExperimentSuiteDesignSchemaV1 = z
  .object({
    schemaVersion: z.literal(1),
    suiteId: z.string().min(1),
    selectedRepoKey: z.string().optional(),
    baseline: ExperimentBaselineDesignSchema,
    variants: z.array(ExperimentVariantDesignSchema).default([]),
    needsConfirm: z.array(NeedsConfirmItemSchema).optional(),
    notes: z.array(z.string()).optional(),
    warnings: z.array(z.string()).optional(),
  })
  .strip();

export type ExperimentSuiteDesignV1 = z.infer<typeof ExperimentSuiteDesignSchemaV1>;

const ExperimentDesignBaseSchemaV2 = ExperimentDesignBaseSchema.extend({
  dagPatchOps: z.array(DagPatchOpSchema).optional(),
}).strip();

export const ExperimentBaselineDesignSchemaV2 = ExperimentDesignBaseSchemaV2.extend({
  id: z.literal("baseline"),
}).strip();
export type ExperimentBaselineDesignV2 = z.infer<typeof ExperimentBaselineDesignSchemaV2>;

export const ExperimentVariantDesignSchemaV2 = ExperimentDesignBaseSchemaV2.refine(
  (value) => value.id !== "baseline",
  {
    message: "variant id must not be baseline",
    path: ["id"],
  },
);
export type ExperimentVariantDesignV2 = z.infer<typeof ExperimentVariantDesignSchemaV2>;

export const ExperimentSuiteDesignSchemaV2 = z
  .object({
    schemaVersion: z.literal(2),
    suiteId: z.string().min(1),
    selectedRepoKey: z.string().optional(),
    baseline: ExperimentBaselineDesignSchemaV2,
    variants: z.array(ExperimentVariantDesignSchemaV2).default([]),
    needsConfirm: z.array(NeedsConfirmItemSchema).optional(),
    notes: z.array(z.string()).optional(),
    warnings: z.array(z.string()).optional(),
  })
  .strip();
export type ExperimentSuiteDesignV2 = z.infer<typeof ExperimentSuiteDesignSchemaV2>;

const ExperimentAxisLevelSchema = ExperimentDesignBaseSchemaV2.strip();
export type ExperimentAxisLevel = z.infer<typeof ExperimentAxisLevelSchema>;

export const ExperimentAxisSchema = z
  .object({
    id: z.string().min(1),
    name: z.string().min(1),
    kind: z.literal("grid").default("grid"),
    levels: z.array(ExperimentAxisLevelSchema).min(1),
  })
  .strip();
export type ExperimentAxis = z.infer<typeof ExperimentAxisSchema>;

export const ExperimentSuiteDesignSchemaV3 = z
  .object({
    schemaVersion: z.literal(3),
    suiteId: z.string().min(1),
    selectedRepoKey: z.string().optional(),
    baseline: ExperimentBaselineDesignSchemaV2,
    axes: z.array(ExperimentAxisSchema).default([]),
    needsConfirm: z.array(NeedsConfirmItemSchema).optional(),
    notes: z.array(z.string()).optional(),
    warnings: z.array(z.string()).optional(),
  })
  .strip();
export type ExperimentSuiteDesignV3 = z.infer<typeof ExperimentSuiteDesignSchemaV3>;

export const ExperimentSuiteDesignSchema = z.union([
  ExperimentSuiteDesignSchemaV3,
  ExperimentSuiteDesignSchemaV2,
  ExperimentSuiteDesignSchemaV1,
]);
export type ExperimentSuiteDesign = z.infer<typeof ExperimentSuiteDesignSchema>;
