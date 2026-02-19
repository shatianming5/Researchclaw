import { z } from "zod";
import { AcceptanceSpecSchema, PlanDagSchema } from "./schema.js";

export const ProposalRefineNeedsConfirmSeveritySchema = z.enum(["warn", "error"]);
export type ProposalRefineNeedsConfirmSeverity = z.infer<
  typeof ProposalRefineNeedsConfirmSeveritySchema
>;

export const ProposalRefineNeedsConfirmItemSchema = z
  .object({
    id: z.string().min(1),
    message: z.string().min(1),
    severity: ProposalRefineNeedsConfirmSeveritySchema.optional(),
  })
  .strip();
export type ProposalRefineNeedsConfirmItem = z.infer<typeof ProposalRefineNeedsConfirmItemSchema>;

export const ProposalRefineOutputSchemaV1 = z
  .object({
    schemaVersion: z.literal(1),
    selectedRepoKey: z.string().min(1).optional(),
    sessionId: z.string().min(1).optional(),
    dag: PlanDagSchema,
    acceptance: AcceptanceSpecSchema.optional(),
    needsConfirm: z.array(ProposalRefineNeedsConfirmItemSchema).default([]),
    notes: z.array(z.string()).default([]),
    warnings: z.array(z.string()).default([]),
  })
  .strip();
export type ProposalRefineOutputV1 = z.infer<typeof ProposalRefineOutputSchemaV1>;
