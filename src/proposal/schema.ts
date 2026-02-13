import { z } from "zod";

export const DiscoveryModeSchema = z.enum(["off", "plan", "sample"]);
export type DiscoveryMode = z.infer<typeof DiscoveryModeSchema>;

export const RepoEntitySchema = z
  .object({
    name: z.string().optional(),
    url: z.string().optional(),
    hintText: z.string().optional(),
    branch: z.string().optional(),
    commit: z.string().optional(),
  })
  .strip();
export type RepoEntity = z.infer<typeof RepoEntitySchema>;

export const DatasetPlatformSchema = z.enum(["hf", "kaggle", "url", "unknown"]);
export type DatasetPlatform = z.infer<typeof DatasetPlatformSchema>;

export const DatasetEntitySchema = z
  .object({
    name: z.string().optional(),
    url: z.string().optional(),
    platform: DatasetPlatformSchema.optional(),
    hintText: z.string().optional(),
  })
  .strip();
export type DatasetEntity = z.infer<typeof DatasetEntitySchema>;

export const MetricGoalSchema = z.enum(["min", "max"]);
export type MetricGoal = z.infer<typeof MetricGoalSchema>;

export const MetricEntitySchema = z
  .object({
    name: z.string().min(1),
    goal: MetricGoalSchema.optional(),
    target: z.union([z.number(), z.string()]).optional(),
    unit: z.string().optional(),
    sourceText: z.string().optional(),
  })
  .strip();
export type MetricEntity = z.infer<typeof MetricEntitySchema>;

export const ProposalConstraintsSchema = z
  .object({
    gpu: z.union([z.number(), z.string()]).optional(),
    cpu: z.union([z.number(), z.string()]).optional(),
    memoryGB: z.number().optional(),
    diskGB: z.number().optional(),
    maxHours: z.number().optional(),
    frameworkHints: z.array(z.string()).optional(),
  })
  .strip();
export type ProposalConstraints = z.infer<typeof ProposalConstraintsSchema>;

export const ProposalEntitiesSchema = z
  .object({
    repos: z.array(RepoEntitySchema).default([]),
    datasets: z.array(DatasetEntitySchema).default([]),
    metrics: z.array(MetricEntitySchema).default([]),
    constraints: ProposalConstraintsSchema.optional(),
    deliverables: z.array(z.string()).default([]),
    notes: z.string().optional(),
  })
  .strip();
export type ProposalEntities = z.infer<typeof ProposalEntitiesSchema>;

export const ResourceSpecSchema = z
  .object({
    gpuCount: z.number().int().positive().optional(),
    gpuType: z.string().optional(),
    gpuMemGB: z.number().positive().optional(),
    cpuCores: z.number().int().positive().optional(),
    ramGB: z.number().positive().optional(),
    diskGB: z.number().positive().optional(),
    estimatedMinutes: z.number().positive().optional(),
  })
  .strip();
export type ResourceSpec = z.infer<typeof ResourceSpecSchema>;

export const PlanToolSchema = z.enum(["shell", "gateway_rpc", "manual"]);
export type PlanTool = z.infer<typeof PlanToolSchema>;

export const PlanNodeSchema = z
  .object({
    id: z.string().min(1),
    title: z.string().optional(),
    description: z.string().optional(),
    type: z.string().min(1),
    tool: PlanToolSchema.default("manual"),
    inputs: z.array(z.string()).default([]),
    outputs: z.array(z.string()).default([]),
    resources: ResourceSpecSchema.optional(),
    commands: z.array(z.string()).optional(),
    env: z.record(z.string(), z.string()).optional(),
    retryPolicyId: z.string().optional(),
  })
  .strip();
export type PlanNode = z.infer<typeof PlanNodeSchema>;

export const PlanEdgeSchema = z
  .object({
    from: z.string().min(1),
    to: z.string().min(1),
    reason: z.string().optional(),
  })
  .strip();
export type PlanEdge = z.infer<typeof PlanEdgeSchema>;

export const PlanDagSchema = z
  .object({
    nodes: z.array(PlanNodeSchema),
    edges: z.array(PlanEdgeSchema).default([]),
  })
  .strip();
export type PlanDag = z.infer<typeof PlanDagSchema>;

export const AcceptanceCheckTypeSchema = z.enum([
  "metric_threshold",
  "artifact_exists",
  "command_exit_code",
  "manual_approval",
]);
export type AcceptanceCheckType = z.infer<typeof AcceptanceCheckTypeSchema>;

export const AcceptanceSuggestedBySchema = z.enum([
  "proposal",
  "llm",
  "network_evidence",
  "compiler",
]);
export type AcceptanceSuggestedBy = z.infer<typeof AcceptanceSuggestedBySchema>;

export const AcceptanceCheckSchema = z
  .object({
    id: z.string().optional(),
    type: AcceptanceCheckTypeSchema,
    selector: z.string().min(1),
    op: z.enum([">=", "<=", "==", ">", "<", "!="]).optional(),
    value: z.union([z.number(), z.string()]).optional(),
    unit: z.string().optional(),
    needs_confirm: z.boolean().default(false),
    suggested_by: AcceptanceSuggestedBySchema.default("compiler"),
    evidence: z.array(z.string()).default([]),
    description: z.string().optional(),
  })
  .strip();
export type AcceptanceCheck = z.infer<typeof AcceptanceCheckSchema>;

export const AcceptanceSpecSchema = z
  .object({
    checks: z.array(AcceptanceCheckSchema).default([]),
  })
  .strip();
export type AcceptanceSpec = z.infer<typeof AcceptanceSpecSchema>;

export const FailureCategorySchema = z.enum([
  "network",
  "rate_limit",
  "build_fail",
  "test_fail",
  "oom",
  "divergence",
  "data_missing",
  "unknown",
]);
export type FailureCategory = z.infer<typeof FailureCategorySchema>;

export const BackoffSchema = z
  .object({
    kind: z.enum(["fixed", "exponential"]).default("exponential"),
    baseMs: z.number().int().positive().default(1_000),
    maxMs: z.number().int().positive().default(30_000),
    jitter: z.boolean().default(true),
  })
  .strip();
export type Backoff = z.infer<typeof BackoffSchema>;

export const RetryPolicySchema = z
  .object({
    id: z.string().min(1),
    category: FailureCategorySchema,
    maxAttempts: z.number().int().min(1).default(3),
    backoff: BackoffSchema.optional(),
    retryablePatterns: z.array(z.string()).default([]),
    repairActions: z.array(z.string()).default([]),
  })
  .strip();
export type RetryPolicy = z.infer<typeof RetryPolicySchema>;

export const RetrySpecSchema = z
  .object({
    policies: z.array(RetryPolicySchema).default([]),
    defaultPolicyId: z.string().optional(),
  })
  .strip();
export type RetrySpec = z.infer<typeof RetrySpecSchema>;

export const DiscoveredRepoSchema = z
  .object({
    input: RepoEntitySchema,
    resolvedUrl: z.string().optional(),
    defaultBranch: z.string().optional(),
    headCommit: z.string().optional(),
    exists: z.boolean().optional(),
    evidence: z.array(z.string()).default([]),
    confidence: z.number().min(0).max(1).optional(),
    warnings: z.array(z.string()).default([]),
  })
  .strip();
export type DiscoveredRepo = z.infer<typeof DiscoveredRepoSchema>;

export const DiscoveredDatasetSchema = z
  .object({
    input: DatasetEntitySchema,
    resolvedId: z.string().optional(),
    resolvedUrl: z.string().optional(),
    platform: DatasetPlatformSchema.default("unknown"),
    exists: z.boolean().optional(),
    sample: z.unknown().optional(),
    evidence: z.array(z.string()).default([]),
    warnings: z.array(z.string()).default([]),
  })
  .strip();
export type DiscoveredDataset = z.infer<typeof DiscoveredDatasetSchema>;

export const DiscoveryReportSchema = z
  .object({
    repos: z.array(DiscoveredRepoSchema).default([]),
    datasets: z.array(DiscoveredDatasetSchema).default([]),
  })
  .strip();
export type DiscoveryReport = z.infer<typeof DiscoveryReportSchema>;

export const NeedsConfirmAreaSchema = z.enum(["repo", "dataset", "metric", "resource", "other"]);
export type NeedsConfirmArea = z.infer<typeof NeedsConfirmAreaSchema>;

export const NeedsConfirmItemSchema = z
  .object({
    id: z.string().min(1),
    area: NeedsConfirmAreaSchema,
    message: z.string().min(1),
    suggested: z.string().optional(),
    evidence: z.array(z.string()).default([]),
  })
  .strip();
export type NeedsConfirmItem = z.infer<typeof NeedsConfirmItemSchema>;

export const CompileReportSchema = z
  .object({
    planId: z.string().min(1),
    createdAt: z.string().min(1),
    model: z.string().optional(),
    discovery: DiscoveryModeSchema,
    warnings: z.array(z.string()).default([]),
    errors: z.array(z.string()).default([]),
    needsConfirm: z.array(NeedsConfirmItemSchema).default([]),
  })
  .strip();
export type CompileReport = z.infer<typeof CompileReportSchema>;
