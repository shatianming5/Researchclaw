import type {
  DiscoveryReport,
  PlanDag,
  PlanEdge,
  PlanNode,
  ProposalEntities,
  ResourceSpec,
} from "./schema.js";

function sanitizeIdPart(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replaceAll(/[^a-z0-9._-]+/g, "-")
    .replaceAll(/-+/g, "-")
    .replaceAll(/^-|-$/g, "");
}

function inferTrainResources(entities: ProposalEntities): ResourceSpec | undefined {
  const c = entities.constraints;
  if (!c) {
    return undefined;
  }
  const gpuCount =
    typeof c.gpu === "number" ? c.gpu : typeof c.gpu === "string" ? Number(c.gpu) : undefined;
  const cpuCores =
    typeof c.cpu === "number" ? c.cpu : typeof c.cpu === "string" ? Number(c.cpu) : undefined;
  return {
    gpuCount: Number.isFinite(gpuCount as number)
      ? Math.max(1, Math.floor(gpuCount as number))
      : undefined,
    cpuCores: Number.isFinite(cpuCores as number)
      ? Math.max(1, Math.floor(cpuCores as number))
      : undefined,
    ramGB: c.memoryGB,
    diskGB: c.diskGB,
    estimatedMinutes: c.maxHours ? Math.max(1, Math.floor(c.maxHours * 60)) : undefined,
  };
}

export function buildSkeletonPlan(params: {
  entities: ProposalEntities;
  discovery: DiscoveryReport;
}): PlanDag {
  const nodes: PlanNode[] = [];
  const edges: PlanEdge[] = [];

  const reviewId = "review.needs_confirm";
  nodes.push({
    id: reviewId,
    type: "manual_review",
    tool: "manual",
    inputs: ["report/needs_confirm.md"],
    outputs: ["plan/overrides.json"],
    description:
      "Review and confirm missing details (thresholds, repos, datasets, resources) before running execution.",
  });

  const fetchNodeIds: string[] = [];
  for (const repo of params.discovery.repos) {
    const label = sanitizeIdPart(repo.input.name ?? repo.resolvedUrl ?? "repo");
    const id = `repo.fetch.${label || "repo"}`;
    fetchNodeIds.push(id);
    const url = repo.resolvedUrl ?? repo.input.url ?? repo.input.name ?? "";
    const branch = repo.defaultBranch;
    const cloneTarget = `cache/git/${label || "repo"}`;
    const args = ["git clone --depth 1", branch ? `--branch ${branch}` : "", url, cloneTarget]
      .filter(Boolean)
      .join(" ");
    nodes.push({
      id,
      type: "fetch_repo",
      tool: "shell",
      inputs: [],
      outputs: [cloneTarget],
      commands: url ? [args] : [],
      retryPolicyId: "retry.network",
      description: url ? `Shallow clone ${url}` : "Resolve and clone the repo",
    });

    const checkId = `repo.check.${label || "repo"}`;
    nodes.push({
      id: checkId,
      type: "static_checks",
      tool: "shell",
      inputs: [cloneTarget],
      outputs: [`report/static_checks/${label || "repo"}.json`],
      retryPolicyId: "retry.unknown",
      description: "Run safe static checks in the cloned repo (no builds or training).",
    });

    edges.push({ from: id, to: checkId, reason: "Repo fetched" });
    edges.push({ from: checkId, to: reviewId, reason: "Confirm repo selection/branch" });
  }

  for (const ds of params.discovery.datasets) {
    const label = sanitizeIdPart(ds.resolvedId ?? ds.input.name ?? ds.resolvedUrl ?? "dataset");
    const id = `data.sample.${label || "dataset"}`;
    fetchNodeIds.push(id);
    const target = `cache/data/${label || "dataset"}`;
    nodes.push({
      id,
      type: "fetch_dataset_sample",
      tool: "manual",
      inputs: [],
      outputs: [target],
      retryPolicyId: "retry.network",
      description:
        ds.platform === "hf"
          ? `Fetch a small sample for HuggingFace dataset ${ds.resolvedId ?? ""}`.trim()
          : ds.platform === "kaggle"
            ? "Kaggle dataset requires credentials; fetch manually or via configured secret store."
            : "Fetch a small data sample.",
    });
    edges.push({ from: id, to: reviewId, reason: "Confirm dataset id/split/credentials" });
  }

  const trainId = "train.run";
  const evalId = "eval.run";
  const reportId = "report.write";
  const trainResources = inferTrainResources(params.entities);

  nodes.push({
    id: trainId,
    type: "train",
    tool: "manual",
    inputs: [],
    outputs: ["artifacts/model", "report/train_metrics.jsonl"],
    resources: trainResources,
    retryPolicyId: "retry.oom",
    description: "Run training as specified in the proposal/repo.",
  });
  nodes.push({
    id: evalId,
    type: "eval",
    tool: "manual",
    inputs: ["artifacts/model"],
    outputs: ["report/eval_metrics.json"],
    retryPolicyId: "retry.network",
    description: "Run evaluation and compute metrics.",
  });
  nodes.push({
    id: reportId,
    type: "report",
    tool: "manual",
    inputs: ["report/eval_metrics.json"],
    outputs: ["report/final_metrics.json", "report/final_report.md"],
    retryPolicyId: "retry.unknown",
    description: "Write the final report and consolidate metrics.",
  });

  edges.push({ from: reviewId, to: trainId, reason: "Plan confirmed" });
  edges.push({ from: trainId, to: evalId, reason: "Model trained" });
  edges.push({ from: evalId, to: reportId, reason: "Metrics computed" });

  // If no fetch nodes exist, still keep review as the first step.
  if (fetchNodeIds.length === 0) {
    // no-op
  }

  return { nodes, edges };
}
