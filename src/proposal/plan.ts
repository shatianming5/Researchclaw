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
  const datasetFullNodeIds: string[] = [];
  let primaryRepoKey: string | undefined;
  let primaryRepoRel: string | undefined;
  for (const repo of params.discovery.repos) {
    const label = sanitizeIdPart(repo.input.name ?? repo.resolvedUrl ?? "repo");
    const id = `repo.fetch.${label || "repo"}`;
    fetchNodeIds.push(id);
    const url = repo.resolvedUrl ?? repo.input.url ?? repo.input.name ?? "";
    const branch = repo.defaultBranch;
    const repoKey = label || "repo";
    const cloneTarget = `cache/git/${repoKey}`;
    if (!primaryRepoKey) {
      primaryRepoKey = repoKey;
      primaryRepoRel = cloneTarget;
    }
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
    const datasetId = (ds.resolvedId ?? ds.input.name ?? "").trim();
    const sampleCommand =
      ds.platform === "hf" && datasetId
        ? `openclaw proposal dataset sample --dataset ${datasetId} --out ${target}`
        : ds.platform === "kaggle" && /^[\w.-]+\/[\w.-]+$/.test(datasetId)
          ? `openclaw proposal dataset sample --platform kaggle --dataset ${datasetId} --out ${target}`
          : "";
    nodes.push({
      id,
      type: "fetch_dataset_sample",
      tool: sampleCommand ? "shell" : "manual",
      inputs: [],
      outputs: [target],
      commands: sampleCommand ? [sampleCommand] : undefined,
      retryPolicyId: "retry.network",
      description:
        ds.platform === "hf"
          ? `Fetch a small sample for HuggingFace dataset ${ds.resolvedId ?? ""}`.trim()
          : ds.platform === "kaggle"
            ? `Fetch a small sample for Kaggle dataset ${ds.resolvedId ?? ds.input.name ?? ""}`.trim()
            : "Fetch a small data sample.",
    });
    edges.push({ from: id, to: reviewId, reason: "Confirm dataset id/split/credentials" });

    if (ds.platform === "kaggle" && datasetId && /^[\w.-]+\/[\w.-]+$/.test(datasetId)) {
      const fullId = `data.fetch.${label || "dataset"}`;
      const fullTarget = `${target}/full`;
      datasetFullNodeIds.push(fullId);
      nodes.push({
        id: fullId,
        type: "fetch_dataset_kaggle",
        tool: "shell",
        inputs: [],
        outputs: [fullTarget],
        commands: [
          `mkdir -p "cache/venv" "cache/pip" "cache/kaggle" "${fullTarget}"`,
          'if [ ! -f "cache/venv/datasets/bin/activate" ]; then python3 -m venv "cache/venv/datasets"; fi',
          '. "cache/venv/datasets/bin/activate"',
          'export PIP_CACHE_DIR="$PWD/cache/pip"',
          "python -m pip install -U pip",
          "python -m pip install -U kaggle",
          'if [ -z "$KAGGLE_USERNAME" ] || [ -z "$KAGGLE_KEY" ]; then echo "Kaggle credentials missing (set KAGGLE_USERNAME/KAGGLE_KEY or openclaw proposal secrets set)." >&2; exit 1; fi',
          'export KAGGLE_CONFIG_DIR="$PWD/cache/kaggle"',
          [
            "python3 - <<'PY'",
            "import json, os, pathlib",
            "cfg_dir = pathlib.Path(os.environ.get('KAGGLE_CONFIG_DIR', '.')).resolve()",
            "cfg_dir.mkdir(parents=True, exist_ok=True)",
            "path = cfg_dir / 'kaggle.json'",
            "payload = {'username': os.environ.get('KAGGLE_USERNAME', ''), 'key': os.environ.get('KAGGLE_KEY', '')}",
            "path.write_text(json.dumps(payload), encoding='utf-8')",
            "PY",
          ].join("\n"),
          'chmod 600 "$KAGGLE_CONFIG_DIR/kaggle.json" || true',
          `kaggle datasets download -d ${datasetId} -p "${fullTarget}" --unzip --force`,
          'rm -f "$KAGGLE_CONFIG_DIR/kaggle.json" || true',
        ],
        retryPolicyId: "retry.network",
        description: `Download Kaggle dataset ${datasetId} into ${fullTarget}`.trim(),
      });
      edges.push({ from: reviewId, to: fullId, reason: "Plan confirmed" });
    }
  }

  const repoKey = primaryRepoKey ?? "repo";
  const repoRel = primaryRepoRel;
  const repoInputs = repoRel ? [repoRel] : [];
  const outputDirRel = `artifacts/model/${repoKey}`;

  const setupId = "setup.venv";
  const installId = "install.deps";
  nodes.push({
    id: setupId,
    type: "setup_venv",
    tool: "manual",
    inputs: repoInputs,
    outputs: [`cache/venv/${repoKey}`, "cache/hf", "cache/pip"],
    retryPolicyId: "retry.unknown",
    description:
      "Create a Python virtual environment under cache/venv and prepare common caches for reproducible runs.",
  });
  nodes.push({
    id: installId,
    type: "install_deps",
    tool: "manual",
    inputs: repoInputs,
    outputs: [`plan/locks/${repoKey}/pip-freeze.txt`],
    retryPolicyId: "retry.network",
    description:
      "Install repo dependencies into the venv and write a pip freeze lock snapshot under plan/locks/.",
  });

  const trainId = "train.run";
  const evalId = "eval.run";
  const reportId = "report.write";
  const trainResources = inferTrainResources(params.entities);

  nodes.push({
    id: trainId,
    type: "train",
    tool: "manual",
    inputs: repoInputs,
    outputs: [outputDirRel, "report/train_metrics.jsonl", "report/checkpoint_manifest.json"],
    resources: trainResources,
    retryPolicyId: "retry.oom",
    description: "Run training as specified in the proposal/repo.",
  });
  nodes.push({
    id: evalId,
    type: "eval",
    tool: "manual",
    inputs: repoRel ? [repoRel, outputDirRel] : [outputDirRel],
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

  edges.push({ from: reviewId, to: setupId, reason: "Plan confirmed" });
  edges.push({ from: setupId, to: installId, reason: "venv ready" });
  edges.push({ from: installId, to: trainId, reason: "Deps installed" });
  for (const fullId of datasetFullNodeIds) {
    edges.push({ from: fullId, to: trainId, reason: "Dataset downloaded" });
  }
  edges.push({ from: trainId, to: evalId, reason: "Model trained" });
  edges.push({ from: evalId, to: reportId, reason: "Metrics computed" });

  // If no fetch nodes exist, still keep review as the first step.
  if (fetchNodeIds.length === 0) {
    // no-op
  }

  return { nodes, edges };
}
