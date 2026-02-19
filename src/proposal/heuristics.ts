import { extractHfDatasetIdFromUrl } from "./hf.js";
import {
  ProposalEntitiesSchema,
  type ProposalEntities,
  type RepoEntity,
  type DatasetEntity,
} from "./schema.js";

function uniqBy<T>(items: T[], key: (item: T) => string): T[] {
  const seen = new Set<string>();
  const out: T[] = [];
  for (const item of items) {
    const k = key(item).trim().toLowerCase();
    if (!k) {
      continue;
    }
    if (seen.has(k)) {
      continue;
    }
    seen.add(k);
    out.push(item);
  }
  return out;
}

function extractGithubRepos(text: string): RepoEntity[] {
  const repos: RepoEntity[] = [];
  const shorthandRe =
    /(?:^|\n)\s*(?:[-*]\s*)?(?:repo(?:sitory)?|github)\s*[:：]\s*([\w.-]+)\/([\w.-]+)(?:\.git)?\b/gi;
  for (const match of text.matchAll(shorthandRe)) {
    const owner = match[1];
    const repo = match[2];
    if (!owner || !repo) {
      continue;
    }
    repos.push({
      name: `${owner}/${repo}`,
      url: `https://github.com/${owner}/${repo}.git`,
      hintText: String(match[0] ?? "").trim(),
    });
  }

  const urlRe = /https?:\/\/github\.com\/([\w.-]+)\/([\w.-]+)(?:\.git)?/gi;
  for (const match of text.matchAll(urlRe)) {
    const owner = match[1];
    const repo = match[2];
    if (!owner || !repo) {
      continue;
    }
    repos.push({
      name: `${owner}/${repo}`,
      url: `https://github.com/${owner}/${repo}.git`,
      hintText: match[0],
    });
  }

  const sshRe = /git@github\.com:([\w.-]+)\/([\w.-]+)(?:\.git)?/gi;
  for (const match of text.matchAll(sshRe)) {
    const owner = match[1];
    const repo = match[2];
    if (!owner || !repo) {
      continue;
    }
    repos.push({
      name: `${owner}/${repo}`,
      url: `git@github.com:${owner}/${repo}.git`,
      hintText: match[0],
    });
  }

  return uniqBy(repos, (r) => r.url ?? r.name ?? "");
}

function extractHfDatasets(text: string): DatasetEntity[] {
  const datasets: DatasetEntity[] = [];
  const urlRe = /https?:\/\/huggingface\.co\/datasets\/[^\s)]+/gi;
  for (const match of text.matchAll(urlRe)) {
    const rawUrl = String(match[0] ?? "").replace(/[),.;]+$/g, "");
    const id = extractHfDatasetIdFromUrl(rawUrl);
    if (!id) {
      continue;
    }
    datasets.push({
      name: id,
      url: `https://huggingface.co/datasets/${id}`,
      platform: "hf",
      hintText: rawUrl,
    });
  }
  return uniqBy(datasets, (d) => d.url ?? d.name ?? "");
}

function extractKaggleDatasets(text: string): DatasetEntity[] {
  const datasets: DatasetEntity[] = [];

  const shorthandRe =
    /(?:^|\n)\s*(?:[-*]\s*)?(?:kaggle\s+dataset|dataset)\s*[:：]\s*([\w.-]+)\/([\w.-]+)\b/gi;
  for (const match of text.matchAll(shorthandRe)) {
    const owner = match[1];
    const dataset = match[2];
    if (!owner || !dataset) {
      continue;
    }
    const id = `${owner}/${dataset}`;
    datasets.push({
      name: id,
      url: `https://www.kaggle.com/datasets/${owner}/${dataset}`,
      platform: "kaggle",
      hintText: String(match[0] ?? "").trim(),
    });
  }

  const urlRe = /https?:\/\/(?:www\.)?kaggle\.com\/datasets\/[^\s)]+/gi;
  for (const match of text.matchAll(urlRe)) {
    const rawUrl = String(match[0] ?? "").replace(/[),.;]+$/g, "");
    const m = rawUrl.match(/kaggle\.com\/datasets\/([\w.-]+)\/([\w.-]+)/i);
    if (!m?.[1] || !m[2]) {
      continue;
    }
    const owner = m[1];
    const dataset = m[2];
    const id = `${owner}/${dataset}`;
    datasets.push({
      name: id,
      url: `https://www.kaggle.com/datasets/${owner}/${dataset}`,
      platform: "kaggle",
      hintText: rawUrl,
    });
  }

  return uniqBy(datasets, (d) => d.url ?? d.name ?? "");
}

export function heuristicExtractEntities(proposalMarkdown: string): ProposalEntities {
  const repos = extractGithubRepos(proposalMarkdown);
  const datasets = uniqBy(
    [...extractHfDatasets(proposalMarkdown), ...extractKaggleDatasets(proposalMarkdown)],
    (d) => d.url ?? d.name ?? "",
  );

  const baseline: ProposalEntities = {
    repos,
    datasets,
    metrics: [],
    deliverables: [],
  };

  return ProposalEntitiesSchema.parse(baseline);
}
