import type { SpawnResult } from "../process/exec.js";
import { runCommandWithTimeout } from "../process/exec.js";
import { extractHfDatasetIdFromUrl } from "./hf.js";
import {
  DatasetPlatformSchema,
  type DatasetEntity,
  type DiscoveryMode,
  type DiscoveryReport,
  type DiscoveredDataset,
  type DiscoveredRepo,
  type RepoEntity,
} from "./schema.js";
import { resolveHuggingFaceToken, resolveKaggleCredentials } from "./secrets.js";

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

function normalizeRepoName(value: string): string {
  return value.trim().replaceAll(/\s+/g, " ");
}

function isProbablyGitUrl(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed) {
    return false;
  }
  if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
    return true;
  }
  if (trimmed.startsWith("git@")) {
    return true;
  }
  if (trimmed.endsWith(".git")) {
    return true;
  }
  return false;
}

function parseOwnerRepo(value: string): { owner: string; repo: string } | null {
  const trimmed = value.trim();
  if (!trimmed || trimmed.includes(" ") || trimmed.includes("\n")) {
    return null;
  }
  const parts = trimmed.split("/");
  if (parts.length !== 2) {
    return null;
  }
  const owner = parts[0]?.trim();
  const repo = parts[1]?.trim();
  if (!owner || !repo) {
    return null;
  }
  if (!/^[\w.-]+$/.test(owner) || !/^[\w.-]+$/.test(repo)) {
    return null;
  }
  return { owner, repo };
}

function repoCandidates(
  input: RepoEntity,
): { url: string; confidence: number; evidence: string }[] {
  const candidates: { url: string; confidence: number; evidence: string }[] = [];

  const url = input.url?.trim();
  if (url) {
    candidates.push({ url, confidence: 0.95, evidence: "proposal.url" });
  }

  const name = normalizeRepoName(input.name ?? "");
  if (name) {
    if (isProbablyGitUrl(name)) {
      candidates.push({ url: name, confidence: 0.9, evidence: "proposal.name_as_url" });
    } else {
      const ownerRepo = parseOwnerRepo(name);
      if (ownerRepo) {
        const https = `https://github.com/${ownerRepo.owner}/${ownerRepo.repo}.git`;
        candidates.push({ url: https, confidence: 0.75, evidence: "heuristic.github_https" });
        const ssh = `git@github.com:${ownerRepo.owner}/${ownerRepo.repo}.git`;
        candidates.push({ url: ssh, confidence: 0.65, evidence: "heuristic.github_ssh" });
      }
    }
  }

  const hint = normalizeRepoName(input.hintText ?? "");
  if (hint && isProbablyGitUrl(hint)) {
    candidates.push({ url: hint, confidence: 0.7, evidence: "proposal.hintText" });
  }

  const uniq = new Map<string, { url: string; confidence: number; evidence: string }>();
  for (const c of candidates) {
    const key = c.url.trim();
    if (!key) {
      continue;
    }
    if (!uniq.has(key)) {
      uniq.set(key, c);
    }
  }
  return [...uniq.values()];
}

async function gitLsRemote(url: string, timeoutMs: number): Promise<SpawnResult> {
  return await runCommandWithTimeout(["git", "ls-remote", "--symref", url, "HEAD"], {
    timeoutMs,
  });
}

function parseGitSymref(stdout: string): { branch?: string; headCommit?: string } {
  // Example:
  // ref: refs/heads/main	HEAD
  // <sha>	HEAD
  let branch: string | undefined;
  let headCommit: string | undefined;
  for (const line of stdout.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }
    if (trimmed.startsWith("ref: ")) {
      const parts = trimmed.split(/\s+/);
      const ref = parts[1] ?? "";
      const m = ref.match(/^refs\/heads\/(.+)$/);
      if (m?.[1]) {
        branch = m[1];
      }
      continue;
    }
    const cols = trimmed.split(/\s+/);
    const sha = cols[0];
    const ref = cols[1];
    if (sha && ref === "HEAD" && /^[0-9a-f]{7,40}$/i.test(sha)) {
      headCommit = sha;
    }
  }
  return { branch, headCommit };
}

export async function discoverRepo(params: {
  input: RepoEntity;
  mode: DiscoveryMode;
  timeoutMs?: number;
}): Promise<DiscoveredRepo> {
  const timeoutMs = params.timeoutMs ?? 12_000;
  const candidates = repoCandidates(params.input);
  if (candidates.length === 0) {
    return {
      input: params.input,
      exists: false,
      warnings: ["No repo candidates found (missing url/name)."],
      evidence: [],
    };
  }

  if (params.mode === "off") {
    const primary = candidates[0];
    return {
      input: params.input,
      resolvedUrl: primary?.url,
      exists: undefined,
      confidence: primary?.confidence,
      evidence: candidates.map((c) => c.url),
      warnings: [],
    };
  }

  const warnings: string[] = [];
  for (const candidate of candidates) {
    try {
      const res = await gitLsRemote(candidate.url, timeoutMs);
      if (res.code !== 0) {
        warnings.push(`git ls-remote failed for ${candidate.url} (code=${res.code ?? "null"}).`);
        continue;
      }
      const parsed = parseGitSymref(res.stdout);
      return {
        input: params.input,
        resolvedUrl: candidate.url,
        defaultBranch: parsed.branch,
        headCommit: parsed.headCommit,
        exists: true,
        confidence: candidate.confidence,
        evidence: [candidate.evidence, candidate.url],
        warnings,
      };
    } catch (err) {
      warnings.push(`git ls-remote error for ${candidate.url}: ${String(err)}`);
      continue;
    }
  }

  return {
    input: params.input,
    resolvedUrl: candidates[0]?.url,
    exists: false,
    confidence: candidates[0]?.confidence,
    evidence: candidates.map((c) => c.url),
    warnings: warnings.length > 0 ? warnings : ["No candidate repo resolved."],
  };
}

function guessDatasetPlatform(input: DatasetEntity): DiscoveredDataset["platform"] {
  const raw = input.platform?.trim();
  if (raw) {
    const parsed = DatasetPlatformSchema.safeParse(raw);
    if (parsed.success) {
      return parsed.data;
    }
  }
  const url = input.url?.trim() ?? "";
  if (url.includes("huggingface.co/datasets/")) {
    return "hf";
  }
  if (url.includes("kaggle.com/datasets/")) {
    return "kaggle";
  }
  if (url.startsWith("http://") || url.startsWith("https://")) {
    return "url";
  }
  return "unknown";
}

function resolveHfDatasetId(input: DatasetEntity): string | null {
  const name = input.name?.trim();
  if (name && /^[\w.-]+(?:\/[\w.-]+)?$/.test(name)) {
    return name;
  }
  const url = input.url?.trim() ?? "";
  const fromUrl = extractHfDatasetIdFromUrl(url);
  return fromUrl ?? null;
}

function parseKaggleHandle(value: string): { owner: string; dataset: string } | null {
  const trimmed = value.trim();
  if (!trimmed || trimmed.includes(" ") || trimmed.includes("\n")) {
    return null;
  }
  const parts = trimmed.split("/");
  if (parts.length !== 2) {
    return null;
  }
  const owner = parts[0]?.trim() ?? "";
  const dataset = parts[1]?.trim() ?? "";
  if (!owner || !dataset) {
    return null;
  }
  if (!/^[\w.-]+$/.test(owner) || !/^[\w.-]+$/.test(dataset)) {
    return null;
  }
  return { owner, dataset };
}

function extractKaggleHandleFromUrl(rawUrl: string): { owner: string; dataset: string } | null {
  const trimmed = rawUrl.trim();
  if (!trimmed) {
    return null;
  }
  try {
    const url = new URL(trimmed);
    const host = url.hostname.toLowerCase();
    if (!host.endsWith("kaggle.com")) {
      return null;
    }
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts.length >= 3 && parts[0] === "datasets") {
      const owner = parts[1]?.trim() ?? "";
      const dataset = parts[2]?.trim() ?? "";
      return parseKaggleHandle(`${owner}/${dataset}`);
    }
    if (parts.length >= 2) {
      const owner = parts[0]?.trim() ?? "";
      const dataset = parts[1]?.trim() ?? "";
      return parseKaggleHandle(`${owner}/${dataset}`);
    }
    return null;
  } catch {
    return null;
  }
}

function resolveKaggleHandle(input: DatasetEntity): { owner: string; dataset: string } | null {
  const name = input.name?.trim() ?? "";
  const url = input.url?.trim() ?? "";
  return parseKaggleHandle(name) ?? extractKaggleHandleFromUrl(url);
}

async function fetchJson(
  fetchFn: FetchLike,
  url: string,
  timeoutMs: number,
  extraHeaders?: Record<string, string>,
): Promise<unknown> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const headers: Record<string, string> = { accept: "application/json" };
    for (const [key, value] of Object.entries(extraHeaders ?? {})) {
      if (typeof value === "string" && value.trim()) {
        headers[key] = value;
      }
    }
    const res = await fetchFn(url, {
      method: "GET",
      headers,
      signal: controller.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText}: ${text.slice(0, 200)}`);
    }
    return await res.json();
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchText(fetchFn: FetchLike, url: string, timeoutMs: number): Promise<string> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetchFn(url, { method: "GET", signal: controller.signal });
    return await res.text();
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchHeadOk(fetchFn: FetchLike, url: string, timeoutMs: number): Promise<boolean> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetchFn(url, { method: "HEAD", signal: controller.signal });
    return res.ok;
  } finally {
    clearTimeout(timeout);
  }
}

export async function discoverDataset(params: {
  input: DatasetEntity;
  mode: DiscoveryMode;
  fetchFn?: FetchLike;
  timeoutMs?: number;
}): Promise<DiscoveredDataset> {
  const fetchFn = params.fetchFn ?? fetch;
  const timeoutMs = params.timeoutMs ?? 12_000;
  const platform = guessDatasetPlatform(params.input);

  if (params.mode === "off") {
    if (platform === "hf") {
      const id = resolveHfDatasetId(params.input);
      const pageUrl = id ? `https://huggingface.co/datasets/${id}` : undefined;
      return {
        input: params.input,
        platform,
        resolvedId: id ?? undefined,
        resolvedUrl: pageUrl,
        exists: undefined,
        evidence: pageUrl ? [pageUrl] : [],
        warnings: [],
      };
    }
    if (platform === "url") {
      const url = params.input.url?.trim();
      return {
        input: params.input,
        platform,
        resolvedUrl: url,
        exists: undefined,
        evidence: url ? [url] : [],
        warnings: [],
      };
    }
    if (platform === "kaggle") {
      const handle = resolveKaggleHandle(params.input);
      const resolvedId = handle
        ? `${handle.owner}/${handle.dataset}`
        : params.input.name?.trim() || undefined;
      const resolvedUrl = handle
        ? `https://www.kaggle.com/datasets/${handle.owner}/${handle.dataset}`
        : params.input.url?.trim() || undefined;
      const evidence: string[] = [];
      if (resolvedUrl) {
        evidence.push(resolvedUrl);
      }
      const rawUrl = params.input.url?.trim();
      if (rawUrl && rawUrl !== resolvedUrl) {
        evidence.push(rawUrl);
      }
      return {
        input: params.input,
        platform,
        resolvedId,
        resolvedUrl,
        exists: undefined,
        evidence,
        warnings: [
          "Kaggle datasets typically require credentials; discovery is disabled (mode=off).",
        ],
      };
    }
    return {
      input: params.input,
      platform,
      exists: undefined,
      evidence: [],
      warnings: [],
    };
  }

  if (platform === "hf") {
    const id = resolveHfDatasetId(params.input);
    if (!id) {
      return {
        input: params.input,
        platform,
        exists: false,
        warnings: ["HuggingFace dataset id not found in name/url."],
        evidence: [],
      };
    }
    const apiUrl = `https://huggingface.co/api/datasets/${encodeURIComponent(id)}`;
    const pageUrl = `https://huggingface.co/datasets/${id}`;
    const token = await resolveHuggingFaceToken();
    const extraHeaders = token ? { authorization: `Bearer ${token}` } : undefined;
    const warnings: string[] = [];
    let exists: boolean | undefined;
    let meta: unknown;
    try {
      meta = await fetchJson(fetchFn, apiUrl, timeoutMs, extraHeaders);
      exists = true;
    } catch (err) {
      warnings.push(`HF dataset metadata fetch failed: ${String(err)}`);
      exists = false;
    }

    let sample: unknown;
    if (params.mode === "sample") {
      try {
        const splitsUrl = `https://datasets-server.huggingface.co/splits?dataset=${encodeURIComponent(
          id,
        )}`;
        const splits = (await fetchJson(fetchFn, splitsUrl, timeoutMs, extraHeaders)) as {
          splits?: Array<{ config?: string; split?: string }>;
        };
        const first = splits.splits?.[0];
        const config = first?.config ?? "default";
        const split = first?.split ?? "train";
        const rowsUrl =
          `https://datasets-server.huggingface.co/rows?dataset=${encodeURIComponent(id)}` +
          `&config=${encodeURIComponent(config)}&split=${encodeURIComponent(split)}` +
          `&offset=0&length=1`;
        const rows = await fetchJson(fetchFn, rowsUrl, timeoutMs, extraHeaders);
        sample = { config, split, rows };
        exists = true;
      } catch (err) {
        warnings.push(`HF sample fetch failed: ${String(err)}`);
      }
    }

    return {
      input: params.input,
      platform,
      resolvedId: id,
      resolvedUrl: pageUrl,
      exists,
      sample: sample ?? meta,
      evidence: [apiUrl, pageUrl],
      warnings,
    };
  }

  if (platform === "kaggle") {
    const handle = resolveKaggleHandle(params.input);
    if (!handle) {
      return {
        input: params.input,
        platform,
        exists: false,
        evidence: [],
        warnings: ["Kaggle dataset handle not found in name/url (expected owner/dataset)."],
      };
    }

    const resolvedId = `${handle.owner}/${handle.dataset}`;
    const pageUrl = `https://www.kaggle.com/datasets/${handle.owner}/${handle.dataset}`;
    const evidence: string[] = [pageUrl];
    const url = params.input.url?.trim();
    if (url && url !== pageUrl) {
      evidence.push(url);
    }

    const creds = await resolveKaggleCredentials();
    if (!creds) {
      return {
        input: params.input,
        platform,
        resolvedId,
        resolvedUrl: pageUrl,
        exists: undefined,
        evidence,
        warnings: [
          "Kaggle credentials not configured. Set KAGGLE_USERNAME/KAGGLE_KEY or use `openclaw proposal secrets set`.",
        ],
      };
    }

    const auth = Buffer.from(`${creds.username}:${creds.key}`).toString("base64");
    const headers = { authorization: `Basic ${auth}` };
    const apiBase = "https://www.kaggle.com/api/v1";
    const viewUrl = `${apiBase}/datasets/view/${handle.owner}/${handle.dataset}`;
    const metadataUrl = `${apiBase}/datasets/metadata/${handle.owner}/${handle.dataset}`;
    evidence.push(viewUrl, metadataUrl);

    const warnings: string[] = [];
    let exists: boolean | undefined;
    let view: unknown;
    try {
      view = await fetchJson(fetchFn, viewUrl, timeoutMs, headers);
      exists = true;
    } catch (err) {
      warnings.push(`Kaggle dataset view fetch failed: ${String(err)}`);
      exists = false;
    }

    let sample: unknown;
    if (params.mode === "sample") {
      try {
        const metadata = await fetchJson(fetchFn, metadataUrl, timeoutMs, headers);
        sample = { view, metadata };
        exists = true;
      } catch (err) {
        warnings.push(`Kaggle dataset metadata fetch failed: ${String(err)}`);
      }
    }

    return {
      input: params.input,
      platform,
      resolvedId,
      resolvedUrl: pageUrl,
      exists,
      sample: sample ?? view,
      evidence,
      warnings,
    };
  }

  if (platform === "url") {
    const url = params.input.url?.trim();
    if (!url) {
      return {
        input: params.input,
        platform,
        exists: false,
        warnings: ["Dataset url is missing."],
        evidence: [],
      };
    }
    const warnings: string[] = [];
    let exists: boolean | undefined;
    try {
      exists = await fetchHeadOk(fetchFn, url, timeoutMs);
      if (!exists && params.mode === "sample") {
        // Fallback: some hosts don't support HEAD; do a small GET and accept any response.
        const text = await fetchText(fetchFn, url, timeoutMs);
        exists = text.length >= 0;
      }
    } catch (err) {
      warnings.push(`URL fetch failed: ${String(err)}`);
      exists = false;
    }
    return {
      input: params.input,
      platform,
      resolvedUrl: url,
      exists,
      evidence: [url],
      warnings,
    };
  }

  return {
    input: params.input,
    platform,
    exists: undefined,
    warnings: ["Unknown dataset platform; please provide a URL or platform hint."],
    evidence: [],
  };
}

export async function discoverAll(params: {
  repos: RepoEntity[];
  datasets: DatasetEntity[];
  mode: DiscoveryMode;
  fetchFn?: FetchLike;
}): Promise<DiscoveryReport> {
  const repos = await Promise.all(
    params.repos.map((repo) => discoverRepo({ input: repo, mode: params.mode })),
  );
  const datasets = await Promise.all(
    params.datasets.map((ds) =>
      discoverDataset({ input: ds, mode: params.mode, fetchFn: params.fetchFn }),
    ),
  );
  return { repos, datasets };
}
