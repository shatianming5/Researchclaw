import type { Dirent } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { runCommandWithTimeout } from "../../process/exec.js";
import {
  gitHeadSha,
  gitRemoteOriginUrl,
  isGitRepo,
  type HostCommandRunner,
} from "../repo-workflow/git.js";
import {
  RepoProfileSchema,
  type RepoProfile,
  type RepoFrameworkGuess,
  type RepoConfigCandidates,
  type RepoEntrypointHints,
  type RepoFileExcerpt,
  type RepoLanguage,
} from "./schema.js";

const DEFAULT_IGNORED_DIRS = new Set([
  ".git",
  "node_modules",
  ".venv",
  "venv",
  "__pycache__",
  ".pytest_cache",
  ".mypy_cache",
  ".ruff_cache",
  "dist",
  "build",
  ".next",
  ".turbo",
]);

function normalizeRel(p: string): string {
  return p.replaceAll("\\", "/");
}

async function pathExists(p: string): Promise<boolean> {
  try {
    await fs.stat(p);
    return true;
  } catch {
    return false;
  }
}

async function readExcerpt(absPath: string, maxChars: number): Promise<string | undefined> {
  try {
    const raw = await fs.readFile(absPath, "utf-8");
    const trimmed = raw.trim();
    if (!trimmed) {
      return undefined;
    }
    return trimmed.length > maxChars ? `${trimmed.slice(0, maxChars)}\n...<clipped>` : trimmed;
  } catch {
    return undefined;
  }
}

async function listFiles(params: {
  repoAbs: string;
  maxFiles: number;
  maxDepth: number;
}): Promise<{ files: string[]; notes: string[] }> {
  const notes: string[] = [];
  const files: string[] = [];

  const queue: Array<{ dirAbs: string; depth: number }> = [{ dirAbs: params.repoAbs, depth: 0 }];
  while (queue.length > 0 && files.length < params.maxFiles) {
    const current = queue.shift();
    if (!current) {
      break;
    }
    if (current.depth > params.maxDepth) {
      continue;
    }

    let entries: Dirent[];
    try {
      entries = await fs.readdir(current.dirAbs, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const entry of entries) {
      if (files.length >= params.maxFiles) {
        break;
      }
      const abs = path.join(current.dirAbs, entry.name);
      const rel = normalizeRel(path.relative(params.repoAbs, abs));

      if (entry.isDirectory()) {
        if (DEFAULT_IGNORED_DIRS.has(entry.name)) {
          continue;
        }
        queue.push({ dirAbs: abs, depth: current.depth + 1 });
        continue;
      }
      if (!entry.isFile()) {
        continue;
      }
      files.push(rel);
    }
  }

  if (files.length >= params.maxFiles) {
    notes.push(`File listing clipped at ${params.maxFiles} entries.`);
  }

  return { files, notes };
}

function detectLanguage(params: { files: string[]; dependencyPaths: string[] }): RepoLanguage {
  const hasPy =
    params.files.some((f) => f.endsWith(".py")) ||
    params.dependencyPaths.includes("pyproject.toml");
  const hasNode =
    params.dependencyPaths.includes("package.json") ||
    params.files.some((f) => f.endsWith(".ts") || f.endsWith(".js"));
  if (hasPy && hasNode) {
    return "mixed";
  }
  if (hasPy) {
    return "python";
  }
  if (hasNode) {
    return "node";
  }
  return "unknown";
}

function detectFrameworks(blobs: string[]): string[] {
  const joined = blobs.join("\n").toLowerCase();
  const out = new Set<string>();

  const patterns: Array<{ id: string; re: RegExp }> = [
    { id: "transformers", re: /\btransformers\b/ },
    { id: "datasets", re: /\bdatasets\b/ },
    { id: "torch", re: /\btorch\b/ },
    { id: "accelerate", re: /\baccelerate\b/ },
    { id: "deepspeed", re: /\bdeepspeed\b/ },
    { id: "lightning", re: /\b(pytorch-lightning|lightning)\b/ },
    { id: "mmengine", re: /\bmmengine\b/ },
    { id: "detectron2", re: /\bdetectron2\b/ },
  ];

  for (const p of patterns) {
    if (p.re.test(joined)) {
      out.add(p.id);
    }
  }
  return [...out].toSorted();
}

function pickReadmePath(files: string[]): string | null {
  const preferred = ["README.md", "README.rst", "README.txt", "readme.md"];
  for (const candidate of preferred) {
    const found = files.find((f) => f.toLowerCase() === candidate.toLowerCase());
    if (found) {
      return found;
    }
  }
  const other = files.find((f) => path.basename(f).toLowerCase().startsWith("readme."));
  return other ?? null;
}

function pickEntrypoints(files: string[]): string[] {
  const patterns: RegExp[] = [
    /(?:^|\/)(train|finetune|fine_tune|run_train|run_finetune)\.py$/i,
    /(?:^|\/)(eval|evaluate|run_eval|run_evaluate)\.py$/i,
    /(?:^|\/)main\.py$/i,
    /(?:^|\/)cli\.py$/i,
    /(?:^|\/)scripts\/.*\.(py|sh)$/i,
    /(?:^|\/)examples\/.*\.(py|sh)$/i,
  ];
  const out: string[] = [];
  for (const file of files) {
    if (out.length >= 32) {
      break;
    }
    if (patterns.some((re) => re.test(file))) {
      out.push(file);
    }
  }
  return out.toSorted();
}

function pickEntrypointHints(files: string[]): RepoEntrypointHints {
  const trainRe: RegExp[] = [
    /(?:^|\/)tools\/train\.py$/i,
    /(?:^|\/)tools\/train_net\.py$/i,
    /(?:^|\/)(train|finetune|fine_tune|run_train|run_finetune)\.py$/i,
    /(?:^|\/)scripts\/train.*\.py$/i,
    /(?:^|\/)examples\/.*train.*\.py$/i,
  ];
  const evalRe: RegExp[] = [
    /(?:^|\/)tools\/test\.py$/i,
    /(?:^|\/)tools\/train_net\.py$/i,
    /(?:^|\/)(eval|evaluate|run_eval|run_evaluate)\.py$/i,
    /(?:^|\/)scripts\/(eval|test).*\.py$/i,
    /(?:^|\/)examples\/.*(eval|test).*\.py$/i,
  ];

  const train: string[] = [];
  const evalEntrypoints: string[] = [];

  for (const file of files) {
    if (train.length < 16 && trainRe.some((re) => re.test(file))) {
      train.push(file);
    }
    if (evalEntrypoints.length < 16 && evalRe.some((re) => re.test(file))) {
      evalEntrypoints.push(file);
    }
    if (train.length >= 16 && evalEntrypoints.length >= 16) {
      break;
    }
  }

  return {
    train: train.toSorted(),
    eval: evalEntrypoints.toSorted(),
  };
}

function collectConfigCandidates(files: string[]): RepoConfigCandidates {
  const maxPerType = 20;

  const mmengine: string[] = [];
  const detectron2: string[] = [];
  const lightning: string[] = [];

  const mmengineRe = /(?:^|\/)configs\/.+\.py$/i;
  const detectron2Re = /(?:^|\/)configs\/.+\.(yaml|yml)$/i;
  const lightningRe = /(?:^|\/)(?:configs|config|conf)\/.+\.(yaml|yml|json)$/i;
  const lightningRootRe = /(?:^|\/)(?:config|configs|hparams)\.(yaml|yml|json)$/i;

  for (const file of files) {
    if (mmengine.length < maxPerType && mmengineRe.test(file)) {
      mmengine.push(file);
    }
    if (detectron2.length < maxPerType && detectron2Re.test(file)) {
      detectron2.push(file);
    }
    if (lightning.length < maxPerType && (lightningRe.test(file) || lightningRootRe.test(file))) {
      lightning.push(file);
    }
    if (
      mmengine.length >= maxPerType &&
      detectron2.length >= maxPerType &&
      lightning.length >= maxPerType
    ) {
      break;
    }
  }

  return {
    lightning: lightning.toSorted(),
    mmengine: mmengine.toSorted(),
    detectron2: detectron2.toSorted(),
  };
}

function buildFrameworkGuesses(params: {
  files: string[];
  frameworks: string[];
  configCandidates: RepoConfigCandidates;
}): RepoFrameworkGuess[] {
  const hasFile = (rel: string) => params.files.includes(rel);
  const hasFrameworkToken = (id: string) => params.frameworks.includes(id);

  const guesses: RepoFrameworkGuess[] = [];

  const addGuess = (guess: RepoFrameworkGuess) => {
    if (guess.confidence <= 0) {
      return;
    }
    guesses.push({
      ...guess,
      confidence: Math.max(0, Math.min(1, guess.confidence)),
      evidence: [...new Set(guess.evidence.map((e) => e.trim()).filter(Boolean))].toSorted(),
    });
  };

  // MMEngine
  {
    const evidence: string[] = [];
    let confidence = 0;
    if (hasFrameworkToken("mmengine")) {
      evidence.push("text:mmengine");
      confidence += 0.6;
    }
    if (
      hasFile("tools/train.py") ||
      params.files.some((f) => /(?:^|\/)tools\/train\.py$/i.test(f))
    ) {
      evidence.push("file:tools/train.py");
      confidence += 0.3;
    }
    if (params.configCandidates.mmengine.length > 0) {
      evidence.push("file:configs/*.py");
      confidence += 0.1;
    }
    addGuess({ id: "mmengine", confidence, evidence });
  }

  // Detectron2
  {
    const evidence: string[] = [];
    let confidence = 0;
    if (hasFrameworkToken("detectron2")) {
      evidence.push("text:detectron2");
      confidence += 0.6;
    }
    if (
      hasFile("tools/train_net.py") ||
      params.files.some((f) => /(?:^|\/)tools\/train_net\.py$/i.test(f))
    ) {
      evidence.push("file:tools/train_net.py");
      confidence += 0.3;
    }
    if (params.configCandidates.detectron2.length > 0) {
      evidence.push("file:configs/*.yaml");
      confidence += 0.1;
    }
    addGuess({ id: "detectron2", confidence, evidence });
  }

  // Lightning
  {
    const evidence: string[] = [];
    let confidence = 0;
    if (hasFrameworkToken("lightning")) {
      evidence.push("text:lightning");
      confidence += 0.7;
    }
    if (params.files.some((f) => /(?:^|\/)train\.py$/i.test(f))) {
      evidence.push("file:train.py");
      confidence += 0.2;
    }
    if (params.configCandidates.lightning.length > 0) {
      evidence.push("file:config*.yaml");
      confidence += 0.1;
    }
    addGuess({ id: "lightning", confidence, evidence });
  }

  // Transformers
  {
    const evidence: string[] = [];
    let confidence = 0;
    if (hasFrameworkToken("transformers")) {
      evidence.push("text:transformers");
      confidence += 0.8;
    }
    if (hasFrameworkToken("accelerate")) {
      evidence.push("text:accelerate");
      confidence += 0.1;
    }
    if (hasFrameworkToken("datasets")) {
      evidence.push("text:datasets");
      confidence += 0.05;
    }
    addGuess({ id: "transformers", confidence, evidence });
  }

  guesses.sort((a, b) => b.confidence - a.confidence || a.id.localeCompare(b.id));
  const best = guesses[0];
  if (!best || best.confidence < 0.35) {
    return [{ id: "unknown", confidence: 0.25, evidence: [] }];
  }

  const out: RepoFrameworkGuess[] = [...guesses];
  if (!out.some((g) => g.id === "unknown")) {
    out.push({ id: "unknown", confidence: 0.2, evidence: [] });
  }
  return out;
}

export async function profileRepo(params: {
  planDir: string;
  repoRel: string;
  repoKey: string;
  maxFiles?: number;
  maxDepth?: number;
  maxExcerptChars?: number;
  runHostCommand?: HostCommandRunner;
}): Promise<RepoProfile> {
  const maxFiles = Math.max(200, params.maxFiles ?? 2000);
  const maxDepth = Math.max(1, params.maxDepth ?? 5);
  const maxExcerptChars = Math.max(800, params.maxExcerptChars ?? 8_000);
  const planDir = path.resolve(params.planDir);
  const repoRel = normalizeRel(params.repoRel).replace(/^\/+/, "");
  const repoAbs = path.resolve(planDir, repoRel);
  const notes: string[] = [];

  if (!(await pathExists(repoAbs))) {
    return RepoProfileSchema.parse({
      schemaVersion: 1,
      repoKey: params.repoKey,
      repoRel,
      exists: false,
      notes: ["Repo path does not exist."],
    });
  }

  const scan = await listFiles({ repoAbs, maxFiles, maxDepth });
  notes.push(...scan.notes);

  const readmePath = pickReadmePath(scan.files);
  const dependencyCandidates = [
    "pyproject.toml",
    "requirements.txt",
    "setup.py",
    "environment.yml",
    "Pipfile",
    "package.json",
  ];
  const dependencyPaths = dependencyCandidates.filter((candidate) =>
    scan.files.includes(candidate),
  );

  const readme: RepoFileExcerpt | undefined = readmePath
    ? {
        path: readmePath,
        excerpt: await readExcerpt(path.join(repoAbs, readmePath), maxExcerptChars),
      }
    : undefined;

  const dependencyFiles: RepoFileExcerpt[] = [];
  for (const dep of dependencyPaths) {
    dependencyFiles.push({
      path: dep,
      excerpt: await readExcerpt(path.join(repoAbs, dep), Math.min(maxExcerptChars, 6_000)),
    });
  }

  const language = detectLanguage({ files: scan.files, dependencyPaths });
  const blobs = [readme?.excerpt ?? "", ...dependencyFiles.map((f) => f.excerpt ?? "")].filter(
    Boolean,
  );
  const frameworks = detectFrameworks(blobs);
  const entrypoints = pickEntrypoints(scan.files);
  const entrypointHints = pickEntrypointHints(scan.files);
  const configCandidates = collectConfigCandidates(scan.files);
  const frameworkGuesses = buildFrameworkGuesses({
    files: scan.files,
    frameworks,
    configCandidates,
  });

  const runHostCommand = params.runHostCommand ?? runCommandWithTimeout;
  const gitRepo = await isGitRepo(repoAbs);
  const headCommit = gitRepo ? await gitHeadSha(runHostCommand, repoAbs) : undefined;
  const originUrl = gitRepo ? await gitRemoteOriginUrl(runHostCommand, repoAbs) : undefined;

  return RepoProfileSchema.parse({
    schemaVersion: 1,
    repoKey: params.repoKey,
    repoRel,
    exists: true,
    isGitRepo: gitRepo,
    headCommit: headCommit ?? undefined,
    originUrl: originUrl ?? undefined,
    language,
    frameworks,
    frameworkGuesses,
    configCandidates,
    entrypointHints,
    readme,
    dependencyFiles,
    entrypoints,
    fileSample: scan.files.slice(0, 200),
    notes,
  });
}
