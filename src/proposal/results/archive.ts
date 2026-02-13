import crypto from "node:crypto";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import path from "node:path";
import type { ArtifactManifest, ArtifactManifestEntry } from "./schema.js";
import { ArtifactManifestSchema } from "./schema.js";

export type ArchiveResult = {
  entries: ArtifactManifestEntry[];
  archived: string[];
  missing: string[];
  warnings: string[];
};

async function sha256File(filePath: string): Promise<string> {
  return await new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const stream = fs.createReadStream(filePath);
    stream.on("error", reject);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("end", () => resolve(hash.digest("hex")));
  });
}

async function copyFileWithHash(params: {
  src: string;
  dest: string;
  relPath: string;
}): Promise<ArtifactManifestEntry> {
  const st = await fsPromises.stat(params.src);
  const sha256 = await sha256File(params.src);
  await fsPromises.mkdir(path.dirname(params.dest), { recursive: true });
  await fsPromises.copyFile(params.src, params.dest);
  return {
    path: params.relPath,
    sourcePath: params.relPath,
    size: st.size,
    sha256,
  };
}

async function copyRelativePath(params: {
  planDir: string;
  runDir: string;
  relPath: string;
  out: ArchiveResult;
}): Promise<void> {
  const src = path.join(params.planDir, params.relPath);
  const dest = path.join(params.runDir, params.relPath);

  let st: fs.Stats;
  try {
    st = await fsPromises.lstat(src);
  } catch {
    params.out.missing.push(params.relPath);
    return;
  }

  if (st.isSymbolicLink()) {
    params.out.warnings.push(`Skipping symlink: ${params.relPath}`);
    return;
  }

  if (st.isDirectory()) {
    const entries = await fsPromises.readdir(src, { withFileTypes: true });
    for (const entry of entries) {
      const nextRel = path.posix.join(params.relPath.replaceAll(path.sep, "/"), entry.name);
      // eslint-disable-next-line no-await-in-loop
      await copyRelativePath({
        planDir: params.planDir,
        runDir: params.runDir,
        relPath: nextRel,
        out: params.out,
      });
    }
    return;
  }

  if (!st.isFile()) {
    params.out.warnings.push(`Skipping non-file: ${params.relPath}`);
    return;
  }

  const entry = await copyFileWithHash({ src, dest, relPath: params.relPath });
  params.out.entries.push(entry);
  params.out.archived.push(params.relPath);
}

export async function archivePlanArtifacts(params: {
  planDir: string;
  runDir: string;
  include: string[];
}): Promise<ArchiveResult> {
  const out: ArchiveResult = { entries: [], archived: [], missing: [], warnings: [] };
  await fsPromises.mkdir(params.runDir, { recursive: true });
  for (const relPathRaw of params.include) {
    const relPath = relPathRaw.replaceAll("\\", "/");
    // eslint-disable-next-line no-await-in-loop
    await copyRelativePath({ planDir: params.planDir, runDir: params.runDir, relPath, out });
  }
  return out;
}

export function mergeArchiveResults(params: {
  base: ArchiveResult;
  extra: ArchiveResult;
}): ArchiveResult {
  const entryByPath = new Map<string, ArtifactManifestEntry>();
  for (const entry of params.base.entries) {
    entryByPath.set(entry.path, entry);
  }
  for (const entry of params.extra.entries) {
    entryByPath.set(entry.path, entry);
  }

  const missing = new Set<string>(params.base.missing);
  for (const relPath of params.extra.archived) {
    missing.delete(relPath);
  }
  for (const relPath of params.extra.missing) {
    missing.add(relPath);
  }

  const archived = new Set<string>(params.base.archived);
  for (const relPath of params.extra.archived) {
    archived.add(relPath);
  }

  return {
    entries: [...entryByPath.values()].toSorted((a, b) => a.path.localeCompare(b.path)),
    archived: [...archived].toSorted(),
    missing: [...missing].toSorted(),
    warnings: [...params.base.warnings, ...params.extra.warnings],
  };
}

export function buildArtifactManifest(params: {
  runId: string;
  createdAt: string;
  planId?: string;
  planDir: string;
  archive: ArchiveResult;
}): ArtifactManifest {
  return ArtifactManifestSchema.parse({
    schemaVersion: 1,
    runId: params.runId,
    createdAt: params.createdAt,
    planId: params.planId,
    planDir: params.planDir,
    entries: params.archive.entries,
    missing: params.archive.missing,
    warnings: params.archive.warnings,
  });
}
