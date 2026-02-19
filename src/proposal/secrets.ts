import fs from "node:fs/promises";
import path from "node:path";
import { z } from "zod";
import { resolveOAuthDir } from "../config/paths.js";

const SecretsFileSchemaV1 = z
  .object({
    schemaVersion: z.literal(1),
    updatedAt: z.string(),
    secrets: z.record(z.string(), z.string()),
  })
  .strip();

export type SecretsFileV1 = z.infer<typeof SecretsFileSchemaV1>;

export type SecretsSnapshot = {
  path: string;
  exists: boolean;
  file: SecretsFileV1;
};

function resolveSecretsPath(env: NodeJS.ProcessEnv = process.env): string {
  return path.join(resolveOAuthDir(env), "secrets.json");
}

export function resolveSecretsFilePath(env: NodeJS.ProcessEnv = process.env): string {
  return resolveSecretsPath(env);
}

function defaultSecretsFile(): SecretsFileV1 {
  return { schemaVersion: 1, updatedAt: new Date().toISOString(), secrets: {} };
}

async function readSecretsSnapshot(env: NodeJS.ProcessEnv = process.env): Promise<SecretsSnapshot> {
  const secretsPath = resolveSecretsPath(env);
  try {
    const raw = await fs.readFile(secretsPath, "utf-8");
    const parsed = SecretsFileSchemaV1.parse(JSON.parse(raw) as unknown);
    return { path: secretsPath, exists: true, file: parsed };
  } catch (err) {
    if (
      err &&
      typeof err === "object" &&
      "code" in err &&
      (err as { code?: string }).code === "ENOENT"
    ) {
      return { path: secretsPath, exists: false, file: defaultSecretsFile() };
    }
    throw new Error(`Failed to read secrets file ${secretsPath}: ${String(err)}`, { cause: err });
  }
}

async function writeSecretsSnapshot(params: {
  env: NodeJS.ProcessEnv;
  file: SecretsFileV1;
}): Promise<SecretsSnapshot> {
  const secretsPath = resolveSecretsPath(params.env);
  await fs.mkdir(path.dirname(secretsPath), { recursive: true });
  const next: SecretsFileV1 = {
    schemaVersion: 1,
    updatedAt: new Date().toISOString(),
    secrets: params.file.secrets,
  };
  const payload = `${JSON.stringify(next, null, 2)}\n`;
  await fs.writeFile(secretsPath, payload, { mode: 0o600 });
  await fs.chmod(secretsPath, 0o600).catch(() => {});
  return { path: secretsPath, exists: true, file: next };
}

export async function listSecretKeys(env: NodeJS.ProcessEnv = process.env): Promise<string[]> {
  const snapshot = await readSecretsSnapshot(env);
  return Object.keys(snapshot.file.secrets).toSorted((a, b) => a.localeCompare(b));
}

export async function getSecret(params: {
  key: string;
  env?: NodeJS.ProcessEnv;
}): Promise<string | undefined> {
  const env = params.env ?? process.env;
  const snapshot = await readSecretsSnapshot(env);
  const key = params.key.trim();
  if (!key) {
    return undefined;
  }
  const value = snapshot.file.secrets[key];
  return typeof value === "string" && value.trim() ? value : undefined;
}

export async function setSecret(params: {
  key: string;
  value: string;
  env?: NodeJS.ProcessEnv;
}): Promise<SecretsSnapshot> {
  const env = params.env ?? process.env;
  const snapshot = await readSecretsSnapshot(env);
  const key = params.key.trim();
  if (!key) {
    throw new Error("Secret key must be non-empty.");
  }
  const nextSecrets = { ...snapshot.file.secrets, [key]: params.value };
  return await writeSecretsSnapshot({ env, file: { ...snapshot.file, secrets: nextSecrets } });
}

export async function unsetSecret(params: {
  key: string;
  env?: NodeJS.ProcessEnv;
}): Promise<{ changed: boolean; snapshot: SecretsSnapshot }> {
  const env = params.env ?? process.env;
  const snapshot = await readSecretsSnapshot(env);
  const key = params.key.trim();
  if (!key) {
    return { changed: false, snapshot };
  }
  if (!(key in snapshot.file.secrets)) {
    return { changed: false, snapshot };
  }
  const nextSecrets = { ...snapshot.file.secrets };
  delete nextSecrets[key];
  const updated = await writeSecretsSnapshot({
    env,
    file: { ...snapshot.file, secrets: nextSecrets },
  });
  return { changed: true, snapshot: updated };
}

export async function resolveHuggingFaceToken(
  env: NodeJS.ProcessEnv = process.env,
): Promise<string | null> {
  const direct = (env.HF_TOKEN ?? env.HUGGINGFACE_HUB_TOKEN ?? "").trim();
  if (direct) {
    return direct;
  }
  try {
    const stored = await getSecret({ key: "huggingface.token", env });
    return stored?.trim() ? stored.trim() : null;
  } catch {
    return null;
  }
}

export async function resolveKaggleCredentials(env: NodeJS.ProcessEnv = process.env): Promise<{
  username: string;
  key: string;
} | null> {
  const usernameRaw = (env.KAGGLE_USERNAME ?? "").trim();
  const keyRaw = (env.KAGGLE_KEY ?? "").trim();
  if (usernameRaw && keyRaw) {
    return { username: usernameRaw, key: keyRaw };
  }
  try {
    const username = (await getSecret({ key: "kaggle.username", env }))?.trim() ?? "";
    const key = (await getSecret({ key: "kaggle.key", env }))?.trim() ?? "";
    if (username && key) {
      return { username, key };
    }
  } catch {
    // ignore
  }
  return null;
}

export async function resolveExecutionSecretsEnv(
  env: NodeJS.ProcessEnv = process.env,
): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  const hf = await resolveHuggingFaceToken(env);
  if (hf) {
    out.HF_TOKEN = hf;
    out.HUGGINGFACE_HUB_TOKEN = hf;
  }
  const kaggle = await resolveKaggleCredentials(env);
  if (kaggle) {
    out.KAGGLE_USERNAME = kaggle.username;
    out.KAGGLE_KEY = kaggle.key;
  }
  return out;
}
