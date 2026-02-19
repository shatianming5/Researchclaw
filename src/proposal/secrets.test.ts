import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  getSecret,
  listSecretKeys,
  resolveSecretsFilePath,
  setSecret,
  unsetSecret,
} from "./secrets.js";

describe("proposal/secrets", () => {
  it("stores secrets under $OPENCLAW_STATE_DIR/credentials/secrets.json", async () => {
    const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-secrets-"));
    const env: NodeJS.ProcessEnv = { ...process.env, OPENCLAW_STATE_DIR: stateDir };

    const secretsPath = resolveSecretsFilePath(env);
    await expect(fs.stat(secretsPath)).rejects.toThrow();

    await setSecret({ key: "huggingface.token", value: "hf_test", env });
    await expect(fs.stat(secretsPath)).resolves.toBeTruthy();

    const keys = await listSecretKeys(env);
    expect(keys).toEqual(["huggingface.token"]);
    await expect(getSecret({ key: "huggingface.token", env })).resolves.toBe("hf_test");

    const unset = await unsetSecret({ key: "huggingface.token", env });
    expect(unset.changed).toBe(true);
    expect(await listSecretKeys(env)).toEqual([]);
  });
});
