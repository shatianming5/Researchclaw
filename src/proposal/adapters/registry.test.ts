import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { describe, expect, it } from "vitest";
import { profileRepo } from "./profile.js";
import { getAdapter, pickBestAdapter } from "./registry.js";

async function profileFixture(fixtureName: string) {
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-adapter-registry-"));
  const planDir = path.join(tmp, "plan");
  const repoKey = fixtureName;
  const repoRel = `cache/git/${repoKey}`;
  const repoAbs = path.join(planDir, repoRel);
  await fs.mkdir(repoAbs, { recursive: true });

  const fixtureAbs = path.resolve(process.cwd(), `test/fixtures/repos/${fixtureName}`);
  await fs.cp(fixtureAbs, repoAbs, { recursive: true });

  const profile = await profileRepo({ planDir, repoRel, repoKey, maxDepth: 5 });
  return { planDir, repoKey, repoRel, profile };
}

describe("proposal/adapters/registry", () => {
  it("picks MMEngine adapter and renders templates", async () => {
    const { planDir, repoKey, repoRel, profile } = await profileFixture("mmengine-minimal");
    const selection = pickBestAdapter(profile);
    expect(selection.id).toBe("mmengine");
    expect(selection.confidence).toBeGreaterThan(0.5);

    const adapter = getAdapter(selection.id);
    const templates = adapter.buildTemplates({
      planDir,
      repoKey,
      repoRel,
      profile,
      outputDirRel: `artifacts/model/${repoKey}`,
      gpuCount: 1,
    });
    expect(templates.setup.length).toBeGreaterThan(0);
    expect(templates.install.length).toBeGreaterThan(0);
    expect(templates.train.length).toBeGreaterThan(0);
    expect(templates.eval.length).toBeGreaterThan(0);
  });

  it("picks Transformers adapter and renders templates", async () => {
    const { planDir, repoKey, repoRel, profile } = await profileFixture("hf-minimal");
    const selection = pickBestAdapter(profile);
    expect(selection.id).toBe("transformers");
    expect(selection.confidence).toBeGreaterThan(0.5);

    const adapter = getAdapter(selection.id);
    const templates = adapter.buildTemplates({
      planDir,
      repoKey,
      repoRel,
      profile,
      outputDirRel: `artifacts/model/${repoKey}`,
      gpuCount: 1,
    });
    expect(templates.setup.length).toBeGreaterThan(0);
    expect(templates.install.length).toBeGreaterThan(0);
    expect(templates.train.length).toBeGreaterThan(0);
    expect(templates.eval.length).toBeGreaterThan(0);
  });
});
