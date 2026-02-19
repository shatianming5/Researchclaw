import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { describe, expect, it } from "vitest";
import { profileRepo } from "./profile.js";

async function profileFixture(fixtureName: string) {
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-repo-profile-fw-"));
  const planDir = path.join(tmp, "plan");
  const repoKey = fixtureName;
  const repoRel = `cache/git/${repoKey}`;
  const repoAbs = path.join(planDir, repoRel);
  await fs.mkdir(repoAbs, { recursive: true });

  const fixtureAbs = path.resolve(process.cwd(), `test/fixtures/repos/${fixtureName}`);
  await fs.cp(fixtureAbs, repoAbs, { recursive: true });

  return await profileRepo({ planDir, repoRel, repoKey, maxDepth: 5 });
}

describe("proposal/adapters/profileRepo framework guesses", () => {
  it("detects lightning configs and entrypoints", async () => {
    const profile = await profileFixture("lightning-minimal");
    expect(profile.frameworkGuesses[0]?.id).toBe("lightning");
    expect(profile.frameworkGuesses[0]?.confidence).toBeGreaterThanOrEqual(0.8);
    expect(profile.configCandidates.lightning).toContain("config.yaml");
    expect(profile.entrypointHints.train).toContain("train.py");
    expect(profile.entrypointHints.eval).toContain("eval.py");
  });

  it("detects mmengine configs and entrypoints", async () => {
    const profile = await profileFixture("mmengine-minimal");
    expect(profile.frameworkGuesses[0]?.id).toBe("mmengine");
    expect(profile.frameworkGuesses[0]?.confidence).toBeGreaterThanOrEqual(0.8);
    expect(profile.configCandidates.mmengine).toContain("configs/example.py");
    expect(profile.entrypointHints.train).toContain("tools/train.py");
    expect(profile.entrypointHints.eval).toContain("tools/test.py");
  });

  it("detects detectron2 configs and entrypoints", async () => {
    const profile = await profileFixture("detectron2-minimal");
    expect(profile.frameworkGuesses[0]?.id).toBe("detectron2");
    expect(profile.frameworkGuesses[0]?.confidence).toBeGreaterThanOrEqual(0.8);
    expect(profile.configCandidates.detectron2).toContain("configs/example.yaml");
    expect(profile.entrypointHints.train).toContain("tools/train_net.py");
    expect(profile.entrypointHints.eval).toContain("tools/train_net.py");
  });
});
