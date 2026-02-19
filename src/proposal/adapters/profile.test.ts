import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { describe, expect, it } from "vitest";
import { profileRepo } from "./profile.js";

describe("proposal/adapters/profileRepo", () => {
  it("detects entrypoints and frameworks", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-repo-profile-"));
    const planDir = path.join(tmp, "plan");
    const repoKey = "hf-minimal";
    const repoRel = `cache/git/${repoKey}`;
    const repoAbs = path.join(planDir, repoRel);
    await fs.mkdir(repoAbs, { recursive: true });

    const fixtureAbs = path.resolve(process.cwd(), "test/fixtures/repos/hf-minimal");
    await fs.cp(fixtureAbs, repoAbs, { recursive: true });

    const profile = await profileRepo({ planDir, repoRel, repoKey, maxDepth: 3 });

    expect(profile.exists).toBe(true);
    expect(profile.language).toBe("python");
    expect(profile.entrypoints).toContain("train.py");
    expect(profile.entrypoints).toContain("eval.py");
    expect(profile.frameworks).toContain("transformers");
    expect(profile.frameworks).toContain("datasets");
  });
});
