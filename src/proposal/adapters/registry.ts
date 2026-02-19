import type { AdapterSelection, FrameworkAdapter } from "./frameworks/types.js";
import type { FrameworkAdapterId, RepoProfile } from "./schema.js";
import { detectron2Adapter } from "./frameworks/detectron2.js";
import { lightningAdapter } from "./frameworks/lightning.js";
import { mmengineAdapter } from "./frameworks/mmengine.js";
import { transformersAdapter } from "./frameworks/transformers.js";

const unknownAdapter: FrameworkAdapter = {
  id: "unknown",
  buildTemplates(ctx) {
    return {
      outputDirRel: ctx.outputDirRel,
      env: {},
      setup: [],
      install: [],
      train: [],
      eval: [],
      notes: [],
      warnings: ["No framework adapter matched; commands must be provided by refine/manual steps."],
    };
  },
};

const ADAPTERS: Record<FrameworkAdapterId, FrameworkAdapter> = {
  transformers: transformersAdapter,
  lightning: lightningAdapter,
  mmengine: mmengineAdapter,
  detectron2: detectron2Adapter,
  unknown: unknownAdapter,
};

export function getAdapter(id: FrameworkAdapterId): FrameworkAdapter {
  return ADAPTERS[id] ?? unknownAdapter;
}

export function pickBestAdapter(profile: RepoProfile): AdapterSelection {
  const guesses = (profile.frameworkGuesses ?? []).toSorted(
    (a, b) => b.confidence - a.confidence || a.id.localeCompare(b.id),
  );
  const top = guesses[0];
  if (top && top.confidence > 0) {
    const id = Object.hasOwn(ADAPTERS, top.id) ? top.id : "unknown";
    return { id, confidence: top.confidence, evidence: top.evidence };
  }

  const frameworks = new Set((profile.frameworks ?? []).map((f) => f.trim().toLowerCase()));
  if (frameworks.has("mmengine")) {
    return { id: "mmengine", confidence: 0.6, evidence: ["text:mmengine"] };
  }
  if (frameworks.has("detectron2")) {
    return { id: "detectron2", confidence: 0.6, evidence: ["text:detectron2"] };
  }
  if (frameworks.has("lightning")) {
    return { id: "lightning", confidence: 0.6, evidence: ["text:lightning"] };
  }
  if (frameworks.has("transformers")) {
    return { id: "transformers", confidence: 0.6, evidence: ["text:transformers"] };
  }
  return { id: "unknown", confidence: 0.25, evidence: [] };
}
