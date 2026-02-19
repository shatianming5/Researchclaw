import type { FrameworkAdapterId, RepoProfile } from "../schema.js";

export type AdapterSelection = {
  id: FrameworkAdapterId;
  confidence: number;
  evidence: string[];
};

export type AdapterContext = {
  planDir: string;
  repoKey: string;
  repoRel: string;
  profile: RepoProfile;
  outputDirRel: string;
  gpuCount?: number;
};

export type AdapterTemplates = {
  outputDirRel: string;
  env: Record<string, string>;
  setup: string[];
  install: string[];
  train: string[];
  eval: string[];
  notes: string[];
  warnings: string[];
};

export type FrameworkAdapter = {
  id: FrameworkAdapterId;
  buildTemplates: (ctx: AdapterContext) => AdapterTemplates;
};
