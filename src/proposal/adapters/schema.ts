import { z } from "zod";

export const RepoLanguageSchema = z.enum(["python", "node", "mixed", "unknown"]);
export type RepoLanguage = z.infer<typeof RepoLanguageSchema>;

export const FrameworkAdapterIdSchema = z.enum([
  "transformers",
  "lightning",
  "mmengine",
  "detectron2",
  "unknown",
]);
export type FrameworkAdapterId = z.infer<typeof FrameworkAdapterIdSchema>;

export const RepoFrameworkGuessSchema = z
  .object({
    id: FrameworkAdapterIdSchema,
    confidence: z.number().min(0).max(1),
    evidence: z.array(z.string()).default([]),
  })
  .strip();
export type RepoFrameworkGuess = z.infer<typeof RepoFrameworkGuessSchema>;

export const RepoConfigCandidatesSchema = z
  .object({
    lightning: z.array(z.string()).default([]),
    mmengine: z.array(z.string()).default([]),
    detectron2: z.array(z.string()).default([]),
  })
  .strip();
export type RepoConfigCandidates = z.infer<typeof RepoConfigCandidatesSchema>;

export const RepoEntrypointHintsSchema = z
  .object({
    train: z.array(z.string()).default([]),
    eval: z.array(z.string()).default([]),
  })
  .strip();
export type RepoEntrypointHints = z.infer<typeof RepoEntrypointHintsSchema>;

export const RepoFileExcerptSchema = z
  .object({
    path: z.string().min(1),
    excerpt: z.string().optional(),
  })
  .strip();
export type RepoFileExcerpt = z.infer<typeof RepoFileExcerptSchema>;

export const RepoProfileSchema = z
  .object({
    schemaVersion: z.literal(1),
    repoKey: z.string().min(1),
    repoRel: z.string().min(1),
    exists: z.boolean(),
    isGitRepo: z.boolean().optional(),
    headCommit: z.string().optional(),
    originUrl: z.string().optional(),
    language: RepoLanguageSchema.default("unknown"),
    frameworks: z.array(z.string()).default([]),
    frameworkGuesses: z.array(RepoFrameworkGuessSchema).default([]),
    configCandidates: RepoConfigCandidatesSchema.default({
      lightning: [],
      mmengine: [],
      detectron2: [],
    }),
    entrypointHints: RepoEntrypointHintsSchema.default({ train: [], eval: [] }),
    readme: RepoFileExcerptSchema.optional(),
    dependencyFiles: z.array(RepoFileExcerptSchema).default([]),
    entrypoints: z.array(z.string()).default([]),
    fileSample: z.array(z.string()).default([]),
    notes: z.array(z.string()).default([]),
  })
  .strip();
export type RepoProfile = z.infer<typeof RepoProfileSchema>;
