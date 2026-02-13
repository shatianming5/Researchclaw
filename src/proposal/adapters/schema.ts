import { z } from "zod";

export const RepoLanguageSchema = z.enum(["python", "node", "mixed", "unknown"]);
export type RepoLanguage = z.infer<typeof RepoLanguageSchema>;

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
    readme: RepoFileExcerptSchema.optional(),
    dependencyFiles: z.array(RepoFileExcerptSchema).default([]),
    entrypoints: z.array(z.string()).default([]),
    fileSample: z.array(z.string()).default([]),
    notes: z.array(z.string()).default([]),
  })
  .strip();
export type RepoProfile = z.infer<typeof RepoProfileSchema>;
