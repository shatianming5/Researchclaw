import type { z } from "zod";
import type { ProposalLlmClient } from "./llm.js";
import type { ProposalEntities } from "./schema.js";
import { heuristicExtractEntities } from "./heuristics.js";
import { completeJsonWithSchema } from "./llm-json.js";
import { buildEntitiesExtractionPrompt } from "./prompts.js";
import { ProposalEntitiesSchema } from "./schema.js";

export async function extractEntities(params: {
  proposalMarkdown: string;
  llmClient?: ProposalLlmClient;
}): Promise<{
  entities: ProposalEntities;
  source: "llm" | "heuristic";
  warnings: string[];
  raw?: string;
}> {
  const warnings: string[] = [];
  if (!params.llmClient) {
    return {
      entities: heuristicExtractEntities(params.proposalMarkdown),
      source: "heuristic",
      warnings,
    };
  }

  const prompt = buildEntitiesExtractionPrompt(params.proposalMarkdown);
  const res = await completeJsonWithSchema({
    client: params.llmClient,
    schema: ProposalEntitiesSchema as z.ZodType<ProposalEntities>,
    prompt,
    maxTokens: 1_400,
    temperature: 0.1,
    timeoutMs: 60_000,
    attempts: 2,
  });

  if (!res.ok) {
    warnings.push(`LLM extraction failed; using heuristics. (${res.error})`);
    return {
      entities: heuristicExtractEntities(params.proposalMarkdown),
      source: "heuristic",
      warnings,
      raw: res.raw,
    };
  }

  return { entities: res.value, source: "llm", warnings, raw: res.raw };
}
