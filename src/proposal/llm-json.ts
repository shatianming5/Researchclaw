import type { z } from "zod";
import type { ProposalLlmClient } from "./llm.js";
import { parseJsonBestEffort } from "./json.js";

function formatZodError(error: z.ZodError): string {
  const lines = error.issues.map((issue) => {
    const path = issue.path.length > 0 ? issue.path.join(".") : "<root>";
    return `${path}: ${issue.message}`;
  });
  return lines.join("; ");
}

export type CompleteJsonResult<T> =
  | { ok: true; value: T; raw: string }
  | { ok: false; error: string; raw?: string };

export async function completeJsonWithSchema<T>(params: {
  client: ProposalLlmClient;
  schema: z.ZodType<T>;
  prompt: string;
  maxTokens?: number;
  temperature?: number;
  timeoutMs?: number;
  attempts?: number;
}): Promise<CompleteJsonResult<T>> {
  const attempts = Math.max(1, params.attempts ?? 2);
  let lastRaw: string | undefined;
  let lastError = "Unknown error";

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const raw = await params.client.completeText({
      prompt: attempt === 1 ? params.prompt : buildRepairPrompt(params.prompt, lastRaw, lastError),
      maxTokens: params.maxTokens,
      temperature: params.temperature,
      timeoutMs: params.timeoutMs,
    });
    lastRaw = raw;

    const parsed = parseJsonBestEffort(raw);
    if (!parsed.ok) {
      lastError = `JSON parse error: ${parsed.error}`;
      continue;
    }

    const validated = params.schema.safeParse(parsed.value);
    if (!validated.success) {
      lastError = `Schema validation error: ${formatZodError(validated.error)}`;
      continue;
    }

    return { ok: true, value: validated.data, raw };
  }

  return { ok: false, error: lastError, raw: lastRaw };
}

function buildRepairPrompt(base: string, priorRaw: string | undefined, error: string): string {
  const prior = (priorRaw ?? "").trim();
  const clipped = prior.length > 12_000 ? `${prior.slice(0, 12_000)}\n...<clipped>` : prior;
  return (
    `${base}\n\n` +
    `---\n` +
    `Your previous output was invalid.\n` +
    `Error: ${error}\n\n` +
    `Fix the output. Return ONLY valid JSON. Do not include markdown fences.\n\n` +
    `Previous output:\n${clipped}`
  );
}
