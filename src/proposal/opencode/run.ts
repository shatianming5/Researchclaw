import type { z } from "zod";
import type { CommandOptions, SpawnResult } from "../../process/exec.js";
import { runCommandWithTimeout } from "../../process/exec.js";
import { parseJsonBestEffort } from "../json.js";

type RunCommandLike = (argv: string[], options: CommandOptions) => Promise<SpawnResult>;

type OpencodeJsonEvent = {
  type?: unknown;
  part?: unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function decodeTextFromEvents(events: OpencodeJsonEvent[]): string {
  const chunks: string[] = [];
  for (const event of events) {
    if (!isRecord(event)) {
      continue;
    }
    if (event.type !== "text") {
      continue;
    }
    const part = isRecord(event.part) ? event.part : null;
    const text = typeof part?.text === "string" ? part.text : "";
    if (!text) {
      continue;
    }
    chunks.push(text);
  }
  return chunks.join("");
}

function parseJsonl(stdout: string): { events: OpencodeJsonEvent[]; warnings: string[] } {
  const warnings: string[] = [];
  const events: OpencodeJsonEvent[] = [];
  for (const line of stdout.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }
    try {
      const obj = JSON.parse(trimmed) as unknown;
      events.push(obj as OpencodeJsonEvent);
    } catch (err) {
      warnings.push(`Failed to parse opencode JSON event: ${String(err)}`);
    }
  }
  return { events, warnings };
}

export type RunOpencodeJsonResult = {
  ok: boolean;
  text: string;
  warnings: string[];
  error?: string;
  stdout?: string;
  stderr?: string;
  exitCode?: number | null;
};

export async function runOpencodeJson(params: {
  message: string;
  model?: string;
  agent?: string;
  files?: string[];
  cwd?: string;
  timeoutMs?: number;
  deps?: { runCommand?: RunCommandLike };
}): Promise<RunOpencodeJsonResult> {
  const runCommand = params.deps?.runCommand ?? runCommandWithTimeout;
  const timeoutMs = Math.max(5_000, params.timeoutMs ?? 120_000);

  const argv: string[] = ["opencode", "run", "--format", "json"];
  if (params.model?.trim()) {
    argv.push("--model", params.model.trim());
  }
  if (params.agent?.trim()) {
    argv.push("--agent", params.agent.trim());
  }
  for (const filePath of params.files ?? []) {
    const trimmed = filePath.trim();
    if (!trimmed) {
      continue;
    }
    argv.push("--file", trimmed);
  }
  argv.push(params.message);

  let result: SpawnResult;
  try {
    result = await runCommand(argv, {
      timeoutMs,
      cwd: params.cwd,
    });
  } catch (err) {
    return {
      ok: false,
      text: "",
      warnings: [],
      error: `Failed to spawn opencode: ${String(err)}`,
    };
  }

  const parsed = parseJsonl(result.stdout);
  const text = decodeTextFromEvents(parsed.events);
  if (result.code !== 0) {
    return {
      ok: false,
      text,
      warnings: parsed.warnings,
      stdout: result.stdout,
      stderr: result.stderr,
      exitCode: result.code,
      error: `opencode exited with code ${result.code ?? "null"}`,
    };
  }
  return {
    ok: true,
    text,
    warnings: parsed.warnings,
    stdout: result.stdout,
    stderr: result.stderr,
    exitCode: result.code,
  };
}

export type OpencodeCompleteJsonResult<T> =
  | { ok: true; value: T; raw: string; warnings: string[] }
  | { ok: false; error: string; raw?: string; warnings: string[] };

function formatZodError(error: z.ZodError): string {
  const lines = error.issues.map((issue) => {
    const path = issue.path.length > 0 ? issue.path.join(".") : "<root>";
    return `${path}: ${issue.message}`;
  });
  return lines.join("; ");
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

export async function completeJsonWithSchemaViaOpencode<T>(params: {
  schema: z.ZodType<T>;
  prompt: string;
  model: string;
  agent?: string;
  files?: string[];
  cwd?: string;
  timeoutMs?: number;
  attempts?: number;
  deps?: { runCommand?: RunCommandLike };
}): Promise<OpencodeCompleteJsonResult<T>> {
  const attempts = Math.max(1, params.attempts ?? 2);
  let lastRaw: string | undefined;
  let lastError = "Unknown error";
  const warnings: string[] = [];

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const prompt =
      attempt === 1 ? params.prompt : buildRepairPrompt(params.prompt, lastRaw, lastError);
    const res = await runOpencodeJson({
      message: prompt,
      model: params.model,
      agent: params.agent,
      files: params.files,
      cwd: params.cwd,
      timeoutMs: params.timeoutMs,
      deps: params.deps,
    });
    warnings.push(...res.warnings);
    if (!res.ok) {
      lastError = res.error ?? "opencode failed";
      lastRaw = res.text;
      continue;
    }
    lastRaw = res.text;

    const parsed = parseJsonBestEffort(res.text);
    if (!parsed.ok) {
      lastError = `JSON parse error: ${parsed.error}`;
      continue;
    }
    const validated = params.schema.safeParse(parsed.value);
    if (!validated.success) {
      lastError = `Schema validation error: ${formatZodError(validated.error)}`;
      continue;
    }
    return { ok: true, value: validated.data, raw: res.text, warnings };
  }

  return { ok: false, error: lastError, raw: lastRaw, warnings };
}
