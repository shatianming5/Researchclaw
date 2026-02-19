import type { z } from "zod";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { CommandOptions, SpawnResult } from "../../process/exec.js";
import { runCommandWithTimeout } from "../../process/exec.js";
import { parseJsonBestEffort } from "../json.js";

type RunCommandLike = (argv: string[], options: CommandOptions) => Promise<SpawnResult>;

type OpencodeJsonEvent = Record<string, unknown>;
type ReadonlyAgentKind = "json" | "text";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function escapeRegExp(value: string): string {
  return value.replaceAll(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function resolveOpencodeConfigDir(opencodeConfigDir: string | undefined): string {
  const provided = (opencodeConfigDir ?? "").trim();
  if (provided) {
    return path.resolve(provided);
  }

  if (process.platform === "win32") {
    const appData = (process.env.APPDATA ?? "").trim();
    if (appData) {
      return path.join(appData, "opencode");
    }
  }

  const home = os.homedir();
  const xdgConfigHome = (process.env.XDG_CONFIG_HOME ?? "").trim() || path.join(home, ".config");
  return path.join(xdgConfigHome, "opencode");
}

function renderReadonlyAgentMarkdown(agentName: string, kind: ReadonlyAgentKind): string {
  const safeName = agentName.trim() || "openclaw-refine";
  const description =
    kind === "json"
      ? `OpenClaw ${safeName} (JSON-only, read-only)`
      : `OpenClaw ${safeName} (text-only, read-only)`;
  const instructions =
    kind === "json"
      ? `Return ONLY valid JSON matching the user's requested schema. Do not include any extra text.\n`
      : `Return ONLY the user's requested text. Do not include markdown fences or commentary.\n`;
  return (
    `---\n` +
    `description: ${description}\n` +
    `mode: primary\n` +
    `tools:\n` +
    `  bash: false\n` +
    `  write: false\n` +
    `  edit: false\n` +
    `  webfetch: false\n` +
    `  task: false\n` +
    `  todowrite: false\n` +
    `  todoread: false\n` +
    `---\n` +
    instructions
  );
}

async function ensureReadonlyOpencodeAgent(params: {
  agentName: string;
  kind?: ReadonlyAgentKind;
  opencodeConfigDir?: string;
  timeoutMs?: number;
  runCommand: RunCommandLike;
}): Promise<
  { ok: true; agentPath: string; created: boolean; updated: boolean } | { ok: false; error: string }
> {
  const agentName = params.agentName.trim();
  if (!agentName) {
    return { ok: false, error: "OpenCode agent name is empty." };
  }
  const kind: ReadonlyAgentKind = params.kind ?? "json";

  const opencodeDir = resolveOpencodeConfigDir(params.opencodeConfigDir);
  const agentDir = path.join(opencodeDir, "agent");
  const agentPath = path.join(agentDir, `${agentName}.md`);
  const timeoutMs = Math.max(5_000, Math.floor(params.timeoutMs ?? 20_000));

  try {
    await fs.stat(agentPath);
    return { ok: true, agentPath, created: false, updated: false };
  } catch {
    // continue
  }

  try {
    const listRes = await params.runCommand(["opencode", "agent", "list"], { timeoutMs });
    if (
      listRes.code === 0 &&
      new RegExp(`^${escapeRegExp(agentName)}\\s*\\(`, "m").test(listRes.stdout)
    ) {
      return { ok: true, agentPath, created: false, updated: false };
    }
  } catch {
    // continue
  }

  const desired = renderReadonlyAgentMarkdown(agentName, kind);
  const tmpRoot = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-opencode-agent-"));
  try {
    const createRes = await params.runCommand(
      [
        "opencode",
        "agent",
        "create",
        "--path",
        tmpRoot,
        "--description",
        `OpenClaw ${agentName} (read-only JSON-only)`,
        "--mode",
        "primary",
        "--tools",
        "read,list,glob,grep",
      ],
      { timeoutMs },
    );

    if (createRes.code === 0) {
      const candidateDir = path.join(tmpRoot, "agent");
      const entries = await fs.readdir(candidateDir, { withFileTypes: true });
      const md = entries.find((entry) => entry.isFile() && entry.name.endsWith(".md"));
      if (md) {
        const generated = await fs.readFile(path.join(candidateDir, md.name), "utf-8");
        await fs.mkdir(agentDir, { recursive: true });
        await fs.writeFile(agentPath, generated, "utf-8");
        return { ok: true, agentPath, created: true, updated: false };
      }
    }
  } catch {
    // continue
  } finally {
    await fs.rm(tmpRoot, { recursive: true, force: true }).catch(() => undefined);
  }

  try {
    await fs.mkdir(agentDir, { recursive: true });
    await fs.writeFile(agentPath, desired, "utf-8");
    return { ok: true, agentPath, created: true, updated: false };
  } catch (err) {
    return { ok: false, error: `Failed to create OpenCode agent file: ${String(err)}` };
  }
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

function extractSessionId(events: OpencodeJsonEvent[]): string | undefined {
  for (const event of events) {
    if (!isRecord(event)) {
      continue;
    }

    const direct =
      typeof event.sessionID === "string"
        ? event.sessionID
        : typeof event.sessionId === "string"
          ? event.sessionId
          : typeof event.session === "string"
            ? event.session
            : undefined;
    if (direct?.trim()) {
      return direct.trim();
    }

    const part = isRecord(event.part) ? event.part : null;
    const fromPart =
      typeof part?.sessionID === "string"
        ? part.sessionID
        : typeof part?.sessionId === "string"
          ? part.sessionId
          : undefined;
    if (fromPart?.trim()) {
      return fromPart.trim();
    }
  }
  return undefined;
}

export type RunOpencodeJsonResult = {
  ok: boolean;
  text: string;
  warnings: string[];
  sessionId?: string;
  error?: string;
  stdout?: string;
  stderr?: string;
  exitCode?: number | null;
};

export async function runOpencodeJson(params: {
  message: string;
  model?: string;
  agent?: string;
  agentKind?: ReadonlyAgentKind;
  files?: string[];
  cwd?: string;
  timeoutMs?: number;
  opencodeConfigDir?: string;
  deps?: { runCommand?: RunCommandLike };
}): Promise<RunOpencodeJsonResult> {
  const runCommand = params.deps?.runCommand ?? runCommandWithTimeout;
  const timeoutMs = Math.max(5_000, params.timeoutMs ?? 120_000);
  const warnings: string[] = [];

  const agentName = params.agent?.trim() || "";
  if (agentName) {
    const ensured = await ensureReadonlyOpencodeAgent({
      agentName,
      kind: params.agentKind,
      opencodeConfigDir: params.opencodeConfigDir,
      timeoutMs: Math.min(timeoutMs, 20_000),
      runCommand,
    });
    if (!ensured.ok) {
      return { ok: false, text: "", warnings: [], error: ensured.error };
    }
    if (ensured.created) {
      warnings.push(`Created OpenCode agent: ${ensured.agentPath}`);
    }
    if (ensured.updated) {
      warnings.push(`Updated OpenCode agent: ${ensured.agentPath}`);
    }
  }

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
      warnings,
      error: `Failed to spawn opencode: ${String(err)}`,
    };
  }

  const parsed = parseJsonl(result.stdout);
  const text = decodeTextFromEvents(parsed.events);
  const sessionId = extractSessionId(parsed.events);
  if (result.code !== 0) {
    return {
      ok: false,
      text,
      warnings: [...warnings, ...parsed.warnings],
      sessionId,
      stdout: result.stdout,
      stderr: result.stderr,
      exitCode: result.code,
      error: `opencode exited with code ${result.code ?? "null"}`,
    };
  }
  return {
    ok: true,
    text,
    warnings: [...warnings, ...parsed.warnings],
    sessionId,
    stdout: result.stdout,
    stderr: result.stderr,
    exitCode: result.code,
  };
}

export type OpencodeCompleteJsonResult<T> =
  | { ok: true; value: T; raw: string; warnings: string[]; sessionId?: string }
  | { ok: false; error: string; raw?: string; warnings: string[]; sessionId?: string };

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
  opencodeConfigDir?: string;
  attempts?: number;
  deps?: { runCommand?: RunCommandLike };
}): Promise<OpencodeCompleteJsonResult<T>> {
  const attempts = Math.max(1, params.attempts ?? 2);
  let lastRaw: string | undefined;
  let lastError = "Unknown error";
  const warnings: string[] = [];
  let sessionId: string | undefined;

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
      opencodeConfigDir: params.opencodeConfigDir,
      deps: params.deps,
    });
    warnings.push(...res.warnings);
    sessionId ||= res.sessionId;
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
    return { ok: true, value: validated.data, raw: res.text, warnings, sessionId };
  }

  return { ok: false, error: lastError, raw: lastRaw, warnings, sessionId };
}
