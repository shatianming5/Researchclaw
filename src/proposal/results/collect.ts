import fs from "node:fs/promises";
import path from "node:path";
import {
  ExecuteLogSchema,
  ManualApprovalsSchema,
  type ExecuteLog,
  type MetricValue,
} from "./schema.js";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.stat(filePath);
    return true;
  } catch {
    return false;
  }
}

async function readJson(filePath: string): Promise<unknown> {
  const raw = await fs.readFile(filePath, "utf-8");
  return JSON.parse(raw) as unknown;
}

function extractScalarMetrics(raw: unknown): {
  values: Record<string, MetricValue>;
  warnings: string[];
} {
  const warnings: string[] = [];
  if (!isRecord(raw)) {
    return { values: {}, warnings: ["Metrics file is not an object."] };
  }

  const candidate = isRecord(raw.metrics) ? raw.metrics : raw;
  const ignoredTopLevel = new Set(["schemaVersion", "createdAt", "planId", "runId", "notes"]);

  const values: Record<string, MetricValue> = {};
  for (const [key, value] of Object.entries(candidate)) {
    if (!isRecord(raw.metrics) && ignoredTopLevel.has(key)) {
      continue;
    }
    if (typeof value === "number" || typeof value === "string") {
      values[key] = value;
      continue;
    }
    if (value === null || value === undefined) {
      continue;
    }
    warnings.push(`Ignoring non-scalar metric: ${key}`);
  }

  return { values, warnings };
}

export async function readMetricsFile(metricsPath: string): Promise<{
  ok: boolean;
  path: string;
  values: Record<string, MetricValue>;
  warnings: string[];
  error?: string;
}> {
  try {
    const raw = await readJson(metricsPath);
    const extracted = extractScalarMetrics(raw);
    return {
      ok: true,
      path: metricsPath,
      values: extracted.values,
      warnings: extracted.warnings,
    };
  } catch (err) {
    return {
      ok: false,
      path: metricsPath,
      values: {},
      warnings: [],
      error: String(err),
    };
  }
}

export async function resolveCurrentMetrics(planDir: string): Promise<{
  path: string;
  values: Record<string, MetricValue>;
  warnings: string[];
} | null> {
  const metricsPath = path.join(planDir, "report", "final_metrics.json");
  if (!(await fileExists(metricsPath))) {
    return null;
  }
  const res = await readMetricsFile(metricsPath);
  if (!res.ok) {
    return {
      path: metricsPath,
      values: {},
      warnings: [`Failed to parse current metrics: ${res.error}`],
    };
  }
  return { path: res.path, values: res.values, warnings: res.warnings };
}

async function findLatestBaselineMetrics(planDir: string): Promise<string | null> {
  const runsDir = path.join(planDir, "report", "runs");
  try {
    const entries = await fs.readdir(runsDir, { withFileTypes: true });
    const runIds = entries
      .filter((e) => e.isDirectory())
      .map((e) => e.name)
      .toSorted()
      .toReversed();
    for (const runId of runIds) {
      const candidate = path.join(runsDir, runId, "report", "final_metrics.json");
      // eslint-disable-next-line no-await-in-loop
      if (await fileExists(candidate)) {
        return candidate;
      }
    }
    return null;
  } catch {
    return null;
  }
}

export async function resolveBaselineMetricsPath(params: {
  planDir: string;
  baselinePath?: string;
}): Promise<{ path?: string; warnings: string[] }> {
  const warnings: string[] = [];
  if (params.baselinePath) {
    const resolved = path.resolve(params.baselinePath);
    if (!(await fileExists(resolved))) {
      warnings.push(`Baseline metrics file not found: ${resolved}`);
      return { warnings };
    }
    return { path: resolved, warnings };
  }

  const latest = await findLatestBaselineMetrics(params.planDir);
  if (!latest) {
    return { warnings };
  }
  return { path: latest, warnings };
}

export async function loadExecuteLog(planDir: string): Promise<{
  log?: ExecuteLog;
  warnings: string[];
}> {
  const warnings: string[] = [];
  const executePath = path.join(planDir, "report", "execute_log.json");
  if (!(await fileExists(executePath))) {
    return { warnings };
  }
  try {
    const raw = await readJson(executePath);
    const parsed = ExecuteLogSchema.parse(raw);
    return { log: parsed, warnings };
  } catch (err) {
    warnings.push(`Failed to parse execute_log.json: ${String(err)}`);
    return { warnings };
  }
}

export async function loadManualApprovals(planDir: string): Promise<{
  approved: Set<string>;
  notes: Record<string, string>;
  warnings: string[];
}> {
  const warnings: string[] = [];
  const approved = new Set<string>();
  let notes: Record<string, string> = {};

  const approvalsPath = path.join(planDir, "report", "manual_approvals.json");
  if (!(await fileExists(approvalsPath))) {
    return { approved, notes, warnings };
  }
  try {
    const raw = await readJson(approvalsPath);
    const parsed = ManualApprovalsSchema.safeParse(raw);
    if (!parsed.success) {
      warnings.push(`Invalid manual_approvals.json: ${parsed.error.message}`);
      return { approved, notes, warnings };
    }
    const data = parsed.data;
    if (Array.isArray(data)) {
      for (const entry of data) {
        approved.add(entry);
      }
      return { approved, notes, warnings };
    }
    if (isRecord(data) && Array.isArray((data as { approved?: unknown }).approved)) {
      const approvalObj = data as { approved: string[]; notes?: Record<string, string> };
      for (const entry of approvalObj.approved) {
        approved.add(entry);
      }
      notes = approvalObj.notes ?? {};
      return { approved, notes, warnings };
    }
    if (isRecord(data)) {
      for (const [key, value] of Object.entries(data)) {
        if (value) {
          approved.add(key);
        }
      }
    }
    return { approved, notes, warnings };
  } catch (err) {
    warnings.push(`Failed to read manual_approvals.json: ${String(err)}`);
    return { approved, notes, warnings };
  }
}
