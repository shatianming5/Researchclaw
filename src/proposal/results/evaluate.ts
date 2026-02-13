import fs from "node:fs/promises";
import path from "node:path";
import type { AcceptanceCheck, AcceptanceSpec } from "../schema.js";
import type { ExecuteLog, MetricValue } from "./schema.js";
import { type AcceptanceCheckResult, type AcceptanceCheckStatus } from "./schema.js";

function parseNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const n = Number(value);
    if (Number.isFinite(n)) {
      return n;
    }
  }
  return null;
}

function compareNumbers(op: string, actual: number, expected: number): boolean {
  switch (op) {
    case ">=":
      return actual >= expected;
    case "<=":
      return actual <= expected;
    case ">":
      return actual > expected;
    case "<":
      return actual < expected;
    case "==":
      return actual === expected;
    case "!=":
      return actual !== expected;
    default:
      return false;
  }
}

function compareStrings(op: string, actual: string, expected: string): boolean | null {
  if (op === "==") {
    return actual === expected;
  }
  if (op === "!=") {
    return actual !== expected;
  }
  return null;
}

async function evaluateArtifactExists(params: {
  planDir: string;
  check: AcceptanceCheck;
}): Promise<Omit<AcceptanceCheckResult, "check">> {
  const selector = params.check.selector;
  const resolved = path.isAbsolute(selector) ? selector : path.resolve(params.planDir, selector);
  try {
    await fs.stat(resolved);
    return {
      status: "pass",
      message: `Found artifact: ${selector}`,
      actual: { path: selector, exists: true },
    };
  } catch {
    return {
      status: "fail",
      message: `Missing artifact: ${selector}`,
      actual: { path: selector, exists: false },
    };
  }
}

function resolveLastExitCode(
  attempts: Array<{ exitCode?: number | null; ok?: boolean }>,
): number | null {
  if (attempts.length === 0) {
    return null;
  }
  const last = attempts.at(-1);
  if (!last) {
    return null;
  }
  if (typeof last.exitCode === "number") {
    return last.exitCode;
  }
  if (last.exitCode === null && last.ok === true) {
    return 0;
  }
  if (last.ok === false) {
    return 1;
  }
  return null;
}

function findNodeResult(params: { executeLog: ExecuteLog; selector: string }) {
  return (
    params.executeLog.results.find((r) => r.nodeId === params.selector) ??
    params.executeLog.results.find((r) => r.type === params.selector) ??
    null
  );
}

function normalizeStatus(params: {
  status: AcceptanceCheckStatus;
  check: AcceptanceCheck;
}): AcceptanceCheckStatus {
  if (params.status === "pass" && params.check.needs_confirm) {
    return "needs_confirm";
  }
  return params.status;
}

function compareMetric(params: {
  check: AcceptanceCheck;
  actual: MetricValue;
  expected: number | string;
}): { ok: boolean; message?: string } {
  const op = params.check.op ?? ">=";
  const actualNum = parseNumber(params.actual);
  const expectedNum = parseNumber(params.expected);

  if (actualNum !== null && expectedNum !== null) {
    const ok = compareNumbers(op, actualNum, expectedNum);
    return {
      ok,
      message: `Expected ${params.check.selector} ${op} ${expectedNum} (got ${actualNum})`,
    };
  }

  if (typeof params.actual === "string" && typeof params.expected === "string") {
    const stringResult = compareStrings(op, params.actual, params.expected);
    if (stringResult === null) {
      return { ok: false, message: `Unsupported op for string metric: ${op}` };
    }
    return {
      ok: stringResult,
      message: `Expected ${params.check.selector} ${op} "${params.expected}" (got "${params.actual}")`,
    };
  }

  return {
    ok: false,
    message: `Metric "${params.check.selector}" cannot be compared (actual=${String(
      params.actual,
    )}, expected=${String(params.expected)})`,
  };
}

async function evaluateMetricThreshold(params: {
  check: AcceptanceCheck;
  metrics: Record<string, MetricValue>;
}): Promise<Omit<AcceptanceCheckResult, "check">> {
  const selector = params.check.selector;
  const actual = params.metrics[selector];
  if (actual === undefined) {
    return {
      status: params.check.needs_confirm ? "needs_confirm" : "fail",
      message: `Metric not found: ${selector}`,
      actual: { metric: selector, value: null },
      expected: { op: params.check.op, value: params.check.value },
    };
  }

  const expected = params.check.value;
  if (expected === undefined) {
    return {
      status: "needs_confirm",
      message: `Metric threshold missing for: ${selector}`,
      actual: { metric: selector, value: actual },
      expected: { op: params.check.op, value: null },
    };
  }

  const compared = compareMetric({ check: params.check, actual, expected });
  return {
    status: compared.ok ? "pass" : "fail",
    message: compared.message,
    actual: { metric: selector, value: actual },
    expected: { op: params.check.op, value: expected },
  };
}

async function evaluateCommandExitCode(params: {
  check: AcceptanceCheck;
  executeLog?: ExecuteLog;
}): Promise<Omit<AcceptanceCheckResult, "check">> {
  if (!params.executeLog) {
    return {
      status: params.check.needs_confirm ? "needs_confirm" : "fail",
      message: `execute_log.json is missing; cannot evaluate command_exit_code for ${params.check.selector}`,
    };
  }

  const node = findNodeResult({ executeLog: params.executeLog, selector: params.check.selector });
  if (!node) {
    return {
      status: params.check.needs_confirm ? "needs_confirm" : "fail",
      message: `Node not found in execute_log.json: ${params.check.selector}`,
    };
  }

  const actualExitCode = resolveLastExitCode(node.attempts);
  if (actualExitCode === null) {
    return {
      status: params.check.needs_confirm ? "needs_confirm" : "fail",
      message: `Exit code unavailable for node: ${params.check.selector}`,
      actual: { nodeId: node.nodeId, exitCode: null },
      expected: { op: params.check.op, value: params.check.value },
    };
  }

  const op = params.check.op ?? "==";
  const expectedRaw = params.check.value ?? 0;
  const expectedNum = parseNumber(expectedRaw);
  if (expectedNum === null) {
    return {
      status: "needs_confirm",
      message: `Exit code threshold is not numeric for ${params.check.selector}`,
      actual: { nodeId: node.nodeId, exitCode: actualExitCode },
      expected: { op, value: expectedRaw },
    };
  }

  const ok = compareNumbers(op, actualExitCode, expectedNum);
  return {
    status: ok ? "pass" : "fail",
    message: `Expected exitCode ${op} ${expectedNum} (got ${actualExitCode})`,
    actual: { nodeId: node.nodeId, exitCode: actualExitCode },
    expected: { op, value: expectedNum },
  };
}

async function evaluateManualApproval(params: {
  check: AcceptanceCheck;
  approved: Set<string>;
}): Promise<Omit<AcceptanceCheckResult, "check">> {
  const id = params.check.id?.trim();
  const key = id || params.check.selector;
  const ok = params.approved.has(key) || params.approved.has(params.check.selector);
  return {
    status: ok ? "pass" : "needs_confirm",
    message: ok ? `Approved: ${key}` : `Needs manual approval: ${key}`,
    actual: { approved: ok, key },
  };
}

async function evaluateCheck(params: {
  planDir: string;
  check: AcceptanceCheck;
  metrics: Record<string, MetricValue>;
  executeLog?: ExecuteLog;
  approved: Set<string>;
}): Promise<AcceptanceCheckResult> {
  const base: Omit<AcceptanceCheckResult, "check"> = await (async (): Promise<
    Omit<AcceptanceCheckResult, "check">
  > => {
    switch (params.check.type) {
      case "artifact_exists":
        return await evaluateArtifactExists({ planDir: params.planDir, check: params.check });
      case "metric_threshold":
        return await evaluateMetricThreshold({ check: params.check, metrics: params.metrics });
      case "command_exit_code":
        return await evaluateCommandExitCode({
          check: params.check,
          executeLog: params.executeLog,
        });
      case "manual_approval":
        return await evaluateManualApproval({ check: params.check, approved: params.approved });
      default:
        return {
          status: "needs_confirm",
          message: "Unsupported acceptance check type.",
        };
    }
  })();

  const normalizedStatus = normalizeStatus({ status: base.status, check: params.check });
  return { check: params.check, ...base, status: normalizedStatus };
}

export async function evaluateAcceptanceSpec(params: {
  planDir: string;
  spec: AcceptanceSpec;
  metrics: Record<string, MetricValue>;
  executeLog?: ExecuteLog;
  approved: Set<string>;
}): Promise<{
  status: AcceptanceCheckStatus;
  checks: AcceptanceCheckResult[];
  summary: { pass: number; fail: number; needs_confirm: number; total: number };
}> {
  const checks: AcceptanceCheckResult[] = [];
  for (const check of params.spec.checks) {
    // eslint-disable-next-line no-await-in-loop
    const res = await evaluateCheck({
      planDir: params.planDir,
      check,
      metrics: params.metrics,
      executeLog: params.executeLog,
      approved: params.approved,
    });
    checks.push(res);
  }

  let pass = 0;
  let fail = 0;
  let needsConfirm = 0;
  for (const entry of checks) {
    if (entry.status === "pass") {
      pass += 1;
    } else if (entry.status === "fail") {
      fail += 1;
    } else {
      needsConfirm += 1;
    }
  }

  const total = checks.length;
  const status: AcceptanceCheckStatus =
    fail > 0 ? "fail" : needsConfirm > 0 ? "needs_confirm" : "pass";

  return {
    status,
    checks,
    summary: { pass, fail, needs_confirm: needsConfirm, total },
  };
}
