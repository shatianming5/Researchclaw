import fs from "node:fs/promises";
import path from "node:path";
import { validateDag } from "./dag.js";
import {
  AcceptanceSpecSchema,
  CompileReportSchema,
  PlanDagSchema,
  RetrySpecSchema,
  type AcceptanceSpec,
  type CompileReport,
  type PlanDag,
  type RetrySpec,
} from "./schema.js";

async function readJson(filePath: string): Promise<unknown> {
  const raw = await fs.readFile(filePath, "utf-8");
  return JSON.parse(raw) as unknown;
}

export type ValidatePlanResult = {
  ok: boolean;
  errors: string[];
  warnings: string[];
  paths: {
    planDag: string;
    acceptance: string;
    retry: string;
    compileReport: string;
  };
  data?: {
    dag: PlanDag;
    acceptance: AcceptanceSpec;
    retry: RetrySpec;
    report: CompileReport;
    needsConfirmCount: number;
  };
};

export async function validatePlanDir(planDir: string): Promise<ValidatePlanResult> {
  const rootDir = path.resolve(planDir);
  const paths = {
    planDag: path.join(rootDir, "plan", "plan.dag.json"),
    acceptance: path.join(rootDir, "plan", "acceptance.json"),
    retry: path.join(rootDir, "plan", "retry.json"),
    compileReport: path.join(rootDir, "report", "compile_report.json"),
  };

  const errors: string[] = [];
  const warnings: string[] = [];

  const [dagRaw, acceptanceRaw, retryRaw, reportRaw] = await Promise.all(
    Object.values(paths).map(async (p) => {
      try {
        return await readJson(p);
      } catch (err) {
        errors.push(`Failed to read ${p}: ${String(err)}`);
        return null;
      }
    }),
  );

  if (errors.length > 0) {
    return { ok: false, errors, warnings, paths };
  }

  const dagParsed = PlanDagSchema.safeParse(dagRaw);
  if (!dagParsed.success) {
    errors.push(`Invalid plan DAG schema: ${dagParsed.error.message}`);
  }
  const acceptanceParsed = AcceptanceSpecSchema.safeParse(acceptanceRaw);
  if (!acceptanceParsed.success) {
    errors.push(`Invalid acceptance schema: ${acceptanceParsed.error.message}`);
  }
  const retryParsed = RetrySpecSchema.safeParse(retryRaw);
  if (!retryParsed.success) {
    errors.push(`Invalid retry schema: ${retryParsed.error.message}`);
  }
  const reportParsed = CompileReportSchema.safeParse(reportRaw);
  if (!reportParsed.success) {
    errors.push(`Invalid compile report schema: ${reportParsed.error.message}`);
  }

  if (errors.length > 0) {
    return { ok: false, errors, warnings, paths };
  }

  if (
    !dagParsed.success ||
    !acceptanceParsed.success ||
    !retryParsed.success ||
    !reportParsed.success
  ) {
    return { ok: false, errors, warnings, paths };
  }

  const dag = dagParsed.data;
  const topo = validateDag(dag);
  if (!topo.ok) {
    errors.push(...topo.errors);
  }

  const acceptance = acceptanceParsed.data;
  const needsConfirmCount = acceptance.checks.filter((c) => c.needs_confirm).length;
  if (needsConfirmCount > 0) {
    warnings.push(`Acceptance has ${needsConfirmCount} checks requiring confirmation.`);
  }

  const report = reportParsed.data;
  if (report.needsConfirm.length > 0) {
    warnings.push(
      `Compile report lists ${report.needsConfirm.length} items requiring confirmation.`,
    );
  }

  const ok = errors.length === 0;
  return {
    ok,
    errors,
    warnings,
    paths,
    data: {
      dag,
      acceptance,
      retry: retryParsed.data,
      report,
      needsConfirmCount,
    },
  };
}
