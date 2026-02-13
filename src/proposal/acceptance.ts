import type { z } from "zod";
import type { ProposalLlmClient } from "./llm.js";
import type { DiscoveryReport, ProposalEntities } from "./schema.js";
import { completeJsonWithSchema } from "./llm-json.js";
import { buildAcceptanceSuggestionPrompt } from "./prompts.js";
import { AcceptanceSpecSchema, type AcceptanceCheck, type AcceptanceSpec } from "./schema.js";

function inferOp(goal: ProposalEntities["metrics"][number]["goal"]): AcceptanceCheck["op"] {
  if (goal === "min") {
    return "<=";
  }
  if (goal === "max") {
    return ">=";
  }
  return ">=";
}

export function buildBaselineAcceptance(params: { entities: ProposalEntities }): AcceptanceSpec {
  const checks: AcceptanceCheck[] = [];

  for (const metric of params.entities.metrics ?? []) {
    checks.push({
      type: "metric_threshold",
      selector: metric.name,
      op: inferOp(metric.goal),
      value: metric.target,
      unit: metric.unit,
      needs_confirm: metric.target === undefined,
      suggested_by: metric.target === undefined ? "compiler" : "proposal",
      evidence: metric.sourceText ? [metric.sourceText] : [],
      description:
        metric.target === undefined
          ? `Set an acceptance threshold for metric "${metric.name}".`
          : `Metric "${metric.name}" meets the target.`,
    });
  }

  // Generic artifact checks for an execution layer to populate.
  checks.push({
    type: "artifact_exists",
    selector: "report/final_metrics.json",
    needs_confirm: false,
    suggested_by: "compiler",
    evidence: [],
    description: "Final metrics report exists.",
  });
  checks.push({
    type: "artifact_exists",
    selector: "report/final_report.md",
    needs_confirm: false,
    suggested_by: "compiler",
    evidence: [],
    description: "Final human-readable report exists.",
  });

  for (const deliverable of params.entities.deliverables ?? []) {
    const trimmed = String(deliverable ?? "").trim();
    if (!trimmed) {
      continue;
    }
    // Heuristic: treat path-like deliverables as artifacts
    if (
      trimmed.includes("/") ||
      trimmed.includes(".") ||
      trimmed.toLowerCase().includes("report")
    ) {
      checks.push({
        type: "artifact_exists",
        selector: trimmed,
        needs_confirm: true,
        suggested_by: "proposal",
        evidence: [],
        description: `Deliverable exists: ${trimmed}`,
      });
    }
  }

  return AcceptanceSpecSchema.parse({ checks });
}

export async function buildAcceptance(params: {
  entities: ProposalEntities;
  discovery: DiscoveryReport;
  llmClient?: ProposalLlmClient;
}): Promise<{
  spec: AcceptanceSpec;
  source: "baseline" | "llm";
  warnings: string[];
  raw?: string;
}> {
  const warnings: string[] = [];
  if (!params.llmClient) {
    return {
      spec: buildBaselineAcceptance({ entities: params.entities }),
      source: "baseline",
      warnings,
    };
  }

  const prompt = buildAcceptanceSuggestionPrompt({
    entities: params.entities,
    discovery: params.discovery,
  });

  const res = await completeJsonWithSchema({
    client: params.llmClient,
    schema: AcceptanceSpecSchema as z.ZodType<AcceptanceSpec>,
    prompt,
    maxTokens: 900,
    temperature: 0.2,
    timeoutMs: 60_000,
    attempts: 2,
  });

  if (!res.ok) {
    warnings.push(`LLM acceptance generation failed; using baseline. (${res.error})`);
    return {
      spec: buildBaselineAcceptance({ entities: params.entities }),
      source: "baseline",
      warnings,
      raw: res.raw,
    };
  }

  // Safety: enforce needs_confirm when metric thresholds are missing values.
  const spec = AcceptanceSpecSchema.parse({
    checks: (res.value.checks ?? []).map((check) => {
      if (check.type === "metric_threshold" && check.value === undefined) {
        return { ...check, needs_confirm: true };
      }
      return check;
    }),
  });

  return { spec, source: "llm", warnings, raw: res.raw };
}
