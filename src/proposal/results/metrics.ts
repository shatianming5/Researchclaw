import type { MetricDelta, MetricValue } from "./schema.js";

function metricToNumber(value: MetricValue | undefined): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

export function computeMetricDeltas(params: {
  current: Record<string, MetricValue>;
  baseline?: Record<string, MetricValue>;
}): MetricDelta[] {
  const keys = new Set<string>(Object.keys(params.current));
  for (const key of Object.keys(params.baseline ?? {})) {
    keys.add(key);
  }

  const out: MetricDelta[] = [];
  for (const name of [...keys].toSorted()) {
    const current = params.current[name];
    const baseline = params.baseline?.[name];
    const currentNum = metricToNumber(current);
    const baselineNum = metricToNumber(baseline);
    out.push({
      name,
      current,
      baseline,
      delta: currentNum !== null && baselineNum !== null ? currentNum - baselineNum : null,
    });
  }
  return out;
}
