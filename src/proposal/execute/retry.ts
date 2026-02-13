import type { FailureCategory, PlanNode, RetryPolicy, RetrySpec } from "../schema.js";

export function resolveRetryPolicy(params: {
  retry: RetrySpec;
  node: PlanNode;
}): RetryPolicy | null {
  const policies = params.retry.policies ?? [];
  if (params.node.retryPolicyId) {
    return policies.find((p) => p.id === params.node.retryPolicyId) ?? null;
  }
  if (params.retry.defaultPolicyId) {
    return policies.find((p) => p.id === params.retry.defaultPolicyId) ?? null;
  }
  return policies.find((p) => p.category === "unknown") ?? policies[0] ?? null;
}

export function classifyFailure(params: {
  retry: RetrySpec;
  node: PlanNode;
  output: string;
}): FailureCategory {
  const policies = params.retry.policies ?? [];
  const haystack = params.output.toLowerCase();
  for (const policy of policies) {
    for (const pattern of policy.retryablePatterns ?? []) {
      const needle = pattern.trim().toLowerCase();
      if (needle && haystack.includes(needle)) {
        return policy.category;
      }
    }
  }
  const byId = resolveRetryPolicy({ retry: params.retry, node: params.node })?.category;
  return byId ?? "unknown";
}

export function computeBackoffMs(
  policy: RetryPolicy | null,
  attempt: number,
  fallbackMs: number,
): number {
  const backoff = policy?.backoff;
  if (!backoff) {
    return fallbackMs;
  }
  const base = Math.max(0, Math.floor(backoff.baseMs ?? fallbackMs));
  const max = Math.max(base, Math.floor(backoff.maxMs ?? base));
  if (backoff.kind === "fixed") {
    return Math.min(base, max);
  }
  const exp = Math.min(max, base * Math.pow(2, Math.max(0, attempt - 1)));
  if (backoff.jitter) {
    return Math.floor(exp * (0.75 + Math.random() * 0.5));
  }
  return Math.floor(exp);
}
