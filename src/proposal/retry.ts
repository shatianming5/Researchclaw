import type { RetryPolicy, RetrySpec } from "./schema.js";

function policy(p: RetryPolicy): RetryPolicy {
  return p;
}

export function buildDefaultRetrySpec(): RetrySpec {
  const policies: RetryPolicy[] = [
    policy({
      id: "retry.network",
      category: "network",
      maxAttempts: 5,
      backoff: { kind: "exponential", baseMs: 1_000, maxMs: 30_000, jitter: true },
      retryablePatterns: [
        "ECONNRESET",
        "ETIMEDOUT",
        "ENOTFOUND",
        "EAI_AGAIN",
        "socket hang up",
        "HTTP 502",
        "HTTP 503",
      ],
      repairActions: [
        "Retry with exponential backoff",
        "Switch to a different mirror if configured",
      ],
    }),
    policy({
      id: "retry.rate_limit",
      category: "rate_limit",
      maxAttempts: 6,
      backoff: { kind: "exponential", baseMs: 2_000, maxMs: 120_000, jitter: true },
      retryablePatterns: ["HTTP 429", "rate limit", "Too Many Requests"],
      repairActions: [
        "Respect Retry-After if provided",
        "Reduce concurrency",
        "Use a fallback model/provider",
      ],
    }),
    policy({
      id: "retry.build_fail",
      category: "build_fail",
      maxAttempts: 2,
      backoff: { kind: "fixed", baseMs: 2_000, maxMs: 2_000, jitter: false },
      retryablePatterns: ["npm ERR!", "pnpm", "pip", "build failed"],
      repairActions: [
        "Retry once",
        "Pin dependency versions",
        "Switch package mirror (if available)",
      ],
    }),
    policy({
      id: "retry.test_fail",
      category: "test_fail",
      maxAttempts: 1,
      retryablePatterns: [],
      repairActions: ["Fail fast and route to manual review"],
    }),
    policy({
      id: "retry.oom",
      category: "oom",
      maxAttempts: 4,
      backoff: { kind: "fixed", baseMs: 1_000, maxMs: 1_000, jitter: false },
      retryablePatterns: ["out of memory", "CUDA out of memory", "OOM"],
      repairActions: [
        "Reduce batch size",
        "Enable gradient accumulation",
        "Enable mixed precision (fp16/bf16)",
        "Reduce sequence length / input size",
      ],
    }),
    policy({
      id: "retry.divergence",
      category: "divergence",
      maxAttempts: 2,
      backoff: { kind: "fixed", baseMs: 1_000, maxMs: 1_000, jitter: false },
      retryablePatterns: ["nan", "diverged", "loss exploded"],
      repairActions: [
        "Reduce learning rate",
        "Add warmup",
        "Enable gradient clipping",
        "Fix random seed",
      ],
    }),
    policy({
      id: "retry.data_missing",
      category: "data_missing",
      maxAttempts: 2,
      backoff: { kind: "fixed", baseMs: 1_000, maxMs: 1_000, jitter: false },
      retryablePatterns: ["No such file or directory", "FileNotFoundError", "dataset not found"],
      repairActions: [
        "Re-check dataset paths",
        "Re-run dataset fetch node",
        "Validate splits/config names",
      ],
    }),
    policy({
      id: "retry.unknown",
      category: "unknown",
      maxAttempts: 2,
      backoff: { kind: "fixed", baseMs: 2_000, maxMs: 2_000, jitter: false },
      retryablePatterns: [],
      repairActions: ["Retry once", "Route to manual review with logs"],
    }),
  ];

  return { policies, defaultPolicyId: "retry.unknown" };
}
