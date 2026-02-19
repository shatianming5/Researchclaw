import type { Command } from "commander";
import fs from "node:fs/promises";
import path from "node:path";
import { resolveAgentWorkspaceDir, resolveDefaultAgentId } from "../agents/agent-scope.js";
import { loadConfig } from "../config/config.js";
import { compileProposal } from "../proposal/compiler.js";
import { discoverDataset } from "../proposal/discovery.js";
import { executeProposalPlan } from "../proposal/execute.js";
import { finalizeProposalPlan } from "../proposal/finalize.js";
import { refineProposalPlan } from "../proposal/refine.js";
import { renderNeedsConfirmMd } from "../proposal/render.js";
import { acceptProposalResults } from "../proposal/results/index.js";
import { runProposalPlanSafeNodes } from "../proposal/run.js";
import { CompileReportSchema, DiscoveryModeSchema } from "../proposal/schema.js";
import {
  listSecretKeys,
  resolveKaggleCredentials,
  resolveHuggingFaceToken,
  resolveSecretsFilePath,
  setSecret,
  unsetSecret,
} from "../proposal/secrets.js";
import { validatePlanDir } from "../proposal/validate.js";
import { defaultRuntime } from "../runtime.js";
import { theme } from "../terminal/theme.js";
import { resolveUserPath, shortenHomePath } from "../utils.js";
import { runCommandWithRuntime } from "./cli-utils.js";

type ProposalCompileOpts = {
  workspace?: string;
  out?: string;
  discovery?: string;
  model?: string;
  agent?: string;
  json?: boolean;
  llm?: boolean;
};

type ProposalValidateOpts = {
  json?: boolean;
  strictResume?: boolean;
};

type ProposalReviewOpts = {
  json?: boolean;
};

type ProposalRunOpts = {
  dryRun?: boolean;
  json?: boolean;
  maxAttempts?: string;
  retryDelayMs?: string;
  commandTimeoutMs?: string;
  failOnNeedsConfirm?: boolean;
};

type ProposalDatasetSampleOpts = {
  platform?: string;
  dataset?: string;
  out?: string;
  timeoutMs?: string;
  json?: boolean;
};

type ProposalSecretsSetOpts = {
  value?: string;
  stdin?: boolean;
  json?: boolean;
};

type ProposalSecretsUnsetOpts = {
  json?: boolean;
};

type ProposalSecretsListOpts = {
  json?: boolean;
};

type ProposalSecretsDoctorOpts = {
  json?: boolean;
};

type ProposalRefineOpts = {
  dryRun?: boolean;
  json?: boolean;
  opencodeModel?: string;
  opencodeAgent?: string;
  timeoutMs?: string;
  writeAcceptance?: boolean;
};

type ProposalExecuteOpts = {
  dryRun?: boolean;
  json?: boolean;
  maxAttempts?: string;
  retryDelayMs?: string;
  commandTimeoutMs?: string;
  failOnNeedsConfirm?: boolean;
  sandbox?: boolean;
  sandboxImage?: string;
  sandboxNetwork?: string;
  repair?: boolean;
  repairAttempts?: string;
  model?: string;
  agent?: string;
  url?: string;
  token?: string;
  timeout?: string;
  invokeTimeoutMs?: string;
  gpuWaitTimeoutMs?: string;
  node?: string;
  nodeApprove?: string;
};

type ProposalAcceptOpts = {
  baseline?: string;
  json?: boolean;
};

type ProposalFinalizeOpts = {
  force?: boolean;
  json?: boolean;
};

async function readStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
  }
  return Buffer.concat(chunks).toString("utf8");
}

export function registerProposalCli(program: Command) {
  const proposal = program
    .command("proposal")
    .description("Compile a proposal.md into a task DAG, acceptance checks, and retry policies");

  proposal
    .command("compile")
    .description("Compile a proposal markdown file into a plan package directory")
    .argument("<proposal>", "Path to proposal.md")
    .option("--workspace <dir>", "Workspace directory for output (defaults to agent workspace)")
    .option(
      "--out <dir>",
      "Output directory (defaults to <workspace>/experiments/workdir/<planId>)",
    )
    .option("--discovery <mode>", "Discovery mode: off|plan|sample (default: plan)", "plan")
    .option("--model <provider/model>", "Override model for LLM extraction (optional)")
    .option(
      "--agent <id>",
      "Agent id for workspace/model selection (default: config default agent)",
    )
    .option("--json", "Output JSON", false)
    .option("--no-llm", "Disable LLM extraction; use heuristics only")
    .action(async (proposalPath: string, opts: ProposalCompileOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const cfg = loadConfig();
        const agentId = (opts.agent?.trim() || resolveDefaultAgentId(cfg)).trim();
        const workspaceDir = opts.workspace?.trim()
          ? resolveUserPath(opts.workspace.trim())
          : resolveAgentWorkspaceDir(cfg, agentId);
        await fs.mkdir(workspaceDir, { recursive: true });

        const discovery = DiscoveryModeSchema.parse(opts.discovery ?? "plan");

        const result = await compileProposal({
          proposalPath,
          cfg,
          agentId,
          workspaceDir,
          outDir: opts.out?.trim() ? resolveUserPath(opts.out.trim()) : undefined,
          discovery,
          modelOverride: opts.model?.trim(),
          useLlm: opts.llm !== false,
        });

        if (opts.json) {
          defaultRuntime.log(
            JSON.stringify(
              {
                ok: result.ok,
                planId: result.planId,
                rootDir: result.rootDir,
                warnings: result.report.warnings,
                errors: result.report.errors,
                needsConfirm: result.report.needsConfirm,
                paths: result.paths,
              },
              null,
              2,
            ),
          );
          return;
        }

        const relRoot = shortenHomePath(result.rootDir);
        defaultRuntime.log(`${theme.heading("Proposal compiled")} ${theme.success("✓")}`);
        defaultRuntime.log(`${theme.muted("Plan:")} ${theme.command(result.planId)}`);
        defaultRuntime.log(`${theme.muted("Dir:")} ${theme.command(relRoot)}`);
        if (result.report.needsConfirm.length > 0) {
          defaultRuntime.log(
            `${theme.warn("Needs confirm:")} ${result.report.needsConfirm.length} item(s)`,
          );
        }
        if (result.report.errors.length > 0) {
          defaultRuntime.log(`${theme.error("Errors:")} ${result.report.errors.length}`);
        }
        defaultRuntime.log("");
        defaultRuntime.log(`Next: openclaw proposal validate "${result.rootDir}"`);
      });
    });

  proposal
    .command("validate")
    .description("Validate a compiled plan package directory")
    .argument("<planDir>", "Plan package directory (experiments/workdir/<planId>)")
    .option(
      "--strict-resume",
      "Enforce checkpoint/resume contract (requires train wrapper scripts + manifest outputs)",
      false,
    )
    .option("--json", "Output JSON", false)
    .action(async (planDir: string, opts: ProposalValidateOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const res = await validatePlanDir(planDir, { strictResume: Boolean(opts.strictResume) });
        if (opts.json) {
          defaultRuntime.log(JSON.stringify(res, null, 2));
          if (!res.ok) {
            defaultRuntime.exit(1);
          }
          return;
        }

        if (res.ok) {
          defaultRuntime.log(`${theme.success("✓")} Plan package is valid.`);
        } else {
          defaultRuntime.log(`${theme.error("✗")} Plan package is invalid.`);
        }
        for (const warning of res.warnings) {
          defaultRuntime.log(theme.warn(`- ${warning}`));
        }
        for (const error of res.errors) {
          defaultRuntime.log(theme.error(`- ${error}`));
        }
        if (!res.ok) {
          defaultRuntime.exit(1);
        }
      });
    });

  proposal
    .command("review")
    .description("Show the Needs Confirm report for a plan package directory")
    .argument("<planDir>", "Plan package directory (experiments/workdir/<planId>)")
    .option("--json", "Output JSON", false)
    .action(async (planDir: string, opts: ProposalReviewOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const rootDir = path.resolve(planDir);
        const reportPath = path.join(rootDir, "report", "compile_report.json");
        const raw = await fs.readFile(reportPath, "utf-8");
        const parsed = CompileReportSchema.parse(JSON.parse(raw) as unknown);
        if (opts.json) {
          defaultRuntime.log(
            JSON.stringify({ planId: parsed.planId, needsConfirm: parsed.needsConfirm }, null, 2),
          );
          return;
        }
        defaultRuntime.log(renderNeedsConfirmMd(parsed));
      });
    });

  proposal
    .command("run")
    .description(
      "Run the safe subset of nodes (fetch_repo, fetch_dataset_sample, static_checks) in a plan package directory",
    )
    .argument("<planDir>", "Plan package directory (experiments/workdir/<planId>)")
    .option("--dry-run", "Do not execute; only validate and render suggestions", false)
    .option("--max-attempts <n>", "Max attempts for retryable actions (default: 3)", "3")
    .option("--retry-delay-ms <ms>", "Base retry delay in ms (default: 1500)", "1500")
    .option("--command-timeout-ms <ms>", "Timeout per command in ms (default: 120000)", "120000")
    .option("--fail-on-needs-confirm", "Fail if plan has needs_confirm items", false)
    .option("--json", "Output JSON", false)
    .action(async (planDir: string, opts: ProposalRunOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const maxAttempts = Math.max(1, Math.floor(Number(opts.maxAttempts ?? "3")));
        const retryDelayMs = Math.max(0, Math.floor(Number(opts.retryDelayMs ?? "1500")));
        const commandTimeoutMs = Math.max(
          5_000,
          Math.floor(Number(opts.commandTimeoutMs ?? "120000")),
        );

        const res = await runProposalPlanSafeNodes({
          planDir,
          opts: {
            dryRun: Boolean(opts.dryRun),
            maxAttempts,
            retryDelayMs,
            commandTimeoutMs,
            failOnNeedsConfirm: Boolean(opts.failOnNeedsConfirm),
          },
        });

        if (opts.json) {
          defaultRuntime.log(JSON.stringify(res, null, 2));
          if (!res.ok) {
            defaultRuntime.exit(1);
          }
          return;
        }

        const okCount = res.results.filter((r) => r.status === "ok").length;
        const skippedCount = res.results.filter((r) => r.status === "skipped").length;
        const failedCount = res.results.filter((r) => r.status === "failed").length;

        defaultRuntime.log(
          `${theme.heading("Proposal run")} ${res.ok ? theme.success("✓") : theme.error("✗")}`,
        );
        if (res.planId) {
          defaultRuntime.log(`${theme.muted("Plan:")} ${theme.command(res.planId)}`);
        }
        defaultRuntime.log(`${theme.muted("Dir:")} ${theme.command(shortenHomePath(res.planDir))}`);
        defaultRuntime.log(
          `${theme.muted("Results:")} ok=${okCount} skipped=${skippedCount} failed=${failedCount}`,
        );
        for (const w of res.warnings) {
          defaultRuntime.log(theme.warn(`- ${w}`));
        }
        for (const e of res.errors) {
          defaultRuntime.log(theme.error(`- ${e}`));
        }
        defaultRuntime.log("");
        defaultRuntime.log(
          `${theme.muted("Log:")} ${theme.command(shortenHomePath(res.paths.runLog))}`,
        );
        defaultRuntime.log(
          `${theme.muted("Suggestions:")} ${theme.command(shortenHomePath(res.paths.suggestions))}`,
        );

        if (!res.ok) {
          defaultRuntime.exit(1);
        }
      });
    });

  const dataset = proposal.command("dataset").description("Dataset helpers for proposal plans");
  dataset
    .command("sample")
    .description("Fetch a small sample for a HuggingFace dataset into cache/data/<label>")
    .option("--platform <platform>", "Dataset platform override: hf|kaggle (default: infer)")
    .requiredOption("--dataset <idOrUrl>", "HuggingFace dataset id or URL")
    .requiredOption("--out <dir>", "Output directory (relative to planDir)")
    .option("--timeout-ms <ms>", "Network timeout per request in ms (default: 12000)", "12000")
    .option("--json", "Output JSON", false)
    .action(async (opts: ProposalDatasetSampleOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const datasetArg = (opts.dataset ?? "").trim();
        const outRelRaw = (opts.out ?? "").trim();
        if (!datasetArg) {
          defaultRuntime.error("error: --dataset is required");
          defaultRuntime.exit(1);
          return;
        }
        if (!outRelRaw) {
          defaultRuntime.error("error: --out is required");
          defaultRuntime.exit(1);
          return;
        }

        const outRel = outRelRaw.replaceAll("\\", "/");
        if (!outRel.startsWith("cache/data/")) {
          defaultRuntime.error('error: --out must be under "cache/data/"');
          defaultRuntime.exit(1);
          return;
        }
        if (outRel.includes("/../") || outRel.startsWith("../") || outRel.startsWith("/")) {
          defaultRuntime.error("error: --out must be a safe relative path");
          defaultRuntime.exit(1);
          return;
        }

        const baseAbs = path.resolve(process.cwd(), "cache", "data");
        const outAbs = path.resolve(process.cwd(), outRel);
        const rel = path.relative(baseAbs, outAbs);
        if (rel.startsWith("..") || path.isAbsolute(rel)) {
          defaultRuntime.error('error: --out must be under "cache/data/"');
          defaultRuntime.exit(1);
          return;
        }

        const timeoutMs = Math.max(1_000, Math.floor(Number(opts.timeoutMs ?? "12000")));

        const platformRaw = (opts.platform ?? "").trim();
        const platform: "hf" | "kaggle" | undefined =
          platformRaw === "hf" ? "hf" : platformRaw === "kaggle" ? "kaggle" : undefined;

        const isUrl = datasetArg.startsWith("http://") || datasetArg.startsWith("https://");
        const input = isUrl
          ? {
              url: datasetArg,
              platform: platform ?? ("hf" as const),
              hintText: datasetArg,
            }
          : {
              name: datasetArg,
              platform: platform ?? ("hf" as const),
              hintText: datasetArg,
            };

        const discovered = await discoverDataset({ input, mode: "sample", timeoutMs });
        if (discovered.exists === false) {
          defaultRuntime.error(
            `error: dataset sample fetch failed (${discovered.warnings.join("; ")})`,
          );
          defaultRuntime.exit(1);
          return;
        }

        await fs.mkdir(outAbs, { recursive: true });
        await fs.writeFile(
          path.join(outAbs, "discovered.json"),
          `${JSON.stringify(discovered, null, 2)}\n`,
          "utf-8",
        );
        if (discovered.sample !== undefined) {
          await fs.writeFile(
            path.join(outAbs, "sample.json"),
            `${JSON.stringify(discovered.sample, null, 2)}\n`,
            "utf-8",
          );
        }

        if (opts.json) {
          defaultRuntime.log(
            JSON.stringify(
              {
                ok: true,
                dataset: discovered.resolvedId ?? datasetArg,
                outDir: outRel,
                warnings: discovered.warnings,
              },
              null,
              2,
            ),
          );
          return;
        }

        defaultRuntime.log(
          `${theme.success("✓")} Wrote dataset sample to ${theme.command(outRel)}`,
        );
        for (const warning of discovered.warnings) {
          defaultRuntime.log(theme.warn(`- ${warning}`));
        }
      });
    });

  const secrets = proposal
    .command("secrets")
    .description("Manage proposal secrets (HuggingFace token, Kaggle credentials)");

  secrets
    .command("set")
    .description("Set a secret key/value under $OPENCLAW_STATE_DIR/credentials/secrets.json")
    .argument("<key>", "Secret key (e.g. huggingface.token, kaggle.username, kaggle.key)")
    .option("--value <value>", "Secret value (use --stdin for safer entry)")
    .option("--stdin", "Read the secret value from stdin", false)
    .option("--json", "Output JSON", false)
    .action(async (key: string, opts: ProposalSecretsSetOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const resolvedKey = key.trim();
        if (!resolvedKey) {
          defaultRuntime.error("error: key is required");
          defaultRuntime.exit(1);
          return;
        }

        const rawValue = opts.stdin ? await readStdin() : String(opts.value ?? "");
        const value = rawValue.trim();
        if (!value) {
          defaultRuntime.error("error: secret value is empty");
          defaultRuntime.exit(1);
          return;
        }

        const snapshot = await setSecret({ key: resolvedKey, value });
        const keys = Object.keys(snapshot.file.secrets).toSorted((a, b) => a.localeCompare(b));

        if (opts.json) {
          defaultRuntime.log(JSON.stringify({ ok: true, path: snapshot.path, keys }, null, 2));
          return;
        }

        defaultRuntime.log(`${theme.success("✓")} Saved secret ${theme.command(resolvedKey)}`);
        defaultRuntime.log(
          `${theme.muted("File:")} ${theme.command(shortenHomePath(snapshot.path))}`,
        );
        defaultRuntime.log(`${theme.muted("Keys:")} ${keys.length}`);
      });
    });

  secrets
    .command("unset")
    .description("Remove a secret key")
    .argument("<key>", "Secret key")
    .option("--json", "Output JSON", false)
    .action(async (key: string, opts: ProposalSecretsUnsetOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const resolvedKey = key.trim();
        if (!resolvedKey) {
          defaultRuntime.error("error: key is required");
          defaultRuntime.exit(1);
          return;
        }
        const res = await unsetSecret({ key: resolvedKey });
        const keys = Object.keys(res.snapshot.file.secrets).toSorted((a, b) => a.localeCompare(b));

        if (opts.json) {
          defaultRuntime.log(
            JSON.stringify(
              { ok: true, changed: res.changed, path: res.snapshot.path, keys },
              null,
              2,
            ),
          );
          return;
        }

        defaultRuntime.log(
          res.changed
            ? `${theme.success("✓")} Removed secret ${theme.command(resolvedKey)}`
            : `${theme.muted("·")} Secret ${theme.command(resolvedKey)} not present`,
        );
        defaultRuntime.log(
          `${theme.muted("File:")} ${theme.command(shortenHomePath(res.snapshot.path))}`,
        );
      });
    });

  secrets
    .command("list")
    .description("List configured secret keys")
    .option("--json", "Output JSON", false)
    .action(async (opts: ProposalSecretsListOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const keys = await listSecretKeys();
        const secretsPath = resolveSecretsFilePath();
        if (opts.json) {
          defaultRuntime.log(JSON.stringify({ ok: true, path: secretsPath, keys }, null, 2));
          return;
        }
        defaultRuntime.log(theme.heading("Proposal secrets"));
        defaultRuntime.log(
          `${theme.muted("File:")} ${theme.command(shortenHomePath(secretsPath))}`,
        );
        if (keys.length === 0) {
          defaultRuntime.log(theme.muted("No secrets configured."));
          return;
        }
        for (const key of keys) {
          defaultRuntime.log(`- ${theme.command(key)}`);
        }
      });
    });

  secrets
    .command("doctor")
    .description("Check whether common secrets are configured (values are never printed)")
    .option("--json", "Output JSON", false)
    .action(async (opts: ProposalSecretsDoctorOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const secretsPath = resolveSecretsFilePath();
        const hf = await resolveHuggingFaceToken();
        const kaggle = await resolveKaggleCredentials();

        const status = {
          "huggingface.token": Boolean(hf),
          "kaggle.username": Boolean(kaggle?.username),
          "kaggle.key": Boolean(kaggle?.key),
        };
        const missing = Object.entries(status)
          .filter(([, ok]) => !ok)
          .map(([key]) => key);

        if (opts.json) {
          defaultRuntime.log(
            JSON.stringify({ ok: true, path: secretsPath, configured: status, missing }, null, 2),
          );
          return;
        }

        defaultRuntime.log(theme.heading("Proposal secrets doctor"));
        defaultRuntime.log(
          `${theme.muted("File:")} ${theme.command(shortenHomePath(secretsPath))}`,
        );
        for (const [key, ok] of Object.entries(status)) {
          defaultRuntime.log(ok ? `${theme.success("✓")} ${key}` : `${theme.warn("!")} ${key}`);
        }
        if (missing.length > 0) {
          defaultRuntime.log("");
          defaultRuntime.log(theme.muted("Tip: set via:"));
          defaultRuntime.log(
            `  ${theme.command("openclaw proposal secrets set huggingface.token --stdin")}`,
          );
          defaultRuntime.log(
            `  ${theme.command("openclaw proposal secrets set kaggle.username --stdin")}`,
          );
          defaultRuntime.log(
            `  ${theme.command("openclaw proposal secrets set kaggle.key --stdin")}`,
          );
        }
      });
    });

  proposal
    .command("refine")
    .description(
      "Generate executable commands for train/eval/report nodes using OpenCode (opencode CLI)",
    )
    .argument("<planDir>", "Plan package directory (experiments/workdir/<planId>)")
    .option("--dry-run", "Do not write updated DAG; only render refine artifacts", false)
    .option(
      "--opencode-model <provider/model>",
      "OpenCode model id (default: opencode/kimi-k2.5-free)",
      "opencode/kimi-k2.5-free",
    )
    .option("--opencode-agent <name>", "OpenCode agent name (optional)")
    .option("--write-acceptance", "Write plan/acceptance.json from refine output (optional)", false)
    .option("--timeout-ms <ms>", "OpenCode timeout in ms (default: 180000)", "180000")
    .option("--json", "Output JSON", false)
    .action(async (planDir: string, opts: ProposalRefineOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const timeoutMs = Math.max(10_000, Math.floor(Number(opts.timeoutMs ?? "180000")));
        const model = opts.opencodeModel?.trim() || "opencode/kimi-k2.5-free";

        const res = await refineProposalPlan({
          planDir,
          opts: {
            dryRun: Boolean(opts.dryRun),
            model,
            agent: opts.opencodeAgent?.trim() || undefined,
            timeoutMs,
            writeAcceptance: Boolean(opts.writeAcceptance),
          },
        });

        if (opts.json) {
          defaultRuntime.log(JSON.stringify(res, null, 2));
          if (!res.ok) {
            defaultRuntime.exit(1);
          }
          return;
        }

        defaultRuntime.log(
          `${theme.heading("Proposal refine")} ${res.ok ? theme.success("✓") : theme.error("✗")}`,
        );
        if (res.planId) {
          defaultRuntime.log(`${theme.muted("Plan:")} ${theme.command(res.planId)}`);
        }
        defaultRuntime.log(`${theme.muted("Dir:")} ${theme.command(shortenHomePath(res.planDir))}`);
        for (const w of res.warnings) {
          defaultRuntime.log(theme.warn(`- ${w}`));
        }
        for (const e of res.errors) {
          defaultRuntime.log(theme.error(`- ${e}`));
        }
        defaultRuntime.log("");
        defaultRuntime.log(
          `${theme.muted("Summary:")} ${theme.command(shortenHomePath(res.paths.refineSummary))}`,
        );
        defaultRuntime.log(
          `${theme.muted("Report:")} ${theme.command(shortenHomePath(res.paths.refineReport))}`,
        );

        if (!res.ok) {
          defaultRuntime.exit(1);
        }
      });
    });

  proposal
    .command("execute")
    .description(
      "Execute a compiled plan DAG using sandbox exec (CPU nodes) and node.invoke (GPU nodes)",
    )
    .argument("<planDir>", "Plan package directory (experiments/workdir/<planId>)")
    .option("--dry-run", "Do not execute; only validate and render artifacts", false)
    .option("--max-attempts <n>", "Max attempts per node (default: 3)", "3")
    .option("--retry-delay-ms <ms>", "Base retry delay in ms (default: 1500)", "1500")
    .option("--command-timeout-ms <ms>", "Timeout per command in ms (default: 600000)", "600000")
    .option("--fail-on-needs-confirm", "Fail if plan has needs_confirm items", false)
    .option("--no-sandbox", "Run CPU commands on host (unsafe)")
    .option("--sandbox-image <image>", "Docker image for sandbox execution (optional)")
    .option("--sandbox-network <name>", "Docker network for sandbox container (default: bridge)")
    .option("--no-repair", "Disable LLM repair loop")
    .option("--repair-attempts <n>", "Max repair patches per node (default: 1)", "1")
    .option("--model <provider/model>", "Override model for repair LLM (optional)")
    .option(
      "--agent <id>",
      "Agent id for workspace/model selection (default: plan context or config default agent)",
    )
    .option("--node <idOrNameOrIp>", "Node id/name/ip for GPU nodes (optional)")
    .option(
      "--node-approve <off|allow-once|allow-always>",
      "Bypass node exec approvals (default: off)",
      "off",
    )
    .option("--url <url>", "Gateway WebSocket URL (optional)")
    .option("--token <token>", "Gateway token (if required)")
    .option("--timeout <ms>", "Gateway timeout in ms (default: 30000)", "30000")
    .option("--invoke-timeout-ms <ms>", "Node invoke timeout in ms (default: 1200000)", "1200000")
    .option(
      "--gpu-wait-timeout-ms <ms>",
      "Wait for eligible GPU nodes before failing (default: 0 = fail fast)",
      "0",
    )
    .option("--json", "Output JSON", false)
    .action(async (planDir: string, opts: ProposalExecuteOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const cfg = loadConfig();
        const maxAttempts = Math.max(1, Math.floor(Number(opts.maxAttempts ?? "3")));
        const retryDelayMs = Math.max(0, Math.floor(Number(opts.retryDelayMs ?? "1500")));
        const commandTimeoutMs = Math.max(
          5_000,
          Math.floor(Number(opts.commandTimeoutMs ?? "600000")),
        );
        const gatewayTimeoutMs = Math.max(5_000, Math.floor(Number(opts.timeout ?? "30000")));
        const invokeTimeoutMs = Math.max(
          5_000,
          Math.floor(Number(opts.invokeTimeoutMs ?? "1200000")),
        );
        const gpuWaitTimeoutMs = Math.max(0, Math.floor(Number(opts.gpuWaitTimeoutMs ?? "0")));
        const repairAttempts = Math.max(0, Math.floor(Number(opts.repairAttempts ?? "1")));

        const res = await executeProposalPlan({
          planDir,
          cfg,
          opts: {
            dryRun: Boolean(opts.dryRun),
            maxAttempts,
            retryDelayMs,
            commandTimeoutMs,
            failOnNeedsConfirm: Boolean(opts.failOnNeedsConfirm),
            sandbox: opts.sandbox !== false,
            sandboxImage: opts.sandboxImage?.trim() || undefined,
            sandboxNetwork: opts.sandboxNetwork?.trim() || undefined,
            repair: opts.repair !== false,
            repairAttempts,
            modelOverride: opts.model?.trim() || undefined,
            agentId: opts.agent?.trim() || undefined,
            node: opts.node?.trim() || undefined,
            nodeApprove:
              opts.nodeApprove === "allow-once" || opts.nodeApprove === "allow-always"
                ? opts.nodeApprove
                : "off",
            gatewayUrl: opts.url?.trim() || undefined,
            gatewayToken: opts.token?.trim() || undefined,
            gatewayTimeoutMs,
            invokeTimeoutMs,
            gpuWaitTimeoutMs,
          },
        });

        if (opts.json) {
          defaultRuntime.log(JSON.stringify(res, null, 2));
          if (!res.ok) {
            defaultRuntime.exit(1);
          }
          return;
        }

        const okCount = res.results.filter((r) => r.status === "ok").length;
        const skippedCount = res.results.filter((r) => r.status === "skipped").length;
        const failedCount = res.results.filter((r) => r.status === "failed").length;

        defaultRuntime.log(
          `${theme.heading("Proposal execute")} ${res.ok ? theme.success("✓") : theme.error("✗")}`,
        );
        if (res.planId) {
          defaultRuntime.log(`${theme.muted("Plan:")} ${theme.command(res.planId)}`);
        }
        defaultRuntime.log(`${theme.muted("Dir:")} ${theme.command(shortenHomePath(res.planDir))}`);
        defaultRuntime.log(
          `${theme.muted("Results:")} ok=${okCount} skipped=${skippedCount} failed=${failedCount}`,
        );
        for (const w of res.warnings) {
          defaultRuntime.log(theme.warn(`- ${w}`));
        }
        for (const e of res.errors) {
          defaultRuntime.log(theme.error(`- ${e}`));
        }
        defaultRuntime.log("");
        defaultRuntime.log(
          `${theme.muted("Log:")} ${theme.command(shortenHomePath(res.paths.executeLog))}`,
        );
        defaultRuntime.log(
          `${theme.muted("Summary:")} ${theme.command(shortenHomePath(res.paths.executeSummary))}`,
        );

        if (!res.ok) {
          defaultRuntime.exit(1);
        }
      });
    });

  proposal
    .command("finalize")
    .description("Generate report/final_metrics.json and report/final_report.md for a plan package")
    .argument("<planDir>", "Plan package directory (experiments/workdir/<planId>)")
    .option("--force", "Overwrite existing artifacts", false)
    .option("--json", "Output JSON", false)
    .action(async (planDir: string, opts: ProposalFinalizeOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const res = await finalizeProposalPlan({
          planDir,
          opts: { force: Boolean(opts.force) },
        });

        if (opts.json) {
          defaultRuntime.log(JSON.stringify(res, null, 2));
          if (!res.ok) {
            defaultRuntime.exit(1);
          }
          return;
        }

        defaultRuntime.log(
          `${theme.heading("Proposal finalize")} ${res.ok ? theme.success("✓") : theme.error("✗")}`,
        );
        if (res.planId) {
          defaultRuntime.log(`${theme.muted("Plan:")} ${theme.command(res.planId)}`);
        }
        defaultRuntime.log(`${theme.muted("Dir:")} ${theme.command(shortenHomePath(res.planDir))}`);
        defaultRuntime.log(
          `${theme.muted("Wrote:")} final_metrics=${res.wrote.finalMetrics ? "yes" : "no"} final_report=${
            res.wrote.finalReport ? "yes" : "no"
          }`,
        );

        for (const w of res.warnings) {
          defaultRuntime.log(theme.warn(`- ${w}`));
        }
        for (const e of res.errors) {
          defaultRuntime.log(theme.error(`- ${e}`));
        }

        defaultRuntime.log("");
        defaultRuntime.log(
          `${theme.muted("Final metrics:")} ${theme.command(shortenHomePath(res.paths.finalMetrics))}`,
        );
        defaultRuntime.log(
          `${theme.muted("Final report:")} ${theme.command(shortenHomePath(res.paths.finalReport))}`,
        );

        if (!res.ok) {
          defaultRuntime.exit(1);
        }
      });
    });

  proposal
    .command("accept")
    .description("Evaluate acceptance checks and archive experiment artifacts for a plan package")
    .argument("<planDir>", "Plan package directory (experiments/workdir/<planId>)")
    .option(
      "--baseline <path>",
      "Baseline metrics JSON file for comparison (defaults to latest run snapshot if available)",
    )
    .option("--json", "Output JSON", false)
    .action(async (planDir: string, opts: ProposalAcceptOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const rootDir = path.resolve(planDir);
        const baselinePath = opts.baseline?.trim()
          ? resolveUserPath(opts.baseline.trim())
          : undefined;

        const res = await acceptProposalResults({ planDir: rootDir, baselinePath });

        if (opts.json) {
          defaultRuntime.log(JSON.stringify(res, null, 2));
          defaultRuntime.exit(res.exitCode);
          return;
        }

        defaultRuntime.log(
          `${theme.heading("Proposal accept")} ${
            res.status === "pass"
              ? theme.success("✓")
              : res.status === "needs_confirm"
                ? theme.warn("!")
                : theme.error("✗")
          }`,
        );
        if (res.planId) {
          defaultRuntime.log(`${theme.muted("Plan:")} ${theme.command(res.planId)}`);
        }
        defaultRuntime.log(`${theme.muted("Dir:")} ${theme.command(shortenHomePath(res.planDir))}`);
        defaultRuntime.log(
          `${theme.muted("Checks:")} pass=${res.summary.pass} fail=${res.summary.fail} needs_confirm=${res.summary.needs_confirm}`,
        );
        if (res.metrics?.baselinePath) {
          defaultRuntime.log(
            `${theme.muted("Baseline:")} ${theme.command(shortenHomePath(res.metrics.baselinePath))}`,
          );
        }
        defaultRuntime.log(`${theme.muted("Run:")} ${theme.command(shortenHomePath(res.runDir))}`);
        defaultRuntime.log(
          `${theme.muted("Report:")} ${theme.command(shortenHomePath(res.paths.reportJson))}`,
        );
        defaultRuntime.log(
          `${theme.muted("Manifest:")} ${theme.command(shortenHomePath(res.paths.manifestJson))}`,
        );

        for (const warning of res.warnings) {
          defaultRuntime.log(theme.warn(`- ${warning}`));
        }
        for (const error of res.errors) {
          defaultRuntime.log(theme.error(`- ${error}`));
        }

        defaultRuntime.exit(res.exitCode);
      });
    });
}
