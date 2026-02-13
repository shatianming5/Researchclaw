import type { Command } from "commander";
import fs from "node:fs/promises";
import path from "node:path";
import { resolveAgentWorkspaceDir, resolveDefaultAgentId } from "../agents/agent-scope.js";
import { loadConfig } from "../config/config.js";
import { compileProposal } from "../proposal/compiler.js";
import { executeProposalPlan } from "../proposal/execute.js";
import { refineProposalPlan } from "../proposal/refine.js";
import { renderNeedsConfirmMd } from "../proposal/render.js";
import { acceptProposalResults } from "../proposal/results/index.js";
import { runProposalPlanSafeNodes } from "../proposal/run.js";
import { CompileReportSchema, DiscoveryModeSchema } from "../proposal/schema.js";
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

type ProposalRefineOpts = {
  dryRun?: boolean;
  json?: boolean;
  opencodeModel?: string;
  opencodeAgent?: string;
  timeoutMs?: string;
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
  node?: string;
  nodeApprove?: string;
};

type ProposalAcceptOpts = {
  baseline?: string;
  json?: boolean;
};

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
    .option("--json", "Output JSON", false)
    .action(async (planDir: string, opts: ProposalValidateOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const res = await validatePlanDir(planDir);
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
