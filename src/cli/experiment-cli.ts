import type { Command } from "commander";
import fs from "node:fs/promises";
import { resolveAgentWorkspaceDir, resolveDefaultAgentId } from "../agents/agent-scope.js";
import { loadConfig } from "../config/config.js";
import { runExperimentPipeline } from "../proposal/pipeline.js";
import { DiscoveryModeSchema } from "../proposal/schema.js";
import { runExperimentSuite } from "../proposal/suite.js";
import { defaultRuntime } from "../runtime.js";
import { theme } from "../terminal/theme.js";
import { resolveUserPath, shortenHomePath } from "../utils.js";
import { runCommandWithRuntime } from "./cli-utils.js";

type ExperimentRunOpts = {
  input?: string;
  message?: string;
  workspace?: string;
  out?: string;
  discovery?: string;
  compileModel?: string;
  agent?: string;
  llm?: boolean;
  dryRun?: boolean;
  json?: boolean;
  suiteCount?: string;
  suiteOut?: string;
  concurrency?: string;
  maxAttempts?: string;
  retryDelayMs?: string;
  commandTimeoutMs?: string;
  failOnNeedsConfirm?: boolean;
  opencodeModel?: string;
  opencodeAgent?: string;
  opencodeTimeoutMs?: string;
  writeAcceptance?: boolean;
  bootstrap?: boolean;
  bootstrapMode?: string;
  bootstrapModel?: string;
  bootstrapAgent?: string;
  bootstrapTimeoutMs?: string;
  bootstrapMaxAttempts?: string;
  bootstrapInstructions?: string;
  sandbox?: boolean;
  sandboxImage?: string;
  sandboxNetwork?: string;
  repair?: boolean;
  repairAttempts?: string;
  repairModel?: string;
  url?: string;
  token?: string;
  gatewayTimeoutMs?: string;
  invokeTimeoutMs?: string;
  gpuWaitTimeoutMs?: string;
  node?: string;
  nodeApprove?: string;
  finalizeForce?: boolean;
  baseline?: string;
};

async function resolveExistingPath(inputPath: string): Promise<{
  ok: boolean;
  resolvedPath?: string;
  isDir?: boolean;
  error?: string;
}> {
  const resolved = resolveUserPath(inputPath);
  try {
    const stat = await fs.stat(resolved);
    return { ok: true, resolvedPath: resolved, isDir: stat.isDirectory() };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

export function registerExperimentCli(program: Command) {
  const experiment = program
    .command("experiment")
    .description("Run a full proposal experiment pipeline");

  experiment
    .command("run")
    .description("Compile (optional), run safe nodes, refine, execute, finalize, and accept")
    .argument("[input]", "Path to proposal.md OR an existing planDir")
    .option("--message <text>", "Free-form proposal/task text to compile (mutually exclusive)")
    .option("--workspace <dir>", "Workspace directory for output (compile only)")
    .option("--out <dir>", "Output directory (compile only)")
    .option("--discovery <mode>", "Discovery mode: off|plan|sample (compile only)", "plan")
    .option(
      "--compile-model <provider/model>",
      "Override model for proposal extraction (compile only)",
    )
    .option("--agent <id>", "Agent id for workspace/model selection (optional)")
    .option("--no-llm", "Disable LLM extraction during compile; use heuristics only")
    .option(
      "--dry-run",
      "Do not execute commands (safe run + execute); still writes plan updates",
      false,
    )
    .option("--max-attempts <n>", "Max attempts for run/execute (default: 3)", "3")
    .option("--retry-delay-ms <ms>", "Base retry delay in ms (default: 1500)", "1500")
    .option("--command-timeout-ms <ms>", "Timeout per command in ms (default: 600000)", "600000")
    .option("--fail-on-needs-confirm", "Fail if plan has needs_confirm items", false)
    .option(
      "--opencode-model <provider/model>",
      "OpenCode model id (default: opencode/kimi-k2.5-free)",
      "opencode/kimi-k2.5-free",
    )
    .option("--opencode-agent <name>", "OpenCode agent name (optional)")
    .option("--opencode-timeout-ms <ms>", "OpenCode timeout in ms (default: 180000)", "180000")
    .option("--write-acceptance", "Write plan/acceptance.json from refine output (optional)", false)
    .option(
      "--bootstrap",
      "Run bootstrap stage to generate/apply pre-exec patches (optional)",
      false,
    )
    .option(
      "--bootstrap-mode <worktree|plan|both>",
      "Bootstrap mode (default: worktree)",
      "worktree",
    )
    .option("--bootstrap-model <provider/model>", "OpenCode model for bootstrap stage (optional)")
    .option("--bootstrap-agent <name>", "OpenCode agent name for bootstrap stage (optional)")
    .option("--bootstrap-timeout-ms <ms>", "Bootstrap timeout in ms (default: 180000)", "180000")
    .option("--bootstrap-max-attempts <n>", "Bootstrap max attempts (default: 2)", "2")
    .option("--bootstrap-instructions <text>", "Extra bootstrap instructions (optional)")
    .option("--no-sandbox", "Run CPU commands on host (unsafe)")
    .option("--sandbox-image <image>", "Docker image for sandbox execution (optional)")
    .option("--sandbox-network <name>", "Docker network for sandbox container (default: bridge)")
    .option("--no-repair", "Disable LLM repair loop")
    .option("--repair-attempts <n>", "Max repair patches per node (default: 1)", "1")
    .option("--repair-model <provider/model>", "Override model for repair LLM (optional)")
    .option("--node <idOrNameOrIp>", "Node id/name/ip for GPU nodes (optional)")
    .option(
      "--node-approve <off|allow-once|allow-always>",
      "Bypass node exec approvals (default: off)",
      "off",
    )
    .option("--url <url>", "Gateway WebSocket URL (optional)")
    .option("--token <token>", "Gateway token (if required)")
    .option("--gateway-timeout-ms <ms>", "Gateway timeout in ms (default: 30000)", "30000")
    .option("--invoke-timeout-ms <ms>", "Node invoke timeout in ms (default: 1200000)", "1200000")
    .option(
      "--gpu-wait-timeout-ms <ms>",
      "Wait for eligible GPU nodes before failing (default: 1800000)",
      "1800000",
    )
    .option("--finalize-force", "Overwrite existing final artifacts", false)
    .option("--baseline <path>", "Baseline metrics JSON file for comparison (optional)")
    .option("--json", "Output JSON (prints acceptance report only)", false)
    .action(async (input: string | undefined, opts: ExperimentRunOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const cfg = loadConfig();

        const message = (opts.message ?? "").trim();
        const inputArg = (input ?? "").trim();
        if (message && inputArg) {
          defaultRuntime.error("error: provide either <input> OR --message, not both");
          defaultRuntime.exit(1);
          return;
        }
        if (!message && !inputArg) {
          defaultRuntime.error("error: missing input; provide <input> OR --message");
          defaultRuntime.exit(1);
          return;
        }

        let pipelineInput:
          | { kind: "planDir"; planDir: string }
          | { kind: "proposalPath"; proposalPath: string }
          | { kind: "proposalMarkdown"; proposalMarkdown: string; proposalSource: string };
        let compile:
          | {
              agentId: string;
              workspaceDir: string;
              outDir?: string;
              discovery: ReturnType<typeof DiscoveryModeSchema.parse>;
              modelOverride?: string;
              useLlm?: boolean;
            }
          | undefined;

        if (message) {
          const agentId = (opts.agent?.trim() || resolveDefaultAgentId(cfg)).trim();
          const workspaceDir = opts.workspace?.trim()
            ? resolveUserPath(opts.workspace.trim())
            : resolveAgentWorkspaceDir(cfg, agentId);
          await fs.mkdir(workspaceDir, { recursive: true });

          const discovery = DiscoveryModeSchema.parse(opts.discovery ?? "plan");
          pipelineInput = {
            kind: "proposalMarkdown",
            proposalMarkdown: `# Proposal\n\n${message}\n`,
            proposalSource: "cli:experiment --message",
          };
          compile = {
            agentId,
            workspaceDir,
            outDir: opts.out?.trim() ? resolveUserPath(opts.out.trim()) : undefined,
            discovery,
            modelOverride: opts.compileModel?.trim() || undefined,
            useLlm: opts.llm !== false,
          };
        } else {
          const existing = await resolveExistingPath(inputArg);
          if (!existing.ok || !existing.resolvedPath) {
            defaultRuntime.error(`error: cannot read input path: ${inputArg}`);
            defaultRuntime.error(`error: ${existing.error}`);
            defaultRuntime.exit(1);
            return;
          }

          if (existing.isDir) {
            pipelineInput = { kind: "planDir", planDir: existing.resolvedPath };
          } else {
            const agentId = (opts.agent?.trim() || resolveDefaultAgentId(cfg)).trim();
            const workspaceDir = opts.workspace?.trim()
              ? resolveUserPath(opts.workspace.trim())
              : resolveAgentWorkspaceDir(cfg, agentId);
            await fs.mkdir(workspaceDir, { recursive: true });

            const discovery = DiscoveryModeSchema.parse(opts.discovery ?? "plan");
            pipelineInput = { kind: "proposalPath", proposalPath: existing.resolvedPath };
            compile = {
              agentId,
              workspaceDir,
              outDir: opts.out?.trim() ? resolveUserPath(opts.out.trim()) : undefined,
              discovery,
              modelOverride: opts.compileModel?.trim() || undefined,
              useLlm: opts.llm !== false,
            };
          }
        }

        const maxAttempts = Math.max(1, Math.floor(Number(opts.maxAttempts ?? "3")));
        const retryDelayMs = Math.max(0, Math.floor(Number(opts.retryDelayMs ?? "1500")));
        const commandTimeoutMs = Math.max(
          5_000,
          Math.floor(Number(opts.commandTimeoutMs ?? "600000")),
        );
        const gatewayTimeoutMs = Math.max(
          5_000,
          Math.floor(Number(opts.gatewayTimeoutMs ?? "30000")),
        );
        const invokeTimeoutMs = Math.max(
          5_000,
          Math.floor(Number(opts.invokeTimeoutMs ?? "1200000")),
        );
        const gpuWaitTimeoutMs = Math.max(
          0,
          Math.floor(Number(opts.gpuWaitTimeoutMs ?? "1800000")),
        );
        const opencodeTimeoutMs = Math.max(
          10_000,
          Math.floor(Number(opts.opencodeTimeoutMs ?? "180000")),
        );
        const bootstrapTimeoutMs = Math.max(
          10_000,
          Math.floor(Number(opts.bootstrapTimeoutMs ?? "180000")),
        );
        const bootstrapMaxAttempts = Math.max(
          1,
          Math.floor(Number(opts.bootstrapMaxAttempts ?? "2")),
        );
        const repairAttempts = Math.max(0, Math.floor(Number(opts.repairAttempts ?? "1")));

        const baselinePath = opts.baseline?.trim()
          ? resolveUserPath(opts.baseline.trim())
          : undefined;

        const bootstrapModeRaw = (opts.bootstrapMode ?? "worktree").trim();
        const bootstrapMode =
          bootstrapModeRaw === "plan" || bootstrapModeRaw === "both"
            ? bootstrapModeRaw
            : "worktree";

        const res = await runExperimentPipeline({
          action: "pipeline",
          input: pipelineInput,
          cfg,
          stages: {
            compile,
            run: {
              dryRun: Boolean(opts.dryRun),
              maxAttempts,
              retryDelayMs,
              commandTimeoutMs,
              failOnNeedsConfirm: Boolean(opts.failOnNeedsConfirm),
            },
            refine: {
              enabled: true,
              dryRun: false,
              model: opts.opencodeModel?.trim() || "opencode/kimi-k2.5-free",
              agent: opts.opencodeAgent?.trim() || undefined,
              timeoutMs: opencodeTimeoutMs,
              writeAcceptance: Boolean(opts.writeAcceptance),
            },
            ...(opts.bootstrap
              ? {
                  bootstrap: {
                    enabled: true,
                    mode: bootstrapMode,
                    dryRun: Boolean(opts.dryRun),
                    model:
                      opts.bootstrapModel?.trim() ||
                      opts.opencodeModel?.trim() ||
                      "opencode/kimi-k2.5-free",
                    agent: opts.bootstrapAgent?.trim() || undefined,
                    timeoutMs: bootstrapTimeoutMs,
                    maxAttempts: bootstrapMaxAttempts,
                    instructions: opts.bootstrapInstructions?.trim() || undefined,
                  },
                }
              : {}),
            execute: {
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
              modelOverride: opts.repairModel?.trim() || undefined,
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
            finalize: { force: Boolean(opts.finalizeForce) },
            accept: { baselinePath },
          },
        });

        if (res.compile?.ok) {
          defaultRuntime.log(`${theme.heading("Experiment compile")} ${theme.success("✓")}`);
          defaultRuntime.log(`${theme.muted("Plan:")} ${theme.command(res.compile.planId)}`);
          defaultRuntime.log(
            `${theme.muted("Dir:")} ${theme.command(shortenHomePath(res.compile.rootDir))}`,
          );
          defaultRuntime.log("");
        }

        if (res.compile && !res.compile.ok) {
          defaultRuntime.log(`${theme.heading("Experiment compile")} ${theme.error("✗")}`);
          defaultRuntime.log(
            `${theme.muted("Dir:")} ${theme.command(shortenHomePath(res.compile.rootDir))}`,
          );
          for (const w of res.compile.report.warnings) {
            defaultRuntime.log(theme.warn(`- ${w}`));
          }
          for (const e of res.compile.report.errors) {
            defaultRuntime.log(theme.error(`- ${e}`));
          }
          defaultRuntime.exit(1);
          return;
        }

        if (res.validate && (!res.validate.ok || !res.validate.data)) {
          defaultRuntime.log(`${theme.heading("Experiment validate")} ${theme.error("✗")}`);
          for (const w of res.validate.warnings) {
            defaultRuntime.log(theme.warn(`- ${w}`));
          }
          for (const e of res.validate.errors) {
            defaultRuntime.log(theme.error(`- ${e}`));
          }
          defaultRuntime.exit(1);
          return;
        }

        if (res.safe && !res.safe.ok) {
          defaultRuntime.log(`${theme.heading("Experiment run")} ${theme.error("✗")}`);
          for (const w of res.safe.warnings) {
            defaultRuntime.log(theme.warn(`- ${w}`));
          }
          for (const e of res.safe.errors) {
            defaultRuntime.log(theme.error(`- ${e}`));
          }
          defaultRuntime.exit(1);
          return;
        }

        if (res.refine && "ok" in res.refine && !res.refine.ok) {
          defaultRuntime.log(`${theme.heading("Experiment refine")} ${theme.error("✗")}`);
          for (const w of res.refine.warnings) {
            defaultRuntime.log(theme.warn(`- ${w}`));
          }
          for (const e of res.refine.errors) {
            defaultRuntime.log(theme.error(`- ${e}`));
          }
          defaultRuntime.exit(1);
          return;
        }

        if (res.execute && !res.execute.ok) {
          defaultRuntime.log(`${theme.heading("Experiment execute")} ${theme.error("✗")}`);
          for (const w of res.execute.warnings) {
            defaultRuntime.log(theme.warn(`- ${w}`));
          }
          for (const e of res.execute.errors) {
            defaultRuntime.log(theme.error(`- ${e}`));
          }
          defaultRuntime.exit(1);
          return;
        }

        if (res.finalize && !res.finalize.ok) {
          defaultRuntime.log(`${theme.heading("Experiment finalize")} ${theme.error("✗")}`);
          for (const w of res.finalize.warnings) {
            defaultRuntime.log(theme.warn(`- ${w}`));
          }
          for (const e of res.finalize.errors) {
            defaultRuntime.log(theme.error(`- ${e}`));
          }
          defaultRuntime.exit(1);
          return;
        }

        const accepted = res.accept;
        if (!accepted) {
          defaultRuntime.log(`${theme.heading("Experiment accept")} ${theme.error("✗")}`);
          defaultRuntime.error("error: missing acceptance report");
          defaultRuntime.exit(1);
          return;
        }

        if (opts.json) {
          defaultRuntime.log(JSON.stringify(accepted, null, 2));
          defaultRuntime.exit(accepted.exitCode);
          return;
        }

        defaultRuntime.log(
          `${theme.heading("Experiment accept")} ${
            accepted.status === "pass"
              ? theme.success("✓")
              : accepted.status === "needs_confirm"
                ? theme.warn("!")
                : theme.error("✗")
          }`,
        );
        if (res.planId) {
          defaultRuntime.log(`${theme.muted("Plan:")} ${theme.command(res.planId)}`);
        }
        defaultRuntime.log(`${theme.muted("Dir:")} ${theme.command(shortenHomePath(res.planDir))}`);
        defaultRuntime.log(
          `${theme.muted("Checks:")} pass=${accepted.summary.pass} fail=${accepted.summary.fail} needs_confirm=${accepted.summary.needs_confirm}`,
        );
        defaultRuntime.log(
          `${theme.muted("Run:")} ${theme.command(shortenHomePath(accepted.runDir))}`,
        );
        defaultRuntime.exit(accepted.exitCode);
      });
    });

  const suite = experiment.command("suite").description("Run a suite of experiment variants");

  suite
    .command("run")
    .description("Plan baseline, design variants with OpenCode, then run baseline + variants")
    .argument("[input]", "Path to proposal.md OR an existing planDir")
    .option("--message <text>", "Free-form proposal/task text to compile (mutually exclusive)")
    .option("--workspace <dir>", "Workspace directory for output (compile only)")
    .option("--discovery <mode>", "Discovery mode: off|plan|sample (compile only)", "plan")
    .option(
      "--compile-model <provider/model>",
      "Override model for proposal extraction (compile only)",
    )
    .option("--agent <id>", "Agent id for workspace/model selection (optional)")
    .option("--no-llm", "Disable LLM extraction during compile; use heuristics only")
    .option(
      "--dry-run",
      "Do not execute commands (safe run + execute); still writes plan updates",
      false,
    )
    .option("--suite-count <n>", "Number of variants to design (default: 4)", "4")
    .option("--concurrency <n>", "Max concurrent variants to run (default: 1)", "1")
    .option("--suite-out <dir>", "Suite output directory (optional)")
    .option("--max-attempts <n>", "Max attempts for run/execute (default: 3)", "3")
    .option("--retry-delay-ms <ms>", "Base retry delay in ms (default: 1500)", "1500")
    .option("--command-timeout-ms <ms>", "Timeout per command in ms (default: 600000)", "600000")
    .option("--fail-on-needs-confirm", "Fail if plan has needs_confirm items", false)
    .option(
      "--opencode-model <provider/model>",
      "OpenCode model id (default: opencode/kimi-k2.5-free)",
      "opencode/kimi-k2.5-free",
    )
    .option("--opencode-agent <name>", "OpenCode agent name (optional)")
    .option("--opencode-timeout-ms <ms>", "OpenCode timeout in ms (default: 180000)", "180000")
    .option("--write-acceptance", "Write plan/acceptance.json from refine output (optional)", false)
    .option(
      "--bootstrap",
      "Run bootstrap stage to generate/apply pre-exec patches (optional)",
      false,
    )
    .option(
      "--bootstrap-mode <worktree|plan|both>",
      "Bootstrap mode (default: worktree)",
      "worktree",
    )
    .option("--bootstrap-model <provider/model>", "OpenCode model for bootstrap stage (optional)")
    .option("--bootstrap-agent <name>", "OpenCode agent name for bootstrap stage (optional)")
    .option("--bootstrap-timeout-ms <ms>", "Bootstrap timeout in ms (default: 180000)", "180000")
    .option("--bootstrap-max-attempts <n>", "Bootstrap max attempts (default: 2)", "2")
    .option("--bootstrap-instructions <text>", "Extra bootstrap instructions (optional)")
    .option("--no-sandbox", "Run CPU commands on host (unsafe)")
    .option("--sandbox-image <image>", "Docker image for sandbox execution (optional)")
    .option("--sandbox-network <name>", "Docker network for sandbox container (default: bridge)")
    .option("--no-repair", "Disable LLM repair loop")
    .option("--repair-attempts <n>", "Max repair patches per node (default: 1)", "1")
    .option("--repair-model <provider/model>", "Override model for repair LLM (optional)")
    .option("--node <idOrNameOrIp>", "Node id/name/ip for GPU nodes (optional)")
    .option(
      "--node-approve <off|allow-once|allow-always>",
      "Bypass node exec approvals (default: off)",
      "off",
    )
    .option("--url <url>", "Gateway WebSocket URL (optional)")
    .option("--token <token>", "Gateway token (if required)")
    .option("--gateway-timeout-ms <ms>", "Gateway timeout in ms (default: 30000)", "30000")
    .option("--invoke-timeout-ms <ms>", "Node invoke timeout in ms (default: 1200000)", "1200000")
    .option(
      "--gpu-wait-timeout-ms <ms>",
      "Wait for eligible GPU nodes before failing (default: 1800000)",
      "1800000",
    )
    .option("--finalize-force", "Overwrite existing final artifacts", false)
    .option("--baseline <path>", "Baseline metrics JSON file for comparison (optional)")
    .option("--json", "Output JSON (suite results)", false)
    .action(async (input: string | undefined, opts: ExperimentRunOpts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const cfg = loadConfig();

        const message = (opts.message ?? "").trim();
        const inputArg = (input ?? "").trim();
        if (message && inputArg) {
          defaultRuntime.error("error: provide either <input> OR --message, not both");
          defaultRuntime.exit(1);
          return;
        }
        if (!message && !inputArg) {
          defaultRuntime.error("error: missing input; provide <input> OR --message");
          defaultRuntime.exit(1);
          return;
        }

        let pipelineInput:
          | { kind: "planDir"; planDir: string }
          | { kind: "proposalPath"; proposalPath: string }
          | { kind: "proposalMarkdown"; proposalMarkdown: string; proposalSource: string };
        let compile:
          | {
              agentId: string;
              workspaceDir: string;
              outDir?: string;
              discovery: ReturnType<typeof DiscoveryModeSchema.parse>;
              modelOverride?: string;
              useLlm?: boolean;
            }
          | undefined;

        if (message) {
          const agentId = (opts.agent?.trim() || resolveDefaultAgentId(cfg)).trim();
          const workspaceDir = opts.workspace?.trim()
            ? resolveUserPath(opts.workspace.trim())
            : resolveAgentWorkspaceDir(cfg, agentId);
          await fs.mkdir(workspaceDir, { recursive: true });

          const discovery = DiscoveryModeSchema.parse(opts.discovery ?? "plan");
          pipelineInput = {
            kind: "proposalMarkdown",
            proposalMarkdown: `# Proposal\n\n${message}\n`,
            proposalSource: "cli:experiment suite --message",
          };
          compile = {
            agentId,
            workspaceDir,
            discovery,
            modelOverride: opts.compileModel?.trim() || undefined,
            useLlm: opts.llm !== false,
          };
        } else {
          const existing = await resolveExistingPath(inputArg);
          if (!existing.ok || !existing.resolvedPath) {
            defaultRuntime.error(`error: cannot read input path: ${inputArg}`);
            defaultRuntime.error(`error: ${existing.error}`);
            defaultRuntime.exit(1);
            return;
          }

          if (existing.isDir) {
            pipelineInput = { kind: "planDir", planDir: existing.resolvedPath };
          } else {
            const agentId = (opts.agent?.trim() || resolveDefaultAgentId(cfg)).trim();
            const workspaceDir = opts.workspace?.trim()
              ? resolveUserPath(opts.workspace.trim())
              : resolveAgentWorkspaceDir(cfg, agentId);
            await fs.mkdir(workspaceDir, { recursive: true });

            const discovery = DiscoveryModeSchema.parse(opts.discovery ?? "plan");
            pipelineInput = { kind: "proposalPath", proposalPath: existing.resolvedPath };
            compile = {
              agentId,
              workspaceDir,
              discovery,
              modelOverride: opts.compileModel?.trim() || undefined,
              useLlm: opts.llm !== false,
            };
          }
        }

        const maxAttempts = Math.max(1, Math.floor(Number(opts.maxAttempts ?? "3")));
        const retryDelayMs = Math.max(0, Math.floor(Number(opts.retryDelayMs ?? "1500")));
        const commandTimeoutMs = Math.max(
          5_000,
          Math.floor(Number(opts.commandTimeoutMs ?? "600000")),
        );
        const gatewayTimeoutMs = Math.max(
          5_000,
          Math.floor(Number(opts.gatewayTimeoutMs ?? "30000")),
        );
        const invokeTimeoutMs = Math.max(
          5_000,
          Math.floor(Number(opts.invokeTimeoutMs ?? "1200000")),
        );
        const gpuWaitTimeoutMs = Math.max(
          0,
          Math.floor(Number(opts.gpuWaitTimeoutMs ?? "1800000")),
        );
        const opencodeTimeoutMs = Math.max(
          10_000,
          Math.floor(Number(opts.opencodeTimeoutMs ?? "180000")),
        );
        const bootstrapTimeoutMs = Math.max(
          10_000,
          Math.floor(Number(opts.bootstrapTimeoutMs ?? "180000")),
        );
        const bootstrapMaxAttempts = Math.max(
          1,
          Math.floor(Number(opts.bootstrapMaxAttempts ?? "2")),
        );
        const repairAttempts = Math.max(0, Math.floor(Number(opts.repairAttempts ?? "1")));
        const suiteCount = Math.max(0, Math.floor(Number(opts.suiteCount ?? "4")));
        const concurrency = Math.max(1, Math.floor(Number(opts.concurrency ?? "1")));

        const suiteOutDir = opts.suiteOut?.trim()
          ? resolveUserPath(opts.suiteOut.trim())
          : undefined;

        const baselinePath = opts.baseline?.trim()
          ? resolveUserPath(opts.baseline.trim())
          : undefined;

        const bootstrapModeRaw = (opts.bootstrapMode ?? "worktree").trim();
        const bootstrapMode =
          bootstrapModeRaw === "plan" || bootstrapModeRaw === "both"
            ? bootstrapModeRaw
            : "worktree";

        const res = await runExperimentSuite({
          input: pipelineInput,
          cfg,
          opts: {
            variantCount: suiteCount,
            suiteOutDir,
            concurrency,
            designModel: opts.opencodeModel?.trim() || "opencode/kimi-k2.5-free",
            designAgent: opts.opencodeAgent?.trim() || undefined,
            designTimeoutMs: opencodeTimeoutMs,
          },
          stages: {
            compile,
            run: {
              dryRun: Boolean(opts.dryRun),
              maxAttempts,
              retryDelayMs,
              commandTimeoutMs,
              failOnNeedsConfirm: Boolean(opts.failOnNeedsConfirm),
            },
            refine: {
              enabled: true,
              dryRun: false,
              model: opts.opencodeModel?.trim() || "opencode/kimi-k2.5-free",
              agent: opts.opencodeAgent?.trim() || undefined,
              timeoutMs: opencodeTimeoutMs,
              writeAcceptance: Boolean(opts.writeAcceptance),
            },
            ...(opts.bootstrap
              ? {
                  bootstrap: {
                    enabled: true,
                    mode: bootstrapMode,
                    dryRun: Boolean(opts.dryRun),
                    model:
                      opts.bootstrapModel?.trim() ||
                      opts.opencodeModel?.trim() ||
                      "opencode/kimi-k2.5-free",
                    agent: opts.bootstrapAgent?.trim() || undefined,
                    timeoutMs: bootstrapTimeoutMs,
                    maxAttempts: bootstrapMaxAttempts,
                    instructions: opts.bootstrapInstructions?.trim() || undefined,
                  },
                }
              : {}),
            execute: {
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
              modelOverride: opts.repairModel?.trim() || undefined,
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
            finalize: { force: Boolean(opts.finalizeForce) },
            accept: { baselinePath },
          },
        });

        if (opts.json) {
          defaultRuntime.log(JSON.stringify(res, null, 2));
          defaultRuntime.exit(res.ok ? 0 : 1);
          return;
        }

        defaultRuntime.log(
          `${theme.heading("Experiment suite")} ${res.ok ? theme.success("✓") : theme.error("✗")}`,
        );
        defaultRuntime.log(`${theme.muted("Suite:")} ${theme.command(res.suiteId)}`);
        defaultRuntime.log(
          `${theme.muted("Dir:")} ${theme.command(shortenHomePath(res.suiteDir))}`,
        );
        defaultRuntime.log("");
        for (const exp of res.experiments) {
          const accepted = exp.pipeline.accept;
          const status = accepted ? accepted.status : exp.pipeline.ok ? "ok" : "failed";
          defaultRuntime.log(`- ${theme.command(exp.id)}: ${status} (${exp.name})`);
          defaultRuntime.log(
            `  ${theme.muted("Dir:")} ${theme.command(shortenHomePath(exp.planDir))}`,
          );
          if (accepted?.runDir) {
            defaultRuntime.log(
              `  ${theme.muted("Run:")} ${theme.command(shortenHomePath(accepted.runDir))}`,
            );
          }
        }

        defaultRuntime.exit(res.ok ? 0 : 1);
      });
    });
}
