import type { Command } from "commander";
import { callGateway } from "../gateway/call.js";
import { defaultRuntime } from "../runtime.js";
import { renderTable } from "../terminal/table.js";
import { theme } from "../terminal/theme.js";
import { GATEWAY_CLIENT_MODES, GATEWAY_CLIENT_NAMES } from "../utils/message-channel.js";
import { withProgress } from "./progress.js";

type GpuJobsRpcOpts = {
  url?: string;
  token?: string;
  password?: string;
  timeout?: string;
  json?: boolean;
  state?: string;
};

type GpuJobSummary = {
  jobId: string;
  state: string;
  paused: boolean;
  pausedReason?: string;
  pauseRequested: boolean;
  assignedNodeId?: string;
  createdAtMs?: number;
  updatedAtMs?: number;
  resources?: { gpuCount?: number; gpuType?: string };
};

function formatAge(msAgo: number): string {
  const s = Math.max(0, Math.floor(msAgo / 1000));
  if (s < 60) {
    return `${s}s`;
  }
  const m = Math.floor(s / 60);
  if (m < 60) {
    return `${m}m`;
  }
  const h = Math.floor(m / 60);
  if (h < 24) {
    return `${h}h`;
  }
  const d = Math.floor(h / 24);
  return `${d}d`;
}

const gpuJobsCallOpts = (cmd: Command, defaults?: { timeoutMs?: number }) =>
  cmd
    .option("--url <url>", "Gateway WebSocket URL (defaults to gateway.remote.url when configured)")
    .option("--token <token>", "Gateway token (if required)")
    .option("--password <password>", "Gateway password (password auth)")
    .option("--timeout <ms>", "Timeout in ms", String(defaults?.timeoutMs ?? 10_000))
    .option("--json", "Output JSON", false);

const callGatewayCli = async (method: string, opts: GpuJobsRpcOpts, params?: unknown) =>
  withProgress(
    {
      label: `GPU ${method}`,
      indeterminate: true,
      enabled: opts.json !== true,
    },
    async () =>
      await callGateway({
        url: opts.url,
        token: opts.token,
        password: opts.password,
        method,
        params,
        timeoutMs: Number(opts.timeout ?? 10_000),
        clientName: GATEWAY_CLIENT_NAMES.CLI,
        mode: GATEWAY_CLIENT_MODES.CLI,
      }),
  );

function parseJobs(value: unknown): GpuJobSummary[] {
  const obj = typeof value === "object" && value !== null ? (value as Record<string, unknown>) : {};
  const jobsRaw = Array.isArray(obj.jobs) ? (obj.jobs as unknown[]) : [];
  const out: GpuJobSummary[] = [];
  for (const entry of jobsRaw) {
    const job =
      typeof entry === "object" && entry !== null ? (entry as Record<string, unknown>) : {};
    const jobId = typeof job.jobId === "string" ? job.jobId : "";
    const state = typeof job.state === "string" ? job.state : "";
    if (!jobId || !state) {
      continue;
    }
    const resources =
      job.resources && typeof job.resources === "object"
        ? (job.resources as Record<string, unknown>)
        : {};
    out.push({
      jobId,
      state,
      paused: job.paused === true,
      pausedReason: typeof job.pausedReason === "string" ? job.pausedReason : undefined,
      pauseRequested: job.pauseRequested === true,
      assignedNodeId: typeof job.assignedNodeId === "string" ? job.assignedNodeId : undefined,
      createdAtMs: typeof job.createdAtMs === "number" ? job.createdAtMs : undefined,
      updatedAtMs: typeof job.updatedAtMs === "number" ? job.updatedAtMs : undefined,
      resources: {
        gpuCount: typeof resources.gpuCount === "number" ? resources.gpuCount : undefined,
        gpuType: typeof resources.gpuType === "string" ? resources.gpuType : undefined,
      },
    });
  }
  return out;
}

function parseJob(value: unknown): GpuJobSummary | null {
  const obj = typeof value === "object" && value !== null ? (value as Record<string, unknown>) : {};
  const jobRaw =
    obj.job && typeof obj.job === "object" ? (obj.job as Record<string, unknown>) : null;
  if (!jobRaw) {
    return null;
  }
  return parseJobs({ jobs: [jobRaw] })[0] ?? null;
}

export function registerGpuCli(program: Command) {
  const gpu = program.command("gpu").description("GPU scheduler helpers");
  const jobs = gpu.command("jobs").description("GPU scheduler jobs");

  gpuJobsCallOpts(
    jobs
      .command("list")
      .description("List GPU scheduler jobs")
      .option("--state <state>", "Filter by state (queued|running|succeeded|failed|canceled)")
      .action(async (opts: GpuJobsRpcOpts) => {
        const state = typeof opts.state === "string" ? opts.state : undefined;
        const result = await callGatewayCli("gpu.job.list", opts, state ? { state } : {});
        const jobList = parseJobs(result);
        if (opts.json) {
          defaultRuntime.log(JSON.stringify(jobList, null, 2));
          return;
        }
        if (!jobList.length) {
          defaultRuntime.log(theme.muted("No GPU jobs."));
          return;
        }
        const tableWidth = Math.max(60, (process.stdout.columns ?? 120) - 1);
        defaultRuntime.log(
          renderTable({
            width: tableWidth,
            columns: [
              { key: "Job", header: "Job", minWidth: 10, flex: true },
              { key: "State", header: "State", minWidth: 10 },
              { key: "Paused", header: "Paused", minWidth: 6 },
              { key: "GPU", header: "GPU", minWidth: 4 },
              { key: "Type", header: "Type", minWidth: 6 },
              { key: "Node", header: "Node", minWidth: 8, flex: true },
              { key: "Age", header: "Age", minWidth: 6 },
            ],
            rows: jobList.map((job) => ({
              Job: job.jobId,
              State: job.state,
              Paused: job.paused ? (job.pausedReason ?? "yes") : "",
              GPU: String(job.resources?.gpuCount ?? ""),
              Type: job.resources?.gpuType ?? "",
              Node: job.assignedNodeId ?? "",
              Age:
                typeof job.createdAtMs === "number"
                  ? `${formatAge(Date.now() - job.createdAtMs)} ago`
                  : "",
            })),
          }).trimEnd(),
        );
      }),
  );

  gpuJobsCallOpts(
    jobs
      .command("get")
      .description("Get a GPU job")
      .argument("<jobId>", "Job id")
      .action(async (jobId: string, opts: GpuJobsRpcOpts) => {
        const result = await callGatewayCli("gpu.job.get", opts, { jobId });
        const job = parseJob(result);
        if (opts.json) {
          defaultRuntime.log(JSON.stringify(job ?? result, null, 2));
          return;
        }
        if (!job) {
          defaultRuntime.log(theme.muted("No job data returned."));
          return;
        }
        defaultRuntime.log(
          [
            `${theme.heading("Job")} ${theme.command(job.jobId)}`,
            `state=${job.state}`,
            `paused=${job.paused ? "true" : "false"}${job.pausedReason ? ` (${job.pausedReason})` : ""}`,
            `gpu=${job.resources?.gpuCount ?? "?"}${job.resources?.gpuType ? ` (${job.resources.gpuType})` : ""}`,
            job.assignedNodeId ? `node=${job.assignedNodeId}` : "",
          ]
            .filter(Boolean)
            .join(" "),
        );
      }),
  );

  gpuJobsCallOpts(
    jobs
      .command("pause")
      .description(
        "Pause a GPU job (queued jobs pause dispatch; running jobs are preempted and requeued)",
      )
      .argument("<jobId>", "Job id")
      .action(async (jobId: string, opts: GpuJobsRpcOpts) => {
        const result = await callGatewayCli("gpu.job.pause", opts, { jobId });
        if (opts.json) {
          defaultRuntime.log(JSON.stringify(result, null, 2));
          return;
        }
        defaultRuntime.log(`${theme.warn("Paused")} ${theme.command(jobId)}`);
      }),
  );

  gpuJobsCallOpts(
    jobs
      .command("resume")
      .description("Resume a paused queued GPU job")
      .argument("<jobId>", "Job id")
      .action(async (jobId: string, opts: GpuJobsRpcOpts) => {
        const result = await callGatewayCli("gpu.job.resume", opts, { jobId });
        if (opts.json) {
          defaultRuntime.log(JSON.stringify(result, null, 2));
          return;
        }
        defaultRuntime.log(`${theme.success("Resumed")} ${theme.command(jobId)}`);
      }),
  );

  gpuJobsCallOpts(
    jobs
      .command("cancel")
      .description("Cancel a GPU job")
      .argument("<jobId>", "Job id")
      .action(async (jobId: string, opts: GpuJobsRpcOpts) => {
        const result = await callGatewayCli("gpu.job.cancel", opts, { jobId });
        if (opts.json) {
          defaultRuntime.log(JSON.stringify(result, null, 2));
          return;
        }
        defaultRuntime.log(`${theme.warn("Canceled")} ${theme.command(jobId)}`);
      }),
  );
}
