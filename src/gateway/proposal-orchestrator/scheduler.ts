import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import type { OpenClawConfig } from "../../config/config.js";
import type { createSubsystemLogger } from "../../logging/subsystem.js";
import type { GpuScheduler } from "../gpu-scheduler/scheduler.js";
import type { NodeRegistry } from "../node-registry.js";
import type {
  ProposalJob,
  ProposalJobEvent,
  ProposalJobRequest,
  ProposalJobState,
  ProposalJobStepId,
  ProposalJobStepSnapshot,
  ProposalJobStepStatus,
} from "./types.js";
import { resolveAgentWorkspaceDir, resolveDefaultAgentId } from "../../agents/agent-scope.js";
import { compileProposal } from "../../proposal/compiler.js";
import { executeProposalPlan } from "../../proposal/execute.js";
import { finalizeProposalPlan } from "../../proposal/finalize.js";
import { refineProposalPlan } from "../../proposal/refine.js";
import { acceptProposalResults } from "../../proposal/results/index.js";
import { runProposalPlanSafeNodes } from "../../proposal/run.js";
import { resolveUserPath } from "../../utils.js";
import { createInProcessGatewayCall } from "./in-process-call.js";
import { readProposalOrchestratorState, writeProposalOrchestratorState } from "./persist.js";

type SubsystemLogger = ReturnType<typeof createSubsystemLogger>;

function nowMs(): number {
  return Date.now();
}

function isTerminal(state: ProposalJobState): boolean {
  return state === "succeeded" || state === "failed" || state === "canceled";
}

const STEP_ORDER: ProposalJobStepId[] = [
  "compile",
  "run",
  "refine",
  "execute",
  "finalize",
  "accept",
];

export type ProposalOrchestratorConfig = {
  enabled: boolean;
  maxConcurrentJobs: number;
  persist: boolean;
  persistPath?: string;
  terminalHistoryLimit: number;
  pollIntervalMs: number;
  eventLimit: number;
};

const DEFAULT_CONFIG: ProposalOrchestratorConfig = {
  enabled: true,
  maxConcurrentJobs: 1,
  persist: true,
  persistPath: undefined,
  terminalHistoryLimit: 200,
  pollIntervalMs: 250,
  eventLimit: 400,
};

function shouldRunStep(req: ProposalJobRequest, step: ProposalJobStepId): boolean {
  const declared = req.steps;
  if (!declared) {
    return true;
  }
  return declared[step] !== false;
}

function buildDefaultStepSnapshots(
  req: ProposalJobRequest,
): Record<ProposalJobStepId, ProposalJobStepSnapshot> {
  const out = {} as Record<ProposalJobStepId, ProposalJobStepSnapshot>;
  for (const step of STEP_ORDER) {
    const enabled = shouldRunStep(req, step);
    const status: ProposalJobStepStatus = enabled ? "pending" : "skipped";
    out[step] = { id: step, status };
  }
  return out;
}

function trimEvents(events: ProposalJobEvent[], limit: number): ProposalJobEvent[] {
  const max = Math.max(0, Math.floor(limit));
  if (max <= 0) {
    return [];
  }
  if (events.length <= max) {
    return events;
  }
  return events.slice(-max);
}

export class ProposalOrchestrator {
  private nodeRegistry: NodeRegistry;
  private gpuScheduler: GpuScheduler;
  private loadConfig: () => OpenClawConfig;
  private log: SubsystemLogger;
  private cfg: ProposalOrchestratorConfig;

  private started = false;
  private closed = false;
  private lock: Promise<void> = Promise.resolve();
  private jobsById = new Map<string, ProposalJob>();
  private queue: string[] = [];
  private pumpTimer: ReturnType<typeof setTimeout> | null = null;
  private pumping = false;
  private waitersByJobId = new Map<string, Set<(job: ProposalJob) => void>>();
  private persistTimer: ReturnType<typeof setTimeout> | null = null;
  private stateVersion = 0;
  private persistedVersion = 0;
  private persistInFlight = false;

  constructor(params: {
    nodeRegistry: NodeRegistry;
    gpuScheduler: GpuScheduler;
    loadConfig: () => OpenClawConfig;
    log: SubsystemLogger;
    config?: Partial<ProposalOrchestratorConfig>;
  }) {
    this.nodeRegistry = params.nodeRegistry;
    this.gpuScheduler = params.gpuScheduler;
    this.loadConfig = params.loadConfig;
    this.log = params.log;
    this.cfg = { ...DEFAULT_CONFIG, ...params.config };
  }

  async start(): Promise<void> {
    if (this.started || this.closed) {
      return;
    }
    this.started = true;
    if (this.cfg.persist) {
      const loaded = await readProposalOrchestratorState({ statePath: this.cfg.persistPath });
      if (loaded?.jobs?.length) {
        let changed = false;
        for (const job of loaded.jobs) {
          if (!job?.jobId || typeof job.jobId !== "string") {
            continue;
          }
          this.jobsById.set(job.jobId, job);
          if (job.state === "queued") {
            this.queue.push(job.jobId);
          }
          if (job.state === "running") {
            job.state = "failed";
            job.updatedAtMs = nowMs();
            job.events.push({
              ts: nowMs(),
              level: "warn",
              message: "gateway restarted while proposal job was running",
            });
            job.events = trimEvents(job.events, this.cfg.eventLimit);
            changed = true;
          }
        }
        this.trimTerminalJobsLocked();
        if (changed) {
          this.markDirtyLocked();
        }
        this.schedulePersistLocked();
      }
    }
    if (this.hasRunnableWork()) {
      this.kick();
    }
  }

  stop(): void {
    this.closed = true;
    if (this.pumpTimer) {
      clearTimeout(this.pumpTimer);
      this.pumpTimer = null;
    }
    if (this.persistTimer) {
      clearTimeout(this.persistTimer);
      this.persistTimer = null;
    }
    this.waitersByJobId.clear();
  }

  async submit(req: ProposalJobRequest): Promise<ProposalJob> {
    if (!this.started) {
      await this.start();
    }
    if (this.closed) {
      throw new Error("proposal orchestrator is stopped");
    }

    const proposalMarkdown =
      typeof req.proposalMarkdown === "string" ? req.proposalMarkdown : undefined;
    const planDirRaw = typeof req.planDir === "string" ? req.planDir.trim() : "";
    const planDir = planDirRaw ? resolveUserPath(planDirRaw) : undefined;

    if (planDir && proposalMarkdown?.trim()) {
      throw new Error("proposalMarkdown and planDir are mutually exclusive");
    }
    if (!planDir && !proposalMarkdown?.trim()) {
      throw new Error("proposal job requires proposalMarkdown or planDir");
    }
    if (!planDir && !shouldRunStep(req, "compile")) {
      throw new Error("proposal job requires compile step when planDir is not provided");
    }

    const job: ProposalJob = {
      version: 1,
      jobId: randomUUID(),
      createdAtMs: nowMs(),
      updatedAtMs: nowMs(),
      state: "queued",
      request: req,
      planDir,
      steps: buildDefaultStepSnapshots(req),
      events: [],
    };

    await this.withLock(async () => {
      this.jobsById.set(job.jobId, job);
      this.queue.push(job.jobId);
      this.markDirtyLocked();
      this.schedulePersistLocked();
    });

    this.kick();
    return job;
  }

  get(jobId: string): ProposalJob | null {
    const id = jobId.trim();
    if (!id) {
      return null;
    }
    return this.jobsById.get(id) ?? null;
  }

  list(params?: { state?: ProposalJobState }): ProposalJob[] {
    const state = params?.state;
    const jobs = [...this.jobsById.values()];
    jobs.sort((a, b) => b.createdAtMs - a.createdAtMs);
    return state ? jobs.filter((job) => job.state === state) : jobs;
  }

  async cancel(jobId: string): Promise<{ ok: boolean; reason?: string }> {
    const id = jobId.trim();
    if (!id) {
      return { ok: false, reason: "jobId required" };
    }
    let changed = false;
    await this.withLock(async () => {
      const job = this.jobsById.get(id);
      if (!job) {
        return;
      }
      if (isTerminal(job.state)) {
        return;
      }
      if (job.state === "queued") {
        job.state = "canceled";
        job.updatedAtMs = nowMs();
        job.events.push({ ts: nowMs(), level: "info", message: "job canceled" });
        job.events = trimEvents(job.events, this.cfg.eventLimit);
        this.markDirtyLocked();
        changed = true;
        this.notifyWaitersLocked(job);
        this.schedulePersistLocked();
        return;
      }
      job.cancelRequested = true;
      job.updatedAtMs = nowMs();
      job.events.push({ ts: nowMs(), level: "info", message: "cancel requested" });
      job.events = trimEvents(job.events, this.cfg.eventLimit);
      this.markDirtyLocked();
      changed = true;
      this.schedulePersistLocked();
    });
    if (changed) {
      this.kick();
      return { ok: true };
    }
    return { ok: false, reason: "unknown jobId or already terminal" };
  }

  async wait(jobId: string, timeoutMs: number): Promise<ProposalJob | null> {
    const id = jobId.trim();
    if (!id) {
      return null;
    }
    const existing = this.jobsById.get(id);
    if (!existing) {
      return null;
    }
    if (isTerminal(existing.state)) {
      return existing;
    }
    const maxWaitMs = Math.max(0, Math.floor(timeoutMs));
    return await new Promise<ProposalJob | null>((resolve) => {
      const timer =
        maxWaitMs > 0
          ? setTimeout(() => {
              unsub();
              resolve(this.jobsById.get(id) ?? null);
            }, maxWaitMs)
          : null;
      const handler = (job: ProposalJob) => {
        if (!isTerminal(job.state)) {
          return;
        }
        if (timer) {
          clearTimeout(timer);
        }
        unsub();
        resolve(job);
      };
      const unsub = () => {
        const set = this.waitersByJobId.get(id);
        if (!set) {
          return;
        }
        set.delete(handler);
        if (set.size === 0) {
          this.waitersByJobId.delete(id);
        }
      };
      const set = this.waitersByJobId.get(id) ?? new Set<(job: ProposalJob) => void>();
      set.add(handler);
      this.waitersByJobId.set(id, set);
    });
  }

  private notifyWaitersLocked(job: ProposalJob): void {
    const set = this.waitersByJobId.get(job.jobId);
    if (!set) {
      return;
    }
    for (const fn of set) {
      try {
        fn(job);
      } catch {
        // ignore
      }
    }
    this.waitersByJobId.delete(job.jobId);
  }

  private kick(): void {
    if (!this.cfg.enabled || this.closed) {
      return;
    }
    if (!this.hasRunnableWork()) {
      return;
    }
    if (this.pumpTimer) {
      return;
    }
    this.pumpTimer = setTimeout(() => {
      this.pumpTimer = null;
      void this.pump().catch((err) => {
        this.log.warn(`proposal orchestrator pump failed: ${String(err)}`);
      });
    }, this.cfg.pollIntervalMs);
  }

  private async pump(): Promise<void> {
    if (this.pumping || this.closed || !this.cfg.enabled) {
      return;
    }
    this.pumping = true;
    try {
      await this.withLock(async () => {
        await this.dispatchLocked();
        this.trimTerminalJobsLocked();
        this.schedulePersistLocked();
      });
    } finally {
      this.pumping = false;
      if (!this.closed && this.hasRunnableWork()) {
        this.kick();
      }
    }
  }

  private hasRunnableWork(): boolean {
    if (!this.cfg.enabled) {
      return false;
    }
    const running = [...this.jobsById.values()].filter((job) => job.state === "running").length;
    if (running >= this.cfg.maxConcurrentJobs) {
      return running > 0;
    }
    return this.queue.some((jobId) => this.jobsById.get(jobId)?.state === "queued");
  }

  private async dispatchLocked(): Promise<void> {
    const runningJobs = [...this.jobsById.values()].filter((job) => job.state === "running").length;
    if (runningJobs >= this.cfg.maxConcurrentJobs) {
      return;
    }
    for (const jobId of this.queue) {
      const job = this.jobsById.get(jobId);
      if (!job) {
        continue;
      }
      if (job.state !== "queued") {
        continue;
      }
      const runningNow = [...this.jobsById.values()].filter((j) => j.state === "running").length;
      if (runningNow >= this.cfg.maxConcurrentJobs) {
        return;
      }

      job.state = "running";
      job.updatedAtMs = nowMs();
      job.events.push({ ts: nowMs(), level: "info", message: "job started" });
      job.events = trimEvents(job.events, this.cfg.eventLimit);
      this.markDirtyLocked();

      void this.runJob(job.jobId).catch((err) => {
        this.log.warn(`proposal job failed: job=${job.jobId} err=${String(err)}`);
      });
    }
  }

  private async runJob(jobId: string): Promise<void> {
    const jobSnapshot = this.jobsById.get(jobId);
    if (!jobSnapshot) {
      return;
    }

    for (const stepId of STEP_ORDER) {
      if (await this.shouldCancel(jobId)) {
        await this.finishCanceled(jobId);
        return;
      }

      const enabled = shouldRunStep(jobSnapshot.request, stepId);
      if (!enabled) {
        await this.markStep(jobId, stepId, { status: "skipped", summary: "disabled" });
        continue;
      }

      if (stepId === "compile") {
        if (jobSnapshot.planDir) {
          await this.markStep(jobId, stepId, {
            status: "skipped",
            summary: "using existing planDir",
          });
          continue;
        }
        await this.runCompileStep(jobId);
        const compiled = this.jobsById.get(jobId);
        if (!compiled?.planDir || compiled.steps.compile.status !== "succeeded") {
          await this.finishFailed(jobId, "compile failed");
          return;
        }
        continue;
      }

      const planDir = (this.jobsById.get(jobId)?.planDir ?? "").trim();
      if (!planDir) {
        await this.markStep(jobId, stepId, { status: "failed", summary: "missing planDir" });
        await this.finishFailed(jobId, "missing planDir");
        return;
      }

      if (stepId === "run") {
        await this.runSafeStep(jobId, planDir);
      } else if (stepId === "refine") {
        await this.runRefineStep(jobId, planDir);
      } else if (stepId === "execute") {
        await this.runExecuteStep(jobId, planDir);
      } else if (stepId === "finalize") {
        await this.runFinalizeStep(jobId, planDir);
      } else if (stepId === "accept") {
        await this.runAcceptStep(jobId, planDir);
      }

      const latest = this.jobsById.get(jobId);
      if (!latest) {
        return;
      }
      const stepStatus = latest.steps[stepId].status;
      if (stepStatus === "failed") {
        await this.finishFailed(jobId, `${stepId} failed`);
        return;
      }
    }

    await this.finishSucceeded(jobId);
  }

  private async shouldCancel(jobId: string): Promise<boolean> {
    const job = this.jobsById.get(jobId);
    return Boolean(job?.cancelRequested);
  }

  private async markStep(
    jobId: string,
    stepId: ProposalJobStepId,
    update: Partial<Omit<ProposalJobStepSnapshot, "id">> & { status: ProposalJobStepStatus },
  ): Promise<void> {
    await this.withLock(async () => {
      const job = this.jobsById.get(jobId);
      if (!job) {
        return;
      }
      const prev = job.steps[stepId];
      job.steps[stepId] = { ...prev, ...update, id: stepId };
      job.updatedAtMs = nowMs();
      this.markDirtyLocked();
      this.schedulePersistLocked();
    });
  }

  private async runCompileStep(jobId: string): Promise<void> {
    await this.markStep(jobId, "compile", { status: "running", startedAtMs: nowMs() });
    await this.appendEvent(jobId, { level: "info", message: "compile started" });

    const job = this.jobsById.get(jobId);
    const markdown = job?.request.proposalMarkdown?.trim() ?? "";
    if (!markdown) {
      await this.markStep(jobId, "compile", {
        status: "failed",
        finishedAtMs: nowMs(),
        ok: false,
        summary: "missing proposalMarkdown",
      });
      return;
    }

    const cfg = this.loadConfig();
    const compileOpts = job?.request.compile ?? {};

    const agentId = (compileOpts.agentId ?? "").trim() || resolveDefaultAgentId(cfg);
    const workspaceDirRaw =
      (compileOpts.workspaceDir ?? "").trim() || resolveAgentWorkspaceDir(cfg, agentId);
    const workspaceDir = resolveUserPath(workspaceDirRaw);
    await fs.mkdir(workspaceDir, { recursive: true });

    const outDirRaw = (compileOpts.outDir ?? "").trim();
    const outDir = outDirRaw ? resolveUserPath(outDirRaw) : undefined;

    const discovery = compileOpts.discovery ?? "plan";
    const useLlm = typeof compileOpts.useLlm === "boolean" ? compileOpts.useLlm : true;
    const modelOverride = (compileOpts.modelOverride ?? "").trim() || undefined;

    try {
      const result = await compileProposal({
        proposalMarkdown: markdown,
        proposalSource: "gateway:proposal.job.compile",
        cfg,
        agentId,
        workspaceDir,
        outDir,
        discovery,
        modelOverride,
        useLlm,
      });

      await this.withLock(async () => {
        const jobLocked = this.jobsById.get(jobId);
        if (!jobLocked) {
          return;
        }
        jobLocked.planId = result.planId;
        jobLocked.planDir = result.rootDir;
        jobLocked.events.push({
          ts: nowMs(),
          level: result.ok ? "info" : "error",
          message: result.ok ? "compile succeeded" : "compile failed",
        });
        jobLocked.events = trimEvents(jobLocked.events, this.cfg.eventLimit);
        jobLocked.updatedAtMs = nowMs();
        this.markDirtyLocked();
        this.schedulePersistLocked();
      });

      await this.markStep(jobId, "compile", {
        status: result.ok ? "succeeded" : "failed",
        finishedAtMs: nowMs(),
        ok: result.ok,
        warningsCount: result.report.warnings.length,
        errorsCount: result.report.errors.length,
        summary: result.ok ? `planId=${result.planId}` : "compile errors",
      });
    } catch (err) {
      await this.appendEvent(jobId, { level: "error", message: `compile threw: ${String(err)}` });
      await this.markStep(jobId, "compile", {
        status: "failed",
        finishedAtMs: nowMs(),
        ok: false,
        summary: String(err instanceof Error ? err.message : err),
      });
    }
  }

  private async runSafeStep(jobId: string, planDir: string): Promise<void> {
    await this.markStep(jobId, "run", { status: "running", startedAtMs: nowMs() });
    await this.appendEvent(jobId, { level: "info", message: "run started" });
    try {
      const job = this.jobsById.get(jobId);
      const result = await runProposalPlanSafeNodes({ planDir, opts: job?.request.run });
      await this.markStep(jobId, "run", {
        status: result.ok ? "succeeded" : "failed",
        finishedAtMs: nowMs(),
        ok: result.ok,
        warningsCount: result.warnings.length,
        errorsCount: result.errors.length,
        summary: result.ok ? "safe nodes ok" : "safe nodes failed",
      });
    } catch (err) {
      await this.appendEvent(jobId, { level: "error", message: `run threw: ${String(err)}` });
      await this.markStep(jobId, "run", {
        status: "failed",
        finishedAtMs: nowMs(),
        ok: false,
        summary: String(err instanceof Error ? err.message : err),
      });
    }
  }

  private async runRefineStep(jobId: string, planDir: string): Promise<void> {
    await this.markStep(jobId, "refine", { status: "running", startedAtMs: nowMs() });
    await this.appendEvent(jobId, { level: "info", message: "refine started" });
    try {
      const job = this.jobsById.get(jobId);
      const opts = job?.request.refine ?? {};
      const result = await refineProposalPlan({ planDir, opts });
      await this.markStep(jobId, "refine", {
        status: result.ok ? "succeeded" : "failed",
        finishedAtMs: nowMs(),
        ok: result.ok,
        warningsCount: result.warnings.length,
        errorsCount: result.errors.length,
        summary: result.ok ? "refine ok" : "refine failed",
      });
    } catch (err) {
      await this.appendEvent(jobId, { level: "error", message: `refine threw: ${String(err)}` });
      await this.markStep(jobId, "refine", {
        status: "failed",
        finishedAtMs: nowMs(),
        ok: false,
        summary: String(err instanceof Error ? err.message : err),
      });
    }
  }

  private async runExecuteStep(jobId: string, planDir: string): Promise<void> {
    await this.markStep(jobId, "execute", { status: "running", startedAtMs: nowMs() });
    await this.appendEvent(jobId, { level: "info", message: "execute started" });
    try {
      const cfg = this.loadConfig();
      const job = this.jobsById.get(jobId);
      const callGateway = createInProcessGatewayCall({
        nodeRegistry: this.nodeRegistry,
        gpuScheduler: this.gpuScheduler,
        loadConfig: this.loadConfig,
      });
      const result = await executeProposalPlan({
        planDir,
        cfg,
        opts: job?.request.execute,
        deps: { callGateway },
      });
      await this.markStep(jobId, "execute", {
        status: result.ok ? "succeeded" : "failed",
        finishedAtMs: nowMs(),
        ok: result.ok,
        warningsCount: result.warnings.length,
        errorsCount: result.errors.length,
        summary: result.ok ? "execute ok" : "execute failed",
      });
    } catch (err) {
      await this.appendEvent(jobId, { level: "error", message: `execute threw: ${String(err)}` });
      await this.markStep(jobId, "execute", {
        status: "failed",
        finishedAtMs: nowMs(),
        ok: false,
        summary: String(err instanceof Error ? err.message : err),
      });
    }
  }

  private async runFinalizeStep(jobId: string, planDir: string): Promise<void> {
    await this.markStep(jobId, "finalize", { status: "running", startedAtMs: nowMs() });
    await this.appendEvent(jobId, { level: "info", message: "finalize started" });
    try {
      const job = this.jobsById.get(jobId);
      const result = await finalizeProposalPlan({ planDir, opts: job?.request.finalize });
      await this.markStep(jobId, "finalize", {
        status: result.ok ? "succeeded" : "failed",
        finishedAtMs: nowMs(),
        ok: result.ok,
        warningsCount: result.warnings.length,
        errorsCount: result.errors.length,
        summary: result.ok ? "finalize ok" : "finalize failed",
      });
    } catch (err) {
      await this.appendEvent(jobId, { level: "error", message: `finalize threw: ${String(err)}` });
      await this.markStep(jobId, "finalize", {
        status: "failed",
        finishedAtMs: nowMs(),
        ok: false,
        summary: String(err instanceof Error ? err.message : err),
      });
    }
  }

  private async runAcceptStep(jobId: string, planDir: string): Promise<void> {
    await this.markStep(jobId, "accept", { status: "running", startedAtMs: nowMs() });
    await this.appendEvent(jobId, { level: "info", message: "accept started" });
    try {
      const job = this.jobsById.get(jobId);
      const report = await acceptProposalResults({
        planDir,
        baselinePath: job?.request.accept?.baselinePath,
      });
      await this.markStep(jobId, "accept", {
        status: report.ok ? "succeeded" : "failed",
        finishedAtMs: nowMs(),
        ok: report.ok,
        warningsCount: report.warnings.length,
        errorsCount: report.errors.length,
        summary: report.ok ? `status=${report.status}` : `status=${report.status}`,
      });
    } catch (err) {
      await this.appendEvent(jobId, { level: "error", message: `accept threw: ${String(err)}` });
      await this.markStep(jobId, "accept", {
        status: "failed",
        finishedAtMs: nowMs(),
        ok: false,
        summary: String(err instanceof Error ? err.message : err),
      });
    }
  }

  private async finishSucceeded(jobId: string): Promise<void> {
    await this.withLock(async () => {
      const job = this.jobsById.get(jobId);
      if (!job) {
        return;
      }
      if (isTerminal(job.state)) {
        return;
      }
      job.state = "succeeded";
      job.updatedAtMs = nowMs();
      job.events.push({ ts: nowMs(), level: "info", message: "job succeeded" });
      job.events = trimEvents(job.events, this.cfg.eventLimit);
      this.notifyWaitersLocked(job);
      this.markDirtyLocked();
      this.schedulePersistLocked();
    });
    this.kick();
  }

  private async finishFailed(jobId: string, reason: string): Promise<void> {
    await this.withLock(async () => {
      const job = this.jobsById.get(jobId);
      if (!job) {
        return;
      }
      if (isTerminal(job.state)) {
        return;
      }
      job.state = "failed";
      job.updatedAtMs = nowMs();
      job.events.push({ ts: nowMs(), level: "error", message: reason });
      job.events = trimEvents(job.events, this.cfg.eventLimit);
      this.notifyWaitersLocked(job);
      this.markDirtyLocked();
      this.schedulePersistLocked();
    });
    this.kick();
  }

  private async finishCanceled(jobId: string): Promise<void> {
    await this.withLock(async () => {
      const job = this.jobsById.get(jobId);
      if (!job) {
        return;
      }
      if (isTerminal(job.state)) {
        return;
      }
      job.state = "canceled";
      job.updatedAtMs = nowMs();
      job.events.push({ ts: nowMs(), level: "info", message: "job canceled" });
      job.events = trimEvents(job.events, this.cfg.eventLimit);
      this.notifyWaitersLocked(job);
      this.markDirtyLocked();
      this.schedulePersistLocked();
    });
    this.kick();
  }

  private async appendEvent(
    jobId: string,
    evt: Omit<ProposalJobEvent, "ts"> & { ts?: number },
  ): Promise<void> {
    await this.withLock(async () => {
      const job = this.jobsById.get(jobId);
      if (!job) {
        return;
      }
      job.events.push({ ts: evt.ts ?? nowMs(), level: evt.level, message: evt.message });
      job.events = trimEvents(job.events, this.cfg.eventLimit);
      job.updatedAtMs = nowMs();
      this.markDirtyLocked();
      this.schedulePersistLocked();
    });
  }

  private schedulePersistLocked(): void {
    if (!this.cfg.persist || this.closed) {
      return;
    }
    if (this.persistInFlight || this.stateVersion <= this.persistedVersion) {
      return;
    }
    if (this.persistTimer) {
      return;
    }
    this.persistTimer = setTimeout(() => {
      this.persistTimer = null;
      void this.persist().catch((err) => {
        this.log.warn(`proposal orchestrator persist failed: ${String(err)}`);
      });
    }, 200);
  }

  private async persist(): Promise<void> {
    if (!this.cfg.persist || this.closed) {
      return;
    }
    if (this.persistInFlight) {
      return;
    }
    this.persistInFlight = true;
    let snapshot: { version: 1; jobs: ProposalJob[] } | null = null;
    let version = 0;
    await this.withLock(async () => {
      version = this.stateVersion;
      snapshot = { version: 1 as const, jobs: [...this.jobsById.values()] };
    });
    try {
      if (snapshot) {
        await writeProposalOrchestratorState(snapshot, { statePath: this.cfg.persistPath });
      }
      await this.withLock(async () => {
        this.persistedVersion = Math.max(this.persistedVersion, version);
      });
    } finally {
      this.persistInFlight = false;
      await this.withLock(async () => {
        this.schedulePersistLocked();
      });
    }
  }

  private trimTerminalJobsLocked(): void {
    const limit = Math.max(0, Math.floor(this.cfg.terminalHistoryLimit));
    if (limit <= 0) {
      return;
    }
    const terminal = [...this.jobsById.values()].filter((job) => isTerminal(job.state));
    if (terminal.length <= limit) {
      return;
    }
    terminal.sort((a, b) => b.updatedAtMs - a.updatedAtMs);
    const keep = new Set(terminal.slice(0, limit).map((job) => job.jobId));
    let changed = false;
    for (const job of terminal.slice(limit)) {
      if (!keep.has(job.jobId)) {
        this.jobsById.delete(job.jobId);
        changed = true;
      }
    }
    this.queue = this.queue.filter((jobId) => this.jobsById.has(jobId));
    if (changed) {
      this.markDirtyLocked();
    }
  }

  private markDirtyLocked(): void {
    this.stateVersion += 1;
  }

  private async withLock<T>(fn: () => Promise<T>): Promise<T> {
    const prev = this.lock;
    let release: (() => void) | undefined;
    this.lock = new Promise<void>((resolve) => {
      release = resolve;
    });
    await prev;
    try {
      return await fn();
    } finally {
      release?.();
    }
  }
}
