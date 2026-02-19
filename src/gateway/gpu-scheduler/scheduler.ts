import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { OpenClawConfig } from "../../config/config.js";
import type { createSubsystemLogger } from "../../logging/subsystem.js";
import type { NodeRegistry, NodeSession } from "../node-registry.js";
import type {
  GpuJob,
  GpuJobAttempt,
  GpuJobPolicy,
  GpuJobPolicyWindow,
  GpuJobState,
  GpuJobSubmitRequest,
  GpuNodeCandidate,
} from "./types.js";
import { isNodeCommandAllowed, resolveNodeCommandAllowlist } from "../node-command-policy.js";
import { readGpuSchedulerState, writeGpuSchedulerState } from "./persist.js";

type SubsystemLogger = ReturnType<typeof createSubsystemLogger>;

const OPENCLAW_PLAN_DIR_ENV = "OPENCLAW_PLAN_DIR";
const OPENCLAW_SCHEDULER_WRAPPED_ENV = "OPENCLAW_GPU_SCHEDULER_WRAPPED";
const MONITOR_DIR_REL = path.join("report", "gpu_scheduler", "jobs");
const HEARTBEAT_INTERVAL_MS = 2_000;
const HEARTBEAT_STALE_MS = 120_000;
const CANCEL_GRACE_MS = 10_000;
const TAIL_CHARS = 4_000;
const MIN_POLL_INTERVAL_MS = 25;
const DEFAULT_POLICY_INTERVAL_MS = 30_000;

const DOW = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"] as const;

function normalizeDayToken(raw: string): (typeof DOW)[number] | null {
  const lowered = raw.trim().toLowerCase();
  const token = lowered.slice(0, 3);
  return (DOW as readonly string[]).includes(token) ? (token as (typeof DOW)[number]) : null;
}

function parseTimeToMinutes(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const m = trimmed.match(/^(\d{1,2}):(\d{2})$/);
  if (!m) {
    return null;
  }
  const hh = Number(m[1]);
  const mm = Number(m[2]);
  if (!Number.isFinite(hh) || !Number.isFinite(mm)) {
    return null;
  }
  if (hh === 24 && mm === 0) {
    return 24 * 60;
  }
  if (hh < 0 || hh > 23 || mm < 0 || mm > 59) {
    return null;
  }
  return hh * 60 + mm;
}

function getZonedDayMinutes(
  date: Date,
  tz?: string,
): { day: (typeof DOW)[number]; minutes: number } | null {
  if (!tz) {
    const day = DOW[date.getDay()];
    return { day, minutes: date.getHours() * 60 + date.getMinutes() };
  }
  try {
    const fmt = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      weekday: "short",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
    const parts = fmt.formatToParts(date);
    const weekday = parts.find((p) => p.type === "weekday")?.value ?? "";
    const hour = parts.find((p) => p.type === "hour")?.value ?? "";
    const minute = parts.find((p) => p.type === "minute")?.value ?? "";
    const day = normalizeDayToken(weekday);
    const hh = Number(hour);
    const mm = Number(minute);
    if (!day || !Number.isFinite(hh) || !Number.isFinite(mm)) {
      return null;
    }
    return {
      day,
      minutes:
        Math.max(0, Math.min(23, Math.floor(hh))) * 60 + Math.max(0, Math.min(59, Math.floor(mm))),
    };
  } catch {
    return null;
  }
}

function isInWindow(date: Date, window: GpuJobPolicyWindow): boolean {
  const start = parseTimeToMinutes(window.start);
  const end = parseTimeToMinutes(window.end);
  if (start === null || end === null) {
    return false;
  }
  if (start === end) {
    return true;
  }

  const zoned = getZonedDayMinutes(date, window.tz);
  if (!zoned) {
    return false;
  }
  const allowedDays = (window.days ?? [])
    .map((d) => normalizeDayToken(d))
    .filter((d): d is NonNullable<typeof d> => Boolean(d));
  if (allowedDays.length > 0 && !allowedDays.includes(zoned.day)) {
    return false;
  }

  if (start < end) {
    return zoned.minutes >= start && zoned.minutes < end;
  }
  // Wrap across midnight.
  return zoned.minutes >= start || zoned.minutes < end;
}

function isInAnyWindow(date: Date, windows: GpuJobPolicyWindow[]): boolean {
  for (const window of windows) {
    if (isInWindow(date, window)) {
      return true;
    }
  }
  return false;
}

function nowMs(): number {
  return Date.now();
}

function tail(text: string, maxChars: number): string {
  if (text.length <= maxChars) {
    return text;
  }
  return text.slice(-maxChars);
}

function safeParseJson(text: string): unknown {
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return null;
  }
}

function isTerminal(state: GpuJobState): boolean {
  return state === "succeeded" || state === "failed" || state === "canceled";
}

function computeInvokeTimeoutMs(job: GpuJob): number {
  const commandTimeoutMs =
    typeof job.exec.commandTimeoutMs === "number" && Number.isFinite(job.exec.commandTimeoutMs)
      ? Math.max(0, Math.floor(job.exec.commandTimeoutMs))
      : 0;
  const invokeTimeoutMs =
    typeof job.exec.invokeTimeoutMs === "number" && Number.isFinite(job.exec.invokeTimeoutMs)
      ? Math.max(0, Math.floor(job.exec.invokeTimeoutMs))
      : 0;
  if (invokeTimeoutMs > 0) {
    return invokeTimeoutMs;
  }
  if (commandTimeoutMs > 0) {
    return commandTimeoutMs + 30_000;
  }
  return 30_000;
}

export type GpuSchedulerConfig = {
  enabled: boolean;
  maxConcurrentJobs: number;
  persist: boolean;
  persistPath?: string;
  terminalHistoryLimit: number;
  pollIntervalMs: number;
};

const DEFAULT_CONFIG: GpuSchedulerConfig = {
  enabled: true,
  maxConcurrentJobs: 1,
  persist: true,
  persistPath: undefined,
  terminalHistoryLimit: 200,
  pollIntervalMs: 250,
};

export class GpuScheduler {
  private nodeRegistry: NodeRegistry;
  private loadConfig: () => OpenClawConfig;
  private log: SubsystemLogger;
  private cfg: GpuSchedulerConfig;

  private started = false;
  private closed = false;
  private lock: Promise<void> = Promise.resolve();
  private jobsById = new Map<string, GpuJob>();
  private queue: string[] = [];
  private recoveredRunningJobIds = new Set<string>();
  private pumpTimer: ReturnType<typeof setTimeout> | null = null;
  private pumping = false;
  private waitersByJobId = new Map<string, Set<(job: GpuJob) => void>>();
  private persistTimer: ReturnType<typeof setTimeout> | null = null;
  private policyTimer: ReturnType<typeof setTimeout> | null = null;
  private policyRunning = false;
  private stateVersion = 0;
  private persistedVersion = 0;
  private persistInFlight = false;

  constructor(params: {
    nodeRegistry: NodeRegistry;
    loadConfig: () => OpenClawConfig;
    log: SubsystemLogger;
    config?: Partial<GpuSchedulerConfig>;
  }) {
    this.nodeRegistry = params.nodeRegistry;
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
      const loaded = await readGpuSchedulerState({ statePath: this.cfg.persistPath });
      if (loaded?.jobs?.length) {
        for (const job of loaded.jobs) {
          if (!job?.jobId || typeof job.jobId !== "string") {
            continue;
          }
          this.jobsById.set(job.jobId, job);
          if (job.state === "queued" || job.state === "running") {
            this.queue.push(job.jobId);
          }
          if (job.state === "running") {
            // Track jobs that were running before restart so we can reconcile them via the shared FS.
            this.recoveredRunningJobIds.add(job.jobId);
          }
        }
        this.queue = Array.from(new Set(this.queue));
        this.trimTerminalJobsLocked();
        this.schedulePersistLocked();
      }
    }
    if (this.hasRunnableWork()) {
      this.kick();
    }
    this.schedulePolicyTick(0);
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
    if (this.policyTimer) {
      clearTimeout(this.policyTimer);
      this.policyTimer = null;
    }
    this.waitersByJobId.clear();
  }

  async submit(req: GpuJobSubmitRequest): Promise<GpuJob> {
    if (!this.started) {
      await this.start();
    }
    if (this.closed) {
      throw new Error("gpu scheduler is stopped");
    }
    const gpuCount = Math.max(1, Math.floor(req.resources.gpuCount));
    const maxAttempts =
      typeof req.maxAttempts === "number" && Number.isFinite(req.maxAttempts)
        ? Math.max(1, Math.floor(req.maxAttempts))
        : 1;

    const job: GpuJob = {
      jobId: randomUUID(),
      createdAtMs: nowMs(),
      updatedAtMs: nowMs(),
      state: "queued",
      policy: req.policy,
      resources: {
        gpuCount,
        gpuType: req.resources.gpuType?.trim() || undefined,
        gpuMemGB: req.resources.gpuMemGB,
        cpuCores: req.resources.cpuCores,
        ramGB: req.resources.ramGB,
      },
      exec: {
        command: req.exec.command.map((part) => String(part)),
        rawCommand: req.exec.rawCommand?.trim() || undefined,
        cwd: req.exec.cwd?.trim() || undefined,
        env: req.exec.env,
        commandTimeoutMs: req.exec.commandTimeoutMs,
        invokeTimeoutMs: req.exec.invokeTimeoutMs,
        approved: req.exec.approved === true,
        approvalDecision: req.exec.approvalDecision,
      },
      maxAttempts,
      attempts: [],
    };

    await this.withLock(async () => {
      this.jobsById.set(job.jobId, job);
      this.queue.push(job.jobId);
      this.markDirtyLocked();
      this.schedulePersistLocked();
    });
    void this.appendJobEvent(job, {
      type: "submitted",
      state: job.state,
      resources: job.resources,
    });
    this.kick();
    return job;
  }

  get(jobId: string): GpuJob | null {
    const id = jobId.trim();
    if (!id) {
      return null;
    }
    return this.jobsById.get(id) ?? null;
  }

  list(params?: { state?: GpuJobState }): GpuJob[] {
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
    let cancelMarkerPath: string | null = null;
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
        this.markDirtyLocked();
        changed = true;
        this.notifyWaitersLocked(job);
        this.schedulePersistLocked();
        return;
      }
      // Running jobs can't be force-killed via node.invoke today.
      job.cancelRequested = true;
      job.updatedAtMs = nowMs();
      cancelMarkerPath = this.resolveCancelMarkerPathLocked(job);
      this.markDirtyLocked();
      changed = true;
      this.schedulePersistLocked();
    });
    if (changed) {
      if (cancelMarkerPath) {
        await this.writeCancelMarker(cancelMarkerPath);
      }
      const job = this.jobsById.get(id);
      if (job) {
        void this.appendJobEvent(job, {
          type: job.state === "canceled" ? "canceled" : "cancel_requested",
          state: job.state,
        });
      }
      this.kick();
      return { ok: true };
    }
    return { ok: false, reason: "unknown jobId or already terminal" };
  }

  async pause(
    jobId: string,
    opts?: { reason?: "manual" | "policy" },
  ): Promise<{ ok: boolean; reason?: string }> {
    const id = jobId.trim();
    if (!id) {
      return { ok: false, reason: "jobId required" };
    }
    const pauseReason = opts?.reason === "policy" ? "policy" : "manual";
    let pauseMarkerPath: string | null = null;
    let changed = false;
    let reason: string | undefined;
    await this.withLock(async () => {
      const job = this.jobsById.get(id);
      if (!job) {
        reason = "unknown jobId";
        return;
      }
      if (isTerminal(job.state)) {
        reason = "job already terminal";
        return;
      }

      if (job.state === "queued") {
        if (job.paused === true) {
          if (!job.pausedReason) {
            job.pausedReason = pauseReason;
            job.updatedAtMs = nowMs();
            this.markDirtyLocked();
            this.schedulePersistLocked();
          }
          changed = true;
          return;
        }
        job.paused = true;
        job.pausedReason = pauseReason;
        job.updatedAtMs = nowMs();
        this.markDirtyLocked();
        changed = true;
        this.schedulePersistLocked();
        return;
      }

      if (job.state !== "running") {
        reason = `job must be queued or running (state=${job.state})`;
        return;
      }

      if (job.pauseRequested === true) {
        if (!job.pausedReason) {
          job.pausedReason = pauseReason;
          job.updatedAtMs = nowMs();
          this.markDirtyLocked();
          this.schedulePersistLocked();
        }
        changed = true;
        return;
      }

      pauseMarkerPath = this.resolvePauseMarkerPathLocked(job);
      if (!pauseMarkerPath) {
        reason = "pause requires OPENCLAW_PLAN_DIR and wrapped sh -lc command";
        return;
      }

      job.pauseRequested = true;
      job.paused = true;
      job.pausedReason = pauseReason;
      job.updatedAtMs = nowMs();
      this.markDirtyLocked();
      changed = true;
      this.schedulePersistLocked();
    });
    if (changed) {
      if (pauseMarkerPath) {
        await this.writePauseMarker(pauseMarkerPath);
      }
      const job = this.jobsById.get(id);
      if (job) {
        void this.appendJobEvent(job, {
          type: pauseMarkerPath ? "pause_requested" : "paused",
          state: job.state,
          reason: pauseReason,
        });
      }
      this.kick();
      return { ok: true };
    }
    return { ok: false, ...(reason ? { reason } : {}) };
  }

  async resume(
    jobId: string,
    _opts?: { reason?: "manual" | "policy" },
  ): Promise<{ ok: boolean; reason?: string }> {
    const id = jobId.trim();
    if (!id) {
      return { ok: false, reason: "jobId required" };
    }
    let changed = false;
    let reason: string | undefined;
    await this.withLock(async () => {
      const job = this.jobsById.get(id);
      if (!job) {
        reason = "unknown jobId";
        return;
      }
      if (isTerminal(job.state)) {
        reason = "job already terminal";
        return;
      }
      if (job.state !== "queued") {
        reason = `job must be queued (state=${job.state})`;
        return;
      }
      if (job.paused !== true) {
        if (job.pausedReason) {
          job.pausedReason = undefined;
          job.updatedAtMs = nowMs();
          this.markDirtyLocked();
          this.schedulePersistLocked();
        }
        changed = true;
        return;
      }
      job.paused = false;
      job.pausedReason = undefined;
      job.updatedAtMs = nowMs();
      this.markDirtyLocked();
      changed = true;
      this.schedulePersistLocked();
    });
    if (changed) {
      const job = this.jobsById.get(id);
      if (job) {
        void this.appendJobEvent(job, { type: "resumed", state: job.state });
      }
      this.kick();
      return { ok: true };
    }
    return { ok: false, ...(reason ? { reason } : {}) };
  }

  async wait(jobId: string, timeoutMs: number): Promise<GpuJob | null> {
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
    return await new Promise<GpuJob | null>((resolve) => {
      const timer =
        maxWaitMs > 0
          ? setTimeout(() => {
              unsub();
              resolve(this.jobsById.get(id) ?? null);
            }, maxWaitMs)
          : null;
      const handler = (job: GpuJob) => {
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
      const set = this.waitersByJobId.get(id) ?? new Set<(job: GpuJob) => void>();
      set.add(handler);
      this.waitersByJobId.set(id, set);
    });
  }

  private getPolicyIntervalMs(): number {
    const cfg = this.loadConfig();
    const raw = cfg.gateway?.gpuScheduler?.policy?.intervalMs;
    if (typeof raw === "number" && Number.isFinite(raw)) {
      return Math.max(5_000, Math.floor(raw));
    }
    return DEFAULT_POLICY_INTERVAL_MS;
  }

  private schedulePolicyTick(delayMs?: number): void {
    if (this.closed) {
      return;
    }
    if (this.policyTimer) {
      clearTimeout(this.policyTimer);
      this.policyTimer = null;
    }
    const ms =
      typeof delayMs === "number" && Number.isFinite(delayMs)
        ? Math.max(0, delayMs)
        : this.getPolicyIntervalMs();
    this.policyTimer = setTimeout(() => {
      this.policyTimer = null;
      void this.runPolicyTick().catch((err) => {
        this.log.warn(`gpu scheduler policy tick failed: ${String(err)}`);
      });
    }, ms);
  }

  private resolveGlobalPolicyDefaults(): {
    autoPause: boolean;
    autoResume: boolean;
    windows: GpuJobPolicyWindow[];
  } | null {
    const cfg = this.loadConfig();
    const policy = cfg.gateway?.gpuScheduler?.policy;
    if (policy?.enabled !== true) {
      return null;
    }
    const windowsRaw = Array.isArray(policy.windows) ? policy.windows : [];
    const windows: GpuJobPolicyWindow[] = [];
    for (const w of windowsRaw) {
      const win = w && typeof w === "object" ? (w as Record<string, unknown>) : null;
      if (!win) {
        continue;
      }
      const start = typeof win.start === "string" ? win.start : "";
      const end = typeof win.end === "string" ? win.end : "";
      if (!start.trim() || !end.trim()) {
        continue;
      }
      windows.push({
        days: Array.isArray(win.days) ? (win.days as unknown[]).map((d) => String(d)) : undefined,
        start: start.trim(),
        end: end.trim(),
        tz: typeof win.tz === "string" ? win.tz.trim() || undefined : undefined,
      });
    }
    return {
      autoPause: policy.autoPause !== false,
      autoResume: policy.autoResume !== false,
      windows,
    };
  }

  private resolveEffectivePolicy(jobPolicy: GpuJobPolicy | undefined): {
    autoPause: boolean;
    autoResume: boolean;
    windows: GpuJobPolicyWindow[];
  } | null {
    const global = this.resolveGlobalPolicyDefaults();
    const windows =
      jobPolicy?.windows && jobPolicy.windows.length > 0
        ? jobPolicy.windows
        : (global?.windows ?? []);
    if (windows.length === 0) {
      return null;
    }
    const autoPause =
      typeof jobPolicy?.autoPause === "boolean"
        ? jobPolicy.autoPause
        : (global?.autoPause ?? false);
    const autoResume =
      typeof jobPolicy?.autoResume === "boolean"
        ? jobPolicy.autoResume
        : (global?.autoResume ?? false);
    if (!autoPause && !autoResume) {
      return null;
    }
    return { autoPause, autoResume, windows };
  }

  private async runPolicyTick(): Promise<void> {
    if (this.closed) {
      return;
    }
    if (this.policyRunning) {
      this.schedulePolicyTick();
      return;
    }
    this.policyRunning = true;
    try {
      const snapshots = await this.withLock(async () => {
        return [...this.jobsById.values()].map((job) => ({
          jobId: job.jobId,
          state: job.state,
          paused: job.paused === true,
          pausedReason: job.pausedReason,
          pauseRequested: job.pauseRequested === true,
          policy: job.policy,
        }));
      });

      const now = new Date();
      const pauseIds: string[] = [];
      const resumeIds: string[] = [];

      for (const job of snapshots) {
        const policy = this.resolveEffectivePolicy(job.policy);
        if (!policy) {
          continue;
        }
        const inWindow = isInAnyWindow(now, policy.windows);

        if (job.state === "queued") {
          if (policy.autoPause && !inWindow && !job.paused) {
            pauseIds.push(job.jobId);
          } else if (policy.autoResume && inWindow && job.paused && job.pausedReason === "policy") {
            resumeIds.push(job.jobId);
          }
        }

        if (job.state === "running") {
          if (policy.autoPause && !inWindow && !job.pauseRequested) {
            pauseIds.push(job.jobId);
          }
        }
      }

      for (const jobId of pauseIds) {
        await this.pause(jobId, { reason: "policy" });
      }
      for (const jobId of resumeIds) {
        await this.resume(jobId, { reason: "policy" });
      }
    } finally {
      this.policyRunning = false;
      this.schedulePolicyTick();
    }
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
    const delayMs = Math.max(MIN_POLL_INTERVAL_MS, Math.floor(this.cfg.pollIntervalMs));
    this.pumpTimer = setTimeout(() => {
      this.pumpTimer = null;
      void this.pump().catch((err) => {
        this.log.warn(`gpu scheduler pump failed: ${String(err)}`);
      });
    }, delayMs);
  }

  private async pump(): Promise<void> {
    if (this.pumping || this.closed || !this.cfg.enabled) {
      return;
    }
    this.pumping = true;
    try {
      await this.reconcileRecoveredRunningJobs();
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
    if (running > 0) {
      return true;
    }
    return this.queue.some((jobId) => {
      const job = this.jobsById.get(jobId);
      return job?.state === "queued" && job.paused !== true;
    });
  }

  private buildNodeCandidatesLocked(): GpuNodeCandidate[] {
    const nodes = this.nodeRegistry.listConnected();
    const cfg = this.loadConfig();
    const runningByNode = new Map<string, number>();
    for (const job of this.jobsById.values()) {
      if (job.state !== "running") {
        continue;
      }
      const nodeId = job.assignedNodeId;
      if (!nodeId) {
        continue;
      }
      runningByNode.set(nodeId, (runningByNode.get(nodeId) ?? 0) + job.resources.gpuCount);
    }

    const candidates: GpuNodeCandidate[] = [];
    for (const node of nodes) {
      if (!this.nodeSupportsSystemRun(cfg, node)) {
        continue;
      }
      const gpuTotal = node.resources?.gpuCount;
      if (typeof gpuTotal !== "number" || !Number.isFinite(gpuTotal) || gpuTotal <= 0) {
        continue;
      }
      candidates.push({
        nodeId: node.nodeId,
        resources: node.resources ?? {},
        allocatedGpu: runningByNode.get(node.nodeId) ?? 0,
      });
    }
    return candidates;
  }

  private nodeSupportsSystemRun(cfg: OpenClawConfig, node: NodeSession): boolean {
    const allowlist = resolveNodeCommandAllowlist(cfg, node);
    const allowed = isNodeCommandAllowed({
      command: "system.run",
      declaredCommands: node.commands,
      allowlist,
    });
    return allowed.ok;
  }

  private resolveBestFitNode(job: GpuJob, candidates: GpuNodeCandidate[]): GpuNodeCandidate | null {
    const reqGpu = job.resources.gpuCount;
    const reqType = job.resources.gpuType?.trim();
    const reqMem = job.resources.gpuMemGB;

    const matches = candidates.filter((cand) => {
      const total = cand.resources.gpuCount ?? 0;
      const free = total - cand.allocatedGpu;
      if (free < reqGpu) {
        return false;
      }
      if (reqType) {
        const nodeType = cand.resources.gpuType?.trim();
        if (!nodeType) {
          return false;
        }
        if (nodeType.toLowerCase() !== reqType.toLowerCase()) {
          return false;
        }
      }
      if (typeof reqMem === "number" && Number.isFinite(reqMem)) {
        const nodeMem = cand.resources.gpuMemGB;
        if (typeof nodeMem !== "number" || !Number.isFinite(nodeMem)) {
          return false;
        }
        if (nodeMem < reqMem) {
          return false;
        }
      }
      return true;
    });

    if (matches.length === 0) {
      return null;
    }

    matches.sort((a, b) => {
      const aFree = (a.resources.gpuCount ?? 0) - a.allocatedGpu;
      const bFree = (b.resources.gpuCount ?? 0) - b.allocatedGpu;
      // Best-fit: lower free GPUs first.
      if (aFree !== bFree) {
        return aFree - bFree;
      }
      return a.nodeId.localeCompare(b.nodeId);
    });

    return matches[0] ?? null;
  }

  private async dispatchLocked(): Promise<void> {
    const runningJobs = [...this.jobsById.values()].filter((job) => job.state === "running").length;
    if (runningJobs >= this.cfg.maxConcurrentJobs) {
      return;
    }
    const candidates = this.buildNodeCandidatesLocked();
    const allocatedByNodeId = new Map(candidates.map((cand) => [cand.nodeId, cand.allocatedGpu]));
    const now = nowMs();
    const nowDate = new Date();

    for (const jobId of this.queue) {
      const job = this.jobsById.get(jobId);
      if (!job) {
        continue;
      }
      if (job.state !== "queued") {
        continue;
      }
      if (job.paused === true) {
        continue;
      }
      if (job.notBeforeMs !== undefined && job.notBeforeMs > now) {
        continue;
      }

      const policy = this.resolveEffectivePolicy(job.policy);
      if (policy && policy.autoPause && !isInAnyWindow(nowDate, policy.windows)) {
        job.paused = true;
        job.pausedReason = "policy";
        job.updatedAtMs = nowMs();
        this.markDirtyLocked();
        this.schedulePersistLocked();
        void this.appendJobEvent(job, { type: "paused", state: job.state, reason: "policy" });
        continue;
      }

      const runningNow = [...this.jobsById.values()].filter((j) => j.state === "running").length;
      if (runningNow >= this.cfg.maxConcurrentJobs) {
        return;
      }
      const selected = this.resolveBestFitNode(
        job,
        candidates.map((cand) => ({
          ...cand,
          allocatedGpu: allocatedByNodeId.get(cand.nodeId) ?? cand.allocatedGpu,
        })),
      );
      if (!selected) {
        continue;
      }
      allocatedByNodeId.set(
        selected.nodeId,
        (allocatedByNodeId.get(selected.nodeId) ?? selected.allocatedGpu) + job.resources.gpuCount,
      );
      const attempt = job.attempts.length + 1;
      const attemptEntry: GpuJobAttempt = {
        attempt,
        nodeId: selected.nodeId,
        startedAtMs: nowMs(),
      };
      job.assignedNodeId = selected.nodeId;
      job.state = "running";
      job.updatedAtMs = nowMs();
      job.attempts.push(attemptEntry);
      this.markDirtyLocked();
      void this.appendJobEvent(job, {
        type: "dispatched",
        state: job.state,
        nodeId: selected.nodeId,
        attempt,
      });

      // Fire-and-forget execution.
      void this.runJob(job.jobId, attempt).catch((err) => {
        this.log.warn(`gpu job failed: job=${job.jobId} err=${String(err)}`);
      });
    }
  }

  private async runJob(jobId: string, attempt: number): Promise<void> {
    const snapshot = this.jobsById.get(jobId);
    if (!snapshot) {
      return;
    }
    const nodeId = snapshot.assignedNodeId;
    if (!nodeId) {
      return;
    }

    const raw = snapshot.exec.rawCommand?.trim() || null;
    const wrapped = this.wrapShellCommandIfSupported(snapshot, attempt);
    const effectiveCommand = wrapped.command;
    const effectiveEnv = wrapped.env;
    const params: Record<string, unknown> = {
      command: effectiveCommand,
      timeoutMs: snapshot.exec.commandTimeoutMs,
    };
    if (raw) {
      params.rawCommand = raw;
    }
    if (snapshot.exec.cwd) {
      params.cwd = snapshot.exec.cwd;
    }
    if (effectiveEnv) {
      params.env = effectiveEnv;
    }
    if (snapshot.exec.approved === true) {
      params.approved = true;
    }
    if (snapshot.exec.approvalDecision) {
      params.approvalDecision = snapshot.exec.approvalDecision;
    }

    const invokeTimeoutMs = computeInvokeTimeoutMs(snapshot);

    const res = await this.nodeRegistry.invoke({
      nodeId,
      command: "system.run",
      params,
      timeoutMs: invokeTimeoutMs,
      idempotencyKey: randomUUID(),
    });

    const payloadRaw = res.payloadJSON ? safeParseJson(res.payloadJSON) : res.payload;
    const payload =
      payloadRaw && typeof payloadRaw === "object" ? (payloadRaw as Record<string, unknown>) : {};
    const stdout = typeof payload.stdout === "string" ? payload.stdout : "";
    const stderr = typeof payload.stderr === "string" ? payload.stderr : "";
    const exitCode = typeof payload.exitCode === "number" ? payload.exitCode : null;
    const timedOut = payload.timedOut === true || (!res.ok && res.error?.code === "TIMEOUT");
    const success = payload.success === true;

    const ok = res.ok && success && !timedOut && (exitCode === null || exitCode === 0);

    await this.withLock(async () => {
      const job = this.jobsById.get(jobId);
      if (!job) {
        return;
      }
      const attemptEntry = job.attempts.find((a) => a.attempt === attempt);
      if (attemptEntry) {
        attemptEntry.finishedAtMs = nowMs();
        attemptEntry.ok = ok;
        attemptEntry.exitCode = exitCode;
        attemptEntry.timedOut = timedOut;
        attemptEntry.stdoutTail = stdout ? tail(stdout, 4000) : undefined;
        attemptEntry.stderrTail = stderr ? tail(stderr, 4000) : undefined;
        if (!ok) {
          attemptEntry.error = res.ok
            ? `remote command failed (exit=${exitCode ?? "null"}, timedOut=${timedOut})`
            : (res.error?.message ?? "node invoke failed");
        }
      }

      const pauseRequested = job.pauseRequested === true;
      job.pauseRequested = undefined;

      if (job.cancelRequested) {
        job.state = "canceled";
      } else if (ok) {
        job.state = "succeeded";
      } else if (pauseRequested) {
        job.state = "queued";
        job.paused = true;
        job.notBeforeMs = nowMs();
        job.assignedNodeId = undefined;
        if (attemptEntry) {
          attemptEntry.error = "paused";
        }
      } else if (attempt < job.maxAttempts) {
        job.state = "queued";
        job.notBeforeMs = nowMs() + Math.min(30_000, 1000 * attempt);
        job.assignedNodeId = undefined;
      } else {
        job.state = "failed";
      }

      job.updatedAtMs = nowMs();
      if (job.state !== "queued") {
        job.paused = undefined;
        job.pausedReason = undefined;
      } else if (job.paused !== true) {
        job.pausedReason = undefined;
      }
      if (isTerminal(job.state)) {
        job.result = {
          exitCode,
          timedOut,
          success: ok,
          stdoutTail: stdout ? tail(stdout, 4000) : undefined,
          stderrTail: stderr ? tail(stderr, 4000) : undefined,
        };
        this.notifyWaitersLocked(job);
      }
      void this.appendJobEvent(job, {
        type: pauseRequested ? "paused" : isTerminal(job.state) ? "finished" : "requeued",
        state: job.state,
        attempt,
        exitCode,
        timedOut,
        ok,
      });
      this.markDirtyLocked();
      this.schedulePersistLocked();
    });

    this.kick();
  }

  private notifyWaitersLocked(job: GpuJob): void {
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
        this.log.warn(`gpu scheduler persist failed: ${String(err)}`);
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
    let snapshot: { version: 1; jobs: GpuJob[] } | null = null;
    let version = 0;
    await this.withLock(async () => {
      version = this.stateVersion;
      snapshot = { version: 1 as const, jobs: [...this.jobsById.values()] };
    });
    try {
      if (snapshot) {
        await writeGpuSchedulerState(snapshot, { statePath: this.cfg.persistPath });
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

  private resolveCancelMarkerPathLocked(job: GpuJob): string | null {
    if (job.state !== "running") {
      return null;
    }
    const attempt = job.attempts.at(-1);
    const monitorDir = attempt ? this.resolveMonitorDirAbs(job, attempt.attempt) : null;
    if (!monitorDir) {
      return null;
    }
    return path.join(monitorDir, "cancel.requested");
  }

  private async writeCancelMarker(markerPath: string): Promise<void> {
    try {
      await fs.mkdir(path.dirname(markerPath), { recursive: true });
      await fs.writeFile(markerPath, `cancelRequestedAtMs=${nowMs()}\n`, "utf8");
    } catch {
      // best-effort
    }
  }

  private resolvePauseMarkerPathLocked(job: GpuJob): string | null {
    if (job.state !== "running") {
      return null;
    }
    const attempt = job.attempts.at(-1);
    const monitorDir = attempt ? this.resolveMonitorDirAbs(job, attempt.attempt) : null;
    if (!monitorDir) {
      return null;
    }
    return path.join(monitorDir, "pause.requested");
  }

  private async writePauseMarker(markerPath: string): Promise<void> {
    try {
      await fs.mkdir(path.dirname(markerPath), { recursive: true });
      await fs.writeFile(markerPath, `pauseRequestedAtMs=${nowMs()}\n`, "utf8");
    } catch {
      // best-effort
    }
  }

  private resolveJobMonitorRootAbs(job: GpuJob): string | null {
    const planDirRaw =
      job.exec.env && typeof job.exec.env[OPENCLAW_PLAN_DIR_ENV] === "string"
        ? job.exec.env[OPENCLAW_PLAN_DIR_ENV]?.trim()
        : "";
    if (!planDirRaw || !path.isAbsolute(planDirRaw)) {
      return null;
    }
    const planDir = path.resolve(planDirRaw);
    return path.join(planDir, MONITOR_DIR_REL, job.jobId);
  }

  private async appendJobEvent(job: GpuJob, event: Record<string, unknown>): Promise<void> {
    const root = this.resolveJobMonitorRootAbs(job);
    if (!root) {
      return;
    }
    const eventsPath = path.join(root, "events.jsonl");
    const payload = {
      schemaVersion: 1,
      atMs: nowMs(),
      jobId: job.jobId,
      ...event,
    };
    try {
      await fs.mkdir(root, { recursive: true });
      await fs.appendFile(eventsPath, `${JSON.stringify(payload)}\n`, "utf8");
    } catch {
      // best-effort
    }
  }

  private resolveMonitorDirAbs(job: GpuJob, attempt: number): string | null {
    const planDirRaw =
      job.exec.env && typeof job.exec.env[OPENCLAW_PLAN_DIR_ENV] === "string"
        ? job.exec.env[OPENCLAW_PLAN_DIR_ENV]?.trim()
        : "";
    if (!planDirRaw) {
      return null;
    }
    if (!path.isAbsolute(planDirRaw)) {
      return null;
    }
    const planDir = path.resolve(planDirRaw);
    return path.join(planDir, MONITOR_DIR_REL, job.jobId, `attempt-${attempt}`);
  }

  private wrapShellCommandIfSupported(
    job: GpuJob,
    attempt: number,
  ): { command: string[]; env?: Record<string, string> } {
    const argv = job.exec.command;
    const env = job.exec.env;
    if (argv.length < 3 || argv[0] !== "sh" || argv[1] !== "-lc") {
      return { command: argv, ...(env ? { env } : {}) };
    }

    const monitorDirAbs = this.resolveMonitorDirAbs(job, attempt);
    if (!monitorDirAbs) {
      return { command: argv, ...(env ? { env } : {}) };
    }

    const original = String(argv[2] ?? "");
    const wrappedScript = this.buildWrapperScript({
      jobId: job.jobId,
      attempt,
      monitorDirAbs,
      originalScript: original,
    });
    const nextEnv = env ? { ...env } : {};
    nextEnv[OPENCLAW_SCHEDULER_WRAPPED_ENV] = "1";
    return { command: ["sh", "-lc", wrappedScript], env: nextEnv };
  }

  private buildWrapperScript(params: {
    jobId: string;
    attempt: number;
    monitorDirAbs: string;
    originalScript: string;
  }): string {
    const heredocTag = this.pickHeredocTag(params.originalScript);
    const monitorDir = params.monitorDirAbs.replaceAll("\\", "/");
    return [
      "set +e",
      `JOB_ID=${this.shellSingleQuote(params.jobId)}`,
      `ATTEMPT=${params.attempt}`,
      `MON_DIR=${this.shellSingleQuote(monitorDir)}`,
      'mkdir -p "$MON_DIR" || exit 1',
      "STARTED_AT_MS=$(( $(date +%s) * 1000 ))",
      `cat > "$MON_DIR/started.json" <<OPENCLAW_STARTED`,
      `{"schemaVersion":1,"jobId":"${params.jobId}","attempt":${params.attempt},"startedAtMs":$STARTED_AT_MS}`,
      "OPENCLAW_STARTED",
      'ORIG_FILE="$MON_DIR/original.sh"',
      `cat > "$ORIG_FILE" <<'${heredocTag}'`,
      params.originalScript,
      heredocTag,
      'chmod +x "$ORIG_FILE" 2>/dev/null || true',
      'OUT_FILE="$MON_DIR/stdout.txt"',
      'ERR_FILE="$MON_DIR/stderr.txt"',
      'CANCEL_FILE="$MON_DIR/cancel.requested"',
      'PAUSE_FILE="$MON_DIR/pause.requested"',
      'HEARTBEAT_FILE="$MON_DIR/heartbeat.txt"',
      'PID=""',
      "if command -v setsid >/dev/null 2>&1; then",
      '  setsid sh "$ORIG_FILE" >"$OUT_FILE" 2>"$ERR_FILE" &',
      "  PID=$!",
      "else",
      '  sh "$ORIG_FILE" >"$OUT_FILE" 2>"$ERR_FILE" &',
      "  PID=$!",
      "fi",
      "CANCELED=0",
      "PAUSED=0",
      'while kill -0 "$PID" 2>/dev/null; do',
      '  date +%s > "$HEARTBEAT_FILE" 2>/dev/null || : > "$HEARTBEAT_FILE"',
      '  if [ "$CANCELED" -eq 0 ] && [ -f "$CANCEL_FILE" ]; then',
      "    CANCELED=1",
      '    kill -TERM "-$PID" 2>/dev/null || kill -TERM "$PID" 2>/dev/null || true',
      `    i=0`,
      `    while kill -0 "$PID" 2>/dev/null && [ "$i" -lt ${Math.ceil(CANCEL_GRACE_MS / 1000)} ]; do`,
      "      sleep 1",
      "      i=$((i+1))",
      "    done",
      '    kill -KILL "-$PID" 2>/dev/null || kill -KILL "$PID" 2>/dev/null || true',
      "  fi",
      '  if [ "$PAUSED" -eq 0 ] && [ -f "$PAUSE_FILE" ]; then',
      "    PAUSED=1",
      '    kill -TERM "-$PID" 2>/dev/null || kill -TERM "$PID" 2>/dev/null || true',
      `    i=0`,
      `    while kill -0 "$PID" 2>/dev/null && [ "$i" -lt ${Math.ceil(CANCEL_GRACE_MS / 1000)} ]; do`,
      "      sleep 1",
      "      i=$((i+1))",
      "    done",
      '    kill -KILL "-$PID" 2>/dev/null || kill -KILL "$PID" 2>/dev/null || true',
      "  fi",
      `  sleep ${Math.max(1, Math.floor(HEARTBEAT_INTERVAL_MS / 1000))}`,
      "done",
      'wait "$PID"',
      "EXIT_CODE=$?",
      "FINISHED_AT_MS=$(( $(date +%s) * 1000 ))",
      "SUCCESS=false",
      'if [ "$EXIT_CODE" -eq 0 ]; then SUCCESS=true; fi',
      `cat > "$MON_DIR/exit.json" <<OPENCLAW_EXIT`,
      `{"schemaVersion":1,"jobId":"${params.jobId}","attempt":${params.attempt},"startedAtMs":$STARTED_AT_MS,"finishedAtMs":$FINISHED_AT_MS,"exitCode":$EXIT_CODE,"timedOut":false,"success":$SUCCESS}`,
      "OPENCLAW_EXIT",
      `tail -c ${TAIL_CHARS} "$OUT_FILE" 2>/dev/null || true`,
      `tail -c ${TAIL_CHARS} "$ERR_FILE" 1>&2 2>/dev/null || true`,
      'exit "$EXIT_CODE"',
    ].join("\n");
  }

  private pickHeredocTag(originalScript: string): string {
    for (let i = 0; i < 6; i += 1) {
      const candidate = `OPENCLAW_ORIG_${randomUUID().replaceAll("-", "")}`.toUpperCase();
      if (!originalScript.includes(candidate)) {
        return candidate;
      }
    }
    return "OPENCLAW_ORIG";
  }

  private shellSingleQuote(value: string): string {
    const escaped = value.split("'").join(`'"'"'`);
    return `'${escaped}'`;
  }

  private async reconcileRecoveredRunningJobs(): Promise<void> {
    if (this.closed || this.recoveredRunningJobIds.size === 0) {
      return;
    }

    const snapshots = await this.withLock(async () => {
      const out: Array<{
        jobId: string;
        attempt: number;
        planDir: string;
      }> = [];

      for (const jobId of this.recoveredRunningJobIds) {
        const job = this.jobsById.get(jobId);
        if (!job || job.state !== "running") {
          this.recoveredRunningJobIds.delete(jobId);
          continue;
        }
        const attemptEntry = job.attempts.at(-1);
        if (!attemptEntry) {
          job.state = "queued";
          job.assignedNodeId = undefined;
          job.notBeforeMs = nowMs();
          job.updatedAtMs = nowMs();
          this.queue.push(job.jobId);
          this.recoveredRunningJobIds.delete(jobId);
          this.markDirtyLocked();
          continue;
        }
        const planDirRaw =
          job.exec.env && typeof job.exec.env[OPENCLAW_PLAN_DIR_ENV] === "string"
            ? job.exec.env[OPENCLAW_PLAN_DIR_ENV]?.trim()
            : "";
        out.push({ jobId, attempt: attemptEntry.attempt, planDir: planDirRaw });
      }
      this.queue = Array.from(new Set(this.queue));
      this.schedulePersistLocked();
      return out;
    });

    const decisions: Array<
      | { kind: "keep_running"; jobId: string; attempt: number }
      | { kind: "apply_exit"; jobId: string; attempt: number; exitPath: string; monitorDir: string }
      | { kind: "requeue"; jobId: string; attempt: number; reason: string }
    > = [];

    const now = nowMs();
    for (const snap of snapshots) {
      const planDirRaw = snap.planDir.trim();
      if (!planDirRaw || !path.isAbsolute(planDirRaw)) {
        decisions.push({
          kind: "requeue",
          jobId: snap.jobId,
          attempt: snap.attempt,
          reason: "gateway restart recovery: missing OPENCLAW_PLAN_DIR; requeued",
        });
        continue;
      }

      const planDir = path.resolve(planDirRaw);
      const monitorDir = path.join(planDir, MONITOR_DIR_REL, snap.jobId, `attempt-${snap.attempt}`);
      const exitPath = path.join(monitorDir, "exit.json");
      if (await this.fileExists(exitPath)) {
        decisions.push({
          kind: "apply_exit",
          jobId: snap.jobId,
          attempt: snap.attempt,
          exitPath,
          monitorDir,
        });
        continue;
      }

      const heartbeatPath = path.join(monitorDir, "heartbeat.txt");
      const hbStat = await this.statIfExists(heartbeatPath);
      if (hbStat && now - hbStat.mtimeMs <= HEARTBEAT_STALE_MS) {
        decisions.push({ kind: "keep_running", jobId: snap.jobId, attempt: snap.attempt });
        continue;
      }

      decisions.push({
        kind: "requeue",
        jobId: snap.jobId,
        attempt: snap.attempt,
        reason: "gateway restart recovery: heartbeat stale; requeued",
      });
    }

    if (decisions.length === 0) {
      return;
    }

    let changed = false;
    await this.withLock(async () => {
      const now = nowMs();
      for (const decision of decisions) {
        const job = this.jobsById.get(decision.jobId);
        if (!job) {
          this.recoveredRunningJobIds.delete(decision.jobId);
          continue;
        }
        if (job.state !== "running") {
          this.recoveredRunningJobIds.delete(decision.jobId);
          continue;
        }
        const attemptEntry = job.attempts.at(-1);
        if (!attemptEntry || attemptEntry.attempt !== decision.attempt) {
          this.recoveredRunningJobIds.delete(decision.jobId);
          continue;
        }

        if (decision.kind === "keep_running") {
          continue;
        }

        if (decision.kind === "apply_exit") {
          const parsed = safeParseJson(
            await fs.readFile(decision.exitPath, "utf8").catch(() => ""),
          );
          const exitObj =
            parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : {};
          const exitCode = typeof exitObj.exitCode === "number" ? exitObj.exitCode : null;
          const timedOut = exitObj.timedOut === true;
          const ok =
            exitObj.success === true || (typeof exitCode === "number" ? exitCode === 0 : false);
          const finishedAtMs =
            typeof exitObj.finishedAtMs === "number" ? exitObj.finishedAtMs : now;

          const stdoutTail = await this.readFileTail(
            path.join(decision.monitorDir, "stdout.txt"),
            TAIL_CHARS,
          );
          const stderrTail = await this.readFileTail(
            path.join(decision.monitorDir, "stderr.txt"),
            TAIL_CHARS,
          );

          attemptEntry.finishedAtMs = finishedAtMs;
          attemptEntry.ok = ok;
          attemptEntry.exitCode = exitCode;
          attemptEntry.timedOut = timedOut;
          attemptEntry.stdoutTail = stdoutTail || undefined;
          attemptEntry.stderrTail = stderrTail || undefined;
          if (!ok) {
            attemptEntry.error = `recovered remote command failed (exit=${exitCode ?? "null"}, timedOut=${timedOut})`;
          }

          const pauseRequested = job.pauseRequested === true;
          job.pauseRequested = undefined;

          if (job.cancelRequested) {
            job.state = "canceled";
          } else if (ok) {
            job.state = "succeeded";
          } else if (pauseRequested) {
            job.state = "queued";
            job.paused = true;
            job.notBeforeMs = now;
            job.assignedNodeId = undefined;
            attemptEntry.error = "paused";
          } else if (decision.attempt < job.maxAttempts) {
            job.state = "queued";
            job.notBeforeMs = now + Math.min(30_000, 1000 * decision.attempt);
            job.assignedNodeId = undefined;
          } else {
            job.state = "failed";
          }

          job.updatedAtMs = now;
          if (job.state !== "queued") {
            job.paused = undefined;
          }
          if (isTerminal(job.state)) {
            job.result = {
              exitCode,
              timedOut,
              success: ok,
              stdoutTail: stdoutTail || undefined,
              stderrTail: stderrTail || undefined,
            };
            this.notifyWaitersLocked(job);
          } else if (job.state === "queued") {
            this.queue.push(job.jobId);
            job.result = undefined;
          }

          this.recoveredRunningJobIds.delete(decision.jobId);
          this.markDirtyLocked();
          changed = true;
          continue;
        }

        if (decision.kind === "requeue") {
          attemptEntry.finishedAtMs = now;
          attemptEntry.ok = false;
          attemptEntry.exitCode = null;
          attemptEntry.timedOut = false;
          attemptEntry.stderrTail = decision.reason;
          attemptEntry.error = decision.reason;

          const pauseRequested = job.pauseRequested === true;
          job.pauseRequested = undefined;

          if (job.cancelRequested) {
            job.state = "canceled";
          } else if (pauseRequested) {
            job.state = "queued";
            job.paused = true;
            job.notBeforeMs = now;
            job.assignedNodeId = undefined;
            attemptEntry.error = "paused";
          } else if (decision.attempt < job.maxAttempts) {
            job.state = "queued";
            job.notBeforeMs = now + Math.min(30_000, 1000 * decision.attempt);
            job.assignedNodeId = undefined;
          } else {
            job.state = "failed";
          }

          job.updatedAtMs = now;
          if (job.state !== "queued") {
            job.paused = undefined;
          }
          if (isTerminal(job.state)) {
            job.result = {
              exitCode: null,
              timedOut: false,
              success: false,
              stderrTail: decision.reason,
            };
            this.notifyWaitersLocked(job);
          } else if (job.state === "queued") {
            this.queue.push(job.jobId);
            job.result = undefined;
          }

          this.recoveredRunningJobIds.delete(decision.jobId);
          this.markDirtyLocked();
          changed = true;
        }
      }

      if (changed) {
        this.queue = Array.from(new Set(this.queue));
        this.schedulePersistLocked();
      }
    });

    if (changed) {
      this.kick();
    }
  }

  private async fileExists(filePath: string): Promise<boolean> {
    try {
      await fs.stat(filePath);
      return true;
    } catch {
      return false;
    }
  }

  private async statIfExists(filePath: string): Promise<{ mtimeMs: number; size: number } | null> {
    try {
      const stat = await fs.stat(filePath);
      return { mtimeMs: stat.mtimeMs, size: stat.size };
    } catch {
      return null;
    }
  }

  private async readFileTail(filePath: string, maxChars: number): Promise<string> {
    const max = Math.max(0, Math.floor(maxChars));
    if (max === 0) {
      return "";
    }
    const stat = await this.statIfExists(filePath);
    if (!stat) {
      return "";
    }
    const maxBytes = Math.max(1, max * 4);
    const start = Math.max(0, stat.size - maxBytes);
    const length = stat.size - start;
    try {
      const handle = await fs.open(filePath, "r");
      try {
        const buffer = Buffer.alloc(length);
        await handle.read(buffer, 0, length, start);
        const text = buffer.toString("utf8");
        return text.length > max ? text.slice(-max) : text;
      } finally {
        await handle.close();
      }
    } catch {
      return "";
    }
  }
}
