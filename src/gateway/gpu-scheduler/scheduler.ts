import { randomUUID } from "node:crypto";
import type { OpenClawConfig } from "../../config/config.js";
import type { createSubsystemLogger } from "../../logging/subsystem.js";
import type { NodeRegistry, NodeSession } from "../node-registry.js";
import type {
  GpuJob,
  GpuJobAttempt,
  GpuJobState,
  GpuJobSubmitRequest,
  GpuNodeCandidate,
} from "./types.js";
import { isNodeCommandAllowed, resolveNodeCommandAllowlist } from "../node-command-policy.js";
import { readGpuSchedulerState, writeGpuSchedulerState } from "./persist.js";

type SubsystemLogger = ReturnType<typeof createSubsystemLogger>;

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
  private pumpTimer: ReturnType<typeof setTimeout> | null = null;
  private pumping = false;
  private waitersByJobId = new Map<string, Set<(job: GpuJob) => void>>();
  private persistTimer: ReturnType<typeof setTimeout> | null = null;
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
            // Gateway restart: we don't know if the remote command is still running.
            // Mark as failed so the caller can decide to resubmit.
            job.state = "failed";
            job.updatedAtMs = nowMs();
            job.result = {
              exitCode: null,
              timedOut: false,
              success: false,
              stderrTail: "gateway restarted while job was running",
            };
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
        this.log.warn(`gpu scheduler pump failed: ${String(err)}`);
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
    if (running > 0) {
      return true;
    }
    const now = nowMs();
    return this.queue.some((jobId) => {
      const job = this.jobsById.get(jobId);
      return job?.state === "queued" && (job.notBeforeMs === undefined || job.notBeforeMs <= now);
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

    for (const jobId of this.queue) {
      const job = this.jobsById.get(jobId);
      if (!job) {
        continue;
      }
      if (job.state !== "queued") {
        continue;
      }
      if (job.notBeforeMs !== undefined && job.notBeforeMs > now) {
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
    const argv = snapshot.exec.command;
    const params: Record<string, unknown> = {
      command: argv,
      timeoutMs: snapshot.exec.commandTimeoutMs,
    };
    if (raw) {
      params.rawCommand = raw;
    }
    if (snapshot.exec.cwd) {
      params.cwd = snapshot.exec.cwd;
    }
    if (snapshot.exec.env) {
      params.env = snapshot.exec.env;
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

      if (job.cancelRequested) {
        job.state = "canceled";
      } else if (ok) {
        job.state = "succeeded";
      } else if (attempt < job.maxAttempts) {
        job.state = "queued";
        job.notBeforeMs = nowMs() + Math.min(30_000, 1000 * attempt);
        job.assignedNodeId = undefined;
      } else {
        job.state = "failed";
      }

      job.updatedAtMs = nowMs();
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
}
