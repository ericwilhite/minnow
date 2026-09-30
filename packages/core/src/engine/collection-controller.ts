import { GarbageCollectionJobConflictError } from "../storage/types.js";
import { MaintenanceBacklogError } from "./errors.js";
import { backgroundErrorSummary } from "./background-diagnostics.js";
import type { MaintenanceStatus } from "./database.js";

const AUTO_COLLECT_COMMIT_INTERVAL = 64;
const AUTO_COLLECT_QUIET_MS = 60_000;
const AUTO_COLLECT_RETRY_MIN_MS = 1_000;
const AUTO_COLLECT_RETRY_MAX_MS = 60_000;

interface CollectionPorts {
  readonly enabled: boolean;
  readonly debtLimit: number;
  readonly now: () => number;
  readonly run: () => Promise<{ moreWork: boolean; reclaimed: boolean }>;
  readonly yield: () => Promise<void>;
  readonly durableDebt: () => Promise<number>;
  readonly isShutdownRefusal: (error: unknown) => boolean;
  readonly report: (error: unknown, context: string) => void;
}

type CollectionStatus = Omit<
  MaintenanceStatus,
  | "postingDeltaTailMarkers"
  | "retainedPlanEntries"
  | "retainedStatementEntries"
  | "backgroundFailureCount"
  | "backgroundErrors"
>;

/** Owns collection scheduling and backpressure. Physical reclamation stays in the engine:
 * the port is called once per bounded run, never per record or row. stop prevents new work;
 * drain joins both an admitted run and its cooperative continuation before store disposal. */
export class CollectionController {
  readonly #ports: CollectionPorts;
  #stopped = false;
  #running = false;
  #requested = false;
  #debt = 0;
  #manualDebtInitialized = false;
  #commits = 0;
  #failures = 0;
  #lastError: { name: string; message: string; at: number } | undefined;
  #startedAt: number | undefined;
  #completedAt: number | undefined;
  #retryAt: number | undefined;
  #retryTimer: ReturnType<typeof setTimeout> | undefined;
  #idleTimer: ReturnType<typeof setTimeout> | undefined;
  #task: Promise<void> | undefined;
  #continuation: Promise<void> | undefined;
  #steps: Promise<unknown> = Promise.resolve();

  constructor(ports: CollectionPorts) {
    this.#ports = ports;
  }

  start(): void {
    this.#armIdle();
  }

  stop(): void {
    this.#stopped = true;
    this.#requested = false;
    if (this.#idleTimer !== undefined) clearTimeout(this.#idleTimer);
    if (this.#retryTimer !== undefined) clearTimeout(this.#retryTimer);
    this.#idleTimer = undefined;
    this.#retryTimer = undefined;
    this.#retryAt = undefined;
  }

  async drain(): Promise<void> {
    // A run can install its continuation while settling, so capture it after joining the run.
    await this.#task;
    await this.#continuation;
  }

  committed(): void {
    this.#commits += 1;
    this.#debt += 1;
    if (
      this.#commits >= AUTO_COLLECT_COMMIT_INTERVAL ||
      (this.#startedAt !== undefined &&
        this.#ports.now() - this.#startedAt >= AUTO_COLLECT_QUIET_MS)
    ) {
      this.schedule();
    }
    this.#armIdle();
  }

  collectedManually(): void {
    this.#debt = 0;
    this.#manualDebtInitialized = true;
    this.#commits = 0;
    this.#completedAt = this.#ports.now();
  }

  async assist(): Promise<void> {
    if (!this.#ports.enabled && !this.#manualDebtInitialized) {
      this.#debt = Math.max(
        this.#debt,
        Math.min(this.#ports.debtLimit, await this.#ports.durableDebt()),
      );
      this.#manualDebtInitialized = true;
    }
    if (this.#debt < this.#ports.debtLimit) return;
    if (!this.#ports.enabled)
      throw new MaintenanceBacklogError(
        this.#debt,
        "automatic collection is disabled; call collectGarbage() before writing again",
      );
    this.schedule(true);
    await this.#task;
    if (this.#debt >= this.#ports.debtLimit)
      throw new MaintenanceBacklogError(
        this.#debt,
        this.#lastError?.message ?? "bounded collection assistance left a backlog",
      );
  }

  async step<T>(step: () => Promise<T>): Promise<T> {
    const run = this.#steps.then(step, step);
    this.#steps = run;
    try {
      return await run;
    } finally {
      if (this.#steps === run) this.#steps = Promise.resolve();
    }
  }

  status(): CollectionStatus {
    const error = this.#lastError;
    return {
      autoCollectionEnabled: this.#ports.enabled,
      collectionRunning: this.#running,
      collectionRequested: this.#requested,
      pendingCommitDebt: this.#debt,
      consecutiveFailures: this.#failures,
      lastStartedAt: this.#startedAt === undefined ? null : new Date(this.#startedAt),
      lastCompletedAt: this.#completedAt === undefined ? null : new Date(this.#completedAt),
      nextRetryAt: this.#retryAt === undefined ? null : new Date(this.#retryAt),
      lastError: error === undefined ? null : { ...error, at: new Date(error.at) },
    };
  }

  schedule(force = false): void {
    if (this.#stopped) return;
    if (!this.#ports.enabled) return;
    if (this.#running) {
      // A fold finishing or a quiet minute passing while a run is under way is a reason for
      // one more run once this one ends — a dropped trigger after the last commit of a burst
      // would otherwise leave the burst's leftovers until the next one.
      this.#requested = true;
      return;
    }
    if (this.#retryTimer !== undefined && !force) return;
    if (force && this.#retryTimer !== undefined) {
      clearTimeout(this.#retryTimer);
      this.#retryTimer = undefined;
      this.#retryAt = undefined;
    }
    this.#running = true;
    this.#requested = false;
    this.#commits = 0;
    this.#startedAt = this.#ports.now();
    const run = this.#ports
      .run()
      .then(({ moreWork, reclaimed }) => {
        this.#failures = 0;
        this.#lastError = undefined;
        this.#completedAt = this.#ports.now();
        this.#retryAt = undefined;
        if (moreWork) this.#requested = true;
        // Debt counts commits the collector has not kept up with. A pass that reclaimed
        // something kept up; only a run that found nothing to reclaim while compaction work
        // is still outstanding leaves the count for the next one to settle.
        if (reclaimed || !moreWork) this.#debt = 0;
      })
      .catch((error: unknown) => {
        // Only our own shutdown refusal is expected. An I/O failure that arrives while close
        // joins this task must still reach the diagnostic hook.
        if (this.#ports.isShutdownRefusal(error)) return;
        // Another connection advanced or finished the job this run was driving. Nothing went
        // wrong -- the work is being done -- so nothing is reported; a later trigger resumes
        // whatever is left.
        if (error instanceof GarbageCollectionJobConflictError) {
          this.#requested = true;
          return;
        }
        this.#failures += 1;
        const at = this.#ports.now();
        this.#lastError = {
          ...backgroundErrorSummary(error),
          at,
        };
        this.#ports.report(error, "auto collection");
        this.#scheduleRetry();
      })
      .finally(() => {
        if (this.#task === run) this.#task = undefined;
        this.#running = false;
        if (this.#requested) {
          this.#continuation = this.#ports
            .yield()
            .then(() => {
              // Keep `collectionRequested` true until the continuation takes ownership. Otherwise
              // maintenanceStatus briefly reports a false idle state between bounded runs, and a
              // caller waiting for quiescence can leave a real backlog behind.
              if (this.#stopped) {
                this.#requested = false;
                return;
              }
              // A commit may already have started the requested run during the yield. Its start
              // clears the flag, and any newer request made while it runs remains set for that
              // run's own continuation.
              if (this.#running) return;
              this.schedule();
            })
            .catch((error: unknown) => this.#ports.report(error, "auto collection continuation"));
        }
      });
    this.#task = run;
    void run;
  }

  #scheduleRetry(): void {
    if (this.#stopped || !this.#ports.enabled || this.#retryTimer !== undefined) return;
    const exponent = Math.min(16, Math.max(0, this.#failures - 1));
    const delay = Math.min(AUTO_COLLECT_RETRY_MAX_MS, AUTO_COLLECT_RETRY_MIN_MS * 2 ** exponent);
    this.#retryAt = this.#ports.now() + delay;
    const timer = setTimeout(() => {
      this.#retryTimer = undefined;
      this.#retryAt = undefined;
      this.schedule(true);
    }, delay);
    (timer as { unref?: () => void }).unref?.();
    this.#retryTimer = timer;
  }

  /**
   * A pass a quiet period after the last commit, for a tab that stops writing: the retained
   * window's age bound lets that pass reclaim what the last burst superseded, which no commit
   * would otherwise arrive to trigger. Re-armed by every commit; unreferenced, so it never
   * keeps a process alive.
   */
  #armIdle(): void {
    if (this.#stopped) return;
    if (!this.#ports.enabled) return;
    if (this.#idleTimer !== undefined) clearTimeout(this.#idleTimer);
    const timer = setTimeout(() => {
      this.#idleTimer = undefined;
      this.schedule();
    }, AUTO_COLLECT_QUIET_MS);
    (timer as { unref?: () => void }).unref?.();
    this.#idleTimer = timer;
  }
}
