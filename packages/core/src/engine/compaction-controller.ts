import type { TableRecord } from "../storage/types.js";

interface Backoff {
  minimumSegments: number;
  failures: number;
  retryTimer: ReturnType<typeof setTimeout> | undefined;
}
interface CompactionPorts {
  enabled: boolean;
  maximumLevelZeroSegments: number;
  dropping: (id: string) => boolean;
  run: (table: TableRecord) => Promise<boolean>;
  check: (id: string) => Promise<void>;
  yield: () => Promise<void>;
  report: (error: unknown, context: string) => void;
}
/** Scheduling owner for one fold per table, retries, write-burst debounce and shutdown drain.
 * Layout counters and the bounded physical fold stay in the engine. */
export class CompactionController {
  #stopped = false;
  readonly #runs = new Map<string, Promise<void>>();
  readonly #cancelledRuns = new WeakSet<Promise<void>>();
  readonly #continuations = new Set<Promise<void>>();
  readonly #requested = new Set<string>();
  readonly #backoff = new Map<string, Backoff>();
  readonly #idleTables = new Set<string>();
  readonly #commits = new Map<string, number>();
  #idleTimer: ReturnType<typeof setTimeout> | undefined;
  constructor(private readonly ports: CompactionPorts) {}

  stop(): void {
    this.#stopped = true;
    if (this.#idleTimer !== undefined) clearTimeout(this.#idleTimer);
    this.#idleTimer = undefined;
    for (const entry of this.#backoff.values())
      if (entry.retryTimer !== undefined) clearTimeout(entry.retryTimer);
    this.#backoff.clear();
    this.#idleTables.clear();
    this.#requested.clear();
    this.#commits.clear();
  }
  async drain(): Promise<void> {
    await Promise.allSettled(this.#runs.values());
    await Promise.allSettled(this.#continuations);
  }
  forget(id: string): void {
    const run = this.#runs.get(id);
    if (run !== undefined) this.#cancelledRuns.add(run);
    this.#clearBackoff(id);
    this.#idleTables.delete(id);
    this.#commits.delete(id);
    this.#requested.delete(id);
  }
  retryPending(id: string): boolean {
    return this.#backoff.get(id)?.retryTimer !== undefined;
  }
  #clearBackoff(id: string): void {
    const previous = this.#backoff.get(id);
    if (previous?.retryTimer !== undefined) clearTimeout(previous.retryTimer);
    this.#backoff.delete(id);
  }
  #track(run: Promise<void>): void {
    this.#continuations.add(run);
    void run.then(
      () => this.#continuations.delete(run),
      () => this.#continuations.delete(run),
    );
  }
  #check(id: string): Promise<void> {
    if (this.#stopped) return Promise.resolve();
    return this.ports
      .check(id)
      .catch((error: unknown) => this.ports.report(error, `automatic compaction check for ${id}`));
  }
  schedule(table: TableRecord, visible: number): void {
    if (this.#stopped || !this.ports.enabled || this.ports.dropping(table.id)) return;
    const backoff = this.#backoff.get(table.id);
    if (
      backoff !== undefined &&
      (backoff.retryTimer !== undefined || visible < backoff.minimumSegments)
    )
      return;
    if (this.#runs.has(table.id)) {
      this.#requested.add(table.id);
      return;
    }
    const run = this.ports
      .run(table)
      .then((folded) => {
        if (this.#cancelledRuns.has(run)) return;
        if (folded) this.#clearBackoff(table.id);
        else this.backOff(table.id, visible);
      })
      .catch((error: unknown) => {
        this.ports.report(error, `automatic compaction for ${table.name}`);
        if (!this.#cancelledRuns.has(run)) this.backOff(table.id, visible, "failed");
      })
      .finally(() => {
        if (this.#runs.get(table.id) === run) this.#runs.delete(table.id);
        if (this.#requested.delete(table.id) && !this.#stopped) {
          this.#track(
            this.ports
              .yield()
              .then(() => this.#check(table.id))
              .catch((error: unknown) =>
                this.ports.report(error, `automatic compaction continuation for ${table.name}`),
              ),
          );
        }
      });
    this.#runs.set(table.id, run);
  }
  /**
   * Holds a table's folds off after one that did not help. A fold compaction declined — the
   * layout or the keys put it out of reach — is retried once the table has grown well past
   * where it was declined, since nothing short of more segments changes the answer. A fold
   * that failed is retried on time alone: what failed it (a lost turn, a transient read) does
   * not depend on how many segments the table has, and an idle table must not wait forever
   * for writes that never come. Either way the retry delay grows with each consecutive miss.
   */
  backOff(id: string, visible: number, outcome: "declined" | "failed" = "declined"): void {
    if (this.#stopped) return;
    const failures = (this.#backoff.get(id)?.failures ?? 0) + 1;
    this.#clearBackoff(id);
    const timer = setTimeout(
      () => {
        const entry = this.#backoff.get(id);
        if (entry?.retryTimer === timer) entry.retryTimer = undefined;
        this.#track(this.#check(id));
      },
      Math.min(60_000, 250 * 2 ** Math.min(16, failures - 1)),
    );
    (timer as { unref?: () => void }).unref?.();
    this.#backoff.set(id, {
      failures,
      retryTimer: timer,
      minimumSegments:
        outcome === "failed"
          ? 0
          : Math.min(this.ports.maximumLevelZeroSegments, Math.max(2, visible * 2)),
    });
  }
  committed(ids: readonly string[]): void {
    if (this.#stopped || !this.ports.enabled) return;
    for (const id of ids) {
      this.#idleTables.add(id);
      const commits = (this.#commits.get(id) ?? 0) + 1;
      if (commits < 8) this.#commits.set(id, commits);
      else {
        this.#commits.delete(id);
        this.#track(this.#check(id));
      }
    }
    if (this.#idleTables.size === 0) return;
    if (this.#idleTimer !== undefined) clearTimeout(this.#idleTimer);
    this.#idleTimer = setTimeout(() => {
      this.#idleTimer = undefined;
      const ids = [...this.#idleTables];
      this.#idleTables.clear();
      for (const id of ids) {
        this.#commits.delete(id);
        this.#track(this.#check(id));
      }
    }, 25);
    (this.#idleTimer as { unref?: () => void }).unref?.();
  }
}
