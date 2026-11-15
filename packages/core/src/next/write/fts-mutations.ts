/** Reference-free WAL invalidations. First accepted keyed mutation invalidates append-only
 * acceleration permanently; repeated mutations while that history is pending are idempotent.
 * One prepaid shared commit cell installs all its table flags atomically without allocation.
 */
import type { ByteBudget } from "../storage/extents.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
import { pauseIfDue } from "../task.js";
import type { FtsMutationEffect } from "./fts-undo.js";
export const FTS_MUTATION_BASE_BYTES = 512;
export const FTS_MUTATION_ENTRY_BYTES = 512;
export const FTS_MUTATION_COMMIT_BYTES = 512;
export interface FtsMutationCommit {
  state: "preparing" | "committed" | "cancelled";
  lsn: number;
  refs: number;
  owned: boolean;
  inUse: boolean;
}
interface Entry {
  readonly tableId: number;
  schemaVersion: number;
  commit: FtsMutationCommit | null;
  previous: Entry | null;
  next: Entry | null;
}
export type FtsMutationSource = () => Iterable<FtsMutationEffect | null>;
export class FtsMutations {
  static create(budget: ByteBudget): FtsMutations {
    if (!budget.allocate(FTS_MUTATION_BASE_BYTES))
      throw new ParseBudgetError(FTS_MUTATION_BASE_BYTES);
    return new FtsMutations(budget);
  }
  readonly #entries = new Map<number, Entry>();
  #bytes = FTS_MUTATION_BASE_BYTES;
  #first: Entry | null = null;
  #last: Entry | null = null;
  #active: FtsMutationCommit | null = null;
  #covered = 0;
  #applied = 0;
  #closed = false;
  #readers = 0;
  #reclaiming: Promise<void> | null = null;
  private constructor(readonly budget: ByteBudget) {}
  get bytes(): number {
    return this.#bytes;
  }
  get appliedLsn(): number {
    return this.#applied;
  }
  #charge(n: number): void {
    if (!this.budget.allocate(n)) throw new ParseBudgetError(n);
    this.#bytes += n;
  }
  #free(n: number): void {
    this.#bytes -= n;
    this.budget.free(n);
  }
  #checkPreparation(): void {
    if (this.#closed || this.#active) throw new Error("FTS mutation preparation is not exclusive");
  }
  async prepare(source: FtsMutationSource, signal?: AbortSignal): Promise<FtsMutationCommit> {
    this.#checkPreparation();
    await this.reclaim();
    this.#checkPreparation();
    signal?.throwIfAborted();
    this.#charge(FTS_MUTATION_COMMIT_BYTES);
    const commit: FtsMutationCommit = {
      state: "preparing",
      lsn: 0,
      refs: 0,
      owned: true,
      inUse: true,
    };
    this.#active = commit;
    let unused = 0;
    try {
      let count = 0;
      for (const e of source()) {
        if (e === null) await pauseIfDue(signal ? { signal } : undefined);
        else if (!this.#entries.has(e.tableId)) count++;
      }
      this.#charge(count * FTS_MUTATION_ENTRY_BYTES);
      unused = count * FTS_MUTATION_ENTRY_BYTES;
      for (const e of source()) {
        if (e === null) {
          await pauseIfDue(signal ? { signal } : undefined);
          continue;
        }
        const existing = this.#entries.get(e.tableId);
        if (existing) {
          if (existing.commit?.state === "cancelled") {
            this.#dropCommit(existing);
            existing.schemaVersion = e.schemaVersion;
            existing.commit = commit;
            commit.refs++;
          }
          continue;
        }
        const entry: Entry = {
          tableId: e.tableId,
          schemaVersion: e.schemaVersion,
          commit,
          previous: this.#last,
          next: null,
        };
        if (this.#last) this.#last.next = entry;
        else this.#first = entry;
        this.#last = entry;
        this.#entries.set(e.tableId, entry);
        commit.refs++;
        unused -= FTS_MUTATION_ENTRY_BYTES;
      }
      if (unused !== 0) throw new Error("FTS mutation source changed during preparation");
      return commit;
    } catch (error) {
      if (unused) this.#free(unused);
      await this.release(commit);
      throw error;
    }
  }
  /** R9: actual LSN, no allocation, IO, failure or per-row/per-table loop. */
  commit(commit: FtsMutationCommit, lsn: number): void {
    commit.lsn = lsn;
    commit.state = "committed";
    this.#applied = Math.max(this.#applied, lsn);
    if (this.#active === commit) this.#active = null;
  }
  async release(commit: FtsMutationCommit): Promise<void> {
    if (commit.state === "preparing") commit.state = "cancelled";
    if (this.#active === commit) this.#active = null;
    commit.inUse = false;
    if (commit.refs === 0 && commit.owned) {
      commit.owned = false;
      this.#free(FTS_MUTATION_COMMIT_BYTES);
    }
    if (commit.state === "cancelled") await this.reclaim();
  }
  covered(lsn: number): void {
    this.#covered = Math.max(this.#covered, lsn);
  }
  async *pending(after: number, through: number, signal?: AbortSignal) {
    const bound = Math.min(through, this.#applied),
      end = this.#last;
    this.#readers++;
    try {
      let entry = this.#first;
      while (entry !== null) {
        await pauseIfDue(signal ? { signal } : undefined);
        const c = entry.commit;
        if (c?.state === "committed" && c.lsn > after && c.lsn <= bound)
          yield { tableId: entry.tableId, schemaVersion: entry.schemaVersion, lsn: c.lsn };
        // A checkpoint visits the prefix it borrowed, never an indefinitely growing tail.
        if (entry === end) break;
        entry = entry.next;
      }
    } finally {
      this.#readers--;
    }
  }
  #dropCommit(entry: Entry): void {
    const commit = entry.commit;
    entry.commit = null;
    if (commit === null) return;
    commit.refs--;
    if (commit.refs === 0 && commit.owned && !commit.inUse) {
      commit.owned = false;
      this.#free(FTS_MUTATION_COMMIT_BYTES);
    }
  }
  reclaim(): Promise<void> {
    if (this.#active || this.#closed || this.#readers > 0) return Promise.resolve();
    if (this.#reclaiming) return this.#reclaiming;
    const run = this.#reclaim();
    this.#reclaiming = run;
    return run.finally(() => {
      if (this.#reclaiming === run) this.#reclaiming = null;
    });
  }
  async #reclaim(): Promise<void> {
    for (const e of this.#entries.values()) {
      await pauseIfDue();
      if (this.#active || this.#closed || this.#readers > 0) return;
      const c = e.commit;
      if (!c || c.state === "preparing" || (c.state === "committed" && c.lsn > this.#covered))
        continue;
      this.#entries.delete(e.tableId);
      if (e.previous) e.previous.next = e.next;
      else this.#first = e.next;
      if (e.next) e.next.previous = e.previous;
      else this.#last = e.previous;
      e.previous = null;
      e.next = null;
      this.#dropCommit(e);
      this.#free(FTS_MUTATION_ENTRY_BYTES);
    }
  }
  async close(): Promise<void> {
    this.#closed = true;
    await this.#reclaiming;
    if (this.#active || this.#readers > 0)
      throw new Error("FTS mutation owner closed before its work drained");
    this.#entries.clear();
    this.#first = null;
    this.#last = null;
    this.#active = null;
    const bytes = this.#bytes;
    this.#bytes = 0;
    this.budget.free(bytes);
  }
}
