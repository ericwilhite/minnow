/**
 * The write path's in-memory state as views see it: immutable `TableView`s collected in a
 * `WriteState`, replaced copy-on-write whenever a table's segment list changes (a memtable freezes,
 * a flush publishes). A snapshot captures one `WriteState` reference: pinning is O(1).
 *
 * Mutable parts a captured view may still observe are append-only and seq-qualified, so a view's
 * bound hides everything newer: memtable rows and marks, key deltas, and `GroupDeletes`.
 */
import type { Space, SpacePin } from "../access/space.js";
import type { MaybePromise } from "../access/key-index.js";
import type { BlobRef, GroupRoot, ManifestRoot, RunRef } from "../format/manifest.js";
import type { GroupDirectory } from "./groups.js";
import type { Held } from "../format/reachable.js";
import type { ByteBudget } from "../storage/extents.js";
import type { Memtable } from "./memtable.js";
import type { RunReader } from "./runs.js";

/**
 * Deletions of rows in durable groups: group id → position → seq. One instance per table for the
 * write path's life, shared by every view: a view reads the entries with
 * `foldedThrough < seq ≤ bound` (older ones are in its root's bitmaps), so a publish that folds
 * entries into bitmaps never changes what an older view sees.
 */
export class GroupDeletes {
  readonly byGroup = new Map<number, Map<number, number>>();

  add(groupId: number, position: number, seq: number): void {
    let positions = this.byGroup.get(groupId);
    if (positions === undefined) {
      positions = new Map();
      this.byGroup.set(groupId, positions);
    }
    positions.set(position, seq);
  }

  remove(groupId: number, position: number): void {
    this.byGroup.get(groupId)?.delete(position);
  }

  restamp(groupId: number, position: number, seq: number): void {
    const positions = this.byGroup.get(groupId);
    if (positions?.has(position) === true) positions.set(position, seq);
  }

  /** Positions of `groupId` deleted with `after < seq ≤ bound`. */
  between(groupId: number, after: number, bound: number): number[] {
    const out: number[] = [];
    for (const [position, seq] of this.byGroup.get(groupId) ?? [])
      if (seq > after && seq <= bound) out.push(position);
    return out;
  }

  /** Forgets positions deleted with seq ≤ `through` (folded into bitmaps every reader uses). */
  prune(through: number): void {
    for (const [groupId, positions] of this.byGroup) {
      for (const [position, seq] of positions) if (seq <= through) positions.delete(position);
      if (positions.size === 0) this.byGroup.delete(groupId);
    }
  }

  get size(): number {
    let size = 0;
    for (const positions of this.byGroup.values()) size += positions.size;
    return size;
  }
}

export interface TableView {
  readonly tableId: number;
  /** Durable groups in row-id order (shared immutable chunks, refcounted by write states). */
  readonly groups: GroupDirectory;
  /** Durable runs per index id, newest first. */
  readonly runs: ReadonlyMap<number, readonly RunRef[]>;
  readonly groupDeletes: GroupDeletes;
  /** Deletions with seq ≤ this are in the groups' bitmaps. */
  readonly foldedThrough: number;
  /**
   * Groups this session published from a memtable: deletions recorded in the memtable (before or
   * during its flush) stay there, and count for the group above `foldedThrough`.
   */
  readonly groupSources: ReadonlyMap<number, Memtable>;
  /** Memtables, oldest first; the last is the live one. */
  readonly memtables: readonly Memtable[];
}

/** Positions of a group deleted with `after < seq ≤ bound`, from both delta sources. */
export function groupDeletions(view: TableView, groupId: number, bound: number): number[] {
  const out = view.groupDeletes.between(groupId, view.foldedThrough, bound);
  const source = view.groupSources.get(groupId);
  if (source !== undefined)
    for (const [position, seq] of source.deletions)
      if (seq > view.foldedThrough && seq <= bound) out.push(position);
  return out;
}

export interface WriteState {
  readonly root: ManifestRoot;
  readonly tables: ReadonlyMap<number, TableView>;
}

/** Live readers, in pin order, so publication pins are released once older readers are gone. */
export class ReaderRegistry {
  #next = 1;
  readonly #live = new Set<number>();
  /** Re-point holds (a pin and retained deltas), each waiting for every reader older than `before`. */
  readonly #pins: Array<{ readonly before: number; readonly release: () => void }> = [];

  open(): number {
    const id = this.#next++;
    this.#live.add(id);
    return id;
  }

  close(id: number): void {
    if (!this.#live.delete(id)) return;
    this.#drain();
  }

  get count(): number {
    return this.#live.size;
  }

  /** Runs `release` once every reader opened before now is closed (at once if there is none). */
  holdUntilOlderClose(release: () => void): void {
    this.#pins.push({ before: this.#next, release });
    this.#drain();
  }

  get heldPins(): number {
    return this.#pins.length;
  }

  #drain(): void {
    let oldest = Infinity;
    for (const id of this.#live) {
      oldest = id;
      break;
    }
    while (this.#pins.length > 0) {
      const first = this.#pins[0];
      if (first === undefined || first.before > oldest) break;
      this.#pins.shift();
      first.release();
    }
  }
}

/** What snapshots and readers share: the space, caches of decoded metadata, the write path's budget. */
export interface Shared {
  readonly governor: import("../memory/governor.js").MemoryGovernor;
  readonly space: Space;
  /** The write path's own budget for resident metadata and short leases. */
  readonly budget: ByteBudget;
  readonly readers: ReaderRegistry;
  /**
   * An extent-space pin on the root of `state`, shared by every view of that state (refcounted:
   * the space pin is taken once per state and released with the last view).
   */
  pinState(state: WriteState, label: string): SpacePin;
  runReader(ref: RunRef): RunReader | Promise<RunReader>;
  /**
   * A group's root through the metadata reader (format/metadata-reader.ts): cached and shared,
   * synchronous on a hit, charged to `budget` until the `Held` is released.
   */
  groupRoot(ref: BlobRef, budget: ByteBudget): MaybePromise<Held<GroupRoot>>;
  /** A group's canonical deletion words (bit set = deleted), checked against its stored row count. */
  bitmap(ref: BlobRef, rowCount: number): Uint32Array | Promise<Uint32Array>;
}
