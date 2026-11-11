/**
 * Reading the key index and secondary indexes (owner: write path + key index, track B3). The
 * executor uses this for point reads (`keyLookup`), index nested-loop joins, index range scans
 * and ordered scans; the write path uses the same structure for constraint checks.
 *
 * ## Structure (for the reader's cost model)
 * An index maps an equality key (keys/key-encoding.ts; scalar types only) to the stable row ids of
 * the rows holding it. Its entries are `(key, rowId, kind)` with kind put or tombstone, in layers:
 * - in memory, per table: the memtable delta, a typed hash keyed by the key bytes, whose entries
 *   carry a visibility `seq` (snapshot.ts), with write-maintained ordered leaves for range reads;
 * - on disk: immutable sorted runs (format/manifest.ts `RunRef`), each with fences (every Nth key
 *   and its offset) and a partitioned Bloom filter. A run is visible to every view that pins its
 *   root, so run entries need no seq.
 * - **Levels (ruling on B3 increment 2a: tiering, not one run per level).** A flush adds a level-0
 *   run; a level holding 8 sorted runs is merged into one sorted run at the next level
 *   (write/merge.ts). With N entries and M entries per memtable flush there are
 *   L = 1 + ⌈log₈(N / M)⌉ levels, and each entry is written L times (2 at 1M keyed rows, 3 at 5M).
 * - **Read amplification bound.** A point read probes, per index, every memtable layer plus the
 *   runs whose key range holds the key: at most 7 per level in steady state (7·L), and while
 *   merges lag, at most 23 at level 0 (writers wait at 24) plus 7 per higher level
 *   (23 + 7·(L − 1)). The split pieces of one merge have disjoint key ranges and count once.
 *   Each probed run costs a fence search and a Bloom test; a block is read only on a Bloom hit
 *   (~1% false positives). At 5M rows (L = 3): ≤ 21 runs steady, ≤ 37 transient.
 * Layers are ordered newest first, and a tombstone is always in the same layer as its put or a
 * newer one (a delete follows its insert; a merge keeps recency order and cancels a put with its
 * tombstone when they meet). A delete writes a tombstone into every index the row was in (the
 * commit frame carries those keys: format/log-frame.ts `TableChange.deletes`), so resolving an
 * index never reads a deletion bitmap.
 *
 * ## Resolution (`resolveVersions` is the reference procedure)
 * Under a view's bound, a put `(K, r)` is live iff it is visible and no visible tombstone
 * `(K, r)` exists (row ids are never reused, so a tombstone names exactly one put). A unique
 * index has at most one live put per key; its lookup stops at the first live put, newest layer
 * first. A second live put in a unique index is `IndexCorruptionError`.
 *
 * ## Keys
 * Callers encode keys with `encodeKeyBatch("equality", …)` from vectors of exactly the index
 * columns' physical types (lowering inserts casts; int32 and int64 keys do not compare). A key
 * with a null component never matches (indexes hold no null-containing keys; PostgreSQL's NULLS
 * DISTINCT). Key byte order is SQL order for `ordered` indexes under the C collation only: a range
 * under an ICU collation must not use an index.
 *
 * ## Output, lifetime, memory
 * - Results are row locators of the reader's view (vector/locator.ts), written into
 *   caller-owned buffers. The reader allocates nothing per key; its scratch (a sort permutation
 *   of one batch, a decoded run block) is fixed-size, reused, and charged to the write path's
 *   grant. Run blocks come through the page cache (governor-charged, evictable).
 * - Calls are synchronous when every block they need is cached (and always for memtable-only
 *   tables) unless the shared worker task clock is due. A Promise represents I/O, admission
 *   pressure or a real task yield: callers `await` only a Promise.
 * - A reader and its cursors are valid while its snapshot is pinned; afterwards they throw
 *   `SnapshotReleasedError`. They are not thread-safe across awaits: one call at a time per
 *   reader (a pipeline owns its reader).
 * - Cancellation: calls are bounded (one batch, one block read per run); callers check their
 *   signal between calls. A cursor is abandoned with `close()`.
 */
import type { Grant } from "../memory/governor.js";
import { compareBytes } from "../vector/varlen.js";
import type { KeyBatch } from "../keys/key-encoding.js";
import type { IndexDef } from "./catalog.js";
import { isVisible } from "./snapshot.js";

export type MaybePromise<T> = T | Promise<T>;

export interface IndexReader {
  readonly def: IndexDef;
  /**
   * Batch point lookup. Writes every live match of keys `0 … keys.count − 1` into `out`, in key
   * order of the batch (all matches of key k before those of k + 1), each key's matches in row-id
   * order. Stops when `out` is full and returns a `resume` token; pass it back (with the same
   * `keys` and a drained `out`) to continue. A unique index with `capacity(out) ≥ keys.count`
   * always finishes in one call. Probe keys may repeat; each occurrence reports its matches.
   */
  probe(keys: KeyBatch, out: ProbeOutput, resume?: ProbeResume): MaybePromise<ProbeResult>;
  /**
   * Live rows whose key lies in `range`, in key order then row-id order (both reversed for
   * `desc`). `ordered` indexes only; an `equality` index throws `TypeError` (use `probe`).
   */
  range(range: KeyRange, direction: "asc" | "desc"): IndexCursor;
  /** Plan without native allocations or admission; range bounds must remain immutable. */
  planRange?(range: KeyRange, direction: "asc" | "desc"): IndexRangePlan;
}

/** A single-use plan for one pinned view. The complete minimum includes resident heads,
 * key copies, navigation and overlapping encoded/decoded I/O. Publication cannot widen it.
 * Open never admits or grows. The caller owns the dedicated grant and closes it only after
 * pending cursor calls settle; cursor close releases allocations, never the caller's grant.
 * The signal cancels reads, waits and traversal. No leases are held before open.
 */
export interface IndexRangePlan {
  readonly minimumBytes: number;
  open(options: { readonly budget: Grant; readonly signal: AbortSignal }): IndexCursor;
}

/** Caller-owned probe output. Capacity is `min(probes.length, locators.length)`. */
export interface ProbeOutput {
  /** Match m came from key `probes[m]` of the batch (an index into the `KeyBatch`). */
  readonly probes: Int32Array;
  readonly locators: Float64Array;
  /** Matches written by the last call (the reader sets it). */
  count: number;
}

/** Opaque continuation of a probe that filled its output. */
export interface ProbeResume {
  readonly key: number;
}

export type ProbeResult =
  { readonly done: true } | { readonly done: false; readonly resume: ProbeResume };

export interface IndexCursor {
  /** Fills `out` from index 0; returns how many locators it wrote, 0 at the end. */
  next(out: Float64Array): MaybePromise<number>;
  /**
   * Advances by at most `count` visible rows in this cursor's order, without materializing
   * locators. Returns the number skipped (possibly fewer at end). Count must be a nonnegative
   * safe integer. Shares next's snapshot, cancellation and single outstanding call contract.
   */
  skip(count: number): MaybePromise<number>;
  /**
   * Abandons the remaining range without enumerating it. Idempotent; cancels pending admission
   * and I/O. An outstanding next or skip settles before its scratch is released. The caller retains
   * out until then; the cursor retains no output buffer once that call settles.
   */
  close(): void;
}

/**
 * A key range over equality-key bytes. Because key components are prefix-free, a bound can name a
 * **prefix**: a composite index with `k1 = a AND k2 > b` is `{ lower: after(enc(a)·enc(b)),
 * upper: through(enc(a)) }`.
 * - `atLeast P`: keys ≥ P. `after P`: keys greater than every key that starts with P.
 * - `before P`: keys < P. `through P`: keys ≤ P or starting with P.
 * - null: unbounded.
 */
export interface KeyRange {
  readonly lower: { readonly kind: "atLeast" | "after"; readonly key: Uint8Array } | null;
  readonly upper: { readonly kind: "before" | "through"; readonly key: Uint8Array } | null;
}

/** All keys starting with `prefix` (equality on leading key columns). */
export function prefixRange(prefix: Uint8Array): KeyRange {
  return { lower: { kind: "atLeast", key: prefix }, upper: { kind: "through", key: prefix } };
}

function concat(left: Uint8Array, right: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(left.length + right.length);
  bytes.set(left);
  bytes.set(right, left.length);
  return bytes;
}

/**
 * The range for `leading = prefix AND next cmp value` on a composite index: `prefix` is the
 * equality-key bytes of the leading columns (empty for none), `value` the equality-key component
 * of the next column (`encodeEqualityKey` of one part).
 */
export function comparisonRange(
  prefix: Uint8Array,
  cmp: "=" | "<" | "<=" | ">" | ">=",
  value: Uint8Array,
): KeyRange {
  const full = concat(prefix, value);
  switch (cmp) {
    case "=":
      return prefixRange(full);
    case ">":
      return { lower: { kind: "after", key: full }, upper: { kind: "through", key: prefix } };
    case ">=":
      return { lower: { kind: "atLeast", key: full }, upper: { kind: "through", key: prefix } };
    case "<":
      return { lower: { kind: "atLeast", key: prefix }, upper: { kind: "before", key: full } };
    case "<=":
      return { lower: { kind: "atLeast", key: prefix }, upper: { kind: "through", key: full } };
  }
}

function startsWith(key: Uint8Array, prefix: Uint8Array): boolean {
  if (key.length < prefix.length) return false;
  for (let index = 0; index < prefix.length; index++)
    if (key[index] !== prefix[index]) return false;
  return true;
}

const compare = (left: Uint8Array, right: Uint8Array): number =>
  compareBytes(left, 0, left.length, right, 0, right.length);

/** The range rule: is `key` inside `range`? (The reference for run scans and tests.) */
export function inKeyRange(range: KeyRange, key: Uint8Array): boolean {
  const { lower, upper } = range;
  if (lower !== null) {
    const order = compare(key, lower.key);
    if (order < 0) return false;
    if (lower.kind === "after" && startsWith(key, lower.key)) return false;
  }
  if (upper !== null) {
    const order = compare(key, upper.key);
    if (upper.kind === "before" ? order >= 0 : order > 0 && !startsWith(key, upper.key))
      return false;
  }
  return true;
}

/** One index entry for one key, as resolution sees it. Run entries have `seq` 0. */
export interface IndexEntry {
  readonly rowId: number;
  readonly seq: number;
  readonly tombstone: boolean;
}

export class IndexCorruptionError extends Error {
  readonly code = "MINNOW_INDEX_CORRUPT";
  constructor(message: string) {
    super(message);
    this.name = "IndexCorruptionError";
  }
}

/**
 * The reference resolution of one key: `layers` are that key's entries per layer, newest layer
 * first. Returns the live row ids under `bound`, ascending. For a unique index it returns at most
 * one, stopping at the first live put (newest first), and throws `IndexCorruptionError` when
 * `verify` is set and a second live put exists.
 */
export function resolveVersions(
  layers: ReadonlyArray<readonly IndexEntry[]>,
  bound: number,
  unique: boolean,
  verify = false,
): number[] {
  const dead = new Set<number>();
  const live: number[] = [];
  for (const layer of layers) {
    for (const entry of layer)
      if (entry.tombstone && isVisible(entry.seq, bound)) dead.add(entry.rowId);
    for (const entry of layer) {
      if (entry.tombstone || !isVisible(entry.seq, bound) || dead.has(entry.rowId)) continue;
      live.push(entry.rowId);
      if (unique && !verify) return live;
    }
  }
  if (unique && live.length > 1)
    throw new IndexCorruptionError(
      `A unique index holds ${String(live.length)} live rows for a key`,
    );
  return live.sort((a, b) => a - b);
}
