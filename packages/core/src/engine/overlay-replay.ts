/**
 * Typed state for the streamed mutation replay (`#buildStreamedOverlayState` in database.ts).
 *
 * A keyed table whose history holds updates, deletes, or upserts scans its insert/base/upsert
 * rows in written order and overlays the deltas on top. Deltas name rows by key, while scan rows
 * are numbered by position, so the replay is a join from key to position: which scan rows are
 * dead, and which delta rows patch which surviving ones. This module holds that join's state in
 * typed arrays — a key index, per-key slots and newest patch locations, and patch lists sorted by
 * slot — so it costs a few bytes per touched key instead of a map entry and an object per patch.
 *
 * Everything here is synchronous and storage-free: database.ts walks the key blocks (zone
 * pruning, the buffer pool, event-loop slicing) and hands each decoded key vector to these
 * classes.
 */
import type { SegmentRecord } from "../storage/types.js";
import type { ColumnVector } from "./vector.js";
import { maybeYieldToEventLoop } from "../work-slicer.js";

/** A unique key read straight out of a vector: no per-row string token, no allocation. */
export type OverlayKey = string | number | boolean;

/** "No slot" and "no patch" in the typed arrays below. Slots and locations stay below it. */
export const OVERLAY_NONE = 0xffffffff;

/**
 * Modeled bytes a kept patch costs: its slot and its newest location, four bytes each. The
 * retained replay of a table is about this times its patched rows, plus a bit per scan row.
 */
export const OVERLAY_PATCH_BYTES = 8;

/** Bytes per key-index cell: the key (eight) and its entry (four). */
const NUMBER_INDEX_CELL_BYTES = 12;
/** Modeled bytes per key in the string/boolean index: a map entry and its key reference. */
const MAP_INDEX_ENTRY_BYTES = 64;
/** Modeled bytes per entry: slot, newest location, and the slot's segment, four bytes each. */
const ENTRY_BYTES = 12;
/** Modeled bytes for an entry's older layers: a map entry plus eight bytes a layer. */
const OLDER_ENTRY_BYTES = 64;
const OLDER_LAYER_BYTES = 8;

/**
 * Modeled bytes per scan row of one lazily replayed slot range: the range's key index and
 * entries plus the patch list it emits.
 */
export const OVERLAY_RANGE_BYTES_PER_ROW =
  2 * NUMBER_INDEX_CELL_BYTES + ENTRY_BYTES + OVERLAY_PATCH_BYTES;

/** Counters tests read to see which bounded path a replay took. */
export const overlayReplayTestHooks = {
  /** Replays that split their touched keys into more than one hash partition. */
  partitionedBuilds: 0,
  /** Slot ranges whose patches a scan replayed because the replay could not keep them. */
  rangeReplays: 0,
  /** Replays of a pruned scan that followed the keys it reads rather than the deltas'. */
  scanKeyedBuilds: 0,
  /** When set, replaces a query's fair share for kept patches: tests force range replay. */
  fairShareBytes: undefined as number | undefined,
  /** When set, replaces the scratch one replay pass may take: tests force partitions. */
  scratchBytes: undefined as number | undefined,
};

const hashScratch = new Float64Array(1);
const hashWords = new Uint32Array(hashScratch.buffer);

/** Murmur3's 32-bit finalizer: every input bit moves every output bit. */
function mix32(value: number): number {
  let hash = value | 0;
  hash ^= hash >>> 16;
  hash = Math.imul(hash, 0x85ebca6b);
  hash ^= hash >>> 13;
  hash = Math.imul(hash, 0xc2b2ae35);
  hash ^= hash >>> 16;
  return hash >>> 0;
}

/** A number key's hash; equal keys (0 and -0 included) hash equally. */
export function overlayNumberHash(key: number): number {
  if ((key | 0) === key) return mix32(key | 0);
  hashScratch[0] = key;
  return mix32((hashWords[0] ?? 0) ^ Math.imul(hashWords[1] ?? 0, 0x9e3779b1));
}

function overlayStringHash(key: string): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < key.length; index += 1) {
    hash = Math.imul(hash ^ key.charCodeAt(index), 0x01000193);
  }
  return mix32(hash);
}

export function overlayKeyHash(key: OverlayKey): number {
  if (typeof key === "number") return overlayNumberHash(key);
  if (typeof key === "string") return overlayStringHash(key);
  return mix32(key ? 1 : 2);
}

/**
 * The partition a key's hash falls in. It reads the hash's high bits, so the keys of one
 * partition still spread over every cell of a key index addressed by the low bits.
 */
export function overlayPartitionOf(hash: number, partitions: number): number {
  return partitions === 1 ? 0 : Math.floor((hash / 4_294_967_296) * partitions);
}

/** Thrown when a replay's scratch would pass the limit its caller set for one pass. */
export class OverlayScratchOverflow extends Error {
  constructor(
    /** Scratch bytes the pass held when it stopped. */
    readonly bytes: number,
    /** The share of the pass's key collection done when it stopped, in (0, 1]. */
    readonly progress: number,
  ) {
    super("Streamed mutation replay scratch overflow");
  }
}

/** Charges scratch bytes; throws (an overflow or a budget error) to stop the pass. */
export type OverlayCharge = (bytes: number) => void;

/** Open addressing from a number key to a dense entry id, entries numbered as added. */
class NumberKeyIndex {
  #keys: Float64Array;
  #entries: Int32Array;
  #mask: number;
  #size = 0;
  readonly #charge: OverlayCharge;

  constructor(charge: OverlayCharge, expected = 8) {
    this.#charge = charge;
    let capacity = 16;
    while (capacity < expected * 2) capacity *= 2;
    charge(capacity * NUMBER_INDEX_CELL_BYTES);
    this.#keys = new Float64Array(capacity);
    this.#entries = new Int32Array(capacity).fill(-1);
    this.#mask = capacity - 1;
  }

  get size(): number {
    return this.#size;
  }

  find(key: number, hash: number): number {
    const keys = this.#keys;
    const entries = this.#entries;
    const mask = this.#mask;
    let cell = hash & mask;
    for (;;) {
      const entry = entries[cell] ?? -1;
      if (entry === -1 || keys[cell] === key) return entry;
      cell = (cell + 1) & mask;
    }
  }

  /** The key's entry, adding it as entry `size` when absent. */
  add(key: number, hash: number): number {
    if ((this.#size + 1) * 2 > this.#entries.length) this.#grow();
    const keys = this.#keys;
    const entries = this.#entries;
    const mask = this.#mask;
    let cell = hash & mask;
    for (;;) {
      const entry = entries[cell] ?? -1;
      if (entry === -1) {
        keys[cell] = key;
        entries[cell] = this.#size;
        this.#size += 1;
        return this.#size - 1;
      }
      if (keys[cell] === key) return entry;
      cell = (cell + 1) & mask;
    }
  }

  #grow(): void {
    const oldKeys = this.#keys;
    const oldEntries = this.#entries;
    const capacity = oldEntries.length * 2;
    // The model charges what stays resident: the old cells die with the rehash.
    this.#charge((capacity - oldEntries.length) * NUMBER_INDEX_CELL_BYTES);
    const keys = new Float64Array(capacity);
    const entries = new Int32Array(capacity).fill(-1);
    const mask = capacity - 1;
    for (let old = 0; old < oldEntries.length; old += 1) {
      const entry = oldEntries[old] ?? -1;
      if (entry === -1) continue;
      const key = oldKeys[old] ?? 0;
      let cell = overlayNumberHash(key) & mask;
      while ((entries[cell] ?? -1) !== -1) cell = (cell + 1) & mask;
      keys[cell] = key;
      entries[cell] = entry;
    }
    this.#keys = keys;
    this.#entries = entries;
    this.#mask = mask;
  }

  /** Every key, unordered. */
  keys(): Float64Array {
    const result = new Float64Array(this.#size);
    let next = 0;
    for (let cell = 0; cell < this.#entries.length; cell += 1) {
      if ((this.#entries[cell] ?? -1) !== -1) result[next++] = this.#keys[cell] ?? 0;
    }
    return result;
  }
}

/**
 * Grows a typed array to at least `length` elements, doubling, new elements OVERLAY_NONE. The
 * charge is the growth: the old array dies with the copy.
 */
function grownUint32(array: Uint32Array, length: number, charge: OverlayCharge): Uint32Array {
  if (length <= array.length) return array;
  let capacity = Math.max(16, array.length * 2);
  while (capacity < length) capacity *= 2;
  charge((capacity - array.length) * Uint32Array.BYTES_PER_ELEMENT);
  const grown = new Uint32Array(capacity).fill(OVERLAY_NONE);
  grown.set(array);
  return grown;
}

/** Reads a key vector's rows as primitives, refusing what a unique key column cannot hold. */
function keyVectorReader(vector: ColumnVector): (row: number) => OverlayKey {
  if (vector.window !== undefined) throw new Error("Unique key vector cannot be read as keys");
  const validity = vector.validity;
  const present = (row: number): void => {
    if (((validity[row >>> 3] ?? 0) & (1 << (row & 7))) === 0) {
      throw new TypeError("Unique key cannot be null");
    }
  };
  if (vector.kind === "string") {
    const { codes, dictionary } = vector;
    return (row) => {
      present(row);
      const key = dictionary[codes[row] ?? OVERLAY_NONE];
      if (key === undefined) throw new Error("String vector code is invalid");
      return key;
    };
  }
  if (vector.kind === "boolean") {
    const values = vector.values;
    return (row) => {
      present(row);
      return values[row] === 1;
    };
  }
  const values = vector.values;
  return (row) => {
    present(row);
    return values[row] ?? 0;
  };
}

/** The number values of a key vector, or undefined for a string or boolean key. */
function numberKeyValues(vector: ColumnVector): Float64Array | undefined {
  if (vector.window !== undefined) throw new Error("Unique key vector cannot be read as keys");
  return vector.kind === "number" || vector.kind === "datetime" ? vector.values : undefined;
}

function assertKeyPresent(validity: Uint8Array, row: number): void {
  if (((validity[row >>> 3] ?? 0) & (1 << (row & 7))) === 0) {
    throw new TypeError("Unique key cannot be null");
  }
}

/**
 * The update and upsert segments a replay can patch rows from, and how a patch names a row in
 * them: a location, the row's ordinal over all their rows in visible order. A segment's columns
 * share one block layout, so one location finds the value in every column the segment carries.
 */
export class OverlayPatchSources {
  readonly segments: readonly SegmentRecord[];
  /** The first location of each source; one extra entry holds the total. */
  readonly rowBase: Float64Array;
  /** Per source, its key blocks' first rows plus its row count, as the replay records them. */
  readonly blockStarts: Uint32Array[];
  /** Per source, whether its columns include every column of every source. */
  readonly fullCover: Uint8Array;
  /** Per source, the columns it carries as a bitset over every source's columns. */
  readonly #columns: Uint32Array[];
  readonly #words: number;
  #covered: Uint32Array;
  readonly #recorded: Uint8Array;

  constructor(segments: readonly SegmentRecord[], keyColumnId: string) {
    this.segments = segments;
    this.rowBase = new Float64Array(segments.length + 1);
    for (let index = 0; index < segments.length; index += 1) {
      this.rowBase[index + 1] = (this.rowBase[index] ?? 0) + (segments[index]?.rowCount ?? 0);
    }
    if ((this.rowBase[segments.length] ?? 0) >= OVERLAY_NONE) {
      throw new RangeError("Update and upsert history is too large to replay");
    }
    this.blockStarts = segments.map(
      (segment) => new Uint32Array((segment.columnBlockIds[keyColumnId]?.length ?? 0) + 1),
    );
    this.#recorded = new Uint8Array(segments.length);
    const columnIndexes = new Map<string, number>();
    for (const segment of segments) {
      for (const id of Object.keys(segment.columnBlockIds)) {
        if (!columnIndexes.has(id)) columnIndexes.set(id, columnIndexes.size);
      }
    }
    this.#words = Math.max(1, Math.ceil(columnIndexes.size / 32));
    this.#covered = new Uint32Array(this.#words);
    this.#columns = segments.map((segment) => {
      const bits = new Uint32Array(this.#words);
      for (const id of Object.keys(segment.columnBlockIds)) {
        const bit = columnIndexes.get(id) ?? 0;
        bits[bit >>> 5] = (bits[bit >>> 5] ?? 0) | (1 << (bit & 31));
      }
      return bits;
    });
    const all = new Uint32Array(this.#words);
    for (const bits of this.#columns) {
      for (let word = 0; word < this.#words; word += 1)
        all[word] = (all[word] ?? 0) | (bits[word] ?? 0);
    }
    this.fullCover = Uint8Array.from(this.#columns, (bits) => {
      for (let word = 0; word < this.#words; word += 1) {
        if (((bits[word] ?? 0) | (all[word] ?? 0)) !== (bits[word] ?? 0)) return 0;
      }
      return 1;
    });
  }

  get total(): number {
    return this.rowBase[this.segments.length] ?? 0;
  }

  /** Modeled retained bytes of the recorded block layout. */
  get bytes(): number {
    let bytes = this.rowBase.byteLength + this.fullCover.byteLength;
    for (const starts of this.blockStarts) bytes += starts.byteLength + 16;
    for (const bits of this.#columns) bytes += bits.byteLength + 16;
    return bytes;
  }

  /** Records one source block's row count; blocks arrive in order, once per source. */
  recordBlock(source: number, blockIndex: number, rows: number): void {
    if (this.#recorded[source] === 1) return;
    const starts = this.blockStarts[source];
    if (starts === undefined || blockIndex + 1 >= starts.length) {
      throw new Error("Update segment block layout mismatch");
    }
    starts[blockIndex + 1] = (starts[blockIndex] ?? 0) + rows;
    if (blockIndex + 2 === starts.length) this.#recorded[source] = 1;
  }

  /** Whether every source's block layout has been recorded. */
  get recorded(): boolean {
    for (let source = 0; source < this.segments.length; source += 1) {
      if (this.#recorded[source] !== 1 && (this.blockStarts[source]?.length ?? 0) > 1) {
        return false;
      }
    }
    return true;
  }

  /** The source a location falls in. */
  sourceOf(location: number): number {
    const bases = this.rowBase;
    let low = 0;
    let high = this.segments.length - 1;
    while (low < high) {
      const middle = (low + high + 1) >>> 1;
      if ((bases[middle] ?? 0) <= location) low = middle;
      else high = middle - 1;
    }
    return low;
  }

  /** The key block holding row `row` of source `source`. */
  blockOf(source: number, row: number): number {
    const starts = this.blockStarts[source];
    if (starts === undefined) throw new Error("Update segment block layout mismatch");
    let low = 0;
    let high = starts.length - 2;
    while (low < high) {
      const middle = (low + high + 1) >>> 1;
      if ((starts[middle] ?? 0) <= row) low = middle;
      else high = middle - 1;
    }
    return low;
  }

  /**
   * The surviving layers once `location` (from source `source`) lands on a row that carried
   * `previous` (newest last): an older layer stays only while some column it carries is not
   * carried by a newer one. A partial update must leave other columns of an earlier
   * upsert/update readable.
   */
  surviving(previous: readonly number[], source: number): number[] {
    const covered = this.#covered;
    covered.set(this.#columns[source] ?? covered);
    const kept: number[] = [];
    for (let index = previous.length - 1; index >= 0; index -= 1) {
      const layer = previous[index] ?? 0;
      const bits = this.#columns[this.sourceOf(layer)];
      if (bits === undefined) continue;
      let newColumn = false;
      for (let word = 0; word < this.#words; word += 1) {
        if (((bits[word] ?? 0) & ~(covered[word] ?? 0)) !== 0) {
          newColumn = true;
          break;
        }
      }
      if (!newColumn) continue;
      kept.push(layer);
      for (let word = 0; word < this.#words; word += 1) {
        covered[word] = (covered[word] ?? 0) | (bits[word] ?? 0);
      }
    }
    return kept.reverse();
  }
}

const BYTE_POPCOUNT = new Uint8Array(256).map((_, byte) => {
  let count = 0;
  for (let value = byte; value !== 0; value &= value - 1) count += 1;
  return count;
});

/** Set bits of `bitmap` over bit indexes `[from, to)`, a byte at a time between the edges. */
function countBits(bitmap: Uint8Array, from: number, to: number): number {
  let count = 0;
  let index = from;
  while (index < to && (index & 7) !== 0) {
    if (((bitmap[index >>> 3] ?? 0) & (1 << (index & 7))) !== 0) count += 1;
    index += 1;
  }
  while (index + 8 <= to) {
    count += BYTE_POPCOUNT[bitmap[index >>> 3] ?? 0] ?? 0;
    index += 8;
  }
  while (index < to) {
    if (((bitmap[index >>> 3] ?? 0) & (1 << (index & 7))) !== 0) count += 1;
    index += 1;
  }
  return count;
}

/** Bits per counted stride of `OverlayDeadCounts`. */
const DEAD_COUNT_STRIDE = 512;

/**
 * Range counts over the dead-row bitmap: the set bits before every 512th bit, so counting a
 * window's dead rows costs two short edges instead of a walk over the whole window. A scan of
 * small batches over a table whose deltas spread patches everywhere counts once per batch.
 */
export class OverlayDeadCounts {
  readonly #bitmap: Uint8Array;
  readonly #before: Uint32Array;

  private constructor(bitmap: Uint8Array, before: Uint32Array) {
    this.#bitmap = bitmap;
    this.#before = before;
  }

  /** Counts `bitmap`, handing the event loop a turn between bounded slices. */
  static async of(bitmap: Uint8Array): Promise<OverlayDeadCounts> {
    const strideBytes = DEAD_COUNT_STRIDE / 8;
    const strides = Math.ceil(bitmap.length / strideBytes);
    const before = new Uint32Array(strides + 1);
    let total = 0;
    for (let stride = 0; stride < strides; stride += 1) {
      before[stride] = total;
      const end = Math.min(bitmap.length, (stride + 1) * strideBytes);
      for (let byte = stride * strideBytes; byte < end; byte += 1) {
        total += BYTE_POPCOUNT[bitmap[byte] ?? 0] ?? 0;
      }
      if ((stride & 1_023) === 1_023) await maybeYieldToEventLoop();
    }
    before[strides] = total;
    return new OverlayDeadCounts(bitmap, before);
  }

  get bytes(): number {
    return this.#before.byteLength;
  }

  /** Set bits over `[from, to)`. */
  count(from: number, to: number): number {
    const first = Math.ceil(from / DEAD_COUNT_STRIDE);
    const last = Math.floor(to / DEAD_COUNT_STRIDE);
    if (last <= first) return countBits(this.#bitmap, from, to);
    return (
      countBits(this.#bitmap, from, first * DEAD_COUNT_STRIDE) +
      (this.#before[last] ?? 0) -
      (this.#before[first] ?? 0) +
      countBits(this.#bitmap, last * DEAD_COUNT_STRIDE, to)
    );
  }
}

/**
 * The scan segments' key blocks in scan order — each one's first scan slot, block id, and the
 * visible-order position of its segment — so a lazily replayed slot range can read its keys.
 */
export class OverlayScanLayout {
  readonly #starts: number[] = [0];
  readonly #blockIds: string[] = [];
  readonly #segments: number[] = [];

  get blocks(): number {
    return this.#blockIds.length;
  }

  /** Modeled retained bytes. */
  get bytes(): number {
    return 64 + this.#blockIds.length * 32;
  }

  clear(): void {
    this.#starts.length = 1;
    this.#blockIds.length = 0;
    this.#segments.length = 0;
  }

  push(blockId: string, rows: number, segment: number): void {
    this.#blockIds.push(blockId);
    this.#segments.push(segment);
    this.#starts.push((this.#starts[this.#starts.length - 1] ?? 0) + rows);
  }

  /** The first block whose rows reach past `slot`. */
  blockAt(slot: number): number {
    let low = 0;
    let high = this.#blockIds.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if ((this.#starts[middle + 1] ?? 0) <= slot) low = middle + 1;
      else high = middle;
    }
    return low;
  }

  start(block: number): number {
    return this.#starts[block] ?? 0;
  }

  end(block: number): number {
    return this.#starts[block + 1] ?? 0;
  }

  blockId(block: number): string {
    const id = this.#blockIds[block];
    if (id === undefined) throw new Error("Streamed scan block layout mismatch");
    return id;
  }

  segment(block: number): number {
    return this.#segments[block] ?? 0;
  }
}

/**
 * Patches sorted by slot: the newest location of each patched slot, and for the rare slot a
 * partial update left more than one layer on, its older surviving layers (oldest first).
 */
export interface OverlayPatchList {
  readonly slots: Uint32Array;
  readonly locations: Uint32Array;
  readonly older: ReadonlyMap<number, readonly number[]> | undefined;
}

/** The patches of one slot range of a list, by reference. */
export function overlayPatchRange(
  list: OverlayPatchList,
  from: number,
  to: number,
): OverlayPatchList {
  const low = lowerBound(list.slots, from);
  const high = lowerBound(list.slots, to);
  if (low === 0 && high === list.slots.length) return list;
  return {
    slots: list.slots.subarray(low, high),
    locations: list.locations.subarray(low, high),
    older: list.older,
  };
}

function lowerBound(sorted: Uint32Array, value: number): number {
  let low = 0;
  let high = sorted.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((sorted[middle] ?? 0) < value) low = middle + 1;
    else high = middle;
  }
  return low;
}

/** Per-key replay state shared by the full replay and the per-range replay. */
abstract class OverlayKeyState {
  protected readonly numbers: NumberKeyIndex | undefined;
  protected readonly map: Map<OverlayKey, number> | undefined;
  protected readonly charge: OverlayCharge;
  protected readonly sources: OverlayPatchSources;
  /** Per entry: its live slot, or OVERLAY_NONE while absent or deleted. */
  protected slot: Uint32Array = new Uint32Array(0);
  /** Per entry: its newest patch location, or OVERLAY_NONE. */
  protected location: Uint32Array = new Uint32Array(0);
  /** Per entry with more than one surviving layer: the older ones, oldest first. */
  protected older: Map<number, number[]> | undefined;
  #olderBytes = 0;
  #olderCharged = 0;

  constructor(numeric: boolean, sources: OverlayPatchSources, charge: OverlayCharge) {
    this.charge = charge;
    this.sources = sources;
    if (numeric) this.numbers = new NumberKeyIndex(charge);
    else this.map = new Map();
  }

  get size(): number {
    return this.numbers?.size ?? this.map?.size ?? 0;
  }

  protected entryCapacity(entries: number): void {
    if (entries <= this.slot.length) return;
    this.slot = grownUint32(this.slot, entries, this.charge);
    this.location = grownUint32(this.location, entries, this.charge);
  }

  protected addKey(key: OverlayKey): number {
    let entry: number;
    if (this.numbers !== undefined) {
      if (typeof key !== "number") throw new TypeError("Invalid number unique key");
      entry = this.numbers.add(key, overlayNumberHash(key));
    } else {
      const map = this.map ?? new Map<OverlayKey, number>();
      const existing = map.get(key);
      if (existing !== undefined) return existing;
      this.charge(MAP_INDEX_ENTRY_BYTES + (typeof key === "string" ? key.length * 2 : 0));
      entry = map.size;
      map.set(key, entry);
    }
    this.entryCapacity(entry + 1);
    return entry;
  }

  protected findKey(key: OverlayKey): number {
    if (this.numbers !== undefined) {
      return typeof key === "number" ? this.numbers.find(key, overlayNumberHash(key)) : -1;
    }
    return this.map?.get(key) ?? -1;
  }

  /** Lands a delta row (`location`, from `source`) on entry `entry`'s live slot. */
  protected layer(entry: number, location: number, source: number): void {
    const previous = this.location[entry] ?? OVERLAY_NONE;
    this.location[entry] = location;
    if (previous === OVERLAY_NONE) return;
    const older = this.older?.get(entry);
    if (this.sources.fullCover[source] === 1) {
      if (older !== undefined) this.dropOlder(entry, older);
      return;
    }
    const kept = this.sources.surviving(
      older === undefined ? [previous] : [...older, previous],
      source,
    );
    if (older !== undefined) this.dropOlder(entry, older);
    if (kept.length === 0) return;
    this.older ??= new Map();
    this.older.set(entry, kept);
    this.#olderBytes += OLDER_ENTRY_BYTES + kept.length * OLDER_LAYER_BYTES;
    // Charge the high-water mark: superseded layers come and go, and scratch is never returned
    // mid-pass, so charging each change would count the same bytes over and over.
    if (this.#olderBytes > this.#olderCharged) {
      this.charge(this.#olderBytes - this.#olderCharged);
      this.#olderCharged = this.#olderBytes;
    }
  }

  protected dropOlder(entry: number, older: readonly number[]): void {
    this.older?.delete(entry);
    this.#olderBytes -= OLDER_ENTRY_BYTES + older.length * OLDER_LAYER_BYTES;
  }
}

/**
 * Receives each pass's surviving patches and keeps them, to be sorted by slot as the retained
 * replay, while their modeled bytes stay within `limitBytes`. Growth is charged through
 * `reserve`, which returns the release of that charge.
 */
export class OverlayPatchCollector {
  #slots = new Uint32Array(0);
  #locations = new Uint32Array(0);
  #length = 0;
  #older: Map<number, readonly number[]> | undefined;
  #olderBytes = 0;
  readonly #limitBytes: number;
  readonly #reserve: (bytes: number) => () => void;
  #release: () => void = () => undefined;
  #reservedBytes = 0;

  constructor(limitBytes: number, reserve: (bytes: number) => () => void) {
    this.#limitBytes = limitBytes;
    this.#reserve = reserve;
  }

  get length(): number {
    return this.#length;
  }

  /** Modeled retained bytes of the kept patches. */
  get bytes(): number {
    return this.#length * OVERLAY_PATCH_BYTES + this.#olderBytes;
  }

  /** Keeps one patch; false, keeping nothing, once the patches would pass the limit. */
  push(slot: number, location: number, older: readonly number[] | undefined): boolean {
    const olderBytes =
      older === undefined ? 0 : OLDER_ENTRY_BYTES + older.length * OLDER_LAYER_BYTES;
    if (this.bytes + OVERLAY_PATCH_BYTES + olderBytes > this.#limitBytes) return false;
    if (this.#length === this.#slots.length) this.#grow(this.#length + 1);
    this.#slots[this.#length] = slot;
    this.#locations[this.#length] = location;
    this.#length += 1;
    if (older !== undefined) {
      this.#older ??= new Map();
      this.#older.set(slot, older);
      this.#olderBytes += olderBytes;
      this.#charge();
    }
    return true;
  }

  #grow(length: number): void {
    let capacity = Math.max(1_024, this.#slots.length * 2);
    while (capacity < length) capacity *= 2;
    // Never reserve past what the limit lets the patches reach.
    capacity = Math.max(
      length,
      Math.min(capacity, Math.floor(this.#limitBytes / OVERLAY_PATCH_BYTES)),
    );
    const bytes = capacity * OVERLAY_PATCH_BYTES + this.#olderBytes;
    const release = this.#reserve(bytes);
    this.#release();
    this.#release = release;
    this.#reservedBytes = bytes;
    const slots = new Uint32Array(capacity);
    slots.set(this.#slots.subarray(0, this.#length));
    const locations = new Uint32Array(capacity);
    locations.set(this.#locations.subarray(0, this.#length));
    this.#slots = slots;
    this.#locations = locations;
  }

  #charge(): void {
    const bytes = this.#slots.length * OVERLAY_PATCH_BYTES + this.#olderBytes;
    if (bytes <= this.#reservedBytes) return;
    const release = this.#reserve(bytes);
    this.#release();
    this.#release = release;
    this.#reservedBytes = bytes;
  }

  /** Drops every kept patch and its charge. */
  clear(): void {
    this.#release();
    this.#release = () => undefined;
    this.#reservedBytes = 0;
    this.#slots = new Uint32Array(0);
    this.#locations = new Uint32Array(0);
    this.#length = 0;
    this.#older = undefined;
    this.#olderBytes = 0;
  }

  /**
   * The kept patches sorted by slot, in arrays of exactly their length. The sort is an LSD
   * radix sort on the slot, eleven bits a pass, with the location carried along and a turn for
   * the event loop between passes; a pass whose digit every slot shares is skipped, and
   * patches already in slot order (a delta written in the table's order) skip the sort. Its
   * scratch — the scatter targets — is charged through `charge`.
   */
  async finish(charge: OverlayCharge): Promise<OverlayPatchList> {
    const length = this.#length;
    let slots: Uint32Array = this.#slots.subarray(0, length);
    let locations: Uint32Array = this.#locations.subarray(0, length);
    let sorted = true;
    for (let index = 1; index < length; index += 1) {
      if ((slots[index - 1] ?? 0) > (slots[index] ?? 0)) {
        sorted = false;
        break;
      }
    }
    if (sorted) {
      // Trimmed copies, so the retained replay holds its patches and not the growth slack.
      charge(length * OVERLAY_PATCH_BYTES);
      return { slots: slots.slice(), locations: locations.slice(), older: this.#older };
    }
    const radix = 2_048;
    charge(length * OVERLAY_PATCH_BYTES * 2 + radix * Uint32Array.BYTES_PER_ELEMENT);
    // Scatter targets: the first scatter leaves the collector's arrays behind, so a later one
    // needs a second exact-length pair; after that the two pairs alternate.
    let targetSlots: Uint32Array | undefined;
    let targetLocations: Uint32Array | undefined;
    let fromCollector = true;
    const counts = new Uint32Array(radix);
    // Counting and scattering both run in slices, with a chance to yield between them.
    const slice = 65_536;
    for (const shift of [0, 11, 22]) {
      counts.fill(0);
      for (let from = 0; from < length; from += slice) {
        await maybeYieldToEventLoop();
        const to = Math.min(length, from + slice);
        for (let index = from; index < to; index += 1) {
          const digit = ((slots[index] ?? 0) >>> shift) & (radix - 1);
          counts[digit] = (counts[digit] ?? 0) + 1;
        }
      }
      if (counts[((slots[0] ?? 0) >>> shift) & (radix - 1)] === length) continue;
      let total = 0;
      for (let digit = 0; digit < radix; digit += 1) {
        const count = counts[digit] ?? 0;
        counts[digit] = total;
        total += count;
      }
      const nextSlots = targetSlots ?? new Uint32Array(length);
      const nextLocations = targetLocations ?? new Uint32Array(length);
      for (let from = 0; from < length; from += slice) {
        await maybeYieldToEventLoop();
        const to = Math.min(length, from + slice);
        for (let index = from; index < to; index += 1) {
          const slot = slots[index] ?? 0;
          const digit = (slot >>> shift) & (radix - 1);
          const position = counts[digit] ?? 0;
          counts[digit] = position + 1;
          nextSlots[position] = slot;
          nextLocations[position] = locations[index] ?? 0;
        }
      }
      targetSlots = fromCollector ? undefined : slots;
      targetLocations = fromCollector ? undefined : locations;
      fromCollector = false;
      slots = nextSlots;
      locations = nextLocations;
    }
    if (fromCollector)
      return { slots: slots.slice(), locations: locations.slice(), older: this.#older };
    return { slots, locations, older: this.#older };
  }
}

/**
 * One partition of the full replay: every key the deltas name whose hash falls in `partition`
 * of `partitions`. Equal keys share a partition and each partition walks the whole history in
 * order, so a key's slots, deaths, and patches come out exactly as a single pass makes them.
 *
 * Collect first (`collect` on every delta block), then replay every block in visible order
 * (`replay`). A row whose key the deltas never name cannot be dead or patched and is skipped.
 */
export class OverlayReplay extends OverlayKeyState {
  readonly #dead: Uint8Array;
  readonly #partition: number;
  readonly #partitions: number;
  readonly #zonePruned: boolean;
  readonly #tableName: string;
  deadCount = 0;
  #minimum = Number.POSITIVE_INFINITY;
  #maximum = Number.NEGATIVE_INFINITY;

  constructor(options: {
    readonly numeric: boolean;
    readonly sources: OverlayPatchSources;
    readonly dead: Uint8Array;
    readonly partition: number;
    readonly partitions: number;
    readonly zonePruned: boolean;
    readonly tableName: string;
    readonly charge: OverlayCharge;
  }) {
    super(options.numeric, options.sources, options.charge);
    this.#dead = options.dead;
    this.#partition = options.partition;
    this.#partitions = options.partitions;
    this.#zonePruned = options.zonePruned;
    this.#tableName = options.tableName;
  }

  /** The smallest and largest collected number key, for zone pruning. */
  get keyRange(): { minimum: number; maximum: number } {
    return { minimum: this.#minimum, maximum: this.#maximum };
  }

  /** Every collected number key, sorted, for an IN zone predicate. */
  sortedKeys(): Float64Array | undefined {
    const keys = this.numbers?.keys();
    keys?.sort();
    return keys;
  }

  /** Registers the keys of one delta block that fall in this partition. */
  collect(vector: ColumnVector, rows: number): void {
    const values = this.numbers === undefined ? undefined : numberKeyValues(vector);
    if (values !== undefined && this.numbers !== undefined) {
      const numbers = this.numbers;
      const validity = vector.validity;
      const partitions = this.#partitions;
      for (let row = 0; row < rows; row += 1) {
        assertKeyPresent(validity, row);
        const key = values[row] ?? 0;
        const hash = overlayNumberHash(key);
        if (partitions !== 1 && overlayPartitionOf(hash, partitions) !== this.#partition) continue;
        const before = numbers.size;
        const entry = numbers.add(key, hash);
        if (numbers.size === before) continue;
        if (key < this.#minimum) this.#minimum = key;
        if (key > this.#maximum) this.#maximum = key;
        this.entryCapacity(entry + 1);
        this.slot[entry] = OVERLAY_NONE;
        this.location[entry] = OVERLAY_NONE;
      }
      return;
    }
    const read = keyVectorReader(vector);
    for (let row = 0; row < rows; row += 1) {
      const key = read(row);
      if (
        this.#partitions !== 1 &&
        overlayPartitionOf(overlayKeyHash(key), this.#partitions) !== this.#partition
      ) {
        continue;
      }
      const before = this.size;
      const entry = this.addKey(key);
      if (this.size === before) continue;
      this.slot[entry] = OVERLAY_NONE;
      this.location[entry] = OVERLAY_NONE;
    }
  }

  /**
   * Replays one key block. `baseSlot` is the scan position of a scan segment's first row in the
   * block; `source` and `locationStart` locate an update/upsert block's first row as a patch
   * (-1 for other segments).
   */
  replay(
    kind: SegmentRecord["kind"],
    vector: ColumnVector,
    rows: number,
    baseSlot: number,
    source: number,
    locationStart: number,
    segmentId: string,
  ): void {
    if (this.size === 0) return;
    const values = this.numbers === undefined ? undefined : numberKeyValues(vector);
    const read = values === undefined ? keyVectorReader(vector) : undefined;
    const numbers = this.numbers;
    const validity = vector.validity;
    const scans = kind === "insert" || kind === "base" || kind === "upsert";
    for (let row = 0; row < rows; row += 1) {
      let entry: number;
      if (values !== undefined && numbers !== undefined) {
        assertKeyPresent(validity, row);
        const key = values[row] ?? 0;
        entry = numbers.find(key, overlayNumberHash(key));
      } else {
        entry = this.findKey(read?.(row) ?? false);
      }
      if (entry < 0) continue;
      const slot = this.slot[entry] ?? OVERLAY_NONE;
      if (scans) {
        if (slot === OVERLAY_NONE) {
          this.slot[entry] = baseSlot + row;
        } else if (kind === "upsert") {
          this.layer(entry, locationStart + row, source);
          this.#markDead(baseSlot + row);
        } else {
          throw new Error(`Stored table contains a duplicate unique key: ${this.#tableName}`);
        }
      } else if (kind === "delete") {
        if (slot !== OVERLAY_NONE) {
          this.#markDead(slot);
          const older = this.older?.get(entry);
          if (older !== undefined) this.dropOlder(entry, older);
          this.location[entry] = OVERLAY_NONE;
          this.slot[entry] = OVERLAY_NONE;
        }
      } else if (slot !== OVERLAY_NONE) {
        this.layer(entry, locationStart + row, source);
      } else if (!this.#zonePruned) {
        throw new Error(`Update segment references a missing key: ${segmentId}`);
      }
    }
  }

  #markDead(slot: number): void {
    const byte = slot >>> 3;
    this.#dead[byte] = (this.#dead[byte] ?? 0) | (1 << (slot & 7));
    this.deadCount += 1;
  }

  /** Live slots carrying at least one patch, and what keeping their patches would cost. */
  patchStats(): { count: number; bytes: number } {
    let count = 0;
    let bytes = 0;
    for (let entry = 0; entry < this.size; entry += 1) {
      if (this.slot[entry] === OVERLAY_NONE || this.location[entry] === OVERLAY_NONE) continue;
      count += 1;
      bytes += OVERLAY_PATCH_BYTES;
      const older = this.older?.get(entry);
      if (older !== undefined) bytes += OLDER_ENTRY_BYTES + older.length * OLDER_LAYER_BYTES;
    }
    return { count, bytes };
  }

  /** Hands every live patched slot to `collector`; false once the collector is full. */
  emit(collector: OverlayPatchCollector): boolean {
    for (let entry = 0; entry < this.size; entry += 1) {
      const slot = this.slot[entry] ?? OVERLAY_NONE;
      const location = this.location[entry] ?? OVERLAY_NONE;
      if (slot === OVERLAY_NONE || location === OVERLAY_NONE) continue;
      if (!collector.push(slot, location, this.older?.get(entry))) return false;
    }
    return true;
  }
}

/**
 * The patches of one range of scan slots, replayed on demand when the retained replay would not
 * fit the query budget. The full replay already fixed which rows are dead, so only the range's
 * live rows matter, and each of them is its key's one live slot: the patches it carries are the
 * update/upsert rows naming its key in segments after its own, in visible order. Deletes need no
 * replay here — a delete after a row was written would have made it dead.
 *
 * Add the range's live rows in slot order (`addLive`), replay every update/upsert block in
 * visible order (`replay`), then `emit` the patches, already in slot order.
 */
export class OverlayRangeReplay extends OverlayKeyState {
  /** Per entry: the visible-order position of the segment holding its slot. */
  #segment: Uint32Array = new Uint32Array(0);
  readonly #tableName: string;
  #minimum = Number.POSITIVE_INFINITY;
  #maximum = Number.NEGATIVE_INFINITY;

  constructor(
    numeric: boolean,
    sources: OverlayPatchSources,
    tableName: string,
    charge: OverlayCharge,
  ) {
    super(numeric, sources, charge);
    this.#tableName = tableName;
  }

  get keyRange(): { minimum: number; maximum: number } {
    return { minimum: this.#minimum, maximum: this.#maximum };
  }

  /**
   * Adds the live rows `[from, to)` of one scan key block, whose row `from` is scan slot
   * `slotStart`, in the segment at visible position `segment`. Rows set in `dead` are skipped.
   */
  addLive(
    vector: ColumnVector,
    from: number,
    to: number,
    slotStart: number,
    segment: number,
    dead: Uint8Array,
  ): void {
    const values = this.numbers === undefined ? undefined : numberKeyValues(vector);
    const read = values === undefined ? keyVectorReader(vector) : undefined;
    const validity = vector.validity;
    for (let row = from; row < to; row += 1) {
      const slot = slotStart + row - from;
      if (((dead[slot >>> 3] ?? 0) & (1 << (slot & 7))) !== 0) continue;
      let key: OverlayKey;
      if (values !== undefined) {
        assertKeyPresent(validity, row);
        key = values[row] ?? 0;
        if (key < this.#minimum) this.#minimum = key;
        if (key > this.#maximum) this.#maximum = key;
      } else {
        key = read?.(row) ?? false;
      }
      const before = this.size;
      const entry = this.addKey(key);
      if (this.size === before) {
        throw new Error(`Stored table contains a duplicate unique key: ${this.#tableName}`);
      }
      if (entry >= this.#segment.length) {
        this.#segment = grownUint32(this.#segment, this.slot.length, this.charge);
      }
      this.slot[entry] = slot;
      this.location[entry] = OVERLAY_NONE;
      this.#segment[entry] = segment;
    }
  }

  /** Replays one key block of update/upsert source `source`, at visible position `segment`. */
  replay(
    vector: ColumnVector,
    rows: number,
    source: number,
    locationStart: number,
    segment: number,
  ): void {
    if (this.size === 0) return;
    const values = this.numbers === undefined ? undefined : numberKeyValues(vector);
    const read = values === undefined ? keyVectorReader(vector) : undefined;
    const numbers = this.numbers;
    const validity = vector.validity;
    for (let row = 0; row < rows; row += 1) {
      let entry: number;
      if (values !== undefined && numbers !== undefined) {
        assertKeyPresent(validity, row);
        const key = values[row] ?? 0;
        entry = numbers.find(key, overlayNumberHash(key));
      } else {
        entry = this.findKey(read?.(row) ?? false);
      }
      if (entry < 0 || (this.#segment[entry] ?? 0) >= segment) continue;
      this.layer(entry, locationStart + row, source);
    }
  }

  /** The range's patches, in slot order. */
  emit(): OverlayPatchList {
    let count = 0;
    for (let entry = 0; entry < this.size; entry += 1) {
      if (this.location[entry] !== OVERLAY_NONE) count += 1;
    }
    this.charge(count * OVERLAY_PATCH_BYTES);
    const slots = new Uint32Array(count);
    const locations = new Uint32Array(count);
    let older: Map<number, readonly number[]> | undefined;
    let next = 0;
    for (let entry = 0; entry < this.size; entry += 1) {
      const location = this.location[entry] ?? OVERLAY_NONE;
      if (location === OVERLAY_NONE) continue;
      const slot = this.slot[entry] ?? 0;
      slots[next] = slot;
      locations[next] = location;
      next += 1;
      const layers = this.older?.get(entry);
      if (layers !== undefined) {
        older ??= new Map();
        older.set(slot, layers);
      }
    }
    return { slots, locations, older };
  }
}
