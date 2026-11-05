/**
 * The hash join's table over the rows of a `ColumnStore` (track B7).
 *
 * ## Shapes (chosen at build from the exact build size, design-joins-agg-spill.md "Tables")
 * - **Direct**: one integer key column whose non-null values span at most
 *   `max(1,024, 4 × rows)` (and ≤ 2^26) values: `heads[key − min]`. No hashing, one read.
 * - **Inline words**: open addressing, linear probing, ≤ 70 % load. A slot is `[head, w0 …]`:
 *   the key's canonical words (join-keys.ts) sit next to the head, so a probe touches one cache
 *   line. Capacity is a power of two.
 * - **Bytes**: a slot is `[head, hash]`; a hash match compares the key bytes (the build column's
 *   own UTF-8 bytes for one text key, else the equality encoding kept per chunk).
 * - `head` is build row + 1 (0 = empty). Equal keys chain through `next` (allocated on the first
 *   duplicate), so `unique` tells the probe it can skip chains.
 *
 * Slots use the bits of `levelHash(h, level)` after its top four, which route partitions, so a
 * table over some partitions of one level is still evenly loaded.
 *
 * ## Memory
 * Every array is allocated from the charge before it exists; `build` returns null when refused,
 * leaving nothing allocated, and the join spills instead (grant-or-spill).
 */
import type { ByteCharge } from "../vector/varlen.js";
import type { Vector } from "../vector/vector.js";
import { KeyBatch } from "../keys/key-encoding.js";
import { KeyScratch, bytesEqual, extractKeys, levelHash, type KeyLayout } from "./join-keys.js";
import type { ColumnStore } from "./column-store.js";

const MAX_LOAD = 0.7;
const DIRECT_LIMIT = 1 << 26;
/** Tables larger than this get the touch pass (overlaps cache misses across a batch). */
const TOUCH_BYTES = 1 << 20;

interface EncodedChunk {
  readonly bytes: Uint8Array;
  readonly offsets: Int32Array;
}

export class JoinTable {
  readonly kind: "direct" | "words" | "bytes";
  readonly rows: number;
  /** Rows inserted (rows with a null `=` key are not). */
  inserted = 0;
  unique = true;
  /** next[row] = the next row with the same key, + 1 (0 = end). */
  next: Int32Array | null = null;
  retainedBytes = 0;
  readonly #layout: KeyLayout;
  readonly #level: number;
  readonly #charge: ByteCharge;
  readonly #store: ColumnStore;
  readonly #keyColumns: readonly number[];
  // Open addressing.
  #slots = new Int32Array(0);
  #mask = 0;
  #shift = 32;
  #stride = 1;
  // Direct addressing.
  #heads = new Int32Array(0);
  #min = 0;
  #range = 0;
  // Bytes family: encoded keys per store chunk (enc), or the raw key column (raw).
  #encoded: EncodedChunk[] = [];
  #touch = false;
  readonly #slotScratch: Int32Array;

  private constructor(
    kind: "direct" | "words" | "bytes",
    store: ColumnStore,
    keyColumns: readonly number[],
    layout: KeyLayout,
    level: number,
    charge: ByteCharge,
    scratchRows: number,
  ) {
    this.kind = kind;
    this.rows = store.rows;
    this.#store = store;
    this.#keyColumns = keyColumns;
    this.#layout = layout;
    this.#level = level;
    this.#charge = charge;
    this.#slotScratch = new Int32Array(scratchRows);
  }

  #allocate(bytes: number): boolean {
    if (!this.#charge.allocate(bytes)) return false;
    this.retainedBytes += bytes;
    return true;
  }

  /** Bytes a table over `rows` needs at most (words / bytes shapes), for planning a grant. */
  static estimateBytes(layout: KeyLayout, rows: number): number {
    let capacity = 2;
    while (capacity * MAX_LOAD < rows) capacity *= 2;
    const stride = layout.family === "words" ? 1 + layout.width : 2;
    return capacity * stride * 4 + rows * 4;
  }

  /**
   * Builds the table over every row of `store` (key columns `keyColumns`), or returns null with
   * nothing allocated when the charge refuses. `scratch` is the caller's key scratch.
   */
  static build(
    store: ColumnStore,
    keyColumns: readonly number[],
    layout: KeyLayout,
    level: number,
    charge: ByteCharge,
    scratch: KeyScratch,
    options: { readonly direct?: boolean } = {},
  ): JoinTable | null {
    const rows = store.rows;
    if (options.direct !== false && layout.directable && level === 0) {
      const direct = JoinTable.#tryDirect(store, keyColumns, layout, charge, scratch);
      if (direct !== undefined) return direct;
    }
    const kind = layout.family === "words" ? "words" : "bytes";
    const table = new JoinTable(kind, store, keyColumns, layout, level, charge, scratch.capacity);
    let capacity = 2;
    while (capacity * MAX_LOAD < Math.max(1, rows)) capacity *= 2;
    table.#stride = kind === "words" ? 1 + layout.width : 2;
    if (!table.#allocate(capacity * table.#stride * 4)) return null;
    table.#slots = new Int32Array(capacity * table.#stride);
    table.#mask = capacity - 1;
    table.#shift = 32 - Math.log2(capacity);
    table.#touch = capacity * table.#stride * 4 > TOUCH_BYTES;
    if (!table.#insertAll(scratch)) {
      table.close();
      return null;
    }
    return table;
  }

  /** Direct addressing when the key range is dense enough, null when refused, else undefined. */
  static #tryDirect(
    store: ColumnStore,
    keyColumns: readonly number[],
    layout: KeyLayout,
    charge: ByteCharge,
    scratch: KeyScratch,
  ): JoinTable | null | undefined {
    const rows = store.rows;
    const wide = layout.parts[0] === "w2i";
    let min = Infinity;
    let max = -Infinity;
    // Stored chunks are flat: read int32 / safe int64 values directly when the column allows.
    const column = keyColumns[0] ?? 0;
    const fast = store.tier(column) === 0 && !store.hasNulls(column);
    if (fast)
      for (let k = 0; k < store.chunkCount; k++) {
        const vector = store.chunk(column, k);
        const values = directValues(vector);
        if (values === null) break;
        for (let i = 0; i < vector.length; i++) {
          const value = values[i] ?? 0;
          if (value < min) min = value;
          if (value > max) max = value;
        }
      }
    else
      for (let k = 0; k < store.chunkCount; k++) {
        const count = extractKeys(
          chunkKeys(store, keyColumns, k),
          null,
          0,
          store.chunk(0, k).length,
          scratch,
          false,
        );
        for (let i = 0; i < count; i++) {
          if ((scratch.nulls[i] ?? 0) !== 0) continue;
          const value = wide
            ? (scratch.words[i * 2 + 1] ?? 0) * 4_294_967_296 + ((scratch.words[i * 2] ?? 0) >>> 0)
            : (scratch.words[i] ?? 0);
          if (value < min) min = value;
          if (value > max) max = value;
        }
      }
    if (min === Infinity) min = max = 0;
    const range = max - min + 1;
    if (!(range <= Math.max(1024, 4 * rows) && range <= DIRECT_LIMIT)) return undefined;
    if (
      wide &&
      !(Math.abs(min) <= Number.MAX_SAFE_INTEGER && Math.abs(max) <= Number.MAX_SAFE_INTEGER)
    )
      return undefined;
    const table = new JoinTable("direct", store, keyColumns, layout, 0, charge, scratch.capacity);
    if (!table.#allocate(range * 4)) return null;
    table.#heads = new Int32Array(range);
    table.#min = min;
    table.#range = range;
    if (!table.#insertAll(scratch)) {
      table.close();
      return null;
    }
    return table;
  }

  #insertAll(scratch: KeyScratch): boolean {
    const store = this.#store;
    const column = this.#keyColumns[0] ?? 0;
    if (this.kind === "direct" && store.tier(column) === 0 && !store.hasNulls(column)) {
      const heads = this.#heads;
      const min = this.#min;
      for (let k = 0; k < store.chunkCount; k++) {
        const vector = store.chunk(column, k);
        const values = directValues(vector);
        if (values === null) break;
        const base = k * store.chunkRows;
        for (let i = 0; i < vector.length; i++) {
          const index = (values[i] ?? 0) - min;
          const head = heads[index] ?? 0;
          if (head !== 0 && !this.#linkDuplicate(base + i, head)) return false;
          heads[index] = base + i + 1;
        }
        this.inserted += vector.length;
        if (k === store.chunkCount - 1) return true;
      }
      if (store.chunkCount === 0) return true;
    }
    for (let k = 0; k < store.chunkCount; k++) {
      const count = store.chunk(0, k).length;
      const keys = chunkKeys(store, this.#keyColumns, k);
      const done = extractKeys(keys, null, 0, count, scratch, this.kind !== "direct");
      if (done < count) return false;
      if (this.#layout.family === "enc" && !this.#keepEncoded(scratch.encoded, count)) return false;
      if (!this.#insertChunk(scratch, k * store.chunkRows, count)) return false;
    }
    return true;
  }

  #keepEncoded(encoded: KeyBatch | null, count: number): boolean {
    if (encoded === null) return false;
    const end = encoded.offsets[count] ?? 0;
    if (!this.#allocate(end + (count + 1) * 4)) return false;
    this.#encoded.push({
      bytes: encoded.writer.bytes.slice(0, end),
      offsets: encoded.offsets.slice(0, count + 1),
    });
    return true;
  }

  #linkDuplicate(row: number, head: number): boolean {
    if (this.next === null) {
      if (!this.#allocate(this.rows * 4)) return false;
      this.next = new Int32Array(this.rows);
    }
    this.next[row] = head;
    this.unique = false;
    return true;
  }

  #insertChunk(scratch: KeyScratch, base: number, count: number): boolean {
    const strict = ~this.#layout.nullSafeMask;
    const nulls = scratch.nulls;
    const words = scratch.words;
    if (this.kind === "direct") {
      const heads = this.#heads;
      const wide = this.#layout.parts[0] === "w2i";
      for (let i = 0; i < count; i++) {
        if (((nulls[i] ?? 0) & strict) !== 0) continue;
        const value = wide
          ? (words[i * 2 + 1] ?? 0) * 4_294_967_296 + ((words[i * 2] ?? 0) >>> 0)
          : (words[i] ?? 0);
        const index = value - this.#min;
        const row = base + i;
        const head = heads[index] ?? 0;
        if (head !== 0 && !this.#linkDuplicate(row, head)) return false;
        heads[index] = row + 1;
        this.inserted++;
      }
      return true;
    }
    const slots = this.#slots;
    const mask = this.#mask;
    const shift = this.#shift;
    const stride = this.#stride;
    const level = this.#level;
    const hashes = scratch.hashes;
    if (this.kind === "words") {
      const width = this.#layout.width;
      for (let i = 0; i < count; i++) {
        if (((nulls[i] ?? 0) & strict) !== 0) continue;
        const h = levelHash(hashes[i] ?? 0, level);
        let s = ((h << 4) >>> shift) & mask;
        const at = i * width;
        for (;;) {
          const slot = s * stride;
          const head = slots[slot] ?? 0;
          if (head === 0) {
            slots[slot] = base + i + 1;
            for (let w = 0; w < width; w++) slots[slot + 1 + w] = words[at + w] ?? 0;
            break;
          }
          let same = true;
          for (let w = 0; w < width && same; w++) same = slots[slot + 1 + w] === words[at + w];
          if (same) {
            if (!this.#linkDuplicate(base + i, head)) return false;
            slots[slot] = base + i + 1;
            break;
          }
          s = (s + 1) & mask;
        }
        this.inserted++;
      }
      return true;
    }
    for (let i = 0; i < count; i++) {
      if (((nulls[i] ?? 0) & strict) !== 0) continue;
      const hash = hashes[i] ?? 0;
      const h = levelHash(hash, level);
      let s = ((h << 4) >>> shift) & mask;
      const row = base + i;
      for (;;) {
        const slot = s * 2;
        const head = slots[slot] ?? 0;
        if (head === 0) {
          slots[slot] = row + 1;
          slots[slot + 1] = hash;
          break;
        }
        if ((slots[slot + 1] ?? 0) >>> 0 === hash && this.#sameBuildKeys(head - 1, row)) {
          if (!this.#linkDuplicate(row, head)) return false;
          slots[slot] = row + 1;
          break;
        }
        s = (s + 1) & mask;
      }
      this.inserted++;
    }
    return true;
  }

  /** Range of the last `#buildBytes` result (fields, so a comparison allocates nothing). */
  #keyStart = 0;
  #keyEnd = 0;
  static readonly #empty = new Uint8Array(0);

  /** Key bytes of build row `row`; the range is `#keyStart … #keyEnd`. */
  #buildBytes(row: number): Uint8Array {
    const store = this.#store;
    const chunk = row >>> store.chunkShift;
    const slot = row & store.chunkMask;
    if (this.#layout.family === "raw") {
      const offsets = this.#rawOffsets[chunk] ?? this.#cacheRaw(chunk);
      this.#keyStart = offsets[slot] ?? 0;
      this.#keyEnd = offsets[slot + 1] ?? 0;
      return this.#rawBytes[chunk] ?? JoinTable.#empty;
    }
    const encoded = this.#encoded[chunk];
    if (encoded === undefined) {
      this.#keyStart = this.#keyEnd = 0;
      return JoinTable.#empty;
    }
    this.#keyStart = encoded.offsets[slot] ?? 0;
    this.#keyEnd = encoded.offsets[slot + 1] ?? 0;
    return encoded.bytes;
  }

  /** Raw key arrays per store chunk (the store is immutable once its table exists). */
  #rawOffsets: Int32Array[] = [];
  #rawBytes: Uint8Array[] = [];
  #cacheRaw(chunk: number): Int32Array {
    const data = this.#store.chunk(this.#keyColumns[0] ?? 0, chunk).data as {
      bytes: Uint8Array;
      offsets: Int32Array;
    };
    this.#rawOffsets[chunk] = data.offsets;
    this.#rawBytes[chunk] = data.bytes;
    return data.offsets;
  }

  #sameBuildKeys(a: number, b: number): boolean {
    const x = this.#buildBytes(a);
    const xs = this.#keyStart;
    const xe = this.#keyEnd;
    const y = this.#buildBytes(b);
    return bytesEqual(x, xs, xe, y, this.#keyStart, this.#keyEnd);
  }

  /**
   * First matching build row + 1 (0: none) of each of the `count` keys in `scratch`, into
   * `heads[0 … count)`. Returns how many keys matched.
   */
  /**
   * Direct tables over a flat int32 or safe int64 key vector: first matches of rows
   * `rows[0 … count)` read straight from the vector (no canonical words). Returns the match
   * count, or −1 when this shape does not apply (the caller extracts keys and calls `lookup`).
   */
  lookupVector(vector: Vector, rows: Uint16Array, count: number, heads: Int32Array): number {
    if (this.kind !== "direct") return -1;
    const values = directValues(vector);
    if (values === null) return -1;
    const table = this.#heads;
    const min = this.#min;
    const range = this.#range;
    const validity = vector.form === "flat" ? vector.validity : null;
    let matched = 0;
    if (values instanceof Int32Array) {
      for (let k = 0; k < count; k++) {
        const row = rows[k] ?? 0;
        const index = (values[row] ?? 0) - min;
        let head = index >= 0 && index < range ? (table[index] ?? 0) : 0;
        if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) head = 0;
        heads[k] = head;
        if (head !== 0) matched++;
      }
    } else {
      for (let k = 0; k < count; k++) {
        const row = rows[k] ?? 0;
        const index = (values[row] ?? 0) - min;
        let head = index >= 0 && index < range ? (table[index] ?? 0) : 0;
        if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) head = 0;
        heads[k] = head;
        if (head !== 0) matched++;
      }
    }
    return matched;
  }

  /** Whether `lookup` reads the scratch's hashes (a direct table does not). */
  get hashed(): boolean {
    return this.kind !== "direct";
  }

  lookup(scratch: KeyScratch, count: number, heads: Int32Array): number {
    const strict = ~this.#layout.nullSafeMask;
    const nulls = scratch.nulls;
    const words = scratch.words;
    let matched = 0;
    if (this.kind === "direct") {
      const table = this.#heads;
      const min = this.#min;
      const range = this.#range;
      if (this.#layout.parts[0] === "w1") {
        for (let k = 0; k < count; k++) {
          const index = (words[k] ?? 0) - min;
          const head =
            ((nulls[k] ?? 0) & strict) === 0 && index >= 0 && index < range
              ? (table[index] ?? 0)
              : 0;
          heads[k] = head;
          if (head !== 0) matched++;
        }
      } else {
        for (let k = 0; k < count; k++) {
          const index = (words[k * 2 + 1] ?? 0) * 4_294_967_296 + ((words[k * 2] ?? 0) >>> 0) - min;
          const head =
            ((nulls[k] ?? 0) & strict) === 0 && index >= 0 && index < range
              ? (table[index] ?? 0)
              : 0;
          heads[k] = head;
          if (head !== 0) matched++;
        }
      }
      return matched;
    }
    const slots = this.#slots;
    const mask = this.#mask;
    const shift = this.#shift;
    const stride = this.#stride;
    const level = this.#level;
    const hashes = scratch.hashes;
    const starts = this.#slotScratch;
    for (let k = 0; k < count; k++)
      starts[k] = ((levelHash(hashes[k] ?? 0, level) << 4) >>> shift) & mask;
    if (this.#touch) {
      // Touch every first slot before comparing: the misses overlap instead of serialising.
      let sink = 0;
      for (let k = 0; k < count; k++) sink ^= slots[(starts[k] ?? 0) * stride] ?? 0;
      if (sink === 0x7fff_ffff) heads[0] = 0;
    }
    if (this.kind === "words") {
      const width = this.#layout.width;
      if (width === 1) {
        for (let k = 0; k < count; k++) {
          let head = 0;
          if (((nulls[k] ?? 0) & strict) === 0) {
            const key = words[k] ?? 0;
            let s = starts[k] ?? 0;
            for (;;) {
              const slot = s * 2;
              const h = slots[slot] ?? 0;
              if (h === 0) break;
              if (slots[slot + 1] === key) {
                head = h;
                break;
              }
              s = (s + 1) & mask;
            }
          }
          heads[k] = head;
          if (head !== 0) matched++;
        }
        return matched;
      }
      if (width === 2) {
        for (let k = 0; k < count; k++) {
          let head = 0;
          if (((nulls[k] ?? 0) & strict) === 0) {
            const lo = words[k * 2] ?? 0;
            const hi = words[k * 2 + 1] ?? 0;
            let s = starts[k] ?? 0;
            for (;;) {
              const slot = s * 3;
              const h = slots[slot] ?? 0;
              if (h === 0) break;
              if (slots[slot + 1] === lo && slots[slot + 2] === hi) {
                head = h;
                break;
              }
              s = (s + 1) & mask;
            }
          }
          heads[k] = head;
          if (head !== 0) matched++;
        }
        return matched;
      }
      for (let k = 0; k < count; k++) {
        let head = 0;
        if (((nulls[k] ?? 0) & strict) === 0) {
          const at = k * width;
          let s = starts[k] ?? 0;
          for (;;) {
            const slot = s * stride;
            const h = slots[slot] ?? 0;
            if (h === 0) break;
            let same = true;
            for (let w = 0; w < width && same; w++) same = slots[slot + 1 + w] === words[at + w];
            if (same) {
              head = h;
              break;
            }
            s = (s + 1) & mask;
          }
        }
        heads[k] = head;
        if (head !== 0) matched++;
      }
      return matched;
    }
    // Bytes family.
    const raw = this.#layout.family === "raw";
    const probeRaw = scratch.rawData;
    const encoded = scratch.encoded;
    for (let k = 0; k < count; k++) {
      let head = 0;
      if (((nulls[k] ?? 0) & strict) === 0) {
        const hash = hashes[k] ?? 0;
        let probeBytes: Uint8Array;
        let start: number;
        let end: number;
        if (raw) {
          const slot = scratch.slots[k] ?? 0;
          probeBytes = probeRaw?.bytes ?? new Uint8Array(0);
          start = probeRaw?.offsets[slot] ?? 0;
          end = probeRaw?.offsets[slot + 1] ?? 0;
        } else {
          probeBytes = encoded?.writer.bytes ?? new Uint8Array(0);
          start = encoded?.offsets[k] ?? 0;
          end = encoded?.offsets[k + 1] ?? 0;
        }
        let s = starts[k] ?? 0;
        for (;;) {
          const slot = s * 2;
          const h = slots[slot] ?? 0;
          if (h === 0) break;
          if ((slots[slot + 1] ?? 0) >>> 0 === hash) {
            const bytes = this.#buildBytes(h - 1);
            if (bytesEqual(bytes, this.#keyStart, this.#keyEnd, probeBytes, start, end)) {
              head = h;
              break;
            }
          }
          s = (s + 1) & mask;
        }
      }
      heads[k] = head;
      if (head !== 0) matched++;
    }
    return matched;
  }

  close(): void {
    if (this.retainedBytes > 0) this.#charge.free(this.retainedBytes);
    this.retainedBytes = 0;
    this.#slots = new Int32Array(0);
    this.#heads = new Int32Array(0);
    this.#encoded = [];
    this.next = null;
  }
}

/** The values of a flat int32/date or safe int64-family vector (direct addressing), else null. */
function directValues(vector: Vector): Int32Array | Float64Array | null {
  if (vector.form !== "flat") return null;
  const data = vector.data;
  if (data.layout === "i32") return data.values;
  if (data.layout === "int64" && data.values.repr === "safe") return data.values.values;
  return null;
}

/** The key column vectors of store chunk k. */
export function chunkKeys(store: ColumnStore, keyColumns: readonly number[], k: number) {
  return keyColumns.map((c) => store.chunk(c, k));
}
