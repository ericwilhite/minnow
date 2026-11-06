/**
 * Dynamic join filters (track B7; PROPOSAL "Dynamic join filters", design-joins-agg-spill.md
 * "Dynamic filters"): when a hash join's build side is complete it publishes, per filtered key
 * column, a membership filter the probe-side scan applies before it decodes payload columns.
 *
 * ## Soundness (the only law that matters here)
 * A filter may keep rows that cannot match (false positives); it never drops a row that can
 * match. So it is published only for joins where an unmatched probe row is dropped anyway —
 * inner, semi and right (`kindPublishes`). Left, full, anti and mark joins output their misses
 * and publish nothing. A probe row with a null key is dropped, unless the key is compared with
 * IS NOT DISTINCT FROM and the build side holds a null.
 *
 * ## Shapes
 * - **Exact bitmap** over `[min, max]` for one integer key with a dense enough range
 *   (≤ 16 bits per build key and ≤ 2^23 values): no false positives.
 * - **Split-block Bloom filter** otherwise: 256-bit blocks (one cache line), 8 bits per key set
 *   one per 32-bit word, 12 bits per key (≈0.5–1 % false positives).
 * - **Bounds**: min/max of an integer, date, timestamp or float key, for page pruning
 *   (`excludes`) and as a first cheap test. NaN is tracked apart (it equals NaN).
 * Membership hashes are the join's own canonical single-column forms (join-keys.ts), so the
 * scan's test agrees with the probe's equality for every representation of a value.
 *
 * ## Cost rule
 * Memory is charged to the join's grant (~1.5 bytes per build key). After `DROP_AFTER_PROBES`
 * probes, a filter that kept more than 90 % of them stops testing (`active` false): it costs
 * more than it saves.
 */
import type { JoinKind } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { Selection, Vector } from "../vector/vector.js";
import { KeyLayout, KeyScratch, extractKeys, fmix32 } from "./join-keys.js";
import type { ColumnStore } from "./column-store.js";
import { Slice, runToEnd, type SlicedJob } from "./slices.js";

export const BLOOM_BITS_PER_KEY = 12;
export const DROP_AFTER_PROBES = 65_536;
export const DROP_KEEP_RATIO = 0.9;
const EXACT_LIMIT = 1 << 23;
const SALTS = new Int32Array([
  0x47b6137b,
  0x44974d91,
  0x8824ad5b | 0,
  0xa2b7289d | 0,
  0x705495c7,
  0x2df1424b,
  0x9efc4947 | 0,
  0x5c6bfb31,
]);

/** Join kinds whose probe misses are dropped, so a filter may run before the join. */
export function kindPublishes(kind: JoinKind): boolean {
  return kind === "inner" || kind === "semi" || kind === "right";
}

export class JoinRuntimeFilter {
  readonly id: number;
  readonly type: PhysicalType;
  /** Smallest and largest non-NaN build value (integers as numbers when safe), or null. */
  min: number | null = null;
  max: number | null = null;
  /** The build side holds NaN (floats) or a null under IS NOT DISTINCT FROM. */
  hasNaN = false;
  hasNull = false;
  /** Probes tested and kept while active. */
  probes = 0;
  kept = 0;
  active = true;
  readonly shape: "exact" | "bloom" | "bounds";
  readonly retainedBytes: number;
  readonly #layout: KeyLayout;
  readonly #scratch: KeyScratch;
  readonly #nullSafe: boolean;
  #bits = new Uint32Array(0);
  #blockShift = 32;
  #exactBase = 0;
  #charge: ByteCharge;

  private constructor(
    id: number,
    type: PhysicalType,
    nullSafe: boolean,
    shape: "exact" | "bloom" | "bounds",
    scratch: KeyScratch,
    layout: KeyLayout,
    charge: ByteCharge,
    bytes: number,
  ) {
    this.id = id;
    this.type = type;
    this.#nullSafe = nullSafe;
    this.shape = shape;
    this.#scratch = scratch;
    this.#layout = layout;
    this.#charge = charge;
    this.retainedBytes = bytes;
  }

  /** Bytes a filter over `rows` build keys holds at most (charged before it is built). */
  static estimateBytes(type: PhysicalType, rows: number, capacity: number): number {
    const layout = new KeyLayout([type], [true]);
    return KeyScratch.bytes(layout, capacity) + Math.ceil((rows * BLOOM_BITS_PER_KEY) / 8) * 2 + 64;
  }

  /**
   * Builds the filter over column `column` of the build store (every row; nulls only count when
   * `nullSafe`). Null when the charge refused its memory: the join then publishes nothing.
   */
  static build(
    id: number,
    store: ColumnStore,
    column: number,
    nullSafe: boolean,
    charge: ByteCharge,
    capacity: number,
  ): JoinRuntimeFilter | null {
    return runToEnd(
      JoinRuntimeFilter.buildJob(
        id,
        store,
        column,
        nullSafe,
        charge,
        capacity,
        new Slice(Infinity),
      ),
    );
  }

  /** `build` as a sliced job (R23.2): it yields when `slice` is due, checked per store chunk. */
  static *buildJob(
    id: number,
    store: ColumnStore,
    column: number,
    nullSafe: boolean,
    charge: ByteCharge,
    capacity: number,
    slice: Slice,
  ): SlicedJob<JoinRuntimeFilter | null> {
    const type = store.types[column];
    if (type === undefined) throw new RangeError("No such build column");
    const layout = new KeyLayout([type], [false]);
    const scratchBytes = KeyScratch.bytes(layout, capacity);
    if (!charge.allocate(scratchBytes)) return null;
    let scratch: KeyScratch;
    try {
      scratch = new KeyScratch(layout, capacity, { allocate: () => true, free: () => undefined });
    } catch {
      charge.free(scratchBytes);
      return null;
    }
    const part = layout.parts[0];
    const integer = part === "w1" || part === "w2i";
    const ordered = integer || part === "w2f";
    // Pass 1: bounds, NaN, null and the key count (kept in an object the visitor mutates).
    const seen = {
      min: Infinity,
      max: -Infinity,
      hasNaN: false,
      hasNull: false,
      unsafe: false,
      keys: 0,
    };
    function* visit(each: (k: number) => void): SlicedJob<void> {
      for (let chunk = 0; chunk < store.chunkCount; chunk++) {
        const vector = store.chunk(column, chunk);
        const count = extractKeys([vector], null, 0, vector.length, scratch);
        for (let k = 0; k < count; k++) {
          if ((scratch.nulls[k] ?? 0) !== 0) {
            seen.hasNull = true;
            continue;
          }
          each(k);
        }
        if (slice.due()) yield;
      }
    }
    yield* visit((k) => {
      seen.keys++;
      if (!ordered) return;
      const value = valueOf(part, scratch, k);
      if (Number.isNaN(value)) {
        if (part === "w2f") seen.hasNaN = true;
        else seen.unsafe = true;
        return;
      }
      if (value < seen.min) seen.min = value;
      if (value > seen.max) seen.max = value;
    });
    const { min, max, hasNaN, hasNull, unsafe, keys } = seen;
    const range = max - min + 1;
    const exact =
      integer &&
      !unsafe &&
      keys > 0 &&
      range <= EXACT_LIMIT &&
      range <= keys * 16 &&
      Number.isFinite(range);
    const words = exact
      ? Math.ceil(range / 32)
      : Math.max(8, nextPow2(Math.ceil((keys * BLOOM_BITS_PER_KEY) / 256)) * 8);
    const shape = keys === 0 ? "bounds" : exact ? "exact" : "bloom";
    const bitsBytes = shape === "bounds" ? 0 : words * 4;
    if (!charge.allocate(bitsBytes)) {
      charge.free(scratchBytes);
      return null;
    }
    const filter = new JoinRuntimeFilter(
      id,
      type,
      nullSafe,
      shape,
      scratch,
      layout,
      charge,
      scratchBytes + bitsBytes,
    );
    filter.hasNaN = hasNaN;
    filter.hasNull = nullSafe && hasNull;
    if (ordered && !unsafe && keys > 0 && min <= max) {
      filter.min = min;
      filter.max = max;
    }
    if (shape === "exact") {
      filter.#bits = new Uint32Array(words);
      filter.#exactBase = min;
      const bits = filter.#bits;
      yield* visit((k) => {
        const index = valueOf(part, scratch, k) - min;
        bits[index >>> 5] = (bits[index >>> 5] ?? 0) | (1 << (index & 31));
      });
    } else if (shape === "bloom") {
      filter.#bits = new Uint32Array(words);
      filter.#blockShift = 32 - Math.log2(words / 8);
      const bits = filter.#bits;
      const shift = filter.#blockShift;
      yield* visit((k) => {
        bloomInsert(bits, shift, scratch.hashes[k] ?? 0);
      });
    }
    return filter;
  }

  /**
   * Narrows the active rows of a probe-side batch (`selection`, or 0 … length) to those whose
   * `vector` value may match. Returns the selection to use: `out`, or the input (`selection`,
   * null for all rows) when the filter is inactive.
   */
  apply(
    vector: Vector,
    length: number,
    selection: Selection | null,
    out: Selection,
  ): Selection | null {
    if (!this.active) return selection;
    const scratch = this.#scratch;
    const count = selection === null ? length : selection.count;
    const ids = selection === null ? null : selection.rows;
    let kept = 0;
    const part = this.#layout.parts[0];
    const ordered = part === "w1" || part === "w2i" || part === "w2f";
    const min = this.min;
    const max = this.max;
    for (let from = 0; from < count; from += scratch.capacity) {
      const n = Math.min(scratch.capacity, count - from);
      extractKeys([vector], ids, from, n, scratch);
      for (let k = 0; k < n; k++) {
        const row = ids === null ? from + k : (ids[from + k] ?? 0);
        let keep: boolean;
        if ((scratch.nulls[k] ?? 0) !== 0) keep = this.#nullSafe && this.hasNull;
        else if (this.shape === "bounds") keep = false;
        else {
          keep = true;
          if (ordered && min !== null && max !== null) {
            const value = valueOf(part, scratch, k);
            if (Number.isNaN(value)) keep = part === "w2f" ? this.hasNaN : true;
            else keep = value >= min && value <= max;
            if (keep && this.shape === "exact") {
              const index = value - this.#exactBase;
              keep = ((this.#bits[index >>> 5] ?? 0) & (1 << (index & 31))) !== 0;
            }
          }
          if (keep && this.shape === "bloom")
            keep = bloomTest(this.#bits, this.#blockShift, scratch.hashes[k] ?? 0);
        }
        if (keep) out.rows[kept++] = row;
      }
    }
    out.count = kept;
    this.probes += count;
    this.kept += kept;
    if (this.probes >= DROP_AFTER_PROBES && this.kept > this.probes * DROP_KEEP_RATIO)
      this.active = false;
    return out;
  }

  /**
   * Whether no row of a page (or segment) with bounds [lo, hi] can match, so it is skipped.
   * `nulls` and `nans` are the page's null and NaN counts (null: unknown). A page that may hold a
   * NULL is never excluded when NULL can match (IS NOT DISTINCT FROM with a NULL in the build),
   * nor one that may hold NaN when NaN can (a NaN in the build: NaN = NaN) — bounds say nothing
   * about either.
   */
  excludes(lo: number, hi: number, nulls: number | null, nans: number | null): boolean {
    if (this.hasNull && nulls !== 0) return false;
    if (this.hasNaN && nans !== 0) return false;
    if (this.shape === "bounds") return true;
    if (this.min === null || this.max === null) return false;
    return hi < this.min || lo > this.max;
  }

  close(): void {
    this.#charge.free(this.retainedBytes);
    this.#bits = new Uint32Array(0);
    this.active = false;
  }
}

function nextPow2(n: number): number {
  let p = 1;
  while (p < n) p *= 2;
  return p;
}

/** Numeric value of key k (integers exact when safe; NaN for an unsafe int64 or a NaN float). */
function valueOf(part: string | undefined, scratch: KeyScratch, k: number): number {
  const words = scratch.words;
  if (part === "w1") return words[k] ?? 0;
  const lo = words[k * 2] ?? 0;
  const hi = words[k * 2 + 1] ?? 0;
  if (part === "w2i") {
    const value = hi * 4_294_967_296 + (lo >>> 0);
    return Math.abs(value) <= Number.MAX_SAFE_INTEGER ? value : Number.NaN;
  }
  f64Words[0] = lo;
  f64Words[1] = hi;
  return f64[0] ?? 0;
}

// Module scratch, sync-only (R23.1): written and read inside one synchronous conversion.
const f64 = new Float64Array(1);
const f64Words = new Int32Array(f64.buffer);

function bloomInsert(bits: Uint32Array, shift: number, hash: number): void {
  const block = shift >= 32 ? 0 : (hash >>> shift) * 8;
  const h2 = fmix32((hash ^ 0x9e37_79b9) >>> 0);
  for (let i = 0; i < 8; i++) {
    const bit = Math.imul(h2, SALTS[i] ?? 0) >>> 27;
    bits[block + i] = (bits[block + i] ?? 0) | (1 << bit);
  }
}

function bloomTest(bits: Uint32Array, shift: number, hash: number): boolean {
  const block = shift >= 32 ? 0 : (hash >>> shift) * 8;
  const h2 = fmix32((hash ^ 0x9e37_79b9) >>> 0);
  for (let i = 0; i < 8; i++) {
    const bit = Math.imul(h2, SALTS[i] ?? 0) >>> 27;
    if (((bits[block + i] ?? 0) & (1 << bit)) === 0) return false;
  }
  return true;
}
