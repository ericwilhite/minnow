/**
 * Decode targets for one scan column: reusable buffers of `capacity` slots in the column's
 * physical layout, charged before they are built, into which `ChunkPages` decodes a row range
 * (`decodeRange` / `decodeCodes`) or selected positions (`gather` / `gatherCodes`).
 *
 * - Dictionary-encoded spans stay codes: a `DictionaryVector` over the chunk's dictionary (whose
 *   bytes the reader charged to the query, ruling R5), one stable `dictionaryId` per dictionary.
 * - int64-family columns decode into the f64 `safe` representation when the pages prove it
 *   (`isSafe`), else into a BigInt64Array; decimals into the chunk's scale or per-slot scales, the
 *   int128 tier when the chunk needs it, specials when it has them.
 * - Varlen bytes grow to what `varlenBytes` / `gatherBytes` say (full value lengths, out-of-line
 *   values included), charged first; the caller sizes batches so that stays bounded.
 * The vector returned is valid until the next decode into the same target.
 */
import type { PhysicalType } from "../types.js";
import { layoutOf } from "../types.js";
import {
  newDictionaryId,
  type FlatVector,
  type Int64Values,
  type Validity,
  type Vector,
  type VectorData,
} from "../vector/vector.js";
import type { ChunkPages } from "./scan.js";

/** Where a target's bytes are charged (an `OutputArena` or a grant-backed `Charged`). */
export interface TargetMemory {
  take(bytes: number): void;
  give(bytes: number): void;
}

/** A row range `[from, to)`, or selected group positions `rows[0 … count)` (ascending). */
export type DecodeSpec =
  | { readonly from: number; readonly to: number }
  | { readonly rows: Uint16Array; readonly count: number };

const dictionaryIds = new WeakMap<FlatVector, number>();

/** The stable id of a chunk dictionary (codes of different dictionaries never compare). */
function dictionaryIdOf(dictionary: FlatVector): number {
  let id = dictionaryIds.get(dictionary);
  if (id === undefined) {
    id = newDictionaryId();
    dictionaryIds.set(dictionary, id);
  }
  return id;
}

type Typed = Uint8Array | Uint16Array | Uint32Array | Int32Array | Float32Array | Float64Array;

export class ColumnTarget {
  readonly #memory: TargetMemory;
  readonly #layout: ReturnType<typeof layoutOf>;
  #charged = 0;
  readonly #buffers = new Map<string, Typed | BigInt64Array>();
  #bytes: Uint8Array = new Uint8Array(0);

  constructor(
    readonly type: PhysicalType,
    readonly capacity: number,
    memory: TargetMemory,
  ) {
    this.#memory = memory;
    this.#layout = layoutOf(type);
  }

  /** A buffer `name` of `length` elements, made (charged first) on first use. */
  #buffer<T extends Typed | BigInt64Array>(
    name: string,
    Type: { new (length: number): T; readonly BYTES_PER_ELEMENT: number },
    length: number,
  ): T {
    const existing = this.#buffers.get(name);
    if (existing !== undefined) return existing as T;
    const bytes = length * Type.BYTES_PER_ELEMENT;
    this.#memory.take(bytes);
    this.#charged += bytes;
    let made: T;
    try {
      made = new Type(length);
    } catch (error) {
      this.#memory.give(bytes);
      this.#charged -= bytes;
      throw error;
    }
    this.#buffers.set(name, made);
    return made;
  }

  /** The varlen buffer size a decode needing `bytes` grows to (`#varlen`'s policy). */
  #grownSize(bytes: number): number {
    return Math.max(bytes, 2 * this.#bytes.length, 4096);
  }

  /**
   * Bytes a decode needing `bytes` of varlen data newly takes (the new buffer, charged before the
   * old one is given back), or 0 when the buffer is large enough.
   */
  growthFor(bytes: number): number {
    return this.#bytes.length >= bytes ? 0 : this.#grownSize(bytes);
  }

  /** Gives back the varlen buffer (after an oversized row, before its loan is repaid). */
  shrink(): void {
    if (this.#bytes.length === 0) return;
    this.#memory.give(this.#bytes.length);
    this.#charged -= this.#bytes.length;
    this.#bytes = new Uint8Array(0);
  }

  /** The varlen byte buffer, at least `bytes` long (grown, charged first). */
  #varlen(bytes: number): Uint8Array {
    if (this.#bytes.length >= bytes) return this.#bytes;
    const size = this.#grownSize(bytes);
    this.#memory.take(size);
    this.#charged += size;
    let next: Uint8Array;
    try {
      next = new Uint8Array(size);
    } catch (error) {
      this.#memory.give(size);
      this.#charged -= size;
      throw error;
    }
    this.#memory.give(this.#bytes.length);
    this.#charged -= this.#bytes.length;
    this.#bytes = next;
    return next;
  }

  #validity(count: number): Validity {
    const validity = this.#buffer("validity", Uint32Array, (this.capacity + 31) >>> 5);
    validity.fill(0, 0, (count + 31) >>> 5);
    return validity;
  }

  /** Bytes a varlen decode of `spec` needs (0 for fixed-width columns). */
  static varlenBytesOf(pages: ChunkPages, type: PhysicalType, spec: DecodeSpec): number {
    const layout = layoutOf(type);
    if (layout !== "varlen") return 0;
    return "from" in spec
      ? pages.varlenBytes(spec.from, spec.to)
      : pages.gatherBytes(spec.rows, 0, spec.count);
  }

  /** Decodes `spec` (pages and out-of-line values loaded) into this target. */
  decode(pages: ChunkPages, spec: DecodeSpec): Vector {
    const count = "from" in spec ? spec.to - spec.from : spec.count;
    if (count > this.capacity) throw new RangeError("A decode larger than its target");
    const [lo, hi] =
      "from" in spec
        ? [spec.from, spec.to]
        : [spec.rows[0] ?? 0, (spec.rows[Math.max(0, spec.count - 1)] ?? 0) + 1];
    if (count > 0 && pages.isDictionaryRange(lo, hi) && pages.dictionary !== null) {
      const codes = this.#buffer("codes", Int32Array, this.capacity);
      const validity = this.#validity(count);
      if ("from" in spec) pages.decodeCodes(spec.from, spec.to, codes, validity, 0);
      else pages.gatherCodes(spec.rows, codes, validity, 0, 0, spec.count);
      return {
        form: "dictionary",
        type: this.type,
        length: count,
        codes,
        validity: allValid(validity, count) ? null : validity,
        dictionary: pages.dictionary,
        dictionaryId: dictionaryIdOf(pages.dictionary),
      };
    }
    const [first, end] = pages.pagesInRange(lo, Math.max(hi, lo + 1));
    const target: FlatVector = {
      form: "flat",
      type: this.type,
      length: count,
      validity: this.#validity(count),
      data: this.#data(pages, spec, first, end),
    };
    if ("from" in spec) pages.decodeRange(spec.from, spec.to, target, 0);
    else pages.gather(spec.rows, target, 0, 0, spec.count);
    return allValid(target.validity, count) ? { ...target, validity: null } : target;
  }

  #int64(pages: ChunkPages, first: number, end: number): Int64Values {
    return pages.isSafe(first, end)
      ? { repr: "safe", values: this.#buffer("f64", Float64Array, this.capacity) }
      : { repr: "int64", values: this.#buffer("i64", BigInt64Array, this.capacity) };
  }

  #data(pages: ChunkPages, spec: DecodeSpec, first: number, end: number): VectorData {
    const capacity = this.capacity;
    switch (this.#layout) {
      case "bool":
        return { layout: "bool", values: this.#buffer("u8", Uint8Array, capacity) };
      case "i32":
        return { layout: "i32", values: this.#buffer("i32", Int32Array, capacity) };
      case "f32":
        return { layout: "f32", values: this.#buffer("f32", Float32Array, capacity) };
      case "f64":
        return { layout: "f64", values: this.#buffer("f64", Float64Array, capacity) };
      case "int64":
        return { layout: "int64", values: this.#int64(pages, first, end) };
      case "interval":
        return {
          layout: "interval",
          months: this.#buffer("months", Int32Array, capacity),
          days: this.#buffer("days", Int32Array, capacity),
          micros: this.#int64(pages, first, end),
        };
      case "fixed16":
        return { layout: "fixed16", bytes: this.#buffer("fixed16", Uint8Array, 16 * capacity) };
      case "decimal": {
        const scale = pages.directory.scale;
        return {
          layout: "decimal",
          scale,
          scales: scale === null ? this.#buffer("scales", Uint16Array, capacity) : null,
          coefficients: pages.needsInt128
            ? { repr: "int128", limbs: this.#buffer("limbs", Uint32Array, 4 * capacity) }
            : this.#int64(pages, first, end),
          specials: pages.hasDecimalSpecials
            ? this.#buffer("specials", Uint8Array, capacity)
            : null,
        };
      }
      case "varlen":
        return {
          layout: "varlen",
          bytes: this.#varlen(ColumnTarget.varlenBytesOf(pages, this.type, spec)),
          offsets: this.#buffer("offsets", Int32Array, capacity + 1),
        };
    }
  }

  /** Bytes this target holds from its memory. */
  get chargedBytes(): number {
    return this.#charged;
  }

  close(): void {
    if (this.#charged > 0) this.#memory.give(this.#charged);
    this.#charged = 0;
    this.#buffers.clear();
    this.#bytes = new Uint8Array(0);
  }
}

function allValid(validity: Validity | null, count: number): boolean {
  if (validity === null) return true;
  const words = count >>> 5;
  for (let w = 0; w < words; w++) if (validity[w] !== 0xffff_ffff) return false;
  const rest = count & 31;
  if (rest === 0) return true;
  const mask = rest === 31 ? 0x7fff_ffff : (1 << rest) - 1;
  return ((validity[words] ?? 0) & mask) === mask;
}

/** Decodes `spec` into `target` (see `ColumnTarget.decode`). */
export function decodeInto(target: ColumnTarget, pages: ChunkPages, spec: DecodeSpec): Vector {
  return target.decode(pages, spec);
}
