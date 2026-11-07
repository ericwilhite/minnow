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
 * - Buffers are sized to the rows a decode needs (at least `MIN_ROWS`, doubling, at most
 *   `capacity`), so a small grant's small batches take small buffers. Varlen bytes grow to what
 *   `varlenBytes` / `gatherBytes` say (full value lengths, out-of-line values included).
 * - Sizing reads the same layout description as decoding (`shapeOf`): `outputBytes` is what a
 *   decode produces (fixed and variable parts) and `growthFor` what it newly charges, so a scan
 *   bounds batches and reserves memory before it decodes anything.
 * `decode` borrows until the next decode. `decodeOwned` instead allocates tight dedicated output
 * buffers (including flat UTF-8 text), charged independently until the caller returns its recorded byte charge.
 * It never exposes dictionary/page/cache buffers; all-valid proofs use verified loaded masks.
 * The vector returned by `decode` is valid until the next decode into the same target.
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

/** One reusable buffer of a target: its element type and how many elements `rows` rows need. */
interface BufferSpec<T extends Typed | BigInt64Array> {
  readonly name: string;
  readonly Type: { new (length: number): T; readonly BYTES_PER_ELEMENT: number };
  readonly elements: (rows: number) => number;
  /** Stands in for the buffer while a decode is only being sized. */
  readonly empty: T;
}

function spec<T extends Typed | BigInt64Array>(
  name: string,
  Type: { new (length: number): T; readonly BYTES_PER_ELEMENT: number },
  perRow = 1,
  extra = 0,
): BufferSpec<T> {
  return { name, Type, elements: (rows) => perRow * rows + extra, empty: new Type(0) };
}

const VALIDITY: BufferSpec<Uint32Array> = {
  name: "validity",
  Type: Uint32Array,
  elements: (rows) => (rows + 31) >>> 5,
  empty: new Uint32Array(0),
};
const CODES = spec("codes", Int32Array);
const U8 = spec("u8", Uint8Array);
const I32 = spec("i32", Int32Array);
const F32 = spec("f32", Float32Array);
const F64 = spec("f64", Float64Array);
const I64 = spec("i64", BigInt64Array);
const MONTHS = spec("months", Int32Array);
const DAYS = spec("days", Int32Array);
const FIXED16 = spec("fixed16", Uint8Array, 16);
const SCALES = spec("scales", Uint16Array);
const LIMBS = spec("limbs", Uint32Array, 4);
const SPECIALS = spec("specials", Uint8Array);
const OFFSETS = spec("offsets", Int32Array, 1, 1);
const NO_BYTES = new Uint8Array(0);

/** Fixed buffers grow to at least this many rows (fewer reallocations for small batches). */
const MIN_ROWS = 64;

/** What a decode takes from a target: its buffers (by spec) and varlen bytes. */
interface Use {
  buffer<T extends Typed | BigInt64Array>(spec: BufferSpec<T>): T;
  varlen(bytes: number): Uint8Array;
}

/** What a decode of a spec produces: dictionary codes, or flat data (validity in both). */
type Shape =
  | {
      readonly codes: Int32Array;
      readonly validity: Uint32Array | null;
      readonly dictionary: FlatVector;
    }
  | { readonly data: VectorData; readonly validity: Uint32Array | null };

/** Fresh decode buffers, never pages/cache/pool storage. Caller returns `bytes` to the allocation memory after ownership ends.
 * A dictionary vector also borrows its immutable dictionary; its caller must retain that reader. */
export interface OwnedColumn {
  readonly vector: Vector;
  readonly buffers: readonly ArrayBuffer[];
  readonly bytes: number;
}
export class ColumnTarget {
  readonly #memory: TargetMemory;
  readonly #layout: ReturnType<typeof layoutOf>;
  #charged = 0;
  /** Buffers by name, with the rows each was made for (at most `capacity`). */
  readonly #buffers = new Map<string, { array: Typed | BigInt64Array; rows: number }>();
  #bytes: Uint8Array = new Uint8Array(0);

  constructor(
    readonly type: PhysicalType,
    readonly capacity: number,
    memory: TargetMemory,
  ) {
    this.#memory = memory;
    this.#layout = layoutOf(type);
  }

  /** Decode directly into tight, dedicated output buffers. Text stays UTF-8 bytes.
   * Only fresh output buffers are marked detachable. With dictionaryCodes, the caller must
   * also retain the reader/dictionary and its charge through output release. Otherwise text
   * pages decode to fresh flat UTF-8. The caller prepays outputBytes with the same policy.
   */
  decodeOwned(
    pages: ChunkPages,
    spec: DecodeSpec,
    dictionaryCodes = false,
    memory: TargetMemory = this.#memory,
  ): OwnedColumn {
    const count = rowsOf(spec);
    if (count > this.capacity) throw new RangeError("A decode larger than its target");
    const buffers: ArrayBuffer[] = [];
    let bytes = 0;
    const create = <T extends Typed | BigInt64Array>(
      Type: { new (n: number): T; readonly BYTES_PER_ELEMENT: number },
      n: number,
    ): T => {
      const size = n * Type.BYTES_PER_ELEMENT;
      memory.take(size);
      bytes += size;
      const array = new Type(n);
      if (!(array.buffer instanceof ArrayBuffer))
        throw new TypeError("Expected dedicated ArrayBuffer");
      buffers.push(array.buffer);
      return array;
    };
    try {
      const shape = shapeOf(
        this.#layout,
        pages,
        spec,
        { buffer: (b) => create(b.Type, b.elements(count)), varlen: (n) => create(Uint8Array, n) },
        dictionaryCodes,
        true,
      );
      const target: Vector =
        "codes" in shape
          ? {
              form: "dictionary",
              type: this.type,
              length: count,
              validity: shape.validity,
              codes: shape.codes,
              dictionary: shape.dictionary,
              dictionaryId: dictionaryIdOf(shape.dictionary),
            }
          : {
              form: "flat",
              type: this.type,
              length: count,
              validity: shape.validity,
              data: shape.data,
            };
      if (target.form === "dictionary") {
        if ("from" in spec) pages.decodeCodes(spec.from, spec.to, target.codes, target.validity);
        else pages.gatherCodes(spec.rows, target.codes, target.validity, 0, 0, spec.count);
      } else if ("from" in spec) pages.decodeRange(spec.from, spec.to, target, 0);
      else pages.gather(spec.rows, target, 0, 0, spec.count);
      if (shape.validity !== null && allValid(shape.validity, count)) {
        target.validity = null;
        const validity = shape.validity;
        const at = buffers.findIndex((b) => b === validity.buffer);
        if (at < 0) throw new Error("Missing owned validity buffer");
        buffers.splice(at, 1);
        const unused = shape.validity.byteLength;
        memory.give(unused);
        bytes -= unused;
      }
      return { vector: target, buffers, bytes };
    } catch (error) {
      memory.give(bytes);
      throw error;
    }
  }

  /** The rows a buffer made for `held` rows grows to for a decode of `rows` (0: no growth). */
  #grownRows(held: number, rows: number): number {
    if (held >= rows) return 0;
    return Math.min(this.capacity, Math.max(rows, 2 * held, MIN_ROWS));
  }

  /**
   * Buffer `spec` with room for `rows` rows: grown (the new one charged first, then the old one
   * given back), or made on first use.
   */
  #buffer<T extends Typed | BigInt64Array>(spec: BufferSpec<T>, rows: number): T {
    const existing = this.#buffers.get(spec.name);
    const held = existing?.rows ?? 0;
    const grown = this.#grownRows(held, rows);
    if (existing !== undefined && grown === 0) return existing.array as T;
    const bytes = spec.elements(grown) * spec.Type.BYTES_PER_ELEMENT;
    this.#memory.take(bytes);
    this.#charged += bytes;
    let made: T;
    try {
      made = new spec.Type(spec.elements(grown));
    } catch (error) {
      this.#memory.give(bytes);
      this.#charged -= bytes;
      throw error;
    }
    if (existing !== undefined) {
      const old = existing.array.byteLength;
      this.#memory.give(old);
      this.#charged -= old;
    }
    this.#buffers.set(spec.name, { array: made, rows: grown });
    return made;
  }

  /** The varlen buffer size a decode needing `bytes` grows to (`#varlen`'s policy). */
  #grownSize(bytes: number): number {
    return Math.max(bytes, 2 * this.#bytes.length, 4096);
  }

  /** Varlen bytes a decode needing `bytes` newly takes (0 when the buffer is large enough). */
  #varlenGrowth(bytes: number): number {
    return this.#bytes.length >= bytes ? 0 : this.#grownSize(bytes);
  }

  /**
   * Bytes decoding `spec` newly takes from this target's memory: every buffer it grows (the new
   * one, charged before the old one is given back) and the varlen buffer. Pages loaded.
   */
  growthFor(pages: ChunkPages, spec: DecodeSpec): number {
    const rows = rowsOf(spec);
    let bytes = 0;
    this.#shape(pages, spec, {
      buffer: (b) => {
        // As `#buffer`: made on first use, or grown.
        const existing = this.#buffers.get(b.name);
        const grown = this.#grownRows(existing?.rows ?? 0, rows);
        if (existing === undefined || grown > 0)
          bytes += b.elements(grown) * b.Type.BYTES_PER_ELEMENT;
        return b.empty;
      },
      varlen: (n) => {
        bytes += this.#varlenGrowth(n);
        return NO_BYTES;
      },
    });
    return bytes;
  }

  /**
   * Bytes of the vector decoding `spec` produces, as decoded: validity, codes or values, scales,
   * limbs, specials, offsets and varlen bytes (full value lengths). Pages loaded.
   */
  static outputBytes(
    pages: ChunkPages,
    type: PhysicalType,
    spec: DecodeSpec,
    dictionaryCodes = true,
    omitAllValid = false,
  ): number {
    const rows = rowsOf(spec);
    let bytes = 0;
    shapeOf(
      layoutOf(type),
      pages,
      spec,
      {
        buffer: (b) => {
          bytes += b.elements(rows) * b.Type.BYTES_PER_ELEMENT;
          return b.empty;
        },
        varlen: (n) => {
          bytes += n;
          return NO_BYTES;
        },
      },
      dictionaryCodes,
      omitAllValid,
    );
    return bytes;
  }

  /** Gives back every buffer (after an oversized row, before its loan is repaid). */
  shrink(): void {
    if (this.#charged > 0) this.#memory.give(this.#charged);
    this.#charged = 0;
    this.#buffers.clear();
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

  #shape(pages: ChunkPages, spec: DecodeSpec, use: Use): Shape {
    return shapeOf(this.#layout, pages, spec, use);
  }

  /** Bytes a varlen decode of `spec` needs (0 for fixed-width columns). */
  static varlenBytesOf(pages: ChunkPages, type: PhysicalType, spec: DecodeSpec): number {
    const layout = layoutOf(type);
    if (layout !== "varlen") return 0;
    return varlenBytes(pages, spec);
  }

  /** Decodes `spec` (pages and out-of-line values loaded) into this target. */
  decode(pages: ChunkPages, spec: DecodeSpec): Vector {
    const count = rowsOf(spec);
    if (count > this.capacity) throw new RangeError("A decode larger than its target");
    const shape = this.#shape(pages, spec, {
      buffer: (b) => this.#buffer(b, count),
      varlen: (n) => this.#varlen(n),
    });
    const validity = shape.validity;
    validity?.fill(0, 0, (count + 31) >>> 5);
    if ("codes" in shape) {
      const codes = shape.codes;
      if ("from" in spec) pages.decodeCodes(spec.from, spec.to, codes, validity, 0);
      else pages.gatherCodes(spec.rows, codes, validity, 0, 0, spec.count);
      return {
        form: "dictionary",
        type: this.type,
        length: count,
        codes,
        validity: allValid(validity, count) ? null : validity,
        dictionary: shape.dictionary,
        dictionaryId: dictionaryIdOf(shape.dictionary),
      };
    }
    const target: FlatVector = {
      form: "flat",
      type: this.type,
      length: count,
      validity,
      data: shape.data,
    };
    if ("from" in spec) pages.decodeRange(spec.from, spec.to, target, 0);
    else pages.gather(spec.rows, target, 0, 0, spec.count);
    return allValid(target.validity, count) ? { ...target, validity: null } : target;
  }

  /** Bytes this target holds from its memory. */
  get chargedBytes(): number {
    return this.#charged;
  }

  close(): void {
    this.shrink();
  }
}

function rowsOf(spec: DecodeSpec): number {
  return "from" in spec ? spec.to - spec.from : spec.count;
}

function varlenBytes(pages: ChunkPages, spec: DecodeSpec): number {
  return "from" in spec
    ? pages.varlenBytes(spec.from, spec.to)
    : pages.gatherBytes(spec.rows, 0, spec.count);
}

/**
 * The buffers a decode of `spec` uses (through `use`): the one description both sizing
 * (`growthFor`, `outputBytes`) and decoding read, so they cannot disagree.
 */
function shapeOf(
  layout: ReturnType<typeof layoutOf>,
  pages: ChunkPages,
  spec: DecodeSpec,
  use: Use,
  dictionaryCodes = true,
  omitAllValid = false,
): Shape {
  const count = rowsOf(spec);
  const [lo, hi] =
    "from" in spec
      ? [spec.from, spec.to]
      : [spec.rows[0] ?? 0, (spec.rows[Math.max(0, spec.count - 1)] ?? 0) + 1];
  const needValidity = !(omitAllValid && count > 0 && pages.isAllValid?.(lo, hi) === true);
  const dictionary = pages.dictionary;
  if (dictionaryCodes && count > 0 && dictionary !== null && pages.isDictionaryRange(lo, hi))
    return {
      codes: use.buffer(CODES),
      validity: needValidity ? use.buffer(VALIDITY) : null,
      dictionary,
    };
  const validity = needValidity ? use.buffer(VALIDITY) : null;
  const [first, end] = pages.pagesInRange(lo, Math.max(hi, lo + 1));
  const int64 = (): Int64Values =>
    pages.isSafe(first, end)
      ? { repr: "safe", values: use.buffer(F64) }
      : { repr: "int64", values: use.buffer(I64) };
  let data: VectorData;
  switch (layout) {
    case "bool":
      data = { layout: "bool", values: use.buffer(U8) };
      break;
    case "i32":
      data = { layout: "i32", values: use.buffer(I32) };
      break;
    case "f32":
      data = { layout: "f32", values: use.buffer(F32) };
      break;
    case "f64":
      data = { layout: "f64", values: use.buffer(F64) };
      break;
    case "int64":
      data = { layout: "int64", values: int64() };
      break;
    case "interval":
      data = {
        layout: "interval",
        months: use.buffer(MONTHS),
        days: use.buffer(DAYS),
        micros: int64(),
      };
      break;
    case "fixed16":
      data = { layout: "fixed16", bytes: use.buffer(FIXED16) };
      break;
    case "decimal": {
      const scale = pages.directory.scale;
      data = {
        layout: "decimal",
        scale,
        scales: scale === null ? use.buffer(SCALES) : null,
        coefficients: pages.needsInt128 ? { repr: "int128", limbs: use.buffer(LIMBS) } : int64(),
        specials: pages.hasDecimalSpecials ? use.buffer(SPECIALS) : null,
      };
      break;
    }
    case "varlen":
      data = {
        layout: "varlen",
        bytes: use.varlen(varlenBytes(pages, spec)),
        offsets: use.buffer(OFFSETS),
      };
      break;
  }
  return { data, validity };
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
