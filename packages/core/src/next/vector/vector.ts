/**
 * The in-memory columnar representation every operator, kernel, spill page and result frame
 * uses. The push protocol that moves batches between operators is exec/pipeline.ts.
 *
 * ## Shapes
 * - A `Batch` is up to `BATCH_ROWS` rows and about `BATCH_TARGET_BYTES` bytes (`batchRowsFor`):
 *   an explicit terminal recycling export may target up to twice that byte budget, bounded by
 *   its result grant and credits. There is one `Vector` per column plus an optional `Selection`
 *   naming the active rows. Rows outside the
 *   selection are present but must be ignored. Wide rows mean fewer rows, never bigger batches;
 *   a single row larger than the target is a batch of one (memory/governor.ts admits it).
 * - A `Vector` is `flat` (one slot per row), `constant` (one value for every row), or
 *   `dictionary` (an Int32Array of codes into an immutable flat dictionary).
 * - `VectorData` is the typed storage of a flat vector, one variant per `Layout`.
 *
 * ## Value rules
 * - Validity is a bitmap, bit set = valid (Arrow's convention); `null` means every slot is valid.
 *   The data under an invalid slot is unspecified: kernels must not branch, throw or allocate on
 *   it (`1 / 0` in an invalid slot is not an error). Errors are raised only for active, valid rows.
 * - int64-family data (`Int64Values`) is exact. `repr: "safe"` holds integers in ±(2^53 − 1) in a
 *   Float64Array, never −0 and never a temporal infinity; `repr: "int64"` holds BigInt64Array. A
 *   producer may use `safe` only when every valid slot is proven in range (page statistics, or a
 *   checked kernel). A kernel whose result can leave the range must check and fall back to `int64`,
 *   or raise PostgreSQL's out-of-range error.
 * - Decimal coefficients have three tiers: int64-family, 128-bit limbs (`Int128Values`, no
 *   allocation per value), and `bigint[]` (`WideCoefficients`, past 128 bits). Kernels widen before
 *   overflow and never narrow implicitly; spill pages keep the tier (exact round trip).
 * - Decimal scale (DECISIONS.md C2): value = coefficient × 10^−s, where s is the vector's uniform
 *   `scale`, or, when values differ (unconstrained NUMERIC), the slot's own entry in `scales`.
 *   s is PostgreSQL's display scale (0 … 16,383, never negative): `1.50` is 150 at scale 2. A
 *   declared negative scale (NUMERIC(p, −s)) only rounds on write; stored scale is 0. Coefficients
 *   are never rescaled to a shared maximum, so one long fraction cannot inflate its neighbours;
 *   kernels that combine scales partition by scale (exec: `sumDecimal`) and align once per
 *   partition, not once per value.
 * - REAL (`f32`) is stored as Float32Array and computed in f64 with `Math.fround` at REAL type
 *   boundaries. Locators (`f64`) are vector/locator.ts.
 * - `varlen` strings are UTF-8 bytes. JS strings are made only when a value leaves the engine
 *   (`varlen.ts` `decodeUtf8`), with one documented exception: ordering under an ICU collation
 *   when its distinct values do not fit memory (DECISIONS.md C8).
 * - A dictionary's entries are distinct and non-null (null lives in the codes' validity). Two
 *   dictionaries with different `dictionaryId`s are unrelated: their codes are never compared.
 *
 * ## Ownership and lifetime
 * 1. The producer of a batch owns every buffer in it. A consumer borrows the batch until it
 *    signals readiness (exec/pipeline.ts: `push` returns `ready`, or `ready()` resolves after
 *    `blocked`). Anything kept past that is copied into the consumer's own buffers, unless the
 *    batch was handed over with `transfer` (then the consumer owns it and calls `release`).
 * 2. A dictionary vector's `dictionary` and a constant vector's `value` are immutable for the life
 *    of the query and may be referenced past the borrow.
 * 3. Nobody writes to a buffer they did not allocate. Kernels write only into output vectors their
 *    caller passes in.
 * 4. Operators allocate their output vectors once, charged to their grant, and reuse them: the
 *    producer recycles a batch's buffers only after the borrow ends. Kernels never allocate per
 *    value and, in steady state, never per batch.
 * 5. A `Selection`'s rows are ascending and unique, each < the batch length.
 * 6. Buffers handed to a helper worker are transferred, not shared: the sender gives up the
 *    ArrayBuffer (SharedArrayBuffer is never required).
 */
import { layoutOf, type Layout, type PhysicalType } from "../types.js";

/** Most rows in a batch: keeps selections in Uint16Array and a narrow column in L1/L2. */
export const BATCH_ROWS = 2048;
/** Target bytes of one batch across all its columns; wide rows get fewer rows. */
export const BATCH_TARGET_BYTES = 1024 * 1024;

/**
 * Rows per batch for rows of about `rowBytes` bytes (all columns, including varlen estimates):
 * at most `BATCH_ROWS`, at most `BATCH_TARGET_BYTES` in total, a multiple of 64 when ≥ 64, and
 * at least 1.
 */
export function batchRowsFor(rowBytes: number): number {
  if (!(rowBytes > 0)) return BATCH_ROWS;
  const rows = Math.floor(BATCH_TARGET_BYTES / rowBytes);
  if (rows >= BATCH_ROWS) return BATCH_ROWS;
  if (rows >= 64) return rows - (rows % 64);
  return Math.max(1, rows);
}

/** A validity bitmap: bit `i % 32` of word `i >>> 5`, set = valid. */
export type Validity = Uint32Array;

/** The active rows of a batch: `rows[0 … count)`, ascending, unique. Owned by its producer. */
export interface Selection {
  readonly rows: Uint16Array;
  count: number;
}

export type Int64Values =
  | { readonly repr: "safe"; readonly values: Float64Array }
  | { readonly repr: "int64"; readonly values: BigInt64Array };

/** 128-bit two's-complement integers: four little-endian Uint32 limbs per slot (int128.ts). */
export interface Int128Values {
  readonly repr: "int128";
  readonly limbs: Uint32Array;
}

/** Decimal coefficients past 128 bits (rare): one bigint per slot, accounted by size. */
export interface WideCoefficients {
  readonly repr: "wide";
  readonly values: bigint[];
}

export const DecimalSpecial = {
  finite: 0,
  nan: 1,
  positiveInfinity: 2,
  negativeInfinity: 3,
} as const;

export type VectorData =
  | { readonly layout: "bool"; readonly values: Uint8Array }
  | { readonly layout: "i32"; readonly values: Int32Array }
  | { readonly layout: "f64"; readonly values: Float64Array }
  | { readonly layout: "f32"; readonly values: Float32Array }
  | { readonly layout: "int64"; readonly values: Int64Values }
  | {
      readonly layout: "decimal";
      /** The scale of every slot, or null when `scales` gives each slot its own. */
      readonly scale: number | null;
      /** Per-slot scales (when `scale` is null). */
      readonly scales: Uint16Array | null;
      readonly coefficients: Int64Values | Int128Values | WideCoefficients;
      /** `DecimalSpecial` per slot, or null when every value is finite. */
      readonly specials: Uint8Array | null;
    }
  | {
      readonly layout: "varlen";
      /** Value i is `bytes[offsets[i] … offsets[i + 1])`. */
      readonly bytes: Uint8Array;
      readonly offsets: Int32Array;
    }
  | { readonly layout: "fixed16"; readonly bytes: Uint8Array }
  | {
      readonly layout: "interval";
      readonly months: Int32Array;
      readonly days: Int32Array;
      readonly micros: Int64Values;
    };

export interface FlatVector {
  readonly form: "flat";
  readonly type: PhysicalType;
  /** Slots in use; ≤ the capacity of the buffers. */
  length: number;
  validity: Validity | null;
  readonly data: VectorData;
}

/** One value for every row: `value` is a flat vector of length 1 (slot 0). */
export interface ConstantVector {
  readonly form: "constant";
  readonly type: PhysicalType;
  length: number;
  readonly value: FlatVector;
}

export interface DictionaryVector {
  readonly form: "dictionary";
  readonly type: PhysicalType;
  length: number;
  /** Index into `dictionary`; meaningful only where `validity` says valid. */
  readonly codes: Int32Array;
  validity: Validity | null;
  readonly dictionary: FlatVector;
  /** Process-unique identity of `dictionary`, for translation caches. */
  readonly dictionaryId: number;
}

export type Vector = FlatVector | ConstantVector | DictionaryVector;

export interface Batch {
  /** Physical rows 0 … length; ≤ BATCH_ROWS. */
  readonly length: number;
  readonly columns: readonly Vector[];
  /** Active rows, or null when all `length` rows are active. */
  readonly selection: Selection | null;
}

let nextDictionaryId = 1;
/** A fresh identity for a newly built dictionary. */
export function newDictionaryId(): number {
  return nextDictionaryId++;
}

function int64Values(capacity: number): Int64Values {
  return { repr: "safe", values: new Float64Array(capacity) };
}

/**
 * Allocates the data buffers of a flat vector for `type` at `capacity` slots. Variable-width
 * data starts with `varlenBytes` bytes; builders (varlen.ts) grow it under a grant. The caller
 * charges `dataBytes` to its grant first.
 */
export function allocateData(type: PhysicalType, capacity: number, varlenBytes = 0): VectorData {
  const layout: Layout = layoutOf(type);
  switch (layout) {
    case "bool":
      return { layout, values: new Uint8Array(capacity) };
    case "i32":
      return { layout, values: new Int32Array(capacity) };
    case "f64":
      return { layout, values: new Float64Array(capacity) };
    case "f32":
      return { layout, values: new Float32Array(capacity) };
    case "int64":
      return { layout, values: int64Values(capacity) };
    case "decimal": {
      const declared = type.kind === "decimal" ? type.scale : null;
      return {
        layout,
        scale: declared === null ? null : Math.max(0, declared),
        scales: declared === null ? new Uint16Array(capacity) : null,
        coefficients: int64Values(capacity),
        specials: null,
      };
    }
    case "varlen":
      return { layout, bytes: new Uint8Array(varlenBytes), offsets: new Int32Array(capacity + 1) };
    case "fixed16":
      return { layout, bytes: new Uint8Array(capacity * 16) };
    case "interval":
      return {
        layout,
        months: new Int32Array(capacity),
        days: new Int32Array(capacity),
        micros: int64Values(capacity),
      };
  }
}

/** Bytes `allocateData(type, capacity, varlenBytes)` allocates, charged to a grant first. */
export function dataBytes(type: PhysicalType, capacity: number, varlenBytes = 0): number {
  switch (layoutOf(type)) {
    case "bool":
      return capacity;
    case "i32":
    case "f32":
      return capacity * 4;
    case "f64":
    case "int64":
      return capacity * 8;
    case "decimal":
      return capacity * (type.kind === "decimal" && type.scale === null ? 10 : 8);
    case "varlen":
      return varlenBytes + (capacity + 1) * 4;
    case "fixed16":
    case "interval":
      return capacity * 16;
  }
}

export function allocateFlat(type: PhysicalType, capacity = BATCH_ROWS): FlatVector {
  return { form: "flat", type, length: 0, validity: null, data: allocateData(type, capacity) };
}
