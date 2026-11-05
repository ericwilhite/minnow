/**
 * The in-memory columnar representation every operator, kernel, spill page and result frame
 * uses.
 *
 * ## Shapes
 * - A `Batch` is up to `BATCH_ROWS` rows: one `Vector` per column plus an optional `Selection`
 *   naming the active rows. Rows outside the selection are present but must be ignored.
 * - A `Vector` is `flat` (one slot per row), `constant` (one value for every row), or
 *   `dictionary` (an Int32Array of codes into an immutable flat dictionary).
 * - `VectorData` is the typed storage of a flat vector, one variant per `Layout`.
 *
 * ## Value rules
 * - Validity is a bitmap, bit set = valid (Arrow's convention); `null` means every slot is valid.
 *   The data under an invalid slot is unspecified: kernels must not branch, throw or allocate on
 *   it (`1 / 0` in an invalid slot is not an error). Errors are raised only for active, valid rows.
 * - int64-family data (`Int64Values`) is exact. `repr: "safe"` holds integers in ±(2^53 − 1) in a
 *   Float64Array; `repr: "int64"` holds BigInt64Array. A producer may use `safe` only when every
 *   valid slot is proven in range (page statistics, or a checked kernel). A kernel whose result can
 *   leave the range must check and fall back to `int64`, or raise PostgreSQL's out-of-range error.
 * - `varlen` strings are UTF-8 bytes. JS strings are made only when a value leaves the engine
 *   (`varlen.ts` `decodeUtf8`), never inside operators.
 * - A dictionary's entries are distinct and non-null (null lives in the codes' validity). Two
 *   dictionaries with different `dictionaryId`s are unrelated: their codes are never compared.
 *
 * ## Ownership and lifetime (who may mutate, when buffers are recycled)
 * 1. The producer of a batch owns every buffer in it. A consumer borrows the batch for the
 *    duration of the call that hands it over (`BatchSink.push`). Anything kept past that call is
 *    copied into the consumer's own buffers (a hash table copies keys into its slabs).
 * 2. Exception: a dictionary vector's `dictionary`, and a constant vector's `value`, are immutable
 *    for the life of the query and may be referenced past the call.
 * 3. Nobody writes to a buffer they did not allocate. Kernels write only into output vectors their
 *    caller passes in.
 * 4. Operators allocate their output vectors once at `BATCH_ROWS` capacity and reuse them for
 *    every batch: the producer recycles a batch's buffers when the next `push` begins. Kernels
 *    never allocate per value and, in steady state, never per batch.
 * 5. A `Selection`'s rows are ascending and unique, each < the batch length.
 * 6. Buffers handed to a helper worker are transferred, not shared: the sender gives up the
 *    ArrayBuffer (SharedArrayBuffer is never required).
 */
import { layoutOf, type Layout, type PhysicalType } from "../types.js";

/**
 * Rows per batch. One constant, not a dial: 2,048 rows keep a column's working set in L1/L2
 * (16 KiB of f64) and let selections use Uint16Array.
 */
export const BATCH_ROWS = 2048;

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

/** Decimal coefficients past int64 (rare): one bigint per slot. */
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
  | { readonly layout: "int64"; readonly values: Int64Values }
  | {
      readonly layout: "decimal";
      /** Every coefficient is scaled by 10^scale. */
      readonly scale: number;
      readonly coefficients: Int64Values | WideCoefficients;
      /**
       * PostgreSQL's per-value display scale (≤ `scale`), or null when every value displays at
       * `scale` (always the case for a column declared NUMERIC(p, s)).
       */
      readonly displayScales: Uint16Array | null;
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

/**
 * The push protocol between pipeline operators. A source pushes batches; each operator either
 * transforms and pushes on (filter, project, probe) or absorbs them (a pipeline breaker: build,
 * aggregate, sort) and becomes the source of the next pipeline when `finish` is called.
 *
 * Kernels and `push` are synchronous. Only sources await I/O (page reads, spill reads), between
 * batches, which is also where cancellation is checked.
 */
export interface BatchSink {
  push(batch: Batch): void;
  finish(): void;
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
 * data starts with `varlenBytes` bytes; builders (varlen.ts) grow it. Allocation happens once
 * per operator, never per batch.
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
    case "int64":
      return { layout, values: int64Values(capacity) };
    case "decimal":
      return {
        layout,
        scale: type.kind === "decimal" ? (type.scale ?? 0) : 0,
        coefficients: int64Values(capacity),
        displayScales: null,
        specials: null,
      };
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

export function allocateFlat(type: PhysicalType, capacity = BATCH_ROWS): FlatVector {
  return { form: "flat", type, length: 0, validity: null, data: allocateData(type, capacity) };
}
