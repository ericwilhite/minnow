/**
 * Test support for the page suites: JS values ⇄ vectors, and a write-then-read round trip
 * through `GroupWriter`, the in-memory data space and `ChunkReader`.
 */
import type { ColumnChunk } from "../format/manifest.js";
import type { PhysicalType } from "../types.js";
import { allocateValidity, setValid } from "../vector/validity.js";
import {
  DecimalSpecial,
  type FlatVector,
  type Int64Values,
  type VectorData,
} from "../vector/vector.js";
import { int128At, setInt128 } from "../vector/int128.js";
import { MemoryDataSpace, MemoryExtentAllocator, MemorySpace } from "./memory-extents.js";
import { ChunkReader } from "./reader.js";
import { GroupWriter, type GroupWriterOptions, type WrittenGroup } from "./writer.js";

export interface IntervalValue {
  readonly months: number;
  readonly days: number;
  readonly micros: bigint;
}
export interface DecimalValue {
  readonly coefficient: bigint;
  readonly special: number;
  /** Per-value scale (a mixed-scale vector); absent: the vector's one scale. */
  readonly scale?: number;
}
export type TestValue =
  boolean | number | bigint | Uint8Array | IntervalValue | DecimalValue | null;

/** `value`, or a thrown error when it is undefined (tests index arrays they just built). */
export function defined<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Expected a value");
  return value;
}

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

export interface VectorOptions {
  /** int64 family: build the f64 `safe` representation when every value allows it. */
  readonly safe?: boolean;
  /** decimal: coefficient tier. */
  readonly tier?: "int64" | "int128";
  readonly scale?: number;
}

/** A flat vector holding `values` (null = invalid slot, with garbage under it). */
export function vectorOf(
  type: PhysicalType,
  values: readonly TestValue[],
  options: VectorOptions = {},
): FlatVector {
  const length = values.length;
  const validity = values.some((value) => value === null)
    ? allocateValidity(Math.max(1, length))
    : null;
  if (validity !== null)
    for (let row = 0; row < length; row++) setValid(validity, row, values[row] !== null);
  const data = dataOf(type, values, options);
  return { form: "flat", type, length, validity, data };
}

function int64Of(values: ReadonlyArray<bigint | null>, safe: boolean): Int64Values {
  const allSafe = values.every(
    (value) => value === null || (value >= -MAX_SAFE && value <= MAX_SAFE),
  );
  if (safe && allSafe) {
    const out = new Float64Array(values.length);
    values.forEach((value, row) => (out[row] = value === null ? 12345 : Number(value)));
    return { repr: "safe", values: out };
  }
  const out = new BigInt64Array(values.length);
  values.forEach((value, row) => (out[row] = value ?? -7n));
  return { repr: "int64", values: out };
}

function dataOf(
  type: PhysicalType,
  values: readonly TestValue[],
  options: VectorOptions,
): VectorData {
  const length = values.length;
  switch (type.kind) {
    case "bool":
      return {
        layout: "bool",
        values: Uint8Array.from(values, (value) => (value === true ? 1 : value === null ? 7 : 0)),
      };
    case "int32":
    case "date":
      return {
        layout: "i32",
        values: Int32Array.from(values, (value) => (value === null ? -99 : (value as number))),
      };
    case "int64":
    case "time":
    case "timestamp":
      return {
        layout: "int64",
        values: int64Of(values as Array<bigint | null>, options.safe ?? false),
      };
    case "float64":
      return {
        layout: "f64",
        values: Float64Array.from(values, (value) =>
          value === null ? Number.NaN : (value as number),
        ),
      };
    case "float32":
      return {
        layout: "f32",
        values: Float32Array.from(values, (value) => (value === null ? 3 : (value as number))),
      };
    case "uuid": {
      const bytes = new Uint8Array(length * 16);
      values.forEach((value, row) => {
        if (value instanceof Uint8Array) bytes.set(value, row * 16);
        else bytes.fill(0xab, row * 16, row * 16 + 16);
      });
      return { layout: "fixed16", bytes };
    }
    case "interval": {
      const months = new Int32Array(length);
      const days = new Int32Array(length);
      const micros: Array<bigint | null> = [];
      values.forEach((value, row) => {
        const interval = value as IntervalValue | null;
        months[row] = interval?.months ?? 5;
        days[row] = interval?.days ?? 6;
        micros.push(interval?.micros ?? null);
      });
      return { layout: "interval", months, days, micros: int64Of(micros, options.safe ?? false) };
    }
    case "decimal": {
      const decimals = values as Array<DecimalValue | null>;
      const specials = decimals.some((value) => (value?.special ?? 0) !== 0)
        ? new Uint8Array(length)
        : null;
      decimals.forEach((value, row) => {
        if (specials !== null) specials[row] = value?.special ?? 0;
      });
      const perSlot = decimals.some((value) => value?.scale !== undefined);
      const scale = perSlot ? null : (options.scale ?? type.scale ?? 0);
      const scales = perSlot ? Uint16Array.from(decimals, (value) => value?.scale ?? 0) : null;
      if (options.tier === "int128") {
        const limbs = new Uint32Array(length * 4);
        decimals.forEach((value, row) =>
          setInt128(limbs, row, value?.special === 0 ? value.coefficient : 0n),
        );
        return {
          layout: "decimal",
          scale,
          scales,
          coefficients: { repr: "int128", limbs },
          specials,
        };
      }
      const coefficients = int64Of(
        decimals.map((value) =>
          value === null ? null : value.special === 0 ? value.coefficient : 0n,
        ),
        options.safe ?? false,
      );
      return { layout: "decimal", scale, scales, coefficients, specials };
    }
    default: {
      const parts = values.map((value) =>
        value instanceof Uint8Array ? value : new Uint8Array(0),
      );
      const offsets = new Int32Array(length + 1);
      let total = 0;
      parts.forEach((part, row) => {
        total += part.length;
        offsets[row + 1] = total;
      });
      const bytes = new Uint8Array(total);
      parts.forEach((part, row) => bytes.set(part, offsets[row] ?? 0));
      return { layout: "varlen", bytes, offsets };
    }
  }
}

export interface ReadOptions {
  /** int64 family: force the BigInt64Array target even when the chunk is safe. */
  readonly forceInt64?: boolean;
}

/** An empty target vector for `rows` slots of the reader's chunk. */
export function targetFor(
  reader: ChunkReader,
  rows: number,
  varlenBytes: number,
  options: ReadOptions = {},
): FlatVector {
  const type = reader.chunk.type;
  const capacity = Math.max(1, rows);
  const validity = allocateValidity(capacity);
  const safe = !options.forceInt64 && reader.isSafe();
  const int64 = (): Int64Values =>
    safe
      ? { repr: "safe", values: new Float64Array(capacity) }
      : { repr: "int64", values: new BigInt64Array(capacity) };
  let data: VectorData;
  switch (type.kind) {
    case "bool":
      data = { layout: "bool", values: new Uint8Array(capacity) };
      break;
    case "int32":
    case "date":
      data = { layout: "i32", values: new Int32Array(capacity) };
      break;
    case "int64":
    case "time":
    case "timestamp":
      data = { layout: "int64", values: int64() };
      break;
    case "float64":
      data = { layout: "f64", values: new Float64Array(capacity) };
      break;
    case "float32":
      data = { layout: "f32", values: new Float32Array(capacity) };
      break;
    case "uuid":
      data = { layout: "fixed16", bytes: new Uint8Array(capacity * 16) };
      break;
    case "interval": {
      const micros: Int64Values =
        reader.isSafe(0, reader.directory.pages.length) && !options.forceInt64
          ? { repr: "safe", values: new Float64Array(capacity) }
          : { repr: "int64", values: new BigInt64Array(capacity) };
      data = {
        layout: "interval",
        months: new Int32Array(capacity),
        days: new Int32Array(capacity),
        micros,
      };
      break;
    }
    case "decimal":
      data = {
        layout: "decimal",
        scale: reader.directory.scale,
        scales: reader.directory.scale === null ? new Uint16Array(capacity) : null,
        coefficients: reader.needsInt128
          ? { repr: "int128", limbs: new Uint32Array(capacity * 4) }
          : int64(),
        specials: reader.hasDecimalSpecials ? new Uint8Array(capacity) : null,
      };
      break;
    default:
      data = {
        layout: "varlen",
        bytes: new Uint8Array(varlenBytes),
        offsets: new Int32Array(capacity + 1),
      };
  }
  return { form: "flat", type, length: rows, validity, data };
}

/** Slot `row` of a flat vector as a test value. */
export function valueAt(vector: FlatVector, row: number): TestValue {
  if (vector.validity !== null && (((vector.validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 0)
    return null;
  const data = vector.data;
  const int64At = (values: Int64Values, index: number): bigint =>
    values.repr === "safe" ? BigInt(values.values[index] ?? 0) : (values.values[index] ?? 0n);
  switch (data.layout) {
    case "bool":
      return (data.values[row] ?? 0) === 1;
    case "i32":
    case "f64":
    case "f32":
      return data.values[row] ?? 0;
    case "int64":
      return int64At(data.values, row);
    case "fixed16":
      return data.bytes.slice(row * 16, row * 16 + 16);
    case "varlen":
      return data.bytes.slice(data.offsets[row] ?? 0, data.offsets[row + 1] ?? 0);
    case "interval":
      return {
        months: data.months[row] ?? 0,
        days: data.days[row] ?? 0,
        micros: int64At(data.micros, row),
      };
    case "decimal": {
      const special = data.specials?.[row] ?? DecimalSpecial.finite;
      const coefficients = data.coefficients;
      const coefficient =
        special !== 0
          ? 0n
          : coefficients.repr === "int128"
            ? int128At(coefficients.limbs, row)
            : coefficients.repr === "wide"
              ? (coefficients.values[row] ?? 0n)
              : int64At(coefficients, row);
      return data.scales === null
        ? { coefficient, special }
        : { coefficient, special, scale: data.scales[row] ?? 0 };
    }
  }
}

/** Equality as stored: floats bit-for-bit except that every NaN is one value. */
export function sameValue(a: TestValue, b: TestValue): boolean {
  if (a === null || b === null) return a === b;
  if (typeof a === "number" && typeof b === "number")
    return Object.is(a, b) || (a !== a && b !== b);
  if (a instanceof Uint8Array && b instanceof Uint8Array)
    return a.length === b.length && a.every((byte, index) => byte === b[index]);
  if (typeof a === "object" && typeof b === "object")
    return JSON.stringify(a, big) === JSON.stringify(b, big);
  return a === b;
}

function big(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? `${value.toString()}n` : value;
}

export interface Written {
  readonly space: MemoryDataSpace;
  readonly allocator: MemoryExtentAllocator;
  readonly group: WrittenGroup;
}

/** Writes `columns` (one value array each) as one group, in batches of `batch` rows. */
export async function writeGroup(
  columns: ReadonlyArray<{
    readonly type: PhysicalType;
    readonly values: readonly TestValue[];
    readonly vector?: VectorOptions;
  }>,
  options: Partial<GroupWriterOptions> & { readonly batch?: number } = {},
): Promise<Written> {
  const space = new MemoryDataSpace();
  const allocator = new MemoryExtentAllocator();
  const writer = new GroupWriter({
    ...options,
    columns: columns.map((column, index) => ({ columnId: index + 1, type: column.type })),
    store: space,
    allocator,
  });
  const rows = columns[0]?.values.length ?? 0;
  const batch = Math.max(1, options.batch ?? 2048);
  for (let from = 0; from < rows; from += batch) {
    const count = Math.min(batch, rows - from);
    const vectors = columns.map((column) =>
      vectorOf(column.type, column.values.slice(from, from + count), column.vector),
    );
    const taken = await writer.append(vectors, count);
    if (taken !== count) throw new Error("The group refused rows");
  }
  return { space, allocator, group: await writer.finish() };
}

/** A budget that admits everything (tests that do not check charging). */
export const UNLIMITED = { allocate: (): boolean => true, free: (): void => undefined };

/** A reader over `data` with no cache: every load reads (and verifies) the stored bytes. */
export async function openChunk(
  data: MemoryDataSpace,
  chunk: ColumnChunk,
  budget = UNLIMITED,
): Promise<ChunkReader> {
  return ChunkReader.open(new MemorySpace(data, { cache: false }), chunk, { budget });
}

/** Reads every row of a chunk back as test values. */
export async function readChunk(
  space: MemoryDataSpace,
  chunk: ColumnChunk,
  options: ReadOptions = {},
): Promise<TestValue[]> {
  const reader = await openChunk(space, chunk);
  const rows = reader.rowCount;
  const [first, end] = reader.pagesInRange(0, rows);
  await reader.load(Array.from({ length: end - first }, (_, index) => first + index));
  const target = targetFor(reader, rows, reader.varlenBytes(0, rows), options);
  reader.decodeRange(0, rows, target, 0);
  return Array.from({ length: rows }, (_, row) => valueAt(target, row));
}
