/** Bounded memory reads fill caller-owned buffers. No output allocation or implicit widening. */
import type { MemoryOutputLayout, MemoryOutputNeed } from "../access/snapshot.js";
import type { PhysicalType } from "../types.js";
import { layoutOf } from "../types.js";
import { int128At, setInt128, INT128_MIN, INT128_MAX } from "../vector/int128.js";
import type { FlatVector, Int64Values, VectorData } from "../vector/vector.js";

/** Publication-independent output tier: physical page encodings never require a wider tier. */
export function memoryOutputLayout(type: PhysicalType): MemoryOutputLayout {
  const layout = layoutOf(type);
  return {
    layout,
    validity: true,
    integer:
      layout === "decimal"
        ? "int128"
        : layout === "int64" || layout === "interval"
          ? "int64"
          : null,
    scales: layout === "decimal",
    scale: null,
    specials: layout === "decimal",
  };
}

export function memoryOutputBytes(
  shape: MemoryOutputLayout,
  rows: number,
  varlenBytes: number,
): number {
  let bytes = rows * 4 + (shape.validity ? Math.ceil(rows / 32) * 4 : 0); // codes + validity
  switch (shape.layout) {
    case "bool":
      bytes += rows;
      break;
    case "i32":
    case "f32":
      bytes += rows * 4;
      break;
    case "f64":
    case "int64":
      bytes += rows * 8;
      break;
    case "fixed16":
    case "interval":
      bytes += rows * 16;
      break;
    case "varlen":
      bytes += (rows + 1) * 4 + varlenBytes;
      break;
    case "decimal":
      bytes +=
        rows *
        ((shape.integer === "int128" ? 16 : 8) + (shape.scales ? 2 : 0) + (shape.specials ? 1 : 0));
      break;
  }
  return bytes;
}

function integerAt(data: Int64Values, row: number): bigint {
  return data.repr === "safe" ? BigInt(data.values[row] ?? 0) : (data.values[row] ?? 0n);
}
function integerFits(data: Int64Values, value: bigint, row: number): boolean {
  return (
    row < data.values.length &&
    (data.repr === "int64"
      ? value >= -(2n ** 63n) && value < 2n ** 63n
      : Number.isSafeInteger(Number(value)))
  );
}
function putInteger(data: Int64Values, value: bigint, row: number): void {
  if (data.repr === "int64") data.values[row] = value;
  else data.values[row] = Number(value);
}
export function memoryValid(vector: FlatVector, row: number): boolean {
  return vector.validity === null || (((vector.validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) !== 0;
}
export function memoryValueBytes(vector: FlatVector, row: number): number {
  return memoryValid(vector, row) && vector.data.layout === "varlen"
    ? (vector.data.offsets[row + 1] ?? 0) - (vector.data.offsets[row] ?? 0)
    : 0;
}

/** Exact next-row need, with enough capacity for the completed prefix and the next row. */
export function memoryNeed(
  columnId: number,
  row: number,
  rows: number,
  shape: MemoryOutputLayout,
  varlenBytes: number,
): MemoryOutputNeed {
  return {
    columnId,
    row,
    rows,
    shape,
    varlenBytes,
    bytes: memoryOutputBytes(shape, rows, varlenBytes),
  };
}

/** Whether a prepared row can be written, without changing any buffer or output metadata. */
export function acceptsMemoryRow(
  target: FlatVector,
  source: FlatVector,
  from: number,
  at: number,
  varlenBytes: number,
): boolean {
  const s = source.data,
    t = target.data;
  if (s.layout !== t.layout || (target.validity !== null && target.validity.length * 32 <= at))
    return false;
  const valid = memoryValid(source, from);
  if (!valid && target.validity === null) return false;
  switch (t.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64":
      return t.values.length > at;
    case "varlen":
      return t.offsets.length > at + 1 && t.bytes.length >= varlenBytes;
    case "fixed16":
      return t.bytes.length >= (at + 1) * 16;
    case "int64":
      return (
        s.layout === "int64" && integerFits(t.values, valid ? integerAt(s.values, from) : 0n, at)
      );
    case "interval":
      return (
        s.layout === "interval" &&
        t.months.length > at &&
        t.days.length > at &&
        integerFits(t.micros, valid ? integerAt(s.micros, from) : 0n, at)
      );
    case "decimal": {
      if (s.layout !== "decimal") return false;
      const scale = s.scale ?? s.scales?.[from] ?? 0;
      const special = valid ? (s.specials?.[from] ?? 0) : 0;
      if (
        (t.scale === null
          ? t.scales === null || t.scales.length <= at
          : valid && t.scale !== scale) ||
        (t.specials === null ? special !== 0 : t.specials.length <= at)
      )
        return false;
      const c = t.coefficients;
      // The caller owns/precharges wide pointer and BigInt slot capacity (ChunkOutput).
      // Native storage's 128-bit bound does not narrow a stronger prepared destination.
      if (c.repr === "wide") return c.values.length > at;
      const value = valid ? coefficient(s, from) : 0n;
      return c.repr === "int128" ? c.limbs.length >= (at + 1) * 4 : integerFits(c, value, at);
    }
  }
}
function coefficient(data: Extract<VectorData, { layout: "decimal" }>, row: number): bigint {
  const c = data.coefficients;
  return c.repr === "int128"
    ? int128At(c.limbs, row)
    : c.repr === "wide"
      ? (c.values[row] ?? 0n)
      : integerAt(c, row);
}

/** Describe the least layout for this row; a caller can instead prepare the hint's stable tier. */
export function memoryRowLayout(source: FlatVector, row: number): MemoryOutputLayout {
  const s = source.data,
    valid = memoryValid(source, row);
  let integer: MemoryOutputLayout["integer"] = null;
  if (s.layout === "int64" || s.layout === "interval") {
    const value = valid ? integerAt(s.layout === "int64" ? s.values : s.micros, row) : 0n;
    integer = Number.isSafeInteger(Number(value)) ? "safe" : "int64";
  } else if (s.layout === "decimal") {
    const value = valid ? coefficient(s, row) : 0n;
    integer = Number.isSafeInteger(Number(value))
      ? "safe"
      : value >= -(2n ** 63n) && value < 2n ** 63n
        ? "int64"
        : "int128";
    if (value < INT128_MIN || value > INT128_MAX)
      throw new RangeError("A memory column exceeds its stored decimal tier");
  }
  return {
    layout: s.layout,
    validity: !valid,
    integer,
    scales: s.layout === "decimal" && s.scale === null,
    scale: s.layout === "decimal" ? s.scale : null,
    specials: s.layout === "decimal" && valid && (s.specials?.[row] ?? 0) !== 0,
  };
}

/** Copies only a row that acceptsMemoryRow already admitted. All storage belongs to the caller. */
export function copyMemoryRow(
  source: FlatVector,
  from: number,
  target: FlatVector,
  at: number,
): void {
  const valid = memoryValid(source, from),
    s = source.data,
    t = target.data;
  if (target.validity !== null) {
    const word = at >>> 5,
      mask = 1 << (at & 31);
    target.validity[word] = valid
      ? (target.validity[word] ?? 0) | mask
      : (target.validity[word] ?? 0) & ~mask;
  }
  if (s.layout === "varlen" && t.layout === "varlen") {
    const begin = at === 0 ? 0 : (t.offsets[at] ?? 0);
    if (at === 0) t.offsets[0] = 0;
    const length = memoryValueBytes(source, from);
    if (valid)
      t.bytes.set(s.bytes.subarray(s.offsets[from] ?? 0, (s.offsets[from] ?? 0) + length), begin);
    t.offsets[at + 1] = begin + length;
  } else if (valid) {
    switch (s.layout) {
      case "bool":
      case "i32":
        if (t.layout === s.layout) t.values[at] = s.values[from] ?? 0;
        break;
      case "f32":
      case "f64":
        // Copy raw bytes, preserving signaling NaN payloads as well as -0.
        if (t.layout === s.layout) {
          const width = s.values.BYTES_PER_ELEMENT;
          new Uint8Array(t.values.buffer, t.values.byteOffset + at * width, width).set(
            new Uint8Array(s.values.buffer, s.values.byteOffset + from * width, width),
          );
        }
        break;
      case "int64":
        if (t.layout === "int64") putInteger(t.values, integerAt(s.values, from), at);
        break;
      case "fixed16":
        if (t.layout === "fixed16")
          t.bytes.set(s.bytes.subarray(from * 16, (from + 1) * 16), at * 16);
        break;
      case "interval":
        if (t.layout === "interval") {
          t.months[at] = s.months[from] ?? 0;
          t.days[at] = s.days[from] ?? 0;
          putInteger(t.micros, integerAt(s.micros, from), at);
        }
        break;
      case "decimal":
        if (t.layout === "decimal") {
          if (t.scales !== null) t.scales[at] = s.scale ?? s.scales?.[from] ?? 0;
          if (t.specials !== null) t.specials[at] = s.specials?.[from] ?? 0;
          const value = coefficient(s, from),
            c = t.coefficients;
          if (c.repr === "int128") setInt128(c.limbs, at, value);
          else if (c.repr === "wide") c.values[at] = value;
          else putInteger(c, value, at);
        }
        break;
    }
  }
  target.length = at + 1;
}
