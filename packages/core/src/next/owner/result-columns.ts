/** Native column views, with one lazy materialization for constants/dictionaries/concatenation. */
import { layoutOf, type PhysicalType } from "../types.js";
import { allocateData, type VectorData } from "../vector/vector.js";
import type { ColumnView } from "./api.js";
import type { WireFlat } from "./frame.js";
import { bigAt, coefficientAt, required } from "./result-values.js";
export interface Slot {
  readonly flat: WireFlat;
  readonly row: number;
}
export const valid = (flat: WireFlat, row: number): boolean =>
  flat.validity === null || ((required(flat.validity[row >>> 5]) >>> (row & 31)) & 1) !== 0;
export function columnView(type: PhysicalType, flat: WireFlat): ColumnView {
  const validity =
    flat.validity === null
      ? null
      : new Uint8Array(flat.validity.buffer, flat.validity.byteOffset, flat.validity.byteLength);
  const data = flat.data;
  switch (data.layout) {
    case "bool":
      return { kind: "bool", values: data.values, validity };
    case "i32":
      return type.kind === "date"
        ? { kind: "date", unit: "days", epoch: "2000-01-01", values: data.values, validity }
        : {
            kind: "int32",
            bits: type.kind === "int32" ? type.bits : 32,
            values: data.values,
            validity,
          };
    case "f64":
      return { kind: "float64", values: data.values, validity };
    case "f32":
      return { kind: "float32", values: data.values, validity };
    case "int64":
      if (type.kind === "timestamp")
        return {
          kind: "timestamp",
          zoned: type.zoned,
          unit: "microseconds",
          epoch: "2000-01-01T00:00:00Z",
          ...data.values,
          validity,
        };
      if (type.kind === "time")
        return { kind: "time", unit: "microseconds", ...data.values, validity };
      return { kind: "int64", ...data.values, validity };
    case "decimal":
      return {
        kind: "decimal",
        scale: data.scale,
        scales: data.scales,
        coefficients: data.coefficients,
        specials: data.specials,
        validity,
      };
    case "varlen":
      return {
        kind:
          type.kind === "bytes" ||
          type.kind === "array" ||
          type.kind === "json" ||
          type.kind === "enum"
            ? type.kind
            : "text",
        encoding: "plain",
        bytes: data.bytes,
        offsets: data.offsets,
        validity,
      };
    case "fixed16":
      return { kind: "uuid", bytes: data.bytes, validity };
    case "interval":
      return {
        kind: "interval",
        months: data.months,
        days: data.days,
        micros: data.micros,
        validity,
      };
  }
}
/** Materialization belongs to the consuming client, outside the owner's execution grant.
 * NULL storage is uninterpreted, including unsafe int64/decimal garbage. */
export function materialize(
  type: PhysicalType,
  length: number,
  slot: (row: number) => Slot,
): WireFlat {
  let bytes = 0;
  if (layoutOf(type) === "varlen")
    for (let i = 0; i < length; i++) {
      const { flat, row } = slot(i);
      if (valid(flat, row) && flat.data.layout === "varlen")
        bytes += required(flat.data.offsets[row + 1]) - required(flat.data.offsets[row]);
    }
  const layout = layoutOf(type);
  let data: VectorData;
  if (layout === "int64")
    data = { layout, values: { repr: "int64", values: new BigInt64Array(length) } };
  else if (layout === "decimal")
    data = {
      layout,
      scale: null,
      scales: new Uint16Array(length),
      coefficients: { repr: "wide", values: Array<bigint>(length).fill(0n) },
      specials: new Uint8Array(length),
    };
  else if (layout === "interval")
    data = {
      layout,
      months: new Int32Array(length),
      days: new Int32Array(length),
      micros: { repr: "int64", values: new BigInt64Array(length) },
    };
  else data = allocateData(type, length, bytes);
  const validity = new Uint32Array(Math.ceil(length / 32));
  let allValid = true,
    offset = 0;
  for (let i = 0; i < length; i++) {
    const { flat, row } = slot(i),
      source = flat.data;
    if (!valid(flat, row)) {
      allValid = false;
    } else {
      validity[i >>> 5] = (validity[i >>> 5] ?? 0) | (1 << (i & 31));
      if (data.layout === "bool" && source.layout === "bool")
        data.values[i] = required(source.values[row]);
      else if (data.layout === "i32" && source.layout === "i32")
        data.values[i] = required(source.values[row]);
      else if (data.layout === "f64" && source.layout === "f64")
        data.values[i] = required(source.values[row]);
      else if (data.layout === "f32" && source.layout === "f32")
        data.values[i] = required(source.values[row]);
      else if (data.layout === "int64" && source.layout === "int64") {
        if (data.values.repr !== "int64") throw new TypeError("Expected int64 destination");
        data.values.values[i] = bigAt(source.values, row);
      } else if (data.layout === "decimal" && source.layout === "decimal") {
        if (data.coefficients.repr !== "wide") throw new TypeError("Expected wide destination");
        required(data.specials)[i] = source.specials?.[row] ?? 0;
        if (required(data.specials)[i] === 0)
          data.coefficients.values[i] = coefficientAt(source, row);
        required(data.scales)[i] = source.scale ?? required(required(source.scales)[row]);
      } else if (data.layout === "varlen" && source.layout === "varlen") {
        const value = source.bytes.subarray(
          required(source.offsets[row]),
          required(source.offsets[row + 1]),
        );
        data.bytes.set(value, offset);
        offset += value.length;
      } else if (data.layout === "fixed16" && source.layout === "fixed16")
        data.bytes.set(source.bytes.subarray(row * 16, (row + 1) * 16), i * 16);
      else if (data.layout === "interval" && source.layout === "interval") {
        data.months[i] = required(source.months[row]);
        data.days[i] = required(source.days[row]);
        if (data.micros.repr !== "int64") throw new TypeError("Expected int64 destination");
        data.micros.values[i] = bigAt(source.micros, row);
      } else throw new TypeError("Result column layout changed between frames");
    }
    if (data.layout === "varlen") data.offsets[i + 1] = offset;
  }
  return { form: "flat", length, data, validity: allValid ? null : validity };
}
