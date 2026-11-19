/** One column grammar, shared by native replay and the page's rekey decoder.
 * The caller supplies bounded byte access and its boundary-specific error class. */
import {
  DURABLE_CODECS,
  requireHeader,
  requireTag,
  type FormatErrorType,
} from "../durable-format.js";
import { layoutOf, type PhysicalType } from "../types.js";
import type { FlatVector, Int64Values, Validity, VectorData } from "./vector.js";
export interface ColumnReader {
  offset: number;
  u8(): number;
  varuint(): number;
  raw(count: number): Uint8Array;
  end(): void;
}
type DecimalData = Extract<VectorData, { layout: "decimal" }>;
const LAYOUT_TAG = DURABLE_CODECS.column;
const bitValid = (validity: Validity | null, row: number): boolean =>
  validity === null || ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;

function readInt64(reader: ColumnReader, count: number, Corruption: FormatErrorType): Int64Values {
  const repr = reader.u8();
  requireTag(repr, DURABLE_CODECS.integerRepr, "integer representation", Corruption);
  const bytes = reader.raw(count * 8).slice();
  if (repr === 0) return { repr: "safe", values: new Float64Array(bytes.buffer) };
  if (repr === 1) return { repr: "int64", values: new BigInt64Array(bytes.buffer) };
  throw new Corruption("Bad int64 representation in a log column");
}

function readFlag(reader: ColumnReader, Corruption: FormatErrorType): boolean {
  const value = reader.u8();
  requireTag(value, DURABLE_CODECS.boolean, "column flag", Corruption);
  return value === 1;
}

/** Decodes an encoded column into a fresh flat vector of `type`. */
export function decodeColumnBytes(
  bytes: Uint8Array,
  type: PhysicalType,
  Reader: new (bytes: Uint8Array) => ColumnReader,
  Corruption: FormatErrorType,
): FlatVector {
  requireHeader("column", bytes, Corruption);
  const reader = new Reader(bytes);
  const tag = reader.u8();
  const layout = layoutOf(type);
  if (LAYOUT_TAG[layout] !== tag)
    throw new Corruption("A log column's layout differs from its type");
  const count = reader.varuint();
  const validity = readFlag(reader, Corruption)
    ? new Uint32Array(reader.raw(Math.ceil(count / 32) * 4).slice().buffer)
    : null;
  if (
    validity !== null &&
    (count & 31) !== 0 &&
    (validity[validity.length - 1] ?? 0) >>> (count & 31) !== 0
  )
    throw new Corruption("Nonzero log column validity padding");
  const copy = (bytesPer: number) => reader.raw(count * bytesPer).slice().buffer;
  let data: VectorData;
  switch (layout) {
    case "bool": {
      const values = new Uint8Array(copy(1));
      for (let row = 0; row < count; row++)
        if (bitValid(validity, row))
          requireTag(values[row] ?? 0, DURABLE_CODECS.boolean, "log boolean", Corruption);
      data = { layout, values };
      break;
    }
    case "i32":
      data = { layout, values: new Int32Array(copy(4)) };
      break;
    case "f64":
      data = { layout, values: new Float64Array(copy(8)) };
      break;
    case "f32":
      data = { layout, values: new Float32Array(copy(4)) };
      break;
    case "int64":
      data = { layout, values: readInt64(reader, count, Corruption) };
      break;
    case "decimal": {
      const mixed = readFlag(reader, Corruption);
      const scales = mixed ? new Uint16Array(copy(2)) : null;
      const scale = mixed ? null : reader.varuint();
      const specials = readFlag(reader, Corruption) ? reader.raw(count).slice() : null;
      if (specials !== null)
        for (let row = 0; row < count; row++)
          if (bitValid(validity, row))
            requireTag(
              specials[row] ?? 0,
              DURABLE_CODECS.decimalSpecial,
              "decimal special",
              Corruption,
            );
      const tier = reader.u8();
      requireTag(tier, DURABLE_CODECS.decimalRepr, "decimal representation", Corruption);
      let coefficients: DecimalData["coefficients"];
      if (tier === 2) coefficients = { repr: "int128", limbs: new Uint32Array(copy(16)) };
      else {
        reader.offset -= 1;
        coefficients = readInt64(reader, count, Corruption);
      }
      data = { layout, scale, scales, coefficients, specials };
      break;
    }
    case "varlen": {
      const relative = new Int32Array(reader.raw((count + 1) * 4).slice().buffer);
      const values = reader.raw(relative[count] ?? 0).slice();
      data = { layout, bytes: values, offsets: relative };
      break;
    }
    case "fixed16":
      data = { layout, bytes: new Uint8Array(copy(16)) };
      break;
    case "interval": {
      const months = new Int32Array(copy(4));
      const days = new Int32Array(copy(4));
      data = { layout, months, days, micros: readInt64(reader, count, Corruption) };
      break;
    }
  }
  reader.end();
  return { form: "flat", type, length: count, validity, data };
}
