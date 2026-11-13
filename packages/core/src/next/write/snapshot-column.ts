/** Portable column1: fixed values are canonical LE lanes, floats copied as words.
 * Varlen rows: NULL=0; otherwise DEFINE=1,u32 length,bytes,REF=2,u32 code0.
 * DEFINE replaces the single-entry dictionary; REF always follows the complete definition.
 * This bounded dictionary grammar preserves bytes without flattening a large value or making strings.
 */
import { DURABLE_CODECS, DURABLE_FORMATS, requireHeader } from "../durable-format.js";
import { SnapshotFormatError } from "../format/snapshot.js";
import { layoutOf, type PhysicalType } from "../types.js";
import { int64At } from "../vector/int64.js";
import { int128At } from "../vector/int128.js";
import type { FlatVector } from "../vector/vector.js";

export const SNAPSHOT_BLOCK_ROWS = 256;
export const SNAPSHOT_COLUMN_HEADER_BYTES = 16;
export function snapshotValueWidth(type: PhysicalType): number {
  switch (layoutOf(type)) {
    case "bool":
      return 1;
    case "i32":
    case "f32":
      return 4;
    case "int64":
    case "f64":
      return 8;
    case "decimal":
      return 19;
    case "fixed16":
    case "interval":
      return 16;
    case "varlen":
      return 0;
  }
}
export function encodeSnapshotColumnHeader(
  type: PhysicalType,
  rows: number,
  into: Uint8Array,
): void {
  if (into.length !== 16 || !Number.isInteger(rows) || rows < 1 || rows > SNAPSHOT_BLOCK_ROWS)
    throw new SnapshotFormatError("Invalid portable column row count/header");
  const tag = DURABLE_CODECS.column[layoutOf(type)];
  if (type.kind === "locator")
    throw new SnapshotFormatError("A locator is not a portable stored value");
  into.fill(0);
  const view = new DataView(into.buffer, into.byteOffset, into.byteLength);
  view.setUint32(0, DURABLE_FORMATS.snapshotColumn.magic, true);
  into[4] = 1;
  into[5] = tag;
  view.setUint32(8, rows, true);
}
export function decodeSnapshotColumnHeader(type: PhysicalType, bytes: Uint8Array): number {
  if (bytes.length !== 16) throw new SnapshotFormatError("Invalid portable column header length");
  requireHeader("snapshotColumn", bytes, SnapshotFormatError);
  const rows = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(8, true);
  if (
    type.kind === "locator" ||
    bytes[5] !== DURABLE_CODECS.column[layoutOf(type)] ||
    rows < 1 ||
    rows > SNAPSHOT_BLOCK_ROWS
  )
    throw new SnapshotFormatError("Portable column type/rows mismatch");
  return rows;
}
export function snapshotValid(vector: FlatVector, row: number): boolean {
  return vector.validity === null || ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
}
/** Caller supplies at least32 charged bytes, emits just the returned prefix. */
export function snapshotFixedValue(vector: FlatVector, row: number, into: Uint8Array): number {
  if (into.length < 32) throw new SnapshotFormatError("Short portable value scratch");
  const data = vector.data,
    view = new DataView(into.buffer, into.byteOffset, into.byteLength);
  into[0] = snapshotValid(vector, row) ? 1 : 0;
  if (into[0] === 0) return 1;
  switch (data.layout) {
    case "bool":
      into[1] = data.values[row] ?? 0;
      break;
    case "i32":
      view.setInt32(1, data.values[row] ?? 0, true);
      break;
    case "f32":
    case "f64": {
      const width = data.values.BYTES_PER_ELEMENT;
      into.set(new Uint8Array(data.values.buffer, data.values.byteOffset + row * width, width), 1);
      break;
    }
    case "int64":
      view.setBigInt64(1, int64At(data.values, row), true);
      break;
    case "decimal": {
      const c = data.coefficients;
      const value =
        c.repr === "int128"
          ? int128At(c.limbs, row)
          : c.repr === "wide"
            ? (c.values[row] ?? 0n)
            : int64At(c, row);
      if (BigInt.asIntN(128, value) !== value)
        throw new SnapshotFormatError("Portable stored decimal exceeds int128");
      const bits = BigInt.asUintN(128, value);
      view.setBigUint64(1, BigInt.asUintN(64, bits), true);
      view.setBigUint64(9, bits >> 64n, true);
      view.setUint16(17, data.scale ?? data.scales?.[row] ?? 0, true);
      into[19] = data.specials?.[row] ?? 0;
      break;
    }
    case "fixed16":
      into.set(data.bytes.subarray(row * 16, row * 16 + 16), 1);
      break;
    case "interval":
      view.setInt32(1, data.months[row] ?? 0, true);
      view.setInt32(5, data.days[row] ?? 0, true);
      view.setBigInt64(9, int64At(data.micros, row), true);
      break;
    case "varlen":
      throw new SnapshotFormatError("Varlen values require streamed dictionary records");
  }
  return 1 + snapshotValueWidth(vector.type);
}
