/**
 * Column arrays to Arrow vectors for DuckDB's `insertArrowTable`, built directly from typed
 * buffers. This is the conversion measurements.md reports separately as "convert": it is the
 * price of handing JavaScript values to DuckDB, not part of DuckDB's own load time.
 */
import * as arrow from "apache-arrow";

const ENCODER = new TextEncoder();

type Value = number | string | boolean | null;

function setBit(bits: Uint8Array, index: number): void {
  bits[index >> 3] = (bits[index >> 3] ?? 0) | (1 << (index & 7));
}

export function utf8Vector(values: readonly Value[]): arrow.Vector {
  const n = values.length;
  const offsets = new Int32Array(n + 1);
  let capacity = 0;
  let nulls = 0;
  for (const value of values) {
    if (value === null) nulls++;
    else capacity += String(value).length * 3;
  }
  const bytes = new Uint8Array(capacity);
  const bitmap = nulls > 0 ? new Uint8Array((n + 7) >> 3) : undefined;
  let position = 0;
  for (let i = 0; i < n; i++) {
    const value = values[i];
    if (value !== null && value !== undefined) {
      position += ENCODER.encodeInto(String(value), bytes.subarray(position)).written;
      if (bitmap !== undefined) setBit(bitmap, i);
    }
    offsets[i + 1] = position;
  }
  return arrow.makeVector(
    arrow.makeData({
      type: new arrow.Utf8(),
      length: n,
      nullCount: nulls,
      ...(bitmap === undefined ? {} : { nullBitmap: bitmap }),
      valueOffsets: offsets,
      data: bytes.subarray(0, position),
    }),
  );
}

export function boolVector(values: readonly Value[]): arrow.Vector {
  const n = values.length;
  const data = new Uint8Array((n + 7) >> 3);
  let nulls = 0;
  for (const value of values) if (value === null) nulls++;
  const bitmap = nulls > 0 ? new Uint8Array((n + 7) >> 3) : undefined;
  for (let i = 0; i < n; i++) {
    const value = values[i];
    if (value === true) setBit(data, i);
    if (bitmap !== undefined && value !== null) setBit(bitmap, i);
  }
  return arrow.makeVector(
    arrow.makeData({
      type: new arrow.Bool(),
      length: n,
      nullCount: nulls,
      ...(bitmap === undefined ? {} : { nullBitmap: bitmap }),
      data,
    }),
  );
}

export function int32Vector(values: readonly Value[]): arrow.Vector {
  return numberVector(values, "int32");
}

export function float64Vector(values: readonly Value[]): arrow.Vector {
  return numberVector(values, "float64");
}

function numberVector(values: readonly Value[], kind: "int32" | "float64"): arrow.Vector {
  const n = values.length;
  const data = kind === "int32" ? new Int32Array(n) : new Float64Array(n);
  let nulls = 0;
  for (const value of values) if (value === null) nulls++;
  const bitmap = nulls > 0 ? new Uint8Array((n + 7) >> 3) : undefined;
  for (let i = 0; i < n; i++) {
    const value = values[i];
    if (value === null || value === undefined) continue;
    data[i] = Number(value);
    if (bitmap !== undefined) setBit(bitmap, i);
  }
  if (bitmap === undefined) return arrow.makeVector(data);
  return kind === "int32"
    ? arrow.makeVector(
        arrow.makeData({
          type: new arrow.Int32(),
          length: n,
          nullCount: nulls,
          nullBitmap: bitmap,
          data: data as Int32Array,
        }),
      )
    : arrow.makeVector(
        arrow.makeData({
          type: new arrow.Float64(),
          length: n,
          nullCount: nulls,
          nullBitmap: bitmap,
          data: data as Float64Array,
        }),
      );
}

export function table(vectors: Record<string, arrow.Vector>): arrow.Table {
  return new arrow.Table(vectors);
}
