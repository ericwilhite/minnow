/**
 * The W1M / W500 wide table's columns (scripts/browser-perf/page/wide-data.ts) as page-format
 * vectors: the physical type of each column and one group's vector, built straight into typed
 * arrays. Shared by the Node benches (scripts/pushdown-bench.mts, scripts/first-batch-bench.mts,
 * scripts/export-bench-build.mts) and the OPFS export bench (packages/core/browser/next-export-worker.ts).
 */
import type { PhysicalType } from "../../../packages/core/src/next/types.js";
import { allocateValidity, setValid } from "../../../packages/core/src/next/vector/validity.js";
import type { FlatVector, VectorData } from "../../../packages/core/src/next/vector/vector.js";
import { wideValue, type WideColumn } from "./wide-data.js";

const encoder = new TextEncoder();

export function typeOf(column: WideColumn): PhysicalType {
  switch (column.family) {
    case "id":
      return { kind: "int64" };
    case "ts":
      return { kind: "timestamp", zoned: false };
    case "day":
      return { kind: "date" };
    case "d":
    case "h":
      return { kind: "text" };
    case "i":
      return { kind: "int32", bits: 32 };
    case "f":
      return { kind: "float64" };
    case "n":
      return { kind: "decimal", precision: 12, scale: 2 };
    case "b":
      return { kind: "bool" };
    case "x":
      return column.mixed === 0
        ? { kind: "int32", bits: 32 }
        : column.mixed === 1
          ? { kind: "float64" }
          : column.mixed === 2
            ? { kind: "text" }
            : { kind: "bool" };
  }
}

/** One group's vector of `column`, built straight into typed arrays. */
/** One group's vector of `column`: rows `[first, first + count)` of a `rows`-row table. */
export function vectorOf(
  column: WideColumn,
  type: PhysicalType,
  first: number,
  count: number,
  rows: number,
): FlatVector {
  let validity: Uint32Array | null = null;
  const valueAt = (k: number): ReturnType<typeof wideValue> => {
    const value = wideValue(column, first + k, rows);
    if (value === null) {
      validity ??= (() => {
        const all = allocateValidity(count);
        for (let r = 0; r < count; r++) setValid(all, r, true);
        return all;
      })();
      setValid(validity, k, false);
    }
    return value;
  };
  let data: VectorData;
  switch (type.kind) {
    case "int64":
    case "timestamp": {
      const values = new Float64Array(count);
      for (let k = 0; k < count; k++) {
        const value = valueAt(k) as number;
        values[k] = type.kind === "timestamp" ? (value - 946_684_800_000) * 1000 : value;
      }
      data = { layout: "int64", values: { repr: "safe", values } };
      break;
    }
    case "date": {
      const values = new Int32Array(count);
      for (let k = 0; k < count; k++)
        values[k] = Math.floor((Date.parse(valueAt(k) as string) - 946_684_800_000) / 86_400_000);
      data = { layout: "i32", values };
      break;
    }
    case "int32": {
      const values = new Int32Array(count);
      for (let k = 0; k < count; k++) values[k] = (valueAt(k) as number | null) ?? 0;
      data = { layout: "i32", values };
      break;
    }
    case "float64": {
      const values = new Float64Array(count);
      for (let k = 0; k < count; k++) values[k] = (valueAt(k) as number | null) ?? 0;
      data = { layout: "f64", values };
      break;
    }
    case "bool": {
      const values = new Uint8Array(count);
      for (let k = 0; k < count; k++) values[k] = valueAt(k) === true ? 1 : 0;
      data = { layout: "bool", values };
      break;
    }
    case "decimal": {
      const values = new Float64Array(count);
      for (let k = 0; k < count; k++) values[k] = Math.round(Number(valueAt(k) as string) * 100);
      data = {
        layout: "decimal",
        scale: 2,
        scales: null,
        coefficients: { repr: "safe", values },
        specials: null,
      };
      break;
    }
    default: {
      const parts = Array.from({ length: count }, (_, k) =>
        encoder.encode(String(valueAt(k) ?? "")),
      );
      const offsets = new Int32Array(count + 1);
      let total = 0;
      parts.forEach((part, k) => {
        total += part.length;
        offsets[k + 1] = total;
      });
      const bytes = new Uint8Array(total);
      parts.forEach((part, k) => bytes.set(part, offsets[k] ?? 0));
      data = { layout: "varlen", bytes, offsets };
    }
  }
  return { form: "flat", type, length: count, validity, data };
}
