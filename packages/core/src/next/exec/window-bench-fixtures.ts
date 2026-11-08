/** Shared prebuilt Gate B inputs; construction is outside all measured/profiled regions. */
import type { PhysicalType } from "../types.js";
import { BATCH_ROWS, type Batch, type FlatVector, type Vector } from "../vector/vector.js";
import type { SortColumn } from "./sort-keys.js";
import type { Frame } from "./window-frames.js";
import type { WindowSpec } from "./window.js";
const BIG: PhysicalType = { kind: "int64" };
const INT: PhysicalType = { kind: "int32", bits: 32 };
const DBL: PhysicalType = { kind: "float64" };
const TEXT: PhysicalType = { kind: "text" };
const encoder = new TextEncoder();

// The analytics harness's generator (gen.js), so values match the W1M dataset's.
function mix(a: number, b: number): number {
  let h = Math.imul(a ^ 0x5bd1e995, 0x9e3779b1) ^ Math.imul(b + 0x7f4a7c15, 0x85ebca77);
  h ^= h >>> 16;
  h = Math.imul(h, 0x7feb352d);
  h ^= h >>> 15;
  h = Math.imul(h, 0x846ca68b);
  h ^= h >>> 16;
  return h >>> 0;
}
const U = 4294967296;
const CARDS = [5, 10, 20, 50, 100, 200, 500, 1000];
const dim = (k: number, i: number): string => {
  const card = CARDS[k % 8] ?? 5;
  const u = mix(i, 10 + k);
  const v = k % 3 === 0 ? Math.floor((u / U) * (u / U) * card) : u % card;
  return `d${String(k)}_${String(v)}`;
};
const int = (k: number, i: number): number => {
  const u = mix(i, 1000 + k);
  return k % 10 === 6 ? u % 65536 : (u % 10000) - 5000;
};
const float = (k: number, i: number): number => {
  const u = mix(i, 2000 + k);
  switch (k % 4) {
    case 1:
      return ((u % 10000) + (mix(i, 2500 + k) % 10000)) / 100 - 100;
    case 2:
      return (u / U) * 1e6;
    default:
      return (u % 100000) / 100;
  }
};

function numbers(
  type: PhysicalType,
  make: (i: number) => number,
  at: number,
  n: number,
): FlatVector {
  const values = Float64Array.from({ length: n }, (_, i) => make(at + i));
  if (type.kind === "int64")
    return {
      form: "flat",
      type,
      length: n,
      validity: null,
      data: { layout: "int64", values: { repr: "safe", values } },
    };
  if (type.kind === "int32")
    return {
      form: "flat",
      type,
      length: n,
      validity: null,
      data: { layout: "i32", values: Int32Array.from(values) },
    };
  return { form: "flat", type, length: n, validity: null, data: { layout: "f64", values } };
}

let dictionaryIds = 1 << 20;
/** A dictionary vector over the batch's distinct values (one dictionary per batch, as a page). */
function dictText(make: (i: number) => string, at: number, n: number): Vector {
  const index = new Map<string, number>();
  const codes = new Int32Array(n);
  const values: string[] = [];
  for (let i = 0; i < n; i++) {
    const value = make(at + i);
    let code = index.get(value);
    if (code === undefined) {
      code = values.length;
      index.set(value, code);
      values.push(value);
    }
    codes[i] = code;
  }
  return {
    form: "dictionary",
    type: TEXT,
    length: n,
    codes,
    validity: null,
    dictionary: flatText(values),
    dictionaryId: dictionaryIds++,
  };
}

function flatText(values: readonly string[]): FlatVector {
  const parts = values.map((value) => encoder.encode(value));
  const offsets = new Int32Array(values.length + 1);
  let total = 0;
  parts.forEach((p, i) => {
    total += p.length;
    offsets[i + 1] = total;
  });
  const bytes = new Uint8Array(total);
  parts.forEach((p, i) => bytes.set(p, offsets[i] ?? 0));
  return {
    form: "flat",
    type: TEXT,
    length: values.length,
    validity: null,
    data: { layout: "varlen", bytes, offsets },
  };
}

export const windowKey = (column: number, direction: "asc" | "desc" = "asc"): SortColumn => ({
  column,
  direction,
  nulls: "last",
  collation: null,
});
const rows = (start: Frame["start"], end: Frame["end"]): Frame => ({
  unit: "rows",
  start,
  end,
  exclude: "no-others",
});

/** One shape: its input (dimension, id, value) and its window spec. */
function compactWindowShape(
  name: string,
  N: number,
  DICT: boolean,
): { types: PhysicalType[]; input: Batch[]; spec: WindowSpec } {
  function text(make: (i: number) => string, at: number, n: number): Vector {
    if (DICT) return dictText(make, at, n);
    return flatText(Array.from({ length: n }, (_, i) => make(at + i)));
  }

  function batches(columns: ReadonlyArray<(at: number, n: number) => Vector>): Batch[] {
    const out: Batch[] = [];
    for (let at = 0; at < N; at += BATCH_ROWS) {
      const n = Math.min(BATCH_ROWS, N - at);
      out.push({ length: n, columns: columns.map((make) => make(at, n)), selection: null });
    }
    return out;
  }

  const id = (at: number, n: number) => numbers(BIG, (i) => i + 1, at, n);
  switch (name) {
    case "topk":
    case "topkGroup": {
      const types = [TEXT, BIG, DBL];
      return {
        types,
        input: batches([
          (a, n) => text((i) => dim(4, i), a, n),
          id,
          (a, n) => numbers(DBL, (i) => float(5, i), a, n),
        ]),
        spec: {
          types,
          partitionBy: [0],
          orderBy: [windowKey(2, "desc"), windowKey(1)],
          functions: [{ fn: "row_number", arg: null, frame: null, type: BIG }],
        },
      };
    }
    case "rank": {
      const types = [TEXT, BIG, INT];
      return {
        types,
        input: batches([
          (a, n) => text((i) => dim(7, i), a, n),
          id,
          (a, n) => numbers(INT, (i) => int(6, i), a, n),
        ]),
        spec: {
          types,
          partitionBy: [0],
          orderBy: [windowKey(2, "desc")],
          functions: [{ fn: "rank", arg: null, frame: null, type: BIG }],
        },
      };
    }
    case "runningSum": {
      const types = [TEXT, BIG, DBL];
      return {
        types,
        input: batches([
          (a, n) => text((i) => dim(0, i), a, n),
          id,
          (a, n) => numbers(DBL, (i) => float(6, i), a, n),
        ]),
        spec: {
          types,
          partitionBy: [0],
          orderBy: [windowKey(1)],
          functions: [
            {
              fn: "sum",
              arg: 2,
              frame: rows(
                { kind: "unbounded-preceding", offset: null },
                { kind: "current-row", offset: null },
              ),
              type: DBL,
            },
          ],
        },
      };
    }
    case "lagLead": {
      const types = [TEXT, BIG, INT];
      return {
        types,
        input: batches([
          (a, n) => text((i) => dim(5, i), a, n),
          id,
          (a, n) => numbers(INT, (i) => int(8, i), a, n),
        ]),
        spec: {
          types,
          partitionBy: [0],
          orderBy: [windowKey(1)],
          functions: [
            { fn: "lag", arg: 2, frame: null, type: INT },
            { fn: "lead", arg: 2, frame: null, type: INT },
          ],
        },
      };
    }
    case "movingAvg": {
      const types = [TEXT, BIG, DBL];
      return {
        types,
        input: batches([
          (a, n) => text((i) => dim(3, i), a, n),
          id,
          (a, n) => numbers(DBL, (i) => float(7, i), a, n),
        ]),
        spec: {
          types,
          partitionBy: [0],
          orderBy: [windowKey(1)],
          functions: [
            {
              fn: "avg",
              arg: 2,
              frame: rows({ kind: "preceding", offset: 9 }, { kind: "current-row", offset: null }),
              type: DBL,
            },
          ],
        },
      };
    }
    default:
      throw new Error(`No shape ${name}`);
  }
}

/** The SQL front end retains selected columns alongside hidden partition/order aliases.
 * These are repeated vector references, exactly as the input projection in the frozen plans.
 * Compact is the historical three-column diagnostic; sql reproduces the executor layout.
 */
export function windowShape(name: string, rows: number, dict: boolean, layout = "compact") {
  const shape = compactWindowShape(name, rows, dict);
  if (layout !== "sql" || (name !== "topk" && name !== "rank")) return shape;
  const columns = name === "topk" ? [0, 1, 2, 0, 2, 1] : [0, 1, 2, 0, 2];
  const types = columns.map((column) => shape.types[column] ?? BIG);
  return {
    types,
    input: shape.input.map((batch) => ({
      ...batch,
      columns: columns.map((column) => {
        const vector = batch.columns[column];
        if (vector === undefined) throw new Error("Missing window input");
        return vector;
      }),
    })),
    spec: {
      ...shape.spec,
      types,
      partitionBy: [3],
      orderBy: shape.spec.orderBy.map((key) => ({
        ...key,
        column: key.column === 2 ? 4 : 5,
        nulls: key.direction === "desc" ? ("first" as const) : ("last" as const),
      })),
    },
  };
}
