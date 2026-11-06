/**
 * Window microbenchmarks (track B7, increment 3): measurements.md's five W1M window shapes
 * through the real operator, inputs prebuilt (the harness generator's values) and untimed. Not a
 * test; bundle and run:
 *
 *     npx esbuild packages/core/src/next/exec/window.bench.ts --bundle --platform=node \
 *       --format=esm --outfile=$HOME/.cache/window-bench.mjs && node --expose-gc $HOME/.cache/window-bench.mjs [shape …]
 *
 * Shapes (DuckDB-Wasm single-thread W1M medians in measurements.md, whole queries):
 * - `topk`: ROW_NUMBER() OVER (PARTITION BY d04 ORDER BY f05 DESC, id) (365 ms; `topkGroup`: the
 *   same through GroupTopKOperator, k = 3)
 * - `rank`: RANK() OVER (PARTITION BY d07 ORDER BY i06 DESC) (282 ms)
 * - `runningSum`: SUM(f06) OVER (PARTITION BY d00 ORDER BY id ROWS UNBOUNDED PRECEDING) (344 ms)
 * - `lagLead`: LAG(i08), LEAD(i08) OVER (PARTITION BY d05 ORDER BY id) (273 ms)
 * - `movingAvg`: AVG(f07) OVER (PARTITION BY d03 ORDER BY id ROWS 9 PRECEDING) (320 ms)
 * Each shape also reports the sort alone (`sort`: the same keys through SortOperator), so the
 * evaluator's share is the difference. `STREAM=1` forces every partition to stream.
 */
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import { BATCH_ROWS, type Batch, type FlatVector, type Vector } from "../vector/vector.js";
import { ArraySource } from "./join-fixtures.js";
import { drive, type Operator, type PushResult } from "./pipeline.js";
import type { SortColumn } from "./sort-keys.js";
import { GroupTopKOperator, SortOperator, sortMinimumBytes } from "./sort.js";
import type { Frame } from "./window-frames.js";
import { WindowOperator, windowMinimumBytes, type WindowSpec } from "./window.js";

const N = Number(process.env.ROWS ?? 1_000_000);
const RUNS = Number(process.env.RUNS ?? 5);
const STREAM = process.env.STREAM === "1";
/** Text dimensions as dictionary vectors (what a scan of dictionary pages yields); DICT=0: flat. */
const DICT = process.env.DICT !== "0";
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

class CountSink implements Operator {
  rows = 0;
  push(batch: Batch): PushResult {
    this.rows += batch.selection === null ? batch.length : batch.selection.count;
    return "ready";
  }
  ready(): Promise<void> {
    return Promise.resolve();
  }
  finish(): void {
    this.rows += 0;
  }
  abort(): void {
    this.rows += 0;
  }
}

const key = (column: number, direction: "asc" | "desc" = "asc"): SortColumn => ({
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
function shape(name: string): { types: PhysicalType[]; input: Batch[]; spec: WindowSpec } {
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
          orderBy: [key(2, "desc"), key(1)],
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
          orderBy: [key(2, "desc")],
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
          orderBy: [key(1)],
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
          orderBy: [key(1)],
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
          orderBy: [key(1)],
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

const stats = (ms: number[]) => {
  const sorted = [...ms].sort((a, b) => a - b);
  return `min ${(sorted[0] ?? 0).toFixed(0)}, median ${(sorted[sorted.length >> 1] ?? 0).toFixed(0)} ms`;
};

async function time(make: (grant: never, sink: Operator) => Operator, minimum: number) {
  const wall: number[] = [];
  const cpu: number[] = [];
  let out = 0;
  for (let r = 0; r <= RUNS; r++) {
    (globalThis as { gc?: () => void }).gc?.();
    const governor = new Governor(4 * 1024 * 1024 * 1024);
    const grant = await governor.admit({ label: "bench", minimumBytes: minimum, spillable: true });
    const sink = new CountSink();
    const usage = process.cpuUsage();
    const start = performance.now();
    await drive(new ArraySource(input), make(grant as never, sink), undefined, {
      yieldIntervalMs: Infinity,
    });
    const ms = performance.now() - start;
    const used = process.cpuUsage(usage);
    out = sink.rows;
    if (r > 0) {
      wall.push(ms);
      cpu.push((used.user + used.system) / 1000);
    }
  }
  return { wall: stats(wall), cpu: stats(cpu), out };
}

let input: Batch[] = [];
const names =
  process.argv.slice(2).length > 0
    ? process.argv.slice(2)
    : ["topk", "topkGroup", "rank", "runningSum", "lagLead", "movingAvg"];
for (const name of names) {
  const s = shape(name);
  input = s.input;
  const spec: WindowSpec = STREAM ? { ...s.spec, streamRows: 1 } : s.spec;
  const context = (grant: never) => ({ grant, spill: new MemorySpillService(), queryId: 1 });
  const keys = [...spec.partitionBy.map((c) => key(c)), ...spec.orderBy];
  const run =
    name === "topkGroup"
      ? await time(
          (grant, sink) =>
            new GroupTopKOperator(
              { types: s.types, partitionBy: spec.partitionBy, orderBy: spec.orderBy, k: 3 },
              context(grant),
              sink,
            ),
          sortMinimumBytes(s.types),
        )
      : await time(
          (grant, sink) => new WindowOperator(spec, context(grant), sink),
          windowMinimumBytes(s.types, spec.functions.length),
        );
  const sort = await time(
    (grant, sink) =>
      new SortOperator(
        { types: s.types, keys, limit: null, offset: 0, withTies: false },
        context(grant),
        sink,
      ),
    sortMinimumBytes(s.types),
  );
  console.log(
    `${name}: ${run.wall} (CPU ${run.cpu}); sort alone ${sort.wall} (CPU ${sort.cpu}); ${String(run.out)} rows`,
  );
}
