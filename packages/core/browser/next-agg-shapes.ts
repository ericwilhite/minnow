/**
 * The analytics harness's aggregation shapes (minnow-plan analytics-harness gen.js and
 * queries.js, PROPOSAL Gate B) as the new executor's aggregate operator sees them: the same
 * column generators, query by query, over prebuilt batches of 2,048 rows (no scan, no SQL).
 * Shared by the Node microbenchmark (scripts/next-agg-bench.mts) and the browser one
 * (next-agg-bench-run.ts); it imports nothing at run time, so each caller passes in its own
 * builds of the vector helpers.
 */
import type { AggregateCall } from "../src/next/ir/ir.js";
import type { PhysicalType } from "../src/next/types.js";
import type { Batch, FlatVector, Vector } from "../src/next/vector/vector.js";

/** The vector and type helpers the shapes are built with (exec/kernels/test-support.ts). */
export interface ShapeLib {
  readonly I32: PhysicalType;
  readonly I64: PhysicalType;
  readonly F64: PhysicalType;
  readonly TEXT: PhysicalType;
  readonly TS: PhysicalType;
  readonly decimal: (precision: number | null, scale: number | null) => PhysicalType;
  readonly numbers: (type: PhysicalType, values: ReadonlyArray<number | null>) => FlatVector;
  readonly texts: (values: ReadonlyArray<string | null>) => FlatVector;
  readonly dictionary: (entries: FlatVector, codes: ReadonlyArray<number | null>) => Vector;
  readonly decimals: (
    type: PhysicalType,
    values: ReadonlyArray<readonly [bigint, number] | null>,
    repr: "safe",
    scale: number,
  ) => Vector;
  readonly col: (index: number, type: PhysicalType) => AggregateCall["args"][number];
}

/** One shape: its input batches and the aggregate's GROUP BY and calls. */
export interface AggShape {
  readonly name: string;
  /** The harness query and DuckDB-Wasm's single-thread W1M time (whole query). */
  readonly reference: string;
  readonly rows: number;
  readonly batches: Batch[];
  readonly input: PhysicalType[];
  readonly groupBy: number[];
  readonly aggregates: AggregateCall[];
}

/** gen.js `mix`: every harness column is a pure function of (row, salt). */
export function mix(a: number, b: number): number {
  let h = Math.imul(a ^ 0x5bd1e995, 0x9e3779b1) ^ Math.imul(b + 0x7f4a7c15, 0x85ebca77);
  h ^= h >>> 16;
  h = Math.imul(h, 0x7feb352d);
  h ^= h >>> 15;
  h = Math.imul(h, 0x846ca68b);
  h ^= h >>> 16;
  return h >>> 0;
}

const U = 4294967296;
const BATCH = 2048;
const DCARDS = [5, 10, 20, 50, 100, 200, 500, 1000];

function batchesOf(rows: number, columns: (start: number, n: number) => Vector[]): Batch[] {
  const out: Batch[] = [];
  for (let start = 0; start < rows; start += BATCH) {
    const n = Math.min(BATCH, rows - start);
    out.push({ length: n, columns: columns(start, n), selection: null });
  }
  return out;
}

const range = <T>(n: number, f: (j: number) => T): T[] => Array.from({ length: n }, (_, j) => f(j));

/** gen.js int column `k` (k ≠ 20). */
function intValue(k: number, i: number): number {
  const u = mix(i, 1000 + k);
  return (
    [
      u % 100,
      u % 1000,
      u % 1000000,
      Math.floor(i / 100),
      (u % 2000000001) - 1000000000,
      u % 10,
      u % 65536,
      u % 1000000,
      (u % 10000) - 5000,
      u % 1000,
    ][k % 10] ?? 0
  );
}

/** gen.js float column `k`. */
function floatValue(k: number, i: number): number {
  const u = mix(i, 2000 + k);
  switch (k % 4) {
    case 0:
      return (u % 1000000) / 1000;
    case 1:
      return ((u % 10000) + (mix(i, 2500 + k) % 10000)) / 100 - 100;
    case 2:
      return (u / U) * 1e6;
    default:
      return (u % 100000) / 100;
  }
}

/** gen.js NUMERIC(12,2) column `k`, as its coefficient in cents. */
function centsValue(k: number, i: number): number {
  const u = mix(i, 3000 + k);
  let cents = u % (k % 2 ? 100000000 : 1000000);
  if (k % 5 === 4) cents -= k % 2 ? 50000000 : 500000;
  return cents;
}

/** gen.js dimension column `k`: its value index (squared skew when k % 3 is 0). */
function dimCode(k: number, i: number): number {
  const card = DCARDS[k % 8] ?? 1;
  const u = mix(i, 10 + k);
  return k % 3 === 0 ? Math.floor((u / U) * (u / U) * card) : u % card;
}

/** The harness's aggregation shapes over `rows` rows (W1M: 1,048,576). */
export function aggShapes(lib: ShapeLib, rows: number, only: readonly string[] = []): AggShape[] {
  const { I32, I64, F64, TEXT, TS, numbers, texts, dictionary, decimals, col } = lib;
  const N12 = lib.decimal(12, 2);
  const D = lib.decimal(null, null);
  const call = (
    fn: AggregateCall["fn"],
    type: PhysicalType,
    index: number | null,
    input: PhysicalType,
    distinct = false,
  ): AggregateCall => ({
    fn,
    args: index === null ? [] : [col(index, input)],
    distinct,
    filter: null,
    orderBy: [],
    type,
  });
  const wanted = (name: string) => only.length === 0 || only.some((o) => name.includes(o));
  const shapes: AggShape[] = [];

  if (wanted("agg_highcard_int"))
    // SELECT i02, COUNT(*), SUM(f01) … GROUP BY i02 (about 632k groups).
    shapes.push({
      name: "agg_highcard_int",
      reference: "DuckDB 74 ms",
      rows,
      batches: batchesOf(rows, (start, n) => [
        numbers(
          I32,
          range(n, (j) => intValue(2, start + j)),
        ),
        numbers(
          F64,
          range(n, (j) => floatValue(1, start + j)),
        ),
      ]),
      input: [I32, F64],
      groupBy: [0],
      aggregates: [call("count_star", I64, null, I64), call("sum", F64, 1, F64)],
    });

  if (wanted("agg_3dims_20aggs")) {
    // SELECT d00, d01, d02, <21 aggregates> … GROUP BY d00, d01, d02 (at most 1,000 groups).
    const entries = [0, 1, 2, 10].map((k) =>
      texts(range(DCARDS[k % 8] ?? 1, (v) => `d${String(k)}_${String(v)}`)),
    );
    const dims = (k: number, d: number, start: number, n: number) =>
      dictionary(
        entries[d] ?? texts([]),
        range(n, (j) => dimCode(k, start + j)),
      );
    const ints = (k: number, start: number, n: number) =>
      numbers(
        I32,
        range(n, (j) => intValue(k, start + j)),
      );
    const floats = (k: number, start: number, n: number) =>
      numbers(
        F64,
        range(n, (j) => floatValue(k, start + j)),
      );
    shapes.push({
      name: "agg_3dims_20aggs",
      reference: "DuckDB 134 ms",
      rows,
      batches: batchesOf(rows, (start, n) => [
        ...[0, 1, 2].map((k, d) => dims(k, d, start, n)),
        ...[10, 11, 12, 13, 15].map((k) => ints(k, start, n)), // 3–7
        ...[10, 11, 12, 13, 14, 15, 16].map((k) => floats(k, start, n)), // 8–14
        ...[16, 17, 18].map((k) => ints(k, start, n)), // 15–17
        ...[17, 18].map((k) => floats(k, start, n)), // 18–19
        // x00: an int, NULL in 10 % of rows.
        numbers(
          I32,
          range(n, (j) => (mix(start + j, 5000) / U < 0.1 ? null : mix(start + j, 6000) % 100000)),
        ), // 20
        dims(10, 3, start, n), // 21: d10
        // ts, µs (gen.js tsOf, in ms).
        numbers(
          TS,
          range(
            n,
            (j) => (1704067200000 + (start + j) * 30000 + (mix(start + j, 1) % 30000)) * 1000,
          ),
        ), // 22
      ]),
      input: [
        TEXT,
        TEXT,
        TEXT,
        I32,
        I32,
        I32,
        I32,
        I32,
        F64,
        F64,
        F64,
        F64,
        F64,
        F64,
        F64,
        I32,
        I32,
        I32,
        F64,
        F64,
        I32,
        TEXT,
        TS,
      ],
      groupBy: [0, 1, 2],
      aggregates: [
        call("count_star", I64, null, I64),
        ...[3, 4, 5, 6, 7].map((c) => call("sum", I64, c, I32)),
        ...[8, 9, 10, 11, 12].map((c) => call("avg", F64, c, F64)),
        call("min", F64, 13, F64),
        call("max", F64, 14, F64),
        call("min", I32, 15, I32),
        call("max", I32, 16, I32),
        call("sum", F64, 18, F64),
        call("sum", F64, 19, F64),
        call("avg", F64, 17, I32),
        call("count", I64, 20, I32),
        call("min", TEXT, 21, TEXT),
        call("max", TS, 22, TS),
      ],
    });
  }

  if (wanted("agg_count_distinct"))
    // SELECT COUNT(DISTINCT h01), COUNT(DISTINCT h05) (emails; accounts over rows / 2).
    shapes.push({
      name: "agg_count_distinct",
      reference: "DuckDB 250 ms",
      rows,
      batches: batchesOf(rows, (start, n) => [
        texts(
          range(n, (j) => {
            const i = start + j;
            return `user${String(mix(i, 210) % 800000)}@mail${String(mix(i, 211) % 50)}.example.com`;
          }),
        ),
        texts(
          range(
            n,
            (j) => `acct-${String(mix(start + j, 250) % Math.max(1, Math.floor(rows / 2)))}`,
          ),
        ),
      ]),
      input: [TEXT, TEXT],
      groupBy: [],
      aggregates: [call("count", I64, 0, TEXT, true), call("count", I64, 1, TEXT, true)],
    });

  if (wanted("agg_count_distinct_dictionary"))
    // Dictionary-oriented workload: 128 entries repeated in each 2,048-row batch.
    // Separate from h01/h05: this changes the distribution as well as the encoding.
    shapes.push({
      name: "agg_count_distinct_dictionary",
      reference: "dictionary variant; no DuckDB reference",
      rows,
      batches: batchesOf(rows, (start, n) =>
        [0, 1].map((column) => {
          const entries = texts(
            range(128, (j) =>
              column === 0
                ? `user${String(mix(start + j, 210) % 800000)}@mail.example.com`
                : `acct-${String(mix(start + j, 250) % Math.max(1, Math.floor(rows / 2)))}`,
            ),
          );
          return dictionary(
            entries,
            range(n, (j) => mix(start + j, 310 + column) % 128),
          );
        }),
      ),
      input: [TEXT, TEXT],
      groupBy: [],
      aggregates: [call("count", I64, 0, TEXT, true), call("count", I64, 1, TEXT, true)],
    });

  if (wanted("agg_numeric_sum"))
    // SELECT SUM(n00), SUM(n01), SUM(n02), SUM(n03), AVG(n04).
    shapes.push({
      name: "agg_numeric_sum",
      reference: "DuckDB 11 ms",
      rows,
      batches: batchesOf(rows, (start, n) =>
        [0, 1, 2, 3, 4].map((k) =>
          decimals(
            N12,
            range(n, (j) => [BigInt(centsValue(k, start + j)), 2] as const),
            "safe",
            2,
          ),
        ),
      ),
      input: [N12, N12, N12, N12, N12],
      groupBy: [],
      aggregates: [...[0, 1, 2, 3].map((k) => call("sum", D, k, N12)), call("avg", D, 4, N12)],
    });

  return shapes;
}
