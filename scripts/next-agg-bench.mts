/**
 * Microbenchmarks of the new executor's aggregation (packages/core/src/next/exec/agg) over
 * in-memory batches of 2,048 rows, against measured shapes (PROPOSAL Gate B, measurements.md):
 *
 *   npx tsc -b packages/core && node --expose-gc --max-semi-space-size=512 \
 *     scripts/next-agg-bench.mts [--repeats 7] [--only <shape substring>]
 *
 * Operator cost only: input batches are prebuilt (no scan or decode), output is consumed by a
 * sink that discards it. Each shape reports the minimum and median of `repeats` runs after
 * warm-ups, and the minimum process CPU time (user + system; excludes time descheduled, but
 * includes GC threads); the machine is shared, so the minimums are the closer estimates of
 * uncontended cost.
 * DuckDB-Wasm single-thread times are whole queries (scan included): an upper bound for this part.
 */
import { performance } from "node:perf_hooks";
import { AggregateOperator } from "../packages/core/dist/next/exec/agg/aggregate.js";
import { OutputArena } from "../packages/core/dist/next/exec/kernels/vectors.js";
import {
  F64,
  I32,
  I64,
  TEXT,
  TS,
  UNCHARGED,
  col,
  decimal,
  decimals,
  dictionary,
  numbers,
  texts,
} from "../packages/core/dist/next/exec/kernels/test-support.js";
import { drive, type Operator } from "../packages/core/dist/next/exec/pipeline.js";
import type { AggregateCall, OutputColumn } from "../packages/core/dist/next/ir/ir.js";
import type { PhysicalType } from "../packages/core/dist/next/types.js";
import type { Batch, Vector } from "../packages/core/dist/next/vector/vector.js";

const repeatsArg = process.argv.indexOf("--repeats");
const REPEATS = repeatsArg >= 0 ? Number(process.argv[repeatsArg + 1]) : 7;
const onlyArg = process.argv.indexOf("--only");
const ONLY = onlyArg >= 0 ? (process.argv[onlyArg + 1] ?? "") : "";
const BATCH = 2048;

function rng(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = Math.imul(state ^ (state >>> 15), state | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const random = rng(0xa66);

function batches(rows: number, columns: (n: number) => Vector[]): Batch[] {
  const out: Batch[] = [];
  for (let start = 0; start < rows; start += BATCH) {
    const n = Math.min(BATCH, rows - start);
    out.push({ length: n, columns: columns(n), selection: null });
  }
  return out;
}

const sink: Operator = {
  push: () => "ready",
  ready: () => Promise.resolve(),
  finish: () => undefined,
  abort: () => undefined,
};

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

async function run(
  data: Batch[],
  input: readonly PhysicalType[],
  groupBy: number[],
  aggregates: AggregateCall[],
): Promise<void> {
  const output: OutputColumn[] = [
    ...groupBy.map((k) => ({ name: "k", type: input[k] as PhysicalType, nullable: true })),
    ...aggregates.map((a) => ({ name: a.fn, type: a.type, nullable: true })),
  ];
  const rel = {
    id: 1,
    op: "aggregate" as const,
    output,
    input: { id: 0, op: "values" as const, output: [], rows: [] },
    groupBy,
    groupingSets: null,
    aggregates,
  };
  const context = { arena: new OutputArena(UNCHARGED, BATCH), params: { slots: [] }, input };
  const operator = new AggregateOperator(rel, context, UNCHARGED, sink);
  let next = 0;
  await drive(
    {
      next: () => (next < data.length ? { batch: data[next++] as Batch } : null),
      abort: () => undefined,
    },
    operator,
  );
}

const results: Array<{
  shape: string;
  rows: number;
  min: number;
  median: number;
  cpu: number;
  reference: string;
}> = [];

async function bench(
  shape: string,
  rows: number,
  reference: string,
  once: () => Promise<void>,
): Promise<void> {
  if (!shape.includes(ONLY)) return;
  for (let w = 0; w < 3; w++) await once();
  const samples: number[] = [];
  const cpu: number[] = [];
  for (let r = 0; r < REPEATS; r++) {
    const start = performance.now();
    const used = process.cpuUsage();
    await once();
    samples.push(performance.now() - start);
    const spent = process.cpuUsage(used);
    cpu.push((spent.user + spent.system) / 1000);
  }
  samples.sort((a, b) => a - b);
  cpu.sort((a, b) => a - b);
  results.push({
    shape,
    rows,
    min: samples[0] ?? 0,
    median: samples[Math.floor(samples.length / 2)] ?? 0,
    cpu: cpu[0] ?? 0,
    reference,
  });
}

const N12 = decimal(12, 2);
const D = decimal(null, null);
const M = 1_048_576;

// agg_numeric_sum: SUM(n00..n03) + AVG(n04) over NUMERIC(12,2).
{
  const data = batches(M, (n) =>
    Array.from({ length: 5 }, () =>
      decimals(
        N12,
        Array.from({ length: n }, () => [BigInt(Math.floor((random() - 0.5) * 2e11)), 2] as const),
        "safe",
        2,
      ),
    ),
  );
  const input = [N12, N12, N12, N12, N12];
  await bench("SUM(numeric(12,2)) × 1", M, "target ≤ 5 ms (PROPOSAL)", () =>
    run(data, input, [], [call("sum", D, 0, N12)]),
  );
  const gateB = [0, 1, 2, 3].map((k) => call("sum", D, k, N12)).concat([call("avg", D, 4, N12)]);
  await bench("SUM × 4 + AVG, numeric(12,2)", M, "DuckDB 11 ms; Gate B ≤ 5 ms", () =>
    run(data, input, [], gateB),
  );
  // The pipeline model: a scan decodes each chunk into a reused batch, so the aggregate reads
  // cache-resident vectors. The same 512 pushes cycle over 4 batches (320 KiB); the streaming
  // rows above read 40 MB of prebuilt coefficients from RAM instead.
  const resident = Array.from({ length: data.length }, (_, b) => data[b % 4] as Batch);
  await bench("SUM(numeric(12,2)) × 1, cache-resident", M, "target ≤ 5 ms (PROPOSAL)", () =>
    run(resident, input, [], [call("sum", D, 0, N12)]),
  );
  await bench("SUM × 4 + AVG, numeric(12,2), cache-resident", M, "Gate B ≤ 5 ms", () =>
    run(resident, input, [], gateB),
  );
  await bench("SUM × 5, numeric(12,2), cache-resident (no AVG)", M, "comparison", () =>
    run(
      resident,
      input,
      [],
      [0, 1, 2, 3, 4].map((k) => call("sum", D, k, N12)),
    ),
  );
  await bench("AVG × 1, numeric(12,2), cache-resident", M, "comparison", () =>
    run(resident, input, [], [call("avg", D, 4, N12)]),
  );
}

// 200k-row group-bys (PROPOSAL targets: 100-group 8.9 ms, 20k-group 16.1 ms, DuckDB whole query).
for (const [groups, reference] of [
  [100, "DuckDB 8.9 ms (whole query)"],
  [20_000, "DuckDB 16.1 ms (whole query)"],
] as const) {
  const rows = 200_000;
  const data = batches(rows, (n) => [
    numbers(
      I32,
      Array.from({ length: n }, () => Math.floor(random() * groups)),
    ),
    numbers(
      F64,
      Array.from({ length: n }, () => random() * 100),
    ),
  ]);
  await bench(`GROUP BY int (${String(groups)} groups), COUNT + SUM(float)`, rows, reference, () =>
    run(data, [I32, F64], [0], [call("count_star", I64, null, I64), call("sum", F64, 1, F64)]),
  );
}

// agg_highcard_int: ~632k int groups, COUNT(*) + SUM(float).
{
  const data = batches(M, (n) => [
    numbers(
      I32,
      Array.from({ length: n }, () => Math.floor(random() * 1_000_000)),
    ),
    numbers(
      F64,
      Array.from({ length: n }, () => random()),
    ),
  ]);
  await bench("GROUP BY i02 (~632k groups), COUNT + SUM", M, "DuckDB 74 ms (whole query)", () =>
    run(data, [I32, F64], [0], [call("count_star", I64, null, I64), call("sum", F64, 1, F64)]),
  );
}

// agg_3dims_20aggs: three low-cardinality text dims (dictionary pages), 20 aggregates.
{
  const dims = [5, 20, 100].map((card) =>
    texts(Array.from({ length: card }, (_, k) => `v${String(k)}`)),
  );
  const data = batches(M, (n) => [
    ...dims.map((entries) =>
      dictionary(
        entries,
        Array.from({ length: n }, () => Math.floor(random() * entries.length)),
      ),
    ),
    numbers(
      I32,
      Array.from({ length: n }, () => Math.floor(random() * 1e6)),
    ),
    numbers(
      F64,
      Array.from({ length: n }, () => random()),
    ),
    decimals(
      N12,
      Array.from({ length: n }, () => [BigInt(Math.floor(random() * 1e9)), 2] as const),
      "safe",
      2,
    ),
  ]);
  const input = [TEXT, TEXT, TEXT, I32, F64, N12];
  const aggregates: AggregateCall[] = [];
  for (let k = 0; k < 4; k++)
    aggregates.push(
      call("sum", I64, 3, I32),
      call("avg", F64, 4, F64),
      call("min", F64, 4, F64),
      call("max", I32, 3, I32),
      k % 2 === 0 ? call("count_star", I64, null, I64) : call("sum", D, 5, N12),
    );
  await bench("GROUP BY 3 text dims, 20 aggregates", M, "DuckDB 134 ms (whole query)", () =>
    run(data, input, [0, 1, 2], aggregates),
  );
  // The composite key alone (TupleKeys over three dictionary columns).
  await bench("GROUP BY 3 text dims, COUNT(*) only", M, "key cost of the shape above", () =>
    run(data, input, [0, 1, 2], [call("count_star", I64, null, I64)]),
  );
}

// agg_3dims_20aggs as the harness runs it (analytics-harness gen.js and queries.js): d00, d01, d02
// of 5 (skewed: u² × 5), 10 and 20 values, so at most 1,000 groups, and its 21 aggregates over
// int4, float8, a nullable int, a text and a timestamp column.
{
  const mix = (a: number, b: number): number => {
    let h = Math.imul(a ^ 0x5bd1e995, 0x9e3779b1) ^ Math.imul(b + 0x7f4a7c15, 0x85ebca77);
    h ^= h >>> 16;
    h = Math.imul(h, 0x7feb352d);
    h ^= h >>> 15;
    h = Math.imul(h, 0x846ca68b);
    h ^= h >>> 16;
    return h >>> 0;
  };
  const U = 4294967296;
  const DCARDS = [5, 10, 20, 50, 100, 200, 500, 1000];
  const dimCodes = (k: number, start: number, n: number): number[] => {
    const card = DCARDS[k % 8] as number;
    return Array.from({ length: n }, (_, j) => {
      const u = mix(start + j, 10 + k);
      return k % 3 === 0 ? Math.floor((u / U) * (u / U) * card) : u % card;
    });
  };
  const dimEntries = (k: number) =>
    texts(Array.from({ length: DCARDS[k % 8] as number }, (_, v) => `d${String(k)}_${String(v)}`));
  const intCol = (k: number, start: number, n: number) =>
    numbers(
      I32,
      Array.from({ length: n }, (_, j) => {
        const u = mix(start + j, 1000 + k);
        return [
          u % 100,
          u % 1000,
          u % 1000000,
          Math.floor((start + j) / 100),
          (u % 2000000001) - 1000000000,
          u % 10,
          u % 65536,
          u % 1000000,
          (u % 10000) - 5000,
          u % 1000,
        ][k % 10] as number;
      }),
    );
  const floatCol = (k: number, start: number, n: number) =>
    numbers(
      F64,
      Array.from({ length: n }, (_, j) => {
        const i = start + j;
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
      }),
    );
  const dims = [0, 1, 2, 10].map(dimEntries);
  let start = 0;
  const data = batches(M, (n) => {
    const columns: Vector[] = [
      ...[0, 1, 2].map((k, d) =>
        dictionary(dims[d] as Vector & { form: "flat" }, dimCodes(k, start, n)),
      ),
      ...[10, 11, 12, 13, 15].map((k) => intCol(k, start, n)), // 3–7
      ...[10, 11, 12, 13, 14, 15, 16].map((k) => floatCol(k, start, n)), // 8–14
      ...[16, 17, 18].map((k) => intCol(k, start, n)), // 15–17
      ...[17, 18].map((k) => floatCol(k, start, n)), // 18–19
      // x00: int, 10 % NULL.
      numbers(
        I32,
        Array.from({ length: n }, (_, j) =>
          mix(start + j, 5000) / U < 0.1 ? null : mix(start + j, 6000) % 100000,
        ),
      ), // 20
      dictionary(dims[3] as Vector & { form: "flat" }, dimCodes(10, start, n)), // 21: d10
      numbers(
        TS,
        Array.from(
          { length: n },
          (_, j) => (1704067200000 + (start + j) * 30000 + (mix(start + j, 1) % 30000)) * 1000,
        ),
      ), // 22: ts (µs)
    ];
    start += n;
    return columns;
  });
  const input: PhysicalType[] = [
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
  ];
  const aggregates: AggregateCall[] = [
    call("count_star", I64, null, I64),
    ...[3, 4, 5, 6, 7].map((c) => call("sum", I64, c, I32)),
    ...[8, 9, 10, 11, 12].map((c) => call("avg", F64, c, F64)),
    call("min", F64, 13, F64),
    call("max", F64, 14, F64),
    call("min", I32, 15, I32),
    call("max", I32, 16, I32),
    call("sum", F64, 18, F64),
    call("sum", F64, 19, F64),
    call("avg", D, 17, I32),
    call("count", I64, 20, I32),
    call("min", TEXT, 21, TEXT),
    call("max", TS, 22, TS),
  ];
  await bench(
    "agg_3dims_20aggs (harness shape, ≤ 1,000 groups)",
    M,
    "DuckDB 134 ms (whole query)",
    () => run(data, input, [0, 1, 2], aggregates),
  );
  await bench("agg_3dims_20aggs, COUNT(*) only", M, "key cost of the shape above", () =>
    run(data, input, [0, 1, 2], [call("count_star", I64, null, I64)]),
  );
  // What each kind of aggregate adds to the key cost (`--only "3dims part"`).
  const parts: Array<[string, AggregateCall[]]> = [
    ["5 × SUM(int4)", aggregates.slice(1, 6)],
    ["5 × AVG(float8)", aggregates.slice(6, 11)],
    ["MIN/MAX float8, int4", aggregates.slice(11, 15)],
    ["2 × SUM(float8)", aggregates.slice(15, 17)],
    ["AVG(int4)", aggregates.slice(17, 18)],
    ["COUNT(nullable int)", aggregates.slice(18, 19)],
    ["MIN(text)", aggregates.slice(19, 20)],
    ["MAX(timestamp)", aggregates.slice(20, 21)],
  ];
  for (const [label, some] of parts)
    await bench(`3dims part: COUNT(*) + ${label}`, M, "minus the key cost", () =>
      run(data, input, [0, 1, 2], [call("count_star", I64, null, I64), ...some]),
    );
}

// agg_count_distinct: COUNT(DISTINCT) over two high-cardinality text columns.
{
  const data = batches(M, (n) => [
    texts(
      Array.from({ length: n }, () => `user${String(Math.floor(random() * 800_000))}@example.com`),
    ),
    texts(Array.from({ length: n }, () => `acct-${String(Math.floor(random() * 200_000))}`)),
  ]);
  await bench("COUNT(DISTINCT h01), COUNT(DISTINCT h05)", M, "DuckDB 250 ms (whole query)", () =>
    run(
      data,
      [TEXT, TEXT],
      [],
      [call("count", I64, 0, TEXT, true), call("count", I64, 1, TEXT, true)],
    ),
  );
}

console.log(`| shape | rows | min ms | median ms | min CPU ms | reference |`);
console.log(`|---|---:|---:|---:|---:|---|`);
for (const r of results)
  console.log(
    `| ${r.shape} | ${r.rows.toLocaleString("en-US")} | ${r.min.toFixed(2)} | ${r.median.toFixed(2)} | ${r.cpu.toFixed(2)} | ${r.reference} |`,
  );
