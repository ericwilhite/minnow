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
  await bench("SUM × 4 + AVG, numeric(12,2)", M, "DuckDB 11 ms; Gate B ≤ 5 ms", () =>
    run(
      data,
      input,
      [],
      [0, 1, 2, 3].map((k) => call("sum", D, k, N12)).concat([call("avg", D, 4, N12)]),
    ),
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
