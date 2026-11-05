/**
 * Microbenchmarks of the new executor's expression kernels (packages/core/src/next/exec/kernels)
 * over in-memory batches of 2,048 rows, against the expression part of measured shapes
 * (PROPOSAL "Targets", measurements.md):
 *
 *   npx tsc -b packages/core && node --expose-gc --max-semi-space-size=512 \
 *     scripts/next-kernels-bench.mts [--repeats 9]
 *
 * Allocation per batch: with a young generation large enough that no collection runs during one
 * pass, the growth of the used heap over the pass, divided by its batches, is the bytes allocated
 * per batch (a garbage-collection observer discards a pass that collected; "—" without
 * `--expose-gc`).
 *
 * It runs the compiled output (packages/core/dist) under plain Node: vite-node rewrites every
 * import into a property read on a module object, which costs a getter per access inside the hot
 * loops and inflated these timings 5–20×.
 *
 * Each shape reports the minimum and the median of `repeats` runs after warm-ups, single thread,
 * Node. The machine is shared, so the minimum is the closer estimate of uncontended cost. These are
 * kernel costs only: no scan, decode or result delivery, and the aggregate is a plain loop over
 * the kernel's output standing in for SUM until the aggregation increment lands. Compare them
 * with the whole-query DuckDB-Wasm times only as an upper bound on what the expression may cost.
 */
import { PerformanceObserver, performance } from "node:perf_hooks";
import type { ScalarExpr } from "../packages/core/dist/next/ir/ir.js";
import { Predicate } from "../packages/core/dist/next/exec/filter.js";
import { compileExpr } from "../packages/core/dist/next/exec/kernels/compile.js";
import { OutputArena, type Compiled } from "../packages/core/dist/next/exec/kernels/vectors.js";
import {
  BOOL,
  F64,
  I32,
  I64,
  TEXT,
  UNCHARGED,
  call,
  cmp,
  col,
  decimal,
  decimals,
  dictionary,
  lit,
  numbers,
  texts,
} from "../packages/core/dist/next/exec/kernels/test-support.js";
import type { PhysicalType } from "../packages/core/dist/next/types.js";
import type { Batch, FlatVector, Vector } from "../packages/core/dist/next/vector/vector.js";

const repeatsArg = process.argv.indexOf("--repeats");
const REPEATS = repeatsArg >= 0 ? Number(process.argv[repeatsArg + 1]) : 9;
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

/** `rows` rows split into batches; `columns(start, n)` builds each batch's vectors. */
function batches(rows: number, columns: (start: number, n: number) => Vector[]): Batch[] {
  const out: Batch[] = [];
  for (let start = 0; start < rows; start += BATCH) {
    const n = Math.min(BATCH, rows - start);
    out.push({ length: n, columns: columns(start, n), selection: null });
  }
  return out;
}

function compile(expr: ScalarExpr, types: readonly PhysicalType[]): Compiled {
  return compileExpr(expr, {
    arena: new OutputArena(UNCHARGED, BATCH),
    params: { slots: [] },
    input: types,
  });
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

function time(run: () => number): { ms: number; min: number; check: number } {
  let check = 0;
  for (let w = 0; w < 5; w++) run();
  const samples: number[] = [];
  for (let r = 0; r < REPEATS; r++) {
    const start = performance.now();
    check = run();
    samples.push(performance.now() - start);
  }
  return { ms: median(samples), min: Math.min(...samples), check };
}

/** Sums a float or safe-int64 output over the batch's rows (the stand-in aggregate). */
function sumOf(vector: Vector, length: number): number {
  const flat = vector.form === "flat" ? vector : null;
  if (flat === null) return 0;
  const data = flat.data;
  const values =
    data.layout === "f64"
      ? data.values
      : data.layout === "int64" && data.values.repr === "safe"
        ? data.values.values
        : data.layout === "i32"
          ? data.values
          : null;
  if (values === null) return 0;
  let s = 0;
  for (let i = 0; i < length; i++) s += values[i] ?? 0;
  return s;
}

const random = rng(0x5eed);
const results: Array<{
  shape: string;
  rows: number;
  ms: number;
  min: number;
  bytes: number | null;
  reference: string;
}> = [];

let collections = 0;
new PerformanceObserver((list) => {
  collections += list.getEntries().length;
}).observe({ entryTypes: ["gc"] });
const gc = (globalThis as { gc?: () => void }).gc;

/** Bytes allocated per batch by one pass of `run`, or null when it cannot be measured. */
function allocationPerBatch(run: () => number, batches: number): number | null {
  if (gc === undefined) return null;
  for (let attempt = 0; attempt < 5; attempt++) {
    gc();
    const before = collections;
    const heap = process.memoryUsage().heapUsed;
    run();
    const grown = process.memoryUsage().heapUsed - heap;
    if (collections === before) return Math.max(0, grown) / batches;
  }
  return null;
}

function bench(shape: string, rows: number, reference: string, run: () => number): void {
  const { ms, min } = time(run);
  const bytes = allocationPerBatch(run, Math.ceil(rows / BATCH));
  results.push({ shape, rows, ms, min, bytes, reference });
}

// --- 200k-row shapes (PROPOSAL Targets: DuckDB-Wasm 1T whole query) ---------------------------
{
  const rows = 200_000;
  const data = batches(rows, (start, n) => [
    numbers(
      F64,
      Array.from({ length: n }, () => random() * 100),
    ),
    numbers(
      I64,
      Array.from({ length: n }, (_, k) => start + k),
    ),
  ]);
  const sumExpr = compile(
    call("add", F64, call("mul", F64, col(0, F64), lit(numbers(F64, [1.1]))), {
      op: "cast",
      input: col(1, I64),
      type: F64,
    }),
    [F64, I64],
  );
  bench("SUM(v * 1.1 + id)", rows, "DuckDB 5.4 ms (whole query)", () => {
    let s = 0;
    for (const batch of data) s += sumOf(sumExpr.evaluate(batch, null), batch.length);
    return s;
  });
  const caseExpr = compile(
    {
      op: "case",
      branches: [{ when: cmp(">", col(0, F64), lit(numbers(F64, [50]))), then: col(0, F64) }],
      otherwise: call("neg", F64, col(0, F64)),
      type: F64,
    },
    [F64, I64],
  );
  bench("SUM(CASE WHEN v > 50 THEN v ELSE -v END)", rows, "DuckDB 4.5 ms (whole query)", () => {
    let s = 0;
    for (const batch of data) s += sumOf(caseExpr.evaluate(batch, null), batch.length);
    return s;
  });
}

// --- 1M-row filters and arithmetic -----------------------------------------------------------
{
  const rows = 1_048_576;
  const D = decimal(12, 2);
  const N = decimal(null, null);
  const labels = texts(Array.from({ length: 100 }, (_, k) => `v${String(k)}`));
  const data = batches(rows, (_start, n) => [
    numbers(
      I32,
      Array.from({ length: n }, () => Math.floor(random() * 1_000_000)),
    ),
    dictionary(
      labels,
      Array.from({ length: n }, () => Math.floor(random() * 100)),
    ),
    decimals(
      D,
      Array.from({ length: n }, () => [BigInt(Math.floor(random() * 1e9)), 2] as const),
      "safe",
      2,
    ),
    decimals(
      D,
      Array.from({ length: n }, () => [BigInt(Math.floor(random() * 1e9)), 2] as const),
      "safe",
      2,
    ),
    decimals(
      N,
      Array.from(
        { length: n },
        () => [BigInt(Math.floor(random() * 1e6)), Math.floor(random() * 4)] as const,
      ),
    ),
    numbers(
      I64,
      Array.from({ length: n }, () => Math.floor(random() * 1e12)),
    ),
  ]);
  const types = [I32, TEXT, D, D, N, I64];
  const context = { arena: new OutputArena(UNCHARGED, BATCH), params: { slots: [] }, input: types };

  const lt = new Predicate(cmp("<", col(0, I32), lit(numbers(I32, [500_000]))), context);
  bench(
    "WHERE i07 < 500000 (selection)",
    rows,
    "DuckDB 8.9 ms (scan_proj3_agg, whole query)",
    () => {
      let kept = 0;
      for (const batch of data) kept += lt.select(batch).count;
      return kept;
    },
  );
  const eq = new Predicate(cmp("=", col(1, TEXT), lit(texts(["v7"]))), context);
  bench("WHERE d = 'v7' (dictionary codes)", rows, "—", () => {
    let kept = 0;
    for (const batch of data) kept += eq.select(batch).count;
    return kept;
  });
  const both = new Predicate(
    {
      op: "and",
      type: BOOL,
      args: [
        cmp("<", col(0, I32), lit(numbers(I32, [500_000]))),
        cmp("=", col(1, TEXT), lit(texts(["v7"]))),
      ],
    },
    context,
  );
  bench("WHERE i07 < 500000 AND d = 'v7'", rows, "—", () => {
    let kept = 0;
    for (const batch of data) kept += both.select(batch).count;
    return kept;
  });
  const decAdd = compile(call("add", N, col(2, D), col(3, D)), types);
  bench("n00 + n01, NUMERIC(12,2)", rows, "agg_numeric_sum target ≤ 5 ms incl. SUM", () => {
    let s = 0;
    for (const batch of data) {
      const out = decAdd.evaluate(batch, null) as FlatVector;
      const c = (out.data as { coefficients: { values: Float64Array } }).coefficients.values;
      s += c[0] ?? 0;
    }
    return s;
  });
  const decMul = compile(call("mul", N, col(4, N), col(4, N)), types);
  bench("n * n, unconstrained NUMERIC (per-slot scales)", rows, "—", () => {
    let s = 0;
    for (const batch of data) {
      const out = decMul.evaluate(batch, null) as FlatVector;
      s += out.length;
    }
    return s;
  });
  const intAdd = compile(call("add", I64, col(5, I64), col(5, I64)), types);
  bench("bigint + bigint (safe representation)", rows, "—", () => {
    let s = 0;
    for (const batch of data) s += sumOf(intAdd.evaluate(batch, null), batch.length);
    return s;
  });
}

console.log(`| shape | rows | min ms | median ms | bytes allocated per batch | reference |`);
console.log(`|---|---:|---:|---:|---:|---|`);
for (const r of results)
  console.log(
    `| ${r.shape} | ${r.rows.toLocaleString("en-US")} | ${r.min.toFixed(2)} | ${r.ms.toFixed(2)} | ${r.bytes === null ? "—" : r.bytes.toFixed(0)} | ${r.reference} |`,
  );
