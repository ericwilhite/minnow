/** Grouped float update component over finite, flat float8 inputs.
 * --implementation states (default) measures real accumulator dispatch and pair eligibility;
 * loops is a diagnostic mirror of the finite loop bodies. Fixture construction, group assignment,
 * allocation, reset and verification are untimed. Per-aggregate input order and SUM's -0 identity
 * are retained. Scan, operator expression dispatch, NULL/overflow fallback, emission and spill
 * are outside this component. PGlite tests check fallback semantics; the whole benchmark checks
 * complete queries. No spill performance is claimed.
 */
import { strict as assert } from "node:assert";
import { cpuUsage } from "node:process";
import { tmpdir, loadavg } from "node:os";
import { mkdir, writeFile, readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { join, resolve } from "node:path";
import { aggShapes } from "../packages/core/browser/next-agg-shapes.ts";
import * as lib from "../packages/core/dist/next/exec/kernels/test-support.js";
import type { Vector } from "../packages/core/dist/next/vector/vector.js";
import { GroupTable } from "../packages/core/dist/next/exec/agg/groups.js";
import { accumulatorFor, updateGroupedPair } from "../packages/core/dist/next/exec/agg/states.js";
function at<T>(values: ArrayLike<T>, index: number): T {
  const value = values[index];
  if (value === undefined) throw new RangeError("Missing fixture/state element");
  return value;
}
const option = (name: string, fallback: string) => {
  const at = process.argv.indexOf(name);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
const rows = Number(option("--rows", "1000000")),
  rounds = Number(option("--rounds", "7")),
  warmups = Number(option("--warmups", "12")),
  iterations = Number(option("--iterations", "16")),
  implementation = option("--implementation", "states"),
  directory = resolve(option("--output", join(tmpdir(), "b4-fused-update")));
for (const [name, n] of Object.entries({ rows, rounds, warmups, iterations }))
  if (!Number.isSafeInteger(n) || n < (name === "rounds" ? 5 : 1)) throw new RangeError(name);
if (implementation !== "loops" && implementation !== "states") throw new Error("implementation");
await mkdir(directory, { recursive: true });
interface Input {
  values: Float64Array;
  other: Float64Array;
  gids: Int32Array;
  rows: Uint16Array;
  count: number;
  left: Vector;
  right: Vector;
}
interface State {
  counts: Float64Array;
  sums: Float64Array;
  has: Uint8Array;
  other: Float64Array;
  otherHas: Uint8Array;
}
function countLoop(counts: Float64Array, batch: Input) {
  const { gids, rows, count } = batch;
  for (let k = 0; k < count; k++) {
    const g = gids[rows[k] ?? 0] ?? 0;
    counts[g] = (counts[g] ?? 0) + 1;
  }
}
function sumLoop(sums: Float64Array, has: Uint8Array, values: Float64Array, batch: Input) {
  const { gids, rows, count } = batch;
  for (let k = 0; k < count; k++) {
    const i = rows[k] ?? 0,
      g = gids[i] ?? 0,
      next = (sums[g] ?? 0) + (values[i] ?? 0);
    if (next - next !== 0) throw new Error("finite fixture exceeded kernel");
    sums[g] = next;
    has[g] = 1;
  }
}
function fusedCountSum(state: State, batch: Input) {
  const { counts, sums, has } = state,
    { values, gids, rows, count } = batch;
  for (let k = 0; k < count; k++) {
    const i = rows[k] ?? 0,
      g = gids[i] ?? 0,
      next = (sums[g] ?? 0) + (values[i] ?? 0);
    if (next - next !== 0) throw new Error("finite fixture exceeded kernel");
    counts[g] = (counts[g] ?? 0) + 1;
    sums[g] = next;
    has[g] = 1;
  }
}
function fusedTwoSums(state: State, batch: Input) {
  const { sums, has, other, otherHas } = state,
    { values, gids, rows, count } = batch;
  for (let k = 0; k < count; k++) {
    const i = rows[k] ?? 0,
      g = gids[i] ?? 0,
      a = (sums[g] ?? 0) + (values[i] ?? 0),
      b = (other[g] ?? 0) + (batch.other[i] ?? 0);
    if (a - a !== 0 || b - b !== 0) throw new Error("finite fixture exceeded kernel");
    sums[g] = a;
    has[g] = 1;
    other[g] = b;
    otherHas[g] = 1;
  }
}
const results: unknown[] = [];
for (const kind of ["highcard", "twoSums"] as const) {
  const shape = aggShapes(lib, rows, [
    kind === "highcard" ? "agg_highcard_int" : "agg_3dims_20aggs",
  ])[0];
  assert.ok(shape);
  const groupTypes = shape.groupBy.map((i) => at(shape.input, i));
  const table = new GroupTable(groupTypes, lib.UNCHARGED),
    input: Input[] = [];
  for (const batch of shape.batches) {
    const gids = new Int32Array(batch.length);
    table.assign(
      shape.groupBy.map((i) => at(batch.columns, i)),
      batch.length,
      null,
      gids,
    );
    const a = batch.columns[kind === "highcard" ? 1 : 18],
      b = batch.columns[kind === "highcard" ? 1 : 19];
    assert.ok(a?.form === "flat" && a.data.layout === "f64" && a.validity === null);
    assert.ok(b?.form === "flat" && b.data.layout === "f64" && b.validity === null);
    input.push({
      left: a,
      right: b,
      values: a.data.values,
      other: b.data.values,
      gids,
      rows: Uint16Array.from({ length: batch.length }, (_, i) => i),
      count: batch.length,
    });
  }
  const groups = table.groupCount;
  table.close();
  const state: State = {
    counts: new Float64Array(groups),
    sums: new Float64Array(groups),
    has: new Uint8Array(groups),
    other: new Float64Array(groups),
    otherHas: new Uint8Array(groups),
  };
  const reset = () => {
    state.counts.fill(0);
    state.sums.fill(-0);
    state.has.fill(0);
    state.other.fill(-0);
    state.otherHas.fill(0);
  };
  const run = (fused: boolean) => {
    for (const batch of input) {
      if (fused) {
        if (kind === "highcard") fusedCountSum(state, batch);
        else fusedTwoSums(state, batch);
      } else {
        if (kind === "highcard") countLoop(state.counts, batch);
        sumLoop(state.sums, state.has, batch.values, batch);
        if (kind === "twoSums") sumLoop(state.other, state.otherHas, batch.other, batch);
      }
    }
  };
  // Compare every result with ordinary production accumulators before measuring.
  const count = accumulatorFor("count_star", lib.I64, lib.I64, lib.UNCHARGED),
    sum = accumulatorFor("sum", lib.F64, lib.F64, lib.UNCHARGED),
    other = accumulatorFor("sum", lib.F64, lib.F64, lib.UNCHARGED);
  for (const acc of [count, sum, other]) acc.grow(groups);
  for (let k = 0; k < input.length; k++) {
    const batch = at(input, k),
      source = at(shape.batches, k);
    count.update(null, batch.gids, batch.rows, batch.count, false);
    sum.update(
      at(source.columns, kind === "highcard" ? 1 : 18),
      batch.gids,
      batch.rows,
      batch.count,
      false,
    );
    other.update(
      at(source.columns, kind === "highcard" ? 1 : 19),
      batch.gids,
      batch.rows,
      batch.count,
      false,
    );
  }
  reset();
  run(false);
  for (let first = 0; first < groups; first += 2048) {
    const n = Math.min(2048, groups - first),
      c = count.emit(first, n),
      a = sum.emit(first, n),
      b = other.emit(first, n);
    for (let i = 0; i < n; i++) {
      if (kind === "highcard") assert.equal(BigInt(at(state.counts, first + i)), lib.valueAt(c, i));
      assert.ok(Object.is(state.sums[first + i], lib.valueAt(a, i)));
      if (kind === "twoSums") assert.ok(Object.is(state.other[first + i], lib.valueAt(b, i)));
    }
  }
  const expected = {
    counts: state.counts.slice(),
    sums: state.sums.slice(),
    has: state.has.slice(),
    other: state.other.slice(),
    otherHas: state.otherHas.slice(),
  };
  reset();
  run(true);
  assert.deepEqual(state, expected);
  for (const acc of [count, sum, other]) acc.close();
  const makePair = () => {
    const first = accumulatorFor(
        kind === "highcard" ? "count_star" : "sum",
        kind === "highcard" ? lib.I64 : lib.F64,
        kind === "highcard" ? null : lib.F64,
        lib.UNCHARGED,
      ),
      second = accumulatorFor("sum", lib.F64, lib.F64, lib.UNCHARGED);
    first.grow(groups);
    second.grow(groups);
    return { first, second };
  };
  const control = makePair(),
    candidate = makePair();
  const execute = (fused: boolean) => {
    if (implementation === "loops") {
      reset();
      run(fused);
      return;
    }
    const pair = fused ? candidate : control;
    for (const batch of input) {
      const left = kind === "highcard" ? null : batch.left;
      if (fused) {
        if (
          !updateGroupedPair(
            pair.first,
            pair.second,
            left,
            batch.right,
            batch.gids,
            batch.rows,
            batch.count,
          )
        )
          throw new Error("component did not reach fusion");
      } else {
        pair.first.update(left, batch.gids, batch.rows, batch.count, false);
        pair.second.update(batch.right, batch.gids, batch.rows, batch.count, false);
      }
    }
  };
  const verifyPairs = () => {
    for (let first = 0; first < groups; first += 2048) {
      const n = Math.min(2048, groups - first);
      assert.deepEqual(candidate.first.emit(first, n), control.first.emit(first, n));
      assert.deepEqual(candidate.second.emit(first, n), control.second.emit(first, n));
    }
  };
  // Every group, before and after timing, including all carried state across measured passes.
  execute(false);
  execute(true);
  verifyPairs();
  for (let i = 0; i < warmups; i++)
    for (const fused of [false, true]) {
      execute(fused);
    }
  const samples = { separate: [] as number[], fused: [] as number[] },
    load: number[][] = [],
    order: string[] = [];
  for (let round = 0; round < rounds; round++) {
    load.push(loadavg());
    for (const fused of round % 2 === 0 ? [false, true] : [true, false]) {
      const policy = fused ? "fused" : "separate";
      let total = 0;
      order.push(policy);
      for (let i = 0; i < iterations; i++) {
        if (implementation === "loops") reset();
        const before = cpuUsage();
        if (implementation === "loops") run(fused);
        else execute(fused);
        const spent = cpuUsage(before);
        total += (spent.user + spent.system) / 1000;
      }
      samples[policy].push(total / iterations);
    }
  }
  verifyPairs();
  for (const pair of [control, candidate]) {
    pair.first.close();
    pair.second.close();
  }
  const stats = Object.fromEntries(
    Object.entries(samples).map(([policy, values]) => {
      const sorted = [...values].sort((a, b) => a - b);
      return [
        policy,
        {
          samples: values,
          median: sorted[Math.floor(sorted.length / 2)],
          spread: at(sorted, sorted.length - 1) - at(sorted, 0),
        },
      ];
    }),
  );
  results.push({
    kind,
    implementation,
    rows,
    groups,
    covers: kind === "highcard" ? "2/2" : "2/21",
    rounds,
    warmups,
    iterations,
    order,
    load,
    cpuMs: stats,
  });
}
const hashes: Record<string, string> = {};
for (const path of [
  "scripts/next-agg-fused-update-bench.mts",
  "packages/core/src/next/exec/agg/states.ts",
  "packages/core/browser/next-agg-shapes.ts",
])
  hashes[path] = createHash("sha256")
    .update(await readFile(path))
    .digest("hex");
const report = {
  lane: "Node",
  timing: "process user+system CPU",
  results,
  hashes,
  limits:
    "Finite flat grouped update component only. States mode compares real accumulator.update calls with updateGroupedPair, including eligibility and carried states; loops mode is a diagnostic mirror. Every group is checked before and after timing. No NULL/exception fallback, scan, emission, spill or whole-query acceptance.",
};
await writeFile(join(directory, "baseline.json"), JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report));
