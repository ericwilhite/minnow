/**
 * Whole-operator A/B comparison over identical prebuilt batches (no scan/decode).
 * Build and snapshot each candidate first; the snapshots must have identical interfaces.
 * Each shape gets two interleaved warmups, then alternating A/B and B/A CPU-time rounds.
 *
 * node scripts/next-agg-compare.mts --a <parent-dist> --b <candidate-dist> [--repeats 9]
 *   [--only <shape substring>]
 *
 * Reuses input across builds and rounds so constructing the retail dataset does not dominate
 * a loaded host's comparison. Output owns its buffers and is discarded by the same sink.
 */
import path from "node:path";
import { pathToFileURL } from "node:url";
import { aggShapes } from "../packages/core/browser/next-agg-shapes.ts";
import type { AggShape } from "../packages/core/browser/next-agg-shapes.ts";
import type { OutputColumn } from "../packages/core/dist/next/ir/ir.js";
import type { Operator } from "../packages/core/dist/next/exec/pipeline.js";

const args = process.argv.slice(2);
function option(name: string): string | undefined {
  const at = args.indexOf(name);
  return at < 0 ? undefined : args[at + 1];
}
const rounds = Number(option("--repeats") ?? 9);
if (!Number.isInteger(rounds) || rounds < 5) throw new RangeError("At least five rounds required");
const only = option("--only") ?? "";
const rows = 1_048_576;
type Aggregate = typeof import("../packages/core/dist/next/exec/agg/aggregate.js");
type Pipeline = typeof import("../packages/core/dist/next/exec/pipeline.js");
type Vectors = typeof import("../packages/core/dist/next/exec/kernels/vectors.js");
type Support = typeof import("../packages/core/dist/next/exec/kernels/test-support.js");
async function load<T>(directory: string, file: string): Promise<T> {
  return (await import(pathToFileURL(path.join(directory, file)).href)) as T;
}
async function build(flag: string) {
  const value = option(flag);
  if (value === undefined) throw new Error(`Missing ${flag} <dist-directory>`);
  const directory = path.resolve(value);
  const aggregate = await load<Aggregate>(directory, "next/exec/agg/aggregate.js");
  const pipeline = await load<Pipeline>(directory, "next/exec/pipeline.js");
  const vectors = await load<Vectors>(directory, "next/exec/kernels/vectors.js");
  const support = await load<Support>(directory, "next/exec/kernels/test-support.js");
  return { directory, ...aggregate, ...pipeline, ...vectors, support };
}
const a = await build("--a");
const b = await build("--b");
const sink: Operator = {
  push: () => "ready",
  ready: () => Promise.resolve(),
  finish: () => undefined,
  abort: () => undefined,
};
async function run(candidate: typeof a, shape: AggShape): Promise<void> {
  const output: OutputColumn[] = [
    ...shape.groupBy.map((k) => ({
      name: "k",
      type: shape.input[k] ?? candidate.support.I32,
      nullable: true,
    })),
    ...shape.aggregates.map((call) => ({ name: call.fn, type: call.type, nullable: true })),
  ];
  const operator = new candidate.AggregateOperator(
    {
      id: 1,
      op: "aggregate",
      output,
      input: { id: 0, op: "values", output: [], rows: [] },
      groupBy: shape.groupBy,
      groupingSets: null,
      aggregates: shape.aggregates,
    },
    {
      arena: new candidate.OutputArena(candidate.support.UNCHARGED, 2048),
      params: { slots: [] },
      input: shape.input,
    },
    candidate.support.UNCHARGED,
    sink,
  );
  let next = 0;
  await candidate.drive(
    {
      next: () => {
        const batch = shape.batches[next++];
        return batch === undefined ? null : { batch };
      },
      abort: () => undefined,
    },
    operator,
  );
}
function summary(runs: number[]) {
  const sorted = [...runs].sort((x, y) => x - y);
  return {
    median: sorted[Math.floor(sorted.length / 2)] ?? 0,
    spread: (sorted[sorted.length - 1] ?? 0) - (sorted[0] ?? 0),
    runs,
  };
}
const results = [];
for (const name of [
  "agg_highcard_int",
  "agg_3dims_20aggs",
  "agg_count_distinct",
  "agg_count_distinct_dictionary",
  "agg_numeric_sum",
]) {
  if (!name.includes(only)) continue;
  const shape = aggShapes(a.support, rows, [name]).find((item) => item.name === name);
  if (shape === undefined) throw new Error(`Missing shape ${name}`);
  for (let w = 0; w < 2; w++) {
    await run(a, shape);
    await run(b, shape);
  }
  const samples = { a: [] as number[], b: [] as number[] };
  for (let round = 0; round < rounds; round++) {
    const order = round % 2 === 0 ? (["a", "b"] as const) : (["b", "a"] as const);
    for (const id of order) {
      const before = process.cpuUsage();
      await run(id === "a" ? a : b, shape);
      const spent = process.cpuUsage(before);
      samples[id].push((spent.user + spent.system) / 1000);
    }
  }
  const result = { shape: name, rows, a: summary(samples.a), b: summary(samples.b) };
  results.push(result);
  console.log(JSON.stringify(result));
}
console.log(
  JSON.stringify({ cpuMilliseconds: true, a: a.directory, b: b.directory, rounds, results }),
);
