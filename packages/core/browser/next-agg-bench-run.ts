/**
 * Page side of browser-tests/next-agg-bench.spec.ts: the harness's aggregation shapes
 * (next-agg-shapes.ts) through the new executor's aggregate operator in a real browser, wall
 * time per run (min and median). Operator cost only, as scripts/next-agg-bench.mts measures it in
 * Node: prebuilt batches, no scan, output discarded. Recorded, not judged.
 */
import { AggregateOperator } from "../src/next/exec/agg/aggregate.js";
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
} from "../src/next/exec/kernels/test-support.js";
import { OutputArena } from "../src/next/exec/kernels/vectors.js";
import { drive, type Operator } from "../src/next/exec/pipeline.js";
import type { OutputColumn } from "../src/next/ir/ir.js";
import type { Batch } from "../src/next/vector/vector.js";
import { aggShapes, type AggShape } from "./next-agg-shapes.js";

export interface AggBenchResult {
  readonly shape: string;
  readonly reference: string;
  readonly rows: number;
  readonly runsMs: readonly number[];
  readonly minMs: number;
  readonly medianMs: number;
}

const sink: Operator = {
  push: () => "ready",
  ready: () => Promise.resolve(),
  finish: () => undefined,
  abort: () => undefined,
};

async function runOnce(shape: AggShape): Promise<void> {
  const output: OutputColumn[] = [
    ...shape.groupBy.map((k) => ({ name: "k", type: shape.input[k] ?? I32, nullable: true })),
    ...shape.aggregates.map((a) => ({ name: a.fn, type: a.type, nullable: true })),
  ];
  const operator = new AggregateOperator(
    {
      id: 1,
      op: "aggregate",
      output,
      input: { id: 0, op: "values", output: [], rows: [] },
      groupBy: shape.groupBy,
      groupingSets: null,
      aggregates: shape.aggregates,
    },
    { arena: new OutputArena(UNCHARGED, 2048), params: { slots: [] }, input: shape.input },
    UNCHARGED,
    sink,
  );
  let next = 0;
  const batches: Batch[] = shape.batches;
  await drive(
    {
      next: () => {
        const batch = batches[next++];
        return batch === undefined ? null : { batch };
      },
      abort: () => undefined,
    },
    operator,
  );
}

/** Each shape (or those whose names contain one of `only`): 2 warm-ups, then `repeats` runs. */
export async function runAggBench(
  rows = 1_048_576,
  repeats = 5,
  only: readonly string[] = [],
): Promise<AggBenchResult[]> {
  const lib = { I32, I64, F64, TEXT, TS, decimal, numbers, texts, dictionary, decimals, col };
  const results: AggBenchResult[] = [];
  for (const shape of aggShapes(lib, rows, only)) {
    for (let w = 0; w < 2; w++) await runOnce(shape);
    const runsMs: number[] = [];
    for (let r = 0; r < repeats; r++) {
      const start = performance.now();
      await runOnce(shape);
      runsMs.push(performance.now() - start);
    }
    const sorted = [...runsMs].sort((a, b) => a - b);
    results.push({
      shape: shape.name,
      reference: shape.reference,
      rows,
      runsMs,
      minMs: sorted[0] ?? 0,
      medianMs: sorted[Math.floor(sorted.length / 2)] ?? 0,
    });
  }
  return results;
}
