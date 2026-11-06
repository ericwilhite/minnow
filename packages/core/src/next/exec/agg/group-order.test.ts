/**
 * Group-ordered updates (aggregate.ts `#sortByGroup`): with many groups and several aggregates the
 * operator hands its accumulators the batch's rows sorted by group id, stably. The law is that
 * this never changes an answer: per group, rows keep their input order, which float SUM / AVG
 * (IEEE sums in input order, PostgreSQL's float8pl) and MIN/MAX ties (the newer value wins)
 * depend on. Generated batches are aggregated with and without the ordering and must agree
 * exactly; the unordered path is the one judged against PostgreSQL (aggregate-pglite.test.ts).
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../../testing/seeds.js";
import type { AggregateCall } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { Batch } from "../../vector/vector.js";
import {
  F64,
  I32,
  I64,
  UNCHARGED,
  col,
  decimal,
  decimals,
  numbers,
  valueAt,
} from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import type { Operator, PushResult } from "../pipeline.js";
import { AggregateOperator } from "./aggregate.js";

const SEEDS = seedsFor("next-aggregate-group-order", [0x50a7, 0x3]);
const D = decimal(null, null);
const INPUT: PhysicalType[] = [I32, F64, D, I32];

const call = (
  fn: AggregateCall["fn"],
  type: PhysicalType,
  column: number | null,
  extra: Partial<AggregateCall> = {},
): AggregateCall => ({
  fn,
  args: column === null ? [] : [col(column, INPUT[column] ?? I32)],
  distinct: false,
  filter: null,
  orderBy: [],
  type,
  ...extra,
});

const AGGREGATES: AggregateCall[] = [
  call("count_star", I64, null),
  call("sum", F64, 1),
  call("avg", F64, 1),
  call("min", F64, 1),
  call("max", F64, 1),
  call("min", D, 2),
  call("max", D, 2),
  call("sum", D, 2),
  call("count", I64, 3, { distinct: true }),
  call("sum", I64, 3),
];

async function aggregate(batches: readonly Batch[], groupOrder: boolean): Promise<unknown[][]> {
  const rows: unknown[][] = [];
  const sink: Operator = {
    push(batch: Batch): PushResult {
      for (let r = 0; r < batch.length; r++)
        rows.push(
          batch.columns.map((c) => {
            const v = valueAt(c, r);
            return typeof v === "number" && Object.is(v, -0) ? "-0" : v;
          }),
        );
      return "ready";
    },
    ready: () => Promise.resolve(),
    finish: () => undefined,
    abort: () => undefined,
  };
  const op = new AggregateOperator(
    {
      id: 1,
      op: "aggregate",
      output: [],
      input: { id: 0, op: "values", output: [], rows: [] },
      groupBy: [0],
      groupingSets: null,
      aggregates: AGGREGATES,
    },
    { arena: new OutputArena(UNCHARGED, 2048), input: INPUT, params: { slots: [] } },
    UNCHARGED,
    sink,
    { groupOrder },
  );
  for (const batch of batches) op.push(batch);
  await op.finish();
  return rows;
}

describe("group-ordered aggregate updates never change an answer", () => {
  it.each(SEEDS)(
    "many groups, order-sensitive sums and ties (seed %i)",
    async (seed) => {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(5000, 20_000, 70_000),
          fc.integer({ min: 4097, max: 9000 }),
          fc.integer(),
          async (rows, groups, inner) => {
            const random = mulberry32(inner);
            const pick = <T>(xs: readonly T[]): T => xs[Math.floor(random() * xs.length)] as T;
            const batches: Batch[] = [];
            for (let from = 0; from < rows;) {
              const n = Math.min(rows - from, 1 + Math.floor(random() * 2048));
              from += n;
              const selected = random() < 0.3;
              batches.push({
                length: n,
                selection: selected
                  ? (() => {
                      const picked = Array.from({ length: n }, (_, k) => k).filter(
                        () => random() < 0.7,
                      );
                      return { rows: Uint16Array.from(picked), count: picked.length };
                    })()
                  : null,
                columns: [
                  numbers(
                    I32,
                    Array.from({ length: n }, () => Math.floor(random() * groups)),
                  ),
                  // Float values whose sum depends on order (1e16 + 1 − 1e16), and ±0 ties.
                  numbers(
                    F64,
                    Array.from({ length: n }, () => pick([1e16, -1e16, 1, 0.1, -0, 0, null])),
                  ),
                  decimals(
                    D,
                    Array.from({ length: n }, () =>
                      pick([[15n, 1], [150n, 2], [-3n, 0], null] as const),
                    ),
                  ),
                  numbers(
                    I32,
                    Array.from({ length: n }, () => Math.floor(random() * 50)),
                  ),
                ],
              });
            }
            expect(await aggregate(batches, true)).toEqual(await aggregate(batches, false));
          },
        ),
        { seed, numRuns: 12 },
      );
    },
    180_000,
  );
});
