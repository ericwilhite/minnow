/** A fixed stream has one FLOAT8 SUM answer regardless of where its batches end.
 * Authority: states.ts's sequential per-group contract and PostgreSQL SUM(v ORDER BY i).
 * Keep source values fixed while generating only boundaries; no batch-local reassociation.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, FLOAT_SUM_BOUNDARIES_REACH } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { AggregateCall } from "../../ir/ir.js";
import type { Batch } from "../../vector/vector.js";
import { F64, I32, UNCHARGED, col, numbers, valueAt } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import type { Operator } from "../pipeline.js";
import { AggregateOperator } from "./aggregate.js";

// 5000 groups force the existing stable-radix path with four independent FLOAT8 states.
// Even groups sum to 0, odd groups to 1. The latter reject summing a batch independently
// before adding its partial: 1e16 + (-1e16 + 1) differs from (1e16 - 1e16) + 1.
const INPUT = Array.from({ length: 5000 }, (_, group) =>
  (group % 2 === 0 ? [1e16, 1, -1e16] : [1e16, -1e16, 1]).map((value) => ({ group, value })),
).flat();
const TYPES = [I32, F64];
const SUM: AggregateCall = {
  fn: "sum",
  args: [col(1, F64)],
  distinct: false,
  filter: null,
  orderBy: [],
  type: F64,
};
let pg: PGlite;
let expected: unknown[][];
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("CREATE TABLE sum_input(i int PRIMARY KEY,g int,v float8)");
  await pg.exec(
    `INSERT INTO sum_input VALUES ${INPUT.map((row, i) => `(${String(i)},${String(row.group)},${String(row.value)})`).join(",")}`,
  );
  expected = (
    await pg.query<{ g: number; s: number }>(
      "SELECT g,SUM(v ORDER BY i) AS s FROM sum_input GROUP BY g ORDER BY g",
    )
  ).rows.map((row) => [row.g, row.s, row.s, row.s, row.s]);
}, 60_000);
afterAll(async () => {
  await pg.close();
});

async function aggregate(pattern: readonly number[], groupOrder: boolean): Promise<unknown[][]> {
  const output: unknown[][] = [];
  const sink: Operator = {
    push(batch) {
      for (let row = 0; row < batch.length; row++)
        output.push(batch.columns.map((column) => valueAt(column, row)));
      return "ready";
    },
    ready: () => Promise.resolve(),
    finish: () => undefined,
    abort: () => undefined,
  };
  const arena = new OutputArena(UNCHARGED, 2048);
  const op = new AggregateOperator(
    {
      id: 1,
      op: "aggregate",
      output: [],
      input: { id: 0, op: "values", rows: [], output: [] },
      groupBy: [0],
      groupingSets: null,
      aggregates: [SUM, SUM, SUM, SUM],
    },
    { arena, input: TYPES, params: { slots: [] } },
    UNCHARGED,
    sink,
    { groupOrder },
  );
  try {
    for (let from = 0, part = 0; from < INPUT.length; part++) {
      const size = pattern[part % pattern.length];
      if (size === undefined) throw new Error("Missing batch boundary");
      const rows = INPUT.slice(from, from + size);
      const batch: Batch = {
        length: rows.length,
        selection: null,
        columns: [
          numbers(
            I32,
            rows.map((row) => row.group),
          ),
          numbers(
            F64,
            rows.map((row) => row.value),
          ),
        ],
      };
      expect(op.push(batch)).toBe("ready");
      from += rows.length;
    }
    await op.finish();
    return output;
  } catch (error) {
    op.abort(error);
    throw error;
  } finally {
    arena.close();
  }
}

it.each(seedsFor("next-float-sum-batch-boundaries", [0xb621, 0xb622]))(
  "keeps the same input stream exact across batch boundaries (seed %i)",
  async (seed) => {
    const reach = createReach<string>("FLOAT8 SUM batch boundaries", FLOAT_SUM_BOUNDARIES_REACH);
    const examples: Array<[number[]]> = [[[1]], [[17]], [[511]], [[2048]], [[1, 17, 511, 2048]]];
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: 1, max: 2048 }), { minLength: 1, maxLength: 5 }),
        async (pattern) => {
          reach.beginHistory();
          for (const groupOrder of [false, true]) {
            const actual = await aggregate(pattern, groupOrder);
            expect(
              actual,
              `boundaries ${pattern.join(",")}, groupOrder ${String(groupOrder)}`,
            ).toEqual(expected);
            reach.reach(groupOrder ? "stable group sorting" : "input-order updates");
          }
          for (const size of [1, 17, 511, 2048])
            if (pattern.includes(size)) reach.reach(`batch ${String(size)}`);
          if (pattern.length > 1) reach.reach("unequal boundaries");
          reach.reach("zero cancellation");
          reach.reach("unit cancellation");
          reach.reach("5000 groups");
        },
      ),
      { seed, numRuns: examples.length + 12, examples },
    );
    reach.assertReached();
  },
  120_000,
);
