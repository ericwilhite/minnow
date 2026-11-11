/** PostgreSQL compares interval duration with 30-day months, retaining original fields. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, INTERVAL_COMPARE_REACH } from "../../../testing/reach.js";
import type { ScalarExpr } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import { compileExpr } from "./compile.js";
import { OutputArena } from "./vectors.js";
import { BOOL, batchOf, col, valueAt } from "./test-support.js";
import { temporalVector, pgTemporal, type IntervalValue } from "./temporal-test-support.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const INTERVAL: PhysicalType = { kind: "interval" };
const ops = ["=", "<>", "<", "<=", ">", ">=", "IS DISTINCT FROM", "IS NOT DISTINCT FROM"] as const;
const interval = fc.record({
  months: fc.integer({ min: -2147483648, max: 2147483647 }),
  days: fc.integer({ min: -2147483648, max: 2147483647 }),
  micros: fc.bigInt({ min: -9223372036854775808n, max: 9223372036854775807n }),
});
const inf: IntervalValue = { months: 2147483647, days: 2147483647, micros: 9223372036854775807n },
  neg: IntervalValue = { months: -2147483648, days: -2147483648, micros: -9223372036854775808n };
const zero: IntervalValue = { months: 0, days: 0, micros: 0n };
function pgValue(v: IntervalValue | null): string {
  return v === inf
    ? "INTERVAL 'infinity'"
    : v === neg
      ? "INTERVAL '-infinity'"
      : pgTemporal(INTERVAL, v);
}
it.each(seedsFor("next-interval-comparison", [0xb664, 0xb665]))(
  "compares exact durations, infinities and NULLs across encodings (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Interval comparison", INTERVAL_COMPARE_REACH);
    const examples: Array<[IntervalValue | null, IntervalValue | null, number, number, boolean]> = [
      [{ ...zero, months: 1 }, { ...zero, days: 30 }, 0, 0, false],
      [{ ...zero, micros: 9007199254740993n }, { ...zero, micros: 9007199254740992n }, 4, 1, true],
      [inf, zero, 4, 2, false],
      [neg, inf, 2, 0, true],
      [inf, inf, 0, 1, false],
      [null, zero, 0, 0, false],
      [null, null, 6, 1, true],
      [zero, null, 7, 2, false],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(interval, fc.constant(inf), fc.constant(neg), fc.constant(null)),
        fc.oneof(interval, fc.constant(inf), fc.constant(neg), fc.constant(null)),
        fc.integer({ min: 0, max: 7 }),
        fc.integer({ min: 0, max: 2 }),
        fc.boolean(),
        async (a, b, mode, form, sparse) => {
          reach.beginHistory();
          const op = ops[mode] ?? "=";
          const expected = (
            await pg.query<{ v: boolean | null }>(`SELECT (${pgValue(a)} ${op} ${pgValue(b)}) AS v`)
          ).rows[0]?.v;
          const l = temporalVector(INTERVAL, [a, inf, a], false, form),
            r = temporalVector(INTERVAL, [b, neg, b], false, form);
          let held = 0;
          const arena = new OutputArena(
            {
              allocate: (n) => {
                held += n;
                return true;
              },
              free: (n) => {
                held -= n;
              },
            },
            32,
          );
          try {
            const expr: ScalarExpr =
              mode < 6
                ? {
                    op: "compare",
                    cmp: op as "=" | "<>" | "<" | "<=" | ">" | ">=",
                    left: col(0, INTERVAL),
                    right: col(1, INTERVAL),
                    collation: null,
                    type: BOOL,
                  }
                : {
                    op: "distinctFrom",
                    negated: mode === 7,
                    left: col(0, INTERVAL),
                    right: col(1, INTERVAL),
                    type: BOOL,
                  };
            const batch = {
              ...batchOf([l, r]),
              selection: sparse
                ? { rows: Uint16Array.of(2, 0), count: 2 }
                : { rows: Uint16Array.of(0), count: 1 },
            };
            const result = compileExpr(expr, {
              arena,
              input: [INTERVAL, INTERVAL],
              params: { slots: [] },
            }).evaluate(batch, batch.selection);
            for (const row of batch.selection.rows) expect(valueAt(result, row)).toBe(expected);
          } finally {
            arena.close();
          }
          expect(held).toBe(0);
          reach.reach(["flat", "dictionary", "constant"][form] ?? "flat");
          reach.reach(sparse ? "sparse" : "selected");
          reach.reach(mode < 6 ? "strict comparison" : "NULL-safe comparison");
          if (a === null || b === null) reach.reach("NULL");
          if (a === inf || a === neg || b === inf || b === neg) reach.reach("infinity");
          if (a?.months === 1 && b?.days === 30) reach.reach("30-day month equality");
          if (
            a !== null &&
            b !== null &&
            (a.micros > 9007199254740991n || a.micros < -9007199254740991n)
          )
            reach.reach("wide exact microseconds");
          reach.reach("returned charge");
        },
      ),
      { seed, numRuns: 70 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);
