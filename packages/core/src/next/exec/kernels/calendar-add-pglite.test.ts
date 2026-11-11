/** PG timestamp/date +/- interval, including physical range edges and wide microseconds. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, CALENDAR_VECTOR_REACH } from "../../../testing/reach.js";
import type { ScalarExpr } from "../../ir/ir.js";
import { TIMESTAMP_MIN, TIMESTAMP_END, TIMESTAMP_POS_INFINITY } from "../../temporal.js";
import { compileExpr } from "./compile.js";
import { OutputArena } from "./vectors.js";
import { TEXT, col, batchOf, valueAt } from "./test-support.js";
import { temporalVector, pgTemporal, type IntervalValue } from "./temporal-test-support.js";
const TS = { kind: "timestamp", zoned: false } as const,
  IV = { kind: "interval" } as const;
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("SET TIME ZONE 'UTC'");
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-calendar-vectors", [0xb670, 0xb671]))(
  "preserves exact edges, selected encodings and overflow (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Calendar physical vectors", CALENDAR_VECTOR_REACH);
    const zero: IntervalValue = { months: 0, days: 0, micros: 0n };
    const examples: Array<[bigint | null, IntervalValue, boolean, number]> = [
      [TIMESTAMP_MIN, { ...zero, micros: -1n }, false, 0],
      [TIMESTAMP_END - 1n, { ...zero, micros: 1n }, false, 1],
      [0n, { ...zero, micros: 9007199254740993n }, false, 2],
      [0n, { ...zero, micros: 9007199254740992n }, true, 1],
      [0n, { ...zero, months: -2147483648 }, true, 0],
      [0n, { ...zero, days: -2147483648 }, true, 1],
      [0n, { ...zero, micros: -9223372036854775808n }, true, 2],
      [TIMESTAMP_POS_INFINITY, zero, false, 0],
      [null, zero, false, 1],
      [0n, { ...zero, months: 2147483647 }, false, 0],
      [TIMESTAMP_MIN, zero, false, 2],
      [9223360790400000000n, { ...zero, months: -4, micros: 1n }, true, 0],
      [TIMESTAMP_END - 1n, zero, false, 1],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(fc.bigInt({ min: TIMESTAMP_MIN, max: TIMESTAMP_END - 1n }), fc.constant(null)),
        fc.record({
          months: fc.integer({ min: -500, max: 500 }),
          days: fc.integer({ min: -100, max: 100 }),
          micros: fc.bigInt({ min: -9223372036854775808n, max: 9223372036854775807n }),
        }),
        fc.boolean(),
        fc.integer({ min: 0, max: 2 }),
        async (value, interval, subtract, form) => {
          reach.beginHistory();
          const sql = `SELECT (${pgTemporal(TS, value)} ${subtract ? "-" : "+"} ${pgTemporal(IV, interval)})::text AS v`;
          let expected: unknown, state: string | undefined;
          try {
            expected = (await pg.query<{ v: string | null }>(sql)).rows[0]?.v;
          } catch (error) {
            state = (error as { code: string }).code;
          }
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
            const arithmetic: ScalarExpr = {
              op: "call",
              fn: subtract ? "sub" : "add",
              args: [col(0, TS), col(1, IV)],
              type: TS,
            };
            const expr: ScalarExpr = { op: "cast", input: arithmetic, type: TEXT };
            const a = temporalVector(TS, [value, 0n, value], false, form),
              b = temporalVector(IV, [interval, zero, interval], false, form);
            const batch = {
              ...batchOf([a, b]),
              selection: { rows: Uint16Array.of(2, 0), count: 2 },
            };
            const run = () =>
              compileExpr(expr, { arena, input: [TS, IV], params: { slots: [] } }).evaluate(
                batch,
                batch.selection,
              );
            if (state) {
              expect(run, sql).toThrow(expect.objectContaining({ sqlState: state }));
              reach.reach("PG error");
            } else {
              const result = run();
              for (const row of batch.selection.rows)
                expect(valueAt(result, row), sql).toEqual(expected);
              reach.reach("exact text");
            }
          } finally {
            arena.close();
          }
          expect(held).toBe(0);
          reach.reach(["flat", "dictionary", "constant"][form] ?? "flat");
          reach.reach(subtract ? "subtract" : "add");
          if (value === null) reach.reach("NULL");
          if (value === TIMESTAMP_POS_INFINITY) reach.reach("infinity");
          if (interval.micros > 9007199254740991n || interval.micros < -9007199254740991n)
            reach.reach("wide microseconds");
          reach.reach("returned charge");
        },
      ),
      { seed, numRuns: 70 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);
