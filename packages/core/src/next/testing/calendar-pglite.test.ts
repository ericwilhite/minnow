/** PostgreSQL calendar arithmetic retains month/day/microsecond fields through lowering. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CALENDAR_SQL_REACH, CALENDAR_IDENTITY_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("SET TIME ZONE 'UTC'");
  core = await new StorageCore().open();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
interface Shape {
  year: number;
  month: number;
  day: number;
  months: number;
  days: number;
  micros: number;
  subtract: boolean;
  kind: number;
  scope: number;
  nil: boolean;
}
it.each(seedsFor("next-calendar-sql", [0xb660, 0xb661]))(
  "keeps calendar fields, month clamping and exact microseconds (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Calendar SQL", CALENDAR_SQL_REACH);
    const first: Shape = {
      year: 2024,
      month: 1,
      day: 31,
      months: 1,
      days: 0,
      micros: 1,
      subtract: false,
      kind: 0,
      scope: 0,
      nil: false,
    };
    const examples: Array<[Shape]> = [
      [first],
      [{ ...first, months: 0, days: 0, micros: 9007199254740991 }],
      [{ ...first, months: 0, days: 0, micros: 9007199254740990 }],
      [{ ...first, subtract: true, scope: 1 }],
      [{ ...first, month: 2, day: 29, months: 12, kind: 1, scope: 2 }],
      [{ ...first, months: -2, days: 3, micros: -7, kind: 2, scope: 3 }],
      [{ ...first, nil: true }],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          year: fc.integer({ min: 1996, max: 2030 }),
          month: fc.integer({ min: 1, max: 12 }),
          day: fc.integer({ min: 1, max: 31 }),
          months: fc.integer({ min: -25, max: 25 }),
          days: fc.integer({ min: -3, max: 3 }),
          micros: fc.integer({ min: -1500001, max: 1500001 }),
          subtract: fc.boolean(),
          kind: fc.integer({ min: 0, max: 2 }),
          scope: fc.integer({ min: 0, max: 3 }),
          nil: fc.boolean(),
        }),
        async (s) => {
          reach.beginHistory();
          const day = Math.min(s.day, new Date(Date.UTC(s.year, s.month, 0)).getUTCDate());
          const date = `${String(s.year)}-${String(s.month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
          const type = ["TIMESTAMP", "DATE", "TIMESTAMPTZ"][s.kind] ?? "TIMESTAMP";
          const literal = s.nil
            ? `NULL::${type}`
            : s.kind === 2
              ? `(TIMESTAMP '${date} 23:59:59.123456')::timestamptz`
              : `${type} '${date}${s.kind === 1 ? "" : " 23:59:59.123456"}'`;
          const expr = `${literal} ${s.subtract ? "-" : "+"} INTERVAL '${String(s.months)} months ${String(s.days)} days ${String(s.micros)} microseconds'`;
          const sql =
            s.scope === 0
              ? `SELECT (${expr})::text AS v`
              : s.scope === 1
                ? `SELECT v::text AS v FROM (SELECT ${expr} AS v) q`
                : s.scope === 2
                  ? `WITH q AS (SELECT ${expr} AS v) SELECT v::text AS v FROM q`
                  : `SELECT v::text AS v FROM calendar_view${String(s.kind)}`;
          if (s.scope === 3) {
            await core.execute(`DROP VIEW IF EXISTS calendar_view${String(s.kind)}`);
            await pg.exec(`DROP VIEW IF EXISTS calendar_view${String(s.kind)}`);
            await core.execute(`CREATE VIEW calendar_view${String(s.kind)} AS SELECT ${expr} AS v`);
            await pg.exec(`CREATE VIEW calendar_view${String(s.kind)} AS SELECT ${expr} AS v`);
          }
          const expected = (await pg.query(sql)).rows;
          const result = await core.execute(sql);
          if (!("batches" in result)) throw new Error("Expected calendar rows");
          expect(oracleRows(result), sql).toEqual(expected);
          reach.reach(["timestamp", "date", "timestamptz"][s.kind] ?? "timestamp");
          reach.reach(["direct", "derived", "CTE", "view"][s.scope] ?? "direct");
          reach.reach(s.subtract ? "subtract" : "add");
          if (s.nil) reach.reach("NULL");
          else {
            if (s.months !== 0 && day > 28) reach.reach("month end");
            if (s.micros % 1000 !== 0) reach.reach("sub-millisecond");
            if (s.months < 0 || s.days < 0 || s.micros < 0) reach.reach("negative field");
          }
        },
      ),
      { seed, numRuns: 65 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);
it.each(seedsFor("next-calendar-identity", [0xb674, 0xb675]))(
  "keeps adjacent interval literals distinct through clones and CSE (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Calendar source identity", CALENDAR_IDENTITY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 999 }),
        fc.boolean(),
        fc.integer({ min: 0, max: 5 }),
        async (micros, wide, scope) => {
          reach.beginHistory();
          const base = wide ? 9007199254739000 + micros : micros;
          const a = `TIMESTAMP '2000-01-01' + INTERVAL '${String(base)} microseconds'`,
            b = `TIMESTAMP '2000-01-01' + INTERVAL '${String(base + 1)} microseconds'`;
          const select = `SELECT TIMESTAMP '2000-01-01' + INTERVAL '${String(base)} microseconds' AS a, TIMESTAMP '2000-01-01' + INTERVAL '${String(base + 1)} microseconds' AS b`;
          let sql =
            scope === 0
              ? select
              : scope === 1
                ? `SELECT a,b FROM(${select}) q`
                : scope === 2
                  ? `WITH q AS(${select}) SELECT a,b FROM q`
                  : "SELECT a,b FROM calendar_identity";
          if (scope === 3) {
            for (const db of [core, pg]) {
              if (db === core) {
                await core.execute("DROP VIEW IF EXISTS calendar_identity");
                await core.execute(`CREATE VIEW calendar_identity AS ${select}`);
              } else {
                await pg.exec("DROP VIEW IF EXISTS calendar_identity");
                await pg.exec(`CREATE VIEW calendar_identity AS ${select}`);
              }
            }
          }
          if (scope === 4) sql = `SELECT MIN(${a}) AS a,MIN(${b}) AS b FROM(VALUES (1),(2)) q(n)`;
          if (scope === 5)
            sql = `SELECT ${a} AS a,${b} AS b FROM(VALUES (1),(2)) q(n) GROUP BY ${a},${b}`;
          sql = `SELECT a::text AS a,b::text AS b FROM(${sql}) q`;
          const result = await core.execute(sql);
          if (!("batches" in result)) throw new Error("Expected calendar rows");
          const rows = oracleRows(result);
          expect(rows, sql).toEqual((await pg.query(sql)).rows);
          expect(rows[0]?.a).not.toBe(rows[0]?.b);
          reach.reach(
            ["direct", "derived", "CTE", "view", "aggregate", "grouped"][scope] ?? "direct",
          );
          reach.reach(wide ? "wide" : "small");
          reach.reach("adjacent exact");
        },
      ),
      {
        seed,
        numRuns: 30,
        examples: [
          [991, true, 0],
          [1, false, 1],
          [1, true, 2],
          [1, false, 3],
          [1, true, 4],
          [1, true, 5],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
