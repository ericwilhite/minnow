/** PostgreSQL14+: DATE_PART is float8, EXTRACT is NUMERIC; overload consumers must see that. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DATE_PART_SQL_REACH } from "../../testing/reach.js";
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
const fields = [
  "epoch",
  "second",
  "milliseconds",
  "microseconds",
  "year",
  "month",
  "day",
  "quarter",
  "week",
] as const;
interface Shape {
  kind: number;
  field: number;
  months: number;
  days: number;
  micros: number;
  scope: number;
  nil: boolean;
}
it.each(seedsFor("next-date-part-sql", [0xb678, 0xb679]))(
  "keeps PG result types, interval values and overload admission (seed %i)",
  async (seed) => {
    const reach = createReach<string>("DATE_PART versus EXTRACT", DATE_PART_SQL_REACH);
    const first: Shape = { kind: 0, field: 0, months: 0, days: 0, micros: 1, scope: 0, nil: false };
    const examples: Array<[Shape]> = [
      ...[0, 1, 2, 3, 4].map((scope) => [{ ...first, scope }] as [Shape]),
      [{ ...first, months: 12, days: -365 }],
      [{ ...first, micros: 9007199254740991, field: 1 }],
      [{ ...first, nil: true }],
      [{ ...first, kind: 1 }],
      [{ ...first, kind: 2 }],
      [{ ...first, kind: 3 }],
      ...fields.map(
        (_, field) => [{ ...first, field, months: -13, days: -9, micros: -1234567 }] as [Shape],
      ),
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          kind: fc.integer({ min: 0, max: 3 }),
          field: fc.integer({ min: 0, max: fields.length - 1 }),
          months: fc.integer({ min: -2147483648, max: 2147483647 }),
          days: fc.integer({ min: -2147483648, max: 2147483647 }),
          micros: fc.integer({ min: -9007199254740991, max: 9007199254740991 }),
          scope: fc.integer({ min: 0, max: 4 }),
          nil: fc.boolean(),
        }),
        async (s) => {
          reach.beginHistory();
          const type = ["interval", "date", "timestamp", "timestamptz"][s.kind] ?? "interval";
          let field = fields[s.field] ?? "epoch";
          if (s.kind === 1 && ["second", "milliseconds", "microseconds"].includes(field))
            field = "day";
          const literal = s.nil
            ? `NULL::${type}`
            : s.kind === 0
              ? `INTERVAL '${String(s.months)} months ${String(s.days)} days ${String(s.micros)} microseconds'`
              : s.kind === 1
                ? "DATE '2000-02-29'"
                : `(TIMESTAMP '2000-02-29 12:34:56.123456')::${type}`;
          const d = `date_part('${field}',${literal})`,
            e = `EXTRACT(${field} FROM ${literal})`,
            select = `SELECT ${d} AS d,${e} AS e`;
          const sql =
            s.scope === 0
              ? select
              : s.scope === 1
                ? `SELECT d,e FROM(${select}) q`
                : s.scope === 2
                  ? `WITH q AS(${select}) SELECT d,e FROM q`
                  : s.scope === 3
                    ? "SELECT d,e FROM date_part_view"
                    : `SELECT MIN(${d}) AS d,MIN(${e}) AS e FROM(VALUES (1),(2)) q(n)`;
          if (s.scope === 3) {
            await core.execute("DROP VIEW IF EXISTS date_part_view");
            await pg.exec("DROP VIEW IF EXISTS date_part_view");
            await core.execute(`CREATE VIEW date_part_view AS ${select}`);
            await pg.exec(`CREATE VIEW date_part_view AS ${select}`);
          }
          const authority = await pg.query<{ dt: string; et: string; d: unknown; e: unknown }>(
            `SELECT pg_typeof(d)::text AS dt,pg_typeof(e)::text AS et,d,e FROM(${sql}) q`,
          );
          expect(authority.rows[0]).toMatchObject({ dt: "double precision", et: "numeric" });
          const lowered = core.lowerer.lower(sql, core.context());
          if (lowered.kind !== "query") throw new Error("Expected query");
          expect(
            lowered.columns.map((col) => col.sqlType),
            sql,
          ).toEqual([authority.rows[0]?.dt, authority.rows[0]?.et]);
          expect(lowered.plan.columns.map((col) => col.type.kind)).toEqual(["float64", "decimal"]);
          if (s.kind === 0) {
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected rows");
            expect(oracleRows(result), sql).toEqual(
              authority.rows.map((row) => ({ d: row.d, e: row.e })),
            );
            reach.reach("exact interval value");
            reach.reach(field);
            if (s.months === 12 && s.days === -365 && s.micros === 1 && field === "epoch")
              reach.reach("float cancellation");
            const round = `SELECT round(${e},6) AS v`;
            const rounded = await core.execute(round);
            if (!("batches" in rounded)) throw new Error("Expected rounded EXTRACT");
            expect(oracleRows(rounded)).toEqual((await pg.query(round)).rows);
            reach.reach("numeric ROUND executes");
          } else {
            await expect(core.execute(sql)).rejects.toMatchObject({
              sqlState: "0A000",
              stage: "execute",
              construct: "function",
            });
            reach.reach("date/time typed refusal");
          }
          const wrong = `SELECT round(${d},6) AS v`;
          await expect(pg.query(wrong)).rejects.toMatchObject({ code: "42883" });
          expect(() => core.lowerer.lower(wrong, core.context())).toThrow(
            expect.objectContaining({ sqlState: "42883" }),
          );
          reach.reach("float ROUND refused");
          reach.reach(type);
          reach.reach(
            ["direct", "derived", "CTE", "view", "aggregate identity"][s.scope] ?? "direct",
          );
          if (s.nil) reach.reach("NULL");
          reach.reach("PG result types");
        },
      ),
      { seed, numRuns: 65 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);

it.each([
  ["SELECT date_part(1,INTERVAL '1 day')", "42883"],
  ["SELECT date_part('epoch',1)", "42883"],
  ["SELECT date_part()", "42883"],
])("keeps PG's DATE_PART overload refusal: %s", async (sql, sqlState) => {
  await expect(pg.query(sql)).rejects.toMatchObject({ code: sqlState });
  expect(() => core.lowerer.lower(sql, core.context())).toThrow(
    expect.objectContaining({ sqlState }),
  );
});

it("keeps DATE_PART's PostgreSQL output label", async () => {
  const sql = "SELECT date_part('epoch',INTERVAL '1 microsecond')";
  const result = await core.execute(sql);
  if (!("batches" in result)) throw new Error("Expected rows");
  expect(oracleRows(result)).toEqual((await pg.query(sql)).rows);
});
