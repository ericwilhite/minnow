/** Timestamp literals must reach actual execution with all six PostgreSQL fraction digits. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import {
  createReach,
  TIMESTAMP_LITERAL_REACH,
  TIMESTAMP_PRECISION_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { daysFromCivil, formatDate } from "../temporal.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import {
  compileQuery,
  compileStatement,
  compileWithDeclaredTypes,
  clonePlanTree,
} from "../../engine/query.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("SET TIME ZONE 'UTC'; SET IntervalStyle='postgres'");
  core = await new StorageCore(32).open();
}, 60_000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
interface Shape {
  day: number;
  second: number;
  millis: number;
  tail: number;
  digits: number;
  gap: number;
  wrapper: number;
  zone: number;
  separator: boolean;
}
const pad = (n: number, size = 2) => String(n).padStart(size, "0");
/** Construct source spelling from integer fields, independently of the timestamp parser/output. */
function spelling(day: number, second: number, micros: number, digits: number, separator = false) {
  while (micros < 0) {
    micros += 1_000_000;
    second--;
  }
  while (micros >= 1_000_000) {
    micros -= 1_000_000;
    second++;
  }
  while (second < 0) {
    second += 86400;
    day--;
  }
  while (second >= 86400) {
    second -= 86400;
    day++;
  }
  return `${formatDate(day)}${separator ? "T" : " "}${pad(Math.floor(second / 3600))}:${pad(Math.floor(second / 60) % 60)}:${pad(second % 60)}${digits === 0 ? "" : `.${pad(micros, 6).slice(0, digits)}`}`;
}
async function check(s: Shape, reach: ReturnType<typeof createReach<string>>) {
  reach.beginHistory();
  const unit = 10 ** (6 - s.digits);
  const tail = s.digits > 3 ? ((s.tail % (10 ** (s.digits - 3) - 1)) + 1) * unit : 0;
  const micros = Math.floor((s.millis * 1000 + tail) / unit) * unit;
  const zone = ["", "Z", "+00:00", "+05:30", "-08"][s.zone] ?? "";
  const a = spelling(s.day, s.second, micros, s.digits, s.separator) + zone;
  const b = spelling(s.day + s.gap, s.second, micros + 1, 6);
  const lo = spelling(s.day, s.second, micros - 1, 6);
  const inner = `SELECT TIMESTAMP '${a}' AS a, TIMESTAMP '${b}' AS b, TIMESTAMP '${lo}' AS lo`;
  const projection =
    "a::text AS said, b::text AS next, (a-b)::text AS difference, a<b AS lt, a=b AS eq, a>b AS gt, a>lo AS above_microsecond";
  let sql: string;
  let authority: string | undefined;
  if (s.wrapper === 2) sql = `WITH c AS (${inner}) SELECT ${projection} FROM c`;
  else if (s.wrapper === 3) {
    await pg.exec(`CREATE VIEW literal_probe AS ${inner}`);
    await core.execute(`CREATE VIEW literal_probe AS ${inner}`);
    sql = `SELECT ${projection} FROM literal_probe`;
  } else if (s.wrapper === 1) sql = `SELECT ${projection} FROM (${inner}) d`;
  else if (s.wrapper === 5)
    sql = `SELECT ${projection} FROM (SELECT MAX(TIMESTAMP '${a}') AS a, MAX(TIMESTAMP '${b}') AS b, MAX(TIMESTAMP '${lo}') AS lo FROM (VALUES (1),(2)) v(k)) d`;
  else
    sql = `SELECT TIMESTAMP '${a}'::text AS said, TIMESTAMP '${b}'::text AS next, (TIMESTAMP '${a}'-TIMESTAMP '${b}')::text AS difference, TIMESTAMP '${a}'<TIMESTAMP '${b}' AS lt, TIMESTAMP '${a}'=TIMESTAMP '${b}' AS eq, TIMESTAMP '${a}'>TIMESTAMP '${b}' AS gt, TIMESTAMP '${a}'>TIMESTAMP '${lo}' AS above_microsecond`;
  if (s.wrapper === 6) {
    const ddl = core.lowerer.lower(
      `CREATE TABLE literal_defaults (a TIMESTAMP DEFAULT TIMESTAMP '${a}', b TIMESTAMP DEFAULT TIMESTAMP '${b}', lo TIMESTAMP DEFAULT TIMESTAMP '${lo}')`,
      core.context(),
    );
    if (ddl.kind !== "ddl" || ddl.change?.kind !== "createTable")
      throw new Error("Expected CREATE TABLE defaults");
    const columns = ddl.change.table.columns;
    const [aSql, bSql, loSql] = columns.map((column) => column.defaultSql);
    if (typeof aSql !== "string" || typeof bSql !== "string" || typeof loSql !== "string")
      throw new Error("Missing timestamp defaults");
    // Candidate defaults run in their column context; PG reads the original source, independently.
    sql = `SELECT ${projection} FROM (SELECT (${aSql})::timestamp AS a, (${bSql})::timestamp AS b, (${loSql})::timestamp AS lo) d`;
    authority = `SELECT ${projection} FROM (${inner}) d`;
  }
  if (s.wrapper === 4)
    sql += ` FROM (VALUES (1),(2)) v(k) GROUP BY TIMESTAMP '${a}', TIMESTAMP '${b}', TIMESTAMP '${lo}'`;
  try {
    const expected = (await pg.query(authority ?? sql)).rows;
    const actual = await core.execute(sql);
    if (!("batches" in actual)) throw new Error("Expected timestamp rows");
    expect(oracleRows(actual), sql).toEqual(expected);
    reach.reach(`fraction ${String(s.digits)}`);
    reach.reach(
      [
        "direct",
        "derived",
        "CTE",
        "view",
        "grouped literals",
        "aggregated literals",
        "default SQL",
      ][s.wrapper] ?? "missing",
    );
    reach.reach(s.day < 0 ? "before PG epoch" : "at or after PG epoch");
    if (s.digits > 3) reach.reach("sub-millisecond");
    if (s.digits > 3 && micros % 10 === 0) reach.reach("trailing fraction zeros");
    if (s.gap < 0) reach.reach("positive day difference");
    if (s.gap > 0) reach.reach("negative day difference");
    if (s.gap === 0) reach.reach("microsecond comparison");
    if (s.day === 59) reach.reach("leap day");
    if (s.second === 86399 && micros === 999999) reach.reach("day carry");
    if (s.day >= daysFromCivil(3000, 1, 1)) reach.reach("wide timestamp");
    reach.reach(s.zone > 2 ? "ignored nonzero literal zone" : "UTC or absent literal zone");
  } finally {
    if (s.wrapper === 3) {
      await core.execute("DROP VIEW literal_probe");
      await pg.exec("DROP VIEW literal_probe");
    }
  }
}
it.each(seedsFor("next-timestamp-literal-microseconds", [0xb627, 0xb628]))(
  "preserves timestamp literal microseconds through SQL lowering and execution (seed %i)",
  async (seed) => {
    const reach = createReach<string>("timestamp SQL literal precision", TIMESTAMP_LITERAL_REACH);
    const base: Shape = {
      day: 59,
      second: 45296,
      millis: 123,
      tail: 399,
      digits: 6,
      gap: 0,
      wrapper: 0,
      zone: 0,
      separator: false,
    };
    const examples: Array<[Shape]> = [];
    for (let digits = 0; digits <= 6; digits++)
      for (let wrapper = 0; wrapper < 7; wrapper++) examples.push([{ ...base, digits, wrapper }]);
    for (const day of [
      daysFromCivil(1, 1, 2),
      -1,
      0,
      daysFromCivil(1900, 3, 1),
      daysFromCivil(3000, 1, 1),
      daysFromCivil(9999, 12, 30),
    ])
      examples.push([{ ...base, day }]);
    for (const gap of [-2, -1, 0, 1, 2])
      examples.push([{ ...base, day: 58, second: 86399, millis: 999, tail: 998, gap }]);
    for (let zone = 0; zone < 5; zone++) examples.push([{ ...base, zone }]);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          day: fc.integer({ min: daysFromCivil(1, 1, 3), max: daysFromCivil(9999, 12, 28) }),
          second: fc.integer({ min: 0, max: 86399 }),
          millis: fc.integer({ min: 0, max: 999 }),
          tail: fc.integer({ min: 0, max: 998 }),
          digits: fc.integer({ min: 0, max: 6 }),
          gap: fc.integer({ min: -2, max: 2 }),
          wrapper: fc.integer({ min: 0, max: 6 }),
          zone: fc.integer({ min: 0, max: 4 }),
          separator: fc.boolean(),
        }),
        (s) => check(s, reach),
      ),
      { seed, numRuns: examples.length + 80, examples },
    );
    reach.assertReached();
  },
  120_000,
);

it("keeps short timestamp forms, BC fractions and Date parameters, and refuses native overflow", async () => {
  for (const text of [
    "2000-02-29",
    "2000-02-29 12:34",
    "2000-02-29T12:34:56.123400",
    "0044-03-15 12:34:56.000001 BC",
    "4714-11-24 00:00:00.000001 BC",
    "2000-02-29 12:34:56.123400 +05:30",
  ]) {
    const sql = `SELECT TIMESTAMP '${text}'::text AS said`;
    const result = await core.execute(sql);
    if (!("batches" in result)) throw new Error("Expected timestamp rows");
    expect(oracleRows(result), sql).toEqual((await pg.query(sql)).rows);
  }
  const sql = "SELECT $1::timestamp::text AS said";
  const date = new Date("2000-02-29T12:34:56.123Z");
  const result = await core.execute(sql, [date]);
  if (!("batches" in result)) throw new Error("Expected parameter rows");
  expect(oracleRows(result)).toEqual((await pg.query(sql, [date])).rows);
  const invalid = "SELECT TIMESTAMP '4714-11-23 23:59:59.999999 BC'::text AS said";
  await expect(pg.query(invalid)).rejects.toMatchObject({ code: "22008" });
  await expect(core.execute(invalid)).rejects.toMatchObject({ sqlState: "22008" });
});

it("confines source provenance to typed compilation and its plan clones", async () => {
  const { timestampLiteralSources } = await import("../../date-value.js");
  const sql = "SELECT TIMESTAMP '2000-02-29 12:34:56.123400' AS a";
  expect(() =>
    compileQuery("SELECT CAST(TIMESTAMP '2000-02-29 12:34:56.123400' AS TIMESTAMP(3))"),
  ).not.toThrow();
  expect(() => compileStatement("CREATE TABLE p (v TIMESTAMP(3))")).not.toThrow();
  for (const optimize of [false, true]) {
    const ordinary = compileQuery(sql, { optimize }).select[0]?.expression;
    const typed = compileWithDeclaredTypes(() => clonePlanTree(compileQuery(sql, { optimize })))
      .select[0]?.expression;
    if (
      ordinary?.kind !== "literal" ||
      !(ordinary.value instanceof Date) ||
      typed?.kind !== "literal" ||
      !(typed.value instanceof Date)
    )
      throw new Error("Expected timestamp literals in the compiled model");
    expect(timestampLiteralSources.has(ordinary.value)).toBe(false);
    expect(timestampLiteralSources.get(typed.value)).toBe("2000-02-29 12:34:56.123400");
  }
});

it.each(seedsFor("next-timestamp-precision-modifiers", [0xb629, 0xb62a]))(
  "admits full precision and refuses unimplemented timestamp rounding (seed %i)",
  async (seed) => {
    const targets = [
      "TIMESTAMP",
      "TIMESTAMP WITHOUT TIME ZONE",
      "TIMESTAMPTZ",
      "TIMESTAMP WITH TIME ZONE",
    ];
    const reach = createReach<string>("timestamp precision modifiers", TIMESTAMP_PRECISION_REACH);
    const examples: Array<[number, number, number, number]> = [];
    for (let precision = 0; precision <= 7; precision++)
      for (let target = 0; target < 4; target++)
        for (let context = 0; context < 2; context++)
          examples.push([precision, target, context, 123400]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 7 }),
        fc.integer({ min: 0, max: 3 }),
        fc.integer({ min: 0, max: 1 }),
        fc.integer({ min: 1, max: 999999 }),
        async (precision, target, context, micros) => {
          reach.beginHistory();
          const name = (targets[target] ?? "TIMESTAMP").replace(
            /^(TIMESTAMPTZ|TIMESTAMP)\b/u,
            `$1(${String(precision)})`,
          );
          const sql = `SELECT CAST(TIMESTAMP '2000-02-29 12:34:56.${pad(micros, 6)}' AS ${name})::text AS said`;
          const expected = (await pg.query(sql)).rows;
          const statement = context === 0 ? sql : `CREATE TABLE precision_probe (v ${name})`;
          if (precision !== 6) {
            const refusal = {
              name: "UnsupportedPlanError",
              sqlState: "0A000",
              stage: "lower",
              construct: "columnType",
            };
            if (context === 0) await expect(core.execute(statement)).rejects.toMatchObject(refusal);
            else
              expect(() => core.lowerer.lower(statement, core.context())).toThrow(
                expect.objectContaining(refusal),
              );
            reach.reach("rounding refused");
          } else if (context === 0) {
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected timestamp rows");
            expect(oracleRows(result)).toEqual(expected);
            reach.reach("full precision executes");
          } else {
            const result = core.lowerer.lower(statement, core.context());
            expect(result.kind).toBe("ddl");
            reach.reach("full precision column");
          }
          reach.reach(`precision ${String(precision)}`);
          reach.reach(`target ${String(target)}`);
          reach.reach(context === 0 ? "cast" : "column");
        },
      ),
      { seed, numRuns: examples.length + 80, examples },
    );
    reach.assertReached();
  },
  120_000,
);
