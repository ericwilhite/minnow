import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PGlite } from "@electric-sql/pglite";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";
import { applyWindowFunctions, applyWindowFunctionsAsync } from "./windows.js";
import { QueryMemoryBudgetError, QueryMemoryContext } from "./memory.js";
import type { QueryResult, WindowSpec } from "../plan/model.js";

const database = new MinnowDatabase(new MemoryBlockStore());
const postgres = new PGlite();
beforeAll(async () => {
  await postgres.waitReady;
});
afterAll(async () => {
  await database.close();
  await postgres.close();
});

describe("SQL audit PostgreSQL regressions", () => {
  it.each([
    "SELECT CAST(2.5 AS INTEGER) AS v, CAST(-2.5 AS INTEGER) AS w",
    "SELECT CAST(CAST(2.5 AS DOUBLE PRECISION) AS INTEGER) AS v",
    "SELECT CAST('  +12 ' AS INTEGER) AS v",
    "SELECT EXTRACT(SECOND FROM TIMESTAMP '2026-01-01 12:34:56.789')::DOUBLE PRECISION AS v",
    "SELECT EXTRACT(HOUR FROM TIME '12:34:56.123456')::DOUBLE PRECISION AS v",
    "SELECT EXTRACT(SECOND FROM TIME '12:34:56.123456')::DOUBLE PRECISION AS v",
    "SELECT EXTRACT(EPOCH FROM TIME '12:34:56.123456')::DOUBLE PRECISION AS v",
    "SELECT EXTRACT(DAY FROM INTERVAL '2 days 25 hours')::DOUBLE PRECISION AS v",
    "SELECT EXTRACT(HOUR FROM INTERVAL '2 days 25 hours')::DOUBLE PRECISION AS v",
    "SELECT EXTRACT(MONTH FROM INTERVAL '-2 years -3 months')::DOUBLE PRECISION AS v",
    "SELECT EXTRACT(SECOND FROM INTERVAL '-61.25 seconds')::DOUBLE PRECISION AS v",
    "SELECT EXTRACT(EPOCH FROM INTERVAL '1 year 2 months 3 days 4 seconds')::DOUBLE PRECISION AS v",
    "SELECT NULL AS v UNION SELECT 1 AS v ORDER BY v",
    "SELECT 'x' AS v UNION SELECT NULL AS v ORDER BY v",
    "SELECT COALESCE(v, 'x') AS v FROM (SELECT NULL AS v) AS q",
    "SELECT v FROM (SELECT NULL AS v UNION ALL SELECT NULL AS v) AS q",
    "SELECT ARRAY[2] < ARRAY[10] AS v, ARRAY[NULL, 1] > ARRAY[2, 1] AS w",
    "SELECT '{\"a\":2}'::JSONB < '{\"a\":10}'::JSONB AS v",
    "SELECT '[9]'::JSONB < '[1,2]'::JSONB AS v",
    'SELECT \'{"b":1,"aa":0}\'::JSONB > \'{"a":9,"bb":0}\'::JSONB AS v',
    "SELECT '[]'::JSONB < 'null'::JSONB AS v, 'true'::JSONB > '100'::JSONB AS w",
  ])("matches PostgreSQL: %s", async (sql) => {
    expect((await database.query(sql)).rows).toEqual((await postgres.query(sql)).rows);
  });

  it.each(["1.5", "1e2", "", "2 3"])("rejects invalid integer text %j", async (text) => {
    const sql = `SELECT CAST('${text}' AS INTEGER) AS v`;
    await expect(database.query(sql)).rejects.toThrow();
    await expect(postgres.query(sql)).rejects.toThrow();
  });

  it("always gives DATE interval arithmetic timestamp metadata", async () => {
    for (const interval of ["1 day", "1 month", "1 second", "0 days"]) {
      const sql = `SELECT DATE '2026-01-01' + INTERVAL '${interval}' AS v`;
      const actual = await database.query(sql);
      expect(actual.rows.map(({ v }) => (v as Date).getTime())).toEqual(
        (await postgres.query<{ v: Date }>(sql)).rows.map(
          ({ v }) => v.getTime() - v.getTimezoneOffset() * 60000,
        ),
      );
      expect(actual.columnDomains).toEqual([null]);
      expect(actual.rows[0]?.v).toBeInstanceOf(Date);
    }
  });

  it("only consumes demanded sequence values", async () => {
    await database.execute("CREATE SEQUENCE audit_s");
    await postgres.exec("CREATE SEQUENCE audit_s");
    for (const sql of [
      "SELECT CASE WHEN FALSE THEN NEXTVAL('audit_s') ELSE 0 END AS v",
      "SELECT COALESCE(7, NEXTVAL('audit_s')) AS v",
      "SELECT NEXTVAL('audit_s') AS v LIMIT 0",
      "SELECT NEXTVAL('audit_s') AS v OFFSET 1",
      "SELECT NEXTVAL('audit_s') AS v WHERE FALSE",
      "SELECT NEXTVAL('audit_s') AS v",
      "SELECT CASE WHEN TRUE THEN NEXTVAL('audit_s') ELSE NEXTVAL('audit_s') END AS v",
      "SELECT COALESCE(NULL, NEXTVAL('audit_s'), NEXTVAL('audit_s')) AS v",
      "SELECT CURRVAL('audit_s') AS v",
    ]) {
      const expected = (await postgres.query<{ v: number | string }>(sql)).rows.map(({ v }) => ({
        v: Number(v),
      }));
      expect((await database.query(sql)).rows, sql).toEqual(expected);
    }
  });
});

describe("window audit regressions", () => {
  it("does not subtract rounded prefixes for single-member or excluded frames", async () => {
    await database.execute("CREATE TABLE audit_float (id INTEGER PRIMARY KEY, x DOUBLE PRECISION)");
    await database.execute("INSERT INTO audit_float VALUES (1, 1e16), (2, 1), (3, 2)");
    for (const aggregate of ["SUM", "AVG", "MIN", "MAX"]) {
      const sql = `SELECT id, ${aggregate}(x) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS v FROM audit_float ORDER BY id`;
      expect((await database.query(sql)).rows).toEqual([
        { id: 1, v: 1e16 },
        { id: 2, v: 1 },
        { id: 3, v: 2 },
      ]);
    }
    expect(
      (
        await database.query(
          "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE GROUP) AS v FROM audit_float ORDER BY id",
        )
      ).rows,
    ).toEqual([{ v: null }, { v: 1e16 }, { v: 1 }]);
  });

  it("matches PostgreSQL for aggregates, peers, offsets and exclusions", async () => {
    const setup =
      "CREATE TABLE audit_window (id INTEGER PRIMARY KEY, p INTEGER, k INTEGER, x INTEGER); INSERT INTO audit_window VALUES " +
      Array.from(
        { length: 36 },
        (_, id) =>
          `(${String(id)},${String(id % 3)},${String(Math.floor(id / 4))},${String(id % 5 === 0 ? "NULL" : id - 18)})`,
      ).join(",");
    for (const statement of setup.split(";")) await database.execute(statement);
    await postgres.exec(setup);
    for (const unit of ["ROWS", "GROUPS"])
      for (const exclude of ["NO OTHERS", "CURRENT ROW", "GROUP", "TIES"]) {
        const over = `(PARTITION BY p ORDER BY k${unit === "ROWS" ? ", id" : ""} ${unit} BETWEEN 2 PRECEDING AND 1 FOLLOWING EXCLUDE ${exclude})`;
        const columns = [
          "SUM(x)",
          "AVG(x)::DOUBLE PRECISION",
          "MIN(x)",
          "MAX(x)",
          "COUNT(x)",
          "FIRST_VALUE(x)",
          "LAST_VALUE(x)",
          "NTH_VALUE(x, 2)",
        ];
        const select = (unit === "GROUPS" ? columns.slice(0, 5) : columns)
          .map((call, index) =>
            call.includes("::")
              ? `AVG(x) OVER ${over}::DOUBLE PRECISION AS c${String(index)}`
              : `${call} OVER ${over} AS c${String(index)}`,
          )
          .join(",");
        const sql = `SELECT id, ${select} FROM audit_window ORDER BY id`;
        const expected = (await postgres.query<Record<string, unknown>>(sql)).rows.map((row) =>
          Object.fromEntries(
            Object.entries(row).map(([key, value]) => [
              key,
              typeof value === "string" ? Number(value) : value,
            ]),
          ),
        );
        expect((await database.query(sql)).rows, sql).toEqual(expected);
      }
  });

  it("accounts window buffers and yields so cancellation releases working state", async () => {
    const result: QueryResult = {
      columns: ["x"],
      columnDomains: [null],
      rows: Array.from({ length: 50000 }, (_, x) => ({ x })),
    };
    const window: WindowSpec = {
      name: "MIN",
      alias: "v",
      argumentAlias: "x",
      partitionAliases: [],
      orderAliases: [],
    };
    const small = new QueryMemoryContext(1024);
    expect(() => applyWindowFunctions(result, [window], { memoryContext: small })).toThrow(
      QueryMemoryBudgetError,
    );
    small.close();
    expect(small.usage.usedBytes).toBe(0);
    const memory = new QueryMemoryContext();
    const controller = new AbortController();
    const pending = applyWindowFunctionsAsync(result, [window], {
      memoryContext: memory,
      signal: controller.signal,
    });
    setTimeout(() => controller.abort(), 0);
    await expect(pending).rejects.toThrow();
    memory.close();
    expect(memory.usage.usedBytes).toBe(0);
  });
});

describe("SQL audit composition enhancements", () => {
  it("composes FULL JOIN with grouping, DISTINCT, wildcards and general predicates", async () => {
    const setup =
      "CREATE TABLE audit_left (id INTEGER, x INTEGER); CREATE TABLE audit_right (id INTEGER, y INTEGER); INSERT INTO audit_left VALUES (1,10),(2,20),(NULL,30); INSERT INTO audit_right VALUES (1,11),(3,31),(NULL,41)";
    for (const sql of setup.split(";")) await database.execute(sql);
    await postgres.exec(setup);
    for (const sql of [
      "SELECT COUNT(*)::INTEGER AS n, SUM(x)::INTEGER AS x, SUM(y)::INTEGER AS y FROM audit_left l FULL JOIN audit_right r ON l.id = r.id",
      "SELECT l.id AS id, COUNT(*)::INTEGER AS n FROM audit_left l FULL JOIN audit_right r ON l.id = r.id GROUP BY l.id ORDER BY l.id",
      "SELECT DISTINCT l.id AS id FROM audit_left l FULL JOIN audit_right r ON l.id = r.id ORDER BY id",
      "SELECT l.*, r.y FROM audit_left l FULL JOIN audit_right r ON l.id = r.id ORDER BY x NULLS LAST, y",
      "SELECT x, y FROM audit_left l FULL JOIN audit_right r ON l.id = r.id AND l.x < r.y ORDER BY x NULLS LAST, y",
      "SELECT x, y FROM audit_left l FULL JOIN audit_right r ON l.id = r.id AND (l.x > 99 OR r.y > 5) ORDER BY x NULLS LAST, y",
    ])
      expect((await database.query(sql)).rows, sql).toEqual((await postgres.query(sql)).rows);
  });

  it("orders by windows and evaluates numeric offset RANGE frames", async () => {
    for (const direction of ["ASC", "DESC"])
      for (const nulls of ["FIRST", "LAST"]) {
        const sql = `SELECT id, SUM(x) OVER (ORDER BY x::DOUBLE PRECISION ${direction} NULLS ${nulls} RANGE BETWEEN 2.5 PRECEDING AND 4 FOLLOWING)::DOUBLE PRECISION AS v FROM audit_window ORDER BY id`;
        expect((await database.query(sql)).rows, sql).toEqual((await postgres.query(sql)).rows);
      }
    const sql = "SELECT id FROM audit_window ORDER BY ROW_NUMBER() OVER (ORDER BY id DESC) LIMIT 3";
    expect((await database.query(sql)).rows).toEqual((await postgres.query(sql)).rows);
  });

  it.each(["y", "ye", "yes", "n", "no", "on", "of", "off", "tru", "fals"])(
    "accepts boolean input %s",
    async (text) => {
      const sql = `SELECT '${text}'::BOOLEAN AS v`;
      expect((await database.query(sql)).rows).toEqual((await postgres.query(sql)).rows);
    },
  );
  it("accepts an empty LIKE ESCAPE", async () => {
    const sql = "SELECT 'a\\b' LIKE 'a\\b' ESCAPE '' AS v";
    expect((await database.query(sql)).rows).toEqual((await postgres.query(sql)).rows);
  });
});

describe("array composition", () => {
  it.each([
    "SELECT x FROM unnest(ARRAY[3,1,2]) AS x",
    "SELECT (ARRAY[2,10,NULL])[2] AS v",
    "SELECT (ARRAY[2,10,NULL])[0] AS v",
    "SELECT (ARRAY['a','b'])[2] AS v",
    "SELECT v FROM UNNEST(ARRAY[2,10,NULL]) AS q(v) ORDER BY v",
    "SELECT v, n::INTEGER AS n FROM UNNEST(ARRAY['a','b']) WITH ORDINALITY AS q(v,n) ORDER BY n",
    "SELECT v FROM UNNEST(ARRAY[1::NUMERIC,2::NUMERIC]) AS q(v) ORDER BY v",
  ])("matches PostgreSQL: %s", async (sql) => {
    expect((await database.query(sql)).rows).toEqual((await postgres.query(sql)).rows);
  });
  it("preserves exact NUMERIC array members beyond Float64 precision", async () => {
    const sql =
      "SELECT ARRAY_AGG(v ORDER BY v) AS v FROM (SELECT 9007199254740993.25::NUMERIC AS v UNION ALL SELECT 9007199254740994.5::NUMERIC AS v) q";
    const actual = (await database.query(sql)).rows[0]?.v;
    expect(JSON.parse(String(actual))).toEqual(["9007199254740993.25", "9007199254740994.5"]);
  });
  it("aggregates arrays, including ordered DISTINCT members and empty input", async () => {
    for (const sql of [
      "SELECT ARRAY_AGG(x ORDER BY id) AS v FROM audit_window",
      "SELECT ARRAY_AGG(DISTINCT x ORDER BY x) AS v FROM audit_window",
      "SELECT ARRAY_AGG(x) AS v FROM audit_window WHERE FALSE",
    ]) {
      const actual = (await database.query(sql)).rows[0]?.v;
      const expected = (await postgres.query<{ v: unknown }>(sql)).rows[0]?.v;
      expect(actual === null ? null : JSON.parse(String(actual)), sql).toEqual(expected);
    }
  });
});

it("does not merge Date and text window arguments or fallback cache identities", async () => {
  const result = await database.query(
    "SELECT LAG(TIMESTAMP '2026-01-01', 0) OVER (ORDER BY id) AS d, LAG('2026-01-01T00:00:00.000Z', 0) OVER (ORDER BY id) AS s FROM audit_window LIMIT 1",
  );
  expect(result.rows).toEqual([
    { d: new Date("2026-01-01T00:00:00.000Z"), s: "2026-01-01T00:00:00.000Z" },
  ]);
});
