import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase, type DatabaseRow } from "./database.js";
import {
  bindPlanParameters,
  compileQuery,
  createPreparedQuery,
  executeQuery,
  executeRowQuery,
} from "./query.js";

const tables = new Map<string, DatabaseRow[]>([
  [
    "rows",
    [
      { id: 1, region: "west", amount: 10 },
      { id: 2, region: "west", amount: 6 },
      { id: 3, region: "east", amount: 3 },
      { id: 4, region: null, amount: 8 },
    ],
  ],
  ["dims", [{ w: 5 }, { w: 7 }]],
  ["empty", []],
]);

function run(sql: string, params?: unknown[]): DatabaseRow[] {
  let plan = compileQuery(sql);
  if (params !== undefined) plan = bindPlanParameters(plan, params as never);
  const vectorized = executeQuery(plan, tables);
  const byRow = executeRowQuery(plan, tables);
  expect(vectorized.rows).toEqual(byRow.rows);
  return vectorized.rows;
}

describe("PostgreSQL FETCH and OFFSET spellings", () => {
  it("treats FETCH FIRST as LIMIT in both orders", () => {
    expect(run("SELECT id FROM rows ORDER BY id OFFSET 1 ROWS FETCH FIRST 2 ROWS ONLY")).toEqual([
      { id: 2 },
      { id: 3 },
    ]);
    expect(run("SELECT id FROM rows ORDER BY id FETCH NEXT ROW ONLY")).toEqual([{ id: 1 }]);
    expect(
      run("SELECT id FROM rows ORDER BY id OFFSET ? ROWS FETCH FIRST ? ROWS ONLY", [2, 1]),
    ).toEqual([{ id: 3 }]);
  });
});

describe("BETWEEN SYMMETRIC", () => {
  it("accepts reversed bounds", () => {
    expect(run("SELECT id FROM rows WHERE amount BETWEEN SYMMETRIC 8 AND 3 ORDER BY id")).toEqual([
      { id: 2 },
      { id: 3 },
      { id: 4 },
    ]);
    expect(
      run("SELECT id FROM rows WHERE amount NOT BETWEEN SYMMETRIC 8 AND 3 ORDER BY id"),
    ).toEqual([{ id: 1 }]);
  });
});

describe("PostgreSQL value domains and predicates", () => {
  it("keeps NUMERIC arithmetic exact across both executors", () => {
    expect(run("SELECT CAST(0.1 AS DECIMAL(30, 10)) + CAST(0.2 AS NUMERIC) AS n")).toEqual([
      { n: "0.3" },
    ]);
    expect(
      run(
        "SELECT CAST('9007199254740993' AS NUMERIC) > CAST('9007199254740992' AS NUMERIC) AS larger",
      ),
    ).toEqual([{ larger: true }]);
    expect(run("SELECT CAST(2.25 AS NUMERIC) + 0.5 BETWEEN 2.75 AND 3 AS inside")).toEqual([
      { inside: true },
    ]);

    const prepared = createPreparedQuery(
      compileQuery("SELECT CAST(0.10 AS NUMERIC) AS n FROM rows LIMIT 1"),
      tables,
    );
    expect(prepared.execute().rows).toEqual([{ n: "0.1" }]);
    prepared.close();
  });

  it("constructs JSON, UUID, ARRAY, TIME, and INTERVAL domain values", () => {
    expect(
      run(
        'SELECT CAST(\'{"b":2,"a":1}\' AS JSONB) AS j, ' +
          "CAST('550E8400-E29B-41D4-A716-446655440000' AS UUID) AS u, " +
          "ARRAY[1, 2, NULL] AS a, TIME '12:34:56.25' AS t, " +
          "CAST('1 month 2 days' AS INTERVAL) AS i",
      ),
    ).toEqual([
      {
        j: '{"a":1,"b":2}',
        u: "550e8400-e29b-41d4-a716-446655440000",
        a: "[1,2,null]",
        t: "12:34:56.25",
        i: "1 mons 2 days 0 usecs",
      },
    ]);
  });

  it("projects constant JSON arrays through JSON_TABLE", () => {
    expect(
      run(
        "SELECT j.a, j.label FROM JSON_TABLE(" +
          '\'[{"a":2,"label":"two"},{"a":1,"label":"one"}]\', \'$[*]\' ' +
          "COLUMNS (a INTEGER PATH '$.a', label TEXT PATH '$.label')) AS j ORDER BY j.a",
      ),
    ).toEqual([
      { a: 1, label: "one" },
      { a: 2, label: "two" },
    ]);
  });

  it("retains JSON_TABLE's declared schema for an empty array", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await expect(
      database.query(
        "SELECT j.a, j.label FROM JSON_TABLE('[]', '$[*]' " +
          "COLUMNS (a INTEGER PATH '$.a', label TEXT PATH '$.label')) AS j",
      ),
    ).resolves.toEqual({ columns: ["a", "label"], columnDomains: [null, null], rows: [] });
    await database.close();
  });

  it("evaluates SIMILAR TO and explicit collations", () => {
    expect(run("SELECT id FROM rows WHERE region SIMILAR TO '(west|north)' ORDER BY id")).toEqual([
      { id: 1 },
      { id: 2 },
    ]);
    expect(run("SELECT id FROM rows WHERE region NOT SIMILAR TO 'w%' ORDER BY id")).toEqual([
      { id: 3 },
    ]);
    expect(
      run(
        'SELECT region COLLATE "C" AS region FROM rows WHERE region IS NOT NULL ORDER BY region COLLATE "C"',
      ),
    ).toEqual([{ region: "east" }, { region: "west" }, { region: "west" }]);
    expect(run("SELECT id FROM rows WHERE region COLLATE \"C\" = 'west' ORDER BY id")).toEqual([
      { id: 1 },
      { id: 2 },
    ]);
    expect(run("SELECT id FROM rows WHERE region COLLATE \"C\" LIKE 'w%' ORDER BY id")).toEqual([
      { id: 1 },
      { id: 2 },
    ]);
    expect(
      run("SELECT id FROM rows WHERE region COLLATE \"C\" SIMILAR TO '(east|west)' ORDER BY id"),
    ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    expect(() => run("SELECT CAST('1 day trailing' AS INTERVAL) AS span")).toThrow(
      "Invalid INTERVAL",
    );
    expect(run("SELECT ARRAY[CAST(0.1 AS NUMERIC), CAST(0.2 AS NUMERIC)] AS values")).toEqual([
      { values: '["0.1","0.2"]' },
    ]);
  });
});

describe("quantified comparisons", () => {
  it("implements ANY/SOME/ALL with three-valued logic", () => {
    expect(run("SELECT id FROM rows WHERE amount > ANY (SELECT w FROM dims) ORDER BY id")).toEqual([
      { id: 1 },
      { id: 2 },
      { id: 4 },
    ]);
    expect(run("SELECT id FROM rows WHERE amount > ALL (SELECT w FROM dims) ORDER BY id")).toEqual([
      { id: 1 },
      { id: 4 },
    ]);
    expect(run("SELECT id FROM rows WHERE amount = SOME (SELECT w FROM dims) ORDER BY id")).toEqual(
      [],
    );
    // Empty subquery: ALL is vacuously true, ANY vacuously false.
    expect(
      run("SELECT id FROM rows WHERE amount > ALL (SELECT amount FROM empty) ORDER BY id"),
    ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
    expect(
      run("SELECT id FROM rows WHERE amount > ANY (SELECT amount FROM empty) ORDER BY id"),
    ).toEqual([]);
    // A NULL in the list makes ALL unknown unless already false.
    expect(
      run("SELECT id FROM rows WHERE amount > ALL (SELECT region FROM rows WHERE id = 4)"),
    ).toEqual([]);
  });

  it("rejects correlated quantified subqueries explicitly", () => {
    expect(() =>
      compileQuery(
        "SELECT r.id FROM rows r WHERE r.amount > ALL (SELECT q.amount FROM rows q WHERE q.region = r.region)",
      ),
    ).toThrow("top-level WHERE");
  });
});

describe("VALUES", () => {
  it("stands alone, joins, renames, and unions", () => {
    expect(run("VALUES (1, 'a'), (2, 'b')")).toEqual([
      { column1: 1, column2: "a" },
      { column1: 2, column2: "b" },
    ]);
    expect(run("SELECT v.n AS n FROM (VALUES (2), (1)) AS v(n) ORDER BY n")).toEqual([
      { n: 1 },
      { n: 2 },
    ]);
    expect(
      run(
        "SELECT r.id AS id, x.column2 AS tag FROM rows r JOIN (VALUES ('west', 'W')) x ON x.column1 = r.region ORDER BY id",
      ),
    ).toEqual([
      { id: 1, tag: "W" },
      { id: 2, tag: "W" },
    ]);
    expect(() => compileQuery("VALUES (1), (2, 3)")).toThrow("share one width");
    expect(() => compileQuery("SELECT v.n FROM (VALUES (1, 2)) v(n) ORDER BY 1")).toThrow(
      "must match the derived table's column count",
    );
  });
});

describe("GROUPING SETS family", () => {
  it("computes rollup subtotals and grand totals", () => {
    expect(
      run(
        "SELECT region, SUM(amount) AS total FROM rows WHERE region IS NOT NULL GROUP BY ROLLUP(region)",
      ),
    ).toEqual([
      { region: "west", total: 16 },
      { region: "east", total: 3 },
      { region: null, total: 19 },
    ]);
  });

  it("computes explicit grouping sets and cube", () => {
    const sets = run(
      "SELECT region, COUNT(*) AS c FROM rows GROUP BY GROUPING SETS ((region), ())",
    );
    expect(sets).toContainEqual({ region: null, c: 4 });
    expect(sets.length).toBe(4);
    const cube = run("SELECT region, COUNT(*) AS c FROM rows GROUP BY CUBE(region)");
    expect(cube.length).toBe(4);
  });

  it("rejects unsupported grouping-set combinations explicitly", () => {
    expect(() =>
      compileQuery(
        "SELECT region, COUNT(*) AS c FROM rows GROUP BY ROLLUP(region) HAVING region = 'west'",
      ),
    ).toThrow("aggregate and literal conditions only");
    expect(() =>
      compileQuery(
        "SELECT region, amount, COUNT(*) AS c FROM rows GROUP BY CUBE(region, amount, id, amount + 1, id + 1, id + 2)",
      ),
    ).toThrow("at most 5");
  });
});

describe("WITH on mutations", () => {
  it("exposes CTEs to INSERT ... SELECT and mutation subqueries", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "p",
      uniqueKey: "name",
      columns: [
        { name: "name", type: "string" },
        { name: "score", type: "number" },
      ],
    });
    await database.execute("INSERT INTO p (name, score) VALUES ('Ada', 1), ('Grace', 9)");
    const inserted = await database.execute(
      "WITH doubled AS (SELECT name || '2' AS name, score * 2 AS score FROM p) INSERT INTO p (name, score) SELECT name, score FROM doubled RETURNING *",
    );
    expect(inserted).toMatchObject({
      returnedRows: [
        { name: "Ada2", score: 2 },
        { name: "Grace2", score: 18 },
      ],
    });
    const deleted = await database.execute(
      "WITH cutoff AS (SELECT MAX(score) AS top FROM p) DELETE FROM p WHERE score >= (SELECT top FROM cutoff) RETURNING name",
    );
    expect(deleted).toMatchObject({ returnedRows: [{ name: "Grace2" }] });
  });
});

describe("LIKE ESCAPE", () => {
  it("treats the escaped character literally", () => {
    const percents = new Map<string, DatabaseRow[]>([
      ["t", [{ v: "100%" }, { v: "100x" }, { v: "a_b" }, { v: "axb" }]],
    ]);
    const plan = compileQuery("SELECT v FROM t WHERE v LIKE '100!%' ESCAPE '!'");
    expect(executeRowQuery(plan, percents).rows).toEqual([{ v: "100%" }]);
    const underscore = compileQuery("SELECT v FROM t WHERE v LIKE 'a!_b' ESCAPE '!'");
    expect(executeRowQuery(underscore, percents).rows).toEqual([{ v: "a_b" }]);
    expect(() =>
      executeRowQuery(compileQuery("SELECT v FROM t WHERE v LIKE 'x!' ESCAPE '!'"), percents),
    ).toThrow("dangling escape");
    expect(() => compileQuery("SELECT v FROM t WHERE v LIKE 'x' ESCAPE '!!'")).toThrow(
      "single character",
    );
  });
});
