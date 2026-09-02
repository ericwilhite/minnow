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

  it("types numeric constants exactly, the way PostgreSQL's NUMERIC typing does", () => {
    // Constant arithmetic folds in exact decimal space; a Float64-representable result comes
    // back as an ordinary number (PostgreSQL's own cast when the value meets a float context).
    expect(run("SELECT 0.1 + 0.2 AS q")).toEqual([{ q: 0.3 }]);
    expect(run("SELECT (0.1 + 0.2) * 3 AS q")).toEqual([{ q: 0.9 }]);
    expect(run("SELECT 0.1 + 0.2 - 0.3 AS q")).toEqual([{ q: 0 }]);
    expect(run("SELECT 0.1 + 0.2 = 0.3 AS q")).toEqual([{ q: true }]);
    // A value Float64 would round stays an exact-NUMERIC string, digits and quotient scale
    // chosen as PostgreSQL chooses them — the written scale of 1.000… floors the selection.
    expect(run("SELECT 1.000000000000000000000000 / 3 AS q")).toEqual([
      { q: "0.333333333333333333333333" },
    ]);
    expect(run("SELECT -1.000000000000000000000000 / 6 AS q")).toEqual([
      { q: "-0.166666666666666666666667" },
    ]);
    // Integer constants beyond 2^53 are exact instead of an error, like PostgreSQL's int8 and
    // NUMERIC typing; scientific notation is a numeric constant.
    expect(run("SELECT 9007199254740993 AS q")).toEqual([{ q: "9007199254740993" }]);
    expect(run("SELECT 1000000000000000000000000000000000000000 / 3 AS q")).toEqual([
      { q: "333333333333333333333333333333333333333" },
    ]);
    expect(run("SELECT 1e2 AS q")).toEqual([{ q: 100 }]);
    expect(run("SELECT 2.5e-1 AS q")).toEqual([{ q: 0.25 }]);
    // A scientific literal that stays exact expands the way PostgreSQL parses it: full digits,
    // display scale from the applied exponent — identical to the same value written out.
    expect(run("SELECT 1e400 AS q")).toEqual([{ q: `1${"0".repeat(400)}` }]);
    expect(run("SELECT 1.000000000000000001e3 AS q")).toEqual([{ q: "1000.000000000000001" }]);
    // Float columns keep binary float semantics: a lone representable constant stays a plain
    // number on the vectorized paths, and an exact constant meeting a float column computes
    // exactly only when Float64 cannot spell it.
    expect(run("SELECT amount * 1.1 AS q FROM rows WHERE id = 3")).toEqual([
      { q: 3.3000000000000003 },
    ]);
    expect(run("SELECT amount + 1.000000000000000000000001 AS q FROM rows WHERE id = 3")).toEqual([
      { q: "4.000000000000000000000001" },
    ]);
    // Division by zero stays NULL through the exact fold, matching execution.
    expect(run("SELECT 0.1 / 0 AS q")).toEqual([{ q: null }]);
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

  it("decorrelates quantified subqueries with empty-set and NULL semantics", () => {
    expect(
      run(
        "SELECT r.id FROM rows r WHERE r.amount = ANY " +
          "(SELECT q.amount FROM rows q WHERE q.region = r.region) ORDER BY r.id",
      ),
    ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    expect(
      run(
        "SELECT r.id FROM rows r WHERE r.amount > ALL " +
          "(SELECT q.amount FROM rows q WHERE q.region = r.region) ORDER BY r.id",
      ),
    ).toEqual([{ id: 4 }]);
    expect(
      run(
        "SELECT r.id FROM rows r WHERE 'z' > ALL " +
          "(SELECT q.region FROM rows q WHERE q.amount < r.amount) ORDER BY r.id",
      ),
    ).toEqual([{ id: 2 }, { id: 3 }, { id: 4 }]);
    expect(
      run(
        "SELECT r.id FROM rows r WHERE r.region = ALL " +
          "(SELECT q.region FROM rows q WHERE q.amount < 0) ORDER BY r.id",
      ),
    ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
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

describe("CASE aggregates over typed dictionaries", () => {
  it("decides NUMERIC and enum conditions through the generic evaluator", async () => {
    // A typed dictionary (NUMERIC, enum) is never decided by string identity against the CASE
    // literal; the domain's own equality rules answer, so 1.5 and 1.50 are one value.
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
    await database.execute(
      "CREATE TABLE feelings (id INTEGER PRIMARY KEY, price NUMERIC(10, 2), feeling mood)",
    );
    await database.execute(
      "INSERT INTO feelings VALUES (1, 1.5, 'sad'), (2, 1.50, 'ok'), (3, 2, 'happy'), (4, NULL, NULL), (5, 1.5, 'ok')",
    );
    const sql =
      "SELECT SUM(CASE WHEN price = 1.5 THEN 1 ELSE 0 END) AS n, SUM(CASE WHEN price = '1.50' THEN 1 ELSE 0 END) AS t, SUM(CASE WHEN feeling = 'ok' THEN 1 ELSE 0 END) AS ok, SUM(CASE WHEN feeling IN ('sad', 'happy') THEN 1 WHEN feeling IS NULL THEN 10 ELSE 0 END) AS extremes FROM feelings";
    const expected = [{ n: 3, t: 3, ok: 2, extremes: 12 }];
    expect((await database.query(sql, { memoize: false })).rows).toEqual(expected);
    await database.close();
  });
});

describe("subquery name resolution", () => {
  // The rewrites that move a subquery into a derived block (an uncorrelated IN becoming a
  // join, an outer predicate entering a correlation probe) run only after the database has
  // resolved every unqualified name, so a subquery that reads the enclosing query's column
  // without naming its table still correlates the way PostgreSQL resolves it.
  async function fixture(): Promise<MinnowDatabase> {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT)");
    await database.execute(
      "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, home_region TEXT)",
    );
    await database.execute("INSERT INTO customers VALUES (1, 'west'), (2, 'east'), (3, 'west')");
    await database.execute(
      "INSERT INTO orders VALUES (10, 1, 'west'), (11, 2, 'west'), (12, 3, 'east'), (13, 9, 'west')",
    );
    return database;
  }

  it("correlates subqueries through an unqualified outer column", async () => {
    const database = await fixture();
    const ids = async (sql: string, params?: unknown[]): Promise<unknown[]> =>
      (
        await database.query(sql, {
          memoize: false,
          ...(params === undefined ? {} : { params: params as never }),
        })
      ).rows.map((row) => row.id);
    expect(
      await ids(
        "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = home_region) ORDER BY id",
      ),
    ).toEqual([10]);
    expect(
      await ids(
        "SELECT id FROM orders WHERE customer_id NOT IN (SELECT id FROM customers WHERE region = home_region) ORDER BY id",
      ),
    ).toEqual([11, 12, 13]);
    expect(
      await ids(
        "SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE id = customer_id AND region = home_region) ORDER BY id",
      ),
    ).toEqual([10]);
    expect(
      await ids(
        "SELECT id FROM orders WHERE (SELECT region FROM customers WHERE id = customer_id) = home_region ORDER BY id",
      ),
    ).toEqual([10]);
    // An uncorrelated subquery becomes the join, with the outer range reaching the key.
    expect(
      await ids(
        "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'west') AND id > ? ORDER BY id",
        [10],
      ),
    ).toEqual([12]);
    await database.close();
  });
});
