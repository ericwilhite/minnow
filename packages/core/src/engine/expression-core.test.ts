import { describe, expect, it } from "vitest";
import type { DatabaseRow } from "./database.js";
import { compileQuery, executeQuery, executeRowQuery } from "./query.js";

const tables = new Map<string, DatabaseRow[]>([
  [
    "rows",
    [
      { id: 1, region: "west", amount: 10, joined: new Date("2026-01-02T03:04:05.000Z") },
      { id: 2, region: null, amount: 6, joined: null },
      { id: 3, region: "east", amount: 3, joined: new Date("2026-02-01T00:00:00.000Z") },
    ],
  ],
  [
    "dims",
    [
      { region: "west", label: "West Coast" },
      { region: "north", label: "North" },
    ],
  ],
]);

function run(sql: string): DatabaseRow[] {
  const plan = compileQuery(sql);
  const vectorized = executeQuery(plan, tables);
  const byRow = executeRowQuery(plan, tables);
  expect(vectorized.rows).toEqual(byRow.rows);
  return vectorized.rows;
}

describe("CAST", () => {
  it("converts between the four logical types", () => {
    expect(
      run(
        "SELECT CAST(amount AS TEXT) AS t, CAST('7' AS INTEGER) AS i, CAST(TRUE AS TEXT) AS b, CAST(joined AS TEXT) AS d FROM rows WHERE id = 1",
      ),
    ).toEqual([{ t: "10", i: 7, b: "true", d: "2026-01-02T03:04:05.000Z" }]);
    expect(
      run("SELECT CAST('2026-01-02T03:04:05.000Z' AS TIMESTAMP) AS d FROM rows WHERE id = 1"),
    ).toEqual([{ d: new Date("2026-01-02T03:04:05.000Z") }]);
    expect(run("SELECT CAST(NULL AS INTEGER) AS n FROM rows WHERE id = 1")).toEqual([{ n: null }]);
    // Integer targets truncate toward zero, including negatives.
    expect(run("SELECT CAST(0 - 2.7 AS INTEGER) AS i FROM rows WHERE id = 1")).toEqual([{ i: -2 }]);
  });

  it("rejects lossy or meaningless casts instead of guessing", () => {
    expect(() => run("SELECT CAST('nope' AS INTEGER) AS x FROM rows")).toThrow(
      "Cannot cast this string to a number",
    );
    expect(() => run("SELECT CAST('maybe' AS BOOLEAN) AS x FROM rows")).toThrow(
      "Cannot cast this string to a boolean",
    );
    expect(() => run("SELECT CAST(2 AS BOOLEAN) AS x FROM rows")).toThrow(
      "Only 0 and 1 cast to boolean",
    );
    expect(() => compileQuery("SELECT CAST(amount AS JSONB) AS x FROM rows")).toThrow(
      "Unsupported CAST target: JSONB",
    );
  });
});

describe("quoted identifiers", () => {
  it("treats quoted names as plain references, never keywords", () => {
    const keyworded = new Map<string, DatabaseRow[]>([["t", [{ select: 1, order: 2 }]]]);
    const plan = compileQuery('SELECT "select", "order" FROM t');
    expect(executeRowQuery(plan, keyworded).rows).toEqual([{ select: 1, order: 2 }]);
  });

  it("rejects empty and unterminated quoted identifiers", () => {
    expect(() => compileQuery('SELECT "" FROM rows')).toThrow("cannot be empty");
    expect(() => compileQuery('SELECT "region FROM rows')).toThrow("Unterminated quoted");
  });
});

describe("NULL ordering", () => {
  it("defaults to NULLs first ascending and last descending", () => {
    expect(run("SELECT id, region FROM rows ORDER BY region, id").map((row) => row.id)).toEqual([
      2, 3, 1,
    ]);
    expect(
      run("SELECT id, region FROM rows ORDER BY region DESC, id").map((row) => row.id),
    ).toEqual([1, 3, 2]);
  });

  it("honors explicit NULLS FIRST/LAST absolutely", () => {
    expect(
      run("SELECT id, region FROM rows ORDER BY region NULLS LAST, id").map((row) => row.id),
    ).toEqual([3, 1, 2]);
    expect(
      run("SELECT id, region FROM rows ORDER BY region DESC NULLS FIRST, id").map((row) => row.id),
    ).toEqual([2, 1, 3]);
  });
});

describe("FULL JOIN", () => {
  it("returns matched plus both unmatched sides", () => {
    expect(
      run(
        "SELECT r.id AS id, d.label AS label FROM rows r FULL JOIN dims d ON d.region = r.region ORDER BY id NULLS LAST",
      ),
    ).toEqual([
      { id: 1, label: "West Coast" },
      { id: 2, label: null },
      { id: 3, label: null },
      { id: null, label: "North" },
    ]);
  });

  it("rejects unsupported combinations explicitly", () => {
    expect(() =>
      compileQuery("SELECT * FROM rows r FULL JOIN dims d ON d.region = r.region"),
    ).toThrow("SELECT *");
    expect(() =>
      compileQuery(
        "SELECT r.region AS g, COUNT(*) AS c FROM rows r FULL JOIN dims d ON d.region = r.region GROUP BY r.region",
      ),
    ).toThrow("grouping");
    expect(() =>
      compileQuery(
        "SELECT r.id AS id FROM rows r FULL JOIN dims d ON d.region = r.region AND d.label = r.region",
      ),
    ).toThrow("single equality");
  });
});

describe("LAG/LEAD and frames", () => {
  it("pins offset-window edge semantics", () => {
    expect(
      run(
        "SELECT id, LAG(amount, 2, 0) OVER (ORDER BY id) AS lag2, LEAD(amount) OVER (ORDER BY id) AS next FROM rows",
      ),
    ).toEqual([
      { id: 1, lag2: 0, next: 6 },
      { id: 2, lag2: 0, next: 3 },
      { id: 3, lag2: 10, next: null },
    ]);
  });

  it("treats an empty frame as COUNT 0 and NULL otherwise", () => {
    expect(
      run(
        "SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) AS c, SUM(amount) OVER (ORDER BY id ROWS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) AS s FROM rows WHERE id = 1",
      ),
    ).toEqual([{ id: 1, c: 0, s: null }]);
  });

  it("rejects malformed windows explicitly", () => {
    expect(() =>
      compileQuery("SELECT LAG(amount) OVER (PARTITION BY region) AS p FROM rows"),
    ).toThrow("requires ORDER BY");
    expect(() => compileQuery("SELECT LAG(amount, id) OVER (ORDER BY id) AS p FROM rows")).toThrow(
      "offset must be a constant",
    );
    expect(() =>
      compileQuery(
        "SELECT SUM(amount) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM rows",
      ),
    ).toThrow("use ROWS");
    expect(() =>
      compileQuery(
        "SELECT SUM(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) AS s FROM rows",
      ),
    ).toThrow("reversed");
    expect(() =>
      compileQuery(
        "SELECT SUM(amount) OVER (ORDER BY id GROUPS UNBOUNDED PRECEDING) AS s FROM rows",
      ),
    ).toThrow("GROUPS");
  });
});

describe("DISTINCT aggregates", () => {
  it("deduplicates before aggregating", () => {
    const duplicated = new Map<string, DatabaseRow[]>([
      ["t", [{ v: 4 }, { v: 4 }, { v: 2 }, { v: null }]],
    ]);
    for (const [sql, expected] of [
      ["SELECT SUM(DISTINCT v) AS x FROM t", 6],
      ["SELECT AVG(DISTINCT v) AS x FROM t", 3],
      ["SELECT COUNT(DISTINCT v) AS x FROM t", 2],
    ] as const) {
      const plan = compileQuery(sql);
      expect(executeRowQuery(plan, duplicated).rows).toEqual([{ x: expected }]);
      expect(executeQuery(plan, duplicated).rows).toEqual([{ x: expected }]);
    }
  });

  it("keeps the existing DISTINCT aggregate restrictions", () => {
    expect(() => compileQuery("SELECT SUM(DISTINCT amount) AS s, COUNT(*) AS c FROM rows")).toThrow(
      "cannot be combined with other aggregates",
    );
    expect(() => compileQuery("SELECT UPPER(DISTINCT region) AS u FROM rows")).toThrow(
      "only supported inside aggregate functions",
    );
  });
});
