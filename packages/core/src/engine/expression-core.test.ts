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
    expect(run("SELECT CAST(amount AS JSONB) AS x FROM rows WHERE id = 1")).toEqual([{ x: "10" }]);
    // Integer targets truncate toward zero, including negatives.
    expect(run("SELECT CAST(0 - 2.7 AS INTEGER) AS i FROM rows WHERE id = 1")).toEqual([{ i: -3 }]);
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
    expect(() => run("SELECT CAST('nope' AS UUID) AS x FROM rows")).toThrow("Invalid UUID");
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
  it("uses PostgreSQL's NULLs-last ascending and NULLs-first descending defaults", () => {
    expect(run("SELECT id, region FROM rows ORDER BY region, id").map((row) => row.id)).toEqual([
      3, 1, 2,
    ]);
    expect(
      run("SELECT id, region FROM rows ORDER BY region DESC, id").map((row) => row.id),
    ).toEqual([2, 1, 3]);
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
    // GROUPS frames count peer groups, which only an ordering defines.
    expect(() =>
      compileQuery("SELECT SUM(amount) OVER (GROUPS UNBOUNDED PRECEDING) AS s FROM rows"),
    ).toThrow("GROUPS frames require ORDER BY");
  });
});

describe("features SQLite cannot arbitrate", () => {
  it("ILIKE matches case-insensitively with LIKE's 3VL", () => {
    expect(run("SELECT id FROM rows WHERE region ILIKE 'WEST%' ORDER BY id")).toEqual([{ id: 1 }]);
    // NULL regions drop from NOT ILIKE, like NOT LIKE.
    expect(run("SELECT id FROM rows WHERE region NOT ILIKE 'w%' ORDER BY id")).toEqual([{ id: 3 }]);
  });

  it("GREATEST and LEAST ignore NULL arguments like PostgreSQL", () => {
    expect(
      run("SELECT GREATEST(1, NULL, 3) AS g, LEAST(NULL, 2, NULL) AS l FROM rows WHERE id = 1"),
    ).toEqual([{ g: 3, l: 2 }]);
  });

  it("EXTRACT reads UTC fields including ISO week and epoch", () => {
    expect(
      run(
        "SELECT EXTRACT(quarter FROM joined) AS q, EXTRACT(week FROM joined) AS w, EXTRACT(hour FROM joined) AS h FROM rows WHERE id = 1",
      ),
    ).toEqual([{ q: 1, w: 1, h: 3 }]);
    expect(() => run("SELECT EXTRACT(timezone_hour FROM joined) AS c FROM rows")).toThrow(
      "Unsupported EXTRACT field",
    );
  });

  it("rejects SQRT of negatives and non-finite POWER", () => {
    expect(() => run("SELECT SQRT(0 - 4) AS s FROM rows")).toThrow("non-negative");
    expect(() => run("SELECT POWER(10, 1000) AS p FROM rows")).toThrow("non-finite");
  });

  it("computes INTERSECT ALL and EXCEPT ALL with bag semantics", () => {
    const bags = new Map<string, DatabaseRow[]>([
      ["a", [{ v: 1 }, { v: 1 }, { v: 1 }, { v: 2 }, { v: 3 }]],
      ["b", [{ v: 1 }, { v: 1 }, { v: 2 }, { v: 4 }]],
    ]);
    const intersect = compileQuery("SELECT v FROM a INTERSECT ALL SELECT v FROM b");
    expect(executeRowQuery(intersect, bags).rows.map((row) => row.v)).toEqual([1, 1, 2]);
    expect(executeQuery(intersect, bags).rows.map((row) => row.v)).toEqual([1, 1, 2]);
    const except = compileQuery("SELECT v FROM a EXCEPT ALL SELECT v FROM b");
    expect(executeRowQuery(except, bags).rows.map((row) => row.v)).toEqual([1, 3]);
    expect(executeQuery(except, bags).rows.map((row) => row.v)).toEqual([1, 3]);
  });

  it("rejects a correlated select-list scalar nested inside an outer aggregate", () => {
    expect(() =>
      compileQuery(
        "SELECT r.region AS g, SUM((SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region)) AS s FROM rows r GROUP BY r.region",
      ),
    ).toThrow("cannot be nested inside an aggregate");
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

  it("keeps its own set per aggregate, beside the plain ones", () => {
    const duplicated = new Map<string, DatabaseRow[]>([
      ["t", [{ v: 4 }, { v: 4 }, { v: 2 }, { v: null }]],
    ]);
    const plan = compileQuery(
      "SELECT SUM(DISTINCT v) AS s, COUNT(*) AS c, COUNT(DISTINCT v) AS d, SUM(v) AS total FROM t",
    );
    const expected = [{ s: 6, c: 4, d: 2, total: 10 }];
    expect(executeRowQuery(plan, duplicated).rows).toEqual(expected);
    expect(executeQuery(plan, duplicated).rows).toEqual(expected);
  });

  it("counts distinct values per group, not per select", () => {
    const rows = new Map<string, DatabaseRow[]>([
      [
        "t",
        [
          { g: "a", v: 1, w: 9 },
          { g: "a", v: 1, w: 8 },
          { g: "a", v: 2, w: 9 },
          { g: "b", v: 5, w: 1 },
        ],
      ],
    ]);
    const plan = compileQuery(
      "SELECT g, COUNT(DISTINCT v) AS vs, COUNT(DISTINCT w) AS ws FROM t GROUP BY g ORDER BY g",
    );
    const expected = [
      { g: "a", vs: 2, ws: 2 },
      { g: "b", vs: 1, ws: 1 },
    ];
    expect(executeRowQuery(plan, rows).rows).toEqual(expected);
    expect(executeQuery(plan, rows).rows).toEqual(expected);
  });

  it("still refuses DISTINCT where no aggregate is being taken", () => {
    expect(() => compileQuery("SELECT UPPER(DISTINCT region) AS u FROM rows")).toThrow(
      "only supported inside aggregate functions",
    );
    expect(() => compileQuery("SELECT g FROM t WHERE COUNT(DISTINCT v) > 1")).toThrow();
  });
});
