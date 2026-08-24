import { describe, expect, it } from "vitest";
import type { DatabaseRow } from "./database.js";
import { compileQuery, executeQuery, executeRowQuery } from "./query.js";

const tables = new Map<string, DatabaseRow[]>([
  [
    "rows",
    [
      { region: "west", amount: 10 },
      { region: "west", amount: 6 },
      { region: "east", amount: 3 },
      { region: null, amount: 8 },
    ],
  ],
  [
    "dims",
    [
      { region: "west", label: "West Coast", rank: 1 },
      { region: "north", label: "North", rank: 3 },
    ],
  ],
  [
    "probes",
    [
      { g: "empty", v: null },
      { g: "clean", v: 1 },
      { g: "clean", v: 2 },
      { g: "clean", v: null },
      { g: "poisoned", v: 1 },
      { g: "poisoned", v: 3 },
    ],
  ],
  [
    "members",
    [
      { g: "clean", v: 2 },
      { g: "clean", v: 3 },
      { g: "poisoned", v: null },
      { g: "poisoned", v: 4 },
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

describe("correlated subquery decorrelation", () => {
  it("answers a correlated scalar aggregate comparison", () => {
    expect(
      run(
        "SELECT r.region, r.amount FROM rows r WHERE r.amount > (SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region)",
      ),
    ).toEqual([{ region: "west", amount: 10 }]);
  });

  it("treats a NULL correlation key as matching nothing", () => {
    // The NULL-region row joins no group: AVG is NULL, the comparison is UNKNOWN, the row drops.
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.amount >= (SELECT MIN(q.amount) FROM rows q WHERE q.region = r.region)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 3 }]);
  });

  it("answers correlated EXISTS and NOT EXISTS", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE EXISTS (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }]);
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE NOT EXISTS (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toEqual([{ amount: 3 }, { amount: 8 }]);
  });

  it("does not multiply outer rows when the subquery matches many inner rows", () => {
    // Both west rows match the two west entries of the self-join; each outer row appears once.
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE EXISTS (SELECT q.amount FROM rows q WHERE q.region = r.region)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 3 }]);
  });

  it("answers correlated COUNT with zero for unmatched groups", () => {
    expect(
      run(
        "SELECT r.region, r.amount FROM rows r WHERE (SELECT COUNT(*) FROM dims d WHERE d.region = r.region) = 0",
      ),
    ).toEqual([
      { region: "east", amount: 3 },
      { region: null, amount: 8 },
    ]);
  });

  it("answers correlated IN", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.region IN (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }]);
  });

  it("decorrelates equality LATERAL sources into a hash join", () => {
    const plan = compileQuery(
      "SELECT r.amount, x.label FROM rows r, " +
        "LATERAL (SELECT d.label FROM dims d WHERE d.region = r.region) x",
    );
    expect(plan.joins.at(-1)?.kind).toBe("inner");
    expect(plan.joins.at(-1)?.on).toBeUndefined();
    expect(executeQuery(plan, tables).rows).toEqual([
      { amount: 10, label: "West Coast" },
      { amount: 6, label: "West Coast" },
    ]);
    expect(executeRowQuery(plan, tables).rows).toEqual([
      { amount: 10, label: "West Coast" },
      { amount: 6, label: "West Coast" },
    ]);
  });

  it("preserves LEFT LATERAL rows and hides decorrelation keys from wildcards", () => {
    expect(
      run(
        "SELECT r.amount, x.* FROM rows r LEFT JOIN LATERAL " +
          "(SELECT d.label FROM dims d WHERE d.region = r.region) x ON TRUE ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 3, "x.label": null },
      { amount: 6, "x.label": "West Coast" },
      { amount: 8, "x.label": null },
      { amount: 10, "x.label": "West Coast" },
    ]);
  });

  it("supports multi-key correlation", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE EXISTS (SELECT q.amount FROM rows q WHERE q.region = r.region AND q.amount = r.amount)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 3 }]);
  });

  it("answers non-equality correlated EXISTS without multiplying outer rows", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE EXISTS (SELECT q.amount FROM rows q WHERE q.amount < r.amount)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 8 }]);
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE NOT EXISTS (SELECT q.amount FROM rows q WHERE r.amount > q.amount)",
      ),
    ).toEqual([{ amount: 3 }]);
  });

  it("combines equality and range correlation in a semi-join", () => {
    const plan = compileQuery(
      "SELECT r.amount FROM rows r WHERE EXISTS " +
        "(SELECT q.amount FROM rows q WHERE q.region = r.region AND q.amount < r.amount)",
    );
    expect(plan.joins.at(-1)?.kind).toBe("semi");
    expect(executeQuery(plan, tables).rows).toEqual([{ amount: 10 }]);
    expect(executeRowQuery(plan, tables).rows).toEqual([{ amount: 10 }]);
  });

  it("uses an anti-join for non-equality NOT EXISTS", () => {
    const plan = compileQuery(
      "SELECT r.amount FROM rows r WHERE NOT EXISTS " +
        "(SELECT q.amount FROM rows q WHERE q.amount < r.amount)",
    );
    expect(plan.joins.at(-1)?.kind).toBe("anti");
    expect(executeQuery(plan, tables).rows).toEqual([{ amount: 3 }]);
    expect(executeRowQuery(plan, tables).rows).toEqual([{ amount: 3 }]);
  });

  it("answers correlated NOT IN with empty-set and NULL semantics", () => {
    const plan = compileQuery(
      "SELECT p.g, p.v FROM probes p WHERE p.v NOT IN " +
        "(SELECT m.v FROM members m WHERE m.g = p.g)",
    );
    expect(plan.joins.slice(-2).map(({ kind, on }) => [kind, on])).toEqual([
      ["anti", undefined],
      ["left", undefined],
    ]);
    expect(executeQuery(plan, tables).rows).toEqual([
      // NULL NOT IN an empty correlated set is true.
      { g: "empty", v: null },
      // 1 has no equal member and the set contains no NULL.
      { g: "clean", v: 1 },
    ]);
    expect(executeRowQuery(plan, tables).rows).toEqual([
      { g: "empty", v: null },
      { g: "clean", v: 1 },
    ]);
  });

  it("answers correlated scalar aggregates in the select list", () => {
    expect(
      run(
        "SELECT r.amount, (SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region) AS a FROM rows r WHERE r.region = 'west'",
      ),
    ).toEqual([
      { amount: 10, a: 8 },
      { amount: 6, a: 8 },
    ]);
  });

  it("orders by a correlated scalar via the hidden select-item desugar", () => {
    // ORDER BY expressions hoist into hidden select items before decorrelation, so a
    // correlated ordering key rides the same rewrite as a visible select item.
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.region IS NOT NULL ORDER BY (SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region), r.amount",
      ),
    ).toEqual([{ amount: 3 }, { amount: 6 }, { amount: 10 }]);
  });

  it("rejects correlated subqueries outside supported positions", () => {
    expect(() =>
      compileQuery(
        "SELECT r.region AS g, COUNT(*) AS c FROM rows r GROUP BY r.region HAVING COUNT(*) > (SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region)",
      ),
    ).toThrow("top-level WHERE");
    expect(() =>
      compileQuery(
        "SELECT r.region FROM rows r WHERE r.amount > 100 OR EXISTS (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toThrow("top-level WHERE");
  });

  it("rejects correlated subqueries combined with SELECT *", () => {
    expect(() =>
      compileQuery(
        "SELECT * FROM rows r WHERE EXISTS (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toThrow("SELECT *");
  });

  it("rejects a correlated scalar subquery without an aggregate", () => {
    expect(() =>
      compileQuery(
        "SELECT r.region FROM rows r WHERE r.amount = (SELECT q.amount FROM rows q WHERE q.region = r.region)",
      ),
    ).toThrow("exactly one aggregate");
  });

  it("leaves uncorrelated subqueries on the existing resolution path", () => {
    expect(
      run("SELECT r.amount FROM rows r WHERE r.amount > (SELECT AVG(q.amount) FROM rows q)"),
    ).toEqual([{ amount: 10 }, { amount: 8 }]);
  });
});
