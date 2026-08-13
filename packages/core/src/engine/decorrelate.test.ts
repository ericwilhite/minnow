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

  it("supports multi-key correlation", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE EXISTS (SELECT q.amount FROM rows q WHERE q.region = r.region AND q.amount = r.amount)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 3 }]);
  });

  it("rejects non-equality correlation explicitly", () => {
    expect(() =>
      compileQuery(
        "SELECT r.region FROM rows r WHERE EXISTS (SELECT d.region FROM dims d WHERE d.amount > r.amount)",
      ),
    ).toThrow("support only equality conditions");
  });

  it("rejects correlated NOT IN with a pointer to NOT EXISTS", () => {
    expect(() =>
      compileQuery(
        "SELECT r.region FROM rows r WHERE r.region NOT IN (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toThrow("use NOT EXISTS");
  });

  it("rejects correlated subqueries outside top-level WHERE conjuncts", () => {
    expect(() =>
      compileQuery(
        "SELECT (SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region) AS a FROM rows r",
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
