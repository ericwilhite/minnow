import { describe, expect, it } from "vitest";
import type { DatabaseRow } from "./database.js";
import { chooseJoinOrder, optimizePlan, renderPlan } from "./optimizer.js";
import { compileQuery, executeQuery, executeRowQuery } from "./query.js";
import { columnarTableFromRows } from "./vector.js";

const rows: DatabaseRow[] = [
  { region: "west", amount: 10, ratio: 1.5 },
  { region: "west", amount: 20, ratio: 2.5 },
  { region: "east", amount: 5, ratio: 0.5 },
  { region: null, amount: 8, ratio: null },
];
const dims: DatabaseRow[] = [
  { region: "west", weight: 2 },
  { region: "east", weight: 3 },
];
const tables = new Map([
  ["rows", rows],
  ["dims", dims],
]);

/** Optimized and raw plans must agree in both executors. */
function expectEquivalent(sql: string): void {
  const raw = compileQuery(sql, { optimize: false });
  const optimized = optimizePlan(raw);
  const reference = executeRowQuery(raw, tables);
  expect(executeRowQuery(optimized, tables), sql).toEqual(reference);
  expect(executeQuery(optimized, tables), sql).toEqual(reference);
}

describe("deterministic plan rewrites", () => {
  it("folds constant arithmetic and ROUND while preserving non-finite runtime semantics", () => {
    const optimized = optimizePlan(
      compileQuery("SELECT amount + 1 * 2 AS bumped FROM rows WHERE amount > 2 + 3", {
        optimize: false,
      }),
    );
    expect(renderPlan(optimized)).toBe(
      ["select bumped: (amount + 2)", "  from table rows", "  where amount > 5"].join("\n"),
    );
    // Division by zero is NULL (SQLite semantics), so the fold produces a NULL literal.
    const division = optimizePlan(
      compileQuery("SELECT amount FROM rows WHERE amount > 1 / 0", { optimize: false }),
    );
    expect(renderPlan(division)).toContain("where amount > null");
    expectEquivalent("SELECT ROUND(2.345, 2) AS folded, amount FROM rows WHERE amount > 2 + 3");
  });

  it("rewrites NOT into the complement its operand already expresses", () => {
    const whereOf = (sql: string): string => {
      const rendered = renderPlan(optimizePlan(compileQuery(sql, { optimize: false })));
      return rendered
        .split("\n")
        .filter((line) => line.includes("where"))
        .join(" | ");
    };
    expect(whereOf("SELECT region FROM rows WHERE NOT (amount = 10)")).toContain("amount != 10");
    expect(whereOf("SELECT region FROM rows WHERE NOT (amount > 10)")).toContain("amount <= 10");
    expect(whereOf("SELECT region FROM rows WHERE NOT (region IS NULL)")).toContain(
      "region IS NOT NULL",
    );
    expect(whereOf("SELECT region FROM rows WHERE NOT (amount IN (1, 2))")).toContain("NOT IN");
    expect(whereOf("SELECT region FROM rows WHERE NOT (NOT (amount = 10))")).toContain(
      "amount = 10",
    );
    // De Morgan: a negated conjunction becomes a disjunction, a negated disjunction becomes
    // two independent predicates the scan can compile a kernel for apiece.
    expect(whereOf("SELECT region FROM rows WHERE NOT (amount > 1 AND amount < 5)")).toContain(
      "(amount <= 1) or (amount >= 5)",
    );
    const negatedOr = optimizePlan(
      compileQuery("SELECT region FROM rows WHERE NOT (amount = 1 OR amount = 5)", {
        optimize: false,
      }),
    );
    expect(negatedOr.predicates).toHaveLength(2);
    expect(negatedOr.predicates.map((predicate) => predicate.operator)).toEqual(["!=", "!="]);
    for (const sql of [
      "SELECT region FROM rows WHERE NOT (amount = 10)",
      "SELECT region FROM rows WHERE NOT (amount > 1 AND amount < 5)",
      "SELECT region FROM rows WHERE NOT (amount = 1 OR amount = 5)",
      "SELECT region FROM rows WHERE NOT (region IS NULL)",
      "SELECT region FROM rows WHERE NOT (ratio IS NULL) AND NOT (amount = 8)",
    ]) {
      expectEquivalent(sql);
    }
  });

  it("splits a conjunction inside one predicate into independent predicates", () => {
    const split = optimizePlan(
      compileQuery("SELECT region FROM rows WHERE (amount > 1 AND amount < 20) AND ratio > 0", {
        optimize: false,
      }),
    );
    expect(split.predicates).toHaveLength(3);
    expectEquivalent("SELECT region FROM rows WHERE (amount > 1 AND amount < 20) AND ratio > 0");
  });

  it("normalizes a disjunction of equalities on one column into an IN list", () => {
    const optimized = optimizePlan(
      compileQuery("SELECT region FROM rows WHERE amount = 10 OR amount = 20 OR amount = 5", {
        optimize: false,
      }),
    );
    expect(renderPlan(optimized)).toContain("where amount IN (10, 20, 5)");
    expectEquivalent("SELECT region FROM rows WHERE amount = 10 OR amount = 20 OR amount = 5");
    // Reversed operands reach the same list, and placeholders stay placeholders so the
    // rewritten list still binds at query time.
    expect(
      renderPlan(
        optimizePlan(
          compileQuery("SELECT region FROM rows WHERE 10 = amount OR amount = 20", {
            optimize: false,
          }),
        ),
      ),
    ).toContain("where amount IN (10, 20)");
    const parameterized = optimizePlan(
      compileQuery("SELECT region FROM rows WHERE amount = ? OR amount = ?", { optimize: false }),
    );
    expect(parameterized.predicates[0]?.operator).toBe("IN");
    expect(parameterized.parameterCount).toBe(2);
  });

  it("keeps a NULL member's unknown result when a disjunction becomes an IN list", () => {
    // region is NULL in one row: `region = 'west' OR region = NULL` must match only 'west',
    // and NOT of it must drop both the match and every unknown row -- exactly IN semantics.
    expectEquivalent("SELECT region FROM rows WHERE region = 'west' OR region = NULL");
    expectEquivalent("SELECT region FROM rows WHERE NOT (region = 'west' OR region = 'east')");
    expectEquivalent("SELECT region FROM rows WHERE region = 'west' OR region = 'east'");
  });

  it("declines the IN rewrite for disjunctions that are not one column's equalities", () => {
    const decline = (sql: string): void => {
      expect(
        optimizePlan(compileQuery(sql, { optimize: false })).predicates[0]?.operator,
        sql,
      ).not.toBe("IN");
      expectEquivalent(sql);
    };
    decline("SELECT region FROM rows WHERE region = 'west' OR amount = 10");
    decline("SELECT region FROM rows WHERE amount = 10 OR amount > 15");
    decline("SELECT region FROM rows WHERE amount = 10 OR amount = ratio");
    decline("SELECT region FROM rows WHERE amount = 10 AND region = 'west'");
  });

  it("pushes predicates into derived tables and CTEs", () => {
    const optimized = optimizePlan(
      compileQuery(
        "WITH scaled AS (SELECT region, amount * 2 AS doubled FROM rows) SELECT doubled FROM scaled WHERE doubled > 10",
        { optimize: false },
      ),
    );
    const rendered = renderPlan(optimized);
    expect(rendered).toContain("where (amount * 2) > 10");
    expect(rendered.split("where")).toHaveLength(2);
    expectEquivalent(
      "WITH scaled AS (SELECT region, amount * 2 AS doubled FROM rows) SELECT doubled FROM scaled WHERE doubled > 10",
    );
  });

  it("pushes only group-key predicates into grouped derived blocks", () => {
    const grouped =
      "SELECT t.region, t.total FROM (SELECT region, SUM(amount) AS total FROM rows GROUP BY region) t WHERE t.region = 'west' AND t.total > 5";
    const optimized = optimizePlan(compileQuery(grouped, { optimize: false }));
    const rendered = renderPlan(optimized);
    expect(rendered).toContain("where region = 'west'");
    expect(rendered).toContain("where t.total > 5");
    expectEquivalent(grouped);
  });

  it("takes the hash key out of a conjunctive inner join and files the rest as predicates", () => {
    const sql =
      "SELECT r.region, r.amount, d.weight FROM rows r JOIN dims d ON d.region = r.region AND d.weight > 2 AND r.amount > 5 ORDER BY amount";
    const rendered = renderPlan(optimizePlan(compileQuery(sql, { optimize: false })));
    // Without this the join renders as `on null = null`: a cross product with a filter.
    expect(rendered).toContain("inner join on d.region = r.region");
    expect(rendered).toContain("where d.weight > 2");
    expect(rendered).toContain("where r.amount > 5");
    expectEquivalent(sql);
  });

  it("takes the first equality of a multi-key join and keeps the others as predicates", () => {
    const sql =
      "SELECT r.region, d.weight FROM rows r JOIN dims d ON d.region = r.region AND d.weight = r.amount";
    const rendered = renderPlan(optimizePlan(compileQuery(sql, { optimize: false })));
    expect(rendered).toContain("inner join on d.region = r.region");
    expect(rendered).toContain("where d.weight = r.amount");
    expectEquivalent(sql);
  });

  /**
   * A self-join pairing rows of one table, which is the shape that made the difference visible:
   * the equality is the only thing standing between this and every row against every row.
   */
  it("takes the hash key out of a self-join", () => {
    const sql =
      "SELECT a.region, COUNT(*) AS pairs FROM rows a JOIN rows b ON b.region = a.region AND b.amount > a.amount GROUP BY a.region";
    const rendered = renderPlan(optimizePlan(compileQuery(sql, { optimize: false })));
    expect(rendered).toContain("inner join on b.region = a.region");
    expectEquivalent(sql);
  });

  it("leaves a left join's condition alone, since a predicate would delete its null rows", () => {
    const sql =
      "SELECT r.region, r.amount, d.weight FROM rows r LEFT JOIN dims d ON d.region = r.region AND d.weight > 2 ORDER BY amount";
    const rendered = renderPlan(optimizePlan(compileQuery(sql, { optimize: false })));
    expect(rendered).toContain("on null = null");
    expect(rendered).not.toContain("where d.weight > 2");
    expectEquivalent(sql);
  });

  it("leaves an ON clause that has no equality across the join", () => {
    const sql = "SELECT r.region, d.weight FROM rows r JOIN dims d ON d.weight > r.amount";
    expect(renderPlan(optimizePlan(compileQuery(sql, { optimize: false })))).toContain(
      "on null = null",
    );
    expectEquivalent(sql);
  });

  it("keeps predicates above left joins onto derived sources", () => {
    const sql =
      "SELECT r.region, d.total FROM rows r LEFT JOIN (SELECT region, SUM(amount) AS total FROM rows GROUP BY region) d ON d.region = r.region WHERE d.total > 10";
    const rendered = renderPlan(optimizePlan(compileQuery(sql, { optimize: false })));
    expect(rendered).toContain("where d.total > 10");
    expectEquivalent(sql);
  });

  it("prunes unreferenced projections from plain derived blocks", () => {
    const sql =
      "SELECT d.amount FROM (SELECT region, amount, ratio FROM rows) d WHERE d.amount > 5";
    const rendered = renderPlan(optimizePlan(compileQuery(sql, { optimize: false })));
    expect(rendered).toContain("select amount: amount");
    expect(rendered).not.toContain("ratio");
    expectEquivalent(sql);
  });

  it("keeps every projection of grouped and distinct derived blocks", () => {
    const sql = "SELECT COUNT(*) AS count FROM (SELECT DISTINCT region, amount FROM rows) d";
    const rendered = renderPlan(optimizePlan(compileQuery(sql, { optimize: false })));
    expect(rendered).toContain("region");
    expect(rendered).toContain("amount");
    expectEquivalent(sql);
  });

  it("combines outer and derived limits", () => {
    const sql = "SELECT d.amount FROM (SELECT amount FROM rows LIMIT 3) d LIMIT 2";
    const rendered = renderPlan(optimizePlan(compileQuery(sql, { optimize: false })));
    // Both the outer block and the derived block carry the combined smaller limit.
    expect(rendered.match(/limit 2/g)).toHaveLength(2);
    expect(rendered).not.toContain("limit 3");
    expectEquivalent(sql);
  });

  it("chooses the smaller inner-join input as the build side from exact row counts", () => {
    const big = Array.from({ length: 100 }, (_, index) => ({
      region: index % 2 === 0 ? "west" : "east",
      amount: index,
    }));
    const inputs = new Map([
      ["rows", columnarTableFromRows("rows", big)],
      ["dims", columnarTableFromRows("dims", dims)],
    ]);
    const swapped = chooseJoinOrder(
      compileQuery("SELECT d.weight, r.amount FROM dims d JOIN rows r ON r.region = d.region"),
      inputs,
    );
    expect(swapped.base.table).toBe("rows");
    expect(swapped.joins[0]?.table).toBe("dims");
    expect(swapped.joins[0]?.kind).toBe("inner");

    const keptSmallBuild = chooseJoinOrder(
      compileQuery("SELECT r.amount, d.weight FROM rows r JOIN dims d ON d.region = r.region"),
      inputs,
    );
    expect(keptSmallBuild.base.table).toBe("rows");

    const keptLeft = chooseJoinOrder(
      compileQuery("SELECT d.weight FROM dims d LEFT JOIN rows r ON r.region = d.region"),
      inputs,
    );
    expect(keptLeft.base.table).toBe("dims");

    const keptWildcard = chooseJoinOrder(
      compileQuery("SELECT * FROM dims d JOIN rows r ON r.region = d.region"),
      inputs,
    );
    expect(keptWildcard.base.table).toBe("dims");

    const bigTables = new Map<string, DatabaseRow[]>([
      ["rows", big],
      ["dims", dims],
    ]);
    const sql = "SELECT d.weight, r.amount FROM dims d JOIN rows r ON r.region = d.region";
    const byRow = (left: Record<string, unknown>, right: Record<string, unknown>) =>
      JSON.stringify(left).localeCompare(JSON.stringify(right));
    const swappedResult = executeQuery(swapped, bigTables);
    const reference = executeRowQuery(compileQuery(sql, { optimize: false }), bigTables);
    expect(swappedResult.columns).toEqual(reference.columns);
    // A swapped scan changes unordered output order, which SQL leaves unspecified.
    expect([...swappedResult.rows].sort(byRow)).toEqual([...reference.rows].sort(byRow));
  });

  it("keeps optimized plans equivalent across representative shapes", () => {
    for (const sql of [
      "SELECT region, amount FROM rows WHERE amount NOT IN (2 + 3, 10)",
      "WITH a AS (SELECT region, amount FROM rows), b AS (SELECT region, amount FROM a WHERE amount > 1 + 1) SELECT region, COUNT(*) AS count FROM b GROUP BY region",
      "SELECT r.amount, d.weight FROM rows r JOIN (SELECT region, weight FROM dims) d ON d.region = r.region WHERE d.weight > 1 AND r.amount > 2",
      "SELECT region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM rows WHERE amount > 1 + 1",
      "SELECT amount FROM rows WHERE amount > 0 UNION SELECT amount FROM rows WHERE amount > 5 + 5 ORDER BY amount",
    ]) {
      expectEquivalent(sql);
    }
  });
});
