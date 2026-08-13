import { describe, expect, it } from "vitest";
import { chooseJoinOrder, type DatabaseRow } from "./database.js";
import { optimizePlan, renderPlan } from "./optimizer.js";
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
