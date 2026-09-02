import { describe, expect, it } from "vitest";
import type { DatabaseRow } from "./database.js";
import { chooseJoinOrder, optimizePlan, renderPlan } from "./optimizer.js";
import {
  bindPendingSelectShapes,
  bindPlanParameters,
  compileQuery,
  executeQuery,
  executeRowQuery,
  type CompiledQuery,
} from "./query.js";
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
  it("preserves optimize:false when wildcard lowering is deferred until schema binding", () => {
    const sql = "SELECT * FROM rows WHERE amount > 2 + 3";
    const columnsOf = (table: string): readonly string[] | undefined =>
      table === "rows" ? ["region", "amount", "ratio"] : undefined;
    const raw = bindPendingSelectShapes(compileQuery(sql, { optimize: false }), columnsOf);
    const optimized = bindPendingSelectShapes(compileQuery(sql), columnsOf);

    expect(renderPlan(raw)).toContain("where amount > (2 + 3)");
    expect(renderPlan(optimized)).toContain("where amount > 5");
  });

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

  it("rewrites calendar equalities into ranges the scan can use", () => {
    const rows: DatabaseRow[] = [
      { id: 1, at: new Date("2025-05-31T23:59:59.999Z") },
      { id: 2, at: new Date("2025-06-01T00:00:00.000Z") },
      { id: 3, at: new Date("2025-06-15T12:00:00.000Z") },
      { id: 4, at: new Date("2025-06-30T23:59:59.999Z") },
      { id: 5, at: new Date("2025-07-01T00:00:00.000Z") },
      { id: 6, at: null },
      { id: 7, at: new Date("2024-12-31T23:59:59.000Z") },
      { id: 8, at: new Date("2025-01-01T00:00:00.000Z") },
    ];
    const tables = new Map<string, DatabaseRow[]>([["events", rows]]);
    const ids = (sql: string): unknown[] => {
      const optimized = optimizePlan(compileQuery(sql));
      const raw = compileQuery(sql, { optimize: false });
      const fromOptimized = executeQuery(optimized, tables).rows;
      expect(fromOptimized, sql).toEqual(executeRowQuery(optimized, tables).rows);
      expect(fromOptimized, sql).toEqual(executeRowQuery(raw, tables).rows);
      return fromOptimized.map((row) => row.id);
    };
    expect(
      ids(
        "SELECT id FROM events WHERE DATE_TRUNC('month', at) = TIMESTAMP '2025-06-01 00:00:00' ORDER BY id",
      ),
    ).toEqual([2, 3, 4]);
    expect(
      ids(
        "SELECT id FROM events WHERE TIMESTAMP '2025-06-01 00:00:00' = DATE_TRUNC('month', at) ORDER BY id",
      ),
    ).toEqual([2, 3, 4]);
    expect(
      ids(
        "SELECT id FROM events WHERE DATE_TRUNC('month', at) = TIMESTAMP '2025-06-01 00:00:01' ORDER BY id",
      ),
    ).toEqual([]);
    expect(
      ids(
        "SELECT id FROM events WHERE DATE_TRUNC('year', at) = TIMESTAMP '2025-01-01 00:00:00' ORDER BY id",
      ),
    ).toEqual([1, 2, 3, 4, 5, 8]);
    expect(
      ids(
        "SELECT id FROM events WHERE DATE_TRUNC('quarter', at) = TIMESTAMP '2025-04-01 00:00:00' ORDER BY id",
      ),
    ).toEqual([1, 2, 3, 4]);
    expect(
      ids(
        "SELECT id FROM events WHERE DATE_TRUNC('day', at) = TIMESTAMP '2025-06-30 00:00:00' ORDER BY id",
      ),
    ).toEqual([4]);
    expect(
      ids(
        "SELECT id FROM events WHERE DATE_TRUNC('week', at) = TIMESTAMP '2025-06-09 00:00:00' ORDER BY id",
      ),
    ).toEqual([3]);
    expect(
      ids(
        "SELECT id FROM events WHERE DATE_TRUNC('hour', at) = TIMESTAMP '2025-06-15 12:00:00' ORDER BY id",
      ),
    ).toEqual([3]);
    expect(ids("SELECT id FROM events WHERE EXTRACT(YEAR FROM at) = 2025 ORDER BY id")).toEqual([
      1, 2, 3, 4, 5, 8,
    ]);
    expect(ids("SELECT id FROM events WHERE EXTRACT(YEAR FROM at) = 2024 ORDER BY id")).toEqual([
      7,
    ]);
    expect(ids("SELECT id FROM events WHERE EXTRACT(YEAR FROM at) = 2025.5 ORDER BY id")).toEqual(
      [],
    );
    // The rewritten shape is a pair of ranges on the column itself.
    const plan = optimizePlan(
      compileQuery(
        "SELECT id FROM events WHERE DATE_TRUNC('month', at) = TIMESTAMP '2025-06-01 00:00:00'",
      ),
    );
    expect(plan.predicates).toEqual([
      {
        left: { kind: "column", reference: "at" },
        operator: ">=",
        right: { kind: "literal", value: new Date("2025-06-01T00:00:00.000Z") },
      },
      {
        left: { kind: "column", reference: "at" },
        operator: "<",
        right: { kind: "literal", value: new Date("2025-07-01T00:00:00.000Z") },
      },
    ]);
  });

  it("mirrors key constants across joins in the directions each join kind allows", () => {
    const tables = new Map<string, DatabaseRow[]>([
      [
        "customers",
        [
          { customer_id: 1, name: "a" },
          { customer_id: 2, name: "b" },
          { customer_id: 3, name: "c" },
        ],
      ],
      [
        "orders",
        [
          { id: 10, customer: 1, amount: 9 },
          { id: 11, customer: 2, amount: 9 },
          { id: 12, customer: 1, amount: 1 },
          { id: 13, customer: 9, amount: 5 },
        ],
      ],
    ]);
    // Correlated subqueries have no runnable unoptimized form (decorrelation is lowering), so
    // those shapes compare the two executors only.
    const check = (sql: string, expected: unknown[], raw = true): void => {
      const optimized = optimizePlan(compileQuery(sql));
      expect(executeQuery(optimized, tables).rows, sql).toEqual(expected);
      expect(executeRowQuery(optimized, tables).rows, sql).toEqual(expected);
      if (raw) {
        expect(executeRowQuery(compileQuery(sql, { optimize: false }), tables).rows, sql).toEqual(
          expected,
        );
      }
    };
    // A range on the outer key reaches the inner side of an inner join and a semi-join. A LEFT
    // join keeps its null-extended rows, which a mirrored predicate would fail, so it gets none.
    check(
      "SELECT o.id FROM customers c JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id <= 1 ORDER BY o.id",
      [{ id: 10 }, { id: 12 }],
    );
    const leftPlan = optimizePlan(
      compileQuery(
        "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id >= 2 ORDER BY c.name, o.id",
      ),
    );
    expect(leftPlan.predicates).toHaveLength(1);
    check(
      "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id >= 2 ORDER BY c.name, o.id",
      [
        { name: "b", id: 11 },
        { name: "c", id: null },
      ],
    );
    check(
      "SELECT c.name FROM customers c WHERE c.customer_id < 3 AND EXISTS (SELECT 1 FROM orders o WHERE o.customer = c.customer_id AND o.amount > 5) ORDER BY c.name",
      [{ name: "a" }, { name: "b" }],
      false,
    );
    check(
      "SELECT c.name FROM customers c WHERE c.customer_id >= 2 AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer = c.customer_id) ORDER BY c.name",
      [{ name: "c" }],
      false,
    );
    // An inner-side constant only travels outward where unmatched outer rows are dropped anyway.
    const leftInner = optimizePlan(
      compileQuery(
        "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer = c.customer_id WHERE o.customer IN (1, 9) ORDER BY c.name",
      ),
    );
    expect(
      leftInner.predicates.some(
        (predicate) =>
          predicate.left.kind === "column" && predicate.left.reference === "c.customer_id",
      ),
    ).toBe(false);
    check(
      "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer = c.customer_id WHERE o.customer IN (1, 9) ORDER BY c.name, o.id",
      [
        { name: "a", id: 10 },
        { name: "a", id: 12 },
      ],
    );
    const inner = optimizePlan(
      compileQuery(
        "SELECT c.name, o.id FROM customers c JOIN orders o ON o.customer = c.customer_id WHERE o.customer > 1 ORDER BY c.name",
      ),
    );
    expect(inner.predicates).toContainEqual({
      left: { kind: "column", reference: "c.customer_id" },
      operator: ">",
      right: { kind: "literal", value: 1 },
    });
  });

  it("mirrors a constant on one inner-join key onto the other key", () => {
    const plan = optimizePlan(
      compileQuery(
        "SELECT o.id FROM customers c JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id = 4242 AND o.amount > 5",
      ),
    );
    expect(plan.predicates).toContainEqual({
      left: { kind: "column", reference: "o.customer" },
      operator: "=",
      right: { kind: "literal", value: 4242 },
    });
    const inList = optimizePlan(
      compileQuery(
        "SELECT o.id FROM customers c JOIN orders o ON c.customer_id = o.customer WHERE o.customer IN (1, 2)",
      ),
    );
    expect(inList.predicates).toContainEqual({
      left: { kind: "column", reference: "c.customer_id" },
      operator: "IN",
      right: {
        kind: "list",
        items: [
          { kind: "literal", value: 1 },
          { kind: "literal", value: 2 },
        ],
      },
    });
    // An outer join keeps unmatched rows whose key is NULL, so nothing is implied.
    const outer = optimizePlan(
      compileQuery(
        "SELECT o.id FROM customers c LEFT JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id = 4242",
      ),
    );
    expect(outer.predicates).toHaveLength(1);
    // The mirrored predicate is a filter, not a change of answer.
    const tables = new Map<string, DatabaseRow[]>([
      [
        "customers",
        [
          { customer_id: 1, name: "a" },
          { customer_id: 2, name: "b" },
        ],
      ],
      [
        "orders",
        [
          { id: 10, customer: 1, amount: 9 },
          { id: 11, customer: 2, amount: 9 },
          { id: 12, customer: 1, amount: 1 },
        ],
      ],
    ]);
    const sql =
      "SELECT o.id FROM customers c JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id = 1 AND o.amount > 5 ORDER BY o.id";
    expect(executeQuery(optimizePlan(compileQuery(sql)), tables).rows).toEqual([{ id: 10 }]);
    expect(executeRowQuery(compileQuery(sql), tables).rows).toEqual([{ id: 10 }]);
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

describe("bound placeholders and correlation probes", () => {
  const events: DatabaseRow[] = [
    { id: 1, at: new Date("2025-05-31T23:59:59.999Z") },
    { id: 2, at: new Date("2025-06-01T00:00:00.000Z") },
    { id: 3, at: new Date("2025-06-15T12:00:00.000Z") },
    { id: 4, at: new Date("2025-06-30T23:59:59.999Z") },
    { id: 5, at: new Date("2025-07-01T00:00:00.000Z") },
    { id: 6, at: null },
    { id: 7, at: new Date("2024-12-31T23:59:59.000Z") },
    { id: 8, at: new Date("2025-01-01T00:00:00.000Z") },
  ];
  const eventTables = new Map<string, DatabaseRow[]>([["events", events]]);
  /** Both executors agree on a plan; the ids they return. */
  const ids = (plan: CompiledQuery, tables = eventTables): unknown[] => {
    const rows = executeQuery(plan, tables).rows;
    expect(rows).toEqual(executeRowQuery(plan, tables).rows);
    return rows.map((row) => row.id);
  };

  it("rewrites a calendar equality once its placeholder is bound, on optimized plans only", () => {
    const sql = "SELECT id FROM events WHERE DATE_TRUNC('month', at) = ? ORDER BY id";
    const plan = compileQuery(sql);
    expect(plan.optimized).toBe(true);
    const june = new Date("2025-06-01T00:00:00.000Z");
    const bound = bindPlanParameters(plan, [june]);
    expect(bound.predicates).toEqual([
      {
        left: { kind: "column", reference: "at" },
        operator: ">=",
        right: { kind: "literal", value: june },
      },
      {
        left: { kind: "column", reference: "at" },
        operator: "<",
        right: { kind: "literal", value: new Date("2025-07-01T00:00:00.000Z") },
      },
    ]);
    expect(ids(bound)).toEqual([2, 3, 4]);
    // Binding is copy-on-write: the cached plan keeps its placeholder for the next binding.
    expect(plan.predicates[0]?.left.kind).toBe("call");
    // A value not aligned to its unit can never equal a truncation.
    const misaligned = bindPlanParameters(plan, [new Date("2025-06-01T00:00:01.000Z")]);
    expect(misaligned.predicates).toEqual([
      { left: { kind: "literal", value: 1 }, operator: "=", right: { kind: "literal", value: 0 } },
    ]);
    expect(ids(misaligned)).toEqual([]);
    const year = bindPlanParameters(
      compileQuery("SELECT id FROM events WHERE EXTRACT(YEAR FROM at) = ? ORDER BY id"),
      [2024],
    );
    expect(year.predicates.map((predicate) => predicate.operator)).toEqual([">=", "<"]);
    expect(ids(year)).toEqual([7]);
    // The unoptimized plan stays the oracle: it evaluates the call per row.
    const raw = bindPlanParameters(compileQuery(sql, { optimize: false }), [june]);
    expect(raw.optimized).toBeUndefined();
    expect(raw.predicates[0]?.left.kind).toBe("call");
    expect(executeRowQuery(raw, eventTables).rows.map((row) => row.id)).toEqual([2, 3, 4]);
    // Nested blocks take the rewrite too.
    const nested = bindPlanParameters(
      compileQuery(
        "SELECT e.id FROM events e WHERE EXISTS (SELECT 1 FROM events x WHERE x.id = e.id AND DATE_TRUNC('day', x.at) = ?) ORDER BY e.id",
      ),
      [new Date("2025-06-15T00:00:00.000Z")],
    );
    const rendered = renderPlan(nested);
    expect(rendered).not.toContain("DATE_TRUNC");
    expect(rendered).toContain("x.at >= date 2025-06-15T00:00:00.000Z");
    expect(ids(nested)).toEqual([3]);
  });

  it("mirrors a bound placeholder across inner-join keys", () => {
    const tables = new Map<string, DatabaseRow[]>([
      [
        "customers",
        [
          { customer_id: 1, name: "a" },
          { customer_id: 2, name: "b" },
          { customer_id: 3, name: "c" },
        ],
      ],
      [
        "orders",
        [
          { id: 10, customer: 1, amount: 9 },
          { id: 11, customer: 2, amount: 9 },
          { id: 12, customer: 1, amount: 1 },
          { id: 13, customer: 9, amount: 5 },
        ],
      ],
    ]);
    const range = compileQuery(
      "SELECT o.id FROM customers c JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id >= ?",
    );
    expect(range.predicates).toContainEqual({
      left: { kind: "column", reference: "o.customer" },
      operator: ">=",
      right: { kind: "parameter", index: 0 },
    });
    const ordered =
      "SELECT o.id FROM customers c JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id >= ? ORDER BY o.id";
    expect(ids(bindPlanParameters(compileQuery(ordered), [2]), tables)).toEqual([11]);
    expect(
      executeRowQuery(bindPlanParameters(compileQuery(ordered, { optimize: false }), [2]), tables)
        .rows,
    ).toEqual([{ id: 11 }]);
    const inList = compileQuery(
      "SELECT o.id FROM customers c JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id IN (?, ?)",
    );
    expect(inList.predicates).toContainEqual({
      left: { kind: "column", reference: "o.customer" },
      operator: "IN",
      right: {
        kind: "list",
        items: [
          { kind: "parameter", index: 0 },
          { kind: "parameter", index: 1 },
        ],
      },
    });
    expect(
      ids(
        bindPlanParameters(
          compileQuery(
            "SELECT o.id FROM customers c JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id IN (?, ?) ORDER BY o.id",
          ),
          [1, 3],
        ),
        tables,
      ),
    ).toEqual([10, 12]);
  });

  describe("correlation probes", () => {
    const tables = new Map<string, DatabaseRow[]>([
      [
        "data",
        [
          { id: 1, amount: 10, label: "a" },
          { id: 2, amount: 20, label: "b" },
          { id: 3, amount: 30, label: "c" },
          { id: 4, amount: 40, label: "d" },
          { id: 5, amount: 50, label: "e" },
          { id: null, amount: 60, label: "n" },
        ],
      ],
      [
        "dims",
        [
          { region: "b", rank: 1 },
          { region: "x", rank: 2 },
        ],
      ],
    ]);
    const scalar = "(SELECT x.amount FROM data x WHERE x.id = d.id) AS a";
    /** The probe block and the inner-rows block of a decorrelated scalar subquery. */
    const blocks = (
      sql: string,
      resolvedNames = true,
    ): { probes: CompiledQuery; innerRows: CompiledQuery } => {
      const plan = optimizePlan(compileQuery(sql, { optimize: false }), { resolvedNames });
      const finalRows = plan.joins.at(-1)?.derived;
      const matched = finalRows?.base.derived;
      const probes = matched?.base.derived;
      const innerRows = matched?.joins[0]?.derived;
      if (probes === undefined || innerRows === undefined) throw new Error("unexpected plan shape");
      return { probes, innerRows };
    };
    const predicate = (reference: string, operator: string, value: unknown) => ({
      left: { kind: "column", reference },
      operator,
      right: { kind: "literal", value },
    });

    it("filters the probes by the outer predicates and mirrors key ranges onto the inner rows", () => {
      const { probes, innerRows } = blocks(
        `SELECT d.id, ${scalar} FROM data d WHERE d.id BETWEEN 2 AND 4 AND d.label <> 'c'`,
      );
      expect(probes.predicates).toEqual([
        predicate("d.id", ">=", 2),
        predicate("d.id", "<=", 4),
        predicate("d.label", "<>", "c"),
      ]);
      expect(innerRows.predicates).toEqual([
        predicate("x.id", ">=", 2),
        predicate("x.id", "<=", 4),
      ]);
      expect(
        executeQuery(
          compileQuery(
            `SELECT d.id, ${scalar} FROM data d WHERE d.id BETWEEN 2 AND 4 AND d.label <> 'c' ORDER BY d.id`,
          ),
          tables,
        ).rows,
      ).toEqual([
        { id: 2, a: 20 },
        { id: 4, a: 40 },
      ]);
    });

    it("keeps statement-scoped calls, subqueries, and later sources out of the probes", () => {
      const volatile = blocks(
        `SELECT d.id, ${scalar} FROM data d WHERE d.id >= 2 AND RANDOM() >= 0`,
      );
      expect(volatile.probes.predicates).toEqual([predicate("d.id", ">=", 2)]);
      const clock = blocks(
        `SELECT d.id, ${scalar} FROM data d WHERE d.id >= 2 AND d.amount < EXTRACT(YEAR FROM CURRENT_TIMESTAMP)`,
      );
      expect(clock.probes.predicates).toEqual([predicate("d.id", ">=", 2)]);
      const subquery = blocks(
        `SELECT d.id, ${scalar} FROM data d WHERE d.id >= 2 AND d.amount > (SELECT MIN(q.amount) FROM data q)`,
      );
      expect(subquery.probes.predicates).toEqual([predicate("d.id", ">=", 2)]);
      // A predicate on a source the probe does not retain cannot travel; the probe keeps only
      // the prefix its keys need.
      const later = blocks(
        `SELECT d.id, ${scalar} FROM data d LEFT JOIN dims m ON m.region = d.label WHERE d.id BETWEEN 2 AND 4 AND m.rank IS NULL`,
      );
      expect(later.probes.joins).toEqual([]);
      expect(later.probes.predicates).toEqual([
        predicate("d.id", ">=", 2),
        predicate("d.id", "<=", 4),
      ]);
      expect(
        executeQuery(
          compileQuery(
            `SELECT d.id, ${scalar} FROM data d LEFT JOIN dims m ON m.region = d.label WHERE d.id BETWEEN 2 AND 4 AND m.rank IS NULL ORDER BY d.id`,
          ),
          tables,
        ).rows,
      ).toEqual([
        { id: 3, a: 30 },
        { id: 4, a: 40 },
      ]);
      // An unqualified name travels only when the probe retains the whole FROM list.
      const single = blocks(`SELECT id, ${scalar} FROM data d WHERE amount > 35`);
      expect(single.probes.predicates).toEqual([predicate("amount", ">", 35)]);
      expect(single.innerRows.predicates).toEqual([]);
      // At compile time the name might still belong to an enclosing query, so it stays out.
      const unresolved = blocks(`SELECT id, ${scalar} FROM data d WHERE amount > 35`, false);
      expect(unresolved.probes.predicates).toEqual([]);
      expect(
        executeQuery(
          compileQuery(`SELECT id, ${scalar} FROM data d WHERE amount > 35 ORDER BY id`),
          tables,
        ).rows,
      ).toEqual([
        { id: 4, a: 40 },
        { id: 5, a: 50 },
        { id: null, a: null },
      ]);
      const joined = blocks(
        `SELECT d.id, ${scalar} FROM data d JOIN dims m ON m.region = d.label WHERE rank > 0`,
      );
      expect(joined.probes.predicates).toEqual([]);
    });
  });

  it("rewrites an uncorrelated IN subquery into a join to its distinct values", () => {
    const tables = new Map<string, DatabaseRow[]>([
      [
        "rows",
        [
          { id: 1, region: "west", amount: 10 },
          { id: 2, region: "east", amount: 6 },
          { id: 3, region: null, amount: 3 },
          { id: 4, region: "north", amount: 8 },
          { id: 5, region: "west", amount: 1 },
        ],
      ],
      [
        "dims",
        [
          { region: "west", weight: 2 },
          { region: "west", weight: 3 },
          { region: "east", weight: 1 },
          { region: null, weight: 9 },
        ],
      ],
    ]);
    const equivalent = (sql: string, expected: unknown[]): void => {
      const raw = compileQuery(sql, { optimize: false });
      const optimized = optimizePlan(raw, { resolvedNames: true });
      expect(executeRowQuery(raw, tables).rows, sql).toEqual(expected);
      expect(executeRowQuery(optimized, tables).rows, sql).toEqual(expected);
      expect(executeQuery(optimized, tables).rows, sql).toEqual(expected);
    };
    const membership =
      "SELECT id FROM rows WHERE region IN (SELECT region FROM dims WHERE weight > 1)";
    // Compilation cannot tell whether an unqualified name inside the subquery is the subquery's
    // own or the enclosing query's, so the rewrite waits for resolved names.
    const unresolved = optimizePlan(compileQuery(membership, { optimize: false }));
    expect(unresolved.joins).toEqual([]);
    expect(unresolved.predicates[0]?.operator).toBe("IN");
    const plan = optimizePlan(compileQuery(membership, { optimize: false }), {
      resolvedNames: true,
    });
    expect(plan.predicates).toEqual([]);
    const join = plan.joins[0];
    expect(join?.kind).toBe("inner");
    expect(join?.on).toBeUndefined();
    expect(join?.derived?.groupBy).toHaveLength(1);
    expect(join?.derived?.base.derived?.predicates).toEqual([
      {
        left: { kind: "column", reference: "weight" },
        operator: ">",
        right: { kind: "literal", value: 1 },
      },
    ]);
    // A NULL probe and a NULL member never match; duplicates do not multiply rows.
    equivalent(`${membership} ORDER BY id`, [{ id: 1 }, { id: 5 }]);
    equivalent(
      "SELECT id FROM rows WHERE region IN (SELECT region FROM dims WHERE weight > 100) ORDER BY id",
      [],
    );
    equivalent(
      "SELECT id FROM rows WHERE region IN (SELECT region FROM dims ORDER BY weight LIMIT 1) ORDER BY id",
      [{ id: 2 }],
    );
    equivalent(
      "SELECT id FROM rows WHERE region IN (SELECT region FROM dims GROUP BY region HAVING COUNT(*) > 1) ORDER BY id",
      [{ id: 1 }, { id: 5 }],
    );
    equivalent(
      "SELECT id FROM rows WHERE region IN (SELECT DISTINCT region FROM dims) ORDER BY id",
      [{ id: 1 }, { id: 2 }, { id: 5 }],
    );
    // Only a bare column probe is rewritten; NOT IN keeps its NULL rules by staying as it is.
    const expression = optimizePlan(
      compileQuery("SELECT id FROM rows WHERE amount + 1 IN (SELECT weight FROM dims)", {
        optimize: false,
      }),
      { resolvedNames: true },
    );
    expect(expression.joins).toEqual([]);
    expect(expression.predicates[0]?.operator).toBe("IN");
    equivalent("SELECT id FROM rows WHERE amount + 1 IN (SELECT weight FROM dims) ORDER BY id", [
      { id: 4 },
      { id: 5 },
    ]);
    const negated = optimizePlan(
      compileQuery(
        "SELECT id FROM rows WHERE region NOT IN (SELECT region FROM dims WHERE weight < 5)",
        {
          optimize: false,
        },
      ),
      { resolvedNames: true },
    );
    expect(negated.joins).toEqual([]);
    equivalent(
      "SELECT id FROM rows WHERE region NOT IN (SELECT region FROM dims WHERE weight > 1) ORDER BY id",
      [],
    );
    equivalent(
      "SELECT id FROM rows WHERE region NOT IN (SELECT region FROM dims WHERE weight < 5) ORDER BY id",
      [{ id: 4 }],
    );
    // A constant on the probe column reaches the subquery's own scan through the join key.
    const pushed = optimizePlan(
      compileQuery(
        "SELECT r.id FROM rows r WHERE r.id >= 3 AND r.id IN (SELECT q.id FROM rows q WHERE q.amount > 5)",
        {
          optimize: false,
        },
      ),
      { resolvedNames: true },
    );
    expect(pushed.joins[0]?.derived?.base.derived?.predicates).toContainEqual({
      left: { kind: "column", reference: "q.id" },
      operator: ">=",
      right: { kind: "literal", value: 3 },
    });
    equivalent(
      "SELECT r.id FROM rows r WHERE r.id >= 3 AND r.id IN (SELECT q.id FROM rows q WHERE q.amount > 5) ORDER BY r.id",
      [{ id: 4 }],
    );
  });
});
