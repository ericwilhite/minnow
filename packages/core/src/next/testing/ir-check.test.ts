/**
 * The plan type checker (ir-check.ts) rejects malformed plans, each for its own reason: lowered
 * plans are well formed (every lowering suite checks them), so each case here breaks one.
 */
import { describe, expect, it } from "vitest";
import type { PhysicalPlan, Rel, ScalarExpr, WindowCall } from "../ir/ir.js";
import { checkPlan } from "./ir-check.js";
import { LoweredDatabase } from "./lowering-harness.js";

const INT4 = { kind: "int32", bits: 32 } as const;

function setup() {
  const next = new LoweredDatabase();
  for (const sql of [
    "CREATE TABLE a (id integer, v integer)",
    "CREATE TABLE b (id integer, v integer)",
  ])
    next.execute(sql);
  const plan = (sql: string): PhysicalPlan => {
    const lowered = next.lower(sql);
    if (lowered.kind !== "query") throw new Error(`Expected a query: ${sql}`);
    checkPlan(lowered.plan, next.catalog.view);
    return structuredClone(lowered.plan);
  };
  const check = (plan: PhysicalPlan) => () => {
    checkPlan(plan, next.catalog.view);
  };
  return { plan, check };
}

/** Every operator under `rel`, depth first. */
function all(rel: Rel): Rel[] {
  const children: Rel[] = [];
  for (const value of Object.values(rel))
    if (typeof value === "object" && value !== null && "op" in value && "output" in value)
      children.push(value as Rel);
  if (rel.op === "setOp") children.push(...rel.inputs);
  return [rel, ...children.flatMap(all)];
}

type Mutable<T> = { -readonly [K in keyof T]: T[K] };

describe("checkPlan traverses init plans and checks slot wiring", () => {
  it("rejects a malformed init-plan body (review B6-p2b finding 7)", () => {
    const { plan, check } = setup();
    const malformed = plan("SELECT (SELECT 1) AS x");
    const [init] = malformed.initPlans;
    if (init === undefined) throw new Error("Expected an init plan");
    (init as Mutable<typeof init>).plan = {
      op: "project",
      id: 999,
      input: { op: "values", id: 998, rows: [[]], output: [] },
      exprs: [{ op: "column", index: 999, type: INT4 }],
      output: [{ name: "bad", type: INT4, nullable: false }],
    };
    expect(check(malformed)).toThrow("column 999 of 0");
  });

  it("rejects an init plan whose column is not its slot's type", () => {
    const { plan, check } = setup();
    const malformed = plan("SELECT (SELECT 1) AS x");
    const [init] = malformed.initPlans;
    if (init === undefined) throw new Error("Expected an init plan");
    (malformed.parameters as Mutable<PhysicalPlan["parameters"]>)[init.slot] = { kind: "text" };
    expect(check(malformed)).toThrow(/init plan for slot \d+: slot \d+ is text, read as int32/u);
  });

  it("rejects two init plans filling one slot, and one filling a placeholder", () => {
    const { plan, check } = setup();
    const twice = plan("SELECT (SELECT 1) AS x");
    const [init] = twice.initPlans;
    if (init === undefined) throw new Error("Expected an init plan");
    (twice as Mutable<PhysicalPlan>).initPlans = [init, init];
    expect(check(twice)).toThrow("slot filled twice");
    const placeholder = plan("SELECT (SELECT 1) AS x, $1::integer AS y");
    const [first] = placeholder.initPlans;
    if (first === undefined) throw new Error("Expected an init plan");
    (placeholder as Mutable<PhysicalPlan>).initPlans = [{ ...first, slot: 0 }];
    expect(check(placeholder)).toThrow("slot is a placeholder or the clock");
  });

  it("rejects a parameter read past the slots or as another type", () => {
    const { plan, check } = setup();
    const past = plan("SELECT $1::integer AS x");
    (past as Mutable<PhysicalPlan>).parameters = [];
    expect(check(past)).toThrow("placeholders past the slots");
    (past as Mutable<PhysicalPlan>).placeholderCount = 0;
    expect(check(past)).toThrow("slot 0 of 0");
    const retyped = plan("SELECT $1::integer AS x");
    (retyped as Mutable<PhysicalPlan>).parameters = [{ kind: "text" }];
    expect(check(retyped)).toThrow("slot 0 is text, read as int32");
  });

  it("rejects an apply binding into a missing slot or of another type", () => {
    const { plan, check } = setup();
    const base = plan("SELECT id FROM a");
    const binding = (slot: number): PhysicalPlan => {
      const expr: ScalarExpr = { op: "column", index: 0, type: INT4 };
      const root: Rel = {
        op: "apply",
        id: 900,
        mode: "scalar",
        outer: base.root,
        inner: {
          op: "values",
          id: 901,
          rows: [[{ op: "param", slot, type: INT4 }]],
          output: [{ name: "x", type: INT4, nullable: true }],
        },
        bindings: [{ slot, expr }],
        quantified: null,
        memoize: true,
        output: [...base.root.output, { name: "x", type: INT4, nullable: true }],
      };
      const [column] = base.columns;
      if (column === undefined) throw new Error("Expected a column");
      return { ...base, root, columns: [...base.columns, { ...column, name: "x" }] };
    };
    expect(check(binding(5))).toThrow("slot 5: is not declared");
    const typed = binding(0);
    (typed as Mutable<PhysicalPlan>).parameters = [{ kind: "text" }];
    expect(check(typed)).toThrow("slot 0 is text, read as int32");
    const wired = binding(0);
    (wired as Mutable<PhysicalPlan>).parameters = [INT4];
    expect(check(wired)).not.toThrow();
  });
});

describe("checkPlan checks window calls and work tables", () => {
  const windowOf = (plan: PhysicalPlan) => {
    const found = all(plan.root).find((rel) => rel.op === "window");
    if (found?.op !== "window") throw new Error("Expected a window");
    return found.functions as Array<Mutable<WindowCall>>;
  };

  it("rejects a frame offset of the wrong type or on the wrong bound", () => {
    const { plan, check } = setup();
    const sql =
      "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM b";
    const rows = plan(sql);
    const [call] = windowOf(rows);
    if (call?.frame === null || call === undefined) throw new Error("Expected a frame");
    call.frame = {
      ...call.frame,
      start: { kind: "preceding", offset: { op: "param", slot: 0, type: INT4 } },
    };
    (rows as Mutable<PhysicalPlan>).parameters = [INT4];
    (rows as Mutable<PhysicalPlan>).placeholderCount = 1;
    expect(check(rows)).toThrow("rows offset is not bigint");
    const misplaced = plan(sql);
    const [other] = windowOf(misplaced);
    if (other?.frame === null || other === undefined) throw new Error("Expected a frame");
    other.frame = {
      ...other.frame,
      end: { kind: "current-row", offset: other.frame.start.offset },
    };
    expect(check(misplaced)).toThrow("frame bound current-row offset");
  });

  it("rejects a window call outside its signature", () => {
    const { plan, check } = setup();
    const ranked = plan("SELECT id, row_number() OVER (ORDER BY id) AS r FROM b");
    const [call] = windowOf(ranked);
    if (call === undefined) throw new Error("Expected a call");
    call.type = INT4;
    expect(check(ranked)).toThrow("window row_number: () → bigint");
  });

  it("rejects a work-table scan outside its recursive step", () => {
    const { plan, check } = setup();
    const recursive = plan(
      "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
    );
    const node = all(recursive.root).find((rel) => rel.op === "recursive");
    if (node?.op !== "recursive") throw new Error("Expected a recursion");
    const scan = all(node.step).find((rel) => rel.op === "workTableScan");
    if (scan?.op !== "workTableScan") throw new Error("Expected a work-table scan");
    (scan as Mutable<typeof scan>).workTable = node.workTable + 100;
    expect(check(recursive)).toThrow("work table outside its recursive step");
  });
});

describe("checkPlan: slot producers and every expression position (review B6-p2b-2 finding 6)", () => {
  it("rejects a declared slot nothing fills (the init plan removed)", () => {
    const { plan, check } = setup();
    const unfilled = plan("SELECT (SELECT 1) AS x");
    (unfilled as Mutable<PhysicalPlan>).initPlans = [];
    expect(check(unfilled)).toThrow("nothing fills it");
    // A declared slot nothing reads either: only the producer count sees it.
    const spare = plan("SELECT id FROM a");
    (spare as Mutable<PhysicalPlan>).parameters = [INT4];
    expect(check(spare)).toThrow("slot 0: nothing fills it");
  });

  it("rejects an apply-bound slot read outside the apply", () => {
    const { plan, check } = setup();
    const base = plan("SELECT id FROM a");
    const param: ScalarExpr = { op: "param", slot: 0, type: INT4 };
    const root: Rel = {
      op: "project",
      id: 950,
      input: {
        op: "apply",
        id: 951,
        mode: "scalar",
        outer: base.root,
        inner: {
          op: "values",
          id: 952,
          rows: [
            [
              {
                op: "literal",
                type: INT4,
                value: base.root.op === "project" ? (base.root.exprs[0] as never) : (null as never),
              },
            ],
          ],
          output: [{ name: "x", type: INT4, nullable: true }],
        },
        bindings: [{ slot: 0, expr: { op: "column", index: 0, type: INT4 } }],
        quantified: null,
        memoize: true,
        output: [...base.root.output, { name: "x", type: INT4, nullable: true }],
      },
      exprs: [param],
      output: [{ name: "y", type: INT4, nullable: true }],
    };
    const outside: PhysicalPlan = {
      ...base,
      root,
      parameters: [INT4],
      columns: base.columns.slice(0, 1).map((column) => ({ ...column, name: "y" })),
    };
    expect(check(outside)).toThrow("slot 0 is read where nothing fills it");
  });

  it("rejects an init plan that reads itself", () => {
    const { plan, check } = setup();
    const cyclic = plan("SELECT (SELECT 1) AS x");
    const [init] = cyclic.initPlans;
    if (init === undefined) throw new Error("Expected an init plan");
    const type = init.plan.output[0]?.type ?? INT4;
    (init as Mutable<typeof init>).plan = {
      op: "values",
      id: 960,
      rows: [[{ op: "param", slot: init.slot, type }]],
      output: [{ name: "x", type, nullable: true }],
    };
    expect(check(cyclic)).toThrow("reads itself");
  });

  it("checks LIMIT / OFFSET bounds", () => {
    const { plan, check } = setup();
    const limited = plan("SELECT id FROM a LIMIT 1");
    const limit = all(limited.root).find((rel) => rel.op === "limit" || rel.op === "sort");
    if (limit === undefined) throw new Error("Expected a limit");
    (limit as unknown as { limit: unknown }).limit = {
      op: "column",
      index: 999,
      type: { kind: "int64" },
    };
    expect(check(limited)).toThrow("malformed bound");
    const negative = plan("SELECT id FROM a LIMIT 1");
    const node = all(negative.root).find((rel) => rel.op === "limit" || rel.op === "sort");
    (node as unknown as { limit: unknown }).limit = { kind: "const", value: -1 };
    expect(check(negative)).toThrow("malformed bound");
  });

  it("checks positions on both sides, aggregate FILTER and ORDER BY, and aggregate signatures", () => {
    const { plan, check } = setup();
    const windowed = plan("SELECT sum(id) OVER (PARTITION BY id) AS s FROM a");
    const window = all(windowed.root).find((rel) => rel.op === "window");
    (window as unknown as { partitionBy: number[] }).partitionBy = [-1];
    expect(check(windowed)).toThrow("partition key -1");
    const grouped = plan("SELECT v, count(*) FILTER (WHERE id > 1) AS n FROM a GROUP BY v");
    const aggregate = all(grouped.root).find((rel) => rel.op === "aggregate");
    if (aggregate?.op !== "aggregate") throw new Error("Expected an aggregate");
    const [call] = aggregate.aggregates;
    (call as unknown as { filter: ScalarExpr }).filter = { op: "column", index: 0, type: INT4 };
    expect(check(grouped)).toThrow("aggregate FILTER is not boolean");
    const keys = plan("SELECT v, count(*) AS n FROM a GROUP BY v");
    const keyed = all(keys.root).find((rel) => rel.op === "aggregate");
    (keyed as unknown as { groupBy: number[] }).groupBy = [-1];
    expect(check(keys)).toThrow("group key -1");
    const summed = plan("SELECT sum(id) AS s FROM a");
    const sum = all(summed.root).find((rel) => rel.op === "aggregate");
    if (sum?.op !== "aggregate") throw new Error("Expected an aggregate");
    (sum.aggregates[0] as unknown as { type: unknown }).type = INT4;
    (sum.output[0] as unknown as { type: unknown }).type = INT4;
    expect(check(summed)).toThrow("aggregate sum");
  });

  it("checks a recursion's widths", () => {
    const { plan, check } = setup();
    const recursive = plan(
      "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
    );
    const node = all(recursive.root).find((rel) => rel.op === "recursive");
    if (node?.op !== "recursive") throw new Error("Expected a recursion");
    (node as unknown as { output: unknown[] }).output = [...node.output, ...node.output];
    // The work-table scan, checked inside the step, sees the mismatch first.
    expect(check(recursive)).toThrow(/work table width|recursive width differs/u);
  });
});

describe("checkPlan checks every aggregate against ir/functions.ts AGGREGATES (review B6-p2b-3 finding 2)", () => {
  /** The review's plan: SUM's argument a lowered text literal, its result and outputs text. */
  function sumOfText(): { plan: PhysicalPlan; check: (plan: PhysicalPlan) => () => void } {
    const { plan, check } = setup();
    const text = plan("SELECT 'a' AS t");
    if (text.root.op !== "project") throw new Error("Expected a project");
    const literal = text.root.exprs[0];
    const summed = plan("SELECT sum(id) AS s FROM a");
    const retype = (value: unknown): void => {
      if (typeof value !== "object" || value === null) return;
      const node = value as Record<string, unknown>;
      if (node.kind === "int64") node.kind = "text";
      if (node.fn === "sum") node.args = [structuredClone(literal)];
      for (const child of Object.values(node)) retype(child);
    };
    retype(summed);
    return { plan: summed, check };
  }

  it("rejects SUM(text) → text, a coherent plan with no such overload", () => {
    const { plan, check } = sumOfText();
    expect(check(plan)).toThrow("aggregate sum(text): no such overload");
  });

  it("rejects every argument type an aggregate has no overload for, in GROUP BY and windows", () => {
    const { plan, check } = setup();
    /** A lowered expression of the type `sql` selects (a literal or a cast of one). */
    const expression = (sql: string): ScalarExpr => {
      const root = plan(sql).root;
      if (root.op !== "project" || root.exprs[0] === undefined) throw new Error(sql);
      return root.exprs[0];
    };
    const cases: ReadonlyArray<readonly [sql: string, arg: string, message: string]> = [
      ["SELECT avg(id) AS x FROM a", "SELECT true AS t", "aggregate avg(bool): no such overload"],
      [
        "SELECT min(id) AS x FROM a",
        "SELECT CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS uuid) AS t",
        "aggregate min(uuid): no such overload",
      ],
      ["SELECT max(id) AS x FROM a", "SELECT CAST('1' AS jsonb) AS t", "aggregate max(json)"],
      [
        "SELECT sum(id) OVER (ORDER BY id) AS x FROM a",
        "SELECT 'a' AS t",
        "window: aggregate sum(text): no such overload",
      ],
      [
        "SELECT avg(id) OVER (ORDER BY id) AS x FROM a",
        "SELECT DATE '2024-01-01' AS t",
        "window: aggregate avg(date): no such overload",
      ],
    ];
    for (const [sql, arg, message] of cases) {
      const lowered = plan(sql);
      for (const rel of all(lowered.root)) {
        const calls =
          rel.op === "aggregate" ? rel.aggregates : rel.op === "window" ? rel.functions : [];
        for (const call of calls)
          (call as { args: readonly ScalarExpr[] }).args = [expression(arg)];
      }
      expect(check(lowered), sql).toThrow(message);
    }
  });

  it("rejects a result type the overload does not give", () => {
    const { plan, check } = setup();
    // sum(int8) is numeric with scale 0 (R17.2): unconstrained numeric is another type.
    const summed = plan("SELECT sum(CAST(id AS bigint)) AS s FROM a");
    const aggregate = all(summed.root).find((rel) => rel.op === "aggregate");
    if (aggregate?.op !== "aggregate") throw new Error("Expected an aggregate");
    const decimal = { kind: "decimal", precision: null, scale: null } as const;
    (aggregate.aggregates[0] as { type: unknown }).type = decimal;
    for (const rel of all(summed.root))
      for (const column of rel.output)
        if (column.type.kind === "decimal") (column as { type: unknown }).type = decimal;
    expect(check(summed)).toThrow("aggregate sum(int64): returns decimal, not decimal");
  });
});
