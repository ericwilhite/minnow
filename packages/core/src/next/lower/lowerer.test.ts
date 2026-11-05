import { describe, expect, it } from "vitest";
import { UnsupportedPlanError } from "../errors.js";
import type { ExecutorCosts } from "../ir/costs.js";
import type { Rel, ScalarExpr } from "../ir/ir.js";
import { createLowerer } from "./create.js";
import { PlanningError } from "./errors.js";
import type { LoweredStatement, LoweringContext } from "./lowerer.js";
import { MemoryCatalog } from "./memory-catalog.js";

const COSTS: ExecutorCosts = {
  decodePerValue: 1,
  filterPerRow: 1,
  hashBuildPerRow: 20,
  hashProbePerRow: 15,
  aggregatePerRow: 5,
  sortPerRowLog: 3,
  emitPerValue: 2,
  spillBytesPerSecond: 600e6,
};

const lowerer = createLowerer();

/** A catalog built by lowering DDL and applying it, as the owner will. */
function database(...ddl: string[]) {
  const catalog = new MemoryCatalog();
  const context = (): LoweringContext => ({
    catalog: catalog.view,
    statistics: { table: () => ({ rowCount: 10_000, column: () => null }) },
    costs: COSTS,
  });
  const run = (sql: string): LoweredStatement => lowerer.lower(sql, context());
  for (const statement of ddl) {
    const lowered = run(statement);
    if (lowered.kind !== "ddl" || lowered.change === null)
      throw new Error(`Expected DDL: ${statement}`);
    catalog.apply(lowered.change);
  }
  return { catalog, run, context };
}

const RETAIL = [
  "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(40) NOT NULL, region TEXT, tier SMALLINT DEFAULT 1, joined TIMESTAMP)",
  "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers (id), amount NUMERIC(10, 2), qty INTEGER, ratio REAL, total DOUBLE PRECISION GENERATED ALWAYS AS (qty * 2.5) STORED, note TEXT, UNIQUE (customer_id, note), CHECK (qty >= 0))",
];

function query(statement: LoweredStatement) {
  if (statement.kind !== "query") throw new Error(`Expected a query, got ${statement.kind}`);
  return statement;
}

function modify(statement: LoweredStatement) {
  if (statement.kind !== "modify") throw new Error(`Expected DML, got ${statement.kind}`);
  return statement;
}

/** Operators from the root down the first-input chain. */
function chain(rel: Rel): string[] {
  const ops: string[] = [rel.op];
  let current: Rel | undefined = rel;
  for (;;) {
    current = "input" in current ? (current as { input: Rel }).input : undefined;
    if (current === undefined) return ops;
    ops.push(current.op);
  }
}

function find<K extends Rel["op"]>(rel: Rel, op: K): Extract<Rel, { op: K }> {
  let current: Rel | undefined = rel;
  while (current !== undefined) {
    if (current.op === op) return current as Extract<Rel, { op: K }>;
    current = "input" in current ? (current as { input: Rel }).input : undefined;
  }
  throw new Error(`No ${op} in the plan`);
}

const typeOf = (expr: ScalarExpr) => expr.type;

describe("DDL", () => {
  it("creates tables with exact declared types, keys, constraints and rules", () => {
    const { catalog } = database(...RETAIL);
    const customers = catalog.view.tableByName("customers");
    const orders = catalog.view.tableByName("orders");
    expect(customers?.columns.map((column) => [column.name, column.type, column.nullable])).toEqual(
      [
        ["id", { kind: "int32", bits: 32 }, false],
        ["name", { kind: "text", maxLength: 40 }, false],
        ["region", { kind: "text" }, true],
        ["tier", { kind: "int32", bits: 16 }, true],
        ["joined", { kind: "timestamp", zoned: false }, true],
      ],
    );
    expect(customers?.columns[3]?.defaultSql).toBe("1");
    expect(orders?.columns.map((column) => column.type.kind)).toEqual([
      "int64",
      "int32",
      "decimal",
      "int32",
      "float32",
      "float64",
      "text",
    ]);
    expect(orders?.columns[5]?.generatedSql).toBe("qty * 2.5");
    expect(orders?.indexes.map((index) => [index.role, index.columnIds.length])).toEqual([
      ["primary", 1],
      ["unique", 2],
    ]);
    expect(orders?.foreignKeys[0]?.parentTableId).toBe(customers?.id);
    expect(orders?.checks[0]?.conditionSql).toBe("qty >= 0");
  });

  it("decides IF [NOT] EXISTS at the catalog version and refuses a clash", () => {
    const { run } = database(...RETAIL);
    expect(run("CREATE TABLE IF NOT EXISTS customers (id INT)")).toMatchObject({
      kind: "ddl",
      change: null,
    });
    expect(run("DROP TABLE IF EXISTS nothing")).toMatchObject({ kind: "ddl", change: null });
    expect(() => run("CREATE TABLE customers (id INT)")).toThrow(PlanningError);
  });

  it("type-checks a default at CREATE time", () => {
    const { run } = database();
    expect(() => run("CREATE TABLE t (a INTEGER DEFAULT 'abc')")).toThrow(
      "invalid input syntax for type int4",
    );
    expect(
      run("CREATE TABLE t (a INTEGER DEFAULT 7, b TEXT DEFAULT 'x', c TIMESTAMPTZ DEFAULT now())"),
    ).toMatchObject({
      kind: "ddl",
    });
  });

  it("refuses what the write path cannot declare yet (R17.7)", () => {
    const { run } = database();
    expect(() => run("CREATE TABLE t (id SERIAL PRIMARY KEY)")).toThrow(UnsupportedPlanError);
    expect(() => run("CREATE TYPE mood AS ENUM ('sad', 'ok')")).toThrow(UnsupportedPlanError);
  });
});

describe("SELECT", () => {
  it("reads one row by primary key as a point read with a typed placeholder", () => {
    const { run } = database(...RETAIL);
    const lowered = query(run("SELECT name, tier FROM customers WHERE id = $1"));
    expect(lowered.pointRead).toBe(true);
    expect(chain(lowered.plan.root)).toEqual(["project", "keyLookup"]);
    const lookup = find(lowered.plan.root, "keyLookup");
    expect(lookup.keys).toEqual([[{ op: "param", slot: 0, type: { kind: "int32", bits: 32 } }]]);
    expect(lowered.parameters).toEqual([{ kind: "int32", bits: 32 }]);
    expect(lowered.columns.map((column) => [column.name, column.sqlType, column.nullable])).toEqual(
      [
        ["name", "character varying(40)", false],
        ["tier", "smallint", true],
      ],
    );
  });

  it("looks up several keys from an IN list, and keeps other conjuncts as a filter", () => {
    const { run } = database(...RETAIL);
    const lowered = query(
      run("SELECT id FROM customers WHERE id IN (1, 2, 3) AND region = 'west'"),
    );
    expect(chain(lowered.plan.root)).toEqual(["project", "filter", "keyLookup"]);
    expect(find(lowered.plan.root, "keyLookup").keys).toHaveLength(3);
  });

  it("scans when a key constant cannot be of the column's type", () => {
    const { run } = database(...RETAIL);
    const lowered = query(run("SELECT id FROM customers WHERE id = 5000000000"));
    expect(lowered.pointRead).toBe(false);
    const scan = find(lowered.plan.root, "scan");
    // int4 = int8: the column is widened, never the constant narrowed.
    expect(scan.filter).toMatchObject({
      op: "compare",
      left: { op: "cast", type: { kind: "int64" } },
    });
  });

  it("reads only the referenced columns, filters in the scan, sorts and limits", () => {
    const { run, catalog } = database(...RETAIL);
    const lowered = query(
      run(
        "SELECT name FROM customers WHERE region LIKE 'w%' ORDER BY joined DESC LIMIT 10 OFFSET $1",
      ),
    );
    expect(chain(lowered.plan.root)).toEqual(["project", "sort", "project", "scan"]);
    const scan = find(lowered.plan.root, "scan");
    const customers = catalog.view.tableByName("customers");
    const name = (id: number) => customers?.columns.find((column) => column.id === id)?.name;
    expect(scan.columns.map(name)).toEqual(["region", "name", "joined"]);
    expect(scan.filter).toMatchObject({ op: "like", mode: "like" });
    const sort = find(lowered.plan.root, "sort");
    expect(sort.keys[0]).toMatchObject({ direction: "desc", nulls: "first" });
    expect([sort.limit, sort.offset]).toEqual([
      { kind: "const", value: 10 },
      { kind: "param", slot: 0 },
    ]);
    expect(lowered.parameters).toEqual([{ kind: "int64" }]);
    expect(lowered.columns.map((column) => column.name)).toEqual(["name"]);
  });

  it("groups, aggregates and renders int8 by ruling R17.1", () => {
    const { run } = database(...RETAIL);
    const lowered = query(
      run(
        "SELECT customer_id, count(*) AS n, count(*) + 1 AS n1, sum(qty) AS q, sum(id) AS s, CAST(qty AS BIGINT) AS b, avg(qty) AS a, sum(amount) AS m FROM orders GROUP BY customer_id, qty HAVING count(*) > 1 ORDER BY 2 DESC",
      ),
    );
    expect(chain(lowered.plan.root)).toEqual([
      "sort",
      "project",
      "filter",
      "aggregate",
      "project",
      "scan",
    ]);
    expect(lowered.columns.map((column) => [column.name, column.sqlType, column.render])).toEqual([
      ["customer_id", "integer", null],
      ["n", "bigint", "number"],
      ["n1", "bigint", "number"],
      ["q", "bigint", "number"],
      ["s", "numeric", "bigint"],
      ["b", "bigint", "bigint"],
      ["a", "double precision", null],
      ["m", "numeric", null],
    ]);
    const aggregate = find(lowered.plan.root, "aggregate");
    expect(aggregate.aggregates.map((call) => call.fn)).toEqual([
      "count_star",
      "sum",
      "sum",
      "avg",
      "sum",
    ]);
  });

  it("refuses a column that is neither grouped nor aggregated", () => {
    const { run } = database(...RETAIL);
    expect(() => run("SELECT region, count(*) FROM customers GROUP BY tier")).toThrow(/GROUP BY/u);
  });

  it("types constants by context and expands wildcards", () => {
    const { run } = database(...RETAIL);
    const lowered = query(
      run("SELECT * FROM customers WHERE joined > '2024-01-01' AND tier = '2'"),
    );
    expect(lowered.columns.map((column) => column.name)).toEqual([
      "id",
      "name",
      "region",
      "tier",
      "joined",
    ]);
    const scan = find(lowered.plan.root, "scan");
    const filter = scan.filter;
    if (filter?.op !== "and") throw new Error("Expected AND");
    expect(filter.args[0]).toMatchObject({
      right: { op: "cast", type: { kind: "timestamp", zoned: false } },
    });
    expect(typeOf((filter.args[1] as { right: ScalarExpr }).right)).toEqual({
      kind: "int32",
      bits: 16,
    });
  });

  it("uses the statement clock slot after the placeholders", () => {
    const { run } = database(...RETAIL);
    const lowered = query(
      run("SELECT id FROM customers WHERE joined < CURRENT_TIMESTAMP AND id > $1"),
    );
    expect(lowered.plan.clockSlot).toBe(1);
    expect(lowered.plan.parameters).toEqual([
      { kind: "int32", bits: 32 },
      { kind: "timestamp", zoned: true },
    ]);
    expect(lowered.parameters).toHaveLength(1);
  });

  it("selects without FROM", () => {
    const { run } = database();
    const lowered = query(run("SELECT 1 AS one, 'x' AS t, 2.50 AS d"));
    expect(lowered.columns.map((column) => column.sqlType)).toEqual(["integer", "text", "numeric"]);
    expect(chain(lowered.plan.root)).toEqual(["project", "values"]);
  });

  it("refuses the milestone M3 surface with a construct key", () => {
    const { run } = database(...RETAIL);
    const construct = (sql: string): string | null => {
      try {
        run(sql);
      } catch (error) {
        if (error instanceof UnsupportedPlanError) return error.construct;
        throw error;
      }
      return null;
    };
    expect(construct("SELECT * FROM customers c JOIN orders o ON o.customer_id = c.id")).toBe(
      "join",
    );
    expect(construct("SELECT (SELECT max(id) FROM orders) FROM customers")).toBe("subquery");
    expect(construct("SELECT row_number() OVER (ORDER BY id) FROM customers")).toBe("window");
    expect(construct("SELECT id FROM customers UNION SELECT id FROM orders")).toBe("setOperation");
    expect(
      construct(
        "MERGE INTO customers c USING orders o ON c.id = o.customer_id WHEN MATCHED THEN DELETE",
      ),
    ).toBe("merge");
  });
});

describe("DML", () => {
  it("inserts complete rows: defaults, generated columns and assignment casts", () => {
    const { run, catalog } = database(...RETAIL);
    const lowered = modify(
      run(
        "INSERT INTO orders (id, customer_id, qty, amount) VALUES (1, $1, 3, 9.99), (2, 7, DEFAULT, NULL) RETURNING id, total",
      ),
    );
    const root = lowered.plan.root;
    if (root.op !== "modify") throw new Error("Expected modify");
    expect([root.kind, root.onConflict]).toEqual(["insert", null]);
    expect(chain(root)).toEqual(["modify", "project", "project", "values"]);
    const orders = catalog.view.tableByName("orders");
    expect(root.input.output.map((column) => column.name)).toEqual(
      orders?.columns.map((column) => column.name),
    );
    expect(root.input.output.map((column) => column.type)).toEqual(
      orders?.columns.map((column) => column.type),
    );
    const values = find(root, "values");
    expect(values.rows[0]?.[3]).toMatchObject({
      op: "cast",
      assignment: true,
      type: { kind: "decimal", precision: 10, scale: 2 },
    });
    expect(lowered.parameters).toEqual([{ kind: "int32", bits: 32 }]);
    expect(lowered.columns.map((column) => [column.name, column.render])).toEqual([
      ["id", "bigint"],
      ["total", null],
    ]);
    expect(() => run("INSERT INTO orders (id, customer_id, total) VALUES (1, 1, 2)")).toThrow(
      /non-DEFAULT/u,
    );
  });

  it("resolves ON CONFLICT arbiters and EXCLUDED", () => {
    const { run, catalog } = database(...RETAIL);
    const orders = catalog.view.tableByName("orders");
    const unique = orders?.indexes.find((index) => index.role === "unique");
    const lowered = modify(
      run(
        "INSERT INTO orders (id, customer_id, note, qty) VALUES (1, 2, 'n', 4) ON CONFLICT (note, customer_id) DO UPDATE SET qty = orders.qty + EXCLUDED.qty",
      ),
    );
    const root = lowered.plan.root;
    if (root.op !== "modify" || root.onConflict === null) throw new Error("Expected an upsert");
    expect(root.kind).toBe("upsert");
    expect(root.onConflict.arbiters).toEqual([unique?.id]);
    expect(root.onConflict.indexId).toBe(unique?.id);
    const qty = orders?.columns.find((column) => column.name === "qty");
    const total = orders?.columns.find((column) => column.name === "total");
    // qty is reassigned, and the generated total that reads it is recomputed.
    expect(root.onConflict.assignments.map((assignment) => assignment.column)).toEqual([
      qty?.id,
      total?.id,
    ]);
    expect(root.onConflict.assignments[0]?.expr).toMatchObject({
      op: "call",
      fn: "add",
      args: [
        { op: "column", index: 3 },
        { op: "column", index: 7 + 3 },
      ],
    });
    expect(() =>
      run("INSERT INTO orders (id, customer_id) VALUES (1, 2) ON CONFLICT (qty) DO NOTHING"),
    ).toThrow(/no unique or exclusion constraint/u);
    const any = modify(
      run("INSERT INTO orders (id, customer_id) VALUES (1, 2) ON CONFLICT DO NOTHING"),
    ).plan.root;
    expect(any.op === "modify" ? any.onConflict : null).toMatchObject({
      arbiters: [],
      indexId: -1,
      action: "nothing",
    });
  });

  it("updates by key with the locator and the generated columns the assignment changes", () => {
    const { run, catalog } = database(...RETAIL);
    const lowered = modify(
      run("UPDATE orders SET qty = qty + 1, note = DEFAULT WHERE id = $1 RETURNING total"),
    );
    const root = lowered.plan.root;
    if (root.op !== "modify") throw new Error("Expected modify");
    const orders = catalog.view.tableByName("orders");
    const id = (name: string) => orders?.columns.find((column) => column.name === name)?.id;
    expect(root.kind).toBe("update");
    expect(root.assigned).toEqual([id("qty"), id("total"), id("note")]);
    expect(chain(root)).toEqual(["modify", "project", "keyLookup"]);
    const lookup = find(root, "keyLookup");
    expect(lookup.emitLocator).toBe(true);
    expect(lookup.output.at(-1)?.type).toEqual({ kind: "locator", tableId: orders?.id });
    expect(lowered.parameters).toEqual([{ kind: "int64" }]);
    expect(() => run("UPDATE orders SET total = 1")).toThrow(/only be updated to DEFAULT/u);
  });

  it("deletes by scan with the locator", () => {
    const { run } = database(...RETAIL);
    const root = modify(run("DELETE FROM customers WHERE region = 'east'")).plan.root;
    expect(chain(root)).toEqual(["modify", "project", "scan"]);
    expect(find(root, "scan").emitLocator).toBe(true);
    expect(chain(modify(run("TRUNCATE customers")).plan.root)).toEqual([
      "modify",
      "project",
      "scan",
    ]);
  });
});

describe("other statements", () => {
  it("lowers CREATE TABLE AS to a table and its INSERT … SELECT", () => {
    const { run } = database(...RETAIL);
    const lowered = run("CREATE TABLE big AS SELECT id, amount FROM orders WHERE qty > 2");
    if (lowered.kind !== "ddl" || lowered.change?.kind !== "createTable")
      throw new Error("Expected CTAS");
    expect(lowered.change.table.columns.map((column) => [column.name, column.type.kind])).toEqual([
      ["id", "int64"],
      ["amount", "decimal"],
    ]);
    expect(lowered.followUp).toMatch(/^INSERT INTO "big" \("id", "amount"\) SELECT/u);
  });

  it("classifies transaction control and answers SHOW", () => {
    const { run } = database();
    expect(run("SAVEPOINT a")).toMatchObject({
      kind: "transactionControl",
      action: "savepoint",
      savepoint: "a",
      controlsTransaction: true,
    });
    expect(run("SHOW server_version")).toMatchObject({ kind: "utility", show: { value: "16.0" } });
  });
});
