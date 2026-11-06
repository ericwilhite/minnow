import { describe, expect, it } from "vitest";
import type { ExecutorCosts } from "../ir/costs.js";
import type { Rel } from "../ir/ir.js";
import { createLowerer } from "./create.js";
import type { LoweringContext } from "./lowerer.js";
import { MemoryCatalog } from "./memory-catalog.js";
import type { ColumnPlanningStatistics, PlanningStatistics } from "./statistics.js";

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

/** A catalog plus statistics by table name: row counts and per-column bounds. */
function setup(
  ddl: readonly string[],
  rows: Record<string, number>,
  columns: Record<string, Record<string, Partial<ColumnPlanningStatistics>>> = {},
) {
  const catalog = new MemoryCatalog();
  const statistics: PlanningStatistics = {
    table: (tableId) => {
      const table = catalog.view.table(tableId);
      if (table === undefined) return null;
      return {
        rowCount: rows[table.name] ?? 1_000,
        column: (columnId) => {
          const name = table.columns.find((column) => column.id === columnId)?.name ?? "";
          const stats = columns[table.name]?.[name];
          return stats === undefined
            ? null
            : { nullCount: 0, distinct: null, min: null, max: null, averageWidth: 8, ...stats };
        },
      };
    },
  };
  const context = (): LoweringContext => ({ catalog: catalog.view, statistics, costs: COSTS });
  for (const sql of ddl) {
    const lowered = lowerer.lower(sql, context());
    if (lowered.kind !== "ddl" || lowered.change === null) throw new Error(sql);
    catalog.apply(lowered.change);
  }
  const plan = (sql: string): Rel => {
    const lowered = lowerer.lower(sql, context());
    if (lowered.kind !== "query") throw new Error(`Expected a query: ${sql}`);
    return lowered.plan.root;
  };
  const tableId = (name: string) => catalog.view.tableByName(name)?.id ?? -1;
  const basis = (sql: string) => {
    const lowered = lowerer.lower(sql, context());
    if (lowered.kind !== "query" && lowered.kind !== "modify") throw new Error(sql);
    return lowered.basis;
  };
  return { plan, tableId, basis };
}

function all(rel: Rel): Rel[] {
  const children: Rel[] = [];
  for (const value of Object.values(rel))
    if (typeof value === "object" && value !== null && "op" in value && "output" in value)
      children.push(value as Rel);
  if (rel.op === "setOp") children.push(...rel.inputs);
  return [rel, ...children.flatMap(all)];
}

/** The tables a relation scans. */
function tablesUnder(rel: Rel): number[] {
  return all(rel).flatMap((node) =>
    node.op === "scan" || node.op === "keyLookup" || node.op === "indexRange" ? [node.tableId] : [],
  );
}

const STAR = [
  "CREATE TABLE sales (id integer PRIMARY KEY, store integer, item integer, day integer, amount double precision)",
  "CREATE TABLE stores (id integer PRIMARY KEY, city text)",
  "CREATE TABLE items (id integer PRIMARY KEY, title text)",
];

describe("join order and build sides (design-joins-agg-spill.md)", () => {
  it("builds hash tables on the small dimensions of a star", () => {
    const { plan, tableId } = setup(STAR, { sales: 1_000_000, stores: 50, items: 2_000 });
    const root = plan(
      "SELECT s.amount, st.city, i.title FROM sales s JOIN stores st ON st.id = s.store JOIN items i ON i.id = s.item",
    );
    const joins = all(root).filter(
      (node): node is Extract<Rel, { op: "join" }> => node.op === "join",
    );
    expect(joins).toHaveLength(2);
    for (const join of joins) {
      expect(join.algorithm).toBe("hash");
      // The fact table is never on the build side.
      expect(tablesUnder(join.right)).not.toContain(tableId("sales"));
    }
  });

  it("estimates a foreign key joined to a key as the child's rows (L × R / R_parent)", () => {
    // sales ⋈ stores keeps ~1M rows; were it estimated below items' 400k, sales would become a
    // build side.
    const { plan, tableId } = setup(STAR, { sales: 1_000_000, stores: 50, items: 400_000 });
    const root = plan(
      "SELECT s.amount FROM sales s JOIN stores st ON st.id = s.store JOIN items i ON i.id = s.item",
    );
    for (const join of all(root).filter(
      (node): node is Extract<Rel, { op: "join" }> => node.op === "join",
    ))
      expect(tablesUnder(join.right)).not.toContain(tableId("sales"));
  });

  // 40 relations: past the exhaustive limit, and past 32 so unit sets cannot be 32-bit masks.
  it.each([10, 40])("orders a %i-relation chain greedily, without cross products", (length) => {
    const tables = Array.from(
      { length },
      (_, index) => `CREATE TABLE t${String(index)} (id integer PRIMARY KEY, next integer)`,
    );
    // Tiny tables at both ends: a cross product of the two looks cheap, but the chain connects.
    const { plan, tableId } = setup(tables, { t0: 2, [`t${String(length - 1)}`]: 2 });
    const joins = Array.from(
      { length: length - 1 },
      (_, index) =>
        `JOIN t${String(index + 1)} ON t${String(index + 1)}.id = t${String(index)}.next`,
    ).join(" ");
    const root = plan(`SELECT t0.id FROM t0 ${joins}`);
    const nodes = all(root).filter(
      (node): node is Extract<Rel, { op: "join" }> => node.op === "join",
    );
    expect(nodes).toHaveLength(length - 1);
    expect(nodes.every((node) => node.algorithm === "hash" && node.kind === "inner")).toBe(true);
    expect(tablesUnder(root).sort((a, b) => a - b)).toEqual(
      tables.map((_, index) => tableId(`t${String(index)}`)).sort((a, b) => a - b),
    );
  });

  it("orders by estimated intermediate rows: a key join before a join on defaults", () => {
    // b ⋈ a on a's key keeps b's 1000 rows; b ⋈ c on columns without statistics keeps
    // 1000 × 1000 / 200. Without selectivities every pair ties and FROM order (b, c) wins.
    const { plan, tableId } = setup(
      [
        "CREATE TABLE a (id integer PRIMARY KEY, v integer)",
        "CREATE TABLE b (id integer PRIMARY KEY, x integer, y integer)",
        "CREATE TABLE c (id integer PRIMARY KEY, y integer)",
      ],
      {},
    );
    const root = plan("SELECT b.id FROM b JOIN c ON b.y = c.y JOIN a ON a.id = b.x");
    const deepest = all(root).find(
      (node): node is Extract<Rel, { op: "join" }> =>
        node.op === "join" &&
        all(node.left).every((child) => child.op !== "join") &&
        all(node.right).every((child) => child.op !== "join"),
    );
    expect(tablesUnder(deepest as Rel).sort()).toEqual([tableId("a"), tableId("b")].sort());
  });

  it("keeps a chain's estimate at its rows past the exhaustive limit (build sides)", () => {
    // Each step joins a foreign key to a key, so the running estimate stays ~1000 rows and the
    // million-row table in the middle is never a build side.
    const tables = Array.from(
      { length: 10 },
      (_, index) => `CREATE TABLE t${String(index)} (id integer PRIMARY KEY, next integer)`,
    );
    const { plan, tableId } = setup(tables, { t5: 1_000_000 });
    const joins = Array.from(
      { length: 9 },
      (_, index) =>
        `JOIN t${String(index + 1)} ON t${String(index + 1)}.id = t${String(index)}.next`,
    ).join(" ");
    const root = plan(`SELECT t0.id FROM t0 ${joins}`);
    for (const join of all(root).filter(
      (node): node is Extract<Rel, { op: "join" }> => node.op === "join",
    ))
      expect(tablesUnder(join.right)).not.toContain(tableId("t5"));
  });

  it("keeps a LEFT join's place and never pushes WHERE into its null-supplied side", () => {
    const { plan, tableId } = setup(STAR, { sales: 1_000_000, stores: 50 });
    const root = plan(
      "SELECT st.city, s.amount FROM stores st LEFT JOIN sales s ON s.store = st.id WHERE s.amount > 10",
    );
    const left = all(root).find((node): node is Extract<Rel, { op: "join" }> => node.op === "join");
    expect(left?.kind).toBe("left");
    if (left === undefined) throw new Error("no join");
    expect(tablesUnder(left.left)).toEqual([tableId("stores")]);
    const salesScan = all(left.right).find((node) => node.op === "scan");
    expect(salesScan?.op === "scan" ? salesScan.filter : "missing").toBeNull();
    // The WHERE is a filter above the join (rows with a NULL-extended amount fail it).
    expect(all(root).some((node) => node.op === "filter" && node.input === left)).toBe(true);
  });

  it("pushes a LEFT join's own ON condition on its null-supplied side into that side", () => {
    const { plan } = setup(STAR, { sales: 1_000_000, stores: 50 });
    const root = plan(
      "SELECT st.city, s.amount FROM stores st LEFT JOIN sales s ON s.store = st.id AND s.amount > 10",
    );
    const left = all(root).find((node): node is Extract<Rel, { op: "join" }> => node.op === "join");
    if (left === undefined) throw new Error("no join");
    const salesScan = all(left.right).find((node) => node.op === "scan");
    expect(salesScan?.op === "scan" ? salesScan.filter : null).toMatchObject({
      op: "compare",
      cmp: ">",
    });
  });
});

describe("index ranges (ir.ts indexRange, lower/access-path.ts)", () => {
  const DDL = [
    "CREATE TABLE events (id integer PRIMARY KEY, day integer, kind text)",
    "CREATE INDEX events_day ON events (day)",
  ];
  const ranges = (root: Rel) => all(root).filter((node) => node.op === "indexRange");

  it("reads a narrow range through the index and a wide one by scan", () => {
    const { plan } = setup(
      DDL,
      { events: 1_000_000 },
      { events: { day: { min: 0, max: 10_000 } } },
    );
    expect(ranges(plan("SELECT id FROM events WHERE day BETWEEN 10 AND 20"))).toHaveLength(1);
    expect(ranges(plan("SELECT id FROM events WHERE day > 100"))).toHaveLength(0);
    expect(ranges(plan("SELECT id FROM events WHERE day = 7"))).toHaveLength(1);
  });

  it("uses PostgreSQL's defaults without statistics, and never a text range under COLLATE", () => {
    const { plan } = setup([...DDL, "CREATE INDEX events_kind ON events (kind)"], {
      events: 1_000_000,
    });
    expect(ranges(plan("SELECT id FROM events WHERE day >= 3 AND day <= 9"))).toHaveLength(1);
    expect(ranges(plan("SELECT id FROM events WHERE day > 3"))).toHaveLength(0);
    expect(ranges(plan("SELECT id FROM events WHERE kind = 'a'"))).toHaveLength(1);
    expect(
      ranges(
        plan(
          `SELECT id FROM events WHERE kind COLLATE "en-US" > 'a' AND kind COLLATE "en-US" < 'b'`,
        ),
      ),
    ).toHaveLength(0);
    const [range] = ranges(plan("SELECT id FROM events WHERE day >= 3 AND day < 9"));
    expect(
      range?.op === "indexRange" ? [range.lower?.inclusive, range.upper?.inclusive] : null,
    ).toEqual([true, false]);
  });
});

describe("statistics basis (lowerer.ts LoweredPlanned.basis)", () => {
  const DDL = [...STAR, "CREATE INDEX sales_day ON sales (day)"];

  it("is empty when no decision about the plan's shape read statistics", () => {
    const { basis } = setup(DDL, { sales: 1_000_000, stores: 50 });
    expect(basis("SELECT amount FROM sales WHERE id = 7")).toEqual([]);
    expect(basis("SELECT amount FROM sales WHERE id IN (1, 2)")).toEqual([]);
    expect(basis("SELECT count(*) AS n FROM sales WHERE store = 3")).toEqual([]);
    expect(basis("DELETE FROM stores WHERE id = 3")).toEqual([]);
  });

  it("names the tables whose counts chose the shape", () => {
    const { basis, tableId } = setup(DDL, { sales: 1_000_000, stores: 50 });
    // A join order and build side.
    expect(
      basis("SELECT s.amount FROM sales s JOIN stores st ON st.id = s.store")
        .map((entry) => entry.tableId)
        .sort(),
    ).toEqual([tableId("sales"), tableId("stores")].sort());
    // An index range costed against the scan.
    expect(basis("SELECT id FROM sales WHERE day BETWEEN 3 AND 9")).toEqual([
      { tableId: tableId("sales"), rowCount: 1_000_000 },
    ]);
  });
});
