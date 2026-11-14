/**
 * How the owner (B5) calls lowering: a plan cache keyed by (text, catalog version) with the
 * statistics check on each hit, CREATE TABLE AS through `followUp`, and table rules cached per
 * (table id, catalog version) for the executor's `compileTableRules`. The lowerer is a stub; the
 * calling pattern is the contract.
 */
import { describe, expect, it } from "vitest";
import type { CatalogView, TableDef } from "../access/catalog.js";
import type { ExecutorCosts } from "../ir/costs.js";
import type { PhysicalPlan } from "../ir/ir.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import type { LoweredStatement, Lowerer, LoweringContext } from "./lowerer.js";
import type { NotParameterizable, ParameterizedText } from "./parameterize.js";
import { resultColumn } from "./result.js";
import { statisticsDrifted, type PlanningStatistics } from "./statistics.js";

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

function catalogAt(version: number): CatalogView {
  return {
    version,
    tables: [],
    sequences: [],
    views: [],
    table: () => undefined,
    tableByName: () => undefined,
    sequence: () => undefined,
    types: [],
    fts: [],
    type: () => undefined,
    typeByName: () => undefined,
  };
}

const countPlan = (version: number): PhysicalPlan => {
  const output = [{ name: "n", type: { kind: "int64" as const }, nullable: false }];
  return {
    root: {
      op: "aggregate",
      id: 2,
      output,
      input: {
        op: "scan",
        id: 1,
        output: [],
        tableId: 7,
        columns: [],
        filter: null,
        runtimeFilters: [],
        emitLocator: false,
      },
      groupBy: [],
      groupingSets: null,
      aggregates: [
        {
          fn: "count_star",
          args: [],
          distinct: false,
          filter: null,
          orderBy: [],
          type: { kind: "int64" },
        },
      ],
    },
    columns: output,
    parameters: [],
    placeholderCount: 0,
    clockSlot: null,
    initPlans: [],
    catalogVersion: version,
    volatile: false,
  };
};

/** A stub: counts calls, and knows two statements. */
class StubLowerer implements Lowerer {
  calls = 0;
  lower(text: string, context: LoweringContext): LoweredStatement {
    this.calls += 1;
    const base = {
      rowLocking: "none" as const,
      text,
      catalogVersion: context.catalog.version,
      controlsTransaction: false,
      parameters: [],
    };
    if (text.startsWith("CREATE TABLE copy AS"))
      return {
        ...base,
        kind: "ddl",
        command: "SELECT",
        change: null,
        followUp: "INSERT INTO copy (n) SELECT COUNT(*) FROM t",
      };
    const plan = countPlan(context.catalog.version);
    const rows = context.statistics.table(7)?.rowCount ?? 1_000;
    return {
      ...base,
      kind: "query",
      command: "SELECT",
      plan,
      columns: plan.columns.map((column) => resultColumn(column, false)),
      reads: [7],
      basis: [{ tableId: 7, rowCount: rows }],
      pointRead: false,
    };
  }
  lowerWrite(): never {
    throw new Error("not used here");
  }
  parameterize(): null {
    return null;
  }
  lowerParameterized(parameterized: ParameterizedText): NotParameterizable {
    return { kind: "notParameterizable", key: parameterized.key };
  }
  lowerSchemaTable(): never {
    throw new Error("not used here");
  }
  readonly rulesLowered: string[] = [];
  lowerTableRules(table: TableDef, catalog: CatalogView): TableRuleExpressions {
    this.calls += 1;
    this.rulesLowered.push(`${String(table.id)}@${String(catalog.version)}`);
    return { checks: [], generated: [], defaults: [] };
  }
}

/** The owner's plan cache, as B5 would keep it. */
class PlanCache {
  readonly #plans = new Map<string, LoweredStatement>();
  constructor(readonly lowerer: Lowerer) {}
  plan(text: string, context: LoweringContext): LoweredStatement {
    const key = `${String(context.catalog.version)}\u0000${text}`;
    const cached = this.#plans.get(key);
    if (
      cached !== undefined &&
      !("basis" in cached && statisticsDrifted(cached.basis, context.statistics))
    )
      return cached;
    const lowered = this.lowerer.lower(text, context);
    this.#plans.set(key, lowered);
    return lowered;
  }
}

const statistics = (rowCount: number): PlanningStatistics => ({
  table: () => ({ rowCount, column: () => null }),
});

describe("the owner's use of Lowerer", () => {
  it("caches by (text, catalog version) and re-lowers when the data moved", () => {
    const lowerer = new StubLowerer();
    const cache = new PlanCache(lowerer);
    const context = { catalog: catalogAt(3), statistics: statistics(10_000), costs: COSTS };
    const first = cache.plan("SELECT COUNT(*) FROM t", context);
    expect(
      cache.plan("SELECT COUNT(*) FROM t", { ...context, statistics: statistics(15_000) }),
    ).toBe(first);
    expect(lowerer.calls).toBe(1);
    // The table quadrupled: costed join orders may be wrong now.
    cache.plan("SELECT COUNT(*) FROM t", { ...context, statistics: statistics(40_000) });
    expect(lowerer.calls).toBe(2);
    // A DDL moved the catalog: a different key.
    cache.plan("SELECT COUNT(*) FROM t", { ...context, catalog: catalogAt(4) });
    expect(lowerer.calls).toBe(3);
    if (first.kind !== "query") throw new Error("Expected a query");
    expect(first.columns[0]).toMatchObject({ sqlType: "bigint", render: "number" });
  });

  it("runs CREATE TABLE AS as its change, then its follow-up at the new catalog version", () => {
    const lowerer = new StubLowerer();
    let catalog = catalogAt(5);
    const executed: string[] = [];
    let text: string | null = "CREATE TABLE copy AS SELECT COUNT(*) AS n FROM t";
    while (text !== null) {
      const lowered = lowerer.lower(text, { catalog, statistics: statistics(10), costs: COSTS });
      executed.push(`${lowered.command}@${String(lowered.catalogVersion)}`);
      if (lowered.kind === "ddl") {
        // statement.changeCatalog(lowered.change) inside the open statement, then:
        catalog = catalogAt(catalog.version + 1);
        text = lowered.followUp;
      } else text = null;
    }
    expect(executed).toEqual(["SELECT@5", "SELECT@6"]);
  });

  it("caches table rules per (table id, catalog version)", () => {
    const lowerer = new StubLowerer();
    const rules = new Map<string, TableRuleExpressions>();
    const table = { id: 9 } as TableDef;
    const rulesFor = (catalog: CatalogView) => {
      const key = `${String(table.id)}@${String(catalog.version)}`;
      let entry = rules.get(key);
      if (entry === undefined) {
        entry = lowerer.lowerTableRules(table, catalog);
        rules.set(key, entry);
        // compileTableRules(table, catalog.version, entry) (exec/rules.ts) is cached beside it.
      }
      return entry;
    };
    rulesFor(catalogAt(1));
    rulesFor(catalogAt(1));
    rulesFor(catalogAt(2));
    expect(lowerer.rulesLowered).toEqual(["9@1", "9@2"]);
  });
});
