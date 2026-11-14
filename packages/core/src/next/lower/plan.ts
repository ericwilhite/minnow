import type { RowLocking } from "../../plan/model.js";
/**
 * Building blocks every lowering module shares: the per-statement `PlanBuilder`, lowered
 * relations with their result-descriptor columns, the lazily-collected `TableRead`, scopes over
 * relations, and column remapping for expressions typed against a virtual layout (lower/from.ts).
 */
import type { CatalogView, ColumnDef, TableDef } from "../access/catalog.js";
import type { ExecutorCosts } from "../ir/costs.js";
import type { OutputColumn, Rel, ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { ParameterTypes, Typed } from "./coerce.js";
import { PlanningError } from "./errors.js";
import type { ExpressionState, Scope } from "./expressions.js";
import type { PlanningStatistics } from "./statistics.js";

/** An output column of a lowered relation, with what result descriptors need. */
export interface RelColumn {
  readonly record?: true;
  readonly name: string;
  readonly type: PhysicalType;
  readonly nullable: boolean;
  /** Renders as a JS bigint (R17.1). */
  readonly big: boolean;
  readonly collation?: string;
}

export interface LoweredRel {
  readonly rel: Rel;
  readonly columns: readonly RelColumn[];
  /** Estimated output rows (planning only; lower/statistics.ts defaults when unknown). */
  readonly rows?: number;
  /**
   * Per output column of a query block, the select item when it is a constant whose type the
   * context decides (PostgreSQL's unknown: NULL, a string literal), before it settled to text.
   * A set operation resolves it against the other side (lower/from.ts `lowerSetOperation`).
   */
  readonly pending?: ReadonlyArray<Typed | undefined>;
}

/**
 * Per-statement plan state: operator ids, read tables, parameter slots beyond the placeholders
 * (the statement clock, init plans, `apply` bindings), init plans, recursive work tables, and
 * the statistics and costs the join order is chosen with.
 */
export class PlanBuilder {
  /** Aggregated while lowering queries, including DML subqueries. */
  rowLocking: RowLocking | undefined;
  #nextId = 1;
  #nextWorkTable = 1;
  #nextSource = 1;
  readonly reads = new Set<number>();
  readonly ftsQueries: ScalarExpr[] = [];
  readonly ftsParameters = new Map<number, number>();
  ftsParameter(source: number): number {
    let slot = this.ftsParameters.get(source);
    if (slot === undefined) {
      slot = this.slot({ kind: "text" });
      this.ftsParameters.set(source, slot);
    }
    return slot;
  }
  readonly rowCounts = new Map<number, number>();
  /**
   * Whether a decision about the plan's shape read statistics (an index range costed against a
   * scan, a join order or build side). False: the shape is the same whatever the counts, and the
   * statement's statistics basis is empty, so the owner skips `statisticsDrifted` on a hit.
   */
  statisticsDependent = false;
  /** Types of the slots after the placeholders, in slot order. */
  readonly slots: PhysicalType[] = [];
  clockSlot: number | null = null;
  readonly initPlans: Array<{ readonly slot: number; readonly plan: Rel }> = [];
  /** Recursive CTE references in scope → their work table and columns. */
  readonly workTables = new Map<
    string,
    { readonly id: number; readonly columns: readonly RelColumn[] }
  >();
  constructor(
    readonly catalog: CatalogView,
    readonly state: ExpressionState,
    readonly statistics: PlanningStatistics,
    readonly costs: ExecutorCosts,
  ) {}
  get parameters(): ParameterTypes {
    return this.state.parameters;
  }
  id(): number {
    return this.#nextId++;
  }
  workTable(): number {
    return this.#nextWorkTable++;
  }
  /** A statement-wide FROM-entry id (lower/from.ts virtual columns). */
  source(): number {
    return this.#nextSource++;
  }
  /**
   * An uncorrelated scalar subquery as an init plan (ir.ts `initPlans`): `single_value` enforces
   * PostgreSQL's cardinality (more than one row is an error; none is NULL). Returns the slot.
   */
  initPlan(inner: LoweredRel): Typed {
    const [only] = inner.columns;
    if (only === undefined || inner.columns.length !== 1)
      throw new PlanningError("subquery must return only one column", "42601");
    const slot = this.slot(only.type);
    this.initPlans.push({
      slot,
      plan: {
        op: "aggregate",
        id: this.id(),
        input: inner.rel,
        groupBy: [],
        groupingSets: null,
        aggregates: [
          {
            fn: "single_value",
            args: [column(0, only.type)],
            distinct: false,
            filter: null,
            orderBy: [],
            type: only.type,
          },
        ],
        output: [{ name: only.name, type: only.type, nullable: true }],
      },
    });
    return { expr: { op: "param", slot, type: only.type }, nullable: true, big: only.big };
  }
  /** An uncorrelated EXISTS as an init plan: `count(*) > 0` over one row of the subquery. */
  existsInitPlan(inner: LoweredRel): Typed {
    const int8: PhysicalType = { kind: "int64" };
    const bool: PhysicalType = { kind: "bool" };
    const slot = this.slot(bool);
    const counted: Rel = {
      op: "aggregate",
      id: this.id(),
      input: {
        op: "limit",
        id: this.id(),
        input: inner.rel,
        limit: { kind: "const", value: 1 },
        offset: null,
        output: inner.rel.output,
      },
      groupBy: [],
      groupingSets: null,
      aggregates: [
        { fn: "count_star", args: [], distinct: false, filter: null, orderBy: [], type: int8 },
      ],
      output: [{ name: "count", type: int8, nullable: false }],
    };
    const zero: ScalarExpr = {
      op: "literal",
      type: int8,
      value: {
        form: "flat",
        type: int8,
        length: 1,
        validity: null,
        data: { layout: "int64", values: { repr: "safe", values: Float64Array.of(0) } },
      },
    };
    this.initPlans.push({
      slot,
      plan: {
        op: "project",
        id: this.id(),
        input: counted,
        exprs: [
          {
            op: "compare",
            cmp: ">",
            left: column(0, int8),
            right: zero,
            collation: null,
            type: bool,
          },
        ],
        output: [{ name: "exists", type: bool, nullable: false }],
      },
    });
    return { expr: { op: "param", slot, type: bool }, nullable: false, big: false };
  }
  /** A new parameter slot of `type` after the placeholders. */
  slot(type: PhysicalType): number {
    this.slots.push(type);
    return this.parameters.count + this.slots.length - 1;
  }
  /** The statement clock's slot, allocated on first use. */
  clock(): number {
    this.clockSlot ??= this.slot({ kind: "timestamp", zoned: true });
    return this.clockSlot;
  }
  /** Estimated live rows of a table (planning statistics; 1,000 when unknown). */
  rowsOf(tableId: number): number {
    return this.statistics.table(tableId)?.rowCount ?? DEFAULT_TABLE_ROWS;
  }
  /** A shape decision consulted the statistics (see `statisticsDependent`). */
  dependsOnStatistics(): void {
    this.statisticsDependent = true;
  }
  read(table: TableDef): void {
    this.reads.add(table.id);
    this.rowCounts.set(table.id, this.statistics.table(table.id)?.rowCount ?? 0);
  }
}

/** Rows assumed for a table with no statistics (lower/statistics.ts). */
export const DEFAULT_TABLE_ROWS = 1_000;

export function outputColumns(columns: readonly RelColumn[]): OutputColumn[] {
  return columns.map(({ name, type, nullable }) => ({ name, type, nullable }));
}

export const column = (index: number, type: PhysicalType): ScalarExpr => ({
  op: "column",
  index,
  type,
});

/** `a` or `t.a` → the column name if the qualifier is one of `qualifiers` (or absent). */
export function unqualified(reference: string, qualifiers: readonly string[]): string | undefined {
  const dot = reference.indexOf(".");
  if (dot === -1) return reference;
  return qualifiers.includes(reference.slice(0, dot)) ? reference.slice(dot + 1) : undefined;
}

/**
 * A base table read: the columns any expression references, collected on first use, so the scan
 * reads only those (late materialization and key lookups use the same list).
 */
export class TableRead {
  readonly columns: number[] = [];
  readonly #positions = new Map<number, number>();
  constructor(
    readonly table: TableDef,
    readonly alias: string,
  ) {}
  /** The output position of a catalog column, added on first use. */
  use(def: ColumnDef): number {
    let position = this.#positions.get(def.id);
    if (position === undefined) {
      position = this.columns.length;
      this.columns.push(def.id);
      this.#positions.set(def.id, position);
    }
    return position;
  }
  /** Uses every column, in catalog order (a full row: MERGE's target, subquery scopes). */
  useAll(): void {
    for (const def of this.table.columns) this.use(def);
  }
  columnByName(name: string): ColumnDef | undefined {
    return this.table.columns.find((candidate) => candidate.name === name);
  }
  /** Output columns of the scan, in `columns` order (plus the locator when asked). */
  output(locator: boolean): OutputColumn[] {
    const columns: OutputColumn[] = this.columns.map((id) => {
      const def = this.table.columns.find((candidate) => candidate.id === id);
      if (def === undefined) throw new PlanningError(`column ${String(id)} vanished`, "XX000");
      return { name: def.name, type: def.type, nullable: def.nullable };
    });
    if (locator)
      columns.push({
        name: "(locator)",
        type: { kind: "locator", tableId: this.table.id },
        nullable: false,
      });
    return columns;
  }
  scope(): Scope {
    const qualifiers = [this.alias, this.table.name];
    return {
      column: (reference) => {
        const name = unqualified(reference, qualifiers);
        const def = name === undefined ? undefined : this.columnByName(name);
        if (def === undefined) return undefined;
        return {
          expr: column(this.use(def), def.type),
          nullable: def.nullable,
          big: def.type.kind === "int64",
        };
      },
    };
  }
}

/** A scope over a lowered relation's named columns (a derived table's output). */
export function relationScope(columns: readonly RelColumn[], qualifiers: readonly string[]): Scope {
  return {
    column: (reference) => {
      const name =
        unqualified(reference, qualifiers) ??
        (columns.some((c) => c.name === reference) ? reference : undefined);
      if (name === undefined) return undefined;
      const index = columns.findIndex((candidate) => candidate.name === name);
      const found = columns[index];
      if (found === undefined) return undefined;
      return {
        expr: column(index, found.type),
        nullable: found.nullable,
        big: found.big,
        ...(found.record ? { record: true as const } : {}),
        ...(found.collation === undefined ? {} : { collation: found.collation }),
      };
    },
  };
}

export function andOf(conjuncts: readonly Typed[]): ScalarExpr | null {
  if (conjuncts.length === 0) return null;
  if (conjuncts.length === 1) return conjuncts[0]?.expr ?? null;
  return { op: "and", args: conjuncts.map((conjunct) => conjunct.expr), type: { kind: "bool" } };
}

/** `expr` with every column index mapped through `f` (literals and params unchanged). */
export function mapColumns(expr: ScalarExpr, f: (index: number) => number): ScalarExpr {
  const map = (inner: ScalarExpr): ScalarExpr => mapColumns(inner, f);
  switch (expr.op) {
    case "column":
      return { ...expr, index: f(expr.index) };
    case "literal":
    case "param":
      return expr;
    case "cast":
      return { ...expr, input: map(expr.input) };
    case "call":
    case "and":
    case "or":
      return { ...expr, args: expr.args.map(map) };
    case "compare":
    case "distinctFrom":
      return { ...expr, left: map(expr.left), right: map(expr.right) };
    case "not":
    case "isNull":
      return { ...expr, arg: map(expr.arg) };
    case "case":
      return {
        ...expr,
        branches: expr.branches.map((branch) => ({
          when: map(branch.when),
          then: map(branch.then),
        })),
        otherwise: expr.otherwise === null ? null : map(expr.otherwise),
      };
    case "inList":
      return { ...expr, arg: map(expr.arg), list: expr.list.map(map) };
    case "like":
      return { ...expr, arg: map(expr.arg), pattern: map(expr.pattern) };
  }
}

/** Every column index `expr` reads, added to `into`. */
export function columnsIn(expr: ScalarExpr, into = new Set<number>()): Set<number> {
  mapColumns(expr, (index) => {
    into.add(index);
    return index;
  });
  return into;
}

/** `value`, which the caller's invariant guarantees is present (XX000 when it is not). */
export function required<T>(value: T | null | undefined, what = "a planned value"): T {
  if (value === null || value === undefined) throw new PlanningError(`${what} is missing`, "XX000");
  return value;
}

/** `items[index]`, which the caller's invariant guarantees exists (XX000 when it does not). */
export function at<T>(items: ArrayLike<T>, index: number): T {
  return required(items[index], `item ${String(index)}`);
}
