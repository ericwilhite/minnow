/**
 * Query blocks (plan/model.ts `CompiledQuery`, after the front-end passes) to relational IR:
 * source (scan, key lookup, derived block, or the one-row FROM-less source), WHERE, GROUP BY with
 * aggregates and HAVING, the select list, ORDER BY, LIMIT / OFFSET.
 *
 * Milestone M1 surface; joins, subqueries, windows, set operations and recursion are refused
 * with `UnsupportedPlanError` (stage "lower") until milestone M3.
 */
import type { CatalogView, ColumnDef, IndexDef, TableDef } from "../access/catalog.js";
import type {
  AggregateCall,
  AggregateFn,
  Bound,
  OutputColumn,
  Rel,
  ScalarExpr,
  SortKey,
} from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { DUAL_TABLE, isAggregateCall } from "../../engine/query.js";
import { encodeQueryIdentity } from "../../engine/query-identity.js";
import type { CompiledQuery, Expression, Predicate, TableSource } from "../../plan/model.js";
import { baseType, coerce, settle, type ParameterTypes, type Typed } from "./coerce.js";
import { PlanningError, unsupported } from "./errors.js";
import {
  asBool,
  lowerCondition,
  lowerExpression,
  type Collated,
  type ExpressionState,
  type Scope,
} from "./expressions.js";

/** An output column of a lowered relation, with what result descriptors need. */
export interface RelColumn {
  readonly name: string;
  readonly type: PhysicalType;
  readonly nullable: boolean;
  /** Renders as a JS bigint (R17.1). */
  readonly big: boolean;
}

export interface LoweredRel {
  readonly rel: Rel;
  readonly columns: readonly RelColumn[];
}

/** Per-statement plan state: operator ids, read tables, expression state. */
export class PlanBuilder {
  #nextId = 1;
  readonly reads = new Set<number>();
  readonly rowCounts = new Map<number, number>();
  constructor(
    readonly catalog: CatalogView,
    readonly state: ExpressionState,
    readonly rowCount: (tableId: number) => number,
  ) {}
  get parameters(): ParameterTypes {
    return this.state.parameters;
  }
  id(): number {
    return this.#nextId++;
  }
  read(table: TableDef): void {
    this.reads.add(table.id);
    this.rowCounts.set(table.id, this.rowCount(table.id));
  }
}

export function outputColumns(columns: readonly RelColumn[]): OutputColumn[] {
  return columns.map(({ name, type, nullable }) => ({ name, type, nullable }));
}

const column = (index: number, type: PhysicalType): ScalarExpr => ({ op: "column", index, type });

/** `a` or `t.a` → the column name if the qualifier is one of `qualifiers` (or absent). */
function unqualified(reference: string, qualifiers: readonly string[]): string | undefined {
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
      return { expr: column(index, found.type), nullable: found.nullable, big: found.big };
    },
  };
}

/** One key tuple of a unique index, from `col = constant` conjuncts (or one `col IN (…)`). */
interface KeyAccess {
  readonly index: IndexDef;
  readonly keys: ReadonlyArray<readonly ScalarExpr[]>;
  /** Positions of the predicates the lookup answers. */
  readonly used: ReadonlySet<number>;
}

const isConstant = (expression: Expression): boolean =>
  expression.kind === "literal" ||
  expression.kind === "parameter" ||
  (expression.kind === "call" &&
    expression.name === "CAST" &&
    expression.arguments[0] !== undefined &&
    isConstant(expression.arguments[0]));

/**
 * A key exactly of `def.type` for a constant expression, or null when it cannot be one (a
 * constant out of the column's range, or a type that needs more than an exact constant).
 */
function keyValue(
  expression: Expression,
  def: ColumnDef,
  scope: Scope,
  builder: PlanBuilder,
): ScalarExpr | null {
  const typed = lowerExpression(expression, scope, builder.state);
  try {
    // Only lossless coercions select key access (review finding 1): the key is the constant at
    // the column's base type, so `varchar2_col = 'abc'` probes 'abc' (no match), never 'ab'.
    const keyed = coerce(typed, baseType(def.type), "implicit", builder.parameters);
    const expr = keyed.expr;
    if (expr.op === "literal" || expr.op === "param") return expr;
    // A constant parsed from text (`DATE '…'`, a uuid string), evaluated once by the executor.
    if (
      expr.op === "cast" &&
      expr.assignment !== true &&
      expr.input.op === "literal" &&
      expr.input.type.kind === "text" &&
      expr.type.kind !== "text"
    )
      return expr;
    return null;
  } catch {
    return null;
  }
}

/**
 * Finds a unique index whose every column is fixed by an equality with a constant (or one column
 * by an IN list of constants): the plan becomes a `keyLookup` (ruling R6's point read). The
 * primary key wins over other unique indexes. A constant that cannot be a key of the column's type
 * (`int4_col = 5000000000`) gives no lookup: the scan's filter answers it (no row matches).
 */
export function findKeyAccess(
  read: TableRead,
  predicates: readonly Predicate[],
  builder: PlanBuilder,
): KeyAccess | null {
  const scope = read.scope();
  const equalities = new Map<number, { position: number; value: Expression }>();
  const lists = new Map<number, { position: number; values: readonly Expression[] }>();
  for (const [position, predicate] of predicates.entries()) {
    const sides: Array<[Expression, Expression]> = [
      [predicate.left, predicate.right],
      [predicate.right, predicate.left],
    ];
    for (const [columnSide, valueSide] of sides) {
      if (columnSide.kind !== "column") continue;
      const name = unqualified(columnSide.reference, [read.alias, read.table.name]);
      const def = name === undefined ? undefined : read.columnByName(name);
      if (def === undefined) continue;
      if (predicate.operator === "=" && isConstant(valueSide)) {
        if (!equalities.has(def.id)) equalities.set(def.id, { position, value: valueSide });
        break;
      }
      if (
        predicate.operator === "IN" &&
        columnSide === predicate.left &&
        valueSide.kind === "list" &&
        valueSide.items.length > 0 &&
        valueSide.items.every(isConstant)
      ) {
        if (!lists.has(def.id)) lists.set(def.id, { position, values: valueSide.items });
        break;
      }
    }
  }
  const unique = read.table.indexes
    .filter((index) => index.unique)
    .sort((a, b) => Number(b.role === "primary") - Number(a.role === "primary"));
  for (const index of unique) {
    const defs = index.columnIds.map((id) =>
      read.table.columns.find((candidate) => candidate.id === id),
    );
    if (defs.some((def) => def === undefined)) continue;
    const columns = defs as ColumnDef[];
    if (columns.every((def) => equalities.has(def.id))) {
      const tuple: ScalarExpr[] = [];
      const used = new Set<number>();
      for (const def of columns) {
        const entry = equalities.get(def.id);
        const key = entry === undefined ? null : keyValue(entry.value, def, scope, builder);
        if (entry === undefined || key === null) break;
        tuple.push(key);
        used.add(entry.position);
      }
      if (tuple.length === columns.length) return { index, keys: [tuple], used };
    }
    const [only] = columns;
    if (columns.length === 1 && only !== undefined && lists.has(only.id)) {
      const entry = lists.get(only.id);
      if (entry === undefined) continue;
      const keys = entry.values.map((value) => keyValue(value, only, scope, builder));
      if (keys.every((key): key is ScalarExpr => key !== null))
        return { index, keys: keys.map((key) => [key]), used: new Set([entry.position]) };
    }
  }
  return null;
}

/** A front-end aggregate call as an IR aggregate over the pre-aggregation projection. */
interface AggregateDraft {
  readonly fn: AggregateFn;
  readonly args: readonly Typed[];
  readonly distinct: boolean;
  readonly orderBy: ReadonlyArray<{
    readonly typed: Typed;
    readonly direction: "asc" | "desc";
    readonly nulls: "first" | "last";
  }>;
  readonly type: PhysicalType;
  readonly nullable: boolean;
  readonly big: boolean;
}

const INT8: PhysicalType = { kind: "int64" };
const FLOAT8: PhysicalType = { kind: "float64" };

function aggregateDraft(
  expression: Extract<Expression, { kind: "call" }>,
  scope: Scope,
  builder: PlanBuilder,
): AggregateDraft {
  const state = builder.state;
  const name = expression.name;
  const distinct = expression.distinct === true;
  const settleArg = (arg: Expression): Typed =>
    settle(lowerExpression(arg, scope, state), builder.parameters);
  const [first] = expression.arguments;
  if (name === "COUNT") {
    if (first === undefined || first.kind === "wildcard")
      return {
        fn: "count_star",
        args: [],
        distinct: false,
        orderBy: [],
        type: INT8,
        nullable: false,
        big: false,
      };
    return {
      fn: "count",
      args: [settleArg(first)],
      distinct,
      orderBy: [],
      type: INT8,
      nullable: false,
      big: false,
    };
  }
  if (first === undefined)
    throw new PlanningError(`${name.toLowerCase()} needs an argument`, "42601");
  const arg = settleArg(first);
  const type = arg.expr.type;
  switch (name) {
    case "SUM": {
      const result: PhysicalType | null =
        type.kind === "int32"
          ? INT8
          : type.kind === "int64"
            ? { kind: "decimal", precision: null, scale: 0 }
            : type.kind === "decimal"
              ? { kind: "decimal", precision: null, scale: null }
              : type.kind === "float32" || type.kind === "float64" || type.kind === "interval"
                ? type
                : null;
      if (result === null)
        throw new PlanningError(`function sum(${type.kind}) does not exist`, "42883");
      // D2 / R17.2: SUM(int8) is exact and renders bigint; SUM(int2/int4) renders number.
      return {
        fn: "sum",
        args: [arg],
        distinct,
        orderBy: [],
        type: result,
        nullable: true,
        big: type.kind === "int64",
      };
    }
    case "AVG": {
      const result: PhysicalType | null =
        type.kind === "int32" ||
        type.kind === "int64" ||
        type.kind === "float32" ||
        type.kind === "float64"
          ? FLOAT8
          : type.kind === "decimal"
            ? { kind: "decimal", precision: null, scale: null }
            : type.kind === "interval"
              ? type
              : null;
      if (result === null)
        throw new PlanningError(`function avg(${type.kind}) does not exist`, "42883");
      return {
        fn: "avg",
        args: [arg],
        distinct,
        orderBy: [],
        type: result,
        nullable: true,
        big: false,
      };
    }
    case "MIN":
    case "MAX":
      if (type.kind === "json" && !type.binary)
        throw new PlanningError(`function ${name.toLowerCase()}(json) does not exist`, "42883");
      return {
        fn: name === "MIN" ? "min" : "max",
        args: [arg],
        distinct,
        orderBy: [],
        type,
        nullable: true,
        big: arg.big,
      };
    case "STRING_AGG": {
      const [, separator] = expression.arguments;
      if (separator === undefined) throw new PlanningError("string_agg needs a delimiter", "42601");
      const text: PhysicalType = { kind: "text" };
      const value = coerce(arg, text, "implicit", builder.parameters);
      const delimiter = coerce(
        lowerExpression(separator, scope, state),
        text,
        "implicit",
        builder.parameters,
      );
      const orderBy = (expression.aggregateOrderBy ?? []).map((order) => ({
        typed: settleArg(order.expression),
        direction: order.direction,
        nulls: order.nulls ?? (order.direction === "desc" ? ("first" as const) : ("last" as const)),
      }));
      return {
        fn: "string_agg",
        args: [value, delimiter],
        distinct,
        orderBy,
        type: text,
        nullable: true,
        big: false,
      };
    }
    case "MINNOW_SINGLE_VALUE":
      return {
        fn: "single_value",
        args: [arg],
        distinct: false,
        orderBy: [],
        type,
        nullable: true,
        big: arg.big,
      };
    default:
      throw unsupported("aggregate", name.toLowerCase());
  }
}

/** The output of a block's GROUP BY: keys first, then aggregates. */
class Grouping {
  readonly keys: Typed[] = [];
  readonly #keyIdentities = new Map<string, number>();
  /** Source column positions of keys that are plain columns. */
  readonly #keyColumns = new Map<number, number>();
  readonly aggregates: AggregateDraft[] = [];
  readonly #aggregateIdentities = new Map<string, number>();

  constructor(
    groupBy: readonly Expression[],
    readonly source: Scope,
    readonly builder: PlanBuilder,
  ) {
    for (const expression of groupBy) {
      const identity = encodeQueryIdentity(expression);
      if (this.#keyIdentities.has(identity)) continue;
      const typed = settle(lowerExpression(expression, source, builder.state), builder.parameters);
      const position = this.keys.length;
      this.keys.push(typed);
      this.#keyIdentities.set(identity, position);
      if (typed.expr.op === "column" && !this.#keyColumns.has(typed.expr.index))
        this.#keyColumns.set(typed.expr.index, position);
    }
  }

  #aggregateColumn(position: number): Typed {
    const draft = this.aggregates[position];
    if (draft === undefined) throw new PlanningError("aggregate vanished", "XX000");
    return {
      expr: column(this.keys.length + position, draft.type),
      nullable: draft.nullable,
      big: draft.big,
    };
  }

  #keyColumn(position: number): Typed {
    const key = this.keys[position];
    if (key === undefined) throw new PlanningError("group key vanished", "XX000");
    return { expr: column(position, key.expr.type), nullable: key.nullable, big: key.big };
  }

  /** The scope of expressions over the grouped rows (HAVING, the select list, ORDER BY). */
  scope(): Scope {
    return {
      match: (expression) => {
        const identity = encodeQueryIdentity(expression);
        const key = this.#keyIdentities.get(identity);
        if (key !== undefined) return this.#keyColumn(key);
        if (!isAggregateCall(expression)) return undefined;
        let position = this.#aggregateIdentities.get(identity);
        if (position === undefined) {
          position = this.aggregates.length;
          this.aggregates.push(aggregateDraft(expression, this.source, this.builder));
          this.#aggregateIdentities.set(identity, position);
        }
        return this.#aggregateColumn(position);
      },
      column: (reference) => {
        const resolved = this.source.column(reference);
        if (resolved === undefined) return undefined;
        const key =
          resolved.expr.op === "column" ? this.#keyColumns.get(resolved.expr.index) : undefined;
        if (key === undefined)
          throw new PlanningError(
            `column "${reference}" must appear in the GROUP BY clause or be used in an aggregate function`,
            "42803",
          );
        return this.#keyColumn(key);
      },
    };
  }

  /** The projection below the aggregate (keys, then every aggregate argument) and the aggregate. */
  build(input: Rel): LoweredRel {
    const exprs: ScalarExpr[] = this.keys.map((key) => key.expr);
    const projected: RelColumn[] = this.keys.map((key, index) => ({
      name: `(group ${String(index + 1)})`,
      type: key.expr.type,
      nullable: key.nullable,
      big: key.big,
    }));
    const argumentColumn = (typed: Typed): ScalarExpr => {
      const index = exprs.length;
      exprs.push(typed.expr);
      projected.push({
        name: `(argument ${String(index + 1)})`,
        type: typed.expr.type,
        nullable: typed.nullable,
        big: typed.big,
      });
      return column(index, typed.expr.type);
    };
    const calls: AggregateCall[] = this.aggregates.map((draft) => ({
      fn: draft.fn,
      args: draft.args.map(argumentColumn),
      distinct: draft.distinct,
      filter: null,
      orderBy: draft.orderBy.map((order): SortKey => ({
        expr: argumentColumn(order.typed),
        direction: order.direction,
        nulls: order.nulls,
        collation: null,
      })),
      type: draft.type,
    }));
    const pre: Rel = {
      op: "project",
      id: this.builder.id(),
      input,
      exprs,
      output: outputColumns(projected),
    };
    const columns: RelColumn[] = [
      ...projected.slice(0, this.keys.length),
      ...this.aggregates.map((draft, index) => ({
        name: `(aggregate ${String(index + 1)})`,
        type: draft.type,
        nullable: draft.nullable,
        big: draft.big,
      })),
    ];
    const rel: Rel = {
      op: "aggregate",
      id: this.builder.id(),
      input: pre,
      groupBy: this.keys.map((_, index) => index),
      groupingSets: null,
      aggregates: calls,
      output: outputColumns(columns),
    };
    return { rel, columns };
  }
}

/** A block's source before its relation is built (table reads collect columns first). */
interface Source {
  readonly scope: Scope;
  build(
    filter: readonly Typed[],
    predicates: readonly Predicate[],
  ): LoweredRel & { readonly pointRead: boolean };
}

export function andOf(conjuncts: readonly Typed[]): ScalarExpr | null {
  if (conjuncts.length === 0) return null;
  if (conjuncts.length === 1) return conjuncts[0]?.expr ?? null;
  return { op: "and", args: conjuncts.map((conjunct) => conjunct.expr), type: { kind: "bool" } };
}

function filtered(
  input: LoweredRel,
  conjuncts: readonly Typed[],
  builder: PlanBuilder,
): LoweredRel {
  const predicate = andOf(conjuncts);
  if (predicate === null) return input;
  return {
    rel: { op: "filter", id: builder.id(), input: input.rel, predicate, output: input.rel.output },
    columns: input.columns,
  };
}

function tableSource(
  table: TableDef,
  alias: string,
  builder: PlanBuilder,
  keyed: boolean,
): Source & { readonly read: TableRead } {
  const read = new TableRead(table, alias);
  return {
    read,
    scope: read.scope(),
    build(conjuncts, predicates) {
      builder.read(table);
      const access = keyed ? findKeyAccess(read, predicates, builder) : null;
      const columnsOf = (): RelColumn[] =>
        read.output(false).map((output) => ({ ...output, big: output.type.kind === "int64" }));
      if (access !== null) {
        const residual = conjuncts.filter((_, position) => !access.used.has(position));
        const lookup: Rel = {
          op: "keyLookup",
          id: builder.id(),
          tableId: table.id,
          indexId: access.index.id,
          keys: access.keys,
          columns: [...read.columns],
          output: read.output(false),
        };
        return {
          ...filtered({ rel: lookup, columns: columnsOf() }, residual, builder),
          pointRead: true,
        };
      }
      const scan: Rel = {
        op: "scan",
        id: builder.id(),
        tableId: table.id,
        columns: [...read.columns],
        filter: andOf(conjuncts),
        runtimeFilters: [],
        emitLocator: false,
        output: read.output(false),
      };
      return { rel: scan, columns: columnsOf(), pointRead: false };
    },
  };
}

function lowerSource(source: TableSource, builder: PlanBuilder): Source {
  if (source.union !== undefined) throw unsupported("setOperation", "UNION / INTERSECT / EXCEPT");
  if (source.recursive !== undefined) throw unsupported("recursive", "WITH RECURSIVE");
  if (source.windowed !== undefined) throw unsupported("window", "window functions");
  if (source.lateral === true) throw unsupported("lateral", "LATERAL");
  if (source.derived !== undefined) {
    const inner = lowerBlock(source.derived, builder);
    const columns =
      source.columnAliases === undefined
        ? inner.columns
        : inner.columns.map((col, index) => ({
            ...col,
            name: source.columnAliases?.[index] ?? col.name,
          }));
    const scope = relationScope(columns, [source.alias]);
    return {
      scope,
      build: (conjuncts) => ({
        ...filtered({ rel: inner.rel, columns }, conjuncts, builder),
        pointRead: false,
      }),
    };
  }
  if (source.table === DUAL_TABLE)
    return {
      scope: { column: () => undefined },
      build: (conjuncts) => ({
        ...filtered(
          { rel: { op: "values", id: builder.id(), rows: [[]], output: [] }, columns: [] },
          conjuncts,
          builder,
        ),
        pointRead: false,
      }),
    };
  const table = builder.catalog.tableByName(source.table);
  if (table === undefined)
    throw new PlanningError(`relation "${source.table}" does not exist`, "42P01");
  return tableSource(table, source.alias, builder, true);
}

function boundOf(
  value: number | undefined,
  parameter: number | undefined,
  builder: PlanBuilder,
): Bound | null {
  if (parameter !== undefined) {
    builder.parameters.fix(parameter, INT8);
    return { kind: "param", slot: parameter };
  }
  return value === undefined ? null : { kind: "const", value };
}

/** One query block as a relation with named output columns. */
export function lowerBlock(
  block: CompiledQuery,
  builder: PlanBuilder,
): LoweredRel & { readonly pointRead?: boolean } {
  if (block.joins.length > 0) throw unsupported("join", "joins (milestone M3)");
  if (block.pendingSelectShape !== undefined)
    throw new PlanningError("unbound select shape", "XX000");
  const state = builder.state;
  const source = lowerSource(block.base, builder);
  const conjuncts = block.predicates.map((predicate) =>
    asBool(
      lowerCondition(
        predicate.operator,
        predicate.left,
        predicate.right,
        predicate.escape,
        source.scope,
        state,
      ),
      state,
    ),
  );
  const grouped =
    block.groupBy.length > 0 ||
    block.select.some((item) => hasAggregateCall(item.expression)) ||
    block.having.length > 0;
  const grouping = grouped ? new Grouping(block.groupBy, source.scope, builder) : null;
  const outputScope = grouping?.scope() ?? source.scope;
  const having = block.having.map((predicate) =>
    asBool(
      lowerCondition(
        predicate.operator,
        predicate.left,
        predicate.right,
        predicate.escape,
        outputScope,
        state,
      ),
      state,
    ),
  );
  const items: Collated[] = block.select.map((item) =>
    settle(lowerExpression(item.expression, outputScope, state), builder.parameters),
  );
  const names = block.select.map((item) => item.alias);
  // ORDER BY: an output column by name, else an expression over the block's rows (hidden).
  const hidden: Collated[] = [];
  const order = block.orderBy.map((entry) => {
    const expression = entry.expression;
    let position = expression.kind === "column" ? names.indexOf(expression.reference) : -1;
    let collation: string | null;
    if (position === -1) {
      const typed = settle(
        lowerExpression(expression, outputScope, state),
        builder.parameters,
      ) as Collated;
      collation = typed.collation ?? null;
      const identity = encodeQueryIdentity(expression);
      const existing = block.select.findIndex(
        (item) => encodeQueryIdentity(item.expression) === identity,
      );
      if (existing !== -1) position = existing;
      else {
        position = items.length + hidden.length;
        hidden.push(typed);
      }
    } else collation = items[position]?.collation ?? null;
    return {
      position,
      direction: entry.direction,
      nulls: entry.nulls ?? (entry.direction === "desc" ? ("first" as const) : ("last" as const)),
      collation,
    };
  });

  // Assemble bottom-up: every expression is typed, so the scan's column list is final.
  const base = source.build(conjuncts, block.predicates);
  let current: LoweredRel = base;
  if (grouping !== null) current = filtered(grouping.build(current.rel), having, builder);
  const projected = [...items, ...hidden];
  const columns: RelColumn[] = projected.map((item, index) => ({
    name: names[index] ?? `(order ${String(index - names.length + 1)})`,
    type: item.expr.type,
    nullable: item.nullable,
    big: item.big,
  }));
  current = {
    rel: {
      op: "project",
      id: builder.id(),
      input: current.rel,
      exprs: projected.map((item) => item.expr),
      output: outputColumns(columns),
    },
    columns,
  };
  const limit = boundOf(block.limit, block.limitParameter, builder);
  const offset = boundOf(block.offset, block.offsetParameter, builder);
  if (order.length > 0) {
    const keys: SortKey[] = order.map((key) => ({
      expr: column(key.position, columns[key.position]?.type ?? { kind: "text" }),
      direction: key.direction,
      nulls: key.nulls,
      collation: key.collation,
    }));
    current = {
      rel: {
        op: "sort",
        id: builder.id(),
        input: current.rel,
        keys,
        limit,
        offset,
        withTies: block.limitWithTies === true,
        output: current.rel.output,
      },
      columns,
    };
  } else if (limit !== null || offset !== null) {
    if (block.limitWithTies === true)
      throw new PlanningError("WITH TIES requires ORDER BY", "42601");
    current = {
      rel: {
        op: "limit",
        id: builder.id(),
        input: current.rel,
        limit,
        offset,
        output: current.rel.output,
      },
      columns,
    };
  }
  if (hidden.length > 0) {
    const visible = columns.slice(0, items.length);
    current = {
      rel: {
        op: "project",
        id: builder.id(),
        input: current.rel,
        exprs: visible.map((col, index) => column(index, col.type)),
        output: outputColumns(visible),
      },
      columns: visible,
    };
  }
  return { ...current, pointRead: base.pointRead && grouping === null && order.length === 0 };
}

function hasAggregateCall(expression: Expression): boolean {
  if (isAggregateCall(expression)) return true;
  switch (expression.kind) {
    case "binary":
    case "condition":
    case "logical":
      return hasAggregateCall(expression.left) || hasAggregateCall(expression.right);
    case "not":
      return hasAggregateCall(expression.operand);
    case "call":
      return expression.arguments.some(hasAggregateCall);
    case "case":
      return (
        expression.branches.some(
          (branch) => hasAggregateCall(branch.when) || hasAggregateCall(branch.then),
        ) ||
        (expression.otherwise !== undefined && hasAggregateCall(expression.otherwise))
      );
    case "list":
      return expression.items.some(hasAggregateCall);
    default:
      return false;
  }
}
