/**
 * A FROM clause (`base` + `joins` of a compiled block, or a DML statement's target and sources)
 * as one relation: its sources, the join tree, and the subqueries its expressions attach.
 *
 * ## Virtual columns
 * Every FROM entry gets a statement-wide source id (`PlanBuilder.source()`); its columns are
 * typed as `source × SOURCE_STRIDE + local`. Expressions are typed once against that layout,
 * whatever the join order; `build` remaps them to positions when the tree exists. Ids are unique
 * across the statement, so a correlated subquery's conditions can mix outer and inner columns.
 *
 * ## Join order (design-joins-agg-spill.md "Statistics and ordering", lower/statistics.ts)
 * - Inner and cross joins between two barriers (LEFT, FULL, semi and anti joins keep their
 *   place, as they do not commute) are ordered by the cost model: every left-deep connected
 *   order (a cross product only when nothing connects) up to `EXHAUSTIVE_JOIN_LIMIT` relations
 *   (`exhaustiveOrder`), greedy above (`greedyOrder`, no limit). The build side (IR `right`) is the smaller
 *   estimate. Cost = hash build + probe + emitted rows (ir/costs.ts), or the product for a nested
 *   loop with no equality key.
 * - Output estimates: `L × R / max(NDV_L, NDV_R)` per equality, 1/3 per other condition.
 *   Distinct counts come from statistics, else a single-column unique key's table rows, else
 *   PostgreSQL's default of 200; a default never outweighs a known count, so a foreign key
 *   joined to its parent's key estimates the child's rows.
 * - A WHERE conjunct over one source is pushed into it (its access path) unless the source is
 *   null-supplied by an outer join; others apply as soon as their sources are joined.
 *
 * ## Subqueries (lower/expressions.ts `SubqueryRequest`)
 * - `x [NOT] IN (SELECT e FROM … WHERE …)` over a simple block (one select item, no grouping,
 *   ordering, limits or nested subqueries) is a **mark join** (ir.ts, R20.1 and R20.5): inner-only
 *   WHERE conjuncts filter the build side, correlated equalities become correlation keys, other
 *   correlated conditions go in `filter`; the tested key is `x = e`. NOT IN is NOT over the mark.
 * - Any other subquery is an **apply** over the clause's rows, its correlated references bound to
 *   parameter slots; `memoize` unless the subquery is volatile.
 * - An uncorrelated scalar subquery is an init plan (query.ts `installSubqueries`).
 */
import { constantRows } from "./constant-rows.js";
import type { TableDef } from "../access/catalog.js";
import type { ExecutorCosts } from "../ir/costs.js";
import type { Rel, ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { DUAL_TABLE } from "../../engine/query.js";
import type { CompiledQuery, Expression, JoinPlan, TableSource } from "../../plan/model.js";
import { isCrossJoinPlan } from "../../plan/model.js";
import { accessPath, selectivityOf } from "./access-path.js";
import { baseType, coerce, commonType, settle, type Typed } from "./coerce.js";
import { PlanningError, unsupported } from "./errors.js";
import {
  asBool,
  lowerCondition,
  lowerExpression,
  type Scope,
  type Collated,
  type SubqueryRequest,
} from "./expressions.js";
import {
  andOf,
  at,
  column,
  columnsIn,
  DEFAULT_TABLE_ROWS,
  mapColumns,
  outputColumns,
  required,
  TableRead,
  type LoweredRel,
  type PlanBuilder,
  type RelColumn,
} from "./plan.js";
import { EXHAUSTIVE_JOIN_LIMIT } from "./statistics.js";
import { lowerWindowed } from "./windows.js";
import { boolLiteral } from "./literals.js";

/** Hash keys use C8 byte equality. Locale-aware equalities remain vector comparisons. */
function byteCollation(collation: string | null | undefined): boolean {
  return collation == null || ["C", "POSIX", "default", "ucs_basic"].includes(collation);
}

export const SOURCE_STRIDE = 1 << 16;
/** The local index of a table source's row locator. */
const LOCATOR_LOCAL = SOURCE_STRIDE - 1;

export const virtual = (source: number, local: number): number => source * SOURCE_STRIDE + local;
export const sourceOf = (index: number): number => Math.floor(index / SOURCE_STRIDE);

/** Resolves a name the clause's own sources do not have: an enclosing query's column. */
export type OuterResolver = ((reference: string) => Typed | undefined) & {
  /** Whether an enclosing query has a FROM entry of this name (Scope.knowsQualifier). */
  knowsQualifier?: (qualifier: string) => boolean;
};

/** Lowers a whole block (query.ts), passed in so this module needs no import of it. */
export type BlockLowerer = (
  block: CompiledQuery,
  builder: PlanBuilder,
  outer?: OuterResolver,
) => LoweredRel;

/** A relation of the join tree: its columns' virtual indexes (`layout`) and estimate. */
interface Unit {
  readonly rel: Rel;
  readonly layout: readonly number[];
  readonly columns: readonly RelColumn[];
  readonly rows: number;
  readonly sources: ReadonlySet<number>;
  readonly pointRead: boolean;
}

type EntryKind = "base" | "inner" | "left" | "semi" | "anti" | "full";

interface Resolved {
  readonly local: number;
  readonly type: PhysicalType;
  readonly nullable: boolean;
  readonly big: boolean;
  readonly collation?: string;
}

interface Entry {
  readonly source: number;
  readonly kind: EntryKind;
  readonly qualifiers: readonly string[];
  /** Null-supplied by an outer join: its columns read NULL-extended, nothing is pushed into it. */
  readonly nullSupplied: boolean;
  resolve(name: string): Resolved | undefined;
  /** Visible column names, for `*`. */
  names(): string[];
  /** The entry's relation over `pushed` conjuncts (typed over its local positions). */
  build(pushed: readonly Typed[], locator: boolean): Unit;
  /** A table read, for its row locator (DML targets). */
  readonly read?: TableRead;
  /** Distinct estimate of a local column, for join estimates. */
  distinct(local: number): number | null;
}

interface Conjunct {
  readonly typed: Typed;
  readonly sources: ReadonlySet<number>;
  used: boolean;
}

interface Attachment {
  readonly source: number;
  readonly attach: (acc: Unit, position: (index: number) => number) => Rel;
  readonly column: RelColumn;
}

function sourcesOf(expr: ScalarExpr): Set<number> {
  return new Set([...columnsIn(expr)].map(sourceOf));
}

const subset = (a: ReadonlySet<number>, b: ReadonlySet<number>) =>
  [...a].every((item) => b.has(item));

function conjunctsOf(typed: Typed): Typed[] {
  if (typed.expr.op !== "and") return [typed];
  return typed.expr.args.map((arg) => ({ expr: arg, nullable: typed.nullable, big: false }));
}

const HIDDEN = "\u0000";

/** A lowered relation as a FROM entry (derived tables, set operations, recursion, windows). */
function relationEntry(
  source: number,
  kind: EntryKind,
  nullSupplied: boolean,
  qualifiers: readonly string[],
  inner: LoweredRel,
  builder: PlanBuilder,
): Entry {
  const columns = inner.columns;
  return {
    source,
    kind,
    qualifiers,
    nullSupplied,
    resolve: (name) => {
      const local = columns.findIndex((candidate) => candidate.name === name);
      const found = columns[local];
      return found === undefined
        ? undefined
        : {
            local,
            type: found.type,
            nullable: found.nullable,
            big: found.big,
            ...(found.collation === undefined ? {} : { collation: found.collation }),
          };
    },
    names: () => columns.map((col) => col.name).filter((name) => !name.startsWith(HIDDEN)),
    build: (pushed) => {
      const predicate = andOf(pushed);
      const rel: Rel =
        predicate === null
          ? inner.rel
          : {
              op: "filter",
              id: builder.id(),
              input: inner.rel,
              predicate,
              output: inner.rel.output,
            };
      return {
        rel,
        layout: columns.map((_, local) => virtual(source, local)),
        columns,
        rows: Math.max(1, (inner.rows ?? DEFAULT_TABLE_ROWS) * selectivityOf(pushed)),
        sources: new Set([source]),
        pointRead: false,
      };
    },
    distinct: () => null,
  };
}

function tableEntry(
  source: number,
  kind: EntryKind,
  nullSupplied: boolean,
  table: TableDef,
  alias: string,
  builder: PlanBuilder,
): Entry {
  const read = new TableRead(table, alias);
  return {
    source,
    kind,
    qualifiers: [alias, table.name],
    nullSupplied,
    read,
    resolve: (name) => {
      const def = read.columnByName(name);
      if (def === undefined) return undefined;
      return {
        local: read.use(def),
        type: def.type,
        nullable: def.nullable,
        big: def.type.kind === "int64",
      };
    },
    names: () => table.columns.map((def) => def.name),
    build: (pushed, locator) => {
      builder.read(table);
      const access = accessPath(read, pushed, builder, locator);
      const output = read.output(locator);
      return {
        rel: access.rel,
        layout: [
          ...read.columns.map((_, local) => virtual(source, local)),
          ...(locator ? [virtual(source, LOCATOR_LOCAL)] : []),
        ],
        columns: output.map((col) => ({ ...col, big: col.type.kind === "int64" })),
        rows: access.rows,
        sources: new Set([source]),
        pointRead: access.pointRead,
      };
    },
    distinct: (local) => {
      const columnId = read.columns[local];
      if (columnId === undefined) return null;
      const known = builder.statistics.table(table.id)?.column(columnId)?.distinct ?? null;
      if (known !== null) return known;
      // A single-column unique key has as many distinct values as its table has rows.
      const unique = table.indexes.some(
        (index) => index.unique && index.columnIds.length === 1 && index.columnIds[0] === columnId,
      );
      return unique ? builder.rowsOf(table.id) : null;
    },
  };
}

export interface FromClause {
  readonly scope: Scope;
  /** The columns `*` (or `alias.*`) names, as column references. */
  wildcard(qualifier: string | undefined): Expression[];
  /** The virtual index of a table entry's row locator (DML targets). */
  locator(entry: number): number;
  /** The entry ids, in FROM order. */
  readonly sources: readonly number[];
  /** The table read of a table entry (by FROM position), or undefined. */
  tableRead(position: number): TableRead | undefined;
  /** Names an entry's `*` covers (by FROM position), as `[virtual index, name]`. */
  columnsOf(
    position: number,
  ): Array<{ readonly index: number; readonly name: string; readonly typed: Typed }>;
  /** Builds the tree under the WHERE conjuncts (typed in `scope`). */
  build(where: readonly Typed[]): Unit;
}

export interface FromOptions {
  readonly outer?: OuterResolver;
  /** Entries (by FROM position) that must also output their row locator. */
  readonly locators?: readonly number[];
  /** Table entries (by FROM position) that read every column (a MERGE target's row). */
  readonly fullRows?: readonly number[];
  readonly lowerBlock: BlockLowerer;
}

/** The FROM clause `[base, ...joins]`. */
export function lowerFrom(
  base: TableSource,
  joins: readonly JoinPlan[],
  builder: PlanBuilder,
  options: FromOptions,
): FromClause {
  const lowerInner = options.lowerBlock;
  const fullAt = joins.reduce((last, join, index) => (join.full === true ? index : last), -1);
  const entries: Entry[] = [base, ...joins].map((source, position) => {
    const join = position === 0 ? null : (joins[position - 1] ?? null);
    const kind: EntryKind =
      join === null
        ? "base"
        : join.full === true
          ? "full"
          : join.kind === "inner"
            ? "inner"
            : join.kind;
    // A source before a FULL join is null-supplied too.
    const nullSupplied =
      kind === "left" || kind === "full" || (fullAt !== -1 && position <= fullAt);
    const id = builder.source();
    const qualifiers = [source.alias];
    if (source.lateral === true) throw unsupported("lateral", "LATERAL");
    const derived = (inner: LoweredRel) => {
      const aliases = source.columnAliases;
      const renamed =
        aliases === undefined
          ? inner
          : {
              ...inner,
              columns: inner.columns.map((col, index) => ({
                ...col,
                name: aliases[index] ?? col.name,
              })),
            };
      return relationEntry(id, kind, nullSupplied, qualifiers, renamed, builder);
    };
    if (source.derived !== undefined)
      return derived(lowerInner(source.derived, builder, options.outer));
    if (source.union !== undefined)
      return derived(lowerSetOperation(source.union, builder, lowerInner, options.outer));
    if (source.recursive !== undefined)
      return derived(lowerRecursive(source.recursive, builder, lowerInner));
    if (source.windowed !== undefined)
      return derived(lowerWindowed(source.windowed, builder, lowerInner, options.outer));
    const work = builder.workTables.get(source.table);
    if (work !== undefined)
      return derived({
        rel: {
          op: "workTableScan",
          id: builder.id(),
          workTable: work.id,
          output: outputColumns(work.columns),
        },
        columns: work.columns,
      });
    if (source.table === DUAL_TABLE)
      return derived({
        rel: { op: "values", id: builder.id(), rows: [[]], output: [] },
        columns: [],
        rows: 1,
      });
    const table = builder.catalog.tableByName(source.table);
    if (table === undefined)
      throw new PlanningError(`relation "${source.table}" does not exist`, "42P01");
    return tableEntry(id, kind, nullSupplied, table, source.alias, builder);
  });
  const byQualifier = (qualifier: string) =>
    entries.filter((entry) => entry.qualifiers.includes(qualifier));
  const typedOf = (entry: Entry, found: Resolved): Typed => ({
    expr: column(virtual(entry.source, found.local), found.type),
    nullable: found.nullable || entry.nullSupplied,
    big: found.big,
    ...(found.collation === undefined ? {} : { collation: found.collation }),
  });
  const attachments: Attachment[] = [];

  const scope: Scope = {
    column: (reference) => {
      const dot = reference.indexOf(".");
      if (dot !== -1) {
        const qualified = byQualifier(reference.slice(0, dot));
        for (const entry of qualified) {
          const found = entry.resolve(reference.slice(dot + 1));
          if (found !== undefined) return typedOf(entry, found);
        }
      }
      const matches = entries.flatMap((entry) => {
        const found = entry.resolve(reference);
        return found === undefined ? [] : [typedOf(entry, found)];
      });
      if (matches.length > 1)
        throw new PlanningError(`column reference "${reference}" is ambiguous`, "42702");
      return matches[0] ?? options.outer?.(reference);
    },
    knowsQualifier: (qualifier) =>
      byQualifier(qualifier).length > 0 || (options.outer?.knowsQualifier?.(qualifier) ?? false),
    subquery: (request) => attachSubquery(request),
  };

  /** A subquery attached to this clause's rows (see the module comment). */
  function attachSubquery(request: SubqueryRequest): Typed {
    const mark = request.kind === "quantified" ? markJoin(request) : null;
    if (mark !== null) return mark;
    const bindings: Array<{ readonly slot: number; readonly typed: Typed }> = [];
    const resolver: OuterResolver = Object.assign(
      (reference: string): Typed | undefined => {
        const outer = scope.column(reference);
        if (outer === undefined) return undefined;
        const slot = builder.slot(outer.expr.type);
        bindings.push({ slot, typed: outer });
        return {
          expr: { op: "param", slot, type: outer.expr.type },
          nullable: outer.nullable,
          big: outer.big,
        };
      },
      { knowsQualifier: (qualifier: string) => scope.knowsQualifier?.(qualifier) ?? false },
    );
    const state = builder.state;
    const wasVolatile = state.volatile;
    state.volatile = false;
    const inner = lowerInner(request.block, builder, resolver);
    const innerVolatile = state.volatile;
    state.volatile ||= wasVolatile;
    if (request.kind !== "exists" && inner.columns.length !== 1)
      throw new PlanningError("subquery must return only one column", "42601");
    if (request.kind === "scalar" && bindings.length === 0) return builder.initPlan(inner);
    const id = builder.source();
    const innerColumn = inner.columns[0];
    let innerRel = inner.rel;
    let quantified: Extract<Rel, { op: "apply" }>["quantified"] = null;
    if (request.kind === "quantified") {
      if (innerColumn === undefined)
        throw new PlanningError("subquery must return only one column", "42601");
      const innerTyped: Typed = {
        expr: column(0, innerColumn.type),
        nullable: innerColumn.nullable,
        big: innerColumn.big,
      };
      const type = baseType(
        commonType([request.test, innerTyped], builder.parameters, "subquery comparison"),
      );
      const test = coerce(request.test, type, "implicit", builder.parameters);
      const cast = coerce(innerTyped, type, "implicit", builder.parameters);
      if (cast.expr.op !== "column")
        innerRel = {
          op: "project",
          id: builder.id(),
          input: innerRel,
          exprs: [cast.expr],
          output: [
            { name: innerColumn.name, type: cast.expr.type, nullable: innerColumn.nullable },
          ],
        };
      quantified = {
        test: test.expr,
        cmp: request.cmp,
        quantifier: request.quantifier,
        collation: request.test.collation ?? null,
      };
    }
    const result: RelColumn =
      request.kind === "scalar"
        ? {
            name: "(subquery)",
            type: innerColumn?.type ?? { kind: "text" },
            nullable: true,
            big: innerColumn?.big ?? false,
          }
        : {
            name: "(subquery)",
            type: { kind: "bool" },
            nullable: request.kind !== "exists",
            big: false,
          };
    // An uncorrelated EXISTS or IN can materialize its complete inner plan once and
    // test it with a mark join. No executor-side apply or per-row re-execution is needed.
    const directExists = bindings.length === 0 && request.kind === "exists";
    const directIn =
      bindings.length === 0 &&
      request.kind === "quantified" &&
      ((request.cmp === "=" && request.quantifier === "any") ||
        (request.cmp === "<>" && request.quantifier === "all"));
    const hashedIn = byteCollation(quantified?.collation);
    const markedInner: Rel = directExists
      ? {
          op: "project",
          id: builder.id(),
          input: innerRel,
          exprs: [boolLiteral(true)],
          output: [{ name: "(exists row)", type: { kind: "bool" }, nullable: false }],
        }
      : innerRel;
    attachments.push({
      source: id,
      column: result,
      attach: (acc, position) =>
        directExists || directIn
          ? {
              op: "join",
              id: builder.id(),
              kind: "mark",
              algorithm: "hash",
              left: acc.rel,
              right: markedInner,
              leftKeys: [
                directExists || !hashedIn
                  ? boolLiteral(true)
                  : mapColumns(required(quantified).test, position),
              ],
              rightKeys: [
                directIn && !hashedIn
                  ? boolLiteral(true)
                  : column(0, required(markedInner.output[0]).type),
              ],
              nullSafe: [false],
              correlated: 0,
              filter: null,
              residual:
                directIn && !hashedIn
                  ? {
                      op: "compare",
                      cmp: "=",
                      left: mapColumns(required(quantified).test, position),
                      right: column(acc.columns.length, required(markedInner.output[0]).type),
                      collation: required(quantified).collation,
                      type: { kind: "bool" },
                    }
                  : null,
              publishes: [],
              output: [...outputColumns(acc.columns), ...outputColumns([result])],
            }
          : {
              op: "apply",
              id: builder.id(),
              mode: request.kind,
              outer: acc.rel,
              inner: innerRel,
              bindings: bindings.map(({ slot, typed }) => ({
                slot,
                expr: mapColumns(typed.expr, position),
              })),
              quantified:
                quantified === null
                  ? null
                  : { ...quantified, test: mapColumns(quantified.test, position) },
              memoize: !innerVolatile,
              output: [...outputColumns(acc.columns), ...outputColumns([result])],
            },
    });
    return {
      expr:
        directIn && request.quantifier === "all"
          ? { op: "not", arg: column(virtual(id, 0), result.type), type: { kind: "bool" } }
          : column(virtual(id, 0), result.type),
      nullable: result.nullable,
      big: result.big,
    };
  }

  /** `x [NOT] IN (simple subquery)` as a mark join (R20.5), or null when the block is not simple. */
  function markJoin(request: Extract<SubqueryRequest, { kind: "quantified" }>): Typed | null {
    const block = request.block;
    const simple =
      ((request.cmp === "=" && request.quantifier === "any") ||
        (request.cmp === "<>" && request.quantifier === "all")) &&
      block.select.length === 1 &&
      block.groupBy.length === 0 &&
      block.having.length === 0 &&
      block.orderBy.length === 0 &&
      block.limit === undefined &&
      block.limitParameter === undefined &&
      block.offset === undefined &&
      block.offsetParameter === undefined &&
      !hasSubqueryOrAggregate(block);
    if (!simple) return null;
    // Outer references are typed directly (virtual ids are statement-wide): conditions over both.
    const inner = lowerFrom(block.base, block.joins, builder, {
      outer: (reference) => scope.column(reference),
      lowerBlock: lowerInner,
    });
    const state = builder.state;
    const where = block.predicates.map((predicate) =>
      asBool(
        lowerCondition(
          predicate.operator,
          predicate.left,
          predicate.right,
          predicate.escape,
          inner.scope,
          state,
        ),
        state,
      ),
    );
    const selected = block.select[0];
    if (selected === undefined) return null;
    const tested = settle(
      lowerExpression(selected.expression, inner.scope, state),
      builder.parameters,
    );
    const innerSources = new Set(inner.sources);
    const outerOnly = (expr: ScalarExpr) =>
      [...sourcesOf(expr)].every((source) => !innerSources.has(source));
    const innerOnly = (expr: ScalarExpr) => subset(sourcesOf(expr), innerSources);
    if (!innerOnly(tested.expr) || !outerOnly(request.test.expr)) return null;
    const type = baseType(commonType([request.test, tested], builder.parameters, "IN"));
    const probeKey = coerce(request.test, type, "implicit", builder.parameters);
    const buildKey = coerce(tested, type, "implicit", builder.parameters);
    const collation = request.test.collation ?? (tested as Collated).collation ?? null;
    const hashedTest = byteCollation(collation);
    const correlated: Array<{ probe: ScalarExpr; build: ScalarExpr }> = [];
    const filter: ScalarExpr[] = [];
    const pushed: Typed[] = [];
    for (const conjunct of where.flatMap(conjunctsOf)) {
      const expr = conjunct.expr;
      if (innerOnly(expr)) {
        pushed.push(conjunct);
        continue;
      }
      if (expr.op === "compare" && expr.cmp === "=" && byteCollation(expr.collation)) {
        if (outerOnly(expr.left) && innerOnly(expr.right)) {
          correlated.push({ probe: expr.left, build: expr.right });
          continue;
        }
        if (innerOnly(expr.left) && outerOnly(expr.right)) {
          correlated.push({ probe: expr.right, build: expr.left });
          continue;
        }
      }
      filter.push(expr);
    }
    const build = inner.build(pushed);
    const id = builder.source();
    const result: RelColumn = {
      name: "(mark)",
      type: { kind: "bool" },
      nullable: true,
      big: false,
    };
    attachments.push({
      source: id,
      column: result,
      attach: (acc, position) => {
        const buildPosition = (index: number) => {
          const found = build.layout.indexOf(index);
          if (found === -1) throw new PlanningError("mark join column vanished", "XX000");
          return found;
        };
        const both = (index: number) =>
          innerSources.has(sourceOf(index))
            ? acc.columns.length + buildPosition(index)
            : position(index);
        return {
          op: "join",
          id: builder.id(),
          kind: "mark",
          algorithm: "hash",
          left: acc.rel,
          right: build.rel,
          leftKeys: [
            ...correlated.map((key) => mapColumns(key.probe, position)),
            hashedTest ? mapColumns(probeKey.expr, position) : boolLiteral(true),
          ],
          rightKeys: [
            ...correlated.map((key) => mapColumns(key.build, buildPosition)),
            hashedTest ? mapColumns(buildKey.expr, buildPosition) : boolLiteral(true),
          ],
          nullSafe: [...correlated.map(() => false), false],
          correlated: correlated.length,
          filter:
            filter.length === 0
              ? null
              : mapColumns(
                  required(andOf(filter.map((expr) => ({ expr, nullable: true, big: false })))),
                  both,
                ),
          residual: hashedTest
            ? null
            : {
                op: "compare",
                cmp: "=",
                left: mapColumns(probeKey.expr, position),
                right: mapColumns(
                  buildKey.expr,
                  (index) => acc.columns.length + buildPosition(index),
                ),
                collation,
                type: { kind: "bool" },
              },
          publishes: [],
          output: [...outputColumns(acc.columns), ...outputColumns([result])],
        };
      },
    });
    const mark: ScalarExpr = column(virtual(id, 0), result.type);
    return {
      expr: request.quantifier === "all" ? { op: "not", arg: mark, type: { kind: "bool" } } : mark,
      nullable: true,
      big: false,
    };
  }

  // Join conditions, typed once every entry exists.
  const joinConditions = new Map<number, Typed[]>();
  for (const [position, join] of joins.entries()) {
    const entry = entries[position + 1];
    if (entry === undefined) continue;
    const conditions: Typed[] = [];
    if (!isCrossJoinPlan(join)) {
      const keyed = !(
        join.left.kind === "literal" &&
        join.left.value === null &&
        join.right.kind === "literal" &&
        join.right.value === null
      );
      if (keyed)
        conditions.push(
          asBool(
            lowerCondition("=", join.left, join.right, undefined, scope, builder.state),
            builder.state,
          ),
        );
      if (join.on !== undefined)
        conditions.push(
          ...conjunctsOf(asBool(lowerExpression(join.on, scope, builder.state), builder.state)),
        );
    }
    joinConditions.set(entry.source, conditions);
  }

  const sources = entries.map((entry) => entry.source);
  for (const position of options.fullRows ?? []) entries[position]?.read?.useAll();
  const locators = new Set(
    (options.locators ?? []).map((position) => entries[position]?.source ?? -1),
  );

  return {
    scope,
    sources,
    locator: (position) => virtual(entries[position]?.source ?? -1, LOCATOR_LOCAL),
    tableRead: (position) => entries[position]?.read,
    columnsOf: (position) => {
      const entry = entries[position];
      if (entry === undefined) return [];
      return entry.names().flatMap((name) => {
        const found = entry.resolve(name);
        if (found === undefined) return [];
        const typed = typedOf(entry, found);
        return [{ index: virtual(entry.source, found.local), name, typed }];
      });
    },
    wildcard: (qualifier) =>
      entries
        .filter(
          (entry) =>
            entry.kind !== "semi" &&
            entry.kind !== "anti" &&
            (qualifier === undefined || entry.qualifiers.includes(qualifier)),
        )
        .flatMap((entry) =>
          entry.names().map((name): Expression => ({
            kind: "column",
            reference: `${entry.qualifiers[0] ?? ""}.${name}`,
          })),
        ),
    build: (where) => {
      const pool: Conjunct[] = [];
      const add = (typed: Typed) => {
        for (const part of conjunctsOf(typed))
          pool.push({ typed: part, sources: sourcesOf(part.expr), used: false });
      };
      // WHERE sees the completed outer-join tree. Any conjunct reading a null-supplied
      // source stays above every barrier, even if an earlier inner group already covers it.
      const nullableSources = new Set(
        entries.filter((entry) => entry.nullSupplied).map((entry) => entry.source),
      );
      const deferred: Typed[] = [];
      for (const typed of where)
        for (const part of conjunctsOf(typed)) {
          if ([...sourcesOf(part.expr)].some((source) => nullableSources.has(source)))
            deferred.push(part);
          else add(part);
        }
      const unitOf = (entry: Entry, extra: readonly Typed[]): Unit => {
        const pushed: Typed[] = [...extra];
        if (!entry.nullSupplied)
          for (const conjunct of pool)
            if (
              !conjunct.used &&
              conjunct.sources.size === 1 &&
              conjunct.sources.has(entry.source)
            ) {
              conjunct.used = true;
              pushed.push(conjunct.typed);
            }
        const local = pushed.map((typed) => ({
          ...typed,
          expr: mapColumns(typed.expr, (index) => index - entry.source * SOURCE_STRIDE),
        }));
        return entry.build(local, locators.has(entry.source));
      };
      let acc: Unit | null = null;
      let group: Unit[] = [];
      for (const entry of entries) {
        if (entry.kind === "base" || entry.kind === "inner") {
          // An INNER ON belongs to this group. A later one-sided condition must not filter
          // the input of an earlier FULL/LEFT join before that join supplies its NULL rows.
          for (const typed of joinConditions.get(entry.source) ?? []) add(typed);
          group.push(unitOf(entry, []));
          continue;
        }
        acc = orderInner(acc === null ? group : [acc, ...group], pool, builder, entries);
        group = [];
        const conditions = joinConditions.get(entry.source) ?? [];
        const own = conditions.filter(
          (typed) =>
            entry.kind !== "full" && subset(sourcesOf(typed.expr), new Set([entry.source])),
        );
        const unit = unitOf(entry, own);
        const rest = conditions.filter((typed) => !own.includes(typed));
        acc = outerJoin(acc, unit, entry.kind, rest, builder);
      }
      acc = orderInner(acc === null ? group : [acc, ...group], pool, builder, entries);
      for (const attachment of attachments) {
        const current: Unit = acc;
        const position = positionIn(current.layout);
        acc = {
          rel: attachment.attach(current, position),
          layout: [...current.layout, virtual(attachment.source, 0)],
          columns: [...current.columns, attachment.column],
          rows: current.rows,
          sources: new Set([...current.sources, attachment.source]),
          pointRead: false,
        };
      }
      const remaining = pool.filter((conjunct) => !conjunct.used);
      for (const conjunct of remaining) conjunct.used = true;
      return filterUnit(
        acc,
        [...remaining.map((conjunct) => conjunct.typed), ...deferred],
        builder,
      );
    },
  };
}

function hasSubqueryOrAggregate(block: CompiledQuery): boolean {
  const walk = (expression: Expression): boolean => {
    switch (expression.kind) {
      case "subquery":
      case "exists":
      case "window":
        return true;
      case "call":
        return (
          ["COUNT", "SUM", "AVG", "MIN", "MAX", "STRING_AGG"].includes(expression.name) ||
          expression.arguments.some(walk)
        );
      case "binary":
      case "condition":
      case "logical":
        return walk(expression.left) || walk(expression.right);
      case "not":
        return walk(expression.operand);
      case "case":
        return (
          expression.branches.some((branch) => walk(branch.when) || walk(branch.then)) ||
          (expression.otherwise !== undefined && walk(expression.otherwise))
        );
      case "list":
        return expression.items.some(walk);
      default:
        return false;
    }
  };
  return (
    block.select.some((item) => walk(item.expression)) ||
    block.predicates.some((predicate) => walk(predicate.left) || walk(predicate.right)) ||
    [block.base, ...block.joins].some(
      (source) =>
        source.derived !== undefined ||
        source.union !== undefined ||
        source.recursive !== undefined ||
        source.windowed !== undefined,
    )
  );
}

/** Position of a virtual index in a layout. */
function positionIn(layout: readonly number[]): (index: number) => number {
  const positions = new Map(layout.map((index, position) => [index, position]));
  return (index) => {
    const position = positions.get(index);
    if (position === undefined)
      throw new PlanningError(`column ${String(index)} is not in this relation`, "XX000");
    return position;
  };
}

function filterUnit(unit: Unit, conjuncts: readonly Typed[], builder: PlanBuilder): Unit {
  if (conjuncts.length === 0) return unit;
  const position = positionIn(unit.layout);
  const predicate = andOf(
    conjuncts.map((typed) => ({ ...typed, expr: mapColumns(typed.expr, position) })),
  );
  if (predicate === null) return unit;
  return {
    ...unit,
    rel: { op: "filter", id: builder.id(), input: unit.rel, predicate, output: unit.rel.output },
    rows: Math.max(1, unit.rows * selectivityOf(conjuncts)),
    pointRead: false,
  };
}

/** Equality conditions between `left` and `right` (as key pairs) and the rest. */
function splitConditions(
  conditions: readonly Typed[],
  left: ReadonlySet<number>,
  right: ReadonlySet<number>,
): { keys: Array<{ left: ScalarExpr; right: ScalarExpr }>; rest: Typed[] } {
  const keys: Array<{ left: ScalarExpr; right: ScalarExpr }> = [];
  const rest: Typed[] = [];
  for (const typed of conditions) {
    const expr = typed.expr;
    if (expr.op === "compare" && expr.cmp === "=" && byteCollation(expr.collation)) {
      const l = sourcesOf(expr.left);
      const r = sourcesOf(expr.right);
      if (l.size > 0 && r.size > 0 && subset(l, left) && subset(r, right)) {
        keys.push({ left: expr.left, right: expr.right });
        continue;
      }
      if (l.size > 0 && r.size > 0 && subset(l, right) && subset(r, left)) {
        keys.push({ left: expr.right, right: expr.left });
        continue;
      }
    }
    rest.push(typed);
  }
  return { keys, rest };
}

const JOIN_KINDS = {
  left: "left",
  semi: "semi",
  anti: "anti",
  full: "full",
  inner: "inner",
} as const;

function joinUnits(
  left: Unit,
  right: Unit,
  kind: "inner" | "left" | "semi" | "anti" | "full",
  conditions: readonly Typed[],
  rows: number,
  builder: PlanBuilder,
): Unit {
  const { keys, rest } = splitConditions(conditions, left.sources, right.sources);
  const leftPosition = positionIn(left.layout);
  const rightPosition = positionIn(right.layout);
  const bothLayout = [...left.layout, ...right.layout];
  const both = positionIn(bothLayout);
  const residual = andOf(rest.map((typed) => ({ ...typed, expr: mapColumns(typed.expr, both) })));
  const keep = kind === "semi" || kind === "anti";
  const nullable = (columns: readonly RelColumn[], extended: boolean) =>
    extended ? columns.map((col) => ({ ...col, nullable: true })) : columns;
  const columns = keep
    ? left.columns
    : [
        ...nullable(left.columns, kind === "full"),
        ...nullable(right.columns, kind === "left" || kind === "full"),
      ];
  return {
    rel: {
      op: "join",
      id: builder.id(),
      kind: kind === "inner" && keys.length === 0 && residual === null ? "cross" : JOIN_KINDS[kind],
      algorithm: keys.length > 0 ? "hash" : "nestedLoop",
      left: left.rel,
      right: right.rel,
      leftKeys: keys.map((key) => mapColumns(key.left, leftPosition)),
      rightKeys: keys.map((key) => mapColumns(key.right, rightPosition)),
      nullSafe: keys.map(() => false),
      residual,
      publishes: [],
      output: outputColumns(columns),
    },
    layout: keep ? left.layout : bothLayout,
    columns,
    rows: Math.max(1, rows),
    sources: keep ? left.sources : new Set([...left.sources, ...right.sources]),
    pointRead: false,
  };
}

function outerJoin(
  acc: Unit,
  unit: Unit,
  kind: "left" | "semi" | "anti" | "full" | "base" | "inner",
  conditions: readonly Typed[],
  builder: PlanBuilder,
): Unit {
  if (kind === "base" || kind === "inner") throw new PlanningError("not an outer join", "XX000");
  const estimate = Math.max(1, (acc.rows * unit.rows) / Math.max(acc.rows, unit.rows));
  const rows =
    kind === "left"
      ? Math.max(acc.rows, estimate)
      : kind === "full"
        ? acc.rows + unit.rows
        : Math.max(1, acc.rows / 2);
  return joinUnits(acc, unit, kind, conditions, rows, builder);
}

/** PostgreSQL's DEFAULT_NUM_DISTINCT: distinct values assumed for a column with no statistics. */
const DEFAULT_NUM_DISTINCT = 200;

/**
 * Distinct estimate of one side of an equality: statistics, else a unique key's table rows,
 * else PostgreSQL's default of 200 (never more than the side's rows). So a foreign key joined to
 * its parent's key estimates the child's rows, as `L × R / R_parent`.
 */
function distinctOf(
  expr: ScalarExpr,
  units: readonly Unit[],
  entries: readonly Entry[],
): { readonly distinct: number; readonly known: boolean } {
  const unit = units.find((candidate) => subset(sourcesOf(expr), candidate.sources));
  const rows = unit?.rows ?? DEFAULT_TABLE_ROWS;
  const fallback = { distinct: Math.min(rows, DEFAULT_NUM_DISTINCT), known: false };
  if (expr.op !== "column") return fallback;
  const entry = entries.find((candidate) => candidate.source === sourceOf(expr.index));
  const distinct = entry?.distinct(expr.index - entry.source * SOURCE_STRIDE) ?? null;
  return distinct === null ? fallback : { distinct: Math.max(1, distinct), known: true };
}

/**
 * Selectivity of an equality: `1 / max(NDV_L, NDV_R)`, except that a side whose distinct count
 * is only the default never outweighs a known one (a foreign key's values are the key's).
 */
function equalitySelectivity(
  left: ReturnType<typeof distinctOf>,
  right: ReturnType<typeof distinctOf>,
): number {
  if (left.known && !right.known) return 1 / left.distinct;
  if (right.known && !left.known) return 1 / right.distinct;
  return 1 / Math.max(left.distinct, right.distinct, 1);
}

/**
 * Joins `units` with inner joins in the cheapest order the cost model finds, applying every
 * pool conjunct as soon as its sources are joined (see the module comment).
 */
function orderInner(
  units: readonly Unit[],
  pool: Conjunct[],
  builder: PlanBuilder,
  entries: readonly Entry[],
): Unit {
  const applyCovered = (unit: Unit): Unit => {
    const covered = pool.filter(
      (conjunct) =>
        !conjunct.used && conjunct.sources.size > 0 && subset(conjunct.sources, unit.sources),
    );
    for (const conjunct of covered) conjunct.used = true;
    return filterUnit(
      unit,
      covered.map((conjunct) => conjunct.typed),
      builder,
    );
  };
  if (units.length === 0) throw new PlanningError("empty FROM", "XX000");
  const ready = units.map(applyCovered);
  if (ready.length === 1) return at(ready, 0);
  // Join order and build sides come from the estimates.
  builder.dependsOnStatistics();
  const n = ready.length;
  // Every open conjunct is planned by the units it reads (indices into `ready`), computed once.
  const unitOfSource = new Map<number, number>();
  ready.forEach((unit, index) => {
    for (const source of unit.sources) unitOfSource.set(source, index);
  });
  /** The units covering `sources`, or null when one is outside this group (never applies). */
  const unitsOf = (sources: ReadonlySet<number>): number[] | null => {
    const units = new Set<number>();
    for (const source of sources) {
      const unit = unitOfSource.get(source);
      if (unit === undefined) return null;
      units.add(unit);
    }
    return [...units];
  };
  const planned: PlannedConjunct[] = [];
  const keys: PlannedKey[] = [];
  for (const conjunct of pool) {
    if (conjunct.used) continue;
    const units = unitsOf(conjunct.sources);
    if (units === null) continue;
    const expr = conjunct.typed.expr;
    const equality = expr.op === "compare" && expr.cmp === "=";
    planned.push({
      units,
      selectivity: equality
        ? equalitySelectivity(
            distinctOf(expr.left, ready, entries),
            distinctOf(expr.right, ready, entries),
          )
        : 1 / 3,
    });
    if (!equality) continue;
    const left = unitsOf(sourcesOf(expr.left));
    const right = unitsOf(sourcesOf(expr.right));
    if (left !== null && right !== null && left.length > 0 && right.length > 0)
      keys.push({ left, right });
  }
  const steps =
    n <= EXHAUSTIVE_JOIN_LIMIT
      ? exhaustiveOrder(ready, planned, keys, builder.costs)
      : greedyOrder(ready, planned, keys, builder.costs);
  const [first, ...rest] = steps;
  let acc = at(ready, first?.unit ?? 0);
  for (const step of rest) {
    const unit = at(ready, step.unit);
    const sources = new Set([...acc.sources, ...unit.sources]);
    const covered = pool.filter(
      (conjunct) =>
        !conjunct.used && conjunct.sources.size > 0 && subset(conjunct.sources, sources),
    );
    for (const conjunct of covered) conjunct.used = true;
    const conditions = covered.map((conjunct) => conjunct.typed);
    // The smaller estimate is the build side (IR `right`).
    acc =
      unit.rows <= acc.rows
        ? joinUnits(acc, unit, "inner", conditions, step.rows, builder)
        : joinUnits(unit, acc, "inner", conditions, step.rows, builder);
  }
  return acc;
}

/** An open conjunct as join ordering sees it: the units it reads and its selectivity. */
interface PlannedConjunct {
  readonly units: readonly number[];
  readonly selectivity: number;
}

/** An equality whose sides read disjoint units: a hash key once each side is on one side. */
interface PlannedKey {
  readonly left: readonly number[];
  readonly right: readonly number[];
}

/** One step of a left-deep order: the unit joined and the estimated rows after it. */
interface JoinStep {
  readonly unit: number;
  readonly rows: number;
}

/** Cost of joining `left` rows with `right` rows into `out`: hash when keyed, else nested loop. */
function stepCost(
  left: number,
  right: number,
  out: number,
  keyed: boolean,
  costs: ExecutorCosts,
): number {
  return keyed
    ? Math.min(left, right) * costs.hashBuildPerRow +
        Math.max(left, right) * costs.hashProbePerRow +
        out * costs.emitPerValue
    : left * right * costs.filterPerRow + out * costs.emitPerValue;
}

/**
 * The cheapest left-deep order of at most `EXHAUSTIVE_JOIN_LIMIT` units, by dynamic programming
 * over unit sets (as bit masks). Only connected extensions are taken while one exists, so a cross
 * product appears only when nothing connects (design-joins-agg-spill.md).
 */
function exhaustiveOrder(
  ready: readonly Unit[],
  planned: readonly PlannedConjunct[],
  keys: readonly PlannedKey[],
  costs: ExecutorCosts,
): JoinStep[] {
  const n = ready.length;
  const maskOf = (units: readonly number[]) => units.reduce((mask, unit) => mask | (1 << unit), 0);
  const conjuncts = planned.map((conjunct) => ({
    mask: maskOf(conjunct.units),
    selectivity: conjunct.selectivity,
  }));
  const keyMasks = keys.map((key) => ({ left: maskOf(key.left), right: maskOf(key.right) }));
  const within = (inner: number, outer: number) => (inner & ~outer) === 0;
  const full = (1 << n) - 1;
  const rows = new Float64Array(full + 1);
  for (let mask = 1; mask <= full; mask += 1) {
    let estimate = 1;
    for (let index = 0; index < n; index += 1)
      if ((mask & (1 << index)) !== 0) estimate *= at(ready, index).rows;
    for (const conjunct of conjuncts)
      if (within(conjunct.mask, mask)) estimate *= conjunct.selectivity;
    rows[mask] = Math.max(1, estimate);
  }
  const connects = (mask: number, bit: number) =>
    keyMasks.some(
      (key) =>
        (within(key.left, mask) && within(key.right, bit)) ||
        (within(key.right, mask) && within(key.left, bit)),
    );
  const cost = new Float64Array(full + 1).fill(Infinity);
  const last = new Int8Array(full + 1).fill(-1);
  for (let index = 0; index < n; index += 1) {
    cost[1 << index] = 0;
    last[1 << index] = index;
  }
  const outside: number[] = [];
  const connected: number[] = [];
  for (let mask = 1; mask <= full; mask += 1) {
    const current = at(cost, mask);
    if (current === Infinity) continue;
    outside.length = 0;
    connected.length = 0;
    for (let index = 0; index < n; index += 1) {
      const bit = 1 << index;
      if ((mask & bit) !== 0) continue;
      outside.push(index);
      if (connects(mask, bit)) connected.push(index);
    }
    const keyed = connected.length > 0;
    for (const index of keyed ? connected : outside) {
      const next = mask | (1 << index);
      const total =
        current + stepCost(at(rows, mask), at(ready, index).rows, at(rows, next), keyed, costs);
      if (total < at(cost, next)) {
        cost[next] = total;
        last[next] = index;
      }
    }
  }
  const steps: JoinStep[] = [];
  for (let mask = full; mask !== 0;) {
    const unit = at(last, mask);
    steps.push({ unit, rows: at(rows, mask) });
    mask &= ~(1 << unit);
  }
  return steps.reverse();
}

/**
 * A left-deep order of any number of units, greedily: start from the smallest, then repeatedly
 * join the cheapest connected unit (a cross product only when nothing connects). Row estimates
 * are kept incrementally, so the order costs O(n² × conjuncts) with no limit on n.
 */
function greedyOrder(
  ready: readonly Unit[],
  planned: readonly PlannedConjunct[],
  keys: readonly PlannedKey[],
  costs: ExecutorCosts,
): JoinStep[] {
  const n = ready.length;
  const placed = new Uint8Array(n);
  const isPlaced = (unit: number) => placed[unit] === 1;
  const within = (units: readonly number[], extra: number) =>
    units.every((unit) => unit === extra || isPlaced(unit));
  const start = ready.reduce(
    (min, unit, index) => (unit.rows < at(ready, min).rows ? index : min),
    0,
  );
  placed[start] = 1;
  // Conjuncts over no unit, or only the start, apply from the first step.
  let rows = at(ready, start).rows;
  for (const conjunct of planned) if (within(conjunct.units, start)) rows *= conjunct.selectivity;
  rows = Math.max(1, rows);
  const steps: JoinStep[] = [{ unit: start, rows }];
  while (steps.length < n) {
    let choice = -1;
    let choiceCost = Infinity;
    let choiceRows = 0;
    for (let index = 0; index < n; index += 1) {
      if (isPlaced(index)) continue;
      const alone = (units: readonly number[]) => units.every((unit) => unit === index);
      const keyed = keys.some(
        (key) =>
          (key.left.every(isPlaced) && alone(key.right)) ||
          (key.right.every(isPlaced) && alone(key.left)),
      );
      let out = rows * at(ready, index).rows;
      for (const conjunct of planned)
        if (conjunct.units.includes(index) && within(conjunct.units, index))
          out *= conjunct.selectivity;
      out = Math.max(1, out);
      const cost =
        stepCost(rows, at(ready, index).rows, out, keyed, costs) +
        (keyed ? 0 : Number.MAX_SAFE_INTEGER / 4);
      if (cost < choiceCost) {
        choice = index;
        choiceCost = cost;
        choiceRows = out;
      }
    }
    placed[choice] = 1;
    rows = choiceRows;
    steps.push({ unit: choice, rows });
  }
  return steps;
}

/** UNION / INTERSECT / EXCEPT [ALL] of blocks, left to right, columns unified by type. */
export function lowerSetOperation(
  union: NonNullable<TableSource["union"]>,
  builder: PlanBuilder,
  lowerInner: BlockLowerer,
  outer: OuterResolver | undefined,
): LoweredRel {
  const [first, ...rest] = union.blocks.map((block) => lowerInner(block, builder, outer));
  if (first === undefined) throw new PlanningError("empty set operation", "XX000");
  let acc: LoweredRel = first;
  for (const [index, next] of rest.entries()) {
    const op = union.ops[index] ?? "union";
    if (next.columns.length !== acc.columns.length)
      throw new PlanningError(
        `each ${op.toUpperCase()} query must have the same number of columns`,
        "42601",
      );
    // PostgreSQL resolves each pair of a set operation: an unknown-typed constant (NULL, a
    // string literal) of one side takes the other side's type, and two unknowns are text. So an
    // arm's unknowns are resolved here, against its partner, never settled first.
    const side = (rel: LoweredRel, position: number): Typed => {
      const col = at(rel.columns, position);
      return (
        rel.pending?.[position] ?? {
          expr: column(position, col.type),
          nullable: col.nullable,
          big: col.big,
        }
      );
    };
    const types = acc.columns.map((_, position) =>
      commonType([side(acc, position), side(next, position)], builder.parameters, op.toUpperCase()),
    );
    const conform = (input: LoweredRel): LoweredRel => {
      const casts = input.columns.map((_, position) =>
        coerce(side(input, position), at(types, position), "implicit", builder.parameters),
      );
      if (casts.every((cast) => cast.expr.op === "column")) return input;
      const columns = input.columns.map((col, position) => ({
        ...col,
        type: casts[position]?.expr.type ?? col.type,
      }));
      return {
        rel: {
          op: "project",
          id: builder.id(),
          input: input.rel,
          exprs: casts.map((cast) => cast.expr),
          output: outputColumns(columns),
        },
        columns,
        ...(input.rows === undefined ? {} : { rows: input.rows }),
      };
    };
    const left = conform(acc);
    const right = conform(next);
    const columns = left.columns.map((col, position) => ({
      ...col,
      nullable: col.nullable || (right.columns[position]?.nullable ?? true),
      big: col.big || (right.columns[position]?.big ?? false),
    }));
    const [kind, all] = op.split(" ") as ["union" | "intersect" | "except", string | undefined];
    acc = {
      rel: {
        op: "setOp",
        id: builder.id(),
        kind,
        all: all === "all",
        inputs: [left.rel, right.rel],
        output: outputColumns(columns),
      },
      columns,
      rows:
        (left.rows ?? DEFAULT_TABLE_ROWS) +
        (kind === "union" ? (right.rows ?? DEFAULT_TABLE_ROWS) : 0),
    };
  }
  const rows = constantRows(acc.rel);
  return rows === null
    ? acc
    : { ...acc, rel: { op: "values", id: builder.id(), rows, output: outputColumns(acc.columns) } };
}

/** WITH RECURSIVE (and GENERATE_SERIES): base, then the step over the work table, to a fixpoint. */
export function lowerRecursive(
  recursive: NonNullable<TableSource["recursive"]>,
  builder: PlanBuilder,
  lowerInner: BlockLowerer,
): LoweredRel {
  const base = lowerInner(recursive.base, builder);
  const workTable = builder.workTable();
  // The work table's columns are the base's, nullable (a step may produce NULLs).
  const columns = base.columns.map((col) => ({ ...col, nullable: true }));
  builder.workTables.set(recursive.reference, { id: workTable, columns });
  const step = lowerInner(recursive.step, builder);
  builder.workTables.delete(recursive.reference);
  if (step.columns.length !== columns.length)
    throw new PlanningError(
      "the recursive term must return as many columns as the non-recursive term",
      "42601",
    );
  const casts = step.columns.map((col, position) =>
    coerce(
      { expr: column(position, col.type), nullable: col.nullable, big: col.big },
      columns[position]?.type ?? col.type,
      "implicit",
      builder.parameters,
    ),
  );
  const stepRel: Rel = casts.every((cast) => cast.expr.op === "column")
    ? step.rel
    : {
        op: "project",
        id: builder.id(),
        input: step.rel,
        exprs: casts.map((cast) => cast.expr),
        output: outputColumns(columns),
      };
  return {
    rel: {
      op: "recursive",
      id: builder.id(),
      workTable,
      all: recursive.all,
      base: base.rel,
      step: stepRel,
      output: outputColumns(columns),
    },
    columns,
  };
}
