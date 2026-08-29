/**
 * Shape analysis for the keyed point-read fast path: a single-table conjunction of
 * column-equals-literal predicates that covers the table's unique key, projecting bare
 * columns — plain or logical-domain typed, since domain scalars externalize through the
 * same result boundary. Such a statement addresses at most one row, so execution can skip parameter
 * binding, plan cloning, streamed-view construction, and the vector pipeline entirely and
 * answer from cached decoded blocks. Anything this module cannot prove eligible falls back to
 * the ordinary executor, which stays the authority on errors and semantics.
 */
import { dateMilliseconds } from "../date-value.js";
import type { CompiledQuery, Expression, QueryValue } from "../plan/model.js";

export type PointReadValue = boolean | number | string | Date;

export interface PointReadEquality {
  column: string;
  value: PointReadValue;
}

export interface PointReadShape {
  table: string;
  /** Conjunctive equalities, in predicate order; may repeat a column. */
  equalities: PointReadEquality[];
  /** Plain column projections, in select order. */
  select: Array<{ column: string; alias: string }>;
}

/** The statement-shaped half of the analysis, computed once per cached plan. */
interface PointReadTemplate {
  table: string;
  equalities: Array<
    { column: string; value: PointReadValue } | { column: string; parameter: number }
  >;
  select: Array<{ column: string; alias: string }>;
}

/**
 * Test-only escape hatch and counters. Not exported from any public entry point: in-repo
 * differential suites import this module directly to force the ordinary executor and to
 * assert the fast path actually served eligible statements.
 */
export const pointReadTestHooks = {
  disabled: false,
  attempted: 0,
  served: 0,
};

const DUAL_TABLE = "(dual)";

/** A base-table column reference: bare, or qualified by the base source's alias. */
function baseColumnReference(expression: Expression, alias: string): string | undefined {
  if (expression.kind !== "column") return undefined;
  const reference = expression.reference;
  const dot = reference.indexOf(".");
  if (dot < 0) return reference;
  if (reference.slice(0, dot) !== alias) return undefined;
  const column = reference.slice(dot + 1);
  // A nested qualifier ("a.b.c") is not a base-table column.
  return column.includes(".") ? undefined : column;
}

/** A usable equality operand: a plain non-null literal of a storage type, verified exactly. */
function equalityValue(value: QueryValue): PointReadValue | undefined {
  if (value === null) return undefined;
  if (typeof value === "number" && !Number.isFinite(value)) return undefined;
  // Strings in the engine's protected NUL namespace are wrapped at the write boundary, so a
  // raw comparison against stored bytes would not reproduce the ordinary path's answer.
  if (typeof value === "string" && value.startsWith("\u0000")) return undefined;
  // The internal-slot read never invokes caller-controlled Date methods.
  if (value instanceof Date && Number.isNaN(dateMilliseconds(value))) return undefined;
  return value;
}

function templateEquality(
  expression: Expression,
): { value: PointReadValue } | { parameter: number } | undefined {
  if (expression.kind === "parameter") return { parameter: expression.index };
  if (expression.kind !== "literal") return undefined;
  // Tagged internal values and logical-domain literals compare under domain rules the fast
  // path does not implement; a NULL literal never equals anything.
  if (expression.internalSqlValue === true || expression.sqlDomain !== undefined) return undefined;
  const value = equalityValue(expression.value);
  return value === undefined ? undefined : { value };
}

/**
 * Recognizes the eligible statement shape. Purely syntactic: catalog checks (column existence,
 * types, key coverage, view expansion, physical history) belong to the caller, which falls
 * back to the ordinary executor whenever they cannot be proven.
 */
function pointReadTemplate(plan: CompiledQuery): PointReadTemplate | undefined {
  const base = plan.base;
  if (
    base.table === DUAL_TABLE ||
    base.derived !== undefined ||
    base.union !== undefined ||
    base.recursive !== undefined ||
    base.windowed !== undefined ||
    base.lateral === true ||
    base.columnAliases !== undefined ||
    plan.joins.length > 0 ||
    plan.groupBy.length > 0 ||
    plan.having.length > 0 ||
    plan.orderBy.length > 0 ||
    plan.select.length === 0 ||
    plan.predicates.length === 0 ||
    plan.limit !== undefined ||
    plan.offset !== undefined ||
    plan.limitParameter !== undefined ||
    plan.offsetParameter !== undefined ||
    (plan.limitValidationParameters?.length ?? 0) > 0 ||
    (plan.offsetValidationParameters?.length ?? 0) > 0 ||
    plan.limitWithTies === true ||
    plan.distinctWildcard === true ||
    plan.usesStatementDatetime === true ||
    plan.usesSequenceCalls === true ||
    plan.usesVolatileFunctions === true
  ) {
    return undefined;
  }
  const select: PointReadTemplate["select"] = [];
  for (const item of plan.select) {
    const column = baseColumnReference(item.expression, base.alias);
    if (column === undefined) return undefined;
    select.push({ column, alias: item.alias });
  }
  const equalities: PointReadTemplate["equalities"] = [];
  for (const predicate of plan.predicates) {
    if (predicate.operator !== "=" || predicate.escape !== undefined) return undefined;
    const leftColumn = baseColumnReference(predicate.left, base.alias);
    const rightColumn = baseColumnReference(predicate.right, base.alias);
    const operand =
      leftColumn !== undefined && rightColumn === undefined
        ? templateEquality(predicate.right)
        : rightColumn !== undefined && leftColumn === undefined
          ? templateEquality(predicate.left)
          : undefined;
    const column = leftColumn ?? rightColumn;
    if (operand === undefined || column === undefined) return undefined;
    equalities.push({ column, ...operand });
  }
  return { table: base.table, equalities, select };
}

const templates = new WeakMap<CompiledQuery, PointReadTemplate | null>();

/** The template for a cached compiled plan, analyzed once per statement. */
export function cachedPointReadTemplate(plan: CompiledQuery): PointReadTemplate | null {
  const cached = templates.get(plan);
  if (cached !== undefined) return cached;
  const template = pointReadTemplate(plan) ?? null;
  templates.set(plan, template);
  return template;
}

/**
 * Substitutes this call's parameters into the statement template. Undefined means a parameter
 * carries a value the fast path cannot compare exactly (NULL, a non-finite number, an invalid
 * Date, or a non-storage value), and the ordinary executor must decide what it means.
 */
export function resolvePointReadShape(
  template: PointReadTemplate,
  params: readonly QueryValue[],
): PointReadShape | undefined {
  const equalities: PointReadEquality[] = [];
  for (const entry of template.equalities) {
    if ("value" in entry) {
      equalities.push(entry);
      continue;
    }
    const raw = params[entry.parameter];
    if (raw === undefined) return undefined;
    const value = equalityValue(raw);
    if (value === undefined) return undefined;
    equalities.push({ column: entry.column, value });
  }
  return { table: template.table, equalities, select: template.select };
}

const ascendingRuns = new WeakMap<Float64Array, boolean>();

/** Whether the array is non-strictly ascending; memoized per immutable decoded array. */
export function valuesAreAscending(values: Float64Array): boolean {
  const cached = ascendingRuns.get(values);
  if (cached !== undefined) return cached;
  let ascending = true;
  for (let index = 1; index < values.length; index += 1) {
    if ((values[index] ?? 0) < (values[index - 1] ?? 0)) {
      ascending = false;
      break;
    }
  }
  ascendingRuns.set(values, ascending);
  return ascending;
}

/** The [begin, end) run of slots equal to `target` over an ascending array. */
export function equalRunRange(
  values: Float64Array,
  target: number,
): { begin: number; end: number } {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((values[middle] ?? 0) < target) low = middle + 1;
    else high = middle;
  }
  const begin = low;
  high = values.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((values[middle] ?? 0) <= target) low = middle + 1;
    else high = middle;
  }
  return { begin, end: low };
}
