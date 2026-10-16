/**
 * Shape analysis for ordered row selection: statements whose answer is decided by the extreme
 * values of one number or datetime column — `ORDER BY col [DESC] LIMIT n [OFFSET m]`, keyset
 * pages (`WHERE col > ? ORDER BY col LIMIT n`), and `MIN(col)`/`MAX(col)` — optionally under
 * range predicates on that same column. Such a statement can only ever return rows holding a
 * handful of values at one end of the column's range, so the scan can be narrowed to those rows
 * before the ordinary executor runs. The executor still evaluates every predicate, the sort,
 * and the limit over the narrowed rows; the selection only has to be a superset of the rows
 * that decide the answer.
 */
import { dateMilliseconds } from "../date-value.js";
import type { CompiledQuery, Expression, PredicateOperator } from "../plan/model.js";
import type { TableColumnRecord, TableRecord } from "../storage/types.js";
import { hasAggregate } from "./query.js";

/**
 * Test-only escape hatch and counter. Not exported from any public entry point: the differential
 * suite imports this module directly to force the ordinary scan and to assert that eligible
 * statements were actually narrowed.
 */
export const orderedSelectionTestHooks = {
  disabled: false,
  served: 0,
};

/** The value range every qualifying row's column value lies in; bounds are epoch ms for dates. */
export interface OrderedRange {
  readonly lower: number;
  readonly lowerInclusive: boolean;
  readonly upper: number;
  readonly upperInclusive: boolean;
  /** Whether any predicate constrains the column; an unconstrained scan also meets NULLs. */
  readonly constrained: boolean;
}

export type OrderedSelectionShape =
  | {
      /** The first `take` rows in column order (descending when `descending`), plus ties. */
      readonly kind: "top";
      readonly column: TableColumnRecord;
      readonly descending: boolean;
      /** Whether NULLs sort ahead of every value under this ORDER BY. */
      readonly nullsFirst: boolean;
      /** LIMIT plus OFFSET: how many leading rows in column order can reach the answer. */
      readonly take: number;
      readonly limit: number;
      readonly offset: number;
      readonly range: OrderedRange;
    }
  | {
      /** A row holding each requested extreme of each column. */
      readonly kind: "extremes";
      readonly extremes: ReadonlyArray<{ column: TableColumnRecord; max: boolean }>;
      readonly range: OrderedRange;
    };

const UNBOUNDED: OrderedRange = {
  lower: Number.NEGATIVE_INFINITY,
  lowerInclusive: true,
  upper: Number.POSITIVE_INFINITY,
  upperInclusive: true,
  constrained: false,
};

/**
 * Recognizes an ordered-selection statement over one append table. Undefined for anything else:
 * joins, grouping, aggregates other than bare MIN/MAX, DISTINCT, predicates other than plain
 * comparisons of the ordered column with a literal of its exact storage type, and full-text or
 * volatile expressions all keep the ordinary scan.
 */
export function orderedSelectionShape(
  plan: CompiledQuery,
  table: TableRecord,
  maxTake: number,
): OrderedSelectionShape | undefined {
  if (
    plan.joins.length > 0 ||
    plan.base.table !== table.name ||
    plan.base.derived !== undefined ||
    plan.base.union !== undefined ||
    plan.base.windowed !== undefined ||
    plan.base.recursive !== undefined ||
    plan.base.columnAliases !== undefined ||
    plan.groupBy.length > 0 ||
    plan.having.length > 0 ||
    plan.distinctWildcard === true ||
    plan.limitWithTies === true ||
    plan.pendingSelectShape !== undefined ||
    plan.usesVolatileFunctions === true ||
    plan.usesSequenceCalls === true ||
    plan.select.length === 0
  ) {
    return undefined;
  }
  const resolve = (expression: Expression): TableColumnRecord | undefined => {
    if (expression.kind !== "column") return undefined;
    const parts = expression.reference.split(".");
    if (parts.length > 2) return undefined;
    if (parts.length === 2 && parts[0] !== plan.base.alias && parts[0] !== table.name) {
      return undefined;
    }
    const name = parts.length === 2 ? parts[1] : parts[0];
    const column = table.columns.find((candidate) => candidate.name === name);
    return column !== undefined &&
      column.hidden !== true &&
      column.sqlDomain === undefined &&
      (column.type === "number" || column.type === "datetime")
      ? column
      : undefined;
  };
  const aggregated = plan.select.some((item) => hasAggregate(item.expression));
  if (aggregated) {
    if (plan.orderBy.length > 0) return undefined;
    const extremes: Array<{ column: TableColumnRecord; max: boolean }> = [];
    for (const item of plan.select) {
      const expression = item.expression;
      if (
        expression.kind !== "call" ||
        (expression.name !== "MIN" && expression.name !== "MAX") ||
        expression.arguments.length !== 1 ||
        (expression.aggregateOrderBy?.length ?? 0) > 0
      ) {
        return undefined;
      }
      const column = resolve(expression.arguments[0] ?? { kind: "wildcard" });
      if (column === undefined) return undefined;
      extremes.push({ column, max: expression.name === "MAX" });
    }
    const first = extremes[0]?.column;
    if (first === undefined) return undefined;
    // Predicates narrow the rows only when they range over the one aggregated column.
    const sameColumn = extremes.every((extreme) => extreme.column.id === first.id);
    if (plan.predicates.length > 0 && !sameColumn) return undefined;
    const range = orderedRange(plan, first, resolve);
    return range === undefined ? undefined : { kind: "extremes", extremes, range };
  }
  const order = plan.orderBy[0];
  if (order === undefined || plan.limit === undefined) return undefined;
  if (plan.limitParameter !== undefined || plan.offsetParameter !== undefined) return undefined;
  const offset = plan.offset ?? 0;
  const take = plan.limit + offset;
  // A deep OFFSET is still served when the column's blocks are disjoint and sorted: the rows
  // before the page are counted rather than selected. The page itself must stay narrow.
  if (!Number.isSafeInteger(take) || take < 0 || plan.limit > maxTake) return undefined;
  const column = orderedOutputColumn(plan, order.expression, resolve);
  if (column === undefined) return undefined;
  const range = orderedRange(plan, column, resolve);
  if (range === undefined) return undefined;
  const descending = order.direction === "desc";
  return {
    kind: "top",
    column,
    descending,
    nullsFirst: (order.nulls ?? (descending ? "first" : "last")) === "first",
    take,
    limit: plan.limit,
    offset,
    range,
  };
}

/**
 * The base column an ORDER BY term sorts by, resolved the way the executor names its output:
 * a wildcard output is the source column of that name, otherwise the first select item that
 * is the same column reference or carries the name as its alias. Only a bare base column
 * qualifies; an alias over an expression does not.
 */
function orderedOutputColumn(
  plan: CompiledQuery,
  expression: Expression,
  resolve: (expression: Expression) => TableColumnRecord | undefined,
): TableColumnRecord | undefined {
  if (expression.kind !== "column") return undefined;
  if (plan.select[0]?.expression.kind === "wildcard") {
    return plan.select.length === 1 ? resolve(expression) : undefined;
  }
  const selected = plan.select.find(
    (item) =>
      (item.expression.kind === "column" && item.expression.reference === expression.reference) ||
      (!expression.reference.includes(".") && item.alias === expression.reference),
  );
  return selected === undefined ? undefined : resolve(selected.expression);
}

/**
 * The conjunction of every predicate as one range over `column`, or undefined when any predicate
 * is not a plain comparison of that column with a literal of its exact storage type.
 */
function orderedRange(
  plan: CompiledQuery,
  column: TableColumnRecord,
  resolve: (expression: Expression) => TableColumnRecord | undefined,
): OrderedRange | undefined {
  if (plan.predicates.length === 0) return UNBOUNDED;
  let lower = Number.NEGATIVE_INFINITY;
  let lowerInclusive = true;
  let upper = Number.POSITIVE_INFINITY;
  let upperInclusive = true;
  for (const predicate of plan.predicates) {
    if (predicate.escape !== undefined) return undefined;
    const leftColumn = resolve(predicate.left);
    const rightColumn = resolve(predicate.right);
    let operator: PredicateOperator;
    let literal: Expression;
    if (leftColumn?.id === column.id && rightColumn === undefined) {
      operator = predicate.operator;
      literal = predicate.right;
    } else if (rightColumn?.id === column.id && leftColumn === undefined) {
      operator = mirrored(predicate.operator);
      literal = predicate.left;
    } else {
      return undefined;
    }
    if (literal.kind !== "literal" || literal.internalSqlValue === true) return undefined;
    if (literal.sqlDomain !== undefined) return undefined;
    const raw = literal.value;
    const value =
      column.type === "datetime"
        ? raw instanceof Date
          ? dateMilliseconds(raw)
          : Number.NaN
        : typeof raw === "number"
          ? raw
          : Number.NaN;
    if (!Number.isFinite(value)) return undefined;
    switch (operator) {
      case "=":
        if (value > lower || (value === lower && !lowerInclusive)) {
          lower = value;
          lowerInclusive = true;
        }
        if (value < upper || (value === upper && !upperInclusive)) {
          upper = value;
          upperInclusive = true;
        }
        break;
      case ">":
      case ">=":
        if (value > lower || (value === lower && operator === ">")) {
          lower = value;
          lowerInclusive = operator === ">=";
        }
        break;
      case "<":
      case "<=":
        if (value < upper || (value === upper && operator === "<")) {
          upper = value;
          upperInclusive = operator === "<=";
        }
        break;
      default:
        return undefined;
    }
  }
  return { lower, lowerInclusive, upper, upperInclusive, constrained: true };
}

function mirrored(operator: PredicateOperator): PredicateOperator {
  switch (operator) {
    case ">":
      return "<";
    case ">=":
      return "<=";
    case "<":
      return ">";
    case "<=":
      return ">=";
    default:
      return operator;
  }
}

/** Whether every value of a block spanning [min, max] lies in the range. */
export function zoneWithinOrderedRange(range: OrderedRange, min: number, max: number): boolean {
  return inOrderedRange(range, min) && inOrderedRange(range, max);
}

/** Whether a finite value lies in the range. */
export function inOrderedRange(range: OrderedRange, value: number): boolean {
  return (
    (range.lowerInclusive ? value >= range.lower : value > range.lower) &&
    (range.upperInclusive ? value <= range.upper : value < range.upper)
  );
}

/** Whether a block whose values span [min, max] can hold a value in the range. */
export function zoneMeetsOrderedRange(range: OrderedRange, min: number, max: number): boolean {
  return (
    (range.lowerInclusive ? max >= range.lower : max > range.lower) &&
    (range.upperInclusive ? min <= range.upper : min < range.upper)
  );
}

/** The [begin, end) slots of an ascending array whose values lie in the range. */
export function ascendingRangeSlots(
  values: Float64Array,
  length: number,
  range: OrderedRange,
): { begin: number; end: number } {
  let low = 0;
  let high = length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    const value = values[middle] ?? 0;
    if (range.lowerInclusive ? value < range.lower : value <= range.lower) low = middle + 1;
    else high = middle;
  }
  const begin = low;
  high = length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    const value = values[middle] ?? 0;
    if (range.upperInclusive ? value <= range.upper : value < range.upper) low = middle + 1;
    else high = middle;
  }
  return { begin, end: low };
}
