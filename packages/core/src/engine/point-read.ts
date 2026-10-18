/**
 * Shape analysis for the keyed point-read fast path: a single-table conjunction of
 * column-equals-literal predicates — at most one of them a membership, `key IN (…)` or
 * `key = ANY($n)`, served as one lookup per member — that covers the table's unique key,
 * projecting bare columns — plain or logical-domain typed, since domain scalars externalize through the
 * same result boundary. Such a statement addresses at most one row, so execution can skip parameter
 * binding, plan cloning, streamed-view construction, and the vector pipeline entirely and
 * answer from cached decoded blocks. Anything this module cannot prove eligible falls back to
 * the ordinary executor, which stays the authority on errors and semantics.
 */
import { dateMilliseconds } from "../date-value.js";
import type { CompiledQuery, Expression, QueryParameter, QueryValue } from "../plan/model.js";

type PointReadValue = boolean | number | string | Date;

export interface PointReadEquality {
  column: string;
  value: PointReadValue;
}

/**
 * Projected columns, "*" for a bare wildcard the catalog expands at execution, or a lone
 * `COUNT(*)` under its output alias.
 */
export type PointReadSelect = Array<{ column: string; alias: string }> | "*" | { count: string };

export interface PointReadShape {
  table: string;
  /** Conjunctive equalities, in predicate order; may repeat a column. */
  equalities: PointReadEquality[];
  /**
   * One more equality whose value is any of these: distinct, NULL-free, ascending. Each member
   * is served as the shape whose equalities end with `column = member`; the signature below is
   * that per-member shape's.
   */
  members?: { column: string; values: PointReadValue[] };
  select: PointReadSelect;
  /**
   * The statement shape without its values: equality columns and projection. Resolved
   * per-table read plans are cached under it, so every execution of one statement shares one.
   */
  signature?: string;
}

type TemplateValue = { value: PointReadValue } | { parameter: number };

/** The statement-shaped half of the analysis, computed once per cached plan. */
interface PointReadTemplate {
  table: string;
  equalities: Array<{ column: string } & TemplateValue>;
  /** An IN list's items (a NULL item matches nothing), or the array placeholder of `= ANY`. */
  membership?: {
    column: string;
    items?: Array<TemplateValue | { null: true }>;
    arrayParameter?: number;
  };
  select: PointReadSelect;
  signature: string;
}

/** Most members one statement serves as separate lookups; longer lists take the executor. */
export const MAX_POINT_READ_MEMBERS = 4_096;

/**
 * The value-free identity of a point read: which columns it pins and what it projects. Two
 * shapes with one signature over one table resolve to the same columns, blocks, and output.
 */
export function pointReadSignature(shape: {
  readonly table: string;
  readonly equalities: ReadonlyArray<{ readonly column: string }>;
  readonly select: PointReadSelect;
  readonly signature?: string;
}): string {
  const select = shape.select;
  return (
    shape.signature ??
    JSON.stringify([
      shape.table,
      shape.equalities.map((equality) => equality.column),
      select === "*"
        ? "*"
        : Array.isArray(select)
          ? select.map((item) => [item.column, item.alias])
          : { count: select.count },
    ])
  );
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
  let select: PointReadTemplate["select"];
  const first = plan.select[0];
  const firstExpression = first?.expression;
  if (
    plan.select.length === 1 &&
    firstExpression?.kind === "wildcard" &&
    (firstExpression.table === undefined || firstExpression.table === base.alias)
  ) {
    // `SELECT * FROM t WHERE key = ?` is the commonest point lookup; the catalog names the
    // columns when the read is served, in declaration order like the ordinary executor.
    select = "*";
  } else if (
    plan.select.length === 1 &&
    first !== undefined &&
    firstExpression?.kind === "call" &&
    firstExpression.name === "COUNT" &&
    firstExpression.distinct !== true &&
    (firstExpression.aggregateOrderBy?.length ?? 0) === 0 &&
    firstExpression.arguments.length === 1 &&
    firstExpression.arguments[0]?.kind === "wildcard" &&
    firstExpression.arguments[0].table === undefined &&
    plan.pendingSelectShape === undefined &&
    plan.pendingOutputAliases === undefined
  ) {
    // `SELECT COUNT(*) FROM t WHERE col = ?` counts the rows the same lookup would return.
    select = { count: first.alias };
  } else {
    const columns: Array<{ column: string; alias: string }> = [];
    for (const item of plan.select) {
      const column = baseColumnReference(item.expression, base.alias);
      if (column === undefined) return undefined;
      columns.push({ column, alias: item.alias });
    }
    select = columns;
  }
  const equalities: PointReadTemplate["equalities"] = [];
  let membership: PointReadTemplate["membership"];
  for (const predicate of plan.predicates) {
    if (predicate.escape !== undefined) return undefined;
    if (predicate.operator === "IN" || predicate.operator === "= ANY") {
      const column = baseColumnReference(predicate.left, base.alias);
      if (column === undefined || membership !== undefined) return undefined;
      if (predicate.operator === "= ANY") {
        if (predicate.right.kind !== "parameter") return undefined;
        membership = { column, arrayParameter: predicate.right.index };
        continue;
      }
      if (predicate.right.kind !== "list" || predicate.right.items.length === 0) return undefined;
      if (predicate.right.items.length > MAX_POINT_READ_MEMBERS) return undefined;
      const items: NonNullable<PointReadTemplate["membership"]>["items"] = [];
      for (const item of predicate.right.items) {
        if (
          item.kind === "literal" &&
          item.value === null &&
          item.internalSqlValue !== true &&
          item.sqlDomain === undefined
        ) {
          items.push({ null: true });
          continue;
        }
        const value = templateEquality(item);
        if (value === undefined) return undefined;
        items.push(value);
      }
      membership = { column, items };
      continue;
    }
    if (predicate.operator !== "=") return undefined;
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
  return {
    table: base.table,
    equalities,
    ...(membership === undefined ? {} : { membership }),
    select,
    // A membership is served one member at a time, as the shape ending with its equality.
    signature: pointReadSignature({
      table: base.table,
      equalities: membership === undefined ? equalities : [...equalities, membership],
      select,
    }),
  };
}

/** Orders members as a key scan meets them, so a lookup per member returns scan order. */
function compareMembers(left: PointReadValue, right: PointReadValue): number {
  const a = left instanceof Date ? dateMilliseconds(left) : left;
  const b = right instanceof Date ? dateMilliseconds(right) : right;
  return a < b ? -1 : a > b ? 1 : 0;
}

/** The distinct, ascending, NULL-free members, or undefined for any value the path declines. */
function resolveMembers(
  membership: NonNullable<PointReadTemplate["membership"]>,
  params: readonly QueryParameter[],
): PointReadValue[] | undefined {
  const raw: QueryParameter[] = [];
  if (membership.arrayParameter !== undefined) {
    const array = params[membership.arrayParameter];
    // A NULL array, JSON array text, or a scalar: the ordinary binder decides what it means.
    if (!Array.isArray(array)) return undefined;
    raw.push(...(array as readonly QueryValue[]));
  } else {
    for (const item of membership.items ?? []) {
      if ("null" in item) continue;
      if ("value" in item) raw.push(item.value);
      else raw.push(params[item.parameter] ?? null);
    }
  }
  if (raw.length > MAX_POINT_READ_MEMBERS) return undefined;
  const values: PointReadValue[] = [];
  for (const member of raw) {
    // NULL never equals a key; an array member or anything else is the binder's to reject.
    if (member === null) continue;
    if (Array.isArray(member)) return undefined;
    const value = equalityValue(member as QueryValue);
    if (value === undefined) return undefined;
    values.push(value);
  }
  return ascendingMembers(values);
}

/**
 * Keys as a membership: distinct and ascending, the order a key scan meets them. Undefined when
 * they are not all one storage kind, or there are none. Sorts `values` in place.
 */
export function ascendingMembers(values: PointReadValue[]): PointReadValue[] | undefined {
  if (values.length === 0) return undefined;
  const kind = typeof values[0];
  const date = values[0] instanceof Date;
  if (values.some((value) => typeof value !== kind || value instanceof Date !== date)) {
    return undefined;
  }
  values.sort(compareMembers);
  const distinct: PointReadValue[] = [];
  for (const value of values) {
    const last = distinct[distinct.length - 1];
    if (last === undefined || compareMembers(last, value) !== 0) distinct.push(value);
  }
  return distinct;
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
  params: readonly QueryParameter[],
): PointReadShape | undefined {
  const equalities: PointReadEquality[] = [];
  for (const entry of template.equalities) {
    if ("value" in entry) {
      equalities.push(entry);
      continue;
    }
    const raw = params[entry.parameter];
    // An array binds only under ANY/ALL; the ordinary binder reports it anywhere else.
    if (raw === undefined || Array.isArray(raw)) return undefined;
    const value = equalityValue(raw as QueryValue);
    if (value === undefined) return undefined;
    equalities.push({ column: entry.column, value });
  }
  const membership = template.membership;
  if (membership === undefined) {
    return {
      table: template.table,
      equalities,
      select: template.select,
      signature: template.signature,
    };
  }
  const values = resolveMembers(membership, params);
  if (values === undefined) return undefined;
  return {
    table: template.table,
    equalities,
    members: { column: membership.column, values },
    select: template.select,
    signature: template.signature,
  };
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
