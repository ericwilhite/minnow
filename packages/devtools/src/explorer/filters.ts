import type { QueryValue } from "@minnowdb/core";
import { sqlColumn, sqlLiteral, type ColumnType } from "../sql/literal.js";

/** Every comparison the engine's WHERE clause supports, in the forms a column filter needs. */
export type FilterOperator =
  | "="
  | "!="
  | "contains"
  | "starts with"
  | ">"
  | ">="
  | "<"
  | "<="
  | "like"
  | "in"
  | "between"
  | "is null"
  | "is not null";

export interface Filter {
  column: string;
  type: ColumnType;
  operator: FilterOperator;
  /** Empty for the null tests, two entries for `between`, one or more for `in`. */
  values: QueryValue[];
}

interface OperatorSpec {
  label: string;
  /** How many values the operator takes; `in` takes any number of them. */
  arity: 0 | 1 | 2 | "many";
  /** Types the operator is offered for. */
  types?: readonly ColumnType[];
  /** Shown in the value box, so the accepted shape is visible before anything is typed. */
  hint?: string;
}

/**
 * Declaration order is menu order, so the substring searches sit next to equality where they are
 * looked for. `contains` and `starts with` exist because a bare `LIKE` is an exact match — typing
 * `crea` into a raw LIKE finds nothing, which reads as a broken filter rather than as SQL working
 * exactly as specified. They add the wildcards; `like` stays for patterns written by hand.
 */
const operators: Record<FilterOperator, OperatorSpec> = {
  "=": { label: "=", arity: 1 },
  "!=": { label: "≠", arity: 1 },
  contains: { label: "contains", arity: 1, types: ["string"], hint: "crea" },
  "starts with": { label: "starts with", arity: 1, types: ["string"], hint: "crea" },
  ">": { label: ">", arity: 1, types: ["number", "string", "datetime"] },
  ">=": { label: "≥", arity: 1, types: ["number", "string", "datetime"] },
  "<": { label: "<", arity: 1, types: ["number", "string", "datetime"] },
  "<=": { label: "≤", arity: 1, types: ["number", "string", "datetime"] },
  like: { label: "like", arity: 1, types: ["string"], hint: "%crea%" },
  in: { label: "in", arity: "many" },
  between: { label: "between", arity: 2, types: ["number", "string", "datetime"] },
  "is null": { label: "is null", arity: 0 },
  "is not null": { label: "is not null", arity: 0 },
};

export function operatorsFor(type: ColumnType): FilterOperator[] {
  return (Object.keys(operators) as FilterOperator[]).filter((operator) => {
    const allowed = operators[operator].types;
    return allowed === undefined || allowed.includes(type);
  });
}

export function operatorLabel(operator: FilterOperator): string {
  return operators[operator].label;
}

export function operatorArity(operator: FilterOperator): 0 | 1 | 2 | "many" {
  return operators[operator].arity;
}

export function operatorHint(operator: FilterOperator): string | undefined {
  return operators[operator].hint;
}

/** A filter missing the values its operator needs is skipped rather than compiled into nonsense. */
export function isComplete(filter: Filter): boolean {
  const arity = operatorArity(filter.operator);
  if (arity === "many") return filter.values.length > 0;
  return filter.values.length === arity;
}

/** Neutralises LIKE wildcards in a typed value, so `contains` searches for exactly what was typed. */
function escapeLikeValue(value: QueryValue | undefined): string {
  return String(value ?? "").replace(/[\\%_]/g, (match) => `\\${match}`);
}

function renderFilter(table: string, filter: Filter): string {
  const column = sqlColumn(table, filter.column);
  const literal = (index: number): string => sqlLiteral(filter.values[index] ?? null, filter.type);

  switch (filter.operator) {
    case "is null":
      return `${column} IS NULL`;
    case "is not null":
      return `${column} IS NOT NULL`;
    case "like":
      return `${column} LIKE ${literal(0)}`;
    // The wildcards are added here rather than typed; `_`, `%`, and the escape character itself
    // inside the typed value are escaped, so `100%` finds the literal string and nothing else.
    case "contains":
      return `${column} LIKE ${sqlLiteral(`%${escapeLikeValue(filter.values[0])}%`, "string")} ESCAPE '\\'`;
    case "starts with":
      return `${column} LIKE ${sqlLiteral(`${escapeLikeValue(filter.values[0])}%`, "string")} ESCAPE '\\'`;
    case "in":
      return `${column} IN (${filter.values
        .map((value) => sqlLiteral(value, filter.type))
        .join(", ")})`;
    case "between":
      return `${column} BETWEEN ${literal(0)} AND ${literal(1)}`;
    default:
      return `${column} ${filter.operator} ${literal(0)}`;
  }
}

/**
 * Filters combine with AND, each parenthesised so no operator precedence surprise can change what
 * the person asked for. Returns undefined when nothing is filtering, so callers can omit WHERE.
 */
export function renderFilters(table: string, filters: readonly Filter[]): string | undefined {
  const rendered = filters.filter(isComplete).map((filter) => `(${renderFilter(table, filter)})`);
  return rendered.length === 0 ? undefined : rendered.join(" AND ");
}

/** One line of human text for the filter chip. */
export function describeFilter(filter: Filter): string {
  const arity = operatorArity(filter.operator);
  if (arity === 0) return `${filter.column} ${operatorLabel(filter.operator)}`;
  const values = filter.values.map((value) => (value === null ? "NULL" : String(value)));
  if (filter.operator === "between") return `${filter.column} between ${values.join(" and ")}`;
  if (filter.operator === "in") return `${filter.column} in (${values.join(", ")})`;
  return `${filter.column} ${operatorLabel(filter.operator)} ${values[0] ?? ""}`;
}
