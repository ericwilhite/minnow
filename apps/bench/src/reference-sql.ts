import type { DatabaseRow } from "@browserdatabase/engine";
import type { AdHocQueryResult } from "./benchmark.js";

type RowContext = Record<string, DatabaseRow | undefined>;
type Scalar = boolean | number | string | Date | null;

interface ColumnExpression {
  kind: "column";
  reference: string;
  alias: string;
}

interface AggregateExpression {
  kind: "aggregate";
  operation: "COUNT" | "SUM" | "AVG" | "MIN" | "MAX";
  reference: string;
  alias: string;
}

interface WildcardExpression {
  kind: "wildcard";
}

type SelectExpression = ColumnExpression | AggregateExpression | WildcardExpression;

interface JoinDefinition {
  table: string;
  alias: string;
  leftReference: string;
  rightReference: string;
  kind: "inner" | "left";
}

interface Predicate {
  reference: string;
  operator: "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=";
  value: Scalar;
}

interface QueryPlan {
  baseTable: string;
  baseAlias: string;
  joins: JoinDefinition[];
  select: SelectExpression[];
  predicates: Predicate[];
  groupBy: string[];
  orderBy?: { name: string; direction: "asc" | "desc" };
  limit?: number;
  tables: string[];
}

interface ExecutionResult {
  rows: Array<Record<string, unknown>>;
  sourceRows: number;
  joinedRows: number;
}

export function executeReferenceSql(
  sql: string,
  tables: ReadonlyMap<string, DatabaseRow[]>,
  runId: string,
  iterations = 7,
): AdHocQueryResult {
  const totalStarted = performance.now();
  const parseStarted = performance.now();
  const plan = parseQuery(sql, tables);
  const parseMs = performance.now() - parseStarted;
  executePlan(plan, tables);
  const samples: number[] = [];
  let execution: ExecutionResult = { rows: [], sourceRows: 0, joinedRows: 0 };
  for (let index = 0; index < iterations; index += 1) {
    const started = performance.now();
    execution = executePlan(plan, tables);
    samples.push(performance.now() - started);
  }
  samples.sort((left, right) => left - right);
  const median = samples[Math.floor(samples.length / 2)] ?? 0;
  const p95 = samples[Math.ceil(samples.length * 0.95) - 1] ?? median;
  const previewRows = execution.rows.slice(0, 100);
  return {
    runId,
    sql: sql.trim(),
    tables: plan.tables,
    columns: Object.keys(previewRows[0] ?? projectedColumnNames(plan.select)),
    rowCount: execution.rows.length,
    previewRows,
    truncated: execution.rows.length > previewRows.length,
    metrics: {
      parseMs,
      executeMedianMs: median,
      executeP95Ms: p95,
      totalMs: performance.now() - totalStarted,
      iterations,
      sourceRows: execution.sourceRows,
      joinedRows: execution.joinedRows,
    },
  };
}

function parseQuery(sql: string, tables: ReadonlyMap<string, DatabaseRow[]>): QueryPlan {
  const normalized = sql.trim().replace(/;$/, "").trim();
  if (normalized.length === 0) throw new TypeError("Enter a SELECT query");
  if (normalized.includes(";")) throw new TypeError("Run one SELECT statement at a time");
  if (/--|\/\*/.test(normalized)) throw new TypeError("SQL comments are not supported");
  const match =
    /^SELECT\s+([\s\S]+?)\s+FROM\s+([\s\S]+?)(?:\s+WHERE\s+([\s\S]+?))?(?:\s+GROUP\s+BY\s+([\s\S]+?))?(?:\s+ORDER\s+BY\s+([\s\S]+?))?(?:\s+LIMIT\s+(\d+))?$/i.exec(
      normalized,
    );
  if (match === null) {
    throw new TypeError(
      "Supported shape: SELECT … FROM … [JOIN … ON …] [WHERE …] [GROUP BY …] [ORDER BY …] [LIMIT n]",
    );
  }
  const select = parseSelect(match[1] ?? "");
  const source = parseSources(match[2] ?? "");
  const predicates = parsePredicates(match[3]);
  const groupBy = match[4] === undefined ? [] : splitComma(match[4]).map(parseReference);
  const orderBy = parseOrderBy(match[5]);
  const limit = match[6] === undefined ? undefined : Number(match[6]);
  if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1 || limit > 10_000)) {
    throw new RangeError("LIMIT must be between 1 and 10,000");
  }
  const tableNames = [source.baseTable, ...source.joins.map((join) => join.table)];
  for (const table of tableNames) {
    if (!tables.has(table)) throw new TypeError(`Unknown table: ${table}`);
  }
  const aliases = new Set([source.baseAlias, ...source.joins.map((join) => join.alias)]);
  const references = [
    ...select.flatMap((expression) =>
      expression.kind === "wildcard" || expression.reference === "*" ? [] : [expression.reference],
    ),
    ...source.joins.flatMap((join) => [join.leftReference, join.rightReference]),
    ...predicates.map((predicate) => predicate.reference),
    ...groupBy,
  ];
  for (const reference of references) {
    if (!reference.includes(".")) continue;
    const [alias] = reference.split(".");
    if (alias !== undefined && !aliases.has(alias))
      throw new TypeError(`Unknown table alias: ${alias}`);
  }
  return {
    baseTable: source.baseTable,
    baseAlias: source.baseAlias,
    joins: source.joins,
    select,
    predicates,
    groupBy,
    ...(orderBy === undefined ? {} : { orderBy }),
    ...(limit === undefined ? {} : { limit }),
    tables: tableNames,
  };
}

function parseSelect(value: string): SelectExpression[] {
  const expressions = splitComma(value).map((part): SelectExpression => {
    if (part === "*") return { kind: "wildcard" };
    const aggregate =
      /^(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*(\*|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s*\)(?:\s+AS\s+([A-Za-z_]\w*))?$/i.exec(
        part,
      );
    if (aggregate !== null) {
      const operation = (aggregate[1] ?? "").toUpperCase() as AggregateExpression["operation"];
      const reference = aggregate[2] ?? "*";
      return {
        kind: "aggregate",
        operation,
        reference,
        alias: aggregate[3] ?? `${operation.toLowerCase()}_${reference.replace(".", "_")}`,
      };
    }
    const column = /^([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)(?:\s+AS\s+([A-Za-z_]\w*))?$/i.exec(part);
    if (column === null) {
      throw new TypeError(`Unsupported SELECT expression: ${part}`);
    }
    const reference = parseReference(column[1] ?? "");
    return {
      kind: "column",
      reference,
      alias: column[2] ?? reference.split(".").at(-1) ?? reference,
    };
  });
  if (expressions.length === 0) throw new TypeError("SELECT needs at least one expression");
  if (expressions.some((expression) => expression.kind === "wildcard") && expressions.length > 1) {
    throw new TypeError("SELECT * cannot be mixed with other expressions");
  }
  return expressions;
}

function parseSources(value: string): {
  baseTable: string;
  baseAlias: string;
  joins: JoinDefinition[];
} {
  const base = /^([A-Za-z_]\w*)(?:\s+(?:AS\s+)?(?!INNER\b|LEFT\b|JOIN\b)([A-Za-z_]\w*))?/i.exec(
    value,
  );
  if (base === null) throw new TypeError("FROM needs a table name");
  const baseTable = base[1] ?? "";
  const baseAlias = base[2] ?? baseTable;
  let remaining = value.slice(base[0].length);
  const joins: JoinDefinition[] = [];
  while (remaining.trim().length > 0) {
    const join =
      /^\s+(?:(INNER|LEFT)\s+)?JOIN\s+([A-Za-z_]\w*)(?:\s+(?:AS\s+)?(?!ON\b)([A-Za-z_]\w*))?\s+ON\s+([A-Za-z_]\w*\.[A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w*\.[A-Za-z_]\w*)/i.exec(
        remaining,
      );
    if (join === null) throw new TypeError(`Unsupported JOIN clause: ${remaining.trim()}`);
    joins.push({
      kind: (join[1]?.toLowerCase() as "inner" | "left" | undefined) ?? "inner",
      table: join[2] ?? "",
      alias: join[3] ?? join[2] ?? "",
      leftReference: parseReference(join[4] ?? ""),
      rightReference: parseReference(join[5] ?? ""),
    });
    remaining = remaining.slice(join[0].length);
  }
  return { baseTable, baseAlias, joins };
}

function parsePredicates(value: string | undefined): Predicate[] {
  if (value === undefined) return [];
  return value.split(/\s+AND\s+/i).map((part) => {
    const match = /^([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)$/i.exec(
      part.trim(),
    );
    if (match === null) throw new TypeError(`Unsupported WHERE predicate: ${part.trim()}`);
    return {
      reference: parseReference(match[1] ?? ""),
      operator: match[2] as Predicate["operator"],
      value: parseLiteral(match[3] ?? ""),
    };
  });
}

function parseLiteral(value: string): Scalar {
  const trimmed = value.trim();
  const date = /^DATE\s+'([^']+)'$/i.exec(trimmed);
  if (date !== null) {
    const parsed = new Date(`${date[1] ?? ""}T00:00:00.000Z`);
    if (!Number.isFinite(parsed.getTime())) throw new TypeError(`Invalid date literal: ${trimmed}`);
    return parsed;
  }
  if (/^'.*'$/.test(trimmed)) return trimmed.slice(1, -1).replaceAll("''", "'");
  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) return Number(trimmed);
  if (/^TRUE$/i.test(trimmed)) return true;
  if (/^FALSE$/i.test(trimmed)) return false;
  if (/^NULL$/i.test(trimmed)) return null;
  throw new TypeError(`Unsupported literal: ${trimmed}`);
}

function parseOrderBy(value: string | undefined): QueryPlan["orderBy"] {
  if (value === undefined) return undefined;
  const match = /^([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)(?:\s+(ASC|DESC))?$/i.exec(value.trim());
  if (match === null) throw new TypeError("ORDER BY supports one column or output alias");
  return {
    name: match[1] ?? "",
    direction: /^DESC$/i.test(match[2] ?? "") ? "desc" : "asc",
  };
}

function executePlan(plan: QueryPlan, tables: ReadonlyMap<string, DatabaseRow[]>): ExecutionResult {
  const baseRows = tables.get(plan.baseTable) ?? [];
  let contexts: RowContext[] = baseRows.map((row) => ({ [plan.baseAlias]: row }));
  let sourceRows = baseRows.length;
  for (const join of plan.joins) {
    const rightRows = tables.get(join.table) ?? [];
    sourceRows += rightRows.length;
    const rightReference = join.rightReference.startsWith(`${join.alias}.`)
      ? join.rightReference
      : join.leftReference;
    const leftReference =
      rightReference === join.rightReference ? join.leftReference : join.rightReference;
    if (!rightReference.startsWith(`${join.alias}.`)) {
      throw new TypeError(`JOIN condition must reference the new alias ${join.alias}`);
    }
    const rightColumn = rightReference.split(".")[1] ?? "";
    const index = new Map<unknown, DatabaseRow[]>();
    for (const row of rightRows) {
      const key = comparable(row[rightColumn]);
      const matches = index.get(key) ?? [];
      matches.push(row);
      index.set(key, matches);
    }
    const joined: RowContext[] = [];
    for (const context of contexts) {
      const matches = index.get(comparable(resolveReference(context, leftReference))) ?? [];
      if (matches.length === 0 && join.kind === "left") {
        joined.push({ ...context, [join.alias]: undefined });
      } else {
        for (const row of matches) joined.push({ ...context, [join.alias]: row });
      }
    }
    contexts = joined;
  }
  const joinedRows = contexts.length;
  contexts = contexts.filter((context) =>
    plan.predicates.every((predicate) =>
      compare(resolveReference(context, predicate.reference), predicate),
    ),
  );
  const hasAggregate = plan.select.some((expression) => expression.kind === "aggregate");
  let resultRows: Array<Record<string, unknown>>;
  if (hasAggregate || plan.groupBy.length > 0) {
    const groups = new Map<string, RowContext[]>();
    if (contexts.length === 0 && plan.groupBy.length === 0) groups.set("[]", []);
    for (const context of contexts) {
      const key = JSON.stringify(
        plan.groupBy.map((reference) => resolveReference(context, reference)),
      );
      const group = groups.get(key) ?? [];
      group.push(context);
      groups.set(key, group);
    }
    resultRows = [...groups.values()].map((group) => projectGroup(plan.select, group));
  } else {
    resultRows = contexts.map((context) => projectRow(plan.select, context));
  }
  if (plan.orderBy !== undefined) {
    const { name, direction } = plan.orderBy;
    const multiplier = direction === "desc" ? -1 : 1;
    const outputName = name.split(".").at(-1) ?? name;
    resultRows.sort(
      (left, right) => compareValues(left[outputName], right[outputName]) * multiplier,
    );
  }
  if (plan.limit !== undefined) resultRows = resultRows.slice(0, plan.limit);
  return { rows: resultRows, sourceRows, joinedRows };
}

function projectRow(
  select: readonly SelectExpression[],
  context: RowContext,
): Record<string, unknown> {
  if (select[0]?.kind === "wildcard") {
    const aliases = Object.keys(context);
    return Object.fromEntries(
      aliases.flatMap((alias) =>
        Object.entries(context[alias] ?? {}).map(([name, value]) => [
          aliases.length === 1 ? name : `${alias}.${name}`,
          value,
        ]),
      ),
    );
  }
  return Object.fromEntries(
    select.map((expression) => {
      if (expression.kind !== "column") throw new Error("Aggregate needs GROUP BY execution");
      return [expression.alias, resolveReference(context, expression.reference)];
    }),
  );
}

function projectGroup(
  select: readonly SelectExpression[],
  contexts: readonly RowContext[],
): Record<string, unknown> {
  const first = contexts[0] ?? {};
  return Object.fromEntries(
    select.map((expression) => {
      if (expression.kind === "wildcard") throw new TypeError("SELECT * cannot be grouped");
      if (expression.kind === "column") {
        return [expression.alias, resolveReference(first, expression.reference)];
      }
      const values =
        expression.reference === "*"
          ? contexts.map(() => 1)
          : contexts
              .map((context) => resolveReference(context, expression.reference))
              .filter((value) => value !== null && value !== undefined);
      switch (expression.operation) {
        case "COUNT":
          return [expression.alias, values.length];
        case "SUM":
          return [
            expression.alias,
            values.reduce<number>((total, value) => total + numeric(value), 0),
          ];
        case "AVG":
          return [
            expression.alias,
            values.length === 0
              ? null
              : values.reduce<number>((total, value) => total + numeric(value), 0) / values.length,
          ];
        case "MIN":
          return [
            expression.alias,
            values.length === 0 ? null : [...values].sort(compareValues)[0],
          ];
        case "MAX":
          return [
            expression.alias,
            values.length === 0 ? null : [...values].sort(compareValues).at(-1),
          ];
      }
    }),
  );
}

function resolveReference(context: RowContext, reference: string): unknown {
  const parts = reference.split(".");
  if (parts.length === 2) return context[parts[0] ?? ""]?.[parts[1] ?? ""];
  const name = parts[0] ?? "";
  const matches = Object.values(context).filter((row) => row !== undefined && name in row);
  if (matches.length !== 1) throw new TypeError(`Ambiguous or missing column: ${reference}`);
  return matches[0]?.[name];
}

function compare(actual: unknown, predicate: Predicate): boolean {
  const left = comparable(actual);
  const right = comparable(predicate.value);
  switch (predicate.operator) {
    case "=":
      return left === right;
    case "!=":
    case "<>":
      return left !== right;
    case ">":
      return compareValues(left, right) > 0;
    case ">=":
      return compareValues(left, right) >= 0;
    case "<":
      return compareValues(left, right) < 0;
    case "<=":
      return compareValues(left, right) <= 0;
  }
}

function comparable(value: unknown): unknown {
  return value instanceof Date ? value.getTime() : value;
}

function compareValues(left: unknown, right: unknown): number {
  const a = comparable(left);
  const b = comparable(right);
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;
  if (typeof a === "number" && typeof b === "number") return a - b;
  return comparableText(a).localeCompare(comparableText(b));
}

function comparableText(value: unknown): string {
  if (typeof value === "string" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  return JSON.stringify(value);
}

function numeric(value: unknown): number {
  if (typeof value !== "number") throw new TypeError("SUM and AVG require numeric columns");
  return value;
}

function parseReference(value: string): string {
  const trimmed = value.trim();
  if (!/^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?$/.test(trimmed)) {
    throw new TypeError(`Invalid column reference: ${trimmed}`);
  }
  return trimmed;
}

function splitComma(value: string): string[] {
  const parts: string[] = [];
  let start = 0;
  let depth = 0;
  let quoted = false;
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];
    if (character === "'" && value[index - 1] !== "\\") quoted = !quoted;
    if (quoted) continue;
    if (character === "(") depth += 1;
    else if (character === ")") depth -= 1;
    else if (character === "," && depth === 0) {
      parts.push(value.slice(start, index).trim());
      start = index + 1;
    }
  }
  parts.push(value.slice(start).trim());
  return parts.filter((part) => part.length > 0);
}

function projectedColumnNames(select: readonly SelectExpression[]): Record<string, null> {
  return Object.fromEntries(
    select
      .filter(
        (expression): expression is ColumnExpression | AggregateExpression =>
          expression.kind !== "wildcard",
      )
      .map((expression) => [expression.alias, null]),
  );
}
