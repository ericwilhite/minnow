import type { DatabaseRow } from "./database.js";
import { QueryMemoryContext, type QueryMemoryUsage } from "./memory.js";
import {
  columnarTableFromRows,
  prepareVectorQuery,
  type ColumnarTable,
  type AsyncQueryExecutionOptions,
  type PreparedVectorQuery,
} from "./vector.js";

export type QueryValue = boolean | number | string | Date | null;
export type QueryRow = Record<string, QueryValue>;

export interface QueryResult {
  columns: string[];
  rows: QueryRow[];
}

export interface PreparedQuery {
  readonly sql: string;
  readonly tables: string[];
  /**
   * Current retained and high-water byte counts for the documented modeled query-memory scope.
   * Result-construction bytes affect the peak but transfer out of the context when execute returns.
   */
  readonly memoryUsage: QueryMemoryUsage;
  execute(): QueryResult;
  executeAsync(options?: AsyncQueryExecutionOptions): Promise<QueryResult>;
  close(): void;
}

export interface QueryExecutionOptions {
  /**
   * Bounds the modeled vector, row-index, group/result payload, and ordering buffers. This is not a
   * total JavaScript heap limit; input preparation, container/allocator overhead, and returned-result
   * lifetime are excluded. The schema-less empty-table row adapter rejects configured budgets;
   * BrowserDatabase retains catalog types and does not use that fallback.
   */
  readonly executionMemoryBudgetBytes?: number;
}

export type BinaryOperator = "+" | "-" | "*" | "/";
export type ComparisonOperator = "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=";
export type AggregateName = "COUNT" | "SUM" | "AVG" | "MIN" | "MAX";

export type Expression =
  | { kind: "literal"; value: QueryValue }
  | { kind: "column"; reference: string }
  | { kind: "wildcard" }
  | { kind: "binary"; operator: BinaryOperator; left: Expression; right: Expression }
  | { kind: "call"; name: AggregateName | "ROUND"; arguments: Expression[] }
  | { kind: "list"; items: Expression[] }
  | { kind: "subquery"; block: CompiledQuery };

export interface SelectItem {
  expression: Expression;
  alias: string;
}

interface TableSource {
  table: string;
  alias: string;
  /** A parenthesized SELECT or expanded CTE body; `table` is then a unique synthetic name. */
  derived?: CompiledQuery;
  /** A top-level set operation; members combine positionally under the first member's schema. */
  union?: { blocks: CompiledQuery[]; alls: boolean[] };
}

interface JoinPlan extends TableSource {
  kind: "inner" | "left";
  left: Expression;
  right: Expression;
}

export type PredicateOperator = ComparisonOperator | "IN" | "NOT IN";

interface Predicate {
  left: Expression;
  operator: PredicateOperator;
  right: Expression;
}

export interface CompiledQuery {
  sql: string;
  base: TableSource;
  joins: JoinPlan[];
  select: SelectItem[];
  predicates: Predicate[];
  groupBy: Expression[];
  having: Predicate[];
  orderBy: Array<{ expression: Expression; direction: "asc" | "desc" }>;
  limit?: number;
}

type RowContext = Record<string, DatabaseRow | undefined>;

interface Token {
  kind: "identifier" | "number" | "string" | "operator" | "punctuation" | "eof";
  text: string;
}

const clauseKeywords = new Set([
  "WHERE",
  "GROUP",
  "HAVING",
  "ORDER",
  "LIMIT",
  "JOIN",
  "INNER",
  "LEFT",
  "UNION",
]);
const aggregateNames = new Set<AggregateName>(["COUNT", "SUM", "AVG", "MIN", "MAX"]);

export function compileQuery(sql: string): CompiledQuery {
  const normalized = sql.trim().replace(/;$/, "").trim();
  if (normalized.length === 0) throw new TypeError("Enter a SELECT query");
  const parser = new Parser(tokenize(normalized));
  return parser.parse(normalized);
}

export interface SubqueryResolutionStep {
  /** The uncorrelated block to execute; earlier steps have already substituted inside it. */
  readonly block: CompiledQuery;
  /** Replaces the subquery node with the executed result as literals. */
  substitute(result: QueryResult): void;
}

/**
 * Clones a plan and returns its subquery sites in post-order: executing each step's block and
 * substituting its result leaves the returned plan free of subquery nodes. A scalar subquery must
 * select one column and return at most one row (empty is NULL); an IN subquery must select one
 * column and becomes a literal membership list. Correlated references fail inside the subquery's
 * own scope as unknown aliases.
 */
export function subqueryResolutionSteps(plan: CompiledQuery): {
  plan: CompiledQuery;
  steps: SubqueryResolutionStep[];
} {
  if (!blockHasSubqueries(plan)) return { plan, steps: [] };
  const clone = structuredClone(plan);
  const steps: SubqueryResolutionStep[] = [];
  const rewrite = (expression: Expression, replace: (next: Expression) => void): void => {
    if (expression.kind === "subquery") {
      collectBlock(expression.block);
      steps.push({
        block: expression.block,
        substitute(result) {
          if (result.columns.length !== 1) {
            throw new TypeError("A scalar subquery must select exactly one column");
          }
          if (result.rows.length > 1) {
            throw new TypeError(`A scalar subquery returned ${String(result.rows.length)} rows`);
          }
          replace({
            kind: "literal",
            value: result.rows[0]?.[result.columns[0] ?? ""] ?? null,
          });
        },
      });
      return;
    }
    if (expression.kind === "binary") {
      rewrite(expression.left, (next) => (expression.left = next));
      rewrite(expression.right, (next) => (expression.right = next));
      return;
    }
    if (expression.kind === "call") {
      expression.arguments.forEach((argument, index) => {
        rewrite(argument, (next) => (expression.arguments[index] = next));
      });
      return;
    }
    if (expression.kind === "list") {
      expression.items.forEach((item, index) => {
        rewrite(item, (next) => (expression.items[index] = next));
      });
    }
  };
  const collectBlock = (block: CompiledQuery): void => {
    for (const source of [block.base, ...block.joins]) {
      if (source.derived !== undefined) collectBlock(source.derived);
      source.union?.blocks.forEach(collectBlock);
    }
    for (const item of block.select) {
      rewrite(item.expression, (next) => (item.expression = next));
    }
    block.groupBy.forEach((expression, index) => {
      rewrite(expression, (next) => (block.groupBy[index] = next));
    });
    for (const predicate of [...block.predicates, ...block.having]) {
      rewrite(predicate.left, (next) => (predicate.left = next));
      const right = predicate.right;
      if (
        (predicate.operator === "IN" || predicate.operator === "NOT IN") &&
        right.kind === "subquery"
      ) {
        collectBlock(right.block);
        steps.push({
          block: right.block,
          substitute(result) {
            if (result.columns.length !== 1) {
              throw new TypeError("An IN subquery must select exactly one column");
            }
            predicate.right = {
              kind: "list",
              items: result.rows.map((row) => ({
                kind: "literal",
                value: row[result.columns[0] ?? ""] ?? null,
              })),
            };
          },
        });
        continue;
      }
      rewrite(predicate.right, (next) => (predicate.right = next));
    }
    for (const order of block.orderBy) {
      rewrite(order.expression, (next) => (order.expression = next));
    }
  };
  collectBlock(clone);
  return { plan: clone, steps };
}

export function blockHasSubqueries(plan: CompiledQuery): boolean {
  const expressionHas = (expression: Expression): boolean => {
    if (expression.kind === "subquery") return true;
    if (expression.kind === "binary") {
      return expressionHas(expression.left) || expressionHas(expression.right);
    }
    if (expression.kind === "call") return expression.arguments.some(expressionHas);
    if (expression.kind === "list") return expression.items.some(expressionHas);
    return false;
  };
  const blockHas = (block: CompiledQuery): boolean =>
    block.select.some((item) => expressionHas(item.expression)) ||
    block.groupBy.some(expressionHas) ||
    [...block.predicates, ...block.having].some(
      (predicate) => expressionHas(predicate.left) || expressionHas(predicate.right),
    ) ||
    block.orderBy.some((order) => expressionHas(order.expression)) ||
    [block.base, ...block.joins].some(
      (source) =>
        (source.derived !== undefined && blockHas(source.derived)) ||
        source.union?.blocks.some(blockHas) === true,
    );
  return blockHas(plan);
}

export type SqlColumnType = "boolean" | "number" | "string" | "datetime";

export interface SqlColumnSchema {
  name: string;
  type: SqlColumnType;
}

/**
 * Infers the typed output schema of one select block from typed source schemas. A column whose
 * type cannot be established (for example a bare NULL literal) is rejected explicitly, so a
 * derived table always has concrete column types even when its result is empty.
 */
export function inferBlockSchema(
  plan: CompiledQuery,
  schemas: ReadonlyMap<string, readonly SqlColumnSchema[]>,
): SqlColumnSchema[] {
  const sources = [plan.base, ...plan.joins];
  if (plan.select[0]?.expression.kind === "wildcard") {
    const multiple = sources.length > 1;
    return sources.flatMap((source) =>
      (schemas.get(source.table) ?? []).map((column) => ({
        name: multiple ? `${source.alias}.${column.name}` : column.name,
        type: column.type,
      })),
    );
  }
  const resolveColumnType = (reference: string): SqlColumnType | "null" => {
    const parts = reference.split(".");
    if (parts.length === 2) {
      const source = sources.find(({ alias }) => alias === parts[0]);
      if (source === undefined) throw new TypeError(`Unknown table alias: ${parts[0] ?? ""}`);
      const column = (schemas.get(source.table) ?? []).find(({ name }) => name === parts[1]);
      if (column === undefined) throw new TypeError(`Unknown column: ${reference}`);
      return column.type;
    }
    const matches = sources.flatMap((source) =>
      (schemas.get(source.table) ?? [])
        .filter(({ name }) => name === parts[0])
        .map((column) => column.type),
    );
    if (matches.length !== 1) throw new TypeError(`Ambiguous or missing column: ${reference}`);
    return matches[0] ?? "string";
  };
  const infer = (expression: Expression): SqlColumnType | "null" => {
    if (expression.kind === "subquery" || expression.kind === "list") {
      throw new TypeError("Subqueries must be resolved before schema inference");
    }
    if (expression.kind === "literal") {
      const value = expression.value;
      if (value === null) return "null";
      if (typeof value === "boolean") return "boolean";
      if (typeof value === "number") return "number";
      if (typeof value === "string") return "string";
      return "datetime";
    }
    if (expression.kind === "wildcard") return "number";
    if (expression.kind === "column") return resolveColumnType(expression.reference);
    if (expression.kind === "binary") {
      for (const side of [expression.left, expression.right]) {
        const type = infer(side);
        if (type === "string" || type === "boolean") {
          throw new TypeError(`Arithmetic requires numeric operands: ${plan.sql}`);
        }
      }
      return "number";
    }
    if (expression.name === "COUNT" || expression.name === "SUM" || expression.name === "AVG") {
      return "number";
    }
    if (expression.name === "ROUND") return "number";
    const argument = expression.arguments[0];
    return argument === undefined ? "null" : infer(argument);
  };
  return plan.select.map((item) => {
    const type = infer(item.expression);
    if (type === "null") {
      throw new TypeError(`Cannot infer a column type for output ${item.alias}`);
    }
    return { name: item.alias, type };
  });
}

export function referencedColumns(
  plan: CompiledQuery,
  schemas: ReadonlyMap<string, readonly string[]>,
): Map<string, string[]> {
  const sources = [plan.base, ...plan.joins];
  const sourceAliases = sources.map((source) => source.alias);
  if (new Set(sourceAliases).size !== sourceAliases.length)
    throw new TypeError("Table aliases must be unique");
  validateGrouping(plan);
  const aliases = new Map(sources.map((source) => [source.alias, source.table]));
  const requested = new Map<string, Set<string>>(
    sources.map((source) => [source.table, new Set<string>()]),
  );
  const outputAliases = new Set(plan.select.map((item) => item.alias));
  const expressions = [
    ...plan.select.map((item) => item.expression),
    ...plan.joins.flatMap((join) => [join.left, join.right]),
    ...plan.predicates.flatMap((predicate) => [predicate.left, predicate.right]),
    ...plan.groupBy,
    ...plan.having.flatMap((predicate) => [predicate.left, predicate.right]),
  ];
  const collect = (expression: Expression, allowOutputAlias: boolean) => {
    for (const reference of expressionColumns(expression)) {
      if (allowOutputAlias && !reference.includes(".") && outputAliases.has(reference)) continue;
      const parts = reference.split(".");
      if (parts.length === 2) {
        const table = aliases.get(parts[0] ?? "");
        if (table === undefined) throw new TypeError(`Unknown table alias: ${parts[0] ?? ""}`);
        const column = parts[1] ?? "";
        if (!(schemas.get(table) ?? []).includes(column)) {
          throw new TypeError(`Unknown column: ${reference}`);
        }
        requested.get(table)?.add(column);
      } else {
        const column = parts[0] ?? "";
        const matches = sources.filter((source) =>
          (schemas.get(source.table) ?? []).includes(column),
        );
        if (matches.length !== 1) throw new TypeError(`Ambiguous or missing column: ${reference}`);
        requested.get(matches[0]?.table ?? "")?.add(column);
      }
    }
    if (expression.kind === "wildcard") {
      for (const source of sources) {
        for (const column of schemas.get(source.table) ?? [])
          requested.get(source.table)?.add(column);
      }
    }
  };
  for (const expression of expressions) collect(expression, false);
  for (const { expression } of plan.orderBy) collect(expression, true);
  return new Map([...requested].map(([table, columns]) => [table, [...columns]]));
}

export function createPreparedQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
  options: QueryExecutionOptions = {},
): PreparedQuery {
  validateGrouping(plan);
  const resolution = subqueryResolutionSteps(plan);
  for (const step of resolution.steps) step.substitute(executeRowQuery(step.block, tables));
  plan = resolution.plan;
  tables = resolveDerivedRowTables(plan, tables);
  const memory = new QueryMemoryContext(options.executionMemoryBudgetBytes);
  if ([...tables.values()].some((rows) => rows.length === 0)) {
    if (options.executionMemoryBudgetBytes !== undefined) {
      memory.close();
      throw new TypeError(
        "Query memory budgets require typed columnar schemas when an input table is empty",
      );
    }
    return createPreparedRowQuery(plan, tables, memory);
  }
  try {
    return createPreparedColumnarQuery(plan, normalizeColumnarTables(plan, tables), memory);
  } catch (error) {
    memory.close();
    throw error;
  }
}

/** Internal columnar entry point used after BrowserDatabase materializes a stable snapshot. */
export function createPreparedColumnarQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, ColumnarTable>,
  memory: QueryMemoryContext = new QueryMemoryContext(),
): PreparedQuery {
  validateGrouping(plan);
  let closed = false;
  let prepared: PreparedVectorQuery | undefined;
  try {
    prepared = prepareVectorQuery(plan, tables, { memoryContext: memory });
  } catch (error) {
    memory.close();
    throw error;
  }
  return {
    sql: plan.sql,
    tables: [plan.base.table, ...plan.joins.map((join) => join.table)],
    get memoryUsage() {
      return memory.usage;
    },
    execute() {
      if (closed || prepared === undefined) throw new Error("Prepared query is closed");
      return prepared.execute();
    },
    async executeAsync(options) {
      if (closed || prepared === undefined) throw new Error("Prepared query is closed");
      return prepared.executeAsync(options);
    },
    close() {
      if (closed) return;
      closed = true;
      prepared?.close();
      prepared = undefined;
      memory.close();
    },
  };
}

function createPreparedRowQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
  memory: QueryMemoryContext,
): PreparedQuery {
  let closed = false;
  let rows: ReadonlyMap<string, DatabaseRow[]> | undefined = cloneRowTables(tables);
  return {
    sql: plan.sql,
    tables: [plan.base.table, ...plan.joins.map((join) => join.table)],
    get memoryUsage() {
      return memory.usage;
    },
    execute() {
      if (closed || rows === undefined) throw new Error("Prepared query is closed");
      return executeRowQuery(plan, cloneRowTables(rows));
    },
    async executeAsync() {
      if (closed || rows === undefined) throw new Error("Prepared query is closed");
      return executeRowQuery(plan, cloneRowTables(rows));
    },
    close() {
      closed = true;
      rows = undefined;
      memory.close();
    },
  };
}

function cloneRowTables(
  tables: ReadonlyMap<string, DatabaseRow[]>,
): ReadonlyMap<string, DatabaseRow[]> {
  return new Map(
    [...tables].map(([name, rows]) => [
      name,
      rows.map((row) =>
        Object.fromEntries(
          Object.entries(row).map(([column, value]) => [
            column,
            value instanceof Date ? new Date(value.getTime()) : value,
          ]),
        ),
      ),
    ]),
  );
}

export function executeQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
  options: QueryExecutionOptions = {},
): QueryResult {
  const prepared = createPreparedQuery(plan, tables, options);
  try {
    return prepared.execute();
  } finally {
    prepared.close();
  }
}

/**
 * Combines set-operation member results positionally under the first member's column names,
 * folding left: UNION deduplicates the entire accumulated set, UNION ALL concatenates. Values
 * compare with SQL grouping semantics (NULLs equal, dates by instant, -0 distinct from 0).
 */
export function combineUnionResults(
  results: readonly QueryResult[],
  alls: readonly boolean[],
): QueryResult {
  const first = results[0];
  if (first === undefined) throw new TypeError("A UNION requires at least one member");
  const columns = first.columns;
  const renamed = results.map((result, index) => {
    if (result.columns.length !== columns.length) {
      throw new TypeError("UNION members must select the same number of columns");
    }
    if (index === 0) return result.rows;
    return result.rows.map((row) =>
      Object.fromEntries(
        columns.map((name, column) => [name, row[result.columns[column] ?? ""] ?? null]),
      ),
    );
  });
  const encode = (row: QueryRow): string =>
    JSON.stringify(
      columns.map((name) => {
        const value = row[name] ?? null;
        if (value === null) return [0];
        if (typeof value === "boolean") return [1, value];
        if (typeof value === "number") return [2, Object.is(value, -0) ? "-0" : String(value)];
        if (typeof value === "string") return [3, value];
        return [4, value.getTime()];
      }),
    );
  let combined = renamed[0] ?? [];
  for (let index = 1; index < renamed.length; index += 1) {
    const next = renamed[index] ?? [];
    if (alls[index - 1] === true) {
      combined = [...combined, ...next];
      continue;
    }
    const seen = new Set<string>();
    const deduplicated: QueryRow[] = [];
    for (const row of [...combined, ...next]) {
      const key = encode(row);
      if (seen.has(key)) continue;
      seen.add(key);
      deduplicated.push(row);
    }
    combined = deduplicated;
  }
  return { columns: [...columns], rows: combined };
}

/** Executes each derived or set-operation source with the row reference. */
function resolveDerivedRowTables(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
): ReadonlyMap<string, DatabaseRow[]> {
  const sources = [plan.base, ...plan.joins];
  if (!sources.some((source) => source.derived !== undefined || source.union !== undefined)) {
    return tables;
  }
  const resolved = new Map(tables);
  for (const source of sources) {
    if (source.union !== undefined) {
      const results = source.union.blocks.map((block) => executeRowQuery(block, tables));
      resolved.set(source.table, combineUnionResults(results, source.union.alls).rows);
      continue;
    }
    if (source.derived === undefined) continue;
    resolved.set(source.table, executeRowQuery(source.derived, tables).rows);
  }
  return resolved;
}

function normalizeColumnarTables(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
): Map<string, ColumnarTable> {
  const requiredTables = new Set([plan.base.table, ...plan.joins.map((join) => join.table)]);
  const schemas = new Map(
    [...tables].map(([name, rows]) => [
      name,
      [...new Set(rows.flatMap((row) => Object.keys(row)))],
    ]),
  );
  const requestedColumns = referencedColumns(plan, schemas);
  return new Map(
    [...tables]
      .filter(([name]) => requiredTables.has(name))
      .map(([name, table]) => [
        name,
        columnarTableFromRows(name, table, requestedColumns.get(name) ?? []),
      ]),
  );
}

/** Correctness reference retained while the vector executor matures. */
export function executeRowQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
): QueryResult {
  const resolution = subqueryResolutionSteps(plan);
  for (const step of resolution.steps) step.substitute(executeRowQuery(step.block, tables));
  plan = resolution.plan;
  tables = resolveDerivedRowTables(plan, tables);
  let contexts: RowContext[] = (tables.get(plan.base.table) ?? []).map((row) => ({
    [plan.base.alias]: row,
  }));
  for (const join of plan.joins)
    contexts = executeJoin(contexts, join, tables.get(join.table) ?? []);
  contexts = contexts.filter((context) =>
    plan.predicates.every((predicate) => evaluatePredicate(predicate, context)),
  );
  const grouped =
    plan.groupBy.length > 0 || plan.select.some((item) => hasAggregate(item.expression));
  let rows: QueryRow[];
  if (grouped) {
    const groups = new Map<string, RowContext[]>();
    if (contexts.length === 0 && plan.groupBy.length === 0) groups.set("[]", []);
    for (const context of contexts) {
      const key = JSON.stringify(
        plan.groupBy.map((expression) => comparable(evaluate(expression, context))),
      );
      const group = groups.get(key) ?? [];
      group.push(context);
      groups.set(key, group);
    }
    rows = [...groups.values()]
      .filter((group) =>
        plan.having.every((predicate) =>
          comparisonHolds(
            predicate.operator,
            evaluate(predicate.left, group[0] ?? {}, group),
            evaluate(predicate.right, group[0] ?? {}, group),
          ),
        ),
      )
      .map((group) => project(plan.select, group[0] ?? {}, group));
  } else {
    rows = contexts.map((context) => project(plan.select, context));
  }
  if (plan.orderBy.length > 0) {
    const sortColumns = plan.orderBy.map(({ expression, direction }) => {
      const outputName = orderOutputName(expression, plan.select);
      if (outputName === undefined)
        throw new TypeError("ORDER BY requires a selected column or output alias");
      return { outputName, multiplier: direction === "desc" ? -1 : 1 };
    });
    rows.sort((left, right) => {
      for (const { outputName, multiplier } of sortColumns) {
        const comparison = compareValues(left[outputName], right[outputName]);
        if (comparison !== 0) return comparison * multiplier;
      }
      return 0;
    });
  }
  if (plan.limit !== undefined) rows = rows.slice(0, plan.limit);
  const columns =
    plan.select[0]?.expression.kind === "wildcard"
      ? Object.keys(rows[0] ?? {})
      : plan.select.map((item) => item.alias);
  return { columns, rows };
}

function orderOutputName(
  expression: Expression,
  select: readonly SelectItem[],
): string | undefined {
  if (expression.kind !== "column") return undefined;
  // A wildcard select exposes source columns under their own names, so bare references order by
  // the matching output column directly.
  if (select[0]?.expression.kind === "wildcard") return expression.reference;
  const selected = select.find(
    (item) =>
      (item.expression.kind === "column" && item.expression.reference === expression.reference) ||
      (!expression.reference.includes(".") && item.alias === expression.reference),
  );
  return selected?.alias;
}

function executeJoin(contexts: RowContext[], join: JoinPlan, rows: DatabaseRow[]): RowContext[] {
  const leftAliases = expressionAliases(join.left);
  const rightAliases = expressionAliases(join.right);
  const rightExpression = rightAliases.has(join.alias) ? join.right : join.left;
  const leftExpression = rightExpression === join.right ? join.left : join.right;
  if (
    !expressionAliases(rightExpression).has(join.alias) ||
    leftAliases.has(join.alias) === rightAliases.has(join.alias)
  ) {
    throw new TypeError(`JOIN condition must reference the new alias ${join.alias} on one side`);
  }
  const index = new Map<unknown, DatabaseRow[]>();
  for (const row of rows) {
    const key = comparable(evaluate(rightExpression, { [join.alias]: row }));
    if (!isSqlJoinKey(key)) continue;
    const matches = index.get(key) ?? [];
    matches.push(row);
    index.set(key, matches);
  }
  const joined: RowContext[] = [];
  for (const context of contexts) {
    const leftKey = comparable(evaluate(leftExpression, context));
    const matches = isSqlJoinKey(leftKey) ? (index.get(leftKey) ?? []) : [];
    if (matches.length === 0 && join.kind === "left")
      joined.push({ ...context, [join.alias]: undefined });
    else for (const row of matches) joined.push({ ...context, [join.alias]: row });
  }
  return joined;
}

function isSqlJoinKey(value: unknown): boolean {
  return (
    value !== null && value !== undefined && !(typeof value === "number" && Number.isNaN(value))
  );
}

function project(
  select: readonly SelectItem[],
  context: RowContext,
  group?: RowContext[],
): QueryRow {
  if (select[0]?.expression.kind === "wildcard") {
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
    select.map((item) => [item.alias, asQueryValue(evaluate(item.expression, context, group))]),
  );
}

function evaluate(expression: Expression, context: RowContext, group?: RowContext[]): unknown {
  switch (expression.kind) {
    case "literal":
      return expression.value;
    case "wildcard":
      return 1;
    case "subquery":
      throw new TypeError("Subqueries are only supported in WHERE, HAVING, SELECT, and IN");
    case "list":
      throw new TypeError("Value lists are only supported with IN");
    case "column":
      return resolveColumn(context, expression.reference);
    case "binary": {
      const left = evaluate(expression.left, context, group);
      const right = evaluate(expression.right, context, group);
      if (left === null || left === undefined || right === null || right === undefined) return null;
      const a = numeric(left);
      const b = numeric(right);
      if (expression.operator === "+") return a + b;
      if (expression.operator === "-") return a - b;
      if (expression.operator === "*") return a * b;
      return a / b;
    }
    case "call": {
      if (aggregateNames.has(expression.name as AggregateName)) {
        if (group === undefined)
          throw new TypeError(`${expression.name} requires grouped execution`);
        const argument = expression.arguments[0] ?? { kind: "wildcard" as const };
        const values =
          argument.kind === "wildcard"
            ? group.map(() => 1)
            : group
                .map((row) => evaluate(argument, row))
                .filter((value) => value !== null && value !== undefined);
        if (expression.name === "COUNT") return values.length;
        if (expression.name === "SUM")
          return values.length === 0
            ? null
            : values.reduce<number>((sum, value) => sum + numeric(value), 0);
        if (expression.name === "AVG")
          return values.length === 0
            ? null
            : values.reduce<number>((sum, value) => sum + numeric(value), 0) / values.length;
        if (expression.name === "MIN")
          return values.reduce<unknown>(
            (best, value) => (best === undefined || compareValues(value, best) < 0 ? value : best),
            undefined,
          );
        return values.reduce<unknown>(
          (best, value) => (best === undefined || compareValues(value, best) > 0 ? value : best),
          undefined,
        );
      }
      const value = evaluate(
        expression.arguments[0] ?? { kind: "literal", value: 0 },
        context,
        group,
      );
      if (value === null || value === undefined) return null;
      const digits =
        expression.arguments[1] === undefined
          ? 0
          : numeric(evaluate(expression.arguments[1], context, group));
      const factor = 10 ** digits;
      return Math.round(numeric(value) * factor) / factor;
    }
  }
}

function evaluatePredicate(predicate: Predicate, context: RowContext): boolean {
  if (predicate.operator === "IN" || predicate.operator === "NOT IN") {
    if (predicate.right.kind !== "list") throw new TypeError("IN requires a value list");
    return inListHolds(
      predicate.operator,
      evaluate(predicate.left, context),
      predicate.right.items.map((item) => evaluate(item, context)),
    );
  }
  return comparisonHolds(
    predicate.operator,
    evaluate(predicate.left, context),
    evaluate(predicate.right, context),
  );
}

/**
 * SQL membership semantics: a NULL probe never matches, and NOT IN cannot be satisfied when the
 * list contains NULL because the comparison is unknown rather than false.
 */
function inListHolds(
  operator: "IN" | "NOT IN",
  value: unknown,
  items: readonly unknown[],
): boolean {
  if (value === null || value === undefined) return false;
  let hasNull = false;
  for (const item of items) {
    if (item === null || item === undefined) {
      hasNull = true;
      continue;
    }
    if (comparable(value) === comparable(item)) return operator === "IN";
  }
  return operator === "NOT IN" && !hasNull;
}

function comparisonHolds(
  operator: PredicateOperator,
  leftValue: unknown,
  rightValue: unknown,
): boolean {
  if (operator === "IN" || operator === "NOT IN") {
    throw new TypeError("IN is only supported in WHERE predicates");
  }
  if (
    leftValue === null ||
    leftValue === undefined ||
    rightValue === null ||
    rightValue === undefined
  )
    return false;
  const left = comparable(leftValue);
  const right = comparable(rightValue);
  if (operator === "=") return left === right;
  if (operator === "!=" || operator === "<>") return left !== right;
  const comparison = compareValues(left, right);
  if (operator === ">") return comparison > 0;
  if (operator === ">=") return comparison >= 0;
  if (operator === "<") return comparison < 0;
  return comparison <= 0;
}

function resolveColumn(context: RowContext, reference: string): unknown {
  const parts = reference.split(".");
  if (parts.length === 2) {
    const alias = parts[0] ?? "";
    if (!(alias in context)) throw new TypeError(`Unknown table alias: ${alias}`);
    return context[alias]?.[parts[1] ?? ""];
  }
  const name = parts[0] ?? "";
  const matches = Object.values(context).filter((row) => row !== undefined && name in row);
  if (matches.length !== 1) throw new TypeError(`Ambiguous or missing column: ${reference}`);
  return matches[0]?.[name];
}

function hasAggregate(expression: Expression): boolean {
  if (expression.kind === "call") {
    return (
      aggregateNames.has(expression.name as AggregateName) ||
      expression.arguments.some(hasAggregate)
    );
  }
  if (expression.kind === "binary") {
    return hasAggregate(expression.left) || hasAggregate(expression.right);
  }
  if (expression.kind === "list") return expression.items.some(hasAggregate);
  return false;
}

function validateGrouping(plan: CompiledQuery): void {
  const grouped =
    plan.groupBy.length > 0 || plan.select.some((item) => hasAggregate(item.expression));
  if (!grouped) return;
  const groupExpressions = new Set(plan.groupBy.map((expression) => JSON.stringify(expression)));
  for (const item of plan.select) {
    if (hasAggregate(item.expression)) continue;
    if (!groupExpressions.has(JSON.stringify(item.expression))) {
      throw new TypeError(`Selected column must appear in GROUP BY: ${item.alias}`);
    }
  }
  const forbiddenAggregates = [
    ...plan.joins.flatMap((join) => [join.left, join.right]),
    ...plan.predicates.flatMap((predicate) => [predicate.left, predicate.right]),
    ...plan.groupBy,
  ];
  if (forbiddenAggregates.some(hasAggregate))
    throw new TypeError("Aggregate functions are not allowed in JOIN, WHERE, or GROUP BY");
}

function expressionColumns(expression: Expression): string[] {
  if (expression.kind === "column") return [expression.reference];
  if (expression.kind === "binary")
    return [...expressionColumns(expression.left), ...expressionColumns(expression.right)];
  if (expression.kind === "call") return expression.arguments.flatMap(expressionColumns);
  if (expression.kind === "list") return expression.items.flatMap(expressionColumns);
  // A subquery resolves inside its own scope before the enclosing block binds.
  return [];
}

function expressionAliases(expression: Expression): Set<string> {
  return new Set(
    expressionColumns(expression).flatMap((reference) =>
      reference.includes(".") ? [reference.split(".")[0] ?? ""] : [],
    ),
  );
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
  if (typeof a === "number" && typeof b === "number") {
    if (Number.isNaN(a)) return Number.isNaN(b) ? 0 : 1;
    if (Number.isNaN(b)) return -1;
    return a - b;
  }
  if (typeof a === "string" && typeof b === "string") return a.localeCompare(b);
  if (typeof a === "boolean" && typeof b === "boolean") return Number(a) - Number(b);
  throw new TypeError("Values must have comparable SQL types");
}

function numeric(value: unknown): number {
  if (typeof value !== "number")
    throw new TypeError("Arithmetic and numeric aggregates require numbers");
  return value;
}

function asQueryValue(value: unknown): QueryValue {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string" ||
    value instanceof Date
  )
    return value;
  if (value === undefined) return null;
  throw new TypeError("Query produced an unsupported value");
}

class Parser {
  #index = 0;
  #derivedSequence = 0;
  readonly #ctes = new Map<string, CompiledQuery>();
  readonly #ctesInProgress = new Set<string>();

  constructor(private readonly tokens: Token[]) {}

  parse(sql: string): CompiledQuery {
    if (this.#isKeyword("WITH")) {
      this.#keyword("WITH");
      for (;;) {
        const name = this.#identifier();
        if (this.#ctes.has(name) || this.#ctesInProgress.has(name)) {
          throw new TypeError(`Duplicate CTE name: ${name}`);
        }
        this.#keyword("AS");
        this.#expectPunctuation("(");
        this.#ctesInProgress.add(name);
        const block = this.#selectBlock("(cte)");
        this.#ctesInProgress.delete(name);
        this.#expectPunctuation(")");
        this.#ctes.set(name, block);
        if (!this.#punctuation(",")) break;
      }
    }
    const firstMember = this.#unionMember(sql);
    let plan = firstMember.block;
    if (this.#isKeyword("UNION")) {
      const members = [firstMember];
      const alls: boolean[] = [];
      while (this.#isKeyword("UNION")) {
        this.#keyword("UNION");
        if (this.#isKeyword("ALL")) {
          this.#keyword("ALL");
          alls.push(true);
        } else alls.push(false);
        members.push(this.#unionMember("(union member)"));
      }
      for (const [index, member] of members.entries()) {
        const last = index === members.length - 1;
        if (
          !member.parenthesized &&
          !last &&
          (member.block.orderBy.length > 0 || member.block.limit !== undefined)
        ) {
          throw new TypeError("ORDER BY or LIMIT in a UNION member requires parentheses");
        }
      }
      // Standard SQL assigns a trailing ORDER BY or LIMIT to the whole compound. After an
      // unparenthesized last member the clause was greedily parsed into that member and lifts
      // out; after a parenthesized member it is still unparsed.
      const last = members[members.length - 1];
      let orderBy: CompiledQuery["orderBy"] = [];
      let limit: number | undefined;
      if (last !== undefined && !last.parenthesized) {
        orderBy = last.block.orderBy;
        limit = last.block.limit;
        last.block.orderBy = [];
        delete last.block.limit;
      } else {
        orderBy = this.#orderByClause();
        limit = this.#limitClause();
      }
      this.#derivedSequence += 1;
      plan = {
        sql,
        base: {
          table: `(union ${String(this.#derivedSequence)})`,
          alias: "union",
          union: { blocks: members.map((member) => member.block), alls },
        },
        joins: [],
        select: [{ expression: { kind: "wildcard" }, alias: "*" }],
        predicates: [],
        groupBy: [],
        having: [],
        orderBy,
        ...(limit === undefined ? {} : { limit }),
      };
    }
    this.#take("eof");
    return plan;
  }

  #unionMember(sql: string): { block: CompiledQuery; parenthesized: boolean } {
    if (this.#punctuation("(")) {
      const block = this.#selectBlock(sql);
      this.#expectPunctuation(")");
      return { block, parenthesized: true };
    }
    return { block: this.#selectBlock(sql), parenthesized: false };
  }

  #orderByClause(): CompiledQuery["orderBy"] {
    const orderBy: CompiledQuery["orderBy"] = [];
    if (this.#isKeyword("ORDER")) {
      this.#keyword("ORDER");
      this.#keyword("BY");
      for (;;) {
        const expression = this.#expression();
        let direction: "asc" | "desc" = "asc";
        if (this.#isKeyword("DESC")) {
          this.#keyword("DESC");
          direction = "desc";
        } else if (this.#isKeyword("ASC")) this.#keyword("ASC");
        orderBy.push({ expression, direction });
        if (!this.#punctuation(",")) break;
      }
    }
    return orderBy;
  }

  #limitClause(): number | undefined {
    if (!this.#isKeyword("LIMIT")) return undefined;
    this.#keyword("LIMIT");
    const limit = Number(this.#take("number").text);
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100_000)
      throw new RangeError("LIMIT must be between 1 and 100,000");
    return limit;
  }

  #selectBlock(sql: string): CompiledQuery {
    this.#keyword("SELECT");
    let distinct = false;
    if (this.#isKeyword("DISTINCT")) {
      this.#keyword("DISTINCT");
      distinct = true;
    }
    const select = this.#selectList();
    this.#keyword("FROM");
    const base = this.#source();
    const joins: JoinPlan[] = [];
    while (this.#isKeyword("JOIN") || this.#isKeyword("INNER") || this.#isKeyword("LEFT")) {
      let kind: JoinPlan["kind"] = "inner";
      if (this.#isKeyword("INNER")) this.#keyword("INNER");
      else if (this.#isKeyword("LEFT")) {
        this.#keyword("LEFT");
        kind = "left";
      }
      this.#keyword("JOIN");
      const source = this.#source();
      this.#keyword("ON");
      const left = this.#expression();
      this.#operator("=");
      const right = this.#expression();
      joins.push({ ...source, kind, left, right });
    }
    const predicates: Predicate[] = [];
    if (this.#isKeyword("WHERE")) {
      this.#keyword("WHERE");
      for (;;) {
        predicates.push(this.#predicate(true));
        if (!this.#isKeyword("AND")) break;
        this.#keyword("AND");
      }
    }
    const groupBy: Expression[] = [];
    if (this.#isKeyword("GROUP")) {
      this.#keyword("GROUP");
      this.#keyword("BY");
      groupBy.push(...this.#expressionList());
    }
    if (distinct) {
      if (select.some((item) => item.expression.kind === "wildcard"))
        throw new TypeError("SELECT DISTINCT * is not supported");
      if (select.some((item) => hasAggregate(item.expression)))
        throw new TypeError("SELECT DISTINCT cannot be combined with aggregate functions");
      if (groupBy.length > 0)
        throw new TypeError("SELECT DISTINCT cannot be combined with GROUP BY");
      // DISTINCT is grouping by every selected expression, so it reuses the grouped executor,
      // its value-carrying spill, and streamed scan inputs unchanged.
      groupBy.push(...select.map((item) => item.expression));
    }
    const having: Predicate[] = [];
    if (this.#isKeyword("HAVING")) {
      this.#keyword("HAVING");
      if (distinct) throw new TypeError("SELECT DISTINCT cannot be combined with HAVING");
      for (;;) {
        having.push(this.#predicate(false));
        if (!this.#isKeyword("AND")) break;
        this.#keyword("AND");
      }
      const grouped = groupBy.length > 0 || select.some((item) => hasAggregate(item.expression));
      if (!grouped) throw new TypeError("HAVING requires GROUP BY or aggregate functions");
      const groupExpressions = new Set(groupBy.map((expression) => JSON.stringify(expression)));
      for (const predicate of having) {
        for (const side of [predicate.left, predicate.right]) {
          if (hasAggregate(side)) continue;
          if (expressionColumns(side).length === 0) continue;
          if (groupExpressions.has(JSON.stringify(side))) continue;
          throw new TypeError(
            "HAVING conditions must use aggregates, literals, or GROUP BY expressions",
          );
        }
      }
    }
    const orderBy = this.#orderByClause();
    const limit = this.#limitClause();
    return {
      sql,
      base,
      joins,
      select,
      predicates,
      groupBy,
      having,
      orderBy,
      ...(limit === undefined ? {} : { limit }),
    };
  }

  #selectList(): SelectItem[] {
    const items: SelectItem[] = [];
    for (;;) {
      const expression = this.#expression();
      let alias = defaultAlias(expression);
      if (this.#isKeyword("AS")) {
        this.#keyword("AS");
        alias = this.#identifier();
      }
      items.push({ expression, alias });
      if (!this.#punctuation(",")) break;
    }
    if (items.some((item) => item.expression.kind === "wildcard") && items.length > 1)
      throw new TypeError("SELECT * cannot be mixed with other expressions");
    const aliases = new Set<string>();
    for (const item of items) {
      if (aliases.has(item.alias)) throw new TypeError(`Duplicate output column: ${item.alias}`);
      aliases.add(item.alias);
    }
    return items;
  }

  #source(): TableSource {
    if (this.#punctuation("(")) {
      const derived = this.#selectBlock("(derived)");
      this.#expectPunctuation(")");
      const alias = this.#sourceAlias();
      if (alias === undefined) throw new TypeError("A derived table requires an alias");
      return this.#derivedSource(derived, alias);
    }
    const table = this.#identifier();
    if (this.#ctesInProgress.has(table)) {
      throw new TypeError(`Recursive CTEs are not supported: ${table}`);
    }
    const alias = this.#sourceAlias() ?? table;
    const cte = this.#ctes.get(table);
    if (cte !== undefined) return this.#derivedSource(structuredClone(cte), alias);
    return { table, alias };
  }

  #sourceAlias(): string | undefined {
    if (this.#isKeyword("AS")) {
      this.#keyword("AS");
      return this.#identifier();
    }
    if (
      this.#peek().kind === "identifier" &&
      !clauseKeywords.has(this.#peek().text.toUpperCase()) &&
      !this.#isKeyword("ON")
    ) {
      return this.#identifier();
    }
    return undefined;
  }

  #derivedSource(derived: CompiledQuery, alias: string): TableSource {
    this.#derivedSequence += 1;
    return { table: `(derived ${String(this.#derivedSequence)}) ${alias}`, alias, derived };
  }

  #expressionList(): Expression[] {
    const values = [this.#expression()];
    while (this.#punctuation(",")) values.push(this.#expression());
    return values;
  }

  #expression(minimumPrecedence = 0): Expression {
    let left = this.#primary();
    for (;;) {
      const operator = this.#peek().text;
      const precedence =
        operator === "*" || operator === "/" ? 20 : operator === "+" || operator === "-" ? 10 : -1;
      if (precedence < minimumPrecedence) break;
      this.#index += 1;
      left = {
        kind: "binary",
        operator: operator as BinaryOperator,
        left,
        right: this.#expression(precedence + 1),
      };
    }
    return left;
  }

  #primary(): Expression {
    const token = this.#peek();
    if (token.kind === "number") {
      this.#index += 1;
      return { kind: "literal", value: Number(token.text) };
    }
    if (token.kind === "string") {
      this.#index += 1;
      return { kind: "literal", value: token.text };
    }
    if (token.kind === "operator" && token.text === "-") {
      this.#index += 1;
      return {
        kind: "binary",
        operator: "*",
        left: { kind: "literal", value: -1 },
        right: this.#primary(),
      };
    }
    if (token.kind === "operator" && token.text === "*") {
      this.#index += 1;
      return { kind: "wildcard" };
    }
    if (this.#punctuation("(")) {
      if (this.#isKeyword("SELECT")) {
        const block = this.#selectBlock("(subquery)");
        this.#expectPunctuation(")");
        return { kind: "subquery", block };
      }
      const expression = this.#expression();
      this.#expectPunctuation(")");
      return expression;
    }
    const identifier = this.#identifier();
    const upper = identifier.toUpperCase();
    if (upper === "TRUE" || upper === "FALSE" || upper === "NULL")
      return { kind: "literal", value: upper === "NULL" ? null : upper === "TRUE" };
    if (upper === "DATE" && this.#peek().kind === "string") {
      const date = new Date(`${this.#take("string").text}T00:00:00.000Z`);
      if (!Number.isFinite(date.getTime())) throw new TypeError("Invalid DATE literal");
      return { kind: "literal", value: date };
    }
    if (this.#punctuation("(")) {
      const name = upper as AggregateName | "ROUND";
      if (!aggregateNames.has(name as AggregateName) && name !== "ROUND")
        throw new TypeError(`Unsupported function: ${identifier}`);
      const args: Expression[] = [];
      if (!this.#punctuation(")")) {
        args.push(...this.#expressionList());
        this.#expectPunctuation(")");
      }
      if (aggregateNames.has(name as AggregateName) && args.length !== 1)
        throw new TypeError(`${name} requires exactly one argument`);
      if (name === "ROUND" && (args.length < 1 || args.length > 2))
        throw new TypeError("ROUND requires one or two arguments");
      return { kind: "call", name, arguments: args };
    }
    let reference = identifier;
    if (this.#punctuation(".")) reference += `.${this.#identifier()}`;
    return { kind: "column", reference };
  }

  #comparison(): ComparisonOperator {
    const token = this.#take("operator");
    if (!["=", "!=", "<>", ">", ">=", "<", "<="].includes(token.text))
      throw new TypeError(`Expected comparison operator, found ${token.text}`);
    return token.text as ComparisonOperator;
  }

  #predicate(allowIn: boolean): Predicate {
    const left = this.#expression();
    if (this.#isKeyword("IN") || this.#isKeyword("NOT")) {
      if (!allowIn) throw new TypeError("IN is not supported in HAVING");
      let operator: PredicateOperator = "IN";
      if (this.#isKeyword("NOT")) {
        this.#keyword("NOT");
        operator = "NOT IN";
      }
      this.#keyword("IN");
      this.#expectPunctuation("(");
      if (this.#isKeyword("SELECT")) {
        const block = this.#selectBlock("(subquery)");
        this.#expectPunctuation(")");
        return { left, operator, right: { kind: "subquery", block } };
      }
      const items = this.#expressionList();
      this.#expectPunctuation(")");
      if (items.some((item) => item.kind === "wildcard" || item.kind === "subquery")) {
        throw new TypeError("IN lists accept only scalar expressions");
      }
      return { left, operator, right: { kind: "list", items } };
    }
    const operator = this.#comparison();
    const right = this.#expression();
    return { left, operator, right };
  }

  #identifier(): string {
    return this.#take("identifier").text;
  }

  #isKeyword(value: string): boolean {
    const token = this.#peek();
    return token.kind === "identifier" && token.text.toUpperCase() === value;
  }

  #keyword(value: string): void {
    if (!this.#isKeyword(value))
      throw new TypeError(`Expected ${value}, found ${this.#peek().text || "end of query"}`);
    this.#index += 1;
  }

  #operator(value: string): void {
    const token = this.#take("operator");
    if (token.text !== value) throw new TypeError(`Expected ${value}, found ${token.text}`);
  }

  #punctuation(value: string): boolean {
    const token = this.#peek();
    if (token.kind !== "punctuation" || token.text !== value) return false;
    this.#index += 1;
    return true;
  }

  #expectPunctuation(value: string): void {
    if (!this.#punctuation(value)) throw new TypeError(`Expected ${value}`);
  }

  #take(kind: Token["kind"]): Token {
    const token = this.#peek();
    if (token.kind !== kind)
      throw new TypeError(`Expected ${kind}, found ${token.text || "end of query"}`);
    this.#index += 1;
    return token;
  }

  #peek(): Token {
    return this.tokens[this.#index] ?? { kind: "eof", text: "" };
  }
}

function defaultAlias(expression: Expression): string {
  if (expression.kind === "column")
    return expression.reference.split(".").at(-1) ?? expression.reference;
  if (expression.kind === "call") return expression.name.toLowerCase();
  if (expression.kind === "wildcard") return "*";
  return "expression";
}

function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  let index = 0;
  while (index < sql.length) {
    const character = sql[index] ?? "";
    if (/\s/.test(character)) {
      index += 1;
      continue;
    }
    if (/[A-Za-z_]/.test(character)) {
      const start = index++;
      while (index < sql.length && /[A-Za-z0-9_]/.test(sql[index] ?? "")) index += 1;
      tokens.push({ kind: "identifier", text: sql.slice(start, index) });
      continue;
    }
    if (/\d/.test(character)) {
      const start = index++;
      while (index < sql.length && /[\d.]/.test(sql[index] ?? "")) index += 1;
      const text = sql.slice(start, index);
      if (!/^\d+(?:\.\d+)?$/.test(text)) throw new TypeError(`Invalid number: ${text}`);
      tokens.push({ kind: "number", text });
      continue;
    }
    if (character === "'") {
      index += 1;
      let value = "";
      let closed = false;
      while (index < sql.length) {
        if (sql[index] === "'" && sql[index + 1] === "'") {
          value += "'";
          index += 2;
        } else if (sql[index] === "'") {
          index += 1;
          closed = true;
          break;
        } else value += sql[index++] ?? "";
      }
      if (!closed) throw new TypeError("Unterminated string literal");
      tokens.push({ kind: "string", text: value });
      continue;
    }
    const pair = sql.slice(index, index + 2);
    if (pair === "--" || pair === "/*") {
      throw new TypeError("SQL comments are not supported");
    }
    if (character === ";") throw new TypeError("Run one SELECT statement at a time");
    if ([">=", "<=", "!=", "<>"].includes(pair)) {
      tokens.push({ kind: "operator", text: pair });
      index += 2;
      continue;
    }
    if (["+", "-", "*", "/", "=", ">", "<"].includes(character))
      tokens.push({ kind: "operator", text: character });
    else if (["(", ")", ",", "."].includes(character))
      tokens.push({ kind: "punctuation", text: character });
    else throw new TypeError(`Unsupported SQL character: ${character}`);
    index += 1;
  }
  tokens.push({ kind: "eof", text: "" });
  return tokens;
}
