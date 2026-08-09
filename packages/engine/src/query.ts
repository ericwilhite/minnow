import type { DatabaseRow } from "./database.js";
import { QueryMemoryContext, type QueryMemoryUsage } from "./memory.js";
import {
  columnarTableFromRows,
  prepareVectorQuery,
  type ColumnarTable,
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
  /** Current and high-water byte counts for the documented modeled query-memory scope. */
  readonly memoryUsage: QueryMemoryUsage;
  execute(): QueryResult;
  close(): void;
}

export interface QueryExecutionOptions {
  /**
   * Bounds the modeled vector payload and row-index buffers. This is not a total JavaScript heap
   * limit; input preparation, object/hash overhead, group/order state, and result rows are excluded.
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
  | { kind: "call"; name: AggregateName | "ROUND"; arguments: Expression[] };

export interface SelectItem {
  expression: Expression;
  alias: string;
}

interface TableSource {
  table: string;
  alias: string;
}

interface JoinPlan extends TableSource {
  kind: "inner" | "left";
  left: Expression;
  right: Expression;
}

interface Predicate {
  left: Expression;
  operator: ComparisonOperator;
  right: Expression;
}

export interface CompiledQuery {
  sql: string;
  base: TableSource;
  joins: JoinPlan[];
  select: SelectItem[];
  predicates: Predicate[];
  groupBy: Expression[];
  orderBy: Array<{ expression: Expression; direction: "asc" | "desc" }>;
  limit?: number;
}

type RowContext = Record<string, DatabaseRow | undefined>;

interface Token {
  kind: "identifier" | "number" | "string" | "operator" | "punctuation" | "eof";
  text: string;
}

const clauseKeywords = new Set(["WHERE", "GROUP", "ORDER", "LIMIT", "JOIN", "INNER", "LEFT"]);
const aggregateNames = new Set<AggregateName>(["COUNT", "SUM", "AVG", "MIN", "MAX"]);

export function compileQuery(sql: string): CompiledQuery {
  const normalized = sql.trim().replace(/;$/, "").trim();
  if (normalized.length === 0) throw new TypeError("Enter a SELECT query");
  if (normalized.includes(";")) throw new TypeError("Run one SELECT statement at a time");
  if (/--|\/\*/.test(normalized)) throw new TypeError("SQL comments are not supported");
  const parser = new Parser(tokenize(normalized));
  return parser.parse(normalized);
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
  const memory = new QueryMemoryContext(options.executionMemoryBudgetBytes);
  if ([...tables.values()].some((rows) => rows.length === 0)) {
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
    rows = [...groups.values()].map((group) => project(plan.select, group[0] ?? {}, group));
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
    if (key === null || key === undefined) continue;
    const matches = index.get(key) ?? [];
    matches.push(row);
    index.set(key, matches);
  }
  const joined: RowContext[] = [];
  for (const context of contexts) {
    const leftKey = comparable(evaluate(leftExpression, context));
    const matches = leftKey === null || leftKey === undefined ? [] : (index.get(leftKey) ?? []);
    if (matches.length === 0 && join.kind === "left")
      joined.push({ ...context, [join.alias]: undefined });
    else for (const row of matches) joined.push({ ...context, [join.alias]: row });
  }
  return joined;
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
  const leftValue = evaluate(predicate.left, context);
  const rightValue = evaluate(predicate.right, context);
  if (
    leftValue === null ||
    leftValue === undefined ||
    rightValue === null ||
    rightValue === undefined
  )
    return false;
  const left = comparable(leftValue);
  const right = comparable(rightValue);
  if (predicate.operator === "=") return left === right;
  if (predicate.operator === "!=" || predicate.operator === "<>") return left !== right;
  const comparison = compareValues(left, right);
  if (predicate.operator === ">") return comparison > 0;
  if (predicate.operator === ">=") return comparison >= 0;
  if (predicate.operator === "<") return comparison < 0;
  return comparison <= 0;
}

function resolveColumn(context: RowContext, reference: string): unknown {
  const parts = reference.split(".");
  if (parts.length === 2) return context[parts[0] ?? ""]?.[parts[1] ?? ""];
  const name = parts[0] ?? "";
  const matches = Object.values(context).filter((row) => row !== undefined && name in row);
  if (matches.length !== 1) throw new TypeError(`Ambiguous or missing column: ${reference}`);
  return matches[0]?.[name];
}

function hasAggregate(expression: Expression): boolean {
  return expression.kind === "call" && aggregateNames.has(expression.name as AggregateName)
    ? true
    : expression.kind === "call"
      ? expression.arguments.some(hasAggregate)
      : expression.kind === "binary"
        ? hasAggregate(expression.left) || hasAggregate(expression.right)
        : false;
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
  if (typeof a === "number" && typeof b === "number") return a - b;
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

  constructor(private readonly tokens: Token[]) {}

  parse(sql: string): CompiledQuery {
    this.#keyword("SELECT");
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
        const left = this.#expression();
        const operator = this.#comparison();
        const right = this.#expression();
        predicates.push({ left, operator, right });
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
    let limit: number | undefined;
    if (this.#isKeyword("LIMIT")) {
      this.#keyword("LIMIT");
      limit = Number(this.#take("number").text);
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100_000)
        throw new RangeError("LIMIT must be between 1 and 100,000");
    }
    this.#take("eof");
    return {
      sql,
      base,
      joins,
      select,
      predicates,
      groupBy,
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
    const table = this.#identifier();
    let alias = table;
    if (this.#isKeyword("AS")) {
      this.#keyword("AS");
      alias = this.#identifier();
    } else if (
      this.#peek().kind === "identifier" &&
      !clauseKeywords.has(this.#peek().text.toUpperCase()) &&
      !this.#isKeyword("ON")
    ) {
      alias = this.#identifier();
    }
    return { table, alias };
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
