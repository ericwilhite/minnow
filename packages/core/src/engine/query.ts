import type { DatabaseRow } from "./database.js";
import { SqlCompileError } from "./errors.js";
import {
  cachedQueryTerms,
  ftsBm25Row,
  FtsStatsAccumulator,
  ftsMatchTruth,
  renderDocumentValue,
  tokenize as ftsTokenize,
  validateFtsQuery,
  type FtsStats,
} from "./fts.js";
import { QueryMemoryContext, type QueryMemoryUsage } from "./memory.js";
import { optimizePlan } from "./optimizer.js";
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
   * MinnowDatabase retains catalog types and does not use that fallback.
   */
  readonly executionMemoryBudgetBytes?: number;
}

export type BinaryOperator = "+" | "-" | "*" | "/";
export type ComparisonOperator = "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=";
export type AggregateName = "COUNT" | "SUM" | "AVG" | "MIN" | "MAX";
export type ScalarFunctionName = "ROUND" | "COALESCE" | "DATE_TRUNC";

export const scalarFunctionNames: ReadonlySet<string> = new Set([
  "ROUND",
  "COALESCE",
  "DATE_TRUNC",
] satisfies ScalarFunctionName[]);

export function isScalarFunctionName(
  name: AggregateName | ScalarFunctionName,
): name is ScalarFunctionName {
  return scalarFunctionNames.has(name);
}

export const dateTruncUnits: ReadonlySet<string> = new Set([
  "year",
  "quarter",
  "month",
  "week",
  "day",
  "hour",
  "minute",
  "second",
]);

/**
 * Truncates a datetime to the start of the given unit, in UTC — the engine stores and
 * compares datetimes by instant and has no session time zone. Weeks start on Monday,
 * matching Postgres.
 */
export function dateTruncValue(unit: unknown, value: unknown): Date | null {
  if (typeof unit !== "string" || !dateTruncUnits.has(unit.toLowerCase())) {
    throw new TypeError(
      "DATE_TRUNC requires a unit of year, quarter, month, week, day, hour, minute, or second",
    );
  }
  if (value === null || value === undefined) return null;
  if (!(value instanceof Date)) throw new TypeError("DATE_TRUNC requires a datetime value");
  const normalized = unit.toLowerCase();
  const year = value.getUTCFullYear();
  const month = value.getUTCMonth();
  const day = value.getUTCDate();
  switch (normalized) {
    case "year":
      return new Date(Date.UTC(year, 0, 1));
    case "quarter":
      return new Date(Date.UTC(year, Math.floor(month / 3) * 3, 1));
    case "month":
      return new Date(Date.UTC(year, month, 1));
    case "week": {
      const start = new Date(Date.UTC(year, month, day));
      start.setUTCDate(start.getUTCDate() - ((start.getUTCDay() + 6) % 7));
      return start;
    }
    case "day":
      return new Date(Date.UTC(year, month, day));
    case "hour":
      return new Date(Date.UTC(year, month, day, value.getUTCHours()));
    case "minute":
      return new Date(Date.UTC(year, month, day, value.getUTCHours(), value.getUTCMinutes()));
    default:
      return new Date(
        Date.UTC(
          year,
          month,
          day,
          value.getUTCHours(),
          value.getUTCMinutes(),
          value.getUTCSeconds(),
        ),
      );
  }
}

export type Expression =
  | { kind: "literal"; value: QueryValue }
  | { kind: "column"; reference: string }
  | { kind: "wildcard" }
  | { kind: "binary"; operator: BinaryOperator; left: Expression; right: Expression }
  | {
      kind: "call";
      name: AggregateName | ScalarFunctionName;
      arguments: Expression[];
      distinct?: boolean;
    }
  | { kind: "list"; items: Expression[] }
  | { kind: "subquery"; block: CompiledQuery }
  | { kind: "condition"; operator: PredicateOperator; left: Expression; right: Expression }
  | { kind: "logical"; operator: "and" | "or"; left: Expression; right: Expression }
  | { kind: "not"; operand: Expression }
  | { kind: "exists"; block: CompiledQuery; negated: boolean }
  | {
      kind: "case";
      branches: Array<{ when: Expression; then: Expression }>;
      otherwise?: Expression;
    }
  | {
      kind: "window";
      name: WindowFunctionName;
      partitionBy: Expression[];
      orderBy: Array<{ expression: Expression; direction: "asc" | "desc" }>;
      argument?: Expression;
    }
  | {
      kind: "fts";
      op: "match" | "bm25";
      /**
       * Column references forming the document, or "*" for every searchable column of the
       * single scan source. "*" stays unexpanded through compilation (both front ends emit the
       * identical node — plan parity), and the engine expands it against the catalog at
       * prepare time via expandFtsColumns.
       */
      columns: Expression[] | "*";
      query: string;
      /**
       * Corpus statistics for BM25, annotated by the executor onto its cloned plan before
       * evaluation; never set by compilation.
       */
      stats?: FtsStats;
    };

export type WindowFunctionName = "ROW_NUMBER" | "RANK" | "DENSE_RANK" | AggregateName;

export interface WindowSpec {
  alias: string;
  name: WindowFunctionName;
  partitionAliases: string[];
  orderAliases: Array<{ alias: string; direction: "asc" | "desc" }>;
  /** Hidden inner alias of an aggregate window's argument; absent for COUNT(*) and rankings. */
  argumentAlias?: string;
}

/** The output column type of one window: rankings and most aggregates count, MIN/MAX carry. */
export function windowOutputType(
  window: WindowSpec,
  innerSchema: readonly SqlColumnSchema[],
): SqlColumnType {
  if ((window.name === "MIN" || window.name === "MAX") && window.argumentAlias !== undefined) {
    return innerSchema.find(({ name }) => name === window.argumentAlias)?.type ?? "number";
  }
  return "number";
}

export interface SelectItem {
  expression: Expression;
  alias: string;
}

export interface TableSource {
  table: string;
  alias: string;
  /** A parenthesized SELECT or expanded CTE body; `table` is then a unique synthetic name. */
  derived?: CompiledQuery;
  /** A top-level set operation; members combine positionally under the first member's schema. */
  union?: { blocks: CompiledQuery[]; ops: SetOperator[] };
  recursive?: RecursiveCte;
  /** A window-function desugar: the inner block executes, then window columns append. */
  windowed?: { block: CompiledQuery; windows: WindowSpec[] };
}

export interface JoinPlan extends TableSource {
  kind: "inner" | "left";
  left: Expression;
  right: Expression;
  /** General ON condition for non-equi or multi-key joins; left/right are inert placeholders. */
  on?: Expression;
}

export type SetOperator = "union" | "union all" | "intersect" | "except";

/**
 * A WITH RECURSIVE source: the base block seeds the working set, then the step block re-executes
 * with `reference` bound to the previous iteration's new rows (linear delta recursion) until no
 * new rows appear. UNION deduplicates against everything seen; UNION ALL appends raw.
 */
export interface RecursiveCte {
  reference: string;
  base: CompiledQuery;
  step: CompiledQuery;
  all: boolean;
}

const MAX_RECURSIVE_ITERATIONS = 10_000;
const MAX_RECURSIVE_ROWS = 1_000_000;

export type PredicateOperator =
  | ComparisonOperator
  | "IN"
  | "NOT IN"
  | "IS NULL"
  | "IS NOT NULL"
  | "LIKE"
  | "NOT LIKE"
  | "IS TRUE";

export interface Predicate {
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
  offset?: number;
}

type RowContext = Record<string, DatabaseRow | undefined>;

interface Token {
  kind: "identifier" | "number" | "string" | "operator" | "punctuation" | "eof";
  text: string;
  /**
   * Half-open character span in the tokenized text. String tokens span their quotes even though
   * `text` holds the unescaped value, so a squiggle covers what the author actually typed.
   */
  start: number;
  end: number;
}

const clauseKeywords = new Set([
  "WHERE",
  "GROUP",
  "HAVING",
  "ORDER",
  "LIMIT",
  "OFFSET",
  "INTERSECT",
  "EXCEPT",
  "JOIN",
  "INNER",
  "LEFT",
  "UNION",
]);
const aggregateNames = new Set<AggregateName>(["COUNT", "SUM", "AVG", "MIN", "MAX"]);

export interface CompileQueryOptions {
  /** Set false to skip deterministic plan rewrites, for example to snapshot the raw plan. */
  readonly optimize?: boolean;
}

/**
 * Trims the statement the parser sees while remembering how far that shifted every position, so
 * compile errors report offsets into the caller's own text rather than the trimmed copy.
 */
function normalizeSql(sql: string): { text: string; offset: number } {
  return { text: sql.trim().replace(/;$/, "").trim(), offset: sql.length - sql.trimStart().length };
}

/**
 * Rethrows a compilation failure with a position. Errors that already carry one only shift into
 * the caller's coordinates; anything else takes the parser's current token as its anchor. Non-type
 * errors pass through untouched — they come from execution, not from reading the text.
 */
function throwLocated(error: unknown, offset: number, span: { start: number; end: number }): never {
  if (error instanceof SqlCompileError) {
    if (offset === 0) throw error;
    throw new SqlCompileError(error.message, error.offset + offset, error.length);
  }
  if (error instanceof TypeError) {
    throw new SqlCompileError(
      error.message,
      offset + span.start,
      Math.max(span.end - span.start, 0),
    );
  }
  throw error;
}

export function compileQuery(sql: string, options: CompileQueryOptions = {}): CompiledQuery {
  const { text, offset } = normalizeSql(sql);
  if (text.length === 0) throw new SqlCompileError("Enter a SELECT query", offset, 0);
  let parser: Parser | undefined;
  let plan: CompiledQuery;
  try {
    parser = new Parser(tokenize(text));
    plan = parser.parse(text);
  } catch (error) {
    throwLocated(error, offset, parser?.span ?? { start: 0, end: text.length });
  }
  return options.optimize === false ? plan : optimizePlan(plan);
}

export type CompiledStatement =
  | { kind: "select"; sql: string }
  | { kind: "insert"; table: string; columns: string[]; rows: QueryValue[][] }
  | {
      kind: "update";
      table: string;
      assignments: Array<{ column: string; expression: Expression }>;
      predicates: Array<{ left: Expression; operator: PredicateOperator; right: Expression }>;
    }
  | {
      kind: "delete";
      table: string;
      predicates: Array<{ left: Expression; operator: PredicateOperator; right: Expression }>;
    };

/**
 * Routes one SQL statement: SELECT and WITH compile through the read-only query pipeline, while
 * INSERT ... VALUES, UPDATE ... SET, and DELETE FROM parse into mutation statements. Any other
 * leading keyword fails explicitly.
 */
export function compileStatement(sql: string): CompiledStatement {
  const { text, offset } = normalizeSql(sql);
  if (text.length === 0) throw new SqlCompileError("Enter a SQL statement", offset, 0);
  let parser: Parser | undefined;
  try {
    const tokens = tokenize(text);
    const first = tokens[0];
    const keyword = first?.kind === "identifier" ? first.text.toUpperCase() : "";
    if (keyword === "INSERT" || keyword === "UPDATE" || keyword === "DELETE") {
      parser = new Parser(tokens);
      return parser.parseMutation(keyword);
    }
    compileQuery(text);
    return { kind: "select", sql: text };
  } catch (error) {
    throwLocated(error, offset, parser?.span ?? { start: 0, end: text.length });
  }
}

/** Evaluates an expression against one row, for UPDATE SET assignment computation. */
export function evaluateRowExpression(
  expression: Expression,
  alias: string,
  row: DatabaseRow,
): QueryValue {
  return asQueryValue(evaluate(expression, { [alias]: row }));
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
      return;
    }
    if (expression.kind === "logical") {
      rewrite(expression.left, (next) => (expression.left = next));
      rewrite(expression.right, (next) => (expression.right = next));
      return;
    }
    if (expression.kind === "not") {
      rewrite(expression.operand, (next) => (expression.operand = next));
      return;
    }
    if (expression.kind === "case") {
      for (const branch of expression.branches) {
        rewrite(branch.when, (next) => (branch.when = next));
        rewrite(branch.then, (next) => (branch.then = next));
      }
      const otherwise = expression.otherwise;
      if (otherwise !== undefined) rewrite(otherwise, (next) => (expression.otherwise = next));
      return;
    }
    if (expression.kind === "exists") {
      collectBlock(expression.block);
      const negated = expression.negated;
      steps.push({
        block: expression.block,
        substitute(result) {
          replace({
            kind: "literal",
            value: negated ? result.rows.length === 0 : result.rows.length > 0,
          });
        },
      });
      return;
    }
    if (expression.kind === "condition") {
      rewrite(expression.left, (next) => (expression.left = next));
      const right = expression.right;
      if (
        (expression.operator === "IN" || expression.operator === "NOT IN") &&
        right.kind === "subquery"
      ) {
        collectBlock(right.block);
        steps.push({
          block: right.block,
          substitute(result) {
            if (result.columns.length !== 1) {
              throw new TypeError("An IN subquery must select exactly one column");
            }
            expression.right = {
              kind: "list",
              items: result.rows.map((row) => ({
                kind: "literal",
                value: row[result.columns[0] ?? ""] ?? null,
              })),
            };
          },
        });
        return;
      }
      rewrite(expression.right, (next) => (expression.right = next));
    }
  };
  const collectBlock = (block: CompiledQuery): void => {
    for (const source of [block.base, ...block.joins]) {
      if (source.derived !== undefined) collectBlock(source.derived);
      source.union?.blocks.forEach(collectBlock);
      if (source.windowed !== undefined) collectBlock(source.windowed.block);
      if (source.recursive !== undefined) {
        collectBlock(source.recursive.base);
        collectBlock(source.recursive.step);
      }
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
    if (expression.kind === "subquery" || expression.kind === "exists") return true;
    return childExpressions(expression).some(expressionHas);
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
        source.union?.blocks.some(blockHas) === true ||
        (source.windowed !== undefined && blockHas(source.windowed.block)) ||
        (source.recursive !== undefined &&
          (blockHas(source.recursive.base) || blockHas(source.recursive.step))),
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
    if (expression.kind === "window") {
      throw new TypeError("Window functions must be desugared before schema inference");
    }
    if (
      expression.kind === "condition" ||
      expression.kind === "logical" ||
      expression.kind === "not" ||
      expression.kind === "exists"
    ) {
      return "boolean";
    }
    if (expression.kind === "case") {
      const outcomes = [
        ...expression.branches.map((branch) => branch.then),
        ...(expression.otherwise === undefined ? [] : [expression.otherwise]),
      ];
      let resolved: SqlColumnType | "null" = "null";
      for (const outcome of outcomes) {
        const type = infer(outcome);
        if (type === "null") continue;
        if (resolved !== "null" && resolved !== type) {
          throw new TypeError("CASE branches must produce one value type");
        }
        resolved = type;
      }
      return resolved;
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
    if (expression.kind === "fts") {
      if (expression.columns !== "*") {
        for (const columnExpression of expression.columns) {
          if (columnExpression.kind !== "column") {
            throw new TypeError("Full-text search takes column references");
          }
          if (infer(columnExpression) === "boolean") {
            throw new TypeError(
              `Full-text search cannot search a boolean column: ${columnExpression.reference}`,
            );
          }
        }
      }
      return expression.op === "match" ? "boolean" : "number";
    }
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
    if (expression.name === "DATE_TRUNC") return "datetime";
    if (expression.name === "COALESCE") {
      let resolved: SqlColumnType | "null" = "null";
      for (const argument of expression.arguments) {
        const type = infer(argument);
        if (type === "null") continue;
        if (resolved !== "null" && resolved !== type) {
          throw new TypeError("COALESCE arguments must produce one value type");
        }
        resolved = type;
      }
      return resolved;
    }
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
    ...plan.joins.flatMap((join) => [
      join.left,
      join.right,
      ...(join.on === undefined ? [] : [join.on]),
    ]),
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

/**
 * Derives a schema-less table's searchable document columns for MATCH(*) expansion: every
 * column that ever held a non-boolean value. Boolean-only and all-null columns contribute
 * nothing to a document either way, so excluding them preserves match semantics exactly.
 */
function rowTableSearchableColumns(
  tables: ReadonlyMap<string, DatabaseRow[]>,
): (tableName: string) => readonly string[] | undefined {
  return (tableName) => {
    const rows = tables.get(tableName);
    if (rows === undefined) return undefined;
    const searchable = new Set<string>();
    const booleans = new Set<string>();
    for (const row of rows) {
      for (const [name, value] of Object.entries(row)) {
        if (typeof value === "boolean") booleans.add(name);
        else if (value !== null) searchable.add(name);
      }
    }
    return [...searchable].filter((name) => !booleans.has(name));
  };
}

export function createPreparedQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
  options: QueryExecutionOptions = {},
): PreparedQuery {
  validateGrouping(plan);
  // Every engine entry expands MATCH(*) exactly once, here against the row tables' own
  // columns; past this point no executor sees the "*" sentinel.
  plan = expandFtsColumns(plan, rowTableSearchableColumns(tables));
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

/** Internal columnar entry point used after MinnowDatabase materializes a stable snapshot. */
export function createPreparedColumnarQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, ColumnarTable>,
  memory: QueryMemoryContext = new QueryMemoryContext(),
  options: { ftsStats?: ReadonlyMap<string, FtsStats> } = {},
): PreparedQuery {
  validateGrouping(plan);
  let closed = false;
  let prepared: PreparedVectorQuery | undefined;
  try {
    prepared = prepareVectorQuery(plan, tables, {
      memoryContext: memory,
      ...(options.ftsStats === undefined ? {} : { ftsStats: options.ftsStats }),
    });
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
  ops: readonly SetOperator[],
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
  const dedupe = (rows: readonly QueryRow[]): QueryRow[] => {
    const seen = new Set<string>();
    const deduplicated: QueryRow[] = [];
    for (const row of rows) {
      const key = encode(row);
      if (seen.has(key)) continue;
      seen.add(key);
      deduplicated.push(row);
    }
    return deduplicated;
  };
  let combined = renamed[0] ?? [];
  for (let index = 1; index < renamed.length; index += 1) {
    const next = renamed[index] ?? [];
    const op = ops[index - 1] ?? "union";
    if (op === "union all") {
      combined = [...combined, ...next];
      continue;
    }
    if (op === "union") {
      combined = dedupe([...combined, ...next]);
      continue;
    }
    // INTERSECT and EXCEPT return distinct left-side rows filtered by right-side membership.
    const membership = new Set(next.map(encode));
    combined = dedupe(combined).filter((row) =>
      op === "intersect" ? membership.has(encode(row)) : !membership.has(encode(row)),
    );
  }
  return { columns: [...columns], rows: combined };
}

function encodeRowKey(columns: readonly string[]): (row: QueryRow) => string {
  return (row) =>
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
}

interface RecursiveCteState {
  readonly columns: readonly string[];
  readonly rows: QueryRow[];
  frontier: QueryRow[];
  iterations: number;
  absorb(step: QueryResult): void;
}

/**
 * Drives one recursive CTE to its fixpoint: absorb() renames a step result positionally onto the
 * base columns, appends what is new (all rows for UNION ALL, unseen rows for UNION), and refreshes
 * the frontier. Hard caps bound runaway recursions explicitly instead of exhausting memory.
 */
export function createRecursiveCteState(base: QueryResult, all: boolean): RecursiveCteState {
  const columns = base.columns;
  const encode = encodeRowKey(columns);
  const seen = new Set<string>();
  const admit = (candidates: readonly QueryRow[]): QueryRow[] => {
    if (all) return [...candidates];
    const fresh: QueryRow[] = [];
    for (const row of candidates) {
      const key = encode(row);
      if (seen.has(key)) continue;
      seen.add(key);
      fresh.push(row);
    }
    return fresh;
  };
  const state: RecursiveCteState = {
    columns,
    rows: [],
    frontier: [],
    iterations: 0,
    absorb(step: QueryResult): void {
      state.iterations += 1;
      if (state.iterations > MAX_RECURSIVE_ITERATIONS) {
        throw new RangeError(
          `Recursive CTE exceeded ${String(MAX_RECURSIVE_ITERATIONS)} iterations`,
        );
      }
      if (step.columns.length !== columns.length) {
        throw new TypeError("Recursive CTE members must select the same number of columns");
      }
      const renamed = step.rows.map((row) =>
        Object.fromEntries(
          columns.map((name, column) => [name, row[step.columns[column] ?? ""] ?? null]),
        ),
      );
      state.frontier = admit(renamed);
      state.rows.push(...state.frontier);
      if (state.rows.length > MAX_RECURSIVE_ROWS) {
        throw new RangeError(`Recursive CTE exceeded ${String(MAX_RECURSIVE_ROWS)} rows`);
      }
    },
  };
  const seeded = admit(base.rows);
  state.rows.push(...seeded);
  state.frontier = seeded;
  return state;
}

/** The direct child expressions of a node; subqueries and EXISTS scope their own blocks. */
export function childExpressions(expression: Expression): Expression[] {
  if (expression.kind === "binary") return [expression.left, expression.right];
  if (expression.kind === "call") return [...expression.arguments];
  if (expression.kind === "list") return [...expression.items];
  if (expression.kind === "condition" || expression.kind === "logical") {
    return [expression.left, expression.right];
  }
  if (expression.kind === "not") return [expression.operand];
  if (expression.kind === "case") {
    return [
      ...expression.branches.flatMap((branch) => [branch.when, branch.then]),
      ...(expression.otherwise === undefined ? [] : [expression.otherwise]),
    ];
  }
  if (expression.kind === "fts") return expression.columns === "*" ? [] : [...expression.columns];
  return [];
}

/**
 * Visits every expression position of one block — select, predicates, HAVING, GROUP BY,
 * ORDER BY, and join conditions — without descending into nested blocks. Every plan feature
 * that scans "all expressions of a block" goes through here, so a future clause is added to
 * one list instead of one per feature.
 */
export function forEachBlockExpression(
  block: CompiledQuery,
  visit: (expression: Expression) => void,
): void {
  for (const item of block.select) visit(item.expression);
  for (const predicate of [...block.predicates, ...block.having]) {
    visit(predicate.left);
    visit(predicate.right);
  }
  block.groupBy.forEach(visit);
  for (const order of block.orderBy) visit(order.expression);
  for (const join of block.joins) {
    visit(join.left);
    visit(join.right);
    if (join.on !== undefined) visit(join.on);
  }
}

/** Visits every nested block of one block's sources (derived, union, windowed, recursive). */
export function forEachNestedBlock(
  block: CompiledQuery,
  visit: (nested: CompiledQuery) => void,
): void {
  for (const source of [block.base, ...block.joins]) {
    if (source.derived !== undefined) visit(source.derived);
    if (source.union !== undefined) source.union.blocks.forEach(visit);
    if (source.windowed !== undefined) visit(source.windowed.block);
    if (source.recursive !== undefined) {
      visit(source.recursive.base);
      visit(source.recursive.step);
    }
  }
}

/** One top-level full-text MATCH conjunct of a plan, with its resolved column expressions. */
export interface FtsMatchConjunct {
  columns: Expression[];
  query: string;
}

/**
 * The plan's top-level `MATCH ... AGAINST` conjuncts — the ones index pruning may exploit,
 * because every result row must satisfy them. This is the sole authority on how a bare MATCH
 * appears in `plan.predicates`: `splitCondition` wraps non-comparison booleans as
 * `IS TRUE` predicates, and a test pins that pairing so an optimizer or parser change that
 * rewraps the conjunct fails loudly instead of silently disabling pruning. Unexpanded `"*"`
 * documents and negated/OR-wrapped matches are deliberately excluded.
 */
export function topLevelFtsMatchConjuncts(plan: CompiledQuery): FtsMatchConjunct[] {
  return plan.predicates.flatMap((predicate) =>
    predicate.operator === "IS TRUE" &&
    predicate.left.kind === "fts" &&
    predicate.left.op === "match" &&
    predicate.left.columns !== "*"
      ? [{ columns: predicate.left.columns, query: predicate.left.query }]
      : [],
  );
}

/** True when any expression in the plan or its nested blocks is a full-text node. */
export function planContainsFts(plan: CompiledQuery, op?: "match" | "bm25"): boolean {
  let found = false;
  const expressionHasFts = (expression: Expression): void => {
    if (found) return;
    if (expression.kind === "fts") {
      found ||= op === undefined || expression.op === op;
      return;
    }
    if (expression.kind === "subquery" || expression.kind === "exists") {
      found ||= planContainsFts(expression.block, op);
      return;
    }
    childExpressions(expression).forEach(expressionHasFts);
  };
  forEachNestedBlock(plan, (nested) => {
    if (!found) found = planContainsFts(nested, op);
  });
  forEachBlockExpression(plan, expressionHasFts);
  return found;
}

/**
 * Expands every `MATCH(*)`/`BM25(*)` in the plan into the single scan source's searchable
 * columns (string, number, datetime — booleans are excluded from documents), and enforces the
 * v1 restriction that a full-text document draws from exactly one base table source. Runs at
 * prepare time when the catalog is known. Copy-on-write: plans without full-text nodes pass
 * through untouched, and full-text plans are cloned before rewriting — the input is often the
 * compile cache's own copy (subquery resolution returns the original plan when it has nothing
 * to resolve), and freezing an expansion into the cache would pin a stale column list across
 * later migrations.
 */
export function expandFtsColumns(
  plan: CompiledQuery,
  searchableColumnsFor: (tableName: string) => readonly string[] | undefined,
): CompiledQuery {
  if (!planContainsFts(plan)) return plan;
  plan = structuredClone(plan);
  const expand = (expression: Expression, block: CompiledQuery): void => {
    if (expression.kind === "subquery" || expression.kind === "exists") {
      expandBlock(expression.block);
      return;
    }
    if (expression.kind !== "fts") {
      for (const child of childExpressions(expression)) expand(child, block);
      return;
    }
    if (block.base.derived !== undefined || block.base.union !== undefined) {
      throw new TypeError("Full-text search requires a scanned base table");
    }
    if (expression.columns !== "*") return;
    if (block.joins.length > 0) {
      throw new TypeError(
        "Full-text search over every column requires a single-table query; name the columns explicitly",
      );
    }
    const names = searchableColumnsFor(block.base.table);
    if (names === undefined || names.length === 0) {
      throw new TypeError(`Full-text search found no searchable columns: ${block.base.table}`);
    }
    expression.columns = names.map((name) => ({ kind: "column", reference: name }));
  };
  const expandBlock = (block: CompiledQuery): void => {
    forEachNestedBlock(block, expandBlock);
    forEachBlockExpression(block, (expression) => {
      expand(expression, block);
    });
  };
  expandBlock(plan);
  return plan;
}

/**
 * Computes one aggregate window over partition-sorted row indexes. The frame is the SQL default:
 * without OVER ordering every partition row shares the whole-partition aggregate; with ordering
 * each row sees the running aggregate from the partition start through its ordering peers
 * (RANGE UNBOUNDED PRECEDING TO CURRENT ROW). State accumulates incrementally, so a partition
 * costs one pass regardless of peer-group count.
 */
function applyAggregateWindow(
  rows: QueryRow[],
  indexes: readonly number[],
  window: WindowSpec,
  samePartition: (left: number, right: number) => boolean,
  sameOrderKeys: (left: number, right: number) => boolean,
): void {
  let start = 0;
  while (start < indexes.length) {
    let end = start + 1;
    while (end < indexes.length && samePartition(indexes[start] ?? 0, indexes[end] ?? 0)) {
      end += 1;
    }
    let consumed = start;
    let rowsInFrame = 0;
    let nonNullCount = 0;
    let sum = 0;
    let best: unknown;
    const advance = (to: number): void => {
      for (; consumed < to; consumed += 1) {
        rowsInFrame += 1;
        if (window.argumentAlias === undefined) continue;
        const value = rows[indexes[consumed] ?? -1]?.[window.argumentAlias] ?? null;
        if (value === null) continue;
        nonNullCount += 1;
        if (window.name === "SUM" || window.name === "AVG") sum += numeric(value);
        else if (window.name === "MIN") {
          if (best === undefined || compareValues(value, best) < 0) best = value;
        } else if (window.name === "MAX") {
          if (best === undefined || compareValues(value, best) > 0) best = value;
        }
      }
    };
    const current = (): unknown => {
      if (window.name === "COUNT") {
        return window.argumentAlias === undefined ? rowsInFrame : nonNullCount;
      }
      if (nonNullCount === 0) return null;
      if (window.name === "SUM") return sum;
      if (window.name === "AVG") return sum / nonNullCount;
      return best ?? null;
    };
    const assign = (from: number, to: number): void => {
      const value = asQueryValue(current());
      for (let position = from; position < to; position += 1) {
        const row = rows[indexes[position] ?? -1];
        if (row !== undefined) row[window.alias] = value;
      }
    };
    if (window.orderAliases.length === 0) {
      advance(end);
      assign(start, end);
    } else {
      let groupStart = start;
      while (groupStart < end) {
        let groupEnd = groupStart + 1;
        while (groupEnd < end && sameOrderKeys(indexes[groupStart] ?? 0, indexes[groupEnd] ?? 0)) {
          groupEnd += 1;
        }
        advance(groupEnd);
        assign(groupStart, groupEnd);
        groupStart = groupEnd;
      }
    }
    start = end;
  }
}

function containsDistinctCount(expression: Expression): boolean {
  if (expression.kind === "call" && expression.distinct === true) return true;
  return childExpressions(expression).some(containsDistinctCount);
}

function containsWindow(expression: Expression): boolean {
  if (expression.kind === "window") return true;
  return childExpressions(expression).some(containsWindow);
}

function containsFtsExpression(expression: Expression): boolean {
  if (expression.kind === "fts") return true;
  return childExpressions(expression).some(containsFtsExpression);
}

/**
 * Appends window-function columns to an executed inner-block result. Rows sort stably by the
 * hidden partition and ordering aliases with the same comparison semantics as ORDER BY;
 * ROW_NUMBER numbers rows per partition, RANK shares ranks across ordering ties with gaps, and
 * DENSE_RANK shares without gaps. Without OVER ordering every partition row is a peer.
 */
export function applyWindowFunctions(
  result: QueryResult,
  windows: readonly WindowSpec[],
): QueryResult {
  const rows = result.rows.map((row) => ({ ...row }));
  for (const window of windows) {
    const indexes = rows.map((_, index) => index);
    const compare = (left: number, right: number): number => {
      const leftRow = rows[left];
      const rightRow = rows[right];
      if (leftRow === undefined || rightRow === undefined) return 0;
      for (const alias of window.partitionAliases) {
        const comparison = compareValues(
          comparable(leftRow[alias] ?? null),
          comparable(rightRow[alias] ?? null),
        );
        if (comparison !== 0) return comparison;
      }
      for (const { alias, direction } of window.orderAliases) {
        const comparison = compareValues(
          comparable(leftRow[alias] ?? null),
          comparable(rightRow[alias] ?? null),
        );
        if (comparison !== 0) return direction === "desc" ? -comparison : comparison;
      }
      return left - right;
    };
    indexes.sort(compare);
    const samePartition = (left: number, right: number): boolean =>
      window.partitionAliases.every(
        (alias) =>
          compareValues(
            comparable(rows[left]?.[alias] ?? null),
            comparable(rows[right]?.[alias] ?? null),
          ) === 0,
      );
    const sameOrderKeys = (left: number, right: number): boolean =>
      window.orderAliases.every(
        ({ alias }) =>
          compareValues(
            comparable(rows[left]?.[alias] ?? null),
            comparable(rows[right]?.[alias] ?? null),
          ) === 0,
      );
    if (window.name !== "ROW_NUMBER" && window.name !== "RANK" && window.name !== "DENSE_RANK") {
      applyAggregateWindow(rows, indexes, window, samePartition, sameOrderKeys);
      continue;
    }
    let rowNumber = 0;
    let rank = 0;
    let denseRank = 0;
    for (const [position, index] of indexes.entries()) {
      const previous = position > 0 ? indexes[position - 1] : undefined;
      if (previous === undefined || !samePartition(previous, index)) {
        rowNumber = 1;
        rank = 1;
        denseRank = 1;
      } else {
        rowNumber += 1;
        if (!sameOrderKeys(previous, index)) {
          rank = rowNumber;
          denseRank += 1;
        }
      }
      const row = rows[index];
      if (row === undefined) continue;
      row[window.alias] =
        window.name === "ROW_NUMBER" ? rowNumber : window.name === "RANK" ? rank : denseRank;
    }
  }
  return {
    columns: [...result.columns, ...windows.map((window) => window.alias)],
    rows,
  };
}

/** Executes each derived or set-operation source with the row reference. */
function resolveDerivedRowTables(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
): ReadonlyMap<string, DatabaseRow[]> {
  const sources = [plan.base, ...plan.joins];
  if (
    !sources.some(
      (source) =>
        source.derived !== undefined ||
        source.union !== undefined ||
        source.windowed !== undefined ||
        source.recursive !== undefined,
    )
  ) {
    return tables;
  }
  const resolved = new Map(tables);
  for (const source of sources) {
    if (source.recursive !== undefined) {
      const { reference, base, step, all } = source.recursive;
      const state = createRecursiveCteState(executeRowQuery(base, tables), all);
      while (state.frontier.length > 0) {
        const stepTables = new Map(tables);
        stepTables.set(reference, state.frontier);
        state.absorb(executeRowQuery(step, stepTables));
      }
      resolved.set(source.table, state.rows);
      continue;
    }
    if (source.union !== undefined) {
      const results = source.union.blocks.map((block) => executeRowQuery(block, tables));
      resolved.set(source.table, combineUnionResults(results, source.union.ops).rows);
      continue;
    }
    if (source.windowed !== undefined) {
      const inner = executeRowQuery(source.windowed.block, tables);
      resolved.set(source.table, applyWindowFunctions(inner, source.windowed.windows).rows);
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
  validateGrouping(plan);
  plan = expandFtsColumns(plan, rowTableSearchableColumns(tables));
  const resolution = subqueryResolutionSteps(plan);
  for (const step of resolution.steps) step.substitute(executeRowQuery(step.block, tables));
  // Stats annotation writes into the plan; when resolution had nothing to clone, the plan is
  // still the caller's (possibly cached) object, and frozen statistics would survive into later
  // executions against different rows.
  plan =
    resolution.plan === plan && planContainsFts(plan, "bm25")
      ? structuredClone(plan)
      : resolution.plan;
  tables = resolveDerivedRowTables(plan, tables);
  annotateRowFtsStats(plan, tables);
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
        plan.having.every(
          (predicate) =>
            evaluateBooleanExpression(
              {
                kind: "condition",
                operator: predicate.operator,
                left: predicate.left,
                right: predicate.right,
              },
              (nested) => evaluate(nested, group[0] ?? {}, group),
            ) === true,
        ),
      )
      .map((group) => project(plan.select, group[0] ?? {}, group));
  } else {
    rows = contexts.map((context) => project(plan.select, context));
  }
  if (plan.orderBy.length > 0) {
    // Only a wildcard select needs the source shapes: every other select resolves against its
    // own output aliases.
    const orderSources =
      plan.select[0]?.expression.kind === "wildcard"
        ? [plan.base, ...plan.joins].map((source) => ({
            alias: source.alias,
            columns: rowTableColumnNames(tables.get(source.table) ?? []),
          }))
        : [];
    const sortColumns = plan.orderBy.map(({ expression, direction }) => ({
      outputName: orderOutputName(expression, plan.select, orderSources),
      multiplier: direction === "desc" ? -1 : 1,
    }));
    rows.sort((left, right) => {
      for (const { outputName, multiplier } of sortColumns) {
        const comparison = compareValues(left[outputName], right[outputName]);
        if (comparison !== 0) return comparison * multiplier;
      }
      return 0;
    });
  }
  if (plan.limit !== undefined || plan.offset !== undefined) {
    const start = plan.offset ?? 0;
    rows = rows.slice(start, plan.limit === undefined ? undefined : start + plan.limit);
  }
  const columns =
    plan.select[0]?.expression.kind === "wildcard"
      ? Object.keys(rows[0] ?? {})
      : plan.select.map((item) => item.alias);
  return { columns, rows };
}

/** One ORDER BY resolution source: an alias and the columns a wildcard select exposes from it. */
export interface OrderSourceShape {
  readonly alias: string;
  readonly columns: readonly string[];
}

/**
 * Resolves one ORDER BY reference to the output column that carries its values, throwing when
 * nothing matches. Dropping an unresolved sort key silently would return rows in an arbitrary
 * order with no sign of the problem, which paging built on ORDER BY turns into skipped and
 * repeated rows.
 */
export function orderOutputName(
  expression: Expression,
  select: readonly SelectItem[],
  sources: readonly OrderSourceShape[],
): string {
  const resolved = resolveOrderOutputName(expression, select, sources);
  if (resolved !== undefined) return resolved;
  const target = expression.kind === "column" ? `: ${expression.reference}` : "";
  throw new TypeError(`ORDER BY requires a selected column or output alias${target}`);
}

function resolveOrderOutputName(
  expression: Expression,
  select: readonly SelectItem[],
  sources: readonly OrderSourceShape[],
): string | undefined {
  if (expression.kind !== "column") return undefined;
  // A wildcard select names its outputs after the source columns: bare from one source, and
  // `alias.column` from several. A reference is rewritten into that naming rather than looked up
  // verbatim, so `SELECT * FROM people ORDER BY people.name` orders by the `name` output.
  if (select[0]?.expression.kind === "wildcard") {
    return wildcardOrderOutputName(expression.reference, sources);
  }
  const selected = select.find(
    (item) =>
      (item.expression.kind === "column" && item.expression.reference === expression.reference) ||
      (!expression.reference.includes(".") && item.alias === expression.reference),
  );
  return selected?.alias;
}

function wildcardOrderOutputName(
  reference: string,
  sources: readonly OrderSourceShape[],
): string | undefined {
  const prefixed = sources.length > 1;
  const separator = reference.indexOf(".");
  if (separator === -1) {
    // One source keeps the column's own name; unknown names are rejected by column resolution
    // before execution, so no shape lookup is needed here.
    if (!prefixed) return reference;
    const matches = sources.filter((source) => source.columns.includes(reference));
    const match = matches.length === 1 ? matches[0] : undefined;
    return match === undefined ? undefined : `${match.alias}.${reference}`;
  }
  const alias = reference.slice(0, separator);
  const column = reference.slice(separator + 1);
  const source = sources.find((candidate) => candidate.alias === alias);
  if (source === undefined) return undefined;
  // A source with no known columns is one whose shape the caller could not supply (a row table
  // with no rows), so only a column known to be absent is rejected.
  if (source.columns.length > 0 && !source.columns.includes(column)) return undefined;
  return prefixed ? `${alias}.${column}` : column;
}

/** Column names a row table exposes, as the union of its rows' keys. */
function rowTableColumnNames(rows: readonly DatabaseRow[]): string[] {
  const names = new Set<string>();
  for (const row of rows) for (const name of Object.keys(row)) names.add(name);
  return [...names];
}

function executeJoin(contexts: RowContext[], join: JoinPlan, rows: DatabaseRow[]): RowContext[] {
  if (join.on !== undefined) {
    const condition = join.on;
    const joined: RowContext[] = [];
    for (const context of contexts) {
      let matched = false;
      for (const row of rows) {
        const candidate = { ...context, [join.alias]: row };
        if (
          evaluateBooleanExpression(condition, (nested) => evaluate(nested, candidate)) === true
        ) {
          matched = true;
          joined.push(candidate);
        }
      }
      if (!matched && join.kind === "left") joined.push({ ...context, [join.alias]: undefined });
    }
    return joined;
  }
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
    case "window":
      throw new TypeError("Window functions are only allowed in the select list");
    case "column":
      return resolveColumn(context, expression.reference);
    case "condition":
    case "logical":
    case "not":
      return evaluateBooleanExpression(expression, (nested) => evaluate(nested, context, group));
    case "exists":
      throw new TypeError("EXISTS subqueries must be resolved before evaluation");
    case "fts": {
      if (expression.columns === "*") {
        throw new TypeError("Full-text search columns must be expanded before evaluation");
      }
      const values = expression.columns.map((column) => evaluate(column, context, group));
      if (expression.op === "match") {
        return ftsMatchTruth(cachedQueryTerms(expression.query), values);
      }
      const stats = expression.stats;
      if (stats === undefined) {
        throw new TypeError("BM25 corpus statistics are missing from the plan");
      }
      return ftsBm25Row(cachedQueryTerms(expression.query), values, stats);
    }
    case "case": {
      for (const branch of expression.branches) {
        const matched = evaluateBooleanExpression(branch.when, (nested) =>
          evaluate(nested, context, group),
        );
        if (matched === true) return evaluate(branch.then, context, group);
      }
      return expression.otherwise === undefined
        ? null
        : evaluate(expression.otherwise, context, group);
    }
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
      if (expression.name === "COALESCE") {
        for (const argument of expression.arguments) {
          const candidate = evaluate(argument, context, group);
          if (candidate !== null && candidate !== undefined) return candidate;
        }
        return null;
      }
      if (expression.name === "DATE_TRUNC") {
        return dateTruncValue(
          evaluate(expression.arguments[0] ?? { kind: "literal", value: null }, context, group),
          evaluate(expression.arguments[1] ?? { kind: "literal", value: null }, context, group),
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
  if (predicate.operator === "IS TRUE") {
    return (
      evaluateBooleanExpression(predicate.left, (nested) => evaluate(nested, context)) === true
    );
  }
  if (predicate.operator === "LIKE" || predicate.operator === "NOT LIKE") {
    return (
      evaluateBooleanExpression(
        {
          kind: "condition",
          operator: predicate.operator,
          left: predicate.left,
          right: predicate.right,
        },
        (nested) => evaluate(nested, context),
      ) === true
    );
  }
  if (predicate.operator === "IN" || predicate.operator === "NOT IN") {
    if (predicate.right.kind !== "list") throw new TypeError("IN requires a value list");
    const membership = cachedListMembership(predicate.right, predicate.right.items);
    if (membership !== null) {
      const value = evaluate(predicate.left, context);
      if (value === null || value === undefined) return false;
      if (membership.set.has(comparable(value))) return predicate.operator === "IN";
      return predicate.operator === "NOT IN" && !membership.hasNull;
    }
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

export interface ListMembership {
  set: ReadonlySet<unknown>;
  hasNull: boolean;
}

const listMembershipCache = new WeakMap<object, ListMembership | null>();

/**
 * IN lists produced by subquery materialization can hold many thousands of literals, and
 * probing them per row makes membership quadratic. A list whose items are all literals
 * gets one hash set of comparable values (plus a null flag), cached weakly on the list
 * node so repeated executions of a prepared plan reuse it. Lists with non-literal items
 * return null and take the general per-row path.
 */
export function cachedListMembership(
  node: object,
  items: ReadonlyArray<{ kind: string }>,
): ListMembership | null {
  let cached = listMembershipCache.get(node);
  if (cached === undefined) {
    if (items.every((item) => item.kind === "literal")) {
      const set = new Set<unknown>();
      let hasNull = false;
      for (const item of items as ReadonlyArray<{ kind: "literal"; value: unknown }>) {
        if (item.value === null || item.value === undefined) hasNull = true;
        else set.add(comparable(item.value));
      }
      cached = { set, hasNull };
    } else {
      cached = null;
    }
    listMembershipCache.set(node, cached);
  }
  return cached;
}

const likeCache = new Map<string, RegExp>();

/** Compiles a LIKE pattern (% = any run, _ = any character) to an anchored RegExp, cached. */
export function likeRegExp(pattern: string): RegExp {
  const cached = likeCache.get(pattern);
  if (cached !== undefined) return cached;
  let source = "^";
  for (const character of pattern) {
    if (character === "%") source += "[\\s\\S]*";
    else if (character === "_") source += "[\\s\\S]";
    else source += character.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }
  source += "$";
  const regExp = new RegExp(source);
  if (likeCache.size >= 128) likeCache.clear();
  likeCache.set(pattern, regExp);
  return regExp;
}

/**
 * Evaluates a boolean expression tree with SQL three-valued logic: comparisons over NULL are
 * unknown (null), AND/OR/NOT propagate unknown, and only the caller collapses unknown to false.
 * Leaf values come from the executor-specific callback so both executors share these semantics.
 */
export function evaluateBooleanExpression(
  expression: Expression,
  evaluateValue: (expression: Expression) => unknown,
): boolean | null {
  if (expression.kind === "logical") {
    const left = evaluateBooleanExpression(expression.left, evaluateValue);
    if (expression.operator === "and") {
      if (left === false) return false;
      const right = evaluateBooleanExpression(expression.right, evaluateValue);
      if (right === false) return false;
      return left === null || right === null ? null : true;
    }
    if (left === true) return true;
    const right = evaluateBooleanExpression(expression.right, evaluateValue);
    if (right === true) return true;
    return left === null || right === null ? null : false;
  }
  if (expression.kind === "not") {
    const value = evaluateBooleanExpression(expression.operand, evaluateValue);
    return value === null ? null : !value;
  }
  if (expression.kind === "exists") {
    throw new TypeError("EXISTS subqueries must be resolved before evaluation");
  }
  if (expression.kind === "condition") {
    const operator = expression.operator;
    if (operator === "IS TRUE") return evaluateBooleanExpression(expression.left, evaluateValue);
    if (operator === "IS NULL" || operator === "IS NOT NULL") {
      const value = evaluateValue(expression.left);
      const isNull = value === null || value === undefined;
      return operator === "IS NULL" ? isNull : !isNull;
    }
    if (operator === "IN" || operator === "NOT IN") {
      if (expression.right.kind !== "list") throw new TypeError("IN requires a value list");
      const probe = evaluateValue(expression.left);
      if (probe === null || probe === undefined) return null;
      let sawNull = false;
      for (const item of expression.right.items) {
        const value = evaluateValue(item);
        if (value === null || value === undefined) {
          sawNull = true;
          continue;
        }
        if (comparable(value) === comparable(probe)) return operator === "IN";
      }
      if (sawNull) return null;
      return operator === "NOT IN";
    }
    if (operator === "LIKE" || operator === "NOT LIKE") {
      const value = evaluateValue(expression.left);
      const pattern = evaluateValue(expression.right);
      if (value === null || value === undefined || pattern === null || pattern === undefined) {
        return null;
      }
      if (typeof value !== "string" || typeof pattern !== "string") {
        throw new TypeError("LIKE requires string operands");
      }
      const matched = likeRegExp(pattern).test(value);
      return operator === "LIKE" ? matched : !matched;
    }
    const left = evaluateValue(expression.left);
    const right = evaluateValue(expression.right);
    if (left === null || left === undefined || right === null || right === undefined) return null;
    const a = comparable(left);
    const b = comparable(right);
    if (operator === "=") return a === b;
    if (operator === "!=" || operator === "<>") return a !== b;
    const comparison = compareValues(a, b);
    if (operator === ">") return comparison > 0;
    if (operator === ">=") return comparison >= 0;
    if (operator === "<") return comparison < 0;
    return comparison <= 0;
  }
  const value = evaluateValue(expression);
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean") return value;
  throw new TypeError("Boolean conditions require boolean operands");
}

function comparisonHolds(
  operator: PredicateOperator,
  leftValue: unknown,
  rightValue: unknown,
): boolean {
  if (operator === "IS NULL") return leftValue === null || leftValue === undefined;
  if (operator === "IS NOT NULL") return leftValue !== null && leftValue !== undefined;
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

/**
 * Computes BM25 corpus statistics for every scoring node in the (already cloned) plan by one
 * pass over the base table's rows, and annotates them onto the nodes before evaluation.
 */
function annotateRowFtsStats(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
): void {
  const annotate = (expression: Expression): void => {
    if (expression.kind !== "fts") {
      childExpressions(expression).forEach(annotate);
      return;
    }
    if (expression.op !== "bm25" || expression.stats !== undefined) return;
    if (plan.joins.length > 0) {
      throw new TypeError("BM25 requires a single-table query");
    }
    if (expression.columns === "*") {
      throw new TypeError("Full-text search columns must be expanded before evaluation");
    }
    const terms = cachedQueryTerms(expression.query);
    const accumulator = new FtsStatsAccumulator(terms);
    const columnNames = expression.columns.map((column) => {
      if (column.kind !== "column") {
        throw new TypeError("Full-text search takes column references");
      }
      return column.reference.split(".").at(-1) ?? column.reference;
    });
    for (const row of tables.get(plan.base.table) ?? []) {
      const values = columnNames.map((name) => row[name]);
      const tokens: string[] = [];
      for (const value of values) {
        const rendered = renderDocumentValue(value);
        if (rendered === undefined) continue;
        tokens.push(...ftsTokenize(rendered));
      }
      accumulator.addDocument(tokens);
    }
    expression.stats = accumulator.stats;
  };
  forEachBlockExpression(plan, annotate);
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

export function hasAggregate(expression: Expression): boolean {
  if (expression.kind === "call" && aggregateNames.has(expression.name as AggregateName)) {
    return true;
  }
  return childExpressions(expression).some(hasAggregate);
}

function validateGrouping(plan: CompiledQuery): void {
  const grouped =
    plan.groupBy.length > 0 || plan.select.some((item) => hasAggregate(item.expression));
  if (!grouped) return;
  const groupExpressions = new Set(plan.groupBy.map((expression) => JSON.stringify(expression)));
  for (const item of plan.select) {
    if (hasAggregate(item.expression)) continue;
    // A constant expression is the same for every group, as standard SQL allows. A full-text
    // node is never constant — MATCH(*) carries no column children before expansion, but it
    // reads every searchable column of its row.
    if (
      expressionColumns(item.expression).length === 0 &&
      !containsFtsExpression(item.expression)
    ) {
      continue;
    }
    if (!groupExpressions.has(JSON.stringify(item.expression))) {
      throw new TypeError(`Selected column must appear in GROUP BY: ${item.alias}`);
    }
  }
  const forbiddenAggregates = [
    ...plan.joins.flatMap((join) => [
      join.left,
      join.right,
      ...(join.on === undefined ? [] : [join.on]),
    ]),
    ...plan.predicates.flatMap((predicate) => [predicate.left, predicate.right]),
    ...plan.groupBy,
  ];
  if (forbiddenAggregates.some(hasAggregate))
    throw new TypeError("Aggregate functions are not allowed in JOIN, WHERE, or GROUP BY");
}

/** The column references inside one expression; a subquery contributes none (its own scope). */
export function expressionColumnNames(expression: Expression): string[] {
  return expressionColumns(expression);
}

function expressionColumns(expression: Expression): string[] {
  if (expression.kind === "column") return [expression.reference];
  // A subquery or EXISTS resolves inside its own scope before the enclosing block binds.
  return childExpressions(expression).flatMap(expressionColumns);
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
  /** Shared with the assembly helpers so parser plans and builder plans number sources alike. */
  readonly nextDerivedSequence = (): number => {
    this.#derivedSequence += 1;
    return this.#derivedSequence;
  };
  readonly #ctes = new Map<string, CompiledQuery>();
  readonly #ctesInProgress = new Set<string>();
  readonly #recursiveCtes = new Map<string, RecursiveCte>();
  readonly #recursiveUses = new Set<string>();
  #recursiveCandidate: { name: string; reference: string } | undefined;

  constructor(private readonly tokens: Token[]) {}

  parse(sql: string): CompiledQuery {
    if (this.#isKeyword("WITH")) {
      this.#keyword("WITH");
      let recursive = false;
      if (this.#isKeyword("RECURSIVE")) {
        this.#keyword("RECURSIVE");
        recursive = true;
      }
      for (;;) {
        const name = this.#identifier();
        if (
          this.#ctes.has(name) ||
          this.#ctesInProgress.has(name) ||
          this.#recursiveCtes.has(name)
        ) {
          throw new TypeError(`Duplicate CTE name: ${name}`);
        }
        this.#keyword("AS");
        this.#expectPunctuation("(");
        if (recursive) {
          this.#recursiveCte(name);
        } else {
          this.#ctesInProgress.add(name);
          const block = this.#selectBlock("(cte)");
          this.#ctesInProgress.delete(name);
          this.#expectPunctuation(")");
          this.#ctes.set(name, block);
        }
        if (!this.#punctuation(",")) break;
      }
    }
    // INTERSECT binds tighter than UNION and EXCEPT, per the SQL standard.
    const firstTerm = this.#setTerm(sql);
    let plan = firstTerm.block;
    if (this.#isKeyword("UNION") || this.#isKeyword("EXCEPT")) {
      const members = [firstTerm];
      const ops: SetOperator[] = [];
      while (this.#isKeyword("UNION") || this.#isKeyword("EXCEPT")) {
        if (this.#isKeyword("UNION")) {
          this.#keyword("UNION");
          if (this.#isKeyword("ALL")) {
            this.#keyword("ALL");
            ops.push("union all");
          } else ops.push("union");
        } else {
          this.#keyword("EXCEPT");
          ops.push("except");
        }
        members.push(this.#setTerm("(union member)"));
      }
      plan = this.#compoundBlock(sql, members, ops);
    }
    this.#take("eof");
    return plan;
  }

  #setTerm(sql: string): { block: CompiledQuery; parenthesized: boolean } {
    const first = this.#unionMember(sql);
    if (!this.#isKeyword("INTERSECT")) return first;
    const members = [first];
    const ops: SetOperator[] = [];
    while (this.#isKeyword("INTERSECT")) {
      this.#keyword("INTERSECT");
      ops.push("intersect");
      members.push(this.#unionMember("(intersect member)"));
    }
    return { block: this.#compoundBlock(sql, members, ops), parenthesized: false };
  }

  #compoundBlock(
    sql: string,
    members: ReadonlyArray<{ block: CompiledQuery; parenthesized: boolean }>,
    ops: SetOperator[],
  ): CompiledQuery {
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
    let orderBy: CompiledQuery["orderBy"];
    let limit: number | undefined;
    let offset: number | undefined;
    if (last !== undefined && !last.parenthesized) {
      orderBy = last.block.orderBy;
      limit = last.block.limit;
      offset = last.block.offset;
      last.block.orderBy = [];
      delete last.block.limit;
      delete last.block.offset;
    } else {
      orderBy = this.#orderByClause();
      limit = this.#limitClause();
      offset = this.#offsetClause(limit);
    }
    return compoundSelectBlock(
      sql,
      members.map((member) => member.block),
      ops,
      {
        orderBy,
        ...(limit === undefined ? {} : { limit }),
        ...(offset === undefined ? {} : { offset }),
      },
      this.nextDerivedSequence,
    );
  }

  parseMutation(keyword: "INSERT" | "UPDATE" | "DELETE"): CompiledStatement {
    const statement =
      keyword === "INSERT"
        ? this.#insertStatement()
        : keyword === "UPDATE"
          ? this.#updateStatement()
          : this.#deleteStatement();
    this.#take("eof");
    return statement;
  }

  #insertStatement(): CompiledStatement {
    this.#keyword("INSERT");
    this.#keyword("INTO");
    const table = this.#identifier();
    this.#expectPunctuation("(");
    const columns: string[] = [];
    for (;;) {
      columns.push(this.#identifier());
      if (!this.#punctuation(",")) break;
    }
    this.#expectPunctuation(")");
    if (new Set(columns).size !== columns.length) {
      throw new TypeError("INSERT columns must be unique");
    }
    this.#keyword("VALUES");
    const rows: QueryValue[][] = [];
    for (;;) {
      this.#expectPunctuation("(");
      const values: QueryValue[] = [];
      for (;;) {
        values.push(this.#constantValue("INSERT values"));
        if (!this.#punctuation(",")) break;
      }
      this.#expectPunctuation(")");
      if (values.length !== columns.length) {
        throw new TypeError("Each INSERT row must match the column list length");
      }
      rows.push(values);
      if (!this.#punctuation(",")) break;
    }
    return { kind: "insert", table, columns, rows };
  }

  #updateStatement(): CompiledStatement {
    this.#keyword("UPDATE");
    const table = this.#identifier();
    this.#keyword("SET");
    const assignments: Array<{ column: string; expression: Expression }> = [];
    for (;;) {
      const column = this.#identifier();
      this.#operator("=");
      const expression = this.#expression();
      if (hasAggregate(expression)) {
        throw new TypeError("Aggregate functions are not allowed in UPDATE assignments");
      }
      assignments.push({ column, expression });
      if (!this.#punctuation(",")) break;
    }
    if (new Set(assignments.map(({ column }) => column)).size !== assignments.length) {
      throw new TypeError("UPDATE assignments must set each column once");
    }
    return { kind: "update", table, assignments, predicates: this.#mutationPredicates() };
  }

  #deleteStatement(): CompiledStatement {
    this.#keyword("DELETE");
    this.#keyword("FROM");
    const table = this.#identifier();
    return { kind: "delete", table, predicates: this.#mutationPredicates() };
  }

  #mutationPredicates(): Array<{
    left: Expression;
    operator: PredicateOperator;
    right: Expression;
  }> {
    const predicates: Array<{
      left: Expression;
      operator: PredicateOperator;
      right: Expression;
    }> = [];
    if (this.#isKeyword("WHERE")) {
      this.#keyword("WHERE");
      for (const predicate of splitCondition(this.#expression())) {
        if (hasAggregate(predicate.left) || hasAggregate(predicate.right)) {
          throw new TypeError("Aggregate functions are not allowed in mutation predicates");
        }
        predicates.push(predicate);
      }
    }
    return predicates;
  }

  #constantValue(label: string): QueryValue {
    const expression = this.#expression();
    if (hasAggregate(expression) || expressionColumns(expression).length > 0) {
      throw new TypeError(`${label} must be constant expressions`);
    }
    return asQueryValue(evaluate(expression, {}));
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
    return validateLimit(Number(this.#take("number").text));
  }

  /** OFFSET is only accepted directly after LIMIT, matching the LIMIT n OFFSET m dialect form. */
  #offsetClause(limit: number | undefined): number | undefined {
    if (limit === undefined || !this.#isKeyword("OFFSET")) return undefined;
    this.#keyword("OFFSET");
    return validateOffset(Number(this.#take("number").text));
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
    let base = this.#source();
    const joins: JoinPlan[] = [];
    let rightJoins = 0;
    while (
      this.#isKeyword("JOIN") ||
      this.#isKeyword("INNER") ||
      this.#isKeyword("LEFT") ||
      this.#isKeyword("RIGHT")
    ) {
      let kind: JoinPlan["kind"] = "inner";
      let right = false;
      if (this.#isKeyword("INNER")) this.#keyword("INNER");
      else if (this.#isKeyword("LEFT")) {
        this.#keyword("LEFT");
        kind = "left";
      } else if (this.#isKeyword("RIGHT")) {
        this.#keyword("RIGHT");
        right = true;
        rightJoins += 1;
      }
      if (this.#isKeyword("OUTER")) this.#keyword("OUTER");
      this.#keyword("JOIN");
      const source = this.#source();
      this.#keyword("ON");
      const condition = this.#expression();
      if (condition.kind === "condition" && condition.operator === "=") {
        joins.push({ ...source, kind, left: condition.left, right: condition.right });
      } else {
        // Any other boolean condition becomes a nested-loop join.
        joins.push({
          ...source,
          kind,
          left: { kind: "literal", value: null },
          right: { kind: "literal", value: null },
          on: condition,
        });
      }
      if (right) {
        // A RIGHT JOIN is the mirrored LEFT JOIN; with a single join the sources just swap.
        if (joins.length !== 1 || rightJoins !== 1) {
          throw new TypeError("RIGHT JOIN is only supported as the sole join");
        }
        if (select.some((item) => item.expression.kind === "wildcard")) {
          throw new TypeError("RIGHT JOIN cannot be combined with SELECT *");
        }
        const [only] = joins;
        if (only !== undefined) {
          const { kind: joinKind, left, right: rightSide, ...joinSource } = only;
          void joinKind;
          const previousBase = base;
          base = joinSource;
          joins[0] = {
            ...previousBase,
            kind: "left",
            left,
            right: rightSide,
            ...(only.on === undefined ? {} : { on: only.on }),
          };
        }
      }
    }
    const predicates: Predicate[] = [];
    if (this.#isKeyword("WHERE")) {
      this.#keyword("WHERE");
      predicates.push(...splitCondition(this.#expression()));
    }
    const groupBy: Expression[] = [];
    if (this.#isKeyword("GROUP")) {
      this.#keyword("GROUP");
      this.#keyword("BY");
      groupBy.push(...this.#expressionList());
    }
    const having: Predicate[] = [];
    if (this.#isKeyword("HAVING")) {
      this.#keyword("HAVING");
      if (distinct) throw new TypeError("SELECT DISTINCT cannot be combined with HAVING");
      having.push(...splitCondition(this.#expression()));
    }
    const orderBy = this.#orderByClause();
    const limit = this.#limitClause();
    const offset = this.#offsetClause(limit);
    return assembleSelectBlock(
      {
        sql,
        base,
        joins,
        select,
        distinct,
        predicates,
        groupBy,
        having,
        orderBy,
        ...(limit === undefined ? {} : { limit }),
        ...(offset === undefined ? {} : { offset }),
      },
      this.nextDerivedSequence,
    );
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

  /**
   * Parses one WITH RECURSIVE body: `base UNION [ALL] step`, where only the step may reference
   * the CTE's own name. A body without a self-reference or UNION registers as a plain CTE.
   */
  #recursiveCte(name: string): void {
    const candidate = { name, reference: `(recursive ${name})` };
    this.#recursiveCandidate = candidate;
    try {
      const base = this.#selectBlock("(cte)");
      if (this.#recursiveUses.has(name)) {
        throw new TypeError("A recursive CTE may only reference itself in its step member");
      }
      if (!this.#isKeyword("UNION")) {
        this.#expectPunctuation(")");
        this.#ctes.set(name, base);
        return;
      }
      this.#keyword("UNION");
      let all = false;
      if (this.#isKeyword("ALL")) {
        this.#keyword("ALL");
        all = true;
      }
      const step = this.#selectBlock("(cte step)");
      if (this.#isKeyword("UNION") || this.#isKeyword("INTERSECT") || this.#isKeyword("EXCEPT")) {
        throw new TypeError("A recursive CTE takes exactly one UNION between base and step");
      }
      this.#expectPunctuation(")");
      if (!this.#recursiveUses.has(name)) {
        // No self-reference: the body is an ordinary two-member compound.
        this.#ctes.set(
          name,
          compoundSelectBlock(
            "(cte)",
            [base, step],
            [all ? "union all" : "union"],
            { orderBy: [] },
            this.nextDerivedSequence,
          ),
        );
        return;
      }
      this.#recursiveCtes.set(name, { reference: candidate.reference, base, step, all });
    } finally {
      this.#recursiveCandidate = undefined;
    }
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
    const candidate = this.#recursiveCandidate;
    if (candidate?.name === table) {
      this.#recursiveUses.add(table);
      return { table: candidate.reference, alias: this.#sourceAlias() ?? table };
    }
    if (this.#ctesInProgress.has(table)) {
      throw new TypeError(`Recursive CTE references require WITH RECURSIVE: ${table}`);
    }
    const alias = this.#sourceAlias() ?? table;
    const cte = this.#ctes.get(table);
    if (cte !== undefined) return this.#derivedSource(structuredClone(cte), alias);
    const recursive = this.#recursiveCtes.get(table);
    if (recursive !== undefined) {
      return {
        table: `(recursive ${String(this.nextDerivedSequence())}) ${table}`,
        alias,
        recursive: structuredClone(recursive),
      };
    }
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
    return derivedTableSource(derived, alias, this.nextDerivedSequence);
  }

  #expressionList(): Expression[] {
    const values = [this.#expression()];
    while (this.#punctuation(",")) values.push(this.#expression());
    return values;
  }

  // Full-precedence expression grammar: OR < AND < NOT < comparison < additive < multiplicative.
  // Comparisons are ordinary boolean-valued expressions, so parenthesized conditions, boolean
  // select items, and NOT/OR trees all share one parse path.
  #expression(): Expression {
    let left = this.#andExpression();
    while (this.#isKeyword("OR")) {
      this.#keyword("OR");
      left = { kind: "logical", operator: "or", left, right: this.#andExpression() };
    }
    return left;
  }

  #andExpression(): Expression {
    let left = this.#notExpression();
    while (this.#isKeyword("AND")) {
      this.#keyword("AND");
      left = { kind: "logical", operator: "and", left, right: this.#notExpression() };
    }
    return left;
  }

  #notExpression(): Expression {
    if (this.#isKeyword("NOT")) {
      this.#keyword("NOT");
      return { kind: "not", operand: this.#notExpression() };
    }
    return this.#comparisonExpression();
  }

  #comparisonExpression(): Expression {
    const left = this.#additive();
    if (this.#isKeyword("IS")) {
      this.#keyword("IS");
      let operator: PredicateOperator = "IS NULL";
      if (this.#isKeyword("NOT")) {
        this.#keyword("NOT");
        operator = "IS NOT NULL";
      }
      const token = this.#peek();
      if (token.kind !== "identifier" || token.text.toUpperCase() !== "NULL") {
        throw new TypeError(`Expected NULL, found ${token.text || "end of query"}`);
      }
      this.#identifier();
      return { kind: "condition", operator, left, right: { kind: "literal", value: null } };
    }
    // After a value expression, NOT can only introduce NOT BETWEEN / NOT IN / NOT LIKE.
    let negated = false;
    if (this.#isKeyword("NOT")) {
      this.#keyword("NOT");
      negated = true;
      if (!this.#isKeyword("BETWEEN") && !this.#isKeyword("IN") && !this.#isKeyword("LIKE")) {
        throw new TypeError(`Expected BETWEEN, IN, or LIKE after NOT`);
      }
    }
    if (this.#isKeyword("BETWEEN")) {
      this.#keyword("BETWEEN");
      const lower = this.#additive();
      this.#keyword("AND");
      const upper = this.#additive();
      // BETWEEN is inclusive-range sugar; the AND here binds at comparison level by the grammar.
      const range: Expression = {
        kind: "logical",
        operator: "and",
        left: { kind: "condition", operator: ">=", left, right: lower },
        right: { kind: "condition", operator: "<=", left: structuredClone(left), right: upper },
      };
      return negated ? { kind: "not", operand: range } : range;
    }
    if (this.#isKeyword("IN")) {
      this.#keyword("IN");
      const operator: PredicateOperator = negated ? "NOT IN" : "IN";
      this.#expectPunctuation("(");
      if (this.#isKeyword("SELECT")) {
        const block = this.#selectBlock("(subquery)");
        this.#expectPunctuation(")");
        return { kind: "condition", operator, left, right: { kind: "subquery", block } };
      }
      const items = this.#expressionList();
      this.#expectPunctuation(")");
      if (items.some((item) => item.kind === "wildcard" || item.kind === "subquery")) {
        throw new TypeError("IN lists accept only scalar expressions");
      }
      return { kind: "condition", operator, left, right: { kind: "list", items } };
    }
    if (this.#isKeyword("LIKE")) {
      this.#keyword("LIKE");
      const operator: PredicateOperator = negated ? "NOT LIKE" : "LIKE";
      return { kind: "condition", operator, left, right: this.#additive() };
    }
    const token = this.#peek();
    if (token.kind === "operator" && ["=", "!=", "<>", ">", ">=", "<", "<="].includes(token.text)) {
      const operator = this.#comparison();
      return { kind: "condition", operator, left, right: this.#additive() };
    }
    return left;
  }

  #additive(minimumPrecedence = 0): Expression {
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
        right: this.#additive(precedence + 1),
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
    if (upper === "CASE") {
      let operand: Expression | undefined;
      if (!this.#isKeyword("WHEN")) operand = this.#expression();
      const branches: Array<{ when: Expression; then: Expression }> = [];
      while (this.#isKeyword("WHEN")) {
        this.#keyword("WHEN");
        let when = this.#expression();
        // The simple form CASE x WHEN v desugars to searched equality comparisons.
        if (operand !== undefined) {
          when = { kind: "condition", operator: "=", left: structuredClone(operand), right: when };
        }
        this.#keyword("THEN");
        branches.push({ when, then: this.#expression() });
      }
      if (branches.length === 0) throw new TypeError("CASE requires at least one WHEN branch");
      let otherwise: Expression | undefined;
      if (this.#isKeyword("ELSE")) {
        this.#keyword("ELSE");
        otherwise = this.#expression();
      }
      this.#keyword("END");
      return { kind: "case", branches, ...(otherwise === undefined ? {} : { otherwise }) };
    }
    if (upper === "EXISTS" && this.#peek().text === "(") {
      this.#expectPunctuation("(");
      if (!this.#isKeyword("SELECT")) throw new TypeError("EXISTS requires a subquery");
      const block = this.#selectBlock("(subquery)");
      this.#expectPunctuation(")");
      // Existence needs at most one row, so cap the subquery unless it already limits itself.
      block.limit ??= 1;
      return { kind: "exists", block, negated: false };
    }
    if ((upper === "MATCH" || upper === "BM25") && this.#peek().text === "(") {
      // MySQL-style full-text grammar for both nodes: MATCH(col, ... | *) AGAINST 'query' is
      // the boolean document predicate, BM25(col, ... | *) AGAINST 'query' the relevance score.
      const columns = this.#ftsColumns();
      this.#keyword("AGAINST");
      const queryToken = this.#peek();
      if (queryToken.kind !== "string") {
        throw new TypeError(`${upper} ... AGAINST requires a string literal query`);
      }
      this.#index += 1;
      validateFtsQuery(queryToken.text);
      return {
        kind: "fts",
        op: upper === "MATCH" ? "match" : "bm25",
        columns,
        query: queryToken.text,
      };
    }
    if (upper === "DATE" && this.#peek().kind === "string") {
      const date = new Date(`${this.#take("string").text}T00:00:00.000Z`);
      if (!Number.isFinite(date.getTime())) throw new TypeError("Invalid DATE literal");
      return { kind: "literal", value: date };
    }
    if (this.#punctuation("(")) {
      if (upper === "ROW_NUMBER" || upper === "RANK" || upper === "DENSE_RANK") {
        this.#expectPunctuation(")");
        this.#keyword("OVER");
        const { partitionBy, orderBy } = this.#overClause();
        return { kind: "window", name: upper, partitionBy, orderBy };
      }
      const name = upper as AggregateName | ScalarFunctionName;
      if (!aggregateNames.has(name as AggregateName) && !scalarFunctionNames.has(name))
        throw new TypeError(`Unsupported function: ${identifier}`);
      let distinct = false;
      if (this.#isKeyword("DISTINCT")) {
        if (name !== "COUNT") throw new TypeError("DISTINCT is only supported inside COUNT");
        this.#keyword("DISTINCT");
        distinct = true;
      }
      const args: Expression[] = [];
      if (!this.#punctuation(")")) {
        args.push(...this.#expressionList());
        this.#expectPunctuation(")");
      }
      if (distinct && (args.length !== 1 || args[0]?.kind === "wildcard")) {
        throw new TypeError("COUNT(DISTINCT) requires exactly one scalar argument");
      }
      if (aggregateNames.has(name as AggregateName) && args.length !== 1)
        throw new TypeError(`${name} requires exactly one argument`);
      if (name === "ROUND" && (args.length < 1 || args.length > 2))
        throw new TypeError("ROUND requires one or two arguments");
      if (name === "COALESCE" && args.length < 1)
        throw new TypeError("COALESCE requires at least one argument");
      if (name === "DATE_TRUNC") {
        if (args.length !== 2)
          throw new TypeError("DATE_TRUNC requires a unit and a datetime argument");
        const unit = args[0];
        if (
          unit?.kind === "literal" &&
          typeof unit.value === "string" &&
          !dateTruncUnits.has(unit.value.toLowerCase())
        ) {
          throw new TypeError(`Unsupported DATE_TRUNC unit: ${unit.value}`);
        }
      }
      if (aggregateNames.has(name as AggregateName) && this.#isKeyword("OVER")) {
        if (distinct) throw new TypeError("DISTINCT window aggregates are not supported");
        this.#keyword("OVER");
        const { partitionBy, orderBy } = this.#overClause();
        const argument = args[0];
        if (argument !== undefined && hasAggregate(argument)) {
          throw new TypeError("Window aggregate arguments cannot contain aggregates");
        }
        return {
          kind: "window",
          name: name as AggregateName,
          partitionBy,
          orderBy,
          ...(argument === undefined || argument.kind === "wildcard" ? {} : { argument }),
        };
      }
      return { kind: "call", name, arguments: args, ...(distinct ? { distinct: true } : {}) };
    }
    let reference = identifier;
    if (this.#punctuation(".")) reference += `.${this.#identifier()}`;
    return { kind: "column", reference };
  }

  /** The parenthesized column set of a full-text expression: `(*)` or `(col[, col...])`. */
  #ftsColumns(): Expression[] | "*" {
    this.#expectPunctuation("(");
    if (this.#peek().text === "*") {
      this.#index += 1;
      this.#expectPunctuation(")");
      return "*";
    }
    const columns: Expression[] = [];
    for (;;) {
      let reference = this.#identifier();
      if (this.#punctuation(".")) reference += `.${this.#identifier()}`;
      columns.push({ kind: "column", reference });
      if (!this.#punctuation(",")) break;
    }
    this.#expectPunctuation(")");
    return columns;
  }

  #overClause(): {
    partitionBy: Expression[];
    orderBy: Array<{ expression: Expression; direction: "asc" | "desc" }>;
  } {
    this.#expectPunctuation("(");
    const partitionBy: Expression[] = [];
    if (this.#isKeyword("PARTITION")) {
      this.#keyword("PARTITION");
      this.#keyword("BY");
      partitionBy.push(...this.#expressionList());
    }
    const orderBy = this.#orderByClause();
    if (this.#isKeyword("ROWS") || this.#isKeyword("RANGE") || this.#isKeyword("GROUPS")) {
      throw new TypeError("Window frames are not supported");
    }
    this.#expectPunctuation(")");
    return { partitionBy, orderBy };
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
    const last = this.tokens[this.tokens.length - 1];
    const end = last === undefined ? 0 : last.end;
    return this.tokens[this.#index] ?? { kind: "eof", text: "", start: end, end };
  }

  /**
   * Span of the token the parser is positioned on. Syntax errors throw while sitting on the token
   * that failed, so this is where the failure belongs; errors raised after a block finishes
   * parsing land on the token that follows it.
   */
  get span(): { start: number; end: number } {
    const { start, end } = this.#peek();
    return { start, end };
  }
}

// --- Shared select-block assembly ---------------------------------------------------------------
//
// The SQL parser and the typed query builder both end at these functions, so a builder query and
// its equivalent SQL produce byte-identical plans: the same validation errors, the same DISTINCT /
// COUNT(DISTINCT) / window desugars, and the same derived-source numbering (callers thread one
// sequence counter through every source they create, in parse order).

export interface SelectBlockParts {
  sql: string;
  base: TableSource;
  joins: JoinPlan[];
  select: SelectItem[];
  distinct: boolean;
  predicates: Predicate[];
  /** Raw GROUP BY expressions; DISTINCT desugaring appends to a copy here. */
  groupBy: Expression[];
  having: Predicate[];
  orderBy: CompiledQuery["orderBy"];
  limit?: number;
  offset?: number;
}

/** Validates and assembles one select block, applying every parser desugar. */
export function assembleSelectBlock(
  parts: SelectBlockParts,
  nextSequence: () => number,
): CompiledQuery {
  if (parts.orderBy.some((order) => order.expression.kind !== "column")) {
    return assembleOrderByExpressionBlock(parts, nextSequence);
  }
  const { sql, base, joins, select, distinct, predicates, having, orderBy, limit, offset } = parts;
  const groupBy = [...parts.groupBy];
  if (distinct) {
    if (select.some((item) => item.expression.kind === "wildcard"))
      throw new TypeError("SELECT DISTINCT * is not supported");
    if (select.some((item) => hasAggregate(item.expression)))
      throw new TypeError("SELECT DISTINCT cannot be combined with aggregate functions");
    if (groupBy.length > 0) throw new TypeError("SELECT DISTINCT cannot be combined with GROUP BY");
    if (having.length > 0) throw new TypeError("SELECT DISTINCT cannot be combined with HAVING");
    // DISTINCT is grouping by every selected expression, so it reuses the grouped executor,
    // its value-carrying spill, and streamed scan inputs unchanged.
    groupBy.push(...select.map((item) => item.expression));
  }
  if (having.length > 0) {
    const grouped =
      parts.groupBy.length > 0 || select.some((item) => hasAggregate(item.expression));
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
  const clauseExpressions = [
    ...predicates.flatMap((predicate) => [predicate.left, predicate.right]),
    ...groupBy,
    ...having.flatMap((predicate) => [predicate.left, predicate.right]),
    ...orderBy.map((order) => order.expression),
  ];
  if (clauseExpressions.some(containsWindow)) {
    throw new TypeError("Window functions are only allowed in the select list");
  }
  if (
    clauseExpressions.some(containsDistinctCount) ||
    select.some(
      (item) =>
        containsDistinctCount(item.expression) &&
        !(item.expression.kind === "call" && item.expression.distinct === true),
    )
  ) {
    throw new TypeError("COUNT(DISTINCT) must be a top-level select item");
  }
  const tail = {
    orderBy,
    ...(limit === undefined ? {} : { limit }),
    ...(offset === undefined ? {} : { offset }),
  };
  const distinctCounts = select.filter(
    (item) => item.expression.kind === "call" && item.expression.distinct === true,
  );
  if (distinctCounts.length > 0) {
    if (distinctCounts.length > 1) {
      throw new TypeError("Only one COUNT(DISTINCT) is supported per select");
    }
    if (
      select.some(
        (item) =>
          item.expression.kind !== "window" &&
          !(item.expression.kind === "call" && item.expression.distinct === true) &&
          hasAggregate(item.expression),
      )
    ) {
      throw new TypeError("COUNT(DISTINCT) cannot be combined with other aggregates yet");
    }
    if (select.some((item) => containsWindow(item.expression))) {
      throw new TypeError("COUNT(DISTINCT) cannot be combined with window functions");
    }
    if (having.length > 0) {
      throw new TypeError("COUNT(DISTINCT) cannot be combined with HAVING yet");
    }
    return desugarDistinctCount(sql, base, joins, select, predicates, groupBy, tail, nextSequence);
  }
  if (select.some((item) => containsWindow(item.expression))) {
    return desugarWindows(
      sql,
      base,
      joins,
      select,
      predicates,
      groupBy,
      having,
      tail,
      nextSequence,
    );
  }
  return {
    sql,
    base,
    joins,
    select,
    predicates,
    groupBy,
    having,
    ...tail,
  };
}

/** Wraps compound members into the set-operation source the executor folds left to right. */
export function compoundSelectBlock(
  sql: string,
  blocks: CompiledQuery[],
  ops: SetOperator[],
  tail: { orderBy: CompiledQuery["orderBy"]; limit?: number; offset?: number },
  nextSequence: () => number,
): CompiledQuery {
  return {
    sql,
    base: {
      table: `(union ${String(nextSequence())})`,
      alias: "union",
      union: { blocks, ops },
    },
    joins: [],
    select: [{ expression: { kind: "wildcard" }, alias: "*" }],
    predicates: [],
    groupBy: [],
    having: [],
    orderBy: tail.orderBy,
    ...(tail.limit === undefined ? {} : { limit: tail.limit }),
    ...(tail.offset === undefined ? {} : { offset: tail.offset }),
  };
}

/**
 * Desugars ORDER BY expressions the way windows already hide their machinery: each expression
 * either reuses a structurally identical select item's alias or becomes a hidden "(order N)"
 * select item, the ordering (and LIMIT/OFFSET) applies inside that block, and an outer block
 * projects only the visible aliases away from a derived source. Runs in the shared assembly,
 * so the builder and SQL front ends produce identical plans. A wildcard select has no named
 * output list to hide items behind, and DISTINCT's output would change if hidden expressions
 * joined its grouping, so both keep the named-column restriction.
 */
function assembleOrderByExpressionBlock(
  parts: SelectBlockParts,
  nextSequence: () => number,
): CompiledQuery {
  if (parts.select.some((item) => item.expression.kind === "wildcard")) {
    throw new TypeError(
      "ORDER BY expressions require a named select list; SELECT * orders by column names only",
    );
  }
  if (parts.distinct) {
    throw new TypeError("SELECT DISTINCT orders by selected columns or output aliases only");
  }
  for (const order of parts.orderBy) {
    if (order.expression.kind === "column") continue;
    if (containsWindow(order.expression)) {
      throw new TypeError("Window functions are only allowed in the select list");
    }
    // A bare literal is almost always a SQL ordinal (ORDER BY 2); sorting by a constant would
    // silently do nothing, so reject it the way the engine always has.
    if (order.expression.kind === "literal") {
      throw new TypeError("ORDER BY ordinals are not supported; name the column or alias");
    }
  }
  const selectSignatures = new Map(
    parts.select.map((item) => [JSON.stringify(item.expression), item.alias] as const),
  );
  const hiddenItems: SelectItem[] = [];
  const rewrittenOrder = parts.orderBy.map((order) => {
    if (order.expression.kind === "column") return order;
    const existingAlias = selectSignatures.get(JSON.stringify(order.expression));
    if (existingAlias !== undefined) {
      return {
        expression: { kind: "column", reference: existingAlias } satisfies Expression,
        direction: order.direction,
      };
    }
    const alias = `(order ${String(hiddenItems.length + 1)})`;
    hiddenItems.push({ expression: order.expression, alias });
    return {
      expression: { kind: "column", reference: alias } satisfies Expression,
      direction: order.direction,
    };
  });
  const inner = assembleSelectBlock(
    { ...parts, select: [...parts.select, ...hiddenItems], orderBy: rewrittenOrder },
    nextSequence,
  );
  // Every ordering expression matched a visible select item — no hidden columns, no wrap.
  if (hiddenItems.length === 0) return inner;
  const source = derivedTableSource(inner, "(ordered)", nextSequence);
  return {
    sql: parts.sql,
    base: source,
    joins: [],
    select: parts.select.map((item) => ({
      expression: { kind: "column", reference: item.alias } satisfies Expression,
      alias: item.alias,
    })),
    predicates: [],
    groupBy: [],
    having: [],
    orderBy: [],
  };
}

/**
 * Detects a pure projection wrapper over one derived block — the shape the ORDER-BY-expression
 * desugar emits: no joins, filters, grouping, ordering, or paging of its own, and every select
 * item passing an inner output alias through under the same name. Whole-plan strategies (the
 * streamed scan today) run the inner block and project its result, so a hidden ordering column
 * never changes which execution paths a query is eligible for — `.search()` performs the same
 * whether or not the caller also selects the score.
 */
export function transparentProjectionSource(
  plan: CompiledQuery,
): { inner: CompiledQuery; aliases: string[] } | undefined {
  if (
    plan.joins.length > 0 ||
    plan.predicates.length > 0 ||
    plan.groupBy.length > 0 ||
    plan.having.length > 0 ||
    plan.orderBy.length > 0 ||
    plan.limit !== undefined ||
    plan.offset !== undefined
  ) {
    return undefined;
  }
  const inner = plan.base.derived;
  if (
    inner === undefined ||
    plan.base.union !== undefined ||
    plan.base.windowed !== undefined ||
    plan.base.recursive !== undefined
  ) {
    return undefined;
  }
  if (inner.select[0]?.expression.kind === "wildcard") return undefined;
  const innerAliases = new Set(inner.select.map((item) => item.alias));
  const aliases: string[] = [];
  for (const item of plan.select) {
    if (
      item.expression.kind !== "column" ||
      item.expression.reference !== item.alias ||
      !innerAliases.has(item.alias)
    ) {
      return undefined;
    }
    aliases.push(item.alias);
  }
  if (aliases.length === 0) return undefined;
  return { inner, aliases };
}

/** Projects a result to a wrapper's visible aliases, preserving row order. */
export function projectResultColumns(result: QueryResult, aliases: readonly string[]): QueryResult {
  return {
    columns: [...aliases],
    rows: result.rows.map((row) => {
      const projected: QueryRow = {};
      for (const alias of aliases) projected[alias] = row[alias] ?? null;
      return projected;
    }),
  };
}

/** Names a derived (subquery or expanded CTE) source under the shared sequence. */
export function derivedTableSource(
  derived: CompiledQuery,
  alias: string,
  nextSequence: () => number,
): TableSource {
  return { table: `(derived ${String(nextSequence())}) ${alias}`, alias, derived };
}

/** The parser's LIMIT range contract, shared with the typed builder. */
export function validateLimit(limit: number): number {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100_000)
    throw new RangeError("LIMIT must be between 1 and 100,000");
  return limit;
}

/** The parser's OFFSET range contract, shared with the typed builder. */
export function validateOffset(offset: number): number {
  if (!Number.isSafeInteger(offset) || offset < 0 || offset > 100_000_000)
    throw new RangeError("OFFSET must be between 0 and 100,000,000");
  return offset;
}

/**
 * Rewrites `COUNT(DISTINCT e)` as counting the deduplicated inner block: the inner block groups
 * by every outer group key plus `e` (deduplication through the ordinary grouped machinery,
 * including its spill), and the wrapper counts the non-null deduplicated values per group. The
 * output column names and order are preserved exactly.
 */
function desugarDistinctCount(
  sql: string,
  base: TableSource,
  joins: JoinPlan[],
  select: SelectItem[],
  predicates: Predicate[],
  groupBy: Expression[],
  tail: { orderBy: CompiledQuery["orderBy"]; limit?: number; offset?: number },
  nextSequence: () => number,
): CompiledQuery {
  const distinctAlias = "(distinct 1)";
  const innerSelect: SelectItem[] = [];
  const outerSelect: SelectItem[] = [];
  for (const item of select) {
    if (item.expression.kind === "call" && item.expression.distinct === true) {
      const argument = item.expression.arguments[0];
      if (argument === undefined) throw new TypeError("COUNT(DISTINCT) argument is missing");
      innerSelect.push({ expression: argument, alias: distinctAlias });
      outerSelect.push({
        expression: {
          kind: "call",
          name: "COUNT",
          arguments: [{ kind: "column", reference: distinctAlias }],
        },
        alias: item.alias,
      });
      continue;
    }
    innerSelect.push(item);
    outerSelect.push({
      expression: { kind: "column", reference: item.alias },
      alias: item.alias,
    });
  }
  const distinctArgument = innerSelect.find(({ alias }) => alias === distinctAlias);
  if (distinctArgument === undefined) {
    throw new TypeError("COUNT(DISTINCT) argument is missing");
  }
  const inner: CompiledQuery = {
    sql: "(count distinct input)",
    base,
    joins,
    select: innerSelect,
    predicates,
    groupBy: [...groupBy, distinctArgument.expression],
    having: [],
    orderBy: [],
  };
  return {
    sql,
    base: derivedTableSource(inner, "distinct", nextSequence),
    joins: [],
    select: outerSelect,
    predicates: [],
    groupBy: outerSelect
      .filter(({ expression }) => expression.kind === "column")
      .map(({ expression }) => expression),
    having: [],
    ...tail,
  };
}

/**
 * Rewrites a block with window select items into a wrapper over a windowed source: the inner
 * block computes every non-window item plus hidden partition and ordering columns, the window
 * columns append after execution, and the wrapper projects the visible aliases and applies the
 * block's ORDER BY and LIMIT after window computation, as SQL requires.
 */
function desugarWindows(
  sql: string,
  base: TableSource,
  joins: JoinPlan[],
  select: SelectItem[],
  predicates: Predicate[],
  groupBy: Expression[],
  having: Predicate[],
  tail: { orderBy: CompiledQuery["orderBy"]; limit?: number; offset?: number },
  nextSequence: () => number,
): CompiledQuery {
  if (
    groupBy.length > 0 ||
    having.length > 0 ||
    select.some((item) => hasAggregate(item.expression))
  ) {
    throw new TypeError(
      "Window functions cannot be combined with GROUP BY, DISTINCT, aggregates, or HAVING",
    );
  }
  for (const item of select) {
    if (item.expression.kind !== "window" && containsWindow(item.expression)) {
      throw new TypeError("Window functions must be top-level select items");
    }
  }
  const innerSelect: SelectItem[] = select.filter((item) => item.expression.kind !== "window");
  const windows: WindowSpec[] = [];
  let hidden = 0;
  for (const item of select) {
    const expression = item.expression;
    if (expression.kind !== "window") continue;
    // Full-text nodes evaluate against a scanned base table; a window's hidden columns
    // compute over the windowed wrapper where no such scan exists.
    const windowInputs = [
      ...expression.partitionBy,
      ...expression.orderBy.map((order) => order.expression),
      ...(expression.argument === undefined ? [] : [expression.argument]),
    ];
    if (windowInputs.some(containsFtsExpression)) {
      throw new TypeError("Full-text MATCH and BM25 are not supported inside OVER(...)");
    }
    const partitionAliases = expression.partitionBy.map((partition) => {
      hidden += 1;
      const alias = `(window ${String(hidden)})`;
      innerSelect.push({ expression: partition, alias });
      return alias;
    });
    const orderAliases = expression.orderBy.map((order) => {
      hidden += 1;
      const alias = `(window ${String(hidden)})`;
      innerSelect.push({ expression: order.expression, alias });
      return { alias, direction: order.direction };
    });
    let argumentAlias: string | undefined;
    if (expression.argument !== undefined) {
      hidden += 1;
      argumentAlias = `(window ${String(hidden)})`;
      innerSelect.push({ expression: expression.argument, alias: argumentAlias });
    }
    windows.push({
      alias: item.alias,
      name: expression.name,
      partitionAliases,
      orderAliases,
      ...(argumentAlias === undefined ? {} : { argumentAlias }),
    });
  }
  if (innerSelect.length === 0) {
    innerSelect.push({ expression: { kind: "literal", value: 1 }, alias: "(window 0)" });
  }
  const inner: CompiledQuery = {
    sql: "(window input)",
    base,
    joins,
    select: innerSelect,
    predicates,
    groupBy: [],
    having: [],
    orderBy: [],
  };
  return {
    sql,
    base: {
      table: `(window ${String(nextSequence())})`,
      alias: "window",
      windowed: { block: inner, windows },
    },
    joins: [],
    select: select.map((item) => ({
      expression: { kind: "column", reference: item.alias },
      alias: item.alias,
    })),
    predicates: [],
    groupBy: [],
    having: [],
    ...tail,
  };
}

/**
 * Splits a parsed boolean expression into the plan's AND-list of predicates. Top-level ANDs and
 * plain comparisons keep the classic {left, operator, right} shape (so predicate pushdown, zone
 * maps, and the dictionary fast path see exactly what they always saw); any OR/NOT subtree or bare
 * boolean expression becomes a single IS TRUE predicate evaluated with three-valued logic.
 */
export function splitCondition(expression: Expression): Predicate[] {
  if (expression.kind === "logical" && expression.operator === "and") {
    return [...splitCondition(expression.left), ...splitCondition(expression.right)];
  }
  if (expression.kind === "condition") {
    return [{ left: expression.left, operator: expression.operator, right: expression.right }];
  }
  return [{ left: expression, operator: "IS TRUE", right: { kind: "literal", value: null } }];
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
      tokens.push({ kind: "identifier", text: sql.slice(start, index), start, end: index });
      continue;
    }
    if (/\d/.test(character)) {
      const start = index++;
      while (index < sql.length && /[\d.]/.test(sql[index] ?? "")) index += 1;
      const text = sql.slice(start, index);
      if (!/^\d+(?:\.\d+)?$/.test(text))
        throw new SqlCompileError(`Invalid number: ${text}`, start, index - start);
      tokens.push({ kind: "number", text, start, end: index });
      continue;
    }
    if (character === "'") {
      const start = index++;
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
      if (!closed)
        throw new SqlCompileError("Unterminated string literal", start, sql.length - start);
      tokens.push({ kind: "string", text: value, start, end: index });
      continue;
    }
    const pair = sql.slice(index, index + 2);
    if (pair === "--" || pair === "/*") {
      throw new SqlCompileError("SQL comments are not supported", index, pair.length);
    }
    if (character === ";")
      throw new SqlCompileError("Run one SELECT statement at a time", index, 1);
    if ([">=", "<=", "!=", "<>"].includes(pair)) {
      tokens.push({ kind: "operator", text: pair, start: index, end: index + 2 });
      index += 2;
      continue;
    }
    if (["+", "-", "*", "/", "=", ">", "<"].includes(character))
      tokens.push({ kind: "operator", text: character, start: index, end: index + 1 });
    else if (["(", ")", ",", "."].includes(character))
      tokens.push({ kind: "punctuation", text: character, start: index, end: index + 1 });
    else throw new SqlCompileError(`Unsupported SQL character: ${character}`, index, 1);
    index += 1;
  }
  tokens.push({ kind: "eof", text: "", start: sql.length, end: sql.length });
  return tokens;
}
