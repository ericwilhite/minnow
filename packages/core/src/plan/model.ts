/**
 * The dependency-light logical plan shared by the SQL compiler, optimizer, vector executor, and
 * external typed-query builders. Keep executable parser/engine code out of this module: importing
 * these types must never create a runtime dependency edge.
 */
import type { SqlDomain } from "../storage/types.js";

export type QueryValue = boolean | number | string | Date | null;
export type QueryRow = Record<string, QueryValue>;

export interface QueryResult {
  columns: string[];
  /** Logical SQL domain for each output column, positionally aligned with `columns`. */
  columnDomains: Array<SqlDomain | null>;
  rows: QueryRow[];
}

export type BinaryOperator = "+" | "-" | "*" | "/" | "%" | "||";
export type ComparisonOperator = "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=";
export type AggregateName =
  "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "JSON_ARRAYAGG" | "STRING_AGG";
export type ScalarFunctionName =
  | "ROUND"
  | "COALESCE"
  | "DATE_TRUNC"
  | "DATE_ADD"
  | "UPPER"
  | "LOWER"
  | "LENGTH"
  | "ABS"
  | "TRIM"
  | "LTRIM"
  | "RTRIM"
  | "SUBSTR"
  | "REPLACE"
  | "INSTR"
  | "NULLIF"
  | "GREATEST"
  | "LEAST"
  | "FLOOR"
  | "CEIL"
  | "MOD"
  | "POWER"
  | "SQRT"
  | "EXTRACT"
  | "CAST"
  | "OCTET_LENGTH"
  | "LPAD"
  | "RPAD"
  | "OVERLAY"
  | "CURRENT_DATE"
  | "CURRENT_TIMESTAMP"
  | "LOCALTIME"
  | "GROUPING"
  | "JSON_VALUE"
  | "JSON_QUERY"
  | "JSON_EXISTS"
  | "JSON_OBJECT"
  | "JSON_ARRAY"
  | "IS_JSON"
  | "ARRAY"
  /** Optimizer-only, prefix-free equality key for hashable multi-column decorrelation. */
  | "MINNOW_TUPLE_KEY"
  /** Parser-produced wrapper carrying one explicit collation through ordering/comparison. */
  | "MINNOW_COLLATE"
  | "NEXTVAL"
  | "CURRVAL"
  | "RANDOM"
  | "GEN_RANDOM_UUID";

/** Exact BM25 corpus statistics attached to a cloned scoring node before execution. */
export interface FtsStats {
  /** Every row of the corpus, including all-null documents. */
  docCount: number;
  /** Total tokens across all documents. */
  totalTokens: number;
  /** Documents containing each query term, aligned with the query's term order. */
  dfByTerm: number[];
}

export type Expression =
  | { kind: "literal"; value: QueryValue; internalSqlValue?: true; sqlDomain?: SqlDomain }
  /** A `?` or `$n` placeholder; `index` is 0-based. Replaced by a literal at bind time. */
  | { kind: "parameter"; index: number }
  | { kind: "column"; reference: string }
  /** `*`, or `alias.*` when `table` is set. */
  | { kind: "wildcard"; table?: string }
  | { kind: "binary"; operator: BinaryOperator; left: Expression; right: Expression }
  | {
      kind: "call";
      name: AggregateName | ScalarFunctionName;
      arguments: Expression[];
      distinct?: boolean;
      aggregateOrderBy?: Array<{
        expression: Expression;
        direction: "asc" | "desc";
        nulls?: "first" | "last";
      }>;
    }
  | { kind: "list"; items: Expression[] }
  | { kind: "subquery"; block: CompiledQuery }
  | {
      kind: "condition";
      operator: PredicateOperator;
      left: Expression;
      right: Expression;
      escape?: string;
    }
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
      orderBy: Array<{
        expression: Expression;
        direction: "asc" | "desc";
        nulls?: "first" | "last";
      }>;
      argument?: Expression;
      offset?: number;
      fallback?: QueryValue;
      frame?: WindowFrame;
    }
  | {
      kind: "fts";
      op: "match" | "bm25";
      /** Column references forming the document, or `*` for all searchable scan columns. */
      columns: Expression[] | "*";
      query: string;
      queryParameter?: number;
      stats?: FtsStats;
    };

export type WindowFunctionName =
  | "ROW_NUMBER"
  | "RANK"
  | "DENSE_RANK"
  | "PERCENT_RANK"
  | "CUME_DIST"
  | "NTILE"
  | "LAG"
  | "LEAD"
  | "FIRST_VALUE"
  | "LAST_VALUE"
  | "NTH_VALUE"
  | AggregateName;

export interface WindowFrameBound {
  kind: "unbounded-preceding" | "preceding" | "current-row" | "following" | "unbounded-following";
  offset?: number;
}

export type WindowFrameExclusion = "no-others" | "current-row" | "group" | "ties";

export interface WindowFrame {
  unit: "rows" | "range" | "groups";
  start: WindowFrameBound;
  end: WindowFrameBound;
  exclude?: WindowFrameExclusion;
}

export interface WindowSpec {
  alias: string;
  name: WindowFunctionName;
  partitionAliases: string[];
  orderAliases: Array<{ alias: string; direction: "asc" | "desc"; nulls?: "first" | "last" }>;
  argumentAlias?: string;
  offset?: number;
  fallback?: QueryValue;
  frame?: WindowFrame;
}

export interface SelectItem {
  expression: Expression;
  alias: string;
}

export interface TableSource {
  table: string;
  alias: string;
  derived?: CompiledQuery;
  union?: { blocks: CompiledQuery[]; ops: SetOperator[] };
  recursive?: RecursiveCte;
  windowed?: { block: CompiledQuery; windows: WindowSpec[] };
  columnAliases?: string[];
  lateral?: true;
}

export interface JoinPlan extends TableSource {
  kind: "inner" | "left" | "semi" | "anti";
  left: Expression;
  right: Expression;
  on?: Expression;
  full?: boolean;
  natural?: boolean;
}

export type SetOperator =
  "union" | "union all" | "intersect" | "intersect all" | "except" | "except all";

export interface RecursiveCte {
  reference: string;
  base: CompiledQuery;
  step: CompiledQuery;
  all: boolean;
}

export type PredicateOperator =
  | ComparisonOperator
  | `${ComparisonOperator} ANY`
  | `${ComparisonOperator} ALL`
  | "IN"
  | "NOT IN"
  | "IS NULL"
  | "IS NOT NULL"
  | "LIKE"
  | "NOT LIKE"
  | "ILIKE"
  | "NOT ILIKE"
  | "SIMILAR TO"
  | "NOT SIMILAR TO"
  | "IS DISTINCT FROM"
  | "IS NOT DISTINCT FROM"
  | "IS TRUE";

export interface Predicate {
  left: Expression;
  operator: PredicateOperator;
  right: Expression;
  escape?: string;
}

export interface CompiledQuery {
  sql: string;
  base: TableSource;
  joins: JoinPlan[];
  select: SelectItem[];
  predicates: Predicate[];
  groupBy: Expression[];
  having: Predicate[];
  orderBy: Array<{
    expression: Expression;
    direction: "asc" | "desc";
    nulls?: "first" | "last";
  }>;
  limit?: number;
  offset?: number;
  distinctWildcard?: boolean;
  limitParameter?: number;
  offsetParameter?: number;
  limitWithTies?: boolean;
  parameterCount?: number;
  usesStatementDatetime?: boolean;
  usesSequenceCalls?: boolean;
  usesVolatileFunctions?: boolean;
}

/** ORDER BY / LIMIT / OFFSET tail of a select or set operation. */
export interface SelectTail {
  orderBy: CompiledQuery["orderBy"];
  limit?: number;
  offset?: number;
  limitParameter?: number;
  offsetParameter?: number;
  limitWithTies?: boolean;
}
