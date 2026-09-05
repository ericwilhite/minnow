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
  | "COUNT"
  | "SUM"
  | "AVG"
  | "MIN"
  | "MAX"
  | "JSON_ARRAYAGG"
  | "STRING_AGG"
  /** Optimizer-only aggregate that enforces scalar-subquery cardinality. */
  | "MINNOW_SINGLE_VALUE";
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
  | "TO_JSON"
  | "IS_JSON"
  | "MINNOW_ARRAY_AT"
  | "MINNOW_ARRAY_FROM_JSON"
  | "MINNOW_ARRAY_ELEMENT"
  | "ARRAY"
  /** Parser-produced `->` JSON member/element access returning a JSON value. */
  | "MINNOW_JSON_GET"
  /** Parser-produced `->>` JSON member/element access returning text. */
  | "MINNOW_JSON_GET_TEXT"
  /** Optimizer-only, prefix-free equality key for hashable multi-column decorrelation. */
  | "MINNOW_TUPLE_KEY"
  /** Parser-produced wrapper carrying one explicit collation through ordering/comparison. */
  | "MINNOW_COLLATE"
  | "NEXTVAL"
  | "CURRVAL"
  | "RANDOM"
  | "GEN_RANDOM_UUID"
  /** Table-driven PostgreSQL string, math, datetime, regex, and formatting functions. */
  | "CONCAT"
  | "CONCAT_WS"
  | "LEFT"
  | "RIGHT"
  | "REVERSE"
  | "REPEAT"
  | "INITCAP"
  | "SPLIT_PART"
  | "STRPOS"
  | "STARTS_WITH"
  | "TRANSLATE"
  | "ASCII"
  | "CHR"
  | "BTRIM"
  | "MD5"
  | "REGEXP_SUBSTR"
  | "NUM_NONNULLS"
  | "NUM_NULLS"
  | "GCD"
  | "LCM"
  | "TO_HEX"
  | "QUOTE_LITERAL"
  | "QUOTE_IDENT"
  | "FORMAT"
  | "REGEXP_REPLACE"
  | "MINNOW_REGEX_MATCH"
  | "EXP"
  | "LN"
  | "LOG"
  | "LOG10"
  | "SIGN"
  | "TRUNC"
  | "PI"
  | "CBRT"
  | "DIV"
  | "WIDTH_BUCKET"
  | "SIN"
  | "COS"
  | "TAN"
  | "ASIN"
  | "ACOS"
  | "ATAN"
  | "ATAN2"
  | "DEGREES"
  | "RADIANS"
  | "TO_CHAR"
  | "TO_DATE"
  | "TO_TIMESTAMP"
  | "MAKE_DATE"
  | "MAKE_TIMESTAMP"
  | "AGE";

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
  | {
      kind: "literal";
      value: QueryValue;
      internalSqlValue?: true;
      sqlDomain?: SqlDomain;
      /**
       * A numeric constant's exact source digits, kept when the value survives the number
       * boundary but the spelling carries more (`1.000000000000000000000000` is the number 1
       * with display scale 24).
       * Constant folding reads these so seeded arithmetic runs in exact decimal space, the way
       * PostgreSQL types every decimal constant NUMERIC before evaluating it.
       */
      exactText?: string;
      /**
       * Written with a decimal point or exponent, or folded from such a constant: PostgreSQL
       * types it NUMERIC, so it is never an integer operand even when its value is whole
       * (`7 / 2.0` is 3.5 where `7 / 2` is 3).
       */
      decimal?: true;
    }
  /** A `?` or `$n` placeholder; `index` is 0-based. Replaced by a literal at bind time. */
  | { kind: "parameter"; index: number }
  | { kind: "column"; reference: string }
  /** `*`, or `alias.*` when `table` is set. */
  | { kind: "wildcard"; table?: string }
  | {
      kind: "binary";
      operator: BinaryOperator;
      left: Expression;
      right: Expression;
      /**
       * Integer division: both operands are integer-typed, so `/` truncates toward zero as
       * PostgreSQL's integer `/` does. Set by the compiler for constants and by catalog
       * binding for columns; absent, `/` is ordinary Float64 or exact-NUMERIC division.
       */
      integer?: true;
    }
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
      /**
       * AVG over a declared-scale NUMERIC column: the argument's declared scale, annotated by a
       * schema-aware pass before execution. PostgreSQL floors its quotient scale at the summed
       * operand's display scale; canonical NUMERIC encoding strips trailing zeros, so without
       * this the internal division cannot know the digits the declared scale will render.
       */
      avgArgumentScale?: number;
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

/**
 * A cross join rides the nested-loop inner-join path with a condition every row pair satisfies:
 * 1 = 1 over null literal key expressions. Producers build the shape with crossJoinPlan and
 * consumers recognize it with isCrossJoinPlan, so the encoding lives in exactly one place.
 */
export function crossJoinPlan(source: TableSource): JoinPlan {
  return {
    ...source,
    kind: "inner",
    left: { kind: "literal", value: null },
    right: { kind: "literal", value: null },
    on: {
      kind: "condition",
      operator: "=",
      left: { kind: "literal", value: 1 },
      right: { kind: "literal", value: 1 },
    },
  };
}

export function isCrossJoinPlan(join: JoinPlan): boolean {
  const condition = join.on;
  return (
    join.kind === "inner" &&
    condition?.kind === "condition" &&
    condition.operator === "=" &&
    condition.left.kind === "literal" &&
    condition.left.value === 1 &&
    condition.right.kind === "literal" &&
    condition.right.value === 1
  );
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
  /**
   * A SELECT whose output width depends on an input wildcard. The parser preserves the raw
   * shape until source schemas are available; the schema-binding pass then expands the
   * wildcard and runs the ordinary SELECT lowering exactly once.
   */
  pendingSelectShape?: {
    distinct: boolean;
    groupingSets?: Expression[][];
  };
  /** Positional output names whose count cannot be checked until a wildcard is expanded. */
  pendingOutputAliases?: {
    columns: string[];
    sourceName: string;
  };
  /** A compound tail whose ORDER BY ordinals depend on its first member's wildcard width. */
  pendingSetOrder?: true;
  /** Preserve `{ optimize: false }` after a deferred wildcard shape becomes concrete. */
  preserveUnoptimizedShape?: true;
  /**
   * Set by the optimizer on the plan it returns. Parameter binding applies the value-dependent
   * rewrites (calendar equalities) only to optimized plans, so `{ optimize: false }` stays the
   * untouched oracle for them.
   */
  optimized?: true;
  distinctWildcard?: boolean;
  limitParameter?: number;
  offsetParameter?: number;
  /** Optimizer-relocated LIMIT placeholders that still need bind-time numeric/range validation. */
  limitValidationParameters?: number[];
  /** Optimizer-relocated OFFSET placeholders that still need bind-time numeric/range validation. */
  offsetValidationParameters?: number[];
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
