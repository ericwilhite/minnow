/**
 * The typed operator IR the new executor consumes. The planner-lowering track produces it from
 * the front end's optimized `CompiledQuery` (lowering.ts); the executor track runs it. Nothing
 * else creates or interprets it.
 *
 * ## Invariants
 * - **Fully typed.** Every scalar expression carries its result `PhysicalType`; every relational
 *   operator lists its output columns. Function and operator overloads are resolved by lowering:
 *   the executor looks up a kernel by (`fn`, argument types) and never infers a type from a value.
 * - **Positional.** Expressions reference input columns by index into the input operator's
 *   `output`. There are no names below `PhysicalPlan.columns`.
 * - **Plain data.** No functions, classes or cycles: a plan can be cached per (statement text,
 *   catalog version), compared structurally, and structured-cloned to a helper.
 * - **Bound once.** Placeholders and the statement clock are parameter slots (`param`), filled per
 *   execution. Uncorrelated scalar subqueries are `initPlans` that fill slots before the main plan
 *   runs. A plan never changes shape with parameter values.
 * - **Semantics stay in the front end's laws.** SQL NULL, three-valued logic, PostgreSQL overflow
 *   errors and rounding are the kernels' job; the IR only names them. Where the front end
 *   rewrote SQL (decorrelation, view expansion, NATURAL/USING), the IR receives the rewritten
 *   form and must not undo it.
 */
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";

/**
 * A collation name as written (`"C"`, `"default"`, an ICU locale); null is the default, which is
 * byte (code point) order, PostgreSQL's C collation (DECISIONS.md C8). An ICU collation orders by
 * query-wide ranks from `Intl.Collator` when the distinct values fit the grant, else by an
 * external comparison sort; ties are broken by bytes, so peers and equality stay byte equality.
 */
export type Collation = string | null;

export type ScalarExpr =
  | { readonly op: "column"; readonly index: number; readonly type: PhysicalType }
  /** A constant: `value` is a flat vector of length 1 (vector.ts), null via its validity. */
  | { readonly op: "literal"; readonly value: FlatVector; readonly type: PhysicalType }
  | { readonly op: "param"; readonly slot: number; readonly type: PhysicalType }
  /**
   * A resolved function or operator: `fn` names the kernel family (`"add"`, `"upper"`,
   * `"date_trunc"`, `"nextval"`…), `args` are already cast to the kernel's parameter types.
   */
  | {
      readonly op: "call";
      readonly fn: string;
      readonly args: readonly ScalarExpr[];
      readonly type: PhysicalType;
    }
  | { readonly op: "cast"; readonly input: ScalarExpr; readonly type: PhysicalType }
  | {
      readonly op: "compare";
      readonly cmp: "=" | "<>" | "<" | "<=" | ">" | ">=";
      readonly left: ScalarExpr;
      readonly right: ScalarExpr;
      readonly collation: Collation;
      readonly type: PhysicalType;
    }
  | {
      readonly op: "distinctFrom";
      readonly negated: boolean;
      readonly left: ScalarExpr;
      readonly right: ScalarExpr;
      readonly type: PhysicalType;
    }
  | {
      readonly op: "and" | "or";
      readonly args: readonly ScalarExpr[];
      readonly type: PhysicalType;
    }
  | { readonly op: "not"; readonly arg: ScalarExpr; readonly type: PhysicalType }
  | {
      readonly op: "isNull";
      readonly negated: boolean;
      readonly arg: ScalarExpr;
      readonly type: PhysicalType;
    }
  | {
      readonly op: "case";
      readonly branches: ReadonlyArray<{ readonly when: ScalarExpr; readonly then: ScalarExpr }>;
      readonly otherwise: ScalarExpr | null;
      readonly type: PhysicalType;
    }
  /** `arg [NOT] IN (list)` with SQL's three-valued result. */
  | {
      readonly op: "inList";
      readonly negated: boolean;
      readonly arg: ScalarExpr;
      readonly list: readonly ScalarExpr[];
      readonly collation: Collation;
      readonly type: PhysicalType;
    }
  | {
      readonly op: "like";
      readonly mode: "like" | "ilike" | "similar";
      readonly negated: boolean;
      readonly arg: ScalarExpr;
      readonly pattern: ScalarExpr;
      readonly escape: string | null;
      readonly type: PhysicalType;
    };

/** LIMIT/OFFSET: a constant or a parameter slot. */
export type Bound =
  | { readonly kind: "const"; readonly value: number }
  | { readonly kind: "param"; readonly slot: number };

export interface SortKey {
  readonly expr: ScalarExpr;
  readonly direction: "asc" | "desc";
  readonly nulls: "first" | "last";
  readonly collation: Collation;
}

export interface OutputColumn {
  readonly name: string;
  readonly type: PhysicalType;
  readonly nullable: boolean;
}

export type AggregateFn =
  | "count_star"
  | "count"
  | "sum"
  | "avg"
  | "min"
  | "max"
  | "bool_and"
  | "bool_or"
  | "string_agg"
  | "array_agg"
  | "json_agg"
  | "percentile_cont"
  | "percentile_disc"
  | "mode"
  /** Enforces scalar-subquery cardinality (front end's MINNOW_SINGLE_VALUE). */
  | "single_value";

export interface AggregateCall {
  readonly fn: AggregateFn;
  readonly args: readonly ScalarExpr[];
  readonly distinct: boolean;
  readonly filter: ScalarExpr | null;
  readonly orderBy: readonly SortKey[];
  readonly type: PhysicalType;
}

export interface WindowFrameBound {
  readonly kind:
    "unbounded-preceding" | "preceding" | "current-row" | "following" | "unbounded-following";
  readonly offset: ScalarExpr | null;
}

export interface WindowCall {
  /** A ranking/navigation function or an `AggregateFn`. */
  readonly fn: string;
  readonly args: readonly ScalarExpr[];
  readonly frame: {
    readonly unit: "rows" | "range" | "groups";
    readonly start: WindowFrameBound;
    readonly end: WindowFrameBound;
    readonly exclude: "no-others" | "current-row" | "group" | "ties";
  } | null;
  readonly type: PhysicalType;
}

/** A Bloom/min-max filter one join's build side publishes into a probe-side scan. */
export interface RuntimeFilter {
  readonly id: number;
  /** Scan output column it filters. */
  readonly column: number;
}

export type JoinKind = "inner" | "left" | "right" | "full" | "semi" | "anti" | "mark" | "cross";

interface RelBase {
  /** Unique within the plan; operator diagnostics and runtime filters refer to it. */
  readonly id: number;
  readonly output: readonly OutputColumn[];
}

export type Rel =
  | (RelBase & {
      readonly op: "scan";
      readonly tableId: number;
      /** Catalog column ids to read, in output order. */
      readonly columns: readonly number[];
      /** Conjuncts over this scan's output, evaluated first (late materialization, pruning). */
      readonly filter: ScalarExpr | null;
      readonly runtimeFilters: readonly RuntimeFilter[];
      /**
       * Late materialization: also output a row-locator column (type `locator`, last), so a
       * later `fetch` can read the columns this scan deferred.
       */
      readonly emitLocator: boolean;
    })
  /**
   * Late materialization (vector/locator.ts): appends `columns` of `tableId`, read through the
   * locator column `locator` of the input, to the input's columns. A null locator (an outer
   * join's null-extended row) yields nulls. Batched: one fetch reads each needed page once.
   */
  | (RelBase & {
      readonly op: "fetch";
      readonly input: Rel;
      readonly tableId: number;
      readonly locator: number;
      readonly columns: readonly number[];
    })
  | (RelBase & {
      readonly op: "keyLookup";
      readonly tableId: number;
      /** The key index, or a secondary index id. */
      readonly indexId: number;
      /** One tuple of key expressions (constants or params) per probe. */
      readonly keys: ReadonlyArray<readonly ScalarExpr[]>;
      readonly columns: readonly number[];
    })
  | (RelBase & { readonly op: "values"; readonly rows: ReadonlyArray<readonly ScalarExpr[]> })
  | (RelBase & { readonly op: "filter"; readonly input: Rel; readonly predicate: ScalarExpr })
  | (RelBase & {
      readonly op: "project";
      readonly input: Rel;
      readonly exprs: readonly ScalarExpr[];
    })
  | (RelBase & {
      readonly op: "join";
      readonly kind: JoinKind;
      readonly algorithm: "hash" | "nestedLoop";
      /** Probe side. */
      readonly left: Rel;
      /** Build side. */
      readonly right: Rel;
      readonly leftKeys: readonly ScalarExpr[];
      readonly rightKeys: readonly ScalarExpr[];
      /** Per key: `IS NOT DISTINCT FROM` instead of `=`. */
      readonly nullSafe: readonly boolean[];
      /** Remaining ON conditions, over left output ++ right output, after the key match. */
      readonly residual: ScalarExpr | null;
      /** Filters this join's build side publishes into scans under `left`. */
      readonly publishes: readonly RuntimeFilter[];
    })
  | (RelBase & {
      readonly op: "aggregate";
      readonly input: Rel;
      /** Group keys as input column indexes (lowering projects expressions first). */
      readonly groupBy: readonly number[];
      /** GROUPING SETS as subsets of `groupBy` positions; null is one set of all keys. */
      readonly groupingSets: ReadonlyArray<readonly number[]> | null;
      readonly aggregates: readonly AggregateCall[];
    })
  | (RelBase & {
      readonly op: "window";
      readonly input: Rel;
      readonly partitionBy: readonly number[];
      readonly orderBy: readonly SortKey[];
      readonly functions: readonly WindowCall[];
    })
  | (RelBase & {
      readonly op: "sort";
      readonly input: Rel;
      readonly keys: readonly SortKey[];
      /** Set for top-N; `withTies` extends the last group of peers. */
      readonly limit: Bound | null;
      readonly offset: Bound | null;
      readonly withTies: boolean;
    })
  | (RelBase & {
      readonly op: "limit";
      readonly input: Rel;
      readonly limit: Bound | null;
      readonly offset: Bound | null;
    })
  | (RelBase & { readonly op: "distinct"; readonly input: Rel })
  | (RelBase & {
      readonly op: "setOp";
      readonly kind: "union" | "intersect" | "except";
      readonly all: boolean;
      readonly inputs: readonly Rel[];
    })
  | (RelBase & {
      readonly op: "recursive";
      readonly workTable: number;
      readonly all: boolean;
      readonly base: Rel;
      readonly step: Rel;
    })
  | (RelBase & { readonly op: "workTableScan"; readonly workTable: number })
  /**
   * A correlated subquery the decorrelator left in place. For each outer row, `bindings` write
   * outer expressions into parameter slots and `inner` runs. Output: the outer columns plus one:
   * - `scalar`: inner's single column; null when inner is empty; more than one row is the
   *   cardinality error (PostgreSQL's "more than one row returned by a subquery").
   * - `exists`: true iff inner has a row (never null). NOT EXISTS is `not` over it.
   * - `quantified`: `test cmp ANY|ALL (inner)` with SQL's three-valued result. ANY: true if some
   *   comparison is true; else null if some comparison is null (a null test value with a non-empty
   *   inner, or a null inner value); else false; an empty inner gives false. ALL: false if some
   *   comparison is false; else null if some is null; else true; an empty inner gives true.
   *   `x IN (q)` is `= ANY`; `x NOT IN (q)` is `<> ALL`. Comparison uses `collation`.
   * - `memoize`: lowering sets it only when the inner subplan has no volatile function, sequence
   *   call or other observable side effect; then inner runs once per distinct binding tuple
   *   (bindings compared by equality keys, null-safe). Otherwise inner runs once per outer row, in
   *   outer order: how many times a volatile expression is evaluated is observable.
   */
  | (RelBase & {
      readonly op: "apply";
      readonly mode: "scalar" | "exists" | "quantified";
      readonly outer: Rel;
      readonly inner: Rel;
      readonly bindings: ReadonlyArray<{ readonly slot: number; readonly expr: ScalarExpr }>;
      readonly quantified: {
        readonly test: ScalarExpr;
        readonly cmp: "=" | "<>" | "<" | "<=" | ">" | ">=";
        readonly quantifier: "any" | "all";
        readonly collation: Collation;
      } | null;
      readonly memoize: boolean;
    })
  | (RelBase & {
      readonly op: "modify";
      readonly kind: "insert" | "update" | "delete" | "upsert";
      readonly tableId: number;
      /** Rows to write (insert/upsert) or row ids plus new values (update/delete). */
      readonly input: Rel;
      readonly onConflict: {
        readonly indexId: number;
        readonly action: "nothing" | "update";
        readonly assignments: ReadonlyArray<{ readonly column: number; readonly expr: ScalarExpr }>;
        readonly where: ScalarExpr | null;
      } | null;
      readonly returning: readonly ScalarExpr[] | null;
    });

export interface PhysicalPlan {
  readonly root: Rel;
  /** Visible output: names and types, in order. */
  readonly columns: readonly OutputColumn[];
  /** Type of every parameter slot: user placeholders first, then lowering's own slots. */
  readonly parameters: readonly PhysicalType[];
  /** How many leading slots are the statement's `$n` placeholders. */
  readonly placeholderCount: number;
  /** Slot of the statement clock (CURRENT_TIMESTAMP and friends), or null. */
  readonly clockSlot: number | null;
  /** Uncorrelated scalar subqueries: run in order, each fills one slot. */
  readonly initPlans: ReadonlyArray<{ readonly slot: number; readonly plan: Rel }>;
  /** Catalog version the plan was lowered against; a mismatch means re-lower. */
  readonly catalogVersion: number;
  /**
   * Volatile functions or sequence calls anywhere in the plan: never cached by result, never
   * reordered, and every `apply` over a volatile inner has `memoize: false`.
   */
  readonly volatile: boolean;
}
