/**
 * The typed operator IR the new executor consumes. The planner-lowering track produces it from
 * the front end's optimized `CompiledQuery` (lower/lowerer.ts); the executor track runs it. Nothing
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
  /**
   * A cast to `type`. Explicit casts (`CAST`, `::`) follow PostgreSQL's explicit rules: a text
   * value longer than a `text` type's `maxLength` is truncated. `assignment` (additive, R17.4)
   * marks PostgreSQL's assignment cast, which lowering puts on every value written to a column
   * (INSERT values, UPDATE SET, ON CONFLICT and MERGE assignments, defaults, generated columns):
   * there a value longer than `maxLength` is SQLSTATE 22001 (string_data_right_truncation), and
   * numeric range and precision errors are raised as for an explicit cast.
   */
  | {
      readonly op: "cast";
      readonly input: ScalarExpr;
      readonly type: PhysicalType;
      readonly assignment?: true;
    }
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

/**
 * `mark` (a decorrelated nullable IN / ANY): the output is the left (probe) columns plus one
 * `bool` column, last: TRUE when some right row matches the keys and the residual, else NULL when
 * a comparison was unknown (a null left key with a non-empty right side, or a null right key),
 * else FALSE; an empty right side gives FALSE. One output row per left row (additive, R17.5).
 */
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
   * Executed through the `RowFetcher` contract (exec/fetch.ts): dense output in active-row order,
   * caller-owned buffers, partial completion on a full or denied varlen builder.
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
      /**
       * One tuple of key expressions (constants or params) per probe, each already of the key
       * column's exact type (lowering casts or drops a constant that does not fit).
       */
      readonly keys: ReadonlyArray<readonly ScalarExpr[]>;
      readonly columns: readonly number[];
      /** Also output a row locator (type `locator`, last), as `scan.emitLocator` (additive). */
      readonly emitLocator?: boolean;
    })
  /**
   * Rows of an `ordered` index between two bounds, in key order (additive, R17.5). `lower` and
   * `upper` are key prefixes (constants or params of the key columns' exact types, as for
   * `keyLookup`), `inclusive` per bound; null is unbounded. Equality on a prefix is
   * `lower = upper`, both inclusive. Lowering uses it only for an `ordered` index and comparisons
   * under the default byte-order collation (C8). A null bound value (a NULL parameter) selects
   * nothing. `descending` returns the rows in reverse key order (for ORDER BY … DESC LIMIT).
   */
  | (RelBase & {
      readonly op: "indexRange";
      readonly tableId: number;
      readonly indexId: number;
      readonly lower: { readonly keys: readonly ScalarExpr[]; readonly inclusive: boolean } | null;
      readonly upper: { readonly keys: readonly ScalarExpr[]; readonly inclusive: boolean } | null;
      readonly descending: boolean;
      readonly columns: readonly number[];
      readonly emitLocator: boolean;
    })
  /**
   * The rows of a client batch write, supplied with the execution rather than read from storage
   * (additive, R17.5): its `output` columns, in order, are the client's columns already converted
   * to these types by the executor's bind (exec/ `ExecuteRequest`, B4 Phase 2). At most one per
   * plan. Lowering builds batch writes (`Lowerer.lowerWrite`) on it, so defaults, generated
   * columns, casts and RETURNING are the same plan as SQL INSERT.
   */
  | (RelBase & { readonly op: "input" })
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
  /**
   * What `modify`'s expressions read (additive wording, R17.5). "Table row" means every column of
   * the table in catalog order (access/catalog.ts `TableDef.columns`), complete.
   * - `onConflict.assignments` and `onConflict.where`: the existing (conflicting) table row, then
   *   the proposed row (PostgreSQL's EXCLUDED), so column `i` is existing and `n + i` proposed.
   * - `returning`: the affected table row (new values for insert, update and upsert; old values
   *   for delete), then the input's passthrough columns (`passthrough`, below).
   * - `merge` actions: see `MergeAction`.
   */
  | (RelBase & {
      readonly op: "modify";
      readonly kind: "insert" | "update" | "delete" | "upsert" | "merge";
      readonly tableId: number;
      /**
       * Rows to write (insert/upsert): every table column in catalog order, defaults and
       * generated columns filled, each of exactly its column's type. Update: the row locator,
       * then one column per `assigned` column, in that order. Delete: the row locator. Merge: see
       * `merge`. For update, delete and merge, the last `passthrough` input columns follow (values
       * RETURNING reads that are not table columns, such as UPDATE … FROM's source columns).
       */
      readonly input: Rel;
      /** Trailing input columns that only RETURNING reads (additive; absent is 0). */
      readonly passthrough?: number;
      /**
       * UPDATE: the catalog column ids the statement assigns, in input order after the locator
       * (access/ `TableWriteOptions.assigned`). Empty for insert, upsert and delete.
       */
      readonly assigned: readonly number[];
      /**
       * A second update or delete of the same row in this statement: `"error"` raises PostgreSQL's
       * cardinality violation, SQLSTATE 21000 (MERGE, ON CONFLICT DO UPDATE); `"first"` keeps the
       * first and skips the repeat (UPDATE … FROM, DELETE … USING, as PostgreSQL does).
       * (access/ `TableWriteOptions.onRepeat`.)
       */
      readonly onRepeat: "error" | "first";
      readonly onConflict: {
        /**
         * The unique indexes a proposed row conflicts on, tried in order (access/
         * `TableWriter.conflicts`); empty means every unique index (a bare ON CONFLICT DO
         * NOTHING). Additive (R17.5): `indexId` is kept and equals `arbiters[0]`, or −1 when
         * `arbiters` is empty.
         */
        readonly arbiters: readonly number[];
        readonly indexId: number;
        readonly action: "nothing" | "update";
        readonly assignments: ReadonlyArray<{ readonly column: number; readonly expr: ScalarExpr }>;
        readonly where: ScalarExpr | null;
      } | null;
      readonly returning: readonly ScalarExpr[] | null;
      /** Present iff `kind` is `"merge"` (additive, R17.5). */
      readonly merge?: MergeSpec;
    });

/**
 * MERGE (additive, R17.5). The modify input is the source LEFT JOIN target on the match
 * condition: the source columns, then the target's row locator (null when no target row
 * matched), then the target row's columns in catalog order (null-extended), then passthrough
 * columns. For each input row the first action whose `when` matches (a locator present for
 * `matched`) and whose `condition` is true (null `condition` is true) runs; a row no action
 * takes is skipped. A target row matched by two source rows is SQLSTATE 21000 (`onRepeat:
 * "error"`). Conditions and values read the input columns.
 */
export interface MergeSpec {
  /** Number of leading source columns; the locator is input column `sourceColumns`. */
  readonly sourceColumns: number;
  readonly actions: readonly MergeAction[];
}

export type MergeAction =
  | {
      readonly when: "matched";
      readonly condition: ScalarExpr | null;
      /** Update: assigned table column ids and their values (assignment-cast to the columns). */
      readonly action:
        | {
            readonly kind: "update";
            readonly assigned: readonly number[];
            readonly values: readonly ScalarExpr[];
          }
        | { readonly kind: "delete" }
        | { readonly kind: "nothing" };
    }
  | {
      readonly when: "notMatched";
      readonly condition: ScalarExpr | null;
      /** Insert: one value per table column in catalog order (defaults filled), or nothing. */
      readonly action:
        | { readonly kind: "insert"; readonly values: readonly ScalarExpr[] }
        | { readonly kind: "nothing" };
    };

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
