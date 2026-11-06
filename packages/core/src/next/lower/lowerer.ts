/**
 * Lowering: the bridge from the existing SQL front end to the new core's typed IR (ir/ir.ts).
 * Owner: planner-lowering track (B6). Consumers: the owner (B5) calls it; the executor (B4) runs
 * what it returns and compiles `lowerTableRules`' output; nothing else.
 *
 * ## One call per statement text
 * `lower(text, context)` runs the whole front end and returns a `LoweredStatement`. Steps, all
 * synchronous, all over `context.catalog` (access/catalog.ts, the only catalog contract, R1):
 * 1. **Parse** under `compileWithDeclaredTypes` (engine/query.ts): `compileStatement`. The
 *    parser then records each numeric column's and CAST target's declared width (C5).
 * 2. **Catalog passes** (lower/frontend.ts), in `MinnowDatabase.#applyCatalogRewrites`' order,
 *    each catalog fact read from `CatalogView`: `foldIdentifierCase`, `expandRowReferences`,
 *    `expandViewSources` (view bodies parsed under the same scope),
 *    `extendGroupByWithKeyDependents`, `bindPendingSelectShapes`, `mergeJoinColumns`,
 *    `expandSourceColumnAliases`, `expandNaturalJoins`, then `optimizePlan` (with the
 *    decorrelator) over `qualifyCorrelatedReferences`. Not run: key forwarding (gone),
 *    domain-literal normalization and the timestamp-text / integer-division annotations (types
 *    decide them), sequence-name resolution (lowering reads `CatalogView.sequences`).
 * 3. **Shape passes**: `prepareColumnarShape` (engine/query.ts), the passes
 *    `createPreparedColumnarQuery` runs: wildcards, column aliases, grouped references,
 *    `validateGrouping`.
 * 4. **Typing.** Every expression gets its `PhysicalType` by PostgreSQL's rules: columns from the
 *    catalog, integer literals int4 → int8 → numeric by magnitude, placeholders from their
 *    position (or `context.parameterTypes`), operator and function overloads resolved against
 *    the executor's kernel signatures, implicit casts made explicit (`cast` nodes).
 * 5. **IR construction** (INVENTORY.md maps each front-end construct to its operator), join
 *    ordering by the cost model (lower/from.ts over lower/statistics.ts), access paths
 *    (lower/access-path.ts), parameter slots, init plans, mark joins, `apply`.
 * 6. **Descriptors**: result columns (lower/result.ts), read and write sets, statistics basis.
 *
 * The front end's semantics are kept, not re-derived: where it rewrote SQL (decorrelation, view
 * expansion, NATURAL/USING, GROUP BY key dependents) lowering receives the rewritten form and
 * never undoes it. What the front end does at execution today (subquery resolution during
 * preparation, `bindPlan`, `chooseJoinOrder`, prepared templates, `resolveStatementDatetimes`,
 * `withTiesPlan`, `annotateAvgArgumentScales`, `inferResultColumnDomains`) is replaced by the IR
 * and is not called.
 *
 * ## Contract
 * - **Pure and synchronous.** No I/O, no awaits, no clock, no randomness: equal inputs give equal
 *   plans (structurally). Runs on the owner's thread; a typical OLTP statement lowers in well
 *   under a millisecond, and the owner caches the result.
 * - **Caching** is the owner's: by (text, catalog version), plus `statisticsDrifted(basis)` on
 *   each hit. A plan never depends on parameter values, the clock or session state.
 * - **Memory.** Lowering allocates plain objects proportional to the statement; it holds no
 *   governor grant (plans are small and bounded by the statement text). The cache that holds
 *   plans is the owner's to bound.
 * - **Errors.** `lower` throws only `PlanningError` (a SQL error with PostgreSQL's SQLSTATE:
 *   42601 syntax, 42P01 relation, 42703 column, 42883 function, 42704 type, …) and
 *   `UnsupportedPlanError` (0A000, a construct the new core does not translate yet). Front-end
 *   failures are translated (errors.ts `frontEndError`): the original is the `cause`, and a parse
 *   error keeps its `position`. Neither leaves state behind.
 * - **Cancellation.** None: lowering is bounded and synchronous.
 */
import type { CatalogChange, CatalogView, TableDef } from "../access/catalog.js";
import type { CreateTableInput } from "../../engine/catalog-mutations.js";
import type { ExecutorCosts } from "../ir/costs.js";
import type { PhysicalPlan } from "../ir/ir.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import type { PhysicalType } from "../types.js";
import type { ResultColumn } from "./result.js";
import type { StatementKind } from "./statement.js";
import type { PlanningStatistics, StatisticsBasis } from "./statistics.js";

export interface LoweringContext {
  readonly catalog: CatalogView;
  readonly statistics: PlanningStatistics;
  readonly costs: ExecutorCosts;
  /**
   * Placeholder types fixed by the client (a typed prepare), `$1` first; a null or missing entry
   * is inferred from the placeholder's position, as PostgreSQL infers it. A placeholder whose
   * type cannot be inferred (`SELECT $1`) is `text`, as in PostgreSQL.
   */
  readonly parameterTypes?: ReadonlyArray<PhysicalType | null>;
}

/** Strongest SELECT row-lock intent after view/CTE expansion; no physical lock is acquired. */
export type LoweredRowLocking = "none" | "keyShare" | "share" | "noKeyUpdate" | "update";

interface LoweredBase {
  /** B5 refuses a query with any non-none intent in READ ONLY (25006), before executing it. */
  readonly rowLocking: LoweredRowLocking;
  readonly kind: StatementKind;
  readonly text: string;
  /** `context.catalog.version`: the plan is valid only at this version. */
  readonly catalogVersion: number;
  /** PostgreSQL's command tag (lower/statement.ts). */
  readonly command: string;
  /** See `StatementClass.controlsTransaction`. */
  readonly controlsTransaction: boolean;
  /** Types of the `$n` placeholders, `$1` first (for plans: `plan.parameters` up to
   * `plan.placeholderCount`). Empty for statements without placeholders. */
  readonly parameters: readonly PhysicalType[];
}

interface LoweredPlanned extends LoweredBase {
  readonly plan: PhysicalPlan;
  /** Result columns, aligned with `plan.columns` (RETURNING for DML; empty without it). */
  readonly columns: readonly ResultColumn[];
  /** Every table id the plan reads (scans, lookups, fetches, through views and subqueries). */
  readonly reads: readonly number[];
  /** Row counts the plan was costed with (lower/statistics.ts). */
  readonly basis: readonly StatisticsBasis[];
}

export interface LoweredQuery extends LoweredPlanned {
  readonly kind: "query";
  /**
   * The plan reads at most a few rows by key: its only data access is `keyLookup` with constant
   * or parameter keys (no scan, join, aggregate or sort; no sequence call). The owner tries
   * `runNow` for these (ruling R6).
   */
  readonly pointRead: boolean;
}

export interface LoweredModify extends LoweredPlanned {
  readonly kind: "modify";
  /** Tables the statement writes directly (the root `modify`); FK actions are the write path's. */
  readonly writes: readonly number[];
}

export interface LoweredDdl extends LoweredBase {
  readonly kind: "ddl";
  /**
   * The catalog change for `Statement.changeCatalog` (access/modify.ts), complete: column types
   * from declared names (lower/columns.ts), defaults, CHECKs and generated columns as the front
   * end's normalized SQL text, constraints by column position. Null when the statement is a no-op
   * at this catalog version (IF NOT EXISTS on an existing object, IF EXISTS on a missing one).
   */
  readonly change: CatalogChange | null;
  /**
   * SQL to lower and execute after `change`, in the same statement and transaction, at the new
   * catalog version: CREATE TABLE AS becomes the table, then `INSERT INTO t (…) <query>`. Null
   * for every other DDL.
   */
  readonly followUp: string | null;
}

export interface LoweredTransactionControl extends LoweredBase {
  readonly kind: "transactionControl";
  /**
   * `setTransaction` is SET TRANSACTION: it changes the current transaction's `modes` (the owner
   * applies them; PostgreSQL refuses a change of access mode or isolation after the
   * transaction's first query, 25001, and a SET TRANSACTION outside a transaction only warns).
   */
  readonly action:
    "begin" | "commit" | "rollback" | "savepoint" | "rollbackTo" | "release" | "setTransaction";
  /** The savepoint name (normalized identifier), else null. */
  readonly savepoint: string | null;
  /**
   * The transaction modes BEGIN / START TRANSACTION or SET TRANSACTION name, after PostgreSQL's
   * rule that the last of each kind applies; null where the statement names none (the current
   * or default mode stays). All null for the other actions.
   */
  readonly modes: TransactionModes;
  /**
   * `modes.readOnly === true` on BEGIN / START TRANSACTION and SET TRANSACTION: the owner pins a
   * snapshot and takes no writer slot (ruling R11.2). False otherwise, READ WRITE included.
   */
  readonly readOnly: boolean;
}

/** Transaction modes as they apply (see `LoweredTransactionControl.modes`). */
export interface TransactionModes {
  readonly readOnly: boolean | null;
  /** Every level the engine accepts is met by its one snapshot level; SERIALIZABLE is refused. */
  readonly isolation: "read uncommitted" | "read committed" | "repeatable read" | null;
  readonly deferrable: boolean | null;
}

export interface LoweredUtility extends LoweredBase {
  readonly kind: "utility";
  /**
   * SHOW's one-row answer (PostgreSQL's setting name and its value as text), or null for SET and
   * RESET, which are accepted and ignored (UTC is the only time zone).
   */
  readonly show: { readonly column: ResultColumn; readonly value: string } | null;
}

export type LoweredStatement =
  LoweredQuery | LoweredModify | LoweredDdl | LoweredTransactionControl | LoweredUtility;

/**
 * A batch write from the client protocol (owner/protocol.ts `WriteRequest`): rows supplied as
 * columns, no SQL text. `names` are the table columns the client supplies, in input order.
 */
export interface WriteShape {
  readonly table: string;
  readonly op: "insert" | "upsert" | "update" | "delete";
  readonly names: readonly string[];
}

export interface Lowerer {
  /** Lowers one SQL statement (see the module comment). */
  lower(text: string, context: LoweringContext): LoweredStatement;
  /**
   * A batch write as a `modify` plan whose input is the client's rows (IR request 3 in
   * INVENTORY.md, an `input` relation typed as the named columns): insert fills omitted columns
   * with their defaults and computes generated columns; upsert conflicts on the primary key and
   * updates the supplied columns; update and delete find rows by primary key, which `names` must
   * include. Cached by the owner per (shape, catalog version).
   */
  lowerWrite(shape: WriteShape, context: LoweringContext): LoweredModify;
  /**
   * The table's CHECK conditions, generated columns and defaults (`nextval` for SERIAL and
   * IDENTITY) lowered from the catalog's SQL text, for `compileTableRules(table, catalog.version,
   * expressions)` (exec/rules.ts, ruling R14.3). The owner caches the result per (table id,
   * catalog version) with the compiled rules. Throws `PlanningError` if stored text no longer
   * types against the table (a catalog bug: DDL validated it).
   */
  lowerTableRules(table: TableDef, catalog: CatalogView): TableRuleExpressions;
  /**
   * A table declared through the schema DSL (`CreateTableInput`, engine/catalog-mutations.ts),
   * as the `createTable` change SQL's CREATE TABLE would give (additive, Phase 2). The DSL names
   * no SQL types: `column.integer()` is INTEGER and `column.number()` DOUBLE PRECISION (R17.3).
   */
  lowerSchemaTable(input: CreateTableInput, catalog: CatalogView): CatalogChange;
}
