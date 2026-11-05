/**
 * The boundary between the existing SQL front end and the new executor: what is lowered, by
 * whom, from which structure. Contract only; the planner-lowering track implements `Lowerer`.
 *
 * ## What the front end keeps doing (unchanged code, run before lowering)
 * 1. Parse: `compileStatement` / `compileQuery` (engine/query.ts) → `CompiledStatement` /
 *    `CompiledQuery` (plan/model.ts).
 * 2. Catalog rewrites, today `MinnowDatabase.#applyCatalogRewrites` (engine/database.ts):
 *    `foldIdentifierCase`, `expandRowReferences`, `expandViewSources`,
 *    `normalizePlanDomainLiterals`, `annotatePlanTimestampText`, `annotatePlanIntegerDivision`,
 *    `extendGroupByWithKeyDependents`, `bindPendingSelectShapes`, `mergeJoinColumns`,
 *    `expandSourceColumnAliases`, `expandNaturalJoins`, then `optimizePlan` (with the
 *    decorrelator) over `qualifyCorrelatedReferences`. These read catalog facts through the
 *    `CatalogView` below instead of the old store. Key forwarding is deleted with the old engine.
 * 3. Shape passes now in `createPreparedColumnarQuery` (engine/query.ts): `expandQualifiedWildcards`,
 *    `expandDistinctWildcard`, `unifyPlanGroupedReferences`, `validateGrouping`, and
 *    `expandFtsColumns`.
 *
 * ## What lowering replaces (deleted at cut-over)
 * - `subqueryResolutionSteps` + `#executeBlockCached`: nested blocks are no longer executed
 *   during preparation; derived tables become subtrees, uncorrelated scalar subqueries become
 *   `initPlans`, leftover correlated ones become `apply`.
 * - `#prepareBlockInputs` and `ColumnarTable`: base tables become `scan` / `keyLookup`.
 * - `chooseJoinOrder`: join order comes from the cost model over page-directory statistics.
 * - `bindPlan` / `prepareVectorQuery` (engine/vector.ts) and `executeRowQuery`: the IR is the
 *   one executor input. There is no row executor.
 * - `resolveStatementDatetimes`, `withTiesPlan`, `annotateAvgArgumentScales`,
 *   `inferResultColumnDomains`, result externalization: types and the statement clock are in
 *   the IR (`PhysicalPlan.clockSlot`, `sort.withTies`, decimal types carry scale).
 * - Prepared-plan templates (engine/prepared-plan.ts) and `bindPlanParameters`: a plan is lowered
 *   once per (statement text, catalog version) and executions fill parameter slots. A placeholder
 *   takes its type from its position at lowering time (PostgreSQL's parameter type inference);
 *   a value that does not fit the slot's type is a bind error, never a re-plan.
 *
 * ## Who
 * - The **planner-lowering track** owns `Lowerer` and the cost model; it may import the front-end
 *   modules above. The front end must also start carrying each column's declared SQL type name
 *   (types.ts `SqlTypeDescriptor.declaredTypeName`): today INTEGER/SMALLINT/BIGINT and
 *   REAL/DOUBLE collapse before lowering could see them.
 * - The **executor track** consumes `PhysicalPlan` and nothing from the front end.
 * - The **ownership track** calls the front end and `Lowerer` inside the engine owner, caches
 *   plans by (text, catalog version), and executes them.
 */
import type { CompiledStatement } from "../../engine/query.js";
import type { CompiledQuery } from "../../plan/model.js";
import type { PhysicalType } from "../types.js";
import type { PhysicalPlan, ScalarExpr } from "./ir.js";

export interface CatalogColumn {
  readonly columnId: number;
  readonly name: string;
  readonly type: PhysicalType;
  readonly nullable: boolean;
  /** Engine-generated (row addressing); never visible to SQL. */
  readonly hidden: boolean;
}

export interface CatalogIndex {
  readonly indexId: number;
  readonly columnIds: readonly number[];
  readonly unique: boolean;
  readonly primary: boolean;
}

/** Planning statistics, read from page directories and sketches (never from pages). */
export interface TableStatistics {
  readonly rowCount: number;
  /** Estimated distinct values per column id (sketch; planning only). */
  distinctCount(columnId: number): number | null;
  /** Fraction of rows a conjunct over one column (column index 0) keeps, from page min/max. */
  selectivity(columnId: number, conjunct: ScalarExpr): number | null;
}

export interface CatalogTable {
  readonly tableId: number;
  readonly name: string;
  readonly columns: readonly CatalogColumn[];
  readonly indexes: readonly CatalogIndex[];
  statistics(): TableStatistics;
}

/** The catalog as the front-end passes and lowering see it, at one catalog version. */
export interface CatalogView {
  readonly version: number;
  table(name: string): CatalogTable | undefined;
  /** View name → SQL text (for `expandViewSources`). */
  view(name: string): string | undefined;
}

export interface Lowerer {
  /** Lowers a SELECT that has been through front-end steps 1–3. */
  lowerQuery(plan: CompiledQuery, catalog: CatalogView): PhysicalPlan;
  /** Lowers INSERT/UPDATE/DELETE (rooted at `modify`); DDL never reaches the IR. */
  lowerStatement(statement: CompiledStatement, catalog: CatalogView): PhysicalPlan;
}
