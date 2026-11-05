/**
 * What the cost model reads about the data, and when a cached plan is stale because the data
 * moved.
 *
 * ## Source and rules
 * - The owner implements `PlanningStatistics` over the write path's published table summaries
 *   (row counts, per-column null counts, bounds and distinct sketches merged at flush and
 *   compaction; design-joins-agg-spill.md "Statistics and ordering") plus memtable counts.
 *   It is synchronous and in memory: planning never reads a page and never awaits
 *   (owner/engine.ts: planning is synchronous).
 * - Everything here is approximate and for planning only. It never authorizes pruning, a range
 *   or overflow shortcut, or a uniqueness assumption: those come from exact page bounds at run
 *   time and from the catalog's constraints.
 * - A table or column with no statistics (`null`) plans with fixed defaults (1,000 rows; a third
 *   of rows kept per range conjunct, PostgreSQL's DEFAULT_INEQ_SEL; 1/200 per equality).
 *
 * ## Join ordering (design-joins-agg-spill.md "Statistics and ordering")
 * Inner joins connected by equi-join predicates are ordered by the cost formula in ir/costs.ts,
 * with output estimated as `L × R / max(NDV_L, NDV_R)` (corrected for NULLs and proven
 * uniqueness). Up to `EXHAUSTIVE_JOIN_LIMIT` relations every left-deep connected order is costed;
 * above it, greedy connected expansion from the cheapest start. Outer, semi, anti, lateral and
 * volatile joins are barriers: their relative order is the front end's.
 *
 * ## Re-planning
 * A plan is cached per (statement text, catalog version) by the owner. It also records the row
 * counts it was costed with (`StatisticsBasis`); the owner re-lowers when `statisticsDrifted`
 * says so, checked on each cache hit (one lookup per table the plan reads).
 */
import type { StatValue } from "../types.js";

/** Every left-deep order is costed up to this many joined relations; greedy above. */
export const EXHAUSTIVE_JOIN_LIMIT = 8;

/** A table's row count may change by this factor before a plan costed on it is re-lowered. */
export const REPLAN_GROWTH_FACTOR = 2;
/** Row counts at or below this never trigger a re-plan (plan choice does not depend on them). */
export const REPLAN_MIN_ROWS = 1_000;

export interface ColumnPlanningStatistics {
  readonly nullCount: number;
  /** Estimated distinct non-null values (sketch), or null when unknown. */
  readonly distinct: number | null;
  /** Bounds in the type's stored encoding (types.ts `StatValue`), or null when unknown. */
  readonly min: StatValue | null;
  readonly max: StatValue | null;
  /** Average decoded bytes per non-null value (8 for int64, UTF-8 length for text…). */
  readonly averageWidth: number;
}

export interface TablePlanningStatistics {
  /** Live rows, approximately (published groups plus memtables, minus deletions). */
  readonly rowCount: number;
  column(columnId: number): ColumnPlanningStatistics | null;
}

export interface PlanningStatistics {
  /** Null for a table the owner has no summary for yet (it plans with defaults). */
  table(tableId: number): TablePlanningStatistics | null;
}

/** One table's row count as the plan was costed (`LoweredStatement.basis`). */
export interface StatisticsBasis {
  readonly tableId: number;
  readonly rowCount: number;
}

/**
 * Whether a plan costed on `basis` should be re-lowered: some table's row count moved by more
 * than `REPLAN_GROWTH_FACTOR` in either direction, ignoring tables that stay at or below
 * `REPLAN_MIN_ROWS`. A table with no statistics now counts as unchanged.
 */
export function statisticsDrifted(
  basis: readonly StatisticsBasis[],
  statistics: PlanningStatistics,
): boolean {
  for (const entry of basis) {
    const now = statistics.table(entry.tableId)?.rowCount;
    if (now === undefined) continue;
    const larger = Math.max(now, entry.rowCount);
    const smaller = Math.max(Math.min(now, entry.rowCount), REPLAN_MIN_ROWS);
    if (larger > REPLAN_GROWTH_FACTOR * smaller) return true;
  }
  return false;
}
