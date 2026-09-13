/**
 * Incremental live-query maintenance: the retained state a subscribed statement keeps, and the
 * row algebra a commit patches it with instead of executing the statement again.
 *
 * A maintained statement projects its key and every ORDER BY term under hidden aliases, so a
 * patch can order and identify rows without re-reading them. The comparator here must match the
 * engine's own ORDER BY, or a patched window would order differently from a fresh execution.
 */
import { dateMilliseconds } from "../date-value.js";
import { estimateValuesBytes } from "./byte-estimates.js";
import { planMemoKey, sameQueryRow as sameLiveRow } from "./query-cache.js";
import {
  externalizeQueryResult,
  type CompiledQuery,
  type QueryResult,
  type QueryRow,
  type QueryValue,
} from "./query.js";
import { compareSqlValues } from "./sql-semantics.js";
import type { SqlDomain } from "../storage/types.js";
import type { LiveAggregate } from "./live-aggregate.js";
import type { LiveMaintainedChange } from "./live.js";

export const LIVE_HIDDEN_PREFIX = "__minnow_live_";
export const LIVE_KEY_ALIAS = `${LIVE_HIDDEN_PREFIX}key`;
export const LIVE_ORDER_ALIAS = `${LIVE_HIDDEN_PREFIX}order_`;
/** More changed rows than this per commit window and the full statement is the cheaper path. */
export const LIVE_MAINTENANCE_MAX_DELTA_ROWS = 2_048;
/** Rows a window keeps beyond its visible edge: at least this many, at most the window itself. */
export const LIVE_WINDOW_MARGIN_MIN = 16;
export const LIVE_WINDOW_MARGIN_MAX = 64;

export interface LiveMaintenanceOrderTerm {
  /** The result column the term's value is read from, public or hidden. */
  readonly alias: string;
  readonly descending: boolean;
  readonly nulls: "first" | "last" | undefined;
}

/** Everything a live set hands back so a later commit can patch the result it retained. */
export interface LiveMaintenanceState {
  readonly aggregate?: LiveAggregate;
  readonly tableId: string;
  readonly keyColumnId: string;
  readonly qualifiedKey: string;
  /** The statement with its key and ORDER BY terms projected under hidden aliases. */
  readonly fullPlan: CompiledQuery;
  /** The same without ORDER BY/LIMIT/OFFSET; a key list is appended per commit window. */
  readonly deltaPlan: CompiledQuery;
  readonly publicColumns: readonly string[];
  readonly columnDomains: ReadonlyArray<SqlDomain | null>;
  readonly orderTerms: readonly LiveMaintenanceOrderTerm[];
  readonly limit: number | undefined;
  readonly offset: number;
  /**
   * Rows fetched beyond a window's visible edge. A member that leaves a full window is then
   * replaced from rows already held rather than by running the statement again; only when
   * the margin runs dry does a full execution refill it.
   */
  readonly margin: number;
  /**
   * Whether the retained rows are every row the statement matches. False once a full
   * execution filled the window and its margin to the brim: rows beyond the retained edge may
   * exist, and a patch that would reach past that edge needs the statement again.
   */
  readonly complete: boolean;
  /** Every retained row — the visible ones first, then the margin — in result order. */
  readonly rows: readonly QueryRow[];
  readonly keys: readonly string[];
  /** Fixed plan cost plus row/key/order payload; updated only for changed and trimmed rows. */
  readonly retainedBytes?: number;
  /** Key lookup avoids scanning and re-encoding retained keys for each small delta. */
  readonly positions: ReadonlyMap<string, number>;
  /** One array per ORDER BY term, each holding that term's value for every retained row. */
  readonly order: ReadonlyArray<readonly QueryValue[]>;
}

export interface LiveMaintainedRows {
  readonly rows: QueryRow[];
  readonly keys: string[];
  /** One array per ORDER BY term. */
  readonly order: QueryValue[][];
  /** For each row, its index among the rows the patch started from, or -1 for a new row. */
  readonly previousIndex: Int32Array;
}

export function liveKeyToken(value: QueryValue): string {
  if (typeof value === "number") return `n:${String(value)}`;
  if (typeof value === "string") return `s:${value}`;
  if (typeof value === "boolean") return value ? "b:1" : "b:0";
  if (value instanceof Date) return `d:${String(dateMilliseconds(value))}`;
  return "z";
}

/** Strips a maintainable plan's hidden projections out of an executed result, keeping them aside. */
export function splitLiveHiddenColumns(
  executed: QueryResult,
  state: Pick<LiveMaintenanceState, "publicColumns" | "orderTerms">,
): { result: QueryResult; keys: string[]; order: QueryValue[][] } {
  const publicCount = state.publicColumns.length;
  const hidden = executed.columns.slice(publicCount);
  const count = executed.rows.length;
  const keys = new Array<string>(count);
  const order = state.orderTerms.map(() => new Array<QueryValue>(count));
  for (let index = 0; index < count; index += 1) {
    const row = executed.rows[index] ?? {};
    keys[index] = liveKeyToken(row[LIVE_KEY_ALIAS] ?? null);
    for (const [term, { alias }] of state.orderTerms.entries()) {
      const values = order[term];
      if (values !== undefined) values[index] = row[alias] ?? null;
    }
    for (const column of hidden) Reflect.deleteProperty(row, column);
  }
  return {
    // Keep SQL-domain tags in the private ordering arrays above. Only public rows cross the
    // externalization boundary: decimal/interval ordering must never compare display strings.
    result: externalizeQueryResult({
      columns: executed.columns.slice(0, publicCount),
      columnDomains: executed.columnDomains.slice(0, publicCount),
      rows: executed.rows,
    }),
    keys,
    order,
  };
}

/**
 * The visible result for a maintenance state: its rows up to the window's limit. Against a
 * previous result, an outcome whose visible rows are all what they were is reported unchanged
 * with that result's objects. Otherwise every row the patch kept reports where it was, and a
 * row that rewrote itself with the values it had keeps its previous object.
 */
export function liveMaintainedOutcome(
  state: LiveMaintenanceState,
  previous: QueryResult | undefined,
  previousIndex?: Int32Array,
): LiveMaintainedChange {
  const positions = new Map<string, number>();
  let retainedBytes =
    state.retainedBytes ??
    128 + planMemoKey(state.fullPlan).length * 2 + planMemoKey(state.deltaPlan).length * 2;
  for (const [index, key] of state.keys.entries()) {
    if (positions.has(key)) throw new TypeError("Duplicate live input key");
    positions.set(key, index);
    if (state.retainedBytes === undefined) retainedBytes += liveRowStateBytes(state, index);
  }
  state = { ...state, positions, retainedBytes };
  const visibleCount =
    state.limit === undefined ? state.rows.length : Math.min(state.rows.length, state.limit);
  const visible = state.rows.slice(0, visibleCount);
  if (previous === undefined) {
    return {
      result: {
        columns: [...state.publicColumns],
        columnDomains: [...state.columnDomains],
        rows: visible,
      },
      state,
      retainedBytes,
      changed: true,
    };
  }
  const previousCount = previous.rows.length;
  const retained = new Int32Array(visibleCount);
  let same = visibleCount === previousCount;
  for (let index = 0; index < visibleCount; index += 1) {
    const was = previousIndex?.[index] ?? -1;
    if (was >= 0 && was < previousCount) {
      retained[index] = was;
      if (was !== index) same = false;
      continue;
    }
    retained[index] = -1;
    const before = previous.rows[index];
    const now = visible[index];
    if (before !== undefined && now !== undefined && sameLiveRow(before, now, previous.columns)) {
      visible[index] = before;
      retained[index] = index;
    } else same = false;
  }
  if (same) return { result: previous, state, retainedBytes, changed: false };
  return {
    result: {
      columns: [...state.publicColumns],
      columnDomains: [...state.columnDomains],
      rows: visible,
    },
    state,
    retainedBytes,
    changed: true,
    retained,
  };
}

export function liveRowStateBytes(
  state: Pick<LiveMaintenanceState, "rows" | "keys" | "order">,
  index: number,
): number {
  let bytes =
    112 +
    (state.keys[index]?.length ?? 0) * 2 +
    estimateValuesBytes(Object.values(state.rows[index] ?? {})) * 2;
  for (const values of state.order) bytes += 8 + estimateValuesBytes([values[index]]) * 2;
  return bytes;
}

export function filterLiveRows(
  state: LiveMaintenanceState,
  keep: (index: number) => boolean,
): LiveMaintainedRows {
  const rows: QueryRow[] = [];
  const keys: string[] = [];
  const order = state.order.map(() => new Array<QueryValue>());
  const previous: number[] = [];
  for (let index = 0; index < state.rows.length; index += 1) {
    const row = state.rows[index];
    if (row === undefined || !keep(index)) continue;
    rows.push(row);
    keys.push(state.keys[index] ?? "z");
    for (const [term, values] of state.order.entries()) {
      order[term]?.push(values[index] ?? null);
    }
    previous.push(index);
  }
  return { rows, keys, order, previousIndex: Int32Array.from(previous) };
}

export function trimLiveRows(rows: LiveMaintainedRows, count: number): LiveMaintainedRows {
  return {
    rows: rows.rows.slice(0, count),
    keys: rows.keys.slice(0, count),
    order: rows.order.map((values) => values.slice(0, count)),
    previousIndex: rows.previousIndex.slice(0, count),
  };
}

/** Compares the row at one index of one term-column set with the row at another index of another. */
type LiveOrderCompare = (
  leftOrder: ReadonlyArray<readonly QueryValue[]>,
  leftIndex: number,
  rightOrder: ReadonlyArray<readonly QueryValue[]>,
  rightIndex: number,
) => number;

/**
 * PostgreSQL's ORDER BY over the projected term values: direction negates the value comparison
 * only, and NULLs go last for ASC and first for DESC unless the term says otherwise.
 */
export function liveOrderComparator(terms: readonly LiveMaintenanceOrderTerm[]): LiveOrderCompare {
  return (leftOrder, leftIndex, rightOrder, rightIndex) => {
    for (const [term, { descending, nulls }] of terms.entries()) {
      const a = leftOrder[term]?.[leftIndex] ?? null;
      const b = rightOrder[term]?.[rightIndex] ?? null;
      if (a === null || b === null) {
        if (a === null && b === null) continue;
        const nullsFirst = nulls === "first" || (nulls === undefined && descending);
        return a === null ? (nullsFirst ? -1 : 1) : nullsFirst ? 1 : -1;
      }
      let comparison = compareSqlValues(a, b);
      if (descending) comparison = -comparison;
      if (comparison !== 0) return comparison;
    }
    return 0;
  };
}

/** Merges rows a commit added into rows it kept; `kept` is already in order when `ordered`. */
export function mergeLiveRows(
  kept: LiveMaintainedRows,
  added: LiveMaintainedRows,
  compare: LiveOrderCompare,
  ordered: boolean,
): LiveMaintainedRows {
  if (added.rows.length === 0) return kept;
  const terms = kept.order.length;
  const total = kept.rows.length + added.rows.length;
  const rows = new Array<QueryRow>(total);
  const keys = new Array<string>(total);
  const order = Array.from({ length: terms }, () => new Array<QueryValue>(total));
  const previousIndex = new Int32Array(total);
  const take = (source: LiveMaintainedRows, from: number, to: number): void => {
    rows[to] = source.rows[from] ?? {};
    keys[to] = source.keys[from] ?? "z";
    for (let term = 0; term < terms; term += 1) {
      const values = order[term];
      if (values !== undefined) values[to] = source.order[term]?.[from] ?? null;
    }
    previousIndex[to] = source.previousIndex[from] ?? -1;
  };
  if (!ordered) {
    for (let index = 0; index < kept.rows.length; index += 1) take(kept, index, index);
    for (let index = 0; index < added.rows.length; index += 1) {
      take(added, index, kept.rows.length + index);
    }
    return { rows, keys, order, previousIndex };
  }
  const addedIndexes = added.rows.map((_, index) => index);
  addedIndexes.sort((left, right) => compare(added.order, left, added.order, right));
  let keptIndex = 0;
  let addedPosition = 0;
  for (let to = 0; to < total; to += 1) {
    const addedIndex = addedIndexes[addedPosition];
    // Ties keep the retained row ahead, as a stable sort keeps earlier input ahead.
    const takeAdded =
      addedIndex !== undefined &&
      (keptIndex >= kept.rows.length ||
        compare(added.order, addedIndex, kept.order, keptIndex) < 0);
    if (takeAdded) {
      take(added, addedIndex, to);
      addedPosition += 1;
    } else {
      take(kept, keptIndex, to);
      keptIndex += 1;
    }
  }
  return { rows, keys, order, previousIndex };
}
