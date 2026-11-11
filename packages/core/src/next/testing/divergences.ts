/**
 * The registry of intended divergences from the current engine (ruling R21): places where the new
 * core deliberately behaves differently from the differential oracle (`MinnowDatabase`). Each entry
 * names its authority (PostgreSQL behaviour, verified in PGlite) and the decision behind it, and
 * carries a matcher. A differential harness that sees the current engine disagree with PostgreSQL
 * asks `intendedDivergence`: a match is reported as intended; no match fails the run.
 *
 * A matcher must establish that its cause explains the discrepancy (review B3-p2a-2 #4): which
 * side refused, for what, and through which statement, not merely that a relevant value appears
 * in the step.
 *
 * R21 lists further entries (25P02 after a failed statement, ON CONFLICT DO UPDATE's 42702, DATE
 * parameters as text, TIMESTAMP as ISO text with µs, BIGINT as bigint, REAL as float32, byte-order
 * text, tables without a primary key). They are added here, with matchers, by the harnesses that
 * observe them.
 */
import { DEFAULT_KEY_FORWARD_RETENTION_MS } from "../../engine/key-forwards.js";
import type { PhysicalPlan, Rel } from "../ir/ir.js";

/** The constraint class a statement failed with, or "ok". */
export type OutcomeClass = "ok" | "unique" | "foreignKey";

/** What one statement of the step does, in terms a matcher can check. */
export interface StatementFacts {
  readonly sql: string;
  readonly kind: "insert" | "update" | "delete" | "rekey";
  /** The table it writes. */
  readonly table: string;
  /** Primary-key values it writes as new keys: inserted rows' keys, a rekey's target. */
  readonly newKeys: readonly number[];
  /** Primary-key values it finds rows by: UPDATE / DELETE … WHERE key = v, a rekey's source. */
  readonly matchedKeys: readonly number[];
  /** Foreign-key values it writes, with the table each references. */
  readonly references: ReadonlyArray<{ readonly table: string; readonly value: number }>;
  /** For an UPDATE: the columns (positions in the table's column order) it assigns. */
  readonly assigned?: readonly number[];
}

/** How one side ended a step. */
export interface StepOutcome {
  readonly outcome: OutcomeClass;
  /** The error message (PostgreSQL's with its DETAIL appended, `message: detail`), if it failed. */
  readonly message: string | null;
  /** The index in `statements` of the statement that failed, when known. */
  readonly statement?: number | null;
}

/** One side's version of a differing row: its foreign-key values, in the table's FK order. */
export interface RowSide {
  /** Every column's value, in the table's column order (review B3-p2b-2 A1). */
  readonly values: readonly unknown[];
  /** Its foreign-key columns: which column (into `values`), the table it references, the value. */
  readonly references: ReadonlyArray<{
    readonly column: number;
    readonly table: string;
    readonly value: number | null;
  }>;
}

/** A row that differs between the two sides after a step both committed. */
export interface DifferingRow {
  readonly table: string;
  /** Its primary key. */
  readonly key: number;
  /** The row on the current engine's side, or null when it lacks it. */
  readonly oracle: RowSide | null;
  /** The row on PostgreSQL's side, or null when it lacks it. */
  readonly authority: RowSide | null;
}

/** One step on which the current engine and the authority disagreed. */
export interface DivergenceObservation {
  readonly statements: readonly StatementFacts[];
  /** The current engine's outcome. */
  readonly oracle: StepOutcome;
  /** PostgreSQL's outcome. */
  readonly authority: StepOutcome;
  /** Both succeeded and their rows differ. */
  readonly rowsDiffer: boolean;
  /** When `rowsDiffer`: exactly the rows that differ (by primary key, either side). */
  readonly differingRows?: readonly DifferingRow[];
  readonly facts: DivergenceFacts;
}

export interface DivergenceFacts {
  /** The current engine's clock (ms), as given by its `now` option. */
  readonly nowMs: number;
  /** Each table's primary-key column (single-column keys). */
  readonly primaryKeys: ReadonlyMap<string, string>;
  /**
   * The same column's position in the table's column order (into `RowSide.values`): a rekey
   * explains a change there only, never in a column that merely holds the key's value
   * (review B3-p2b-4 A3).
   */
  readonly keyColumns: ReadonlyMap<string, number>;
  /** Primary keys a committed rekey moved a row away from, per table: key → when (same clock). */
  readonly rekeyedAway: ReadonlyMap<string, ReadonlyMap<number, number>>;
  /** Where each of those keys moved, per table: old key → new key (a later rekey may move it on). */
  readonly movedTo?: ReadonlyMap<string, ReadonlyMap<number, number>>;
  /** The current engine's `keyForwardRetentionMs` (default 10 days). */
  readonly keyForwardRetentionMs?: number;
}

export interface Divergence {
  readonly id: string;
  readonly summary: string;
  /** Why the new core is right: PostgreSQL behaviour (verified in PGlite) and the decision. */
  readonly authority: string;
  matches(observation: DivergenceObservation): boolean;
}

/** Whether `key` of `table` still forwards in the current engine (rekeyed away in the window). */
function forwarding(observation: DivergenceObservation, table: string, key: number): boolean {
  const window = observation.facts.keyForwardRetentionMs ?? DEFAULT_KEY_FORWARD_RETENTION_MS;
  const at = observation.facts.rekeyedAway.get(table)?.get(key);
  return at !== undefined && observation.facts.nowMs - at < window;
}

const DUPLICATE = /^Duplicate value for (\w+)\.(\w+): (-?\d+)$/u;
/** PostgreSQL's 23503 DETAIL for a missing parent: `Key (col)=(v) is not present in table "t".` */
const MISSING_PARENT = /Key \((\w+)\)=\((-?\d+)\) is not present in table "(\w+)"/u;

/**
 * The key a forwarding key of `table` reaches in the current engine: its row's key now, following
 * later rekeys of that row; null when the move is unknown.
 */
function forwardTarget(
  observation: DivergenceObservation,
  table: string,
  key: number,
): number | null {
  const moves = observation.facts.movedTo?.get(table);
  const first = moves?.get(key);
  if (moves === undefined || first === undefined) return null;
  let at: number = first;
  for (let hop = 0; hop < 64; hop++) {
    const next: number | undefined = moves.get(at);
    if (next === undefined || !forwarding(observation, table, at)) return at;
    at = next;
  }
  return at;
}

/**
 * Key forwarding explains a step when its effect is the one forwarding has (docs:
 * engine/rekeying, "The old key keeps working"):
 * - the current engine refuses a duplicate PostgreSQL accepts, and the duplicate is a forwarding
 *   key in its table's primary-key column, written as a new key (an INSERT, or a rekey onto it);
 * - the current engine accepts a foreign-key write PostgreSQL refuses: the statement PostgreSQL
 *   refused writes a reference to the key PostgreSQL names as missing, and that key forwards (the
 *   engine stored the key it forwards to);
 * - the current engine refuses (RESTRICT) a DELETE or rekey PostgreSQL accepts, and that refused
 *   statement matched a forwarding key (the engine reached the moved row and its children);
 * - both succeed with different rows, and every differing row is one the current engine reached
 *   through a forwarding key an UPDATE, DELETE or rekey matched: the row it forwards to (a rekey's
 *   new key too), or a child referencing one of those.
 */
function explainedByForwarding(observation: DivergenceObservation): boolean {
  const { oracle, authority, statements } = observation;
  if (oracle.outcome === "unique" && authority.outcome === "ok") {
    const duplicate = DUPLICATE.exec(oracle.message ?? "");
    if (duplicate === null) return false;
    const [, table = "", column = "", text = ""] = duplicate;
    const key = Number(text);
    return (
      observation.facts.primaryKeys.get(table) === column &&
      forwarding(observation, table, key) &&
      statements.some(
        (statement) =>
          statement.table === table &&
          (statement.kind === "insert" || statement.kind === "rekey") &&
          statement.newKeys.includes(key),
      )
    );
  }
  if (oracle.outcome === "ok" && authority.outcome === "foreignKey") {
    // PostgreSQL refused a reference to a missing parent: the refused key must be a forwarding
    // key, written as a reference by the very statement PostgreSQL refused (review B3-p2a-3 #4).
    const missing = MISSING_PARENT.exec(authority.message ?? "");
    if (missing === null) return false;
    const [, , text = "", table = ""] = missing;
    const key = Number(text);
    const at = authority.statement;
    if (at === undefined || at === null || !forwarding(observation, table, key)) return false;
    return (
      statements[at]?.references.some(
        (reference) => reference.table === table && reference.value === key,
      ) ?? false
    );
  }
  if (oracle.outcome === "foreignKey" && authority.outcome === "ok") {
    // The statement the current engine refused must itself be the DELETE or rekey that reached a
    // forwarding key (review B3-p2a-4 #1): the engine followed it to the moved row's children.
    const at = oracle.statement;
    const refused = at === undefined || at === null ? undefined : statements[at];
    return (
      refused !== undefined &&
      (refused.kind === "delete" || refused.kind === "rekey") &&
      refused.matchedKeys.some((key) => forwarding(observation, refused.table, key))
    );
  }
  if (oracle.outcome === "ok" && authority.outcome === "ok" && observation.rowsDiffer) {
    // Every differing row must be one the current engine reached through a forwarding key
    // (review B3-p2a-4 #2): the parent it forwards to (or a rekey's new key), or a child whose
    // difference is exactly what following the forward produces: a DELETE removes the child or
    // sets its reference to NULL (CASCADE / SET NULL), a rekey moves its reference to the new key.
    // A non-key UPDATE explains no child difference.
    const differing = observation.differingRows ?? [];
    if (differing.length === 0) return false;
    interface Reach {
      readonly table: string;
      readonly kind: StatementFacts["kind"];
      readonly target: number;
      readonly newKeys: readonly number[];
      readonly assigned: readonly number[];
    }
    const reaches: Reach[] = [];
    for (const statement of statements) {
      if (statement.kind === "insert") continue;
      for (const key of statement.matchedKeys) {
        if (!forwarding(observation, statement.table, key)) continue;
        const target = forwardTarget(observation, statement.table, key);
        if (target !== null)
          reaches.push({
            table: statement.table,
            kind: statement.kind,
            target,
            newKeys: statement.kind === "rekey" ? statement.newKeys : [],
            assigned: statement.kind === "update" ? (statement.assigned ?? []) : [],
          });
      }
    }
    if (reaches.length === 0) return false;
    // A parent row is explained only by the effect its operation has (review B3-p2b-3 A2): a
    // forwarded DELETE removed the forwarded-to row (the current engine lacks it, PostgreSQL keeps
    // it as it was); a rekey moved it to the new key (the moved row equals PostgreSQL's original
    // apart from the key column, found by position: review B3-p2b-4 A3); an UPDATE changed only
    // the columns it assigns.
    const differs = (a: readonly unknown[], b: readonly unknown[]): number[] | null => {
      if (a.length !== b.length) return null;
      const out: number[] = [];
      for (let column = 0; column < a.length; column++)
        if (!Object.is(a[column], b[column])) out.push(column);
      return out;
    };
    const parent = (row: DifferingRow) =>
      reaches.some((reach) => {
        if (reach.table !== row.table) return false;
        const { oracle: after, authority: before } = row;
        if (reach.kind === "delete")
          return row.key === reach.target && after === null && before !== null;
        if (reach.kind === "update") {
          if (row.key !== reach.target || after === null || before === null) return false;
          const changed = differs(before.values, after.values);
          return changed?.every((column) => reach.assigned.includes(column)) === true;
        }
        if (reach.kind === "rekey") {
          if (row.key === reach.target) return after === null && before !== null;
          if (!reach.newKeys.includes(row.key) || after === null || before !== null) return false;
          // The moved row: PostgreSQL's original, with the old key replaced by the new one.
          const original = differing.find(
            (candidate) => candidate.table === row.table && candidate.key === reach.target,
          )?.authority;
          const keyColumn = observation.facts.keyColumns.get(row.table);
          if (original === undefined || original === null || keyColumn === undefined) return false;
          const changed = differs(original.values, after.values);
          return (
            changed?.length === 1 &&
            changed[0] === keyColumn &&
            Object.is(original.values[keyColumn], reach.target) &&
            Object.is(after.values[keyColumn], row.key)
          );
        }
        return false;
      });
    // A child is explained only if every column that differs is explained (review B3-p2b-2
    // A1): one reference the forward explains cannot excuse another reference or a value.
    const child = (row: DifferingRow) => {
      const before = row.authority;
      if (before === null) return false;
      const after = row.oracle;
      // Gone on the current engine's side: a CASCADE through a DELETE of a parent it referenced.
      if (after === null)
        return before.references.some((reference) =>
          reaches.some(
            (reach) =>
              reach.kind === "delete" &&
              reach.table === reference.table &&
              reach.target === reference.value,
          ),
        );
      if (before.values.length !== after.values.length) return false;
      for (let column = 0; column < before.values.length; column++) {
        if (Object.is(before.values[column], after.values[column])) continue;
        const reference = before.references.find((candidate) => candidate.column === column);
        if (reference === undefined) return false; // a non-key value changed
        const now = after.references.find((candidate) => candidate.column === column)?.value;
        const explained = reaches.some(
          (reach) =>
            reach.table === reference.table &&
            reach.target === reference.value &&
            ((reach.kind === "delete" && now === null) ||
              (reach.kind === "rekey" && now != null && reach.newKeys.includes(now))),
        );
        if (!explained) return false;
      }
      return true;
    };
    return differing.every((row) => parent(row) || child(row));
  }
  return false;
}

export const DIVERGENCES: readonly Divergence[] = [
  {
    id: "key-forwarding",
    summary:
      "A primary key rekeyed away keeps forwarding in the current engine for keyForwardRetentionMs " +
      "(docs: engine/rekeying): an INSERT of it, or a rekey onto it, is refused as a duplicate, and " +
      "an UPDATE, DELETE or foreign-key write naming it reaches the row it moved to. The new core " +
      "has no key forwarding: the old key is free at once.",
    authority:
      "Decision D14 removes key forwarding; PostgreSQL frees a key the moment its row moves " +
      "(after `UPDATE t SET id = 15 WHERE id = 51`, `INSERT INTO t VALUES (51, …)` succeeds and " +
      "`WHERE id = 51` matches nothing), verified in PGlite.",
    matches: explainedByForwarding,
  },
];

/** The registered divergence that explains `observation`, or null (an unexplained difference). */
export function intendedDivergence(observation: DivergenceObservation): Divergence | null {
  return DIVERGENCES.find((divergence) => divergence.matches(observation)) ?? null;
}

/** Documented query compatibility/representation decisions; matchers stay specific. */
export const QUERY_DIVERGENCES = [
  {
    id: "current-json-number-lexical-normalization",
    authority:
      'PostgreSQL JSON retains numeric spelling; ->> on {"n":2.00} returns 2.00, verified in json-pglite.test.ts. The shipped engine normalizes this number text to 2. Native extraction follows PG. https://www.postgresql.org/docs/18/functions-json.html',
  },
  {
    id: "integer-average-float64",
    authority:
      "next/DECISIONS.md C11: AVG of integer/float inputs returns float64; AVG(NUMERIC) stays NUMERIC. PostgreSQL's pg_typeof(avg(integer)) is numeric (PGlite probe in executor-catalogue.test.ts).",
  },
  {
    id: "current-null-bound-refusal",
    authority:
      "PostgreSQL 7.6 https://www.postgresql.org/docs/18/queries-limit.html: NULL LIMIT means unlimited; NULL OFFSET means zero. Verified against PGlite by executor-key-order.test.ts. The new core follows PG; the current engine's bindBlockLimits rejects NULL as nonnumeric.",
  },
] as const;

/** A NULL must belong to an active bound, the refusal must come from its numeric binder,
 * and an independent retry with equivalent finite bounds must match PG's original rows. */
export function currentNullBoundRefusal(
  plan: PhysicalPlan,
  parameters: readonly unknown[],
  error: unknown,
  retryMatchesAuthority: boolean,
): string | null {
  if (
    !retryMatchesAuthority ||
    !(error instanceof TypeError) ||
    error.message !== "Arithmetic and numeric aggregates require numbers" ||
    !error.stack?.includes("bindBlockLimits")
  )
    return null;
  let rel = plan.root;
  while (rel.op === "project" && rel.exprs.every((expr) => expr.op === "column")) rel = rel.input;
  if (rel.op !== "sort" && rel.op !== "limit") return null;
  const slots = new Set(
    [rel.limit, rel.offset].flatMap((bound) => (bound?.kind === "param" ? [bound.slot] : [])),
  );
  const nulls = parameters.flatMap((value, i) => (value === null ? [i] : []));
  return nulls.length > 0 && nulls.every((slot) => slots.has(slot))
    ? "current-null-bound-refusal"
    : null;
}

/** Narrow R21 witness: an exact literal decimal with zero fraction, extracted as text.
 * The caller also runs PG's independent int4::text witness; unrelated mismatches never match. */
export function currentJsonNumberText(
  plan: PhysicalPlan,
  oracle: ReadonlyArray<Record<string, unknown>>,
  native: ReadonlyArray<Record<string, unknown>>,
  canonicalPG: ReadonlyArray<Record<string, unknown>>,
): string | null {
  if (plan.root.op !== "project" || plan.root.exprs.length !== 1 || plan.columns[0]?.name !== "j")
    return null;
  const expr = plan.root.exprs[0];
  if (expr?.op !== "call" || expr.fn !== "json_get_text" || expr.args.length !== 2) return null;
  const [document, key] = expr.args;
  if (
    document?.op !== "literal" ||
    document.type.kind !== "json" ||
    document.type.binary ||
    document.value.validity !== null ||
    document.value.data.layout !== "varlen" ||
    key?.op !== "literal" ||
    key.type.kind !== "text" ||
    key.value.validity !== null ||
    key.value.data.layout !== "varlen"
  )
    return null;
  const text = (data: { bytes: Uint8Array; offsets: Int32Array }): string =>
    new TextDecoder().decode(data.bytes.subarray(data.offsets[0] ?? 0, data.offsets[1] ?? 0));
  if (text(key.value.data) !== "n") return null;
  const match = /^\{"n":(-?(?:0|[1-9]\d{0,5})\.0{1,6})\}$/u.exec(text(document.value.data));
  const raw = match?.[1];
  if (raw === undefined) return null;
  const canonical = raw.replace(/\.0+$/u, "");
  const one = (rows: ReadonlyArray<Record<string, unknown>>, value: string): boolean =>
    rows.length === 1 && Object.keys(rows[0] ?? {}).length === 1 && rows[0]?.j === value;
  return one(native, raw) && one(oracle, canonical) && one(canonicalPG, canonical)
    ? "current-json-number-lexical-normalization"
    : null;
}

/** Trace an unchanged output position to the aggregate that produced it. Computed expressions,
 * joins and other transformations are deliberately outside this representation rule. */
function integerAverage(rel: Rel, column: number): boolean {
  if (rel.op === "project") {
    const expr = rel.exprs[column];
    return expr?.op === "column" && integerAverage(rel.input, expr.index);
  }
  if (rel.op === "sort" || rel.op === "limit" || rel.op === "filter")
    return integerAverage(rel.input, column);
  if (rel.op === "window" && column < rel.input.output.length)
    return integerAverage(rel.input, column);
  if (rel.op !== "aggregate" && rel.op !== "window") return false;
  const call =
    rel.op === "aggregate"
      ? rel.aggregates[column - rel.groupBy.length]
      : rel.functions[column - rel.input.output.length];
  const kind = call?.args[0]?.type.kind;
  return (
    call?.fn === "avg" && call.type.kind === "float64" && (kind === "int32" || kind === "int64")
  );
}

/** C11 authorizes converting this NUMERIC wire text to float64, not a tolerance or a changed
 * value. The harness then compares exact IEEE bytes. A wrong next-core value still fails.
 * No conversion for AVG(NUMERIC), an arbitrary float projection, or another aggregate. */
export function queryAuthorityValue(plan: PhysicalPlan, column: number, value: unknown): unknown {
  if (typeof value !== "string" || !integerAverage(plan.root, column)) return value;
  if (!/^[+-]?\d+(?:\.\d+)?$/u.test(value)) return value;
  const result = Number(value);
  return Number.isFinite(result) ? result : value;
}

export const ROUND_OVERLOAD_AUTHORITY =
  "PostgreSQL 18 functions-math: round(double precision) and round(numeric, integer), no round(double precision, integer); R19 keeps the overload signatures exact. https://www.postgresql.org/docs/18/functions-math.html";

/** Capability refusals are not successful executions or intended result divergences.
 * R17.6 (next/errors.ts) owns UnsupportedPlanError's stage/construct. The three ROUND
 * overload refusals were corrected and refrozen with explicit NUMERIC inputs. */
export type CatalogueRefusal =
  | { readonly class: "PlanningError"; readonly sqlState: "42883" }
  | {
      readonly class: "UnsupportedPlanError";
      readonly sqlState: "0A000";
      readonly stage: "lower" | "execute";
      readonly construct: string | null;
    };
export const CATALOGUE_REFUSALS: Readonly<Record<string, CatalogueRefusal>> = {
  "guide-7": {
    class: "UnsupportedPlanError",
    sqlState: "0A000",
    stage: "lower",
    construct: "function",
  },
  "guide-8": {
    class: "UnsupportedPlanError",
    sqlState: "0A000",
    stage: "lower",
    construct: "function",
  },
};

/** Each remaining cell has an explicit missing semantic capability and an authority outcome.
 * These explain capability refusals, never waive a successful result comparison. */
export const CATALOGUE_REFUSAL_REASONS: Readonly<
  Record<string, { readonly reason: string; readonly pgState: "00000" | "42725" }>
> = {
  "guide-7": {
    reason:
      "JSON_OBJECT / JSON_ARRAYAGG need typed heterogeneous constructors and a charged, spillable JSON accumulator; the R19 scalar table has no JSON constructor overload yet.",
    pgState: "00000",
  },
  "guide-8": {
    reason:
      "Whole-row json_agg requires record typing and bounded JSON accumulation; lowering currently expands the record to an unsupported JSON_OBJECT.",
    pgState: "00000",
  },
};

/** PostgreSQL's unordered FLOAT8 SUM permits different accumulation orders. These two
 * unchanged catalogue cells promise next-core input order (agg/states.ts, stable group radix
 * order in agg/aggregate.ts). The catalogue verifies the stored orders scan is order_id ASC.
 * Their LAG window emits month ASC before the global SUM window, so that sum is month ASC.
 * Pin only the independent PG authority to those same orders; no epsilon or SQL refreeze.
 * https://www.postgresql.org/docs/18/functions-aggregate.html (input order unspecified).
 */
export const CATALOGUE_QUERY_AUTHORITY: Readonly<Record<string, string>> = {
  "guide-13": `WITH monthly AS (
    SELECT DATE_TRUNC('month', placed_at) AS month, SUM(total ORDER BY order_id) AS revenue
    FROM orders WHERE status = 'completed' GROUP BY DATE_TRUNC('month', placed_at)
  ) SELECT month, revenue, revenue - LAG(revenue) OVER (ORDER BY month) AS change
  FROM monthly ORDER BY month`,
  "guide-17": `WITH monthly AS (
    SELECT DATE_TRUNC('month', placed_at) AS month, SUM(total ORDER BY order_id) AS revenue
    FROM orders WHERE status = 'completed' GROUP BY DATE_TRUNC('month', placed_at)
  ) SELECT month, revenue, revenue - LAG(revenue) OVER (ORDER BY month) AS change,
  100.0 * revenue / SUM(revenue) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS pct_of_total
  FROM monthly ORDER BY month`,
};

/** R21: witnesses for four frozen catalogue cells. The new executor is still compared exactly
 * with the original PostgreSQL SQL. The legacy engine is compared exactly with these independent
 * PG answers, with no epsilon, rounding, or changed next-core expectation.
 * PostgreSQL documents float8 as inexact and aggregate input order as unspecified without an
 * aggregate ORDER BY (docs/18/datatype-numeric.html and docs/18/functions-aggregate.html).
 * Legacy windows return source order, so their outer float SUM accumulates in id order.
 * C2/R17.2 types 1.0 as NUMERIC; the old engine instead uses a plain double in S_q10.
 */
// R21: the shipped engine's EXTRACT yields float8, while PostgreSQL/next use NUMERIC.
// Cast only the independent legacy witness. The frozen guide-15 SQL and next PG answer stay exact.
export const CATALOGUE_CURRENT_AUTHORITY: Readonly<Record<string, string>> = {
  "guide-15": `SELECT day, COUNT(o.order_id) AS orders,
    (EXTRACT(EPOCH FROM MAX(o.placed_at)-MIN(o.placed_at))/3600)::double precision AS busy_hours
    FROM generate_series(TIMESTAMP '2025-03-01',TIMESTAMP '2025-03-31',INTERVAL '1 day') AS day
    LEFT JOIN orders o ON DATE_TRUNC('day',o.placed_at)=day GROUP BY day ORDER BY day`,
  join_fact_fact_int:
    "SELECT COUNT(*) AS n, SUM(b.f01 ORDER BY a.id) AS s FROM events_wide a JOIN events_wide b ON b.id=a.i20 WHERE a.i05<5",
  win_running_sum:
    "SELECT COUNT(*) AS n,SUM(rs ORDER BY id) AS total,MAX(rs) AS mx FROM(SELECT id,SUM(f06) OVER(PARTITION BY d00 ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs FROM events_wide)t",
  win_moving_avg:
    "SELECT SUM(ma ORDER BY id) AS s,MAX(ma) AS mx FROM(SELECT id,AVG(f07) OVER(PARTITION BY d03 ORDER BY id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma FROM events_wide)t",
  S_q10:
    "SELECT p.category,COUNT(*) AS items,COUNT(r.item_id) AS returns,COUNT(r.item_id)*CAST(1.0 AS DOUBLE PRECISION)/COUNT(*) AS return_rate FROM products p JOIN order_items i ON i.product_id=p.product_id LEFT JOIN(SELECT DISTINCT item_id AS item_id FROM returns)r ON r.item_id=i.item_id GROUP BY p.category ORDER BY p.category",
};

/** R24.2 explicitly keeps the well-defined non-equi FULL extension. PostgreSQL refuses the
 * original with 0A000; independent evidence uses LEFT JOIN UNION ALL unmatched right rows. */
export const KEYLESS_FULL_AUTHORITY = {
  id: "non-equi-full-join",
  ruling: "PHASE-B-joint-review-1.md R24.2",
  sqlState: "0A000",
  authority:
    "PostgreSQL FULL JOIN requires a hash/merge-joinable condition; FULL's bag semantics are LEFT JOIN plus each unmatched right row exactly once. https://www.postgresql.org/docs/18/queries-table-expressions.html#QUERIES-JOIN",
} as const;

/** R17.1/R21: a narrow setup-refusal witness, never a waiver of next-core query results.
 * PostgreSQL BIGINT spans signed 64 bits; the shipped INTEGER/BIGINT storage model is number
 * (docs/sql/ddl) and refuses values outside the JS safe range. The next core uses int64.
 * The ordered-set precision test first proves the INSERT and result against PGlite.
 * https://www.postgresql.org/docs/18/datatype-numeric.html#DATATYPE-INT
 */
export const ORDERED_SET_LEGACY_BIGINT = {
  authority: "PostgreSQL signed int64; R17.1 bigint rendering versus shipped safe-integer storage",
  matches(ddl: string, values: readonly bigint[], error: unknown): boolean {
    return (
      ddl === "CREATE TABLE precise_ordered(id integer PRIMARY KEY,x bigint)" &&
      values.length > 0 &&
      values.every((v) => v >= -(1n << 63n) && v < 1n << 63n) &&
      values.some(
        (v) => v < BigInt(Number.MIN_SAFE_INTEGER) || v > BigInt(Number.MAX_SAFE_INTEGER),
      ) &&
      error instanceof TypeError &&
      error.message === "x must be a safe integer"
    );
  },
} as const;

/** C6/R21 REAL boundary witness: original input retained as FLOAT8 for the legacy comparison.
 * Always compare next with the original REAL table first. No rounding/epsilon is applied to
 * next outputs. The independent FLOAT8 table represents the shipped number storage model.
 * https://www.postgresql.org/docs/18/datatype-numeric.html#DATATYPE-FLOAT
 */
export const ORDERED_SET_CURRENT_REAL_AUTHORITY = {
  table: "ordered_real_source",
  type: "double precision",
  authority: "C6/R21: REAL rounds on storage to float32; shipped REAL columns store JS doubles",
} as const;
