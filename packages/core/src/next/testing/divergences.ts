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
}

/** How one side ended a step. */
export interface StepOutcome {
  readonly outcome: OutcomeClass;
  /** The error message (PostgreSQL's with its DETAIL appended, `message: detail`), if it failed. */
  readonly message: string | null;
  /** The index in `statements` of the statement that failed, when known. */
  readonly statement?: number | null;
}

/** A row that differs between the two sides after a step both committed. */
export interface DifferingRow {
  readonly table: string;
  /** Its primary key. */
  readonly key: number;
  /** Foreign-key values it holds on either side, with the table each references. */
  readonly references: ReadonlyArray<{ readonly table: string; readonly value: number }>;
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
 * - the current engine refuses (RESTRICT) a DELETE or rekey PostgreSQL accepts, which matched a
 *   forwarding key (the engine reached the moved row and its children);
 * - both succeed with different rows, and every differing row is one the current engine reached
 *   through a forwarding key an UPDATE, DELETE or rekey matched: the row it forwards to (a rekey's
 *   new key too), or a child referencing one of those.
 */
function explainedByForwarding(observation: DivergenceObservation): boolean {
  const { oracle, authority, statements } = observation;
  const matchedForwarding = (kinds: ReadonlyArray<StatementFacts["kind"]>) =>
    statements.some(
      (statement) =>
        kinds.includes(statement.kind) &&
        statement.matchedKeys.some((key) => forwarding(observation, statement.table, key)),
    );
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
  if (oracle.outcome === "foreignKey" && authority.outcome === "ok")
    return matchedForwarding(["delete", "rekey"]);
  if (oracle.outcome === "ok" && authority.outcome === "ok" && observation.rowsDiffer) {
    // Every differing row must be one the current engine reached through a forwarding key: the
    // row it forwards to (and a rekey's new key), or a child referencing one of those.
    const differing = observation.differingRows ?? [];
    if (differing.length === 0) return false;
    const reached = new Map<string, Set<number>>();
    const add = (table: string, key: number) => {
      let keys = reached.get(table);
      if (keys === undefined) reached.set(table, (keys = new Set()));
      keys.add(key);
    };
    for (const statement of statements) {
      if (statement.kind === "insert") continue;
      for (const key of statement.matchedKeys) {
        if (!forwarding(observation, statement.table, key)) continue;
        const target = forwardTarget(observation, statement.table, key);
        if (target === null) continue;
        add(statement.table, target);
        if (statement.kind === "rekey")
          for (const next of statement.newKeys) add(statement.table, next);
      }
    }
    return differing.every(
      (row) =>
        reached.get(row.table)?.has(row.key) === true ||
        row.references.some((reference) => reached.get(reference.table)?.has(reference.value)),
    );
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
