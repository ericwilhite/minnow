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

/** One step on which the current engine and the authority disagreed. */
export interface DivergenceObservation {
  readonly statements: readonly StatementFacts[];
  /** The current engine's outcome, and its error message if it failed. */
  readonly oracle: { readonly outcome: OutcomeClass; readonly message: string | null };
  /** PostgreSQL's outcome, likewise. */
  readonly authority: { readonly outcome: OutcomeClass; readonly message: string | null };
  /** Both succeeded and their rows differ. */
  readonly rowsDiffer: boolean;
  readonly facts: DivergenceFacts;
}

export interface DivergenceFacts {
  /** The current engine's clock (ms), as given by its `now` option. */
  readonly nowMs: number;
  /** Each table's primary-key column (single-column keys). */
  readonly primaryKeys: ReadonlyMap<string, string>;
  /** Primary keys a committed rekey moved a row away from, per table: key → when (same clock). */
  readonly rekeyedAway: ReadonlyMap<string, ReadonlyMap<number, number>>;
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

/**
 * Key forwarding explains a step when its effect is the one forwarding has (docs:
 * engine/rekeying, "The old key keeps working"):
 * - the current engine refuses a duplicate PostgreSQL accepts, and the duplicate is a forwarding
 *   key in its table's primary-key column, written as a new key (an INSERT, or a rekey onto it);
 * - the current engine accepts a foreign-key write PostgreSQL refuses, which names a forwarding
 *   key of the referenced table (the engine stored the key it forwards to);
 * - the current engine refuses (RESTRICT) a DELETE or rekey PostgreSQL accepts, which matched a
 *   forwarding key (the engine reached the moved row and its children);
 * - both succeed with different rows, and an UPDATE, DELETE or rekey matched a forwarding key.
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
  if (oracle.outcome === "ok" && authority.outcome === "foreignKey")
    return statements.some((statement) =>
      statement.references.some((reference) =>
        forwarding(observation, reference.table, reference.value),
      ),
    );
  if (oracle.outcome === "foreignKey" && authority.outcome === "ok")
    return matchedForwarding(["delete", "rekey"]);
  if (oracle.outcome === "ok" && authority.outcome === "ok" && observation.rowsDiffer)
    return matchedForwarding(["update", "delete", "rekey"]);
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
