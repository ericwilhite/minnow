/**
 * The registry of intended divergences from the current engine (ruling R21): places where the new
 * core deliberately behaves differently from the differential oracle (`MinnowDatabase`). Each entry
 * names its authority (PostgreSQL behaviour, verified in PGlite) and the decision behind it, and
 * carries a matcher. A differential harness that sees the current engine disagree with the new core
 * asks `intendedDivergence`: a match is reported as intended; no match fails the run.
 *
 * R21 lists further entries (25P02 after a failed statement, ON CONFLICT DO UPDATE's 42702, DATE
 * parameters as text, TIMESTAMP as ISO text with µs, BIGINT as bigint, REAL as float32, byte-order
 * text, tables without a primary key). They are added here, with matchers, by the harnesses that
 * observe them.
 */
import { DEFAULT_KEY_FORWARD_RETENTION_MS } from "../../engine/key-forwards.js";

/** One step on which the current engine and the authority disagreed. */
export interface DivergenceObservation {
  /** The statements of the step (one transaction). */
  readonly statements: readonly string[];
  /** Primary-key values the statements name (written, matched or referenced), per table. */
  readonly keys: ReadonlyMap<string, readonly number[]>;
  /** The current engine's outcome: null when it succeeded, else its error message. */
  readonly oracleError: string | null;
  /** PostgreSQL's outcome, likewise. */
  readonly authorityError: string | null;
  /** What the harness knows about the history the step ran on. */
  readonly facts: DivergenceFacts;
}

export interface DivergenceFacts {
  /** The current engine's clock (ms), as given by its `now` option. */
  readonly nowMs: number;
  /** Primary keys a committed rekey moved away from, per table: key → when (ms, same clock). */
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

/** Keys of the step still forwarding in the current engine (rekeyed away within the window). */
function forwardedKeys(observation: DivergenceObservation): number[] {
  const window = observation.facts.keyForwardRetentionMs ?? DEFAULT_KEY_FORWARD_RETENTION_MS;
  const forwarded: number[] = [];
  for (const [table, keys] of observation.keys) {
    const retired = observation.facts.rekeyedAway.get(table);
    if (retired === undefined) continue;
    for (const key of keys) {
      const at = retired.get(key);
      if (at !== undefined && observation.facts.nowMs - at < window) forwarded.push(key);
    }
  }
  return forwarded;
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
    matches: (observation) => forwardedKeys(observation).length > 0,
  },
];

/** The registered divergence that explains `observation`, or null (an unexplained difference). */
export function intendedDivergence(observation: DivergenceObservation): Divergence | null {
  return DIVERGENCES.find((divergence) => divergence.matches(observation)) ?? null;
}
