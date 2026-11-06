/**
 * Declared reach for a generated campaign.
 *
 * A generated suite enforces its laws only in the states its histories actually enter. A run can
 * pass any number of seeds and still check nothing where a law matters: the generator never
 * produced a matching UPDATE, every fault hit a missing row, every live delivery was a full
 * re-run. Counting passing runs does not show that; seed 1349451771 in regression-seeds.json is
 * the case where it did not.
 *
 * A campaign declares the states its laws depend on. Each history (a seed's plan, a fast-check
 * run) starts with `beginHistory()` and calls `reach(state)` when it enters one. After the
 * campaign, `assertReached()` fails for every declared state fewer than `minimum` histories
 * entered. A history counts once per state however often it enters it.
 *
 * A `MINNOW_SEED` replay checks one history, not the campaign, so the assertion stands aside
 * then; so does a filtered run in which no history ran. The idea and the name are TanStack DB's
 * (`packages/db/tests/oracle-reach.ts`). Like ./seeds.ts, this module is excluded from the
 * published tarball.
 */
export interface Reach<State extends string> {
  /** Starts one generated history. */
  beginHistory(): void;
  /** The current history entered a declared state. */
  reach(state: State): void;
  /** Records one whole history from the states it entered, ignoring undeclared ones. */
  recordHistory(entered: Iterable<string>): void;
  /** How many histories entered each declared state. */
  counts(): ReadonlyMap<State, number>;
  /** Fails for each declared state that too few histories entered. */
  assertReached(): void;
}

export function createReach<State extends string>(
  campaign: string,
  states: readonly State[],
  { minimum = 1 }: { minimum?: number } = {},
): Reach<State> {
  const counts = new Map<State, number>(states.map((state) => [state, 0]));
  let current = new Set<State>();
  let histories = 0;
  return {
    beginHistory() {
      current = new Set();
      histories += 1;
    },
    reach(state) {
      const count = counts.get(state);
      if (count === undefined) throw new Error(`${campaign}: undeclared reach state ${state}`);
      if (histories === 0) throw new Error(`${campaign}: reach(${state}) before beginHistory()`);
      if (current.has(state)) return;
      current.add(state);
      counts.set(state, count + 1);
    },
    recordHistory(entered) {
      this.beginHistory();
      for (const state of entered) {
        if (counts.has(state as State)) this.reach(state as State);
      }
    },
    counts: () => counts,
    assertReached() {
      const replaying = (process.env.MINNOW_SEED ?? "").trim() !== "";
      if (replaying || histories === 0) return;
      const missed = [...counts]
        .filter(([, count]) => count < minimum)
        .map(([state, count]) => `${state} (${String(count)})`);
      if (missed.length > 0) {
        throw new Error(
          `${campaign}: of ${String(histories)} generated histories, fewer than ` +
            `${String(minimum)} reached these states, so their laws were not enforced there: ` +
            missed.join(", "),
        );
      }
    },
  };
}

/**
 * The scaled-float codec at the safe-integer bounds (next/page/scaled-float.test.ts, review
 * B2-p2i-2 #3): a stored page whose base lies within 2^32 of +(2^53 − 1) or −(2^53 − 1), for each
 * delta width and each exponent band, read from the stored header (base sign, width byte, exponent
 * in aux), never inferred from the generator's inputs. Width 0 is the constant-integer case and
 * needs no bound of its own; every cell listed here is reachable: a page of n / 10^e for n within
 * the width's range of the bound, e in the band.
 */
export const SCALED_FLOAT_BOUNDARY_REACH = (["positive", "negative"] as const).flatMap((sign) =>
  ([1, 2, 4] as const).flatMap((width) =>
    (["exponent 0", "exponents 1-6", "exponents 7-15"] as const).map(
      (band) => `${sign} bound, width ${String(width)}, ${band}` as const,
    ),
  ),
);

/** Rebuild campaign: each source plus both successful and refused SQL must execute. */
export const DIFFERENTIAL_REACH = [
  "conformance",
  "fast-check",
  "interaction",
  "sqllogictest",
  "api-contract",
  "schema",
  "insertBatch",
  "upsertBatch",
  "deleteBatch",
  "reopen",
  "transaction",
  "returning",
  "success",
  "refusal",
] as const;

/** Required boundary states for the integer arithmetic/aggregate campaign. */
export const INTEGER_EXACTNESS_STATES = ["positive overflow", "negative overflow", "safe"] as const;

export const INTEGER_MUTATION_EXACTNESS_STATES = [
  ...INTEGER_EXACTNESS_STATES,
  "integer trigger source",
  "float trigger source",
  "date-derived safe",
  "date-derived overflow",
  "sequence safe",
  "sequence overflow",
] as const;

export const INTEGER_ARITHMETIC_EXACTNESS_STATES = [
  ...INTEGER_EXACTNESS_STATES,
  "fractional parameter",
  "dictionary aggregate",
  "integer text safe",
  "integer text refusal",
  "fractional text refusal",
] as const;

/** DOUBLE aggregates retain float arithmetic regardless of whole-valued contributions. */
export const DOUBLE_AGGREGATE_STATES = [
  "positive whole safe inputs",
  "negative whole safe inputs",
  "positive whole unsafe inputs",
  "negative whole unsafe inputs",
  "fractional inputs",
  "dictionary",
  "window",
  "unknown inputs",
] as const;

/**
 * The client's statement classifier against the front end (next/testing/owner-statement-kind
 * .test.ts): every kind the front end compiles, multi-statement text, and text it refuses.
 */
export const STATEMENT_KIND_STATES = [
  "compiled begin",
  "compiled commit",
  "compiled rollback",
  "compiled savepoint",
  "compiled other",
  "multi-statement",
  "refused by the front end",
] as const;

/**
 * Pending requests resolved through the ledger after an owner death (next/testing/owner-election
 * .test.ts, review 3): a lost reply answered from the ledger, an absence proven, an effect left
 * unknown.
 */
export const LEDGER_RESOLUTION_STATES = [
  "commit replayed",
  "commit none",
  "inc replayed",
  "inc unknown",
] as const;

/**
 * A session's barrier after a reconnect (next/testing/owner-election.test.ts, B5-p2d review 1):
 * a fresh request refused while a loss stands, and run after a confirmed end discharged it.
 */
export const BARRIER_RESOLUTION_STATES = [
  "refused while lost",
  "ran after a confirmed end",
  "ran after an explicit ROLLBACK",
  "lost again after a confirmed end",
  "ran, never lost",
] as const;

/**
 * The storage kill campaign (browser-tests/next-storage.spec.ts, Gate A) on IndexedDB: kills must
 * land inside each kind of step's storage work: the worker had posted `began` and not `settled`,
 * and a platform request (an IndexedDB transaction created and not ended) was open (review B1-p2g
 * finding 2: `began` alone lands `window-<step>`, which reaches nothing).
 */
export const STORAGE_KILL_REACH = [
  "in-write",
  "in-publish",
  "in-reclaim",
  "in-log-flush",
  "in-truncate",
  "in-spill",
] as const;

/**
 * The same campaign on OPFS, whose storage calls are synchronous: termination stops the worker
 * between JavaScript steps, so a kill lands at a step's edges (before it began, between its calls,
 * or after it returned) or after a sync call began and before its end was posted (the call itself
 * has returned), never halfway through a call. Each phase must be hit at an edge; interruption
 * inside the synchronous operations is the simulated physical crash sweeps' (the storage kit over
 * simulated handles, space-crash.test.ts), which crash at every write step.
 */
export const STORAGE_KILL_EDGE_REACH = [
  "at-write",
  "at-publish",
  "at-reclaim",
  "at-log-flush",
  "at-truncate",
  "at-spill",
] as const;

/**
 * The concurrent kill campaign on IndexedDB: kills at wall-clock moments must land with a platform
 * request open (a transaction created and not ended; the backend runs one at a time per
 * connection), of each kind, and while two or more operations are in their windows (review B1-p2g
 * finding 2: windows alone are reported, not counted).
 */
export const STORAGE_CONCURRENT_KILL_REACH = [
  "request-open",
  "publish-request-open",
  "log-request-open",
  "spill-request-open",
  "data-request-open",
  "request-open-with-two-operations-in-windows",
] as const;

/**
 * The concurrent workload under simulated power loss (spill/chaos-crash.test.ts): the crash must
 * land while several storage calls (session and spill calls that have run and not yet returned
 * their result, review B1-p2g finding 2) are issued, data and log calls among them.
 */
export const STORAGE_CONCURRENT_CRASH_REACH = [
  "two-or-more-calls-issued",
  "three-or-more-calls-issued",
  "data-call-with-log-call",
  // A data write issued while two write operations are open (began, not settled): the space's
  // write lock admits one data write at a time, so two issued data writes never coexist.
  "data-write-with-two-writers-open",
  "spill-call-with-another",
] as const;

/**
 * Acquisitions sharing one IndexedDB connection object (storage/idb-chain.test.ts): steals and
 * releases must land while another acquisition has a transaction in flight on it, from both
 * backends over the object.
 */
export const STORAGE_SHARED_CONNECTION_REACH = [
  "steal-while-another-in-flight",
  "release-while-another-in-flight",
  "both-backends-owned",
] as const;
