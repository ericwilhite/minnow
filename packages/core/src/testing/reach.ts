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

/** Full-scan shared windows: the default and override must cover cold and resident reads. */
export const SCAN_WINDOW_RESIDENCY_REACH = [
  "default cold",
  "default resident",
  "default mixed",
  "explicit cold",
  "explicit resident",
  "explicit mixed",
  "disabled",
] as const;

/** R23.5 inside one coalesced scan window, not only at batch/operator boundaries. */
export const SCAN_WINDOW_PACING_REACH = [
  "multi-read window",
  "immediately resolved storage",
  "timer inside window",
  "cancel message inside window",
  "carried task budget",
  "concurrent follower",
  "follower across later yield",
] as const;

/** Abort stops new backend work, drains started buffers, then permits a fresh scan. */
export const SCAN_WINDOW_CANCEL_REACH = [
  "first native read blocked",
  "later native read blocked",
  "started work remains charged",
  "no backend calls after cancel",
  "signal forwarded to storage",
  "fresh signal retry",
] as const;

/** Closing one participant must not cancel the other readers of a shared read. */
export const SHARED_WINDOW_CLOSE_REACH = [
  "closed jobs omitted",
  "all readers closed",
  "live readers complete",
  "closed suffix",
  "alternating closure",
  "independent cancel signals",
] as const;

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

/**
 * A broker replaced under its ports (next/testing/owner-election.test.ts, WebKit 26.6's
 * SharedWorker restart, next/owner/webkit-sharedworker.md): the restart overlapped a client's
 * broker request; it emptied a live owner's registration; it emptied a client's wait for one; a
 * request id reached one instance twice.
 */
export const BROKER_RESTART_STATES = [
  "restart during connect",
  "restart while the owner is registered",
  "restart with a client waiting",
  "duplicate request delivered",
] as const;

/** Discovery must finish despite responsive negative replies, failed hellos or stuck startup. */
export const OWNER_DISCOVERY_STATES = [
  "delayed event-loop turn",
  "noOwner",
  "unreachable",
  "handshake timeout",
  "handshake refusal",
  "handshake newer",
  "never ready",
  "lock wait",
  "sent unanswered",
  "never sent",
  "startup throws",
  "broker throws",
] as const;

/** Registration loss is independent of instance announcements and includes lost acknowledgements. */
export const BROKER_REGISTRATION_STATES = [
  "register lost after announcement",
  "ack lost",
  "recovered registration",
  "terminal failure",
  "stop cancels retry",
  "host fence cancels retry",
  "same instance repeated",
] as const;

/** Owner lease release (next/testing/owner-step-down.test.ts): publication waits for release. */
export const OWNER_RELEASE_STATES = [
  "storage session close awaited",
  "active work drains",
  "disconnected work drains",
  "unsettled report",
  "release pending",
  "clean void",
  "clean report",
  "sync unsettled",
  "async unsettled",
  "freeze",
  "pagehide",
  "hidden",
  "superseded",
  "closing",
  "lockLost",
] as const;

/** Standalone load errors must already have an observer while planning is yielded. */
export const WINDOW_CANCEL_REJECTION_REACH = [
  "dictionary pending",
  "cancel while yielded",
  "rejection observed",
] as const;

/** Descriptor reuse across scan phases, endpoint page spans and residency changes. */
export const SCAN_SHAPE_REACH = [
  "512 B raw page",
  "2 KiB raw page",
  "same page span",
  "narrower page span",
  "release gap",
  "reload gap",
  "owned and borrowed",
  "gather same endpoints",
  "gather across unloaded gap",
  "dictionary",
  "plain varlen",
  "mixed safe integers",
  "all null",
  "all valid",
  "int128 mixed scales",
] as const;

/** One verified page-span lookup across repeated scan planning phases. */
export const SCAN_SHAPE_LOOKUP_REACH = [
  "safe integers",
  "wide integers",
  "same endpoint pages",
] as const;

/** Density adaptation at raw buffer capacities on both sides of its 64-row preference. */
export const SMALL_PAGE_CAPACITY_REACH = [
  "capacity below 64",
  "capacity at least 64",
  "second flush",
  "interval",
  "fixed16",
  "decimal128",
] as const;

/** Each small-page writer representation and source form runs this independent campaign. */
export const SMALL_PAGE_REPRESENTATION_REACH = [
  "capacity below 64",
  "capacity at least 64",
  "write completed",
  "ordinary read completed",
  "source equality",
  "second flush",
] as const;

/** Stratified independently for every type/representation and source form. */
export const SMALL_PAGE_NULL_STATES = ["all valid", "sparse nulls"] as const;

/** Independent source-sized shape differential over real cache eviction and group switches. */
export const SHAPE_DIFFERENTIAL_REACH = [
  "separate page and metadata caches",
  "independent source lengths",
  "actual cache eviction",
  "backend reload after eviction",
  "same target switches group",
  "all-valid group",
  "sparse-null group",
  "all-null group",
  "empty range",
  "empty gather",
  "same endpoints different interiors",
] as const;

/** Rollback must not revive a memtable whose frozen rows a publication owns. */
export const WRITE_FREEZE_ROLLBACK_STATES = [
  "memory",
  "indexeddb",
  "failed statement rollback",
  "successful statement rollback",
  "flush owns partially full input",
  "one later row",
  "two later rows",
  "secondary index checked",
  "primary only",
  "publication retains later rows",
  "durable reopen",
] as const;

/** Each original write-differential seed retains all steps under all three task schedules. */
export const WRITE_DIFFERENTIAL_CLOCK_STATES = [
  "real clock",
  "every third check",
  "every seventh check",
] as const;

/**
 * SQL conformance (engine/sql-conformance.test.ts): the generator dimensions of the qualified
 * ORDER BY, correlated aggregate, EXISTS-over-aggregate and DISTINCT ON fixes. A state counts when
 * a case carrying it agreed with every oracle in a corpus run.
 */
const JOIN_KINDS = ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] as const;
export const QUALIFIED_ORDER_STATES = [
  ...JOIN_KINDS.flatMap((kind) => [`${kind} ON`, `${kind} USING`]),
  ...JOIN_KINDS.filter((kind) => kind !== "FULL JOIN").flatMap((kind) => [
    `${kind} ON window`,
    `${kind} USING window`,
  ]),
  "single table",
  "derived FULL JOIN",
] as const;

export const CORRELATED_AGGREGATE_STATES = [
  "bound LIMIT 1",
  "bound LIMIT 0",
  "bound LIMIT 1 OFFSET 1",
  "bound OFFSET 0",
  "bound OFFSET 1",
  "bound ORDER BY 1 DESC LIMIT 1",
  "equality correlation",
  "range correlation",
  "select list",
  "where",
] as const;

export const EXISTS_AGGREGATE_STATES = [
  "exists COUNT(*)",
  "exists MAX(d.amount)",
  "exists unbounded",
  "exists LIMIT 0",
  "exists OFFSET 1",
  "where exists",
  "where not exists",
  "selected exists",
] as const;

export const DISTINCT_ON_STATES = [
  "ordinal key",
  "ordinal sort",
  "qualified expression key",
] as const;

/** A generated refusal campaign: both outcomes, compared against PostgreSQL. */
export const REFUSAL_STATES = ["refused", "accepted"] as const;

/** Outer locking target shapes compared with PostgreSQL in the read-only campaign. */
export const ROW_LOCK_TARGET_STATES = [
  "unused CTE",
  "constant derived",
  "CTE",
  "table derived",
  "OF constant",
  "OF table",
  "series",
  "empty table",
] as const;

/** A view remains a relation lock target even when its body has no table. */
export const ROW_LOCK_VIEW_STATES = ["read only refused", "read write allowed"] as const;

/** Real IndexedDB events count even when no per-request adapter promise is waiting for them. */
export const INDEXEDDB_NATIVE_PROGRESS_STATES = [
  "get",
  "getKey",
  "count",
  "cursor",
  "cursor continuation",
  "prevented request error",
  "transaction abort",
  "same connection",
  "other connection",
  "one waiter",
  "several waiters",
  "small budget",
  "large budget",
  "continuous native progress",
  "silence still bounded",
  "shared failure",
] as const;

/** Admission follows PG; the strength-free SELECT diagnostic never describes a syntax-only lock. */
export const ROW_LOCK_DIAGNOSTIC_STATES = [
  "read only refused",
  "read only allowed",
  "read write allowed",
  "early",
  "expanded view",
  "unlocked view",
  "retained cte",
  "nested",
  "point read",
  "weaker outer",
  "stronger outer",
  "cte boundary",
  "unused cte",
  "strongest KEY SHARE",
  "strongest SHARE",
  "strongest NO KEY UPDATE",
  "strongest UPDATE",
  "execute",
  "query",
  "typed",
  "cloned",
  "serialized",
  "cursor",
] as const;

/** Ordered transaction-mode coverage against PostgreSQL or the lossless lowering contract. */
export const TRANSACTION_MODE_ORDER_STATES = [
  "top level",
  "savepoint",
  "after query",
  "already read only",
  "begin read only",
  "begin read write",
  "execute",
  "compiled",
  "accepted",
  "refused",
  "duplicates",
] as const;

/** PostgreSQL CTE refcounts count unused referrers, but respect the enclosing query scope. */
export const ROW_LOCK_GRAPH_STATES = [
  "empty view catalog",
  "nonempty view catalog",
  "recursive base",
  "recursive step",
  "recursive both",
  "recursive none",
  "recursive unused",
  "recursive chain",
  "recursive consumed",
  "recursive nested counted",
  "recursive discarded scope",
  "referenced by unused",
  "unreferenced",
  "direct",
  "view",
  "nested view",
  "view graph",
  "materialized",
  "not materialized",
  "chain",
  "multiple references",
  "strongest final clause",
  "point read",
  "scalar view",
  "dead expression",
  "nested scope counted",
  "nested scope discarded",
] as const;

/** PostgreSQL variable.c admission guards are per attribute and preserve idempotent isolation. */
export const TRANSACTION_ATTRIBUTE_STATES = [
  "begin modes",
  "set modes",
  "before query",
  "after query",
  "top level",
  "savepoint",
  "release",
  "rollback to",
  "same isolation",
  "changed isolation",
  "same deferrable",
  "changed deferrable",
  "same access",
  "changed access",
  "earlier write",
  "execute",
  "compiled",
  "accepted",
  "refused",
  "duplicate item",
  "mixed attributes",
  "isolation query refusal",
  "isolation savepoint refusal",
  "deferrable query refusal",
  "deferrable savepoint refusal",
] as const;

/** Production Gate A executor bridge over real transfers and delayed cumulative credits. */
export const EXECUTOR_EXPORT_REACH = [
  "production executor preparation",
  "multiple source groups",
  "sparse nulls",
  "out of line >64KiB",
  "exact transferred values",
  "task-delayed cumulative credit",
  "source survives transfer",
  "execution charges returned",
] as const;

/** Stratified terminal full export and nonterminal/subset/filter policy checks. */
export const EXECUTOR_POLICY_REACH = {
  full: "terminal one-pass window",
  cached: "selective/consumer cache policy",
} as const;

/** Production dictionary frames remain exact through stream deltas and real detachment. */
export const RESULT_DICTIONARY_REACH = {
  equality: "dictionary client equality and returned charges",
  codes: "stream dictionary codes and owned deltas",
  fallback: "stream dictionary cap fallback",
  loan: "dictionary delta progress loan repaid before finish",
  predecode: "dictionary wire form chosen before first frame",
  abandoned: "unpublished dictionary reservation returned on cancel",
} as const;

/** R8: a terminal result adopts authorized dedicated output, never a page or pooled slab. */
export const RESULT_OWNED_REACH = {
  equality: "source and transferred values equal with returned charges",
  cache: "no cache or pooled page buffer transferred",
  adopt: "dedicated result frame charge held until credit",
  copy: "unmarked foreign selected or capped input copied",
  refusal: "terminal dedicated refusal falls back to exact borrowed output",
  capped: "capped dictionary copied beside adopted fixed buffers",
  dictCodes: "source decodes an admitted dictionary to owned codes",
  dictFlat: "source decodes a refused dictionary to owned UTF-8",
} as const;

/** Every native owned layout, including floating point payload words, survives transfer exactly. */
export const RESULT_OWNED_LAYOUT_REACH = [
  "bool",
  "i32",
  "f32",
  "f64",
  "i64-safe",
  "i64-wide",
  "fixed16",
  "interval",
  "text",
  "decimal-safe",
  "decimal-i64",
  "decimal-i128",
  "decimal-mixed",
] as const;

/** Discard-only benchmark receiver: columns end at consume, dictionaries at stream close. */
export const DISCARD_RECEIVER_REACH = [
  "disposed columns",
  "live dictionary",
  "sparse",
  "all valid",
  "empty frame",
] as const;

/** Larger direct terminal batches keep exact values and the existing owner credit bound. */
export const TERMINAL_BATCH_REACH = [
  "all valid",
  "sparse NULLs",
  "owned frame above 1MiB",
  "unchanged credit bound",
  "exact received values",
  "small credit cap",
] as const;
