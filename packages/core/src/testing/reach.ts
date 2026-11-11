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

/** Fixed run arenas may forget holes, but cannot reuse unknown or live bytes. */
export const BOUNDED_FREE_ARENA_REACH = [
  "overflow",
  "unknown",
  "tracked first fit",
  "coalesce",
  "reopen",
  "closed",
] as const;

/** Private metadata and operator spill have independent lifetimes under the same fence. */
export const METADATA_STORE_REACH = [
  "rewrite",
  "spill sweep isolation",
  "metadata reset isolation",
  "fenced",
  "replacement reset",
] as const;

/** Moving an existing allocation changes ownership, never capacity or the progress reserve. */
export const CACHE_TRANSFER_REACH = [
  "at capacity",
  "queued admission unchanged",
  "loan capture refused",
  "ordinary transfer beside loan",
  "both owners close",
] as const;

/** A pin-delayed journal backlog changes disk pages, never resident frame capacity. */
export const METADATA_JOURNAL_REACH = [
  "multi page input",
  "cursor protects released owner",
  "constant time chain reuse",
  "disk queue backlog",
  "admitted before frames",
  "close returns charge",
] as const;

/** Pair streams keep the same memory reservation across external sort and list set operations. */
export const METADATA_EXTENTS_REACH = [
  "extended cancellation history",
  "external passes",
  "odd external tail",
  "exact duplicates coalesce",
  "difference",
  "union",
  "wide offsets",
  "fixed admission",
] as const;

/** Every scripted private-journal write failure must preserve its cause and return memory. */
export const METADATA_WRITE_FAULT_REACH = ["native error preserved", "zero final charge"] as const;

/** Ownership transitions wait across native publication, while synchronous reservations progress. */
export const STORAGE_MUTATION_REACH = [
  "reservations during publish",
  "abort held",
  "discard held",
  "seal held",
  "transitions drain",
  "audit after interleaving",
] as const;

/** Native record clipping and list reads stay inside the prepaid response/copy allowance. */
export const STORAGE_BOUNDED_IO_REACH = [
  "one boundary response",
  "bounded copy",
  "serial record read",
  "outside bytes preserved",
  "lazy",
  "eager",
] as const;

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

/** XXH32 specialization must retain reference bytes across length, backing, and range offsets. */
export const XXHASH32_REACH = [
  "byte reads",
  "word reads",
  "word reads at an odd byte offset",
  "a range ending at its buffer's end",
  "short key other than five",
  "five byte key",
  "five byte key with shifted backing and range",
  "reused view short words",
  "reused view long words",
  "reused view odd offset",
  "reused view byte tail",
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
 * The concurrent kill campaign on IndexedDB: a seeded delay arms a native-request gate; kills land with a platform
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
export const OUTPUT_CLASS_SCAN_REACH = [
  "bucket bounded scan",
  "exact bounded scan",
  "bounded padding",
] as const;

export const OUTPUT_BUCKET_POOL_REACH = [
  "held is never reused",
  "release after close",
  "neighbor size reused",
  "slack charged",
  "tight views only",
  "all charges released",
] as const;

export const OUTPUT_CLASS_REACH = [
  "exact output",
  "bucketed output",
  "charged zero padding",
] as const;

export const SCAN_SHAPE_REACH = [
  "sparse NULLs in every layout",
  "recycled native bytes",
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

/**
 * RANGE offset frames over non-integer keys against PostgreSQL (next/testing/
 * lowering-pglite-review.test.ts, review B6-p2b-3 finding 1): exact NUMERIC around 2^53 and
 * with fractions, NUMERIC NaN / ±Infinity peers, float ±Infinity and NaN peers, and temporal
 * keys with interval offsets (refused, 0A000, until lowering takes them).
 */
export const RANGE_KEY_STATES = [
  "numeric exact",
  "numeric special",
  "float infinite",
  "float NaN",
  "temporal refused",
] as const;

/** Frame offset errors against PostgreSQL (review B6-p2b-3 finding 3). */
export const FRAME_OFFSET_STATES = [
  "accepted",
  "null offset",
  "negative offset",
  "refused",
  // Review B6-p2b-4: a minus sign on zero is zero; ROWS / GROUPS round decimals to bigint; a
  // decimal on an integer RANGE key is PostgreSQL's 0A000.
  "signed zero accepted",
  "rounded offset accepted",
  "numeric offset on an integer key",
  // Review B6-p2b-5: a ROWS / GROUPS offset past bigint (1e400) is 22003.
  "bigint out of range",
  // Review B6-p2b-6: the signed bigint range exactly: 2^63 - 1 runs, -2^63 is negative (22013).
  "bigint maximum accepted",
  "bigint minimum negative",
] as const;

/** HAVING names against PostgreSQL (review B6-p2b-3 finding 3). */
export const HAVING_NAME_STATES = [
  "accepted",
  "unknown column",
  "ungrouped column",
  "unknown table",
] as const;

/**
 * NULL keys in RANGE offset frames against PostgreSQL (next/testing/lowering-pglite-review.test.ts,
 * the B7 report): for a non-NULL current row, NULLs sort below every key (NULLS FIRST) or above
 * (NULLS LAST), so a frame that runs to that end includes them; a NULL row's frame is its NULL
 * peers.
 */
export const RANGE_NULL_STATES = [
  "nulls first in a frame",
  "nulls last in a frame",
  "nulls outside a frame",
  "null row",
] as const;

/** Exact NUMERIC window distances, including parser clones and expression identity. */
export const RANGE_DECIMAL_STATES = [
  "fractional distance",
  "2^53 distance",
  "2^63 distance",
  "direct frame",
  "CTE clone",
  "derived frame",
  "adjacent frame identity",
] as const;

/** Private frame metadata must survive the parser's plan-copy boundaries. */
export const FRAME_METADATA_STATES = [
  "exact ROWS",
  "exact GROUPS",
  "exact RANGE",
  "numeric RANGE refusal",
  "direct",
  "CTE",
  "derived",
] as const;

/** Planned locking reads: relation targets, CTE boundaries, expanded views and strongest intent. */
export const ROW_LOCK_INTENT_STATES = [
  "unused stronger CTE",
  "base relation",
  "FROM propagation",
  "constant source",
  "CTE boundary",
  "CTE body",
  "unused CTE",
  "OF target",
  "view clause",
  "nested view",
  "view target",
  "strongest",
] as const;

/** Ordered transaction-mode coverage against PostgreSQL or the lossless lowering contract. */
export const TRANSACTION_MODE_ITEM_STATES = [
  "empty",
  "access",
  "isolation",
  "deferrable",
  "duplicates",
  "mixed kinds",
  "begin",
  "start",
  "set",
] as const;

/** Exact basic NUMERIC operations in the reference, checked as text against PostgreSQL. */
export const REFERENCE_DECIMAL_ARITH_STATES = [
  "add",
  "sub",
  "mul",
  "neg",
  "abs",
  "finite",
  "special",
  "zero",
  "large",
  "fractional",
] as const;

/**
 * Literal auto-parameterization over integer boundaries (next/testing/autoparameterize.test.ts):
 * a literal lifted into an int4 or int8 slot, a family refused (the original text lowers alone),
 * and a literal the scanner leaves as written (past int8). Reachable before and after integer
 * literals past 2^53 lower as int8.
 */
export const AUTOPARAMETERIZE_LITERAL_STATES = [
  "lifted int4",
  "lifted int8",
  "refused",
  "not lifted",
] as const;

/** Every lock intent survives the parameterized point-read cache. */
export const ROW_LOCK_CACHE_STATES = [
  "none",
  "keyShare",
  "share",
  "noKeyUpdate",
  "update",
] as const;

/** BOOL's Uint8-backed keys exercise the direct table's full logical domain. */
export const JOIN_BOOLEAN_REACH = [
  "inner",
  "left",
  "right",
  "full",
  "semi",
  "anti",
  "mark",
  "TRUE only",
  "FALSE only",
  "both",
  "NULL",
  "empty",
  "direct",
  "hashed",
  "released",
] as const;

/** Invalid bound-kind order differs from a legal same-kind frame with no members. */
export const WINDOW_FRAME_ORDER_REACH = [
  "shorthand",
  "ROWS",
  "RANGE",
  "GROUPS",
  "current then preceding",
  "following then current",
  "following then preceding",
  "allowed empty",
] as const;

/** M3 joins through actual storage/execution and both independent SQL engines. */
export const LOWER_M3_JOINS_REACH = [
  "keyless full",
  "key-only",
  "zero-column",
  "USING",
  "USING chain",
  "NATURAL",
  "inner",
  "left",
  "right",
  "full",
  "cross",
  "residual",
  "outer residual",
  "full residual",
  "null safe",
  "outer WHERE",
  "semi",
  "anti",
  "IN",
  "NOT IN",
  "correlated NOT IN",
  "mark",
  "EXISTS mark",
  "star",
  "uncorrelated EXISTS",
  "uncorrelated anti",
  "limited NOT IN",
  "OR EXISTS",
  "empty probe",
  "empty build",
  "NULL",
  "duplicates",
  "flushed",
  "reopened",
  "complete",
] as const;

/** SQL windows run through durable storage and the actual executor, not a plan interpreter. */
export const LOWER_M3_WINDOWS_REACH = [
  "literal window amounts",
  "duplicate window inputs",
  "min max",
  "integer average",
  "text order",
  "row_number",
  "rank",
  "running sum",
  "lag lead",
  "moving avg",
  "ranking",
  "value functions",
  "default peers",
  "whole partition",
  "different sorts",
  "named",
  "ROWS",
  "RANGE",
  "GROUPS",
  "empty",
  "singleton",
  "NULL peers",
  "flushed",
  "reopened",
  "complete",
] as const;

/** Exact key families, coefficients, non-finite RANGE and parser-owned offset precision. */
export const LOWER_M3_TYPES_REACH = [
  "int64 RANGE",
  "float4 offset",
  "outer chain",
  "INNER",
  "LEFT",
  "RIGHT",
  "FULL",
  "CROSS",
  "k",
  "t",
  "f",
  "n",
  "x",
  "IN",
  "NOT IN",
  "exact offsets",
  "parameters",
  "non-finite RANGE",
  "wide sum and lag",
  "invalid offsets",
  "released",
] as const;

/** Startup checks differ from RANGE's comparison-time sign check and optimized ranking. */
export const LOWER_M3_OFFSETS_REACH = [
  "reference startup",
  "empty",
  "NULL only",
  "mixed",
  "non-null",
  "ROWS",
  "RANGE",
  "GROUPS",
  "literal",
  "parameter",
  "PRECEDING",
  "FOLLOWING",
  "NULL offset",
  "NaN offset",
  "negative offset",
  "aggregate",
  "navigation",
  "optimized ranking",
  "LIMIT 0",
  "released",
] as const;

/** Empty input and NULL-only keys do not invoke RANGE's sign-checking support function. */
export const WINDOW_OFFSET_STARTUP_REACH = [
  "resident",
  "streamed",
  "empty",
  "NULL only",
  "NULL 22004",
  "NaN accepted",
  "negative accepted",
  "PRECEDING",
  "FOLLOWING",
  "aggregate",
  "navigation",
  "released",
] as const;

/** Cross-width btree in_range overloads, with exact small COUNT outputs against PostgreSQL. */
export const REFERENCE_RANGE_TYPES_REACH = [
  "SMALLINT",
  "INTEGER",
  "REAL",
  "literal",
  "parameter",
  "ASC",
  "DESC",
  "FIRST",
  "LAST",
  "NULL key",
  "rows match",
] as const;

/** Exact NUMERIC literals and float conversion errors across planning and startup boundaries. */
export const RANGE_LITERAL_WIDTH_REACH = [
  "adjacent frames",
  "cloned frames",
  "NUMERIC",
  "NUMERIC(12,2)",
  "DOUBLE PRECISION",
  "empty",
  "NULL only",
  "values",
  "count(*)",
  "row_number()",
  "evaluated",
  "LIMIT 0",
  "typmod distance",
  "tiny or negative",
  "large",
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

/** Worker-local graceful storage-campaign stop boundaries. */
export const STORAGE_STEP_DOWN_STATES = [
  "paused log lane",
  "admitted append",
  "go in transit",
] as const;

/** Native-request parks must be real, correctly filtered and after a fully posted prefix. */
export const STORAGE_REQUEST_GATE_REACH = [
  "request-after-body-issue",
  "prefix-fully-posted",
  "queued-write-native-batch",
  "no-credit-for-ended-request",
  "no-credit-for-window",
  "concurrent-kind-and-two-windows",
  "bounded-park-wake-never-resumes-storage",
] as const;

/** Controller-side native gate histories, including empty attempts and port teardown. */
export const STORAGE_PHASE_CONTROLLER_REACH = [
  "short-request-phase-parked",
  "no-request-occurrence-advanced",
  "parked-prefix-drained-before-teardown",
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
  "ordinary target unchanged",
  "all valid",
  "sparse NULLs",
  "owned frame above 1MiB",
  "unchanged credit bound",
  "exact received values",
  "small credit cap",
] as const;

/** Frozen rejected packed-frame diagnostic: native bytes, dictionaries, malformed views and cleanup. */
export const PACKED_FRAME_REACH = [
  "all valid",
  "sparse NULLs",
  "large varlen",
  "dictionary delta",
  "dictionary append",
  "one fresh buffer",
  "foreign",
  "overlap",
  "misaligned",
  "out of bounds",
  "wrong type",
  "short lane",
  "complete",
  "abort",
  "cancel",
] as const;

/** Explicit output relinquishment and one-use return capabilities. */
export const OUTPUT_RETURN_REACH = [
  "held",
  "released",
  "reused",
  "foreign",
  "not detached",
  "wrong size",
  "replay",
  "detached reuse refused",
  "late",
  "cap",
  "closed",
] as const;

export const EXPORT_RETURN_REACH = [
  "explicit frame return",
  "credit before frame return",
  "cancel while retained",
  "late return after cancel",
] as const;

export const OUTPUT_RETURN_PRESSURE_REACH = [
  "stale size replaced",
  "pressure close returns charges",
] as const;

export const OWNER_EXPORT_REACH = [
  "credit independent of return",
  "ordinary lifetime",
  "held after credit",
  "release after cancel",
  "early close",
  "release after reconnect",
  "release after owner change",
  "lost return does not block",
  "detached across reuse",
  "double release",
  "small frames",
  "large frames",
] as const;

export const OWNER_EXPORT_DATA_REACH = {
  real: ["real factory and SQL", "wide int64 and large text", "sparse source", "all-valid source"],
  dictionary: ["compacted dictionary delta", "later dictionary append"],
  views: ["first view partial", "later alias partial"],
  preconnect: ["close before connect", "cancel before connect", "concurrent next refused"],
} as const;

/** The owner cache exercises successful families, refusal fallbacks, binding and bounded retention. */
export const OWNER_PLAN_CACHE_STATES = [
  "integer template",
  "text template",
  "marker fallback",
  "transaction query",
  "bound write",
  "client parameters",
  "cache eviction",
] as const;

/** Owner plan retention is bounded by bytes, reclaimable, and LRU across all entry kinds. */
export const OWNER_PLAN_CACHE_POLICY_STATES = [
  "raw charged",
  "template charged",
  "marker charged",
  "oversized refused",
  "byte eviction",
  "governor reclaim",
  "close releases",
  "raw LRU",
  "template LRU",
  "mixed LRU",
] as const;

/** Private uncommitted-schema plans carry the same accounting and end with their transaction. */
export const OWNER_PRIVATE_PLAN_CACHE_STATES = [
  "private charged",
  "private rollback",
  "private commit",
  "private close",
] as const;

/** PG per-attribute mode transitions, statement attempts, savepoints and transaction cleanup. */
export const OWNER_TRANSACTION_MODE_STATES = [
  "read-only begin",
  "read-write begin",
  "read-only refusal",
  "mode refusal",
  "query work",
  "modify work",
  "ddl work",
  "show work",
  "constant snapshot",
  "scan snapshot",
  "point snapshot",
  "set read-only",
  "set read-write",
  "isolation metadata",
  "deferrable",
  "not deferrable",
  "commit reset",
  "rollback reset",
  "rollback modes",
  "release modes",
  "last read-only wins",
  "last read-write wins",
  "write prefix retained",
  "rollback snapshot boundary",
  "failed write snapshot",
] as const;

/** Sequence effects are checked when executed; session reads and unexecuted calls stay allowed. */
export const OWNER_SEQUENCE_EFFECT_STATES = [
  "currval",
  "nonexecuted inverse parameter",
  "nextval",
  "nonexecuted filter",
  "nonexecuted case",
  "executed case",
  "nonexecuted parameter",
  "executed parameter",
  "read-only effects",
  "read-write effects",
  "savepoint effects",
  "prepared effects",
  "direct effects",
  "sequence metadata",
] as const;

/** Effective lock admission after expansion, including cached and synchronous owner paths. */
export const OWNER_ROW_LOCK_STATES = [
  "direct",
  "point",
  "empty",
  "nested",
  "nested-body",
  "view",
  "view-body",
  "cte-body",
  "cte-reference",
  "unused-cte",
  "cte-graph-one",
  "cte-graph-two",
  "recursive-base",
  "recursive-step",
  "constants",
  "cold locking refusal",
  "cached locking refusal",
  "normal point refusal",
  "synchronous point refusal",
  "UPDATE",
  "NO KEY UPDATE",
  "SHARE",
  "KEY SHARE",
] as const;
export const OWNER_MODE_ORDER_STATES = [
  "BEGIN",
  "START TRANSACTION",
  "SET TRANSACTION",
  "readOnly then readWrite",
  "readWrite then readOnly",
  "root SET",
  "one savepoint SET",
  "nested savepoint SET",
  "read-only SET entry",
  "read-write SET entry",
  "readOnly then readWrite under writable savepoint",
  "readWrite then readOnly under writable savepoint",
  "readOnly then readWrite under read-only savepoint",
  "readWrite then readOnly under read-only savepoint",
] as const;

/** Executor benchmark inputs retain charged encoded roots and storage lease cleanup. */
export const BENCH_ROOT_ADMISSION_REACH = [
  "single group",
  "multiple groups",
  "warm charged parse",
  "cold charged parse",
  "parse refusal cleanup",
  "mixed lease",
  "payload refusal cleanup",
  "cancelled read",
] as const;

/** Production API benchmark fixture stays on real SQL/owner transport, across repeated queries. */
export const OWNER_API_BENCH_REACH = [
  "real SQL identity projection",
  "wire totals",
  "first export",
  "same connection export",
  "sparse NULLs",
  "all valid",
] as const;

export const OWNER_API_ENVELOPE_REACH = ["rows envelope", "done envelope"] as const;

/** Native R25 list pages are decoded from paid slab views, without per-extent objects. */
export const NATIVE_EXTENT_LIST_REACH = [
  "shared prepaid page windows",
  "shared page descriptor validation",
  "full safe integer offsets",
  "coalesced page reads",
  "synchronous scalar pairs",
] as const;

/** R8's queue grows on disk and frees bounded batches without publication-time admission. */
export const RETIREMENT_TICKETS_REACH = [
  "pin delayed disk backlog",
  "source owners released",
  "full ordinary capacity",
  "caller loan remains held",
  "next generation guard",
  "two bounded batches per ticket",
] as const;

/** Publication/free progress with a caller that releases ordinary/loan holdings only afterwards. */
export const STORAGE_PRESSURE_REACH = [
  "pinned retirement backlog",
  "no publication admission",
  "caller loan held",
  "abort completes at full cap",
  "audit after reclamation",
] as const;

/** Every native I/O/task suspension of final validation/publication admits additions, never frees. */
export const STORAGE_FINAL_HISTORY_REACH = [
  "tracked hole consumed",
  "monotonic open and extend",
  "publishing writer rejects additions",
  "queued owners retain records",
  "task suspension",
  "metadata suspension",
  "native publish suspension",
  "audit after all orderings",
] as const;

export const FLUSH_ROLLBACK_REACH = [
  "flush held before publication",
  "rollback removes the new tail",
  "later committed row survives publication and reopen",
] as const;

export const ROOT_CREDIT_REACH = [
  "five overlapping root frames prepaid",
  "large frame credit and encoded compatibility",
  "refusal keeps owned roots and charge intact",
  "independent descriptor accounting",
  "late snapshot release refunds its owner",
] as const;

export const STORAGE_SERIAL_INDEX_REACH = [
  "bounded native index page",
  "multiple index pages",
  "exact tiny-record image",
] as const;

/** Seed598's real loads keep idle ordinary estimates while free-map mutation completes. */
export const FREE_MAP_PRESSURE_REACH = [
  "memory",
  "indexeddb",
  "startup returned",
  "startup retained",
  "one byte ordinary headroom",
  "caller loan held",
  "real merge completed",
  "abort completed",
  "durable reopen and audit",
  "closed without charge",
] as const;

/** Native open uses immutable list pages directly; their credit survives startup lifetime. */
export const NATIVE_OPEN_REACH = [
  "no payload materialization",
  "startup closed with charged descriptors",
  "caller loan held",
  "audit first",
  "unused close has no payload copy",
  "first publication and reclaim",
  "reopen and audit",
  "full safe-integer coordinates",
  "unordered physical allocations",
  "all credit returned",
] as const;

/** Each original crash-sweep seed must finish every backend/placement combination. */
export const SPACE_CRASH_SWEEP_REACH = [
  "files/growing",
  "files/relocating",
  "IndexedDB/growing",
  "IndexedDB/relocating",
] as const;

/** Page-close expectations include every ack, even when recovery precedes several publishes. */
export const STORAGE_PAGE_BOUNDARY_REACH = [
  "waits for the armed acknowledgement",
  "next intent held",
  "one root since recovery",
  "two roots since recovery",
] as const;

/** B4's publication-stable memory provider, independently bounded before value I/O. */
export const MEMORY_READ_BOUND_STATES = [
  "single-slot wide resident",
  "single-slot wide repointed",
  "exact int128 to wide repro",
  "consumed before publication",
  "resumed after publication",
  "multi blob resumed",
  "retain pin during read",
  "release pin after read",
  "wide decimal destination",
  "native decimal destination",
  "decimal destination repointed",
  "decimal destination resident",
  "negative destination coefficient",
  "nullable decimal destination",
  "decimal capacity continuation",
  "wide destination independently charged",
  "text",
  "bytes",
  "json",
  "array",
  "int64",
  "decimal",
  "interval",
  "float64",
  "uuid",
  "resident",
  "repointed",
  "lazy root",
  "invisible suffix",
  "no sizing borrow",
  "byte prefix",
  "row prefix",
  "oversized row",
  "multi blob",
  "mixed columns",
  "deletions",
  "recovered",
  "precharged native copy",
  "staged",
  "frozen",
  "task clock",
  "cancel chunk",
  "release chunk",
  "cancel sizing",
  "release sizing",
] as const;

/** Catalogue order checks preserve peers but reject reordered distinct key groups. */
export const CATALOGUE_ORDER_REACH = [
  "ascending",
  "descending",
  "multiple keys",
  "ordinal",
  "null keys",
  "ties",
  "window order only",
] as const;

/** Ordered-set SQL is checked against independent PostgreSQL and shipped-engine outcomes. */
export const ORDERED_SET_REACH = [
  "percentile_cont",
  "percentile_disc",
  "mode",
  "mode tie",
  "interpolation",
  "integer",
  "double precision",
  "numeric(12,2)",
  "bigint",
  "grouped",
  "scalar",
  "ascending",
  "descending",
  "empty input",
  "null values",
  "null group",
  "null fraction",
  "zero fraction",
  "one fraction",
  "range error",
] as const;

export const ORDERED_SET_MEMORY_REACH = [
  "single group",
  "many groups",
  "multiple ordered calls",
  "percentile_cont",
  "percentile_disc",
  "mode",
  "cancelled append",
  "pending bytes retained",
  "backpressure",
  "task clock",
  "real clock",
  "spill",
  "zero leak",
] as const;

/** PostgreSQL numeric_round across storage tiers, vector forms, scales and precision limits. */
export const NUMERIC_ROUND_REACH = [
  "safe precision boundary",
  "sixteen-digit reduction",
  "special NULL wins",
  ...Array.from({ length: 26 }, (_, index) => `scale ${String(index - 5)}`),
  "safe",
  "int64",
  "int128",
  "wide",
  "flat",
  "dictionary",
  "constant",
  "selected",
  "all rows",
  "null input",
  "null scale",
  "null garbage",
  "unselected garbage",
  "positive",
  "negative",
  "positive tie",
  "negative tie",
  "NaN",
  "Infinity",
  "-Infinity",
  "1000-digit precision",
  "largest integer precision",
  "high scale clamp",
  "low scale clamp",
  "overflow",
  "reused output",
  "uniform input",
] as const;

/** Unary numeric spelling must be typed before overload resolution, arithmetic after it. */
export const SIGNED_LITERAL_REACH = [
  "cached int4",
  "cached int8",
  "unlifted expression",
  "cached int4 minimum",
  "cached int8 minimum",
  "bare",
  "parenthesized",
  "multiply",
  "subtract",
  "double sign",
  "cast",
  "integer spelling",
  "decimal spelling",
  "int4 boundary",
  "int8 boundary",
  "ROUND rows",
  "overload refusal",
] as const;

/** DATE constants preserve the complete finite range and sentinel values before execution. */
export const DATE_LITERAL_REACH = [
  "finite",
  "BC parser",
  "maximum parser",
  "before epoch",
  "at or after epoch",
  "minimum",
  "maximum",
  "leap day",
  "infinity",
  "-infinity",
] as const;

/** Calendar units and active-vector paths for timestamp DATE_TRUNC in fixed UTC. */
export const DATE_TRUNC_REACH = [
  "microseconds",
  "milliseconds",
  "second",
  "minute",
  "hour",
  "day",
  "week",
  "month",
  "quarter",
  "year",
  "decade",
  "century",
  "millennium",
  "timestamp",
  "timestamptz",
  "flat",
  "dictionary",
  "constant",
  "selected",
  "all rows",
  "null garbage",
  "NULL unit",
  "unselected garbage",
  "safe tier",
  "int64 tier",
  "infinity",
  "before epoch",
  "int64 precision",
  "aliases or invalid units",
  "rows",
  "22008",
  "22023",
  "0A000",
] as const;

/** SQL literals retain source fractions before the legacy Date boundary, in every scope. */
export const TIMESTAMP_LITERAL_REACH = [
  "fraction 0",
  "fraction 1",
  "fraction 2",
  "fraction 3",
  "fraction 4",
  "fraction 5",
  "fraction 6",
  "direct",
  "derived",
  "CTE",
  "view",
  "grouped literals",
  "aggregated literals",
  "default SQL",
  "before PG epoch",
  "at or after PG epoch",
  "sub-millisecond",
  "trailing fraction zeros",
  "positive day difference",
  "negative day difference",
  "microsecond comparison",
  "leap day",
  "day carry",
  "wide timestamp",
  "ignored nonzero literal zone",
  "UTC or absent literal zone",
] as const;

/** Typemod admission must not silently turn native microseconds into an unrounded timestamp(p). */
export const TIMESTAMP_PRECISION_REACH = [
  "precision 0",
  "precision 1",
  "precision 2",
  "precision 3",
  "precision 4",
  "precision 5",
  "precision 6",
  "precision 7",
  "target 0",
  "target 1",
  "target 2",
  "target 3",
  "cast",
  "column",
  "rounding refused",
  "full precision executes",
  "full precision column",
] as const;

/** SQL output functions preserve temporal fields and only evaluate active non-NULL rows. */
export const TEMPORAL_TEXT_REACH = [
  "date",
  "timestamp",
  "timestamptz",
  "interval",
  "flat",
  "dictionary",
  "constant",
  "selected",
  "all rows",
  "sparse nonascending selection",
  "NULL garbage",
  "constant NULL",
  "unselected garbage",
  "safe timestamp",
  "wide timestamp",
  "infinities",
  "BC",
  "mixed signs",
] as const;

/** Native date/timestamp differences preserve PG interval fields, strictness and errors. */
export const TEMPORAL_SUB_REACH = [
  "date",
  "timestamp",
  "timestamptz",
  "flat",
  "dictionary",
  "constant",
  "selected",
  "all rows",
  "strict NULL",
  "constant NULL",
  "unselected garbage",
  "safe timestamp",
  "wide timestamp",
  "rows",
  "22008",
  "infinite interval refused",
  "positive infinity refused",
  "negative infinity refused",
] as const;

/** Literal JSON_TABLE desugaring keeps row order, multiplicity, NULLs and text exact. */
export const JSON_TABLE_VALUES_REACH = [
  "empty",
  "one row",
  "multiple rows",
  "NULL text",
  "NULL integer",
  "duplicates",
  "escaped text",
  "negative integer",
] as const;

/** JSON_TABLE NULL values carry declarations before UNION ALL resolves unknown constants. */
export const JSON_TABLE_NULL_REACH = [
  "smallint",
  "integer",
  "bigint",
  "real",
  "double precision",
  "boolean",
  "text",
  "numeric",
  "date",
  "timestamp without time zone",
  "timestamp with time zone",
  "empty",
  "single NULL",
  "NULL-only prefix",
] as const;

/** Native groups acquire one complete minimum; moving its ordinary credit never re-admits. */
export const NATIVE_CREDIT_STATES = [
  "private progress group",
  "transfer keeps queued admission waiting",
  "named native split",
  "same owner transfer",
  "live allocation protected",
  "loan credit protected",
  "foreign owner refused",
  "closed side refused",
  "close returns exact holdings",
  "queued group holds nothing",
  "zero group takes no credit",
  "queued cancellation",
  "queued request snapshot",
  "invalid credit rejected",
  "invalid group rejected",
  "served cancellation",
] as const;

/** Required in the B1 integration, whose cache can take allocated native credit. */
export const NATIVE_CREDIT_CACHE_STATES = [
  "takeover owns real buffer",
  "transfer after takeover",
  "close leaves retained owner paid",
] as const;

/** Append reuse changes allocation cost, never the simulated disk's physical fault law. */
export const SIM_DISK_APPEND_REACH = [
  "amortized append growth",
  "planned physical crash",
  "overwrite truncate and gap",
  "recovery forks before an old suffix",
  "retained immutable views",
  "honest flush",
  "lying flush",
] as const;

/** Rolled-back staging cannot survive in a table retained by a held catalog checkpoint. */
export const STAGING_DROP_RETIREMENT_REACH = [
  "sim",
  "memory",
  "rollback",
  "held build",
  "dropped table retained",
  "exact retained root",
  "reopen",
] as const;

/** The same FLOAT8 input stream must keep its order across every batch partition. */
export const FLOAT_SUM_BOUNDARIES_REACH = [
  "batch 1",
  "batch 17",
  "batch 511",
  "batch 2048",
  "unequal boundaries",
  "stable group sorting",
  "input-order updates",
  "zero cancellation",
  "unit cancellation",
  "5000 groups",
] as const;

/** Run Bloom arithmetic preserves the stored probes across bitmap sizes and unsigned hashes. */
export const RUN_BLOOM_STEP_STATES = [
  "minimum bitmap",
  "rounded bitmap",
  "unsigned high hash",
  "wrapped step",
] as const;

/** Feasible durable writes under small caps must share idle holdings with maintenance. */
export const WRITE_LOW_CAP_STATES = [
  "memory",
  "indexeddb",
  "startup returned",
  "startup retained",
  "active progress loan",
  "idle estimate headroom",
  "merge transfers idle row credit under global pressure",
  "merge completed",
  "metadata grew under pressure",
  "durable reopen",
  "allocated write above minimum",
  "commit buffers independently charged",
] as const;

/** B3: low-cap commit encoding owns a bounded, independently counted set of actual buffers. */
export const WRITE_COMMIT_BUFFER_STATES = [
  "request metadata",
  "value over 64 KiB",
  "memory",
  "indexeddb",
  "catalog and rows together",
  "streamed body",
  "single-buffer body",
  "strict",
  "relaxed",
  "durable reopen",
  "ownership released once",
  "streamed catalog and bigint tags",
  "observed constructor precharge",
  "production constructor precharge",
] as const;

/** B3: physical power loss at multipart prefixes and the complete but unacknowledged body. */
export const WRITE_STREAM_CRASH_STATES = [
  "strict",
  "relaxed",
  "prefix",
  "complete",
  "retry and reopen",
] as const;

/** B3: bounded canonical row-id ordering of a large deletion body. */
export const WRITE_DELETE_STREAM_STATES = [
  "reverse",
  "zigzag",
  "charged order loan",
  "bounded rank walk",
  "empty after reopen",
] as const;

/** B3: the bounded byte sink preserves all supported frame-column representations. */
export const WRITE_COLUMN_WINDOW_STATES = [
  "bool",
  "f32",
  "f64",
  "int64",
  "decimal64",
  "decimal128",
  "fixed16",
  "interval",
  "short range",
  "long range",
  "signaling f32",
] as const;

/** B3: string metadata uses owned UTF-8 storage or bounded streaming before stringification. */
export const WRITE_REQUEST_BUFFER_STATES = [
  "streamed metadata",
  "single-buffer metadata",
  "sequence UTF-8",
  "no unowned string buffer",
] as const;

/** B3/B5: async sequence callers stop at cancellation before continuing batched inputs. */
export const OWNER_SEQUENCE_PRESSURE_STATES = [
  "plain sequence expression",
  "case then sequence expression",
  "case else sequence expression",
  "last value",
  "remaining values",
  "no later sequence call",
  "statement rolled back",
] as const;

/** B3: one oversized input call admits bounded pieces without changing statement atomicity. */
export const WRITE_BATCH_ADMISSION_STATES = [
  "memory",
  "indexeddb",
  "startup retained",
  "other live working memory",
  "oversized input",
  "staged within call",
  "durable reopen",
] as const;

/** B3 review: transient reader memory must delay metadata publication, never poison it. */
export const WRITE_METADATA_PRESSURE_STATES = [
  "memory",
  "indexeddb",
  "reader owns real bytes",
  "metadata waits",
  "reader released",
  "flush published",
  "staging published",
  "merge completed",
  "durable reopen",
] as const;

/** R23.5 / key-index chain ordering: a popular non-unique key can fill the whole memtable. */
export const WRITE_HOT_KEY_STATES = [
  "ascending",
  "descending",
  "mixed",
  "row-id ties",
  "full memtable chain",
  "filtered chain",
  "task yields",
  "short traversals accumulate",
] as const;

/** Write rule 4: actual native capacities, overlap, refusal and suspension lifetimes. */
export const WRITE_DELTA_ACCOUNTING_STATES = [
  "constructor precharge",
  "initial constructor failure",
  "order buffer",
  "chain growth overlap",
  "full memtable",
  "visitor failure",
  "range cache",
  "range replacement overlap",
  "denial before allocation",
  "retained growth overlap",
  "constructor failure refund",
  "last entry array failure",
  "last key array failure",
  "release while suspended",
  "publication workspace pressure",
] as const;

/** An unfunded nonempty range is unknown, never evidence that a layer is empty. */
export const WRITE_RANGE_PRESSURE_STATES = [
  "memory",
  "indexeddb",
  "single row",
  "batch rows",
  "single conflict",
  "batch conflict",
  "accepted fresh keys",
  "nonempty range unavailable",
  "publication",
  "durable reopen",
] as const;

/** A completed asynchronous read must advance rather than require a synchronous cache replay. */
export const WRITE_ASYNC_PROBE_STATES = [
  "memory",
  "indexeddb",
  "unique",
  "nonunique",
  "asynchronous metadata",
  "asynchronous blocks",
  "output continuation",
  "older pin",
  "visible delete",
] as const;

/** A pending tombstone does not name a durable frame, even when its memtable's rows are committed. */
export const WRITE_PENDING_FLUSH_STATES = [
  "earlier statement",
  "same statement",
  "memory",
  "indexeddb",
  "frozen committed rows with pending tombstone",
  "rollback",
  "commit",
  "publication and reopen",
  "close settles",
] as const;

/** Streaming index ranges preserve the pinned view while visiting only demanded output. */
export const WRITE_STREAM_RANGE_STATES = [
  "memory",
  "32 MiB cap",
  "64 MiB cap",
  "indexeddb",
  "ascending",
  "descending",
  "unique",
  "nonunique",
  "hot nonunique",
  "memtable input",
  "stored input",
  "staged input",
  "bounded range",
  "empty range",
  "output continuation",
  "earlier statement",
  "own writes invisible",
  "older pin",
  "committed delete",
] as const;

/** Maintained order survives growth and rollback while a cursor keeps its position. */
export const WRITE_STREAM_DELTA_STATES = [
  "ordered inserts",
  "unordered pending inserts",
  "leaf-sized tail",
  "arena growth",
  "ascending",
  "descending",
  "rollback while pinned",
  "resume after mutation",
  "invisible tail",
] as const;

/** Reverse run traversal handles the existing interleaved blocks and Bloom partitions. */
export const WRITE_RANGE_DIRECTORY_STATES = [
  "asynchronous metadata",
  "cached metadata",
  "one hot key",
  "distinct keys",
  "ascending",
  "descending",
  "crossed Bloom boundaries",
  "bounded prefix seek",
] as const;

/** New ordered-leaf buffers must not bypass the memtable's native-capacity ownership. */
export const WRITE_STREAM_ORDER_STATES = [
  "denied before constructor",
  "funded retry",
  "native allocation witness",
  "constructor failure refund",
  "release once",
] as const;

/** An allocation failure after a key prefix cannot leave index effects of aborted rows. */
export const WRITE_PARTIAL_DELTA_STATES = [
  "memory",
  "insert prefix",
  "delete prefix",
  "indexeddb",
  "primary",
  "unique",
  "nonunique",
  "native failure",
  "budget denial",
  "partial key prefix",
  "all deltas rolled back",
  "retry accepted",
  "durable reopen",
] as const;

/** Copied source buffers are collectible while chunks and owned views remain live. */
export const WRITE_COLUMN_BORROW_STATES = [
  "bool",
  "i32",
  "f32",
  "f64",
  "int64",
  "decimal",
  "varlen",
  "fixed16",
  "interval",
  "flat",
  "constant",
  "dictionary",
  "copied",
  "full",
  "denied",
  "selected rows",
  "dense rows",
  "nullable source",
  "all valid source",
  "single row",
  "partial chunk",
  "full chunk",
  "failed after copied prefix",
] as const;

/** R23/R5 cursor state is bounded by run count; shared cold indexes are separately governed. */
export const WRITE_RANGE_CARDINALITY_STATES = [
  "1024 keys",
  "16384 keys",
  "131072 keys",
  "governor eviction",
  "shared precharged index",
  "cold all asc",
  "warm all asc",
  "cold all desc",
  "warm all desc",
  "cold bounded asc",
  "warm bounded asc",
  "cold bounded desc",
  "warm bounded desc",
] as const;

/** Cold fills share one governed descriptor; cancellation and refusal preserve other users. */
export const WRITE_RANGE_NAVIGATION_STATES = [
  "concurrent followers",
  "follower cancellation",
  "owner cancellation retry",
  "resumed partial fill",
  "timer during cold fill",
  "released I/O holds",
  "shared ready descriptor",
  "optional cache refusal",
  "pressure descending",
  "pressure ascending",
] as const;

/** An open cursor must not root cache-owned arrays or encoded bytes after eviction. */
export const WRITE_RANGE_NAVIGATION_LIFETIME_STATES = [
  "desc",
  "bounded asc",
  "bounded desc",
  "1024 keys",
  "16384 keys",
  "131072 keys",
] as const;

export const WRITE_PRIVATE_MINIMUM_STATES = [
  "memory",
  "indexeddb",
  "32 MiB",
  "48 MiB",
  "64 MiB",
  "two input policy",
  "three input policy",
  "merge debt prepared",
  "protected holder owns real bytes",
  "merge progressed while holder retained",
  "complete current snapshot",
  "durable reopen",
  "all credit returned",
] as const;

/** An admitted writer funds one bounded step without taking another execution's minimum. */
export const WRITE_CREDIT_STATES = [
  "atomic owner admitted",
  "merge while protected holder retained",
  "idle row credit transferred",
  "merge credit reused",
  "metadata step retained and returned",
  "all accounts returned once",
  "growth waits on real retained bytes",
  "operation cancellation",
  "owner cancellation",
] as const;

/** Publication-minimum navigation shares the task clock and closes its retained state once. */
export const WRITE_MINIMUM_CLOCK_STATES = [
  "task turn during cumulative small tables",
  "close drains sliced state pin",
  "shared clock due repeatedly",
  "all state credit returned",
] as const;

/** Rollback discards staged rows while preserving unrelated, sequence-qualified index entries. */
export const WRITE_STAGED_RESIDUAL_STATES = [
  "memory",
  "sim",
  "indexeddb",
  "transaction",
  "statement",
  "savepoint",
  "none",
  "unique",
  "nonunique",
  "earlier pending delete",
  "committed delete only",
  "snapshot before delete",
  "snapshot after delete",
  "held residual flush",
  "unheld residual flush",
  "merges",
  "no merges",
  "residual staged",
  "pending residual waits for commit",
  "residual flush blocked",
  "deleted key reused",
  "reopened",
] as const;

/** PG-valid guide-9 SQL must retain text extraction and distinguish absent/JSON-null paths. */
export const GUIDE9_JSON_CAST_REACH = [
  "integer",
  "negative",
  "string",
  "escaped string",
  "boolean",
  "JSON null",
  "missing member",
  "missing parent",
  "null parent",
  "explicit JSON",
  "text result",
  "original ambiguity",
  "native typed refusal",
] as const;

/** A cancelled build still belongs to catalog prefixes before its DROP checkpoint. */
export const WRITE_CATALOG_DROP_STATES = [
  "32 MiB owner cap",
  "memory",
  "sim",
  "indexeddb",
  "empty cancelled build",
  "nonempty cancelled build",
  "single cancellation",
  "multiple cancellations",
  "later table creation",
  "existing blocker table",
  "reverse drop order",
  "creation drop order",
  "interrupted publication",
  "ordinary publication",
  "catalog prefix checked",
  "checkpoint retreats before cancelled CREATE",
  "build records charged and returned",
  "atomic create drop replay",
  "historical backfill stored",
  "historical empty index stored",
  "replay uncheckpointed drop",
  "continued durable writes",
  "replacement after reopen",
  "durable reopen",
  "all credit returned",
] as const;

/** Scalar pair loops yield at native page I/O or shared-clock slices, not per scalar. */
export const PAIR_PROGRESS_REACH = [
  "slice-bounded clock checks",
  "merge cancellation",
  "frame view reuse",
  "union",
  "difference",
  "retirements",
  "incoming",
  "unused supplied output",
  "page boundary",
  "exact cleanup",
] as const;

/** Partial directory pages amortize singleton appends without losing rows or ownership. */
export const DIRECTORY_APPEND_REACH = [
  "variable batches",
  "multi-group flush",
  "mixed mutations",
  "bounded tail after mutation",
  "singleton append",
  "geometric tail",
  "changed page",
  "exact extent ownership",
  "reopen preserves rows",
  "exact cleanup",
] as const;

/** Every legal twelve-bit canonical directory tail is opened by both supported builds. */
export const DIRECTORY_COMPAT_TAILS = Array.from({ length: 4096 }, (_, n) => `tail ${String(n)}`);
export const DIRECTORY_COMPAT_REACH = [
  "current to baseline continuation",
  "baseline to current continuation",
  "every value preserved",
  "exact cleanup",
  "range end reopened",
  "full-page carry",
  "full-page plus tail",
  "frozen packed tail",
  "partial extent retained after edit",
  "shared extent carry retired",
] as const;

/** Physical publication edges, recovery/continuation and competing cross-build openers. */
export const DIRECTORY_TRANSITION_REACH = [
  "packed normalization",
  "full carry",
  "partial edit",
  "shared carry",
  "page written",
  "index written",
  "data flushed",
  "root written",
  "root truncated",
  "root flushed",
  "acknowledged values recovered",
  "retry both writers",
  "interrupted current writer",
  "interrupted baseline writer",
  "concurrent current owner",
  "concurrent baseline owner",
  "damaged page refused",
  "damaged index refused",
  "damage refused by current",
  "damage refused by baseline",
] as const;

/** The fused publication proof, including exact rejection and a non-timing scan-count guard. */
export const PUBLICATION_MERGE_REACH = [
  "mixed ownership",
  "one root scan",
  "exact rounded retirements",
  "unused sealed output",
  "empty current",
  "empty next",
  "all shared",
  "missing claim",
  "extra claim",
  "wrong claim length",
  "duplicate claim",
  "changed shared length",
  "missing sealed owner",
  "wrong sealed length",
  "writer overlaps shared",
  "incoming overlaps left",
  "incoming overlaps right",
  "exact cleanup",
] as const;

/** Prevalidated worklists stay under R8 with roots held, avoiding a second root scan. */
export const RETIREMENT_PROOF_REUSE_REACH = [
  "validated difference retained",
  "fallback difference reconstructed",
  "caller drops proof owner",
  "no second root scan",
  "proof waits behind pin",
  "proof waits for next generation",
  "exact proof reclamation",
  "proof abort releases reference",
] as const;

/** A released write column is reusable only after its last reader gives it back. */
export const WRITE_COLUMN_POOL_STATES = [
  "memory",
  "indexeddb",
  "rollback after reuse",
  "backing buffer reused",
  "null to valid",
  "reopen",
  "zero final charge",
] as const;

/** Row assembly can suspend with resident sources beside stored rows. */
export const WRITE_ROW_BORROW_STATES = [
  "resident first",
  "stored first",
  "success",
  "async error",
  "sync error",
  "released after suspension",
] as const;

/** Cached write hashes retain both canonical seeds through ownership and run publication. */
export const WRITE_KEY_HASH_REACH = [
  "first hash collision",
  "empty",
  "five bytes",
  "stripes",
  "long key",
  "high bytes",
  "shifted range",
  "growth",
  "rollback and reinsert",
  "identical run bytes",
  "released charge",
] as const;

/** Ingress hashing must not be repeated by table insertion after a probe. */
export const WRITE_KEY_HASH_ONCE_REACH = [
  "one key",
  "many keys",
  "shifted range",
  "duplicate keys",
] as const;

/** Physical row ranges must preserve the existing selected-row write contract. */
export const WRITE_ROW_PLAN_REACH = [
  "range",
  "selected",
  "nullable",
  "dictionary",
  "constant",
  "empty",
  "admission boundary",
  "written selection",
  "producer reused",
  "primary keys",
  "reopen",
  "inactive null",
  "NOT NULL refusal",
  "secondary null omission",
] as const;

/** The owner prepays one bounded row-plan record through admission, settlement and close. */
export const WRITE_ROW_PLAN_ACCOUNTING_REACH = [
  "range",
  "selected",
  "single chunk",
  "multiple chunks",
  "success",
  "error",
  "cancel",
  "sync error",
  "paid after settlement",
  "released on close",
] as const;

/** EXCEPT counts equality classes (including NULL), unlike strict join equality. */
export const EXCEPT_SQL_REACH = [
  "nested",
  "binary",
  "ALL",
  "DISTINCT",
  "empty left",
  "empty right",
  "empty result",
  "NULL keys",
  "escaped Unicode",
  "left duplicates",
  "right duplicates",
  "bag output",
] as const;

/** Grant-or-spill may change grouping layout, never SQL equality-class multiplicity. */
export const EXCEPT_SPILL_REACH = [
  "oversized page",
  "scratch backend",
  "memory backend",
  "collision fallback",
  "spill",
  "resident",
  "ALL",
  "DISTINCT",
  "dictionary",
  "flat",
  "NULL keys",
  "unequal batches",
  "exact PG",
  "cleanup",
  "left duplicates",
  "right exceeds left",
] as const;

/** Canonical equality cannot discard the chosen left tuple's presentation. */
export const EXCEPT_REPRESENTATIVE_REACH = [
  "wide integer",
  "interval fields",
  "decimal scale",
  "negative zero",
  "ALL",
  "DISTINCT",
  "spill",
  "collision",
  "cleanup",
] as const;

/** Cancellation must retain I/O credit until settlement, without a late publish or borrow leak. */
export const EXCEPT_LIFECYCLE_REACH = [
  "append",
  "read",
  "output",
  "borrow returned",
  "cleanup",
  "no work after abort",
  "I/O fence",
] as const;

/** Visibility-safe cursor OFFSET without a planner route. */
export const WRITE_RANGE_SKIP_REACH = [
  "asc",
  "desc",
  "zero",
  "repeated skip",
  "end",
  "past end",
  "stored",
  "memtable",
  "insert",
  "delete",
  "update",
  "other staged rows",
  "old pin",
  "own earlier statement",
  "rollback",
  "disjoint blocks",
  "whole run count",
  "fixed page counts",
  "pending commit while pinned",
  "fixed page counts generated",
  "reverse insertion",
  "descending multiple pages",
] as const;

/** Bounded private IndexedDB metadata cache, including asynchronous dirty eviction. */
export const IDB_METADATA_CACHE_REACH = [
  "coalesced private writes",
  "exact compact charge",
  "entry and byte cap pressure",
  "credit refused native fallback",
  "close and reopen",
  "cached owner fenced",
  "concurrent sessions and reset",
  "dirty eviction wakes admission",
  "malformed native page refused",
  "failed refused replacement keeps old bytes",
  "failed dirty eviction and reset retry",
  "governor routed through chooser",
  "unsettled native frame retains credit",
  "queued operations fenced before execution",
] as const;
