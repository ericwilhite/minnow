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
/** Reset refusal must see every legacy marker and both OPFS capability states. */
export const DIRECTORY_REFUSAL_REACH = [
  "old development root refused",
  "old development writer refused",
  "payload bytes unchanged",
  "native refusal before fencing",
  "zero native writes",
  "entire native image unchanged",
  "packed-0",
  "packed-4095",
  "packed-4099",
] as const;
export const DURABLE_LONG_KEY_REACH = [
  "WAL-only invalidation",
  "replayed invalidation",
  "memory",
  "sim",
  "13107-unit boundary",
  "older OPFS zero writes",
  "older IDB zero writes",
  "first root slot",
  "second root slot",
  "complete native tree",
  "partial native tree",
] as const;
export const DURABLE_REGISTRY_REACH = [
  "snapshot",
  "snapshotFrame",
  "snapshotFooter",
  "snapshotColumn",
  "rootSlot",
  "epochSlot",
  "logHeader",
  "logFrame",
  "manifest",
  "groupRoot",
  "groupPage",
  "deletionBitmap",
  "groupIndex",
  "tableStatistics",
  "extentList",
  "pageDirectory",
  "page",
  "largeValue",
  "sketch",
  "commitBody",
  "column",
  "catalog",
  "catalogChange",
  "runBlock",
  "runIndex",
  "runFences",
  "runBloomDirectory",
  "runBloom",
  "runKeyPointer",
  "runKeyDescriptor",
  "runKeyDirectory",
  "runKeyPayload",
  "arrayValue",
] as const;
export const DELETION_BITMAP_REACH = [
  "empty",
  "partial word",
  "whole word",
  "multiple words",
  "unaligned input",
  "short",
  "long",
  "nonword length",
  "tail bit",
  "unchanged input",
  "cache hit",
  "async read",
] as const;

export const DURABLE_FIELD_REACH = [
  "request flag",
  "column flag",
  "column boolean",
  "NULL payload",
  "array boolean",
  "catalog operation",
  "run flags",
  "array bitmap",
  "page padding",
] as const;

export const DURABLE_STARTUP_REACH = [
  "minimum reader",
  "manifest format",
  "root-0",
  "root-1",
  "fresh snapshot",
  "stale snapshot",
  "partial tree",
  "complete tree",
  "OPFS unchanged",
  "IDB unchanged",
  "zero writes",
] as const;
export const DURABLE_ALIAS_REACH = ["logical manifest", "lazy root", "dictionary"] as const;
export const DURABLE_NATIVE_REACH = ["rootSlot", "logHeader", "OPFS", "IDB"] as const;
export const DURABLE_CATALOG_REACH = [
  "physical",
  "index kind",
  "index role",
  "foreign action",
  "scalar number",
  "missing form",
  "missing layout",
] as const;
export const DURABLE_READ_REACH = ["blocked", "progress"] as const;

export const LEGACY_PARTIAL_TREE_REACH = [
  "partial",
  "no marker",
  "ack only",
  "unknown file",
  "unknown directory",
  "native name directory",
] as const;
export const LEGACY_HELD_WRITER_REACH = ["held", "closed", "I1 blocked", "I2", "I8"] as const;

export const LEGACY_REFUSAL_REACH = {
  opfs: ["O5", "O6", "O7", "O8", "O9", "O10", "O11", "O12", "O13", "sync", "no sync", "pre-marker"],
  idb: ["I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8"],
} as const;

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

export const WIDE_KEY_BATCH_STATES = [
  "wide insert slices",
  "wide sparse selected offsets",
  "wide dense offsets",
  "wide backfill slices",
  "wide backfill visible before replay",
  "wide replay tail",
  "wide conflict tail",
  "wide update tail",
  "wide delete tail",
  "wide thirty-two MiB",
  "wide sixty-four MiB",
  "wide scalar provenance",
  "wide one-twenty-eight MiB",
  "wide tuple provenance",
] as const;

export const CATALOG_RUN_LIMIT_STATES = [
  "catalog capacity refusal",
  "intact capacity continues",
  "refused open retries",
] as const;

export const LONG_KEY_ACTION_STATES = [
  "pending byte freeze",
  "byte freeze rollback",
  "byte freeze savepoint",
  "bounded overflow drain",
  "thirty-two MiB flush",
  "sixty-four MiB flush",
  "forward planned",
  "reverse planned",
  "future width at fixed admission",
  "automatic overflow selection",
  "NUL published key",
  "invalid support rekey",
  "selective durable fallback",
  "fallback allocation cleanup",
  "foreign cascade",
  "foreign restrict",
  "foreign set null",
  "pre-full pending pressure",
  "wide ingress at thirty-two MiB",
  "whole wide ingress below cap",
  "escaped key expansion at admission",
] as const;

export const WIDE_ROW_RECYCLE_STATES = [
  "bounded reservations",
  "charged encoder",
  "complete append",
  "inline wide values",
  "large value boundary",
  "JSONB columns 1",
  "JSONB columns 4",
  "JSONB columns 6",
  "JSONB columns 8",
  "width 16383",
  "width 32767",
  "width 43690",
  "width 65520",
  "width 65537",
  "width 131072",
  "two rows",
  "three rows",
  "validity bitmap",
  "no validity bitmap",
  "exact native readback",
  "exact reopened readback",
  "all charges released",
] as const;

export const KNOWN_ROW_ESTIMATE_STATES = [
  "native bytes identical",
  "bounded hint",
  "long row projection",
] as const;

export const LONG_KEY_LIMIT_STATES = [
  "catalog policy corruption refusal",
  "float32 full equality width",
  "other fixed equality widths",
  "canonical domain term costs",
  "enum without native cache",
  "other domain components",
  "typed semantic refusal",
  "legacy scalar secondary",
  "composite primary budget",
  "secondary tuple budget",
  "scalar primary budget",
  "flat",
  "constant",
  "dictionary",
  "UTF16 astral",
  "embedded NUL",
  "other Unicode",
  "UTF8 width is not units",
  "write refusal atomic",
  "invalid secondary scans",
  "prepared replan",
  "update and delete",
  "durable catalog policy",
  "old ready snapshot",
  "transaction undo",
  "savepoint undo",
  "backfill invalidation",
  "backfill replay",
] as const;

export const OVERFLOW_PUBLICATION_STATES = [
  "hundreds of retained summaries",
  "maximum text pieces",
  "other piece widths",
  "thirty-two inputs",
  "other input counts",
  "thirty-two MiB cap",
  "sixty-four MiB cap",
  "before data flush",
  "before root write",
  "before root truncate",
  "before root flush",
  "recovered retry continues",
  "independent current openers",
] as const;

export const OVERFLOW_INTEGRITY_STATES = [
  "minimum contradiction",
  "maximum contradiction",
  "capacity outside data",
  "cross-extent refusal",
  "unknown run version",
  "shared-prefix corruption",
  "different-prefix corruption",
  "checksummed future directory",
  "native payload CRC",
] as const;

export const OVERFLOW_ROOT_STATES = [
  "ordinary point reads",
  "admitted ascending",
  "admitted descending",
  "allocation-free planning",
  "published overflow root",
  "old snapshot after publication",
  "durable reopen",
  "continued writes",
  "shipped UTF16 maximum",
  "other key lengths",
] as const;

export const OVERFLOW_SUMMARY_STATES = [
  "bounded native summaries",
  "crossed partition boundary",
  "full-key Bloom",
  "supplied hashes",
  "computed hashes",
  "multiple chunks",
  "single chunk",
  "ascending",
  "descending",
  "prefix ties retained",
  "pending heads",
  "range bounds and skip",
  "fan in eight",
  "other fan in",
  "hot overflow partitions",
] as const;

export const MERGE_STOP_STATES = [
  "slice callback",
  "no entries after stop",
  "working charge returned",
  "single entry slice",
  "partial block slice",
  "clock chunk slice",
] as const;

export const OVERFLOW_READER_STATES = [
  "point reads",
  "missing shared prefix",
  "cursor drain",
  "ascending",
  "descending",
  "prefix seek",
  "mixed inline blocks",
  "cached reads",
  "cold reads",
  "multi chunk",
  "single chunk",
  "equal prefixes full comparison",
  "wide row ids",
  "read failure",
  "retry",
  "unknown directory version",
  "corrupt descriptor",
  "capacity refusal",
  "cancellation",
  "split merge",
  "owned merge heads",
  "fan in 32",
  "other fan in",
  "hot key crosses fences",
  "close during read",
  "cancel during read",
  "interleaved readers",
  "native precharge",
  "metadata precharge",
  "invalid entry count",
  "shipped key maximum",
  "exact inline prefix",
] as const;

export const KEY_POINTER_CODEC_STATES = [
  "shipped maximum",
  "other lengths",
  "adjacent directory",
  "directory padding",
  "round trip",
  "invalid references",
  "unknown tag",
  "truncation",
] as const;

export const OVERFLOW_WRITER_STATES = [
  "wide row identifiers",
  "interleaved scratch",
  "bulk supplied hashes",
  "bounded root round trip",
  "payload failed",
  "directory failed",
  "block failed",
  "tail failed",
  "credit failed",
  "native failed",
  "construct failed",
  "write failed",
  "finish failed",
  "waiting before encoder",
  "cancelled admission",
  "resumed admission",
  "extended blocks",
  "inline blocks",
  "mixed blocks",
  "funded until consumed",
  "borrowed input",
  "copied input",
  "bottom tombstones",
  "retained tombstones",
  "acknowledged buffers detached",
  "shipped maximum",
  "multiple payload chunks",
  "empty output",
  "bounded budget",
  "clean failure retry",
] as const;

export const RUN_KEY_CODEC_STATES = [
  "run encodings",
  "unknown run encoding",
  "inline",
  "overflow",
  "shipped text maximum",
  "multiple chunks",
  "unknown tag",
  "truncation",
  "extent bounds",
  "length prefix count checksum",
  "overlap",
] as const;
export const ENCODING_VERSION_STATES = ["old development", "newer", "previous root"] as const;

export const WRITE_MERGE_SUMMARY_PRESSURE_STATES = [
  "other pinned bytes retained",
  "unwound before wait",
  "waiting at barrier",
  "cancelled without retry",
  "resumed same split policy",
] as const;
export const WRITE_MERGE_KEY_LIMIT_STATES = [
  "ordinary typed refusal",
  "bulk typed refusal",
  "four KiB refusal",
  "refusal cleanup",
] as const;

/** Completed merge pieces leave reusable working credit but remain funded until publication. */
export const WRITE_MERGE_SUMMARY_STATES = [
  "fan one",
  "fan thirty two",
  "intermediate fan",
  "hundreds of pieces",
  "thousands of pieces",
  "exact two KiB keys",
  "small cap",
  "fund before refund",
  "failure after funding",
  "retry after failure",
  "retained output owned",
  "output consumed",
] as const;

/** Default split metadata must finish within prepaid credit, without a copied-output peak. */
export const WRITE_MERGE_FINISH_STATES = [
  "borrowed keys",
  "copied keys",
  "other minimum held",
  "charged metadata handoff",
  "default split boundary",
  "near private allowance",
  "completed finish",
  "abort after transfer",
  "no credit refusal",
] as const;

/** Storage borrows emitted parts until append settles, including its failure path. */
export const WRITE_RUN_OUTPUT_STATES = [
  "append failure",
  "append acknowledgement",
  "boundary ownership",
  "abort or publication cleanup",
] as const;

/** Native run capacities are prepaid and ownership survives delivery, abort and retry. */
export const WRITE_RUN_ACCOUNTING_STATES = [
  "bulk",
  "ordinary",
  "budget denial",
  "native failure",
  "constructor refund",
  "drain rejection",
  "abort",
  "retry",
  "retained output",
  "consumed output",
  "bloom partition",
  "bounded stream",
  "acknowledged consumption",
  "pressure wait",
  "pressure cancellation",
  "pressure resume",
] as const;

/** Bulk sealed blocks preserve the ordinary run encoder's complete native output. */
export const WRITE_DIRECT_RUN_STATES = [
  "single",
  "unordered",
  "chains",
  "filter",
  "partition",
  "long",
  "empty key",
  "wide row id",
  "two-byte length",
  "batch callback",
  "per-row fallback",
  "mixed widths",
  "partial fallback",
  "filtered batch",
  "implicit order batch",
  "permuted order batch",
] as const;

/** Batched normalized-key admission checks unsigned payloads and the prior arena boundary. */
export const WRITE_KEY_BATCH_PROOF_STATES = [
  "ordered batch",
  "one boundary",
  "batch fallback",
  "unsigned payload",
  "signed boundary",
  "boundary rejection",
] as const;

/** Retained key-slot order proofs survive ties, disorder, chains and rollback. */
export const WRITE_KEY_ORDER_PROOF_STATES = [
  "ascending",
  "descending",
  "late disorder",
  "prefix ties",
  "empty and zero extension",
  "rollback",
  "duplicates",
  "zero workspace",
  "byte fallback",
] as const;

/** A 64-bit key table's half-full resize is funded before assignment. */
export const INT64_GROWTH_REACH = [
  "funded transition",
  "half-full boundary above 8192",
  "second half-full boundary",
  "wide int64",
  "safe int64",
  "selected input",
  "NULL group",
  "zero charges",
] as const;

export const SEMI_ANTI_KEY_GROWTH_REACH = [
  "catalogue semi",
  "catalogue anti",
  "more than 8192 right keys",
  "wide correlation",
  "NULL correlation",
  "PG exact",
  "zero resources",
] as const;

/** Capacity errors do not mutate a private progress lane or mask ownership errors. */
export const STEP_LOANS_REFUSAL_REACH = [
  "oversized refused without allocation",
  "ownership violation stays distinct",
  "capacity boundary and retry succeed",
  "abort has precedence",
  "closed has precedence",
] as const;

/** Complete prepared hash output credit survives reclaim until it is consumed or released. */
export const HASH_PREPARED_CREDIT_REACH = [
  "reserved credit is not a live buffer",
  "credit consumed once",
  "unused released before wait",
] as const;

export const HASH_OUTPUT_CREDIT_REACH = [
  "reclaim after prepare",
  "unused capacity reclaimed",
  "PG exact output",
  "cancel funded output",
  "nullable membership",
  "all charges returned",
] as const;

/** Logical aliases own independent frame data, never source or another column. */
export const RESULT_ALIAS_FRAME_REACH = [
  "independent backing allocations",
  "consumer aliases isolated",
  "dedicated aliases copy",
  "dictionary delta",
  "wide numeric clone",
  "cancel blocked",
  "exact values",
  "selected order",
  "float bits",
  "all charges returned",
] as const;

export const SORT_ADMISSION_ERROR_REACH = [
  "ordinary room exhausted",
  "repeated SQL projection",
  "bounded spill and merge succeed",
  "bounded topN succeeds",
  "computed fields preserved",
  "separate variable sources preserved",
  "fixed-width alias rows exact",
  "tiny variable aliases remain admissible",
  "five aliases",
  "six aliases",
  "ascending alias key",
  "descending alias key",
  "leading marker",
  "interleaved marker",
  "NULL and unequal text rows",
  "cleanup returns all resources",
] as const;

/** Full numeric sorts preserve SQL order and budget/borrow lifetimes on both physical paths. */
export const COLUMNAR_SORT_REACH = [
  "small stable merge",
  "large LSD",
  "stable ties",
  "selected input",
  "float peers",
  "wide int64",
  "safe int64",
  "null keys",
  "nullable dictionary payload",
  "asc",
  "desc",
  "first",
  "last",
  "fallback replay",
  "fallback reclaim fence",
  "external spill",
  "arrival refusal",
  "sort scratch refusal",
  "abort fence",
  "resolved readiness preserves deadline",
] as const;

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
  "export private miss",
  "ordinary retained metadata",
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
  "implicit ordered traversal",
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
  "native execution",
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

/** Every legal twelve-bit canonical directory tail is opened by independent current builds. */
export const DIRECTORY_COMPAT_TAILS = Array.from({ length: 4096 }, (_, n) => `tail ${String(n)}`);
export const DIRECTORY_COMPAT_REACH = [
  "current to peer continuation",
  "peer to current continuation",
  "every value preserved",
  "exact cleanup",
  "range end reopened",
  "full-page carry",
  "full-page plus tail",
  "reused packed tail",
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
  "interrupted peer writer",
  "concurrent current owner",
  "concurrent peer owner",
  "damaged page refused",
  "damaged index refused",
  "damage refused by current",
  "damage refused by peer",
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
  "tight oversized pressure",
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

/** Exact non-retaining export leases, distinct from optional gap/pool reservations. */
export const EXACT_READ_REACH = [
  "hit",
  "miss",
  "gap",
  "overlap",
  "duplicate",
  "refused",
  "cancelled",
  "error-cleanup",
  "close-held",
  "ordinary-warm",
] as const;

export const EXACT_STEP_REACH = [
  "active",
  "queued",
  "cancelled",
  "drained",
  "default",
  "private",
] as const;

export const EXACT_PACE_REACH = ["timer", "cancelled", "completed", "released"] as const;

export const EXACT_FAST_REACH = [
  "private window",
  "private fallback",
  "private exact",
  "ordinary window",
  "ordinary fallback",
  "ordinary exact",
  "released",
] as const;

/** Both exact-read implementations run every existing seed/history count. */
export const EXACT_READER_REACH = { storage: "storage reader", memory: "memory reader" } as const;

/** JSON extraction keeps lexical values and the last matching field, including escaped names. */
export const JSON_EXTRACTION_REACH = [
  "array Unicode",
  "nested raw output",
  "decoded text",
  "raw JSON",
  "direct",
  "derived",
  "CTE",
  "NULL selector",
  "negative index",
  "exact number text",
  "duplicate escaped key",
  "JSON null",
  "missing field",
  "Unicode error",
] as const;

/** JSON extraction frames retain scan order and prepaid workspace across physical encodings. */
export const JSON_FRAME_REACH = [
  "flat",
  "dictionary",
  "constant",
  "forward selection",
  "reverse selection",
  "NULL and inactive garbage",
  "prepaid workspace",
  "released borrow",
] as const;

/** Runtime text casts must validate syntax, including unselected spelling features. */
export const JSON_VALIDATION_REACH = [
  "array comma",
  "object comma",
  "leading zero",
  "exponent",
  "escape",
  "valid decimal",
] as const;

/** The front end normalizes these values before lowering; do not credit an inexact execution. */
export const JSON_MATERIALIZED_REACH = [
  "direct",
  "CTE",
  "typed refusal",
  "negative",
  "scale preserved by PG",
] as const;

/** Every JSON navigation phase consumes the same worker task budget, even zero-length tokens. */
export const JSON_SCAN_BUDGET_REACH = [
  "closing stack",
  "empty-string negative index",
  "empty-string positive index",
  "shared clock",
] as const;

/** Narrow lexical normalization witness against the shipped engine and two PG queries. */
export const JSON_CURRENT_LEXICAL_REACH = [
  "zero",
  "negative",
  "positive",
  "exact PG result",
  "canonical PG witness",
  "wrong value rejected",
] as const;

/** Wide JSON projections resume the suspended column instead of replaying completed columns. */
export const JSON_PROJECTION_REACH = [
  "one column",
  "many columns",
  "64 columns",
  "linear evaluation",
  "returned charges",
] as const;

/** PG admits text/int4 selectors (including int2 widening), not boolean/numeric/int8. */
export const JSON_KEY_TYPES_REACH = [
  "boolean",
  "fractional",
  "numeric integral",
  "int8",
  "int2 widening",
  "text",
  "42883",
  "text extraction",
  "raw extraction",
] as const;

/** Text projections cannot hide a parameter document from source-envelope admission. */
export const JSON_PARAMETER_ENVELOPE_REACH = [
  "direct",
  "derived text",
  "CTE text",
  "VALUES text",
  "SQL NULL",
  "decimal spelling",
  "typed bound refusal",
] as const;

/** Literal JSON can be larger than the default result-progress allowance in UTF-8. */
export const JSON_LITERAL_PROGRESS_REACH = [
  "one output",
  "many outputs",
  "literal only",
  "stored source plus literal",
  "UTF8 over private default",
  "competing pressure",
  "returned all grants",
] as const;

/** Calendar operators keep the interval's fields and clamp months before adding days/time. */
export const CALENDAR_SQL_REACH = [
  "timestamp",
  "date",
  "timestamptz",
  "direct",
  "derived",
  "CTE",
  "view",
  "add",
  "subtract",
  "NULL",
  "month end",
  "sub-millisecond",
  "negative field",
] as const;

/** INTERVAL EXTRACT keeps PostgreSQL field definitions, numeric scales and error admission. */
export const INTERVAL_EXTRACT_SQL_REACH = [
  "week",
  "negative quarter",
  "epoch",
  "year",
  "month",
  "day",
  "hour",
  "minute",
  "second",
  "milliseconds",
  "microseconds",
  "decade",
  "century",
  "millennium",
  "quarter",
  "unsupported field",
  "unknown field",
  "NULL",
  "negative fields",
  "microsecond scale",
  "large exact coefficient",
  "direct",
  "derived",
  "CTE",
] as const;

/** Native interval ordering uses PG's 30-day month comparison, with exact wide fields. */
export const INTERVAL_COMPARE_REACH = [
  "flat",
  "dictionary",
  "constant",
  "sparse",
  "selected",
  "strict comparison",
  "NULL-safe comparison",
  "NULL",
  "infinity",
  "30-day month equality",
  "wide exact microseconds",
  "returned charge",
] as const;

/** SRF arity/type resolution, strict NULL arguments and overflow-safe integer termination. */
export const SERIES_SQL_REACH = [
  "direct",
  "derived",
  "CTE",
  "empty",
  "NULL argument",
  "int4",
  "int8",
  "int2 widening",
  "timestamp",
  "microsecond step",
  "calendar step",
  "last integer overflow stops",
  "ambiguous overload",
  "zero step",
] as const;
/** Native series has no inherited materialization cap and obeys consumer demand. */
export const SERIES_LARGE_REACH = [
  "past shipped row cap",
  "LIMIT stops huge range",
  "LIMIT 0 skips zero step",
] as const;

/** Binary carry directory stays bounded and traverses spill pages in append order. */
export const RECURSIVE_TAPE_REACH = [
  "one page",
  "power-of-two carry",
  "ragged roots",
  "FIFO rows",
  "corruption refusal",
  "bounded metadata",
  "returned bytes",
] as const;

/** UNION ALL includes all seed duplicates and NULL values until the step predicate empties. */
export const RECURSIVE_SQL_REACH = [
  "limited demand",
  "filtered step",
  "later selected slot",
  "base only",
  "step rows",
  "empty base",
  "NULL seed",
  "duplicate seed",
  "multiple seeds",
  "negative step",
  "zero value step",
  "exact PG",
] as const;
/** Work-table spill preserves input order, hence the promised float8 accumulation order. */
export const RECURSIVE_SPILL_REACH = [
  "competing pressure",
  "FIFO exact rows",
  "ordered PG SUM",
  "spill",
  "scratch service",
  "memory service",
  "small cap",
  "returned all bytes",
] as const;
/** Native calendar operations use full-range integer fields, never a millisecond Date. */
export const CALENDAR_VECTOR_REACH = [
  "flat",
  "dictionary",
  "constant",
  "add",
  "subtract",
  "NULL",
  "infinity",
  "wide microseconds",
  "PG error",
  "exact text",
  "returned charge",
] as const;
/** Every in-flight I/O/consumer borrow outlives cancellation and precedes resource release. */
export const RECURSIVE_LIFECYCLE_REACH = [
  "append",
  "read",
  "consumer",
  "append failure",
  "read failure",
  "consumer failure",
  "borrow fence",
  "fault cleanup",
  "returned all bytes",
] as const;

/** Adjacent parser intervals remain separate across CSE and cloned query scopes. */
export const CALENDAR_IDENTITY_REACH = [
  "aggregate",
  "grouped",
  "direct",
  "derived",
  "CTE",
  "view",
  "wide",
  "small",
  "adjacent exact",
] as const;
/** R17.1 output rendering depends on explicit bigint operands, not an implicit widening. */
export const SERIES_RENDER_REACH = [
  "explicit bigint",
  "implicit int8",
  "derived",
  "direct",
  "PG exact",
] as const;
/** Function identity affects SQL return types and overload selection, including through clones/CSE. */
export const DATE_PART_SQL_REACH = [
  "interval",
  "date",
  "timestamp",
  "timestamptz",
  "direct",
  "derived",
  "CTE",
  "view",
  "aggregate identity",
  "NULL",
  "PG result types",
  "exact interval value",
  "date/time typed refusal",
  "float ROUND refused",
  "numeric ROUND executes",
  "float cancellation",
  "epoch",
  "second",
  "milliseconds",
  "microseconds",
  "year",
  "month",
  "day",
  "quarter",
  "week",
] as const;

/** Coverage-sized SQL histories retain every original phase/pressure state. */
export const AGG_HANDOFF_SQL_REACH = [
  "several executions",
  "scalar",
  "grouped",
  "skewed dominant group",
  "many small groups",
  "filtered transitions",
  "pressure checkpoint",
  "restore admission",
  "temporary pinned holder",
  "holder releases",
  "cancel pending phase",
  "cancel restore pass",
  "snapshot retained",
  "owned parameter",
  "empty owned parameter",
  "resident follower",
  "empty filtered resident state",
  "PG exact",
  "scratch service",
  "zero",
  "bounded 8192-row source",
  "owned parameter at most 8KiB",
  "1024 small groups retained",
  "stable restore entered",
] as const;

/** Aggregation waits before retaining state, then grows solely inside its admitted workspace. */
export const AGGREGATE_PROGRESS_REACH = [
  "stored dictionary certificate",
  "bounded memory certificate",
  "temporary pinned holder waits",
  "cancelled credit wait",
  "competing holder releases",
  "no shared growth headroom",
  "numeric promotion",
  "private resize",
  "codebook translation growth",
  "tuple growth",
  "PG exact bits",
  "concurrent prepared executions",
  "all credit returned",
] as const;

/** Admitted index ranges: publication, traversal, cancellation and atomic query admission. */
export const WRITE_RANGE_ADMISSION_STATES = [
  "asc",
  "desc",
  "publication",
  "next",
  "skip",
  "cancel",
  "close",
  "concurrent",
] as const;

/** Large-key cursor admission across publication before and during traversal. */
export const WRITE_RANGE_ADMISSION_HISTORY_STATES = [
  "asc",
  "desc",
  "large text keys",
  "multiple memtables",
  "publication before open",
  "publication during iteration",
  "large text publication during iteration",
  "next",
  "skip after next",
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

/** Failed private transactions preserve acknowledged bytes and cannot publish clean state. */
export const IDB_METADATA_FAILURE_REACH = [
  "dirty acknowledged page ahead of storage",
  "failed first transaction",
  "old flush committed before failed replacement",
  "consecutive aborts",
  "consecutive callback failures",
  "successful transactions between failed bursts",
  "full to compact replacement",
  "compact to full replacement",
  "both publication oracles after every action",
  "close and reopen agreement",
] as const;

/** Scratch ownership/free metadata is paged, including direct appends and close descriptors. */
export const SCRATCH_METADATA_REACH = [
  "interleaved direct appends",
  "metadata pressure fallback",
  "round trip",
  "fragmented reuse",
  "close returns charge",
  "bounded release descriptors",
  "metadata pages reused",
  "release failure and sweep retry",
  "metadata corruption refusal",
  "nonadjacent frees beyond a metadata page",
  "metadata namespace isolation",
  "allocator fence",
  "unsettled native metadata retains charge",
] as const;

/** Independent checksum and retained-object guards for scratch allocator metadata. */
export const SCRATCH_METADATA_GUARD_REACH = [
  "inspected private ownership across growing extent counts",
  "constant resident inventory matches prepaid workspace",
  "inspected bounded release bookkeeping",
  "inspected buffers released with charge",
  "valid aligned offset redirected into live peer",
  "valid aligned length changed",
  "checksum byte flipped",
  "unused padding byte flipped",
  "corruption refused before release and reuse",
  "corrupted first entry position",
  "corrupted middle entry position",
  "corrupted last entry position",
] as const;

/** Concurrent wide sorts never retain one step loan while requesting another. */
export const SORT_PROGRESS_PRESSURE_REACH = [
  "five wide columns",
  "normal room exhausted",
  "single owner loan",
  "exact PG output",
  "two sorts fit alone",
  "queued sort has no loan",
  "with ties boundary row",
  "peer rows across runs",
  "both complete",
  "cancel waiting sort",
  "later admitted sort",
  "zero resources",
] as const;

/** A legacy sort waits only before owning its progress step; another holder can finish. */
export const SORT_PROGRESS_LOANS_REACH = [
  "legacy shared reserve",
  "no overlapping loan",
  "first sort holds complete merge",
  "second sort waits unloaned",
  "both sorts complete",
  "waiting sort cancelled",
  "PG exact mixed payload",
  "released all resources",
  "numeric first key",
  "text first key",
  "multi-run wide merge",
  "null payload",
  "misaligned spill reads",
  "grouped boundary row",
  "global top-k boundary row",
  "moving wide fields",
  "many short output columns",
  "short rows after a wide row",
  "small row step before input",
  "repaid before source readiness",
  "backend buffering refused loan credit",
] as const;

/** JSONB canonical bytes preserve SQL NUMERIC scale, decoded key order and last duplicates. */
export const JSONB_SQL_REACH = [
  "profile bytes",
  "EXCEPT below JSON",
  "stored TEXT",
  "EXCEPT left",
  "EXCEPT right",
  "derived",
  "CTE",
  "view",
  "direct",
  "key order",
  "exact number",
  "last duplicate",
  "Unicode",
  "array",
  "negative index",
  "number expansion",
  "JSON null",
  "scalar",
  "missing or mismatch",
] as const;

/** JSON retry owns files/parameters but no live producer grant while a native group queues. */
export const JSONB_PRESSURE_REACH = [
  "large parameter fragments",
  "owned parameters",
  "complete producer retirement",
  "FIFO wait",
  "spill checkpoint",
  "unary",
  "EXCEPT right",
  "queued cancellation",
  "recovery",
  "resume",
  "returned files and grants",
] as const;

/** JSONB numeric spelling exercises both exponent signs, trailing scale and exact coefficients. */
export const JSONB_NUMBER_REACH = [
  "negative",
  "positive",
  "zero",
  "negative exponent",
  "positive exponent",
  "zero exponent",
  "long scale",
  "short scale",
] as const;

/** Canonical traversal keeps decoded key ordering/last duplicates through nested containers. */
export const JSONB_CANONICAL_REACH = [
  "flat object",
  "nested array",
  "nested object",
  "deep nesting",
  "duplicate aliases",
  "decoded UTF8 keys",
  "beyond float precision",
  "negative exponent",
  "positive exponent",
] as const;

/** Fragmented JSON checkpoints restore a single admitted row under independent pressure. */
export const JSONB_WIDE_REACH = [
  "multi-byte UTF8",
  "observed byte progress",
  "observed fragment progress",
  "UTF8 fan-out megabyte",
  "multiple payload fragments",
  "large parameter at cap",
  "stored source",
  "zero source",
  "one output",
  "five outputs",
  "UTF8 megabyte",
  "competing pressure",
  "returned files and grants",
] as const;

/** Completed prefixes and unread suffixes partition a selected stream across repeated cuts. */
export const JSONB_CONTINUATION_REACH = [
  "completed prefix",
  "current row",
  "unread suffix",
  "multiple growth cuts",
  "source opened once",
  "selection",
  "dense",
  "dictionary",
  "flat",
  "unequal boundaries",
] as const;

/** LIMIT/filter demand survives JSON growth without eagerly surfacing later row errors. */
export const JSONB_DEMAND_REACH = [
  "nested JSON error",
  "nested data error",
  "producer filter error",
  "limit 0",
  "limit 1",
  "limit 2",
  "project",
  "filter",
  "producer error",
  "growth before demand satisfied",
  "early error",
] as const;

/** A cut may checkpoint retaining ancestors, never a finishing descendant or a sibling. */
export const JSONB_PARENT_REACH = [
  "left arm",
  "right arm",
  "retaining window supported",
  "unary/EXCEPT supported",
  "descendant excluded",
  "sibling excluded",
] as const;

/** A blocking unary consumer delivers preceding rows before their saved terminal data error. */
export const JSONB_DEFERRED_REACH = [
  "arithmetic error",
  "JSON error",
  "limit 0",
  "limit 1",
  "limit 2",
  "growth",
  "no growth",
] as const;

/** PostgreSQL JSONB error precedence across independently defective input fragments. */
export const JSONB_ERROR_ORDER_REACH = [
  "array",
  "object",
  "nested",
  "defective keys",
  "two defects",
  "three or more defects",
  "token lookahead",
  "malformed numeric token",
  "same string defects",
  "Unicode before lexical",
  "numeric before lexical",
  "lexical before Unicode",
  "SQLSTATE 22P02",
  "SQLSTATE 22P05",
  "SQLSTATE 22003",
] as const;

/** Only native byte/fragment/result advancement keeps the JSONB pressure deadline alive. */
export const JSONB_WIDE_DEADLINE_REACH = [
  "productive work",
  "past old total deadline",
  "stalled work cancelled",
  "returned timer owners",
] as const;

/** Dedicated sorting outputs move to result ownership without weakening SQL ordering. */
export const SORT_TRANSFER_REACH = [
  "dedicated output",
  "foreign reclassification",
  "pure projection",
  "nulls and NaNs",
  "asc",
  "desc",
] as const;

/** Encoded text remains a code gather, with exact cross-page payload identities. */
export const SORT_DICTIONARY_REACH = [
  "genuine dictionaries",
  "local code rebasing",
  "stable dictionary identity",
  "NULL payload",
  "UTF8 payload",
  "wide int64 payload",
  "asc",
  "desc",
  "byte hash collision",
  "appended dictionary prefix",
  "raw octets",
  "codebook bound",
  "mixed encoding fallback",
  "borrow-only receiver",
] as const;

/** Exact compact ordering words preserve all keys and require proof before omitting suffixes. */
export const SORT_COMPRESSION_REACH = [
  "ordered suffix proof",
  "shuffled suffix retained",
  "composite packing",
  "tuple ranks",
  "probe bound",
  "explicit tied IDs",
  "LSD threshold",
  "above threshold",
  "float peers",
  "wide int64",
  "NULL keys",
  "hash collision",
  "one word",
  "word boundary",
  "asc",
  "desc",
] as const;

/** Native buffers change accounts exactly once, including failed adoption and frame credit. */
export const SORT_TRANSFER_ACCOUNTING_REACH = [
  "adopt",
  "refuse",
  "cancel",
  "error",
  "loan",
  "loan credit stays at source",
  "single owner after readiness",
  "single owner before publication",
  "irreversible detach",
  "operator close with live frames",
  "multiple source chunks",
] as const;

/** Actual result frames retain exact dictionary values across source close and spill replay. */
export const SORT_DICTIONARY_LIFETIME_REACH = [
  "resident union frames",
  "actual spill and merge",
  "union replay under reclaim",
  "stable wire id and delta",
  "local dictionary codes",
  "NULL text",
  "UTF8 text",
  "exact wide int64",
  "after source close",
  "earlier frames detached",
  "asc",
  "desc",
  "PG full output",
] as const;

/** Every prefix of a multi-column transfer is safe to cancel or fail, before or after prior credit. */
export const SORT_TRANSFER_FAILURE_REACH = [
  "error",
  "cancel",
  "zero columns",
  "interior prefix",
  "all columns",
  "first frame",
  "later frame",
  "wide int64",
  "nullable validity",
  "single owner every step",
  "source close before credit",
  "idempotent credit",
  "non-finite payload",
  "dictionary header before failure",
] as const;

/** Bounded ranking preserves PostgreSQL row-count ranks and complete boundary peers. */
export const WINDOW_TOPK_REACH = [
  "row_number",
  "rank",
  "rank gaps",
  "boundary peers",
  "all peers without ORDER BY",
  "NULL keys",
  "non-finite keys",
  "wide int64",
  "C text partitions",
  "unpartitioned",
  "empty input",
  "selected dictionary",
  "ascending",
  "descending",
  "NULLS FIRST",
  "NULLS LAST",
  "compaction",
  "spill",
  "frame fallback",
  "raw rejection",
  "zero resources",
] as const;

export const WINDOW_TOPK_SQL_REACH = [
  "rank",
  "row_number",
  "aliases",
  "unbounded control",
  "ordered PG rows",
  "empty",
  "rank frame canonicalization",
  "computed guard",
  "multiple functions",
  "shared consumer",
  "reverse comparison",
  "strict bound",
] as const;

export const WINDOW_TOPK_LIFECYCLE_REACH = [
  "append",
  "read",
  "consumer",
  "charged until settlement",
  "closed waits",
  "all peers spill",
  "queued loan cancellation",
  "complete legacy step refused",
  "zero resources",
] as const;

export const SQL_ALIAS_ISOLATION_REACH = [
  "text alias mutation",
  "integer alias mutation",
  "structured transfer",
  "nullable aliases",
  "all resources returned",
] as const;
export const FRAME_PACING_REACH = [
  "flat aliases",
  "dictionary aliases",
  "wide arrays counted once",
  "distinct backing",
  "transferred payload",
] as const;

/** Native retaining-parent continuation and ownership histories. */
export const RETAINED_JOIN_REACH = [
  "build cut",
  "probe cut",
  "resident",
  "spilled",
  "multiple cuts",
  "NULL keys",
  "duplicates",
  "keyless residual",
  "ON residual",
  "right/full match bits",
  "zero resources",
] as const;

/** Native retaining-parent continuation and ownership histories. */
export const RETAINED_WINDOW_REACH = [
  "two cuts",
  "peer groups",
  "nonassociative floats",
  "spilled prefix",
  "zero resources",
  "selected dictionary input",
  "NULL partition/order keys",
  "RANGE peers and lag",
  "non-finite floats",
  "multichunk grouped prefix",
  "generated multichunk prefix",
  "lazy grouped cut",
  "lazy columnar cut",
  "lazy general cut",
] as const;

/** Native retaining-parent continuation and ownership histories. */
export const RETAINED_PARENT_IO_REACH = [
  "join",
  "window",
  "append",
  "read",
  "cancel fence",
  "fault cleanup",
  "close drains both owners",
  "close once after failure",
  "task deadline",
  "abort task resumption",
  "reclaim waits for checkpoint",
  "top-k window",
] as const;

/** SQL producer cuts must pass through the shared retaining-parent controller. */
export const RETAINING_CONSUMER_REACH = [
  "join",
  "window",
  "build",
  "probe",
  "JSON cut",
  "native checkpoint",
  "PG exact",
  "kind INNER",
  "kind LEFT",
  "kind RIGHT",
  "kind FULL",
  "kind CROSS",
  "kind semi",
  "kind anti",
  "kind mark",
  "descendant excluded",
  "actual build",
  "actual probe",
] as const;

/** Observe both producer kinds reaching the SAME retaining-parent admission path. */
export const RETAINING_AGGREGATE_REACH = [
  "join",
  "window",
  "build",
  "probe",
  "aggregate cut",
  "JSON cut",
  "mixed producers",
  "observed spill",
  "shared idle account",
  "PG exact",
  "zero grants/files",
  "kind INNER",
  "kind LEFT",
  "kind RIGHT",
  "kind FULL",
  "kind CROSS",
  "kind semi",
  "kind anti",
  "kind mark",
  "EXCEPT left retained",
  "actual build",
  "actual probe",
] as const;

/** Consumer-level owners, not merely native checkpoint API calls, survive pressure/failure. */
export const RETAINING_LIFECYCLE_REACH = [
  "producer json",
  "producer aggregate",
  "producer mixed",
  "edge checkpoint",
  "edge suspend",
  "edge admission",
  "edge restore",
  "edge close",
  "resume",
  "cancel",
  "fault",
  "queued pressure",
  "snapshot publication",
  "owned parameters",
  "demand",
  "mixed transition",
  "concurrent survivor",
  "one idle account",
  "large binding",
  "small binding",
  "limit 1",
  "limit 2",
  "limit 3",
  "zero files/grants",
  "restore bytes before construction",
  "consumed build excluded",
] as const;

/** A native retaining parent must preserve demand and saved terminal error position. */
export const RETAINING_ERROR_REACH = [
  "zero demand",
  "valid prefix",
  "terminal error",
  "state 22P02",
  "state 22003",
  "state 22P05",
  "limit 0",
  "limit 1",
  "limit 5",
  "limit 6",
  "streaming projection",
  "eager barrier",
] as const;

/** The same ancestry law applies to JSON and aggregate producers, independent of live callbacks. */
export const RETAINING_GRAPH_REACH = [
  "JSON producer",
  "aggregate producer",
  "join ancestor",
  "window ancestor",
  "EXCEPT ancestor",
  "left path",
  "right path",
  "descendant excluded",
  "sibling excluded",
  "no retaining ancestor",
] as const;

export const RETAINING_WINDOW_ROUTE_REACH = [
  "JSON producer",
  "hybrid producer",
  "row_number",
  "rank",
  "sum",
  "lag",
  "avg",
  "bounded rank",
  "bounded row_number",
] as const;

export const COLUMNAR_CHECKPOINT_REACH = [
  "reclaim waits for replay",
  "cancel retains replay credit",
  "sealed full prefix",
  "zero resources",
] as const;

/** Top-k is sufficient state before a cut; generic restore must preserve its peer prefix. */
export const RETAINED_TOPK_REACH = [
  "rank",
  "row_number",
  "fast before cut",
  "empty cut",
  "nonempty cut",
  "two cuts",
  "compacted prefix",
  "generated compacted prefix",
  "spilled prefix",
  "peer ties",
  "NULL keys",
  "no order",
  "selected dictionary",
  "zero resources",
  "zero-payload rows",
  "zero-payload grouped",
  "zero-payload general",
  "zero-payload stream",
  "zero-payload grouped stream without cut",
  "zero-payload multichunk",
] as const;

export const RETAINING_TOPK_SQL_CUT_REACH = [
  "hybrid",
  "JSONB",
  "rank",
  "row_number",
  "empty cut",
  "nonempty cut",
  "two cuts",
  "bounded after restore",
  "exact PostgreSQL",
  "zero resources",
] as const;

export const NATIVE_BENCH_IMAGE_IO_REACH = [
  "empty file",
  "one chunk",
  "chunk tail",
  "exact restore",
  "immutable image",
  "corrupt refusal",
  "retry",
  "cancel cleanup",
  "capacity retained",
  "borrowed owner live",
] as const;
export const NATIVE_BENCH_IMAGE_SQL_REACH = [
  "NULL validity",
  "dictionary text",
  "UTF8",
  "signed zero",
  "ordinary reopen",
  "continued writes",
] as const;

export const INDEPENDENT_CPU_REACH = [
  "fresh process per pair",
  "one live VM",
  "AB startup",
  "BA startup",
  "roots hook",
  "sizing hook",
  "all-thread CPU",
  "drained close",
  "before and after verification",
  "identity refusal",
  "fingerprint refusal",
  "post-run refusal",
  "active cancellation",
  "setup cancellation",
] as const;

/** Query roots survive sizing handoff without retaining producer grants. */
export const QUERY_ROOT_REUSE_REACH = [
  "sizing and execution share validated root facts",
  "multiple columns and batches",
  "fresh query owns its roots",
  "later query reuses owner validation",
  "cancel after output",
  "zero charges after close",
] as const;

/** Query-root ownership histories include pressure and cancellation, not only steady hits. */
export const QUERY_ROOT_OWNERSHIP_REACH = [
  "exact borrower charge",
  "optional memo refusal keeps complete root",
  "full reference isolation",
  "active pin survives eviction",
  "idle eviction reparses safely",
  "pending cancellation drains",
  "zero owner charges",
] as const;

export const QUERY_ROOT_VM_REACH = [
  "eighteen distinct real workers",
  "reused worker refusal",
  "forged arithmetic refusal",
  "AB build startup",
  "BA build startup",
  "independent fixture restore",
  "exact row volume",
  "all workers drained",
  "complete decode in both arms",
  "owner cache in both arms",
] as const;

export const QUERY_ROOT_VOLUME_REACH = [
  "one lifetime",
  "many lifetimes",
  "empty result",
  "nonempty result",
  "exact callback work",
  "volume drift refusal",
] as const;

/** Heterogeneous constructor output is compared byte-for-byte, never parsed/reordered. */
export const JSON_CONSTRUCT_REACH = [
  "json_build_object",
  "json_object",
  "json_build_array",
  "json_array",
  "duplicate keys",
  "NULL handling",
  "int8 precision and numeric scale",
  "nested typed JSON",
  "UTF8",
  "escapes",
  "exact text",
] as const;

export const JSON_CONSTRUCT_PRESSURE_REACH = [
  "observed admission wait",
  "cancel waiting admission",
  "resume waiting admission",
  "growth handoff",
  "unary ancestor",
  "EXCEPT ancestor",
  "exact text",
  "zero resources",
] as const;

export const JSON_CONSTRUCT_NUMERIC_REACH = [
  "wide coefficient",
  "maximum integer digits",
  "maximum scale",
  "positive",
  "negative",
  "exact text",
] as const;

export const JSON_CONSTRUCT_CAP_REACH = [
  "JSONB streaming canonicalization",
  "JSON construction",
  "largest fit",
  "above result window floor",
  "small result window",
  "first cannot fit",
  "result buffer identity",
  "two copies",
  "exact PostgreSQL",
  "zero resources",
] as const;

export const JSON_CONSTRUCT_NESTED_REACH = [
  "JSON",
  "JSONB",
  "deep dependencies",
  "multi-fragment control expansion",
  "projected sibling pins",
  "exact PostgreSQL",
  "zero resources",
] as const;

export const JSON_CONSTRUCT_HISTORY_REACH = [
  "afterEach restoration",
  "abandoned shrinker stopped",
  "queued execution cancelled",
  "late closer inert",
  "idempotent close",
  "zero resources",
] as const;

export const JSON_RESULT_OWNERSHIP_REACH = [
  "live result charge",
  "delivery fence",
  "one column",
  "two columns",
  "generated read cut",
  "cancel while read",
  "read fault",
  "zero resources",
] as const;

export const JSON_RESULT_CONCURRENT_REACH = [
  "two queries one governor",
  "queued native admission",
  "held opposite credit",
  "cancel queued follower",
  "resume queued follower",
  "concurrent delivery",
  "cancel concurrent delivery",
  "exact PostgreSQL",
  "zero resources",
] as const;

export const JSON_EXCEPT_RESULT_BOUND_REACH = [
  "JSON",
  "JSONB",
  "larger retained left",
  "exact PostgreSQL",
  "zero spill",
] as const;

export const JSON_CONSTRUCT_ARITY_REACH = [
  "JSON",
  "JSONB",
  "at boundary",
  "above boundary",
  "exact PostgreSQL",
] as const;

export const JSONB_KEY_CURSOR_REACH = [
  "decoded UTF8 key lengths",
  "same-length byte order",
  "escaped Unicode aliases",
  "supplementary Unicode aliases",
  "control and syntax keys",
  "last duplicate in generated order",
  "constant-space key traversal",
  "exact PostgreSQL",
  "zero resources",
] as const;

export const JSON_RESULT_MIXED_REACH = [
  "wide JS coefficient copy",
  "compact coefficient copy",
  "JSON",
  "JSONB",
  "fits whole cap",
  "exact PostgreSQL",
  "zero resources",
] as const;

export const JSONB_CONSTRUCT_ERROR_REACH = [
  "Unicode before NULL key",
  "numeric before NULL key",
  "NULL key before Unicode",
  "typed key before value",
  "surrogate conversion",
  "three or more defects",
  "exact PostgreSQL SQLSTATE",
  "zero spill",
] as const;

export const JSONB_ALLOCATION_FAULT_REACH = [
  "allocator arena",
  "allocator pool",
  "allocator grant",
  "early constructor",
  "middle constructor",
  "late constructor",
  "original failure cause",
  "zero files/grants before close",
] as const;

export const JSON_FLOAT_REACH = [
  "decimal tie rounds down to even",
  "decimal tie rounds up to even",
  "float4 bits",
  "float8 bits",
  "near 2^24",
  "near 2^53",
  "rounding ties and neighbours",
  "float4 midpoint exclusion",
  "float8 midpoint exclusion",
  "float4 subnormal",
  "float8 subnormal",
  "negative zero",
  "positive zero",
  "special values",
  "exponent transitions",
  "scalar JSON and JSONB",
  "array constructors",
  "scalar keys",
  "exact PostgreSQL text",
] as const;

export const EXTENT_PAIR_CODEC_REACH = [
  "distinct offsets",
  "shared offset",
  "mixed offsets",
  "low-word collisions",
  "wide safe integers",
  "small integers",
  "zero length",
  "promoted set remembers every length",
  "exact duplicate refusal",
  "charge refusal before parse",
  "zero charges after failure",
] as const;

/** Exact NUMERIC partials and winners: supported shapes plus every certificate fallback. */
export const NUMERIC_FIXED_REACH = [
  "fixed",
  "selected",
  "nulls",
  "unsafe",
  "int64",
  "int64 negative",
  "int64 exact",
  "int64 boundaries",
  "int64 carry overflow",
  "int64 batch attempted",
  "int128",
  "wide",
  "mixed",
  "special",
  "dictionary",
  "constant",
  "many groups",
  "full batch",
  "batch winners once per group",
  "saved prefix before suffix",
  "PGlite exact",
  "zero charges",
] as const;

/** Negative declared scales expand the stored integer coefficient. */
export const NUMERIC_COEFFICIENT_BOUND_REACH = [
  "negative scale",
  "ordinary update",
  "fused admission",
  "PGlite exact",
  "zero charges",
] as const;

export const NUMERIC_FIXED_CHARGE_REACH = [
  "sum",
  "min",
  "max",
  "charged before construction",
  "zero charges",
] as const;

export const NUMERIC_BENCH_REACH = [
  "native and plain exact",
  "single row",
  "partial batch",
  "multiple batches",
  "dictionary epochs",
  "full timed volume",
  "zero charges",
] as const;

export const NUMERIC_FIXED_SQL_REACH = [
  "small",
  "int64 fit",
  "positive overflow",
  "negative overflow",
  "cancelling overflow",
  "grouped",
  "scalar",
  "selection",
  "flushed scan",
  "multi-batch",
  "PGlite exact",
  "zero resources",
] as const;

/** PG float transitions across frames and spill pages, including errors before later specials. */
export const WINDOW_FLOAT_REACH = [
  "signed zero",
  "overflow",
  "special values",
  "float4",
  "page boundaries",
  "sum",
  "avg",
  "resident",
  "streamed",
  "22003",
  "unused prefix overflow",
  "exclusion fragments",
] as const;

/** Window transition errors belong to demanded output frames, including early consumers. */
export const WINDOW_FLOAT_DEMAND_REACH = [
  "resident",
  "streamed",
  "sum",
  "avg",
  "LIMIT",
  "OFFSET",
  "error after demand",
  "error within demand",
  "NaN after overflow",
  "cursor close",
  "batch boundary",
] as const;

export const WINDOW_FLOAT_SQL_DEMAND_REACH = [
  "LIMIT",
  "OFFSET",
  "existing order",
  "result sort",
  "whole partition",
  "top-k filter",
] as const;

/** SQL shift overload resolution, per-row evaluation and untyped argument contexts. */
export const WINDOW_SHIFT_SQL_REACH = [
  "lag",
  "lead",
  "partitions",
  "negative offset",
  "zero offset",
  "NULL offset",
  "row offset",
  "row default",
  "numeric common type",
  "float common type",
  "unknown default",
  "NULL default",
  "text default",
  "int64 exact",
  "lazy default",
  "parameter offset",
  "parameter default",
  "no ORDER BY",
  "invalid offset type",
  "default demand",
] as const;

export const WINDOW_SHIFT_DELIVERY_REACH = [
  "resident",
  "streamed",
  "lag",
  "lead",
  "NULL values",
  "page boundary",
  "default after demand",
] as const;

/** Current-row offset expressions must obey downstream window demand. */
export const WINDOW_OFFSET_DEMAND_REACH = [
  "offset coercion errors",
  "lag",
  "lead",
  "zero demand",
  "error demanded",
  "error after demand",
] as const;

/** Volatile defaults execute exactly once for each demanded out-of-range target. */
export const WINDOW_VOLATILE_DEFAULT_REACH = [
  "lag",
  "lead",
  "resident",
  "streamed",
  "zero demand",
  "OFFSET",
  "no default evaluation",
  "default evaluation",
  "dictionary offset validity",
] as const;

/** Partition/order reuse must preserve global ordering and incremental frame demand. */
export const WINDOW_PARTITION_DEMAND_REACH = [
  "sum",
  "avg",
  "computed partition",
  "nullable partitions",
  "ascending",
  "descending",
  "matching order",
  "partition prefix",
  "mismatching order",
  "full ordered result",
  "multiple partition keys",
] as const;

/** Complete owner validation and private selective materialization are independently observed. */
export const VALIDATED_ROOT_REACH = [
  "owner isolation",
  "warm hit skips all validation",
  "only selected columns materialize",
  "many-alias selection stays funded",
  "fresh ref and statistic ownership",
  "pinned roots remain charged",
  "full reference identity",
  "optional refusal preserves ordinary minimum",
  "owned encoded bytes survive source release",
  "unselected corruption refused",
  "complete eager equality",
  "Buffer inputs own a tight copy",
  "concurrent admission drains",
  "cancelled borrower hands off or releases",
  "zero charges and detached binding",
] as const;

export const VALIDATED_ROOT_PRESSURE_REACH = [
  "optional workspace refused synchronously",
  "ordinary native bound unchanged",
  "no optional FIFO wait",
  "zero charges after fallback",
] as const;

/** Packing interns extent identity within a complete root, never between selected graphs. */
export const ROOT_PACKING_REACH = [
  "one packed extent",
  "row IDs share selected data",
  "row IDs share unselected data",
  "complete before selection",
  "selection before complete",
  "fresh selected extent graphs",
  "canonical bytes and zero charges",
] as const;

/** Canonical star TEXT classes, shared across source dictionaries with exact NULL equality. */
export const STAR_DICTIONARY_REACH = [
  "flat",
  "dictionary",
  "constant",
  "selection",
  "NULL",
  "dictionary epochs",
  "cross-source equality",
  "owned input",
  "decoded groups",
  "PGlite equality",
  "zero resources",
] as const;

export const STAR_DICTIONARY_FUNDING_REACH = [
  "charged before construction",
  "denied construction",
  "zero charges",
] as const;

export const STAR_COMPONENT_REACH = [
  "native text",
  "dictionary IDs",
  "complete repeated lifetimes",
  "empty result",
  "nonempty result",
  "zero charges",
] as const;

export const STAR_FIXTURE_PREFIX_REACH = [
  "growing identity",
  "old prefix intact",
  "owned backing",
  "stable prefix reused",
] as const;

export const STAR_BOUND_REACH = [
  "complete resident bounds",
  "missing",
  "not-PK",
  "wide",
  "many",
  "volatile",
  "alias",
] as const;

export const STAR_SQL_REACH = [
  "like",
  "ilike",
  "not-like",
  "escaped-like",
  "one-dimension",
  "two-dimensions",
  "attribute-boolean",
  "left-fallback",
  "stored",
  "single-key",
  "memory",
  "mixed",
  "non-PK",
  "composite",
  "parameter",
  "wide",
  "computed",
  "zero",
  "resident joins",
  "PG exact",
  "generic exact",
  "ascending",
  "descending",
  "aggregate order",
  "lexical top-N",
] as const;

export const STAR_EDGE_REACH = ["ready", "blocked", "cancel", "map-error", "push-error"] as const;

export const STAR_PRESSURE_REACH = [
  "release",
  "wait-cancel",
  "read-cancel",
  "read-fault",
  "output-cancel",
  "concurrent",
  "publish",
  "reclaim",
  "atomic wait",
  "holder release",
  "PG exact",
  "protected prepared credit",
  "pinned growth",
  "zero resources",
] as const;

export const STAR_FLOAT_REACH = [
  "sum",
  "avg",
  "min",
  "max",
  "sxx",
  "nulls",
  "finite",
  "special",
  "overflow",
  "empty",
  "batch boundary",
  "nonempty",
  "identical native sequence",
  "bit exact result",
  "PG ordered transitions",
] as const;

export const NATIVE_DIRECTORY_BUDGET_REACH = [
  "parsed cache hit",
  "independent native peak",
  "wide float payload",
  "sparse NULLs",
  "all valid",
  "fixed native peak funds page loads",
  "zero charges after cleanup",
] as const;

export const LIKE_REACH = [
  "like",
  "ilike",
  "negated",
  "values",
  "escape error",
  "flat",
  "constant",
  "dictionary",
  "selected",
  "null-arg",
  "null-pattern",
  "C",
  "POSIX",
  "unicode scalar",
  "no escape",
  "unicode escape",
] as const;

/** Compound set/join progress with a separate grant retaining all shared headroom. */
export const COMPOUND_PROGRESS_REACH = [
  "EXCEPT repeated parameter",
  "join repeated parameter",
  "EXCEPT literal",
  "EXCEPT stored",
  "EXCEPT",
  "EXCEPT ALL",
  "duplicates",
  "join INNER",
  "join LEFT",
  "join RIGHT",
  "join FULL",
  "join RIGHT empty probe",
  "join stored",
  "join residual payload",
  "join RIGHT empty payload",
  "semi",
  "anti",
  "mark",
  "PG exact",
  "all ordinary credit occupied",
  "cleanup",
  "large recursive join",
] as const;

export const NATIVE_COMPOUND_REACH = [
  "word key payload",
  "empty probe",
  "wide collision page",
  "medium collision page",
  "ordinary collision page",
  "ALL",
  "DISTINCT",
  "collision fallback",
  "NULL and duplicate keys",
  "selected dictionary",
  "raw key",
  "encoded key",
  "inner",
  "left",
  "right",
  "full",
  "semi",
  "anti",
  "mark",
  "backpressure",
] as const;

/** Encoded C groups retain a separate key table beside the full-key MARK table. */
export const MARK_GROUP_PROGRESS_REACH = [
  "native",
  "stored",
  "TRUE",
  "FALSE",
  "UNKNOWN",
  "bounded encoder",
  "cleanup",
] as const;

export const COMPOUND_HISTORY_REACH = [
  "afterEach restoration",
  "abandoned shrinker stopped",
  "abandoned observer stopped",
  "queued execution cancelled",
  "late closer inert",
  "idempotent close",
  "zero resources",
] as const;

export const COMPOUND_PROJECTION_REACH = [
  "record",
  "except",
  "hidden large text",
  "hidden integer",
  "selected dictionary",
  "selected flat",
  "wide UTF8 prefix",
  "nonzero slot",
  "loan",
  "PostgreSQL exact",
  "zero resources",
] as const;

/** Every physical key occurrence must be prepaid, including duplicate input indexes. */
export const JOIN_REPEATED_PROGRESS_REACH = [
  "inner",
  "left",
  "right",
  "full",
  "semi",
  "anti",
  "mark",
  "unique positions",
  "duplicated position",
  "repeated position",
  "asymmetric repetitions",
  "arena boundary",
  "seventeen repeats",
  "fixed repeats with large payload",
  "partitioned four MB key page",
  "forwarded distinct-key partition",
  "small key large payload",
  "encoded repeated correlation",
  "retired grant and private restore lane",
  "selected dictionary",
  "ordinary credit occupied",
  "observed chunk and loan",
  "PG exact",
  "zero resources",
] as const;

export const JOIN_REPEATED_SQL_REACH = [
  "inner",
  "left",
  "right",
  "full",
  "stored source",
  "repeated parameter",
  "asymmetric repeated predicates",
  "cross-field grid",
  "single-row source envelope",
  "ordinary credit occupied",
  "small key large payload",
  "32 MiB cap",
  "independent key and payload slots",
  "independent key and payload literals",
  "8 MiB literal cap",
  "PG exact",
  "zero resources",
] as const;

/** Pressure restore sizes each owned slot independently, in its original position. */
export const OWNED_PARAMETER_COLUMN_BOUNDS_REACH = [
  "independent slot bounds",
  "original slot order",
  "phase binding bounds",
] as const;

/** Native window emission shares prepaid workspace under complete competing admission. */
export const WINDOW_PEER_PRESSURE_REACH = [
  "jsonb",
  "array",
  "all credit occupied",
  "unpressured",
] as const;

export const JSON_AGGREGATE_REACH = [
  "object aggregation",
  "array aggregation",
  "JSON",
  "JSONB",
  "ascending",
  "descending",
  "empty input",
  "all NULL values",
  "omit NULL values",
  "duplicate object keys",
  "unique object keys",
  "exact text",
  "UTF8 key order",
] as const;

export const JSON_AGGREGATE_CAP_REACH = [
  "exact cap divided by two succeeds",
  "first byte above boundary refuses",
  "JSON",
  "JSONB",
  "small inputs large aggregate",
  "zero resources",
] as const;

export const JSON_AGGREGATE_PRESSURE_REACH = JSON_CONSTRUCT_PRESSURE_REACH;

export const JSON_AGGREGATE_TYPED_REACH = [
  "int4",
  "numeric",
  "float4",
  "float8",
  "json",
  "jsonb",
  "descending",
  "ascending",
  "FILTER",
  "empty group",
  "ordinary siblings",
  "exact text",
  "DISTINCT typed scalar",
] as const;

export const JSON_AGGREGATE_ERROR_REACH = [
  "JSON",
  "JSONB",
  "unique",
  "strict",
  "NULL key",
  "value conversion",
  "descending",
  "ascending",
  "PG SQLSTATE",
  "duplicate omitted NULL values",
] as const;

export const JSON_AGGREGATE_GROUP_REACH = [
  "sixteen groups",
  "thirty-two groups",
  "observed native sort spill",
  "ordered nonuniform members",
  "ordinary sibling",
  "exact PG text",
  "zero resources",
] as const;

export const JSON_BLOB_REACH = [
  "multiple native pages",
  "forward byte order",
  "zero resources",
] as const;

export const JSON_RECORD_REACH = [
  "pretty record",
  "compact record",
  "nested ROW",
  "JSONB record",
  "present all-NULL row",
  "absent outer row",
  "aggregate record separators",
  "exact PG text",
] as const;

export const JSON_BLOB_LIFETIME_REACH = [
  "pending visitor",
  "close waits",
  "visitor fault",
  "visitor resumes",
  "zero resources",
] as const;

export const JSON_AGGREGATE_PARENT_REACH = [
  "retaining join",
  "retaining window",
  "bound true",
  "bound false",
  "consumed build arm",
  "JSONB",
  "JSON",
  "exact text",
  "zero files",
] as const;

export const JSON_AGGREGATE_FILTER_REACH = [
  "kept error",
  "false mask",
  "NULL mask",
  "value expression",
  "key expression",
  "ordering expression",
  "JSON",
  "JSONB",
  "mixed accepted and rejected rows",
  "PG rows or SQLSTATE",
  "filter error before value error",
] as const;

export const JSON_AGGREGATE_KEY_REACH = [
  "scalar",
  "object",
  "array",
  "one fractional zero",
  "several fractional zeros",
  "PG semantic equal pairs",
  "JSONB key",
  "SQL array key",
  "numeric array scale equality",
  "exact PostgreSQL groups",
  "zero spill files",
] as const;

export const JSON_RECORD_NAMES_REACH = [
  "duplicate",
  "dotted",
  "case-sensitive",
  "embedded quote",
  "mixed labels",
  "derived",
  "CTE",
  "wildcard",
  "nested record",
  "hidden ORDER BY",
  "DISTINCT",
  "CTE column list",
  "derived column list",
  "window fields",
  "literal UNION ALL",
  "aliased wildcard",
  "computed fields",
  "typed fields",
  "pretty",
  "compact",
  "exact JSON and JSONB text",
] as const;

export const JSON_AGGREGATE_SMALL_CAP_REACH = [
  "json_agg",
  "jsonb_agg",
  "json_object_agg",
  "jsonb_object_agg",
  "four MiB cap",
  "larger small cap",
  "empty aggregate",
  "nonempty aggregate",
  "zero resources",
  "tiny JSONB group key",
] as const;

export const JSON_GROUP_ROW_PRESSURE_REACH = [
  "observed spilled representative",
  "no retained key loan",
  "observed loan repaid",
  "semantic JSONB key",
  "semantic interval array key",
  "observed loan within compiled envelope",
  "byte text key",
  "multibyte key",
  "ASCII key",
  "cancel next restore",
  "restore for output",
  "exact PG equality",
  "zero resources",
] as const;

export const JSON_GROUP_SQL_PRESSURE_REACH = [
  "ordinary credit occupied",
  "async initial group admission",
  "producer credit occupied",
  "delivery credit occupied",
  "wide retained TEXT key",
  "wide semantic JSONB key",
  "expanded JSONB key",
  "exact PG aggregate",
  "observed spill",
  "zero resources",
] as const;

/** Independent field widths keep semantic grouping from inheriting payload copies. */
export const JSON_GROUP_WIDTH_REACH = [
  "payload wider than key",
  "key at least payload width",
  "65536-byte payload",
  "other payload width",
  "wide restored payload tiny key",
  "tiny key",
  "wide key",
  "sixteen MiB cap",
  "independent small cap",
  "exact PostgreSQL output",
  "zero resources",
  "expanding numeric key",
  "ordinary canonical key",
] as const;

export const JSON_RECORD_WIDTH_REACH = [
  "copy after seal",
  "first field bound",
  "second field bound",
  "append does not shrink bounds",
  "exact restored bytes",
  "UTF8 fields",
  "ASCII fields",
  "zero resources",
] as const;

/** Actual grouping consumers own key operands; unrelated lanes and delivery do not. */
export const JSON_GROUP_SCOPED_REACH = [
  "one key",
  "four distinct keys",
  "ASCII keys",
  "UTF8 keys",
  "multiple inputs",
  "competing credit includes reserve",
  "observed spill",
  "sixteen MiB cap",
  "exact PG output",
  "zero resources",
] as const;

/** Byte-valued array group equality borrows one encoded element at a time. */
export const JSON_BYTE_ARRAY_GROUP_REACH = [
  "text",
  "bytes",
  "enum",
  "0 dimensions",
  "1 dimensions",
  "2 dimensions",
  "same",
  "value",
  "bounds",
  "shape",
  "null",
  "redundant bitmap equality",
  "NULL elements",
  "NULL arrays",
  "exact PG equality",
] as const;

/** Additional consumer phases keep their independent cap and pressure reach explicit. */
export const JSON_GROUP_CONSUMER_REACH = {
  single: ["single input", "ten MiB cap"],
  composed: ["thirty-two MiB cap"],
  ordered: ["structural aggregate ORDER BY"],
  parent: ["two groups", "parent consumer pressure"],
  sort: ["sort"],
  window: ["window", "bounded window key range"],
  opaque: [
    "opaque aggregate result",
    "small result",
    "large result",
    "ASCII literal",
    "UTF8 literal",
    "two groups",
    "four groups",
    "twelve MiB cap",
    "sixteen MiB cap",
    "parent consumer pressure",
    "competing credit includes reserve",
    "observed spill",
    "exact PG output",
    "zero resources",
  ],
} as const;

export const SNAPSHOT_CODEC_REACH = [
  "empty tables",
  "table rows",
  "zero columns",
  "multiple columns",
  "split input",
  "large part",
  "truncation",
  "corruption",
  "reorder",
  "quota",
  "cancel",
  "trailing",
  "all kinds",
] as const;

export const SNAPSHOT_EXPORT_REACH = [
  "deleted rows",
  "all live",
  "resident",
  "persisted",
  "repoint",
  "during export",
  "quota",
  "cancel",
] as const;

export const SNAPSHOT_PHYSICAL_REACH = [
  "bool",
  "i32",
  "f32",
  "f64",
  "int64",
  "interval",
  "decimal",
  "fixed16",
] as const;

export const SNAPSHOT_INPUT_REACH = [
  "short id",
  "long id",
  "invalid lsn",
  "no writes",
  "zero charges",
  "legacy refusal",
  "version refusal",
  "sync close failure",
  "async close failure",
] as const;

export const SNAPSHOT_EXPORT_LIFETIME_REACH = [
  "one persisted group",
  "many persisted groups",
  "stalled producer cancelled",
  "stalled",
  "throws",
  "rejects",
] as const;

export const SNAPSHOT_SOURCE_REACH = [
  "resident vector",
  "published constant page",
  "published dictionary page",
  "array byte equality",
] as const;

/** Independent histories retain the streamed ring's exact capacity boundary under coverage. */
export const WINDOW_RING_GROWTH_CASES = [
  { rows: 16_383, state: "below 16384 rows" },
  { rows: 16_384, state: "at 16384 rows" },
  { rows: 16_385, state: "above 16384 rows" },
  { rows: 30_000, state: "original 30000-row partition" },
] as const;

export const AGG_WIDE_PROGRESS_REACH = [
  "repeated parameter",
  "distinct parameters",
  "UTF8 widths",
  "ASCII widths",
  "pinned ordinary room",
  "bounded sort loan",
  "PG exact",
  "partial staged row",
  "cancelled loan",
] as const;

export const AGG_ADMISSION_PEAK_REACH = [
  "distinct",
  "one key",
  "sixteen keys",
  "eight key witness",
  "twelve key witness",
  "short",
  "null-prefix",
  "direct",
  "max",
  "case",
  "nullif",
  "UTF8",
  "ASCII",
  "near parent peak",
  "32MiB short witness",
  "64MiB restore witness",
  "PG exact",
  "peak no higher",
  "zero",
  "nested",
  "nullable-max",
  "max not sum",
  "small output lane",
] as const;

export const AGG_STEP_PEAK_REACH = [
  "raw restore pages",
  "saved prefix",
  "distinct keys",
  "repeated keys",
  "repinned after admission",
  "restore",
  "shared peak",
  "trim live key",
  "cancel live key",
  "PG exact",
  "UTF8",
  "ASCII",
  "one logical wide key",
  "sixteen logical keys",
  "zero",
] as const;

export const AGG_PACKED_BINDINGS_REACH = [
  "computed key",
  "logical aliases",
  "distinct params",
  "typmod preserved",
  "nulls",
  "remapped arguments",
  "PG exact",
] as const;

export const AGG_COLUMN_PROGRESS_REACH = [
  "one key",
  "sixteen keys",
  "NULL",
  "ASCII",
  "UTF8",
  "pinned",
  "PG exact",
  "zero",
] as const;

export const AGG_CALL_PROGRESS_REACH = [
  "max",
  "min",
  "omitted extrema DISTINCT",
  "wide extrema DISTINCT",
  "preceding DISTINCT",
  "following DISTINCT",
  "wide DISTINCT",
  "ASCII",
  "UTF8",
  "pinned competitor",
  "transition loan",
  "PG exact",
  "cancelled",
] as const;

export const AGG_CALL_PREFIX_REACH = [
  "MIN",
  "MAX",
  "ASCII",
  "UTF8",
  "nonempty saved value",
  "encoded framing excluded",
  "one exact loan",
  "PG suffix exact",
  "zero charges",
] as const;

export const AGG_SORT_WORKSPACE_REACH = [
  "value sort",
  "ordinal sort",
  "exact arena",
  "rounded arena",
  "page output",
  "arena output",
  "owner correction propagated",
  "metadata retained",
] as const;

export const PROJECTION_PROGRESS_REACH = [
  "CASE",
  "nested COALESCE NULLIF",
  "column CASE",
  "short reachable branch",
  "cancelled",
  "PG exact",
  "pinned competitor",
  "ASCII",
  "UTF8",
] as const;
