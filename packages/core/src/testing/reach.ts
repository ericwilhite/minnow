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
export const EXECUTION_RETIREMENT_REACH = [
  "task-yield cancellation",
  "IO cancellation",
  "launch then program",
  "immediate program",
  "queued admission aborted",
  "cancel",
  "dispose",
  "delivery remains prompt",
  "IO abort prompt",
  "sizing drained before retirement",
  "zero charges",
] as const;

/** Ordered hot-key traversal stays linear relative to the legacy input scan. */
export const HOT_KEY_SORT_WORK_STATES = [
  "exact ordered rows",
  "linear stream work",
  "bounded legacy ratio",
] as const;

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
  "coherent older profile loaded",
  "older own floor accepted",
  "same manifest reader refusal",
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
  "ftsJournal",
  "ftsRoot",
  "ftsTerms",
  "ftsPostings",
  "ftsDocuments",
  "ftsDelta",
  "snapshot",
  "snapshotFrame",
  "snapshotFooter",
  "snapshotColumn",
  "snapshotCatalog",
  "snapshotSequence",
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
  "catalogRoot",
  "catalogPage",
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
  "minimum writer",
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
  "filtered raw aggregate",
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
  "filtered raw aggregate",
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
  "headroom released at scan open",
  "growth boundary pressure",
  "followers start at first handoff",
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

/** Read-source admission: actual wide prefixes, not hypothetical maximum-format publications. */
export const SOURCE_WIDE_ADMISSION_REACH = [
  "six or more wide columns",
  "scheduler-normalized allocation peak",
  "few columns",
  "single row",
  "multiple rows",
  "UTF8",
  "ASCII",
  "exact PostgreSQL rows",
  "zero resources",
] as const;

export const SOURCE_READ_RECIPE_REACH = [
  "owner-charged publication retention",
  "competing credit without partial execution holdings",
  "cancelled admission",
  "exact PostgreSQL rows",
  "actual published root recipe",
  "caller-funded complete roots",
  "invisible suffix excluded",
  "producer handoff",
  "zero resources before owner pin release",
] as const;

/** Source ownership is checked after native column reads, not just before admission. */
export const SOURCE_MIDSCAN_REACH = [
  "after first column",
  "after second column",
  "publication during scan",
  "observed compaction",
  "writer close retains charged source",
  "snapshot release during scan",
  "cancellation during scan",
  "source recipe returns borrow after execution",
  "last pin returns retained charge",
  "two reader release fence",
  "exact PostgreSQL rows",
  "zero query resources",
] as const;

/** Each fixed key-count cell generates independent widths/payloads and real pressure. */
export const JSON_GROUP_STEP_REACH = [
  "sixteen MiB cap",
  "all competing credit occupied",
  "exact PostgreSQL text",
  "wide payload",
  "tiny payload",
  "UTF8",
  "ASCII",
  "FILTER",
  "unfiltered",
  "SQL NULL key",
  "nonnull keys",
  "multiple inputs",
  "observed spill",
  "zero resources",
] as const;

/** Every literal/computed key count is a permanent step-peak generator dimension. */
export const JSON_GROUP_STEP_COUNTS_REACH = [
  "keys 1",
  "keys 2",
  "keys 3",
  "keys 4",
  "keys 5",
  "keys 6",
  "keys 7",
  "keys 8",
  "keys 9",
  "keys 10",
  "keys 11",
  "keys 12",
  "keys 13",
  "keys 14",
  "keys 15",
  "keys 16",
] as const;

/** Nested structural controls must never exceed the frozen fitting main peaks. */
export const JSON_GROUP_NESTED_PEAK_REACH = [
  "wide first key",
  "wide middle key",
  "wide last key",
  "uniform widths",
  "UTF8",
  "ASCII",
  "ascending aggregate",
  "descending aggregate",
  "NULL first key",
  "NULL middle key",
  "NULL last key",
  "sixteen MiB",
  "nested keys",
  "exact PostgreSQL text",
  "no peak rise",
  "zero resources",
] as const;

/** New widths/container depths and NULL-group layouts, beyond frozen peak controls. */
export const JSON_GROUP_NESTED_GENERATED_REACH = [
  "one key",
  "multiple keys",
  "six keys",
  "wide widths",
  "narrow widths",
  "shallow nesting",
  "nested containers",
  "deep nesting",
  "SQL NULL groups",
  "no NULL groups",
  "multiple NULL groups",
  "ASCII",
  "UTF8",
  "observed resident cache",
  "decoded cache retired",
  "unused growth returned",
  "new random shape",
  "exact PostgreSQL text",
  "zero resources",
] as const;

/** Shared FTS v1 semantics independently checked against shipped SQL before native wiring. */
export const FTS_SHARED_REACH = [
  "filtered ranking retains whole-corpus stats",
  "whole corpus includes NULL/empty rows",
  "cross-column AND",
  "prefix df union",
  "repeated query terms",
  "empty query and NULL truth",
] as const;

export const PROJECTION_PROGRESS_REACH = [
  "wide result delivery",
  "NULL input",
  "NULLIF equal",
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

export const PROJECTION_SPLIT_REACH = [
  "adaptive to ordinary",
  "multi-row prefix",
  "dense",
  "sparse",
  "exact values",
  "one evaluation",
  "no payload allocation",
  "held borrow",
  "cancel wait",
  "error once",
] as const;

export const PROJECTION_PEAK_REACH = [
  "fitting first",
  "ordinary second",
  "no peak increase",
] as const;

export const EXPRESSION_ERROR_REACH = [
  "denied prefix error",
  "single row",
  "full batch",
  "filter",
  "target list",
  "pinned competitor",
  "ordinary",
  "emitted prefix",
  "first row error",
  "division first",
  "overflow first",
  "lazy COALESCE CASE",
] as const;

export const EXPRESSION_FENCE_REACH = [
  "shallow ambient stack",
  "join key",
  "sort key",
  "cancelled suspension",
  "helper close fence",
  "no post-cancel rows",
  "cleanup zero",
] as const;

/** MCAT fields must carry exact values and distinguish corruption from future semantics. */
export const CATALOG_RECORD_REACH = [
  "record round trip",
  "single charged output",
  "exact bigint",
  "unknown optional read only",
  "unknown optional writer refused",
  "unknown required refused",
  "truncated refused",
  "duplicate/out-of-order refused",
  "small field id",
  "wide field id",
  "maximum field id",
  "reserved field flags refused",
] as const;

export const CATALOG_PAGE_REACH = [
  "single page",
  "multiple pages",
  "leaf index",
  "internal index",
  "exact page bytes",
  "exact references",
  "truncation refused",
  "future version refused",
  "flags refused",
  "zero charged resources",
] as const;

export const CATALOG_STORAGE_REACH = [
  "exact catalog values",
  "bounded workspace",
  "actual data pages",
  "cancelled iterator cleanup",
  "zero resources",
] as const;

export const CATALOG_STORAGE_FAULT_REACH = [
  "data write cutoff",
  "index write cutoff",
  "old root unchanged",
  "writer aborted",
  "zero temporary allocations",
] as const;

export const CATALOG_STREAM_REACH = [
  "empty catalog",
  "multiple tables",
  "WAL exact round trip",
  "one owned record",
  "future version refused",
  "flags refused",
  "truncation refused",
  "zero buffers",
] as const;

export const CATALOG_FRAGMENT_REACH = [
  "single-byte fragments",
  "multiple-byte fragments",
  "empty fragments",
  "exact descriptors",
  "iterator closed",
  "cancellation",
  "zero buffers",
] as const;

export const CATALOG_NATIVE_REACH = [
  "WAL replay exact",
  "native checkpoint exact",
  "continued writes exact",
  "single data page",
  "multiple data pages",
  "quoted UTF8 identifiers",
  "exact sequence config",
  "zero resources",
] as const;

export const CATALOG_GRAPH_REACH = [
  "counter rewind",
  "duplicate columns",
  "dangling index",
  "dangling foreign key",
  "dangling identity",
  "relation name collision",
  "dangling primary key",
  "duplicate enum labels",
] as const;

export const CATALOG_LOG_SHAPE_REACH = [
  "createFts",
  "upsertFts",
  "dropFts",
  "createTrigger",
  "dropTrigger",
  "alterTable",
  "createTable",
  "dropTable",
  "createIndex",
  "invalidateIndex",
  "dropIndex",
  "putSequence",
  "dropSequence",
  "createView",
  "replaceView",
  "dropView",
  "createType",
  "dropType",
  "missing required field",
  "contradictory field",
] as const;

export const CATALOG_TREE_REACH = [
  "exact fragments",
  "parent summary refused",
  "page offset refused",
  "early return cleanup",
  "cancellation cleanup",
  "zero resources",
] as const;

export const CATALOG_RECYCLE_REACH = [
  "publish-held sequence reservation",
  "deferral observed",
  "reserved values durable",
] as const;

export const CATALOG_REVIEW_REACH = [
  "enum missing labels",
  "enum empty labels",
  "missing vector huge length",
  "missing vector short data",
  "declared mismatch",
  "physical wrong conditional field",
  "physical missing width",
  "decimal missing precision",
  "vector wrong layout",
  "support wrong columns",
  "missing support",
  "parent key type",
  "short validity",
  "array missing element",
] as const;

export const CATALOG_INFORMATIONAL_REACH = ["sim", "memory", "checkpoint", "WAL only"] as const;

export const CATALOG_WAL_VERSION_REACH = [
  "sim",
  "memory",
  "live-assigned version gap preserved",
  "bad predecessor refused",
  "no earlier row effect applied",
] as const;

export const CATALOG_MISSING_REACH = [
  "bool",
  "i32",
  "f32",
  "f64",
  "int64",
  "fixed16",
  "interval",
  "decimal",
  "varlen",
  "safe",
  "int128",
  "wide",
  "per-value scale",
  "scale-only declaration",
  "native bytes",
  "length",
  "validity",
  "type",
  "conditional payload",
] as const;

export const CATALOG_NULL_DECIMAL_REACH = [
  "live special refused",
  "live scale refused",
  "live value",
  "NULL payload ignored",
  "scale zero",
  "scale boundary",
] as const;

export const CATALOG_SAFE_VALUES_REACH = [
  "int64",
  "time",
  "timestamp",
  "timestamptz",
  "interval",
  "decimal",
  "exact live",
  "NULL ignored",
  "invalid live refused",
  "NaN",
  "infinity",
  "negative zero",
  "fraction",
  "unsafe integer",
] as const;

export const CATALOG_RESERVATION_REACH = [
  "reserved reader refused",
  "fts 8",
  "fts 9",
  "fts 10",
  "optional",
  "required",
  "optional reader tolerated",
  "optional writer refused",
] as const;

export const BYTE_GROWTH_REACH = [
  "key denial split",
  "funded transition",
  "dictionary",
  "flat",
  "single key",
  "compound keys",
  "selected",
  "large then small",
  "zero",
] as const;

export const HASH_TRANSITION_REACH = [
  "12 keys",
  "16 keys",
  "16 MiB",
  "32 MiB",
  "64 MiB",
  "128 MiB",
  "mixed fixed/byte keys",
  "resident prefix",
  "saved float prefix",
  "ordinary",
  "pinned spill",
  "reclaim release",
  "cancel reclaim",
  "PG exact",
  "distinct parameters",
  "repeated parameters",
  "compound keys",
  "UTF8",
  "zero",
] as const;

export const HASH_PARENT_PEAK_REACH = [
  "2 keys",
  "3 keys",
  "4 keys",
  "8 keys",
  "16 keys",
  "dictionary",
  "flat",
  "selected",
  "dense",
  "unequal widths",
  "later batch",
  "parent peak",
  "zero",
] as const;

export const HASH_RESULT_PROGRESS_REACH = [
  "emitted aliases",
  "small row tail",
  "COUNT only",
  "emitted key",
  "variable aggregate",
  "one-row scan",
  "column",
  "parameter",
  "competitor",
  "12 keys",
  "16 keys",
  "16 MiB",
  "32 MiB",
  "64 MiB",
  "128 MiB",
  "unreachable wide branch",
  "fitting peak",
  "PG exact",
  "zero",
] as const;

export const HASH_SOURCE_VALUE_REACH = [
  "progress floor insufficient",
  "large values",
  "UTF8",
  "repeated rows",
  "multiple columns",
  "charged sizing",
  "zero",
] as const;

export const LISTED_REOPEN_CATALOG_REACH = [
  "native catalog",
  "single publication",
  "multiple publications",
  "audit",
  "no IO charge",
] as const;

/** Native scan FTS: query grammar and snapshot-corpus statistics are separate axes. */
export const FTS_SCAN_REACH = [
  "whole corpus",
  "NULL empty",
  "prefix union",
  "Unicode",
  "duplicate terms and columns",
  "parameters",
  "schema and wildcard rebind",
  "numeric display width",
] as const;

export const FTS_SCAN_PRESSURE_REACH = [
  "competing grant",
  "spill",
  "df union",
  "backpressure",
  "exact scores",
  "drained",
] as const;

export const FTS_RENDER_REACH = [
  "versioned",
  "TIME canonical",
  "ARRAY canonical",
  "JSON SQL text",
  "JSONB SQL text",
  "NULL elements and escaping",
  "shipped unchanged",
] as const;

export const FTS_SQL_PRESSURE_REACH = [
  "wide FTS projection",
  "shipped exact",
  "all credit occupied",
  "native SQL spill",
  "drained",
] as const;

export const FTS_JSON_REACH = [
  "nested",
  "siblings",
  "CASE demand",
  "owned parameters across cut",
  "drained",
] as const;

export const FTS_CANCEL_REACH = [
  "append suspended",
  "source still charged",
  "closed fenced",
  "drained",
] as const;

export const FTS_WRITE_REACH = [
  "UPDATE",
  "DELETE",
  "whole statement corpus",
  "parameters and threshold",
  "fresh inserted corpus",
] as const;

export const FTS_QUERY_REACH = [
  "static query source",
  "bounded terms",
  "UTF16 truncation",
  "charged metadata",
  "constant parsed once",
  "drained",
] as const;

/** Renderer-v2 array output uses PG's character length and array_out quoting. */
export const FTS_ARRAY_RENDER_REACH = [
  "leading whitespace",
  "trailing whitespace",
  "ASCII whitespace",
  "Unicode whitespace",
  "exact PostgreSQL text",
  "below character limit",
  "at character limit",
  "above limit refused",
  "BIGINT encoded over64KiB",
  "MATCH BM25 SQL",
  "drained",
] as const;

export const FTS_DEFINITION_REACH = [
  "exact definition",
  "large IDs",
  "tokenizerVersion",
  "rendererVersion",
  "scorerVersion",
  "config version",
  "invalid IDs",
  "physical state excluded",
  "fixed config",
] as const;

export const FTS_CODEC_REACH = [
  "ftsRoot",
  "ftsTerms",
  "ftsPostings",
  "ftsDocuments",
  "ftsDelta",
  "leaf",
  "branch",
  "wide IDs",
  "raw UTF16",
  "charged ownership",
  "drained",
] as const;

export const FTS_SELECTOR_REACH = [
  "reader10 barrier",
  "writer10 barrier",
  "manifest13 barrier",
  "commit body9",
  "wide sequence preserved",
  "checksummed roundtrip",
] as const;

export const ARRAY_VISITOR_REACH = [
  "empty",
  "nonempty",
  "NULL",
  "beforeBytes hook",
  "borrowed bytes",
  "owned decoder bytes",
  "exact shapes",
] as const;

export const FTS_CATALOG_REACH = [
  "WAL",
  "native pages",
  "fragmented stream",
  "portable definitions",
  "stable IDs",
  "drop table",
  "stale WAL",
  "invalid binding",
  "unknown semantics",
  "optional reader tolerated",
  "optional writer refused",
  "drained",
] as const;
export const FTS_STORAGE_REACH = [
  "early close",
  "IO failure",
  "root",
  "directory",
  "terms branch",
  "postings branch",
  "documents branch",
  "delta branch",
  "exact graph",
  "orphan excluded",
  "parent bounds",
  "cancel",
  "write fence",
  "read fence",
  "drained",
] as const;

export const FTS_CATALOG_FEATURE_REACH = [
  "duplicate supported",
  "out of order",
  "future typed",
] as const;

export const FTS_REFERENCE_GUARD_REACH = [
  "checksummed oversized root",
  "beyond generic read cap",
  "outside extent",
  "zero length",
  "negative offset",
  "fractional length",
  "unsafe extent end",
  "invalid checksum",
  "nonfinite",
  "legal boundary reaches IO",
  "all five grammars",
  "initial walker root",
  "no allocation or IO",
] as const;

export const FTS_TERM_FENCE_REACH = [
  "equal fences multiple terms",
  "singleton distinct fences",
  "reversed fences",
  "adjacent prefix capacity",
  "maximum length interval capacity",
  "ftsTerms",
  "ftsRoot",
  "ftsDelta",
  "exact boundary accepted",
  "raw surrogate",
  "NUL code unit",
  "no child read needed",
  "drained",
] as const;

export const ORDERED_WIDE_REACH = [
  "aliases",
  "distinct bindings",
  "16 keys",
  "12 keys",
  "pinned",
  "top-N",
  "full sort",
  "PG exact",
  "zero",
] as const;

export const SEQUENCE_SQL_REACH = [
  "element modifiers",
  "string",
  "typed array",
  "NULL element",
  "empty input",
  "DISTINCT",
  "ORDER BY",
  "groups",
  "FILTER",
  "PG exact",
  "zero",
] as const;

export const ARRAY_CONCAT_REACH = [
  "derived boundary",
  "bounds 54000",
  "prepend 22003",
  "charged output",
  "cat",
  "append",
  "prepend",
  "NULL",
  "empty",
  "multidimensional",
  "lower bound",
  "constant",
  "dimension error",
  "PG exact",
  "zero",
] as const;

export const SEQUENCE_PRESSURE_REACH = [
  "independent aliases",
  "string",
  "array",
  "concat",
  "pinned",
  "handoff",
  "cancel",
  "expansion",
  "PG exact",
  "zero",
] as const;

export const SEQUENCE_CAP_REACH = [
  "string",
  "array",
  "NULL bitmap",
  "exact boundary",
  "next byte refuses",
  "zero",
] as const;

export const SEQUENCE_BYTES_REACH = [
  "empty",
  "large",
  "shared prefix",
  "caller mutation",
  "PG exact",
  "zero",
] as const;

export const SEQUENCE_ARRAY_INPUT_REACH = [
  "bounds precedence",
  "bounds 54000",
  "numeric scale equality",
  "DISTINCT",
  "same",
  "null",
  "empty",
  "mismatch",
  "multidim",
  "PG exact",
  "bounds",
  "FILTER",
  "DESC",
  "zero",
] as const;

export const SEQUENCE_PREFIX_REACH = [
  "grouped prefix",
  "54000",
  "22003",
  "pinned",
  "ordinary",
  "prefix",
  "first row",
  "zero",
] as const;

export const CONCAT_SLICE_REACH = ["complete", "cancel", "bitmap yield", "zero"] as const;

export const TEXT_OUTPUT_REACH = [
  "int2",
  "int4",
  "int8",
  "numeric",
  "float4",
  "float8",
  "boolean",
  "date",
  "time",
  "timestamp",
  "timestamptz",
  "interval",
  "uuid",
  "json",
  "jsonb",
  "array",
  "NULL",
  "numeric scale",
  "24:00",
  "infinity",
  "direct",
  "derived",
  "CTE",
  "PG exact",
  "zero",
] as const;

export const ARRAY_TEXT_OUTPUT_REACH = [
  "enum",
  "numeric specials",
  "mixed lower bounds",
  "text",
  "bytes",
  "scalar elements",
  "numeric scale",
  "boolean t/f",
  "PG float",
  "temporal elements",
  "multidimensional",
  "bounds",
  "NULL array",
  "NULL elements",
  "empty array",
  "shared FTS",
  "PG exact",
  "zero",
] as const;

export const TEXT_OUTPUT_PRESSURE_REACH = [
  "array",
  "numeric",
  "bytes",
  "PG exact",
  "cancel",
  "pinned",
  "handoff",
  "independent aliases",
  "owned byte input",
  "ten MB result",
  "zero",
] as const;

export const TEXT_OPERATOR_REACH = [
  "left text",
  "right text",
  "unknown text",
  "JSONB stays unsupported",
  "undefined operator",
  "zero",
] as const;
export const TEXT_OUTPUT_SLICE_REACH = [
  "funded decode",
  "complete",
  "before decode yield",
  "cancel",
  "redundant sign bytes",
  "zero",
] as const;

export const CATALOG_RELATIONSHIPS_REACH = [
  "dangling column",
  "generated self",
  "default column",
  "identity default",
  "closed set",
  "named type",
  "view column",
  "view cycle",
  "view outputs",
  "ordered dependencies",
  "identity owner",
] as const;

export const CATALOG_SEMANTIC_FIELDS_REACH = [
  "managed",
  "typeId",
  "allowedValues",
  "defaultDependencies",
  "generatedDependencies",
  "autoIncrement",
  "identityAlways",
  "directions",
  "dependencies",
  "outputs",
  "viewDependencies",
  "viewSourceSchemas",
  "expressionTypes",
  "downgrade refused",
] as const;

export const CATALOG_ENUM_REACH = [
  "NULL",
  "valid label",
  "invalid label",
  "unicode label",
  "empty label",
  "different named enum",
  "closed NULL",
  "closed valid",
  "closed invalid",
  "closed text order",
] as const;

export const CATALOG_ENUM_LABELS_REACH = [
  "valid label",
  "wide label",
  "ASCII",
  "BMP",
  "astral",
  "63 bytes",
  "64 bytes",
  "rank limit fits",
  "rank limit refused",
] as const;

export const CATALOG_ALTER_REACH = [
  "published",
  "resident",
  "old snapshot",
  "backfill",
  "rename",
  "drop",
  "reopen",
  "rollback",
] as const;

export const CATALOG_ALTER_WIDE_REACH = [
  "multipart",
  "small commit",
  "exact backfill",
  "new-schema rows",
] as const;

export const CATALOG_MIGRATION_REACH = [
  "view declaration",
  "invalid view rollback",
  "DSL number counter",
  "DSL integer counter",
  "published integer maximum",
  "resident integer maximum",
  "managed",
  "rename",
  "widen set",
  "backfill",
  "adopt counter",
  "explicit advances",
  "idempotent",
  "stale CAS",
  "NOT NULL rollback",
  "reopen",
  "unmanaged survives",
] as const;

export const CATALOG_OBJECTS_REACH = [
  "replace identity",
  "view drop dependency",
  "output compatibility",
  "duplicate output",
  "type dependency",
  "quoted view output",
  "drop type",
  "sequence dependency",
  "checkpoint restart",
  "WAL restart",
  "called restart",
  "uncalled restart",
  "drop sequence",
] as const;

export const CATALOG_MIGRATION_PUBLICATION_REACH = [
  "publication after read",
  "NULL only in suffix",
  "maximum only in suffix",
  "three chunks",
  "two chunks",
] as const;

export const CATALOG_OWNED_NAMES_REACH = [
  "collision",
  "no collision",
  "long ASCII",
  "long Unicode",
  "quoted names",
] as const;

export const CATALOG_STAGE2_DDL_REACH = [
  "index descending",
  "index ascending",
  "self FK enforcement",
  "named enum",
  "self unique",
  "self primary",
  "identity",
  "explicit integer",
  "plain",
  "case",
  "dot",
  "quote",
  "view outputs",
  "view dependency",
  "checkpoint reopen",
] as const;

export const CATALOG_TYPE_NAMESPACE_REACH = [
  "scalar builtin",
  "array builtin",
  "catalog composite",
  "pseudo builtin",
  "implicit pg_catalog first",
  "quoted alias",
  "novel custom",
] as const;

export const CATALOG_EXPRESSION_TYPES_REACH = [
  "default",
  "generated",
  "check",
  "literal data",
  "check sequence",
  "checkpoint",
  "WAL",
  "allocated IDs",
  "dangling type",
  "duplicate type",
  "noncanonical types",
] as const;

export const CATALOG_VIEW_PRUNING_REACH = [
  "plain unused",
  "unused referrer",
  "nested unused",
  "recursive unused",
  "materialized",
  "used CTE admitted",
  "ordinary query preserved",
  "replacement identity",
  "column precision",
] as const;

export const CATALOG_GENERATED_ENUM_REACH = [
  "explicit stable refused",
  "assignment allowed",
  "default allowed",
  "text",
  "varchar",
] as const;

export const CATALOG_FTS_ALTER_REACH = [
  "SQL dependency refusal",
  "direct dependency refusal",
  "rename",
  "original name",
  "checkpoint",
  "WAL",
  "scan after ALTER",
  "first column",
  "middle column",
  "last column",
] as const;

export const ALTER_DEPENDENCIES_REACH = [
  "generated",
  "generated only",
  "check",
  "view",
  "index",
  "forward drops",
  "reverse drops",
  "multiple drops",
  "short row",
  "long row",
  "quoted name",
  "checkpoint",
  "WAL",
  "referenced drop refused",
] as const;

export const ADDED_ENUM_REACH = [
  "plain",
  "quoted",
  "checkpoint",
  "WAL",
  "type drop refused",
] as const;

export const CATALOG_VIEW_SOURCES_REACH = [
  "simple",
  "qualified",
  "derived",
  "CTE",
  "USING",
  "NATURAL",
  "nested replacement",
  "column alias list",
  "plain name",
  "quoted name",
  "checkpoint",
  "WAL",
  "reopen",
  "binding corruption refused",
] as const;

export const ROW_RULES_REACH = [
  "dictionary",
  "flat",
  "inactive preserved",
  "CHECK false",
  "generated parity",
  "NULL passes",
  "generated CHECK dependency",
  "charged buffers",
  "selection",
  "dense",
  "OLD unchanged",
  "row error precedence",
  "expression error",
] as const;
export const ROW_RULES_FK_REACH = [
  "published source",
  "resident source",
  "OLD wrapper preserved",
  "CHECK rollback",
  "generated recomputed",
  "statement atomicity",
] as const;

export const ROW_RULE_ORDER_REACH = [
  "check",
  "generated",
  "null",
  "names",
  "success",
  "single row",
  "split batch",
  "whole batch",
  "reverse declaration",
  "forward declaration",
  "selection",
  "dense",
  "valid prefix",
  "first row failure",
  "plain name",
  "quoted name",
  "UTF8 names",
] as const;

export const IDENTITY_ALWAYS_REACH = [
  "explicit refused",
  "explicit allowed",
  "insert select",
  "update",
  "upsert",
  "merge",
  "default allowed",
  "batch input",
  "checkpoint",
  "WAL",
  "invalid mode refused",
  "smallint",
  "integer",
  "bigint",
  "plain",
  "quoted",
] as const;

export const TRIGGER_PREFIX_REACH = [
  "independent pins",
  "expired writer refused",
  "published",
  "resident",
  "start view unchanged",
  "prefix rewrite",
  "foreign pin refused",
  "released pin refused",
  "rollback",
  "commit",
  "end releases pin",
  "multiple bodies",
  "one body",
] as const;

export const SEMANTIC_CLEANUP_REACH = [
  "native drain pending",
  "checkpoint before delete",
  "terminate after deletion",
  "client sync failure",
  "client async failure",
  "database close failure",
  "delete failure",
  "all failures retained",
] as const;

export const SEMANTIC_NATIVE_CLEANUP_REACH = [
  "zero drain turns",
  "multiple drain turns",
  "close acknowledged before drain",
  "checkpoint drained before delete",
  "files deleted",
  "worker terminated after deletion",
] as const;

export const TRIGGER_CATALOG_REACH = [
  "introspection",
  "repeated binding",
  "single binding",
  "global name uniqueness",
  "invalid definition refused",
  "dependency refused",
  "checkpoint",
  "WAL",
  "reopen",
  "corruption refused",
  "identity not reused",
  "owner drop",
  "insert",
  "update",
  "delete",
  "before",
  "after",
  "plain binding",
  "quoted binding",
  "positional body",
  "named body",
  "ordered bodies",
  "single body",
] as const;

export const CATALOG_FK_ACTION_REACH = [
  "duplicate update refused",
  "duplicate delete refused",
  "introspection",
  "delete omitted",
  "delete noAction",
  "delete restrict",
  "delete cascade",
  "delete setNull",
  "delete setDefault",
  "update omitted",
  "update noAction",
  "update restrict",
  "update cascade",
  "update setNull",
  "update setDefault",
  "self reference",
  "external reference",
  "named constraint",
  "inline constraint",
  "update clause first",
  "delete clause first",
  "checkpoint",
  "WAL",
  "plain column",
  "quoted column",
  "reopen",
  "invalid mode refused",
] as const;

export const TRIGGER_RESERVED_NAME_REACH = [
  "reserved refused",
  "nearby quoted allowed",
  "folded allowed",
  "UTF8 suffix",
  "shipped accepts",
  "PG accepts",
] as const;

export const TRIGGER_WIRE_REACH = [
  "reserved refused",
  "required refused",
  "optional reader tolerated",
  "optional writer refused",
  "dialect refused",
  "source refused",
  "insert",
  "update",
  "delete",
  "before",
  "after",
  "one body",
  "many bodies",
  "plain name",
  "UTF8 name",
] as const;

export const TRIGGER_PREFIX_LIFECYCLE_REACH = [
  "cancel after several batches",
  "maintenance before later cancellation",
  "borrow ends before independent pin",
  "publication with borrowed prefix",
  "merge with prefix alive",
  "flush only",
  "cancel read",
  "cancel write",
  "committed bodies",
  "multiple write batches",
  "cached and fresh rereads refused",
  "cached cursor read refused",
  "zero native retention and registrations",
  "single row batches",
  "several row batches",
] as const;

export const TRIGGER_AFTER_QUEUE_REACH = [
  "spilled pages",
  "append during drain",
  "cancel continuation",
  "FIFO order",
  "wide IDs exact",
  "empty",
  "one row",
  "multiple blocks or rows",
  "fixed metadata and ordinary credit",
  "zero ownership",
] as const;

export const TRIGGER_QUEUE_READ_REACH = [
  "actual competing-credit loan",
  "progress returned before continuation",
  "directory carry",
  "one page",
  "wide scalar IDs",
  "ordinary IDs",
  "zero ownership",
] as const;

export const TRIGGER_BINDING_REACH = [
  "unique repeated bindings",
  "release before body",
  "unrequested payload unread",
  "exact native buffers",
  "PG typed body value",
  "zero ownership",
  "flat",
  "constant",
  "dictionary",
  "NULL",
  "fragmented UTF8",
  "decimal safe",
  "decimal int64",
  "decimal int128",
  "decimal wide",
  "wide int64",
  "timestamp microseconds",
] as const;

export const TRIGGER_BINDING_LAYOUT_REACH = [
  "SQL array",
  "interval",
  "UUID",
  "decimal NaN",
  "decimal Infinity",
  "decimal -Infinity",
  "exact admitted boundary",
  "NULL payload ignored",
  "live payload",
  "flat",
  "constant",
  "dictionary",
] as const;
export const TRIGGER_BUDGET_REACH = [
  "root budget",
  "body zero budget",
  "terminal negative RI budget",
  "two derived writes succeed",
  "third trigger rejected and rolled back",
  "competing credit",
  "BEFORE chain",
  "AFTER chain",
  "multiple root rows",
  "zero ownership",
] as const;

export const TRIGGER_BODY_REACH = [
  "prefix input sealed",
  "complete modify metadata",
  "binding copy retired before write",
  "native rules and PG constraint",
  "primary and cleanup errors retained",
  "PG commit",
  "multiple writer batches",
  "PG root rollback",
  "source failure",
  "apply failure",
  "cancel sealed input",
  "cancel partial write",
  "cancel image release",
  "cancel binding retirement",
  "cancel before source",
  "pin survives input retirement",
  "zero owned copies and readers",
  "pending source cancellation fenced",
  "pending retirement cancellation fenced",
  "invalid programs refuse before effects",
  "native generated NOT NULL matches PG",
] as const;

export const TRIGGER_CONTROLLER_REACH = [
  "completed command refuses more primary rows",
  "PG exact ordering",
  "parent update and RI child",
  "parent delete and RI child",
  "nested body before next statement",
  "three live frames",
  "competing grants",
  "UTF8 names across surrogate boundary",
  "old parent queue before RI child AFTER",
  "observed queue spill",
  "zero queues inputs pins and credit",
] as const;

export const TRIGGER_PREPARATION_REACH = [
  "terminal BEFORE",
  "terminal AFTER",
  "BEFORE omission refused",
  "AFTER omission refused",
  "RI route omission refused",
  "body route omission refused",
  "update route",
  "delete route",
  "terminal definition needs no compiled body",
  "zero queues copies and readers",
] as const;

export const TRIGGER_RI_UNIQUE_REACH = [
  "queue",
  "id",
  "entry",
  "no outer AFTER drain",
  "first action conflict",
  "successive actions one command",
  "two earlier action checkpoints pass",
  "PG error priority",
  "zero queues pins and credit",
] as const;

export const CATALOG_CREATE_BACKFILL_REACH = [
  "integer signed zero",
  "overridden Date methods",
  "throwing Date methods",
  "generated backfill refused before mutation",
  "boolean",
  "integer",
  "float",
  "text",
  "enum",
  "timestamp",
  "timestamptz",
  "WAL reopen",
  "checkpoint reopen",
  "continue writing",
  "zero resources",
] as const;

export const CATALOG_INFORMATIONAL_NAMES_REACH = [
  "dropped parent keeps names",
  "native checkpoint",
  "WAL replay",
  "scalar",
  "composite",
  "plain",
  "case dot UTF8",
  "quote",
  "remains informational",
  "later child ALTER",
  "replacement parent does not retarget",
  "zero resources",
] as const;

export const CATALOG_FREEZE_TYPES_REACH = [
  "int32",
  "int64",
  "float32",
  "float64",
  "decimal",
  "text",
  "bool",
  "date",
  "time",
  "timestamp",
  "json",
  "uuid",
  "interval",
  "array",
  "enum",
  "checkpoint",
  "WAL",
  "continue DDL",
  "zero resources",
] as const;

export const CATALOG_INFORMATIONAL_WIRE_REACH = [
  "missing both",
  "missing table",
  "missing columns",
  "wrong arity",
  "empty label",
  "required flags10 and11",
  "scalar",
  "composite",
] as const;

export const CATALOG_MIGRATE_BACKFILL_REACH = [
  "integer signed zero",
  "boolean",
  "integer",
  "float",
  "text",
  "enum",
  "timestamp",
  "timestamptz",
  "generator exactly once",
  "literal",
  "checkpoint",
  "WAL",
  "idempotent after reopen",
  "failure atomic",
  "zero resources",
] as const;

export const CATALOG_REAL_BACKFILL_REACH = [
  "ordinary",
  "overflow",
  "underflow",
  "catalog unchanged",
  "zero resources",
] as const;

export const CATALOG_DEFAULT_SOURCE_REACH = [
  "integer signed zero",
  "overridden Date methods",
  "throwing Date methods",
  "boolean",
  "integer",
  "float",
  "text",
  "enum",
  "timestamp",
  "timestamptz",
  "numeric-number",
  "numeric-string",
  "expression spelling",
  "literal JS type",
  "checkpoint",
  "WAL",
  "idempotent",
  "unrelated ALTER preserves source",
  "ALTER source kind",
  "default removal clears source",
  "signed zero",
  "empty text",
  "zero resources",
] as const;

export const CATALOG_DEFAULT_COUNTER_REACH = [
  "adopt counter",
  "remove counter",
  "float counter",
  "integer counter",
  "expression",
  "literal",
  "reopen idempotent",
  "zero resources",
] as const;
export const CATALOG_DEFAULT_WIRE_REACH = [
  "unknown tag",
  "SQL absent",
  "source type mismatch",
  "nonfinite",
  "invalid datetime",
  "unclosed text",
  "noncanonical boolean",
  "generated default",
  "noncanonical number",
  "literal NULL",
  "field16 required",
] as const;

export const CATALOG_BACKFILL_SIGN_REACH = [
  "int32",
  "int64",
  "false sign",
  "missing absent",
  "missing NULL",
  "nonzero value",
  "noninteger type",
  "required flag",
  "downgrade refused",
] as const;

export const CATALOG_CAPABILITIES_REACH = [
  "optional field",
  "required field",
  "unknown flags",
  "duplicate field",
  "out-of-order field",
  "unknown feature",
  "duplicate feature",
  "out-of-order feature",
  "feature disagreement",
  "unknown family",
  "record version",
  "stream version",
  "expression dialect",
  "semantic downgrade",
  "section order",
  "optional reader tolerated",
  "inspection cannot rewrite",
  "metadata",
  "nested counters",
  "nested physical",
  "fragment cleanup",
] as const;

export const CATALOG_SEMANTIC_CAPABILITIES_REACH = [
  "integer width",
  "decimal bounds",
  "array/JSON mode",
  "text bounds",
  "timestamp mode",
  "declared integer",
  "declared domain",
  "declared text/enum",
  "vector payload",
  "integer payload",
  "index state",
  "WAL operand",
] as const;
export const CATALOG_CAPABILITY_OPEN_REACH = [
  "optional field",
  "required field",
  "unknown flags",
  "duplicate field",
  "out-of-order field",
  "unknown feature",
  "duplicate feature",
  "out-of-order feature",
  "feature disagreement",
  "unknown family",
  "record version",
  "stream version",
  "expression dialect",
  "semantic downgrade",
  "section order",
  "both root slots unchanged",
  "WAL header and tail unchanged",
  "no native writer",
  "retry unchanged",
  "zero resources",
  "metadata",
  "nested counters",
  "nested physical",
] as const;

export const CATALOG_CAPABILITY_WAL_REACH = [
  "optional field",
  "required field",
  "unknown flags",
  "duplicate field",
  "out-of-order field",
  "top-level change",
  "nested view",
  "inspection cannot replay",
  "inspection cannot serialize",
] as const;

/** Inspection operands cannot pass through copying catalog planning/replay boundaries. */
export const CATALOG_INSPECTION_INPUT_REACH = [
  "createTable",
  "alterTable",
  "createIndex",
  "createSequence",
  "alterSequence",
  "createView",
  "replaceView",
  "createType",
  "createFts",
  "upsertFts",
  "createTrigger",
  "fresh replay wrapper",
  "source unchanged",
  "own",
  "inherited",
  "hidden inherited",
  "invalid root refuses",
] as const;

export const CATALOG_INSPECTION_NESTED_REACH = [
  "physical",
  "declared",
  "ftsConfig",
  "triggerBody",
  "viewOutput",
] as const;

export const CATALOG_INSPECTION_ENTRY_REACH = ["native", "stand-in"] as const;

export const CATALOG_WORK_REACH = [
  "shared DAG linear",
  "shared view operand",
  "late inspection refuses",
] as const;

export const FTS_APPEND_REACH = [
  "exact documents",
  "exact postings",
  "wide IDs",
  "raw UTF16",
  "NULL documents",
  "empty renderable",
  "charged workspace",
  "drained",
  "empty",
] as const;

export const FTS_ROW_PUBLICATION_REACH = [
  "row and FTS body",
  "protected before append",
  "NULL",
  "empty",
  "rollback",
  "savepoint",
  "multiple statements",
  "strict",
  "relaxed",
] as const;

export const FTS_PUBLICATION_FAULT_REACH = [
  "prepare fault",
  "append fault",
  "row and FTS commit",
  "no uncommitted FTS",
  "committed FTS survives",
  "orphan retirement",
] as const;

export const FTS_PUBLICATION_CANCEL_REACH = [
  "write 0",
  "write 1",
  "write 2",
  "write 3",
  "retained across close",
  "drained",
] as const;

export const FTS_PUBLICATION_PRESSURE_REACH = [
  "funded insert",
  "pressure invalidation",
  "rows preserved",
  "NULL",
  "text",
] as const;

export const FTS_STAGED_PUBLICATION_REACH = [
  "rows actually staged",
  "one publishedRows and FTS body",
  "atomic missing-delta invalidation",
  "replayed rows",
  "checkpoint",
] as const;

export const FTS_UTF8_BOM_REACH = [
  "leading",
  "interior",
  "none",
  "empty",
  "within bound",
  "exact bound",
  "over bound",
] as const;

/** Native keyed undo must invalidate, independent of executor/DML notifications. */
export const FTS_KEYED_PUBLICATION_REACH = [
  "update",
  "delete",
  "memtable",
  "durable group",
  "one row and FTS commit",
  "older snapshot",
  "checkpoint and reopen",
] as const;

/** Accepted keyed/upsert markers and history are undo-owned, never declaration-dependent. */
export const FTS_MUTATION_HISTORY_REACH = [
  "accepted",
  "no definition",
  "definition",
  "no publication",
  "upsert",
  "savepoint",
  "statement",
  "empty",
  "delete",
  "competing grant",
  "reopen",
] as const;

/** FTS has independent actual-LSN coverage; replay and conservative global roots apply once. */
export const FTS_INDEPENDENT_REPLAY_REACH = [
  "global trails FTS",
  "rows once",
  "each claim once",
  "actual LSNs",
  "repeated reopen",
  "recycle",
] as const;

/** Native FTS work must occur after the real owner ledger's exactly-once admission. */
export const FTS_OWNER_DEDUPE_REACH = [
  "in flight",
  "terminal retry",
  "replayed ledger",
  "rows once",
  "parts once",
  "checkpointed ledger",
  "later transaction untouched",
] as const;

/** Native FTS checkpoint authority cannot be replaced by row/global coverage. */
export const FTS_REPLAY_COVERAGE_REACH = [
  "FTS covered rows replay",
  "new claim",
  "missing WAL refused",
  "complete WAL",
  "global independent",
] as const;

/** A stale write adapter invalidates in the row commit, never refuses a legal row. */
export const FTS_STALE_WRITER_REACH = [
  "before install",
  "before commit",
  "during prepare",
  "late declaration",
  "tokenizer",
  "renderer",
  "scorer",
  "config",
  "workspace",
  "partial parts dominated",
  "same body",
  "rows and history survive",
] as const;

/** Keyed undo and its invalidation are neither both published early nor separated at faults. */
export const FTS_KEYED_FAULT_REACH = [
  "update",
  "delete",
  "prepare",
  "append",
  "none",
  "rows and invalidation agree",
  "orphan retirement",
] as const;

/** Statement FTS completion is bounded by current undo, independent of prior statement count. */
export const FTS_STATEMENT_BOUND_REACH = [
  "ordinary insert",
  "accepted notice",
  "prior history excluded",
  "current statement checked",
  "commit",
] as const;

/** Shared task-clock slices and abort-on-resume in the current statement's FTS undo scan. */
export const FTS_STATEMENT_CLOCK_REACH = [
  "bounded slice",
  "abort on resume",
  "finish",
  "drained",
] as const;

/** End-of-statement FTS admission observes its own signal before another holder releases credit. */
export const FTS_ADMISSION_CANCEL_REACH = [
  "options",
  "end",
  "actual admission wait",
  "abort while credit held",
  "abort on admission resume",
  "same transaction usable",
  "no FTS preparation",
  "writer usable",
  "update",
  "delete",
  "upsert",
] as const;

/** Retiring a failed FTS forecast preserves allocated buffers plus the next simultaneous step. */
export const FTS_CREDIT_RETIREMENT_REACH = [
  "live buffer funded",
  "all ordinary credit retained",
  "overlap funded",
  "owned bytes intact",
  "idle credit returned",
  "all accounts returned",
] as const;

/** FTS staging overlays must preserve native publication retirement bookkeeping (R25). */
export const FTS_ROOT_TRACKING_REACH = ["FTS publication retires native staging"] as const;

export const FTS_INLINE_MUTATION_REACH = [
  "wide commit LSN",
  "cancelled record rebound",
  "stable checkpoint prefix",
  "bounded journal pages",
  "journal growth boundary",
  "denied before entries",
  "cancel after k",
  "constant-time R9",
  "same table idempotent",
  "borrowed checkpoint fenced",
  "checkpoint reclaims",
  "funded resize",
] as const;

export const FTS_INLINE_PUBLICATION_REACH = [
  "32MiB cap",
  "128MiB cap",
  "update",
  "delete",
  "upsert",
  "definition",
  "no definition",
  "competing grant",
  "no second root publish",
  "older snapshot",
  "actual-LSN checkpoint",
  "exact reopen",
] as const;

export const FTS_INLINE_MIXED_REACH = [
  "independent coverage",
  "same-body append and keyed",
  "uncheckpointed reopen",
  "separate actual LSNs",
  "NULL append",
  "exact rows",
  "checkpoint replay",
  "all charges returned",
] as const;

export const FTS_INLINE_CODEC_REACH = [
  "registered tag",
  "wide identifiers",
  "zero table refused",
  "zero schema refused",
  "truncation refused",
  "trailing bytes refused",
] as const;

/** Stage4 review: retirement at every awaited preparation edge must dominate its row claim. */
export const FTS_AWAIT_STALE_REACH = [
  "lock resume",
  "admitted workspace",
  "every IO boundary",
  "final journal",
  "publication",
  "builder IO",
  "same row claim",
  "all touched bindings",
  "exact reopen",
] as const;

export const FTS_COMMIT_WORK_REACH = [
  "append ranges",
  "1000 ranges",
  "2000 ranges",
  "linear commit reads",
  "keyed skips payload",
  "exact rows",
  "bounded eligibility slice",
  "eligibility abort on resume",
] as const;

export const FTS_PUBLICATION_SEAL_REACH = [
  "late retirement invalidated",
  "valid claim preserved",
  "same row body",
  "replayed disposition",
  "checkpointed disposition",
] as const;

export const LOG_SEAL_REACH = [
  "allocation retirement",
  "synchronous fitting",
  "queue drain",
  "deferred recycle",
  "multipart",
  "one byte final body",
  "atomic LSN seal",
  "checked bytes",
  "charges drained",
] as const;

export const FTS_CLAIM_CODEC_REACH = [
  "valid flag",
  "invalidated flag",
  "wide identity",
  "fixed final byte",
  "reserved flags refused",
  "missing flag refused",
  "development predecessor refused",
] as const;

export const OWNER_CLOCK_REACH = ["clock tick exact", "over target remains over"] as const;

export const NATIVE_CONSTRAINT_ORDER_REACH = [
  "check-null",
  "null-check",
  "error-null",
  "columns",
  "success",
  "width1",
  "width2",
  "width8",
  "complete validator",
  "native fallback",
  "vectorized success",
  "selection",
  "dense",
  "valid prefix",
  "first row",
  "reverse CHECK declarations",
  "forward CHECK declarations",
  "ASCII names",
  "UTF8 names",
] as const;

/** Actual SQL DML shares atomic row/FTS undo, WAL and reopen boundaries. */
export const MODIFY_FTS_REACH = [
  "insert",
  "updateFrom",
  "deleteUsing",
  "upsertInsert",
  "upsertUpdate",
  "nothing",
  "whereFalse",
  "commit",
  "rollback",
  "savepoint",
  "failure",
  "definition",
  "no definition",
  "coalesced insert",
  "late statement error",
  "survivor after rollback",
  "same WAL body",
  "undo accounting",
  "reopen",
  "PG and shipped",
] as const;

export const MODIFY_FTS_COALESCING_REACH = [
  "commit flag",
  "rollback flag",
  "first flag",
  "later flag",
  "ordinary append after flag",
  "one funded entry",
] as const;

export const GENERAL_MODIFY_REACH = [
  "multi-row VALUES",
  "empty producer",
  "expression update",
  "INSERT SELECT snapshot",
  "primary key update snapshot",
  "failure rollback",
  "predicate delete",
] as const;

export const MODIFY_SOURCES_REACH = [
  "UPDATE FROM duplicate matches",
  "first distinct source wins",
  "joined RETURNING",
  "DELETE USING duplicate matches",
  "delete cascade",
  "missing parent rollback",
] as const;

/** Native modification phases must retire producer credit before a writer can wait. */
export const WRITE_MERGE_RETIREMENT_REACH = [
  "queued reader",
  "dynamic working credit retired",
  "metadata while reader retained",
  "all credit returned",
] as const;

export const MODIFY_PROGRESS_REACH = [
  "finished merge credit retired",
  "healthy merge completion",
  "64 MiB",
  "single UPDATE no target set",
  "single DELETE no target set",
  "read grants retired",
  "20000 updated",
  "20000 deleted",
  "bounded spill workspace",
] as const;

/** Returned row versions must survive phase retirement and commit-gated delivery. */
export const MODIFY_RETURNING_REACH = [
  "snapshotted parameters",
  "inserted rows",
  "new updated rows",
  "old deleted rows",
  "empty",
  "insert select",
] as const;

/** RETURNING must not make statement execution depend on the reader or leak after cancellation. */
export const MODIFY_RETURNING_LIFECYCLE_REACH = [
  "gate insert",
  "gate update",
  "gate delete",
  "atCommit hidden",
  "atExecuted",
  "cancel gate",
  "delivery cleanup",
] as const;

/** Large returned values must spill without a second live loan or exceeding the governor. */
export const MODIFY_RETURNING_PRESSURE_REACH = [
  "funded before admission",
  "64 MiB",
  "wide input page",
  "new rows",
  "old rows",
  "repeated output value",
  "all credit returned",
] as const;

/** Publication keeps original values/indices; extra retained references require a prepaid budget. */
export const MIXED_SOURCE_RECIPE_REACH = [
  "multiple source segments",
  "metadata paid",
  "metadata denied no retain",
  "locators preserved",
  "publication retained",
  "cancelled",
  "exact original rows",
  "all credit returned",
] as const;

export const MODIFY_UPSERT_REACH = [
  "without RETURNING",
  "proposed NOT NULL before conflict",
  "composite primary target",
  "repeated unique target",
  "FK valid update",
  "FK rollback",
  "primary target",
  "unique target",
  "target permutation",
  "target order",
  "EXCLUDED",
  "WHERE true",
  "WHERE false",
  "targetless nothing",
  "targeted nothing",
  "multi-row proposals",
  "insert select",
  "null unique tuples",
  "repeated existing",
  "repeated inserted",
  "non-arbiter unique rollback",
  "arbiter assignment refused",
] as const;

export const MODIFY_INDEX_REACH = [
  "keyLookup",
  "indexRange",
  "duplicate keys",
  "text key",
  "composite key",
  "inclusive bound",
  "exclusive bound",
  "nonunique range",
  "parameter bounds",
  "rekey snapshot",
  "key delete",
  "range delete",
  "absent target",
  "null key",
  "stored segments",
  "memory segments",
] as const;

export const MODIFY_ROW_RULES_REACH = [
  "omitted follow policy",
  "insert generated",
  "update generated",
  "generated DEFAULT",
  "insert select generated",
  "excluded generated",
  "duplicate valid match",
  "generated assignment refused",
  "FK generated rekey",
  "FK generated set null",
  "CHECK rollback",
] as const;

export const ROOT_RULE_ORDER_REACH = [
  "NOT NULL before generated error",
  "CHECK before generated error",
  "constraint identity",
  "statement rollback",
] as const;

export const MODIFY_RULE_PRESSURE_REACH = [
  "funded admission",
  "wide generated insert",
  "wide generated update",
  "64 MiB",
  "spill cleanup",
] as const;

/** Repeat-tracker cache eviction and identity domains; generated b5c6 histories. */
export const MODIFY_TARGET_SET_REACH = [
  "evicted leaf",
  "repeated target",
  "separate tables",
  "wide row identity",
  "fixed charged envelope",
  "complete cleanup",
] as const;

/** Large shared-scope inputs and native repeat policy; b5c7 includes20k target witness. */
export const MODIFY_LARGE_TARGETS_REACH = [
  "120s per history",
  "coalesced row undo",
  "joined first match",
  "large upsert",
  "joined delete",
  "no native repeat Set",
  "64 MiB",
  "complete cleanup",
] as const;

/** Create/write/read borrows survive cancellation until the backend returns. */
export const MODIFY_TARGET_CANCEL_REACH = [
  "create",
  "append",
  "read",
  "borrow fenced",
  "idempotent cleanup",
] as const;

/** Single-row write coalescing must retain catalog boundaries, rollback and recovery. */
export const WRITE_ROW_UNDO_REACH = [
  "visible row shortcut",
  "new row classified",
  "coalesced append",
  "catalog boundary",
  "frozen memtable",
  "commit",
  "statement rollback",
  "transaction rollback",
  "reopen",
] as const;

/** Moving repeat suppression ahead of generated evaluation loses required errors. */
export const MODIFY_DUPLICATE_RULES_REACH = [
  "ignored generated error",
  "statement rollback",
  "duplicate source values",
] as const;

/** Prefix pin expiry can occur while the bounded target tracker awaits its own spill I/O. */
export const TRIGGER_PREFIX_TARGET_REACH = [
  "one-row batch",
  "suspended on final row",
  "suspended before final row",
  "expired during wait",
  "UPDATE",
  "DELETE",
  "published",
  "resident",
  "no primary effect",
] as const;

/** Immutable trigger targets and FK edges must enter native rule/admission reach. */
export const STATEMENT_RULE_REACH = [
  "rule minimum includes body targets",
  "raw executor guarded",
  "informational update only",
  "generated key propagation",
  "FK preserves budget",
  "budget diamond",
  "budget zero fires",
  "negative budget writes without firing",
  "unrelated update",
  "insert stops FK edge",
  "delete event differs",
  "RETURNING guarded",
  "plain guarded",
  "guard before mutation",
] as const;

/** Closed text sets cannot disappear when complete-row CHECK/generated validation is installed. */
export const MODIFY_CLOSED_SET_REACH = [
  "INSERT",
  "UPDATE",
  "upsert insert",
  "upsert proposed",
  "upsert assignment",
  "CHECK",
  "no CHECK",
  "generated",
  "no generated",
  "first invalid row",
  "column identity",
  "rollback",
  "NULL and UTF8 allowed",
] as const;

/** Closed-set checks share normal and exceptional generated-row validation. */
export const CLOSED_SET_REPLAY_REACH = [
  "flat",
  "dictionary",
  "constant",
  "selection",
  "all rows",
  "before later generated error",
  "single active row",
  "physical row and column",
] as const;

/** Arbitration must consume completed asynchronous reads without relying on cache admission. */
export const MODIFY_UPSERT_RESOLUTION_REACH = [
  "fresh",
  "primary",
  "unique",
  "targetless",
  "published",
  "resident",
  "completed read used",
  "bounded lookup count",
] as const;

/** Raw native SQL execution must not opt into the legacy uncharged target Set. */
export const MODIFY_TRACKING_CAPABILITY_REACH = [
  "update",
  "delete",
  "upsert",
  "RETURNING",
  "plain",
  "cached writer",
  "fresh writer",
  "raw caller refused",
  "owned SQL tracked",
] as const;

/** Oversized proposals fail with the index-path identity before arbitration encoding/copies. */
export const MODIFY_UPSERT_KEY_LIMIT_REACH = [
  "range keys",
  "nothing",
  "targetless",
  "update",
  "prefix rollback",
  "empty proposal",
  "ASCII",
  "UTF8",
  "before key encoding",
  "plain identity",
  "shipped cap PG difference",
  "bounded arity",
] as const;

/** Point arbitration fetches bound physical decode before and after publication. */
export const MODIFY_UPSERT_POINT_REACH = [
  "first physical chunk",
  "later physical chunk",
  "resident",
  "publication after capture",
  "first",
  "middle",
  "last",
  "NULL",
  "text",
  "one physical row",
] as const;

/** Action metadata cannot select a different old executor policy while its RI driver is absent. */
export const MODIFY_FK_GUARDS_REACH = [
  "update",
  "upsert",
  "delete",
  "CASCADE",
  "SET NULL",
  "SET DEFAULT",
  "RESTRICT",
  "NO ACTION",
  "informational",
  "deleteDefault",
  "follow",
  "non-key update allowed",
  "DO NOTHING allowed",
  "raw guarded",
  "owned guarded",
  "RETURNING",
  "plain",
] as const;

/** The independent IR reference must implement deferred generated rows after every assignment. */
export const MODIFY_REFERENCE_GENERATED_REACH = [
  "UPDATE",
  "SQL upsert update",
  "batch upsert update",
  "upsert insert",
  "stored and RETURNING",
  "multiple generated columns",
  "NULL",
  "non-NULL",
] as const;

export const MODIFY_INDEX_GATE_REACH = [
  "update",
  "delete",
  "insert",
  "upsert",
  "keyLookup",
  "indexRange",
  "zero SELECT limit",
  "positive SELECT limit",
  "query unset refused",
  "query false refused",
  "query opt-in prepared",
  "DML unset admitted",
  "DML false admitted",
  "PG and shipped rows",
  "zero spill",
  "nested join refused",
  "nested set refused",
] as const;

/** Root scalar registries have a fixed charged directory independent of row/event count. */
export const REGISTRY_DIRECTORY_REACH = [
  "evicted records",
  "overwritten records",
  "separate namespaces",
  "wide IDs",
  "max safe ID",
  "missing record",
  "fixed metadata",
  "one spill file",
  "descriptor count above cap",
  "zero ownership",
] as const;

export const REGISTRY_DIRECTORY_IO_REACH = [
  "create",
  "append",
  "read",
  "external cancellation",
  "close cancellation",
  "charge survives I/O",
  "zero ownership",
] as const;

export const REGISTRY_DIRECTORY_FAULT_REACH = [
  "create",
  "append",
  "read",
  "checksum",
  "structure",
  "close failure",
  "failure poisons further work",
  "zero ownership",
] as const;

export const REGISTRY_DIRECTORY_ALLOCATION_REACH = [
  "first allocation",
  "partial construction",
  "denied before allocation",
  "zero ownership",
] as const;

export const REGISTRY_DIRECTORY_OWNER_REACH = [
  "same row different tables",
  "wide table ID",
  "max safe table ID",
  "over-cap descriptors spill",
  "zero ownership",
] as const;

export const REGISTRY_FRAGMENT_REACH = [
  "empty",
  "exact fragment",
  "ragged fragment",
  "payload larger than scratch",
  "bounded copy and I/O",
  "task clock per fragment",
  "one file across payloads",
  "replay without ref list",
  "zero ownership",
] as const;

export const REGISTRY_FRAGMENT_IO_REACH = [
  "create",
  "append",
  "read",
  "fill",
  "clock",
  "external cancellation",
  "close cancellation",
  "zero ownership",
] as const;

export const REGISTRY_FRAGMENT_CORRUPTION_REACH = [
  "checksum",
  "header",
  "cycle",
  "truncated",
  "oversized",
  "fractional",
  "bounded reads",
  "zero ownership",
] as const;

export const REGISTRY_SCALAR_REACH = [
  "bool",
  "i32",
  "f32",
  "f64",
  "int64",
  "fixed16",
  "interval",
  "varlen",
  "decimal",
  "decimal safe",
  "decimal int64",
  "decimal int128",
  "decimal wide",
  "flat",
  "constant",
  "dictionary",
  "NULL",
  "non-NULL",
  "payload above scratch",
  "wide coefficient above fragment",
  "zero ownership",
] as const;

export const NATIVE_PRIMARY_IMAGE_WIDTH_REACH = [
  "insert",
  "update",
  "NULL",
  "non-NULL",
  "empty",
  "small",
  "above fragment",
  "constraint rollback",
  "accepted row",
  "writer prepared before lease",
  "zero ownership",
] as const;

export const REGISTRY_IMAGE_SIZING_REACH = [
  "insert",
  "update",
  "delete",
  "NULL",
  "non-NULL",
  "empty",
  "small",
  "above fragment",
  "no payload read",
  "no admission",
  "constant ownership",
  "foreign root refused",
  "continued full read",
  "zero ownership",
] as const;

export const REGISTRY_IMAGE_SIZING_LIFETIME_REACH = [
  "caller cancellation",
  "close during IO",
  "charge until IO drain",
  "zero ownership",
] as const;

export const REGISTRY_IMAGES_REACH = [
  "INSERT images",
  "UPDATE images",
  "DELETE images",
  "full physical row",
  "projected stable IDs",
  "unread is not NULL",
  "large unrelated payload skipped",
  "duplicate bindings alias",
  "caller source retired",
  "wide row replay",
  "foreign root refused",
  "two files independent of rows",
  "fixed frozen minimum",
  "zero ownership",
] as const;

export const REGISTRY_IMAGES_LIFETIME_REACH = [
  "close waits lease",
  "lease charge survives close",
  "caller cancels prepare",
  "close cancels prepare",
  "zero ownership",
] as const;

export const REGISTRY_IMAGES_IO_REACH = [
  "close cancels read",
  "caller cancels read",
  "charge survives payload IO",
  "no read after cancel",
  "zero ownership",
] as const;

export const REGISTRY_TARGET_REACH = [
  "parent swap captures",
  "updated once",
  "multiple physical versions",
  "deleted target skipped",
  "same row different tables",
  "wide IDs",
  "empty range replay",
  "foreign root refused",
  "one file across ranges",
  "over-cap targets",
  "fixed metadata",
  "zero ownership",
] as const;

export const REGISTRY_TARGET_IO_REACH = [
  "caller cancellation",
  "close cancellation",
  "input return fenced",
  "pending input remains charged",
  "zero ownership",
] as const;

export const REGISTRY_IMAGES_WIDE_REACH = [
  "capture scratch prepaid",
  "signed wide coefficient",
  "wide source retired",
  "projected wide payload skipped",
  "exact tier and scale",
  "zero ownership",
] as const;

export const REGISTRY_COMMAND_REACH = [
  "root budget 1",
  "body budget 0",
  "terminal budget -1",
  "three live frames",
  "retired command refused",
  "incremental action uniqueness",
  "separate reference cursor",
  "immutable RI replay",
  "retired RI refused",
  "wide row IDs",
  "spilled accepted rows",
  "fixed metadata",
  "zero ownership",
] as const;

export const REGISTRY_COMMAND_IO_REACH = [
  "caller cancellation",
  "close cancellation",
  "verification IO fenced",
  "metadata remains paid",
  "zero ownership",
] as const;

export const REGISTRY_IMAGES_WORKSPACE_REACH = [
  "many wide columns",
  "shared decode scratch",
  "exact retained buffers",
  "negative coefficients",
  "positive coefficients",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_CHECK_REACH = [
  "resident sources",
  "published sources",
  "unique 23505",
  "primary 23505",
  "missing parent 23503",
  "valid reference",
  "NULL reference skipped",
  "NULL key skipped",
  "valid command",
  "obsolete version skipped",
  "root prepaid cursor",
  "image released before progress",
  "projected unrelated payload",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_ACTION_CHECK_REACH = [
  "root checkpoint already passed",
  "earlier actions passed",
  "new action uniqueness before later error",
  "resident sources",
  "published sources",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_CHECK_LIFETIME_REACH = [
  "image gone before wait",
  "close cancellation",
  "caller cancellation",
  "pending progress remains paid",
  "no cursor after cancellation",
  "zero ownership",
  "capture close",
  "capture cancellation",
] as const;

export const PREPARED_ROW_CREDIT_REACH = [
  "one prepared slot",
  "owner close refuses live step",
  "reclaim keeps floor",
  "merge keeps floor",
  "metadata transfer keeps floor",
  "cancel before consumption",
  "native allocations survive reservation release",
  "idempotent retirement",
  "idle floor returned",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_CHECK_CONSTRUCTION_REACH = [
  "digit scratch fault",
  "key buffer fault",
  "offset buffer fault",
  "output buffer fault",
  "private account restored",
  "zero ownership",
] as const;

export const NATIVE_PRIMARY_REACH = [
  "insert",
  "update",
  "delete",
  "key swap",
  "PG immediate swap refused",
  "product swap deferred PG control",
  "resident sources",
  "published sources",
  "no admission with image lease",
  "no legacy deferred queues",
  "whole statement rollback",
  "NOT NULL",
  "CHECK",
  "command uniqueness",
  "later callback failure",
  "root end once",
  "zero ownership",
] as const;

export const NATIVE_PRIMARY_OWNERSHIP_REACH = [
  "insert",
  "update",
  "delete",
  "conflicts",
  "DDL",
  "prepared end",
  "foreign command",
  "retired command",
  "forged preparation",
  "no unchecked mutation",
  "zero ownership",
] as const;

export const NATIVE_PRIMARY_LIFETIME_REACH = [
  "close waits native reservation",
  "retirement during reservation",
  "no mutation after await",
  "prepared credit retired",
  "zero ownership",
] as const;

export const NATIVE_PRIMARY_ADMISSION_REACH = [
  "native growth queued",
  "competing credit retained",
  "operation cancellation",
  "cancel completes at admission boundary",
  "same transaction continues",
  "zero ownership",
] as const;

export const NATIVE_PRIMARY_OWNER_CLOSE_REACH = [
  "resident source",
  "published source",
  "pending callback fenced",
  "rollback waits callback",
  "prefix pin retired by rollback",
  "native primary charge retained",
  "zero ownership",
] as const;

export const COMMAND_MAINTENANCE_WAIT_REACH = [
  "one native operation factory",
  "one completion observer",
  "cancelled listeners removed",
  "later command attaches",
  "one active waiter",
  "owner failure preserved",
  "owner completion preserved",
  "settled listener removed",
] as const;

export const NATIVE_PRIMARY_END_FENCE_REACH = [
  "resident source",
  "published source",
  "end callback fenced",
  "cancelled end cannot succeed",
  "rollback after fence",
  "charge retained until callback",
  "zero ownership",
] as const;

export const NATIVE_PRIMARY_CLOSE_FAULT_REACH = [
  "async callback fault",
  "immediate callback fault",
  "native rollback despite fault",
  "metadata refunded after fence",
  "original callback fault retained",
  "zero ownership",
] as const;

export const MODIFY_SOURCE_ERROR_ORDER_REACH = [
  "insert",
  "nothing",
  "upsert",
  "update",
  "check",
  "null",
  "none",
  "RETURNING",
  "no RETURNING",
  "multiple input pages",
  "short prefix",
  "atomic rollback",
  "zero spill",
] as const;

export const MODIFY_RETURNING_ERROR_ORDER_REACH = [
  "CHECK binding",
  "NOT NULL without CHECK",
  "insert",
  "upsert",
  "update",
  "same batch",
  "across batches",
  "PG row order",
  "shipped batch-order difference",
  "atomic rollback",
] as const;

export const MODIFY_SOURCE_FAULT_ORDER_REACH = [
  "57014",
  "0A000",
  "XX000",
  "53200",
  "no writer continuation",
  "zero spill",
] as const;

export const MODIFY_WINDOW_ERROR_ORDER_REACH = [
  "earlier CHECK",
  "window argument error",
  "atomic rollback",
  "zero spill",
] as const;

export const MODIFY_RETURNING_SCOPE_REACH = [
  "bound native error order",
  "raw missing capability refusal",
  "zero spill",
] as const;

export const COLUMN_RULE_BINDING_REACH = [
  "CHECK refusal",
  "generated refusal",
  "minimum covers binding",
  "complete NOT NULL validator",
  "zero charge",
] as const;

export const REGISTRY_COMMAND_TARGET_REACH = [
  "three live commands",
  "table isolation",
  "command isolation",
  "reused frame",
  "ancestor survives child",
  "cold bitmap revisit",
  "neighbor bits and block boundary",
  "wide native ID",
  "zero native ID",
  "one shared file",
  "constant metadata",
  "zero ownership",
] as const;

export const REGISTRY_COMMAND_TARGET_PIN_REACH = [
  "boundary1",
  "boundary2",
  "boundary3",
  "boundary4",
  "one final target",
  "no accepted mutation",
  "zero ownership",
] as const;

export const REGISTRY_COMMAND_TARGET_FENCE_REACH = [
  "cancel",
  "retire",
  "close",
  "cancel on final await",
  "retire on final await",
  "close on final await",
  "close retains metadata through IO",
  "no accepted mutation",
  "zero ownership",
] as const;

export const NATIVE_ACTION_UNIQUE_REACH = [
  "entry",
  "id",
  "resident",
  "published",
  "one action",
  "two actions",
  "three actions",
  "shared command token",
  "native23505 before later22012",
  "atomic rollback",
  "zero ownership",
] as const;

export const NATIVE_BOUNDARY_REACH = [
  "id",
  "entry",
  "forged",
  "retired",
  "foreign",
  "invalid before effects",
  "retired action refused",
  "command retired and reused",
  "budget-1",
  "budget0",
  "budget1",
  "zero ownership",
] as const;

export const NATIVE_BOUNDARY_FENCE_REACH = [
  "follow",
  "image",
  "check",
  "action",
  "caller cancels root",
  "undelivered lease released",
  "reentrant close shares fence",
  "IO fence before credit return",
  "zero ownership",
] as const;

export const NATIVE_BOUNDARY_LISTENER_REACH = [
  "external signal",
  "root signal",
  "no settled listener history",
  "constant metadata",
  "zero ownership",
] as const;

export const NATIVE_BOUNDARY_REFERENCE_REACH = [
  "published",
  "resident",
  "native missing parent23503",
  "native parent exists",
  "command end checkpoint",
  "zero ownership",
] as const;

export const NATIVE_BOUNDARY_NESTING_REACH = [
  "one live frame",
  "two live frames",
  "three live frames",
  "RI preserves budget-1",
  "outer action drain refused before child effects",
  "zero ownership",
] as const;

export const NATIVE_BOUNDARY_IMAGE_CANCEL_REACH = [
  "projected lease",
  "full lease",
  "released before boundary close",
  "zero ownership",
] as const;

export const STAGED_FRAGMENT_MAXIMA_REACH = [
  "stage",
  "flush",
  "one fragment",
  "multiple fragments",
  "over 64 fragments",
  "rollback",
  "fold committed rows",
  "hidden staging",
  "append after reopen",
  "zero charge",
] as const;

/** Internal RI writers and actual selected index/empty SQL DML retain FTS undo/WAL atomicity. */
export const MODIFY_FTS_PATHS_REACH = [
  "ri update",
  "ri delete",
  "keyLookup update",
  "keyLookup delete",
  "indexRange update",
  "indexRange delete",
  "zero update",
  "zero delete",
  "ri commit",
  "ri rollback",
  "ri savepoint",
  "keyLookup commit",
  "keyLookup rollback",
  "keyLookup savepoint",
  "indexRange commit",
  "indexRange rollback",
  "indexRange savepoint",
  "commit",
  "rollback",
  "savepoint",
  "resident",
  "stored",
  "reopen",
] as const;

/** The staged native primary capability must preserve ordinary FTS undo/WAL semantics. */
export const NATIVE_PRIMARY_FTS_REACH = [
  "insert",
  "update",
  "delete",
  "commit",
  "rollback",
  "savepoint",
  "published",
  "resident",
  "undo and WAL",
  "reopen",
  "zero ownership",
] as const;

/** RI inputs seal full OLD-key child identities with fixed metadata and one native cursor. */
export const NATIVE_CHILD_CAPTURE_REACH = [
  "current prefix",
  "published",
  "resident",
  "empty",
  "matching OLD",
  "multiple cursor fills",
  "spilled target directory",
  "NEW differs",
  "fixed metadata",
  "released cursor",
  "zero ownership",
] as const;

/** Native row ranges coalesce inside one command, retaining exactly one funded FTS flag. */
export const NATIVE_COMMAND_UNDO_REACH = [
  "commit",
  "rollback",
  "savepoint",
  "root/body/nested",
  "reused frame slots",
  "returned parent command",
  "ordinary insert flags",
  "coalesced same-command rows",
  "zero ownership",
  "sequential bodies reuse frame",
] as const;

/** Follow preparation cannot advance or borrow native uniqueness/reference check positions. */
export const REGISTRY_THREE_CHECKPOINT_REACH = [
  "unique",
  "followTargets",
  "references",
  "follow before references",
  "references before follow",
  "wide IDs",
  "small IDs",
  "incremental prefixes",
  "no completed-prefix replay",
  "fixed metadata",
  "zero ownership",
] as const;

export const SNAPSHOT_LOAD_REACH = [
  "interrupted prefix",
  "old target intact",
  "retry complete",
  "native reopen",
  "continued writes",
  "quota safe",
] as const;

export const SNAPSHOT_LOAD_FAULT_REACH = [
  "quota",
  "before publication",
  "after publication",
  "cancel",
] as const;

export const SNAPSHOT_NATIVE_LAYOUT_REACH = [
  "sparse nulls",
  "all valid",
  "all scalar and byte layouts",
] as const;

export const SNAPSHOT_LOAD_OPEN_REACH = ["wait until discard", "wait until publication"] as const;

export const SNAPSHOT_CATALOG_REFUSAL_REACH = [
  "duplicate table name",
  "counter floor",
  "nextColumnId floor",
  "nextIndexId floor",
  "nextConstraintId floor",
  "nextSequenceId floor",
  "nextViewId floor",
  "nextTypeId floor",
  "nextFtsId floor",
  "nextTriggerId floor",
  "duplicate column id",
  "unsafe id",
  "unresolved reference",
  "unsupported field",
  "unsupported trigger",
  "unsupported fts",
] as const;

export const SNAPSHOT_RETIREMENT_REACH = [
  "small old inventory",
  "large old inventory",
  "constant replacement workspace",
] as const;

export const SNAPSHOT_RETIRE_CURRENT_REACH = [
  "sharing refused",
  "mixed claims refused",
  "cancel intact",
  "pin survives replacement",
  "exact retired inventory",
] as const;

export const SNAPSHOT_ADMISSION_REACH = [
  "unwind before wait",
  "cancel queued",
  "release and replay",
  "only above cap refused",
  "one shot via spool",
] as const;

export const SNAPSHOT_STRUCTURED_REACH = [
  "array version",
  "array element tag",
  "array dimensions",
  "array flags",
  "array dimension length",
  "array lower bound",
  "array element length",
  "array truncated element",
  "array null tail",
  "array boolean",
  "array decimal special",
  "array decimal scale",
  "array decimal overflow",
  "array unsupported element",
  "json grammar",
  "json utf8",
  "enum label",
  "text utf8",
  "text length",
  "text bom length",
  "jsonb nul",
  "jsonb high surrogate",
  "jsonb low surrogate",
  "jsonb surrogate mismatch",
  "jsonb exponent",
  "jsonb scale",
  "jsonb precision",
] as const;

export const SNAPSHOT_LOAD_ROOT_REACH = [
  "oversized native root",
  "same governor handoff",
  "zero after reopen",
] as const;

export const SNAPSHOT_JSON_SEMANTICS_REACH = [
  "plain JSON escapes",
  "surrogate across window",
  "surrogate pair across window",
  "JSONB exponent boundary",
  "JSONB scale boundary",
  "JSONB precision boundary",
  "long zero exponent",
] as const;

export const SNAPSHOT_SOURCE_STABILITY_REACH = [
  "valid archive",
  "single bit",
  "truncated",
  "spool",
  "stable retry",
] as const;

export const SNAPSHOT_SEQUENCE_CAPTURE_REACH = [
  "ascending",
  "descending",
  "cycling",
  "setval",
  "restored",
  "uninitialized",
  "read only",
  "checkpoint history preserved",
  "released",
  "denied",
] as const;

export const SNAPSHOT_CATALOG_SCOPE_REACH = [
  "bigint horizons",
  "typed missing",
  "logged typed lanes",
  "strict horizon count",
  "strict horizon id",
  "canonical decimal",
  "sequence reserved",
  "catalog reserved",
  "unknown version",
  "trailing bytes",
  "before first await",
  "continued sequence",
  "cancel release",
] as const;

export const SNAPSHOT_SEQUENCE_LOAD_REACH = [
  "owned identity",
  "ascending",
  "descending",
  "pin time",
  "continue writing",
  "reopen",
  "horizon refusal",
  "sequence counter floor",
  "sequence owner reference",
  "identity reference",
  "sequence name uniqueness",
] as const;

export const SNAPSHOT_INDEX_REACH = [
  "inline keys",
  "overflow keys",
  "multi block merge",
  "native probes",
  "continue reopen",
  "null FK",
  "FK modes",
  "unique refusal",
  "FK refusal",
] as const;

export const SNAPSHOT_SORTED_MERGE_REACH = [
  "one piece",
  "many pieces",
  "ordered complete output",
  "blocked cancel",
  "no resident blooms",
  "fixed working bound",
] as const;

export const SNAPSHOT_INDEX_FAILURE_REACH = [
  "partial native output",
  "quota",
  "cancel",
  "old root and rows",
] as const;

export const SNAPSHOT_INDEX_SCOPE_REACH = [
  "invalid unique",
  "orphan primary",
  "scalar composite policy",
] as const;
export const SNAPSHOT_LONG_FK_REACH = [
  "long parent scalar",
  "invalid support retained",
  "FK actions retained",
  "continued FK enforcement",
] as const;

export const SNAPSHOT_INDEX_LEASE_REACH = [
  "real grant pressure",
  "drop and retry",
  "four lease bound",
  "zero after close",
] as const;

export const SNAPSHOT_SEQUENCE_DURABILITY_REACH = [
  "emitted ahead of durable",
  "durable ahead of emitted",
  "restored horizon",
  "ascending",
  "descending",
  "single emitted value",
  "multiple emitted values",
  "pin before await",
  "ordinary load nextval",
  "ordinary reopen nextval",
  "read only allocation",
  "charges released",
] as const;

export const SNAPSHOT_SEQUENCE_DROP_REACH = [
  "pending DROP in strict pin",
  "capture before await",
  "ordinary load advances past emitted",
  "ordinary reopen advances",
  "reclaimed after durability",
  "ascending",
  "descending",
  "one emission",
  "many emissions",
  "zero charges",
] as const;

export const SNAPSHOT_FTS_RETIREMENT_REACH = [
  "old FTS inventory exactly once",
  "canonical empty FTS state",
  "previous root FTS remains readable",
  "old FTS physical retirement exactly once",
  "ordinary reopen and continued write",
  "one checkpoint",
  "several checkpoints",
  "zero charges",
] as const;

export const PAGE_CORRUPTION_REACH = [
  "plain",
  "constant",
  "bitmap",
  "for",
  "dictionary",
  "scaled",
  "gzip",
  "encoding",
  "value tag",
  "reserved",
  "unused aux",
  "directory flags",
  "all valid",
  "sparse NULLs",
  "checksum-valid refusal",
  "root and WAL unchanged",
  "reopenable",
  "charges released",
] as const;

export const CANONICAL_WRITER_REACH = [
  "stored all-NULL constant",
  "stored validity bitmap",
  "stored safe integer lane",
  "stored zero-width delta",
  "stored mixed scales and specials",
  "stored uniform scale",
  "stored out-of-line pointers",
  "stored bool bitmap",
  "stored plain i32",
  "stored plain f32",
  "stored plain f64",
  "stored plain decimal128",
  "stored plain fixed16",
  "stored safe interval",
  "stored wide interval",
  "stored varlen offsets16",
  "stored varlen offsets32",
  "stored FOR i32",
  "stored wide FOR i64",
  "stored safe FOR i64",
  "stored wide FOR decimal64",
  "stored constant i32",
  "stored constant varlen",
  "stored constant dictionary code",
  "stored varlen dictionary codes",
  "stored fixed16 dictionary codes",
  "stored scaled float without patches",
  "stored scaled float with raw patch",
  "stored gzip page",
  "stored dictionary fixed16 payload",
  "stored dictionary offsets32 payload",
] as const;

export const CANONICAL_REFUSAL_REACH = [
  "allocation-free HLL validation",
  "NULL scalar payload",
  "unused aux",
  "all-NULL aux",
  "nondecimal specials",
  "duplicate sparse",
  "unordered sparse",
  "dense ranks",
  "dense at sparse density",
  "sparse at dense density",
  "writer HLL bytes",
] as const;

export const NATIVE_PROFILE_REACH = [
  "required catalog field",
  "read-optional field",
  "catalog wire type",
  "required feature",
  "assigned vocabulary",
  "unassigned nested vocabulary",
  "unassigned reader",
  "manifest mismatch",
  "assigned selector in preserved population",
] as const;

export const DURABLE_OCTET_REACH = [
  "every unknown octet",
  "page encodings",
  "page value tags",
  "metadata versions",
  "outer selectors",
  "other nested selectors",
] as const;

export const DURABLE_EMBEDDED_OCTET_REACH = [
  "row-id kind",
  "physical type",
  "statistic",
  "physical flags and widths",
  "nested physical tag",
  "directory page encoding",
  "array decimal special",
  "run encoding",
] as const;

export const CORRUPT_CHECKPOINT_REACH = [
  "checkpoint consumes staged root",
  "typed corruption",
  "root and WAL unchanged",
  "reopenable",
  "charges released",
] as const;

export const CORRUPT_MERGE_REACH = [
  "inline run",
  "overflow run",
  "merge consumes damaged block",
  "partial output discarded",
  "flag bit1",
  "flag bit2",
  "flag bit3",
  "flag bit4",
  "flag bit5",
  "flag bit6",
  "flag bit7",
  "typed corruption",
  "root and WAL unchanged",
  "reopenable",
  "charges released",
] as const;

export const STRUCTURED_PARTS_REACH = [
  "all array scalar kinds",
  "array NULL bitmap",
  "JSON",
  "JSONB",
  "internal corruption",
  "funding before producer",
  "cancellation releases producer",
  "borrowed parts copied before advance",
  "charges released",
  "large bounded bitmap",
] as const;

export const STRUCTURED_EXPORT_REACH = [
  "version",
  "element tag",
  "dimensions",
  "flags",
  "element length",
  "decimal special",
  "real stored source",
  "typed query refusal",
  "typed export refusal",
  "no footer",
  "root and WAL unchanged",
  "reopenable",
  "charges released",
] as const;

export const NATIVE_HLL_REACH = [
  "dense rank",
  "sparse duplicate",
  "sparse order",
  "real stored source",
  "checksums valid",
  "typed first-read refusal",
  "root and WAL unchanged",
  "reopenable",
  "charges released",
] as const;

export const CORRUPT_MERGE_STRUCTURE_REACH = [
  "short block",
  "zero block count",
  "offset table bounds",
  "block byte cap",
  "offset tiling",
  "offset outside block",
  "truncated key length",
  "nonminimal key length",
  "continued key length",
  "key width cap",
  "entry bounds",
  "trailing block bytes",
  "checksums valid",
  "merge consumes damaged block",
  "partial output discarded",
  "typed corruption",
  "root and WAL unchanged",
  "reopenable",
  "charges released",
] as const;

export const STRUCTURED_BOUNDARIES_REACH = [
  "JSONB surrogate and NUL boundaries",
  "JSONB integer precision boundary",
  "JSONB exponent boundary",
  "JSONB scale boundary",
  "JSON syntax boundaries",
  "array boolean boundaries",
  "array TIME lower and upper bounds",
  "array SMALLINT lower and upper bounds",
  "text code points and exact enum labels",
  "fatal UTF-8 in JSON and array text",
  "array NUMERIC scale and special boundaries",
  "array NUMERIC signed precision boundary",
  "array NUMERIC redundant sign extension",
  "borrowed fragments and zero remaining charge",
] as const;

export const SNAPSHOT_VALIDATOR_LEASE_REACH = [
  "fragmented accepted values",
  "fragmented refused values",
  "leases returned before grant close",
  "parser funded before producer entry",
  "cancel after lease acquisition",
] as const;

export const SNAPSHOT_FTS_CUT_REACH = [
  "checkpoint target",
  "checkpoint and staged target",
  "document and term payloads",
  "every physical publication step",
  "one power loss per cut",
  "old target recovered",
  "complete replacement recovered",
  "old history and payload intact",
  "canonical empty replacement",
  "retry after interruption",
  "old FTS retirement exactly once",
  "ordinary reopen",
  "zero charges",
] as const;

/** B7 arbitrary disk-backed release inventories retain R25/R8 under fixed complete credit. */
export const BOUNDED_RETIREMENT_REACH = [
  "large fixed-cap inventory",
  "reverse source",
  "multiple replay windows",
  "previous-root protection",
  "pin protection after both roots drain",
  "exactly-once physical release",
] as const;
/** Every accepted native retirement read, producer and publication keeps its ownership fence. */
export const BOUNDED_RETIREMENT_FENCE_REACH = [
  "packed alias subtraction",
  "exact duplicate union",
  "space-close read fence",
  "release read fence",
  "producer settlement fence",
  "accepted publication fence",
] as const;
/** Refusing admission cannot create a native retirement allocation or consume caller input. */
export const BOUNDED_RETIREMENT_ADMISSION_REACH = [
  "preparation before source",
  "cursor before native read",
  "publication before queue",
] as const;

/** Bounded array thresholds and same-space nested capture retain the exact release set. */
export const BOUNDED_RETIREMENT_ARRAY_REACH = [
  "array-4095",
  "array-4096",
  "array-4097",
  "array-8193",
  "mixed-source reentry",
  "bounded resident I/O saving",
] as const;
/** Recovery may use either durable slot, but never the discarded private descriptor file. */
export const BOUNDED_RETIREMENT_CRASH_REACH = [
  "durable exact root",
  "retry then both roots drain",
  "handle every publication step",
  "epoch every publication step",
] as const;
/** Every scalar/fixed-window boundary and reentrant charge may abandon an unfinished capture. */
export const BOUNDED_RETIREMENT_FAULT_REACH = [
  "empty source failure",
  "first item failure",
  "before window failure",
  "at window failure",
  "after window failure",
  "multiple window failure",
  "close at preparation charge",
  "close at owner charge",
  "release at cursor charge",
] as const;

export const FTS_RETIREMENT_PRESSURE_REACH = [
  "owned through publication",
  "released after settlement",
  "shared source refs",
  "unique source refs",
  "large preparation",
  "fixed native credit",
  "disk dedupe",
  "no fresh admission",
  "reopen exact rows",
  "zero charges",
] as const;

export const RETIREMENT_FAST_FALLBACK_REACH = [
  "throwing refusal",
  "boolean refusal",
  "at threshold",
  "below threshold",
  "disk fits192KiB",
  "exact survivor",
  "no grant growth",
  "foreign error preserved",
  "original root intact",
] as const;

export const ROOT_DUAL_GATE_REACH = [
  "short capability header corrupt",
  "u32 reader and writer",
  "high sequence independent",
  "borrowed manifest only",
] as const;

export const ROOT_NATIVE_DUAL_GATE_REACH = [
  "minimum reader",
  "minimum writer",
  "root-0",
  "root-1",
  "partial tree",
  "complete tree",
  "OPFS unchanged",
  "IDB unchanged",
  "concurrent refusals",
  "live lock untouched",
  "zero writes",
] as const;

export const NATIVE_WRITER_PROFILE_REACH = [
  "assigned rewrite vocabulary",
  "unassigned writer",
  "unassigned rewrite vocabulary",
  "read optional field requires writer gate",
  "all record kinds",
  "assigned selector in preserved population",
] as const;

export const ROOT_WRITE_GATE_REACH = [
  "durable reader gate before append",
  "durable writer gate before append",
  "FTS claims after gate",
  "zero charges",
] as const;

export const ROOT_GATE_RACE_REACH = [
  "IDB",
  "OPFS",
  "reader",
  "writer",
  "publication first",
  "probe-steal interval",
  "steal first",
  "refused live owner survives",
  "live owner still writes",
  "stolen queued publication refuses",
] as const;

export const ROOT_PUBLICATION_ORDER_REACH = [
  "batched earlier write and root atomic",
  "unbatched earlier write precedes root",
  "same extent overwrite after root",
] as const;

export const ROOT_GATE_LIVENESS_REACH = [
  "IDB",
  "OPFS",
  "gate granted",
  "future gate granted",
  "before owner steal",
  "after owner steal",
  "immediate notification",
  "delayed notification",
  "publisher completes while opener suspended",
  "same epoch higher generation",
  "stale opener no writes",
  "live owner still writes",
  "compatible takeover preserved",
  "before native fence",
] as const;

export const ROOT_FENCE_BOUNDARY_REACH = [
  "IDB",
  "OPFS",
  "before owner grant",
  "after owner grant",
  "fresh takeover while obsolete opener frozen",
  "durable identity before lease exposure",
  "cancelled waiter never steals later",
  "root and WAL preserved at cut",
] as const;

export const OWNER_IDENTITY_REACH = [
  "single-word epoch",
  "multi-word epoch",
  "both checksums and zero reserved",
  "all identity fields affect authority",
  "same epoch recovery",
  "takeover advances epoch",
  "hint ties use higher epoch",
] as const;

export const NATIVE_FIXTURE_REACH = [
  "one-byte chunks",
  "split chunks",
  "ordinary native open and exact bytes",
  "target-local epoch advances",
  "source unchanged",
  "occupied target refused unchanged",
] as const;

/** Baseline ratios are meaningful only for the recorded SQL and dataset scale. */
export const BROWSER_WORKLOAD_REACH = ["SQL changed", "dataset changed", "scale changed"] as const;

/** Shared hosts observe R6; acceptance needs independently qualified host evidence. */
export const R6_OBSERVATION_REACH = [
  "over-target observation",
  "unqualified acceptance refused",
  "qualified target judging",
  "invariants always enforced",
] as const;

/** A version label, fabricated status or missing browser cannot bypass R6 acceptance. */
export const R6_RELEASE_REACH = [
  "valid record",
  "over target refused",
  "unqualified refused",
  "incomplete browsers refused",
  "per-run overshoot refused",
  "0.x unchanged",
  "1.x gated",
] as const;

/** Catalog and row checkpoints must retain positional WAL schemas across ALTER. */
export const DDL_ROW_SCHEMA_REACH = [
  "position 0",
  "position 1",
  "position 2",
  "position 3",
  "FTS publication",
  "plain publication",
  "delete before ALTER",
  "insert only",
  "stored rows",
  "resident rows",
  "empty root before rows",
  "no initial empty root",
  "reopen after publication",
  "new schema write",
  "second ALTER",
  "checkpoint and maintenance",
] as const;

/** Rejected replay schemas cannot consume ownership or prevent repaired reopen. */
export const DDL_SCHEMA_CORRUPTION_REACH = [
  "past schema",
  "future schema",
  "typed refusal",
  "charge returned",
  "repair retry",
] as const;

export const DDL_ATOMIC_SCHEMA_REACH = [
  "drop",
  "add",
  "rename",
  "replace",
  "FTS",
  "plain",
  "atomic reopened",
] as const;

export const DDL_INDEX_SCHEMA_REACH = [
  "drop",
  "add",
  "rename",
  "first",
  "middle",
  "last",
  "shared DDL LSN",
  "publication advances",
  "exact reopen",
] as const;

export const ROW_IDENTITIES_REACH = [
  "stable columns",
  "stable tombstone indexes",
  "older identified rows",
  "positional mismatch refused",
  "invalid version refused",
  "malformed IDs refused",
  "unknown flag refused",
] as const;

export const DDL_PUBLICATION_SCHEMA_REACH = [
  "drop",
  "add",
  "rename",
  "first",
  "middle",
  "last",
  "route observed",
  "version aligned",
  "exact reopen",
] as const;

export const DDL_STAGED_ALTER_REACH = [
  "varlen",
  "fixed",
  "frozen default",
  "NULL missing",
  "first",
  "middle",
  "last",
  "FTS checkpoint",
  "plain staging checkpoint",
  "staging observed",
  "new catalog with unfolded claim",
  "exact pre-fold reopen",
  "native reads",
  "exact post-fold reopen",
] as const;

export const REPLAY_MAPPING_WORK_REACH = [
  "one table",
  "multiple tables",
  "wide",
  "narrow",
  "two physical versions",
  "unchanged schema",
  "unrelated DDL",
  "no unrelated DDL",
  "mapping reuse bound",
  "charged then released",
  "exact replay values",
  "zero retained",
] as const;

export const EXEC_CALL_LIST_REACH = [
  "empty",
  "ordinary",
  "above native argument limit",
  "nullable maximum",
  "parameter maximum and sum",
  "structural key maximum",
  "aggregate lane maximum",
] as const;

export const NEXT_CALL_LIST_REACH = ["empty", "ordinary", "large", "exact list"] as const;

export const REPLAY_MAPPING_MEMORY_REACH = [
  "wide IDs",
  "small IDs",
  "stable identity reuse",
  "permuted layout reuse",
  "same version identity refusal",
  "independent memtable ownership",
  "pressure discards inactive cache",
  "catalog invalidation",
  "row ownership outlives cache",
  "zero retained",
] as const;

export const SQL_REGEX_MATCH_MODES_REACH = [
  "astral input",
  "newline input",
  "start anchor",
  "end anchor",
  "captured substring",
  "boolean match",
  "boolean rejection",
  "zero-width match",
] as const;

export const SQL_REGEX_WORK_REACH = [
  "nested repetition",
  "ambiguous alternation",
  "repeated wildcards",
  "nullable end match",
  "anchored empty cycles",
] as const;

export const SQL_REGEX_FLAGS_REACH = [
  "newline mode",
  "case-sensitive mode",
  "case-insensitive mode",
] as const;

export const SQL_REGEX_ACCEPTANCE_REACH = [
  "long literal match",
  "long literal alternative",
  "failed-start cache rollover",
] as const;

export const SQL_REGEX_GENERATED_REACH = [
  "nullable",
  "nested",
  "alternative",
  "optional",
  "sequence",
  "~",
  "~*",
  "!~",
  "!~*",
  "start anchor",
  "end anchor",
  "Unicode",
  "astral input",
  "newline input",
  "boolean true",
  "boolean false",
  "captures compared",
  "PostgreSQL captures compared",
  "unmatched capture",
  "empty match",
  "typed limit distinct from result",
] as const;

export const PROTOTYPE_RUN_LIFETIME_REACH = [
  "already cancelled",
  "cancelled in flight",
  "query error",
  "deferred cleanup",
  "microtask gap",
  "task gap",
  "next observer intact",
] as const;

export const WRITER_INVENTORY_REACH = [
  "fixed record window",
  "bounded hidden record arrays",
  "discard and exact reuse",
  "exact full and ordinal snapshots",
  "all ownership credit returned",
] as const;

export const WRITER_INVENTORY_STORAGE_REACH = ["resident ownership", "spilled ownership"] as const;

export const WRITER_OWNERSHIP_PRESSURE_REACH = [
  "bounded synchronous view",
  "exact immutable source",
  "published source drained",
  "aborted source drained",
  "all-credit competitor",
  "zero ownership",
] as const;

export const WRITER_OWNED_PRODUCERS_REACH = [
  "large single producer",
  "competing publication credit",
  "exact committed and reopened rows",
  "zero ownership",
] as const;

export const WRITER_FOREIGN_OWNER_REACH = [
  "two live owners beyond bank",
  "creator grant closes before target",
  "snapshot survives creator",
  "zero both governors",
] as const;

export const WRITER_INCREMENTAL_REACH = [
  "linear capture IO",
  "partial chunk and new chunks",
  "bounded resident ownership",
  "older cut falls back exactly",
  "full snapshot remains exact",
  "all ownership returned",
] as const;

export const WRITER_FLUSH_CANCEL_REACH = [
  "compact pages only",
  "cancel after scratch writes",
  "resident ownership survives cancel",
  "retry exact ownership",
  "all cancellation credit returned",
] as const;

export const WRITER_SMALL_CURSOR_REACH = [
  "count bounded window",
  "maximum window",
  "empty no read",
  "all cursor credit returned",
] as const;

export const WRITER_RESIDENT_REACH = [
  "payload cleared before one refund",
  "resident zero journal writes",
  "crossing cut uses disk fallback",
  "resident suffix after earlier spill",
  "ordinal holes preserved",
  "input is not retirement authority",
  "creator closes after ownership transfer",
  "exact sealed receipts",
  "release fences accepted response",
  "all ownership returned",
] as const;

export const WRITER_RESIDENT_BACKING_REACH = [
  "small view with oversized backing refused",
  "no owner credit leak",
] as const;

export const WRITER_RESIDENT_DEFER_REACH = [
  "copy deferred to sliced disk route",
  "single admitted owner transferred once",
  "exact suffix after clock deferral",
  "all bytes released",
] as const;

export const WRITER_MIXED_CUT_REACH = [
  "many resident-only increasing cuts",
  "next crossing cut has bounded old overlap",
  "exact crossing suffix",
  "all mixed-cut credit returned",
] as const;

/** Canonical Result decoding changes representation without changing values or frame ownership. */
export const PUBLIC_RESULT_REACH = [
  "exact int64",
  "three decimal tiers",
  "ISO microseconds",
  "NULL garbage ignored",
  "dictionary deltas",
  "constant expansion",
  "multi-frame concatenation",
  "zero-copy flat view",
] as const;

export const PUBLIC_RESULT_PG_REACH = [
  "executor frames",
  "PG exact numeric",
  "D1 timestamp",
  "D2 bigint",
  "float32 boundary",
  "SQL array",
  "NULL result",
  "typed column metadata",
] as const;

export const PUBLIC_RESULT_CHANNEL_REACH = [
  "MessageChannel native execution",
  "safe int8 stays bigint",
  "wide int8 stays exact",
  "numeric display scale",
] as const;

export const PUBLIC_TRANSPORT_REACH = [
  "prepared parameter types",
  "structured admin reply",
  "ordinary cursor metadata and completion",
  "views live after close",
  "immutable dictionary prefix",
  "unsent close and cancellation",
  "early return terminal fence",
] as const;

export const PUBLIC_FACADE_REACH = [
  "owner query-only guard",
  "transaction isolation and scope fence",
  "callback rollback",
  "savepoint recovery",
  "private preparation and owned transaction boundary",
  "prepared catalog invalidation",
  "charged bounded prepared cache",
  "uncached prepared fallback",
  "cursor return retains Result and drains pin",
] as const;

export const PUBLIC_FACADE_OUTCOME_REACH = [
  "abort before commit is no effect",
  "commit wins over abort",
  "lost reply ledger replay",
  "lost RETURNING stays committed without result",
  "cursor abort preserves prefix and terminal reason",
] as const;

export const PUBLIC_CLIENT_BINDING_REACH = [
  "int32 input",
  "float64 input",
  "int64 input",
  "empty input",
  "validity word boundary",
  "terminal success beats abort",
  "no-effect reason and unknown effect",
  "cursor metadata abort reason",
] as const;

export const PUBLIC_BOOT_REACH = [
  "native fixed-minimum refusal releases fence",
  "native IDB scratch service",
  "memory backend spill exception",
  "recovery and continued writes",
  "exact native rows",
  "all native charges closed",
] as const;

export const PUBLIC_WORKER_BOOT_REACH = [
  "worker native SQL and exact decode",
  "worker isolated transaction",
  "worker cursor close retains views",
  "worker complete native teardown",
] as const;

export const PUBLIC_WORKER_LIFETIME_REACH = ["before-init", "during-resolve", "malformed"] as const;

export const PUBLIC_CURSOR_CONCURRENCY_REACH = [
  "abort all reads settle",
  "close all reads settle",
  "two calls",
  "many calls",
] as const;

export const PUBLIC_CONNECT_REACH = [
  "queued SQL and captured settings",
  "native owner restart and recovery",
  "native owner retained",
  "exact continued native writes",
  "all started owners and locks drained",
] as const;

export const PUBLIC_CONNECT_CLOSE_REACH = ["blocked", "reentrant"] as const;

/** Caller-owned Result projections reuse one decoded payload per dictionary/constant entry. */
export const PUBLIC_RESULT_RETENTION_REACH = [
  "dictionary entry shared",
  "constant entry shared",
  "nested array shared",
  "bytea shared",
  "row before values",
  "tuples before values",
  "views only on explicit request",
] as const;

/** Ordinary delivery relinquishes engine charge without recycling caller-held buffers. */
export const PUBLIC_RESULT_OWNERSHIP_REACH = [
  "delivered frame charge released",
  "actual native return-pool reuse",
  "held Results survive zero owner charge",
  "small cloned delivery",
  "large transferred delivery",
] as const;

/** Callback scopes own no persistent server, preparation or transaction-state entries. */
export const PUBLIC_SESSION_RETIRE_REACH = [
  "sequential callbacks",
  "concurrent callbacks",
  "rollback retires session",
  "commit retires session",
  "twenty or more callback rounds",
  "zero retained callback credit",
  "scoped preparations retired",
  "unprepared callbacks",
] as const;

export const PUBLIC_REENTRANT_CLOSE_REACH = [
  "body closes without waiting on itself",
  "savepoint closes without waiting on itself",
  "cursor closes without waiting on itself",
  "closed scope rejects every entry",
  "reentrant teardown drains native state",
] as const;

export const CALLBACK_LEDGER_RETIRE_REACH = [
  "single final marker",
  "concurrent final markers",
  "transaction retry refused before ACK",
  "COMMIT replay retained before ACK",
  "ACK frees callback tombstones",
  "old requests stale after ACK",
] as const;

export const PUBLIC_SESSION_OUTCOME_REACH = [
  "lost COMMIT replays after retirement",
  "failed rollback reply retires local recovery state",
  "callback body never replayed",
  "default session is not retired",
] as const;

export const PUBLIC_SESSION_FLAGS_REACH = ["default", "missing", "null", "wrongflag"] as const;

/** Internal per-store boot: each selected backend reaches the real owner and drains its lease. */
export const PUBLIC_STORE_ENTRY_REACH = [
  "memory native worker",
  "indexeddb native worker",
  "opfs native worker",
  "exact worker rows",
  "captured lifecycle policy",
  "worker lease and charge cleanup",
] as const;

/** Companion adapters use the exact canonical facade, not the legacy Result/Date boundary. */
export const NATIVE_KYSELY_REACH = [
  "exact int8",
  "nullable text",
  "NUMERIC scale",
  "timestamp microseconds",
  "bounded stream",
  "early cursor close",
  "isolated callback",
  "rollback",
  "savepoint recovery",
  "catalog SQL widths",
  "explicit decoding",
  "live unbound",
] as const;

export const NATIVE_EXPORT_REACH = [
  "BIGINT decimal tokens",
  "NUMERIC and temporal text",
  "escaped CSV",
  "NULL and arrays",
  "bytea hex",
  "multiple frames",
  "empty header",
  "duplicate columns",
  "cancel closes cursor",
] as const;

export const NATIVE_WORKER_BUNDLE_REACH = [
  "variable lag",
  "variable lead",
  "lazy default",
  "signed row offset",
  "minified real entry SQL",
  "native cleanup",
] as const;

export const PUBLIC_RESULT_LABEL_REACH = [
  "direct",
  "derived",
  "cte",
  "union",
  "except",
  "distinct",
  "group",
  "window",
  "join",
  "view",
  "empty",
  "unquoted",
  "aliasList",
  "0.x binding identities unchanged",
  "dotted labels",
  "case-sensitive labels",
  "object-property labels",
  "escaped labels",
  "ambiguous name rejects",
  "prepared labels",
  "cursor labels",
] as const;

/** Original follow capture follows each successful incremental unique checkpoint. */
export const NATIVE_BOUNDARY_FOLLOW_REACH = [
  "root preparation",
  "uniqueness before capture",
  "same token later rows",
  "successful action preparation",
  "failed uniqueness skips capture",
  "multiple actions",
  "budget-1",
  "budget0",
  "budget1",
  "zero ownership",
] as const;

/** Deferred original follow targets bind by immutable action ID, not a resident rowset map. */
export const REGISTRY_FOLLOWING_REACH = [
  "published",
  "resident",
  "original key swap targets",
  "no recapture",
  "immutable queued descriptor",
  "incremental same-token work",
  "independent FK reference failure",
  "fixed metadata",
  "zero ownership",
] as const;

/** Follow input cancellation must fence native IO before its prepaid metadata retires. */
export const REGISTRY_FOLLOWING_LIFETIME_REACH = [
  "unprepared",
  "retired",
  "corrupt",
  "cancel",
  "close",
  "published",
  "resident",
  "paid pending capture",
  "no retained image lease",
  "zero ownership",
] as const;

/** 1.0 deliberately reports later row rules before statement-end UNIQUE/PK errors. */
export const MODIFY_UNIQUE_TIMING_REACH = [
  "spooled input",
  "direct input",
  "PK",
  "UNIQUE",
  "NOT NULL",
  "CHECK",
  "VALUES",
  "SELECT",
  "same page",
  "across pages",
  "published",
  "resident",
  "RETURNING",
  "no RETURNING",
  "native row-level first",
  "PG unique first",
  "shipped primary unique-first",
  "shipped secondary row-first",
  "unique-only rejected",
  "row-only rejected",
  "atomic rollback",
  "success",
] as const;

/** Full native OLD payload capture stays inside source/image ownership. */
export const NATIVE_PREFIX_IMAGES_REACH = [
  "single-row native sizing",
  "source progress admission",
  "fragmented payload",
  "small payload",
  "published",
  "resident",
  "NULLs",
  "non-null",
  "wide decoder tier",
  "native decoder tier",
  "full physical order",
  "no legacy copies",
  "source retired before replay",
  "zero ownership",
] as const;

/** Payload capture fences native source/preparation work before retiring root scratch. */
export const NATIVE_PREFIX_IMAGES_LIFETIME_REACH = [
  "reentrant capture",
  "reentrant close shares fence",
  "prepare",
  "payload",
  "phase",
  "fault",
  "synchronous close",
  "external close",
  "caller cancellation",
  "published",
  "resident",
  "paid pending work",
  "source loan repaid",
  "zero ownership",
] as const;

/** Complete native RI payload inputs remain replayable through mutations and handle retirement. */
export const NATIVE_CHILD_INPUT_REACH = [
  "shared files",
  "empty",
  "nonempty",
  "many inputs",
  "resident",
  "published",
  "fragmented payload",
  "small payload",
  "complete before continuation",
  "replay after mutation",
  "retired handle",
  "images survive retirement",
  "foreign handle",
  "fixed metadata",
  "zero ownership",
] as const;

export const NATIVE_CHILD_INPUT_FENCE_REACH = [
  "IO fault",
  "between rows",
  "payload IO",
  "phase change",
  "caller cancellation",
  "external close",
  "pending metadata",
  "no partial handle",
  "source retired",
  "zero ownership",
] as const;

/** RI image proposals preserve canonical OLD and share unchanged fragmented payloads. */
export const REGISTRY_DERIVED_REACH = [
  "insert NEW",
  "linked parent scalar",
  "owned scalar",
  "published",
  "resident",
  "NULL replacement",
  "value replacement",
  "reverse replacements",
  "ordered replacements",
  "large shared payload",
  "small shared payload",
  "no payload copy",
  "canonical OLD",
  "successive derivations",
  "zero ownership",
] as const;

export const NATIVE_PARENT_KEY_REACH = [
  "removed parent",
  "retained parent",
  "resident",
  "published",
  "primary",
  "secondary",
  "changed",
  "unchanged",
  "parent present",
  "parent absent",
  "NULL OLD",
  "NULL NEW",
  "zero ownership",
] as const;

export const NATIVE_RI_RESTRICTION_REACH = [
  "resident",
  "published",
  "restrict",
  "noAction",
  "empty children",
  "referenced",
  "replacement parent",
  "no replacement",
  "end failure",
  "end success",
  "zero ownership",
] as const;

export const NATIVE_INFORMATIONAL_CAPTURE_REACH = [
  "resident",
  "published",
  "empty",
  "matching",
  "large unrelated payload",
  "small payload",
  "projected memory source",
  "fixed metadata",
  "zero ownership",
] as const;

export const NATIVE_RI_MUTATION_REACH = [
  "plain",
  "notNull",
  "check",
  "generated",
  "CASCADE",
  "SET NULL",
  "SET DEFAULT",
  "RESTRICT",
  "NO ACTION",
  "rekey",
  "delete",
  "published",
  "resident",
  "children",
  "empty",
  "valid",
  "same",
  "missing",
  "commit",
  "rollback",
  "zero ownership",
] as const;

export const NATIVE_RI_FOLLOW_REACH = [
  "informational scan",
  "indexed follow",
  "published",
  "resident",
  "swapped identities",
  "generated child",
  "zero ownership",
] as const;

export const NATIVE_RI_INCREMENTAL_UNIQUE_REACH = [
  "published",
  "resident",
  "actions 1",
  "actions 2",
  "actions 3",
  "child unique before later error",
  "old reference before later error",
  "zero ownership",
] as const;

export const NATIVE_RI_COMPOSITE_REACH = [
  "CASCADE",
  "SET NULL",
  "SET DEFAULT",
  "reversed key",
  "declared key",
  "rekey",
  "delete",
  "published",
  "resident",
  "zero ownership",
] as const;

export const NATIVE_RI_PRESSURE_REACH = [
  "rekey",
  "delete",
  "published",
  "resident",
  "64MiB",
  "competing credit",
  "many child segments",
  "bounded files",
  "zero ownership",
] as const;

export const NATIVE_PREFIX_FIXED_STEP_REACH = [
  "published",
  "resident",
  "payload",
  "empty payload",
  "64MiB competing credit",
  "zero ownership",
] as const;

export const MEMORY_SPILL_OFFSET_REACH = [
  "between pages",
  "empty",
  "many pages",
  "few pages",
  "absent",
  "present",
  "exact offset",
] as const;

export const MEMORY_SPILL_NATIVE_OFFSET_REACH = [
  "checksum",
  "payload",
  "many pages",
  "few pages",
  "zero ownership",
] as const;

export const NATIVE_RI_LIFETIME_REACH = [
  "published",
  "resident",
  "close fence",
  "caller cancel",
  "complete child input",
  "no borrow across continuation",
  "rollback",
  "zero ownership",
] as const;

export const NATIVE_RI_GENERATED_KEY_REACH = [
  "published",
  "resident",
  "delete cascade allowed",
  "assignment refused",
  "zero ownership",
] as const;

export const NATIVE_RI_REPLACEMENT_REACH = [
  "CASCADE",
  "SET NULL",
  "SET DEFAULT",
  "rekey",
  "delete",
  "published",
  "resident",
  "replacement parent",
  "zero ownership",
] as const;

export const NATIVE_ROW_BORROW_REACH = [
  "BEFORE placeholders",
  "published",
  "resident",
  "frozen leases",
  "generated values",
  "zero ownership",
] as const;

export const NATIVE_REFERENCE_CHECKPOINT_REACH = [
  "rekey",
  "delete",
  "current transaction insert",
  "current transaction update",
  "committed version",
  "published",
  "resident",
  "zero ownership",
] as const;

export const NATIVE_PARAMETER_REACH = [
  "clock",
  "no clock",
  "NULL",
  "int64 exact",
  "timestamp exact",
  "numeric exact",
  "payload",
  "fixed",
  "FTS alias",
  "zero ownership",
] as const;

export const PREPAID_QUERY_REACH = [
  "resident metadata",
  "no recipe",
  "many parts",
  "few parts",
  "loan",
  "idle transfer",
  "no hold and wait",
  "zero ownership",
] as const;

export const NATIVE_INPUT_REACH = [
  "resident",
  "published",
  "index input",
  "scan input",
  "VALUES input",
  "terminal error prefix",
  "successful prefix",
  "parameters retired before apply",
  "no writes",
  "zero ownership",
] as const;

export const NATIVE_BODY_HOST_REACH = [
  "INSERT",
  "UPDATE",
  "DELETE",
  "published",
  "resident",
  "zero rows",
  "rowset",
  "later body prefix",
  "rollback",
  "success",
  "zero ownership",
] as const;

export const NATIVE_BODY_RI_REACH = [
  "CASCADE",
  "SET NULL",
  "SET DEFAULT",
  "RESTRICT",
  "NO ACTION",
  "rekey",
  "delete",
  "published",
  "resident",
  "rowset",
  "empty",
  "rollback",
  "named continuations",
  "zero ownership",
] as const;

export const NATIVE_BODY_ERROR_REACH = [
  "earlier CHECK",
  "terminal expression",
  "published",
  "resident",
  "zero ownership",
] as const;

export const NATIVE_BODY_PRESSURE_REACH = [
  "published",
  "resident",
  "wide unused payload",
  "small rows",
  "64MiB competing credit",
  "bounded files",
  "zero ownership",
] as const;

export const NATIVE_BODY_LIFETIME_REACH = [
  "root close",
  "caller cancel",
  "published",
  "resident",
  "pending producer fence",
  "parameter lifetime",
  "rollback",
  "zero ownership",
] as const;

export const NATIVE_BODY_JOIN_REACH = [
  "DELETE USING",
  "UPDATE FROM",
  "published",
  "resident",
  "duplicate targets",
  "distinct source values",
  "accepted images only",
  "zero ownership",
  "duplicate expression error",
] as const;

export const NATIVE_PARAMETER_VALIDATION_REACH = [
  "count",
  "clock slot",
  "FTS slot",
  "shape",
  "type",
  "int64 bound",
  "int32 bound",
] as const;

export const NATIVE_INPUT_RETENTION_REACH = [
  "empty literal",
  "small literal",
  "large literal",
  "shared aliases",
  "zero ownership",
] as const;

export const NATIVE_BODY_REFERENCE_REACH = [
  "rekey",
  "delete",
  "earlier transaction INSERT",
  "committed child",
  "published",
  "resident",
  "zero ownership",
] as const;

export const NATIVE_FOLLOW_GROUP_REACH = [
  "explicit CASCADE PG",
  "omitted follow shipped",
  "swap",
  "rotation",
  "nonunique completed group",
  "unique completed group",
  "child constraint identity",
  "failure before later AFTER",
  "published",
  "resident",
  "zero ownership",
  "whole group OLD before writes",
] as const;

export const NATIVE_FOLLOW_GROUP_PRESSURE_REACH = [
  "published",
  "resident",
  "rotation",
  "64MiB competing credit",
  "bounded files",
  "fixed charged metadata",
  "zero ownership",
] as const;

export const NATIVE_RI_SCHEMA_IDENTITY_REACH = [
  "drop earlier position",
  "drop later position",
  "published",
  "resident",
  "rekey",
  "delete",
  "rollback",
  "commit",
  "stable index identities",
  "add after RI",
  "ordinary reopen",
  "zero ownership",
] as const;

export const NATIVE_SELF_FOLLOW_REACH = [
  "swap",
  "rotation",
  "published",
  "resident",
  "one follow per original target",
  "shipped data",
  "zero ownership",
] as const;

export const NATIVE_BODY_FOLLOW_GROUP_REACH = [
  "swap",
  "rotation",
  "published",
  "resident",
  "23505 before user22012",
  "parent and child AFTER see complete group",
  "explicit CASCADE PG boundary",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_ROOT_REACH = [
  "INSERT",
  "UPDATE",
  "DELETE",
  "published",
  "resident",
  "zero matches",
  "rollback",
  "commit",
  "atomic row/BEFORE error",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_ROOT_LIFETIME_REACH = [
  "complete end tail",
  "sealed source continuation",
  "published",
  "resident",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_ROOT_FOREIGN_REACH = [
  "foreign primary refused",
  "original graph usable",
  "published",
  "resident",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_ROOT_PRESSURE_REACH = [
  "published",
  "resident",
  "64MiB competing credit",
  "wide unrelated payload",
  "bounded files",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_REACH = [
  "frames1",
  "frames2",
  "frames3",
  "published",
  "resident",
  "zero rows",
  "terminal catalog error",
  "unreachable volatile body excluded",
  "UTF8 order",
  "shipped data/error",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_FK_REACH = [
  "follow",
  "cascade",
  "setNull",
  "setDefault",
  "restrict",
  "noAction",
  "DELETE",
  "UPDATE",
  "body generated parent key",
  "informational follow",
  "non-key update pruned",
  "charged fixed metadata",
  "zero ownership",
] as const;

export const NATIVE_COMMAND_REACH_LIFETIME = [
  "pause retains prepaid metadata",
  "cancel",
  "complete",
  "zero ownership",
] as const;

export const NATIVE_FOLLOW_VISITED_REACH = [
  "wide IDs",
  "low and high coexist",
  "low IDs",
  "dense bits",
  "sparse blocks",
  "command isolation",
  "FK isolation",
  "fixed charged metadata",
  "one shared file",
  "spilled replay",
  "zero ownership",
] as const;

export const NATIVE_PREFIX_WIDE_STEP_REACH = [
  "memory source",
  "stored source",
  "64MiB",
  "128MiB",
  "complete ordinary retry",
  "pin and decoder retired",
  "no reserve loan",
  "exact payload",
  "zero ownership",
] as const;

export const NATIVE_PREFIX_WIDE_LIFETIME_REACH = [
  "cancel",
  "close",
  "no producer pin or credit",
  "metadata fenced through admission",
  "zero ownership",
] as const;

export const FOLLOW_NESTED_AFTER_REACH = [
  "swap",
  "rotation",
  "first position",
  "middle position",
  "last position",
  "published",
  "resident",
  "group23505 before nested22012",
  "immutable nested images FIFO",
  "mid-swap RI uniqueness waits",
  "nested follow membership overlap",
  "nested explicit RI",
  "nested RI visible to next body",
  "deferred nested versions checked",
  "explicit PG action unchanged",
  "zero native charge",
  "body expression reads immediate RI",
] as const;

export const FOLLOW_NESTED_PRESSURE_REACH = [
  "published",
  "resident",
  "64MiB competing credit",
  "wide unchanged payload",
  "bounded active files",
  "deferred FIFO exact",
  "zero native charge",
] as const;

export const FOLLOW_NESTED_BOUNDARY_REACH = [
  "published",
  "resident",
  "nonmember immediate uniqueness",
  "spent budget preserved",
  "depth after completed group",
  "atomic rollback",
  "zero native charge",
] as const;

export const FOLLOW_UNIQUE_ALLOCATION_REACH = [
  "frames1",
  "frames2",
  "frames3",
  "denied before buffers",
  "granted and released once",
  "zero native charge",
] as const;

/** Follow's deferred USER AFTER state ends at the statement/savepoint, not transaction close. */
export const FOLLOW_NESTED_RECOVERY_REACH = [
  "deferred packets spilled",
  "cancel after spill",
  "failed statement inside savepoint",
  "successful deferred effects before rollback",
  "rollback to savepoint undoes successful deferred effects",
  "zero graph charge and spill files before continuation",
  "prefix and later command commit",
  "resident",
  "published",
  "zero native charge",
] as const;

export const WRITER_ABORT_STATS_REACH = [
  "stats observed mid-abort",
  "metadata await observed",
  "release batch boundary",
  "idle publish observed mid-abort",
  "fragmented builder",
  "spilled sealed inventory",
  "failed ranges remain owned",
  "partial release failure",
  "successful abort audit",
  "capture refuses aborting writer",
  "zero final charge",
] as const;

export const NATIVE_WRITE_PRESSURE_REACH = [
  "one funded root list through reopen",
  "idle row credit reclaimed",
  "competing reader owns real bytes",
  "admission after provisional unwind",
  "six 1 MiB native commits",
  "exact durable rows",
  "32 MiB peak",
  "zero final charge",
] as const;

export const NATIVE_WRITE_OPTIONAL_REACH = [
  "preferred size declined",
  "cheaper path exact",
  "no grant growth",
  "zero final charge",
] as const;

export const NATIVE_ROOT_LIST_FUNDING_REACH = [
  "pending funding preserved",
  "standalone keeper stays funded",
  "different identity refused",
  "one root reservation",
  "root survives keeper close",
  "zero final charge",
] as const;

export const NATIVE_WRITE_GROWTH_REACH = [
  "real competing buffers",
  "complete deficit after surrender",
  "task runs while growth waits",
  "rows",
  "metadata",
  "cancellation without competitor release",
  "net new credit completes",
  "zero final charge",
] as const;

export const LIVE_PUBLICATION_REACH = [
  "catalog publication",
  "savepoint rollback",
  "transaction rollback",
  "explicit commit",
  "observer isolation",
  "zero charges",
] as const;

export const LIVE_CLOSE_REACH = [
  "native captures released",
  "active decoder and replacement",
  "handle captures released",
  "client replacement released",
  "no delivery after disposal",
  "zero charges",
] as const;

export const LIVE_LIFECYCLE_REACH = [
  "full parameter backing preflight",
  "parameter copy funded before allocation",
  "parameter bytes captured privately",
  "shared parameters refused",
  "loading closes subscription",
  "loading refresh supersedes registration",
  "loading refresh transition",
  "loading reconnect transition",
  "loading owner-change transition",
  "one current client registration",
  "peer retirement during registration",
  "registration admission retirement",
  "parameter-copy retirement",
  "owner retirement",
  "runtime retirement",
  "dependency admission retirement",
  "no retired dependency links",
  "planning proof unknown",
  "dependency proof admission refused",
  "new dependency invalidation",
  "unknown proof widens globally",
  "recovered proof excludes unaffected tables",
  "scratch oracle after commits",
  "zero charges",
] as const;

export const LIVE_ERROR_REACH = [
  "error baseline evictable",
  "diagnostic admission retirement",
  "one unacknowledged error",
  "error burst coalesced",
  "error memory bounded",
  "same-row ready recovery",
  "error ACK preserves accepted rekeys",
  "error callback rejection isolated",
  "error refresh reset",
  "error reconnect reset",
  "error owner-change reset",
  "query error",
  "admission error",
  "zero charges",
] as const;

export const LIVE_RUNTIME_REACH = [
  "async delivery rejection",
  "sync delivery rejection",
  "rejected delivery rekey continuity",
  "rejected delivery final value",
  "result cleanup fence",
  "catalog identity reset",
  "missed middle notification",
  "late registration",
  "rekey covered",
  "rekey reset",
  "rekey owner reset",
  "row identity reuse",
  "small cap",
  "subscribe",
  "unsubscribe",
  "window",
  "commit",
  "rollback",
  "reconnect",
  "owner change",
  "refresh",
  "unaffected table",
  "zero charges",
] as const;

export const LIVE_ROW_VALUE_REACH = [
  "retained previous rows",
  "composite notices",
  "Date keys",
  "Date value changes",
  "captured input",
  "invalid key refused",
  "zero charges",
] as const;

export const LIVE_OBSERVER_REENTRY_REACH = [
  "subscribe",
  "unsubscribe",
  "replace",
  "eligible frontier",
  "bounded replacement chain",
  "zero charges",
] as const;

export const LIVE_DISPATCH_REACH = [
  "writer released",
  "response available",
  "bounded coalescing",
  "uncommitted excluded",
  "borrow stable during callout",
  "zero charges",
] as const;

export const LIVE_EFFECT_INTEGRATION_REACH = [
  "RI",
  "trigger body",
  "partial statement",
  "migration",
  "snapshot load",
  "zero charges",
] as const;

export const LIVE_ADMISSION_CLOSE_REACH = [
  "cohort durability",
  "begin",
  "next frame",
  "next end",
  "frame durability",
  "adoption",
  "done",
  "cleaned",
  "dependencies",
  "history",
  "copy",
  "deliver",
  "handle",
  "runtime",
  "zero charges",
] as const;

export const LIVE_BACKING_REACH = [
  "bytes",
  "words",
  "data",
  "buffer",
  "refused before clone",
  "whole backing charged",
  "previous row charged",
  "old diff released",
  "zero charges",
] as const;
