/**
 * The write path (access/write-path.ts): one engine per open database, in the owner.
 *
 * - **Writer slot.** `begin()` queues FIFO; the holder's transaction writes into the shared
 *   structures under pending seqs (transaction.ts).
 * - **Commit.** The transaction's effects become one log transaction (body.ts, log.ts); its changes
 *   are restamped to the commit LSN in the same synchronous step, so the next writer sees them.
 * - **Flush.** A frozen memtable whose rows and deltas are committed, durable, and not shared with
 *   a transaction that continues into the next memtable is written as a column group (page
 *   format's `GroupWriter`) plus one run per index, published in a new root through the extent
 *   space, and its readers are re-pointed (ruling R16.3).
 * - **Recovery** (`openWritePath`): the single log scan, truncation and replay of startup step 4.
 */
import {
  groupRootOf,
  LazyGroupRoot,
  Memtable,
  MEMTABLE_ROWS,
  type StagedPart,
} from "./memtable.js";
import type { KeyDelta } from "./key-hash.js";

/**
 * `openWritePath`'s input plus internal tuning for tests (never a public dial): smaller memtables
 * make crash sweeps cross many flushes and publishes in a short scenario.
 */
export type EngineInput = OpenWritePathInput & {
  readonly tuning?: {
    readonly memtableRows?: number;
    /**
     * Index run merges (merge.ts): `auto` (the default) merges whenever a level is in debt;
     * `manual` only in `runMaintenance()`; `off` never (benchmark baselines). The policy fields
     * shrink merges for tests.
     */
    readonly merges?: Partial<MergePolicy> & {
      readonly mode?: "auto" | "manual" | "off";
      /** Level-0 runs of one index at which writers wait for merges (default 3 × fan-in). */
      readonly stallRuns?: number;
    };
    /** Tests: awaited by a flush after it wrote its groups, before it takes the publish lock. */
    readonly beforeFlushPublish?: () => Promise<void>;
    /** Log bytes past which the log is recycled (default `LOG_RECYCLE_BYTES`). */
    readonly logRecycleBytes?: number;
    /** Tests: called after each R25 prepare, before the root is encoded; a throw fails there. */
    readonly afterDeltaPrepared?: (kind: PublicationKind, sequence: number) => void;
    /** The garbage floor of the idle publish (ruling R10; default `IDLE_GARBAGE_BYTES`). */
    readonly idleGarbageBytes?: number;
    /**
     * Tests: called at each step of a staging publication, for part `part` (the memtable's index
     * among those it stages in its table; −1 for the directory); a throw fails there.
     */
    readonly stageFault?: (step: "group" | "runs" | "directory", part: number) => void;
  };
};
import { CatalogChangeError, type TableDef } from "../access/catalog.js";
import { FROZEN_MEMTABLE_CAP_BYTES, MAX_LSN } from "../access/snapshot.js";
import type { Space, SpacePin } from "../access/space.js";
import {
  WritePathClosedError,
  type BeginOptions,
  type ColumnSummary,
  type CommitReceipt,
  type CommitRequest,
  type Durability,
  type ExtentDeltaResult,
  type ExtentDeltaSink,
  type OpenWritePathInput,
  type PinOptions,
  type TableSummary,
  type WritePath,
  type WritePathStats,
} from "../access/write-path.js";
import { RequestLedger } from "../format/ledger.js";
import {
  LogCorruptionError,
  newestLogHeader,
  RECOVERY_SCAN_BYTES,
  scanLogIncremental,
  type BodyEffect,
  type CommitFrameBody,
  type LogScanSummary,
} from "../format/log-frame.js";
import {
  MANIFEST_FORMAT_VERSION,
  type BlobRef,
  type ExtentRef,
  type GroupRef,
  type GroupRoot,
  type IndexRoot,
  type ManifestRoot,
  type RunRef,
  type TableRoot,
  type ColumnTotals,
  type TableStatistics,
} from "../format/manifest.js";
import { Replayer, type Effect } from "../format/replay.js";
import { walkReachable } from "../format/reachable.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import type { Grant } from "../memory/governor.js";
import { GroupWriter } from "../page/writer.js";
import type { StoreSession } from "../storage/contract.js";
import type { ExtentWriter } from "../storage/extents.js";
import type { StatValue } from "../types.js";
import { layoutOf } from "../types.js";
import type { Vector } from "../vector/vector.js";
import { compareBytes } from "../vector/varlen.js";
import { decodeBody, encodeBody } from "./body.js";
import { MemtableMemoryError } from "./bytes.js";
import type { MaybePromise } from "../access/key-index.js";
import { ByteWriter } from "./bytes.js";
import { Catalog, decodeLogged, type LoggedChange } from "./catalog.js";
import { decodeColumn, isNullAt, MEMORY_CHUNK_ROWS } from "./columns.js";
import { IDLE_FLUSH_MS, LogAppender, type LogPosition } from "./log.js";
import { BlobCache } from "../format/blob-cache.js";
import {
  decodeManifestRoot,
  encodeGroupRoot,
  encodeManifestRoot,
  GROUPS_PER_PAGE,
  isEmptyDirectory,
  ParseBudgetError,
  parseCharge,
  decodeTableStatistics,
  encodeTableStatistics,
  ManifestCorruptionError,
  writeGroupDirectory,
  type GroupPageEntry,
} from "../format/manifest-codec.js";
import { manifestReader, MetadataReader } from "../format/metadata-reader.js";
import { readRows } from "./rows.js";
import { encodeRun, RunBuilder, RunReader, runRef } from "./runs.js";
import { Sequences } from "./sequences.js";
import {
  DEFAULT_MERGE_POLICY,
  levelZero,
  MERGE_FIXED_BYTES,
  MERGE_INPUT_BYTES,
  MergeAbortedError,
  mergeRuns,
  planMerge,
  runExtent,
  yieldTask,
  YIELD_INTERVAL_MS,
  type MergePlan,
  type MergePolicy,
} from "./merge.js";
import { GroupDirectory } from "./groups.js";
import {
  GroupDeletes,
  groupDeletions,
  ReaderRegistry,
  type Shared,
  type TableView,
  type WriteState,
} from "./state.js";
import { hexId, TransactionImpl, type EngineHooks, type PublishedRows } from "./transaction.js";
import { SnapshotImpl } from "./view.js";
import { need } from "./bytes.js";

/**
 * Log bytes past which the write path recycles the log (access/write-path.ts "Log recycling"):
 * every table is flushed through the log's end, a root covering it switches to a new incarnation,
 * and the log is reset. The log stays within about this plus what accumulates while a recycle
 * waits for the writer slot. Chosen so the open's log scan stays under ~100 ms
 * (scripts/write-log-scan-bench.mts, Node, memory store, load ~16: a full 32 MiB log cost 303 ms of
 * write-path open, 8 MiB 89 ms, 4 MiB 77 ms, of which ~70 ms is fixed); a 5M-row keyed load
 * recycles 4 times instead of once, at the same write amplification.
 */
export const LOG_RECYCLE_BYTES = 8 * 1024 * 1024;

/**
 * Ruling R10: on an idle database whose retired space waiting for the next publish passes
 * max(this, 5% of the data space), the write path publishes one no-change root so it becomes
 * reusable.
 */
export const IDLE_GARBAGE_BYTES = 16 * 1024 * 1024;

/** The write path's working memory admitted at open (scan buffer, key batches, metadata). */
const MINIMUM_BYTES = RECOVERY_SCAN_BYTES + 4 * 1024 * 1024;

/** One merge to run: every run of `plan.level` of one index. */
interface MergeJob {
  readonly state: TableState;
  readonly indexId: number;
  readonly plan: MergePlan;
  readonly inputs: readonly RunRef[];
}

interface TableState {
  readonly tableId: number;
  view: TableView;
  /** The table's durable root, or null until a root first includes it. */
  root: TableRoot | null;
  /** LSN of the transaction that created it (0: before the root at open). */
  createdLsn: number;
  flushedThroughLsn: number;
  nextRowId: number;
  nextGroupId: number;
  /** The pages of its durable group directory (`root.groups`), in row-id order. */
  pages: readonly GroupPageEntry[];
  /**
   * Bytes charged for `pages` (`pageIndexBytes`) on the metadata grant. The groups themselves are
   * charged by their directory chunks (groups.ts), refcounted by the write states that hold them.
   */
  metadataBytes: number;
  /** Large transactions' staged rows not yet folded into `groups`, by staging id. */
  stagings: Map<number, Staging>;
}

/** Pins and residual deltas a staging publication made and has not installed yet. */
interface Provisional {
  readonly pins: SpacePin[];
  readonly deltas: KeyDelta[];
}

/**
 * One large transaction's staged rows in one table (format/replay.ts "Large transactions"): the
 * memtables staged so far and the root entry that lists them (`TableRoot.staged`).
 */
interface Staging {
  readonly id: number;
  /** Staged memtables in row-id order (a rollback may leave some empty or out of the view). */
  memtables: Memtable[];
  /** The entry as the table's root has it (null until a root first lists it). */
  entry: TableRoot["staged"][number] | null;
  /** The entry's directory blobs (index and pages): retired when it is rewritten or folded. */
  directory: ExtentRef[];
  /** Sequence of the root that published `entry` (a commit's `publishedRows` names it). */
  rootSequence: number;
}

/**
 * Resident bytes of a table's directory page index (one `GroupPageEntry` per `GROUPS_PER_PAGE`
 * groups: a page `BlobRef`, its extent, row-id range and counts), charged before it is
 * materialized. V8 measures about 193 B per decoded entry with its array slot; 512 B covers any
 * field shape (four objects of at most 8 fields, every number a heap double). It is O(groups / 4,096):
 * 25 entries at 100k groups.
 */
const PAGE_ENTRY_BYTES = 512;
const pageIndexBytes = (pages: number): number => pages * PAGE_ENTRY_BYTES;

/** `TableRoot.groups` of a table without groups (format/manifest-codec.ts: nothing is read). */
const EMPTY_DIRECTORY: BlobRef = {
  extent: { offset: 0, length: 0 },
  offset: 0,
  length: 0,
  checksum: 0,
};

function blobKey(ref: BlobRef): string {
  return `${String(ref.extent.offset)}:${String(ref.offset)}:${String(ref.length)}:${String(ref.checksum)}`;
}

function sameExtent(a: ExtentRef, b: ExtentRef): boolean {
  return a.offset === b.offset && a.length === b.length;
}

/** The highest seq of anything in a memtable (rows, delta entries). */
function maxSeq(memtable: Memtable, rows: number): number {
  // The smallest bound that shows every row, from the marks (at most MAX_LSN: committed rows).
  let max = Math.min(memtable.seqCovering(rows), MAX_LSN);
  for (const delta of memtable.deltas.values())
    for (let entry = 0; entry < delta.entryCount; entry++) max = Math.max(max, delta.seq(entry));
  return max;
}

const RESOLVED: Promise<void> = Promise.resolve();

/** A commit's receipt: its durability promise is made when first read (most callers never do). */
class Receipt implements CommitReceipt {
  readonly wrote = true;
  #durable: Promise<void> | null = null;

  constructor(
    readonly lsn: number,
    readonly appender: LogAppender,
  ) {}

  get durable(): Promise<void> {
    return (this.#durable ??= this.appender.durable(this.lsn) ?? RESOLVED);
  }
}

/** One holder's share of a state's space pin; the last release drops the pin. */
class StatePin implements SpacePin {
  #released = false;

  constructor(
    readonly entry: { readonly pin: SpacePin; count: number },
    readonly label: string,
    readonly pins: Map<WriteState, unknown>,
    readonly state: WriteState,
    /** The last holder of the state is gone: its generation's metadata is released. */
    readonly onLast: (state: WriteState) => void,
  ) {}

  get generation(): number {
    return this.entry.pin.generation;
  }

  get released(): boolean {
    return this.#released;
  }

  release(): void {
    if (this.#released) return;
    this.#released = true;
    this.entry.count -= 1;
    if (this.entry.count > 0) return;
    this.entry.pin.release();
    this.pins.delete(this.state);
    this.onLast(this.state);
  }
}

/** The lowest seq of anything in a memtable (its first rows or delta entries), or Infinity. */
function minSeq(memtable: Memtable): number {
  // The bound that shows its first row, from the marks (a pending seq when it holds only
  // pending rows; Infinity when none is marked).
  let min = memtable.rowCount > 0 ? memtable.seqCovering(1) : Infinity;
  for (const delta of memtable.deltas.values())
    for (let entry = 0; entry < delta.entryCount; entry++) min = Math.min(min, delta.seq(entry));
  return min;
}

function compareStat(a: StatValue, b: StatValue): number {
  if (a instanceof Uint8Array && b instanceof Uint8Array)
    return compareBytes(a, 0, a.length, b, 0, b.length);
  if (typeof a === "bigint" || typeof b === "bigint") {
    const x = typeof a === "bigint" ? a : BigInt(Number(a));
    const y = typeof b === "bigint" ? b : BigInt(Number(b));
    return x < y ? -1 : x > y ? 1 : 0;
  }
  const x = Number(a);
  const y = Number(b);
  if (Number.isNaN(x) || Number.isNaN(y)) return Number.isNaN(x) ? (Number.isNaN(y) ? 0 : 1) : -1;
  return x < y ? -1 : x > y ? 1 : 0;
}

/** A column's planning totals while they are maintained (format/manifest.ts `ColumnTotals`). */
type Totals = { -readonly [K in keyof ColumnTotals]: ColumnTotals[K] };

/** A table's totals by column id, copied before a publish changes them. */
function cloneTotals(totals: ReadonlyMap<number, Totals> | undefined): Map<number, Totals> {
  const out = new Map<number, Totals>();
  for (const [id, total] of totals ?? []) out.set(id, { ...total });
  return out;
}

/** Adds one group's chunk statistics to `totals`. */
function addGroup(totals: Map<number, Totals>, root: GroupRoot): void {
  for (const chunk of root.columns) {
    let total = totals.get(chunk.columnId);
    if (total === undefined) {
      total = {
        columnId: chunk.columnId,
        nullCount: 0,
        values: 0,
        bytes: 0,
        unbounded: false,
        min: null,
        max: null,
      };
      totals.set(chunk.columnId, total);
    }
    const statistics = chunk.statistics;
    total.nullCount += statistics.nullCount;
    total.values += root.rowCount - statistics.nullCount;
    total.bytes += chunk.data.length;
    if (statistics.min === null || statistics.max === null) {
      if (root.rowCount > statistics.nullCount) total.unbounded = true;
      continue;
    }
    if (total.min === null || compareStat(statistics.min, total.min) < 0)
      total.min = statistics.min;
    if (total.max === null || compareStat(statistics.max, total.max) > 0)
      total.max = statistics.max;
  }
}

/** The stored form of a table's totals (`TableRoot.statistics`). */
function tableStatistics(groupCount: number, totals: ReadonlyMap<number, Totals>): TableStatistics {
  const columns = [...totals.values()]
    .map((total) =>
      total.unbounded || total.values === 0 ? { ...total, min: null, max: null } : { ...total },
    )
    .sort((a, b) => a.columnId - b.columnId);
  return { groupCount, columns };
}

export class WritePathImpl implements WritePath, EngineHooks {
  readonly durability: Durability;
  readonly ledger: RequestLedger;
  readonly sequences: Sequences;
  readonly shared: Shared;
  readonly keys: KeyBatch;
  /** Reused per commit: inserted column bytes, then the encoded body. */
  readonly #columns = new ByteWriter(64 * 1024);
  readonly #body = new ByteWriter(64 * 1024);
  readonly #session: StoreSession;
  readonly #space: Space;
  readonly #grant: Grant;
  readonly #now: () => number;
  readonly #signal: AbortSignal | undefined;
  #appender!: LogAppender;
  #catalog: Catalog;
  /** The catalog of the current root (as of its global LSN). */
  #baseCatalog: Catalog;
  readonly #catalogHistory: Array<{ lsn: number; catalog: Catalog }> = [];
  #version: number;
  readonly #tables = new Map<number, TableState>();
  #state!: WriteState;
  #root: ManifestRoot;
  #failure: Error | null = null;
  #closed = false;
  #holder: TransactionImpl | null = null;
  /** Writers and maintenance waiting for the slot, FIFO; `take` claims it for the waiter. */
  readonly #queue: Array<{
    resolve: () => void;
    reject: (error: unknown) => void;
    take: () => void;
  }> = [];
  #flushes: Promise<void> = Promise.resolve();
  #flushScheduled = false;
  readonly #runReaders = new Map<string, RunReader | Promise<RunReader>>();
  /** The same readers by `RunRef` identity (the hot path: no key string per probe). */
  readonly #runsByRef = new WeakMap<RunRef, RunReader>();
  /**
   * The long-lived charge for metadata the engine keeps resident (the root, every table's
   * directory state), apart from the evictable caches: a grant of its own, so it shows under its
   * label and a refusal raises `ParseBudgetError` instead of growing the heap unseen.
   */
  readonly #metadataGrant: Grant;
  /**
   * Decoded roots and their parse charges, refcounted by the write states that hold them (review
   * B3-p2a-3 #2): an older root stays charged until the last reader of its generation closes. A
   * root starts with one reference, owned by its publication until a state installs it.
   */
  readonly #roots = new Map<ManifestRoot, { refs: number; readonly bytes: number }>();
  /** Group roots, directory pages and indexes: decoded once, cached under the governor. */
  readonly #metadata: MetadataReader;
  readonly #metadataCaches: ReadonlyArray<BlobCache<unknown>>;
  /** Planning totals per table (from `TableRoot.statistics` at open; updated at each flush). */
  readonly #summaries = new Map<number, Map<number, Totals>>();
  #labels = 0;
  /** Published memtables older readers may still hold (deltas, borrowed chunks). */
  readonly #published = new Set<Memtable>();
  readonly #governorCap: number;
  readonly #memtableRows: number;
  /** True while open replays the log (no flush runs before the appender exists). */
  #recovering = true;
  readonly #governor: EngineInput["governor"];
  readonly #mergePolicy: MergePolicy;
  readonly #mergeMode: "auto" | "manual" | "off";
  readonly #stallRuns: number;
  readonly #beforeFlushPublish: (() => Promise<void>) | null;
  readonly #logRecycleBytes: number;
  /** The running log recycle, if any. */
  #recycling: Promise<void> | null = null;
  readonly #idleGarbageBytes: number;
  readonly #extentDeltas: ExtentDeltaSink | null;
  readonly #afterDeltaPrepared: ((kind: PublicationKind, sequence: number) => void) | null;
  readonly #stageFault: NonNullable<EngineInput["tuning"]>["stageFault"] | null;
  /** R25 attempts so far (the initial root of a new store was attempt 1). */
  #attempts = 1;
  /** A sink's commit or abort threw: the write path fails (a durable publication stays installed). */
  readonly #sinkFailed = (error: ExtentSinkError): void => {
    this.#failure ??= error;
  };
  readonly #nextAttempt = (): number => ++this.#attempts;
  /** The armed idle-publish check (R10), if any. */
  #idleTimer: ReturnType<typeof setTimeout> | null = null;
  /** An idle publication the timer started and that has not finished. */
  #idleRun: Promise<void> | null = null;
  /** `performance.now()` of the last commit (idleness for R10). */
  #lastCommitAt = 0;
  /** True while maintenance (a log recycle) holds the writer slot. */
  #maintenanceHolds = false;
  /** The merge chain (one merge at a time, beside the flush chain). */
  #merges: Promise<void> = Promise.resolve();
  #mergeScheduled = false;
  /**
   * The publish lock: a flush and a merge each build their root from the current state, publish
   * and install it while holding it, so neither builds on a root the other is replacing.
   */
  #locked: Promise<void> = Promise.resolve();
  /** Maintenance counters (benchmarks, tests): bytes written by flushes and merges, merges. */
  readonly maintenance = {
    flushes: 0,
    /** Bytes every flush wrote (groups, runs, directory, catalog). */
    flushBytes: 0,
    /** Of those, index runs. */
    flushRunBytes: 0,
    flushRunEntries: 0,
    merges: 0,
    /** Merges whose inputs reached the oldest run (tombstones dropped). */
    bottomMerges: 0,
    /** Merges whose output was split into several runs. */
    splitMerges: 0,
    mergeEntriesIn: 0,
    mergeEntriesOut: 0,
    /** Entries a put and its tombstone cancelled. */
    mergeCancelled: 0,
    mergeBytes: 0,
    /** Task yields merges took (R23 slices). */
    mergeSlices: 0,
    /** Flushes published while a merge was running. */
    flushesDuringMerge: 0,
    /** Merges published while a flush was writing (the flush then builds on the merged runs). */
    mergesDuringFlush: 0,
    /** Merges dropped at publish because their inputs changed (a drop). */
    mergesAbandoned: 0,
    /** Flushes dropped at publish because their table was dropped meanwhile. */
    flushesAbandoned: 0,
    /** Stage publications (large transactions' rows written before commit), rows staged, folds. */
    stages: 0,
    stagedRows: 0,
    stagedFolds: 0,
    /** Staging an open found no commit for (dropped and retired). */
    stagingDropped: 0,
    /** Staged memtables a commit's restage truncated (tail deleted) or dropped (emptied). */
    stagedTruncations: 0,
    stagedMemtablesDropped: 0,
    /** Stagings a rollback dropped; staged commits replayed at open. */
    stagingRollbacks: 0,
    publishedReplays: 0,
    /** Delta entries staged memtables kept resident (tombstones of older rows). */
    stagedResidualEntries: 0,
    /** Flushes held because their table is newer than the root an index build holds back. */
    flushesHeld: 0,
    /** Index builds on quiet tables a published root settled (empty index, checkpoint moved). */
    buildsSettled: 0,
    /** Merges that failed before their publish (inputs kept), and the last failure's message. */
    mergeFailures: 0,
    lastMergeFailure: null as string | null,
    /** Log recycles completed, and the log bytes each one cut. */
    recycles: 0,
    recycledBytes: 0,
    /** Recycles given up before switching (an index build not yet published). */
    recyclesAbandoned: 0,
    /** Tables a recycle flushed, and tables it only moved the checkpoint of. */
    recycleFlushedTables: 0,
    recycleQuietTables: 0,
    /** Frames appended while a recycle switched incarnations (written to the new log). */
    deferredFrames: 0,
    /** Runs written for memtables without rows (their tombstones). */
    rowlessRuns: 0,
    /** No-change roots published on an idle database to free waiting space (R10). */
    idlePublishes: 0,
    mergeMs: 0,
    mergesByLevel: [] as number[],
  };
  #merging = false;
  /** Flushes writing their groups (started, not yet holding the publish lock). */
  #flushesWriting = 0;
  /** Publications (flushes that reached a durable root). */
  publications = 0;

  private constructor(
    input: EngineInput,
    grant: Grant,
    root: ManifestRoot,
    catalog: Catalog,
    metadata: { readonly grant: Grant; readonly rootCharge: number },
  ) {
    this.#metadataGrant = metadata.grant;
    this.#roots.set(root, { refs: 1, bytes: metadata.rootCharge });
    this.durability = input.options.durability;
    this.#memtableRows = input.tuning?.memtableRows ?? MEMTABLE_ROWS;
    this.#session = input.session;
    this.#space = input.space;
    this.#grant = grant;
    const caches = {
      groupRoots: new BlobCache<GroupRoot>((cache) => input.governor.registerCache(cache)),
      groupPages: new BlobCache<readonly GroupRef[]>((cache) =>
        input.governor.registerCache(cache),
      ),
      groupIndexes: new BlobCache<readonly GroupPageEntry[]>((cache) =>
        input.governor.registerCache(cache),
      ),
    };
    this.#metadataCaches = [caches.groupRoots, caches.groupPages, caches.groupIndexes];
    this.#metadata = new MetadataReader(input.space, caches);
    this.#governorCap = input.governor.capBytes;
    this.#governor = input.governor;
    const merges = input.tuning?.merges;
    this.#mergePolicy = { ...DEFAULT_MERGE_POLICY, ...merges };
    this.#mergeMode = merges?.mode ?? "auto";
    this.#stallRuns = merges?.stallRuns ?? 3 * this.#mergePolicy.fanIn;
    this.#beforeFlushPublish = input.tuning?.beforeFlushPublish ?? null;
    this.#logRecycleBytes = input.tuning?.logRecycleBytes ?? LOG_RECYCLE_BYTES;
    this.#idleGarbageBytes = input.tuning?.idleGarbageBytes ?? IDLE_GARBAGE_BYTES;
    this.#extentDeltas = input.extentDeltas ?? null;
    this.#afterDeltaPrepared = input.tuning?.afterDeltaPrepared ?? null;
    this.#stageFault = input.tuning?.stageFault ?? null;
    this.#now = input.options.now ?? Date.now;
    this.#signal = input.options.signal;
    this.#root = root;
    this.#catalog = catalog;
    this.#baseCatalog = catalog;
    this.#version = catalog.version;
    this.ledger = RequestLedger.restore(root.clients, this.#now());
    this.sequences = new Sequences({
      definition: (id) => this.#catalog.sequence(id) ?? this.#holder?.catalog.sequence(id),
      logReservation: (sequenceId, reservedThrough) => {
        const lsn = this.#appender.append(
          encodeBody({
            request: null,
            effects: [{ kind: "sequence", sequenceId, reservedThrough }],
          }),
        );
        // Standalone frames count toward the log bound too (review B3-p2b-1 #5).
        this.#afterAppend();
        return lsn;
      },
      now: this.#now,
    });
    for (const { sequenceId, reservedThrough } of root.sequences)
      this.sequences.restore(sequenceId, reservedThrough, root.globalLsn);
    this.keys = new KeyBatch(grant);
    const space = input.space;
    this.shared = {
      space,
      budget: grant,
      readers: new ReaderRegistry(),
      pinState: (state, label) => this.#pinState(state, label),
      runReader: (ref) => this.#runReader(ref),
      groupRoot: (ref, budget) => this.#metadata.groupRoot(ref, budget),
      bitmap: (ref) => this.#bitmap(ref),
    };
  }

  // ── Open and recovery ────────────────────────────────────────────────────────────────────

  static async open(input: EngineInput): Promise<WritePathImpl> {
    const grant = await input.governor.admit({
      label: "write path",
      minimumBytes: MINIMUM_BYTES,
      spillable: true,
      ...(input.options.signal === undefined ? {} : { signal: input.options.signal }),
    });
    const metadataGrant = await input.governor.admit({
      label: "write path metadata",
      minimumBytes: 0,
      spillable: true,
    });
    try {
      const { space, session } = input;
      let rootCharge = 0;
      let root: ManifestRoot;
      let catalog: Catalog;
      if (space.root === null) {
        catalog = Catalog.empty();
        const writer = space.beginWrite("initial root");
        const written = await writer.write([catalog.encode()]);
        const salt = (crypto.getRandomValues(new Uint32Array(1))[0] ?? 1) >>> 0;
        root = {
          formatVersion: MANIFEST_FORMAT_VERSION,
          sequence: 1,
          epoch: session.epoch,
          log: { salt, firstLsn: 1, rootSequence: 1 },
          globalLsn: 0,
          catalogVersion: 0,
          catalog: need(written.blobs[0]),
          nextTableId: 1,
          sequences: [],
          tables: [],
          clients: [],
        };
        await session.resetLog();
        const sink: { failure: ExtentSinkError | null } = { failure: null };
        const delta = new DeltaPublication(
          "initial",
          input.extentDeltas ?? null,
          () => 1,
          input.tuning?.afterDeltaPrepared ?? null,
          (error) => {
            sink.failure ??= error;
          },
        );
        let failed: unknown = null;
        try {
          await delta.prepare(root.sequence, writer, []);
          const bytes = encodeManifestRoot(root);
          rootCharge = parseCharge(bytes.length);
          if (!metadataGrant.allocate(rootCharge)) throw new ParseBudgetError(rootCharge);
          await delta.publish(space, bytes, writer, []);
        } catch (error) {
          failed = error;
        } finally {
          delta.settle();
        }
        // A sink that failed to settle is the open's failure, as it is the write path's later.
        if (sink.failure !== null) throw sink.failure;
        if (failed !== null)
          throw failed instanceof Error
            ? failed
            : new Error("The first root failed", { cause: failed });
      } else {
        // The decoded root stays resident for the session: its parse charge stays with it.
        const parsed = decodeManifestRoot(space.root, metadataGrant);
        rootCharge = parsed.charge;
        root = parsed.value;
        catalog = Catalog.decode(await readBlob(space, root.catalog, grant));
      }
      const engine = new WritePathImpl(input, grant, root, catalog, {
        grant: metadataGrant,
        rootCharge,
      });
      await engine.#load();
      await engine.#recover();
      engine.#recovering = false;
      // A resolved promise would start the fold before the open result reaches the owner.
      // Track the task delay in the flush chain so quiesce/close also wait for it.
      engine.#flushes = new Promise<void>((resolve) => setTimeout(resolve, 0));
      engine.#scheduleFlush();
      engine.#scheduleMerge();
      if (engine.#appender.end >= engine.#logRecycleBytes) engine.#maybeRecycle();
      return engine;
    } catch (error) {
      metadataGrant.close();
      grant.close();
      throw error;
    }
  }

  async #load(): Promise<void> {
    const root = this.#root;
    const built: GroupDirectory[] = [];
    try {
      for (const table of root.tables) {
        let groups = GroupDirectory.EMPTY;
        let pages: readonly GroupPageEntry[] = [];
        let metadataBytes = 0;
        if (!isEmptyDirectory(table.groups)) {
          const index = await this.#metadata.groupIndex(table.groups, this.#grant);
          // The page index is charged before it is materialized; each directory chunk is charged
          // (exactly) before its buffer exists (review B3-p2a-3, ruling on findings 1, 2, 6).
          metadataBytes = pageIndexBytes(index.value.length);
          try {
            this.#chargeMetadata(metadataBytes);
          } catch (error) {
            index.release();
            throw error;
          }
          pages = [...index.value];
          index.release();
          const builder = GroupDirectory.builder(this.#metadataGrant).expect(table.groupCount);
          try {
            for (const page of pages) {
              const held = await this.#metadata.groupPage(page.ref, this.#grant);
              try {
                for (const group of held.value) builder.add(group);
              } finally {
                held.release();
              }
            }
            groups = builder.finish();
          } catch (error) {
            builder.abandon();
            this.#chargeMetadata(-metadataBytes);
            throw error;
          }
          built.push(groups);
        }
        const runs = new Map<number, readonly RunRef[]>();
        for (const index of [table.keyIndex, ...table.secondaryIndexes])
          if (index !== null) runs.set(index.indexId, index.runs);
        const state: TableState = {
          tableId: table.tableId,
          view: {
            tableId: table.tableId,
            groups,
            runs,
            groupDeletes: new GroupDeletes(),
            foldedThrough: table.flushedThroughLsn,
            groupSources: new Map(),
            memtables: [],
          },
          root: table,
          createdLsn: 0,
          flushedThroughLsn: table.flushedThroughLsn,
          nextRowId: table.nextRowId,
          nextGroupId: groups.nextGroupId,
          stagings: new Map(),
          pages,
          metadataBytes,
        };
        // Planning totals come from the table's statistics blob: one read per table, never a
        // group root (Gate A reopen). Its parse charge stays with the table's metadata.
        if (table.statistics !== null) {
          const parsed = decodeTableStatistics(
            await readBlob(this.#space, table.statistics, this.#grant),
            this.#metadataGrant,
          );
          state.metadataBytes += parsed.charge;
          if (parsed.value.groupCount !== table.groupCount)
            throw new ManifestCorruptionError(
              `Table ${String(table.tableId)}'s statistics cover ${String(parsed.value.groupCount)} of ${String(table.groupCount)} groups`,
            );
          this.#summaries.set(
            table.tableId,
            new Map(parsed.value.columns.map((column) => [column.columnId, { ...column }])),
          );
        } else if (table.groupCount > 0)
          throw new ManifestCorruptionError(
            `Table ${String(table.tableId)} has groups but no statistics`,
          );
        this.#tables.set(table.tableId, state);
      }
      this.#publishState();
    } finally {
      // The first state holds them now (or the open failed and nothing does).
      for (const directory of built) directory.releaseCreation();
      this.#releaseRoot(root);
    }
  }

  async #recover(): Promise<void> {
    const root = this.#root;
    const session = this.#session;
    const checkpoint = {
      rootSequence: root.sequence,
      globalLsn: root.globalLsn,
      nextTableId: root.nextTableId,
      tables: new Map(
        root.tables.map((table) => [
          table.tableId,
          {
            flushedThroughLsn: table.flushedThroughLsn,
            schemaVersion: table.schemaVersion,
            staged: new Map(
              table.staged.map((staged) => [
                staged.transaction,
                { indexIds: staged.indexRuns.map((runs) => runs.indexId) },
              ]),
            ),
          },
        ]),
      ),
    };
    const replayer = new Replayer(checkpoint);
    for (const table of root.tables)
      for (const staged of table.staged)
        this.#nextStagingId = Math.max(this.#nextStagingId, staged.transaction + 1);
    const headers = await session.readLogHeaders();
    let parts: Uint8Array[] = [];
    const summary: LogScanSummary = await scanLogIncremental(
      { length: session.logLength(), read: (offset, length) => session.readLog(offset, length) },
      headers,
      root.log,
      {
        part: (frame) => {
          parts.push(frame.body.slice());
        },
        commit: async (_transaction, lastLsn) => {
          const bytes = parts.length === 1 ? need(parts[0]) : concat(parts);
          parts = [];
          await this.#replay(replayer, lastLsn, decodeBody(bytes));
        },
        abandon: () => {
          parts = [];
        },
      },
      this.#grant,
    );
    Replayer.checkCoverage(checkpoint, summary.lastLsn);
    // Staging no frame claimed: its transaction never committed. The first root leaves it out
    // and retires its rows and runs together.
    for (const unclaimed of replayer.unclaimedStaging()) {
      const state = this.#tables.get(unclaimed.tableId);
      const table = state?.root;
      const entry = table?.staged.find((staged) => staged.transaction === unclaimed.transaction);
      if (state === undefined || table == null || entry === undefined) continue;
      // Its extents are found by a walk of the entry, done by the first root (after the open,
      // which reads no group root: review B3-p2c-1 #2).
      this.#unclaimedWalks.push({
        ...table,
        groups: EMPTY_DIRECTORY,
        groupCount: 0,
        liveRowCount: 0,
        keyIndex: null,
        secondaryIndexes: [],
        statistics: null,
        staged: [entry],
      });
      state.root = { ...table, staged: table.staged.filter((staged) => staged !== entry) };
      this.maintenance.stagingDropped += 1;
    }
    if (session.logLength() > summary.validEnd) await session.truncateLog(summary.validEnd);
    const { header, nextSlot } = newestLogHeader(headers, root.log.salt);
    const position: LogPosition = {
      salt: root.log.salt,
      chain: summary.chain,
      end: summary.validEnd,
      lastLsn: summary.lastLsn,
      generation: (header?.generation ?? 0) + 1,
      nextSlot,
      acked:
        header === null
          ? { end: 0, chain: root.log.salt >>> 0, lsn: root.log.firstLsn - 1 }
          : { end: header.ackEnd, chain: header.ackChain, lsn: header.ackLsn },
      rootSequence: root.log.rootSequence,
    };
    this.#appender = new LogAppender(session, position, this.durability === "strict", (error) => {
      this.#failure = error;
    });
  }

  async #replay(replayer: Replayer, lsn: number, body: CommitFrameBody): Promise<void> {
    const effects: Effect[] = body.effects.map((effect) => replayEffect(effect));
    if (body.request !== null) effects.push({ kind: "request" });
    const decisions = replayer.next(lsn, effects);
    let published: Set<number> | null = null;
    for (let k = 0; k < body.effects.length; k++) {
      if (decisions[k] !== true) continue;
      const effect = need(body.effects[k]);
      switch (effect.kind) {
        case "createTable":
        case "catalog":
        case "dropTable":
          await this.#replayCatalog(effect, lsn);
          break;
        case "sequence":
          this.sequences.restore(effect.sequenceId, effect.reservedThrough, lsn);
          break;
        case "rows":
          await this.#replayRows(effect, lsn);
          break;
        case "publishedRows":
          await this.#replayPublished(effect, lsn);
          (published ??= new Set()).add(effect.tableId);
          break;
        case "alterTable":
          throw new LogCorruptionError(`The write path cannot replay ${effect.kind} yet`);
      }
    }
    // As at commit: a staged commit's live tail freezes, so its fold can run once replay ends.
    for (const tableId of published ?? []) {
      const state = this.#tables.get(tableId);
      const last = state?.view.memtables[state.view.memtables.length - 1];
      if (state !== undefined && last?.state === "live" && last.rowCount > 0) {
        last.state = "frozen";
        last.measure();
        this.#setView(state, { ...state.view });
      }
    }
    if (body.request !== null && decisions[body.effects.length] === true)
      this.ledger.record(hexId(body.request.clientId), body.request.sequence, {
        kind: "committed",
        lsn,
        counts: body.request.counts,
        recordedAtMs: this.#now(),
      });
  }

  async #replayCatalog(effect: BodyEffect, lsn: number): Promise<void> {
    let change: LoggedChange;
    if (effect.kind === "dropTable") change = { op: "dropTable", tableId: effect.tableId };
    else if (effect.kind === "createTable" || effect.kind === "catalog")
      change = decodeLogged(effect.definition);
    else return;
    const catalog = this.#catalog.apply(change, ++this.#version);
    this.#catalog = catalog;
    this.#catalogHistory.push({ lsn, catalog });
    if (change.op === "createTable") {
      this.createTableState(change.table, change.table.id);
      const state = this.#tables.get(change.table.id);
      if (state !== undefined) {
        state.createdLsn = lsn;
        state.flushedThroughLsn = lsn - 1;
      }
    }
    if (change.op === "dropTable") this.#dropTableState(change.tableId);
    if (change.op === "dropSequence") this.sequences.drop(change.sequenceId);
    if (change.op === "createIndex") {
      const state = this.#tables.get(change.index.tableId);
      // A root whose table checkpoint covers the build has its entries in the runs already.
      if (state !== undefined && state.flushedThroughLsn < lsn) {
        await this.#backfill(change.index.tableId, change.index.id, lsn);
        this.#pendingIndexes.set(change.index.id, { tableId: change.index.tableId, lsn });
      }
    }
    if (change.op === "dropIndex") this.#pendingIndexes.delete(change.indexId);
  }

  /** Replay of createIndex: entries for every live row, with the creating commit's seq. */
  async #backfill(tableId: number, indexId: number, seq: number): Promise<void> {
    const memtable = this.addIndexDelta(tableId, indexId);
    const def = this.#catalog.table(tableId);
    const index = def?.indexes.find((candidate) => candidate.id === indexId);
    if (def === undefined || index === undefined) return;
    const snapshot = this.#internalSnapshot();
    try {
      const table = snapshot.internalTable(tableId);
      const rowIds: number[] = [];
      for (const segment of table.segments) {
        const deletions = await segment.deletions();
        for (let position = 0; position < segment.rowCount; position++)
          if (!deletions?.isDeleted(position))
            rowIds.push(table.rowId(segment.index * 65_536 + position));
      }
      for (let from = 0; from < rowIds.length; from += MEMORY_CHUNK_ROWS) {
        const ids = rowIds.slice(from, from + MEMORY_CHUNK_ROWS);
        const vectors = await readRows(table, ids);
        this.#putEntries(def, [index], memtable, vectors, null, ids, seq);
      }
    } finally {
      snapshot.releaseInternal();
    }
  }

  async #replayRows(effect: Extract<BodyEffect, { kind: "rows" }>, lsn: number): Promise<void> {
    const def = this.#catalog.table(effect.tableId);
    const state = this.#tables.get(effect.tableId);
    if (def === undefined || state === undefined)
      throw new LogCorruptionError(`Rows for table ${String(effect.tableId)}, which replay lacks`);
    const inserts = effect.inserts;
    if (inserts !== null) {
      const columns = def.columns.map((column, c) => {
        const bytes = inserts.columns[c];
        if (bytes === undefined) throw new LogCorruptionError("A logged insert lacks a column");
        return decodeColumn(bytes, column.type);
      });
      let done = 0;
      while (done < inserts.rowCount) {
        let { memtable } = this.liveMemtable(effect.tableId, inserts.rowCount - done);
        const logged = inserts.firstRowId + done;
        // Staging moves the root's next row id past its rows; after a rollback the ids it used
        // (never committed) may be used again by rows logged but not flushed. An empty memtable
        // starts at the logged row then, never before what precedes it.
        if (
          memtable.rowCount === 0 &&
          logged < memtable.firstRowId &&
          logged >= this.#rowFloor(state, memtable)
        )
          memtable = this.#liveAt(state, logged);
        // Row ids a rolled-back staging used and the next rows skipped: a forward gap.
        if (memtable.firstRowId + memtable.rowCount < logged)
          memtable = this.#liveAt(state, logged);
        const next = memtable.firstRowId + memtable.rowCount;
        if (next !== inserts.firstRowId + done)
          throw new LogCorruptionError(
            `Logged rows of table ${String(effect.tableId)} start at row ${String(inserts.firstRowId + done)}, not ${String(next)}`,
          );
        const take = Math.min(memtable.capacity, inserts.rowCount - done);
        const before = memtable.rowCount;
        memtable.append(columns, null, done, take);
        memtable.mark(lsn);
        const rowIds = Array.from({ length: take }, (_, k) => memtable.firstRowId + before + k);
        const rows = Uint16Array.from({ length: take }, (_, k) => done + k);
        this.#putEntries(def, def.indexes, memtable, columns, rows, rowIds, lsn);
        done += take;
      }
    }
    const deletes = effect.deletes;
    if (deletes !== null) {
      const live = this.liveMemtable(effect.tableId, 0).memtable;
      deletes.rowIds.forEach((rowId, k) => {
        const keys = deletes.keys[k] ?? [];
        def.indexes.forEach((index, i) => {
          const key = keys[i];
          if (key === undefined || key.length === 0) return;
          live.deltas.get(index.id)?.add(key, 0, key.length, rowId, lsn, true);
        });
        this.#markDeleted(state, rowId, lsn);
      });
    }
  }

  /**
   * Replay of a large transaction's commit (format/replay.ts "Large transactions"): its staged
   * groups become staged memtables committed at `lsn`, after the memtables replayed so far, in
   * row-id order; a later flush folds them in (with `flushedThroughLsn ≥ lsn`).
   */
  async #replayPublished(
    effect: Extract<BodyEffect, { kind: "publishedRows" }>,
    lsn: number,
  ): Promise<void> {
    const def = this.#catalog.table(effect.tableId);
    const state = this.#tables.get(effect.tableId);
    const entry = state?.root?.staged.find((staged) => staged.transaction === effect.transaction);
    if (def === undefined || state === undefined || entry === undefined)
      throw new LogCorruptionError(
        `Transaction ${String(effect.transaction)} is not staged for table ${String(effect.tableId)}`,
      );
    const refs: GroupRef[] = [];
    const directory: ExtentRef[] = [];
    if (!isEmptyDirectory(entry.groups)) {
      directory.push(entry.groups.extent);
      const index = await this.#metadata.groupIndex(entry.groups, this.#grant);
      try {
        for (const page of index.value) {
          directory.push(page.ref.extent);
          const held = await this.#metadata.groupPage(page.ref, this.#grant);
          try {
            refs.push(...held.value);
          } finally {
            held.release();
          }
        }
      } finally {
        index.release();
      }
    }
    const memtables = [...state.view.memtables];
    const last = memtables[memtables.length - 1];
    if (last?.state === "live") {
      if (last.rowCount === 0 && [...last.deltas.values()].every((delta) => delta.entryCount === 0))
        memtables.pop();
      else {
        last.state = "frozen";
        last.measure();
      }
    }
    let end = memtables[memtables.length - 1]?.rowIdEnd ?? 0;
    const staged: Memtable[] = [];
    for (const [k, ref] of refs.entries()) {
      if (ref.firstRowId < end)
        throw new LogCorruptionError("Staged rows overlap rows replayed before them");
      // Read when first needed (rows read, or the fold's statistics): an open reads no group
      // root (R25; review B3-p2c-1 #2).
      const groupRoot = new LazyGroupRoot(async () => {
        const held = await this.#metadata.groupRoot(ref.root, this.#grant);
        try {
          return held.value;
        } finally {
          held.release();
        }
      });
      const memtable = new Memtable(
        def.id,
        ref.firstRowId,
        def.schemaVersion,
        def.columns,
        def.indexes.map((index) => index.id),
        this.#grant,
        this.#memtableRows,
      );
      memtable.rowCount = ref.rowCount;
      memtable.storedRows = ref.rowCount;
      memtable.mark(lsn);
      if (ref.deletions !== null) {
        const bits = await this.#bitmap(ref.deletions);
        for (let position = 0; position < ref.rowCount; position++)
          if (((bits[position >>> 5] ?? 0) & (1 << (position & 31))) !== 0)
            memtable.deletions.set(position, lsn);
      }
      // Every staged run of the commit is visible with its rows: the first memtable holds them.
      const runs = new Map<number, readonly RunRef[]>(
        k === 0 ? entry.indexRuns.map((index) => [index.indexId, index.runs]) : [],
      );
      const pin = this.#space.pin(`staged t${String(def.id)}`);
      memtable.staged = {
        stagingId: effect.transaction,
        group: ref,
        groupRoot,
        runs,
        pin,
        extents: [],
      };
      memtable.published = { group: ref, groupRoot, runs: new Map(), pin };
      memtable.state = "staged";
      staged.push(memtable);
      end = memtable.rowIdEnd;
    }
    state.nextRowId = Math.max(state.nextRowId, end);
    this.#setView(state, { ...state.view, memtables: [...memtables, ...staged] });
    this.maintenance.publishedReplays += 1;
    state.stagings.set(effect.transaction, {
      id: effect.transaction,
      memtables: staged,
      entry,
      directory,
      rootSequence: this.#root.sequence,
    });
  }

  #markDeleted(state: TableState, rowId: number, seq: number): void {
    for (const memtable of state.view.memtables)
      if (rowId >= memtable.firstRowId && rowId < memtable.firstRowId + memtable.rowCount) {
        memtable.deletions.set(rowId - memtable.firstRowId, seq);
        return;
      }
    const groups = state.view.groups;
    const at = groups.indexOfRow(rowId);
    if (at < 0) throw new LogCorruptionError(`A logged delete names unknown row ${String(rowId)}`);
    state.view.groupDeletes.add(groups.groupId(at), rowId - groups.firstRowId(at), seq);
  }

  /** Index puts for row ids `rowIds` (physical `rows` of `vectors`, or all) into `memtable`, at `seq`. */
  #putEntries(
    def: TableDef,
    indexes: TableDef["indexes"],
    memtable: Memtable,
    vectors: readonly Vector[],
    rows: Uint16Array | null,
    rowIds: readonly number[],
    seq: number,
  ): void {
    const keys = new KeyBatch(this.#grant);
    try {
      for (const index of indexes) {
        const delta = memtable.deltas.get(index.id);
        if (delta === undefined) continue;
        const columns = index.columnIds.map((columnId) => ({
          vector: need(vectors[def.columns.findIndex((column) => column.id === columnId)]),
          direction: "asc" as const,
          nulls: "first" as const,
        }));
        const length = vectors[0]?.length ?? rowIds.length;
        const selection = rows === null ? null : { rows, count: rows.length };
        encodeKeyBatch("equality", columns, { length, selection }, keys);
        for (let k = 0; k < rowIds.length; k++) {
          const row = rows === null ? k : (rows[k] ?? 0);
          if (columns.some((column) => isNullAt(column.vector, row))) continue;
          delta.add(
            keys.writer.bytes,
            keys.offsets[k] ?? 0,
            keys.offsets[k + 1] ?? 0,
            rowIds[k] ?? 0,
            seq,
            false,
          );
        }
      }
    } finally {
      keys.close();
    }
  }

  // ── State ────────────────────────────────────────────────────────────────────────────────

  /** The engine's own reference on the current state's pin (views of it reuse that pin). */
  #currentPin: SpacePin | null = null;

  #publishState(): void {
    const tables = new Map<number, TableView>();
    for (const [id, state] of this.#tables) tables.set(id, state.view);
    this.#state = { root: this.#root, tables };
    const previous = this.#currentPin;
    this.#currentPin = this.#pinState(this.#state, "current state");
    previous?.release();
  }

  #setView(state: TableState, view: TableView): void {
    state.view = view;
    this.#publishState();
  }

  state(): WriteState {
    return this.#state;
  }

  committedCatalog(): Catalog {
    return this.#catalog;
  }

  get catalog(): Catalog {
    return this.#catalog;
  }

  nextCatalogVersion(): number {
    this.#version += 1;
    return this.#version;
  }

  label(): string {
    this.#labels += 1;
    return `tx#${String(this.#labels)}`;
  }

  #def(tableId: number): TableDef {
    const def = this.#holder?.catalog.table(tableId) ?? this.#catalog.table(tableId);
    if (def === undefined) throw new CatalogChangeError(`Table ${String(tableId)} does not exist`);
    return def;
  }

  /**
   * The memtable `rows` new rows go to (0: tombstones only, never a freeze). A memtable that
   * cannot take them all and holds none of the open transaction's rows freezes first, so a
   * transaction's batch never straddles two memtables unless the transaction already wrote rows in
   * the full one (then the flush waits for both: format/replay.ts per-table checkpoints).
   */
  liveMemtable(
    tableId: number,
    rows = 1,
  ): { memtable: Memtable; created: boolean; frozen: Memtable | null } {
    const state = this.#tables.get(tableId);
    if (state === undefined) throw new RangeError(`Table ${String(tableId)} has no state`);
    const memtables = state.view.memtables;
    const last = memtables[memtables.length - 1];
    if (last?.state === "live") {
      if (rows === 0) return { memtable: last, created: false, frozen: null };
      const pending = last.visibleRows(MAX_LSN) < last.rowCount;
      const fits = !last.full && last.capacity >= Math.min(rows, MEMORY_CHUNK_ROWS);
      if (fits || (pending && !last.full)) return { memtable: last, created: false, frozen: null };
      if (last.rowCount === 0) return { memtable: last, created: false, frozen: null };
    }
    let frozen: Memtable | null = null;
    if (last?.state === "live") {
      last.state = "frozen";
      last.measure();
      frozen = last;
    }
    const def = this.#def(tableId);
    const firstRowId =
      last === undefined ? state.nextRowId : Math.max(state.nextRowId, last.rowIdEnd);
    const memtable = new Memtable(
      tableId,
      firstRowId,
      def.schemaVersion,
      def.columns,
      def.indexes.map((index) => index.id),
      this.#grant,
      this.#memtableRows,
    );
    this.#setView(state, { ...state.view, memtables: [...memtables, memtable] });
    if (frozen !== null && !this.#recovering) this.#scheduleFlush();
    return { memtable, created: true, frozen };
  }

  /** The first row id `memtable` may start at: past the memtables before it and every group. */
  #rowFloor(state: TableState, memtable: Memtable): number {
    const memtables = state.view.memtables;
    const at = memtables.indexOf(memtable);
    const previous = at > 0 ? memtables[at - 1] : undefined;
    if (previous !== undefined) return previous.rowIdEnd;
    const groups = state.view.groups;
    return groups.count > 0 ? groups.get(groups.count - 1).lastRowId + 1 : 1;
  }

  /** A live memtable starting at `firstRowId` (replay past a gap): the current one ends first. */
  #liveAt(state: TableState, firstRowId: number): Memtable {
    const memtables = [...state.view.memtables];
    const last = memtables[memtables.length - 1];
    if (last?.state === "live") {
      if (last.rowCount === 0 && [...last.deltas.values()].every((delta) => delta.entryCount === 0))
        memtables.pop();
      else {
        last.state = "frozen";
        last.measure();
      }
    }
    const def = this.#def(state.tableId);
    const memtable = new Memtable(
      state.tableId,
      firstRowId,
      def.schemaVersion,
      def.columns,
      def.indexes.map((index) => index.id),
      this.#grant,
      this.#memtableRows,
    );
    this.#setView(state, { ...state.view, memtables: [...memtables, memtable] });
    return memtable;
  }

  undoFreeze(tableId: number, created: Memtable, frozen: Memtable | null): void {
    const state = this.#tables.get(tableId);
    if (state === undefined) return;
    const memtables = state.view.memtables.filter((memtable) => memtable !== created);
    // The emptied memtable leaves: its memory returns once no older reader can reach it.
    if (memtables.length !== state.view.memtables.length)
      this.shared.readers.holdUntilOlderClose(() => created.releaseAll());
    if (
      frozen !== null &&
      frozen.state === "frozen" &&
      memtables[memtables.length - 1] === frozen &&
      !frozen.full
    )
      frozen.state = "live";
    this.#setView(state, { ...state.view, memtables });
  }

  createTableState(def: TableDef | null, tableId: number): void {
    if (def === null) {
      this.#tables.delete(tableId);
      this.#publishState();
      return;
    }
    this.#tables.set(tableId, {
      tableId,
      view: {
        tableId,
        groups: GroupDirectory.EMPTY,
        runs: new Map(),
        groupDeletes: new GroupDeletes(),
        foldedThrough: 0,
        groupSources: new Map(),
        memtables: [],
      },
      root: null,
      createdLsn: 0,
      flushedThroughLsn: 0,
      nextRowId: 1,
      nextGroupId: 1,
      stagings: new Map(),
      pages: [],
      metadataBytes: 0,
    });
    this.#publishState();
  }

  /**
   * CREATE INDEX builds whose entries are still only in a memtable delta (ruling 1): a root's
   * global checkpoint stays below their LSN until their table's checkpoint covers them, so a
   * reopen replays the build.
   */
  readonly #pendingIndexes = new Map<number, { readonly tableId: number; readonly lsn: number }>();

  /** Charges (or, negative, returns) resident metadata bytes; a refusal is `ParseBudgetError`. */
  #chargeMetadata(bytes: number): void {
    if (bytes < 0) this.#metadataGrant.free(-bytes);
    else if (bytes > 0 && !this.#metadataGrant.allocate(bytes)) throw new ParseBudgetError(bytes);
  }

  #dropTableState(tableId: number): void {
    for (const [indexId, pending] of this.#pendingIndexes)
      if (pending.tableId === tableId) this.#pendingIndexes.delete(indexId);
    const state = this.#tables.get(tableId);
    this.#tables.delete(tableId);
    this.#summaries.delete(tableId);
    this.#publishState();
    // Older readers may still read its memtables (ruling 5): their memory goes when they close,
    // and after a flush of the table still writing them (it abandons at its publish).
    if (state !== undefined)
      this.shared.readers.holdUntilOlderClose(() => {
        const release = () => {
          for (const memtable of state.view.memtables) memtable.releaseAll();
          this.#chargeMetadata(-state.metadataBytes);
          state.metadataBytes = 0;
          this.#dropRunReaders([...state.view.runs.values()].flat());
        };
        const flushing = this.#flushing.get(state);
        if (flushing === undefined) release();
        else void flushing.then(release, release);
      });
  }

  /** The flush writing each table now, if any (a drop releases the table's memory after it). */
  readonly #flushing = new Map<TableState, Promise<void>>();

  addIndexDelta(tableId: number, indexId: number): Memtable {
    const { memtable } = this.liveMemtable(tableId);
    if (!memtable.deltas.has(indexId)) memtable.addDelta(indexId);
    return memtable;
  }

  /**
   * Bytes charged for the current generation's resident metadata: the root, every table's
   * directory state, and the opened runs' resident parts.
   */
  get residentMetadataBytes(): number {
    let total = this.#roots.get(this.#root)?.bytes ?? 0;
    for (const state of this.#tables.values())
      total += state.metadataBytes + state.view.groups.chargedBytes;
    // The resident parts of the index runs opened (fences, block refs, Bloom filters).
    for (const reader of this.#runReaders.values())
      if (!(reader instanceof Promise)) total += reader.bytes;
    return total;
  }

  #releaseRoot(root: ManifestRoot): void {
    const entry = this.#roots.get(root);
    if (entry === undefined) return;
    entry.refs -= 1;
    if (entry.refs > 0) return;
    this.#roots.delete(root);
    this.#chargeMetadata(-entry.bytes);
  }

  /** A write state now holds its root and every table's directory chunks (one reference each). */
  #retainState(state: WriteState): void {
    const root = this.#roots.get(state.root);
    if (root !== undefined) root.refs += 1;
    for (const view of state.tables.values()) view.groups.retain();
  }

  /** The last holder of `state` is gone. */
  #releaseState(state: WriteState): void {
    for (const view of state.tables.values()) view.groups.release();
    this.#releaseRoot(state.root);
  }

  /** Total bytes of every table's memtables (live, frozen, retained). */
  #memtableBytes(): number {
    let total = 0;
    for (const state of this.#tables.values())
      for (const memtable of state.view.memtables) total += memtable.bytes;
    return total;
  }

  /** The cap on memtable bytes across tables: a quarter of the memory cap, at least 16 MiB. */
  get #memtableCap(): number {
    return Math.max(16 * 1024 * 1024, Math.floor(this.#governorCap / 4));
  }

  /**
   * Ensures the write path's grant can take `bytes` more memtable memory before an insert
   * (ruling 6): grows the grant, and when the governor refuses, freezes the largest live
   * memtables that hold no pending rows and waits for their flushes. Throws
   * `MemtableMemoryError` when nothing more can be freed.
   */
  admit(bytes: number): MaybePromise<void> {
    // A large transaction's full memtables are staged first: its rows never stay resident whole.
    const stage = this.#stageCandidates();
    if (stage !== null)
      return this.#stage(stage).then(() =>
        this.#headroom(bytes) ? undefined : this.#relieve(bytes),
      );
    if (this.#headroom(bytes)) return;
    return this.#relieve(bytes);
  }

  #headroom(bytes: number): boolean {
    const free = this.#grant.held - this.#grant.allocated;
    return free >= bytes || this.#grant.tryGrow(bytes - free);
  }

  async #relieve(bytes: number): Promise<void> {
    for (let attempt = 0; attempt < 8; attempt++) {
      const live = [...this.#tables.values()]
        .map((state) => state.view.memtables[state.view.memtables.length - 1])
        .filter(
          (memtable): memtable is Memtable =>
            memtable?.state === "live" &&
            memtable.rowCount > 0 &&
            memtable.visibleRows(MAX_LSN) === memtable.rowCount,
        )
        .sort((a, b) => b.bytes - a.bytes);
      for (const memtable of live.slice(0, 2)) {
        memtable.state = "frozen";
        this.#setView(need(this.#tables.get(memtable.tableId)), {
          ...need(this.#tables.get(memtable.tableId)).view,
        });
      }
      this.#scheduleFlush();
      await this.#appender.flush();
      await this.#flushes;
      this.#check();
      if (this.#headroom(bytes)) return;
    }
    throw new MemtableMemoryError(bytes);
  }

  // ── Public API ───────────────────────────────────────────────────────────────────────────

  get appliedLsn(): number {
    return this.#appender.lastLsn;
  }

  get durableLsn(): number {
    return this.#appender.durableLsn;
  }

  get failure(): Error | null {
    return this.#failure ?? this.#appender.failure;
  }

  #check(): void {
    if (this.#closed) throw new WritePathClosedError();
    const failure = this.failure;
    if (failure !== null) throw failure;
  }

  /** The committed catalog visible at `lsn` (≥ the root's global LSN). */
  #catalogAt(lsn: number): Catalog {
    for (let k = this.#catalogHistory.length - 1; k >= 0; k--) {
      const entry = this.#catalogHistory[k];
      if (entry !== undefined && entry.lsn <= lsn) return entry.catalog;
    }
    return this.#baseCatalog;
  }

  /**
   * After a publication: every table's root is the published one (a root may reconcile a table it
   * did not otherwise change, review B3-p2b-3 C3), so later publications build on what is durable.
   */
  #adoptTableRoots(root: ManifestRoot): void {
    for (const table of root.tables) {
      const state = this.#tables.get(table.tableId);
      if (state === undefined) continue;
      if (state.root !== null) state.root = table;
      state.flushedThroughLsn = Math.max(state.flushedThroughLsn, table.flushedThroughLsn);
    }
    this.maintenance.buildsSettled += this.#settles.get(root) ?? 0;
    const drained = this.#drainedBy.get(root);
    if (drained !== undefined) {
      const gone = new Set(drained);
      this.#pendingRetire = this.#pendingRetire.filter((extent) => !gone.has(extent));
    }
    // An index build a durable table checkpoint covers has its entries in that table's runs.
    for (const [indexId, pending] of this.#pendingIndexes) {
      const table = root.tables.find((candidate) => candidate.tableId === pending.tableId);
      if (table !== undefined && table.flushedThroughLsn >= pending.lsn)
        this.#pendingIndexes.delete(indexId);
    }
  }

  /** Forgets catalog history a root at `globalLsn` covers. */
  #pruneCatalogs(globalLsn: number): void {
    let last = -1;
    for (let k = 0; k < this.#catalogHistory.length; k++)
      if ((this.#catalogHistory[k]?.lsn ?? Infinity) <= globalLsn) last = k;
    if (last < 0) return;
    this.#baseCatalog = (this.#catalogHistory[last] as { catalog: Catalog }).catalog;
    this.#catalogHistory.splice(0, last + 1);
  }

  pin(options: PinOptions = {}): SnapshotImpl {
    this.#check();
    const applied = this.appliedLsn;
    let bound = this.durability === "strict" ? this.durableLsn : applied;
    if (options.atLeastLsn !== undefined)
      bound = Math.max(bound, Math.min(options.atLeastLsn, applied));
    return new SnapshotImpl({
      state: this.#state,
      bound,
      observedLsn: bound,
      catalog: this.#catalogAt(bound),
      shared: this.shared,
      label: options.label ?? "read",
      statementView: false,
    });
  }

  #internalSnapshot(): SnapshotImpl {
    return new SnapshotImpl({
      state: this.#state,
      bound: MAX_LSN,
      // Internal: never delivered, so it observes nothing a client could wait for.
      observedLsn: 0,
      catalog: this.#catalog,
      shared: this.shared,
      label: "write path",
      statementView: false,
    });
  }

  /** The writer slot is held (by a transaction or by maintenance). */
  get #slotBusy(): boolean {
    return this.#holder !== null || this.#maintenanceHolds || this.#claimed;
  }

  /** A writer that has its turn and waits on backpressure holds the slot already. */
  #claimed = false;

  async begin(options: BeginOptions = {}): Promise<TransactionImpl> {
    this.#check();
    await this.#takeSlot("writer", options.signal);
    try {
      this.#check();
      // Backpressure: frozen memtables awaiting publication past their cap, or memtables across
      // every table past theirs, make writers wait (ruling 6). None of it needs the slot.
      // Every round changes state or ends the wait, and yields a task (review B3-p2b-3 C1): a
      // commit spanning memtables can only flush whole, so when the boundary is blocked by the
      // live memtable that commit continues into, that (committed) tail is frozen too.
      while (this.#frozenBytes() > FROZEN_MEMTABLE_CAP_BYTES) {
        if (
          this.#nextFlushReady() === null &&
          !this.#freezeBlockingTails() &&
          !this.#freezeBuilds()
        )
          break;
        const before = this.#flushProgress();
        this.#scheduleFlush();
        await this.#flushes;
        await yieldTask();
        this.#check();
        if (this.#flushProgress() === before) break;
      }
      if (this.#memtableBytes() > this.#memtableCap) await this.#relieve(0);
      // Level-0 runs past their stall bound make writers wait for merges (merges never wait on
      // writers, so this cannot deadlock).
      // The wait is on a real signal (the merge chain) with a task yield per round, and ends
      // when no merge is left that could reduce the count (review B3-p2b-2 B2).
      while (this.#mergeMode === "auto" && this.#levelZeroMax() >= this.#stallRuns) {
        if (this.#nextMerge() === null) break;
        this.#scheduleMerge();
        await this.#merges;
        await yieldTask();
        this.#check();
      }
    } catch (error) {
      this.#claimed = false;
      this.#passSlot();
      throw error;
    }
    this.#claimed = false;
    const tx = new TransactionImpl(this, this.appliedLsn);
    this.#holder = tx;
    return tx;
  }

  /** Waits (FIFO) until the slot is free; the caller takes it before its next await. */
  /**
   * Takes the slot for a writer (`claimed`, until its transaction exists) or for maintenance:
   * at once when it is free and nobody waits, else in FIFO order. The hand-off claims the slot for
   * the next waiter in the same step that frees it, so nothing can take it between (a writer and a
   * recycle both holding it would deadlock: the recycle defers the writer's commit frames).
   */
  async #takeSlot(kind: "writer" | "maintenance", signal?: AbortSignal): Promise<void> {
    const take = () => {
      if (kind === "writer") this.#claimed = true;
      else this.#maintenanceHolds = true;
    };
    if (!this.#slotBusy && this.#queue.length === 0) {
      take();
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const waiter = { resolve, reject, take };
      this.#queue.push(waiter);
      signal?.addEventListener(
        "abort",
        () => {
          const index = this.#queue.indexOf(waiter);
          if (index >= 0) {
            this.#queue.splice(index, 1);
            const reason: unknown = signal.reason;
            reject(reason instanceof Error ? reason : new Error("Aborted", { cause: reason }));
          }
        },
        { once: true },
      );
    });
  }

  /** Frees the slot and hands it to the next waiter, claimed for it in the same step. */
  #passSlot(): void {
    const next = this.#queue.shift();
    if (next === undefined) return;
    next.take();
    next.resolve();
  }

  release(tx: TransactionImpl): void {
    if (this.#holder !== tx) return;
    // Staging a commit did not claim (a rollback, or a commit that failed before its frame).
    const staging = this.#txStaging.get(tx);
    if (staging !== undefined) {
      this.#txStaging.delete(tx);
      this.#dropStaging(staging);
    }
    this.#holder = null;
    this.#passSlot();
    // A frozen memtable may have waited for this writer's pending rows.
    if (this.#hasFrozen()) this.#scheduleFlush();
  }

  /** A value that changes whenever a flush makes progress (see the flush loop's invariant). */
  #flushProgress(): string {
    return `${String(this.publications)}:${String(this.maintenance.flushesAbandoned)}:${String(this.durableLsn)}`;
  }

  /**
   * Freezes the live memtable of every table whose frozen memtables cannot flush because their
   * last commit continues into it (its rows are all committed). True when it froze one.
   */
  #freezeBlockingTails(): boolean {
    let froze = false;
    for (const state of this.#tables.values()) {
      const memtables = state.view.memtables;
      const last = memtables[memtables.length - 1];
      const previous = memtables[memtables.length - 2];
      if (
        last?.state !== "live" ||
        previous === undefined ||
        !awaitsFlush(previous) ||
        last.rowCount === 0 ||
        last.visibleRows(MAX_LSN) < last.rowCount ||
        minSeq(last) > maxSeq(previous, previous.rowCount)
      )
        continue;
      last.state = "frozen";
      last.measure();
      this.#setView(state, { ...state.view });
      froze = true;
    }
    return froze;
  }

  #hasFrozen(): boolean {
    for (const state of this.#tables.values())
      for (const memtable of state.view.memtables) if (awaitsFlush(memtable)) return true;
    return false;
  }

  commit(tx: TransactionImpl, request: CommitRequest | null): Promise<CommitReceipt> {
    const staging = this.#txStaging.get(tx);
    if (staging === undefined) return this.#commitNow(tx, request, () => null);
    // A large transaction: its staging is made exact first, then its frame names that root.
    return this.#restage(tx, staging).then(
      (names) => this.#commitNow(tx, request, (tableId) => names.get(tableId) ?? null),
      (error: unknown) => {
        tx.undoTo(0);
        this.release(tx);
        throw error;
      },
    );
  }

  #commitNow(
    tx: TransactionImpl,
    request: CommitRequest | null,
    staged: (tableId: number) => PublishedRows | null,
  ): Promise<CommitReceipt> {
    try {
      this.#check();
      const effects = tx.effects(this.#columns, staged);
      if (effects.length === 0) {
        this.release(tx);
        this.#afterAppend(); // sequence frames may have grown the log meanwhile
        return Promise.resolve({ wrote: false, lsn: tx.baseLsn, durable: Promise.resolve() });
      }
      const lsn = this.#appender.append(encodeBody({ request, effects }, this.#body));
      tx.restamp(lsn);
      // Claimed by the frame. A staged commit spans memtables, and a commit flushes whole: its
      // live tail (committed now) freezes, so the fold can run at once.
      if (this.#txStaging.delete(tx))
        for (const state of this.#tables.values()) {
          const last = state.view.memtables[state.view.memtables.length - 1];
          if (
            last?.state === "live" &&
            last.rowCount > 0 &&
            last.visibleRows(MAX_LSN) === last.rowCount &&
            state.view.memtables.some((memtable) => memtable.staged !== null)
          ) {
            last.state = "frozen";
            last.measure();
            this.#setView(state, { ...state.view });
          }
        }
      if (tx.catalog !== this.#catalog) {
        this.#catalog = tx.catalog;
        this.#catalogHistory.push({ lsn, catalog: tx.catalog });
        for (const effect of effects) {
          if (effect.kind === "createTable") {
            const state = this.#tables.get(effect.tableId);
            if (state !== undefined) {
              state.createdLsn = lsn;
              state.flushedThroughLsn = lsn - 1;
            }
          }
          if (effect.kind === "dropTable") {
            // Strict readers pin the durable LSN: keep the state until the drop is durable.
            const tableId = effect.tableId;
            const wait = this.durability === "strict" ? this.#appender.durable(lsn) : null;
            if (wait === null) this.#dropTableState(tableId);
            else
              void wait.then(
                () => {
                  if (this.#catalog.table(tableId) === undefined) this.#dropTableState(tableId);
                },
                () => undefined,
              );
          }
          if (effect.kind === "catalog") {
            const change = decodeLogged(effect.definition);
            if (change.op === "dropSequence") this.sequences.drop(change.sequenceId);
            if (change.op === "createIndex")
              this.#pendingIndexes.set(change.index.id, { tableId: change.index.tableId, lsn });
            if (change.op === "dropIndex") this.#pendingIndexes.delete(change.indexId);
          }
        }
      }
      this.#afterAppend();
      this.#lastCommitAt = performance.now();
      const receipt = new Receipt(lsn, this.#appender);
      if (request !== null) {
        const record = () => {
          this.ledger.record(hexId(request.clientId), request.sequence, {
            kind: "committed",
            lsn,
            counts: request.counts,
            recordedAtMs: this.#now(),
          });
        };
        if (this.durability === "strict") void receipt.durable.then(record, () => undefined);
        else record();
      }
      this.release(tx);
      return Promise.resolve(receipt);
    } catch (error) {
      tx.undoTo(0);
      this.release(tx);
      return Promise.reject(error instanceof Error ? error : new Error(String(error)));
    }
  }

  durable(lsn: number): Promise<void> | null {
    return this.#appender.durable(lsn);
  }

  /** Waits until everything appended is durable and no flush can run (tests, benchmarks, close). */
  async quiesce(): Promise<void> {
    for (;;) {
      await this.#recycling;
      await this.#stagingDrops;
      await this.#appender.flush();
      await this.#flushes;
      await this.#merges;
      this.#check();
      const merging = this.#mergeMode === "auto" && this.#nextMerge() !== null;
      if (
        this.#nextFlushReady() === null &&
        !merging &&
        this.#appender.durableLsn >= this.#appender.lastLsn
      )
        return;
      this.#scheduleFlush();
      this.#scheduleMerge();
    }
  }

  /** Whether a flush could run now, ignoring durability waits. */
  #nextFlushReady(): true | null {
    for (const state of this.#tables.values()) {
      if (this.#catalog.table(state.tableId) === undefined) continue;
      const memtables = state.view.memtables;
      let through = 0;
      for (let k = 0; k < memtables.length; k++) {
        const memtable = need(memtables[k]);
        if (!awaitsFlush(memtable) || memtable.visibleRows(MAX_LSN) < memtable.rowCount) break;
        through = Math.max(through, maxSeq(memtable, memtable.rowCount));
        const next = memtables[k + 1];
        if (next === undefined || minSeq(next) > through) {
          if (this.#flushable(state, Math.max(through, state.flushedThroughLsn))) return true;
          break;
        }
      }
    }
    return null;
  }

  flush(): Promise<void> {
    return this.#appender.flush();
  }

  statistics(tableId: number): TableSummary | null {
    const def = this.#catalog.table(tableId);
    const state = this.#tables.get(tableId);
    if (def === undefined || state === undefined) return null;
    // Only groups with deletions above the bitmaps are visited (not every group).
    let rowCount = state.view.groups.liveRowCount;
    const changed = new Set<number>(state.view.groupDeletes.byGroup.keys());
    for (const groupId of state.view.groupSources.keys()) changed.add(groupId);
    for (const groupId of changed) rowCount -= groupDeletions(state.view, groupId, MAX_LSN).length;
    for (const memtable of state.view.memtables)
      rowCount += memtable.rowCount - memtable.deletions.size;
    const totals = this.#summaries.get(tableId);
    const columns = new Map(def.columns.map((column) => [column.id, column]));
    return {
      rowCount: Math.max(0, rowCount),
      column: (columnId): ColumnSummary | null => {
        const column = columns.get(columnId);
        const total = totals?.get(columnId);
        if (column === undefined) return null;
        const fixed = fixedWidth(column.type);
        if (total === undefined)
          return { nullCount: 0, distinct: null, min: null, max: null, averageWidth: fixed ?? 16 };
        return {
          nullCount: total.nullCount,
          distinct: null,
          min: total.unbounded ? null : total.min,
          max: total.unbounded ? null : total.max,
          averageWidth: fixed ?? (total.values === 0 ? 16 : total.bytes / total.values),
        };
      },
    };
  }

  stats(): WritePathStats {
    let memtableBytes = 0;
    let frozen = 0;
    let runs = 0;
    for (const state of this.#tables.values()) {
      for (const memtable of state.view.memtables) {
        memtableBytes += memtable.bytes;
        if (awaitsFlush(memtable)) frozen += 1;
      }
      for (const list of state.view.runs.values()) runs += levelZero(list);
    }
    const space = this.#space.stats;
    return {
      appliedLsn: this.appliedLsn,
      durableLsn: this.durableLsn,
      logBytes: this.#appender.end,
      memtableBytes,
      frozenMemtables: frozen,
      frozenMemtableBytes: this.#frozenBytes(),
      borrowedChunkBytes: [...this.#published].reduce(
        (sum, memtable) => sum + memtable.borrowedBytes,
        0,
      ),
      repointedReaders: this.shared.readers.heldPins > 0 ? this.shared.readers.count : 0,
      repointPins: this.shared.readers.heldPins,
      levelZeroRuns: runs,
      garbageBytes: space.waitingBytes,
      pins: space.pinCount,
      oldestPin: space.oldestPin,
      backpressure:
        this.#frozenBytes() > FROZEN_MEMTABLE_CAP_BYTES ||
        (this.#mergeMode === "auto" && this.#levelZeroMax() >= this.#stallRuns),
    };
  }

  /**
   * The write path's own resident bytes: memtables and the resident parts of the index runs it
   * has opened (fences, block refs, Bloom filters). Pages and run blocks live in the space's cache,
   * which the governor accounts for apart.
   */
  residentBytes(): number {
    let total = 0;
    for (const state of this.#tables.values())
      for (const memtable of state.view.memtables) total += memtable.bytes;
    for (const reader of this.#runReaders.values())
      if (!(reader instanceof Promise)) total += reader.bytes;
    return total;
  }

  #frozenBytes(): number {
    let total = 0;
    for (const state of this.#tables.values())
      for (const memtable of state.view.memtables)
        if (awaitsFlush(memtable)) total += memtable.bytes;
    return total;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#idleTimer !== null) clearTimeout(this.#idleTimer);
    this.#idleTimer = null;
    // Staging owns memtables until its writer has settled. Rollback may release them.
    await Promise.allSettled(this.#stagingRuns);
    this.#holder?.rollback();
    for (const waiter of this.#queue.splice(0)) waiter.reject(new WritePathClosedError());
    await this.#recycling;
    await this.#flushes;
    await this.#stagingDrops;
    // A running merge stops at its next slice and aborts its writer.
    await this.#merges;
    // An idle publication already started finishes (or, not yet publishing, sees close and
    // stops) before anything is released: nothing publishes after close resolves.
    await this.#idleRun;
    if (this.#appender.failure === null) await this.#appender.close();
    this.keys.close();
    for (const state of this.#tables.values())
      for (const memtable of state.view.memtables) memtable.releaseAll();
    for (const memtable of this.#published) memtable.releaseAll();
    this.#currentPin?.release();
    this.#currentPin = null;
    for (const cache of this.#metadataCaches) cache.clear();
    this.#metadataGrant.close();
    this.#grant.close();
  }

  // ── Shared caches ────────────────────────────────────────────────────────────────────────

  readonly #statePins = new Map<WriteState, { pin: SpacePin; count: number }>();

  #pinState(state: WriteState, label: string): SpacePin {
    let held = this.#statePins.get(state);
    if (held === undefined) {
      held = { pin: this.#space.pin(label), count: 0 };
      this.#statePins.set(state, held);
      this.#retainState(state);
    }
    held.count += 1;
    return new StatePin(held, label, this.#statePins, state, (released) => {
      this.#releaseState(released);
    });
  }

  #runReader(ref: RunRef): RunReader | Promise<RunReader> {
    const known = this.#runsByRef.get(ref);
    if (known !== undefined) return known;
    const key = blobKey(ref.entries);
    const cached = this.#runReaders.get(key);
    if (cached !== undefined) {
      if (!(cached instanceof Promise)) this.#runsByRef.set(ref, cached);
      return cached;
    }
    // Its resident parts are charged to the metadata grant before they are built (runs.ts).
    const opened = RunReader.open(this.#space, ref, this.#grant, this.#metadataGrant);
    if (opened instanceof Promise) {
      const pending = opened.then((reader) => {
        // A drop while it opened took the entry away: give the charge back.
        if (this.#runReaders.get(key) === pending) this.#runReaders.set(key, reader);
        else this.#chargeMetadata(-reader.bytes);
        return reader;
      });
      this.#runReaders.set(key, pending);
      pending.catch(() => this.#runReaders.delete(key));
      return pending;
    }
    this.#runReaders.set(key, opened);
    return opened;
  }

  #bitmap(ref: BlobRef): Uint32Array | Promise<Uint32Array> {
    const peeked = this.#space.peek(ref);
    const decode = (bytes: Uint8Array) => new Uint32Array(bytes.slice().buffer);
    if (peeked !== null) return decode(peeked);
    return readBlob(this.#space, ref, this.#grant).then(decode);
  }

  // ── Flush and publish ────────────────────────────────────────────────────────────────────

  #scheduleFlush(): void {
    if (this.#flushScheduled || this.#closed) return;
    this.#flushScheduled = true;
    this.#flushes = this.#flushes.then(async () => {
      this.#flushScheduled = false;
      for (;;) {
        if (this.failure !== null || this.#closed) return;
        const work = this.#nextFlush();
        if (work === null) {
          this.#armIdlePublish();
          return;
        }
        const before = this.#flushProgress();
        try {
          await work;
        } catch (error) {
          this.#failure ??= error instanceof Error ? error : new Error(String(error));
          return;
        }
        // Invariant: every flush the loop runs makes progress (a publication, an abandoned flush
        // of a dropped table, or durability for a wait); otherwise it stops rather than spin.
        if (this.#flushProgress() === before) return;
      }
    });
  }

  /** The next table flush that can run now (null when none), or a wait for durability. */
  #nextFlush(): Promise<void> | null {
    for (const state of this.#tables.values()) {
      if (this.#catalog.table(state.tableId) === undefined) continue;
      const memtables = state.view.memtables;
      const set: Memtable[] = [];
      let through = 0;
      for (let k = 0; k < memtables.length; k++) {
        const memtable = need(memtables[k]);
        if (!awaitsFlush(memtable)) break;
        const rows = memtable.rowCount;
        if (memtable.visibleRows(MAX_LSN) < rows) break; // pending rows
        through = Math.max(through, maxSeq(memtable, rows));
        set.push(memtable);
        const next = memtables[k + 1];
        if (next === undefined || minSeq(next) > through) {
          if (through > this.durableLsn) {
            const wait = this.#appender.durable(through);
            if (wait === null) continue;
            return wait;
          }
          const to = Math.max(through, state.flushedThroughLsn);
          if (!this.#flushable(state, to)) {
            this.maintenance.flushesHeld += 1;
            // Waits for an index build elsewhere; that table's flush settles it.
            if (this.#freezeBuilds()) return this.#nextFlush();
            break;
          }
          return this.#flushTable(state, set, to);
        }
      }
    }
    return null;
  }

  #flushTable(state: TableState, set: readonly Memtable[], through: number): Promise<void> {
    const flush = this.#writeFlush(state, set, through);
    this.#flushing.set(state, flush);
    const done = () => {
      if (this.#flushing.get(state) === flush) this.#flushing.delete(state);
    };
    void flush.then(done, done);
    return flush;
  }

  async #writeFlush(state: TableState, set: readonly Memtable[], through: number): Promise<void> {
    const def = this.#catalog.table(state.tableId);
    if (def === undefined || !this.#flushable(state, through)) return;
    const writer = this.#space.beginWrite(`flush t${String(state.tableId)}`);
    const retired: ExtentRef[] = [];
    // Directory chunks and the root this publication made: it owns them until a state holds them.
    let created: GroupDirectory | null = null;
    let madeRoot: ManifestRoot | null = null;
    let unlock: (() => void) | null = null;
    const delta = new DeltaPublication(
      "flush",
      this.#extentDeltas,
      this.#nextAttempt,
      this.#afterDeltaPrepared,
      this.#sinkFailed,
    );
    this.#flushesWriting += 1;
    let writing = true;
    try {
      const view = state.view;
      const newGroups: GroupRef[] = [];
      const published = new Map<
        Memtable,
        { group: GroupRef; root: GroupRoot; runs: Map<number, RunRef> }
      >();
      let nextGroupId = state.nextGroupId;
      /** Runs of memtables without rows (a recycle flushes their tombstones). */
      const rowless = new Map<Memtable, Map<number, RunRef>>();
      /** Staging ids whose staged memtables this flush folds into the table's groups. */
      const foldedStagings = new Set<number>();
      for (const memtable of set) {
        if (memtable.rowCount === 0) {
          const runs = new Map<number, RunRef>();
          for (const index of def.indexes) {
            const delta = memtable.deltas.get(index.id);
            if (delta === undefined || delta.entryCount === 0) continue;
            const run = encodeRun(delta.sorted(through));
            if (run === null) continue;
            runs.set(index.id, runRef(run, (await writer.write(run.parts)).blobs));
          }
          if (runs.size > 0) rowless.set(memtable, runs);
          this.maintenance.rowlessRuns += runs.size;
          continue;
        }
        const part = memtable.staged;
        if (part !== null) {
          // A committed large transaction's staged rows: the group and its runs are written
          // already; deletions since staging are folded into its bitmap, and the other entries
          // the memtable kept (tombstones of older rows) become a run of their own.
          foldedStagings.add(part.stagingId);
          const base =
            part.group.deletions === null ? null : await this.#bitmap(part.group.deletions);
          const isSet = (position: number) =>
            base !== null && ((base[position >>> 5] ?? 0) & (1 << (position & 31))) !== 0;
          const deleted = memtable
            .deletedUnder(through, memtable.rowCount)
            .filter((position) => !isSet(position));
          let deletions = part.group.deletions;
          if (deleted.length > 0) {
            deletions = await this.#writeBitmap(writer, memtable.storedRows, base, deleted);
            if (part.group.deletions !== null) retired.push(part.group.deletions.extent);
          }
          const group: GroupRef = {
            ...part.group,
            groupId: nextGroupId++,
            liveRowCount: part.group.liveRowCount - deleted.length,
            deletions,
          };
          const runs = new Map<number, RunRef>();
          for (const index of def.indexes) {
            const delta = memtable.deltas.get(index.id);
            if (delta === undefined) continue;
            const run = encodeRun(delta.sorted(through));
            if (run === null) continue;
            runs.set(index.id, runRef(run, (await writer.write(run.parts)).blobs));
          }
          // Staged runs of an index dropped since staging do not fold: they are retired here.
          const indexIds = new Set(def.indexes.map((index) => index.id));
          for (const [indexId, list] of part.runs)
            if (!indexIds.has(indexId)) retired.push(...list.map(runExtent));
          newGroups.push(group);
          published.set(memtable, { group, root: await groupRootOf(part.groupRoot), runs });
          this.maintenance.stagedFolds += 1;
          continue;
        }
        const groupRoot = await this.#writeGroup(writer, def, memtable);
        const rootBlob = need((await writer.write([encodeGroupRoot(groupRoot)])).blobs[0]);
        const deleted = memtable.deletedUnder(through, memtable.rowCount);
        let deletions: BlobRef | null = null;
        if (deleted.length > 0)
          deletions = await this.#writeBitmap(writer, memtable.rowCount, null, deleted);
        const group: GroupRef = {
          groupId: nextGroupId++,
          firstRowId: memtable.firstRowId,
          lastRowId: memtable.firstRowId + memtable.rowCount - 1,
          rowCount: memtable.rowCount,
          liveRowCount: memtable.rowCount - deleted.length,
          root: rootBlob,
          deletions,
        };
        const runs = new Map<number, RunRef>();
        for (const index of def.indexes) {
          const delta = memtable.deltas.get(index.id);
          if (delta === undefined) continue;
          const run = encodeRun(delta.sorted(through));
          if (run === null) continue;
          const written = await writer.write(run.parts);
          runs.set(index.id, runRef(run, written.blobs));
        }
        newGroups.push(group);
        published.set(memtable, { group, root: groupRoot, runs });
      }
      // Fold deletions (≤ through) of older groups into their bitmaps: only groups with pending
      // deletions are visited (review B3-p2a-2 #5), and the first changed one bounds page reuse.
      const before = view.groups;
      const changes = new Map<number, GroupRef>();
      let firstChanged = before.count + newGroups.length;
      const candidates = new Set<number>(view.groupDeletes.byGroup.keys());
      for (const groupId of view.groupSources.keys()) candidates.add(groupId);
      for (const groupId of [...candidates].sort((x, y) => x - y)) {
        const at = before.indexOfGroupId(groupId);
        if (at < 0) continue;
        const folded = groupDeletions(view, groupId, through);
        if (folded.length === 0) continue;
        const group = before.get(at);
        const old = group.deletions === null ? null : await this.#bitmap(group.deletions);
        const deletions = await this.#writeBitmap(writer, group.rowCount, old, folded);
        if (group.deletions !== null) retired.push(group.deletions.extent);
        changes.set(at, { ...group, deletions, liveRowCount: group.liveRowCount - folded.length });
        firstChanged = Math.min(firstChanged, at);
      }
      firstChanged = Math.min(firstChanged, before.count);
      // The next generation's directory: unchanged chunks shared, changed ones and the tail
      // copied, each charged before it is allocated (review B3-p2a-3 #1, #2, #6). This publication
      // owns the new chunks' creation references until a state holds them (or it fails).
      const groups = before.with(changes, newGroups, this.#metadataGrant);
      created = groups;
      const liveRowCount = groups.liveRowCount;
      const { directory, pages } = await this.#writeDirectory(
        writer,
        state,
        groups,
        firstChanged,
        retired,
      );
      // The table's planning totals with the new groups, stored with the root (`statistics`), so
      // an open never reads group roots for them. Folded deletions do not change them (they
      // cover stored rows).
      const totals = cloneTotals(this.#summaries.get(state.tableId));
      for (const entry of published.values()) addGroup(totals, entry.root);
      let statistics: BlobRef | null = state.root?.statistics ?? null;
      let statisticsCharge = 0;
      if (newGroups.length > 0 || (statistics === null && groups.count > 0)) {
        const bytes = encodeTableStatistics(tableStatistics(groups.count, totals));
        statisticsCharge = parseCharge(bytes.length);
        const old = statistics;
        statistics = need((await writer.write([bytes])).blobs[0]);
        if (old !== null) retired.push(old.extent);
      } else if (statistics !== null && state.root !== null)
        statisticsCharge = state.metadataBytes - pageIndexBytes(state.pages.length);
      // From here to the install, the root is built from the current state: a merge that
      // published while this flush wrote its groups has replaced runs (`state.view.runs`, not the
      // view this flush started from).
      if (this.#beforeFlushPublish !== null) await this.#beforeFlushPublish();
      unlock = await this.#lock();
      this.#flushesWriting -= 1;
      writing = false;
      // A table dropped while this flush wrote its groups publishes nothing: the next root
      // retires everything the dropped table reached, and these groups would be reachable from
      // no root (found by the R25 delta test: extents retired twice). Its memtables go with the
      // drop.
      if (this.#tables.get(state.tableId) !== state) {
        await writer.abort();
        this.maintenance.flushesAbandoned += 1;
        return;
      }
      if (this.#merging) this.maintenance.flushesDuringMerge += 1;
      const runs = new Map<number, readonly RunRef[]>();
      for (const index of def.indexes) {
        // One run per memtable that had entries, in memtable order (oldest first).
        const fresh = set.flatMap((memtable) => {
          const own =
            published.get(memtable)?.runs.get(index.id) ?? rowless.get(memtable)?.get(index.id);
          // A folded memtable's staged puts are older than the entries it kept. Only a memtable
          // this flush folds (a dropped staged memtable's runs are retired, not the table's).
          const staged =
            memtable.staged !== null && published.has(memtable)
              ? (memtable.staged.runs.get(index.id) ?? [])
              : [];
          return own === undefined ? [...staged] : [...staged, own];
        });
        runs.set(index.id, [...fresh.reverse(), ...(state.view.runs.get(index.id) ?? [])]);
      }
      const nextRowId = Math.max(
        state.nextRowId,
        state.root?.nextRowId ?? 0,
        ...newGroups.map((group) => group.lastRowId + 1),
      );
      // Other transactions' staging stays listed; a folded one leaves with its directory.
      for (const id of foldedStagings) {
        const staging = state.stagings.get(id);
        if (
          staging === undefined ||
          staging.memtables.some((memtable) => memtable.rowCount > 0 && !set.includes(memtable))
        )
          throw new TypeError("A commit's staged rows must fold in one flush");
        retired.push(...staging.directory);
      }
      const tableRoot: TableRoot = {
        tableId: state.tableId,
        schemaVersion: def.schemaVersion,
        nextRowId,
        flushedThroughLsn: through,
        liveRowCount,
        groups: directory,
        groupCount: groups.count,
        staged: (state.root?.staged ?? []).filter(
          (staged) => !foldedStagings.has(staged.transaction),
        ),
        keyIndex: indexRoot(def, def.primaryKey, runs),
        secondaryIndexes: def.indexes
          .filter((index) => index.id !== def.primaryKey)
          .map((index) => indexRoot(def, index.id, runs))
          .filter((index): index is IndexRoot => index !== null),
        statistics,
      };
      const root = await this.#nextRoot(writer, retired, new Map([[state.tableId, tableRoot]]));
      await delta.prepare(root.sequence, writer, retired);
      const rootBytes = encodeManifestRoot(root);
      // The resident state this publish adds (page index growth, the new root) is charged first.
      // The pending publication owns both charges across the await (review B3-p2a-3 #3): they
      // pass to the table state and the new write state when it installs, or are refunded.
      const metadataBytes = pageIndexBytes(pages.length) + statisticsCharge;
      const growth = metadataBytes - state.metadataBytes;
      const rootCharge = parseCharge(rootBytes.length);
      this.#chargeMetadata(growth);
      try {
        this.#chargeMetadata(rootCharge);
      } catch (error) {
        this.#chargeMetadata(-growth);
        throw error;
      }
      this.#roots.set(root, { refs: 1, bytes: rootCharge });
      madeRoot = root;
      try {
        await delta.publish(this.#space, rootBytes, writer, retired);
      } catch (error) {
        this.#chargeMetadata(-growth);
        throw error;
      }
      // A table dropped while the publish was in flight never holds the new page index: its own
      // charge went (or goes) with the drop, so the growth is refunded here.
      this.maintenance.flushes += 1;
      this.maintenance.flushBytes += writer.bytes;
      for (const runs of [
        ...[...published.values()].map((entry) => entry.runs),
        ...rowless.values(),
      ])
        for (const run of runs.values()) {
          this.maintenance.flushRunBytes += runExtent(run).length;
          this.maintenance.flushRunEntries += run.entryCount;
        }
      const live = this.#tables.get(state.tableId) === state;
      if (live) {
        state.metadataBytes = metadataBytes;
        this.#summaries.set(state.tableId, totals);
      } else this.#chargeMetadata(-growth);
      // Re-point readers in the same step the publish resolves (ruling R16.3).
      const pin: SpacePin = this.#space.pin(`re-point t${String(state.tableId)}`);
      const flushed = [...set];
      // Older readers keep the published group (pin) and the memtables' key deltas (ruling 2).
      for (const memtable of flushed) this.#published.add(memtable);
      // Folded deletions stay for readers older than this publication, then go (with the pin).
      const folded = state.view.groupDeletes;
      this.shared.readers.holdUntilOlderClose(() => {
        folded.prune(through);
        pin.release();
        for (const memtable of flushed) {
          memtable.releaseAll();
          this.#published.delete(memtable);
        }
      });
      for (const memtable of set) {
        const entry = published.get(memtable);
        if (entry !== undefined)
          memtable.published = { group: entry.group, groupRoot: entry.root, runs: entry.runs, pin };
        memtable.state = "published";
        memtable.release();
      }
      this.#root = root;
      this.#pruneCatalogs(root.globalLsn);
      this.#adoptTableRoots(root);
      state.root = tableRoot;
      state.pages = pages;
      state.flushedThroughLsn = through;
      for (const [indexId, pending] of this.#pendingIndexes)
        if (pending.tableId === state.tableId && pending.lsn <= through)
          this.#pendingIndexes.delete(indexId);
      state.nextGroupId = nextGroupId;
      state.nextRowId = nextRowId;
      for (const id of foldedStagings) state.stagings.delete(id);
      // Rebuilt from the view as it is now: memtables, deletions and sources added during the
      // flush stay (the flush only removes what it published).
      const current = state.view;
      const sources = new Map(current.groupSources);
      // A source whose deletions are all folded now has nothing left to say (later deletions of
      // its rows go to groupDeletes): drop it, so a flush visits only groups that changed.
      for (const [groupId, source] of sources) {
        let pending = false;
        for (const seq of source.deletions.values()) if (seq > through) pending = true;
        if (!pending) sources.delete(groupId);
      }
      for (const [memtable, entry] of published) sources.set(entry.group.groupId, memtable);

      const nextView: TableView = {
        tableId: state.tableId,
        groups,
        runs,
        groupDeletes: current.groupDeletes,
        foldedThrough: through,
        groupSources: sources,
        memtables: current.memtables.filter((memtable) => !set.includes(memtable)),
      };
      // The new state retains the root and (for a live table) the directory's chunks.
      if (live) this.#setView(state, nextView);
      else {
        state.view = nextView;
        this.#publishState();
      }
      this.publications += 1;
    } catch (error) {
      if (writer.state === "open") await writer.abort().catch(() => undefined);
      throw error;
    } finally {
      delta.settle();
      created?.releaseCreation();
      if (madeRoot !== null) this.#releaseRoot(madeRoot);
      unlock?.();
      if (writing) this.#flushesWriting -= 1;
    }
    this.#scheduleMerge();
  }

  // ── Staged large transactions (format/replay.ts "Large transactions") ─────────────────────

  /** Staging ids: unique per open write path, above every id a root at open listed. */
  #nextStagingId = 1;
  /** The open transaction's staging id, once it staged anything. */
  readonly #txStaging = new WeakMap<TransactionImpl, number>();
  /** Staging an open dropped (no frame claimed it), whose extents the first root walks. */
  #unclaimedWalks: TableRoot[] = [];
  /** Extents of dropped staging the durable root still reaches: retired by the next root. */
  #pendingRetire: ExtentRef[] = [];
  /** Rollbacks' staging drops, each under the publish lock (close and quiesce wait for them). */
  #stagingDrops: Promise<void> = Promise.resolve();

  /**
   * Frozen memtables holding only the open transaction's rows that can be staged now, per table.
   * The transaction must not have changed the table's catalog entry, a root must be able to carry
   * the table now (`#flushable`), and every earlier memtable with the transaction's rows must be
   * staged already, except a first one that also holds committed rows: so the commit's effects
   * stay in row-id order (rows, then one `publishedRows`, then rows).
   */
  #stageCandidates(): Map<TableState, Memtable[]> | null {
    const tx = this.#holder;
    if (tx === null || this.#recovering || this.#closed) return null;
    let out: Map<TableState, Memtable[]> | null = null;
    for (const state of this.#tables.values()) {
      const committed = this.#catalog.table(state.tableId);
      const now = tx.catalog.table(state.tableId);
      if (
        committed === undefined ||
        now?.schemaVersion !== committed.schemaVersion ||
        now.indexes.length !== committed.indexes.length ||
        now.indexes.some((index, k) => index.id !== committed.indexes[k]?.id)
      )
        continue;
      let list: Memtable[] | null = null;
      let staged = false;
      for (const memtable of state.view.memtables) {
        if (memtable.staged !== null) {
          staged = true;
          continue;
        }
        const committedRows = memtable.visibleRows(MAX_LSN);
        if (committedRows === memtable.rowCount) continue;
        if (committedRows > 0 && !staged) continue; // the first memtable, holding committed rows
        if (memtable.state !== "frozen" || committedRows > 0 || !this.#stageable(tx, memtable))
          break;
        (list ??= []).push(memtable);
        staged = true;
      }
      if (list !== null && this.#flushable(state, state.flushedThroughLsn))
        (out ??= new Map()).set(state, list);
    }
    return out;
  }

  /**
   * Whether each undo entry into `memtable`'s deltas is all own puts (the staged run takes them)
   * or all other entries (they stay resident): staging remaps entries, never splits one.
   */
  #stageable(tx: TransactionImpl, memtable: Memtable): boolean {
    if (memtable.rowCount === 0) return false;
    const first = memtable.firstRowId;
    const end = first + memtable.rowCount;
    const deltas = new Set(memtable.deltas.values());
    for (const entry of tx.undo) {
      if (entry.kind !== "delta" || !deltas.has(entry.delta)) continue;
      const delta = entry.delta;
      const own = (at: number) =>
        !delta.isTombstone(at) && delta.rowId(at) >= first && delta.rowId(at) < end;
      const kind = own(entry.entry);
      for (let at = entry.entry + 1; at < entry.entry + entry.count; at++)
        if (own(at) !== kind) return false;
    }
    return true;
  }

  /** Stages `candidates` (from `admit`, the writer's own call): one publication. */
  #stagingRuns = new Set<Promise<unknown>>();

  #trackStaging<T>(run: Promise<T>): Promise<T> {
    this.#stagingRuns.add(run);
    void run.finally(() => this.#stagingRuns.delete(run)).catch(() => undefined);
    return run;
  }

  #stage(candidates: ReadonlyMap<TableState, readonly Memtable[]>): Promise<void> {
    return this.#trackStaging(this.#stageNow(candidates));
  }

  async #stageNow(candidates: ReadonlyMap<TableState, readonly Memtable[]>): Promise<void> {
    const tx = need(this.#holder);
    const unlock = await this.#lock();
    try {
      this.#check();
      await this.#publishStaging(tx, candidates, false);
      this.#check();
    } finally {
      unlock();
    }
  }

  /**
   * At commit: brings every table's staging of `tx` up to date (memtables a rollback emptied
   * dropped, truncated ones' tails marked deleted, their runs rewritten without them) and returns
   * what each table's `publishedRows` names. The rows' only durable form is then exactly the
   * committed rows.
   */
  #restage(tx: TransactionImpl, id: number): Promise<Map<number, PublishedRows>> {
    return this.#trackStaging(this.#restageNow(tx, id));
  }

  async #restageNow(tx: TransactionImpl, id: number): Promise<Map<number, PublishedRows>> {
    const unlock = await this.#lock();
    try {
      this.#check();
      await this.#publishStaging(tx, new Map(), true);
      this.#check();
    } finally {
      unlock();
    }
    const out = new Map<number, PublishedRows>();
    for (const state of this.#tables.values()) {
      const staging = state.stagings.get(id);
      if (staging?.entry == null) continue;
      out.set(state.tableId, {
        transaction: id,
        rootSequence: staging.rootSequence,
        indexIds: staging.entry.indexRuns.map((runs) => runs.indexId),
      });
    }
    return out;
  }

  /**
   * One root with `tx`'s staging brought up to date in each table it touches: `adds` written as
   * staged groups and runs; memtables a rollback emptied or removed dropped (their extents
   * retired); with `final`, truncated memtables' tails marked deleted. The caller holds the lock.
   */
  async #publishStaging(
    tx: TransactionImpl,
    adds: ReadonlyMap<TableState, readonly Memtable[]>,
    final: boolean,
  ): Promise<void> {
    let id = this.#txStaging.get(tx);
    const tables = new Set<TableState>(adds.keys());
    if (id !== undefined)
      for (const state of this.#tables.values()) if (state.stagings.has(id)) tables.add(state);
    if (tables.size === 0) return;
    id ??= this.#nextStagingId++;
    const writer = this.#space.beginWrite("stage");
    const write = writer.write.bind(writer);
    writer.write = async (...args) => {
      this.#check();
      const result = await write(...args);
      this.#check();
      return result;
    };
    const delta = new DeltaPublication(
      "stage",
      this.#extentDeltas,
      this.#nextAttempt,
      this.#afterDeltaPrepared,
      this.#sinkFailed,
    );
    const retired: ExtentRef[] = [];
    const changed = new Map<number, TableRoot>();
    const installs: Array<() => void> = [];
    const remaps = new Map<KeyDelta, { delta: KeyDelta; entries: Int32Array }>();
    // What the new parts hold before they are installed (review B3-p2c-1 #1): a failure at any
    // step, writing any part, gives every pin and residual delta back.
    const owned: Provisional = { pins: [], deltas: [] };
    let installed = false;
    let madeRoot: ManifestRoot | null = null;
    try {
      for (const state of tables) {
        if (this.#tables.get(state.tableId) !== state) continue;
        const def = this.#catalog.table(state.tableId);
        if (def === undefined) continue;
        const staging: Staging = state.stagings.get(id) ?? {
          id,
          memtables: [],
          entry: null,
          directory: [],
          rootSequence: 0,
        };
        const kept: Memtable[] = [];
        const dropped: Memtable[] = [];
        for (const memtable of staging.memtables)
          (memtable.rowCount > 0 && state.view.memtables.includes(memtable) ? kept : dropped).push(
            memtable,
          );
        const parts = new Map<Memtable, StagedPart>();
        const residuals = new Map<Memtable, Map<number, KeyDelta>>();
        for (const memtable of dropped) retired.push(...need(memtable.staged).extents);
        // Tails a rollback truncated: deleted in the group's bitmap, gone from its runs.
        let rewrote = false;
        if (final)
          for (const memtable of kept) {
            const part = need(memtable.staged);
            if (part.group.liveRowCount === memtable.rowCount) continue;
            parts.set(memtable, await this.#truncateStaged(writer, memtable, part, retired));
            this.#check();
            rewrote = true;
            this.maintenance.stagedTruncations += 1;
          }
        for (const [part, memtable] of (adds.get(state) ?? []).entries()) {
          const written = await this.#writeStaged(writer, def, memtable, id, owned, part);
          this.#check();
          parts.set(memtable, written.part);
          residuals.set(memtable, written.residual);
          for (const [old, map] of written.remaps) remaps.set(old, map);
        }
        if (dropped.length === 0 && !rewrote && (adds.get(state)?.length ?? 0) === 0) continue;
        const memtables = [...kept, ...(adds.get(state) ?? [])];
        const partOf = (memtable: Memtable) => parts.get(memtable) ?? need(memtable.staged);
        retired.push(...staging.directory);
        let entry: TableRoot["staged"][number] | null = null;
        let directory: ExtentRef[] = [];
        if (memtables.length > 0) {
          this.#stageFault?.("directory", -1);
          const refs = memtables.map((memtable, k) => ({
            ...partOf(memtable).group,
            groupId: k + 1,
          }));
          const written = await writeGroupDirectory(writer, refs);
          this.#check();
          directory = [written.ref.extent, ...written.pages.map((page) => page.ref.extent)];
          entry = {
            transaction: id,
            firstRowId: need(refs[0]).firstRowId,
            rowCount: refs.reduce((sum, ref) => sum + ref.rowCount, 0),
            groups: written.ref,
            indexRuns: def.indexes.map((index) => ({
              indexId: index.id,
              runs: memtables.flatMap((memtable) => partOf(memtable).runs.get(index.id) ?? []),
            })),
          };
        }
        const base = state.root ?? emptyTableRoot(def, state.nextRowId, state.flushedThroughLsn);
        const end = Math.max(0, ...memtables.map((memtable) => memtable.rowIdEnd));
        const others = base.staged.filter((staged) => staged.transaction !== id);
        const tableRoot: TableRoot = {
          ...base,
          nextRowId: Math.max(base.nextRowId, end),
          staged: entry === null ? others : [...others, entry],
        };
        changed.set(state.tableId, tableRoot);
        const stagingId = id;
        installs.push(() => {
          state.root = tableRoot;
          state.nextRowId = Math.max(state.nextRowId, end);
          for (const [memtable, part] of parts) {
            memtable.staged = part;
            memtable.published = {
              group: part.group,
              groupRoot: part.groupRoot,
              runs: new Map(),
              pin: part.pin,
            };
            const residual = residuals.get(memtable);
            if (residual !== undefined) {
              memtable.storedRows = memtable.rowCount;
              memtable.state = "staged";
              memtable.release();
              memtable.replaceDeltas(residual);
              for (const delta of residual.values())
                this.maintenance.stagedResidualEntries += delta.entryCount;
              this.maintenance.stagedRows += memtable.rowCount;
            }
          }
          this.maintenance.stagedMemtablesDropped += dropped.length;
          for (const memtable of dropped) {
            const gone = memtable;
            this.shared.readers.holdUntilOlderClose(() => gone.releaseAll());
          }
          if (entry === null) state.stagings.delete(stagingId);
          else
            state.stagings.set(stagingId, {
              ...staging,
              memtables,
              entry,
              directory,
              rootSequence: 0, // set below
            });
          // Dropped staged memtables (emptied by a rollback) leave the view: their extents are
          // retired by this root.
          const gone = new Set(dropped);
          this.#setView(state, {
            ...state.view,
            memtables: state.view.memtables.filter((memtable) => !gone.has(memtable)),
          });
        });
      }
      if (changed.size === 0) {
        await writer.abort();
        return;
      }
      const root = await this.#nextRoot(writer, retired, changed);
      this.#check();
      await delta.prepare(root.sequence, writer, retired);
      this.#check();
      const bytes = encodeManifestRoot(root);
      const charge = parseCharge(bytes.length);
      this.#chargeMetadata(charge);
      this.#roots.set(root, { refs: 1, bytes: charge });
      madeRoot = root;
      await delta.publish(this.#space, bytes, writer, retired);
      this.#root = root;
      this.#pruneCatalogs(root.globalLsn);
      this.#adoptTableRoots(root);
      for (const install of installs) install();
      installed = true; // the memtables own the pins and residual deltas now
      for (const state of tables) {
        const staging = state.stagings.get(id);
        if (staging !== undefined) staging.rootSequence = root.sequence;
      }
      tx.remapDeltas(remaps);
      this.#txStaging.set(tx, id);
      this.maintenance.stages += 1;
      this.publications += 1;
    } catch (error) {
      if (writer.state === "open") await writer.abort().catch(() => undefined);
      throw error;
    } finally {
      delta.settle();
      if (madeRoot !== null) this.#releaseRoot(madeRoot);
      if (!installed) {
        for (const pin of owned.pins) pin.release();
        for (const residual of owned.deltas) residual.releaseCharge();
      }
    }
  }

  /**
   * Writes one frozen memtable of pending rows as a staged group and runs of its own puts; the
   * other delta entries go to residual deltas (same order, so undo can be remapped). Its pin and
   * residual deltas are recorded in `owned` as they are made: provisional until installed.
   */
  async #writeStaged(
    writer: ExtentWriter,
    def: TableDef,
    memtable: Memtable,
    stagingId: number,
    owned: Provisional,
    index: number,
  ): Promise<{
    part: StagedPart;
    residual: Map<number, KeyDelta>;
    remaps: Map<KeyDelta, { delta: KeyDelta; entries: Int32Array }>;
  }> {
    const from = writer.extents.length;
    this.#stageFault?.("group", index);
    const groupRoot = await this.#writeGroup(writer, def, memtable);
    this.#check();
    const rootBlob = need((await writer.write([encodeGroupRoot(groupRoot)])).blobs[0]);
    this.#check();
    const first = memtable.firstRowId;
    const end = first + memtable.rowCount;
    const runs = new Map<number, RunRef[]>();
    const residual = new Map<number, KeyDelta>();
    const remaps = new Map<KeyDelta, { delta: KeyDelta; entries: Int32Array }>();
    this.#stageFault?.("runs", index);
    for (const [indexId, old] of memtable.deltas) {
      const own = old
        .sorted(Infinity)
        .map(({ key, entries }) => ({
          key,
          entries: entries.filter(
            (entry) => !entry.tombstone && entry.rowId >= first && entry.rowId < end,
          ),
        }))
        .filter((key) => key.entries.length > 0);
      const run = encodeRun(own);
      if (run !== null) {
        const written = await writer.write(run.parts);
        this.#check();
        runs.set(indexId, [runRef(run, written.blobs)]);
      }
      const next = memtable.newDelta();
      owned.deltas.push(next);
      const entries = new Int32Array(old.entryCount).fill(-1);
      const arena = old.keyArena();
      for (let at = 0; at < old.entryCount; at++) {
        const rowId = old.rowId(at);
        if (!old.isTombstone(at) && rowId >= first && rowId < end) continue;
        const slot = old.keyOf(at);
        entries[at] = next.add(
          arena,
          old.keyStart(slot),
          old.keyEnd(slot),
          rowId,
          old.seq(at),
          old.isTombstone(at),
        );
      }
      residual.set(indexId, next);
      remaps.set(old, { delta: next, entries });
    }
    const group: GroupRef = {
      groupId: 0,
      firstRowId: first,
      lastRowId: end - 1,
      rowCount: memtable.rowCount,
      liveRowCount: memtable.rowCount,
      root: rootBlob,
      deletions: null,
    };
    const pin = this.#space.pin(`staged t${String(memtable.tableId)}`);
    owned.pins.push(pin);
    const part: StagedPart = {
      stagingId,
      group,
      groupRoot,
      runs,
      pin,
      extents: writer.extents.slice(from),
    };
    return { part, residual, remaps };
  }

  /** A staged memtable whose tail a rollback truncated: tail deleted in a bitmap, runs filtered. */
  async #truncateStaged(
    writer: ExtentWriter,
    memtable: Memtable,
    part: StagedPart,
    retired: ExtentRef[],
  ): Promise<StagedPart> {
    const from = writer.extents.length;
    const live = memtable.rowCount;
    const tail = Array.from({ length: memtable.storedRows - live }, (_, k) => live + k);
    const deletions = await this.#writeBitmap(writer, memtable.storedRows, null, tail);
    this.#check();
    const old: ExtentRef[] = [];
    if (part.group.deletions !== null) old.push(part.group.deletions.extent);
    const end = memtable.firstRowId + live;
    const runs = new Map<number, RunRef[]>();
    for (const [indexId, list] of part.runs) {
      const kept: RunRef[] = [];
      for (const run of list) {
        old.push(runExtent(run));
        const reader = await this.#runReader(run);
        this.#check();
        const builder = new RunBuilder();
        await reader.cursor(this.#space, this.#grant, (key, rowId, tombstone) => {
          if (rowId < end) builder.add(key.slice(), rowId, tombstone);
          return false;
        });
        this.#check();
        const encoded = builder.finish();
        if (encoded !== null) {
          const written = await writer.write(encoded.parts);
          this.#check();
          kept.push(runRef(encoded, written.blobs));
        }
      }
      if (kept.length > 0) runs.set(indexId, kept);
    }
    retired.push(...old);
    const extents = [
      ...part.extents.filter((extent: ExtentRef) => !old.some((gone) => sameExtent(gone, extent))),
      ...writer.extents.slice(from),
    ];
    return {
      ...part,
      group: { ...part.group, deletions, liveRowCount: live },
      runs,
      extents,
    };
  }

  /** Drops `id`'s staging after its transaction rolled back (or failed before its frame). */
  #dropStaging(id: number): void {
    const drops: Array<{ state: TableState; staging: Staging }> = [];
    for (const state of this.#tables.values()) {
      const staging = state.stagings.get(id);
      if (staging === undefined) continue;
      state.stagings.delete(id);
      drops.push({ state, staging });
      const gone = new Set(staging.memtables);
      this.#setView(state, {
        ...state.view,
        memtables: state.view.memtables.filter((memtable) => !gone.has(memtable)),
      });
      this.shared.readers.holdUntilOlderClose(() => {
        for (const memtable of gone) memtable.releaseAll();
      });
    }
    if (drops.length === 0) return;
    this.maintenance.stagingRollbacks += 1;
    // The durable root still lists them: the next root leaves them out and retires their extents
    // (under the lock, so no publication in flight adopts the old entry back).
    this.#stagingDrops = this.#stagingDrops.then(async () => {
      const unlock = await this.#lock();
      try {
        for (const { state, staging } of drops) {
          if (staging.entry === null) continue;
          this.#pendingRetire.push(...staging.directory);
          for (const memtable of staging.memtables)
            this.#pendingRetire.push(...need(memtable.staged).extents);
          if (state.root !== null)
            state.root = {
              ...state.root,
              staged: state.root.staged.filter((staged) => staged.transaction !== id),
            };
        }
      } finally {
        unlock();
      }
    });
  }

  // ── Idle maintenance (ruling R10) ───────────────────────────────────────────────────────

  /** Whether the write path has failed (a call, so a check after an await is not narrowed away). */
  #hasFailed(): boolean {
    return this.failure !== null;
  }

  /** True once `close` began. */
  get closed(): boolean {
    return this.#closed;
  }

  #garbageOverBound(): boolean {
    const stats = this.#space.stats;
    return stats.waitingBytes > Math.max(this.#idleGarbageBytes, 0.05 * stats.dataBytes);
  }

  /**
   * Arms one idle check `IDLE_FLUSH_MS` from now when retired space waits past its bound; called
   * when maintenance runs out of work. Nothing is armed otherwise, so an idle database with
   * little garbage has no timer at all.
   */
  #armIdlePublish(): void {
    if (this.#idleTimer !== null || this.#closed || this.failure !== null || this.#recovering)
      return;
    if (!this.#garbageOverBound()) return;
    this.#idleTimer = setTimeout(() => {
      this.#idleTimer = null;
      // Kept, so close() waits for a publication the timer already started (review B3-p2b-2 B3).
      const run = this.#idlePublish()
        .catch((error: unknown) => {
          this.#failure ??= error instanceof Error ? error : new Error(String(error));
        })
        .finally(() => {
          if (this.#idleRun === run) this.#idleRun = null;
        });
      this.#idleRun = run;
    }, IDLE_FLUSH_MS);
    (this.#idleTimer as { unref?: () => void }).unref?.();
  }

  /** Whether nothing runs or is due: no writer, maintenance, flush, merge or unflushed commit. */
  #idle(): boolean {
    return (
      !this.#slotBusy &&
      this.#queue.length === 0 &&
      this.#recycling === null &&
      !this.#merging &&
      this.#flushesWriting === 0 &&
      this.#nextFlushReady() === null &&
      (this.#mergeMode !== "auto" || this.#nextMerge() === null) &&
      this.#appender.durableLsn >= this.#appender.lastLsn
    );
  }

  /** Publishes one no-change root when the database is idle and garbage is over its bound (R10). */
  async #idlePublish(): Promise<void> {
    if (this.#closed || this.failure !== null || !this.#garbageOverBound()) return;
    // Busy (a writer, maintenance, unflushed commits, or a commit within the interval): look
    // again after the interval. Once the publish is made, nothing is armed until garbage returns.
    if (!this.#idle() || performance.now() - this.#lastCommitAt < IDLE_FLUSH_MS) {
      this.#armIdlePublish();
      return;
    }
    const unlock = await this.#lock();
    let madeRoot: ManifestRoot | null = null;
    const delta = new DeltaPublication(
      "idle",
      this.#extentDeltas,
      this.#nextAttempt,
      this.#afterDeltaPrepared,
      this.#sinkFailed,
    );
    const writer = this.#space.beginWrite("idle publish");
    try {
      // The lock may have waited: closed, or garbage reclaimed by a publish meanwhile.
      if (this.closed || !this.#garbageOverBound()) {
        await writer.abort();
        return;
      }
      const released: ExtentRef[] = [];
      const root = await this.#nextRoot(writer, released, new Map());
      await delta.prepare(root.sequence, writer, released);
      const bytes = encodeManifestRoot(root);
      const charge = parseCharge(bytes.length);
      this.#chargeMetadata(charge);
      this.#roots.set(root, { refs: 1, bytes: charge });
      madeRoot = root;
      await delta.publish(this.#space, bytes, writer, released);
      this.#root = root;
      this.#pruneCatalogs(root.globalLsn);
      this.#adoptTableRoots(root);
      this.#publishState();
      this.maintenance.idlePublishes += 1;
    } catch (error) {
      if (writer.state === "open") await writer.abort().catch(() => undefined);
      throw error;
    } finally {
      delta.settle();
      if (madeRoot !== null) this.#releaseRoot(madeRoot);
      unlock();
    }
  }

  // ── Log recycling ───────────────────────────────────────────────────────────────────────

  /**
   * After any append (a commit, a standalone sequence frame) or an empty commit: past the log
   * bound, a recycle is scheduled (one at a time; appends deferred by a running recycle start
   * none).
   */
  #afterAppend(): void {
    if (this.#appender.end >= this.#logRecycleBytes) this.#maybeRecycle();
  }

  /** Starts a log recycle unless one is running (or the write path cannot run one). */
  #maybeRecycle(): void {
    if (this.#recycling !== null || this.#closed || this.#recovering || this.failure !== null)
      return;
    this.#recycling = this.#recycleLog()
      .catch((error: unknown) => {
        if (this.#closed) return; // a close refused its wait for the slot
        this.#failure ??= error instanceof Error ? error : new Error(String(error));
      })
      .finally(() => {
        this.#recycling = null;
        this.#armIdlePublish();
      });
  }

  /** Recycles the log now (tests); resolves when it is done or was not possible. */
  recycleLog(): Promise<void> {
    this.#maybeRecycle();
    return this.#recycling ?? Promise.resolve();
  }

  /**
   * Recycles the log (access/write-path.ts "Log recycling", DECISIONS.md difference 4):
   * 1. takes the writer slot (no commit runs meanwhile; sequence frames keep coming);
   * 2. fixes the cut: every later append is deferred by the appender (log.ts "Recycling"), and
   *    everything through the cut is made durable;
   * 3. flushes every table through the cut (rows, tombstones, folded deletions), or, for a table
   *    with nothing in memory, only moves its checkpoint to the cut;
   * 4. publishes a root whose global state and every table are checkpointed at the cut and which
   *    names a new incarnation (new salt, first LSN cut + 1, its own sequence);
   * 5. only once that publish resolved, resets the log and starts the new incarnation with the
   *    deferred frames.
   * A crash before 4 resolves recovers from the old root and log; after it, from the new root with
   * an empty (or old-salt) log. An index build whose entries are not yet in runs keeps the global
   * checkpoint below it; then the recycle gives up and the deferred frames go to the old log.
   */
  async #recycleLog(): Promise<void> {
    await this.#takeSlot("maintenance");
    if (this.#closed) {
      this.#maintenanceHolds = false;
      this.#passSlot();
      return;
    }
    let switched = false;
    let unlock: (() => void) | null = null;
    let madeRoot: ManifestRoot | null = null;
    const delta = new DeltaPublication(
      "recycle",
      this.#extentDeltas,
      this.#nextAttempt,
      this.#afterDeltaPrepared,
      this.#sinkFailed,
    );
    let writer: ExtentWriter | null = null;
    try {
      this.#check();
      const bytes = this.#appender.end;
      const cut = this.#appender.beginRecycle();
      await this.#appender.flushEncoded();
      // A flush in flight finishes first. The recycle then flushes each table's frozen and live
      // memtables itself, through the cut: a frozen memtable whose last commit continues into the
      // live one cannot flush alone, so waiting for the ordinary flushes to drain them would never
      // end.
      await this.#flushes;
      this.#check();
      const quiet = new Map<number, TableRoot>();
      // Tables with an index build first: until they flush, the others' roots may be held back.
      const building = new Set(
        [...this.#pendingIndexes.values()].map((pending) => pending.tableId),
      );
      const states = [...this.#tables.values()].sort(
        (a, b) => Number(building.has(b.tableId)) - Number(building.has(a.tableId)),
      );
      for (const state of states) {
        const def = this.#catalog.table(state.tableId);
        if (def === undefined) continue;
        const last = state.view.memtables[state.view.memtables.length - 1];
        if (quietTable(state)) {
          this.maintenance.recycleQuietTables += 1;
          state.flushedThroughLsn = Math.max(state.flushedThroughLsn, cut);
          // An index built on it before the cut has no entries: its root gets the empty index.
          const atCut = this.#catalogAt(cut).table(state.tableId);
          if (state.root !== null && state.root.flushedThroughLsn < cut && atCut !== undefined)
            quiet.set(state.tableId, {
              ...withCatalogIndexes(state.root, atCut, true),
              flushedThroughLsn: cut,
            });
          continue;
        }
        if (last?.state === "live") {
          last.state = "frozen";
          last.measure();
          this.#setView(state, { ...state.view });
        }
        this.maintenance.recycleFlushedTables += 1;
        // On the flush chain, with whatever is still frozen when it runs.
        const flush = this.#flushes.then(async () => {
          const set = state.view.memtables.filter(awaitsFlush);
          if (set.length > 0) await this.#flushTable(state, set, cut);
        });
        this.#flushes = flush.catch(() => undefined);
        await flush;
        this.#check();
      }
      unlock = await this.#lock();
      // Every table must now be checkpointed at the cut (or have nothing to checkpoint), and no
      // index build may wait for its runs; otherwise the log cannot go yet.
      const uncovered = [...this.#tables.values()].some(
        (state) =>
          this.#catalog.table(state.tableId) !== undefined &&
          !quiet.has(state.tableId) &&
          state.flushedThroughLsn < cut,
      );
      const unbuilt = [...this.#pendingIndexes.values()].some(
        (pending) => (this.#tables.get(pending.tableId)?.flushedThroughLsn ?? 0) < pending.lsn,
      );
      if (unbuilt || uncovered) {
        this.maintenance.recyclesAbandoned += 1;
        return;
      }
      for (const tableId of [...quiet.keys()]) {
        const state = this.#tables.get(tableId);
        if (state?.root == null || state.root.flushedThroughLsn >= cut) quiet.delete(tableId);
        else quiet.set(tableId, { ...state.root, flushedThroughLsn: cut });
      }
      writer = this.#space.beginWrite("log recycle");
      const salt = (crypto.getRandomValues(new Uint32Array(1))[0] ?? 1) >>> 0 || 1;
      const sequence = this.#root.sequence + 1;
      const incarnation = { salt, firstLsn: cut + 1, rootSequence: sequence };
      const retired: ExtentRef[] = [];
      const root = await this.#nextRoot(writer, retired, quiet, incarnation);
      if (
        root.globalLsn !== cut ||
        root.tables.some((table) => table.flushedThroughLsn < cut) ||
        root.sequence !== sequence
      )
        throw new RangeError("A log recycle's root does not cover the log it would reset");
      await delta.prepare(root.sequence, writer, retired);
      const rootBytes = encodeManifestRoot(root);
      const rootCharge = parseCharge(rootBytes.length);
      this.#chargeMetadata(rootCharge);
      this.#roots.set(root, { refs: 1, bytes: rootCharge });
      madeRoot = root;
      await delta.publish(this.#space, rootBytes, writer, retired);
      switched = true;
      this.#root = root;
      this.#pruneCatalogs(root.globalLsn);
      this.#adoptTableRoots(root);
      for (const [tableId, table] of quiet) {
        const state = this.#tables.get(tableId);
        if (state !== undefined) state.root = table;
      }
      this.#publishState();
      // The root covering the old log is durable: only now may the log go.
      await this.#session.resetLog();
      this.maintenance.deferredFrames += this.#appender.deferredCount;
      await this.#appender.finishRecycle(incarnation);
      this.maintenance.recycles += 1;
      this.maintenance.recycledBytes += bytes;
    } catch (error) {
      if (writer?.state === "open") await writer.abort().catch(() => undefined);
      throw error;
    } finally {
      // Before the switch the deferred frames go to the old log; a failure after it (the reset,
      // the new incarnation) fails the appender, so nothing waits on the deferred frames forever.
      if (!switched) this.#appender.abandonRecycle();
      else if (this.#appender.recycling)
        this.#appender.failRecycle(new Error("The log reset after a recycle's publish failed"));
      delta.settle();
      if (madeRoot !== null) this.#releaseRoot(madeRoot);
      unlock?.();
      this.#maintenanceHolds = false;
      this.#passSlot();
    }
  }

  // ── Merges (merge.ts) ───────────────────────────────────────────────────────────────────

  /** Takes the publish lock; resolves with its release. */
  async #lock(): Promise<() => void> {
    const previous = this.#locked;
    let release!: () => void;
    this.#locked = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    return release;
  }

  #scheduleMerge(): void {
    if (this.#mergeMode === "auto") this.#runMerges();
  }

  /** Runs merges until no level is in debt (any mode but `off`); resolves when they stop. */
  runMaintenance(): Promise<void> {
    this.#runMerges();
    return this.#merges;
  }

  #runMerges(): void {
    if (this.#mergeScheduled || this.#closed || this.#mergeMode === "off") return;
    this.#mergeScheduled = true;
    this.#merges = this.#merges.then(async () => {
      this.#mergeScheduled = false;
      for (;;) {
        if (this.failure !== null || this.#closed) return;
        const job = this.#nextMerge();
        if (job === null) {
          this.#armIdlePublish();
          return;
        }
        // A merge that fails before its publish leaves its inputs as they are (review B3-p2b-1
        // #3): it is retried once with a quarter of the split bound, then its inputs are left
        // alone. Only a failed publish (the space failed) fails the write path.
        let done = false;
        for (const divisor of [1, 4]) {
          try {
            await this.#merge(job.state, job.indexId, job.plan, job.inputs, divisor);
            done = true;
            break;
          } catch (error) {
            if (error instanceof MergeAbortedError) return;
            // The extent-list sink failed: never published around (access/write-path.ts).
            if (error instanceof ExtentSinkError) {
              this.#failure ??= error;
              return;
            }
            // The publish failed (the space refuses everything after) or something else failed.
            if (this.#space.failed !== null || this.#hasFailed()) {
              this.#failure ??= error instanceof Error ? error : new Error(String(error));
              return;
            }
            this.maintenance.mergeFailures += 1;
            this.maintenance.lastMergeFailure =
              error instanceof Error ? error.message : String(error);
          }
        }
        if (!done) this.#unmergeable.add(need(job.inputs[0]));
      }
    });
  }

  /** First inputs of merges that failed twice: not tried again until their level changes. */
  readonly #unmergeable = new WeakSet<RunRef>();

  /** The lowest level in debt across every table's indexes, or null. */
  #nextMerge(): MergeJob | null {
    let best: MergeJob | null = null;
    for (const state of this.#tables.values()) {
      const def = this.#catalog.table(state.tableId);
      if (def === undefined || state.root === null) continue;
      for (const index of def.indexes) {
        const runs = state.view.runs.get(index.id) ?? [];
        const plan = planMerge(runs, this.#mergePolicy.fanIn);
        if (plan !== null && this.#unmergeable.has(need(runs[plan.from]))) continue;
        if (plan !== null && (best === null || plan.level < best.plan.level))
          best = { state, indexId: index.id, plan, inputs: runs.slice(plan.from, plan.to) };
      }
    }
    return best;
  }

  /** The most level-0 runs any index has (writers wait at `#stallRuns`). */
  #levelZeroMax(): number {
    let most = 0;
    for (const state of this.#tables.values()) {
      const def = this.#catalog.table(state.tableId);
      if (def === undefined) continue;
      for (const index of def.indexes) {
        const runs = state.view.runs.get(index.id) ?? [];
        // Level-0 runs whose merge failed twice are not debt writers can wait out (review
        // B3-p2b-2 B2): no merge will reduce them until a new flush changes the level, which a
        // waiting writer would prevent.
        const first = runs[0];
        if (first?.level === 0 && this.#unmergeable.has(first)) continue;
        most = Math.max(most, levelZero(runs));
      }
    }
    return most;
  }

  async #merge(
    state: TableState,
    indexId: number,
    plan: MergePlan,
    inputs: readonly RunRef[],
    /** The split bound is divided by this (a retry after a failure). */
    divisor = 1,
  ): Promise<void> {
    const started = performance.now();
    const grant = await this.#governor.admit({
      label: `merge t${String(state.tableId)} i${String(indexId)}`,
      minimumBytes: MERGE_FIXED_BYTES + inputs.length * MERGE_INPUT_BYTES,
      spillable: false,
    });
    // The encoder's buffers and the staging queue (the space charges leases and its own staging).
    const working = MERGE_FIXED_BYTES - 512 * 1024;
    if (!grant.allocate(working)) {
      grant.close();
      throw new RangeError("A merge's admitted grant cannot hold its working memory");
    }
    const writer = this.#space.beginWrite(`merge t${String(state.tableId)} i${String(indexId)}`, {
      budget: grant,
    });
    let unlock: (() => void) | null = null;
    let madeRoot: ManifestRoot | null = null;
    const delta = new DeltaPublication(
      "merge",
      this.#extentDeltas,
      this.#nextAttempt,
      this.#afterDeltaPrepared,
      this.#sinkFailed,
    );
    this.#merging = true;
    try {
      const readers: RunReader[] = [];
      for (const ref of inputs) readers.push(await this.#runReader(ref));
      const output = await mergeRuns({
        space: this.#space,
        writer,
        grant,
        readers,
        level: plan.level + 1,
        bottom: plan.bottom,
        policy:
          divisor === 1
            ? this.#mergePolicy
            : {
                ...this.#mergePolicy,
                splitBytes: Math.floor(this.#mergePolicy.splitBytes / divisor),
              },
        stopped: () => this.#closed || this.failure !== null,
      });
      unlock = await this.#lock();
      // The inputs must still be where they were: contiguous in the current list of a live table
      // whose root holds the index (a drop or DROP INDEX meanwhile abandons the merge).
      const current = state.view.runs.get(indexId) ?? [];
      const at = current.indexOf(need(inputs[0]));
      const root = state.root;
      const holds =
        root !== null &&
        (root.keyIndex?.indexId === indexId ||
          root.secondaryIndexes.some((index) => index.indexId === indexId));
      if (
        this.#tables.get(state.tableId) !== state ||
        this.#catalog.table(state.tableId)?.indexes.some((index) => index.id === indexId) !==
          true ||
        !holds ||
        at < 0 ||
        inputs.some((run, k) => current[at + k] !== run)
      ) {
        await writer.abort();
        this.maintenance.mergesAbandoned += 1;
        return;
      }
      // Flushes only prepend: inputs that reached the oldest run still do.
      const runs = [...current.slice(0, at), ...output.runs, ...current.slice(at + inputs.length)];
      const withRuns = (index: IndexRoot): IndexRoot =>
        index.indexId === indexId ? { ...index, runs } : index;
      const tableRoot: TableRoot = {
        ...root,
        keyIndex: root.keyIndex === null ? null : withRuns(root.keyIndex),
        secondaryIndexes: root.secondaryIndexes.map(withRuns),
      };
      const retired: ExtentRef[] = inputs.map(runExtent);
      const next = await this.#nextRoot(writer, retired, new Map([[state.tableId, tableRoot]]));
      await delta.prepare(next.sequence, writer, retired);
      const rootBytes = encodeManifestRoot(next);
      const rootCharge = parseCharge(rootBytes.length);
      this.#chargeMetadata(rootCharge);
      this.#roots.set(next, { refs: 1, bytes: rootCharge });
      madeRoot = next;
      await delta.publish(this.#space, rootBytes, writer, retired);
      this.#root = next;
      this.#pruneCatalogs(next.globalLsn);
      this.#adoptTableRoots(next);
      state.root = tableRoot;
      const viewRuns = new Map(state.view.runs);
      viewRuns.set(indexId, runs);
      this.#setView(state, { ...state.view, runs: viewRuns });
      // Readers older than this publication may still read the inputs (their pins keep the
      // extents); the inputs' resident parts go once they close.
      this.shared.readers.holdUntilOlderClose(() => {
        this.#dropRunReaders(inputs);
      });
      const counters = this.maintenance;
      counters.merges += 1;
      if (this.#flushesWriting > 0) counters.mergesDuringFlush += 1;
      if (plan.bottom) counters.bottomMerges += 1;
      if (output.runs.length > 1) counters.splitMerges += 1;
      counters.mergeEntriesIn += output.entriesIn;
      counters.mergeEntriesOut += output.entriesOut;
      counters.mergeCancelled += output.cancelled;
      counters.mergeBytes += output.bytesWritten;
      counters.mergeSlices += output.slices;
      counters.mergesByLevel[plan.level] = (counters.mergesByLevel[plan.level] ?? 0) + 1;
      counters.mergeMs += performance.now() - started;
    } catch (error) {
      if (writer.state === "open") await writer.abort().catch(() => undefined);
      throw error;
    } finally {
      this.#merging = false;
      delta.settle();
      if (madeRoot !== null) this.#releaseRoot(madeRoot);
      unlock?.();
      grant.close();
    }
  }

  /** Forgets the resident parts of runs no view can reach any more, returning their charge. */
  #dropRunReaders(runs: readonly RunRef[]): void {
    for (const run of runs) {
      const key = blobKey(run.entries);
      const reader = this.#runReaders.get(key);
      if (reader === undefined) continue;
      this.#runReaders.delete(key);
      // One still opening returns its charge when it resolves (#runReader sees it gone).
      if (!(reader instanceof Promise)) this.#chargeMetadata(-reader.bytes);
    }
  }

  async #writeGroup(writer: ExtentWriter, def: TableDef, memtable: Memtable): Promise<GroupRoot> {
    const group = new GroupWriter({
      columns: def.columns.map((column) => ({ columnId: column.id, type: column.type })),
      writer,
      charge: this.#grant,
    });
    const chunks = Math.ceil(memtable.rowCount / MEMORY_CHUNK_ROWS);
    for (let chunk = 0; chunk < chunks; chunk++) {
      const length = Math.min(MEMORY_CHUNK_ROWS, memtable.rowCount - chunk * MEMORY_CHUNK_ROWS);
      const vectors = memtable.chunks.map((list) => {
        const stored = list[chunk];
        if (stored === undefined) throw new RangeError("A memtable chunk is missing");
        return stored.view(length);
      });
      await group.append(vectors, length, 0);
    }
    const written = await group.finish();
    return { rowCount: written.rowCount, rowIds: { kind: "dense" }, columns: written.columns };
  }

  async #writeBitmap(
    writer: ExtentWriter,
    rows: number,
    base: Uint32Array | null,
    positions: readonly number[],
  ): Promise<BlobRef> {
    const bits = new Uint32Array(Math.ceil(rows / 32));
    if (base !== null) bits.set(base.subarray(0, bits.length));
    for (const position of positions)
      bits[position >>> 5] = (bits[position >>> 5] ?? 0) | (1 << (position & 31));
    return need((await writer.write([new Uint8Array(bits.buffer)])).blobs[0]);
  }

  /**
   * Writes a table's group directory for `groups`, of which those before `firstChanged` are the
   * durable ones unchanged: leading full pages wholly before it are kept (format/manifest-codec.ts
   * "paged group directory"), the rest are rewritten. Extents of the old directory that the new one does not use
   * are added to `retired` (the pages one flush writes share an extent, which goes with its last
   * page).
   */
  async #writeDirectory(
    writer: ExtentWriter,
    state: TableState,
    groups: GroupDirectory,
    firstChanged: number,
    retired: ExtentRef[],
  ): Promise<{ directory: BlobRef; pages: readonly GroupPageEntry[] }> {
    // Full pages wholly before the first changed group are kept: O(pages), no group is compared.
    const keep: GroupPageEntry[] = [];
    let kept = 0;
    for (const page of state.pages) {
      if (page.groupCount !== GROUPS_PER_PAGE || kept + page.groupCount > firstChanged) break;
      keep.push(page);
      kept += page.groupCount;
    }
    let directory = EMPTY_DIRECTORY;
    let pages: readonly GroupPageEntry[] = [];
    if (groups.count > 0) {
      // Only the rewritten pages' groups are decoded (at most a page plus changed pages).
      const written = await writeGroupDirectory(writer, groups.slice(kept), keep);
      directory = written.ref;
      pages = written.pages;
    }
    const live = new Set<number>();
    if (!isEmptyDirectory(directory)) live.add(directory.extent.offset);
    for (const page of pages) live.add(page.ref.extent.offset);
    const old: ExtentRef[] = state.pages.map((page) => page.ref.extent);
    const index = state.root?.groups;
    if (index !== undefined && !isEmptyDirectory(index)) old.push(index.extent);
    for (const extent of old)
      if (!live.has(extent.offset) && !retired.some((known) => sameExtent(known, extent)))
        retired.push(extent);
    return { directory, pages };
  }

  /** The next root: global state as of the durable LSN, `changed` tables replaced. */
  /** Pending retirements each root took (they leave `#pendingRetire` once it is installed). */
  readonly #drainedBy = new WeakMap<ManifestRoot, readonly ExtentRef[]>();

  /** Quiet index builds each root made settles (counted once it is published). */
  readonly #settles = new WeakMap<ManifestRoot, number>();

  /**
   * The LSN the next root can claim, given the table roots it changes: the durable LSN, held below
   * each index build no table checkpoint covers yet (its catalog must not be in a root whose
   * table has no runs for it). A quiet table's build never holds it: that root settles it.
   */
  #rootLsn(changed: ReadonlyMap<number, { readonly flushedThroughLsn: number }>): number {
    let globalLsn = Math.max(this.#root.globalLsn, this.durableLsn);
    for (const pending of this.#pendingIndexes.values()) {
      const state = this.#tables.get(pending.tableId);
      const covered =
        changed.get(pending.tableId)?.flushedThroughLsn ?? state?.flushedThroughLsn ?? 0;
      if (covered >= pending.lsn) continue;
      if (!changed.has(pending.tableId) && state !== undefined && quietTable(state)) continue;
      globalLsn = Math.min(globalLsn, pending.lsn - 1);
    }
    return Math.max(globalLsn, this.#root.globalLsn);
  }

  /**
   * Whether a flush of `state` through `through` can publish now: the root it makes claims an LSN
   * held below other tables' index builds, and its catalog must have this table as it is now (the
   * flushed rows' schema and indexes). A table created, altered or indexed after that LSN waits
   * for the build to flush (found by the R25 delta test: the root left the table out, and the
   * flush's extents were allocated, reported added, and reachable from no root).
   */
  #flushable(state: TableState, through: number): boolean {
    const now = this.#catalog.table(state.tableId);
    if (now === undefined) return true; // dropped: the flush abandons itself
    const then = this.#catalogAt(
      this.#rootLsn(new Map([[state.tableId, { flushedThroughLsn: through }]])),
    ).table(state.tableId);
    if (then?.schemaVersion !== now.schemaVersion) return false;
    // Every index the root's catalog has must still be there; an index newer than it only when it
    // is this table's own build this flush does not reach (the flushed memtables predate it, so it
    // has no entries in them).
    const unbuilt = (indexId: number) => {
      const pending = this.#pendingIndexes.get(indexId);
      return pending?.tableId === state.tableId && pending.lsn > through;
    };
    return (
      then.indexes.every((index) => now.indexes.some((other) => other.id === index.id)) &&
      now.indexes.every(
        (index) => then.indexes.some((other) => other.id === index.id) || unbuilt(index.id),
      )
    );
  }

  /**
   * Freezes the committed live memtable of each table whose index build holds the root's LSN back,
   * so its flush (which settles the build) can run; true when it froze any.
   */
  #freezeBuilds(): boolean {
    let froze = false;
    for (const pending of this.#pendingIndexes.values()) {
      const state = this.#tables.get(pending.tableId);
      const last = state?.view.memtables[state.view.memtables.length - 1];
      if (
        state === undefined ||
        last?.state !== "live" ||
        state.flushedThroughLsn >= pending.lsn ||
        quietTable(state) ||
        last.visibleRows(MAX_LSN) < last.rowCount
      )
        continue;
      last.state = "frozen";
      last.measure();
      this.#setView(state, { ...state.view });
      froze = true;
    }
    return froze;
  }

  async #nextRoot(
    writer: ExtentWriter,
    retired: ExtentRef[],
    changed: ReadonlyMap<number, TableRoot>,
    /** A new log incarnation (a recycle's switching root). */
    incarnation: ManifestRoot["log"] | null = null,
  ): Promise<ManifestRoot> {
    const globalLsn = this.#rootLsn(changed);
    // Dropped staging the durable root still reaches (a rollback's), retired by this root.
    // Never twice: a dropped table's walk below also reaches staging its root still lists. They
    // leave the pending list only once this root is installed: a publication that fails after
    // this point keeps them for the next one (found by the staged-failure test: they leaked).
    if (this.#unclaimedWalks.length > 0) {
      const walks = [...this.#unclaimedWalks];
      const found: ExtentRef[] = [];
      for (const stub of walks) found.push(...(await this.#tableExtents(stub)));
      this.#unclaimedWalks = this.#unclaimedWalks.filter((stub) => !walks.includes(stub));
      this.#pendingRetire.push(...found);
    }
    const drained = [...this.#pendingRetire];
    if (drained.length > 0) retireOnce(retired, drained);
    // A quiet table's index build has no entries: this root settles it (an empty index root and a
    // checkpoint past the build), so the build stops holding the root's LSN back. Without this an
    // index on a table that gets no rows held every later root (and log recycling) back for good.
    const settled = new Map<number, number>();
    for (const pending of this.#pendingIndexes.values()) {
      const state = this.#tables.get(pending.tableId);
      if (
        state !== undefined &&
        !changed.has(pending.tableId) &&
        state.flushedThroughLsn < pending.lsn &&
        pending.lsn <= globalLsn &&
        quietTable(state)
      )
        settled.set(pending.tableId, Math.max(settled.get(pending.tableId) ?? 0, pending.lsn));
    }
    const catalog = this.#catalogAt(globalLsn);
    for (const tableId of changed.keys())
      if (this.#catalog.table(tableId) !== undefined && catalog.table(tableId) === undefined)
        throw new TypeError("A root must carry every table root it publishes (see #flushable)");
    let catalogRef = this.#root.catalog;
    if (catalog.version !== this.#root.catalogVersion) {
      catalogRef = need((await writer.write([catalog.encode()])).blobs[0]);
      retired.push(this.#root.catalog.extent);
    }
    const tables: TableRoot[] = [];
    for (const def of catalog.tables) {
      const state = this.#tables.get(def.id);
      const replaced = changed.get(def.id);
      if (replaced !== undefined) {
        tables.push(replaced);
        continue;
      }
      const settles = settled.get(def.id);
      if (state?.root !== null && state?.root !== undefined) {
        // Reconciled with this root's catalog (review B3-p2b-3 C3): an index the catalog dropped
        // leaves the table root in this publication even when the table itself did not change;
        // the index-drop pass below retires its runs.
        const root = withCatalogIndexes(state.root, def, settles !== undefined);
        tables.push(
          settles === undefined
            ? root
            : { ...root, flushedThroughLsn: Math.max(root.flushedThroughLsn, settles) },
        );
        continue;
      }
      if (state === undefined) {
        // Dropped after this root's LSN (an index build holds it back): replay starts before the
        // drop and its frames name the table, so the root keeps the table as the previous root
        // had it, and its extents stay reachable until a root covers the drop.
        const kept = this.#root.tables.find((previous) => previous.tableId === def.id);
        if (kept !== undefined) tables.push(withCatalogIndexes(kept, def, true));
        else tables.push(emptyTableRoot(def, 1, globalLsn));
        continue;
      }
      // First root to include the table: an empty group directory (nothing written).
      const root: TableRoot = emptyTableRoot(
        def,
        state.nextRowId,
        Math.max(state.flushedThroughLsn, settles ?? 0),
      );
      state.root = root;
      tables.push(root);
    }
    // The root agrees with its catalog (format/manifest.ts "Checkpoints and replay"): every table
    // root has every index its catalog entry has (review B3-p2b-4 D2).
    for (const table of tables) {
      const def = catalog.table(table.tableId);
      const has = new Set(
        [table.keyIndex, ...table.secondaryIndexes]
          .filter((index): index is IndexRoot => index !== null)
          .map((index) => index.indexId),
      );
      if (def?.indexes.some((index) => !has.has(index.id)) === true)
        throw new TypeError("A root's table must carry every index its catalog has");
    }
    // Indexes a table's previous root had and its new root lacks were dropped: their runs go.
    // (Found by the R25 delta test: they were left allocated and unreachable.)
    for (const table of tables) {
      const before = this.#root.tables.find((previous) => previous.tableId === table.tableId);
      if (before === undefined || before === table) continue;
      const kept = new Set(
        [table.keyIndex, ...table.secondaryIndexes]
          .filter((index): index is IndexRoot => index !== null)
          .map((index) => index.indexId),
      );
      for (const index of [before.keyIndex, ...before.secondaryIndexes])
        if (index !== null && !kept.has(index.indexId)) {
          for (const run of index.runs) retired.push(runExtent(run));
          const runs = index.runs;
          this.shared.readers.holdUntilOlderClose(() => {
            this.#dropRunReaders(runs);
          });
        }
    }
    // Tables the previous root had and this one lacks were dropped: retire everything they reach.
    for (const previous of this.#root.tables)
      if (!tables.some((table) => table.tableId === previous.tableId))
        retireOnce(retired, await this.#tableExtents(previous));
    const clients = this.ledger.checkpoint().map((entry) => ({
      ...entry,
      outcomes: entry.outcomes.filter((outcome) => outcome.lsn <= globalLsn),
    }));
    const next: ManifestRoot = {
      formatVersion: MANIFEST_FORMAT_VERSION,
      sequence: this.#root.sequence + 1,
      epoch: this.#session.epoch,
      log: incarnation ?? this.#root.log,
      globalLsn,
      catalogVersion: catalog.version,
      catalog: catalogRef,
      nextTableId: catalog.counters.nextTableId,
      sequences: this.sequences.checkpoint(globalLsn),
      tables,
      clients,
    };
    if (settled.size > 0) this.#settles.set(next, settled.size);
    if (drained.length > 0) this.#drainedBy.set(next, drained);
    return next;
  }

  /** Every extent one table of the current root reaches (for a dropped table's retirement). */
  async #tableExtents(table: TableRoot): Promise<ExtentRef[]> {
    const single: ManifestRoot = { ...this.#root, tables: [table] };
    const extents: ExtentRef[] = [];
    const seen = new Set<string>();
    // Until the shared worker task clock lands (R23.5), keep the deadline across all
    // cached metadata continuations, resetting only after a real task yield.
    let start = performance.now();
    for await (const reached of walkReachable(
      single,
      manifestReader(this.#metadata, this.#grant),
    )) {
      this.#check();
      this.#signal?.throwIfAborted();
      if (performance.now() - start >= YIELD_INTERVAL_MS) {
        await yieldTask();
        start = performance.now();
        this.#check();
        this.#signal?.throwIfAborted();
      }
      if (reached.kind === "catalog") continue;
      const extent = "extent" in reached.ref ? reached.ref.extent : reached.ref;
      const key = `${String(extent.offset)}:${String(extent.length)}`;
      if (!seen.has(key)) {
        seen.add(key);
        extents.push(extent);
      }
    }
    return extents;
  }
}

/** `root` without the indexes `def` (the publication's catalog) no longer has. */
/**
 * `root` with exactly the index roots `def` (the publishing root's catalog) has: dropped ones
 * leave. With `addBuilt`, an index the root lacks is added with no runs; only for a table with no
 * unpublished data (`quietTable`), whose index build therefore has no entries.
 */
function withCatalogIndexes(root: TableRoot, def: TableDef, addBuilt = false): TableRoot {
  const ids = new Set(def.indexes.map((index) => index.id));
  let keyIndex = root.keyIndex !== null && !ids.has(root.keyIndex.indexId) ? null : root.keyIndex;
  const secondaryIndexes = root.secondaryIndexes.filter((index) => ids.has(index.indexId));
  if (addBuilt) {
    keyIndex ??= indexRoot(def, def.primaryKey, new Map());
    for (const index of def.indexes)
      if (
        index.id !== def.primaryKey &&
        !secondaryIndexes.some((candidate) => candidate.indexId === index.id)
      )
        secondaryIndexes.push(need(indexRoot(def, index.id, new Map())));
  }
  // Unchanged only if every index root is the same one (review B3-p2b-4 D2: a drop and a create
  // of the same count is a change).
  if (
    keyIndex === root.keyIndex &&
    secondaryIndexes.length === root.secondaryIndexes.length &&
    secondaryIndexes.every((index, k) => index === root.secondaryIndexes[k])
  )
    return root;
  return { ...root, keyIndex, secondaryIndexes };
}

/**
 * No unpublished data: no rows, index entries or deletions outside published memtables and the
 * folded bitmaps. Every frame of the table so far is in its published state.
 */
function quietTable(state: TableState): boolean {
  return !(
    state.view.memtables.some(
      (memtable) =>
        memtable.state !== "published" &&
        (memtable.rowCount > 0 ||
          [...memtable.deltas.values()].some((delta) => delta.entryCount > 0) ||
          memtable.deletions.size > 0),
    ) ||
    [...state.view.groupDeletes.byGroup.values()].some((positions) =>
      [...positions.values()].some((seq) => seq > state.view.foldedThrough),
    ) ||
    [...state.view.groupSources.values()].some((source) =>
      [...source.deletions.values()].some((seq) => seq > state.view.foldedThrough),
    )
  );
}

/** Adds `extents` to a publication's retirements, each at most once. */
function retireOnce(retired: ExtentRef[], extents: readonly ExtentRef[]): void {
  const known = new Set(retired.map((extent) => extent.offset));
  for (const extent of extents)
    if (!known.has(extent.offset)) {
      known.add(extent.offset);
      retired.push(extent);
    }
}

/** A table's root with nothing written: an empty group directory and no runs. */
function emptyTableRoot(def: TableDef, nextRowId: number, flushedThroughLsn: number): TableRoot {
  return {
    tableId: def.id,
    schemaVersion: def.schemaVersion,
    nextRowId,
    flushedThroughLsn,
    liveRowCount: 0,
    groups: EMPTY_DIRECTORY,
    groupCount: 0,
    staged: [],
    keyIndex: indexRoot(def, def.primaryKey, new Map()),
    secondaryIndexes: def.indexes
      .filter((index) => index.id !== def.primaryKey)
      .map((index) => indexRoot(def, index.id, new Map()))
      .filter((index): index is IndexRoot => index !== null),
    statistics: null,
  };
}

function indexRoot(
  def: TableDef,
  indexId: number | null,
  runs: ReadonlyMap<number, readonly RunRef[]>,
): IndexRoot | null {
  if (indexId === null) return null;
  const index = def.indexes.find((candidate) => candidate.id === indexId);
  if (index === undefined) return null;
  return {
    indexId,
    columnIds: index.columnIds,
    unique: index.unique,
    runs: runs.get(indexId) ?? [],
  };
}

function fixedWidth(type: TableDef["columns"][number]["type"]): number | null {
  switch (layoutOf(type)) {
    case "bool":
      return 1;
    case "i32":
    case "f32":
      return 4;
    case "f64":
    case "int64":
    case "decimal":
      return 8;
    case "fixed16":
    case "interval":
      return 16;
    case "varlen":
      return null;
  }
}

function replayEffect(effect: BodyEffect): Effect {
  switch (effect.kind) {
    case "createTable":
      return { kind: "createTable", tableId: effect.tableId };
    case "alterTable":
      return { kind: "alterTable", tableId: effect.tableId, schemaVersion: effect.schemaVersion };
    case "dropTable":
      return { kind: "dropTable", tableId: effect.tableId };
    case "catalog":
      return { kind: "catalog" };
    case "sequence":
      return { kind: "sequence" };
    case "rows":
      return { kind: "rows", tableId: effect.tableId, schemaVersion: effect.schemaVersion };
    case "publishedRows":
      return effect;
  }
}

function concat(parts: readonly Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.length;
  }
  return out;
}

async function readBlob(space: Space, ref: BlobRef, budget: Grant): Promise<Uint8Array> {
  const lease = await space.read([ref], { budget });
  try {
    return (lease.bytes[0] ?? new Uint8Array(0)).slice();
  } finally {
    lease.release();
  }
}

/** What a publication is (tests and diagnostics). */
export type PublicationKind = "initial" | "flush" | "merge" | "recycle" | "idle" | "stage";

/**
 * A memtable whose flush is due once its rows are committed: frozen, or staged (its flush folds
 * the staged group in).
 */
function awaitsFlush(memtable: Memtable): boolean {
  return memtable.state === "frozen" || memtable.state === "staged";
}

/**
 * The R25 sink failed (its `prepare` threw, or asked for what this build cannot do): the write
 * path fails rather than publish around storage's reachability record (access/write-path.ts
 * `ExtentDeltaSink`). A merge does not retry it.
 */
export class ExtentSinkError extends Error {
  readonly code = "MINNOW_EXTENT_SINK";
  constructor(cause: unknown) {
    super("The extent-list sink refused a publication", { cause });
    this.name = "ExtentSinkError";
  }
}

/**
 * One publication attempt's R25 conversation with the sink (access/write-path.ts
 * `ExtentDeltaSink`): `prepare`, then `publish`, then `settle` in the publication's `finally`,
 * after the publisher installed what it published: it commits an attempt whose root is durable
 * and aborts any other (admission, encoding, a refused or rejected publish, close). A durable
 * publication is a fact (review B3-p2b-4 D1): it is installed before the sink hears of it, and
 * a sink that throws from `commit` or `abort` fails the write path (`fail`, with
 * `ExtentSinkError`) without undoing the install, retrying, or skipping the publisher's cleanup.
 */
class DeltaPublication {
  #attempt: number | null = null;
  #sequence = 0;
  #durable = false;

  constructor(
    readonly kind: PublicationKind,
    readonly sink: ExtentDeltaSink | null,
    readonly nextAttempt: () => number,
    readonly afterPrepared: ((kind: PublicationKind, sequence: number) => void) | null,
    readonly fail: (error: ExtentSinkError) => void,
  ) {}

  async prepare(sequence: number, writer: ExtentWriter, retired: ExtentRef[]): Promise<void> {
    if (this.sink === null) return;
    const attempt = this.nextAttempt();
    this.#attempt = attempt;
    this.#sequence = sequence;
    let result: ExtentDeltaResult | null;
    try {
      result = await this.sink.prepare(attempt, {
        added: [...writer.extents],
        released: [...retired],
        writer,
      });
    } catch (error) {
      throw new ExtentSinkError(error); // settle() aborts the attempt
    }
    if (result !== null) {
      if (result.pages.length > 0)
        throw new ExtentSinkError(
          new RangeError("The root cannot reference extent-list pages yet (R25's format part)"),
        );
      // Retired by this publication like `released`: held under R8 (both durable roots and every
      // pin) before the space reuses them.
      retired.push(...result.retire);
    }
    this.afterPrepared?.(this.kind, sequence);
  }

  /** Publishes; a refused or rejected publish propagates (`settle` aborts the attempt). */
  async publish(
    space: Space,
    root: Uint8Array,
    writer: ExtentWriter,
    retired: readonly ExtentRef[],
  ): Promise<void> {
    await space.publish(root, { writers: [writer], retired });
    this.#durable = true;
  }

  /** Commits (durable) or aborts the attempt, once. Never throws: a sink failure fails the path. */
  settle(): void {
    const attempt = this.#attempt;
    this.#attempt = null;
    if (attempt === null || this.sink === null) return;
    try {
      if (this.#durable) this.sink.commit(attempt, this.#sequence);
      else this.sink.abort(attempt);
    } catch (error) {
      this.fail(new ExtentSinkError(error));
    }
  }
}

/** `openWritePath` (access/write-path.ts `OpenWritePath`). */
export async function openWritePath(input: OpenWritePathInput): Promise<WritePath> {
  return WritePathImpl.open(input);
}
