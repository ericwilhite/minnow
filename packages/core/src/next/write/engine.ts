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
import { Memtable, MEMTABLE_ROWS } from "./memtable.js";

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
import { encodeRun, RunReader, runRef } from "./runs.js";
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
import { hexId, TransactionImpl, type EngineHooks } from "./transaction.js";
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
    this.#now = input.options.now ?? Date.now;
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
      let rootCharge: number;
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
        const delta = new DeltaPublication("initial", input.extentDeltas ?? null);
        try {
          await delta.prepare(root.sequence, writer, []);
          const bytes = encodeManifestRoot(root);
          rootCharge = parseCharge(bytes.length);
          if (!metadataGrant.allocate(rootCharge)) throw new ParseBudgetError(rootCharge);
          await delta.publish(space, bytes, writer, []);
        } finally {
          delta.settle();
        }
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
          { flushedThroughLsn: table.flushedThroughLsn, schemaVersion: table.schemaVersion },
        ]),
      ),
    };
    const replayer = new Replayer(checkpoint);
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
        case "alterTable":
        case "publishedRows":
          throw new LogCorruptionError(`The write path cannot replay ${effect.kind} yet`);
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
        const { memtable } = this.liveMemtable(effect.tableId, inserts.rowCount - done);
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
      last === undefined
        ? state.nextRowId
        : Math.max(state.nextRowId, last.firstRowId + last.rowCount);
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

  undoFreeze(tableId: number, created: Memtable, frozen: Memtable | null): void {
    const state = this.#tables.get(tableId);
    if (state === undefined) return;
    const memtables = state.view.memtables.filter((memtable) => memtable !== created);
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
    // Older readers may still read its memtables (ruling 5): their memory goes when they close.
    if (state !== undefined)
      this.shared.readers.holdUntilOlderClose(() => {
        for (const memtable of state.view.memtables) memtable.releaseAll();
        this.#chargeMetadata(-state.metadataBytes);
        state.metadataBytes = 0;
        this.#dropRunReaders([...state.view.runs.values()].flat());
      });
  }

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
        if (this.#nextFlushReady() === null && !this.#freezeBlockingTails()) break;
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
        previous?.state !== "frozen" ||
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
      for (const memtable of state.view.memtables) if (memtable.state === "frozen") return true;
    return false;
  }

  commit(tx: TransactionImpl, request: CommitRequest | null): Promise<CommitReceipt> {
    try {
      this.#check();
      const effects = tx.effects(this.#columns);
      if (effects.length === 0) {
        this.release(tx);
        this.#afterAppend(); // sequence frames may have grown the log meanwhile
        return Promise.resolve({ wrote: false, lsn: tx.baseLsn, durable: Promise.resolve() });
      }
      const lsn = this.#appender.append(encodeBody({ request, effects }, this.#body));
      tx.restamp(lsn);
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
        if (memtable.state !== "frozen" || memtable.visibleRows(MAX_LSN) < memtable.rowCount) break;
        through = Math.max(through, maxSeq(memtable, memtable.rowCount));
        const next = memtables[k + 1];
        if (next === undefined || minSeq(next) > through) return true;
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
        if (memtable.state === "frozen") frozen += 1;
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
        if (memtable.state === "frozen") total += memtable.bytes;
    return total;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#idleTimer !== null) clearTimeout(this.#idleTimer);
    this.#idleTimer = null;
    this.#holder?.rollback();
    for (const waiter of this.#queue.splice(0)) waiter.reject(new WritePathClosedError());
    await this.#recycling;
    await this.#flushes;
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
        if (memtable.state !== "frozen") break;
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
          return this.#flushTable(state, set, Math.max(through, state.flushedThroughLsn));
        }
      }
    }
    return null;
  }

  async #flushTable(state: TableState, set: readonly Memtable[], through: number): Promise<void> {
    const def = this.#catalog.table(state.tableId);
    if (def === undefined) return;
    const writer = this.#space.beginWrite(`flush t${String(state.tableId)}`);
    const retired: ExtentRef[] = [];
    // Directory chunks and the root this publication made: it owns them until a state holds them.
    let created: GroupDirectory | null = null;
    let madeRoot: ManifestRoot | null = null;
    let unlock: (() => void) | null = null;
    const delta = new DeltaPublication("flush", this.#extentDeltas, this.#afterDeltaPrepared);
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
        const fresh = set
          .map(
            (memtable) =>
              published.get(memtable)?.runs.get(index.id) ?? rowless.get(memtable)?.get(index.id),
          )
          .filter((run): run is RunRef => run !== undefined);
        runs.set(index.id, [...fresh.reverse(), ...(state.view.runs.get(index.id) ?? [])]);
      }
      const nextRowId = Math.max(state.nextRowId, ...newGroups.map((group) => group.lastRowId + 1));
      const tableRoot: TableRoot = {
        tableId: state.tableId,
        schemaVersion: def.schemaVersion,
        nextRowId,
        flushedThroughLsn: through,
        liveRowCount,
        groups: directory,
        groupCount: groups.count,
        staged: [],
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
      state.root = tableRoot;
      state.pages = pages;
      state.flushedThroughLsn = through;
      for (const [indexId, pending] of this.#pendingIndexes)
        if (pending.tableId === state.tableId && pending.lsn <= through)
          this.#pendingIndexes.delete(indexId);
      state.nextGroupId = nextGroupId;
      state.nextRowId = nextRowId;
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
    const delta = new DeltaPublication("idle", this.#extentDeltas, this.#afterDeltaPrepared);
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
    const delta = new DeltaPublication("recycle", this.#extentDeltas, this.#afterDeltaPrepared);
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
      for (const state of [...this.#tables.values()]) {
        const def = this.#catalog.table(state.tableId);
        if (def === undefined) continue;
        const last = state.view.memtables[state.view.memtables.length - 1];
        const pending =
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
          );
        if (!pending) {
          this.maintenance.recycleQuietTables += 1;
          state.flushedThroughLsn = Math.max(state.flushedThroughLsn, cut);
          if (state.root !== null && state.root.flushedThroughLsn < cut)
            quiet.set(state.tableId, { ...state.root, flushedThroughLsn: cut });
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
          const set = state.view.memtables.filter((memtable) => memtable.state === "frozen");
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
      if (this.#pendingIndexes.size > 0 || uncovered) {
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
    const delta = new DeltaPublication("merge", this.#extentDeltas, this.#afterDeltaPrepared);
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
  async #nextRoot(
    writer: ExtentWriter,
    retired: ExtentRef[],
    changed: ReadonlyMap<number, TableRoot>,
    /** A new log incarnation (a recycle's switching root). */
    incarnation: ManifestRoot["log"] | null = null,
  ): Promise<ManifestRoot> {
    let globalLsn = Math.max(this.#root.globalLsn, this.durableLsn);
    for (const pending of this.#pendingIndexes.values()) {
      const covered =
        changed.get(pending.tableId)?.flushedThroughLsn ??
        this.#tables.get(pending.tableId)?.flushedThroughLsn ??
        0;
      if (covered < pending.lsn) globalLsn = Math.min(globalLsn, pending.lsn - 1);
    }
    globalLsn = Math.max(globalLsn, this.#root.globalLsn);
    const catalog = this.#catalogAt(globalLsn);
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
      if (state?.root !== null && state?.root !== undefined) {
        tables.push(state.root);
        continue;
      }
      if (state === undefined) continue;
      // First root to include the table: an empty group directory (nothing written).
      const directory = EMPTY_DIRECTORY;
      const root: TableRoot = {
        tableId: def.id,
        schemaVersion: def.schemaVersion,
        nextRowId: state.nextRowId,
        flushedThroughLsn: state.flushedThroughLsn,
        liveRowCount: 0,
        groups: directory,
        groupCount: 0,
        staged: [],
        keyIndex: indexRoot(def, def.primaryKey, new Map()),
        secondaryIndexes: def.indexes
          .filter((index) => index.id !== def.primaryKey)
          .map((index) => indexRoot(def, index.id, new Map()))
          .filter((index): index is IndexRoot => index !== null),
        statistics: null,
      };
      state.root = root;
      tables.push(root);
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
        retired.push(...(await this.#tableExtents(previous)));
    const clients = this.ledger.checkpoint().map((entry) => ({
      ...entry,
      outcomes: entry.outcomes.filter((outcome) => outcome.lsn <= globalLsn),
    }));
    return {
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
  }

  /** Every extent one table of the current root reaches (for a dropped table's retirement). */
  async #tableExtents(table: TableRoot): Promise<ExtentRef[]> {
    const single: ManifestRoot = { ...this.#root, tables: [table] };
    const extents: ExtentRef[] = [];
    for await (const reached of walkReachable(
      single,
      manifestReader(this.#metadata, this.#grant),
    )) {
      if (reached.kind === "catalog") continue;
      const extent = "extent" in reached.ref ? reached.ref.extent : reached.ref;
      if (!extents.some((known) => sameExtent(known, extent))) extents.push(extent);
    }
    return extents;
  }
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

/**
 * One publication's R25 conversation with the sink (access/write-path.ts `ExtentDeltaSink`):
 * `prepare`, then `publish`, which settles it as published or failed; `settle` (always called in
 * the publication's finally) abandons a prepared delta that was never published.
 */
/** What a publication is (tests and diagnostics). */
export type PublicationKind = "initial" | "flush" | "merge" | "recycle" | "idle";

class DeltaPublication {
  #pending: number | null = null;

  constructor(
    readonly kind: PublicationKind,
    readonly sink: ExtentDeltaSink | null,
    readonly afterPrepared: ((kind: PublicationKind, sequence: number) => void) | null = null,
  ) {}

  async prepare(sequence: number, writer: ExtentWriter, retired: ExtentRef[]): Promise<void> {
    if (this.sink === null) return;
    const result = await this.sink.prepare({
      sequence,
      added: [...writer.extents],
      released: [...retired],
      writer,
    });
    this.#pending = sequence;
    if (result !== null) retired.push(...result.retire);
    this.afterPrepared?.(this.kind, sequence);
  }

  async publish(
    space: Space,
    root: Uint8Array,
    writer: ExtentWriter,
    retired: readonly ExtentRef[],
  ): Promise<void> {
    try {
      await space.publish(root, { writers: [writer], retired });
    } catch (error) {
      const pending = this.#pending;
      this.#pending = null;
      if (pending !== null) this.sink?.failed(pending, error);
      throw error;
    }
    const pending = this.#pending;
    this.#pending = null;
    if (pending !== null) this.sink?.published(pending);
  }

  settle(): void {
    const pending = this.#pending;
    this.#pending = null;
    if (pending !== null) this.sink?.abandoned(pending);
  }
}

/** `openWritePath` (access/write-path.ts `OpenWritePath`). */
export async function openWritePath(input: OpenWritePathInput): Promise<WritePath> {
  return WritePathImpl.open(input);
}
