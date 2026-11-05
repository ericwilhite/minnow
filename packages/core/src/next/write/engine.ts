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
import { Memtable } from "./memtable.js";
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
import { ByteWriter } from "./bytes.js";
import { Catalog, decodeLogged, type LoggedChange } from "./catalog.js";
import { decodeColumn, isNullAt, MEMORY_CHUNK_ROWS } from "./columns.js";
import { KeyDelta } from "./key-hash.js";
import { LogAppender, type LogPosition } from "./log.js";
import {
  decodeGroupRoot,
  decodeGroups,
  decodeRoot,
  encodeGroupRoot,
  encodeGroups,
  encodeRoot,
} from "./manifest-codec.js";
import { readRows } from "./rows.js";
import { encodeRun, RunReader, runRef } from "./runs.js";
import { Sequences } from "./sequences.js";
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

/** The write path's working memory admitted at open (scan buffer, key batches, metadata). */
const MINIMUM_BYTES = RECOVERY_SCAN_BYTES + 4 * 1024 * 1024;

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
  /** Decoded group roots of its durable groups (statistics, re-point). */
  readonly groupRoots: Map<number, GroupRoot>;
}

function blobKey(ref: BlobRef): string {
  return `${String(ref.extent.offset)}:${String(ref.offset)}:${String(ref.length)}:${String(ref.checksum)}`;
}

function sameExtent(a: ExtentRef, b: ExtentRef): boolean {
  return a.offset === b.offset && a.length === b.length;
}

/** The highest seq of anything in a memtable (rows, delta entries). */
function maxSeq(memtable: Memtable, rows: number): number {
  let max = memtable.visibleRows(0) === rows ? 0 : -1;
  // Marks are non-decreasing: the row seq bound is the smallest bound that shows every row.
  let lo = 0;
  let hi = MAX_LSN;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (memtable.visibleRows(mid) >= rows) hi = mid;
    else lo = mid + 1;
  }
  max = Math.max(max, lo);
  for (const delta of memtable.deltas.values())
    for (let entry = 0; entry < delta.entryCount; entry++) max = Math.max(max, delta.seq(entry));
  return max;
}

/** The lowest seq of anything in a memtable (its first rows or delta entries), or Infinity. */
function minSeq(memtable: Memtable): number {
  let min = Infinity;
  if (memtable.rowCount > 0) {
    let lo = 0;
    let hi = Number.MAX_SAFE_INTEGER;
    while (lo < hi) {
      const mid = Math.floor((lo + hi) / 2);
      if (memtable.visibleRows(mid) > 0) hi = mid;
      else lo = mid + 1;
    }
    min = lo;
  }
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

interface ColumnTotals {
  nullCount: number;
  min: StatValue | null;
  max: StatValue | null;
  unknown: boolean;
  bytes: number;
  values: number;
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
  readonly #queue: Array<{ resolve: () => void; reject: (error: unknown) => void }> = [];
  #flushes: Promise<void> = Promise.resolve();
  #flushScheduled = false;
  readonly #runReaders = new Map<string, RunReader | Promise<RunReader>>();
  /** The same readers by `RunRef` identity (the hot path: no key string per probe). */
  readonly #runsByRef = new WeakMap<RunRef, RunReader>();
  readonly #groupRootCache = new Map<string, GroupRoot | Promise<GroupRoot>>();
  readonly #summaries = new Map<number, Map<number, ColumnTotals>>();
  #labels = 0;
  /** True while open replays the log (no flush runs before the appender exists). */
  #recovering = true;
  /** Publications (flushes that reached a durable root). */
  publications = 0;

  private constructor(
    input: OpenWritePathInput,
    grant: Grant,
    root: ManifestRoot,
    catalog: Catalog,
  ) {
    this.durability = input.options.durability;
    this.#session = input.session;
    this.#space = input.space;
    this.#grant = grant;
    this.#now = input.options.now ?? Date.now;
    this.#root = root;
    this.#catalog = catalog;
    this.#baseCatalog = catalog;
    this.#version = catalog.version;
    this.ledger = RequestLedger.restore(root.clients, this.#now());
    this.sequences = new Sequences({
      definition: (id) => this.#catalog.sequence(id) ?? this.#holder?.catalog.sequence(id),
      logReservation: (sequenceId, reservedThrough) =>
        this.#appender.append(
          encodeBody({
            request: null,
            effects: [{ kind: "sequence", sequenceId, reservedThrough }],
          }),
        ),
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
      groupRoot: (ref) => this.#groupRoot(ref),
      bitmap: (ref) => this.#bitmap(ref),
    };
  }

  // ── Open and recovery ────────────────────────────────────────────────────────────────────

  static async open(input: OpenWritePathInput): Promise<WritePathImpl> {
    const grant = await input.governor.admit({
      label: "write path",
      minimumBytes: MINIMUM_BYTES,
      spillable: true,
      ...(input.options.signal === undefined ? {} : { signal: input.options.signal }),
    });
    try {
      const { space, session } = input;
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
        await space.publish(encodeRoot(root), { writers: [writer], retired: [] });
      } else {
        root = decodeRoot(space.root);
        catalog = Catalog.decode(await readBlob(space, root.catalog, grant));
      }
      const engine = new WritePathImpl(input, grant, root, catalog);
      await engine.#load();
      await engine.#recover();
      engine.#recovering = false;
      engine.#scheduleFlush();
      return engine;
    } catch (error) {
      grant.close();
      throw error;
    }
  }

  async #load(): Promise<void> {
    const root = this.#root;
    for (const table of root.tables) {
      const groups =
        table.groupCount === 0
          ? []
          : [...decodeGroups(await readBlob(this.#space, table.groups, this.#grant))];
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
        nextGroupId: groups.reduce((max, group) => Math.max(max, group.groupId + 1), 1),
        groupRoots: new Map(),
      };
      for (const group of groups) {
        const groupRoot = await this.#groupRoot(group.root);
        state.groupRoots.set(group.groupId, groupRoot);
        this.#summarize(table.tableId, groupRoot);
      }
      this.#tables.set(table.tableId, state);
    }
    this.#publishState();
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
    if (change.op === "createIndex")
      await this.#backfill(change.index.tableId, change.index.id, lsn);
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
            `Logged rows of table ${String(effect.tableId)} start at an unexpected row id`,
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
      state.nextRowId = Math.max(state.nextRowId, inserts.firstRowId + inserts.rowCount);
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
    const group = state.view.groups.find(
      (candidate) => rowId >= candidate.firstRowId && rowId <= candidate.lastRowId,
    );
    if (group === undefined)
      throw new LogCorruptionError(`A logged delete names unknown row ${String(rowId)}`);
    state.view.groupDeletes.add(group.groupId, rowId - group.firstRowId, seq);
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
        groups: [],
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
      groupRoots: new Map(),
    });
    this.#publishState();
  }

  #dropTableState(tableId: number): void {
    this.#tables.delete(tableId);
    this.#summaries.delete(tableId);
    this.#publishState();
  }

  addIndexDelta(tableId: number, indexId: number): Memtable {
    const { memtable } = this.liveMemtable(tableId);
    if (!memtable.deltas.has(indexId)) memtable.deltas.set(indexId, new KeyDelta());
    return memtable;
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

  async begin(options: BeginOptions = {}): Promise<TransactionImpl> {
    this.#check();
    if (this.#holder !== null || this.#queue.length > 0)
      await new Promise<void>((resolve, reject) => {
        const waiter = { resolve, reject };
        this.#queue.push(waiter);
        options.signal?.addEventListener(
          "abort",
          () => {
            const index = this.#queue.indexOf(waiter);
            if (index >= 0) {
              this.#queue.splice(index, 1);
              const reason: unknown = options.signal?.reason;
              reject(reason instanceof Error ? reason : new Error("Aborted", { cause: reason }));
            }
          },
          { once: true },
        );
      });
    this.#check();
    // Backpressure: frozen memtables awaiting publication past the cap make writers wait.
    while (this.#frozenBytes() > FROZEN_MEMTABLE_CAP_BYTES) {
      this.#scheduleFlush();
      await this.#flushes;
      this.#check();
    }
    const tx = new TransactionImpl(this, this.appliedLsn);
    this.#holder = tx;
    return tx;
  }

  release(tx: TransactionImpl): void {
    if (this.#holder !== tx) return;
    this.#holder = null;
    const next = this.#queue.shift();
    if (next !== undefined) {
      // The next writer takes the slot as soon as it resumes.
      this.#holder = null;
      next.resolve();
    }
    this.#scheduleFlush();
  }

  commit(tx: TransactionImpl, request: CommitRequest | null): Promise<CommitReceipt> {
    try {
      this.#check();
      const effects = tx.effects(this.#columns);
      if (effects.length === 0) {
        this.release(tx);
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
          if (effect.kind === "dropTable") this.#dropTableState(effect.tableId);
          if (effect.kind === "catalog") {
            const change = decodeLogged(effect.definition);
            if (change.op === "dropSequence") this.sequences.drop(change.sequenceId);
          }
        }
      }
      const durable = this.#appender.durable(lsn) ?? Promise.resolve();
      if (request !== null) {
        const record = () => {
          this.ledger.record(hexId(request.clientId), request.sequence, {
            kind: "committed",
            lsn,
            counts: request.counts,
            recordedAtMs: this.#now(),
          });
        };
        if (this.durability === "strict") void durable.then(record, () => undefined);
        else record();
      }
      this.release(tx);
      return Promise.resolve({ wrote: true, lsn, durable });
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
      await this.#appender.flush();
      await this.#flushes;
      this.#check();
      if (this.#nextFlushReady() === null && this.#appender.durableLsn >= this.#appender.lastLsn)
        return;
      this.#scheduleFlush();
    }
  }

  /** Whether a flush could run now, ignoring durability waits. */
  #nextFlushReady(): true | null {
    for (const state of this.#tables.values()) {
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
    let rowCount = 0;
    for (const group of state.view.groups) rowCount += group.liveRowCount;
    for (const group of state.view.groups)
      rowCount -= groupDeletions(state.view, group.groupId, MAX_LSN).length;
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
          min: total.unknown ? null : total.min,
          max: total.unknown ? null : total.max,
          averageWidth: fixed ?? (total.values === 0 ? 16 : total.bytes / total.values),
        };
      },
    };
  }

  #summarize(tableId: number, root: GroupRoot): void {
    let totals = this.#summaries.get(tableId);
    if (totals === undefined) {
      totals = new Map();
      this.#summaries.set(tableId, totals);
    }
    for (const chunk of root.columns) {
      let total = totals.get(chunk.columnId);
      if (total === undefined) {
        total = { nullCount: 0, min: null, max: null, unknown: false, bytes: 0, values: 0 };
        totals.set(chunk.columnId, total);
      }
      const statistics = chunk.statistics;
      total.nullCount += statistics.nullCount;
      total.values += root.rowCount - statistics.nullCount;
      total.bytes += chunk.data.length;
      if (statistics.min === null || statistics.max === null) {
        if (root.rowCount > statistics.nullCount) total.unknown = true;
        continue;
      }
      if (total.min === null || compareStat(statistics.min, total.min) < 0)
        total.min = statistics.min;
      if (total.max === null || compareStat(statistics.max, total.max) > 0)
        total.max = statistics.max;
    }
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
      for (const list of state.view.runs.values()) runs += list.length;
    }
    const space = this.#space.stats;
    return {
      appliedLsn: this.appliedLsn,
      durableLsn: this.durableLsn,
      logBytes: this.#appender.end,
      memtableBytes,
      frozenMemtables: frozen,
      frozenMemtableBytes: this.#frozenBytes(),
      borrowedChunkBytes: 0,
      repointedReaders: this.shared.readers.heldPins > 0 ? this.shared.readers.count : 0,
      repointPins: this.shared.readers.heldPins,
      levelZeroRuns: runs,
      garbageBytes: space.waitingBytes,
      pins: space.pinCount,
      oldestPin: space.oldestPin,
      backpressure: this.#frozenBytes() > FROZEN_MEMTABLE_CAP_BYTES,
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
    this.#holder?.rollback();
    for (const waiter of this.#queue.splice(0)) waiter.reject(new WritePathClosedError());
    await this.#flushes;
    if (this.#appender.failure === null) await this.#appender.close();
    this.keys.close();
    this.#currentPin?.release();
    this.#currentPin = null;
    this.#grant.close();
  }

  // ── Shared caches ────────────────────────────────────────────────────────────────────────

  readonly #statePins = new Map<WriteState, { pin: SpacePin; count: number }>();

  #pinState(state: WriteState, label: string): SpacePin {
    let held = this.#statePins.get(state);
    if (held === undefined) {
      held = { pin: this.#space.pin(label), count: 0 };
      this.#statePins.set(state, held);
    }
    held.count += 1;
    const entry = held;
    let released = false;
    return {
      generation: entry.pin.generation,
      label,
      get released() {
        return released;
      },
      release: () => {
        if (released) return;
        released = true;
        entry.count -= 1;
        if (entry.count > 0) return;
        entry.pin.release();
        this.#statePins.delete(state);
      },
    };
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
    const opened = RunReader.open(this.#space, ref, this.#grant);
    if (opened instanceof Promise) {
      const pending = opened.then((reader) => {
        this.#runReaders.set(key, reader);
        return reader;
      });
      this.#runReaders.set(key, pending);
      pending.catch(() => this.#runReaders.delete(key));
      return pending;
    }
    this.#runReaders.set(key, opened);
    return opened;
  }

  #groupRoot(ref: BlobRef): GroupRoot | Promise<GroupRoot> {
    const key = blobKey(ref);
    const cached = this.#groupRootCache.get(key);
    if (cached !== undefined) return cached;
    const peeked = this.#space.peek(ref);
    if (peeked !== null) {
      const root = decodeGroupRoot(peeked);
      this.#groupRootCache.set(key, root);
      return root;
    }
    const pending = readBlob(this.#space, ref, this.#grant).then((bytes) => {
      const root = decodeGroupRoot(bytes);
      this.#groupRootCache.set(key, root);
      return root;
    });
    this.#groupRootCache.set(key, pending);
    pending.catch(() => this.#groupRootCache.delete(key));
    return pending;
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
        if (work === null) return;
        try {
          await work;
        } catch (error) {
          this.#failure ??= error instanceof Error ? error : new Error(String(error));
          return;
        }
      }
    });
  }

  /** The next table flush that can run now (null when none), or a wait for durability. */
  #nextFlush(): Promise<void> | null {
    for (const state of this.#tables.values()) {
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
    try {
      const view = state.view;
      const newGroups: GroupRef[] = [];
      const published = new Map<
        Memtable,
        { group: GroupRef; root: GroupRoot; runs: Map<number, RunRef> }
      >();
      let nextGroupId = state.nextGroupId;
      for (const memtable of set) {
        if (memtable.rowCount === 0) continue;
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
      // Fold deletions (≤ through) of older groups into their bitmaps.
      const groups: GroupRef[] = [];
      for (const group of view.groups) {
        const folded = groupDeletions(view, group.groupId, through);
        if (folded.length === 0) {
          groups.push(group);
          continue;
        }
        const old = group.deletions === null ? null : await this.#bitmap(group.deletions);
        const deletions = await this.#writeBitmap(writer, group.rowCount, old, folded);
        if (group.deletions !== null) retired.push(group.deletions.extent);
        groups.push({ ...group, deletions, liveRowCount: group.liveRowCount - folded.length });
      }
      groups.push(...newGroups);
      const directory = need((await writer.write([encodeGroups(groups)])).blobs[0]);
      if (state.root !== null && state.root.groupCount > 0) retired.push(state.root.groups.extent);
      else if (state.root !== null) retired.push(state.root.groups.extent);
      const runs = new Map<number, readonly RunRef[]>();
      for (const index of def.indexes) {
        const fresh = [...published.values()]
          .map((entry) => entry.runs.get(index.id))
          .filter((run): run is RunRef => run !== undefined);
        runs.set(index.id, [...fresh.reverse(), ...(view.runs.get(index.id) ?? [])]);
      }
      const liveRowCount = groups.reduce((sum, group) => sum + group.liveRowCount, 0);
      const nextRowId = Math.max(state.nextRowId, ...newGroups.map((group) => group.lastRowId + 1));
      const tableRoot: TableRoot = {
        tableId: state.tableId,
        schemaVersion: def.schemaVersion,
        nextRowId,
        flushedThroughLsn: through,
        liveRowCount,
        groups: directory,
        groupCount: groups.length,
        staged: [],
        keyIndex: indexRoot(def, def.primaryKey, runs),
        secondaryIndexes: def.indexes
          .filter((index) => index.id !== def.primaryKey)
          .map((index) => indexRoot(def, index.id, runs))
          .filter((index): index is IndexRoot => index !== null),
        statistics: null,
      };
      const root = await this.#nextRoot(writer, retired, new Map([[state.tableId, tableRoot]]));
      await this.#space.publish(encodeRoot(root), { writers: [writer], retired });
      // Re-point readers in the same step the publish resolves (ruling R16.3).
      const pin: SpacePin = this.#space.pin(`re-point t${String(state.tableId)}`);
      this.shared.readers.holdUntilOlderClose(pin);
      for (const memtable of set) {
        const entry = published.get(memtable);
        if (entry !== undefined)
          memtable.published = { group: entry.group, groupRoot: entry.root, runs: entry.runs, pin };
        memtable.state = "published";
        memtable.release();
        if (entry !== undefined) {
          state.groupRoots.set(entry.group.groupId, entry.root);
          this.#summarize(state.tableId, entry.root);
        }
      }
      this.#root = root;
      this.#pruneCatalogs(root.globalLsn);
      state.root = tableRoot;
      state.flushedThroughLsn = through;
      state.nextGroupId = nextGroupId;
      state.nextRowId = nextRowId;
      // Rebuilt from the view as it is now: memtables, deletions and sources added during the
      // flush stay (the flush only removes what it published).
      const current = state.view;
      const sources = new Map(current.groupSources);
      for (const [memtable, entry] of published) sources.set(entry.group.groupId, memtable);
      this.#setView(state, {
        tableId: state.tableId,
        groups,
        runs,
        groupDeletes: current.groupDeletes,
        foldedThrough: through,
        groupSources: sources,
        memtables: current.memtables.filter((memtable) => !set.includes(memtable)),
      });
      this.publications += 1;
    } catch (error) {
      if (writer.state === "open") await writer.abort().catch(() => undefined);
      throw error;
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

  /** The next root: global state as of the durable LSN, `changed` tables replaced. */
  async #nextRoot(
    writer: ExtentWriter,
    retired: ExtentRef[],
    changed: ReadonlyMap<number, TableRoot>,
  ): Promise<ManifestRoot> {
    const globalLsn = Math.max(this.#root.globalLsn, this.durableLsn);
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
      // First root to include the table: an empty group directory.
      const directory = need((await writer.write([encodeGroups([])])).blobs[0]);
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
      log: this.#root.log,
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
    for await (const reached of walkReachable(single, {
      groups: async (ref) => decodeGroups(await readBlob(this.#space, ref, this.#grant)),
      groupRoot: async (ref) => decodeGroupRoot(await readBlob(this.#space, ref, this.#grant)),
    })) {
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

/** `openWritePath` (access/write-path.ts `OpenWritePath`). */
export async function openWritePath(input: OpenWritePathInput): Promise<WritePath> {
  return WritePathImpl.open(input);
}
