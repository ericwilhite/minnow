/**
 * Snapshots and their readers (access/snapshot.ts, access/key-index.ts) over a captured
 * `WriteState`: segments in locator order, deletion vectors, memory chunks (zero copy, or decoded
 * from the published group after a re-point), and index probes and ranges that resolve put and
 * tombstone entries across memtable deltas and durable runs under the view's bound.
 */
import { MemoryReads } from "./memory-read.js";
import type { Held } from "../format/reachable.js";
import type { CatalogView, ColumnDef, IndexDef, TableDef } from "../access/catalog.js";
import {
  inKeyRange,
  prefixRange,
  type IndexCursor,
  type IndexReader,
  type KeyRange,
  type MaybePromise,
  type ProbeOutput,
  type ProbeResult,
  type ProbeResume,
} from "../access/key-index.js";
import {
  SnapshotReleasedError,
  UnknownTableError,
  type ChunkOutput,
  type ChunkResult,
  type DeletionVector,
  type GroupSegment,
  type MemorySegment,
  type MemoryReadRequest,
  type MemoryReadContext,
  type MemoryReadHint,
  type MemoryChunkStep,
  type PageSummary,
  type Segment,
  type Snapshot,
  type TableReadOptions,
  type TableSnapshot,
} from "../access/snapshot.js";
import type { ByteBudget, SpacePin } from "../access/space.js";
import type {
  ColumnChunk as StoredChunk,
  GroupRef,
  GroupRoot,
  RunRef,
} from "../format/manifest.js";
import type { KeyBatch } from "../keys/key-encoding.js";
import { ChunkReader } from "../page/reader.js";
import type { ColumnStatistics } from "../types.js";
import { packLocator } from "../vector/locator.js";
import { allocateData } from "../vector/vector.js";
import type { FlatVector, Selection } from "../vector/vector.js";
import { compareBytes } from "../vector/varlen.js";
import { MEMORY_CHUNK_ROWS } from "./columns.js";
import type { GroupDirectory } from "./groups.js";
import { groupRootOf, type Memtable } from "./memtable.js";
import type { KeyDelta } from "./key-hash.js";
import type { RunEntry, RunReader } from "./runs.js";
import { groupDeletions, type Shared, type TableView, type WriteState } from "./state.js";
import { need } from "./bytes.js";

const settle = <T, R>(
  value: MaybePromise<T>,
  then: (value: T) => MaybePromise<R>,
): MaybePromise<R> => (value instanceof Promise ? value.then(then) : then(value));

// ── Deletion vectors ───────────────────────────────────────────────────────────────────────────

export class BitDeletions implements DeletionVector {
  readonly deletedCount: number;
  constructor(
    readonly bits: Uint32Array,
    count: number,
  ) {
    this.deletedCount = count;
  }

  isDeleted(position: number): boolean {
    return ((this.bits[position >>> 5] ?? 0) & (1 << (position & 31))) !== 0;
  }

  liveRows(from: number, length: number, input: Selection | null, out: Selection): void {
    let count = 0;
    const total = input === null ? length : input.count;
    for (let k = 0; k < total; k++) {
      const row = input === null ? k : (input.rows[k] ?? 0);
      if (!this.isDeleted(from + row)) out.rows[count++] = row;
    }
    out.count = count;
  }
}

function deletionVector(
  rows: number,
  base: Uint32Array | null,
  positions: readonly number[],
): BitDeletions | null {
  if (base === null && positions.length === 0) return null;
  const bits = new Uint32Array(Math.ceil(rows / 32));
  if (base !== null) bits.set(base.subarray(0, bits.length));
  for (const position of positions)
    if (position < rows)
      bits[position >>> 5] = (bits[position >>> 5] ?? 0) | (1 << (position & 31));
  let count = 0;
  for (let position = 0; position < rows; position++)
    if (((bits[position >>> 5] ?? 0) & (1 << (position & 31))) !== 0) count += 1;
  return count === 0 ? null : new BitDeletions(bits, count);
}

// ── Segments ───────────────────────────────────────────────────────────────────────────────────

function missingStatistics(column: ColumnDef | undefined, rows: number): ColumnStatistics {
  void column;
  return { nullCount: rows, min: null, max: null, dictionarySize: null };
}

class GroupSegmentImpl implements GroupSegment {
  readonly kind = "group" as const;
  readonly staged = false;
  readonly rowCount: number;
  readonly #groupId: number;
  #group: GroupRef | null = null;
  #root: GroupRoot | null = null;
  #deleted: number | null = null;

  /** Group `position` of `directory`, read from its columns (`group` decodes it on demand). */
  constructor(
    readonly index: number,
    readonly directory: GroupDirectory,
    readonly position: number,
    readonly view: TableSnapshotImpl,
  ) {
    this.rowCount = directory.rowCount(position);
    this.#groupId = directory.groupId(position);
  }

  get group(): GroupRef {
    return (this.#group ??= this.directory.get(this.position));
  }

  get liveRowCount(): number {
    if (this.#deleted === null) {
      const delta = groupDeletions(this.view.tableView, this.#groupId, this.view.snapshot.bound);
      // The durable bitmap is in the group's live count; delta positions are not in it.
      this.#deleted = this.rowCount - this.directory.liveRows(this.position) + delta.length;
    }
    return this.rowCount - this.#deleted;
  }

  deletions(): MaybePromise<DeletionVector | null> {
    this.view.snapshot.check();
    const delta = groupDeletions(this.view.tableView, this.#groupId, this.view.snapshot.bound);
    const ref = this.directory.deletions(this.position);
    if (ref === null) return deletionVector(this.rowCount, null, delta);
    return settle(this.view.snapshot.shared.bitmap(ref), (bits) =>
      deletionVector(this.rowCount, bits, delta),
    );
  }

  /** The group root's read while it is in flight (concurrent first calls share it). */
  #pending: Promise<Held<GroupRoot>> | null = null;

  /**
   * The group root, through the write path's metadata reader (cached, synchronous on a hit). The
   * segment holds it, charged to the table snapshot's budget, until its snapshot is released.
   */
  root(): MaybePromise<GroupRoot> {
    this.view.snapshot.check();
    if (this.#root !== null) return this.#root;
    const got =
      this.#pending ??
      this.view.snapshot.shared.groupRoot(this.directory.root(this.position), this.view.budget);
    if (got instanceof Promise && this.#pending !== got) {
      this.#pending = got;
      // A failed read is not kept: concurrent callers share its rejection, a later call retries
      // (review B3-p2a-2 #3).
      got.catch(() => {
        if (this.#pending === got) this.#pending = null;
      });
    }
    return settle(got, (held) => {
      if (this.#root === null) {
        this.#root = held.value;
        this.view.snapshot.hold(held);
      }
      return this.#root;
    });
  }

  #requireRoot(): GroupRoot {
    if (this.#root === null) throw new TypeError("Call root() before reading a group's chunks");
    return this.#root;
  }

  chunk(columnId: number): StoredChunk | null {
    return this.#requireRoot().columns.find((chunk) => chunk.columnId === columnId) ?? null;
  }

  statistics(columnId: number): ColumnStatistics | null {
    const chunk = this.chunk(columnId);
    if (chunk === null) return missingStatistics(this.view.column(columnId), this.rowCount);
    return chunk.statistics;
  }

  pages(columnId: number): MaybePromise<readonly PageSummary[]> {
    return settle(this.root(), () => {
      const chunk = this.chunk(columnId);
      if (chunk === null)
        return [
          { firstRow: 0, rowCount: this.rowCount, ...missingStatistics(undefined, this.rowCount) },
        ];
      return settle(
        ChunkReader.open(this.view.snapshot.shared.space, chunk, { budget: this.view.budget }),
        (reader) => {
          const pages = reader.directory.pages.map((entry) => ({
            firstRow: entry.firstRow,
            rowCount: entry.rowCount,
            nullCount: entry.nullCount,
            min: entry.min,
            max: entry.max,
            dictionarySize: null,
          }));
          reader.close();
          return pages;
        },
      );
    });
  }
}

class MemorySegmentImpl implements MemorySegment {
  readonly kind = "memory" as const;
  readonly rowCount: number;
  /** The chunk index the reader currently borrows (−1: none). */
  borrowed = -1;
  #reads: MemoryReads | null = null;

  #bounded(): MemoryReads {
    return (this.#reads ??= new MemoryReads({
      rowCount: this.rowCount,
      column: (id) => this.view.column(id),
      check: () => this.view.snapshot.check(),
      publishedRoot: () => need(this.memtable.published).group.root,
      resident: (columnId, chunk) => {
        if (this.memtable.published !== null) return null;
        const stored = this.memtable.chunks[this.view.columnIndex(columnId)]?.[chunk];
        if (stored === undefined) throw new RangeError("A memtable chunk is missing");
        return stored.view(Math.min(MEMORY_CHUNK_ROWS, this.rowCount - chunk * MEMORY_CHUNK_ROWS));
      },
      watch: (close) =>
        this.view.snapshot.watchCursor({
          next: () => {
            throw new TypeError("A memory read is not an index cursor");
          },
          close,
        }),
    }));
  }

  readHint(request: MemoryReadRequest, context: MemoryReadContext): MaybePromise<MemoryReadHint> {
    return this.#bounded().hint(request, context);
  }

  constructor(
    readonly index: number,
    readonly memtable: Memtable,
    readonly view: TableSnapshotImpl,
  ) {
    this.rowCount = memtable.visibleRows(view.snapshot.bound);
  }

  get liveRowCount(): number {
    return (
      this.rowCount - this.memtable.deletedUnder(this.view.snapshot.bound, this.rowCount).length
    );
  }

  deletions(): DeletionVector | null {
    this.view.snapshot.check();
    return deletionVector(
      this.rowCount,
      null,
      this.memtable.deletedUnder(this.view.snapshot.bound, this.rowCount),
    );
  }

  statistics(): ColumnStatistics | null {
    return null;
  }

  pages(columnId: number): readonly PageSummary[] {
    const c = this.view.columnIndex(columnId);
    const out: PageSummary[] = [];
    for (let first = 0; first < this.rowCount; first += MEMORY_CHUNK_ROWS) {
      const rows = Math.min(MEMORY_CHUNK_ROWS, this.rowCount - first);
      const published = this.memtable.published;
      let nullCount = 0;
      if (published === null) {
        const chunk = this.memtable.chunks[c]?.[first / MEMORY_CHUNK_ROWS];
        const validity = chunk?.validity ?? null;
        if (validity !== null)
          for (let slot = 0; slot < rows; slot++)
            if (((validity[slot >>> 5] ?? 0) & (1 << (slot & 31))) === 0) nullCount += 1;
      } else nullCount = -1;
      out.push({
        firstRow: first,
        rowCount: rows,
        // After a re-point the per-chunk null count is unknown here: report the whole range as
        // possibly null (sound: it never prunes a chunk that has values).
        nullCount: nullCount < 0 ? 0 : nullCount,
        min: null,
        max: null,
        dictionarySize: null,
      });
    }
    return out;
  }

  chunk(
    columnId: number,
    chunk: number,
    out: ChunkOutput,
    startRow = 0,
    step?: MemoryChunkStep,
  ): MaybePromise<ChunkResult> {
    this.view.snapshot.check();
    if (step !== undefined) return this.#bounded().chunk(columnId, chunk, out, startRow, step);
    const first = chunk * MEMORY_CHUNK_ROWS;
    const length = Math.min(MEMORY_CHUNK_ROWS, this.rowCount - first);
    if (!(length > 0)) throw new RangeError(`Chunk ${String(chunk)} is outside the segment`);
    if (!(Number.isInteger(startRow) && startRow >= 0 && startRow < length))
      throw new RangeError("startRow is outside the chunk");
    const published = this.memtable.published;
    if (published === null) {
      // One borrowed chunk per segment, charged to the write path until the borrow ends.
      this.memtable.borrow(chunk, this.borrowed);
      this.borrowed = chunk;
      const c = this.view.columnIndex(columnId);
      const stored = this.memtable.chunks[c]?.[chunk];
      if (stored === undefined) throw new RangeError("A memtable chunk is missing");
      return {
        vector: stored.view(length),
        firstRow: 0,
        rowCount: length,
        complete: true,
        reason: null,
      };
    }
    const root = groupRootOf(published.groupRoot);
    return root instanceof Promise
      ? root.then((resolved) =>
          this.#decodePublished(resolved, columnId, first, startRow, length, out),
        )
      : this.#decodePublished(root, columnId, first, startRow, length, out);
  }

  #decodePublished(
    root: GroupRoot,
    columnId: number,
    first: number,
    startRow: number,
    length: number,
    out: ChunkOutput,
  ): MaybePromise<ChunkResult> {
    const stored = root.columns.find((candidate) => candidate.columnId === columnId);
    if (stored === undefined)
      throw new RangeError(`The published group lacks column ${String(columnId)}`);
    const shared = this.view.snapshot.shared;
    const from = first + startRow;
    const to = first + length;
    return settle(
      ChunkReader.open(shared.space, stored, { budget: this.view.budget }),
      (reader) => {
        const [firstPage, endPage] = reader.pagesInRange(from, to);
        const pages = Array.from({ length: endPage - firstPage }, (_, k) => firstPage + k);
        // Out-of-line values (page/large-value.ts) decode only once loaded.
        return settle(reader.load(pages), () =>
          settle(reader.loadValues(from, to), () => {
            try {
              return decodeInto(reader, stored, from, to, startRow, out);
            } finally {
              reader.close();
            }
          }),
        );
      },
    );
  }
}

/** Decodes rows `[from, to)` of a published chunk into `out`, honouring varlen batch limits. */
function decodeInto(
  reader: ChunkReader,
  stored: StoredChunk,
  from: number,
  to: number,
  startRow: number,
  out: ChunkOutput,
): ChunkResult {
  const count = to - from;
  const varlenBytes =
    stored.type.kind === "text" ||
    stored.type.kind === "bytes" ||
    stored.type.kind === "json" ||
    stored.type.kind === "enum" ||
    stored.type.kind === "array"
      ? reader.varlenBytes(from, to)
      : 0;
  const temp: FlatVector = {
    form: "flat",
    type: stored.type,
    length: 0,
    validity: stored.statistics.nullCount === 0 ? null : new Uint32Array(Math.ceil(count / 32)),
    data: allocateData(stored.type, count, varlenBytes),
  };
  reader.decodeRange(from, to, temp, 0);
  temp.length = count;
  const target = out.flat;
  if (temp.data.layout !== "varlen" || out.varlen === null) {
    copyFlat(temp, target, count);
    return { vector: target, firstRow: startRow, rowCount: count, complete: true, reason: null };
  }
  const builder = out.varlen;
  const data = temp.data;
  let filled = 0;
  let reason: "full" | "denied" | null = null;
  for (; filled < count; filled++) {
    const valid =
      temp.validity === null || ((temp.validity[filled >>> 5] ?? 0) & (1 << (filled & 31))) !== 0;
    const result = valid
      ? builder.appendBytes(data.bytes, data.offsets[filled] ?? 0, data.offsets[filled + 1] ?? 0)
      : builder.appendEmpty();
    if (result !== "ok") {
      reason = result;
      break;
    }
  }
  const validity =
    temp.validity === null ? null : temp.validity.slice(0, Math.ceil(filled / 32) + 1);
  const vector: FlatVector = {
    form: "flat",
    type: stored.type,
    length: filled,
    validity,
    data: builder.data(),
  };
  return { vector, firstRow: startRow, rowCount: filled, complete: reason === null, reason };
}

function copyFlat(source: FlatVector, target: FlatVector, count: number): void {
  const s = source.data;
  const t = target.data;
  if (s.layout !== t.layout) throw new TypeError("Output layout differs from the column's");
  switch (s.layout) {
    case "bool":
    case "i32":
    case "f64":
    case "f32":
      (t as typeof s).values.set(s.values.subarray(0, count));
      break;
    case "int64": {
      const tv = (t as typeof s).values;
      if (tv.repr !== s.values.repr) throw new TypeError("Output int64 representation differs");
      (tv.values as Float64Array).set(s.values.values.subarray(0, count) as Float64Array);
      break;
    }
    default:
      // Decimal, fixed16, interval and varlen without a builder: hand back the decoded vector.
      Object.assign(target, { data: s, validity: source.validity, length: count });
      return;
  }
  target.validity = source.validity;
  target.length = count;
}

// ── Table snapshots ────────────────────────────────────────────────────────────────────────────

/** Column id → position, per table definition (definitions are immutable), computed once. */
const columnIndexOf = new WeakMap<TableDef, ReadonlyMap<number, number>>();

function columnIndex(def: TableDef): ReadonlyMap<number, number> {
  let index = columnIndexOf.get(def);
  if (index === undefined) {
    index = new Map(def.columns.map((column, c) => [column.id, c]));
    columnIndexOf.set(def, index);
  }
  return index;
}

export class TableSnapshotImpl implements TableSnapshot {
  readonly tableId: number;
  readonly schemaVersion: number;
  readonly columns: readonly ColumnDef[];
  #segments: readonly Segment[] | null = null;
  readonly #columnIndex: ReadonlyMap<number, number>;

  constructor(
    readonly snapshot: SnapshotImpl,
    readonly def: TableDef,
    readonly tableView: TableView,
    readonly budget: ByteBudget,
  ) {
    this.tableId = def.id;
    this.schemaVersion = def.schemaVersion;
    this.columns = def.columns;
    this.#columnIndex = columnIndex(def);
  }

  /** Built on first use (point reads and inserts never need them). */
  get segments(): readonly Segment[] {
    if (this.#segments === null) {
      const segments: Segment[] = [];
      const groups = this.tableView.groups;
      for (let position = 0; position < groups.count; position++)
        segments.push(new GroupSegmentImpl(segments.length, groups, position, this));
      for (const memtable of this.tableView.memtables)
        segments.push(new MemorySegmentImpl(segments.length, memtable, this));
      this.#segments = segments;
    }
    return this.#segments;
  }

  get liveRowCount(): number {
    let total = 0;
    for (const segment of this.segments) total += segment.liveRowCount;
    return total;
  }

  /** Ends every memory segment's borrow (the snapshot is released). */
  endBorrows(): void {
    if (this.#segments === null) return;
    for (const segment of this.#segments)
      if (segment instanceof MemorySegmentImpl && segment.borrowed >= 0) {
        segment.memtable.unborrow(segment.borrowed);
        segment.borrowed = -1;
      }
  }

  column(columnId: number): ColumnDef | undefined {
    const index = this.#columnIndex.get(columnId);
    return index === undefined ? undefined : this.columns[index];
  }

  columnIndex(columnId: number): number {
    const index = this.#columnIndex.get(columnId);
    if (index === undefined)
      throw new RangeError(`Table ${String(this.tableId)} has no column ${String(columnId)}`);
    return index;
  }

  /**
   * The locator of `rowId` in this view: segments are the groups (searched in the directory's
   * columns), then the memtables. Nothing per view is materialized.
   */
  locator(rowId: number): number {
    const memtables = this.tableView.memtables;
    const groups = this.tableView.groups;
    for (let k = memtables.length - 1; k >= 0; k--) {
      const first = need(memtables[k]).firstRowId;
      if (first <= rowId) return packLocator(groups.count + k, rowId - first);
    }
    // Last group whose first row id ≤ rowId.
    let lo = 0;
    let hi = groups.count;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (groups.firstRowId(mid) <= rowId) lo = mid + 1;
      else hi = mid;
    }
    const segment = lo - 1;
    if (segment < 0) throw new RangeError(`Row ${String(rowId)} is not in this view`);
    return packLocator(segment, rowId - groups.firstRowId(segment));
  }

  /** The row id a locator names. */
  rowId(locator: number): number {
    const segment = Math.floor(locator / 65_536);
    const groups = this.tableView.groups;
    const memtable = this.tableView.memtables[segment - groups.count];
    const first =
      segment < groups.count && segment >= 0 ? groups.firstRowId(segment) : memtable?.firstRowId;
    if (first === undefined) throw new RangeError("A locator names no segment of this view");
    return first + (locator % 65_536);
  }
}

// ── Snapshots ──────────────────────────────────────────────────────────────────────────────────

export interface SnapshotParts {
  readonly state: WriteState;
  readonly bound: number;
  readonly observedLsn: number;
  readonly catalog: CatalogView & { table(tableId: number): TableDef | undefined };
  readonly shared: Shared;
  readonly label: string;
  /** Statement views are released by their statement only. */
  readonly statementView: boolean;
}

export class SnapshotImpl implements Snapshot {
  readonly rootSequence: number;
  readonly bound: number;
  readonly observedLsn: number;
  readonly catalog: CatalogView;
  readonly shared: Shared;
  readonly state: WriteState;
  readonly pages: Shared["space"];
  readonly #pin: SpacePin;
  readonly #reader: number;
  readonly #statementView: boolean;
  #released = false;
  // Made on first use: most write-path views read one table through one index, or nothing.
  #indexes: Map<string, ViewIndex> | null = null;
  #tables: Map<number, TableSnapshotImpl> | null = null;
  #first: TableSnapshotImpl | null = null;

  constructor(parts: SnapshotParts) {
    this.state = parts.state;
    this.rootSequence = parts.state.root.sequence;
    this.bound = parts.bound;
    this.observedLsn = parts.observedLsn;
    this.catalog = parts.catalog;
    this.shared = parts.shared;
    this.pages = parts.shared.space;
    this.#pin = parts.shared.pinState(parts.state, parts.label);
    this.#reader = parts.shared.readers.open();
    this.#statementView = parts.statementView;
  }

  get released(): boolean {
    return this.#released;
  }

  check(): void {
    if (this.#released) throw new SnapshotReleasedError();
  }

  #def(tableId: number): { def: TableDef; view: TableView } {
    this.check();
    const def = this.catalog.table(tableId);
    const view = this.state.tables.get(tableId);
    if (def === undefined || view === undefined) throw new UnknownTableError(tableId);
    return { def, view };
  }

  table(tableId: number, options: TableReadOptions): TableSnapshotImpl {
    const { def, view } = this.#def(tableId);
    const table = new TableSnapshotImpl(this, def, view, options.budget);
    (this.#handed ??= []).push(table);
    return table;
  }

  /** The write path's own table view (constraint checks, index probes): one per table. */
  internalTable(tableId: number): TableSnapshotImpl {
    // One table needs no map (the usual case for write-path views).
    if (this.#first?.tableId === tableId) return this.#first;
    let table = this.#tables?.get(tableId);
    if (table === undefined) {
      const { def, view } = this.#def(tableId);
      table = new TableSnapshotImpl(this, def, view, this.shared.budget);
      if (this.#first === null) this.#first = table;
      else (this.#tables ??= new Map()).set(tableId, table);
    }
    return table;
  }

  index(tableId: number, indexId: number): ViewIndex {
    const key = `${String(tableId)}:${String(indexId)}`;
    this.#indexes ??= new Map();
    let reader = this.#indexes.get(key);
    if (reader === undefined) {
      const table = this.internalTable(tableId);
      const def = table.def.indexes.find((index) => index.id === indexId);
      if (def === undefined)
        throw new RangeError(`Table ${String(tableId)} has no index ${String(indexId)}`);
      reader = new ViewIndex(def, table);
      this.#indexes.set(key, reader);
    }
    return reader;
  }

  release(): void {
    if (this.#statementView) throw new TypeError("A statement view is released by its statement");
    this.releaseInternal();
  }

  /** Table snapshots handed out (their memory segments' borrows end with the snapshot). */
  #handed: TableSnapshotImpl[] | null = null;

  /** Metadata the snapshot's segments hold (group roots), released with it. */
  #holds: Array<{ release(): void }> | null = null;

  /** Keeps `held` until the snapshot is released (at once, if it already is). */
  hold(held: { release(): void }): void {
    if (this.#released) {
      held.release();
      return;
    }
    (this.#holds ??= []).push(held);
  }

  // Allocated only for bounded reads; ordinary snapshots keep their existing fast path.
  #cursors: Set<IndexCursor> | null = null;
  watchCursor(cursor: IndexCursor): () => void {
    this.check();
    const cursors = (this.#cursors ??= new Set());
    cursors.add(cursor);
    return () => {
      cursors.delete(cursor);
      if (this.#released && cursors.size === 0) this.#releasePin();
    };
  }
  #pinReleased = false;
  #releasePin(): void {
    if (this.#pinReleased) return;
    this.#pinReleased = true;
    this.#pin.release();
    this.shared.readers.close(this.#reader);
  }

  releaseInternal(): void {
    if (this.#released) return;
    this.#released = true;
    if (this.#holds !== null) for (const held of this.#holds) held.release();
    this.#holds = null;
    if (this.#handed !== null) for (const table of this.#handed) table.endBorrows();
    if (this.#cursors !== null) for (const cursor of this.#cursors) cursor.close();
    if ((this.#cursors?.size ?? 0) === 0) this.#releasePin();
  }
}

// ── Index reading ──────────────────────────────────────────────────────────────────────────────

interface Resume extends ProbeResume {
  /** Matches of `key` already written. */
  readonly skip: number;
}

/** Resolution of one key across the view's layers. */
const NO_RUNS: readonly RunRef[] = [];

/** A fresh single key beyond every other layer needs no constraint-check snapshot. */
export function otherIndexLayersEndBefore(
  table: TableView,
  indexId: number,
  key: Uint8Array,
  own: KeyDelta,
): boolean {
  const before = (max: Uint8Array) => compareBytes(max, 0, max.length, key, 0, key.length) < 0;
  for (const run of table.runs.get(indexId) ?? NO_RUNS) if (!before(run.maxKey)) return false;
  for (const memtable of table.memtables) {
    for (const run of memtable.staged?.runs.get(indexId) ?? NO_RUNS)
      if (!before(run.maxKey)) return false;
    const published = memtable.published;
    if (published !== null && memtable.deltasDropped) {
      const run = published.runs.get(indexId);
      if (run !== undefined && !before(run.maxKey)) return false;
      continue;
    }
    const delta = memtable.deltas.get(indexId);
    if (delta === undefined || delta === own) continue;
    const range = delta.keyRange();
    if (range === null ? delta.keyCount > 0 : !before(range.max)) return false;
  }
  return true;
}

export class ViewIndex implements IndexReader {
  /** Row ids a newer layer tombstoned (made on the first tombstone). */
  #deadRows: Set<number> | null = null;
  readonly #layer: RunEntry[] = [];

  get #dead(): Set<number> {
    return (this.#deadRows ??= new Set());
  }

  constructor(
    readonly def: IndexDef,
    readonly table: TableSnapshotImpl,
  ) {}

  /**
   * Live row ids of `key` under the view's bound, ascending (unique: at most one). Layers are
   * visited newest first: memtable deltas (or, once a memtable is published, its runs), then the
   * table's durable runs. Synchronous when every run block it needs is cached; otherwise the
   * Promise continues from the layer that had to read.
   */
  resolve(key: Uint8Array, out: number[]): MaybePromise<void> {
    return this.resolveRange(key, 0, key.length, out);
  }

  /** `resolve` of the key `bytes[start … end)` (a view is made only if a run is searched). */
  resolveRange(bytes: Uint8Array, start: number, end: number, out: number[]): MaybePromise<void> {
    const done = this.#begin(bytes, start, end);
    return done === undefined ? this.#deliver(out) : done.then(() => this.#deliver(out));
  }

  /** How many live rows hold the key `bytes[start … end)` (`resolve` without the row ids). */
  countRange(bytes: Uint8Array, start: number, end: number): MaybePromise<number> {
    const done = this.#begin(bytes, start, end);
    return done === undefined ? this.#hitCount : done.then(() => this.#hitCount);
  }

  /**
   * Row ids found by the current resolution, in `#hits[0 … #hitCount)`: written by index and never
   * truncated, so repeated resolutions reuse the array's storage.
   */
  readonly #hits: number[] = [];
  #hitCount = 0;

  #hit(rowId: number): void {
    if (this.#hitCount < this.#hits.length) this.#hits[this.#hitCount] = rowId;
    else this.#hits.push(rowId);
    this.#hitCount += 1;
  }

  #begin(bytes: Uint8Array, start: number, end: number): Promise<void> | undefined {
    this.#hitCount = 0;
    this.#deadRows?.clear();
    const done = this.#resolveFrom(bytes, start, end, 0, 0);
    return done instanceof Promise ? done : undefined;
  }

  #deliver(out: number[]): void {
    out.length = 0;
    for (let k = 0; k < this.#hitCount; k++) out.push(this.#hits[k] ?? 0);
    if (!this.def.unique && out.length > 1) out.sort((a, b) => a - b);
  }

  /**
   * Runs `resolve` may skip for keys within `[low, high]` (byte order): runs whose key range does
   * not overlap it. A batch of new keys checks every run's range once instead of once per key.
   */
  skipRunsOutside(low: Uint8Array | null, high: Uint8Array | null): void {
    this.#skip = null;
    if (low === null || high === null) return;
    let skip: Set<RunRef> | null = null;
    const outside = (run: RunRef) =>
      compareBytes(run.maxKey, 0, run.maxKey.length, low, 0, low.length) < 0 ||
      compareBytes(run.minKey, 0, run.minKey.length, high, 0, high.length) > 0;
    for (const run of this.table.tableView.runs.get(this.def.id) ?? NO_RUNS)
      if (outside(run)) (skip ??= new Set()).add(run);
    for (const memtable of this.table.tableView.memtables) {
      const run = memtable.published?.runs.get(this.def.id);
      if (run !== undefined && outside(run)) (skip ??= new Set()).add(run);
      for (const staged of memtable.staged?.runs.get(this.def.id) ?? NO_RUNS)
        if (outside(staged)) (skip ??= new Set()).add(staged);
    }
    this.#skip = skip;
  }

  #skip: Set<RunRef> | null = null;

  /**
   * Whether any layer but the delta `own` (the live memtable's, which a statement writes into)
   * could hold a key in `[low, high]` (byte order): durable runs, staged and published runs and
   * other memtables' deltas are compared by their key ranges (perf step 3).
   */
  othersOverlap(low: Uint8Array, high: Uint8Array, own: KeyDelta): boolean {
    const outside = (min: Uint8Array, max: Uint8Array) =>
      compareBytes(max, 0, max.length, low, 0, low.length) < 0 ||
      compareBytes(min, 0, min.length, high, 0, high.length) > 0;
    for (const run of this.table.tableView.runs.get(this.def.id) ?? NO_RUNS)
      if (!outside(run.minKey, run.maxKey)) return true;
    for (const memtable of this.table.tableView.memtables) {
      for (const run of memtable.staged?.runs.get(this.def.id) ?? NO_RUNS)
        if (!outside(run.minKey, run.maxKey)) return true;
      const published = memtable.published;
      if (published !== null && memtable.deltasDropped) {
        const run = published.runs.get(this.def.id);
        if (run !== undefined && !outside(run.minKey, run.maxKey)) return true;
        continue;
      }
      const delta = memtable.deltas.get(this.def.id);
      if (delta === undefined || delta === own) continue;
      const range = delta.keyRange();
      if (range === null ? delta.keyCount > 0 : !outside(range.min, range.max)) return true;
    }
    return false;
  }

  /**
   * Resolution from layer `from`, sub-layer `sub`. A memtable is one layer: sub-layer 0 is its
   * delta (or, once published and its deltas dropped, its published run), sub-layer j ≥ 1 its
   * staged run j − 1, whose entries count only for rows the view sees (runs carry no seq).
   */
  #resolveFrom(
    bytes: Uint8Array,
    start: number,
    end: number,
    from: number,
    sub: number,
  ): MaybePromise<void> {
    const bound = this.table.snapshot.bound;
    const unique = this.def.unique;
    const memtables = this.table.tableView.memtables;
    const runs = this.table.tableView.runs.get(this.def.id) ?? NO_RUNS;
    const layers = memtables.length + runs.length;
    const key = () => (start === 0 && end === bytes.length ? bytes : bytes.subarray(start, end));
    for (let layer = from; layer < layers; layer++, sub = 0) {
      if (layer < memtables.length) {
        const memtable = need(memtables[memtables.length - 1 - layer]);
        if (sub === 0) {
          const published = memtable.published;
          // Retained deltas answer for readers older than the publication (ruling 2).
          if (published === null || !memtable.deltasDropped) {
            if (this.#deltaLayer(memtable, bytes, start, end, bound) && unique) return;
          } else {
            const run = published.runs.get(this.def.id);
            if (run !== undefined && this.#skip?.has(run) !== true) {
              const pending = this.#runLayer(run, key(), layer, 1, Infinity);
              if (pending !== null) return pending;
              if (unique && this.#hitCount > 0) return;
            }
          }
        }
        const staged = memtable.staged?.runs.get(this.def.id);
        if (staged === undefined) continue;
        const limit = memtable.visibleRowIdEnd(bound);
        for (let k = Math.max(0, sub - 1); k < staged.length; k++) {
          const run = need(staged[k]);
          if (this.#skip?.has(run) === true) continue;
          const pending = this.#runLayer(run, key(), layer, k + 2, limit);
          if (pending !== null) return pending;
          if (unique && this.#hitCount > 0) return;
        }
        continue;
      }
      const run = runs[layer - memtables.length];
      if (run === undefined || this.#skip?.has(run) === true) continue;
      const pending = this.#runLayer(run, key(), layer, -1, Infinity);
      if (pending !== null) return pending;
      if (unique && this.#hitCount > 0) return;
    }
  }

  /** One delta layer; true when it added a live row. */
  #deltaLayer(
    memtable: Memtable,
    bytes: Uint8Array,
    start: number,
    end: number,
    bound: number,
  ): boolean {
    const delta = memtable.deltas.get(this.def.id);
    if (delta === undefined) return false;
    const keySlot = delta.find(bytes, start, end);
    if (keySlot < 0) return false;
    for (let entry = delta.head(keySlot); entry >= 0; entry = delta.nextEntry(entry))
      if (delta.isTombstone(entry) && delta.seq(entry) <= bound) this.#dead.add(delta.rowId(entry));
    let added = false;
    for (let entry = delta.head(keySlot); entry >= 0; entry = delta.nextEntry(entry)) {
      if (delta.isTombstone(entry) || delta.seq(entry) > bound) continue;
      const rowId = delta.rowId(entry);
      if (this.#deadRows?.has(rowId) === true) continue;
      this.#hit(rowId);
      added = true;
      if (this.def.unique) return true;
    }
    return added;
  }

  /**
   * One run in `layer`; a Promise that finishes resolution when it had to read, else null.
   * It continues at (`layer`, `next`), or at the next layer when `next` is −1. Puts at or past
   * `limit` (a row id) are not visible.
   */
  #runLayer(
    run: RunRef,
    key: Uint8Array,
    layer: number,
    next: number,
    limit: number,
  ): Promise<void> | null {
    const shared = this.table.snapshot.shared;
    const reader = shared.runReader(run);
    if (reader instanceof Promise)
      return reader.then(async (loaded) => {
        const pending = this.#readRunLayer(loaded, key, layer, next, limit);
        if (pending !== null) return await pending;
        if (this.def.unique && this.#hitCount > 0) return;
        await this.#continueRun(key, layer, next);
      });
    return this.#readRunLayer(reader, key, layer, next, limit);
  }

  /** Consume the reader returned by an asynchronous open, even if it cannot stay cached. */
  #readRunLayer(
    reader: RunReader,
    key: Uint8Array,
    layer: number,
    next: number,
    limit: number,
  ): Promise<void> | null {
    const shared = this.table.snapshot.shared;
    const entries = this.#layer;
    entries.length = 0;
    const read = reader.lookup(shared.space, key, entries, shared.budget);
    if (read instanceof Promise)
      return read.then(async () => {
        this.#applyLayer(entries, limit);
        if (this.def.unique && this.#hitCount > 0) return;
        await this.#continueRun(key, layer, next);
      });
    this.#applyLayer(entries, limit);
    return null;
  }

  #continueRun(key: Uint8Array, layer: number, next: number): MaybePromise<void> {
    return next < 0
      ? this.#resolveFrom(key, 0, key.length, layer + 1, 0)
      : this.#resolveFrom(key, 0, key.length, layer, next);
  }

  #applyLayer(layer: readonly RunEntry[], limit: number): void {
    for (const entry of layer) if (entry.tombstone) this.#dead.add(entry.rowId);
    for (const entry of layer) {
      if (entry.tombstone || entry.rowId >= limit || this.#deadRows?.has(entry.rowId) === true)
        continue;
      this.#hit(entry.rowId);
      if (this.def.unique) return;
    }
  }

  probe(keys: KeyBatch, out: ProbeOutput, resume?: ProbeResume): MaybePromise<ProbeResult> {
    this.table.snapshot.check();
    out.count = 0;
    const start = resume as Resume | undefined;
    return this.#probeFrom(keys, out, start?.key ?? 0, start?.skip ?? 0);
  }

  #probeFrom(
    keys: KeyBatch,
    out: ProbeOutput,
    from: number,
    skip: number,
  ): MaybePromise<ProbeResult> {
    const rows: number[] = [];
    for (let k = from; k < keys.count; k++) {
      const result = this.resolve(keys.key(k), rows);
      if (result instanceof Promise)
        return result.then(() => {
          // The completed call already produced these rows. Cache admission is optional.
          const resume = this.#copyProbeRows(out, k, k === from ? skip : 0, rows);
          return resume === null ? this.#probeFrom(keys, out, k + 1, 0) : { done: false, resume };
        });
      const resume = this.#copyProbeRows(out, k, k === from ? skip : 0, rows);
      if (resume !== null) return { done: false, resume };
    }
    return { done: true };
  }

  #copyProbeRows(
    out: ProbeOutput,
    key: number,
    first: number,
    rows: readonly number[],
  ): Resume | null {
    const capacity = Math.min(out.probes.length, out.locators.length);
    for (let m = first; m < rows.length; m++) {
      if (out.count >= capacity) return { key, skip: m };
      out.probes[out.count] = key;
      out.locators[out.count++] = this.table.locator(rows[m] ?? 0);
    }
    return null;
  }

  /**
   * Whether a live row holds `key`: exactly (all key columns), or, with `prefix`, as the leading
   * columns of this index's key (a foreign key's supporting index may cover more columns).
   */
  async exists(key: Uint8Array, prefix: boolean): Promise<boolean> {
    if (!prefix) return (await this.countRange(key, 0, key.length)) > 0;
    return (await this.collect(prefixRange(key))).length > 0;
  }

  /** Live row ids holding `key`, exactly or (with `prefix`) as the key's leading columns. */
  async holders(key: Uint8Array, prefix: boolean): Promise<number[]> {
    if (prefix) return (await this.collect(prefixRange(key))).map((entry) => entry.rowId);
    const rows: number[] = [];
    const result = this.resolve(key, rows);
    if (result instanceof Promise) await result;
    return rows;
  }

  range(range: KeyRange, direction: "asc" | "desc"): IndexCursor {
    if (this.def.kind !== "ordered") throw new TypeError("An equality index serves lookups only");
    return new RangeCursor(this, range, direction);
  }

  /** Every live (key, rowId) in `range`, resolved, sorted by key then row id. */
  async collect(range: KeyRange): Promise<Array<{ key: Uint8Array; rowId: number }>> {
    const snapshot = this.table.snapshot;
    const shared = snapshot.shared;
    const bound = snapshot.bound;
    const accept = (key: Uint8Array): -1 | 0 | 1 => {
      if (range.lower !== null) {
        const order = compareBytes(key, 0, key.length, range.lower.key, 0, range.lower.key.length);
        if (order < 0) return -1;
      }
      if (inKeyRange(range, key)) return 0;
      if (range.upper !== null) {
        const order = compareBytes(key, 0, key.length, range.upper.key, 0, range.upper.key.length);
        if (order > 0 && !startsWith(key, range.upper.key)) return 1;
      }
      return -1;
    };
    // Candidate keys from every layer, then resolve each key once.
    const keys = new Map<string, Uint8Array>();
    const add = (key: Uint8Array) => {
      if (inKeyRange(range, key)) keys.set(String.fromCharCode(...key), key.slice());
    };
    for (const memtable of this.table.tableView.memtables) {
      for (const run of memtable.staged?.runs.get(this.def.id) ?? []) {
        const reader = await shared.runReader(run);
        await reader.scan(shared.space, shared.budget, accept, (key) => add(key));
      }
      const published = memtable.published;
      if (published !== null && memtable.deltasDropped) {
        const run = published.runs.get(this.def.id);
        if (run !== undefined) {
          const reader = await shared.runReader(run);
          await reader.scan(shared.space, shared.budget, accept, (key) => add(key));
        }
        continue;
      }
      const delta = memtable.deltas.get(this.def.id);
      if (delta === undefined) continue;
      for (let slot = 0; slot < delta.keyCount; slot++) {
        let visible = false;
        for (let entry = delta.head(slot); entry >= 0; entry = delta.nextEntry(entry))
          if (delta.seq(entry) <= bound) visible = true;
        if (visible) add(delta.keyBytes(slot));
      }
    }
    for (const run of this.table.tableView.runs.get(this.def.id) ?? []) {
      const reader = await shared.runReader(run);
      await reader.scan(shared.space, shared.budget, accept, (key) => add(key));
    }
    const sorted = [...keys.values()].sort((a, b) => compareBytes(a, 0, a.length, b, 0, b.length));
    const out: Array<{ key: Uint8Array; rowId: number }> = [];
    const rows: number[] = [];
    for (const key of sorted) {
      const result = this.resolve(key, rows);
      if (result instanceof Promise) await result;
      for (const rowId of [...rows].sort((a, b) => a - b)) out.push({ key, rowId });
    }
    return out;
  }
}

function startsWith(key: Uint8Array, prefix: Uint8Array): boolean {
  if (key.length < prefix.length) return false;
  for (let k = 0; k < prefix.length; k++) if (key[k] !== prefix[k]) return false;
  return true;
}

class RangeCursor implements IndexCursor {
  #rows: number[] | null = null;
  #at = 0;
  #closed = false;

  constructor(
    readonly index: ViewIndex,
    readonly range: KeyRange,
    readonly direction: "asc" | "desc",
  ) {}

  next(out: Float64Array): MaybePromise<number> {
    this.index.table.snapshot.check();
    if (this.#closed) return 0;
    if (this.#rows === null)
      return this.index.collect(this.range).then((entries) => {
        const rows = entries.map((entry) => this.index.table.locator(entry.rowId));
        this.#rows = this.direction === "asc" ? rows : rows.reverse();
        return this.#fill(out);
      });
    return this.#fill(out);
  }

  #fill(out: Float64Array): number {
    const rows = this.#rows ?? [];
    let filled = 0;
    while (filled < out.length && this.#at < rows.length) out[filled++] = rows[this.#at++] ?? 0;
    return filled;
  }

  close(): void {
    this.#closed = true;
    this.#rows = null;
  }
}
