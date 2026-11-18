import { ftsMutationScan } from "./fts-undo.js";
import type { FtsClaimSeal } from "./fts-renderer.js";
import { taskClock, yieldTask } from "../task.js";
/**
 * Write transactions, statements and table writers (access/write-path.ts, access/modify.ts).
 *
 * ## Pending changes live in the shared structures
 * A transaction writes straight into the tables' live memtables, key deltas and deletion maps,
 * every change tagged `pendingSeq(statement)` (access/snapshot.ts), so nothing is copied at commit
 * and no reader sees it. An **undo log** records each change; rollback (of the transaction, a
 * statement or to a savepoint) walks it backwards, and commit walks it forwards to restamp the
 * changes with the commit LSN and to build the log body.
 *
 * ## Constraint timing (access/modify.ts)
 * NOT NULL, CHECK (`TableRules.check`) and the row-affected-twice rule run in the call. Unique keys
 * and foreign keys are checked at `Statement.end()`: during the statement a key that collides with
 * a live row is only a suspect, re-verified at the end against everything the statement wrote.
 */
import { requireWritableCatalogInput } from "../access/catalog-capabilities.js";
import { indexLimitViolation, indexMaximumKeyBytes } from "./index-limits.js";
import { scanIndexHolders } from "./index-scan.js";
import { SMALL_COMMIT_BYTES, type BufferAllocator } from "./commit-buffers.js";
import {
  CatalogChangeError,
  type CatalogChange,
  type IndexDef,
  type TableDef,
} from "../access/catalog.js";
import type { MaybePromise } from "../access/key-index.js";
import {
  CardinalityViolationError,
  CheckViolationError,
  ConflictKind,
  ForeignKeyViolationError,
  NotNullViolationError,
  ClosedSetViolationError,
  UniqueViolationError,
  type ConflictOutput,
  type Statement,
  type StatementOptions,
  type TableRules,
  type TableWriteOptions,
  type TableWriter,
} from "../access/modify.js";
import {
  type Snapshot,
  MAX_STATEMENTS,
  pendingSeq,
  statementBound,
  PENDING_BASE,
} from "../access/snapshot.js";
import {
  TransactionStateError,
  type CommitReceipt,
  type CommitRequest,
  type Savepoint,
  type WriteTransaction,
} from "../access/write-path.js";
import type { BodyEffect } from "../format/log-frame.js";
import { encodeKeyBatch, KeyBatch, type KeyColumn } from "../keys/key-encoding.js";
import { layoutOf, samePhysicalType } from "../types.js";
import type { WriteEvent } from "../access/statement-command.js";
import { int64At } from "../vector/int64.js";
import { compareBytes, varlenMatchesString } from "../vector/varlen.js";
import {
  BATCH_TARGET_BYTES,
  type Batch,
  type FlatVector,
  type Selection,
  type Vector,
} from "../vector/vector.js";
import type { ByteWriter } from "./bytes.js";
import { Catalog, encodeLogged, type LoggedChange } from "./catalog.js";
import { ColumnChunk, encodeColumn, isNullAt, sourceSlot, MEMORY_CHUNK_ROWS } from "./columns.js";
import type { KeyDelta } from "./key-hash.js";
import type { Memtable } from "./memtable.js";
import { readRows } from "./rows.js";
import { validateAlterNullability } from "./alter-validation.js";
import type { GroupDeletes, Shared, WriteState } from "./state.js";
import {
  SnapshotImpl,
  ViewIndex,
  otherIndexLayersEndBefore,
  type TableSnapshotImpl,
} from "./view.js";
import { need, MemtableMemoryError, byteString } from "./bytes.js";
import type { PreparedWriteCredit } from "./credits.js";
import { NativePrimaryAdapter, type NativePrimaryScope } from "./primary-command.js";
import type {
  NativeWriteStepBoundary,
  NativeWriteStepOptions,
} from "../access/native-write-step.js";

/** Everything committed and pending (constraint checks see the whole transaction). */
export const ALL = PENDING_BASE + MAX_STATEMENTS;
/** Prepaid embedded notification slot; remains owned with row undo, not the statement. */
const FTS_ROW_UNDO_BYTES = 32;

/** What transactions need from the write path. */
export interface EngineHooks {
  prepareNativeStep?(
    statement: StatementImpl,
    bytes: number,
    signal: AbortSignal,
    options?: NativeWriteStepOptions,
  ): Promise<NativeWriteStepBoundary>;
  checkNativeStepReady?(statement: StatementImpl): void;
  closeNativeStep?(statement: StatementImpl): Promise<void>;
  readonly shared: Shared;
  /** The one key batch writers encode into (one writer at a time; consumed before reuse). */
  readonly keys: KeyBatch;
  state(): WriteState;
  committedCatalog(): Catalog;
  nextCatalogVersion(): number;
  /** Prepay schema-dependent metadata before synchronous DDL state changes. */
  prepareCatalog(catalog: Catalog): MaybePromise<void>;
  /**
   * The table's live memtable with room, freezing a full one first: `created` is set when this
   * call created the memtable (and `frozen` is the one it froze, if any).
   */
  liveMemtable(
    tableId: number,
    rows?: number,
  ): {
    readonly memtable: Memtable;
    readonly created: boolean;
    readonly frozen: Memtable | null;
  };
  /**
   * Undoes a freeze: drops `created` (now empty). `frozen` may become live again only if no
   * in-flight flush owns its immutable rows; later inserts otherwise use another memtable.
   */
  undoFreeze(tableId: number, created: Memtable, frozen: Memtable | null): void;
  /** Creates (or, with null, removes) the in-memory state of a table this transaction creates. */
  createTableState(def: TableDef | null, tableId: number): void;
  /** Adds an index's delta to the table's live memtable (createIndex). */
  addIndexDelta(tableId: number, indexId: number): Memtable;
  /** Ensures memtable memory for about `bytes` more before an insert (backpressure). */
  admit(bytes: number, growth?: () => number): MaybePromise<void>;
  /** End-of-statement admission after producer borrows and check views retire. */
  prepareFtsMutation?(signal?: AbortSignal): MaybePromise<void>;
  /** Command primary prepayment, before the root borrows a decoded image. */
  reserveCommandRows?(
    bytes: number,
    growth: () => number,
    signal: AbortSignal,
  ): Promise<PreparedWriteCredit>;
  commit(tx: TransactionImpl, request: CommitRequest | null): Promise<CommitReceipt>;
  release(tx: TransactionImpl): void;
  label(): string;
  advanceIdentity(sequenceId: number, atLeast: bigint): MaybePromise<void>;
}

export type Undo =
  | {
      readonly kind: "rows";
      /** Present only on native, funded entries; mutation rolls back with this accepted range. */
      ftsKeyed?: boolean;
      readonly statement: number;
      readonly tableId: number;
      readonly memtable: Memtable;
      readonly before: number;
      readonly count: number;
    }
  | {
      /** Entries `entry … entry + count − 1` of one delta, added in order. */
      readonly kind: "delta";
      readonly statement: number;
      readonly delta: KeyDelta;
      readonly entry: number;
      readonly count: number;
    }
  | {
      readonly kind: "memDelete";
      readonly statement: number;
      readonly tableId: number;
      readonly memtable: Memtable;
      readonly position: number;
      readonly rowId: number;
      readonly keys: readonly Uint8Array[];
      readonly schemaVersion: number;
      readonly indexIds: readonly number[];
    }
  | {
      readonly kind: "groupDelete";
      readonly statement: number;
      readonly tableId: number;
      readonly deletes: GroupDeletes;
      readonly groupId: number;
      readonly position: number;
      readonly rowId: number;
      readonly keys: readonly Uint8Array[];
      readonly schemaVersion: number;
      readonly indexIds: readonly number[];
    }
  | {
      readonly kind: "freeze";
      readonly statement: number;
      readonly tableId: number;
      readonly created: Memtable;
      readonly frozen: Memtable | null;
    }
  | {
      readonly kind: "catalog";
      readonly statement: number;
      readonly before: Catalog;
      readonly logged: readonly LoggedChange[];
    }
  | {
      readonly kind: "indexDelta";
      readonly statement: number;
      readonly memtable: Memtable;
      readonly indexId: number;
    }
  | {
      /** A change staging made durable (a staged memtable's own index puts): nothing to undo. */
      readonly kind: "staged";
      readonly statement: number;
    };

/** What a commit's `publishedRows` effect names for one table (the engine's staging). */
export interface PublishedRows {
  readonly transaction: number;
  readonly rootSequence: number;
  readonly indexIds: readonly number[];
}

const settle = <T, R>(
  value: MaybePromise<T>,
  then: (value: T) => MaybePromise<R>,
): MaybePromise<R> => (value instanceof Promise ? value.then(then) : then(value));

function all(values: ReadonlyArray<MaybePromise<void>>): MaybePromise<void> {
  const pending = values.filter((value): value is Promise<void> => value instanceof Promise);
  return pending.length === 0 ? undefined : Promise.all(pending).then(() => undefined);
}

/** A bounded slice of physical rows or of a selected-row list. No per-row iterator/result. */
interface RowPlan {
  readonly rows: Uint16Array | null;
  readonly from: number;
  readonly count: number;
}

function activeRows(batch: Batch): Uint16Array {
  if (batch.selection !== null) return batch.selection.rows.subarray(0, batch.selection.count);
  const rows = new Uint16Array(batch.length);
  for (let k = 0; k < batch.length; k++) rows[k] = k;
  return rows;
}

function hexId(bytes: Uint8Array): string {
  let out = "";
  for (const byte of bytes) out += byte.toString(16).padStart(2, "0");
  return out;
}

export class TransactionImpl implements WriteTransaction {
  readonly baseLsn: number;
  state: WriteTransaction["state"] = "open";
  catalog: Catalog;
  readonly undo: Undo[] = [];
  #ftsUndoBytes = 0;
  /** Working minimum retained through publication; rollback may leave safe spare credit. */
  ftsMutationPrepared = false;

  /** Fund the embedded notification before native row append constructs its undo. */
  reserveFtsRowUndo(): void {
    if (!this.hooks.shared.budget.allocate(FTS_ROW_UNDO_BYTES))
      throw new MemtableMemoryError(FTS_ROW_UNDO_BYTES);
    this.#ftsUndoBytes += FTS_ROW_UNDO_BYTES;
  }
  releaseFtsRowUndo(): void {
    this.#ftsUndoBytes -= FTS_ROW_UNDO_BYTES;
    this.hooks.shared.budget.free(FTS_ROW_UNDO_BYTES);
  }
  /** Prepared and protected by the engine before either commit codec is called. */
  ftsEffect: Extract<BodyEffect, { kind: "fts" }> | null = null;
  ftsSeal: FtsClaimSeal | null = null;
  #ordinal = 0;
  #open: StatementImpl | null = null;
  #savepointMap: Map<number, number> | null = null;

  get #savepoints(): Map<number, number> {
    return (this.#savepointMap ??= new Map());
  }
  /** Requested on the transaction by the engine at commit. */
  readonly hooks: EngineHooks;

  constructor(hooks: EngineHooks, baseLsn: number) {
    this.hooks = hooks;
    this.baseLsn = baseLsn;
    this.catalog = hooks.committedCatalog();
  }

  #requireOpen(): void {
    if (this.state !== "open") throw new TransactionStateError(`The transaction is ${this.state}`);
  }

  statement(options: StatementOptions): StatementImpl {
    this.#requireOpen();
    if (this.#open !== null)
      throw new TransactionStateError("The previous statement has not ended");
    this.#ordinal += 1;
    const statement = new StatementImpl(this, this.#ordinal, options);
    this.#open = statement;
    return statement;
  }

  /** @internal */
  ended(statement: StatementImpl): void {
    if (this.#open === statement) this.#open = null;
  }

  get nativeCommands(): boolean {
    return this.#open?.nativeCommandMode === true;
  }

  checkCurrentStatement(statement: StatementImpl): void {
    this.#requireOpen();
    if (this.#open !== statement) throw new TransactionStateError("Foreign statement");
  }

  /** Owner close fences command callbacks before synchronous native rollback. */
  closeNativeCommands(): Promise<void> {
    return this.#open?.closeNativeCommands() ?? RESOLVED;
  }

  savepoint(): Savepoint {
    this.#requireOpen();
    if (this.#open !== null) throw new TransactionStateError("A statement is open");
    this.#savepoints.set(this.#ordinal, this.undo.length);
    return { statement: this.#ordinal };
  }

  /**
   * Staging replaced a memtable's key deltas (`replaced`: old delta → new delta and, per old
   * entry, its index in the new one or −1 for an own put the staged run now holds). Undo entries
   * keep their places (savepoints and statements hold undo lengths): an own-put entry becomes
   * `staged`, any other points at the new delta.
   */
  remapDeltas(replaced: ReadonlyMap<KeyDelta, { delta: KeyDelta; entries: Int32Array }>): void {
    for (let k = 0; k < this.undo.length; k++) {
      const entry = need(this.undo[k]);
      if (entry.kind !== "delta") continue;
      const map = replaced.get(entry.delta);
      if (map === undefined) continue;
      const first = map.entries[entry.entry] ?? -1;
      this.undo[k] =
        first < 0
          ? { kind: "staged", statement: entry.statement }
          : { ...entry, delta: map.delta, entry: first };
    }
  }

  rollbackTo(savepoint: Savepoint): void {
    this.#requireOpen();
    if (this.#open !== null) throw new TransactionStateError("A statement is open");
    const length = this.#savepoints.get(savepoint.statement);
    if (length === undefined) throw new TransactionStateError("Unknown savepoint");
    this.undoTo(length);
    for (const statement of this.#savepoints.keys())
      if (statement > savepoint.statement) this.#savepoints.delete(statement);
  }

  release(savepoint: Savepoint): void {
    for (const statement of [...this.#savepoints.keys()])
      if (statement >= savepoint.statement) this.#savepoints.delete(statement);
  }

  /** Undoes the log back to `length` entries. */
  undoTo(length: number): void {
    while (this.undo.length > length) {
      const entry = this.undo.pop();
      if (entry === undefined) break;
      undoEntry(this, entry);
      if (entry.kind === "rows" && entry.ftsKeyed !== undefined) this.releaseFtsRowUndo();
    }
  }

  async commit(request: CommitRequest | null): Promise<CommitReceipt> {
    this.#requireOpen();
    if (this.#open !== null) throw new TransactionStateError("A statement is still open");
    this.state = "committing";
    try {
      const receipt = await this.hooks.commit(this, request);
      // The engine consumed codecs, restamping and DDL iteration before releasing the slot.
      // Drop undo objects before refunding their embedded notification slots.
      this.undo.length = 0;
      this.hooks.shared.budget.free(this.#ftsUndoBytes);
      this.#ftsUndoBytes = 0;
      this.state = "committed";
      return receipt;
    } catch (error) {
      this.state = "failed";
      throw error;
    }
  }

  rollback(): void {
    if (this.state !== "open") return;
    this.#open?.rollback();
    this.undoTo(0);
    this.state = "rolledBack";
    this.hooks.release(this);
  }

  /**
   * The log body's effects. Inserted columns are views into `scratch` (reused across commits):
   * valid until the body is encoded.
   */
  effects(
    scratch: ByteWriter,
    staged: (tableId: number) => PublishedRows | null = () => null,
    allocator?: BufferAllocator,
  ): BodyEffect[] {
    scratch.reset();
    const effects: BodyEffect[] = [];
    // Column byte ranges in `scratch` as (effect's column list, start, end) triples: views are
    // made once every column is encoded (the scratch buffer may grow and move until then).
    const columnLists: Uint8Array[][] = [];
    const spans: number[] = [];
    // Row changes accumulate between catalog changes and are emitted before each one, so the body
    // lists effects in the order the transaction made them (format/replay.ts "ordered effects").
    // Usually one memtable: a short list beats a map.
    const inserts: Array<{ memtable: Memtable; from: number; to: number }> = [];
    // A staged table's rows are one `publishedRows` effect, at its first staged memtable.
    const published = new Set<number>();
    let deletes: Map<
      number,
      Array<{
        rowId: number;
        keys: readonly Uint8Array[];
        schemaVersion: number;
        indexIds: readonly number[];
      }>
    > | null = null;
    const emitRows = () => {
      for (const range of inserts) {
        const memtable = range.memtable;
        if (memtable.staged !== null) {
          if (published.has(memtable.tableId) || memtable.rowCount === 0) continue;
          published.add(memtable.tableId);
          const names = need(staged(memtable.tableId));
          effects.push({
            kind: "publishedRows",
            tableId: memtable.tableId,
            schemaVersion: memtable.schemaVersion,
            transaction: names.transaction,
            rootSequence: names.rootSequence,
            indexIds: names.indexIds,
          });
          continue;
        }
        for (let from = range.from; from < range.to;) {
          const chunk = Math.floor(from / MEMORY_CHUNK_ROWS);
          const slot = from % MEMORY_CHUNK_ROWS;
          const end = Math.min(range.to, (chunk + 1) * MEMORY_CHUNK_ROWS);
          const count = end - from;
          const columns: Uint8Array[] = [];
          const list = columnLists.length;
          columnLists.push(columns);
          for (const stored of memtable.chunks) {
            const chunkData = stored[chunk];
            if (chunkData === undefined)
              throw new RangeError("A pending memtable chunk is missing");
            const start = scratch.length;
            encodeColumn(chunkData.view(), slot, count, scratch);
            spans.push(list, start, scratch.length);
          }
          effects.push({
            kind: "rows",
            tableId: memtable.tableId,
            schemaVersion: memtable.schemaVersion,
            inserts: {
              firstRowId: memtable.firstRowId + from,
              rowCount: count,
              columns,
              columnIds: memtable.columns.map((column) => column.id),
            },
            deletes: null,
          });
          from = end;
        }
      }
      if (deletes !== null)
        for (const [tableId, list] of deletes) {
          const { schemaVersion, indexIds } = need(list[0]);
          list.sort((x, y) => x.rowId - y.rowId);
          const rowIds =
            allocator === undefined
              ? Float64Array.from(list, (item) => item.rowId)
              : new Float64Array(allocator.create(list.length * 8).buffer);
          if (allocator !== undefined)
            for (let k = 0; k < list.length; k++) rowIds[k] = need(list[k]).rowId;
          effects.push({
            kind: "rows",
            tableId,
            schemaVersion,
            inserts: null,
            deletes: {
              rowIds,
              indexIds,
              keys: list.map((item) => item.keys),
            },
          });
        }
      inserts.length = 0;
      deletes?.clear();
    };
    for (const entry of this.undo) {
      switch (entry.kind) {
        case "rows": {
          const range = inserts.find((candidate) => candidate.memtable === entry.memtable);
          if (range === undefined)
            inserts.push({
              memtable: entry.memtable,
              from: entry.before,
              to: entry.before + entry.count,
            });
          else range.to = entry.before + entry.count;
          break;
        }
        case "memDelete":
        case "groupDelete": {
          deletes ??= new Map();
          let list = deletes.get(entry.tableId);
          if (list === undefined) {
            list = [];
            deletes.set(entry.tableId, list);
          }
          list.push({
            rowId: entry.rowId,
            keys: entry.keys,
            schemaVersion: entry.schemaVersion,
            indexIds: entry.indexIds,
          });
          break;
        }
        case "catalog":
          emitRows();
          for (const change of entry.logged) effects.push(catalogEffect(change, allocator));
          break;
        default:
          break;
      }
    }
    emitRows();
    // Column bytes are final now: point the effects at views of the scratch buffer.
    for (let k = 0; k < spans.length; k += 3)
      need(columnLists[spans[k] ?? 0]).push(scratch.bytes.subarray(spans[k + 1], spans[k + 2]));
    for (const mutation of ftsMutationScan(this)) if (mutation !== null) effects.push(mutation);
    if (this.ftsEffect) effects.push(this.ftsEffect);
    return effects;
  }

  /** Conservative temporary object/array bound; large workspaces use the streaming codec. */
  commitWorkspaceBytes(): number {
    let bytes = 512;
    for (const entry of this.undo) {
      bytes += 64;
      if (entry.kind === "rows") {
        const chunks = Math.ceil(
          ((entry.before % MEMORY_CHUNK_ROWS) + entry.count) / MEMORY_CHUNK_ROWS,
        );
        bytes += chunks * (192 + entry.memtable.chunks.length * 192);
      } else if (entry.kind === "memDelete" || entry.kind === "groupDelete") bytes += 96;
      else if (entry.kind === "catalog") bytes += 192 * entry.logged.length;
      if (bytes > SMALL_COMMIT_BYTES) break;
    }
    return bytes;
  }

  /** Large metadata must be counted before the fast codec allocates definitions or row IDs. */
  needsCommitPreflight(): boolean {
    let bytes = 0;
    for (const entry of this.undo) {
      if (entry.kind === "memDelete" || entry.kind === "groupDelete") {
        bytes += 8;
        for (const key of entry.keys) bytes += key.length + 8;
      } else if (entry.kind === "catalog") {
        for (const change of entry.logged) bytes += jsonBound(change, SMALL_COMMIT_BYTES - bytes);
      }
      if (bytes > SMALL_COMMIT_BYTES) return true;
    }
    return false;
  }

  *loggedChanges(): Generator<LoggedChange> {
    for (const entry of this.undo) if (entry.kind === "catalog") yield* entry.logged;
  }

  /** A table's schema version in the transaction's catalog, or the committed one (dropped later). */
  tableVersion(tableId: number): number | undefined {
    return (this.catalog.table(tableId) ?? this.hooks.committedCatalog().table(tableId))
      ?.schemaVersion;
  }

  /** Restamps every pending change with `lsn` (after the frame is appended). */
  restamp(lsn: number): void {
    // Restamping a memtable is idempotent: consecutive entries of one memtable restamp it once.
    let last: Memtable | null = null;
    for (const entry of this.undo) {
      switch (entry.kind) {
        case "rows":
          if (entry.memtable !== last) entry.memtable.restamp(PENDING_BASE, lsn);
          last = entry.memtable;
          break;
        case "delta":
          entry.delta.restampRange(entry.entry, entry.entry + entry.count, lsn);
          break;
        case "memDelete":
          entry.memtable.deletions.set(entry.position, lsn);
          break;
        case "groupDelete":
          entry.deletes.restamp(entry.groupId, entry.position, lsn);
          break;
        default:
          break;
      }
    }
  }
}

/** Conservative JSON bound without an encoded allocation; exact counting is only on this lane. */
export function jsonBound(value: unknown, limit: number): number {
  if (typeof value === "string") return 2 + value.length * 6;
  if (typeof value === "bigint") return 20 + value.toString().length;
  if (value instanceof Uint8Array) return 16 + value.length * 4;
  if (typeof value !== "object" || value === null) return 32;
  let bytes = 2;
  for (const key in value) {
    if (!Object.hasOwn(value, key)) continue;
    bytes += key.length * 6 + 4 + jsonBound((value as Record<string, unknown>)[key], limit - bytes);
    if (bytes > limit) break;
  }
  return bytes;
}

function catalogEffect(change: LoggedChange, allocator?: BufferAllocator): BodyEffect {
  if (change.op === "alterTable")
    return {
      kind: "alterTable",
      tableId: change.table.id,
      schemaVersion: change.table.schemaVersion,
      definition: encodeLogged(change, allocator),
    };
  if (change.op === "createTable")
    return {
      kind: "createTable",
      tableId: change.table.id,
      definition: encodeLogged(change, allocator),
    };
  if (change.op === "dropTable")
    return {
      kind: "dropTable",
      tableId: change.tableId,
      definition: encodeLogged(change, allocator),
    };
  return { kind: "catalog", definition: encodeLogged(change, allocator) };
}

function undoEntry(tx: TransactionImpl, entry: Undo): void {
  switch (entry.kind) {
    case "rows":
      entry.memtable.truncate(entry.before, pendingSeq(entry.statement));
      break;
    case "delta":
      for (let added = entry.entry + entry.count - 1; added >= entry.entry; added--)
        entry.delta.pop(added);
      break;
    case "memDelete":
      entry.memtable.deletions.delete(entry.position);
      break;
    case "groupDelete":
      entry.deletes.remove(entry.groupId, entry.position);
      break;
    case "freeze":
      tx.hooks.undoFreeze(entry.tableId, entry.created, entry.frozen);
      break;
    case "catalog":
      for (const change of entry.logged)
        if (change.op === "createTable") tx.hooks.createTableState(null, change.table.id);
      tx.catalog = entry.before;
      break;
    case "indexDelta":
      entry.memtable.removeDelta(entry.indexId);
      break;
    case "staged":
      break;
  }
}

const RESOLVED: Promise<void> = Promise.resolve();

const NONE: ReadonlySet<number> = new Set();
/** Per catalog (immutable): table id → its indexes other tables' enforced foreign keys name. */
const referencedOf = new WeakMap<Catalog, Map<number, ReadonlySet<number>>>();

function referencedIndexes(catalog: Catalog, tableId: number): ReadonlySet<number> {
  let tables = referencedOf.get(catalog);
  if (tables === undefined) {
    const built = new Map<number, Set<number>>();
    for (const table of catalog.tables)
      for (const fk of table.foreignKeys) {
        if (!fk.enforced) continue;
        let indexes = built.get(fk.parentTableId);
        if (indexes === undefined) built.set(fk.parentTableId, (indexes = new Set()));
        indexes.add(fk.parentIndexId);
      }
    tables = built;
    referencedOf.set(catalog, tables);
  }
  return tables.get(tableId) ?? NONE;
}

/** An index definition read as non-unique (constraint checks count every live row). */
const nonUniqueOf = new WeakMap<IndexDef, IndexDef>();

function nonUnique(def: IndexDef): IndexDef {
  let read = nonUniqueOf.get(def);
  if (read === undefined) {
    read = { ...def, unique: false };
    nonUniqueOf.set(def, read);
  }
  return read;
}

// ── Statements ─────────────────────────────────────────────────────────────────────────────────

interface FkCheck {
  readonly childTableId: number;
  /** The child row version the check is for: skipped if the statement deleted it since. */
  readonly rowId: number;
  readonly fkId: number;
  readonly parentTableId: number;
  readonly parentIndexId: number;
  readonly key: Uint8Array;
}

interface ParentCheck {
  readonly parentTableId: number;
  readonly indexId: number;
  /** The key that left the parent (deleted, or changed by an UPDATE). */
  readonly key: Uint8Array;
  /**
   * For a rekey (an UPDATE that changed the key): the new row, whose key columns the children's
   * references follow. Null for a delete.
   */
  readonly next: { readonly vectors: readonly Vector[]; readonly row: number } | null;
}

export class StatementImpl implements Statement {
  readonly ordinal: number;
  readonly seq: number;
  readonly tx: TransactionImpl;
  readonly #rules: (tableId: number) => TableRules;
  #signal: AbortSignal | undefined;
  readonly #start: number;
  #state: "open" | "failed" | "ended" = "open";
  #writers: Map<string, TableWriterImpl> | null = null;
  #plain: TableWriterImpl | null = null;
  #commandMode = false;
  #commands: NativePrimaryAdapter | null = null;
  /** Row ids this statement updated or deleted, per table (row-affected-twice). */
  #touchedMap: Map<number, Set<number>> | null = null;
  readonly targets: StatementOptions["targets"];

  get touched(): Map<number, Set<number>> {
    return (this.#touchedMap ??= new Map());
  }
  /** Whether this statement inserted row `rowId` of `tableId` (ON CONFLICT `statement`). */
  inserted(tableId: number, rowId: number): boolean {
    // Native IDs only advance. A row below the statement-visible high-water mark
    // cannot have been inserted by this statement. Do not rescan its entire growing
    // undo stream for every existing-row upsert; inspect only immutable view facts.
    const start = this.view.internalTable(tableId);
    const groups = start.tableView.groups;
    let before = groups.count === 0 ? -1 : groups.get(groups.count - 1).lastRowId;
    for (const memtable of start.tableView.memtables) {
      const count = memtable.visibleRows(this.view.bound);
      if (count > 0) before = Math.max(before, memtable.firstRowId + count - 1);
    }
    if (rowId <= before) return false;
    const undo = this.tx.undo;
    for (let k = this.#start; k < undo.length; k++) {
      const entry = undo[k];
      if (entry?.kind !== "rows" || entry.tableId !== tableId) continue;
      const first = entry.memtable.firstRowId + entry.before;
      if (rowId >= first && rowId < first + entry.count) return true;
    }
    return false;
  }
  /** Notification only: ordinary update/delete remain covered by their native undo. */
  noteFtsMutation(tableId: number, kind: "update" | "delete" | "upsert"): void {
    this.#checkOpen();
    switch (kind) {
      case "update":
      case "delete":
      case "upsert":
        break;
      default:
        throw new TypeError("Unknown FTS mutation kind");
    }
    if (!this.catalog.table(tableId)) throw new RangeError("Unknown FTS mutation table");
    for (let i = this.tx.undo.length - 1; i >= this.#start; i--) {
      const e = this.tx.undo[i];
      if (e?.kind === "rows" && e.tableId === tableId && e.count > 0) {
        e.ftsKeyed = true;
        return;
      }
      if ((e?.kind === "memDelete" || e?.kind === "groupDelete") && e.tableId === tableId) return;
    }
    // No accepted row in this statement (including DO NOTHING): no history or allocation.
  }

  readonly suspects: Array<{ tableId: number; indexId: number; key: Uint8Array }> = [];
  readonly childChecks: FkCheck[] = [];
  /** Row ids this statement deleted, per table, while child checks were pending. */
  readonly deleted = new Map<number, Set<number>>();
  readonly parentChecks: ParentCheck[] = [];

  constructor(tx: TransactionImpl, ordinal: number, options: StatementOptions) {
    this.tx = tx;
    this.ordinal = ordinal;
    this.seq = pendingSeq(ordinal);
    this.#rules = options.rules;
    this.#signal = options.signal;
    this.targets = options.targets;
    this.#start = tx.undo.length;
    this.#startCatalog = tx.catalog;
  }

  #view: SnapshotImpl | null = null;
  /** The catalog when the statement began (what its view shows). */
  readonly #startCatalog: Catalog;

  /**
   * The statement view, made on first use (an INSERT of values never reads one). Rows are
   * visible by seq (`statementBound`), so a view made later shows what one made at the start
   * would; the catalog is the one the statement began with.
   */
  get view(): SnapshotImpl {
    this.#view ??= new SnapshotImpl({
      state: this.tx.hooks.state(),
      bound: statementBound(this.ordinal),
      observedLsn: this.tx.baseLsn,
      catalog: this.#startCatalog,
      shared: this.tx.hooks.shared,
      label: `${this.tx.hooks.label()}/statement#${String(this.ordinal)}`,
      statementView: true,
    });
    return this.#view;
  }

  get catalog(): Catalog {
    return this.tx.catalog;
  }

  rules(tableId: number): TableRules {
    return this.#rules(tableId);
  }

  /** Internal composition capability; no SQL caller installs it until its whole route is ready. */
  openNativeCommands(scope: Omit<NativePrimaryScope, "statement">): NativePrimaryAdapter {
    this.#checkOpen();
    if (this.#commandMode || this.tx.undo.length !== this.#start)
      throw new TransactionStateError("Native command scope must precede statement writes");
    const commands = new NativePrimaryAdapter({ ...scope, statement: this });
    this.#commands = commands;
    this.#commandMode = true;
    return commands;
  }

  /** Existing/retained legacy writers must not bypass command ownership after installation. */
  checkLegacyWrite(): void {
    this.#checkOpen();
    if (this.#commandMode)
      throw new TransactionStateError("Native command writes require their primary capability");
  }

  checkCommandPrimary(commands: NativePrimaryAdapter): void {
    this.#checkOpen();
    if (this.#commands !== commands)
      throw new TransactionStateError("Foreign native command primary capability");
  }

  async closeNativeCommands(): Promise<void> {
    const results = await Promise.allSettled([
      this.#commands?.close() ?? RESOLVED,
      this.tx.hooks.closeNativeStep?.(this) ?? RESOLVED,
    ]);
    const errors = results.flatMap((r) => (r.status === "rejected" ? [r.reason as unknown] : []));
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) throw new AggregateError(errors, "Native statement close failed");
  }

  get nativeCommandMode(): boolean {
    return this.#commandMode;
  }

  checkNativeFunding(): void {
    this.#checkOpen();
    this.tx.checkCurrentStatement(this);
  }

  /** No image, source borrow or loan may span this funding boundary. A source's
   * caller releases the witness before running SQL with its admitted workspace. */
  prepareNativeStep(
    bytes: number,
    signal: AbortSignal,
    options?: NativeWriteStepOptions,
  ): Promise<NativeWriteStepBoundary> {
    this.checkNativeFunding();
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Explicit owner in call below.
    const prepare = this.tx.hooks.prepareNativeStep;
    if (!prepare) throw new TransactionStateError("Native write funding is unavailable");
    return prepare.call(this.tx.hooks, this, bytes, signal, options);
  }

  /** Every active view owned by this statement, irrespective of query pin order. */
  get nativeReaderCount(): number {
    let count = this.#view && !this.#view.released ? 1 : 0;
    if (this.#current && this.#current !== this.#view && !this.#current.released) count++;
    if (this.#ownReads) for (const view of this.#ownReads) if (!view.released) count++;
    return count;
  }

  /** Runs `work`; any error fails the statement (its only valid call is then `rollback`). */
  guard<T>(work: () => MaybePromise<T>): MaybePromise<T> {
    this.#checkOpen();
    try {
      const result = work();
      if (result instanceof Promise)
        return result.catch((error: unknown) => {
          this.#state = "failed";
          throw error;
        });
      return result;
    } catch (error) {
      this.#state = "failed";
      throw error;
    }
  }

  #checkOpen(): void {
    if (this.#state !== "open") throw new TransactionStateError(`The statement is ${this.#state}`);
  }

  #current: SnapshotImpl | null = null;
  #ownReads: Set<SnapshotImpl> | null = null;
  #ownOrigins: WeakSet<SnapshotImpl> | null = null;
  #mutation = 0;

  /** Data-phase fence; locator translation keeps the pin alive through every write batch. */
  beginMutation(): void {
    if (this.#ownReads !== null) this.#mutation++;
  }

  /** Metadata-only data-phase fence. Unlike a pin it retains no native reader or
   * source; callers use it while unwinding and re-funding a representation retry. */
  readOwnCheck(): () => void {
    this.#checkOpen();
    this.#ownReads ??= new Set();
    const mutation = this.#mutation;
    return () => {
      this.#checkOpen();
      if (mutation !== this.#mutation)
        throw new TransactionStateError(
          "A current-prefix pin is locator-only after mutation begins",
        );
    };
  }

  readOwn(): SnapshotImpl {
    const checkRead = this.readOwnCheck();
    const view = this.#fresh(() => this.#ownReads?.delete(view), checkRead);
    (this.#ownReads ??= new Set()).add(view);
    (this.#ownOrigins ??= new WeakSet()).add(view);
    return view;
  }
  /** Native origin check, shared by construction and locator use; never trust matching IDs. */
  writerView(view: Snapshot): SnapshotImpl {
    this.#checkOpen();
    if (!(view instanceof SnapshotImpl) || !this.#ownOrigins?.has(view))
      throw new TransactionStateError("A writer inputView must come from this statement's readOwn");
    view.check();
    return view;
  }

  /**
   * A view of everything committed and pending, for checks: one per statement, replaced when the
   * write path's state changed (a freeze or publish), released with the statement. Callers must
   * not release it.
   */
  current(): SnapshotImpl {
    const state = this.tx.hooks.state();
    if (
      this.#current !== null &&
      this.#current.state === state &&
      this.#current.catalog === this.tx.catalog
    )
      return this.#current;
    this.#current?.releaseInternal();
    this.#current = this.#fresh();
    return this.#current;
  }

  #fresh(onRelease?: () => void, checkRead?: () => void): SnapshotImpl {
    return new SnapshotImpl({
      state: this.tx.hooks.state(),
      bound: ALL,
      observedLsn: this.tx.baseLsn,
      catalog: this.tx.catalog,
      shared: this.tx.hooks.shared,
      label: `${this.tx.hooks.label()}/checks`,
      statementView: false,
      ...(onRelease === undefined ? {} : { onRelease }),
      ...(checkRead === undefined ? {} : { checkRead }),
    });
  }

  table(tableId: number, options: TableWriteOptions = {}): TableWriterImpl {
    if (this.#state !== "open") throw new TransactionStateError(`The statement is ${this.#state}`);
    if (options.requireTrackedTargets === true && this.targets === undefined)
      throw new TypeError("SQL modification requires admitted target tracking");
    if (options.inputView !== undefined) {
      const def = this.tx.catalog.table(tableId);
      if (def === undefined) throw new RangeError(`Table ${String(tableId)} does not exist`);
      // A body owns this writer/pin pair; never alias it with the start-view writer cache.
      return new TableWriterImpl(this, def, options);
    }
    // The common case (no options, the same table again) skips the keyed lookup.
    const plain =
      options.assigned === undefined &&
      options.onRepeat === undefined &&
      options.uniqueTargets !== true;
    if (plain && this.#plain?.tableId === tableId) return this.#plain;
    if (plain && this.#plain === null && this.#writers === null) {
      // The first writer of a statement needs no map.
      const def = this.tx.catalog.table(tableId);
      if (def === undefined) throw new RangeError(`Table ${String(tableId)} does not exist`);
      this.#plain = new TableWriterImpl(this, def, options);
      return this.#plain;
    }
    if (this.#writers === null) {
      this.#writers = new Map();
      if (this.#plain !== null)
        this.#writers.set(`${String(this.#plain.tableId)}::error:false`, this.#plain);
    }
    const key = `${String(tableId)}:${(options.assigned ?? []).join(",")}:${options.onRepeat ?? "error"}:${String(options.uniqueTargets === true)}`;
    let writer = this.#writers.get(key);
    if (writer === undefined) {
      const def = this.tx.catalog.table(tableId);
      if (def === undefined) throw new RangeError(`Table ${String(tableId)} does not exist`);
      writer = new TableWriterImpl(this, def, options);
      this.#writers.set(key, writer);
    }
    if (plain) this.#plain = writer;
    return writer;
  }

  /** Internal, logged and undoable: never drop a key from a planner-ready nonunique index. */
  invalidateIndexes(tableId: number, ids: readonly number[]): MaybePromise<void> {
    const before = this.tx.catalog;
    const version = this.tx.hooks.nextCatalogVersion();
    const logged: LoggedChange[] = ids
      .filter((id) => before.index(id)?.invalid !== true)
      .map((indexId, position) => ({
        op: "invalidateIndex",
        tableId,
        indexId,
        expectedVersion: position === 0 ? before.version : version,
        version,
      }));
    if (logged.length === 0) return;
    const catalog = logged.reduce((next, change) => next.apply(change, version), before);
    return settle(this.tx.hooks.prepareCatalog(catalog), () => {
      this.tx.undo.push({ kind: "catalog", statement: this.ordinal, before, logged });
      this.tx.catalog = catalog;
    });
  }

  changeCatalog(change: CatalogChange): MaybePromise<{ readonly id: number | null }> {
    return this.guard(() => {
      requireWritableCatalogInput(change);
      this.checkLegacyWrite();
      this.beginMutation();
      const before = this.tx.catalog;
      const { catalog, logged, id } = before.change(change, this.tx.hooks.nextCatalogVersion());
      const validate =
        change.kind === "alterTable"
          ? validateAlterNullability(
              this.current().internalTable(change.tableId),
              need(catalog.table(change.tableId)),
              () => this.#checkOpen(),
            )
          : undefined;
      return settle(validate, () =>
        settle(this.tx.hooks.prepareCatalog(catalog), () =>
          this.guard(() => {
            this.tx.undo.push({ kind: "catalog", statement: this.ordinal, before, logged });
            this.tx.catalog = catalog;
            const work: Array<MaybePromise<void>> = [];
            for (const entry of logged) {
              if (entry.op === "createTable")
                this.tx.hooks.createTableState(entry.table, entry.table.id);
              if (entry.op === "createIndex") work.push(this.#backfill(entry.index));
            }
            return settle(all(work), () => ({ id }));
          }),
        ),
      );
    });
  }

  /** Index entries for every live row of a table that gains an index in this statement. */
  #backfill(index: IndexDef): MaybePromise<void> {
    const memtable = this.tx.hooks.addIndexDelta(index.tableId, index.id);
    this.tx.undo.push({ kind: "indexDelta", statement: this.ordinal, memtable, indexId: index.id });
    const current = this.current();
    const table = current.internalTable(index.tableId);
    const def = need(this.tx.catalog.table(index.tableId));
    const work = async () => {
      const rowIds: number[] = [];
      for (const segment of table.segments) {
        const deletions = await segment.deletions();
        for (let position = 0; position < segment.rowCount; position++)
          if (!deletions?.isDeleted(position))
            rowIds.push(table.rowId(segment.index * 65_536 + position));
      }
      const writer = new TableWriterImpl(this, def, {});
      for (let from = 0; from < rowIds.length; from += MEMORY_CHUNK_ROWS) {
        const ids = rowIds.slice(from, from + MEMORY_CHUNK_ROWS);
        const vectors = await readRows(table, ids);
        const put = writer.putKeys([index], vectors, null, ids, memtable);
        if (put instanceof Promise) await put;
      }
    };
    return work();
  }

  /** Statement ranges are disjoint: revisiting all older undo makes N statements quadratic.
   * Large current statements still share the worker clock and recheck cancellation on resume. */
  #needsFtsPreparation(from: number): MaybePromise<boolean> {
    this.#checkOpen();
    this.#signal?.throwIfAborted();
    for (let i = from; i < this.tx.undo.length; i++) {
      const e = this.tx.undo[i];
      if (
        e &&
        (e.kind === "memDelete" ||
          e.kind === "groupDelete" ||
          (e.kind === "rows" && e.ftsKeyed === true))
      )
        return true;
      if ((i & 1023) === 1023 && taskClock.due())
        return yieldTask().then(() => this.#needsFtsPreparation(i + 1));
    }
    return false;
  }

  end(signal?: AbortSignal): Promise<void> {
    this.tx.hooks.checkNativeStepReady?.(this);
    this.#signal =
      signal && this.#signal && signal !== this.#signal
        ? AbortSignal.any([this.#signal, signal])
        : (signal ?? this.#signal);
    const finish = (): MaybePromise<void> =>
      this.guard(() => {
        this.tx.hooks.checkNativeStepReady?.(this);
        this.#releaseViews();
        return settle(this.#needsFtsPreparation(this.#start), (keyed) =>
          settle(keyed ? this.tx.hooks.prepareFtsMutation?.(this.#signal) : undefined, () => {
            this.#signal?.throwIfAborted();
            this.#state = "ended";
            this.tx.ended(this);
          }),
        );
      });
    if (this.#commands !== null) {
      const commands = this.#commands;
      return Promise.resolve(
        this.guard(async () => {
          this.beginMutation();
          await commands.finishRoot(this.#signal);
          if (this.suspects.length || this.childChecks.length || this.parentChecks.length)
            throw new TransactionStateError(
              "Legacy deferred work escaped native command ownership",
            );
        }),
      ).then(finish);
    }
    if (
      this.suspects.length === 0 &&
      this.childChecks.length === 0 &&
      this.parentChecks.length === 0
    ) {
      // Nothing to re-verify: end now (no check view, no promise chain).
      if (this.#state !== "open")
        throw new TransactionStateError(`The statement is ${this.#state}`);
      const pending = finish();
      return pending instanceof Promise ? pending : RESOLVED;
    }
    return Promise.resolve(
      this.guard(async () => {
        this.beginMutation();
        await this.#actions();
        await this.#verify();
      }),
    ).then(finish);
  }

  #releaseViews(): void {
    for (const view of this.#ownReads ?? []) view.releaseInternal();
    this.#ownReads = null;
    this.#ownOrigins = null;
    this.#view?.releaseInternal();
    this.#view = null;
    this.#current?.releaseInternal();
    this.#current = null;
  }

  async #verify(): Promise<void> {
    if (this.suspects.length === 0 && this.childChecks.length === 0) return;
    const current = this.current();
    try {
      const lookup = async (tableId: number, indexId: number, key: Uint8Array): Promise<number> => {
        const table = current.internalTable(tableId);
        const def = table.def.indexes.find((index) => index.id === indexId);
        if (def === undefined) return 0;
        return new ViewIndex(nonUnique(def), table).countRange(key, 0, key.length);
      };
      for (const suspect of this.suspects)
        if ((await lookup(suspect.tableId, suspect.indexId, suspect.key)) > 1)
          throw new UniqueViolationError(suspect.tableId, suspect.indexId, suspect.key);
      for (const check of this.childChecks)
        if (
          this.deleted.get(check.childTableId)?.has(check.rowId) !== true &&
          (await lookup(check.parentTableId, check.parentIndexId, check.key)) === 0
        )
          throw new ForeignKeyViolationError(
            check.childTableId,
            check.fkId,
            "missingParent",
            check.key,
          );
    } finally {
      // `current` belongs to the statement (released with it).
    }
  }

  /**
   * Foreign-key actions, run to a fixpoint before the checks (PostgreSQL runs them as the
   * statement's AFTER triggers): for each key that left a referenced parent and is not live there
   * again, the children holding it are deleted (ON DELETE CASCADE), have their reference set to
   * NULL (SET NULL), make the statement fail (RESTRICT), or, when an UPDATE changed the key, have
   * their reference rewritten to the new key (rekey follow: always, as today's engine does). Rows
   * these actions write go through the same writers, so their own constraints and actions apply.
   */
  /**
   * Runs the foreign-key actions the statement's parent changes call for, in rounds: each round
   * takes every check queued so far (an action may queue more: a cascade into a further table).
   * - **Follow (a parent's key changed, ON UPDATE CASCADE).** Children follow their parent row's
   *   identity (review B3-p2b-1 #1): for each child foreign key, the children of every key that
   *   changed in the round are found in one view before any of them is updated, so a key another
   *   parent took meanwhile (a swap, a shift, a rotation) never captures the wrong children.
   *   PostgreSQL's row-at-a-time cascade gives the same result for every statement it accepts.
   * - **Delete (a parent row left).** Nothing happens when the key is live again; else CASCADE,
   *   SET NULL or RESTRICT per child foreign key.
   * Every lookup reads the current view (review B3-p2b-1 #2): an action can freeze a memtable or
   * replace the state, and a later action must see the rows it wrote.
   */
  async #actions(): Promise<void> {
    while (this.parentChecks.length > 0) {
      const round = this.parentChecks.splice(0);
      const follows = round.filter((check) => check.next !== null);
      if (follows.length > 0) await this.#follow(follows);
      for (const check of round) if (check.next === null) await this.#deleted(check);
    }
  }

  /**
   * Children of a statement's changed parent keys follow them (see `#actions`). One view serves
   * every child foreign key of the round (review B3-p2b-2 B1): each child row's new reference
   * values are computed from the parents' original keys before any child is updated, and a row
   * that several foreign keys reach (sharing a column or not) is updated once, with all of them.
   */
  async #follow(checks: readonly ParentCheck[]): Promise<void> {
    const view = this.current();
    /** Child table → row id → column id → its new value. */
    type NewValues = Map<number, { readonly vector: Vector; readonly row: number }>;
    const plans = new Map<number, Map<number, NewValues>>();
    for (const child of this.tx.catalog.tables)
      for (const fk of child.foreignKeys) {
        if (!fk.enforced || fk.supportIndexId === null) continue;
        const mine = checks.filter((check) => check.indexId === fk.parentIndexId);
        if (mine.length === 0) continue;
        const support = need(child.indexes.find((index) => index.id === fk.supportIndexId));
        const index = new ViewIndex(nonUnique(support), view.internalTable(child.id));
        for (const check of mine) {
          const scanned =
            support.invalid === true
              ? await scanIndexHolders(view.internalTable(child.id), fk.columnIds, check.key)
              : null;
          try {
            const children =
              scanned?.value ??
              (await index.holders(check.key, support.columnIds.length > fk.columnIds.length));
            if (children.length === 0) continue;
            const parent = need(this.tx.catalog.table(check.parentTableId));
            const key = need(parent.indexes.find((candidate) => candidate.id === check.indexId));
            let rows = plans.get(child.id);
            if (rows === undefined) plans.set(child.id, (rows = new Map<number, NewValues>()));
            for (const rowId of children) {
              let columns = rows.get(rowId);
              if (columns === undefined) rows.set(rowId, (columns = new Map() as NewValues));
              fk.columnIds.forEach((columnId, k) => {
                if (columns.has(columnId)) return; // a shared column: the same original key
                const parentColumn = key.columnIds[k];
                columns.set(columnId, {
                  vector: need(
                    check.next?.vectors[
                      parent.columns.findIndex((column) => column.id === parentColumn)
                    ],
                  ),
                  row: check.next?.row ?? 0,
                });
              });
            }
          } finally {
            scanned?.release();
          }
        }
      }
    for (const [childId, rows] of plans) {
      const def = need(this.tx.catalog.table(childId));
      for (const [rowId, columns] of rows)
        await new TableWriterImpl(this, def, { onRepeat: "first" }).applyUpdate(
          [rowId],
          [...columns.keys()],
          [...columns.values()],
          true,
        );
    }
  }

  /** A parent row left: CASCADE, SET NULL or RESTRICT per child foreign key (see `#actions`). */
  async #deleted(check: ParentCheck): Promise<void> {
    const parentDef = this.current().internalTable(check.parentTableId).def;
    const key = parentDef.indexes.find((index) => index.id === check.indexId);
    // The key is live in the parent again (another row took it): nothing to do.
    if (
      key !== undefined &&
      (await new ViewIndex(
        nonUnique(key),
        this.current().internalTable(check.parentTableId),
      ).countRange(check.key, 0, check.key.length)) > 0
    )
      return;
    for (const child of this.tx.catalog.tables)
      for (const fk of child.foreignKeys) {
        if (!fk.enforced || fk.parentIndexId !== check.indexId || fk.supportIndexId === null)
          continue;
        const support = need(child.indexes.find((index) => index.id === fk.supportIndexId));
        // A covering supporting index holds the reference as a key prefix (ruling 4). The view is
        // read now: an earlier action may have rewritten these children.
        const table = this.current().internalTable(child.id);
        const scanned =
          this.tx.catalog.index(support.id)?.invalid === true
            ? await scanIndexHolders(table, fk.columnIds, check.key)
            : null;
        try {
          const children =
            scanned?.value ??
            (await new ViewIndex(nonUnique(support), table).holders(
              check.key,
              support.columnIds.length > fk.columnIds.length,
            ));
          if (children.length === 0) continue;
          const writer = new TableWriterImpl(this, child, { onRepeat: "first" });
          if (fk.onDelete === "cascade") await writer.applyDelete(children);
          else if (fk.onDelete === "setNull")
            await writer.applyUpdate(
              children,
              fk.columnIds,
              fk.columnIds.map((columnId) => ({
                vector: nullVector(
                  need(child.columns.find((column) => column.id === columnId)).type,
                ),
                row: 0,
              })),
              true,
            );
          else
            throw new ForeignKeyViolationError(check.parentTableId, fk.id, "referenced", check.key);
        } finally {
          scanned?.release();
        }
      }
  }

  rollback(): void {
    if (this.#state === "ended") return;
    this.#commands?.checkRollbackReady();
    this.tx.hooks.checkNativeStepReady?.(this);
    this.tx.undoTo(this.#start);
    this.#state = "ended";
    this.#releaseViews();
    this.tx.ended(this);
  }
}

// ── Table writers ──────────────────────────────────────────────────────────────────────────────

const NULL_KEY = new Uint8Array(0);

export class TableWriterImpl implements TableWriter {
  readonly tableId: number;
  readonly #keys: KeyBatch;
  readonly #assigned: readonly number[];
  readonly #onRepeat: "error" | "first";
  readonly #inputView: SnapshotImpl | null;
  readonly #uniqueTargets: boolean;
  // One scalar cursor per existing writer: repeated one-row upserts must not leave
  // a row effect per input for the bounded commit encoder's repeated passes.
  #lastRowsUndo = -1;
  #lastRowsCatalog = -1;
  #lastRowsCommand = 0;
  /** Indexes of other tables' foreign keys that reference this table's keys. */
  readonly #referenced: ReadonlySet<number>;

  constructor(
    readonly statement: StatementImpl,
    readonly def: TableDef,
    options: TableWriteOptions,
  ) {
    this.tableId = def.id;
    this.#keys = statement.tx.hooks.keys;
    this.#assigned = options.assigned ?? [];
    this.#inputView =
      options.inputView === undefined ? null : statement.writerView(options.inputView);
    this.#onRepeat = options.onRepeat ?? "error";
    this.#uniqueTargets = options.uniqueTargets === true;
    this.#referenced = referencedIndexes(statement.catalog, def.id);
  }

  #columnIndex(columnId: number): number {
    const index = this.def.columns.findIndex((column) => column.id === columnId);
    if (index < 0) throw new RangeError(`Table ${this.def.name} has no column ${String(columnId)}`);
    return index;
  }

  /** Pure conservative one-row sizing. No native/image input is retained across admission. */
  commandPrimarySizing(
    commands: NativePrimaryAdapter,
    event: WriteEvent,
    widths: readonly number[],
  ): { readonly bytes: number; readonly growth: () => number } {
    this.statement.checkCommandPrimary(commands);
    const bytes =
      65536 * (this.def.columns.length + this.def.indexes.length) +
      (event === "delete" ? 0 : 2 * widths.reduce((n, width) => n + width, 0));
    return {
      bytes,
      growth: () => {
        const live = this.statement.tx.hooks.state().tables.get(this.tableId)?.memtables.at(-1);
        if (live?.state !== "live") return 0;
        let growth = 0;
        if (event !== "delete")
          for (const [c, width] of widths.entries()) {
            const chunk = live.chunks[c]?.at(-1),
              arena = chunk?.arena;
            if (!chunk || !arena) continue;
            const wanted = (chunk.offsets?.[chunk.length] ?? 0) + width;
            if (wanted <= arena.length) continue;
            let size = arena.length * 2;
            while (size < wanted) size *= 2;
            growth += size;
          }
        for (const index of this.def.indexes) {
          if (index.invalid === true) continue;
          const delta = live.deltas.get(index.id);
          // At most an old tombstone plus a new put; cover every delta backing-array resize
          // and ordered-node split, including the old/new overlap, before the primary call.
          growth +=
            2 * (delta?.bytes ?? 0) +
            (delta?.appendKeyBytes(2 * indexMaximumKeyBytes(index, this.def)) ?? 0);
        }
        return growth;
      },
    };
  }

  /** Full physical images supplied by the root. No legacy row read/copy/deferred queues.
   * Root already ran BEFORE and generation; complete-row constraints still run here. */
  async commandPrimary(
    commands: NativePrimaryAdapter,
    event: WriteEvent,
    oldRowId: number | null,
    old: Batch | null,
    next: Batch | null,
    widths: readonly number[],
    changed: readonly number[] | null,
    signal: AbortSignal,
    checkCommand: () => void,
    commandId: number,
  ): Promise<number | null> {
    this.statement.checkCommandPrimary(commands);
    checkCommand();
    if (!Number.isSafeInteger(commandId) || commandId < 1)
      throw new TypeError("Invalid native row command identity");
    this.statement.beginMutation();
    const ids = this.def.columns.map((column) => column.id);
    if ((event !== "insert") !== (old !== null) || (event !== "delete") !== (next !== null))
      throw new TypeError("Native primary image sides do not match the write");
    for (const input of [old, next]) {
      if (!input) continue;
      if (input.length !== 1 || input.selection !== null)
        throw new TypeError("Native primary requires one complete physical row");
      this.#checkShape(input.columns, 0, ids);
    }
    if (next) {
      for (const [c, vector] of next.columns.entries())
        if (
          layoutOf(vector.type) === "varlen" &&
          this.#inputBytes(vector, { rows: null, from: 0, count: 1 }) > (widths[c] ?? -1)
        )
          throw new TypeError("Native row exceeded its prepared byte bound");
      this.#validate(next.columns, null, 1, changed);
      for (const index of this.def.indexes) {
        if (index.invalid === true) continue;
        const failure = indexLimitViolation(index, this.def, next.columns, null, 0, 1);
        if (failure) throw failure;
      }
    }
    signal.throwIfAborted();
    if (oldRowId !== null) {
      const pin = this.statement.readOwn();
      try {
        const table = pin.internalTable(this.tableId),
          locator = table.locator(oldRowId),
          segment = table.segments[Math.floor(locator / 65536)],
          position = locator % 65536;
        if (
          !segment ||
          position >= segment.rowCount ||
          (await segment.deletions())?.isDeleted(position) === true
        )
          throw new TransactionStateError("Native primary target changed before continuation");
        signal.throwIfAborted();
        checkCommand();
        await this.#deleteRows(table, [oldRowId], need(old).columns);
      } finally {
        pin.release();
      }
    }
    let inserted: number | null = null;
    if (next)
      await this.#insertRows(
        next.columns,
        { rows: null, from: 0, count: 1 },
        true,
        (rowId) => {
          inserted = rowId;
        },
        commandId,
      );
    signal.throwIfAborted();
    if (event !== "insert") this.statement.noteFtsMutation(this.tableId, event);
    return inserted;
  }

  #checkShape(columns: readonly Vector[], offset: number, columnIds: readonly number[]): void {
    if (columns.length !== offset + columnIds.length)
      throw new TypeError(
        `Expected ${String(offset + columnIds.length)} columns, got ${String(columns.length)}`,
      );
    columnIds.forEach((columnId, k) => {
      const column = this.def.columns[this.#columnIndex(columnId)];
      const vector = columns[offset + k];
      if (
        column === undefined ||
        vector === undefined ||
        !samePhysicalType(column.type, vector.type)
      )
        throw new TypeError(
          `Column ${String(columnId)} of ${this.def.name} received a vector of another type`,
        );
    });
  }

  /** NOT NULL and CHECK over complete new rows. */
  #validate(
    columns: readonly Vector[],
    rows: Uint16Array | null,
    length: number,
    changed: readonly number[] | null,
  ): void {
    if (
      this.def.checks.length > 0 ||
      this.def.columns.some((column) => column.generatedSql !== null)
    ) {
      const rules = this.statement.rules(this.tableId);
      if (rules.validate) {
        rules.validate(
          { columns, length, selection: rows === null ? null : { rows, count: rows.length } },
          changed,
        );
        return;
      }
    }
    try {
      this.#validateColumns(columns, rows, length);
    } catch (error) {
      if (!(error instanceof NotNullViolationError || error instanceof ClosedSetViolationError))
        throw error;
      // The common path remains columnar. Replay a column violation in row order, then
      // check the complete valid prefix before reporting it. CHECK already guarantees
      // row/name order (including expression errors). Generated errors replay in TableRules.
      // Borrow selected rows directly; no row list, payload copy or admission on failure.
      const count = rows?.length ?? length;
      for (let k = 0; k < count; k++) {
        try {
          this.#validateColumns(columns, rows, length, k, 1);
        } catch (precise) {
          if (!(
            precise instanceof NotNullViolationError || precise instanceof ClosedSetViolationError
          ))
            throw precise;
          this.#checkConstraints(
            columns,
            rows === null ? null : rows.subarray(0, k),
            rows === null ? k : length,
            changed,
          );
          throw precise;
        }
      }
      throw error; // deterministic physical input must reproduce the violation
    }
    this.#checkConstraints(columns, rows, length, changed);
  }

  #validateColumns(
    columns: readonly Vector[],
    rows: Uint16Array | null,
    length: number,
    from = 0,
    count = rows?.length ?? length,
  ): void {
    this.def.columns.forEach((column, c) => {
      if (column.nullable && column.allowedValues === undefined) return;
      const vector = need(columns[c]);
      if (vector.form === "flat" && vector.validity === null && column.allowedValues === undefined)
        return;
      for (let k = from; k < from + count; k++) {
        const row = rows === null ? k : (rows[k] ?? 0);
        if (isNullAt(vector, row)) {
          if (!column.nullable) throw new NotNullViolationError(this.tableId, column.id, row);
          continue;
        }
        if (column.allowedValues !== undefined) {
          const slot = { flat: null as FlatVector | null, index: -1 };
          sourceSlot(vector, row, slot);
          const data = slot.flat.data;
          if (data.layout !== "varlen")
            throw new TypeError("A closed text set received a non-text vector");
          if (!column.allowedValues.some((label) => varlenMatchesString(data, slot.index, label)))
            throw new ClosedSetViolationError(this.tableId, column.id, row);
        }
      }
    });
  }

  #checkConstraints(
    columns: readonly Vector[],
    rows: Uint16Array | null,
    length: number,
    changed: readonly number[] | null,
  ): void {
    if (this.def.checks.length === 0) return;
    const selection: Selection | null = rows === null ? null : { rows, count: rows.length };
    const failure = this.statement
      .rules(this.tableId)
      .check({ length, columns, selection }, changed);
    if (failure !== null) throw new CheckViolationError(this.tableId, failure.checkId, failure.row);
  }

  insert(batch: Batch, written: Selection | null): MaybePromise<number> {
    return this.statement.guard(() => {
      this.statement.checkLegacyWrite();
      this.statement.beginMutation();
      this.#checkShape(
        batch.columns,
        0,
        this.def.columns.map((column) => column.id),
      );
      // Only this already-contiguous, non-null physical input takes the range plan. Foreign-key
      // input retains the selected-row path used by deferred child checks.
      const contiguous =
        batch.selection === null &&
        batch.length <= 0x10000 &&
        this.def.foreignKeys.length === 0 &&
        batch.columns.every((vector) => vector.form === "flat" && vector.validity === null);
      const rows = contiguous ? null : activeRows(batch);
      const count = rows?.length ?? batch.length;
      this.#validate(batch.columns, rows, batch.length, null);
      const pending = this.insertRows(batch.columns, rows, count);
      if (written !== null) {
        if (rows === null) for (let k = 0; k < count; k++) written.rows[k] = k;
        else written.rows.set(rows);
        written.count = count;
      }
      return settle(pending, () => count);
    });
  }

  /** Appends complete validated rows; null denotes their contiguous physical range.
   * One plan is active at a time under the single-writer contract (EngineHooks.keys). The
   * owner prepays its fixed OBJECT_OVERHEAD_BYTES allowance through admission and settlement. */
  insertRows(
    columns: readonly Vector[],
    rows: Uint16Array | null,
    count = rows?.length ?? 0,
  ): MaybePromise<void> {
    this.statement.checkLegacyWrite();
    const advances: Array<MaybePromise<void>> = [];
    for (const [position, column] of this.def.columns.entries())
      if (column.autoIncrement === true && column.identitySequence !== null) {
        let maximum = 0n;
        const vector = need(columns[position]),
          slot = { flat: null as FlatVector | null, index: -1 };
        for (let k = 0; k < count; k++) {
          const row = rows?.[k] ?? k;
          sourceSlot(vector, row, slot);
          if (slot.index < 0) continue;
          const data = slot.flat.data;
          let value: bigint;
          if (data.layout === "int64") value = int64At(data.values, slot.index);
          else if (data.layout === "i32" || data.layout === "f64") {
            const number = data.values[slot.index] ?? 0;
            if (!Number.isSafeInteger(number))
              throw new CatalogChangeError("An auto-increment value must be a safe whole number");
            value = BigInt(number);
          } else throw new TypeError("Auto-increment input has another layout");
          if (value > maximum) maximum = value;
        }
        advances.push(
          this.statement.tx.hooks.advanceIdentity(column.identitySequence, maximum + 1n),
        );
      }
    return settle(all(advances), () =>
      settle(this.#prepareKeyLimits(this.def.indexes, columns, rows, 0, count), () =>
        this.#insertPreparedRows(columns, rows, count),
      ),
    );
  }

  #insertPreparedRows(
    columns: readonly Vector[],
    rows: Uint16Array | null,
    count: number,
  ): MaybePromise<void> {
    if (count <= MEMORY_CHUNK_ROWS) {
      const plan: RowPlan = { rows, from: 0, count },
        bytes = this.#estimate(columns, plan);
      if (bytes <= BATCH_TARGET_BYTES || count <= 1 || !this.#wideKeys(columns, plan))
        return settle(
          this.statement.tx.hooks.admit(bytes, () => this.#growth(columns, plan)),
          () => this.#insertRows(columns, plan),
        );
    }
    // Keep ordinary batches unchanged. Wide borrowed input is admitted and staged in byte
    // slices, never as one oversized native column/key arena or incomplete key batch.
    let offset = 0;
    const next = (): MaybePromise<void> =>
      this.statement.guard(() => {
        while (offset < count) {
          let plan: RowPlan = {
            rows,
            from: offset,
            count: Math.min(MEMORY_CHUNK_ROWS, count - offset),
          };
          let bytes = this.#estimate(columns, plan);
          while (plan.count > 1 && bytes > BATCH_TARGET_BYTES && this.#wideKeys(columns, plan)) {
            plan = { rows, from: offset, count: Math.ceil(plan.count / 2) };
            bytes = this.#estimate(columns, plan);
          }
          offset += plan.count;
          const pending = settle(
            this.statement.tx.hooks.admit(bytes, () => this.#growth(columns, plan)),
            () => this.statement.guard(() => this.#insertRows(columns, plan)),
          );
          if (pending instanceof Promise) return pending.then(next);
        }
      });
    return next();
  }

  #inputBytes(vector: Vector, plan: RowPlan): number {
    if (vector.form === "flat" && vector.data.layout === "varlen" && plan.rows === null)
      return (
        (vector.data.offsets[plan.from + plan.count] ?? 0) - (vector.data.offsets[plan.from] ?? 0)
      );
    if (
      vector.type.kind !== "text" &&
      vector.type.kind !== "bytes" &&
      vector.type.kind !== "enum" &&
      vector.type.kind !== "json" &&
      vector.type.kind !== "array"
    )
      return 0;
    const slot: { flat: FlatVector | null; index: number } = { flat: null, index: -1 };
    let bytes = 0;
    for (let i = plan.from; i < plan.from + plan.count; i++) {
      sourceSlot(vector, plan.rows === null ? i : (plan.rows[i] ?? 0), slot);
      if (slot.index < 0 || slot.flat.data.layout !== "varlen") continue;
      bytes +=
        (slot.flat.data.offsets[slot.index + 1] ?? 0) - (slot.flat.data.offsets[slot.index] ?? 0);
    }
    return bytes;
  }

  /** Recomputed after relief: native capacity growth is paid BEFORE append, including the
   * old/new arena overlap. A freeze/stage returns zero for the old arenas instead of making
   * the next small batch wait for a stale multi-MiB growth estimate. */
  #growth(columns: readonly Vector[], plan: RowPlan): number {
    const live = this.statement.tx.hooks.state().tables.get(this.tableId)?.memtables.at(-1);
    if (live?.state !== "live") return 0;
    let bytes = 0;
    for (let c = 0; c < columns.length; c++) {
      const chunk = live.chunks[c]?.at(-1),
        arena = chunk?.arena;
      if (chunk === undefined || arena == null) continue;
      const wanted =
        (chunk.offsets?.[chunk.length] ?? 0) + this.#inputBytes(need(columns[c]), plan);
      if (wanted <= arena.length) continue;
      let size = arena.length * 2;
      while (size < wanted) size *= 2;
      bytes += size;
    }
    for (const index of this.def.indexes) {
      if (this.statement.catalog.index(index.id)?.invalid === true) continue;
      let wanted = 32 * plan.count * index.columnIds.length;
      for (const id of index.columnIds)
        wanted += 2 * this.#inputBytes(need(columns[this.#columnIndex(id)]), plan);
      bytes += live.deltas.get(index.id)?.appendKeyBytes(wanted) ?? 0;
    }
    return bytes;
  }

  /** Only key-bearing input needs the new byte-sliced ingress. Large unindexed payloads keep
   * their existing admission/publication boundaries. Escaping may double raw key bytes; fixed
   * components and tuple headers fit within the per-column upper bound used here. */
  #wideKeys(columns: readonly Vector[], plan: RowPlan): boolean {
    let bytes = 0;
    for (const index of this.def.indexes) {
      if (this.statement.catalog.index(index.id)?.invalid === true) continue;
      bytes += plan.count * 17 * index.columnIds.length;
      for (const id of index.columnIds)
        bytes += 2 * this.#inputBytes(need(columns[this.#columnIndex(id)]), plan);
      if (bytes > BATCH_TARGET_BYTES) return true;
    }
    return false;
  }

  /** Same capacity estimate; a contiguous varlen range uses its two end offsets. */
  #estimate(columns: readonly Vector[], plan: RowPlan): number {
    let bytes =
      4096 * columns.length +
      plan.count * (16 * columns.length + 96 * this.def.indexes.length + FTS_ROW_UNDO_BYTES);
    for (const vector of columns) bytes += 2 * this.#inputBytes(vector, plan);
    return bytes;
  }

  #insertRows(
    columns: readonly Vector[],
    plan: RowPlan,
    command = false,
    inserted?: (rowId: number) => void,
    commandId = 0,
  ): MaybePromise<void> {
    const tx = this.statement.tx,
      seq = this.statement.seq;
    const rowIds: number[] | null = !command && this.def.foreignKeys.length > 0 ? [] : null;
    let done = 0;
    const next = (): MaybePromise<void> => {
      while (done < plan.count) {
        const { memtable, created, frozen } = tx.hooks.liveMemtable(
          this.tableId,
          plan.count - done,
        );
        if (created)
          tx.undo.push({
            kind: "freeze",
            statement: this.statement.ordinal,
            tableId: this.tableId,
            created: memtable,
            frozen,
          });
        const take = Math.min(memtable.capacity, plan.count - done),
          before = memtable.rowCount;
        const previous = tx.undo[this.#lastRowsUndo];
        const coalesce =
          previous?.kind === "rows" &&
          previous.ftsKeyed !== undefined &&
          previous.statement === this.statement.ordinal &&
          previous.memtable === memtable &&
          previous.before + previous.count === before &&
          this.#lastRowsCatalog === this.statement.catalog.version &&
          this.#lastRowsCommand === commandId;
        // FTS reserves one embedded flag per retained entry, not per append. An
        // extension keeps its funded flag (including true) and needs no new slot.
        if (!coalesce) tx.reserveFtsRowUndo();
        try {
          memtable.append(columns, plan.rows, plan.from + done, take);
          memtable.mark(seq);
        } catch (error) {
          if (!coalesce) tx.releaseFtsRowUndo();
          throw error;
        }
        if (coalesce) {
          tx.undo[this.#lastRowsUndo] = { ...previous, count: previous.count + take };
        } else {
          this.#lastRowsUndo = tx.undo.length;
          this.#lastRowsCatalog = this.statement.catalog.version;
          this.#lastRowsCommand = commandId;
          tx.undo.push({
            kind: "rows",
            ftsKeyed: false,
            statement: this.statement.ordinal,
            tableId: this.tableId,
            memtable,
            before,
            count: take,
          });
        }
        const firstRowId = memtable.firstRowId + before;
        inserted?.(firstRowId);
        if (rowIds !== null) for (let k = 0; k < take; k++) rowIds.push(firstRowId + k);
        const put = this.#putValidKeys(
          this.def.indexes,
          columns,
          plan.rows,
          firstRowId,
          memtable,
          plan.from + done,
          take,
          command,
        );
        done += take;
        if (put instanceof Promise) return put.then(next);
      }
      // Foreign-key insertions keep the materialized selection contract above.
      if (rowIds !== null)
        this.#childChecks(
          columns,
          need(plan.rows).subarray(plan.from, plan.from + plan.count),
          rowIds,
        );
    };
    return next();
  }

  #keyColumns(index: IndexDef, columns: readonly Vector[]): KeyColumn[] {
    return index.columnIds.map((columnId) => ({
      vector: need(columns[this.#columnIndex(columnId)]),
      direction: "asc",
      nulls: "first",
    }));
  }

  /** Encodes the keys of `index` for `rows` (physical rows of `columns`, or all when null). */
  /** KeyBatch's byte target is a work boundary, never a limit on a borrowed mutation batch.
   * Each yielded view is borrowed until the next iteration; consumers copy only retained keys.
   * Allocation refusal is distinct from a full batch and never silently drops a completed tail. */
  *#encode(
    index: IndexDef,
    columns: readonly Vector[],
    rows: Uint16Array | null,
    count: number,
    from = 0,
    take = (rows?.length ?? count) - from,
  ): Generator<{ keys: KeyBatch; offset: number; count: number }> {
    const keys = this.#keys,
      selection = rows === null ? null : { rows, count: from + take };
    for (let cursor = from; cursor < from + take;) {
      const result = encodeKeyBatch(
        "equality",
        this.#keyColumns(index, columns),
        { length: rows === null ? from + take : count, selection },
        keys,
        cursor,
      );
      if (!result.done && result.reason === "denied")
        throw new MemtableMemoryError(result.requiredBytes);
      if (result.encoded === 0) throw new Error("A nonempty key batch made no progress");
      yield { keys, offset: cursor - from, count: result.encoded };
      cursor += result.encoded;
    }
  }

  #hasNullKey(index: IndexDef, columns: readonly Vector[], row: number): boolean {
    for (const columnId of index.columnIds)
      if (isNullAt(need(columns[this.#columnIndex(columnId)]), row)) return true;
    return false;
  }

  /**
   * Puts index entries for new rows (`rows` of `columns`) into `memtable`'s deltas, and records
   * unique suspects. `rowIds` lists the rows' ids, or is the first of consecutive ids.
   */
  putKeys(
    indexes: readonly IndexDef[],
    columns: readonly Vector[],
    rows: Uint16Array | null,
    rowIds: readonly number[] | number,
    memtable: Memtable,
    from = 0,
    count = rows?.length ?? (typeof rowIds === "number" ? 0 : rowIds.length),
  ): MaybePromise<void> {
    return settle(this.#prepareKeyLimits(indexes, columns, rows, from, count), () =>
      this.#putValidKeys(indexes, columns, rows, rowIds, memtable, from, count),
    );
  }

  #prepareKeyLimits(
    indexes: readonly IndexDef[],
    columns: readonly Vector[],
    rows: Uint16Array | null,
    from: number,
    count: number,
  ): MaybePromise<void> {
    const invalid: number[] = [];
    for (const index of indexes) {
      if (this.statement.catalog.index(index.id)?.invalid === true) continue;
      const failure = indexLimitViolation(index, this.def, columns, rows, from, count);
      if (failure !== null) {
        if (index.unique) throw failure;
        invalid.push(index.id);
      }
    }
    return this.statement.invalidateIndexes(this.tableId, invalid);
  }

  #putValidKeys(
    indexes: readonly IndexDef[],
    columns: readonly Vector[],
    rows: Uint16Array | null,
    rowIds: readonly number[] | number,
    memtable: Memtable,
    from: number,
    count: number,
    command = false,
  ): MaybePromise<void> {
    const tx = this.statement.tx;
    const seq = this.statement.seq;
    let checks: Array<Promise<void>> | null = null;
    for (const index of indexes) {
      if (this.statement.catalog.index(index.id)?.invalid === true) continue;
      const delta = memtable.deltas.get(index.id);
      if (delta === undefined) continue;
      const noNulls = index.columnIds.every((id) => {
        const vector = columns[this.#columnIndex(id)];
        return vector?.form === "flat" && vector.validity === null;
      });
      const first = delta.entryCount;
      try {
        for (const { keys, offset, count: part } of this.#encode(
          index,
          columns,
          rows,
          columns[0]?.length ?? count,
          from,
          count,
        )) {
          const appended =
            rows === null &&
            typeof rowIds === "number" &&
            noNulls &&
            delta.addOrderedBatch(keys.writer.bytes, keys.offsets, part, rowIds + offset, seq);
          if (!appended)
            for (let k = 0; k < part; k++) {
              const at = offset + k,
                row = rows === null ? from + at : (rows[from + at] ?? 0);
              if (!noNulls && this.#hasNullKey(index, columns, row)) continue;
              const rowId = typeof rowIds === "number" ? rowIds + at : (rowIds[at] ?? 0);
              delta.add(
                keys.writer.bytes,
                keys.offsets[k] ?? 0,
                keys.offsets[k + 1] ?? 0,
                rowId,
                seq,
                false,
              );
            }
        }
      } finally {
        // Record completed entries even if a later allocation throws.
        const added = delta.entryCount - first;
        if (added > 0)
          tx.undo.push({
            kind: "delta",
            statement: this.statement.ordinal,
            delta,
            entry: first,
            count: added,
          });
      }
      const added = delta.entryCount - first;
      if (added === 0) continue;
      if (command) continue; // Accepted image registry owns deferred command uniqueness.
      if (!index.unique) continue;
      const check =
        added === 1
          ? this.#suspectOne(index, delta, first)
          : this.#suspect(index, delta, first, delta.entryCount);
      if (check !== undefined) (checks ??= []).push(check);
    }
    return checks === null ? undefined : Promise.all(checks).then(() => undefined);
  }

  /**
   * Records every key of delta entries `from … to − 1` that more than one live row holds now
   * (re-verified at the end of the statement). The keys are read from the delta's arena, which
   * keeps them (no copies).
   */
  #suspectOne(index: IndexDef, delta: KeyDelta, from: number): Promise<void> | undefined {
    // Retained writers across DDL keep the existing catalog/view checks.
    if (this.statement.catalog.table(this.tableId) !== this.def)
      return this.#suspect(index, delta, from, from + 1);
    const slot = delta.keyOf(from);
    if (delta.head(slot) === from && delta.nextEntry(from) < 0) {
      const key = delta.keyBytes(slot);
      const table = need(this.statement.tx.hooks.state().tables.get(this.tableId));
      // A synchronous range/chain proof needs no snapshot, pin, reader or skipped-run Set.
      if (otherIndexLayersEndBefore(table, index.id, key, delta)) return undefined;
    }
    return this.#suspect(index, delta, from, from + 1);
  }

  /** The existing batch checker, with the single-key proof outside its hot body. */
  #suspect(index: IndexDef, delta: KeyDelta, from: number, to: number): Promise<void> | undefined {
    const current = this.statement.current();
    const reader = new ViewIndex(nonUnique(index), current.internalTable(this.tableId));
    // The batch's key range, compared in the arena (views only for the two bounds).
    const bytes = delta.keyArena();
    let low = delta.keyOf(from);
    let high = low;
    const proven = to <= delta.ascendingEntryPrefix;
    if (proven) high = delta.keyOf(to - 1);
    for (let entry = from + 1; !proven && entry < to; entry++) {
      const slot = delta.keyOf(entry);
      const start = delta.keyStart(slot);
      const end = delta.keyEnd(slot);
      if (delta.ascendingKeys) {
        if (slot < low) low = slot;
        if (slot > high) high = slot;
      } else {
        if (compareBytes(bytes, start, end, bytes, delta.keyStart(low), delta.keyEnd(low)) < 0)
          low = slot;
        if (compareBytes(bytes, start, end, bytes, delta.keyStart(high), delta.keyEnd(high)) > 0)
          high = slot;
      }
    }
    const lowKey = delta.keyBytes(low);
    const highKey = delta.keyBytes(high);
    // No other layer can hold the batch's keys (perf step 3): a key is a suspect only if its own
    // chain in this delta holds another entry, so most keys need no lookup at all.
    const alone = !reader.othersOverlap(lowKey, highKey, delta);
    reader.skipRunsOutside(lowKey, highKey);
    const run = (first: number): Promise<void> | undefined => {
      for (let entry = first; entry < to; entry++) {
        const slot = delta.keyOf(entry);
        if (alone && delta.head(slot) === entry && delta.nextEntry(entry) < 0) continue;
        const found = reader.countRange(
          delta.keyArena(),
          delta.keyStart(slot),
          delta.keyEnd(slot),
          delta.keyHash(slot),
          delta.keyBloomHash(slot),
        );
        if (found instanceof Promise)
          return found.then((count) => {
            if (count > 1)
              this.statement.suspects.push({
                tableId: this.tableId,
                indexId: index.id,
                key: delta.keyBytes(slot).slice(),
              });
            return run(entry + 1);
          });
        if (found > 1)
          this.statement.suspects.push({
            tableId: this.tableId,
            indexId: index.id,
            key: delta.keyBytes(slot).slice(),
          });
      }
      return undefined;
    };
    return run(from);
  }

  #childChecks(
    columns: readonly Vector[],
    rows: Uint16Array | null,
    rowIds: readonly number[],
  ): void {
    const count = rows === null ? (columns[0]?.length ?? 0) : rows.length;
    for (const fk of this.def.foreignKeys) {
      if (!fk.enforced) continue;
      const parent = this.statement.catalog.table(fk.parentTableId);
      const parentIndex = parent?.indexes.find((index) => index.id === fk.parentIndexId);
      if (parentIndex === undefined) continue;
      // Encode the child columns in the parent key's order (same types: DDL checked).
      const index: IndexDef = { ...parentIndex, tableId: this.tableId, columnIds: fk.columnIds };
      for (const { keys, offset, count: part } of this.#encode(
        index,
        columns,
        rows,
        columns[0]?.length ?? count,
      ))
        for (let k = 0; k < part; k++) {
          const at = offset + k,
            row = rows === null ? at : (rows[at] ?? 0);
          if (this.#hasNullKey(index, columns, row)) continue;
          this.statement.childChecks.push({
            childTableId: this.tableId,
            rowId: rowIds[at] ?? 0,
            fkId: fk.id,
            parentTableId: fk.parentTableId,
            parentIndexId: fk.parentIndexId,
            key: keys.key(k).slice(),
          });
        }
    }
  }

  /** Row ids of the active rows' locators (column 0), applying the row-affected-twice rule. */
  #targets(batch: Batch): MaybePromise<{ rows: Uint16Array; rowIds: number[] }> {
    const locators = batch.columns[0];
    if (
      locators?.form !== "flat" ||
      locators.data.layout !== "f64" ||
      locators.type.kind !== "locator"
    )
      throw new TypeError("Column 0 must be a flat locator vector");
    const snapshot = this.#inputView ?? this.statement.view;
    snapshot.check();
    const view = snapshot.internalTable(this.tableId);
    const locatorValues = locators.data.values;
    const tracker = this.#uniqueTargets ? undefined : this.statement.targets;
    let touched =
      this.#uniqueTargets || tracker ? undefined : this.statement.touched.get(this.tableId);
    if (!this.#uniqueTargets && !tracker && touched === undefined) {
      touched = new Set();
      this.statement.touched.set(this.tableId, touched);
    }
    const kept: number[] = [];
    const rowIds: number[] = [];
    const active = activeRows(batch);
    const visit = (from: number): MaybePromise<void> => {
      for (let k = from; k < active.length; k++) {
        const row = need(active[k]);
        if (
          locators.validity !== null &&
          ((locators.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0
        )
          throw new TypeError("An update or delete target has a null locator");
        snapshot.check();
        const rowId = view.rowId(locatorValues[row] ?? 0);
        const keep = (seen: boolean) => {
          if (seen) {
            if (this.#onRepeat === "error") throw new CardinalityViolationError(this.tableId, row);
            return;
          }
          touched?.add(rowId);
          kept.push(row);
          rowIds.push(rowId);
        };
        const seen = tracker?.testAndSet(this.tableId, rowId) ?? touched?.has(rowId) ?? false;
        if (seen instanceof Promise)
          return seen.then((value) => {
            snapshot.check();
            keep(value);
            return visit(k + 1);
          });
        keep(seen);
      }
    };
    return settle(visit(0), () => ({ rows: Uint16Array.from(kept), rowIds }));
  }

  update(batch: Batch, written: Selection | null): MaybePromise<number> {
    return this.statement.guard(() => {
      this.statement.checkLegacyWrite();
      this.statement.beginMutation();
      this.#checkShape(batch.columns, 1, this.#assigned);
      return settle(this.#targets(batch), ({ rows, rowIds }) => {
        if (written !== null) {
          written.rows.set(rows);
          written.count = rows.length;
        }
        if (rowIds.length === 0) return 0;
        const sources = this.#assigned.map((_, k) => ({
          vector: need(batch.columns[k + 1]),
          rows,
        }));
        return settle(this.#update(rowIds, this.#assigned, sources, false), () => rowIds.length);
      });
    });
  }

  /**
   * An update a foreign-key action writes: every row gets the same value per assigned column
   * (`row` of `vector`), and stored generated columns are recomputed (`TableRules.generate`).
   * Row ids are this table's current rows; more than a chunk is done chunk by chunk.
   */
  async applyUpdate(
    rowIds: readonly number[],
    columnIds: readonly number[],
    values: ReadonlyArray<{ readonly vector: Vector; readonly row: number }>,
    generate: boolean,
  ): Promise<void> {
    this.statement.checkLegacyWrite();
    for (let from = 0; from < rowIds.length; from += MEMORY_CHUNK_ROWS) {
      const ids = rowIds.slice(from, from + MEMORY_CHUNK_ROWS);
      const rows = new Uint16Array(ids.length);
      const sources = values.map((value) => {
        rows.fill(value.row);
        return { vector: value.vector, rows: rows.slice() };
      });
      const done = this.#update(ids, columnIds, sources, generate);
      if (done instanceof Promise) await done;
    }
  }

  /** A delete a foreign-key action writes (ON DELETE CASCADE). */
  async applyDelete(rowIds: readonly number[]): Promise<void> {
    this.statement.checkLegacyWrite();
    for (let from = 0; from < rowIds.length; from += MEMORY_CHUNK_ROWS) {
      const ids = rowIds.slice(from, from + MEMORY_CHUNK_ROWS);
      const done = this.#delete(ids);
      if (done instanceof Promise) await done;
    }
  }

  /**
   * Rewrites rows `rowIds`: column `columnIds[c]` takes `sources[c].vector` at physical rows
   * `sources[c].rows[k]` for row k; the others keep their old values.
   */
  #update(
    rowIds: readonly number[],
    columnIds: readonly number[],
    sources: ReadonlyArray<{ readonly vector: Vector; readonly rows: Uint16Array }>,
    generate: boolean,
  ): MaybePromise<void> {
    const current = this.statement.current();
    const table = current.internalTable(this.tableId);
    return settle(readRows(table, rowIds), (old) => {
      const assigned = new Map(columnIds.map((columnId, k) => [this.#columnIndex(columnId), k]));
      const one = new Uint16Array(1);
      const columns: FlatVector[] = this.def.columns.map((column, c) => {
        const k = assigned.get(c);
        // Generated outputs replace data in this wrapper. OLD must keep its own wrapper
        // for deletion, parent-key comparisons and rollback; underlying bytes stay borrowed.
        if (k === undefined) {
          const previous = need(old[c]);
          return generate && column.generatedSql !== null ? { ...previous } : previous;
        }
        const source = need(sources[k]);
        const chunk = new ColumnChunk(column.type);
        for (const row of source.rows) {
          one[0] = row;
          chunk.append(source.vector, one, 0, 1);
        }
        return chunk.view();
      });
      const all = Uint16Array.from({ length: rowIds.length }, (_, k) => k);
      if (generate)
        this.statement
          .rules(this.tableId)
          .generate({ length: rowIds.length, columns, selection: null }, columnIds);
      this.#validate(columns, all, rowIds.length, columnIds);
      const removed = this.#deleteRows(table, rowIds, old);
      const added = this.insertRows(columns, all);
      this.#parentChecksForChange(old, columns, rowIds.length);
      return settle(all_(removed, added), () => {
        if (rowIds.length > 0) this.statement.noteFtsMutation(this.tableId, "update");
      });
    });
  }

  delete(batch: Batch, written: Selection | null): MaybePromise<number> {
    return this.statement.guard(() => {
      this.statement.checkLegacyWrite();
      this.statement.beginMutation();
      if (batch.columns.length !== 1)
        throw new TypeError("A delete batch holds the locator column only");
      return settle(this.#targets(batch), ({ rows, rowIds }) => {
        if (written !== null) {
          written.rows.set(rows);
          written.count = rows.length;
        }
        if (rowIds.length === 0) return 0;
        return settle(this.#delete(rowIds), () => rowIds.length);
      });
    });
  }

  /** Deletes current rows `rowIds` (≤ a chunk) and queues the keys they free for FK actions. */
  #delete(rowIds: readonly number[]): MaybePromise<void> {
    const current = this.statement.current();
    const table = current.internalTable(this.tableId);
    return settle(readRows(table, rowIds), (old) => {
      const removed = this.#deleteRows(table, rowIds, old);
      for (const index of this.def.indexes)
        if (this.#referenced.has(index.id)) {
          for (const { keys, offset, count } of this.#encode(index, old, null, rowIds.length))
            for (let k = 0; k < count; k++)
              if (!this.#hasNullKey(index, old, offset + k))
                this.statement.parentChecks.push({
                  parentTableId: this.tableId,
                  indexId: index.id,
                  key: keys.key(k).slice(),
                  next: null,
                });
        }
      return settle(removed, () => {
        if (rowIds.length > 0) this.statement.noteFtsMutation(this.tableId, "delete");
      });
    });
  }

  /** For rows whose referenced key changed (an UPDATE), the old key must not be left referenced. */
  #parentChecksForChange(old: readonly Vector[], next: readonly Vector[], count: number): void {
    for (const index of this.def.indexes) {
      if (!this.#referenced.has(index.id)) continue;
      const oldKeys: Uint8Array[] = [];
      for (const { keys, count: part } of this.#encode(index, old, null, count))
        for (let k = 0; k < part; k++) oldKeys.push(keys.key(k).slice());
      for (const { keys, offset, count: part } of this.#encode(index, next, null, count))
        for (let k = 0; k < part; k++) {
          const at = offset + k;
          if (this.#hasNullKey(index, old, at)) continue;
          const key = need(oldKeys[at]),
            now = keys.key(k);
          const same = key.length === now.length && key.every((byte, b) => byte === now[b]);
          if (!same)
            this.statement.parentChecks.push({
              parentTableId: this.tableId,
              indexId: index.id,
              key,
              next: { vectors: next, row: at },
            });
        }
    }
  }

  /** Tombstones every index entry of the rows and marks them deleted, pending. */
  #deleteRows(
    table: TableSnapshotImpl,
    rowIds: readonly number[],
    old: readonly Vector[],
  ): MaybePromise<void> {
    const tx = this.statement.tx;
    const seq = this.statement.seq;
    // A version deleted later in the statement (an update's old row) needs no foreign-key check.
    if (this.statement.childChecks.length > 0) {
      let deleted = this.statement.deleted.get(this.tableId);
      if (deleted === undefined) this.statement.deleted.set(this.tableId, (deleted = new Set()));
      for (const rowId of rowIds) deleted.add(rowId);
    }
    // Tombstones go to the newest memtable, full or not (no freeze just for them).
    const live = tx.hooks.liveMemtable(this.tableId, 0).memtable;
    const keysPerRow: Uint8Array[][] = rowIds.map(() => []);
    for (const index of this.def.indexes) {
      if (this.statement.catalog.index(index.id)?.invalid === true) {
        for (const keys of keysPerRow) keys.push(NULL_KEY);
        continue;
      }
      const delta = live.deltas.get(index.id);
      const first = delta?.entryCount ?? 0;
      try {
        for (const { keys, offset, count } of this.#encode(index, old, null, rowIds.length))
          for (let k = 0; k < count; k++) {
            const at = offset + k;
            if (this.#hasNullKey(index, old, at)) {
              keysPerRow[at]?.push(NULL_KEY);
              continue;
            }
            const start = keys.offsets[k] ?? 0,
              end = keys.offsets[k + 1] ?? 0;
            keysPerRow[at]?.push(keys.writer.bytes.slice(start, end));
            delta?.add(keys.writer.bytes, start, end, rowIds[at] ?? 0, seq, true);
          }
      } finally {
        if (delta !== undefined && delta.entryCount > first)
          tx.undo.push({
            kind: "delta",
            statement: this.statement.ordinal,
            delta,
            entry: first,
            count: delta.entryCount - first,
          });
      }
    }
    const indexIds = this.def.indexes.map((index) => index.id);
    rowIds.forEach((rowId, k) => {
      const locator = table.locator(rowId);
      const segment = table.segments[Math.floor(locator / 65_536)];
      const position = locator % 65_536;
      const keys = keysPerRow[k] ?? [];
      if (segment?.kind === "memory") {
        const memtable = (segment as unknown as { memtable: Memtable }).memtable;
        memtable.deletions.set(position, seq);
        tx.undo.push({
          kind: "memDelete",
          statement: this.statement.ordinal,
          tableId: this.tableId,
          memtable,
          position,
          rowId,
          keys,
          schemaVersion: this.def.schemaVersion,
          indexIds,
        });
      } else if (segment?.kind === "group") {
        const deletes = table.tableView.groupDeletes;
        deletes.add(segment.group.groupId, position, seq);
        tx.undo.push({
          kind: "groupDelete",
          statement: this.statement.ordinal,
          tableId: this.tableId,
          deletes,
          groupId: segment.group.groupId,
          position,
          rowId,
          keys,
          schemaVersion: this.def.schemaVersion,
          indexIds,
        });
      }
    });
    return undefined;
  }

  conflicts(batch: Batch, arbiters: readonly number[], out: ConflictOutput): MaybePromise<void> {
    return this.statement.guard(() => {
      this.statement.checkLegacyWrite();
      this.#checkShape(
        batch.columns,
        0,
        this.def.columns.map((column) => column.id),
      );
      const indexes =
        arbiters.length === 0
          ? this.def.indexes.filter((index) => index.unique)
          : arbiters.map((id) => {
              const index = this.def.indexes.find((candidate) => candidate.id === id);
              if (!index?.unique)
                throw new RangeError(`Index ${String(id)} is not a unique key of ${this.def.name}`);
              return index;
            });
      const rows = activeRows(batch);
      this.#validate(batch.columns, rows, batch.length, null);
      // Reject before encoding/copying arbiter keys, just as the insertion path does.
      for (const index of indexes) {
        const failure = indexLimitViolation(index, this.def, batch.columns, rows, 0, rows.length);
        if (failure) throw failure;
      }
      const current = this.statement.current();
      const table = current.internalTable(this.tableId);
      const view = this.statement.view.internalTable(this.tableId);
      const seen = indexes.map(() => new Map<string, number>());
      const found: number[] = [];
      const keyed = indexes.map((index) => {
        const copied: Uint8Array[] = [];
        for (const { keys, count } of this.#encode(index, batch.columns, rows, batch.length))
          for (let k = 0; k < count; k++) copied.push(keys.key(k).slice());
        return copied;
      });
      const decide = (k: number, from = 0, resolved = false): MaybePromise<void> => {
        for (; k < rows.length; k++) {
          const row = rows[k] ?? 0;
          out.kinds[row] = ConflictKind.none;
          for (let i = from; i < indexes.length; i++) {
            const index = need(indexes[i]);
            if (this.#hasNullKey(index, batch.columns, row)) continue;
            const key = need(keyed[i]?.[k]);
            if (!resolved) {
              const result = new ViewIndex(index, table).resolve(key, found);
              // Completion already filled `found`, even when optional cache admission failed.
              if (result instanceof Promise) return result.then(() => decide(k, i, true));
            }
            resolved = false;
            if (found.length > 0) {
              const rowId = need(found[0]);
              out.kinds[row] = this.statement.inserted(this.tableId, rowId)
                ? ConflictKind.statement
                : ConflictKind.existing;
              out.locators[row] = view.locator(rowId);
              out.indexIds[row] = index.id;
              break;
            }
            const text = byteString(key);
            const earlier = seen[i]?.get(text);
            if (earlier !== undefined) {
              out.kinds[row] = ConflictKind.batch;
              out.earlier[row] = earlier;
              out.indexIds[row] = index.id;
              break;
            }
          }
          if (out.kinds[row] === ConflictKind.none)
            for (let i = 0; i < indexes.length; i++) {
              const index = need(indexes[i]);
              if (this.#hasNullKey(index, batch.columns, row)) continue;
              const text = byteString(need(keyed[i]?.[k]));
              if (!seen[i]?.has(text)) seen[i]?.set(text, row);
            }
          from = 0;
        }
        return undefined;
      };
      return decide(0);
    });
  }
}

/** A one-row vector of `type` whose only value is NULL. */
function nullVector(type: import("../types.js").PhysicalType): Vector {
  const chunk = new ColumnChunk(type);
  chunk.append(
    {
      form: "constant",
      type,
      length: 1,
      value: { ...chunk.view(0), length: 1, validity: new Uint32Array(1) },
    },
    null,
    0,
    1,
  );
  return chunk.view();
}

function all_(a: MaybePromise<void>, b: MaybePromise<void>): MaybePromise<void> {
  return all([a, b]);
}

export { hexId, CatalogChangeError };
