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
  UniqueViolationError,
  type ConflictOutput,
  type Statement,
  type StatementOptions,
  type TableRules,
  type TableWriteOptions,
  type TableWriter,
} from "../access/modify.js";
import { MAX_STATEMENTS, pendingSeq, statementBound, PENDING_BASE } from "../access/snapshot.js";
import {
  TransactionStateError,
  type CommitReceipt,
  type CommitRequest,
  type Savepoint,
  type WriteTransaction,
} from "../access/write-path.js";
import type { BodyEffect } from "../format/log-frame.js";
import { encodeKeyBatch, KeyBatch, type KeyColumn } from "../keys/key-encoding.js";
import { samePhysicalType } from "../types.js";
import { compareBytes } from "../vector/varlen.js";
import type { Batch, FlatVector, Selection, Vector } from "../vector/vector.js";
import type { ByteWriter } from "./bytes.js";
import { Catalog, encodeLogged, type LoggedChange } from "./catalog.js";
import { ColumnChunk, encodeColumn, isNullAt, MEMORY_CHUNK_ROWS } from "./columns.js";
import type { KeyDelta } from "./key-hash.js";
import type { Memtable } from "./memtable.js";
import { readRows } from "./rows.js";
import type { GroupDeletes, Shared, WriteState } from "./state.js";
import {
  SnapshotImpl,
  ViewIndex,
  otherIndexLayersEndBefore,
  type TableSnapshotImpl,
} from "./view.js";
import { need } from "./bytes.js";

/** Everything committed and pending (constraint checks see the whole transaction). */
export const ALL = PENDING_BASE + MAX_STATEMENTS;

/** What transactions need from the write path. */
export interface EngineHooks {
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
  admit(bytes: number): MaybePromise<void>;
  commit(tx: TransactionImpl, request: CommitRequest | null): Promise<CommitReceipt>;
  release(tx: TransactionImpl): void;
  label(): string;
}

export type Undo =
  | {
      readonly kind: "rows";
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
    }
  }

  async commit(request: CommitRequest | null): Promise<CommitReceipt> {
    this.#requireOpen();
    if (this.#open !== null) throw new TransactionStateError("A statement is still open");
    this.state = "committing";
    try {
      const receipt = await this.hooks.commit(this, request);
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
    let deletes: Map<number, Array<{ rowId: number; keys: readonly Uint8Array[] }>> | null = null;
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
            inserts: { firstRowId: memtable.firstRowId + from, rowCount: count, columns },
            deletes: null,
          });
          from = end;
        }
      }
      if (deletes !== null)
        for (const [tableId, list] of deletes) {
          let schemaVersion: number | undefined;
          for (const range of inserts)
            if (range.memtable.tableId === tableId) schemaVersion = range.memtable.schemaVersion;
          schemaVersion ??= need(this.tableVersion(tableId));
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
          list.push({ rowId: entry.rowId, keys: entry.keys });
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
  if (change.op === "createTable")
    return {
      kind: "createTable",
      tableId: change.table.id,
      definition: encodeLogged(change, allocator),
    };
  if (change.op === "dropTable") return { kind: "dropTable", tableId: change.tableId };
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
  readonly #start: number;
  #state: "open" | "failed" | "ended" = "open";
  #writers: Map<string, TableWriterImpl> | null = null;
  #plain: TableWriterImpl | null = null;
  /** Row ids this statement updated or deleted, per table (row-affected-twice). */
  #touchedMap: Map<number, Set<number>> | null = null;

  get touched(): Map<number, Set<number>> {
    return (this.#touchedMap ??= new Map());
  }
  /** Whether this statement inserted row `rowId` of `tableId` (ON CONFLICT `statement`). */
  inserted(tableId: number, rowId: number): boolean {
    const undo = this.tx.undo;
    for (let k = this.#start; k < undo.length; k++) {
      const entry = undo[k];
      if (entry?.kind !== "rows" || entry.tableId !== tableId) continue;
      const first = entry.memtable.firstRowId + entry.before;
      if (rowId >= first && rowId < first + entry.count) return true;
    }
    return false;
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

  /** Runs `work`; any error fails the statement (its only valid call is then `rollback`). */
  guard<T>(work: () => MaybePromise<T>): MaybePromise<T> {
    if (this.#state !== "open") throw new TransactionStateError(`The statement is ${this.#state}`);
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

  #current: SnapshotImpl | null = null;

  /**
   * A view of everything committed and pending, for checks: one per statement, replaced when the
   * write path's state changed (a freeze or publish), released with the statement. Callers must
   * not release it.
   */
  current(): SnapshotImpl {
    const state = this.tx.hooks.state();
    if (this.#current !== null && this.#current.state === state) return this.#current;
    this.#current?.releaseInternal();
    this.#current = this.#fresh();
    return this.#current;
  }

  #fresh(): SnapshotImpl {
    return new SnapshotImpl({
      state: this.tx.hooks.state(),
      bound: ALL,
      observedLsn: this.tx.baseLsn,
      catalog: this.tx.catalog,
      shared: this.tx.hooks.shared,
      label: `${this.tx.hooks.label()}/checks`,
      statementView: false,
    });
  }

  table(tableId: number, options: TableWriteOptions = {}): TableWriterImpl {
    if (this.#state !== "open") throw new TransactionStateError(`The statement is ${this.#state}`);
    // The common case (no options, the same table again) skips the keyed lookup.
    const plain = options.assigned === undefined && options.onRepeat === undefined;
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
        this.#writers.set(`${String(this.#plain.tableId)}::error`, this.#plain);
    }
    const key = `${String(tableId)}:${(options.assigned ?? []).join(",")}:${options.onRepeat ?? "error"}`;
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

  changeCatalog(change: CatalogChange): MaybePromise<{ readonly id: number | null }> {
    return this.guard(() => {
      const before = this.tx.catalog;
      const { catalog, logged, id } = before.change(change, this.tx.hooks.nextCatalogVersion());
      return settle(this.tx.hooks.prepareCatalog(catalog), () =>
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

  end(): Promise<void> {
    const finish = () => {
      this.#state = "ended";
      this.#releaseViews();
      this.tx.ended(this);
    };
    if (
      this.suspects.length === 0 &&
      this.childChecks.length === 0 &&
      this.parentChecks.length === 0
    ) {
      // Nothing to re-verify: end now (no check view, no promise chain).
      if (this.#state !== "open")
        throw new TransactionStateError(`The statement is ${this.#state}`);
      finish();
      return RESOLVED;
    }
    return Promise.resolve(
      this.guard(async () => {
        await this.#actions();
        await this.#verify();
      }),
    ).then(finish);
  }

  #releaseViews(): void {
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
          const children = await index.holders(
            check.key,
            support.columnIds.length > fk.columnIds.length,
          );
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
        const children = await new ViewIndex(
          nonUnique(support),
          this.current().internalTable(child.id),
        ).holders(check.key, support.columnIds.length > fk.columnIds.length);
        if (children.length === 0) continue;
        const writer = new TableWriterImpl(this, child, { onRepeat: "first" });
        if (fk.onDelete === "cascade") await writer.applyDelete(children);
        else if (fk.onDelete === "setNull")
          await writer.applyUpdate(
            children,
            fk.columnIds,
            fk.columnIds.map((columnId) => ({
              vector: nullVector(need(child.columns.find((column) => column.id === columnId)).type),
              row: 0,
            })),
            true,
          );
        else
          throw new ForeignKeyViolationError(check.parentTableId, fk.id, "referenced", check.key);
      }
  }

  rollback(): void {
    if (this.#state === "ended") return;
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
    this.#onRepeat = options.onRepeat ?? "error";
    this.#referenced = referencedIndexes(statement.catalog, def.id);
  }

  #columnIndex(columnId: number): number {
    const index = this.def.columns.findIndex((column) => column.id === columnId);
    if (index < 0) throw new RangeError(`Table ${this.def.name} has no column ${String(columnId)}`);
    return index;
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
    rows: Uint16Array,
    length: number,
    changed: readonly number[] | null,
  ): void {
    this.def.columns.forEach((column, c) => {
      if (column.nullable) return;
      const vector = need(columns[c]);
      for (const row of rows)
        if (isNullAt(vector, row)) throw new NotNullViolationError(this.tableId, column.id, row);
    });
    if (this.def.checks.length === 0) return;
    const selection: Selection | null =
      rows.length === length ? null : { rows: Uint16Array.from(rows), count: rows.length };
    const failure = this.statement
      .rules(this.tableId)
      .check({ length, columns, selection }, changed);
    if (failure !== null) throw new CheckViolationError(this.tableId, failure.checkId, failure.row);
  }

  insert(batch: Batch, written: Selection | null): MaybePromise<number> {
    return this.statement.guard(() => {
      this.#checkShape(
        batch.columns,
        0,
        this.def.columns.map((column) => column.id),
      );
      const rows = activeRows(batch);
      this.#validate(batch.columns, rows, batch.length, null);
      const pending = this.insertRows(batch.columns, rows);
      if (written !== null) {
        written.rows.set(rows);
        written.count = rows.length;
      }
      return settle(pending, () => rows.length);
    });
  }

  /** Appends `rows` of `columns` (complete, validated) and their index entries. */
  insertRows(columns: readonly Vector[], rows: Uint16Array): MaybePromise<void> {
    // Memtable memory is admitted before anything is appended (ruling 6).
    if (rows.length <= MEMORY_CHUNK_ROWS)
      return settle(this.statement.tx.hooks.admit(this.#estimate(columns, rows)), () =>
        this.#insertRows(columns, rows),
      );
    // An input call can exceed the ordinary cap even though its individual chunks fit.
    // Admit only the next chunk, letting completed pending chunks stage at each boundary.
    // These are still one statement: checks and rollback include every appended piece.
    let offset = 0;
    const next = (): MaybePromise<void> =>
      this.statement.guard(() => {
        while (offset < rows.length) {
          const piece = rows.subarray(offset, offset + MEMORY_CHUNK_ROWS);
          offset += piece.length;
          const pending = settle(
            this.statement.tx.hooks.admit(this.#estimate(columns, piece)),
            () => this.statement.guard(() => this.#insertRows(columns, piece)),
          );
          if (pending instanceof Promise) return pending.then(next);
        }
      });
    return next();
  }

  /** An upper estimate of the memtable bytes `rows` of `columns` take (rows and index entries). */
  #estimate(columns: readonly Vector[], rows: Uint16Array): number {
    let bytes =
      4096 * columns.length + rows.length * (16 * columns.length + 96 * this.def.indexes.length);
    for (const vector of columns)
      if (vector.form === "flat" && vector.data.layout === "varlen") {
        const offsets = vector.data.offsets;
        for (const row of rows) bytes += 2 * ((offsets[row + 1] ?? 0) - (offsets[row] ?? 0));
      }
    return bytes;
  }

  #insertRows(columns: readonly Vector[], rows: Uint16Array): MaybePromise<void> {
    const tx = this.statement.tx;
    const seq = this.statement.seq;
    let puts: Array<Promise<void>> | null = null;
    // Row ids of the new rows, for their foreign-key checks (only tables with foreign keys).
    const rowIds: number[] | null = this.def.foreignKeys.length > 0 ? [] : null;
    let done = 0;
    while (done < rows.length) {
      const { memtable, created, frozen } = tx.hooks.liveMemtable(this.tableId, rows.length - done);
      if (created)
        tx.undo.push({
          kind: "freeze",
          statement: this.statement.ordinal,
          tableId: this.tableId,
          created: memtable,
          frozen,
        });
      const take = Math.min(memtable.capacity, rows.length - done);
      const before = memtable.rowCount;
      memtable.append(columns, rows, done, take);
      memtable.mark(seq);
      tx.undo.push({
        kind: "rows",
        statement: this.statement.ordinal,
        tableId: this.tableId,
        memtable,
        before,
        count: take,
      });
      const firstRowId = memtable.firstRowId + before;
      if (rowIds !== null) for (let k = 0; k < take; k++) rowIds.push(firstRowId + k);
      // Index entries go to the memtable that took the rows (its layer covers them).
      const piece = take === rows.length ? rows : rows.subarray(done, done + take);
      const put = this.putKeys(this.def.indexes, columns, piece, firstRowId, memtable);
      if (put instanceof Promise) (puts ??= []).push(put);
      done += take;
    }
    if (rowIds !== null) this.#childChecks(columns, rows, rowIds);
    return puts === null ? undefined : Promise.all(puts).then(() => undefined);
  }

  #keyColumns(index: IndexDef, columns: readonly Vector[]): KeyColumn[] {
    return index.columnIds.map((columnId) => ({
      vector: need(columns[this.#columnIndex(columnId)]),
      direction: "asc",
      nulls: "first",
    }));
  }

  /** Encodes the keys of `index` for `rows` (physical rows of `columns`, or all when null). */
  #encode(
    index: IndexDef,
    columns: readonly Vector[],
    rows: Uint16Array | null,
    count: number,
  ): KeyBatch {
    const keys = this.#keys;
    const selection = rows === null ? null : { rows, count: rows.length };
    const result = encodeKeyBatch(
      "equality",
      this.#keyColumns(index, columns),
      { length: count, selection },
      keys,
    );
    if (!result.done) throw new RangeError("An index key batch did not fit its byte target");
    return keys;
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
  ): MaybePromise<void> {
    const tx = this.statement.tx;
    const seq = this.statement.seq;
    const count = rows === null ? (typeof rowIds === "number" ? 0 : rowIds.length) : rows.length;
    let checks: Array<Promise<void>> | null = null;
    for (const index of indexes) {
      const delta = memtable.deltas.get(index.id);
      if (delta === undefined) continue;
      const keys = this.#encode(index, columns, rows, columns[0]?.length ?? count);
      const first = delta.entryCount;
      try {
        for (let k = 0; k < count; k++) {
          const row = rows === null ? k : (rows[k] ?? 0);
          if (this.#hasNullKey(index, columns, row)) continue;
          const rowId = typeof rowIds === "number" ? rowIds + k : (rowIds[k] ?? 0);
          delta.add(
            keys.writer.bytes,
            keys.offsets[k] ?? 0,
            keys.offsets[k + 1] ?? 0,
            rowId,
            seq,
            false,
          );
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
    for (let entry = from + 1; entry < to; entry++) {
      const slot = delta.keyOf(entry);
      const start = delta.keyStart(slot);
      const end = delta.keyEnd(slot);
      if (compareBytes(bytes, start, end, bytes, delta.keyStart(low), delta.keyEnd(low)) < 0)
        low = slot;
      if (compareBytes(bytes, start, end, bytes, delta.keyStart(high), delta.keyEnd(high)) > 0)
        high = slot;
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
        const found = reader.countRange(delta.keyArena(), delta.keyStart(slot), delta.keyEnd(slot));
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
      const keys = this.#encode(index, columns, rows, columns[0]?.length ?? count);
      for (let k = 0; k < count; k++) {
        const row = rows === null ? k : (rows[k] ?? 0);
        if (this.#hasNullKey(index, columns, row)) continue;
        this.statement.childChecks.push({
          childTableId: this.tableId,
          rowId: rowIds[k] ?? 0,
          fkId: fk.id,
          parentTableId: fk.parentTableId,
          parentIndexId: fk.parentIndexId,
          key: keys.key(k).slice(),
        });
      }
    }
  }

  /** Row ids of the active rows' locators (column 0), applying the row-affected-twice rule. */
  #targets(batch: Batch): { rows: Uint16Array; rowIds: number[] } {
    const locators = batch.columns[0];
    if (
      locators?.form !== "flat" ||
      locators.data.layout !== "f64" ||
      locators.type.kind !== "locator"
    )
      throw new TypeError("Column 0 must be a flat locator vector");
    const view = this.statement.view.internalTable(this.tableId);
    let touched = this.statement.touched.get(this.tableId);
    if (touched === undefined) {
      touched = new Set();
      this.statement.touched.set(this.tableId, touched);
    }
    const kept: number[] = [];
    const rowIds: number[] = [];
    for (const row of activeRows(batch)) {
      if (
        locators.validity !== null &&
        ((locators.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0
      )
        throw new TypeError("An update or delete target has a null locator");
      const rowId = view.rowId(locators.data.values[row] ?? 0);
      if (touched.has(rowId)) {
        if (this.#onRepeat === "error") throw new CardinalityViolationError(this.tableId, row);
        continue;
      }
      touched.add(rowId);
      kept.push(row);
      rowIds.push(rowId);
    }
    return { rows: Uint16Array.from(kept), rowIds };
  }

  update(batch: Batch, written: Selection | null): MaybePromise<number> {
    return this.statement.guard(() => {
      this.#checkShape(batch.columns, 1, this.#assigned);
      const { rows, rowIds } = this.#targets(batch);
      if (written !== null) {
        written.rows.set(rows);
        written.count = rows.length;
      }
      if (rowIds.length === 0) return 0;
      const sources = this.#assigned.map((_, k) => ({ vector: need(batch.columns[k + 1]), rows }));
      return settle(this.#update(rowIds, this.#assigned, sources, false), () => rowIds.length);
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
        if (k === undefined) return need(old[c]);
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
      return all_(removed, added);
    });
  }

  delete(batch: Batch, written: Selection | null): MaybePromise<number> {
    return this.statement.guard(() => {
      if (batch.columns.length !== 1)
        throw new TypeError("A delete batch holds the locator column only");
      const { rows, rowIds } = this.#targets(batch);
      if (written !== null) {
        written.rows.set(rows);
        written.count = rows.length;
      }
      if (rowIds.length === 0) return 0;
      return settle(this.#delete(rowIds), () => rowIds.length);
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
          const keys = this.#encode(index, old, null, rowIds.length);
          for (let k = 0; k < rowIds.length; k++)
            if (!this.#hasNullKey(index, old, k))
              this.statement.parentChecks.push({
                parentTableId: this.tableId,
                indexId: index.id,
                key: keys.key(k).slice(),
                next: null,
              });
        }
      return removed;
    });
  }

  /** For rows whose referenced key changed (an UPDATE), the old key must not be left referenced. */
  #parentChecksForChange(old: readonly Vector[], next: readonly Vector[], count: number): void {
    for (const index of this.def.indexes) {
      if (!this.#referenced.has(index.id)) continue;
      const before = this.#encode(index, old, null, count);
      const oldKeys = Array.from({ length: count }, (_, k) => before.key(k).slice());
      const after = this.#encode(index, next, null, count);
      for (let k = 0; k < count; k++) {
        if (this.#hasNullKey(index, old, k)) continue;
        const key = oldKeys[k] as Uint8Array;
        const now = after.key(k);
        const same = key.length === now.length && key.every((byte, b) => byte === now[b]);
        if (!same)
          this.statement.parentChecks.push({
            parentTableId: this.tableId,
            indexId: index.id,
            key,
            next: { vectors: next, row: k },
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
      const delta = live.deltas.get(index.id);
      const keys = this.#encode(index, old, null, rowIds.length);
      const first = delta?.entryCount ?? 0;
      try {
        for (let k = 0; k < rowIds.length; k++) {
          if (this.#hasNullKey(index, old, k)) {
            keysPerRow[k]?.push(NULL_KEY);
            continue;
          }
          const start = keys.offsets[k] ?? 0;
          const end = keys.offsets[k + 1] ?? 0;
          keysPerRow[k]?.push(keys.writer.bytes.slice(start, end));
          delta?.add(keys.writer.bytes, start, end, rowIds[k] ?? 0, seq, true);
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
        });
      }
    });
    return undefined;
  }

  conflicts(batch: Batch, arbiters: readonly number[], out: ConflictOutput): MaybePromise<void> {
    return this.statement.guard(() => {
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
      const current = this.statement.current();
      const table = current.internalTable(this.tableId);
      const view = this.statement.view.internalTable(this.tableId);
      const seen = indexes.map(() => new Map<string, number>());
      const found: number[] = [];
      const keyed = indexes.map((index) => {
        const keys = this.#encode(index, batch.columns, rows, batch.length);
        return Array.from({ length: rows.length }, (_, k) => keys.key(k).slice());
      });
      const decide = (k: number): MaybePromise<void> => {
        for (; k < rows.length; k++) {
          const row = rows[k] ?? 0;
          out.kinds[row] = ConflictKind.none;
          for (let i = 0; i < indexes.length; i++) {
            const index = need(indexes[i]);
            if (this.#hasNullKey(index, batch.columns, row)) continue;
            const key = keyed[i]?.[k] as Uint8Array;
            const result = new ViewIndex(index, table).resolve(key, found);
            if (result instanceof Promise) return result.then(() => decide(k));
            if (found.length > 0) {
              const rowId = need(found[0]);
              out.kinds[row] = this.statement.inserted(this.tableId, rowId)
                ? ConflictKind.statement
                : ConflictKind.existing;
              out.locators[row] = view.locator(rowId);
              out.indexIds[row] = index.id;
              break;
            }
            const text = String.fromCharCode(...key);
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
              const text = String.fromCharCode(...(keyed[i]?.[k] as Uint8Array));
              if (!seen[i]?.has(text)) seen[i]?.set(text, row);
            }
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
