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
import { SnapshotImpl, ViewIndex, type TableSnapshotImpl } from "./view.js";
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
  /** Undoes a freeze: drops `created` (now empty) and makes `frozen` live again if it still can be. */
  undoFreeze(tableId: number, created: Memtable, frozen: Memtable | null): void;
  /** Creates (or, with null, removes) the in-memory state of a table this transaction creates. */
  createTableState(def: TableDef | null, tableId: number): void;
  /** Adds an index's delta to the table's live memtable (createIndex). */
  addIndexDelta(tableId: number, indexId: number): Memtable;
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
    }
  | {
      readonly kind: "delta";
      readonly statement: number;
      readonly delta: KeyDelta;
      readonly entry: number;
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
    };

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
  readonly #savepoints = new Map<number, number>();
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
  effects(scratch: ByteWriter): BodyEffect[] {
    scratch.reset();
    const ranges_: Array<{ readonly effect: number; readonly spans: Array<[number, number]> }> = [];
    const effects: BodyEffect[] = [];
    for (const entry of this.undo)
      if (entry.kind === "catalog")
        for (const change of entry.logged) effects.push(catalogEffect(change));
    const dropped = new Set<number>();
    for (const change of effects) if (change.kind === "dropTable") dropped.add(change.tableId);
    // Inserted ranges per memtable, in order; then deletes per table.
    const ranges = new Map<Memtable, number>();
    const deletes = new Map<number, Array<{ rowId: number; keys: readonly Uint8Array[] }>>();
    for (const entry of this.undo) {
      if (entry.kind === "rows" && !ranges.has(entry.memtable))
        ranges.set(entry.memtable, entry.before);
      if (entry.kind === "memDelete" || entry.kind === "groupDelete") {
        let list = deletes.get(entry.tableId);
        if (list === undefined) {
          list = [];
          deletes.set(entry.tableId, list);
        }
        list.push({ rowId: entry.rowId, keys: entry.keys });
      }
    }
    for (const [memtable, first] of ranges) {
      if (dropped.has(memtable.tableId) || memtable.rowCount <= first) continue;
      for (let from = first; from < memtable.rowCount;) {
        const chunk = Math.floor(from / MEMORY_CHUNK_ROWS);
        const slot = from % MEMORY_CHUNK_ROWS;
        const end = Math.min(memtable.rowCount, (chunk + 1) * MEMORY_CHUNK_ROWS);
        const count = end - from;
        const spans: Array<[number, number]> = memtable.chunks.map((list) => {
          const stored = list[chunk];
          if (stored === undefined) throw new RangeError("A pending memtable chunk is missing");
          const start = scratch.length;
          encodeColumn(stored.view(), slot, count, scratch);
          return [start, scratch.length];
        });
        ranges_.push({ effect: effects.length, spans });
        effects.push({
          kind: "rows",
          tableId: memtable.tableId,
          schemaVersion: memtable.schemaVersion,
          inserts: { firstRowId: memtable.firstRowId + from, rowCount: count, columns: [] },
          deletes: null,
        });
        from = end;
      }
    }
    for (const [tableId, list] of deletes) {
      if (dropped.has(tableId)) continue;
      const def = this.catalog.table(tableId);
      if (def === undefined) continue;
      list.sort((a, b) => a.rowId - b.rowId);
      effects.push({
        kind: "rows",
        tableId,
        schemaVersion: def.schemaVersion,
        inserts: null,
        deletes: {
          rowIds: Float64Array.from(list, (item) => item.rowId),
          keys: list.map((item) => item.keys),
        },
      });
    }
    // Column bytes are final now: point the effects at views of the scratch buffer.
    for (const { effect, spans } of ranges_) {
      const rows = effects[effect] as Extract<BodyEffect, { kind: "rows" }>;
      const inserts = rows.inserts;
      if (inserts === null) continue;
      effects[effect] = {
        ...rows,
        inserts: {
          ...inserts,
          columns: spans.map(([start, end]) => scratch.bytes.subarray(start, end)),
        },
      };
    }
    return effects;
  }

  /** Restamps every pending change with `lsn` (after the frame is appended). */
  restamp(lsn: number): void {
    const memtables = new Set<Memtable>();
    for (const entry of this.undo) {
      switch (entry.kind) {
        case "rows":
          memtables.add(entry.memtable);
          break;
        case "delta":
          entry.delta.restamp(entry.entry, lsn);
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
    for (const memtable of memtables) memtable.restamp(PENDING_BASE, lsn);
  }
}

function catalogEffect(change: LoggedChange): BodyEffect {
  if (change.op === "createTable")
    return { kind: "createTable", tableId: change.table.id, definition: encodeLogged(change) };
  if (change.op === "dropTable") return { kind: "dropTable", tableId: change.tableId };
  return { kind: "catalog", definition: encodeLogged(change) };
}

function undoEntry(tx: TransactionImpl, entry: Undo): void {
  switch (entry.kind) {
    case "rows":
      entry.memtable.truncate(entry.before, pendingSeq(entry.statement));
      break;
    case "delta":
      entry.delta.pop(entry.entry);
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
      entry.memtable.deltas.delete(entry.indexId);
      break;
  }
}

// ── Statements ─────────────────────────────────────────────────────────────────────────────────

interface FkCheck {
  readonly childTableId: number;
  readonly fkId: number;
  readonly parentTableId: number;
  readonly parentIndexId: number;
  readonly key: Uint8Array;
}

interface ParentCheck {
  readonly parentTableId: number;
  readonly indexId: number;
  readonly key: Uint8Array;
}

export class StatementImpl implements Statement {
  readonly ordinal: number;
  readonly view: SnapshotImpl;
  readonly seq: number;
  readonly tx: TransactionImpl;
  readonly #rules: (tableId: number) => TableRules;
  readonly #start: number;
  #state: "open" | "failed" | "ended" = "open";
  readonly #writers = new Map<string, TableWriterImpl>();
  /** Row ids this statement updated or deleted, per table (row-affected-twice). */
  readonly touched = new Map<number, Set<number>>();
  /** Row ids this statement inserted, per table (ON CONFLICT `statement`). */
  readonly inserted = new Map<number, Set<number>>();
  readonly suspects: Array<{ tableId: number; indexId: number; key: Uint8Array }> = [];
  readonly childChecks: FkCheck[] = [];
  readonly parentChecks: ParentCheck[] = [];

  constructor(tx: TransactionImpl, ordinal: number, options: StatementOptions) {
    this.tx = tx;
    this.ordinal = ordinal;
    this.seq = pendingSeq(ordinal);
    this.#rules = options.rules;
    this.#start = tx.undo.length;
    this.view = new SnapshotImpl({
      state: tx.hooks.state(),
      bound: statementBound(ordinal),
      observedLsn: tx.baseLsn,
      catalog: tx.catalog,
      shared: tx.hooks.shared,
      label: `${tx.hooks.label()}/statement#${String(ordinal)}`,
      statementView: true,
    });
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
    const key = `${String(tableId)}:${(options.assigned ?? []).join(",")}:${options.onRepeat ?? "error"}`;
    let writer = this.#writers.get(key);
    if (writer === undefined) {
      const def = this.tx.catalog.table(tableId);
      if (def === undefined) throw new RangeError(`Table ${String(tableId)} does not exist`);
      writer = new TableWriterImpl(this, def, options);
      this.#writers.set(key, writer);
    }
    return writer;
  }

  changeCatalog(change: CatalogChange): MaybePromise<{ readonly id: number | null }> {
    return this.guard(() => {
      const before = this.tx.catalog;
      const { catalog, logged, id } = before.change(change, this.tx.hooks.nextCatalogVersion());
      this.tx.undo.push({ kind: "catalog", statement: this.ordinal, before, logged });
      this.tx.catalog = catalog;
      const work: Array<MaybePromise<void>> = [];
      for (const entry of logged) {
        if (entry.op === "createTable") this.tx.hooks.createTableState(entry.table, entry.table.id);
        if (entry.op === "createIndex") work.push(this.#backfill(entry.index));
      }
      return settle(all(work), () => ({ id }));
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
    return Promise.resolve(this.guard(() => this.#verify())).then(() => {
      this.#state = "ended";
      this.#releaseViews();
      this.tx.ended(this);
    });
  }

  #releaseViews(): void {
    this.view.releaseInternal();
    this.#current?.releaseInternal();
    this.#current = null;
  }

  async #verify(): Promise<void> {
    if (
      this.suspects.length === 0 &&
      this.childChecks.length === 0 &&
      this.parentChecks.length === 0
    )
      return;
    const current = this.current();
    try {
      const rows: number[] = [];
      const lookup = async (tableId: number, indexId: number, key: Uint8Array): Promise<number> => {
        const table = current.internalTable(tableId);
        const def = table.def.indexes.find((index) => index.id === indexId);
        if (def === undefined) return 0;
        const reader = new ViewIndex({ ...def, unique: false }, table);
        const result = reader.resolve(key, rows);
        if (result instanceof Promise) await result;
        return rows.length;
      };
      for (const suspect of this.suspects)
        if ((await lookup(suspect.tableId, suspect.indexId, suspect.key)) > 1)
          throw new UniqueViolationError(suspect.tableId, suspect.indexId, suspect.key);
      for (const check of this.childChecks)
        if ((await lookup(check.parentTableId, check.parentIndexId, check.key)) === 0)
          throw new ForeignKeyViolationError(
            check.childTableId,
            check.fkId,
            "missingParent",
            check.key,
          );
      for (const check of this.parentChecks) {
        if ((await lookup(check.parentTableId, check.indexId, check.key)) > 0) continue;
        for (const child of this.tx.catalog.tables)
          for (const fk of child.foreignKeys) {
            if (!fk.enforced || fk.parentIndexId !== check.indexId || fk.supportIndexId === null)
              continue;
            if ((await lookup(child.id, fk.supportIndexId, check.key)) > 0)
              throw new ForeignKeyViolationError(
                check.parentTableId,
                fk.id,
                "referenced",
                check.key,
              );
          }
      }
    } finally {
      // `current` belongs to the statement (released with it).
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
    const referenced = new Set<number>();
    for (const table of statement.catalog.tables)
      for (const fk of table.foreignKeys)
        if (fk.enforced && fk.parentTableId === def.id) referenced.add(fk.parentIndexId);
    this.#referenced = referenced;
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
    const tx = this.statement.tx;
    const seq = this.statement.seq;
    const rowIds: number[] = new Array<number>(rows.length);
    const puts: Array<MaybePromise<void>> = [];
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
      });
      for (let k = 0; k < take; k++) rowIds[done + k] = memtable.firstRowId + before + k;
      // Index entries go to the memtable that took the rows (its layer covers them).
      puts.push(
        this.putKeys(
          this.def.indexes,
          columns,
          rows.subarray(done, done + take),
          rowIds.slice(done, done + take),
          memtable,
        ),
      );
      done += take;
    }
    let inserted = this.statement.inserted.get(this.tableId);
    if (inserted === undefined) {
      inserted = new Set();
      this.statement.inserted.set(this.tableId, inserted);
    }
    for (const rowId of rowIds) inserted.add(rowId);
    this.#childChecks(columns, rows);
    return all(puts);
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
   * Puts index entries for new rows (`rows` of `columns`; row ids `rowIds`) into `memtable`'s
   * deltas, and records unique suspects.
   */
  putKeys(
    indexes: readonly IndexDef[],
    columns: readonly Vector[],
    rows: Uint16Array | null,
    rowIds: readonly number[],
    memtable: Memtable,
  ): MaybePromise<void> {
    const tx = this.statement.tx;
    const seq = this.statement.seq;
    const count = rows === null ? rowIds.length : rows.length;
    const checks: Array<MaybePromise<void>> = [];
    for (const index of indexes) {
      const delta = memtable.deltas.get(index.id);
      if (delta === undefined) continue;
      const keys = this.#encode(index, columns, rows, columns[0]?.length ?? count);
      const unique: Uint8Array[] = [];
      for (let k = 0; k < count; k++) {
        const row = rows === null ? k : (rows[k] ?? 0);
        if (this.#hasNullKey(index, columns, row)) continue;
        const start = keys.offsets[k] ?? 0;
        const end = keys.offsets[k + 1] ?? 0;
        const entry = delta.add(keys.writer.bytes, start, end, rowIds[k] ?? 0, seq, false);
        tx.undo.push({ kind: "delta", statement: this.statement.ordinal, delta, entry });
        if (index.unique) unique.push(keys.writer.bytes.slice(start, end));
      }
      if (unique.length > 0) checks.push(this.#suspect(index, unique));
    }
    return all(checks);
  }

  /** Records every key of `keys` that more than one live row holds now (re-verified at end). */
  #suspect(index: IndexDef, keys: readonly Uint8Array[]): MaybePromise<void> {
    const current = this.statement.current();
    const reader = new ViewIndex({ ...index, unique: false }, current.internalTable(this.tableId));
    let low: Uint8Array | null = null;
    let high: Uint8Array | null = null;
    for (const key of keys) {
      if (low === null || compareBytes(key, 0, key.length, low, 0, low.length) < 0) low = key;
      if (high === null || compareBytes(key, 0, key.length, high, 0, high.length) > 0) high = key;
    }
    reader.skipRunsOutside(low, high);
    const rows: number[] = [];
    const run = (from: number): MaybePromise<void> => {
      for (let k = from; k < keys.length; k++) {
        const key = need(keys[k]);
        const result = reader.resolve(key, rows);
        if (result instanceof Promise)
          return result.then(() => {
            if (rows.length > 1)
              this.statement.suspects.push({ tableId: this.tableId, indexId: index.id, key });
            return run(k + 1);
          });
        if (rows.length > 1)
          this.statement.suspects.push({ tableId: this.tableId, indexId: index.id, key });
      }
      return undefined;
    };
    return run(0);
  }

  #childChecks(columns: readonly Vector[], rows: Uint16Array | null): void {
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
      if (rowIds.length === 0) {
        if (written !== null) written.count = 0;
        return 0;
      }
      const current = this.statement.current();
      const table = current.internalTable(this.tableId);
      const work = settle(readRows(table, rowIds), (old) => {
        // New rows: assigned columns from the batch, the rest from the old rows.
        const assigned = new Map(
          this.#assigned.map((columnId, k) => [this.#columnIndex(columnId), k + 1]),
        );
        const one = new Uint16Array(1);
        const columns: FlatVector[] = this.def.columns.map((column, c) => {
          const source = assigned.get(c);
          if (source === undefined) return need(old[c]);
          const chunk = new ColumnChunk(column.type);
          for (const row of rows) {
            one[0] = row;
            chunk.append(need(batch.columns[source]), one, 0, 1);
          }
          return chunk.view();
        });
        const all = Uint16Array.from({ length: rowIds.length }, (_, k) => k);
        this.#validate(columns, all, rowIds.length, this.#assigned);
        const removed = this.#deleteRows(table, rowIds, old);
        const added = this.insertRows(columns, all);
        this.#parentChecksForChange(old, columns, rowIds.length);
        if (written !== null) {
          written.rows.set(rows);
          written.count = rows.length;
        }
        return settle(all_(removed, added), () => rowIds.length);
      });
      return work;
    });
  }

  delete(batch: Batch, written: Selection | null): MaybePromise<number> {
    return this.statement.guard(() => {
      if (batch.columns.length !== 1)
        throw new TypeError("A delete batch holds the locator column only");
      const { rows, rowIds } = this.#targets(batch);
      if (rowIds.length === 0) {
        if (written !== null) written.count = 0;
        return 0;
      }
      const current = this.statement.current();
      const table = current.internalTable(this.tableId);
      const work = settle(readRows(table, rowIds), (old) => {
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
                });
          }
        if (written !== null) {
          written.rows.set(rows);
          written.count = rows.length;
        }
        return settle(removed, () => rowIds.length);
      });
      return work;
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
          this.statement.parentChecks.push({ parentTableId: this.tableId, indexId: index.id, key });
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
    // Tombstones go to the newest memtable, full or not (no freeze just for them).
    const live = tx.hooks.liveMemtable(this.tableId, 0).memtable;
    const keysPerRow: Uint8Array[][] = rowIds.map(() => []);
    for (const index of this.def.indexes) {
      const delta = live.deltas.get(index.id);
      const keys = this.#encode(index, old, null, rowIds.length);
      for (let k = 0; k < rowIds.length; k++) {
        if (this.#hasNullKey(index, old, k)) {
          keysPerRow[k]?.push(NULL_KEY);
          continue;
        }
        const start = keys.offsets[k] ?? 0;
        const end = keys.offsets[k + 1] ?? 0;
        keysPerRow[k]?.push(keys.writer.bytes.slice(start, end));
        if (delta === undefined) continue;
        const entry = delta.add(keys.writer.bytes, start, end, rowIds[k] ?? 0, seq, true);
        tx.undo.push({ kind: "delta", statement: this.statement.ordinal, delta, entry });
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
      const inserted = this.statement.inserted.get(this.tableId) ?? new Set<number>();
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
              out.kinds[row] = inserted.has(rowId) ? ConflictKind.statement : ConflictKind.existing;
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

function all_(a: MaybePromise<void>, b: MaybePromise<void>): MaybePromise<void> {
  return all([a, b]);
}

export { hexId, CatalogChangeError };
