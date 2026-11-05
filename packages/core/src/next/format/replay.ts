/**
 * Replay rules: which parts of each logged transaction recovery applies, given the durable root.
 * The write path (engine) and recovery both follow them; `Replayer` is the reference decision
 * procedure, tested by the crash sweeps in recovery-sweep.test.ts.
 *
 * ## State and checkpoints
 * - **Global state** (catalog, sequences, request ledger) changes only through logged
 *   transactions, applied in LSN order like row data. The root records `globalLsn`: its catalog,
 *   sequences and ledger include every transaction with LSN ≤ `globalLsn`.
 * - **Table rows** are checkpointed per table: `flushedThroughLsn` says the table's groups contain
 *   its rows from every transaction with LSN ≤ it.
 * - A root may be published at any time with any `globalLsn` ≤ the last durable LSN (the owner
 *   always holds current global state in memory).
 *
 * ## Protocol obligations of the writer
 * 1. **Flush before a schema change.** Before logging ALTER of table T, flush T's memtable and
 *    publish a root with `T.flushedThroughLsn` ≥ the LSN before the ALTER. So no row of T encoded
 *    under the old schema is ever replayed against the new one; replay verifies it (schema
 *    versions must match).
 * 2. **Table ids are never reused.** An id below `nextTableId` that the catalog lacks was dropped.
 * 3. **Recycle the log only when covered:** every table's `flushedThroughLsn` and `globalLsn` reach
 *    the old log's last LSN, in the root that switches salt (format/manifest.ts).
 *
 * ## Rules for a transaction with LSN L (applied when its last part is read)
 * - Global effects (catalog change, sequence reservations, ledger outcome) apply iff L > globalLsn.
 *   Catalog changes apply first, then rows.
 *   - createTable(id): id ≥ nextTableId; the table starts with flushedThroughLsn = L, version 1.
 *   - alterTable(id, v): the table exists, v = version + 1, and no rows of it were replayed yet.
 *   - dropTable(id): the table exists; rows replayed for it are discarded.
 * - Rows of table T apply iff T exists at that point of replay, L > T.flushedThroughLsn, and the
 *   change's schema version equals T's. A table absent from the catalog with id < nextTableId was
 *   dropped: its rows are skipped if L ≤ globalLsn (rows before the drop) and are corruption if
 *   L > globalLsn (rows after their table was dropped). An unknown id is corruption.
 * - **Coverage:** the log must reach every checkpoint the root claims: the last valid LSN must be
 *   ≥ globalLsn and ≥ every flushedThroughLsn (a log that ends earlier lost acknowledged work;
 *   only a lying flush produces it).
 */
import { LogCorruptionError } from "./log-frame.js";

export interface TableCheckpoint {
  readonly flushedThroughLsn: number;
  readonly schemaVersion: number;
}

export interface ReplayCheckpoint {
  readonly globalLsn: number;
  readonly nextTableId: number;
  readonly tables: ReadonlyMap<number, TableCheckpoint>;
}

/** What one transaction touches, read from its body. */
export interface TransactionEffects {
  readonly catalog:
    | { readonly kind: "createTable"; readonly tableId: number }
    | { readonly kind: "alterTable"; readonly tableId: number; readonly schemaVersion: number }
    | { readonly kind: "dropTable"; readonly tableId: number }
    | { readonly kind: "other" }
    | null;
  /** Sequences or a request outcome: global state without a catalog change. */
  readonly otherGlobal: boolean;
  readonly tables: ReadonlyArray<{ readonly tableId: number; readonly schemaVersion: number }>;
}

export interface ReplayDecision {
  /** Apply the catalog change, sequences and ledger outcome. */
  readonly global: boolean;
  /** Apply the rows of these tables (a subset of the transaction's tables, in its order). */
  readonly tables: readonly number[];
  /** Discard the replayed rows of this table (it was dropped). */
  readonly dropped: number | null;
}

export class Replayer {
  readonly #globalLsn: number;
  readonly #tables: Map<number, TableCheckpoint>;
  readonly #dirty = new Set<number>();
  #nextTableId: number;
  #lastLsn = 0;

  constructor(checkpoint: ReplayCheckpoint) {
    this.#globalLsn = checkpoint.globalLsn;
    this.#nextTableId = checkpoint.nextTableId;
    this.#tables = new Map(checkpoint.tables);
  }

  /** The rule's coverage check; call with the scan's `lastLsn` before replaying. */
  static checkCoverage(checkpoint: ReplayCheckpoint, lastLsn: number): void {
    let needed = checkpoint.globalLsn;
    for (const table of checkpoint.tables.values())
      needed = Math.max(needed, table.flushedThroughLsn);
    if (lastLsn < needed)
      throw new LogCorruptionError(
        `The log ends at LSN ${String(lastLsn)} before the root's checkpoint at ${String(needed)}`,
      );
  }

  next(lsn: number, effects: TransactionEffects): ReplayDecision {
    if (lsn <= this.#lastLsn) throw new LogCorruptionError("Transactions out of LSN order");
    this.#lastLsn = lsn;
    const global = lsn > this.#globalLsn;
    let dropped: number | null = null;
    const change = effects.catalog;
    if (global && change !== null && change.kind !== "other") {
      const table = this.#tables.get(change.tableId);
      if (change.kind === "createTable") {
        if (table !== undefined || change.tableId < this.#nextTableId)
          throw new LogCorruptionError(`Table ${String(change.tableId)} is created twice`);
        this.#tables.set(change.tableId, { flushedThroughLsn: lsn, schemaVersion: 1 });
        this.#nextTableId = change.tableId + 1;
      } else if (table === undefined) {
        throw new LogCorruptionError(`DDL on unknown table ${String(change.tableId)}`);
      } else if (change.kind === "alterTable") {
        if (change.schemaVersion !== table.schemaVersion + 1)
          throw new LogCorruptionError(`Table ${String(change.tableId)} skips a schema version`);
        if (this.#dirty.has(change.tableId))
          throw new LogCorruptionError(
            `Table ${String(change.tableId)} has unflushed rows under its old schema`,
          );
        this.#tables.set(change.tableId, { ...table, schemaVersion: change.schemaVersion });
      } else {
        this.#tables.delete(change.tableId);
        this.#dirty.delete(change.tableId);
        dropped = change.tableId;
      }
    }
    const tables: number[] = [];
    for (const { tableId, schemaVersion } of effects.tables) {
      const table = this.#tables.get(tableId);
      if (table === undefined) {
        if (tableId >= this.#nextTableId)
          throw new LogCorruptionError(`Rows for unknown table ${String(tableId)}`);
        if (global)
          throw new LogCorruptionError(`Rows for table ${String(tableId)} after it was dropped`);
        continue;
      }
      if (lsn <= table.flushedThroughLsn) continue;
      if (schemaVersion !== table.schemaVersion)
        throw new LogCorruptionError(
          `Rows for table ${String(tableId)} were written under schema ${String(schemaVersion)}, not ${String(table.schemaVersion)}`,
        );
      this.#dirty.add(tableId);
      tables.push(tableId);
    }
    return { global: global && (change !== null || effects.otherGlobal), tables, dropped };
  }
}
