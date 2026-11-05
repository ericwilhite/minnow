/**
 * Replay rules: which effects of each logged transaction recovery applies, given the durable
 * root. The write path and recovery both follow them; `Replayer` is the reference decision
 * procedure, exercised by the crash sweeps in recovery-sweep.test.ts.
 *
 * ## Transactions are ordered effect lists
 * A transaction's body is an ordered list of effects (log-frame.ts `CommitFrameBody.effects`):
 * catalog changes (create/alter/drop table, other catalog changes), sequence reservations, row
 * changes (per table, under a schema version), and at most one request outcome. Effects apply in
 * list order, so one transaction can create a table and insert into it, run several DDL
 * statements, or change a table's schema between two row changes.
 *
 * ## State and checkpoints
 * - **Global state** (catalog, sequences, request ledger) changes only through logged effects.
 *   The root's catalog, sequences and ledger include every transaction with LSN ≤ `globalLsn`.
 * - **Table rows** are checkpointed per table: `flushedThroughLsn` says the table's groups hold
 *   its rows from every transaction with LSN ≤ it. A table created by the transaction at LSN L
 *   starts at `flushedThroughLsn = L − 1`, so that transaction's own rows replay.
 *
 * ## Large transactions commit through a staged root
 * A transaction too large to log row by row writes its rows to extents, then publishes a root that
 * holds them as **staged** under the transaction's id (`TableRoot.staged`: present, invisible),
 * and only then logs its commit frame with a `publishedRows` effect naming that root's sequence.
 * The frame is the commit point. Replay of `publishedRows` at LSN L:
 * - follows the rows rule for an absent table: skipped when the table was dropped by a checkpointed
 *   drop (L ≤ globalLsn), corruption for an unknown table or after the drop;
 * - skipped when L ≤ T.flushedThroughLsn: a later root already folded the rows in (so it is never
 *   applied twice);
 * - otherwise the durable root must be at least the named root (else the root was rolled back:
 *   corruption) and must still stage that transaction (else corruption); applying makes the
 *   staged rows and their staged index runs visible together, and consumes the staging. The frame
 *   names the indexes it built runs for; if they differ from the staged runs, replay refuses.
 * After replay, staging no frame claimed belongs to a transaction that never committed: recovery
 * drops it (`Replayer.unclaimedStaging`) with a publish whose root no longer lists it. That publish
 * retires the staging's extents; they are reused only once neither durable root (current and
 * previous slot) nor any pin reaches them (manifest.ts "Reclaim (two generations)"). A root that
 * folds staged rows into the table's groups sets `flushedThroughLsn ≥ L` and removes the staging
 * entry in the same publish.
 *
 * ## Writer obligations
 * 1. **A checkpoint covers the rows before the schema it records.** A root whose `globalLsn` covers
 *    a schema change (ALTER) of table T at LSN A must have `T.flushedThroughLsn ≥ A`: before
 *    publishing such a root, flush T (its memtable rows were converted when the ALTER applied).
 *    So replay never meets a row of T encoded under a schema older than the root's; replay checks
 *    schema versions and reports a violation as corruption.
 * 2. **Table ids are never reused.** An id below `nextTableId` that the catalog lacks was dropped.
 * 3. **Recycle the log only when covered:** `globalLsn` and every table's `flushedThroughLsn` reach
 *    the old log's last LSN, in the root that switches salt (format/manifest.ts).
 *
 * ## Rules for the transaction at LSN L (applied when its last part is read)
 * - Global effects apply iff L > globalLsn, in order:
 *   - createTable(id): id ≥ nextTableId; the table starts with flushedThroughLsn = L − 1, version 1.
 *   - alterTable(id, v): the table exists and v = version + 1.
 *   - dropTable(id): the table exists; rows replayed for it are discarded.
 * - A row change of table T under version v applies iff T exists at that point of replay,
 *   L > T.flushedThroughLsn, and v is T's version at that point (else corruption). If T is absent
 *   with id < nextTableId it was dropped: the change is skipped when L ≤ globalLsn (rows before
 *   the drop) and is corruption when L > globalLsn. An unknown id is corruption.
 * - **Coverage:** the log must reach every checkpoint the root claims: the last valid LSN must be
 *   ≥ globalLsn and ≥ every flushedThroughLsn (only a lying flush produces a shorter log).
 */
import { LogCorruptionError } from "./log-frame.js";

export interface TableCheckpoint {
  readonly flushedThroughLsn: number;
  readonly schemaVersion: number;
  /** Transactions this root stages (rows and their index runs, published, not yet visible). */
  readonly staged?: ReadonlyMap<number, StagedTransaction>;
}

/** A staged transaction as the root holds it: its row groups and the indexes it has runs for. */
export interface StagedTransaction {
  readonly indexIds: readonly number[];
}

export interface ReplayCheckpoint {
  /** The durable root's sequence (for `publishedRows`). */
  readonly rootSequence?: number;
  readonly globalLsn: number;
  readonly nextTableId: number;
  readonly tables: ReadonlyMap<number, TableCheckpoint>;
}

/** One effect of a transaction, as replay sees it (payloads omitted). */
export type Effect =
  | { readonly kind: "createTable"; readonly tableId: number }
  | { readonly kind: "alterTable"; readonly tableId: number; readonly schemaVersion: number }
  | { readonly kind: "dropTable"; readonly tableId: number }
  | { readonly kind: "catalog" }
  | { readonly kind: "sequence" }
  | { readonly kind: "request" }
  | { readonly kind: "rows"; readonly tableId: number; readonly schemaVersion: number }
  | {
      readonly kind: "publishedRows";
      readonly tableId: number;
      readonly schemaVersion: number;
      /** The staging id the root holds the rows under. */
      readonly transaction: number;
      /** Sequence of the root that staged them. */
      readonly rootSequence: number;
      /** The indexes whose staged runs become visible with the rows. */
      readonly indexIds: readonly number[];
    };

export class Replayer {
  readonly #globalLsn: number;
  readonly #rootSequence: number;
  readonly #tables: Map<number, TableCheckpoint>;
  readonly #unclaimed = new Map<number, Map<number, StagedTransaction>>();
  #nextTableId: number;
  #lastLsn = 0;

  constructor(checkpoint: ReplayCheckpoint) {
    this.#globalLsn = checkpoint.globalLsn;
    this.#rootSequence = checkpoint.rootSequence ?? 0;
    this.#nextTableId = checkpoint.nextTableId;
    this.#tables = new Map(checkpoint.tables);
    for (const [tableId, table] of checkpoint.tables)
      if (table.staged !== undefined && table.staged.size > 0)
        this.#unclaimed.set(tableId, new Map(table.staged));
  }

  /**
   * Staging no replayed commit claimed: transactions that never committed. Drop their groups and
   * index runs together.
   */
  unclaimedStaging(): Array<{
    readonly tableId: number;
    readonly transaction: number;
    readonly indexIds: readonly number[];
  }> {
    return [...this.#unclaimed].flatMap(([tableId, transactions]) =>
      [...transactions].map(([transaction, staged]) => ({
        tableId,
        transaction,
        indexIds: staged.indexIds,
      })),
    );
  }

  /** The coverage rule; call with the scan's `lastLsn` before replaying. */
  static checkCoverage(checkpoint: ReplayCheckpoint, lastLsn: number): void {
    let needed = checkpoint.globalLsn;
    for (const table of checkpoint.tables.values())
      needed = Math.max(needed, table.flushedThroughLsn);
    if (lastLsn < needed)
      throw new LogCorruptionError(
        `The log ends at LSN ${String(lastLsn)} before the root's checkpoint at ${String(needed)}`,
      );
  }

  /**
   * Rows (ordinary or published) for a table the catalog lacks at this point of replay: skipped
   * when the table was dropped and the drop is covered by `globalLsn` (rows from before the drop);
   * corruption for an unknown id or rows after the drop.
   */
  #absentTable(tableId: number, global: boolean): false {
    if (tableId >= this.#nextTableId)
      throw new LogCorruptionError(`Rows for unknown table ${String(tableId)}`);
    if (global)
      throw new LogCorruptionError(`Rows for table ${String(tableId)} after it was dropped`);
    return false;
  }

  /** Per effect, in order: apply it or not. Throws `LogCorruptionError` on a rule violation. */
  next(lsn: number, effects: readonly Effect[]): boolean[] {
    if (lsn <= this.#lastLsn) throw new LogCorruptionError("Transactions out of LSN order");
    this.#lastLsn = lsn;
    const global = lsn > this.#globalLsn;
    return effects.map((effect) => {
      switch (effect.kind) {
        case "catalog":
        case "sequence":
        case "request":
          return global;
        case "createTable": {
          if (!global) return false;
          if (this.#tables.has(effect.tableId) || effect.tableId < this.#nextTableId)
            throw new LogCorruptionError(`Table ${String(effect.tableId)} is created twice`);
          this.#tables.set(effect.tableId, { flushedThroughLsn: lsn - 1, schemaVersion: 1 });
          this.#nextTableId = effect.tableId + 1;
          return true;
        }
        case "alterTable":
        case "dropTable": {
          if (!global) return false;
          const table = this.#tables.get(effect.tableId);
          if (table === undefined)
            throw new LogCorruptionError(`DDL on unknown table ${String(effect.tableId)}`);
          if (effect.kind === "dropTable") {
            this.#tables.delete(effect.tableId);
            return true;
          }
          if (effect.schemaVersion !== table.schemaVersion + 1)
            throw new LogCorruptionError(`Table ${String(effect.tableId)} skips a schema version`);
          this.#tables.set(effect.tableId, { ...table, schemaVersion: effect.schemaVersion });
          return true;
        }
        case "publishedRows": {
          const table = this.#tables.get(effect.tableId);
          // The same dropped/unknown-table rule as ordinary rows.
          if (table === undefined) return this.#absentTable(effect.tableId, global);
          if (lsn <= table.flushedThroughLsn) return false;
          if (effect.schemaVersion !== table.schemaVersion)
            throw new LogCorruptionError(
              `Published rows for table ${String(effect.tableId)} under the wrong schema`,
            );
          if (this.#rootSequence < effect.rootSequence)
            throw new LogCorruptionError("A committed transaction's staging root was rolled back");
          const staged = this.#unclaimed.get(effect.tableId)?.get(effect.transaction);
          if (staged === undefined)
            throw new LogCorruptionError(
              `Transaction ${String(effect.transaction)} is not staged for table ${String(effect.tableId)}`,
            );
          // Rows and index runs are one unit: the commit must name exactly the staged runs.
          const named = [...effect.indexIds].sort((a, b) => a - b).join();
          if ([...staged.indexIds].sort((a, b) => a - b).join() !== named)
            throw new LogCorruptionError(
              `Transaction ${String(effect.transaction)} commits index runs [${named}] but stages [${staged.indexIds.join()}]`,
            );
          this.#unclaimed.get(effect.tableId)?.delete(effect.transaction);
          return true;
        }
        case "rows": {
          const table = this.#tables.get(effect.tableId);
          if (table === undefined) return this.#absentTable(effect.tableId, global);
          if (lsn <= table.flushedThroughLsn) return false;
          if (effect.schemaVersion !== table.schemaVersion)
            throw new LogCorruptionError(
              `Rows for table ${String(effect.tableId)} were written under schema ${String(effect.schemaVersion)}, not ${String(table.schemaVersion)}`,
            );
          return true;
        }
      }
    });
  }
}
