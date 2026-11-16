/** Native primary continuation. Root owns BEFORE/rules/image capture and RI sequencing.
 * This private adapter consumes pre-admitted progress, never a producer loan. */
import type { CommandToken, WriteEvent } from "../access/statement-command.js";
import type { TableDef } from "../access/catalog.js";
import type { Batch } from "../vector/vector.js";
import type { ByteCharge } from "../vector/varlen.js";
import { layoutOf } from "../types.js";
import type { PreparedWriteCredit } from "./credits.js";
import type { StatementImpl, TableWriterImpl } from "./transaction.js";

export const NATIVE_PRIMARY_BYTES = 4096;
export interface NativePrimaryScope {
  readonly statement: StatementImpl;
  readonly tables: readonly number[];
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  /** Root registry validates exact live command/frame origin synchronously. */
  commandBudget(command: CommandToken): number;
  /** Root controller checkpoint, not a second native end or commit. */
  finishRoot(signal: AbortSignal): Promise<void>;
  /** External cancellation fence for controller/body work, before native rollback. */
  closeRoot(): Promise<void>;
}
export interface NativePreparedRow {
  readonly command: CommandToken;
  readonly tableId: number;
  readonly event: WriteEvent;
  readonly oldRowId: number | null;
}
interface Prepared {
  readonly token: NativePreparedRow;
  readonly table: TableDef;
  readonly writer: TableWriterImpl;
  /** Root-owned prepaid scalar metadata, immutable until apply/close. No decoded payload. */
  readonly widths: readonly number[];
  readonly credit: PreparedWriteCredit;
}
function invalid(message: string): never {
  throw new TypeError(`Invalid native primary ${message}`);
}
/** Temporary staging restriction, checked for ALL reachable tables before any root writes.
 * Wider indexed domains/auto-increment need their separate key/identity preflight phase. */
function supported(table: TableDef): void {
  if (table.columns.some((column) => column.autoIncrement === true))
    invalid("auto-increment preflight is unavailable");
  for (const index of table.indexes)
    if (
      index.columnIds.length > 16 ||
      index.columnIds.some((id) => {
        const type = table.columns.find((column) => column.id === id)?.type;
        return !type || ["varlen", "decimal"].includes(layoutOf(type));
      })
    )
      invalid("indexed domain preflight is unavailable");
}
export class NativePrimaryAdapter {
  readonly #stop = new AbortController();
  #scope: NativePrimaryScope | null;
  #signal: AbortSignal | null;
  #prepared: Prepared | null = null;
  #pending: Promise<unknown> | null = null;
  #finishing: Promise<void> | null = null;
  #closed = false;
  #closing: Promise<void> | null = null;
  constructor(scope: NativePrimaryScope) {
    scope.signal.throwIfAborted();
    for (const id of scope.tables) supported(scope.statement.catalog.table(id) ?? invalid("table"));
    if (!scope.charge.allocate(NATIVE_PRIMARY_BYTES))
      throw new RangeError("Native primary must be admitted");
    this.#scope = scope;
    try {
      this.#signal = AbortSignal.any([scope.signal, this.#stop.signal]);
    } catch (error) {
      this.#scope = null;
      scope.charge.free(NATIVE_PRIMARY_BYTES);
      throw error;
    }
  }
  private get scope(): NativePrimaryScope {
    return this.#scope ?? invalid("retired scope");
  }
  #check(): void {
    if (this.#closed) invalid("closed scope");
    this.#signal?.throwIfAborted();
    this.scope.statement.checkCommandPrimary(this);
  }
  #work<T>(run: () => Promise<T>): Promise<T> {
    this.#check();
    if (this.#pending) invalid("concurrent operation");
    const work = Promise.resolve(this.scope.statement.guard(run));
    const pending = work.finally(() => {
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  /** Called with no image/producer lease. Widths are byte counts for each NEW varlen cell,
   * zero for other physical layouts; DELETE supplies zeroes. Root retains this paid metadata. */
  prepare(
    command: CommandToken,
    tableId: number,
    event: WriteEvent,
    oldRowId: number | null,
    widths: readonly number[],
  ): Promise<NativePreparedRow> {
    return this.#work(async () => {
      if (this.#prepared || !this.scope.tables.includes(tableId)) invalid("prepared slot");
      this.scope.commandBudget(command);
      const table = this.scope.statement.catalog.table(tableId) ?? invalid("table");
      if (
        !(["insert", "update", "delete"] as const).includes(event) ||
        (event === "insert") !== (oldRowId === null) ||
        (oldRowId !== null && (!Number.isSafeInteger(oldRowId) || oldRowId < 0)) ||
        widths.length !== table.columns.length ||
        widths.some(
          (n, c) =>
            !Number.isSafeInteger(n) ||
            n < 0 ||
            (n !== 0 &&
              (event === "delete" ||
                layoutOf(table.columns[c]?.type ?? invalid("column")) !== "varlen")),
        )
      )
        invalid("row shape");
      const writer = this.scope.statement.table(tableId),
        sizing = writer.commandPrimarySizing(this, event, widths),
        reserve = this.scope.statement.tx.hooks.reserveCommandRows?.bind(
          this.scope.statement.tx.hooks,
        );
      if (!reserve) invalid("writer reservation capability");
      const credit = await reserve(sizing.bytes, sizing.growth, this.#signal ?? invalid("signal"));
      try {
        this.#check();
        this.scope.commandBudget(command);
        const token = Object.freeze({ command, tableId, event, oldRowId });
        this.#prepared = { token, table, writer, widths, credit };
        return token;
      } catch (error) {
        credit.release();
        throw error;
      }
    });
  }
  apply(
    token: NativePreparedRow,
    old: Batch | null,
    next: Batch | null,
    changed: readonly number[] | null = null,
  ): Promise<number | null> {
    return this.#work(async () => {
      const prepared = this.#prepared;
      if (
        prepared?.token !== token ||
        this.scope.statement.catalog.table(token.tableId) !== prepared.table
      )
        invalid("foreign or stale row");
      try {
        prepared.credit.check();
        this.scope.commandBudget(token.command);
        return await prepared.writer.commandPrimary(
          this,
          token.event,
          token.oldRowId,
          old,
          next,
          prepared.widths,
          changed,
          this.#signal ?? invalid("signal"),
          () => {
            this.scope.commandBudget(token.command);
          },
          token.command.id,
        );
      } finally {
        this.#prepared = null;
        prepared.credit.release();
      }
    });
  }
  finishRoot(signal?: AbortSignal): Promise<void> {
    this.#check();
    if (this.#pending || this.#prepared || this.#finishing) invalid("unfinished row at end");
    const combined = signal
      ? AbortSignal.any([signal, this.#signal ?? invalid("signal")])
      : (this.#signal ?? invalid("signal"));
    const pending = this.scope
      .finishRoot(combined)
      .then(() => {
        this.#check();
        combined.throwIfAborted();
      })
      .finally(() => {
        if (this.#finishing === pending) this.#finishing = null;
      });
    this.#finishing = pending;
    return pending;
  }
  checkRollbackReady(): void {
    if (!this.#closed || this.#scope !== null || this.#pending || this.#prepared || this.#finishing)
      invalid("rollback before command fence");
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      this.#stop.abort(new Error("Native primary closed"));
      const scope = this.scope;
      const errors: unknown[] = [];
      try {
        await scope.closeRoot();
      } catch (error) {
        errors.push(error);
      }
      await this.#pending?.catch(() => undefined);
      await this.#finishing?.catch(() => undefined);
      this.#finishing = null;
      this.#prepared?.credit.release();
      this.#prepared = null;
      this.#pending = null;
      this.#signal = null;
      this.#scope = null;
      scope.charge.free(NATIVE_PRIMARY_BYTES);
      if (errors.length) throw new AggregateError(errors, "Native command close failed");
    })());
  }
}
