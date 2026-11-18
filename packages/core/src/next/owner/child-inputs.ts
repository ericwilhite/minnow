/** Complete OLD inputs for one RI action. Scalar handles and immutable row packets share
 * the root directory; neither input count nor row count adds resident metadata/files. */
import type { RowImageRef, RowTargetRange } from "../access/statement-command.js";
import type { MaybePromise } from "../access/key-index.js";
import type { StatementImpl } from "../write/transaction.js";
import type { ByteCharge } from "../vector/varlen.js";
import { SpillCorruptionError } from "../spill/contract.js";
import type { RegistryDirectory } from "./registry-directory.js";
import type { RegistryTargets } from "./registry-targets.js";
import type { NativePrefixImages } from "./prefix-images.js";

export const NATIVE_CHILD_INPUT_BYTES = 1024;
const INPUT = 10,
  ROW = 11;
export interface NativeChildInputRef {
  readonly rootId: number;
  readonly id: number;
}
export interface NativeChildInputRow {
  readonly tableId: number;
  readonly originalId: number;
  readonly rowId: number;
  readonly images: RowImageRef;
}
export interface NativeChildInputScope {
  readonly rootId: number;
  readonly statement: StatementImpl;
  readonly targets: Pick<RegistryTargets, "rows">;
  readonly images: Pick<NativePrefixImages, "capture">;
  readonly directory: RegistryDirectory;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  abortRoot(reason: unknown): void;
  checkpoint(): MaybePromise<void>;
}
function invalid(): never {
  throw new TypeError("Invalid, unsealed or retired native child input");
}
function corrupt(): never {
  throw new SpillCorruptionError("Invalid native child input packet");
}
function integer(value: number, minimum = 0): number {
  if (!Number.isSafeInteger(value) || value < minimum) invalid();
  return value;
}
const viewOf = (bytes: Uint8Array) =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

export class NativeChildInputs {
  readonly #stop = new AbortController();
  #scope: NativeChildInputScope | null;
  #record: Uint8Array | null = null;
  #nextInput = 0;
  #nextRow = 0;
  #pending: Promise<unknown> | null = null;
  #closing: Promise<void> | null = null;
  #closed = false;
  #failure: { reason: unknown } | null = null;
  constructor(scope: NativeChildInputScope) {
    scope.signal.throwIfAborted();
    integer(scope.rootId, 1);
    if (!scope.charge.allocate(NATIVE_CHILD_INPUT_BYTES))
      throw new RangeError("Child input metadata must be admitted");
    this.#scope = scope;
    try {
      this.#record = new Uint8Array(128);
    } catch (error) {
      scope.charge.free(NATIVE_CHILD_INPUT_BYTES);
      this.#scope = null;
      throw error;
    }
  }
  private get scope(): NativeChildInputScope {
    return this.#scope ?? invalid();
  }
  #check(signal?: AbortSignal): void {
    if (this.#closed) invalid();
    this.scope.signal.throwIfAborted();
    signal?.throwIfAborted();
    if (this.#failure) throw this.#failure.reason;
  }
  #work<T>(signal: AbortSignal, run: (stop: AbortSignal) => Promise<T>): Promise<T> {
    this.#check(signal);
    if (this.#pending) invalid();
    const operation = new AbortController(),
      root = this.scope.signal,
      closed = this.#stop.signal,
      rootAbort = () => operation.abort(root.reason as unknown),
      callerAbort = () => {
        const reason: unknown = signal.reason;
        operation.abort(reason);
        this.scope.abortRoot(reason);
      },
      closeAbort = () => operation.abort(closed.reason as unknown);
    root.addEventListener("abort", rootAbort, { once: true });
    if (signal !== root) signal.addEventListener("abort", callerAbort, { once: true });
    closed.addEventListener("abort", closeAbort, { once: true });
    if (root.aborted) rootAbort();
    if (signal !== root && signal.aborted) callerAbort();
    if (closed.aborted) closeAbort();
    // Publish pending before source/clock callbacks can synchronously reenter close.
    const work = Promise.resolve().then(() => {
      this.#check(operation.signal);
      return run(operation.signal);
    });
    const pending = work
      .catch((reason: unknown) => {
        this.#failure = { reason };
        throw reason;
      })
      .finally(() => {
        root.removeEventListener("abort", rootAbort);
        if (signal !== root) signal.removeEventListener("abort", callerAbort);
        closed.removeEventListener("abort", closeAbort);
        if (this.#pending === pending) this.#pending = null;
      });
    this.#pending = pending;
    return pending;
  }
  /** Captures every live identity's complete OLD payload before publishing the handle.
   * No proposed NEW/default/generated/volatile expression is evaluated here. The phase
   * fence rejects any intervening mutation, including between separate point reads.
   * Source base-credit retirement/transfer remains the admitted root's responsibility. */
  seal(
    tableId: number,
    targets: RowTargetRange,
    signal: AbortSignal,
  ): Promise<NativeChildInputRef> {
    return this.#work(signal, async (stop) =>
      this.scope.statement.guard(async () => {
        const table = this.scope.statement.catalog.table(tableId) ?? invalid(),
          id = integer(this.#nextInput + 1, 1),
          from = integer(this.#nextRow + 1, 1),
          pin = this.scope.statement.readOwn();
        this.#nextInput = id;
        let count = 0;
        try {
          for await (const row of this.scope.targets.rows(targets, stop)) {
            this.#check(stop);
            pin.checkRead();
            if (row.tableId !== tableId) invalid();
            await this.scope.checkpoint();
            this.#check(stop);
            pin.checkRead();
            const images = await this.scope.images.capture(tableId, row.rowId, stop);
            this.#check(stop);
            pin.checkRead();
            if (images.spoolId !== this.scope.rootId) invalid();
            integer(images.row, 1);
            const next = integer(this.#nextRow + 1, 1),
              record = this.#record ?? invalid(),
              view = viewOf(record);
            record.fill(0);
            view.setFloat64(0, this.scope.rootId, true);
            view.setFloat64(8, id, true);
            view.setFloat64(16, count, true);
            view.setFloat64(24, tableId, true);
            view.setFloat64(32, row.originalId, true);
            view.setFloat64(40, row.rowId, true);
            view.setFloat64(48, images.row, true);
            await this.scope.directory.put(ROW, next, record);
            this.#check(stop);
            pin.checkRead();
            this.#nextRow = next;
            count++;
          }
          this.#check(stop);
          pin.checkRead();
          const record = this.#record ?? invalid(),
            view = viewOf(record);
          record.fill(0);
          view.setFloat64(0, this.scope.rootId, true);
          view.setFloat64(8, id, true);
          view.setFloat64(16, tableId, true);
          view.setFloat64(24, table.schemaVersion, true);
          view.setFloat64(32, from, true);
          view.setFloat64(40, count, true);
          view.setUint8(48, 1);
          // The header is the publication point. An errored prefix never has a handle.
          await this.scope.directory.put(INPUT, id, record);
          this.#check(stop);
          pin.checkRead();
          return { rootId: this.scope.rootId, id };
        } finally {
          pin.release();
        }
      }),
    );
  }
  async #header(ref: NativeChildInputRef, signal: AbortSignal) {
    if (ref.rootId !== this.scope.rootId) invalid();
    integer(ref.id, 1);
    if (ref.id > this.#nextInput) invalid();
    const record = this.#record ?? invalid();
    if (!(await this.scope.directory.get(INPUT, ref.id, record))) invalid();
    this.#check(signal);
    const view = viewOf(record);
    if (view.getFloat64(0, true) !== ref.rootId || view.getFloat64(8, true) !== ref.id) corrupt();
    if (view.getUint8(48) !== 1) invalid();
    const tableId = integer(view.getFloat64(16, true)),
      table = this.scope.statement.catalog.table(tableId) ?? invalid(),
      from = integer(view.getFloat64(32, true), 1),
      count = integer(view.getFloat64(40, true));
    if (view.getFloat64(24, true) !== table.schemaVersion || from + count > this.#nextRow + 1)
      corrupt();
    return { tableId, from, count };
  }
  describe(
    ref: NativeChildInputRef,
    signal: AbortSignal,
  ): Promise<{ tableId: number; count: number }> {
    return this.#work(signal, async (stop) => {
      const { tableId, count } = await this.#header(ref, stop);
      return { tableId, count };
    });
  }
  row(ref: NativeChildInputRef, index: number, signal: AbortSignal): Promise<NativeChildInputRow> {
    return this.#work(signal, async (stop) => {
      const header = await this.#header(ref, stop);
      if (integer(index) >= header.count) invalid();
      const record = this.#record ?? invalid();
      if (!(await this.scope.directory.get(ROW, header.from + index, record))) corrupt();
      this.#check(stop);
      const view = viewOf(record);
      if (
        view.getFloat64(0, true) !== ref.rootId ||
        view.getFloat64(8, true) !== ref.id ||
        view.getFloat64(16, true) !== index ||
        view.getFloat64(24, true) !== header.tableId
      )
        corrupt();
      return {
        tableId: header.tableId,
        originalId: integer(view.getFloat64(32, true)),
        rowId: integer(view.getFloat64(40, true)),
        images: { spoolId: ref.rootId, row: integer(view.getFloat64(48, true), 1) },
      };
    });
  }
  /** Only the input handle retires; accepted/queued image refs remain valid until root close. */
  retire(ref: NativeChildInputRef, signal: AbortSignal): Promise<void> {
    return this.#work(signal, async (stop) => {
      await this.#header(ref, stop);
      const record = this.#record ?? invalid();
      viewOf(record).setUint8(48, 0);
      await this.scope.directory.put(INPUT, ref.id, record);
      this.#check(stop);
    });
  }
  close(): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    this.#closing = Promise.resolve().then(async () => {
      const reason = new Error("Native child inputs closed"),
        errors: unknown[] = [];
      this.#stop.abort(reason);
      try {
        this.scope.abortRoot(reason);
      } catch (error) {
        errors.push(error);
      }
      await this.#pending?.catch(() => undefined);
      this.#record = null;
      this.#failure = null;
      this.scope.charge.free(NATIVE_CHILD_INPUT_BYTES);
      this.#scope = null;
      if (errors.length) throw new AggregateError(errors, "Native child input close failed");
    });
    return this.#closing;
  }
}
