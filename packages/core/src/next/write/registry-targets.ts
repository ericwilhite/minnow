/** Original child identity ranges for deferred actions. All ranges share the root directory;
 * current versions are a spilled table/row radix, never a Map or a file per rowset. */
import type { RowTargetRange } from "../access/statement-command.js";
import { SpillCorruptionError } from "../spill/contract.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { MaybePromise } from "../access/key-index.js";
import type { RegistryDirectory } from "./registry-directory.js";

export const REGISTRY_TARGET_BYTES = 1024;
const TARGET = 3,
  LINEAGE = 4;
export interface NativeRowIdentity {
  readonly tableId: number;
  readonly rowId: number;
}
export interface CapturedRowIdentity extends NativeRowIdentity {
  readonly originalId: number;
}
export interface RegistryTargetScope {
  readonly rootId: number;
  readonly directory: RegistryDirectory;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  checkpoint(): MaybePromise<void>;
}
function invalid(): never {
  throw new TypeError("Invalid or retired native target range");
}
function corrupt(): never {
  throw new SpillCorruptionError("Invalid native target record");
}
const viewOf = (bytes: Uint8Array) =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
function rowId(id: number): void {
  if (!Number.isSafeInteger(id) || id < 0) invalid();
}

export class RegistryTargets {
  readonly #stop = new AbortController();
  #scope: RegistryTargetScope | null;
  #record: Uint8Array | null = null;
  #next = 0;
  #pending: Promise<unknown> | null = null;
  #closed = false;
  #closing: Promise<void> | null = null;
  #failure: { reason: unknown } | null = null;
  private get scope(): RegistryTargetScope {
    return this.#scope ?? invalid();
  }
  constructor(scope: RegistryTargetScope) {
    scope.signal.throwIfAborted();
    rowId(scope.rootId);
    if (scope.rootId < 1) invalid();
    this.#scope = scope;
    if (!scope.charge.allocate(REGISTRY_TARGET_BYTES))
      throw new RangeError("Target metadata must be admitted");
    try {
      this.#record = new Uint8Array(128);
    } catch (error) {
      this.#scope = null;
      scope.charge.free(REGISTRY_TARGET_BYTES);
      throw error;
    }
  }
  #check(signal?: AbortSignal): void {
    if (this.#closed) invalid();
    this.scope.signal.throwIfAborted();
    signal?.throwIfAborted();
    if (this.#failure) throw this.#failure.reason;
  }
  #work<T>(run: () => Promise<T>): Promise<T> {
    this.#check();
    if (this.#pending) invalid();
    const task = run().catch((reason: unknown) => {
      this.#failure = { reason };
      throw reason;
    });
    const pending = task.finally(() => {
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  async #resolve(tableId: number, id: number): Promise<CapturedRowIdentity | null> {
    rowId(tableId);
    rowId(id);
    const record = this.#record;
    if (!record) invalid();
    const found = await this.scope.directory.get(LINEAGE, id, record, tableId);
    this.#check();
    if (!found) return { tableId, rowId: id, originalId: id };
    const originalId = viewOf(record).getFloat64(0, true);
    rowId(originalId);
    if (originalId !== id) {
      if (!(await this.scope.directory.get(LINEAGE, originalId, record, tableId))) corrupt();
      this.#check();
      if (viewOf(record).getFloat64(0, true) !== originalId) corrupt();
    }
    const current = viewOf(record).getFloat64(8, true);
    if (current === -1) return null;
    if (!Number.isSafeInteger(current) || current < originalId) corrupt();
    return { tableId, rowId: current, originalId };
  }
  resolve(tableId: number, id: number): Promise<CapturedRowIdentity | null> {
    return this.#work(() => this.#resolve(tableId, id));
  }
  /** Record AFTER a native accepted update/delete, with actual old/new native IDs. An old
   * alias is not a second mutation target: callers must resolve its latest version first. */
  note(tableId: number, before: number, after: number | null): Promise<void> {
    return this.#work(async () => {
      rowId(tableId);
      rowId(before);
      if (after !== null) {
        rowId(after);
        if (after <= before) invalid();
      }
      const current = await this.#resolve(tableId, before),
        record = this.#record;
      if (current?.rowId !== before || !record) invalid();
      const view = viewOf(record);
      record.fill(0);
      view.setFloat64(0, current.originalId, true);
      view.setFloat64(8, after ?? -1, true);
      await this.scope.directory.put(LINEAGE, current.originalId, record, tableId);
      this.#check();
      if (after !== null) {
        // An alias names its immutable origin; only the canonical record owns currentness.
        await this.scope.directory.put(LINEAGE, after, record, tableId);
        this.#check();
      }
    });
  }
  #allocate(): number {
    if (!Number.isSafeInteger(this.#next + 1)) throw new RangeError("Target ID space exhausted");
    return ++this.#next;
  }
  /** Fully seal the source before any action writes. The source is already root-admitted and
   * obeys this combined cancellation signal; close fences its terminal I/O/return. */
  capture(
    source: (signal: AbortSignal) => AsyncIterable<NativeRowIdentity>,
  ): Promise<RowTargetRange> {
    return this.#work(async () => {
      const from = this.#allocate(),
        signal = AbortSignal.any([this.scope.signal, this.#stop.signal]);
      let count = 0;
      for await (const row of source(signal)) {
        this.#check(signal);
        await this.scope.checkpoint();
        this.#check(signal);
        const current = await this.#resolve(row.tableId, row.rowId),
          record = this.#record;
        if (!current || !record) continue;
        const id = this.#allocate(),
          view = viewOf(record);
        record.fill(0);
        view.setFloat64(0, current.tableId, true);
        view.setFloat64(8, current.originalId, true);
        view.setUint8(16, 1);
        await this.scope.directory.put(TARGET, id, record);
        this.#check(signal);
        count++;
      }
      this.#check(signal);
      const record = this.#record;
      if (!record) invalid();
      record.fill(0);
      viewOf(record).setFloat64(0, count, true);
      await this.scope.directory.put(TARGET, from, record);
      this.#check(signal);
      return { spoolId: this.scope.rootId, from, count };
    });
  }
  async *rows(range: RowTargetRange, signal: AbortSignal): AsyncIterable<CapturedRowIdentity> {
    this.#check(signal);
    if (
      range.spoolId !== this.scope.rootId ||
      !Number.isSafeInteger(range.from) ||
      range.from < 1 ||
      !Number.isSafeInteger(range.count) ||
      range.count < 0 ||
      range.from + range.count > this.#next
    )
      invalid();
    await this.#work(async () => {
      const record = this.#record;
      if (!record) invalid();
      if (!(await this.scope.directory.get(TARGET, range.from, record))) invalid();
      this.#check(signal);
      const view = viewOf(record);
      if (view.getUint8(16) !== 0 || view.getFloat64(0, true) !== range.count) invalid();
    });
    for (let offset = 1; offset <= range.count; offset++) {
      const row = await this.#work(async () => {
        this.#check(signal);
        await this.scope.checkpoint();
        this.#check(signal);
        const record = this.#record;
        if (!record) invalid();
        if (!(await this.scope.directory.get(TARGET, range.from + offset, record))) corrupt();
        this.#check(signal);
        const view = viewOf(record),
          tableId = view.getFloat64(0, true),
          originalId = view.getFloat64(8, true);
        if (view.getUint8(16) !== 1) corrupt();
        return this.#resolve(tableId, originalId);
      });
      this.#check(signal);
      // No directory read or mutable record survives the continuation. It may note the next
      // accepted physical version without invalidating this original-identity range.
      if (row) yield row;
    }
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      this.#stop.abort(new TypeError("Native targets are closed"));
      await this.#pending?.catch(() => undefined);
      this.#record = null;
      this.#pending = null;
      this.#failure = null;
      this.scope.charge.free(REGISTRY_TARGET_BYTES);
      this.#scope = null;
    })());
  }
}
