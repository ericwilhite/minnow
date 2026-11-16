/** Native, non-ending command checks over immutable accepted images and bounded index cursors.
 * Root funds the shared cursor step; image leases end before its progress callback. */
import type { CatalogView, ColumnDef, IndexDef, TableDef } from "../access/catalog.js";
import type { IndexCursor, IndexRangePlan } from "../access/key-index.js";
import { ForeignKeyViolationError, UniqueViolationError } from "../access/modify.js";
import {
  ROW_IMAGE_UNREAD,
  type NativeCommandBoundary,
  type RowTargetRange,
} from "../access/statement-command.js";
import { encodeKeyBatch, KeyBatch, type KeyColumn } from "../keys/key-encoding.js";
import { MinimumBudget } from "../memory/working-set.js";
import type { Grant } from "../memory/governor.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { Vector } from "../vector/vector.js";
import type { RegistryTargets } from "./registry-targets.js";
import type { AcceptedCommandRow } from "./registry-commands.js";
import { isNullAt } from "../write/columns.js";
import { indexLimitViolation, indexMaximumKeyBytes } from "../write/index-limits.js";
import type { StatementImpl } from "../write/transaction.js";
import { ViewIndex } from "../write/view.js";

function invalid(): never {
  throw new TypeError("Invalid native command constraint scope");
}
function tableOf(catalog: CatalogView, id: number): TableDef {
  return catalog.table(id) ?? invalid();
}
function shape(catalog: CatalogView, tables: readonly number[]) {
  let columns = 0,
    keyBytes = 4096;
  for (const id of tables) {
    const table = tableOf(catalog, id);
    columns = Math.max(columns, table.columns.length);
    for (const index of table.indexes)
      keyBytes = Math.max(keyBytes, 2 * indexMaximumKeyBytes(index, table) + 64);
  }
  let capacity = 4096;
  while (capacity < keyBytes) capacity *= 2;
  return { columns, capacity, metadata: 2048 + 2 * capacity + 256 * columns + 8 * tables.length };
}
export function commandCheckMinimumBytes(catalog: CatalogView, tables: readonly number[]): number {
  const sizes = shape(catalog, tables);
  return sizes.metadata + sizes.capacity + 260;
}
export interface NativeCommandCheckScope {
  readonly statement: StatementImpl;
  readonly tables: readonly number[];
  readonly images: Pick<NativeCommandBoundary, "readRowBindings">;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  /** Root-owned shared range step. No image borrow exists here. No nested self-admission;
   * root obtains/retains its prepaid cursor grant and releases idle producer credit as needed. */
  prepareRange(plan: IndexRangePlan, signal: AbortSignal): Promise<Grant>;
}
export class NativeCommandChecks {
  readonly #stop = new AbortController();
  #scope: NativeCommandCheckScope | null;
  #keys: KeyBatch | null = null;
  #account: MinimumBudget | null = null;
  #columns: ColumnDef[] = [];
  #vectors: Vector[] = [];
  #parts: KeyColumn[] = [];
  #out: Float64Array | null = null;
  #pending: Promise<unknown> | null = null;
  #closing: Promise<void> | null = null;
  #closed = false;
  readonly #metadata: number;
  constructor(scope: NativeCommandCheckScope) {
    scope.signal.throwIfAborted();
    const sizes = shape(scope.statement.catalog, scope.tables);
    this.#metadata = sizes.metadata;
    this.#scope = scope;
    const account = new MinimumBudget(
      scope.charge,
      commandCheckMinimumBytes(scope.statement.catalog, scope.tables),
    );
    if (!account.allocate(this.#metadata)) throw new RangeError("Checks must be admitted");
    this.#account = account;
    try {
      this.#keys = new KeyBatch(account, sizes.capacity, sizes.capacity);
      this.#out = new Float64Array(2);
    } catch (error) {
      this.#keys?.close();
      this.#keys = null;
      this.#scope = null;
      // A dependency constructor may throw after charging a provisional buffer.
      // No such object escaped; return its entire private account after unwinding it.
      account.free(account.allocated);
      this.#account = null;
      throw error;
    }
  }
  private get scope(): NativeCommandCheckScope {
    return this.#scope ?? invalid();
  }
  #check(signal: AbortSignal): void {
    signal.throwIfAborted();
    this.scope.signal.throwIfAborted();
    if (this.#closed) invalid();
  }
  #work<T>(signal: AbortSignal, run: (signal: AbortSignal) => Promise<T>): Promise<T> {
    this.#check(signal);
    if (this.#pending) invalid();
    const combined = AbortSignal.any([signal, this.scope.signal, this.#stop.signal]);
    const work = Promise.resolve(this.scope.statement.guard(() => run(combined)));
    const pending = work.finally(() => {
      this.#columns.length = this.#vectors.length = this.#parts.length = 0;
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  async #live(row: AcceptedCommandRow, signal: AbortSignal): Promise<boolean> {
    this.#check(signal);
    if (!this.scope.tables.includes(row.tableId)) invalid();
    if (row.newRowId === null) return false;
    const pin = this.scope.statement.readOwn();
    try {
      const table = pin.internalTable(row.tableId),
        locator = table.locator(row.newRowId),
        segment = table.segments[Math.floor(locator / 65536)],
        position = locator % 65536;
      if (!segment || position >= segment.rowCount) invalid();
      const deleted = await segment.deletions();
      this.#check(signal);
      return deleted?.isDeleted(position) !== true;
    } finally {
      pin.release();
    }
  }
  /** Encode only requested columns, then release the projected image before cursor funding.
   * Equality keys are copied into one prepaid KeyBatch, never a key per accepted row. */
  async #key(
    row: Pick<AcceptedCommandRow, "tableId" | "images">,
    index: IndexDef,
    ids: readonly number[],
    signal: AbortSignal,
    side: "old" | "new" = "new",
  ): Promise<Uint8Array | null> {
    const keys = this.#keys ?? invalid(),
      table = tableOf(this.scope.statement.catalog, row.tableId),
      image = await this.scope.images.readRowBindings(
        row.images,
        { old: side === "old" ? ids : [], new: side === "new" ? ids : [] },
        signal,
      );
    try {
      this.#check(signal);
      const values = image[side];
      if (image.tableId !== table.id || values?.length !== table.columns.length) invalid();
      for (const id of ids) {
        const position = table.columns.findIndex((column) => column.id === id),
          column = table.columns[position],
          vector = values[position];
        if (!column || !vector || vector === ROW_IMAGE_UNREAD || vector.length !== 1) invalid();
        if (isNullAt(vector, 0)) return null;
        this.#columns.push(column);
        this.#vectors.push(vector);
        this.#parts.push({ vector, direction: "asc", nulls: "first" });
      }
      const projected = { ...index, tableId: table.id, columnIds: ids };
      const failure = indexLimitViolation(
        projected,
        { columns: this.#columns },
        this.#vectors,
        null,
        0,
        1,
      );
      if (failure) throw failure;
      const encoded = encodeKeyBatch("equality", this.#parts, { length: 1, selection: null }, keys);
      if (!encoded.done || encoded.encoded !== 1) invalid();
      return keys.key(0);
    } finally {
      this.#columns.length = this.#vectors.length = this.#parts.length = 0;
      image.release();
    }
  }
  async #matches(
    tableId: number,
    index: IndexDef,
    key: Uint8Array,
    maximum: number,
    signal: AbortSignal,
  ): Promise<number> {
    this.#check(signal);
    if (!this.scope.tables.includes(tableId)) invalid();
    const view = this.scope.statement.readOwn();
    let cursor: IndexCursor | null = null,
      pending: Promise<number> | null = null;
    try {
      const reader = new ViewIndex({ ...index, unique: false }, view.internalTable(tableId)),
        plan = reader.planProbe(key),
        budget = await this.scope.prepareRange(plan, signal);
      this.#check(signal);
      if (budget.loaned !== 0 || budget.held - budget.allocated < plan.minimumBytes) invalid();
      cursor = plan.open({ budget, signal });
      const out = (this.#out ?? invalid()).subarray(0, maximum);
      let count = 0;
      while (count < maximum) {
        const next = cursor.next(out.subarray(0, maximum - count));
        pending = next instanceof Promise ? next : null;
        const part = await next;
        this.#check(signal);
        if (part === 0) break;
        count += part;
      }
      return count;
    } finally {
      cursor?.close();
      try {
        await pending?.catch(() => undefined);
      } finally {
        view.release();
      }
    }
  }

  /** Seal one OLD parent key's complete child identity set through the native bounded
   * support-index cursor. Caller captures ALL follow inputs before any RI write;
   * DELETE/explicit actions call this at their ordered RI event against a fresh prefix.
   * No payload/image/producer lease survives cursor preparation or the returned range.
   * The root must preflight a valid leading support index for this implementation; the
   * separately guarded informational/invalid-index scan route is not supplied here. */
  captureChildren(
    parent: Pick<AcceptedCommandRow, "tableId" | "images">,
    childId: number,
    foreignKeyId: number,
    targets: RegistryTargets,
    signal: AbortSignal,
  ): Promise<RowTargetRange> {
    return this.#work(signal, async (stop) => {
      const child = tableOf(this.scope.statement.catalog, childId),
        fk = child.foreignKeys.find((candidate) => candidate.id === foreignKeyId),
        def = tableOf(this.scope.statement.catalog, parent.tableId);
      if (
        !this.scope.tables.includes(childId) ||
        !this.scope.tables.includes(parent.tableId) ||
        fk?.parentTableId !== parent.tableId
      )
        invalid();
      const index = def.indexes.find((candidate) => candidate.id === fk.parentIndexId),
        support = child.indexes.find((candidate) => candidate.id === fk.supportIndexId);
      if (
        !index ||
        !support ||
        support.invalid === true ||
        !fk.columnIds.every((id, position) => support.columnIds[position] === id)
      )
        invalid();
      const key = await this.#key(parent, index, index.columnIds, stop, "old"),
        scope = this.scope,
        out = this.#out ?? invalid();
      return targets.capture(async function* (captureSignal) {
        const combined = AbortSignal.any([stop, captureSignal]);
        combined.throwIfAborted();
        if (!key) return;
        const pin = scope.statement.readOwn();
        let cursor: IndexCursor | null = null,
          pending: Promise<number> | null = null;
        try {
          const table = pin.internalTable(childId),
            reader = new ViewIndex({ ...support, unique: false }, table),
            plan = reader.planProbe(key),
            budget = await scope.prepareRange(plan, combined);
          combined.throwIfAborted();
          pin.checkRead();
          if (budget.loaned !== 0 || budget.held - budget.allocated < plan.minimumBytes) invalid();
          cursor = plan.open({ budget, signal: combined });
          for (;;) {
            const next = cursor.next(out);
            pending = next instanceof Promise ? next : null;
            const count = await next;
            combined.throwIfAborted();
            pin.checkRead();
            if (count === 0) break;
            for (let i = 0; i < count; i++) {
              combined.throwIfAborted();
              pin.checkRead();
              yield { tableId: childId, rowId: table.rowId(out[i] ?? invalid()) };
              // A continuation may have yielded/cancelled on the LAST result too.
              combined.throwIfAborted();
              pin.checkRead();
            }
          }
        } finally {
          cursor?.close();
          try {
            await pending?.catch(() => undefined);
          } finally {
            pin.release();
          }
        }
      });
    });
  }

  verifyUnique(row: AcceptedCommandRow, signal: AbortSignal): Promise<void> {
    return this.#work(signal, async (stop) => {
      if (!(await this.#live(row, stop))) return;
      const table = tableOf(this.scope.statement.catalog, row.tableId);
      for (const index of table.indexes) {
        if (!index.unique || index.invalid === true) continue;
        const key = await this.#key(row, index, index.columnIds, stop);
        if (key && (await this.#matches(table.id, index, key, 2, stop)) > 1)
          throw new UniqueViolationError(table.id, index.id, key.slice());
      }
    });
  }
  verifyReference(row: AcceptedCommandRow, fkId: number, signal: AbortSignal): Promise<void> {
    return this.#work(signal, async (stop) => {
      const table = tableOf(this.scope.statement.catalog, row.tableId),
        fk = table.foreignKeys.find((fk) => fk.id === fkId);
      if (!fk) invalid();
      if (!fk.enforced || !(await this.#live(row, stop))) return;
      const parent = tableOf(this.scope.statement.catalog, fk.parentTableId),
        index = parent.indexes.find((index) => index.id === fk.parentIndexId) ?? invalid(),
        key = await this.#key(row, index, fk.columnIds, stop);
      if (key && (await this.#matches(parent.id, index, key, 1, stop)) === 0)
        throw new ForeignKeyViolationError(table.id, fk.id, "missingParent", key.slice());
    });
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      this.#stop.abort(new TypeError("Native command checks closed"));
      await this.#pending?.catch(() => undefined);
      this.#keys?.close();
      this.#keys = null;
      this.#out = null;
      this.#columns.length = this.#vectors.length = this.#parts.length = 0;
      const account = this.#account ?? invalid();
      account.free(this.#metadata);
      this.#account = null;
      this.#scope = null;
    })());
  }
}
