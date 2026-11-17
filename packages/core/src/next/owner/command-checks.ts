/** Native, non-ending command checks over immutable accepted images and bounded index cursors.
 * Root funds the shared cursor step; image leases end before its progress callback. */
import type { CatalogView, ColumnDef, IndexDef, TableDef } from "../access/catalog.js";
import type { IndexCursor, IndexRangePlan } from "../access/key-index.js";
import {
  ForeignKeyViolationError,
  UniqueViolationError,
  RestrictViolationError,
} from "../access/modify.js";
import {
  ROW_IMAGE_UNREAD,
  type NativeCommandBoundary,
  type RowTargetRange,
} from "../access/statement-command.js";
import { encodeKeyBatch, KeyBatch, type KeyColumn } from "../keys/key-encoding.js";
import { MinimumBudget } from "../memory/working-set.js";
import type { Grant } from "../memory/governor.js";
import type { ByteCharge } from "../vector/varlen.js";
import { BATCH_ROWS, type Vector } from "../vector/vector.js";
import type { RegistryTargets } from "./registry-targets.js";
import type { AcceptedCommandRow } from "./registry-commands.js";
import { isNullAt } from "../write/columns.js";
import { indexLimitViolation, indexMaximumKeyBytes } from "../write/index-limits.js";
import type { StatementImpl } from "../write/transaction.js";
import type { ChunkReads } from "../exec/scan.js";
import { TableScan, memoryScanBufferBytes } from "../exec/table-scan.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { MEMORY_READ_HINT_BYTES, MEMORY_READ_RESIDENT_BYTES } from "../write/memory-read.js";
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
  /** Shared root source step for informational references without a support index. */
  readonly scan?: {
    readonly reads: ChunkReads;
    prepare(table: TableDef, minimum: number, signal: AbortSignal): Promise<Grant>;
  };
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
  #previous: Uint8Array | null = null;
  #one: Uint16Array | null = null;
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
      this.#previous = new Uint8Array(sizes.capacity);
      this.#one = new Uint16Array(1);
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
   * Enforced references require a valid leading support index. Informational references
   * without one use the root-funded projected scan; unrelated payloads are not decoded. */
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
      if (!index) invalid();
      if (!support && !fk.enforced) {
        const key = await this.#key(parent, index, index.columnIds, stop, "old");
        return this.#captureScan(child, fk.columnIds, key, targets, stop);
      }
      if (
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

  /** Only FK columns are decoded; unrelated child payloads are never materialized.
   * This source runs before any RI writer, and yields into the fixed target directory,
   * not a callback/body/admission consumer. All source loans/pins end before return. */
  async #captureScan(
    child: TableDef,
    ids: readonly number[],
    key: Uint8Array | null,
    targets: RegistryTargets,
    signal: AbortSignal,
  ): Promise<RowTargetRange> {
    const scope = this.scope,
      source = scope.scan ?? invalid(),
      previous = this.#previous ?? invalid(),
      one = this.#one ?? invalid(),
      keys = this.#keys ?? invalid();
    if (key && key.length > previous.length) invalid();
    if (key) previous.set(key);
    const length = key?.length ?? 0;
    return targets.capture(async function* (stop) {
      if (!key) return;
      signal.throwIfAborted();
      stop.throwIfAborted();
      const types = ids.map(
          (id) => child.columns.find((column) => column.id === id)?.type ?? invalid(),
        ),
        minimum =
          MEMORY_READ_HINT_BYTES +
          MEMORY_READ_RESIDENT_BYTES +
          memoryScanBufferBytes(types) +
          131072 +
          32768 * ids.length,
        budget = await source.prepare(child, minimum, signal);
      signal.throwIfAborted();
      stop.throwIfAborted();
      if (budget.loaned !== 0 || budget.held - budget.allocated < minimum) invalid();
      const pin = scope.statement.readOwn();
      let scan: TableScan | null = null,
        arena: OutputArena | null = null;
      const abort = () => scan?.abort();
      signal.addEventListener("abort", abort, { once: true });
      stop.addEventListener("abort", abort, { once: true });
      try {
        const table = pin.internalTable(child.id);
        arena = new OutputArena(budget, BATCH_ROWS);
        scan = new TableScan({
          table,
          columns: ids,
          filter: null,
          emitLocator: true,
          pages: pin.pages,
          reads: source.reads,
          budget,
          context: { arena, params: { slots: [] }, input: types },
          boundedMemory: true,
          loans: { borrow: (bytes) => budget.borrow(bytes, signal) },
        });
        for (;;) {
          const item = await scan.next();
          signal.throwIfAborted();
          stop.throwIfAborted();
          pin.checkRead();
          if (!item) break;
          try {
            const batch = item.batch,
              locator = batch.columns[ids.length];
            if (locator?.form !== "flat" || locator.data.layout !== "f64") invalid();
            const parts: KeyColumn[] = [];
            for (let c = 0; c < ids.length; c++)
              parts.push({
                vector: batch.columns[c] ?? invalid(),
                direction: "asc",
                nulls: "first",
              });
            for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
              const row = batch.selection?.rows[k] ?? k;
              if (parts.some((part) => isNullAt(part.vector, row))) continue;
              one[0] = row;
              const encoded = encodeKeyBatch(
                "equality",
                parts,
                { length: batch.length, selection: { rows: one, count: 1 } },
                keys,
              );
              if (!encoded.done || encoded.encoded !== 1) invalid();
              const candidate = keys.key(0);
              if (candidate.length !== length) continue;
              let equal = true;
              for (let i = 0; i < length; i++)
                if (candidate[i] !== previous[i]) {
                  equal = false;
                  break;
                }
              if (equal) {
                yield {
                  tableId: child.id,
                  rowId: table.rowId(locator.data.values[row] ?? invalid()),
                };
                signal.throwIfAborted();
                stop.throwIfAborted();
                pin.checkRead();
              }
            }
          } finally {
            item.transfer?.release();
          }
        }
      } finally {
        signal.removeEventListener("abort", abort);
        stop.removeEventListener("abort", abort);
        scan?.abort();
        arena?.close();
        pin.release();
      }
    });
  }

  /** Unchanged and NULL OLD keys have no RI action. Save equality bytes in prepaid
   * scratch before encoding NEW; the shared KeyBatch is overwritten on each encode. */
  parentKeyChanged(
    row: Pick<AcceptedCommandRow, "tableId" | "images">,
    indexId: number,
    signal: AbortSignal,
  ): Promise<boolean> {
    return this.#work(signal, async (stop) => {
      const table = tableOf(this.scope.statement.catalog, row.tableId),
        index = table.indexes.find((index) => index.id === indexId) ?? invalid(),
        old = await this.#key(row, index, index.columnIds, stop, "old"),
        previous = this.#previous ?? invalid();
      if (!old) return false;
      if (old.length > previous.length) invalid();
      previous.set(old);
      const length = old.length,
        next = await this.#key(row, index, index.columnIds, stop, "new");
      if (next?.length !== length) return true;
      for (let i = 0; i < length; i++) if (previous[i] !== next[i]) return true;
      return false;
    });
  }
  /** A NULL NEW reference has no child check. Unlike parent actions, NULL OLD to
   * non-NULL NEW is a change and must not be silently skipped. */
  referenceNeedsCheck(
    row: AcceptedCommandRow,
    fkId: number,
    signal: AbortSignal,
  ): Promise<boolean> {
    return this.#work(signal, async (stop) => {
      if (row.event === "delete") return false;
      const table = tableOf(this.scope.statement.catalog, row.tableId),
        fk = table.foreignKeys.find((fk) => fk.id === fkId) ?? invalid(),
        parent = tableOf(this.scope.statement.catalog, fk.parentTableId),
        index = parent.indexes.find((index) => index.id === fk.parentIndexId) ?? invalid(),
        next = await this.#key(row, index, fk.columnIds, stop, "new"),
        previous = this.#previous ?? invalid();
      if (!next) return false;
      if (row.event === "insert") return true;
      if (next.length > previous.length) invalid();
      previous.set(next);
      const length = next.length,
        old = await this.#key(row, index, fk.columnIds, stop, "old");
      if (old?.length !== length) return true;
      for (let i = 0; i < length; i++) if (previous[i] !== old[i]) return true;
      return false;
    });
  }
  /** NO ACTION checks the final prefix: an equivalent replacement parent can satisfy
   * the OLD key. RESTRICT must not use this exemption. No image survives range funding. */
  oldParentKeyExists(
    row: Pick<AcceptedCommandRow, "tableId" | "images">,
    indexId: number,
    signal: AbortSignal,
  ): Promise<boolean> {
    return this.#work(signal, async (stop) => {
      const table = tableOf(this.scope.statement.catalog, row.tableId),
        index = table.indexes.find((index) => index.id === indexId) ?? invalid(),
        key = await this.#key(row, index, index.columnIds, stop, "old");
      return key !== null && (await this.#matches(table.id, index, key, 1, stop)) !== 0;
    });
  }
  /** Ordered parent constraint action. A complete fresh child capture established
   * referenced; no callback or mutation runs between that capture and this check. */
  verifyParentRestriction(
    row: Pick<AcceptedCommandRow, "tableId" | "images">,
    foreignKeyId: number,
    referenced: boolean,
    restrict: boolean,
    signal: AbortSignal,
  ): Promise<void> {
    return this.#work(signal, async (stop) => {
      if (!referenced) return;
      const table = tableOf(this.scope.statement.catalog, row.tableId);
      let indexId: number | null = null;
      for (const child of this.scope.statement.catalog.tables)
        for (const fk of child.foreignKeys)
          if (fk.id === foreignKeyId && fk.parentTableId === table.id && fk.enforced)
            indexId = fk.parentIndexId;
      const index = table.indexes.find((candidate) => candidate.id === indexId) ?? invalid(),
        key = await this.#key(row, index, index.columnIds, stop, "old");
      if (!key) return;
      if (!restrict && (await this.#matches(table.id, index, key, 1, stop)) !== 0) return;
      if (restrict) throw new RestrictViolationError(table.id, foreignKeyId, key.slice());
      throw new ForeignKeyViolationError(table.id, foreignKeyId, "referenced", key.slice());
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
      this.#previous = null;
      this.#one = null;
      this.#columns.length = this.#vectors.length = this.#parts.length = 0;
      const account = this.#account ?? invalid();
      account.free(this.#metadata);
      this.#account = null;
      this.#scope = null;
    })());
  }
}
