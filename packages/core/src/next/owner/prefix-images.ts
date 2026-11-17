/** Native OLD payload capture. The source step is prepaid, one physical row is decoded,
 * then streamed to the root image file. No legacy uncharged row copies or user callbacks. */
import type { TableDef } from "../access/catalog.js";
import type { MaybePromise } from "../access/key-index.js";
import type { RowImageRef } from "../access/statement-command.js";
import {
  MEMORY_CHUNK_ROWS,
  type MemorySegment,
  type MemoryReadHint,
  type MemoryChunkStep,
} from "../access/snapshot.js";
import { selectedSegment } from "../exec/late-fetch.js";
import { TableScan, memoryScanBufferBytes } from "../exec/table-scan.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import type { ChunkReads } from "../exec/scan.js";
import type { Grant, Loan } from "../memory/governor.js";
import type { ByteCharge } from "../vector/varlen.js";
import { BATCH_ROWS, type FlatVector, type Vector, type ConstantVector } from "../vector/vector.js";
import { setInt128 } from "../vector/int128.js";
import type { PhysicalType } from "../types.js";
import { sourceSlot } from "../write/columns.js";
import type { StatementImpl } from "../write/transaction.js";
import { MAX_READ_BYTES } from "../access/space.js";
import { scratchBytes as predicateScratchBytes } from "../page/select.js";
import { MEMORY_READ_HINT_BYTES, MEMORY_READ_RESIDENT_BYTES } from "../write/memory-read.js";
import type { RegistryImages } from "./registry-images.js";

export interface NativePrefixImageScope {
  readonly rootId: number;
  readonly statement: StatementImpl;
  readonly tables: readonly number[];
  readonly images: Pick<RegistryImages, "append">;
  readonly reads: ChunkReads;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  /** Root-owned source step; no source/image borrow exists when this is called. Root
   * reclaims/transfers idle base credit after sealing the rowset, BEFORE writer growth. */
  prepareSource(table: TableDef, minimum: number, signal: AbortSignal): Promise<Grant>;
  /** Synchronous root AbortController operation, never a recursive close/rollback. */
  abortRoot(reason: unknown): void;
  checkpoint(): MaybePromise<void>;
}
/** Ordinary helper storage; payload decode is the separate reusable source step. */
export function nativePrefixImageBytes(columns: number): number {
  if (!Number.isSafeInteger(columns) || columns < 0) invalid();
  const bytes = 4096 + columns * 2048;
  if (!Number.isSafeInteger(bytes)) invalid();
  return bytes;
}
/** Fixed native decode/IO belongs to ordinary admission. Only a row's variable
 * payload may use the shared reserve after the publication-stable hint. */
const MEMORY_STEP_BYTES =
  MEMORY_READ_HINT_BYTES +
  MEMORY_READ_RESIDENT_BYTES +
  MAX_READ_BYTES +
  65536 * 8 +
  predicateScratchBytes(4096);
export function nativePrefixSourceBytes(table: TableDef): number {
  return (
    MEMORY_STEP_BYTES +
    memoryScanBufferBytes(table.columns.map((column) => column.type)) +
    6 * BATCH_ROWS * Uint16Array.BYTES_PER_ELEMENT +
    2048 * table.columns.length +
    8192
  );
}
function invalid(): never {
  throw new TypeError("Invalid or retired native prefix image source");
}
/** A true one-row memory source. A sparse deletion mask would make MemorySegmentScan
 * size/decode preceding rows in the chunk, even though its consumer needs only one. */
function pointMemory(source: MemorySegment, position: number): MemorySegment {
  const chunk = Math.floor(position / MEMORY_CHUNK_ROWS),
    row = position % MEMORY_CHUNK_ROWS;
  let nativeHint: MemoryReadHint | null = null;
  return {
    kind: "memory",
    index: 0,
    rowCount: 1,
    liveRowCount: 1,
    deletions: () => null,
    statistics: (column) => source.statistics(column),
    pages: async (column) => {
      const page = (await source.pages(column)).find(
        (entry) => position >= entry.firstRow && position < entry.firstRow + entry.rowCount,
      );
      if (!page) invalid();
      return [{ ...page, firstRow: 0, rowCount: 1 }];
    },
    readHint: async (request, context) => {
      if (request.chunk !== 0 || request.startRow !== 0 || request.maxRows < 1) invalid();
      const hint = await source.readHint({ ...request, chunk, startRow: row, maxRows: 1 }, context);
      nativeHint = hint;
      return { ...hint, request, outputs: hint.outputs.map((output) => ({ ...output, row: 0 })) };
    },
    chunk: async (column, logicalChunk, out, start = 0, step) => {
      if (logicalChunk !== 0 || start !== 0 || !step || !nativeHint) invalid();
      const mapped: MemoryChunkStep = { context: step.context, hint: nativeHint };
      const result = await source.chunk(column, chunk, out, row, mapped);
      return {
        ...result,
        firstRow: 0,
        complete: result.rowCount === 1,
        reason: result.rowCount === 1 ? null : result.reason,
        ...(result.need ? { need: { ...result.need, row: 0 } } : {}),
      };
    },
  };
}
type MutableFlat = { -readonly [P in keyof FlatVector]: FlatVector[P] };
type MutableConstant = { -readonly [P in keyof ConstantVector]: ConstantVector[P] };
interface DecimalSlot {
  readonly flat: MutableFlat;
  readonly constant: MutableConstant;
  readonly limbs: Uint32Array;
  readonly scales: Uint16Array;
  readonly specials: Uint8Array;
  readonly validity: Uint32Array;
}
function decimalSlot(): DecimalSlot {
  const limbs = new Uint32Array(4),
    scales = new Uint16Array(1),
    specials = new Uint8Array(1),
    validity = new Uint32Array(1),
    type: PhysicalType = { kind: "decimal", precision: null, scale: null };
  const flat: MutableFlat = {
    form: "flat",
    type,
    length: 1,
    validity,
    data: {
      layout: "decimal",
      scale: null,
      scales,
      specials,
      coefficients: { repr: "int128", limbs },
    },
  };
  return {
    flat,
    constant: { form: "constant", type, length: 1, value: flat },
    limbs,
    scales,
    specials,
    validity,
  };
}
export class NativePrefixImages {
  readonly #stop = new AbortController();
  #scope: NativePrefixImageScope | null;
  #pending: Promise<unknown> | null = null;
  #closing: Promise<void> | null = null;
  #scan: TableScan | null = null;
  #positions: Uint16Array | null = null;
  #columns: Vector[] = [];
  #ids: number[] = [];
  #decimals: DecimalSlot[] = [];
  #failure: { reason: unknown } | null = null;
  #closed = false;
  readonly #bytes: number;
  readonly #catalogVersion: number;
  constructor(scope: NativePrefixImageScope) {
    scope.signal.throwIfAborted();
    if (!Number.isSafeInteger(scope.rootId) || scope.rootId < 1) invalid();
    let columns = 0;
    for (const id of scope.tables)
      columns = Math.max(columns, (scope.statement.catalog.table(id) ?? invalid()).columns.length);
    this.#bytes = nativePrefixImageBytes(columns);
    this.#catalogVersion = scope.statement.catalog.version;
    if (!scope.charge.allocate(this.#bytes)) throw new RangeError("Prefix images must be admitted");
    this.#scope = scope;
    try {
      this.#positions = new Uint16Array(1);
      this.#decimals = Array.from({ length: columns }, decimalSlot);
    } catch (error) {
      this.#positions = null;
      this.#decimals = [];
      this.#scope = null;
      scope.charge.free(this.#bytes);
      throw error;
    }
  }
  private get scope(): NativePrefixImageScope {
    return this.#scope ?? invalid();
  }
  #check(signal?: AbortSignal): void {
    if (this.#closed) invalid();
    this.scope.signal.throwIfAborted();
    signal?.throwIfAborted();
    if (this.#failure) throw this.#failure.reason;
  }
  /** Only stored OLD values: the native stored coefficient domain is int128.
   * Normalize a wide decoder/missing-value view without dynamic BigInt serialization
   * scratch. Thus image append cannot ask for admission while a source loan is live. */
  async #normalize(
    batch: import("../vector/vector.js").Batch,
    row: number,
    signal: AbortSignal,
    pin: Pick<ReturnType<StatementImpl["readOwn"]>, "checkRead">,
  ) {
    const slot: { flat: FlatVector | null; index: number } = { flat: null, index: -1 };
    this.#columns.length = 0;
    for (const [i, vector] of batch.columns.entries()) {
      await this.scope.checkpoint();
      this.#check(signal);
      pin.checkRead();
      sourceSlot(vector, row, slot);
      const data = slot.flat.data;
      if (data.layout !== "decimal" || data.coefficients.repr !== "wide") {
        this.#columns.push(vector);
        continue;
      }
      const target = this.#decimals[i] ?? invalid();
      target.flat.type = target.constant.type = vector.type;
      target.constant.length = batch.length;
      target.validity[0] = slot.index < 0 ? 0 : 1;
      target.limbs.fill(0);
      target.scales[0] = 0;
      target.specials[0] = 0;
      if (slot.index >= 0) {
        setInt128(target.limbs, 0, data.coefficients.values[slot.index] ?? 0n);
        const scale = data.scale ?? data.scales?.[slot.index];
        if (scale === undefined) invalid();
        target.scales[0] = scale;
        target.specials[0] = data.specials?.[slot.index] ?? 0;
      }
      this.#columns.push(target.constant);
    }
  }
  capture(tableId: number, rowId: number, signal: AbortSignal): Promise<RowImageRef> {
    this.#check(signal);
    if (
      this.#pending ||
      this.scope.statement.catalog.version !== this.#catalogVersion ||
      !this.scope.tables.includes(tableId) ||
      !Number.isSafeInteger(rowId) ||
      rowId < 0
    )
      invalid();
    // No root-dependent composite history per captured row. All operation listeners
    // retire with its one pending slot, even when an un-aborted root lives a long time.
    const operation = new AbortController(),
      stop = operation.signal,
      rootSignal = this.scope.signal,
      closeSignal = this.#stop.signal;
    const abort = () => {
        this.#scan?.abort();
      },
      rootAbort = () => operation.abort(rootSignal.reason as unknown),
      callerAbort = () => {
        const reason: unknown = signal.reason;
        operation.abort(reason);
        this.scope.abortRoot(reason);
      },
      closeAbort = () => operation.abort(closeSignal.reason as unknown);
    stop.addEventListener("abort", abort, { once: true });
    rootSignal.addEventListener("abort", rootAbort, { once: true });
    if (signal !== rootSignal) signal.addEventListener("abort", callerAbort, { once: true });
    closeSignal.addEventListener("abort", closeAbort, { once: true });
    if (rootSignal.aborted) rootAbort();
    if (signal.aborted && signal !== rootSignal) callerAbort();
    if (closeSignal.aborted) closeAbort();
    // Install pending ownership before even a synchronous preparation callback can
    // abort/reenter close. Close then fences the whole operation, not only its reads.
    const work = Promise.resolve().then(() => {
      this.#check(stop);
      return this.scope.statement.guard(async () => {
        const table = this.scope.statement.catalog.table(tableId) ?? invalid(),
          minimum = nativePrefixSourceBytes(table),
          budget = await this.scope.prepareSource(table, minimum, stop);
        this.#check(stop);
        if (budget.loaned !== 0 || budget.held - budget.allocated < minimum) invalid();
        const pin = this.scope.statement.readOwn();
        let arena: OutputArena | null = null;
        let loan: Loan | null = null;
        try {
          const source = pin.internalTable(tableId),
            locator = source.locator(rowId),
            segment = source.segments[Math.floor(locator / 65536)],
            position = locator % 65536;
          if (
            !segment ||
            position >= segment.rowCount ||
            (await segment.deletions())?.isDeleted(position) === true
          )
            invalid();
          this.#check(stop);
          pin.checkRead();
          const positions = this.#positions ?? invalid();
          positions[0] = position;
          this.#ids.length = 0;
          for (const column of table.columns) this.#ids.push(column.id);
          if (segment.kind === "memory") {
            // B3's publication-stable hint sizes the WHOLE native read, not just output
            // buffers. Its transient sizing workspace is in minimum. No payload borrow
            // survives the hint, so obtaining the shared progress lane cannot self-wait.
            const hint = await segment.readHint(
              {
                columns: this.#ids,
                chunk: Math.floor(position / MEMORY_CHUNK_ROWS),
                startRow: position % MEMORY_CHUNK_ROWS,
                maxRows: 1,
                targetBytes: 1,
              },
              { budget, pages: pin.pages, signal: stop },
            );
            this.#check(stop);
            pin.checkRead();
            const complete = Math.max(
              minimum,
              minimum -
                MEMORY_STEP_BYTES +
                hint.residentBytes +
                hint.scratchBytes +
                hint.ioBytes +
                hint.outputBytes,
            );
            if (!Number.isSafeInteger(complete)) invalid();
            const shortfall = complete - (budget.held - budget.allocated);
            if (shortfall > 0) loan = await budget.borrow(shortfall, stop);
            this.#check(stop);
            pin.checkRead();
          }
          arena = new OutputArena(budget, 1);
          const scan = new TableScan({
            table: {
              tableId,
              schemaVersion: source.schemaVersion,
              columns: source.columns,
              segments: [
                segment.kind === "memory"
                  ? pointMemory(segment, position)
                  : selectedSegment(segment, 0, positions),
              ],
              liveRowCount: 1,
            },
            columns: this.#ids,
            filter: null,
            emitLocator: false,
            pages: pin.pages,
            reads: this.scope.reads,
            budget,
            context: { arena, params: { slots: [] }, input: [] },
            boundedMemory: true,
            rowsNeeded: () => 1,
            loans: { borrow: (bytes) => budget.borrow(bytes, stop) },
          });
          this.#scan = scan;
          const item = await scan.next();
          this.#check(stop);
          pin.checkRead();
          if (!item || (item.batch.selection?.count ?? item.batch.length) !== 1) invalid();
          try {
            const row = item.batch.selection?.rows[0] ?? 0;
            await this.#normalize(item.batch, row, stop, pin);
            pin.checkRead();
            const image = await this.scope.images.append(
              tableId,
              { batch: { ...item.batch, columns: this.#columns }, row },
              null,
            );
            this.#check(stop);
            pin.checkRead();
            if (image.spoolId !== this.scope.rootId) invalid();
            return image;
          } finally {
            item.transfer?.release();
          }
        } finally {
          this.#scan?.abort();
          this.#scan = null;
          this.#columns.length = this.#ids.length = 0;
          arena?.close();
          pin.release();
          loan?.repay();
        }
      });
    });
    const pending = work
      .catch((reason: unknown) => {
        this.#failure = { reason };
        throw reason;
      })
      .finally(() => {
        stop.removeEventListener("abort", abort);
        rootSignal.removeEventListener("abort", rootAbort);
        if (signal !== rootSignal) signal.removeEventListener("abort", callerAbort);
        closeSignal.removeEventListener("abort", closeAbort);
        if (this.#pending === pending) this.#pending = null;
      });
    this.#pending = pending;
    return pending;
  }
  close(): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    // Publish the fence before abortRoot can synchronously reenter close.
    this.#closing = Promise.resolve().then(async () => {
      const reason = new Error("Native prefix images closed"),
        errors: unknown[] = [];
      this.#stop.abort(reason);
      try {
        this.scope.abortRoot(reason);
      } catch (error) {
        errors.push(error);
      }
      await this.#pending?.catch(() => undefined);
      this.#positions = null;
      this.#columns.length = this.#ids.length = this.#decimals.length = 0;
      this.#failure = null;
      this.scope.charge.free(this.#bytes);
      this.#scope = null;
      if (errors.length) throw new AggregateError(errors, "Native prefix image close failed");
    });
    return this.#closing;
  }
}
