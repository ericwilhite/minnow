/** A resident full numeric sort: payload once, typed words once, row-ID LSD, one gather.
 * Refusal/reclamation replays the payload into the existing external sorter before emitting.
 */
import type { PhysicalType } from "../types.js";
import { layoutOf } from "../types.js";
import {
  BATCH_ROWS,
  BATCH_TARGET_BYTES,
  type Batch,
  type FlatVector,
  type DictionaryVector,
} from "../vector/vector.js";
import { ColumnStore, type ChunkTransfer } from "./column-store.js";
import {
  isPromise,
  YIELD_INTERVAL_MS,
  type Operator,
  type PushResult,
  type Transfer,
} from "./pipeline.js";
import { RadixScratch, isByteCollation, mergeSortJob } from "./sort-keys.js";
import { lsdSortIds } from "./sort-lsd.js";
import { Slice, runSliced, type SlicedJob } from "./slices.js";
import { frameTransferList } from "./frame-buffers.js";
import { Charge } from "./spill-pool.js";
import {
  encodeNumericColumns,
  gatherBytes,
  gatherColumn,
  gatherSource,
  moveGatherColumn,
  retireGatherChunk,
  numericWordCount,
  varlenGatherBytes,
  type GatherColumn,
} from "./sort-columnar-data.js";
import type { SortContext, SortProfile, SortSpec } from "./sort.js";
import { SortKeyCompression, type CompressionStats } from "./sort-compress.js";
import { SortDictionary } from "./sort-dictionary.js";
import type { Loan } from "../memory/governor.js";

interface GeneralSort extends Operator {
  readonly profile: SortProfile;
  readonly closed: Promise<void>;
  reclaim(): void | Promise<void>;
  discard(): void;
}
// At most sixteen active chunk handoffs: closure/Map entries, output descriptors and views.
const META_BYTES = 16 * 1024;
const MAX_INPUT_BYTES = 256 * 1024;
const SMALL_SORT = 65_536;

function payloadType(type: PhysicalType): boolean {
  return numericWordCount(type) > 0 || type.kind === "text" || type.kind === "bytes";
}
export function columnarNumericSort(spec: SortSpec): boolean {
  return (
    spec.limit === null &&
    spec.offset === 0 &&
    !spec.withTies &&
    spec.keys.length > 0 &&
    spec.keys.length <= 4 &&
    spec.types.length <= 16 &&
    spec.types.every(payloadType) &&
    spec.keys.every((key) => {
      const type = spec.types[key.column];
      return type !== undefined && numericWordCount(type) > 0 && isByteCollation(key.collation);
    })
  );
}

export class ColumnarSortOperator implements Operator {
  readonly #charge: Charge;
  #store: ColumnStore;
  #dictionaries: Array<SortDictionary | null> = [];
  #dictionaryInitialized = false;
  readonly #slice: Slice;
  readonly #abort = new AbortController();
  readonly #onAbort: () => void;
  readonly closed: Promise<void>;
  readonly #resolveClosed: () => void;
  readonly #rejectClosed: (reason: unknown) => void;
  readonly #loans = new Set<Loan>();
  readonly #inflight = new Set<Promise<void>>();
  #deferredFree = 0;
  #fallback: GeneralSort | null = null;
  #working: Promise<void> | null = null;
  #pending: Promise<void> | null = null;
  #reclaiming: Promise<void> | null = null;
  #finishing = false;
  #done = false;
  #reason: unknown;
  #reserved: number;
  #scratchBytes = 0;
  #compression: SortKeyCompression | null = null;
  #compressionStats: CompressionStats | null = null;
  readonly #radixProfile = { passes: 0 };
  #words: Uint32Array[] = [];
  #ids: Int32Array | null = null;
  #temporary: Int32Array | null = null;
  #radix: RadixScratch | null = null;
  #floating: Float64Array | null = null;
  #floatingWords: Uint32Array | null = null;
  #columns: GatherColumn[] = [];
  #rowsOut = 0;
  #columnarRows = 0;

  constructor(
    readonly spec: SortSpec,
    readonly context: SortContext,
    readonly downstream: Operator,
    reserve: number,
    readonly general: (context: SortContext) => GeneralSort,
  ) {
    this.#charge = new Charge(context.grant);
    // Prepay the external sort's minimum. It also bounds this path's histogram/output step.
    // No radix arrays or payload chunks exist before this succeeds.
    if (!this.#charge.allocate(reserve)) throw new RangeError("Columnar sort minimum not admitted");
    this.#reserved = reserve;
    this.#store = new ColumnStore(spec.types, this.#charge);
    this.#slice = new Slice(YIELD_INTERVAL_MS, context.clock);
    let resolve!: () => void, reject!: (reason: unknown) => void;
    this.closed = new Promise<void>((done, failed) => {
      resolve = done;
      reject = failed;
    });
    this.#resolveClosed = resolve;
    this.#rejectClosed = reject;
    this.#onAbort = () => this.abort(context.signal?.reason);
    context.signal?.addEventListener("abort", this.#onAbort, { once: true });
    context.grant.onReclaimRequest(() => this.reclaim());
  }

  get profile(): SortProfile & { readonly columnarRows: number } {
    if (this.#fallback !== null)
      return {
        ...this.#fallback.profile,
        peakHeldBytes: Math.max(this.#charge.peak, this.#fallback.profile.peakHeldBytes),
        columnarRows: this.#columnarRows,
      };
    return {
      mode: "keys",
      rowsIn: this.#store.rows || this.#columnarRows,
      rowsOut: this.#rowsOut,
      rejected: 0,
      compactions: 0,
      runs: 0,
      mergePasses: 0,
      spilledBytes: 0,
      peakHeldBytes: this.#charge.peak,
      loans: 0,
      operandBytes: 0,
      thresholdBytes: 0,
      columnarRows: this.#columnarRows,
      ...(this.#compressionStats === null
        ? {}
        : {
            keyBits: this.#compressionStats.bits,
            packedWords: this.#compressionStats.words,
            rankedKeys: this.#compressionStats.rankedKeys,
            orderedSuffixKeys: this.#compressionStats.orderedSuffixKeys,
            rankRefusals: this.#compressionStats.rankRefusals,
          }),
      radixPasses: this.#radixProfile.passes,
    };
  }

  #check(): void {
    this.context.signal?.throwIfAborted();
    if (this.#abort.signal.aborted) throw this.#reason;
    if (this.#done) throw new TypeError("Columnar sort is closed");
  }

  #track(work: Promise<void>): Promise<void> {
    const pending = work.catch((error: unknown) => {
      this.abort(error);
      throw error;
    });
    this.#working = pending;
    this.#inflight.add(pending);
    void pending.then(
      () => {
        this.#inflight.delete(pending);
        if (this.#working === pending) this.#working = null;
      },
      () => {
        this.#inflight.delete(pending);
        if (this.#working === pending) this.#working = null;
      },
    );
    return pending;
  }

  #run<T>(job: SlicedJob<T>): T | Promise<T> {
    // runSliced's abort check can throw while a delegated generator still holds native
    // buffers. Close that generator before its owner's charges are released.
    try {
      const result = runSliced(job, this.#slice, () => this.#check());
      if (isPromise(result))
        return result.finally(() => {
          job.return(undefined as T);
        });
      return result;
    } catch (error) {
      job.return(undefined as T);
      throw error;
    }
  }

  *#arrival(batch: Batch): SlicedJob<void> {
    this.#check();
    if (this.#slice.due()) yield;
    let bytes = 0;
    try {
      const prepared =
        this.#fallback === null && this.#bounded(batch)
          ? yield* this.#dictionaryBatch(batch)
          : { batch, bytes: 0 };
      bytes = prepared?.bytes ?? 0;
      const work =
        prepared === null
          ? this.#external().then(() => this.#forward(batch))
          : this.#add(prepared.batch, batch);
      if (isPromise(work)) yield work;
    } finally {
      if (bytes) this.#charge.free(bytes);
    }
  }

  *#dictionaryBatch(batch: Batch): SlicedJob<{ batch: Batch; bytes: number } | null> {
    if (
      this.context.columnarDictionaries === false ||
      this.downstream.reclassifiesTransfers !== true
    )
      return { batch, bytes: 0 };
    if (!this.#dictionaryInitialized) {
      this.#dictionaryInitialized = true;
      const types = this.spec.types.slice();
      for (let c = 0; c < types.length; c++) {
        const type = types[c],
          vector = batch.columns[c];
        if (type?.kind !== "text" || vector?.form !== "dictionary") {
          this.#dictionaries.push(null);
          continue;
        }
        const dictionary = new SortDictionary(type, this.#charge);
        this.#dictionaries.push(dictionary);
        if (!dictionary.init()) return null;
        types[c] = { kind: "int32", bits: 32 };
      }
      if (this.#dictionaries.some((d) => d !== null)) {
        this.#store.close();
        this.#store = new ColumnStore(types, this.#charge);
      }
    }
    const count = this.#dictionaries.filter((d) => d !== null).length;
    if (!count) return { batch, bytes: 0 };
    const bytes = count * (128 + batch.length * 4);
    if (!this.#charge.allocate(bytes)) return null;
    let published = false;
    try {
      const columns = batch.columns.slice();
      for (let c = 0; c < this.#dictionaries.length; c++) {
        const dictionary = this.#dictionaries[c];
        if (!dictionary) continue;
        const source = batch.columns[c];
        if (source?.form !== "dictionary") return null;
        const translation = yield* dictionary.translate(source.dictionary, this.#slice);
        if (translation === null) return null;
        const codes = new Int32Array(batch.length);
        for (let row = 0; row < batch.length; row++)
          codes[row] = translation[source.codes[row] ?? 0] ?? 0;
        columns[c] = {
          form: "flat",
          type: { kind: "int32", bits: 32 },
          length: source.length,
          validity: source.validity,
          data: { layout: "i32", values: codes },
        };
        if (this.#slice.due()) yield;
      }
      published = true;
      return { batch: { ...batch, columns }, bytes };
    } finally {
      if (!published) this.#charge.free(bytes);
    }
  }
  #dropDictionaries(): void {
    for (const dictionary of this.#dictionaries) dictionary?.close();
    this.#dictionaries.length = 0;
  }

  #bounded(batch: Batch): boolean {
    if (batch.length > BATCH_ROWS) return false;
    let variableBytes = 0;
    return batch.columns.every((vector) => {
      const flat =
        vector.form === "flat"
          ? vector
          : vector.form === "constant"
            ? vector.value
            : vector.dictionary;
      if (flat.data.layout === "varlen") variableBytes += flat.data.bytes.byteLength;
      // Bound the combined payload of one row, including constant/dictionary expansion.
      // The general path owns larger rows and its progress-loan/error contract.
      return variableBytes <= MAX_INPUT_BYTES;
    });
  }

  #forward(batch: Batch): void | Promise<void> {
    const fallback = this.#fallback;
    if (fallback === null) throw new TypeError("Missing external sorter");
    return fallback.push(batch) === "blocked" ? fallback.ready() : undefined;
  }

  #add(batch: Batch, original: Batch = batch): void | Promise<void> {
    this.#check();
    if (this.#fallback !== null) return this.#forward(batch);
    if (!this.#bounded(batch)) return this.#external().then(() => this.#forward(batch));
    const before = this.#store.rows,
      count = batch.selection?.count ?? batch.length;
    if (this.#store.append(batch.columns, batch.selection?.rows ?? null, 0, count) === count)
      return;
    // Roll back this arrival, so the original borrowed batch can be forwarded without a new
    // uncharged selection buffer or an overlapping/omitted row.
    this.#store.truncate(before);
    return this.#external().then(() => this.#forward(original));
  }

  push(batch: Batch, transfer?: Transfer): PushResult {
    let work: void | Promise<void>;
    try {
      work =
        this.#reclaiming === null
          ? this.#run(this.#arrival(batch))
          : this.#reclaiming.then(() => this.#run(this.#arrival(batch)));
    } catch (error) {
      transfer?.release();
      this.abort(error);
      throw error;
    }
    if (!isPromise(work)) {
      transfer?.release();
      return "ready";
    }
    this.#pending = this.#track(work.finally(() => transfer?.release()));
    return "blocked";
  }
  ready(): Promise<void> {
    const pending = this.#pending ?? Promise.resolve();
    this.#pending = null;
    return pending;
  }

  #dropScratch(): void {
    this.#words.length = 0;
    this.#compression?.close();
    this.#compression = null;
    this.#ids = null;
    this.#temporary = null;
    this.#columns.length = 0;
    this.#radix = null;
    this.#floatingWords = null;
    this.#floating = null;
    if (this.#scratchBytes > 0) this.#charge.free(this.#scratchBytes);
    this.#scratchBytes = 0;
  }
  #releaseReserve(): void {
    if (this.#reserved > 0) this.#charge.free(this.#reserved);
    this.#reserved = 0;
  }

  async #external(): Promise<void> {
    this.#check();
    if (this.#fallback !== null) return;
    this.#dropScratch();
    this.#releaseReserve();
    // Keep the wrapper's reclaim dispatcher and termination fence. The original sorter must
    // not close the shared grant while replay/reclaim still borrows resident payload or pages.
    const grant = new Proxy(this.context.grant, {
      get: (target, property) => {
        if (property === "close" || property === "onReclaimRequest") return () => undefined;
        if (property === "free")
          return (bytes: number) => {
            if (this.#abort.signal.aborted) this.#deferredFree += bytes;
            else target.free(bytes);
          };
        if (property === "release")
          return (bytes: number) => {
            if (!this.#abort.signal.aborted) target.release(bytes);
          };
        if (property === "borrow")
          return async (bytes: number, signal?: AbortSignal): Promise<Loan> => {
            const loan = await target.borrow(bytes, signal);
            this.#loans.add(loan);
            return {
              bytes: loan.bytes,
              repay: () => {
                if (this.#abort.signal.aborted) return;
                loan.repay();
                this.#loans.delete(loan);
              },
            };
          };
        const value: unknown = Reflect.get(target, property);
        return typeof value === "function" ? (value.bind(target) as unknown) : value;
      },
    });
    const fallback = (this.#fallback = this.general({
      ...this.context,
      grant,
      columnar: false,
      signal: this.#abort.signal,
    }));
    for (let k = 0; k < this.#store.chunkCount; k++) {
      this.#check();
      const columns = this.spec.types.map((_, c) => {
        const vector = this.#store.chunk(c, k);
        return this.#dictionaries[c]?.wrap(vector) ?? vector;
      });
      const length = columns[0]?.length ?? 0;
      if (fallback.push({ length, columns, selection: null }) === "blocked") await fallback.ready();
      // Even immediately-ready replay is part of the shared worker task.
      const pause = this.#run(this.#pause());
      if (isPromise(pause)) await pause;
    }
    this.#store.close();
    this.#dropDictionaries();
  }
  *#pause(): SlicedJob<void> {
    if (this.#slice.due()) yield;
  }

  reclaim(): void | Promise<void> {
    if (this.#done || this.#finishing || this.#abort.signal.aborted) return;
    if (this.#fallback !== null) {
      const work = this.#fallback.reclaim();
      return isPromise(work) ? this.#track(work) : work;
    }
    if (this.#reclaiming !== null) return this.#reclaiming;
    const previous = this.#working;
    const work = (async () => {
      if (previous !== null) await previous;
      if (!this.#done) await this.#external();
    })();
    this.#reclaiming = work;
    void work.then(
      () => {
        this.#reclaiming = null;
      },
      (error: unknown) => {
        this.#reclaiming = null;
        this.abort(error);
      },
    );
    return work;
  }

  *#order(): SlicedJob<boolean> {
    const n = this.#store.rows;
    let words = 0;
    const groups: number[] = [];
    for (const key of this.spec.keys) {
      const type = this.spec.types[key.column];
      if (type === undefined) throw new TypeError("Missing key type");
      const count = numericWordCount(type) + Number(this.#store.hasNulls(key.column));
      words += count;
      groups.push(count);
    }
    const bytes =
      META_BYTES +
      (words + 2) * n * 4 +
      // Cached chunk refs/typed views and dynamic-array growth overlap.
      this.spec.types.length * (512 + this.#store.chunkCount * 256);
    if (n > 0x7fff_ffff || !this.#charge.allocate(bytes)) return false;
    this.#scratchBytes = bytes;
    this.#words = Array.from({ length: words }, () => new Uint32Array(n));
    this.#ids = new Int32Array(n);
    this.#temporary = new Int32Array(n);
    this.#floating = new Float64Array(1);
    this.#floatingWords = new Uint32Array(this.#floating.buffer);
    yield* encodeNumericColumns(
      this.#store,
      this.spec.keys,
      this.#words,
      this.#floating,
      this.#floatingWords,
      this.#slice,
    );
    if (n >= SMALL_SORT) {
      if (this.context.columnarCompression !== false) {
        this.#compression = new SortKeyCompression(this.#charge);
        const packed = yield* this.#compression.pack(this.#words, groups, n, this.#slice);
        if (packed === null) {
          this.#compression.close();
          this.#compression = null;
        } else {
          this.#words = packed;
          this.#compressionStats = { ...this.#compression.stats };
        }
      }
      // Both histograms remain inside the prepaid minimum, allocated only after its charge.
      this.#radix = new RadixScratch();
      yield* lsdSortIds(
        this.#words,
        this.#ids,
        this.#temporary,
        n,
        this.#slice,
        this.#radix,
        this.#radixProfile,
        (sorted, temporary) => {
          this.#ids = sorted;
          this.#temporary = temporary;
        },
      );
    } else {
      for (let i = 0; i < n; i++) this.#ids[i] = i;
      yield* mergeSortJob(
        this.#ids,
        this.#temporary,
        (a, b) => {
          for (const column of this.#words) {
            const x = column[a] ?? 0,
              y = column[b] ?? 0;
            if (x !== y) return x < y ? -1 : 1;
          }
          return 0;
        },
        this.#slice,
      );
    }
    for (let c = 0; c < this.spec.types.length; c++)
      this.#columns.push(yield* gatherSource(this.#store, c, this.#slice));
    // Oversized payloads take the existing progress-loan path, before any output is emitted.
    for (const column of this.#columns)
      if (layoutOf(column.type) === "varlen")
        for (const chunk of column.chunks) {
          if (chunk?.data.layout !== "varlen")
            throw new TypeError("Invalid varlen payload");
          for (let i = 0; i < chunk.length; i++)
            if ((chunk.data.offsets[i + 1] ?? 0) - (chunk.data.offsets[i] ?? 0) > MAX_INPUT_BYTES)
              return false;
          if (this.#slice.due()) yield;
        }
    return true;
  }

  *#emit(): SlicedJob<void> {
    const ids = this.#ids;
    if (ids === null) throw new TypeError("Missing sort permutation");
    const shift = this.#store.chunkShift,
      mask = this.#store.chunkMask;
    for (let at = 0; at < ids.length;) {
      let n = Math.min(BATCH_ROWS, ids.length - at),
        sizes: number[],
        bytes: number;
      for (;;) {
        sizes = this.#columns.map((column) =>
          layoutOf(column.type) === "varlen"
            ? varlenGatherBytes(column, ids, at, n, shift, mask)
            : 0,
        );
        bytes =
          META_BYTES +
          this.#columns.reduce((sum, column, c) => sum + gatherBytes(column, n, sizes[c] ?? 0), 0);
        if (bytes <= Math.min(BATCH_TARGET_BYTES, this.#reserved - RadixScratch.BYTES - META_BYTES))
          break;
        if (n === 1) throw new RangeError("Columnar output exceeded its reserved frame");
        n = Math.max(1, n >>> 1);
      }
      // This frame and radix are covered by the admitted reserve, never a progress loan.
      const columns: Array<FlatVector | DictionaryVector> = [];
      const movedChunks: ChunkTransfer[] = [];
      const canTransfer =
        this.context.columnarOwnership !== false && this.downstream.reclassifiesTransfers === true;
      try {
        for (let c = 0; c < this.#columns.length; c++) {
          const column = this.#columns[c];
          if (column === undefined) throw new TypeError("Missing payload column");
          const moved =
            this.context.columnarOwnership === false
              ? null
              : moveGatherColumn(column, ids, at, n, shift, mask);
          const gathered = moved ?? gatherColumn(column, ids, at, n, shift, mask, sizes[c] ?? 0);
          if (moved !== null && canTransfer) {
            const k = (ids[at] ?? 0) >>> shift;
            movedChunks.push(this.#store.transferChunk(c, k, () => retireGatherChunk(column, k)));
          }
          columns.push(this.#dictionaries[c]?.wrap(gathered) ?? gathered);
          if (this.#slice.due()) yield;
        }
        const transfer: Transfer | undefined = !canTransfer
          ? undefined
          : {
              detachableBuffers: new Set(
                frameTransferList({
                  rowCount: n,
                  columns: columns.map((v) =>
                    v.form === "flat"
                      ? v
                      : {
                          form: "dictionary",
                          length: n,
                          codes: v.codes,
                          validity: v.validity,
                          dictionary: 0,
                        },
                  ),
                  dictionaries: [],
                }),
              ),
              detachableBudget: this.context.grant,
              detachableReclassify: true,
              reclassify: (buffers) => this.#retireChunks(movedChunks, buffers),
              // Preserve the next gather's reserve; retire only resident chunk capacities.
              // Copied/refused buffers leave the store when this complete borrow ends.
              release: () => this.#retireChunks(movedChunks),
            };
        if (this.downstream.push({ length: n, columns, selection: null }, transfer) === "blocked")
          yield this.downstream.ready();
        this.#rowsOut += n;
      } finally {
        columns.length = 0;
      }
      at += n;
      if (this.#slice.due()) yield;
    }
  }

  #retireChunks(chunks: readonly ChunkTransfer[], buffers?: ReadonlySet<ArrayBuffer>): void {
    let bytes = 0;
    for (const chunk of chunks)
      bytes += buffers === undefined ? chunk.release() : chunk.reclassify(buffers);
    // Return ordinary idle holding too; the still-allocated next-gather reserve and any
    // loan-backed credit stay put. No ownership event transfers progress-loan credit.
    const grant = this.context.grant;
    const idle = Math.max(0, grant.held - grant.allocated - grant.loaned);
    if (bytes > 0 && idle > 0) grant.release(Math.min(bytes, idle));
  }

  finish(): Promise<void> {
    this.#finishing = true;
    return this.#track(
      (async () => {
        if (this.#reclaiming !== null) await this.#reclaiming;
        this.#check();
        if (this.#fallback === null) {
          const ordered = await this.#run(this.#order());
          if (!ordered) await this.#external();
        }
        if (this.#fallback !== null) {
          await this.#fallback.finish();
          await this.#fallback.closed;
          this.#close();
          return;
        }
        this.#columnarRows = this.#store.rows;
        await this.#run(this.#emit());
        this.#close();
        await this.downstream.finish();
      })(),
    );
  }

  #close(failure?: { readonly reason: unknown }): void {
    if (this.#done) return;
    this.#done = true;
    this.context.signal?.removeEventListener("abort", this.#onAbort);
    this.#dropScratch();
    this.#store.close();
    this.#dropDictionaries();
    this.#releaseReserve();
    if (this.#deferredFree > 0) this.context.grant.free(this.#deferredFree);
    this.#deferredFree = 0;
    for (const loan of this.#loans) loan.repay();
    this.#loans.clear();
    this.context.grant.close();
    if (failure) this.#rejectClosed(failure.reason);
    else this.#resolveClosed();
  }
  discard(): void {
    this.abort(new Error("Columnar sort discarded"));
  }
  abort(reason: unknown): void {
    if (this.#done || this.#abort.signal.aborted) return;
    this.#reason = reason;
    this.#abort.abort(reason);
    this.#fallback?.abort(reason);
    // A suspended append/replay/downstream still borrows these buffers and their charges.
    void Promise.allSettled([
      Promise.allSettled([...this.#inflight]),
      this.#reclaiming,
      this.#fallback?.closed,
    ]).then((results) => {
      const closed = results[2];
      this.#close(closed.status === "rejected" ? { reason: closed.reason } : undefined);
    });
  }
}
