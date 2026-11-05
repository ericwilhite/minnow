/**
 * The spill service over a backend's scratch space (contract.ts "Implementations"): OPFS's `spill`
 * file or IndexedDB's `spill` object store, through the same `ScratchSpace`.
 *
 * - **Space.** One `FreeSpaceMap` over the scratch space. A file is a list of extents; each batch
 *   write first tries to extend the file's last extent in place (`allocateAt`), so a run is written
 *   and read back sequentially.
 * - **Batching.** A file asks its `budget` for a 256 KiB batch buffer when created. The batch's
 *   extent is reserved when its first page arrives, so every ref names its final offset at once;
 *   appends copy pages into the buffer, which is written as one extent (its unused tail returned to
 *   the map) when the next page would not fit. Reads of batched pages are served from the buffer;
 *   close drops it unwritten. Without the buffer (the budget refused), each page is written
 *   directly as its own extent: spilling never needs memory beyond the page the operator holds.
 * - **Pages never straddle extents**, so a page is one contiguous read. A page larger than the
 *   buffer flushes the buffer and is written alone.
 * - **Reads** verify xxHash32 and return a fresh array (`read`) or fill the caller's (`readInto`).
 *   A page still in the batch buffer is served from it.
 * - **Release.** `close` hands the file's extents to `ScratchSpace.release` and, once that
 *   resolves, back to the map. `sweep` (startup) truncates the space to zero and starts a new map.
 * - **Errors.** `StoreQuotaError` from the scratch space becomes `SpillQuotaError`; the query fails,
 *   the database is unaffected. A fenced owner's scratch calls fail with `OwnershipLostError`.
 * - **Never in recovery.** Nothing is flushed and nothing durable names spill space.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import {
  DATA_ALIGN,
  StoreQuotaError,
  type DataRange,
  type ScratchSpace,
} from "../storage/contract.js";
import type { ByteBudget } from "../storage/extents.js";
import { FreeSpaceMap } from "../storage/free-space.js";
import {
  SpillCorruptionError,
  SpillQuotaError,
  type SpillFile,
  type SpillPageRef,
  type SpillService,
} from "./contract.js";

/** The batch buffer a file asks its budget for. */
export const SPILL_BATCH_BYTES = 256 * 1024;

function spillError(error: unknown): unknown {
  return error instanceof StoreQuotaError ? new SpillQuotaError(error.message) : error;
}

export class ScratchSpillService implements SpillService {
  readonly #scratch: ScratchSpace;
  #map = new FreeSpaceMap(DATA_ALIGN);
  readonly #files = new Set<ScratchSpillFile>();
  /** Bumped by `sweep`: files from before it are dead. */
  #generation = 0;

  constructor(scratch: ScratchSpace) {
    this.#scratch = scratch;
  }

  get bytesInUse(): number {
    let bytes = 0;
    for (const file of this.#files) bytes += file.extentBytes;
    return bytes;
  }

  async create(queryId: number, budget: ByteBudget): Promise<SpillFile> {
    const file = new ScratchSpillFile(this, queryId, budget, this.#generation);
    this.#files.add(file);
    return file;
  }

  async sweep(): Promise<void> {
    for (const file of this.#files) file.kill();
    this.#files.clear();
    this.#generation += 1;
    this.#map = new FreeSpaceMap(DATA_ALIGN);
    await this.#scratch.truncate(0);
  }

  get generation(): number {
    return this.#generation;
  }

  /** Space for `bytes`, after `last` if that space is free (a sequential run). */
  allocate(bytes: number, last: DataRange | null): DataRange {
    const size = this.#map.sizeOf(bytes);
    if (last !== null) {
      const after = last.offset + this.#map.sizeOf(last.length);
      if (this.#map.allocateAt(after, size)) return { offset: after, length: size };
    }
    return { offset: this.#map.allocate(size), length: size };
  }

  async write(offset: number, bytes: Uint8Array): Promise<void> {
    try {
      await this.#scratch.write(offset, bytes);
    } catch (error) {
      throw spillError(error);
    }
  }

  async readInto(offset: number, target: Uint8Array): Promise<void> {
    await this.#scratch.readInto(offset, target);
  }

  /** Frees the never-written tail of a reserved `extent` beyond `used` bytes; returns its size. */
  trim(extent: DataRange, used: number): number {
    const kept = this.#map.sizeOf(used);
    if (kept < extent.length) this.#map.free(extent.offset + kept, extent.length - kept);
    return extent.length - kept;
  }

  /** Returns a closed file's extents: released to the platform, then reusable. */
  async release(file: ScratchSpillFile, ranges: readonly DataRange[]): Promise<void> {
    this.#files.delete(file);
    if (ranges.length === 0 || file.generation !== this.#generation) return;
    await this.#scratch.release(ranges);
    if (file.generation !== this.#generation) return;
    for (const range of ranges) this.#map.free(range.offset, range.length);
  }
}

class ScratchSpillFile implements SpillFile {
  readonly queryId: number;
  readonly generation: number;
  readonly #service: ScratchSpillService;
  readonly #budget: ByteBudget;
  /**
   * The file's space, adjacent ranges coalesced (a sequential run is one entry), so bookkeeping is
   * O(1) per flush (review B1-p2b finding 3). The pending batch's reservation is always the tail of
   * the last entry.
   */
  readonly #extents: Array<{ offset: number; length: number }> = [];
  #extentBytes = 0;
  #closing: Promise<void> | null = null;
  /** The batch buffer, or null when the budget refused it or the file is closed. */
  #batch: Uint8Array | null;
  /** Where the batch's bytes will go: the scratch offset of its first byte, once allocated. */
  #batchLength = 0;
  /** Refs of pages still in the batch: page offset → its place in the batch. */
  readonly #batched = new Map<number, number>();
  #length = 0;
  #state: "open" | "closed" | "dead" = "open";
  /** Appends and reads run one at a time (the batch buffer is shared state). */
  #queue: Promise<unknown> = Promise.resolve();

  constructor(
    service: ScratchSpillService,
    queryId: number,
    budget: ByteBudget,
    generation: number,
  ) {
    this.#service = service;
    this.queryId = queryId;
    this.#budget = budget;
    this.generation = generation;
    this.#batch = budget.allocate(SPILL_BATCH_BYTES) ? new Uint8Array(SPILL_BATCH_BYTES) : null;
  }

  get length(): number {
    return this.#length;
  }

  get extentBytes(): number {
    return this.#extentBytes;
  }

  /** Runs of contiguous space the file holds (diagnostics: a sequential file holds one). */
  get extentRuns(): number {
    return this.#extents.length;
  }

  /** Records newly allocated space, extending the last entry when it is adjacent. */
  #claim(range: DataRange): void {
    const last = this.#extents.at(-1);
    if (last !== undefined && last.offset + last.length === range.offset)
      last.length += range.length;
    else this.#extents.push({ offset: range.offset, length: range.length });
    this.#extentBytes += range.length;
  }

  /** The service swept: nothing of this file exists any more. */
  kill(): void {
    this.#state = "dead";
    this.#dropBatch();
  }

  #serial<T>(work: () => Promise<T>): Promise<T> {
    const run = this.#queue.then(() => {
      if (this.#state !== "open")
        throw new TypeError(`Spill file ${String(this.queryId)} is ${this.#state}`);
      return work();
    });
    this.#queue = run.catch(() => undefined);
    return run;
  }

  append(page: Uint8Array): Promise<SpillPageRef> {
    return this.#serial(async () => {
      const checksum = xxHash32(page);
      const batch = this.#batch;
      if (batch !== null && page.length <= batch.length) {
        if (this.#batchLength + page.length > batch.length) await this.#flush();
        const ref = this.#placeInBatch(page, checksum);
        return ref;
      }
      // No batch buffer, or a page larger than it: flush what is batched, then write it alone.
      await this.#flush();
      const extent = this.#service.allocate(page.length, this.#extents.at(-1) ?? null);
      this.#claim(extent);
      await this.#service.write(extent.offset, page);
      this.#length += page.length;
      return { offset: extent.offset, length: page.length, checksum };
    });
  }

  /** Copies a page into the batch, reserving the batch's extent with its first page. */
  #placeInBatch(page: Uint8Array, checksum: number): SpillPageRef {
    const batch = this.#batch;
    if (batch === null) throw new Error("No batch buffer");
    if (this.#batchLength === 0) {
      this.#batchExtent = this.#service.allocate(batch.length, this.#extents.at(-1) ?? null);
      this.#claim(this.#batchExtent);
    }
    const extent = this.#batchExtent;
    if (extent === null) throw new Error("No batch extent");
    const offset = extent.offset + this.#batchLength;
    batch.set(page, this.#batchLength);
    this.#batched.set(offset, this.#batchLength);
    this.#batchLength += page.length;
    this.#length += page.length;
    return { offset, length: page.length, checksum };
  }

  #batchExtent: DataRange | null = null;

  /** Writes the batch into its extent and returns the extent's unused tail. */
  async #flush(): Promise<void> {
    const batch = this.#batch;
    const extent = this.#batchExtent;
    if (batch === null || extent === null || this.#batchLength === 0) return;
    await this.#service.write(extent.offset, batch.subarray(0, this.#batchLength));
    // The batch's reservation is the tail of the last entry: give back its unused part there.
    const freed = this.#service.trim(extent, this.#batchLength);
    const last = this.#extents.at(-1);
    if (last !== undefined) last.length -= freed;
    this.#extentBytes -= freed;
    this.#batched.clear();
    this.#batchLength = 0;
    this.#batchExtent = null;
  }

  read(ref: SpillPageRef): Promise<Uint8Array> {
    return this.#serial(async () => {
      const target = new Uint8Array(ref.length);
      await this.#fill(ref, target);
      return target;
    });
  }

  readInto(ref: SpillPageRef, target: Uint8Array): Promise<void> {
    if (target.length !== ref.length)
      return Promise.reject(new RangeError("Spill read target has the wrong size"));
    return this.#serial(() => this.#fill(ref, target));
  }

  async #fill(ref: SpillPageRef, target: Uint8Array): Promise<void> {
    const inBatch = this.#batched.get(ref.offset);
    if (inBatch !== undefined && this.#batch !== null)
      target.set(this.#batch.subarray(inBatch, inBatch + ref.length));
    else await this.#service.readInto(ref.offset, target);
    if (xxHash32(target) !== ref.checksum)
      throw new SpillCorruptionError(`Spill page at ${String(ref.offset)} failed verification`);
  }

  #dropBatch(): void {
    if (this.#batch === null) return;
    this.#batch = null;
    this.#budget.free(SPILL_BATCH_BYTES);
  }

  /** Every caller shares one cleanup (review B1-p2b finding 4): all resolve when it is done. */
  close(): Promise<void> {
    this.#closing ??= this.#close();
    return this.#closing;
  }

  async #close(): Promise<void> {
    const live = this.#state === "open";
    this.#state = "closed";
    await this.#queue;
    this.#dropBatch();
    this.#batched.clear();
    const extents = live ? this.#extents.splice(0) : [];
    this.#extentBytes = 0;
    this.#length = 0;
    await this.#service.release(this, extents);
  }
}
