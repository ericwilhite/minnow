/**
 * Shared scratch allocator. Ownership and coalesced free ranges live in checksummed scratch
 * pages, with one cached page, two I/O frames and sixteen pooled release descriptors. The owner
 * admits SCRATCH_CONTROL_BYTES before construction; this fixed workspace remains charged until
 * all file I/O and releases settle. Metadata growth never asks an operator for more memory.
 * Optional 256 KiB batching uses the operator's budget; refusal selects direct writes. File
 * handles retain scalar chain coordinates, never a resident extent list or per-page batch map.
 * Allocator operations are serialized, with no admission wait while holding that serialization.
 * Released ranges become reusable only after the native release succeeds. Nothing is recovered:
 * startup sweep discards payload and private allocator pages under the owner's fence.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import {
  DATA_ALIGN,
  StoreQuotaError,
  type DataRange,
  type ScratchSpace,
} from "../storage/contract.js";
import type { ByteBudget, CacheMemory } from "../storage/extents.js";
import { RangeChain, ScratchRanges, SCRATCH_RELEASE_BATCH } from "./scratch-ranges.js";
import {
  SpillCorruptionError,
  SpillQuotaError,
  type SpillFile,
  type SpillPageRef,
  type SpillService,
} from "./contract.js";

/** The batch buffer a file asks its budget for. */
export const SPILL_BATCH_BYTES = 256 * 1024;

function spillError(error: unknown): Error {
  return error instanceof StoreQuotaError
    ? new SpillQuotaError(error.message)
    : error instanceof Error
      ? error
      : new Error(String(error));
}

export class ScratchSpillService implements SpillService {
  readonly #scratch: ScratchSpace;
  readonly #ranges: ScratchRanges;
  readonly #pool: Array<{ offset: number; length: number }>;
  readonly #release: DataRange[];
  #first: ScratchSpillFile | null = null;
  #generation = 0;
  #identity = 0;
  #end = 0;
  #queue: Promise<unknown> = Promise.resolve();
  #failure: Error | null = null;
  #closing: Promise<void> | null = null;
  #sweeping = false;

  constructor(
    scratch: ScratchSpace,
    options: { readonly budget: ByteBudget; readonly memory?: CacheMemory },
  ) {
    this.#scratch = scratch;
    this.#ranges = new ScratchRanges(() => {
      const metadata = scratch.metadata?.();
      if (metadata === undefined) throw new TypeError("Scratch allocator needs a metadata backing");
      return metadata;
    }, options.budget);
    this.#pool = Array.from({ length: SCRATCH_RELEASE_BATCH }, () => ({ offset: 0, length: 0 }));
    this.#release = [];
    try {
      if (options.memory !== undefined) scratch.chargeTo?.(options.memory);
    } catch (error) {
      this.#ranges.close();
      throw error;
    }
  }

  get residentExtentEntries(): number {
    return this.#ranges.residentEntries + this.#pool.length + this.#release.length;
  }
  get bytesInUse(): number {
    let bytes = 0;
    for (let file = this.#first; file !== null; file = file.next) bytes += file.extentBytes;
    return bytes;
  }

  async create(queryId: number, budget: ByteBudget): Promise<SpillFile> {
    if (this.#closing !== null || this.#sweeping)
      throw new TypeError("The spill service is closed or sweeping");
    if (this.#failure !== null) throw this.#failure;
    const file = new ScratchSpillFile(this, queryId, budget, this.#generation, ++this.#identity);
    file.next = this.#first;
    if (this.#first !== null) this.#first.previous = file;
    this.#first = file;
    return file;
  }

  #unlink(file: ScratchSpillFile): void {
    if (file.previous !== null) file.previous.next = file.next;
    else this.#first = file.next;
    if (file.next !== null) file.next.previous = file.previous;
    file.previous = null;
    file.next = null;
  }

  #serial<T>(work: () => Promise<T>): Promise<T> {
    const run = this.#queue.then(async () => {
      if (this.#failure !== null) throw this.#failure;
      try {
        return await work();
      } catch (error) {
        this.#failure = spillError(error);
        throw this.#failure;
      }
    });
    this.#queue = run.catch(() => undefined);
    return run;
  }

  async #drain(): Promise<void> {
    while (this.#first !== null) await this.#first.close().catch(() => undefined);
    await this.#queue;
  }

  async sweep(): Promise<void> {
    if (this.#closing !== null || this.#sweeping)
      throw new TypeError("The spill service is closed or sweeping");
    this.#sweeping = true;
    try {
      // Keep the workspace and batches charged until all previously started I/O has settled.
      await this.#drain();
      this.#generation++;
      await this.#scratch.truncate(0);
      await this.#ranges.reset();
      await this.#scratch.dispose?.();
      this.#end = 0;
      this.#failure = null;
    } finally {
      this.#sweeping = false;
    }
  }

  close(): Promise<void> {
    this.#closing ??= this.#close();
    return this.#closing;
  }
  async #close(): Promise<void> {
    try {
      await this.#drain();
      await this.#scratch.dispose?.();
    } finally {
      this.#generation++;
      this.#pool.length = 0;
      this.#release.length = 0;
      this.#ranges.close();
    }
  }
  get generation(): number {
    return this.#generation;
  }

  allocate(bytes: number, chain: RangeChain): Promise<DataRange> {
    return this.#serial(async () => {
      const size = Math.ceil(bytes / DATA_ALIGN) * DATA_ALIGN;
      if (!Number.isSafeInteger(size) || size <= 0)
        throw new RangeError("Invalid spill allocation");
      const after = chain.lastOffset + chain.lastLength;
      let offset = chain.tail === 0 ? null : await this.#ranges.take(size, after);
      if (offset === null && chain.tail !== 0 && after === this.#end) offset = this.#end;
      offset ??= await this.#ranges.take(size, null);
      offset ??= this.#end;
      this.#end = Math.max(this.#end, offset + size);
      await this.#ranges.append(chain, offset, size);
      return { offset, length: size };
    });
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

  trim(extent: DataRange, used: number, chain: RangeChain): Promise<number> {
    return this.#serial(async () => {
      const freed = extent.length - Math.ceil(used / DATA_ALIGN) * DATA_ALIGN;
      if (freed > 0) {
        await this.#ranges.trim(chain, freed);
        await this.#ranges.add(extent.offset + extent.length - freed, freed);
      }
      return freed;
    });
  }

  async release(file: ScratchSpillFile): Promise<void> {
    try {
      await this.#serial(async () => {
        const ranges = this.#ranges,
          frame = ranges.releaseFrame,
          view = new DataView(frame.buffer);
        let id = file.chain.head;
        while (id !== 0) {
          await ranges.read(id, file.chain.owner, frame);
          const next = view.getFloat64(16, true),
            count = view.getUint32(24, true);
          for (let start = 0; start < count; start += SCRATCH_RELEASE_BATCH) {
            this.#release.length = 0;
            for (let i = 0; i < Math.min(SCRATCH_RELEASE_BATCH, count - start); i++) {
              const range = this.#pool[i];
              if (range === undefined) throw new Error("Missing release descriptor");
              range.offset = view.getFloat64(32 + (start + i) * 16, true);
              range.length = view.getFloat64(40 + (start + i) * 16, true);
              this.#release.push(range);
            }
            await this.#scratch.release(this.#release);
            for (const range of this.#release) await ranges.add(range.offset, range.length);
          }
          await ranges.recycle(id);
          id = next;
        }
        this.#end = await ranges.dropTail(this.#end);
        // Truncate only after release and free-directory updates have both settled.
        await this.#scratch.truncate(this.#end);
      });
    } finally {
      this.#unlink(file);
    }
  }
}

class ScratchSpillFile implements SpillFile {
  readonly queryId: number;
  readonly generation: number;
  readonly #service: ScratchSpillService;
  readonly #budget: ByteBudget;
  readonly chain: RangeChain;
  next: ScratchSpillFile | null = null;
  previous: ScratchSpillFile | null = null;
  #extentBytes = 0;
  #closing: Promise<void> | null = null;
  /** The batch buffer, or null when the budget refused it or the file is closed. */
  #batch: Uint8Array | null;
  /** Where the batch's bytes will go: the scratch offset of its first byte, once allocated. */
  #batchLength = 0;
  #length = 0;
  #state: "open" | "closed" | "dead" = "open";
  /** Appends and reads run one at a time (the batch buffer is shared state). */
  #queue: Promise<unknown> = Promise.resolve();

  constructor(
    service: ScratchSpillService,
    queryId: number,
    budget: ByteBudget,
    generation: number,
    identity: number,
  ) {
    this.chain = new RangeChain(identity);
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
    return this.chain.count;
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
        const ref = await this.#placeInBatch(page, checksum);
        return ref;
      }
      // No batch buffer, or a page larger than it: flush what is batched, then write it alone.
      await this.#flush();
      const extent = await this.#service.allocate(page.length, this.chain);
      this.#extentBytes += extent.length;
      await this.#service.write(extent.offset, page);
      this.#length += page.length;
      return { offset: extent.offset, length: page.length, checksum };
    });
  }

  /** Copies a page into the batch, reserving the batch's extent with its first page. */
  async #placeInBatch(page: Uint8Array, checksum: number): Promise<SpillPageRef> {
    const batch = this.#batch;
    if (batch === null) throw new Error("No batch buffer");
    if (this.#batchLength === 0) {
      this.#batchExtent = await this.#service.allocate(batch.length, this.chain);
      this.#extentBytes += this.#batchExtent.length;
    }
    const extent = this.#batchExtent;
    if (extent === null) throw new Error("No batch extent");
    const offset = extent.offset + this.#batchLength;
    batch.set(page, this.#batchLength);
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
    const freed = await this.#service.trim(extent, this.#batchLength, this.chain);
    this.#extentBytes -= freed;
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
    const inBatch = this.#batchExtent === null ? -1 : ref.offset - this.#batchExtent.offset;
    if (inBatch >= 0 && inBatch + ref.length <= this.#batchLength && this.#batch !== null)
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
    this.#state = "closed";
    await this.#queue;
    this.#dropBatch();
    this.#extentBytes = 0;
    this.#length = 0;
    try {
      await this.#service.release(this);
    } finally {
      this.chain.head = 0;
      this.chain.tail = 0;
      this.chain.count = 0;
      this.chain.lastOffset = 0;
      this.chain.lastLength = 0;
    }
  }
}
