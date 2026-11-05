/**
 * The durable store over four long-lived files, written through sync handles:
 * - `log`: two `LOG_HEADER_SLOT_BYTES` header slots, then the log body;
 * - `data`: the data space (extents at `DATA_ALIGN` offsets, reused in place);
 * - `root-0`, `root-1`: the two root slots (root-slot.ts), written alternately.
 *
 * OPFS gives it `FileSystemSyncAccessHandle`s; tests give it sim-disk.ts handles. All work is
 * synchronous on open handles: each async method runs its whole body before its first await
 * point, so the ownership check (`guard`) and the effect happen in one step.
 *
 * Publication order (guarantee 1): flush the data file; write the inactive root slot; flush it.
 * A torn slot fails its checksum and the other slot, the previous root, stays current.
 */
import { chooseRoot, encodeRootSlot } from "./root-slot.js";
import {
  DATA_ALIGN,
  LOG_HEADER_SLOT_BYTES,
  StoreClosedError,
  StoreCorruptionError,
  type DataRange,
  type StoreSession,
} from "./contract.js";

/** The subset of `FileSystemSyncAccessHandle` the store uses. */
export interface SyncFile {
  read(buffer: Uint8Array, options: { at: number }): number;
  write(buffer: Uint8Array, options: { at: number }): number;
  truncate(size: number): void;
  getSize(): number;
  flush(): void;
  close(): void;
}

export interface FileSet {
  readonly log: SyncFile;
  readonly data: SyncFile;
  readonly roots: readonly [SyncFile, SyncFile];
}

export const FILE_NAMES = ["log", "data", "root-0", "root-1"] as const;
const HEADER_REGION = 2 * LOG_HEADER_SLOT_BYTES;
/** Coalescing: merge ranges whose gap is at most this, into slabs of at most `MAX_SLAB`. */
const MAX_GAP = 64 * 1024;
const MAX_SLAB = 1024 * 1024;

function readExact(file: SyncFile, at: number, length: number): Uint8Array {
  const buffer = new Uint8Array(length);
  let done = 0;
  while (done < length) {
    const read = file.read(buffer.subarray(done), { at: at + done });
    if (read <= 0) throw new StoreCorruptionError("Short read inside the file");
    done += read;
  }
  return buffer;
}

function writeAll(file: SyncFile, bytes: Uint8Array, at: number): void {
  let done = 0;
  while (done < bytes.length) {
    const written = file.write(bytes.subarray(done), { at: at + done });
    if (written <= 0) throw new StoreCorruptionError("Short write");
    done += written;
  }
}

export class FileStoreSession implements StoreSession {
  readonly epoch: number;
  readonly #files: FileSet;
  readonly #guard: () => void;
  #logLength: number;
  #closed = false;

  /** `guard` throws `OwnershipLostError` once the lease is lost; it runs before every effect. */
  constructor(files: FileSet, epoch: number, guard: () => void) {
    this.#files = files;
    this.epoch = epoch;
    this.#guard = guard;
    this.#logLength = Math.max(0, files.log.getSize() - HEADER_REGION);
  }

  #live(): FileSet {
    if (this.#closed) throw new StoreClosedError();
    return this.#files;
  }

  #owned(): FileSet {
    const files = this.#live();
    this.#guard();
    return files;
  }

  async readRoot(): Promise<Uint8Array | null> {
    return this.#chooseRoot().current?.root ?? null;
  }

  #chooseRoot() {
    const [first, second] = this.#live().roots.map((file) => readExact(file, 0, file.getSize()));
    return chooseRoot(first ?? null, second ?? null);
  }

  async publish(root: Uint8Array): Promise<void> {
    const files = this.#owned();
    const { nextSlot, nextSequence } = this.#chooseRoot();
    files.data.flush();
    const slot = encodeRootSlot(nextSequence, root);
    const file = files.roots[nextSlot];
    writeAll(file, slot, 0);
    file.truncate(slot.length);
    file.flush();
  }

  async writeData(offset: number, bytes: Uint8Array): Promise<void> {
    if (offset % DATA_ALIGN !== 0 || offset < 0) throw new RangeError("Unaligned data extent");
    writeAll(this.#owned().data, bytes, offset);
  }

  async readData(ranges: readonly DataRange[]): Promise<Uint8Array[]> {
    const data = this.#live().data;
    const size = data.getSize();
    for (const { offset, length } of ranges)
      if (offset < 0 || length < 0 || offset + length > size)
        throw new StoreCorruptionError("Read outside the data space");
    const order = ranges
      .map((range, position) => ({ range, position }))
      .sort((a, b) => a.range.offset - b.range.offset);
    const results = new Array<Uint8Array>(ranges.length);
    let index = 0;
    for (let first = order[0]; first !== undefined; first = order[index]) {
      let end = first.range.offset + first.range.length;
      let last = index;
      for (let next = order[last + 1]; next !== undefined; next = order[last + 1]) {
        const nextEnd = Math.max(end, next.range.offset + next.range.length);
        if (next.range.offset - end > MAX_GAP || nextEnd - first.range.offset > MAX_SLAB) break;
        end = nextEnd;
        last += 1;
      }
      const start = first.range.offset;
      const slab = readExact(data, start, end - start);
      for (const { range, position } of order.slice(index, last + 1))
        results[position] = slab.subarray(
          range.offset - start,
          range.offset - start + range.length,
        );
      index = last + 1;
    }
    return results;
  }

  async releaseData(ranges: readonly DataRange[]): Promise<void> {
    const data = this.#owned().data;
    // Only a free tail can be returned; interior space is reused by the engine's allocator.
    let size = data.getSize();
    for (const range of [...ranges].sort((a, b) => b.offset - a.offset))
      if (range.offset + range.length >= size && range.offset < size) size = range.offset;
    if (size < data.getSize()) data.truncate(size);
  }

  async dataLength(): Promise<number> {
    return this.#live().data.getSize();
  }

  logLength(): number {
    this.#live();
    return this.#logLength;
  }

  async readLog(offset: number, length: number): Promise<Uint8Array> {
    const log = this.#live().log;
    if (offset < 0 || length < 0 || offset + length > this.#logLength)
      throw new StoreCorruptionError("Read outside the log");
    return readExact(log, HEADER_REGION + offset, length);
  }

  async readLogHeaders(): Promise<readonly [Uint8Array | null, Uint8Array | null]> {
    const log = this.#live().log;
    const size = log.getSize();
    const slot = (index: number) =>
      size >= (index + 1) * LOG_HEADER_SLOT_BYTES
        ? readExact(log, index * LOG_HEADER_SLOT_BYTES, LOG_HEADER_SLOT_BYTES)
        : null;
    return [slot(0), slot(1)];
  }

  async appendLog(bytes: Uint8Array): Promise<number> {
    const log = this.#owned().log;
    const offset = this.#logLength;
    if (log.getSize() < HEADER_REGION) log.truncate(HEADER_REGION);
    writeAll(log, bytes, HEADER_REGION + offset);
    this.#logLength += bytes.length;
    return offset;
  }

  async writeLogHeader(slot: 0 | 1, bytes: Uint8Array): Promise<void> {
    if (bytes.length > LOG_HEADER_SLOT_BYTES) throw new RangeError("Log header slot overflow");
    const log = this.#owned().log;
    const padded = new Uint8Array(LOG_HEADER_SLOT_BYTES);
    padded.set(bytes);
    writeAll(log, padded, slot * LOG_HEADER_SLOT_BYTES);
  }

  async flushLog(): Promise<void> {
    this.#owned().log.flush();
  }

  async truncateLog(length: number): Promise<void> {
    const log = this.#owned().log;
    if (length < 0 || length > this.#logLength) throw new RangeError("Truncate outside the log");
    log.truncate(HEADER_REGION + length);
    log.flush();
    this.#logLength = length;
  }

  async resetLog(): Promise<void> {
    const log = this.#owned().log;
    log.truncate(HEADER_REGION);
    writeAll(log, new Uint8Array(HEADER_REGION), 0);
    log.flush();
    this.#logLength = 0;
  }

  async close(): Promise<void> {
    this.#closed = true;
  }
}
