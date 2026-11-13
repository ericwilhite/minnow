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
import { ROOT_SLOT_OVERHEAD } from "../durable-format.js";
import { chooseRoot, encodeRootSlot } from "./root-slot.js";
import { preparedRoot } from "./root-memory.js";
import {
  DATA_ALIGN,
  METADATA_PAGE_BYTES,
  METADATA_SMALL_BYTES,
  type MetadataStore,
  LOG_HEADER_SLOT_BYTES,
  StoreClosedError,
  StoreCorruptionError,
  type DataRange,
  type ScratchSpace,
  type ScratchMetadata,
  type StoreSession,
  type SyncFile,
} from "./contract.js";
import { planSlabs } from "./slabs.js";

export type { SyncFile } from "./contract.js";

export interface FileSet {
  readonly log: SyncFile;
  readonly data: SyncFile;
  readonly roots: readonly [SyncFile, SyncFile];
  readonly metadata?: SyncFile;
  readonly metadataIndex?: SyncFile;
  readonly spillMetadata?: SyncFile;
}

export const FILE_NAMES = ["log", "data", "root-0", "root-1"] as const;
const HEADER_REGION = 2 * LOG_HEADER_SLOT_BYTES;

function readFully(file: SyncFile, at: number, buffer: Uint8Array): Uint8Array {
  let done = 0;
  while (done < buffer.length) {
    const read = file.read(buffer.subarray(done), { at: at + done });
    if (read <= 0) throw new StoreCorruptionError("Short read inside the file");
    done += read;
  }
  return buffer;
}

const readExact = (file: SyncFile, at: number, length: number): Uint8Array =>
  readFully(file, at, new Uint8Array(length));

function writeAll(file: SyncFile, bytes: Uint8Array, at: number): void {
  let done = 0;
  while (done < bytes.length) {
    const written = file.write(bytes.subarray(done), { at: at + done });
    // A count past what was asked is no more trustworthy than none (opfs.ts, QuotaMappedFile).
    if (written <= 0 || written > bytes.length - done)
      throw new StoreCorruptionError(`Write reported ${String(written)} bytes`);
    done += written;
  }
}

export class FileStoreSession implements StoreSession {
  readonly epoch: number;
  readonly rootWriterToken = {};
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

  #metadata: MetadataStore | null = null;
  metadata(): MetadataStore | null {
    this.#owned();
    const file = this.#files.metadata;
    const index = this.#files.metadataIndex;
    if (file === undefined || index === undefined) return null;
    return (this.#metadata ??= new FileMetadata(file, index, () => {
      this.#owned();
    }));
  }

  async readRoot(): Promise<Uint8Array | null> {
    return this.#chooseRoot().current?.root ?? null;
  }

  async readRootSlot(slot: 0 | 1): Promise<Uint8Array | null> {
    const file = this.#live().roots[slot],
      length = file.getSize();
    if (length > (1 << 20) + ROOT_SLOT_OVERHEAD)
      throw new StoreCorruptionError("Root slot exceeds1MiB payload");
    return length === 0 ? null : readExact(file, 0, length);
  }

  async readPreviousRoot(): Promise<Uint8Array | null> {
    return this.#chooseRoot().previous?.root ?? null;
  }

  #chooseRoot() {
    const [first, second] = this.#live().roots.map((file) => readExact(file, 0, file.getSize()));
    return chooseRoot(first ?? null, second ?? null);
  }

  async publish(root: Uint8Array): Promise<void> {
    const files = this.#owned();
    const prepared = preparedRoot(this, root);
    const chosen = prepared === undefined ? this.#chooseRoot() : null;
    const nextSlot = prepared?.slot ?? chosen?.nextSlot ?? 0;
    files.data.flush();
    const slot = prepared?.frame.slotBytes ?? encodeRootSlot(chosen?.nextSequence ?? 1, root);
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
    const results = new Array<Uint8Array>(ranges.length);
    for (const slab of planSlabs(ranges)) {
      const bytes = readExact(data, slab.offset, slab.length);
      for (const position of slab.members) {
        const range = ranges[position];
        if (range === undefined) continue;
        const start = range.offset - slab.offset;
        results[position] = bytes.subarray(start, start + range.length);
      }
    }
    return results;
  }

  async readDataInto(offset: number, target: Uint8Array): Promise<void> {
    const data = this.#live().data;
    if (offset < 0 || offset + target.length > data.getSize())
      throw new StoreCorruptionError("Read outside the data space");
    readFully(data, offset, target);
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

/**
 * The scratch space over one sync handle (OPFS `spill`, or a simulated file). `guard` is the
 * lease's ownership check, run before every call touches the handle, so a fenced owner gets
 * `OwnershipLostError` rather than the closed handle's `InvalidStateError`.
 */
export class FileScratch implements ScratchSpace {
  readonly #file: SyncFile;
  readonly #guard: () => void;

  constructor(
    file: SyncFile,
    guard: () => void,
    readonly metadataFile?: SyncFile,
  ) {
    this.#file = file;
    this.#guard = guard;
  }

  metadata(): ScratchMetadata {
    const file = this.metadataFile;
    if (file === undefined) throw new TypeError("Scratch allocator metadata backing is missing");
    const check = (page: number, bytes: Uint8Array) => {
      if (!Number.isSafeInteger(page) || page < 0 || bytes.byteLength !== DATA_ALIGN)
        throw new RangeError("Invalid scratch metadata page");
    };
    return {
      read: async (page, target) => {
        check(page, target);
        this.#guard();
        readFully(file, page * DATA_ALIGN, target);
      },
      write: async (page, bytes) => {
        check(page, bytes);
        this.#guard();
        writeAll(file, bytes, page * DATA_ALIGN);
      },
      reset: async () => {
        this.#guard();
        file.truncate(0);
      },
    };
  }

  async write(offset: number, bytes: Uint8Array): Promise<void> {
    if (offset % DATA_ALIGN !== 0 || offset < 0) throw new RangeError("Unaligned scratch extent");
    this.#guard();
    writeAll(this.#file, bytes, offset);
  }

  async readInto(offset: number, target: Uint8Array): Promise<void> {
    this.#guard();
    if (offset < 0 || offset + target.length > this.#file.getSize())
      throw new StoreCorruptionError("Read outside the scratch space");
    readFully(this.#file, offset, target);
  }

  async release(ranges: readonly DataRange[]): Promise<void> {
    this.#guard();
    // Only a free tail can be returned; interior space is reused by the spill service's map.
    let size = this.#file.getSize();
    for (const range of [...ranges].sort((a, b) => b.offset - a.offset))
      if (range.offset + range.length >= size && range.offset < size) size = range.offset;
    if (size < this.#file.getSize()) this.#file.truncate(size);
  }

  async truncate(size: number): Promise<void> {
    this.#guard();
    if (size < this.#file.getSize()) this.#file.truncate(size);
  }
}

/** Fixed pages over a dedicated file: no record directory or growable managed buffers. */
/** Fixed data-page addresses and a compact512-byte directory. Small records live entirely
 * in the directory; neither file needs a resident page/offset index. Both reset per owner. */
class FileMetadata implements MetadataStore {
  readonly #header = new Uint8Array(4);
  constructor(
    readonly file: SyncFile,
    readonly index: SyncFile,
    readonly guard: () => void,
  ) {}
  #check(page: number, bytes: Uint8Array, reading: boolean): void {
    if (
      !Number.isSafeInteger(page) ||
      page < 0 ||
      !Number.isSafeInteger((page + 1) * METADATA_PAGE_BYTES)
    )
      throw new RangeError("Bad metadata page");
    if (
      bytes.byteLength !== METADATA_PAGE_BYTES &&
      (reading || bytes.byteLength !== METADATA_SMALL_BYTES)
    )
      throw new RangeError("Bad metadata frame");
  }
  async read(page: number, target: Uint8Array): Promise<void> {
    this.guard();
    this.#check(page, target, true);
    const at = page * 512;
    if (at + 4 > this.index.getSize()) throw new StoreCorruptionError("Missing metadata page");
    readFully(this.index, at, this.#header);
    const length = new DataView(this.#header.buffer).getUint32(0, true);
    if (length !== METADATA_PAGE_BYTES && length !== METADATA_SMALL_BYTES)
      throw new StoreCorruptionError("Malformed metadata directory");
    const file = length === METADATA_SMALL_BYTES ? this.index : this.file;
    const offset = length === METADATA_SMALL_BYTES ? at + 4 : page * METADATA_PAGE_BYTES;
    if (offset + length > file.getSize())
      throw new StoreCorruptionError("Missing metadata page body");
    target.fill(0);
    readFully(file, offset, target.subarray(0, length));
  }
  async write(page: number, bytes: Uint8Array): Promise<void> {
    this.guard();
    this.#check(page, bytes, false);
    const at = page * 512;
    if (bytes.length === METADATA_SMALL_BYTES) writeAll(this.index, bytes, at + 4);
    else writeAll(this.file, bytes, page * METADATA_PAGE_BYTES);
    // The body precedes the directory publication. Interrupted scratch is discarded on open.
    new DataView(this.#header.buffer).setUint32(0, bytes.length, true);
    writeAll(this.index, this.#header, at);
  }
  async reset(): Promise<void> {
    this.guard();
    this.file.truncate(0);
    this.index.truncate(0);
  }
}
