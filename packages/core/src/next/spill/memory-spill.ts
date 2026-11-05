/**
 * An in-memory spill service: the test double operators run against until the OPFS service
 * exists. It checks checksums on read like the real one, so corruption tests can flip bytes.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import {
  SpillCorruptionError,
  type SpillFile,
  type SpillPageRef,
  type SpillService,
} from "./contract.js";

export class MemorySpillService implements SpillService {
  readonly #files = new Set<MemorySpillFile>();

  get bytesInUse(): number {
    let bytes = 0;
    for (const file of this.#files) bytes += file.length;
    return bytes;
  }

  /** Pages are kept in memory, so no batching buffer is charged to a budget. */
  async create(queryId: number): Promise<SpillFile> {
    const file = new MemorySpillFile(queryId, () => this.#files.delete(file));
    this.#files.add(file);
    return file;
  }

  async sweep(): Promise<void> {
    for (const file of [...this.#files]) await file.close();
  }
}

export class MemorySpillFile implements SpillFile {
  readonly queryId: number;
  readonly #onClose: () => void;
  readonly #pages: Uint8Array[] = [];
  readonly #offsets: number[] = [];
  #length = 0;
  #closed = false;

  constructor(queryId: number, onClose: () => void) {
    this.queryId = queryId;
    this.#onClose = onClose;
  }

  get length(): number {
    return this.#length;
  }

  async append(page: Uint8Array): Promise<SpillPageRef> {
    if (this.#closed) throw new TypeError("Spill file is closed");
    const ref = { offset: this.#length, length: page.length, checksum: xxHash32(page) };
    this.#pages.push(page.slice());
    this.#offsets.push(this.#length);
    this.#length += page.length;
    return ref;
  }

  async read(ref: SpillPageRef): Promise<Uint8Array> {
    if (this.#closed) throw new TypeError("Spill file is closed");
    const index = this.#offsets.indexOf(ref.offset);
    const page = this.#pages[index];
    if (page?.length !== ref.length || xxHash32(page) !== ref.checksum)
      throw new SpillCorruptionError(`Spill page at ${String(ref.offset)} failed verification`);
    return page;
  }

  async readInto(ref: SpillPageRef, target: Uint8Array): Promise<void> {
    if (target.length !== ref.length) throw new RangeError("Spill read target has the wrong size");
    target.set(await this.read(ref));
  }

  /** Test seam: flips one byte of a stored page. */
  corrupt(ref: SpillPageRef, byte: number): void {
    const page = this.#pages[this.#offsets.indexOf(ref.offset)];
    if (page !== undefined) page[byte] = (page[byte] ?? 0) ^ 0xff;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#pages.length = 0;
    this.#length = 0;
    this.#onClose();
  }
}
