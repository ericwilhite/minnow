/**
 * The in-memory backend: the file store (file-store.ts) over growable in-memory files, for
 * databases that live only as long as their owner (`MemoryBackend`), and for the end-to-end suites
 * that need a fast store. Nothing is durable across the process; within it, the files outlive
 * leases, so reacquiring sees what the last owner published.
 *
 * - Files grow by doubling (writes are amortized O(bytes)); `flush` does nothing; `truncate` keeps
 *   the capacity, so a reused data space does not reallocate.
 * - The fence is the handle fence, as on OPFS: a file has one open handle at a time, and an owner
 *   whose lock is stolen closes its handles at once (`InvalidStateError` afterwards).
 * - There is no scratch space: spill stays in memory (ruling on B1's open questions).
 */
import {
  OwnershipLostError,
  type AcquireOptions,
  type OwnerLease,
  type ScratchSpace,
  type StorageBackend,
  type StoreSession,
  type SyncFile,
} from "./contract.js";
import { acquireFenced, type HeldLock, type LockManagerLike } from "./fence.js";
import { FileStoreSession, FILE_NAMES, type FileSet } from "./file-store.js";
import { SimLocks } from "./sim-disk.js";

function namedError(name: string, message: string): Error {
  const error = new Error(message);
  error.name = name;
  return error;
}

interface Contents {
  bytes: Uint8Array;
  size: number;
  holder: MemoryFile | null;
}

export class MemoryFile implements SyncFile {
  readonly #contents: Contents;
  #closed = false;

  constructor(contents: Contents) {
    this.#contents = contents;
  }

  #live(): Contents {
    if (this.#closed) throw namedError("InvalidStateError", "The handle is closed");
    return this.#contents;
  }

  read(buffer: Uint8Array, options: { at: number }): number {
    const contents = this.#live();
    const available = Math.max(0, Math.min(buffer.length, contents.size - options.at));
    if (available > 0) buffer.set(contents.bytes.subarray(options.at, options.at + available));
    return available;
  }

  write(buffer: Uint8Array, options: { at: number }): number {
    const contents = this.#live();
    const end = options.at + buffer.length;
    this.#reserve(contents, end);
    if (options.at > contents.size) contents.bytes.fill(0, contents.size, options.at);
    contents.bytes.set(buffer, options.at);
    contents.size = Math.max(contents.size, end);
    return buffer.length;
  }

  truncate(size: number): void {
    const contents = this.#live();
    if (size > contents.size) {
      this.#reserve(contents, size);
      contents.bytes.fill(0, contents.size, size);
    }
    contents.size = size;
  }

  getSize(): number {
    return this.#live().size;
  }

  flush(): void {
    this.#live();
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#contents.holder === this) this.#contents.holder = null;
  }

  #reserve(contents: Contents, size: number): void {
    if (size <= contents.bytes.length) return;
    let capacity = Math.max(4096, contents.bytes.length);
    while (capacity < size) capacity *= 2;
    const grown = new Uint8Array(capacity);
    grown.set(contents.bytes.subarray(0, contents.size));
    contents.bytes = grown;
  }
}

class MemoryLease implements OwnerLease {
  readonly epoch: number;
  readonly files: FileSet;
  readonly #lock: HeldLock;
  readonly #lost = new AbortController();

  constructor(lock: HeldLock, files: FileSet, epoch: number) {
    this.#lock = lock;
    this.files = files;
    this.epoch = epoch;
    lock.lost.addEventListener("abort", () => {
      this.#lose();
    });
  }

  get signal(): AbortSignal {
    return this.#lost.signal;
  }

  guard(): void {
    if (this.#lost.signal.aborted) throw new OwnershipLostError(this.epoch);
  }

  #lose(): void {
    if (this.#lost.signal.aborted) return;
    this.#lost.abort(new OwnershipLostError(this.epoch));
    for (const file of [this.files.log, this.files.data, ...this.files.roots, this.files.metadata])
      file?.close();
  }

  async release(): Promise<void> {
    this.#lose();
    this.#lock.release();
  }
}

/** An in-memory `StorageBackend` (handle fence, no scratch space). */
export class MemoryBackend implements StorageBackend {
  readonly fence = "handle" as const;
  readonly locks: LockManagerLike;
  readonly name: string;
  readonly #files = new Map<string, Contents>();
  readonly #leases = new WeakSet<MemoryLease>();
  #epoch = 0;

  constructor(options: { locks?: LockManagerLike; name?: string } = {}) {
    this.locks = options.locks ?? new SimLocks();
    this.name = options.name ?? "minnow:memory";
  }

  #open(name: string): MemoryFile {
    let contents = this.#files.get(name);
    if (contents === undefined) {
      contents = { bytes: new Uint8Array(0), size: 0, holder: null };
      this.#files.set(name, contents);
    }
    if (contents.holder !== null) throw namedError("NoModificationAllowedError", `${name} is open`);
    const file = new MemoryFile(contents);
    contents.holder = file;
    return file;
  }

  #openFiles(): FileSet {
    const opened: MemoryFile[] = [];
    try {
      for (const name of [...FILE_NAMES, "space-meta"]) opened.push(this.#open(name));
    } catch (error) {
      for (const file of opened) file.close();
      throw error;
    }
    const [log, data, root0, root1, metadata] = opened as [
      MemoryFile,
      MemoryFile,
      MemoryFile,
      MemoryFile,
      MemoryFile,
    ];
    return { log, data, roots: [root0, root1], metadata };
  }

  async acquire(options: AcquireOptions = {}): Promise<OwnerLease> {
    const { lock, fence } = await acquireFenced(
      this.locks,
      this.name,
      () => this.#openFiles(),
      (files) => {
        for (const file of [files.log, files.data, ...files.roots, files.metadata]) file?.close();
      },
      options,
    );
    this.#epoch += 1;
    const lease = new MemoryLease(lock, fence, this.#epoch);
    this.#leases.add(lease);
    return lease;
  }

  async open(lease: OwnerLease): Promise<StoreSession> {
    if (!(lease instanceof MemoryLease) || !this.#leases.has(lease))
      throw new TypeError("The lease was not acquired from this backend");
    lease.guard();
    return new FileStoreSession(lease.files, lease.epoch, () => {
      lease.guard();
    });
  }

  scratch(): ScratchSpace | null {
    return null;
  }
}
