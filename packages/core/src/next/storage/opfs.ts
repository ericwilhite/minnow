/**
 * The OPFS backend (`createOpfsBackend`): the file store (file-store.ts) over real
 * `FileSystemSyncAccessHandle`s, fenced by them.
 *
 * ## Files
 * One directory per database, holding six long-lived files, each with one sync handle opened at
 * acquisition and kept open until the lease is released:
 * - `log`, `data`, `root-0`, `root-1` (file-store.ts);
 * - `spill`: the scratch file the spill service uses (`StorageBackend.scratch`);
 * - `epoch`: 8 bytes, the last acquisition's epoch. Each acquisition reads it, adds one and
 *   writes it back, so epochs increase across acquisitions (`OwnerLease.epoch`). It is not
 *   flushed: a crash can only lose the epoch of an owner that died, which no live owner holds.
 * No file is created or removed after the first acquisition: Chromium and WebKit keep quota
 * charged after a truncate (300 MiB written then truncated still counts 384 MiB), WebKit fails
 * opens after ~252 handles per origin (bug 293955), and every
 * `createSyncAccessHandle` is async, costs 0.2–12 ms, and waits behind a busy page on WebKit.
 *
 * ## Acquisition (`storage/fence.ts` `acquireFenced`)
 * 1. The Web Lock `minnow:<db>` (`navigator.locks`, adapted to `LockManagerLike`).
 * 2. Every file's sync handle, opened in a fixed order with bounded retry: the lock frees
 *    2 ms–2 s before a dead owner's handles. Busy is `NoModificationAllowedError` (Chromium,
 *    Firefox) or `InvalidStateError` (WebKit). Handles opened before a busy one are closed before
 *    the retry. `mode` is not passed: Firefox and WebKit ignore it and Chromium's non-default
 *    modes do not exclude.
 * 3. On lock loss (`AbortError` on the held request) the lease closes every handle at once: the
 *    handle, not the lock, is what stops a hung owner.
 *
 * ## Errors
 * - `QuotaExceededError` from any write or truncate, and `InvalidStateError` from a *write* whose
 *   ownership guard passed (WebKit's disk-full, bug 327929), become `StoreQuotaError`. Matched by
 *   name, never by class (the subclass with `quota`/`requested` ships only in Chrome 138+).
 * - A short read inside the file is `StoreCorruptionError`.
 *
 * ## Not here
 * Page lifecycle (`pagehide` terminate, `freeze` step-down) belongs to the ownership track. The
 * engine never touches OPFS outside these handles, and clients never open these files: any
 * other opener would block the owner.
 */
import {
  OwnershipLostError,
  StoreQuotaError,
  type AcquireOptions,
  type OwnerLease,
  type ScratchSpace,
  type StorageBackend,
  type StoreSession,
  type SyncFile,
} from "./contract.js";
import { acquireFenced, type HeldLock, type LockManagerLike } from "./fence.js";
import { FileScratch, FileStoreSession, type FileSet } from "./file-store.js";

/** `FileSystemFileHandle`, as far as the backend uses it. */
export interface OpfsFileHandleLike {
  createSyncAccessHandle(): Promise<SyncFile>;
}

/** `FileSystemDirectoryHandle`, as far as the backend uses it. */
export interface OpfsDirectoryLike {
  getFileHandle(name: string, options?: { create?: boolean }): Promise<OpfsFileHandleLike>;
}

export interface OpfsBackendOptions {
  /** The database's own directory (under `navigator.storage.getDirectory()`). */
  readonly directory: OpfsDirectoryLike;
  /** The database name; the lock is `minnow:<name>`. */
  readonly name: string;
  readonly locks: LockManagerLike;
}

/** The files of one database directory, in acquisition order. */
export const OPFS_FILE_NAMES = ["log", "data", "root-0", "root-1", "spill", "epoch"] as const;

const errorName = (error: unknown): string =>
  typeof error === "object" && error !== null && "name" in error ? String(error.name) : "";

/**
 * A sync handle whose quota signals become `StoreQuotaError`: `QuotaExceededError` from a write
 * or a truncate, and `InvalidStateError` from a write while the lease still holds (WebKit's
 * disk-full, bug 327929). After the lease is lost, `InvalidStateError` means a closed handle and
 * passes through (the session's guard reports `OwnershipLostError` first anyway).
 */
class QuotaMappedFile implements SyncFile {
  readonly #handle: SyncFile;
  readonly #lost: () => boolean;

  constructor(handle: SyncFile, lost: () => boolean) {
    this.#handle = handle;
    this.#lost = lost;
  }

  read(buffer: Uint8Array, options: { at: number }): number {
    return this.#handle.read(buffer, options);
  }

  write(buffer: Uint8Array, options: { at: number }): number {
    try {
      return this.#handle.write(buffer, options);
    } catch (error) {
      const name = errorName(error);
      if (name === "QuotaExceededError" || (name === "InvalidStateError" && !this.#lost()))
        throw new StoreQuotaError(`OPFS refused a ${String(buffer.length)}-byte write (${name})`);
      throw error;
    }
  }

  truncate(size: number): void {
    try {
      this.#handle.truncate(size);
    } catch (error) {
      if (errorName(error) === "QuotaExceededError")
        throw new StoreQuotaError(`OPFS refused to grow a file to ${String(size)} bytes`);
      throw error;
    }
  }

  getSize(): number {
    return this.#handle.getSize();
  }

  flush(): void {
    this.#handle.flush();
  }

  close(): void {
    this.#handle.close();
  }
}

interface OpenFiles {
  readonly files: FileSet;
  readonly scratch: SyncFile;
  readonly epoch: number;
  readonly handles: readonly SyncFile[];
}

class OpfsLease implements OwnerLease {
  readonly epoch: number;
  readonly files: FileSet;
  readonly scratchFile: SyncFile;
  readonly #handles: readonly SyncFile[];
  readonly #lock: HeldLock;
  readonly #lost = new AbortController();

  constructor(lock: HeldLock, opened: OpenFiles) {
    this.#lock = lock;
    this.#handles = opened.handles;
    this.epoch = opened.epoch;
    const lost = () => this.#lost.signal.aborted;
    const wrap = (file: SyncFile) => new QuotaMappedFile(file, lost);
    this.files = {
      log: wrap(opened.files.log),
      data: wrap(opened.files.data),
      roots: [wrap(opened.files.roots[0]), wrap(opened.files.roots[1])],
    };
    this.scratchFile = wrap(opened.scratch);
    // The handle, not the lock, stops a hung owner: close every handle the moment the lock goes.
    if (lock.lost.aborted) this.#lose();
    else
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
    for (const handle of this.#handles) {
      try {
        handle.close();
      } catch {
        // A handle the platform already closed.
      }
    }
  }

  async release(): Promise<void> {
    this.#lose();
    this.#lock.release();
  }
}

/** Opens every file's handle in order; on any failure closes the ones it opened, then throws. */
async function openFiles(directory: OpfsDirectoryLike): Promise<OpenFiles> {
  const handles: SyncFile[] = [];
  try {
    for (const name of OPFS_FILE_NAMES) {
      const file = await directory.getFileHandle(name, { create: true });
      handles.push(await file.createSyncAccessHandle());
    }
  } catch (error) {
    for (const handle of handles) handle.close();
    throw error;
  }
  const [log, data, root0, root1, scratch, epochFile] = handles as [
    SyncFile,
    SyncFile,
    SyncFile,
    SyncFile,
    SyncFile,
    SyncFile,
  ];
  const stored = new Uint8Array(8);
  const read = epochFile.getSize() >= 8 ? epochFile.read(stored, { at: 0 }) : 0;
  const view = new DataView(stored.buffer);
  const last = read === 8 ? view.getUint32(0, true) + view.getUint32(4, true) * 2 ** 32 : 0;
  const epoch = last + 1;
  view.setUint32(0, epoch >>> 0, true);
  view.setUint32(4, Math.floor(epoch / 2 ** 32), true);
  epochFile.write(stored, { at: 0 });
  return { files: { log, data, roots: [root0, root1] }, scratch, epoch, handles };
}

/** The OPFS `StorageBackend`: handle fence, scratch file, quota errors mapped by name. */
export function createOpfsBackend(options: OpfsBackendOptions): StorageBackend {
  const leases = new WeakSet<OpfsLease>();
  const own = (lease: OwnerLease): OpfsLease => {
    if (!(lease instanceof OpfsLease) || !leases.has(lease))
      throw new TypeError("The lease was not acquired from this backend");
    return lease;
  };
  return {
    fence: "handle",
    async acquire(acquire: AcquireOptions = {}): Promise<OwnerLease> {
      const { lock, fence } = await acquireFenced(
        options.locks,
        `minnow:${options.name}`,
        () => openFiles(options.directory),
        (opened) => {
          for (const handle of opened.handles) handle.close();
        },
        acquire,
      );
      const lease = new OpfsLease(lock, fence);
      leases.add(lease);
      return lease;
    },
    async open(lease: OwnerLease): Promise<StoreSession> {
      const owned = own(lease);
      owned.guard();
      return new FileStoreSession(owned.files, owned.epoch, () => {
        owned.guard();
      });
    },
    scratch(lease: OwnerLease): ScratchSpace {
      const owned = own(lease);
      return new FileScratch(owned.scratchFile, () => {
        owned.guard();
      });
    },
  };
}

/** The slice of `navigator.locks` the adapter uses. */
export interface WebLocksLike {
  request(
    name: string,
    options: { steal?: boolean; signal?: AbortSignal },
    callback: (lock: unknown) => Promise<void>,
  ): Promise<unknown>;
}

/**
 * `navigator.locks` as a `LockManagerLike`. The lock is held while the request's callback
 * promise is pending; `release` settles it. A steal makes the holder's request reject with
 * `AbortError` after the grant, which aborts `lost`. (`steal` and `signal` cannot be combined in
 * the platform API, so a stealing request is not cancellable; it is granted at once anyway.)
 */
export function webLocks(locks: WebLocksLike): LockManagerLike {
  return {
    request(name, { steal, signal }) {
      return new Promise<HeldLock>((resolve, reject) => {
        const lost = new AbortController();
        let granted = false;
        let release!: () => void;
        const held = new Promise<void>((done) => {
          release = done;
        });
        locks
          .request(
            name,
            steal === true ? { steal: true } : signal === undefined ? {} : { signal },
            () => {
              granted = true;
              resolve({ lost: lost.signal, release });
              return held;
            },
          )
          .catch((error: unknown) => {
            if (granted) lost.abort(error);
            else reject(error instanceof Error ? error : new Error(String(error)));
          });
      });
    },
  };
}
