/**
 * The OPFS backend (`createOpfsBackend`): the file store (file-store.ts) over real
 * `FileSystemSyncAccessHandle`s, fenced by them.
 *
 * ## Files
 * One directory per database, holding nine long-lived files, each with one sync handle opened at
 * acquisition and kept open until the lease is released:
 * - `log`, `data`, `root-0`, `root-1` (file-store.ts);
 * - `space-meta` and `space-meta-index`: owner-private reconstructible extent-space metadata;
 * - `spill` and `spill-meta`: scratch payload and allocator pages; `spill` is the scratch file the spill service uses (`StorageBackend.scratch`);
 * - `epoch`: the epoch counter, two checksummed slots (`nextEpoch`). Each acquisition writes the
 *   next value into the other slot and flushes it before the lease exists, so epochs increase
 *   across acquisitions and crashes (`OwnerLease.epoch`); the flush costs one platform flush per
 *   acquisition, never per commit.
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
import { xxHash32 } from "../hash/xxhash32.js";
import {
  OwnershipLostError,
  StoreCorruptionError,
  StoreQuotaError,
  type AcquireOptions,
  type OwnerLease,
  type ScratchSpace,
  type StorageBackend,
  type StoreSession,
  type StorageObserver,
  type SyncFile,
} from "./contract.js";
import { acquireFenced, type HeldLock, type LockManagerLike } from "./fence.js";
import { refuseLegacyOpfs } from "./legacy-data.js";
import { FileScratch, FileStoreSession, type FileSet } from "./file-store.js";

/** `FileSystemFileHandle`, as far as the backend uses it. */
export interface OpfsFileHandleLike {
  createSyncAccessHandle(): Promise<SyncFile>;
}

/** `FileSystemDirectoryHandle`, as far as the backend uses it. */
export interface OpfsDirectoryLike {
  /** Present on native handles: inspect before opening any writable sync handle. */
  keys?(): AsyncIterableIterator<string>;
  getFileHandle(name: string, options?: { create?: boolean }): Promise<OpfsFileHandleLike>;
}

export interface OpfsBackendOptions {
  /** The database's own directory (under `navigator.storage.getDirectory()`). */
  readonly directory: OpfsDirectoryLike;
  /** The database name; the lock is `minnow:<name>`. */
  readonly name: string;
  readonly locks: LockManagerLike;
  /** Told about every sync call the sessions make (test instrumentation). */
  readonly observe?: StorageObserver;
}

/** The files of one database directory, in acquisition order. */
export const OPFS_FILE_NAMES = [
  "log",
  "data",
  "root-0",
  "root-1",
  "spill",
  "epoch",
  "space-meta",
  "space-meta-index",
  "spill-meta",
] as const;

const errorName = (error: unknown): string =>
  typeof error === "object" && error !== null && "name" in error ? String(error.name) : "";

/**
 * A sync handle whose quota signals become `StoreQuotaError`: `QuotaExceededError` from a write
 * or a truncate, `InvalidStateError` from a write while the lease still holds (WebKit's
 * disk-full, bug 327929), and a write that reports anything but its whole length. After the
 * lease is lost, `InvalidStateError` means a closed handle and passes through (the session's
 * guard reports `OwnershipLostError` first anyway).
 *
 * The count matters: Chromium 151 does not throw for a write the quota cuts short; it writes what
 * fits and returns 2^32 − 8 (a 1 MiB write with 0.5 MiB left; probed 2026-11-05 while adding the
 * B1-p2d quota continuation). Taken at its word, that count is "everything and more", and a
 * caller would publish bytes that never landed.
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
    let written: number;
    try {
      written = this.#handle.write(buffer, options);
    } catch (error) {
      const name = errorName(error);
      if (name === "QuotaExceededError" || (name === "InvalidStateError" && !this.#lost()))
        throw new StoreQuotaError(`OPFS refused a ${String(buffer.length)}-byte write (${name})`);
      throw error;
    }
    if (written !== buffer.length)
      throw new StoreQuotaError(
        `OPFS cut a ${String(buffer.length)}-byte write short (it reported ${String(written)})`,
      );
    return written;
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

/** A sync handle whose calls are reported to `observe` before they run and after they return. */
class ObservedFile implements SyncFile {
  readonly #handle: SyncFile;
  readonly #name: string;
  readonly #observe: StorageObserver;

  constructor(handle: SyncFile, name: string, observe: StorageObserver) {
    this.#handle = handle;
    this.#name = name;
    this.#observe = observe;
  }

  #call<T>(op: string, call: () => T): T {
    const done = this.#observe({ target: this.#name, op });
    try {
      return call();
    } finally {
      done();
    }
  }

  read(buffer: Uint8Array, options: { at: number }): number {
    return this.#call("read", () => this.#handle.read(buffer, options));
  }

  write(buffer: Uint8Array, options: { at: number }): number {
    return this.#call("write", () => this.#handle.write(buffer, options));
  }

  truncate(size: number): void {
    this.#call("truncate", () => {
      this.#handle.truncate(size);
    });
  }

  getSize(): number {
    return this.#handle.getSize();
  }

  flush(): void {
    this.#call("flush", () => {
      this.#handle.flush();
    });
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

  constructor(lock: HeldLock, opened: OpenFiles, observe: StorageObserver | null) {
    this.#lock = lock;
    this.#handles = opened.handles;
    this.epoch = opened.epoch;
    const lost = () => this.#lost.signal.aborted;
    const wrap = (file: SyncFile, name: string): SyncFile => {
      const mapped = new QuotaMappedFile(file, lost);
      return observe === null ? mapped : new ObservedFile(mapped, name, observe);
    };
    this.files = {
      log: wrap(opened.files.log, "log"),
      data: wrap(opened.files.data, "data"),
      roots: [wrap(opened.files.roots[0], "root-0"), wrap(opened.files.roots[1], "root-1")],
      ...(opened.files.metadata === undefined
        ? {}
        : { metadata: wrap(opened.files.metadata, "space-meta") }),
      ...(opened.files.metadataIndex === undefined
        ? {}
        : { metadataIndex: wrap(opened.files.metadataIndex, "space-meta-index") }),
      ...(opened.files.spillMetadata === undefined
        ? {}
        : { spillMetadata: wrap(opened.files.spillMetadata, "spill-meta") }),
    };
    this.scratchFile = wrap(opened.scratch, "spill");
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
    closeAll(this.#handles);
  }

  async release(): Promise<void> {
    this.#lose();
    this.#lock.release();
  }
}

/** Closes every handle, even when some close throws (a handle the platform already closed). */
function closeAll(handles: readonly SyncFile[]): void {
  for (const handle of handles) {
    try {
      handle.close();
    } catch {
      // Keep closing the rest: one leaked handle blocks every later owner.
    }
  }
}

/**
 * The epoch counter (review B1-p2b finding 2): two checksummed slots in the `epoch` file, one per
 * 512-byte sector so a torn write damages only the slot being written, chosen like root slots
 * (the valid slot with the higher value wins; the next write goes to the other). Acquisition
 * writes value + 1 into the other slot and flushes before the lease exists, so no epoch handed to a
 * live owner can repeat after a crash; an interrupted acquisition leaves the old value, which no
 * live owner holds. Slot: magic u32 "MNEP" | value u32 lo | value u32 hi | xxHash32 of the 12.
 */
export const EPOCH_SLOT_BYTES = 512;
const EPOCH_MAGIC = 0x50454e4d; // "MNEP"

export function encodeEpochSlot(value: number): Uint8Array {
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, EPOCH_MAGIC, true);
  view.setUint32(4, value >>> 0, true);
  view.setUint32(8, Math.floor(value / 2 ** 32), true);
  view.setUint32(12, xxHash32(bytes, 0, 0, 12), true);
  return bytes;
}

/** A slot's value, or null when the slot is empty, torn or not an epoch slot. */
function decodeEpochSlot(bytes: Uint8Array): number | null {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.length < 16 || view.getUint32(0, true) !== EPOCH_MAGIC) return null;
  if (view.getUint32(12, true) !== xxHash32(bytes, 0, 0, 12)) return null;
  const value = view.getUint32(4, true) + view.getUint32(8, true) * 2 ** 32;
  return Number.isSafeInteger(value) && value >= 1 ? value : null;
}

function readAll(file: SyncFile, at: number, length: number): Uint8Array {
  const buffer = new Uint8Array(length);
  let done = 0;
  while (done < length) {
    const read = file.read(buffer.subarray(done), { at: at + done });
    if (read <= 0) break;
    done += read;
  }
  return buffer;
}

/**
 * Reads the counter, writes the next value into the other slot and flushes. No valid slot is a
 * fresh start only while the store has never been used (`unused`: every other file empty), which
 * is also the only time a torn write can leave none: once one slot is flushed, every later write
 * goes to the other. Otherwise it is `StoreCorruptionError`, never a silent restart at 1. A counter
 * that cannot increase within safe integers is refused.
 */
function nextEpoch(file: SyncFile, unused: boolean): number {
  const size = file.getSize();
  const slots = [0, 1].map((slot) =>
    decodeEpochSlot(
      readAll(
        file,
        slot * EPOCH_SLOT_BYTES,
        Math.min(16, Math.max(0, size - slot * EPOCH_SLOT_BYTES)),
      ),
    ),
  );
  const [first, second] = slots as [number | null, number | null];
  if (first === null && second === null && size > 0 && !unused)
    throw new StoreCorruptionError("The epoch file holds no valid epoch");
  const current = Math.max(first ?? 0, second ?? 0);
  const next = current + 1;
  if (!Number.isSafeInteger(next)) throw new StoreCorruptionError("The epoch counter is exhausted");
  const slot = first !== null && first === current ? 1 : 0;
  file.write(encodeEpochSlot(next), { at: slot * EPOCH_SLOT_BYTES });
  file.flush();
  return next;
}

/**
 * Opens every file's handle in order and advances the epoch; on any failure closes every handle it
 * opened, then throws (quota failures as `StoreQuotaError`), so a retry finds the files free.
 */
async function openFiles(directory: OpfsDirectoryLike, location: string): Promise<OpenFiles> {
  await refuseLegacyOpfs(directory, location, OPFS_FILE_NAMES);
  const handles: SyncFile[] = [];
  try {
    for (const name of OPFS_FILE_NAMES) {
      const file = await directory.getFileHandle(name, { create: true });
      handles.push(await file.createSyncAccessHandle());
    }
    const [log, data, root0, root1, scratch, epochFile, metadata, metadataIndex, spillMetadata] =
      handles as [
        SyncFile,
        SyncFile,
        SyncFile,
        SyncFile,
        SyncFile,
        SyncFile,
        SyncFile,
        SyncFile,
        SyncFile,
      ];
    const unused = [log, data, root0, root1].every((file) => file.getSize() === 0);
    const epoch = nextEpoch(new QuotaMappedFile(epochFile, () => false), unused);
    return {
      files: { log, data, roots: [root0, root1], metadata, metadataIndex, spillMetadata },
      scratch,
      epoch,
      handles,
    };
  } catch (error) {
    closeAll(handles);
    throw error;
  }
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
        () => openFiles(options.directory, options.name),
        (opened) => {
          closeAll(opened.handles);
        },
        acquire,
      );
      const lease = new OpfsLease(lock, fence, options.observe ?? null);
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
      return new FileScratch(
        owned.scratchFile,
        () => {
          owned.guard();
        },
        owned.files.spillMetadata,
      );
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
