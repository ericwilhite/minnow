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
 * - `epoch`: the epoch counter, two checksummed identity slots (`writeIdentity`). Each acquisition writes the
 *   next value into the other slot and flushes it before the lease exists, so epochs increase
 *   across acquisitions and crashes (`OwnerLease.epoch`); the flush costs one platform flush per
 *   acquisition, never per commit.
 * No file is created or removed after the first acquisition: Chromium and WebKit keep quota
 * charged after a truncate (300 MiB written then truncated still counts 384 MiB), WebKit fails
 * opens after ~252 handles per origin (bug 293955), and every
 * `createSyncAccessHandle` is async, costs 0.2–12 ms, and waits behind a busy page on WebKit.
 *
 * ## Acquisition (`storage/fence.ts` `acquireFenced`)
 * 1. The identity-specific Web Lock (`owner-identity.ts`) (`navigator.locks`, adapted to `LockManagerLike`).
 * 2. Every file's sync handle, opened in a fixed order with bounded retry: the lock frees
 *    2 ms–2 s before a dead owner's handles. Read-only root/log/epoch snapshots reject
 *    unsupported headers before file creation; the root minimum-reader barrier is rechecked
 *    under the handles before the epoch advances. Busy is `NoModificationAllowedError` (Chromium,
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
 * engine uses read-only Blob snapshots for pre-fence format checks; afterward it uses only
 * these handles. Clients never open these files: any
 * other opener would block the owner.
 */
import {
  ROOT_SLOT_OVERHEAD,
  DURABLE_READER_VERSION,
  DURABLE_WRITER_VERSION,
} from "../durable-format.js";
import { decodeRootSlot } from "./root-slot.js";
import { validLogHeaderEnvelope } from "./durable-header.js";
import {
  OwnershipLostError,
  OwnerUnresponsiveError,
  isBusyError,
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
import {
  DEFAULT_ACQUIRE_TIMEOUT_MS,
  type HeldLock,
  type LockManagerLike,
  type PublicationGate,
} from "./fence.js";
import { acquireNative, NativeAuthority } from "./native-fence.js";
import {
  EMPTY_IDENTITY,
  OWNER_IDENTITY_BYTES,
  decodeOwnerIdentity,
  encodeOwnerIdentity,
  requireIdentity,
  sameIdentity,
  identityFromNames,
  type OwnerIdentity,
} from "./owner-identity.js";
import { refuseLegacyOpfs } from "./legacy-data.js";
import { FileScratch, FileStoreSession, type FileSet } from "./file-store.js";

/** `FileSystemFileHandle`, as far as the backend uses it. */
export interface OpfsFileHandleLike {
  /** Native read-only snapshot, used before creating files in a possibly future store. */
  getFile?(): Promise<Blob>;
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
  /** The logical database name; native owner/gate lock names include its durable identity. */
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
  readonly identity: OwnerIdentity;
  readonly epochFile: SyncFile;
  readonly handles: readonly SyncFile[];
}

class OpfsLease implements OwnerLease {
  readonly epoch: number;
  readonly files: FileSet;
  readonly scratchFile: SyncFile;
  readonly #handles: readonly SyncFile[];
  readonly #lock: NativeAuthority;
  readonly publication: PublicationGate;
  readonly #lost = new AbortController();

  constructor(lock: NativeAuthority, opened: OpenFiles, observe: StorageObserver | null) {
    this.#lock = lock;
    this.#handles = opened.handles;
    this.epoch = opened.identity.epoch;
    this.publication = lock.publication(
      async (expected, next) => {
        this.guard();
        writeIdentity(opened.epochFile, expected, next, false);
      },
      async (expected) => {
        this.guard();
        requireIdentity(readIdentity(opened.epochFile, false).identity, expected);
      },
    );
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
 * The native owner fence: two checksummed MNE2 records, one per512-byte sector.
 * Both identities are validated before selection. The higher generation wins; acquisition
 * advances epoch and generation, bounded publication recovery advances generation only.
 * owner-identity.ts defines the exact40-byte record, reserved bits and capability floors.
 */
export const EPOCH_SLOT_BYTES = 512;
/** Test/fixture convenience: production passes the full identity. */
export function encodeEpochSlot(value: number): Uint8Array {
  return encodeOwnerIdentity({
    epoch: value,
    generation: value,
    reader: DURABLE_READER_VERSION,
    writer: DURABLE_WRITER_VERSION,
  });
}

function readAll(file: SyncFile, at: number, length: number): Uint8Array {
  const buffer = new Uint8Array(length);
  let done = 0;
  while (done < length) {
    const read = file.read(buffer.subarray(done), { at: at + done });
    if (read <= 0)
      throw new StoreCorruptionError("Native header read made no progress inside the file");
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
function chooseIdentity(
  slots: ReadonlyArray<OwnerIdentity | null>,
  unused: boolean,
): { identity: OwnerIdentity; slot: number } {
  const [first, second] = slots;
  if (first == null && second == null) {
    if (!unused) throw new StoreCorruptionError("The epoch file holds no valid identity");
    return { identity: EMPTY_IDENTITY, slot: 0 };
  }
  if (first != null && second != null) {
    if (first.generation === second.generation && !sameIdentity(first, second))
      throw new StoreCorruptionError("Conflicting native fence identities");
    const older = first.generation < second.generation ? first : second,
      newer = older === first ? second : first;
    if (older.epoch > newer.epoch) throw new StoreCorruptionError("Native fence epoch regressed");
  }
  return first != null && (second == null || first.generation >= second.generation)
    ? { identity: first, slot: 1 }
    : { identity: second ?? EMPTY_IDENTITY, slot: 0 };
}
function readIdentity(file: SyncFile, unused: boolean): { identity: OwnerIdentity; slot: number } {
  const size = file.getSize();
  return chooseIdentity(
    [0, 1].map((slot) =>
      decodeOwnerIdentity(
        readAll(
          file,
          slot * EPOCH_SLOT_BYTES,
          Math.min(OWNER_IDENTITY_BYTES, Math.max(0, size - slot * EPOCH_SLOT_BYTES)),
        ),
      ),
    ),
    unused,
  );
}
function writeIdentity(
  file: SyncFile,
  expected: OwnerIdentity,
  next: OwnerIdentity,
  unused: boolean,
): void {
  const current = readIdentity(file, unused);
  requireIdentity(current.identity, expected);
  file.write(encodeOwnerIdentity(next), { at: current.slot * EPOCH_SLOT_BYTES });
  file.flush();
}

/** Only an unused store may recover a torn first fence as virtual identity zero. */
async function existingUnused(directory: OpfsDirectoryLike): Promise<boolean> {
  for (const name of ["log", "data", "root-0", "root-1"]) {
    let handle: OpfsFileHandleLike;
    try {
      handle = await directory.getFileHandle(name, { create: false });
    } catch (error) {
      if (errorName(error) === "NotFoundError") continue;
      throw error;
    }
    if (handle.getFile !== undefined) {
      if ((await handle.getFile()).size !== 0) return false;
    } else {
      const file = await handle.createSyncAccessHandle();
      try {
        if (file.getSize() !== 0) return false;
      } finally {
        closeAll([file]);
      }
    }
  }
  return true;
}

/** Native Blob snapshots read only bounded root/header bytes before missing files are created.
 * Payload roots stay opaque to storage; taking the owner fence still precedes semantic replay.
 */
async function checkExistingFormats(directory: OpfsDirectoryLike): Promise<void> {
  for (const name of ["root-0", "root-1", "log", "epoch"]) {
    let handle: OpfsFileHandleLike;
    try {
      handle = await directory.getFileHandle(name, { create: false });
    } catch (error) {
      if (errorName(error) === "NotFoundError") continue;
      throw error;
    }
    if (handle.getFile === undefined) continue; // Trusted raw storage-kit adapters.
    const file = await handle.getFile();
    if (name.startsWith("root-")) {
      if (file.size > (1 << 20) + ROOT_SLOT_OVERHEAD)
        throw new StoreCorruptionError("Root slot exceeds1MiB payload");
      decodeRootSlot(new Uint8Array(await file.arrayBuffer()), true);
    } else
      for (const slot of [0, 1]) {
        const start = slot * EPOCH_SLOT_BYTES;
        const bytes = new Uint8Array(
          await file
            .slice(start, start + (name === "epoch" ? OWNER_IDENTITY_BYTES : 512))
            .arrayBuffer(),
        );
        if (name === "epoch") decodeOwnerIdentity(bytes);
        else validLogHeaderEnvelope(bytes);
      }
  }
}

/**
 * Opens every file's handle in order and advances the epoch; on any failure closes every handle it
 * opened, then throws (quota failures as `StoreQuotaError`), so a retry finds the files free.
 */
async function openFiles(
  directory: OpfsDirectoryLike,
  location: string,
  expected: OwnerIdentity,
  next: OwnerIdentity,
  beforeMutation: () => void,
): Promise<OpenFiles> {
  await refuseLegacyOpfs(directory, location, OPFS_FILE_NAMES);
  const handles: SyncFile[] = [];
  try {
    const existing = new Map<string, SyncFile>();
    // Own and validate existing files BEFORE creating missing files. A departing owner may
    // have replaced a root since the Blob snapshot; a partial future tree must stay partial.
    for (const name of OPFS_FILE_NAMES) {
      let file: OpfsFileHandleLike;
      try {
        file = await directory.getFileHandle(name, { create: false });
      } catch (error) {
        if (errorName(error) === "NotFoundError") continue;
        throw error;
      }
      const handle = await file.createSyncAccessHandle();
      handles.push(handle);
      existing.set(name, handle);
      if (name.startsWith("root-")) {
        const length = handle.getSize();
        if (length > (1 << 20) + ROOT_SLOT_OVERHEAD)
          throw new StoreCorruptionError("Root slot exceeds1MiB payload");
        decodeRootSlot(readAll(handle, 0, length), true);
      } else if (name === "log" || name === "epoch")
        for (const slot of [0, 1]) {
          const start = slot * EPOCH_SLOT_BYTES;
          const bytes = readAll(
            handle,
            start,
            Math.min(
              name === "epoch" ? OWNER_IDENTITY_BYTES : 512,
              Math.max(0, handle.getSize() - start),
            ),
          );
          if (name === "epoch") decodeOwnerIdentity(bytes);
          else validLogHeaderEnvelope(bytes);
        }
    }
    const unused = ["log", "data", "root-0", "root-1"].every(
      (name) => (existing.get(name)?.getSize() ?? 0) === 0,
    );
    let epochFile = existing.get("epoch");
    beforeMutation();
    if (epochFile === undefined) {
      if (!unused) throw new StoreCorruptionError("A used store has no native fence");
      requireIdentity(EMPTY_IDENTITY, expected);
      const file = await directory.getFileHandle("epoch", { create: true });
      epochFile = await file.createSyncAccessHandle();
      handles.push(epochFile);
      existing.set("epoch", epochFile);
    }
    // The durable identity is recorded before any other file is created or changed.
    beforeMutation();
    writeIdentity(new QuotaMappedFile(epochFile, () => false), expected, next, unused);
    const ordered: SyncFile[] = [];
    for (const name of OPFS_FILE_NAMES) {
      let handle = existing.get(name);
      if (handle === undefined) {
        const file = await directory.getFileHandle(name, { create: true });
        handle = await file.createSyncAccessHandle();
        handles.push(handle);
      }
      ordered.push(handle);
    }
    const [log, data, root0, root1, scratch, , metadata, metadataIndex, spillMetadata] =
      ordered as [
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
    return {
      files: { log, data, roots: [root0, root1], metadata, metadataIndex, spillMetadata },
      scratch,
      identity: next,
      epochFile: new QuotaMappedFile(epochFile, () => false),
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
      const inspect = async (): Promise<OwnerIdentity> => {
        await refuseLegacyOpfs(options.directory, options.name, OPFS_FILE_NAMES);
        try {
          await checkExistingFormats(options.directory);
          let handle: OpfsFileHandleLike;
          try {
            handle = await options.directory.getFileHandle("epoch", { create: false });
          } catch (error) {
            if (errorName(error) === "NotFoundError") return EMPTY_IDENTITY;
            throw error;
          }
          if (handle.getFile !== undefined) {
            const blob = await handle.getFile();
            const slots = await Promise.all(
              [0, 1].map(async (slot) =>
                decodeOwnerIdentity(
                  new Uint8Array(
                    await blob
                      .slice(
                        slot * EPOCH_SLOT_BYTES,
                        slot * EPOCH_SLOT_BYTES + OWNER_IDENTITY_BYTES,
                      )
                      .arrayBuffer(),
                  ),
                ),
              ),
            );
            return chooseIdentity(slots, await existingUnused(options.directory)).identity;
          }
          const file = await handle.createSyncAccessHandle();
          try {
            return readIdentity(file, await existingUnused(options.directory)).identity;
          } finally {
            closeAll([file]);
          }
        } catch (error) {
          if (!isBusyError(error)) throw error;
          // Some engines deny even read-only Blob snapshots while a SAH is held. A names
          // snapshot supplies only the immutable target hint; owned handles still do the CAS.
          const hinted = identityFromNames(options.name, (await options.locks.query?.()) ?? []);
          if (hinted === undefined)
            throw new OwnerUnresponsiveError(acquire.timeoutMs ?? DEFAULT_ACQUIRE_TIMEOUT_MS);
          return hinted;
        }
      };
      const { authority, opened } = await acquireNative(
        options.locks,
        options.name,
        inspect,
        async (expected, next, deadline) => {
          const beforeMutation = () => {
            if (Date.now() > deadline)
              throw new OwnerUnresponsiveError(acquire.timeoutMs ?? DEFAULT_ACQUIRE_TIMEOUT_MS);
          };
          let delay = 1;
          for (;;) {
            requireIdentity(await inspect(), expected);
            beforeMutation();
            try {
              return await openFiles(
                options.directory,
                options.name,
                expected,
                next,
                beforeMutation,
              );
            } catch (error) {
              if (!isBusyError(error)) throw error;
              // A busy open can outlive the pre-fence snapshot while a healthy owner recovers.
              requireIdentity(await inspect(), expected);
              beforeMutation();
              await new Promise<void>((resolve) => {
                setTimeout(resolve, delay);
              });
              delay = Math.min(delay * 2, 100);
            }
          }
        },
        (files) => closeAll(files.handles),
        acquire,
      );
      const lease = new OpfsLease(authority, opened, options.observe ?? null);
      leases.add(lease);
      return lease;
    },
    async open(lease: OwnerLease): Promise<StoreSession> {
      const owned = own(lease);
      owned.guard();
      return new FileStoreSession(
        owned.files,
        owned.epoch,
        () => {
          owned.guard();
        },
        owned.publication,
      );
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
  query?(): Promise<{ held?: ReadonlyArray<{ name?: string }> }>;
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
    ...(locks.query === undefined
      ? {}
      : {
          query: async () =>
            (await locks.query?.())?.held?.flatMap(({ name }) =>
              name === undefined ? [] : [name],
            ) ?? [],
        }),
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
