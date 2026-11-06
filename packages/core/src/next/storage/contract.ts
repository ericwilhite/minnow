/**
 * The durable store contract: what the engine owner needs from OPFS, IndexedDB, or memory, and
 * nothing more. Bytes in, bytes out; the store knows no table, page format or SQL.
 *
 * ## Ownership comes first (`StorageBackend.acquire`)
 * Exactly one owner may mutate a database. Acquisition is separate from opening and has two
 * layers, both owned by the backend (the fence is pluggable):
 * 1. **Web Lock** `minnow:<db>` (FIFO; `steal` takes it from a live holder).
 * 2. **Backend fence**:
 *    - `handle` (OPFS): opening the store's files with sync access handles. While one owner holds
 *      them no other context can open, write, remove or move them, even after stealing the lock.
 *      The lock frees 2 ms–2 s before the handles, so acquisition retries the open with backoff;
 *      `NoModificationAllowedError` (Chromium, Firefox) and `InvalidStateError` (WebKit) mean busy
 *      (`isBusyError`). An owner whose lock is stolen (AbortError) closes its handles at once.
 *    - `epoch` (IndexedDB, which has no exclusive handle): acquisition increments a durable epoch
 *      record; every readwrite transaction reads it first and aborts if it moved.
 * If the fence is still held when `timeoutMs` passes, acquisition fails with
 * `OwnerUnresponsiveError` (a frozen or hung owner: the caller reports it and retries later).
 * A lock steal never lets a second owner write while the first still holds the handle fence.
 *
 * ## Objects
 * - **Root.** One small byte string (the manifest root). `publish` replaces it atomically.
 * - **Data space.** One long-lived byte space (OPFS: one file; IndexedDB: chunk records) in which
 *   the engine allocates extents (`DATA_ALIGN`-aligned). Space is reused in place: the engine
 *   frees an extent only when neither the current nor the previous durable root reaches it and no
 *   pin does (format/manifest.ts "Reclaim": two generations; extents.ts) and writes new extents there. Files are never created or deleted per object: browsers do not
 *   return quota on truncate (Chromium, WebKit), and WebKit does not let IndexedDB reuse space
 *   freed by deleting OPFS files.
 * - **Log.** An append-only byte sequence with two fixed header slots. The write path appends
 *   commit frames and flushes once per commit group.
 * - **Scratch** (`ScratchSpace`). The spill service's byte space: never flushed, never named by a
 *   root, emptied on open. OPFS: the `spill` file. IndexedDB: a dedicated `spill` object store.
 *
 * ## Startup order (lead rulings R13/R16.6; the ownership track drives it)
 * 1. `acquire` the fence (Web Lock, then handles or epoch).
 * 2. `open` the store session.
 * 3. Open the extent space (`extents.ts` `OpenExtentSpace`): read the current and previous roots
 *    from the root slots, walk both, build the free map, start the cache. It reads no log.
 * 4. Open the write path over the extent space and the session. It does the **single** log scan:
 *    validate the acknowledged boundary, truncate an invalid tail, replay (`publishedRows` staging
 *    included), restore the ledger (format/replay.ts).
 * 5. Sweep the scratch space (`SpillService.sweep`).
 * 6. Ready: serve requests.
 * Each step needs the ones before it. Nothing writes the data space before step 3 has built the
 * free map, and nothing appends to the log before step 4 has truncated it.
 *
 * ## Calls
 * Every mutating call is async and checks ownership in the same synchronous step that applies
 * its effect, so an owner that resumes after losing ownership changes nothing. Every call must be
 * implementable either as synchronous work on open sync handles, or as staging in memory plus one
 * IndexedDB transaction whose requests are all issued from its own callbacks (no await on non-IDB
 * work inside a transaction). No call holds a transaction across calls. File handles are opened
 * at acquisition, never on the hot path (WebKit delays a busy tab's worker's async OPFS calls;
 * reads and writes on an open handle are not delayed).
 *
 * ## Guarantees ("durable" = survives a crash, tab kill or power loss, given an honest flush)
 * 1. **Publish is atomic and durable.** When `publish(root)` resolves, every data write made
 *    before it, and then the root, are durable, in that order. A crash before it resolves leaves
 *    the previous root or the new one; `readRoot` never returns a torn or mixed root.
 * 2. **Data reads are exact.** A read of written bytes returns them; a read past the data length
 *    throws `StoreCorruptionError` (no short reads). Unwritten space inside the length reads as
 *    unspecified bytes.
 * 3. **Log flush is a barrier.** When `flushLog` resolves, every byte appended and every header
 *    slot written before the call is durable. One flush costs one platform flush (3.4–4.7 ms on
 *    every engine measured), which is why commits are grouped.
 * 4. **Unflushed bytes are undetermined after a crash.** Each log or data byte written after the
 *    last barrier covering it may read back new, previous, or zero; writes may land in any
 *    subset (torn and reordered). The observed log length is at least the flushed length. An
 *    unflushed header slot may read back old, new or torn. Checksums detect all of this.
 * 5. **Release is a hint.** `releaseData` tells the store the bytes are free; it may return
 *    space (truncating a free tail) or do nothing. Released bytes read as unspecified.
 * 6. **Truncate and reset are durable on resolve.** A crash during either may leave any state
 *    between the old log and the result.
 * 7. **Fencing.** Once a newer owner has acquired, every mutating call of an older session
 *    rejects with `OwnershipLostError` and changes nothing. Its reads may reject the same way
 *    (OPFS closes its handles; IndexedDB checks the epoch in every read transaction), and are never
 *    answered from a store the newer owner has rearranged.
 *
 * With an honest flush every acknowledged commit survives. With a lying flush (a write-back cache
 * lost on power failure) a cleanly truncated log loses at most the newest group, and that loss is
 * detected; an in-order cache loss can silently drop a run of recent acknowledged groups, which no
 * on-device format can detect. The log never reorders or invents a group, and a rolled-back root is
 * detected (format/log-frame.ts).
 *
 * ## Engine obligations (the store cannot check them)
 * - Write data only into free space: never over an extent the current or previous durable root, or
 *   a pinned root, reaches.
 * - Never `resetLog` or `truncateLog` below a byte the durable root still needs.
 *
 * ## Who calls what
 * - The data space (`writeData`, `readData`, `readDataInto`, `releaseData`, `publish`) has one
 *   caller: the extent space (`extents.ts`), which allocates, caches, reclaims and publishes. No
 *   other module writes data or publishes a root directly.
 * - The log calls belong to the write path (group commit, recovery).
 *
 * ## Implementations
 * - `file-store.ts`: the store over four long-lived files (log, data, two root slots; OPFS adds
 *   `spill` and `epoch`) through a
 *   `SyncFile` interface with the shape of `FileSystemSyncAccessHandle`. OPFS supplies real
 *   handles (`opfs.ts`); `sim-disk.ts` supplies simulated ones with a physical crash model.
 * - `indexeddb.ts`: the same contract on IndexedDB (Safari and Firefox private browsing), with the
 *   epoch fence checked inside every read-write transaction, ArrayBuffer values (never Blobs) and
 *   byte records keyed by offset; over the platform's IndexedDB in browsers, over `sim-idb.ts` (a
 *   write-ahead log on the simulated disk, same crash model) in the storage kit.
 * - `memory-store.ts`: the file store over in-memory files (in-memory databases, the M1 suites).
 * - `select.ts` (`chooseBackend`): OPFS where sync handles work, else IndexedDB, else memory.
 * Every implementation runs `storage/conformance.ts`.
 */

/** Data-space extents start at multiples of this. */
export const DATA_ALIGN = 4096;
/** Size of each log header slot. A slot write is at most this many bytes. */
export const LOG_HEADER_SLOT_BYTES = 512;

/**
 * The subset of `FileSystemSyncAccessHandle` the stores use. Every method is synchronous (Safari
 * 16.4+), so a call on an open handle is never delayed by its page's main thread (WebKit bug
 * 326345 delays only the async OPFS calls).
 */
export interface SyncFile {
  read(buffer: Uint8Array, options: { at: number }): number;
  write(buffer: Uint8Array, options: { at: number }): number;
  truncate(size: number): void;
  getSize(): number;
  flush(): void;
  close(): void;
}

/**
 * The spill service's byte space. Nothing in it is durable or recovered: writes are never flushed,
 * and a crash leaves garbage that the next owner's sweep (`truncate(0)`) discards. Every call
 * checks ownership like a store mutation (`OwnershipLostError`), so a fenced owner's late spill
 * write can never land in its successor's scratch space. Quota is `StoreQuotaError`.
 * - OPFS: synchronous calls on the open `spill` sync handle.
 * - IndexedDB: records in the `spill` object store keyed by extent offset (ArrayBuffers), one
 *   transaction per call with the epoch check inside it.
 */
export interface ScratchSpace {
  /** Writes `bytes` at `offset` (a multiple of `DATA_ALIGN`). */
  write(offset: number, bytes: Uint8Array): Promise<void>;
  /** Reads exactly `target.length` bytes written at `offset`; `StoreCorruptionError` past the end. */
  readInto(offset: number, target: Uint8Array): Promise<void>;
  /** Space the spill service freed (IndexedDB deletes the records; OPFS returns a free tail). */
  release(ranges: readonly DataRange[]): Promise<void>;
  /** Drops everything at or past `size`. The sweep on open is `truncate(0)`. */
  truncate(size: number): Promise<void>;
}

export interface DataRange {
  readonly offset: number;
  readonly length: number;
}

export interface AcquireOptions {
  /** Take the Web Lock from a live holder. The handle fence still waits for it to let go. */
  readonly steal?: boolean;
  /** Give up with `OwnerUnresponsiveError` after this long. Default 5,000 ms. */
  readonly timeoutMs?: number;
}

export interface OwnerLease {
  /** Strictly increasing across acquisitions of one database. */
  readonly epoch: number;
  /** Aborted when ownership is lost (lock stolen, epoch moved). The session refuses mutations. */
  readonly signal: AbortSignal;
  /** Closes handles and releases the lock. Idempotent. */
  release(): Promise<void>;
}

export interface StorageBackend {
  readonly fence: "handle" | "epoch";
  acquire(options?: AcquireOptions): Promise<OwnerLease>;
  /** A session over the store, valid while `lease` is held. */
  open(lease: OwnerLease): Promise<StoreSession>;
  /**
   * The spill service's scratch space under this lease (OPFS: the `spill` file, opened under the
   * same fence at acquisition; IndexedDB: the `spill` object store, epoch-checked in every
   * transaction), or null for the in-memory backend. Its contents mean nothing across leases.
   */
  scratch(lease: OwnerLease): ScratchSpace | null;
}

export interface StoreSession {
  readonly epoch: number;

  /** The durable root, or null for a new store. */
  readRoot(): Promise<Uint8Array | null>;
  /**
   * The previous durable root (the one the current replaced): the other root slot's root when that
   * slot is valid and older, else null (a new store, one publish only, or a slot torn by an
   * interrupted publish). A reopen falls back to it if the current root's slot is lost, so startup
   * walks both and the extent space keeps the extents either reaches until the next publish is
   * durable (format/manifest.ts "Reclaim": two generations).
   */
  readPreviousRoot(): Promise<Uint8Array | null>;
  /** Guarantee 1. */
  publish(root: Uint8Array): Promise<void>;

  /**
   * Writes bytes at `offset` (a multiple of `DATA_ALIGN`). Durable at the next publish. The caller
   * may reuse `bytes` once the call resolves (the extent space writes from one reused slab).
   */
  writeData(offset: number, bytes: Uint8Array): Promise<void>;
  /**
   * Reads byte ranges. Adjacent ranges may be coalesced into one platform read. Results are in
   * request order, may share one ArrayBuffer, and must not be mutated or transferred (copy first).
   */
  readData(ranges: readonly DataRange[]): Promise<Uint8Array[]>;
  /**
   * Reads `target.length` bytes at `offset` into `target` (one platform read on OPFS). The page
   * cache reads coalesced slabs into one reused buffer this way. Throws `StoreCorruptionError` past
   * the data length.
   */
  readDataInto(offset: number, target: Uint8Array): Promise<void>;
  /** Guarantee 5. */
  releaseData(ranges: readonly DataRange[]): Promise<void>;
  /** Bytes in the data space (the allocator's high-water mark on open). */
  dataLength(): Promise<number>;

  /** Body length in bytes (excluding header slots), as this session sees it. */
  logLength(): number;
  /** Reads `length` body bytes at `offset`; throws `StoreCorruptionError` past the end. */
  readLog(offset: number, length: number): Promise<Uint8Array>;
  /** Both header slots as stored (zeros or null for a slot never written since the last reset). */
  readLogHeaders(): Promise<readonly [Uint8Array | null, Uint8Array | null]>;
  /** Appends at the end of the body and returns the offset. Not durable until `flushLog`. */
  appendLog(bytes: Uint8Array): Promise<number>;
  /** Replaces one header slot (≤ LOG_HEADER_SLOT_BYTES). Not durable until `flushLog`. */
  writeLogHeader(slot: 0 | 1, bytes: Uint8Array): Promise<void>;
  /** Guarantee 3. */
  flushLog(): Promise<void>;
  /** Cuts the body to `length` bytes, durably. Recovery uses it to drop an invalid tail. */
  truncateLog(length: number): Promise<void>;
  /** Empties the body and both header slots, durably. */
  resetLog(): Promise<void>;

  /** Ends the session (the lease keeps the handles until released). */
  close(): Promise<void>;
}

/** Error names a sync-handle open throws while another context holds the file (probe 2026-11-05). */
export function isBusyError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "name" in error &&
    (error.name === "NoModificationAllowedError" || error.name === "InvalidStateError")
  );
}

/** A mutation from a session whose ownership has been lost (stolen lock or moved epoch). */
export class OwnershipLostError extends Error {
  readonly code = "MINNOW_OWNERSHIP_LOST";
  constructor(epoch: number) {
    super(`Store ownership of epoch ${String(epoch)} was lost`);
    this.name = "OwnershipLostError";
  }
}

/** Defined once in next/errors.ts: storage acquisition and the owner/client protocol share it. */
export { OwnerUnresponsiveError } from "../errors.js";

/** Stored bytes that cannot be what was written: out-of-range reads, failed checksums. */
export class StoreCorruptionError extends Error {
  readonly code = "MINNOW_STORE_CORRUPT";
  constructor(message: string) {
    super(message);
    this.name = "StoreCorruptionError";
  }
}

export class StoreClosedError extends Error {
  readonly code = "MINNOW_STORE_CLOSED";
  constructor() {
    super("Store session is closed");
    this.name = "StoreClosedError";
  }
}

/**
 * The platform refused space. Implementations map every quota signal to it by error name:
 * `QuotaExceededError` everywhere, and `InvalidStateError` from a sync-handle *write* on WebKit
 * when the disk is full (bug 327929).
 */
export class StoreQuotaError extends Error {
  readonly code = "MINNOW_STORE_QUOTA";
  constructor(message: string) {
    super(message);
    this.name = "StoreQuotaError";
  }
}
