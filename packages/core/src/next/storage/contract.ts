/**
 * The durable store contract: what the engine owner needs from OPFS, IndexedDB, or memory, and
 * nothing more. Bytes in, bytes out; the store knows no table, page format or SQL.
 *
 * ## Objects
 * - **Packs.** Immutable byte blobs, written once through a `PackWriter` and named by a `PackId`
 *   the engine allocates. A column group, a key-index run or a catalog blob is one pack (or part
 *   of one). Reads are byte ranges of packs.
 * - **Root.** One small byte string (the manifest root). `publish` replaces it atomically.
 * - **Log.** An append-only byte sequence with two fixed header slots. The write path appends
 *   commit frames and flushes once per commit group.
 *
 * ## Guarantees ("durable" = survives a crash, power loss, tab kill or browser restart, given an
 * honest platform flush)
 * 1. **Publish is atomic and durable.** When `publish(root, packs)` resolves, the packs and then
 *    the root are durable, in that order: no crash can expose a root whose packs are incomplete.
 *    A crash before it resolves leaves either the previous root or the new one; `readRoot`
 *    never returns a torn or mixed root.
 * 2. **Packs are immutable.** A read of a range inside a pack referenced by the durable root
 *    returns exactly the bytes written. A read outside the pack's length throws
 *    `StoreCorruptionError`; there are no short reads.
 * 3. **Log flush is a barrier.** When `flushLog` resolves, every byte appended and every header
 *    slot written before the call is durable. One flush costs one platform flush.
 * 4. **Unflushed log bytes are undetermined after a crash.** Each byte appended after the last
 *    completed flush may read back as the new value, the previous value, or zero, independently
 *    (torn and reordered writes). The observed body length is at least the flushed length. An
 *    unflushed header slot may read back old, new or torn. The format detects all of this with
 *    checksums (format/log-frame.ts); the store does not.
 * 5. **Reclaim is idempotent and may be lost.** A pack reclaimed just before a crash may reappear
 *    after it. The engine sweeps unreferenced packs on open (reachability from the root).
 * 6. **Truncate and reset are durable on resolve.** A crash during either may leave any state
 *    between the old log and the result.
 * 7. **Epoch fencing.** `open` increments a durable epoch. Once a newer session has opened, every
 *    mutating call of an older session rejects with `StaleEpochError` and changes nothing. Reads
 *    of an older session may still succeed. (OPFS: the exclusive sync access handle already
 *    fences; IndexedDB: checked inside each readwrite transaction.)
 *
 * ## Engine obligations (the store cannot check them)
 * - Never reuse a `PackId`; `createPack` of an existing id rejects.
 * - Reclaim a pack only after a durable root that no longer references it, and after no reader
 *   pins an older root that does.
 * - Never `resetLog` or `truncateLog` below a byte the durable root still needs (format/manifest.ts
 *   states the log rule).
 *
 * ## Implementations
 * - Memory (`memory-store.ts`): reference implementation and test double.
 * - OPFS (storage track): packs are files, written through sync access handles; the root is two
 *   checksummed slot files (`root-slot.ts`); the log is one file with header slots at its start.
 * - IndexedDB (storage track; required because Safari private browsing has no OPFS): packs are
 *   chunk records, the root is one record, the log is records keyed by offset; each mutation is
 *   one `durability: "strict"` transaction that first checks the epoch record.
 * Every implementation runs `storage/conformance.ts`.
 */

/** A pack's name: a positive integer the engine allocates and never reuses (< 2^53). */
export type PackId = number;

export interface PackRange {
  readonly pack: PackId;
  readonly offset: number;
  readonly length: number;
}

/** Size of each log header slot. A slot write is at most this many bytes. */
export const LOG_HEADER_SLOT_BYTES = 512;

export interface PackWriter {
  readonly id: PackId;
  /** Bytes appended so far (the offset the next append gets). */
  readonly length: number;
  /** Appends `bytes` and returns their offset. Not durable until a publish lists this pack. */
  append(bytes: Uint8Array): Promise<number>;
  /** Ends writing. The pack becomes durable when a `publish` lists it. */
  seal(): Promise<SealedPack>;
  /** Discards the pack (best effort; a crash may leave it as an orphan). */
  abort(): Promise<void>;
}

export interface SealedPack {
  readonly id: PackId;
  readonly length: number;
}

export interface StoreSession {
  /** This session's fencing epoch (≥ 1). */
  readonly epoch: number;

  /** The durable root, or null for a new store. */
  readRoot(): Promise<Uint8Array | null>;
  /**
   * Makes `packs` durable, then replaces the root with `root`, atomically (guarantee 1). Every
   * pack must have been sealed in this session.
   */
  publish(root: Uint8Array, packs: readonly SealedPack[]): Promise<void>;

  createPack(id: PackId): PackWriter;
  /**
   * Reads byte ranges. The store may coalesce adjacent ranges into one platform read. Results
   * are in request order, may share one underlying ArrayBuffer, and must not be mutated or
   * transferred (copy first).
   */
  read(ranges: readonly PackRange[]): Promise<Uint8Array[]>;
  /** Every pack present, sealed or orphaned, in ascending id order. */
  listPacks(): Promise<PackId[]>;
  /** Deletes packs. Idempotent; unknown ids are ignored. */
  reclaim(ids: readonly PackId[]): Promise<void>;

  /** Body length in bytes (excluding header slots), as this session sees it. */
  logLength(): number;
  /** Reads `length` body bytes at `offset`; throws `StoreCorruptionError` past the end. */
  readLog(offset: number, length: number): Promise<Uint8Array>;
  /** Both header slots as last written (null = never written since the last reset). */
  readLogHeaders(): Promise<readonly [Uint8Array | null, Uint8Array | null]>;
  /** Appends at the end of the body and returns the offset. Not durable until `flushLog`. */
  appendLog(bytes: Uint8Array): number;
  /** Replaces one header slot (≤ LOG_HEADER_SLOT_BYTES). Not durable until `flushLog`. */
  writeLogHeader(slot: 0 | 1, bytes: Uint8Array): void;
  /** Guarantee 3. */
  flushLog(): Promise<void>;
  /** Cuts the body to `length` bytes, durably. Recovery uses it to drop an invalid tail. */
  truncateLog(length: number): Promise<void>;
  /** Empties the body and both header slots, durably. */
  resetLog(): Promise<void>;

  /** Releases handles. Pending unflushed log bytes are undetermined (guarantee 4). */
  close(): void;
}

export interface DurableStore {
  /** Opens a new session, fencing every older one (guarantee 7). */
  open(): Promise<StoreSession>;
}

/** A mutation from a session that a newer session has fenced. */
export class StaleEpochError extends Error {
  readonly code = "MINNOW_STALE_EPOCH";
  constructor(epoch: number, current: number) {
    super(`Store session epoch ${String(epoch)} is fenced by epoch ${String(current)}`);
    this.name = "StaleEpochError";
  }
}

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
