/**
 * The IndexedDB backend: the store contract (contract.ts) for browsers without OPFS (Safari and
 * Firefox private browsing, storage probe 2026-11-05), over an `IdbDatabaseLike` (idb-like.ts):
 * the platform's IndexedDB in browsers, the simulated one (sim-idb.ts) in the storage kit.
 *
 * ## Fence (`epoch`)
 * Acquisition takes the Web Lock, then bumps a durable epoch record in one strict transaction.
 * Every read-write transaction reads the epoch first, inside itself, and aborts with
 * `OwnershipLostError` if it moved, so a stale owner that resumes changes nothing, whatever it
 * staged. A stolen lock also fails the lease at once.
 *
 * ## Layout (one database `minnow-next:<name>`, five object stores)
 * - `meta`: `epoch`, the two log header slots (`header-0`, `header-1`) and `flush` (written by
 *   each `flushLog`, so the strict commit has something to commit).
 * - `roots`: the two root slots (`slot-0`, `slot-1`) in root-slot.ts's encoding. A publish puts
 *   one in a strict transaction; the other keeps the previous root.
 * - `data`, `log`, `spill`: byte records keyed by offset, each `{ offset, bytes: ArrayBuffer }`
 *   holding one write. A write first clears its range (deleting records inside it and trimming
 *   ones that straddle a bound), so records never overlap; a read assembles its range from the
 *   record before it and the records inside it, and reads gaps as zeros. Freed space is deleted
 *   (`releaseData`), which returns it to the platform: unlike OPFS, IndexedDB does give quota back.
 *
 * ## Durability and batching
 * Relaxed writes are queued and committed together, in call order (`IdbSession`); `publish` and
 * `flushLog` commit the queue and themselves in one strict transaction, and a strict
 * transaction always writes something (`flushLog` its marker), since an engine need not flush one
 * that changed nothing. `releaseData`, `truncateLog` and `resetLog` commit the queue, then run
 * alone. The platforms keep one write-ahead log per database and commit in order, so a strict
 * commit makes every earlier commit durable too (idb-like.ts). Read-write transactions all
 * include `meta`, so they run in call order.
 *
 * ## Epoch record
 * Missing means a new store (epoch 1). A record that is not a safe positive integer, or one that
 * cannot be incremented, refuses ownership with `StoreCorruptionError`: two owners must never
 * share an epoch.
 */
import {
  LOG_HEADER_SLOT_BYTES,
  OwnershipLostError,
  StoreClosedError,
  StoreCorruptionError,
  type AcquireOptions,
  type DataRange,
  type OwnerLease,
  type ScratchSpace,
  type StorageBackend,
  type StoreSession,
} from "./contract.js";
import { acquireFenced, type HeldLock, type LockManagerLike } from "./fence.js";
import type { ByteRecord, IdbDatabaseLike, IdbStoreLike, IdbTransactionLike } from "./idb-like.js";
import { chooseRoot, encodeRootSlot } from "./root-slot.js";

export interface IndexedDbBackendOptions {
  /** Opens the database (once per backend). */
  readonly database: () => Promise<IdbDatabaseLike> | IdbDatabaseLike;
  /** The database name; the lock is `minnow:<name>`. */
  readonly name: string;
  readonly locks: LockManagerLike;
  /** Queue relaxed writes into batches (the default); false commits each alone (benchmarks). */
  readonly batching?: boolean;
}

const record = (offset: number, bytes: ArrayBuffer): ByteRecord => ({ offset, bytes });

/** An exact-size copy (never a view: structured clone would store the whole underlying buffer). */
const exact = (bytes: Uint8Array): ArrayBuffer => bytes.slice().buffer;

/**
 * Removes `[start, end)` from a byte store, trimming the records that straddle its bounds, then
 * calls `then`. Loads at most the two straddling records, never the records inside.
 */
function clearRange(store: IdbStoreLike, start: number, end: number, then: () => void): void {
  // An empty range clears nothing (and IndexedDB refuses an empty key range: DataError).
  if (end <= start) {
    then();
    return;
  }
  store.last({ upper: start, upperOpen: true }, (value) => {
    const before = value as ByteRecord | undefined;
    if (before !== undefined) {
      const beforeEnd = before.offset + before.bytes.byteLength;
      if (beforeEnd > start) {
        store.put(
          before.offset,
          record(before.offset, before.bytes.slice(0, start - before.offset)),
        );
        if (beforeEnd > end) store.put(end, record(end, before.bytes.slice(end - before.offset)));
      }
    }
    store.last({ lower: start, upper: end, upperOpen: true }, (inside) => {
      const last = inside as ByteRecord | undefined;
      store.deleteRange({ lower: start, upper: end, upperOpen: true });
      if (last !== undefined) {
        const lastEnd = last.offset + last.bytes.byteLength;
        if (lastEnd > end) store.put(end, record(end, last.bytes.slice(end - last.offset)));
      }
      then();
    });
  });
}

/** Fills `target` with the bytes at `start` (gaps read as zeros), then calls `then`. */
function readRange(store: IdbStoreLike, start: number, target: Uint8Array, then: () => void): void {
  target.fill(0);
  const end = start + target.length;
  const copy = (found: ByteRecord) => {
    const from = Math.max(start, found.offset);
    const to = Math.min(end, found.offset + found.bytes.byteLength);
    if (to > from)
      target.set(new Uint8Array(found.bytes, from - found.offset, to - from), from - start);
  };
  store.last({ upper: start, upperOpen: true }, (value) => {
    if (value !== undefined) copy(value as ByteRecord);
    if (target.length === 0) {
      then();
      return;
    }
    store.getAll({ lower: start, upper: end, upperOpen: true }, (values) => {
      for (const found of values as ByteRecord[]) copy(found);
      then();
    });
  });
}

/** The end of the last record of a byte store (its length), then `then(length)`. */
function lengthOf(store: IdbStoreLike, then: (length: number) => void): void {
  store.last(null, (value) => {
    const last = value as ByteRecord | undefined;
    then(last === undefined ? 0 : last.offset + last.bytes.byteLength);
  });
}

class IdbLease implements OwnerLease {
  readonly epoch: number;
  readonly database: IdbDatabaseLike;
  readonly #lock: HeldLock;
  readonly #lost = new AbortController();

  constructor(lock: HeldLock, database: IdbDatabaseLike, epoch: number) {
    this.#lock = lock;
    this.database = database;
    this.epoch = epoch;
    if (lock.lost.aborted) this.lose();
    else
      lock.lost.addEventListener("abort", () => {
        this.lose();
      });
  }

  get signal(): AbortSignal {
    return this.#lost.signal;
  }

  lose(): void {
    if (!this.#lost.signal.aborted) this.#lost.abort(new OwnershipLostError(this.epoch));
  }

  guard(): void {
    if (this.#lost.signal.aborted) throw new OwnershipLostError(this.epoch);
  }

  /**
   * One read-write transaction over `stores` (plus `meta`) whose body runs only if the durable
   * epoch is still this lease's: the fence, checked inside the transaction that applies the effect.
   */
  write(
    stores: readonly string[],
    durability: "strict" | "relaxed",
    body: (tx: IdbTransactionLike) => void,
  ): Promise<void> {
    this.guard();
    return this.database.transaction(
      [...new Set([...stores, "meta"])],
      "readwrite",
      durability,
      (tx) => {
        tx.store("meta").get("epoch", (stored) => {
          if (stored !== this.epoch) {
            this.lose();
            tx.abort(new OwnershipLostError(this.epoch));
            return;
          }
          body(tx);
        });
      },
    );
  }

  async release(): Promise<void> {
    this.lose();
    this.#lock.release();
  }
}

/** One queued relaxed write: issues its requests, then calls `next` (so writes keep their order). */
type QueuedWrite = (tx: IdbTransactionLike, next: () => void) => void;

/** A batch of relaxed writes is committed once it holds this many bytes or operations. */
export const IDB_BATCH_BYTES = 4 * 1024 * 1024;
export const IDB_BATCH_OPERATIONS = 256;

/**
 * The session. Relaxed writes (`writeData`, `appendLog`, `writeLogHeader`) are queued and committed
 * together, in call order, in one transaction (review B1-p2c finding 6): at the next macrotask,
 * when the batch reaches `IDB_BATCH_BYTES` or `IDB_BATCH_OPERATIONS` (then the call waits for it:
 * backpressure), before any read, and inside the same transaction as a barrier (`publish`,
 * `flushLog`), so a barrier commits everything queued before it, atomically and in order. A
 * queued write's failure (quota, a moved epoch) fails the session: every later call rejects with
 * it, and nothing of the batch applied. Releases, truncation and reset flush the queue, then run
 * alone. The queue holds copies (at most one batch, ≤ 4 MiB, plus the batch in flight).
 */
class IdbSession implements StoreSession {
  readonly epoch: number;
  readonly #lease: IdbLease;
  readonly #database: IdbDatabaseLike;
  #dataLength: number;
  #logLength: number;
  #closed = false;
  #queue: QueuedWrite[] = [];
  #queuedStores = new Set<string>();
  #queuedBytes = 0;
  #scheduled = false;
  /** Every commit runs after the previous one (call order). */
  #committing: Promise<void> = Promise.resolve();
  #failure: Error | null = null;

  readonly #batching: boolean;

  constructor(lease: IdbLease, dataLength: number, logLength: number, batching: boolean) {
    this.#lease = lease;
    this.#database = lease.database;
    this.epoch = lease.epoch;
    this.#dataLength = dataLength;
    this.#logLength = logLength;
    this.#batching = batching;
  }

  #live(): void {
    if (this.#closed) throw new StoreClosedError();
  }

  /** Mutations also refuse once a batch failed (reads go on: the extent space still reads). */
  #mutable(): void {
    this.#live();
    if (this.#failure !== null) throw this.#failure;
  }

  /** Queues a relaxed write; waits only when the batch is full. */
  async #enqueue(stores: readonly string[], bytes: number, write: QueuedWrite): Promise<void> {
    this.#mutable();
    this.#lease.guard();
    this.#queue.push(write);
    for (const store of stores) this.#queuedStores.add(store);
    this.#queuedBytes += bytes;
    if (
      !this.#batching ||
      this.#queuedBytes >= IDB_BATCH_BYTES ||
      this.#queue.length >= IDB_BATCH_OPERATIONS
    ) {
      await this.#commit();
      return;
    }
    if (!this.#scheduled) {
      this.#scheduled = true;
      setTimeout(() => {
        this.#scheduled = false;
        this.#commit().catch(() => undefined);
      }, 0);
    }
  }

  /**
   * Commits everything queued, with `barrier` (a strict write) last in the same transaction when
   * given. Resolves when that transaction completes.
   */
  #commit(
    barrier: { readonly stores: readonly string[]; readonly write: QueuedWrite } | null = null,
  ): Promise<void> {
    const writes = this.#queue;
    const stores = [...this.#queuedStores, ...(barrier?.stores ?? [])];
    this.#queue = [];
    this.#queuedStores = new Set();
    this.#queuedBytes = 0;
    const all = barrier === null ? writes : [...writes, barrier.write];
    const run = this.#committing.then(async () => {
      if (this.#failure !== null) throw this.#failure;
      if (all.length === 0) return;
      try {
        await this.#lease.write(stores, barrier === null ? "relaxed" : "strict", (tx) => {
          const next = (index: number) => {
            const write = all[index];
            if (write !== undefined)
              write(tx, () => {
                next(index + 1);
              });
          };
          next(0);
        });
      } catch (error) {
        this.#failure ??= error instanceof Error ? error : new Error(String(error));
        throw this.#failure;
      }
    });
    this.#committing = run.catch(() => undefined);
    return run;
  }

  /** A read: everything queued is committed first, so it sees every earlier write. */
  async #read(stores: readonly string[], body: (tx: IdbTransactionLike) => void): Promise<void> {
    this.#live();
    if (this.#failure === null) await this.#commit();
    await this.#database.transaction(stores, "readonly", "relaxed", body);
  }

  async #slots(): Promise<ReturnType<typeof chooseRoot>> {
    let slots: [Uint8Array | null, Uint8Array | null] = [null, null];
    await this.#read(["roots"], (tx) => {
      const roots = tx.store("roots");
      roots.get("slot-0", (first) => {
        roots.get("slot-1", (second) => {
          slots = [slotBytes(first), slotBytes(second)];
        });
      });
    });
    return chooseRoot(slots[0], slots[1]);
  }

  async readRoot(): Promise<Uint8Array | null> {
    return (await this.#slots()).current?.root ?? null;
  }

  async readPreviousRoot(): Promise<Uint8Array | null> {
    return (await this.#slots()).previous?.root ?? null;
  }

  /** Guarantee 1: every queued write, then the root, in one strict transaction. */
  async publish(root: Uint8Array): Promise<void> {
    this.#mutable();
    this.#lease.guard();
    const copy = root.slice();
    await this.#commit({
      stores: ["roots"],
      write: (tx, next) => {
        const roots = tx.store("roots");
        roots.get("slot-0", (first) => {
          roots.get("slot-1", (second) => {
            const { nextSlot, nextSequence } = chooseRoot(slotBytes(first), slotBytes(second));
            roots.put(`slot-${String(nextSlot)}`, encodeRootSlot(nextSequence, copy).buffer);
            next();
          });
        });
      },
    });
  }

  async writeData(offset: number, bytes: Uint8Array): Promise<void> {
    this.#mutable();
    if (offset % 4096 !== 0 || offset < 0) throw new RangeError("Unaligned data extent");
    if (bytes.length === 0) return;
    const end = offset + bytes.length;
    const copy = exact(bytes);
    this.#dataLength = Math.max(this.#dataLength, end);
    await this.#enqueue(["data"], bytes.length, (tx, next) => {
      const data = tx.store("data");
      clearRange(data, offset, end, () => {
        data.put(offset, record(offset, copy));
        next();
      });
    });
  }

  #checkData(offset: number, length: number): void {
    if (offset < 0 || length < 0 || offset + length > this.#dataLength)
      throw new StoreCorruptionError("Read outside the data space");
  }

  async readData(ranges: readonly DataRange[]): Promise<Uint8Array[]> {
    this.#live();
    for (const { offset, length } of ranges) this.#checkData(offset, length);
    const results = ranges.map(({ length }) => new Uint8Array(length));
    await this.#read(["data"], (tx) => {
      const data = tx.store("data");
      ranges.forEach(({ offset }, index) => {
        readRange(data, offset, results[index] ?? new Uint8Array(0), () => undefined);
      });
    });
    return results;
  }

  async readDataInto(offset: number, target: Uint8Array): Promise<void> {
    this.#live();
    this.#checkData(offset, target.length);
    await this.#read(["data"], (tx) => {
      readRange(tx.store("data"), offset, target, () => undefined);
    });
  }

  async releaseData(ranges: readonly DataRange[]): Promise<void> {
    this.#mutable();
    await this.#commit();
    let length = this.#dataLength;
    await this.#lease.write(["data"], "relaxed", (tx) => {
      const data = tx.store("data");
      const next = (index: number) => {
        const range = ranges[index];
        if (range === undefined) {
          lengthOf(data, (end) => {
            length = end;
          });
          return;
        }
        clearRange(data, range.offset, range.offset + range.length, () => {
          next(index + 1);
        });
      };
      next(0);
    });
    this.#dataLength = length;
  }

  async dataLength(): Promise<number> {
    this.#live();
    return this.#dataLength;
  }

  logLength(): number {
    this.#live();
    return this.#logLength;
  }

  async readLog(offset: number, length: number): Promise<Uint8Array> {
    this.#live();
    if (offset < 0 || length < 0 || offset + length > this.#logLength)
      throw new StoreCorruptionError("Read outside the log");
    const target = new Uint8Array(length);
    if (length === 0) return target;
    await this.#read(["log"], (tx) => {
      readRange(tx.store("log"), offset, target, () => undefined);
    });
    return target;
  }

  async readLogHeaders(): Promise<readonly [Uint8Array | null, Uint8Array | null]> {
    let headers: [Uint8Array | null, Uint8Array | null] = [null, null];
    await this.#read(["meta"], (tx) => {
      const meta = tx.store("meta");
      meta.get("header-0", (first) => {
        meta.get("header-1", (second) => {
          headers = [slotBytes(first), slotBytes(second)];
        });
      });
    });
    return headers;
  }

  async appendLog(bytes: Uint8Array): Promise<number> {
    this.#mutable();
    const offset = this.#logLength;
    if (bytes.length === 0) return offset;
    this.#logLength += bytes.length;
    const copy = exact(bytes);
    await this.#enqueue(["log"], bytes.length, (tx, next) => {
      tx.store("log").put(offset, record(offset, copy));
      next();
    });
    return offset;
  }

  async writeLogHeader(slot: 0 | 1, bytes: Uint8Array): Promise<void> {
    this.#mutable();
    if (bytes.length > LOG_HEADER_SLOT_BYTES) throw new RangeError("Log header slot overflow");
    const padded = new Uint8Array(LOG_HEADER_SLOT_BYTES);
    padded.set(bytes);
    await this.#enqueue(["meta"], LOG_HEADER_SLOT_BYTES, (tx, next) => {
      tx.store("meta").put(`header-${String(slot)}`, padded.buffer);
      next();
    });
  }

  /** Guarantee 3: every queued write, then a flush marker, in one strict transaction. */
  async flushLog(): Promise<void> {
    this.#mutable();
    this.#lease.guard();
    const length = this.#logLength;
    await this.#commit({
      stores: ["meta"],
      write: (tx, next) => {
        tx.store("meta").put("flush", length);
        next();
      },
    });
  }

  async truncateLog(length: number): Promise<void> {
    this.#mutable();
    if (length < 0 || length > this.#logLength) throw new RangeError("Truncate outside the log");
    await this.#commit();
    await this.#lease.write(["log"], "strict", (tx) => {
      clearRange(tx.store("log"), length, Infinity, () => undefined);
    });
    this.#logLength = length;
  }

  async resetLog(): Promise<void> {
    this.#mutable();
    await this.#commit();
    await this.#lease.write(["log"], "strict", (tx) => {
      tx.store("log").clear();
      tx.store("meta").delete("header-0");
      tx.store("meta").delete("header-1");
    });
    this.#logLength = 0;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    await this.#commit().catch(() => undefined);
    this.#closed = true;
  }
}

const slotBytes = (value: unknown) => (value instanceof ArrayBuffer ? new Uint8Array(value) : null);

/** The spill store under the epoch fence: records in `spill`, relaxed, never recovered. */
class IdbScratch implements ScratchSpace {
  readonly #lease: IdbLease;

  constructor(lease: IdbLease) {
    this.#lease = lease;
  }

  async write(offset: number, bytes: Uint8Array): Promise<void> {
    if (offset % 4096 !== 0 || offset < 0) throw new RangeError("Unaligned scratch extent");
    const copy = exact(bytes);
    const end = offset + bytes.length;
    await this.#lease.write(["spill"], "relaxed", (tx) => {
      const spill = tx.store("spill");
      clearRange(spill, offset, end, () => {
        spill.put(offset, record(offset, copy));
      });
    });
  }

  async readInto(offset: number, target: Uint8Array): Promise<void> {
    this.#lease.guard();
    let length = 0;
    await this.#lease.database.transaction(["spill"], "readonly", "relaxed", (tx) => {
      const spill = tx.store("spill");
      lengthOf(spill, (end) => {
        length = end;
        if (offset >= 0 && offset + target.length <= end)
          readRange(spill, offset, target, () => undefined);
      });
    });
    if (offset < 0 || offset + target.length > length)
      throw new StoreCorruptionError("Read outside the scratch space");
  }

  async release(ranges: readonly DataRange[]): Promise<void> {
    await this.#lease.write(["spill"], "relaxed", (tx) => {
      const spill = tx.store("spill");
      const next = (index: number) => {
        const range = ranges[index];
        if (range !== undefined)
          clearRange(spill, range.offset, range.offset + range.length, () => {
            next(index + 1);
          });
      };
      next(0);
    });
  }

  async truncate(size: number): Promise<void> {
    await this.#lease.write(["spill"], "relaxed", (tx) => {
      clearRange(tx.store("spill"), size, Infinity, () => undefined);
    });
  }
}

/** The IndexedDB `StorageBackend` (epoch fence, spill store). */
export function createIndexedDbBackend(options: IndexedDbBackendOptions): StorageBackend {
  const leases = new WeakSet<IdbLease>();
  let opened: Promise<IdbDatabaseLike> | null = null;
  const database = () => (opened ??= Promise.resolve(options.database()));
  const own = (lease: OwnerLease): IdbLease => {
    if (!(lease instanceof IdbLease) || !leases.has(lease))
      throw new TypeError("The lease was not acquired from this backend");
    return lease;
  };
  return {
    fence: "epoch",
    async acquire(acquire: AcquireOptions = {}): Promise<OwnerLease> {
      const { lock, fence } = await acquireFenced(
        options.locks,
        `minnow:${options.name}`,
        async () => {
          const db = await database();
          let epoch = 0;
          // The fence: one strict transaction moves the durable epoch past every older owner's.
          await db.transaction(["meta"], "readwrite", "strict", (tx) => {
            const meta = tx.store("meta");
            meta.get("epoch", (stored) => {
              // Missing: a new store. Anything else must be a counter that can still increase
              // (review B1-p2c finding 1): a malformed or exhausted one would let two owners share
              // an epoch, so ownership is refused instead.
              if (stored === undefined) epoch = 1;
              else if (typeof stored !== "number" || !Number.isSafeInteger(stored) || stored < 1) {
                tx.abort(new StoreCorruptionError("The epoch record is malformed"));
                return;
              } else if (!Number.isSafeInteger(stored + 1)) {
                tx.abort(new StoreCorruptionError("The epoch counter is exhausted"));
                return;
              } else epoch = stored + 1;
              meta.put("epoch", epoch);
            });
          });
          return { db, epoch };
        },
        () => undefined,
        acquire,
      );
      const lease = new IdbLease(lock, fence.db, fence.epoch);
      leases.add(lease);
      return lease;
    },
    async open(lease: OwnerLease): Promise<StoreSession> {
      const owned = own(lease);
      owned.guard();
      let dataLength = 0;
      let logLength = 0;
      await owned.database.transaction(["data", "log"], "readonly", "relaxed", (tx) => {
        lengthOf(tx.store("data"), (length) => {
          dataLength = length;
        });
        lengthOf(tx.store("log"), (length) => {
          logLength = length;
        });
      });
      return new IdbSession(owned, dataLength, logLength, options.batching ?? true);
    },
    scratch(lease: OwnerLease): ScratchSpace {
      return new IdbScratch(own(lease));
    },
  };
}
