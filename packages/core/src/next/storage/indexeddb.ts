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
 * ## Durability
 * Writes commit `relaxed`; `publish`, `flushLog`, `truncateLog` and `resetLog` commit `strict`.
 * The platforms keep one write-ahead log per database and commit in order, so a strict commit
 * makes every earlier commit durable too (idb-like.ts). Each call is one transaction, and
 * read-write transactions all include `meta`, so they run in call order.
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
}

const record = (offset: number, bytes: ArrayBuffer): ByteRecord => ({ offset, bytes });

/** An exact-size copy (never a view: structured clone would store the whole underlying buffer). */
const exact = (bytes: Uint8Array): ArrayBuffer => bytes.slice().buffer;

/**
 * Removes `[start, end)` from a byte store, trimming the records that straddle its bounds, then
 * calls `then`. Loads at most the two straddling records, never the records inside.
 */
function clearRange(store: IdbStoreLike, start: number, end: number, then: () => void): void {
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

class IdbSession implements StoreSession {
  readonly epoch: number;
  readonly #lease: IdbLease;
  readonly #database: IdbDatabaseLike;
  #dataLength: number;
  #logLength: number;
  #closed = false;

  constructor(lease: IdbLease, dataLength: number, logLength: number) {
    this.#lease = lease;
    this.#database = lease.database;
    this.epoch = lease.epoch;
    this.#dataLength = dataLength;
    this.#logLength = logLength;
  }

  #live(): void {
    if (this.#closed) throw new StoreClosedError();
  }

  #read(stores: readonly string[], body: (tx: IdbTransactionLike) => void): Promise<void> {
    this.#live();
    return this.#database.transaction(stores, "readonly", "relaxed", body);
  }

  async #slots(): Promise<ReturnType<typeof chooseRoot>> {
    let slots: [Uint8Array | null, Uint8Array | null] = [null, null];
    await this.#read(["roots"], (tx) => {
      const roots = tx.store("roots");
      roots.get("slot-0", (first) => {
        roots.get("slot-1", (second) => {
          const bytes = (value: unknown) =>
            value instanceof ArrayBuffer ? new Uint8Array(value) : null;
          slots = [bytes(first), bytes(second)];
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

  async publish(root: Uint8Array): Promise<void> {
    this.#live();
    await this.#lease.write(["roots"], "strict", (tx) => {
      const roots = tx.store("roots");
      roots.get("slot-0", (first) => {
        roots.get("slot-1", (second) => {
          const bytes = (value: unknown) =>
            value instanceof ArrayBuffer ? new Uint8Array(value) : null;
          const { nextSlot, nextSequence } = chooseRoot(bytes(first), bytes(second));
          roots.put(`slot-${String(nextSlot)}`, encodeRootSlot(nextSequence, root).buffer);
        });
      });
    });
  }

  async writeData(offset: number, bytes: Uint8Array): Promise<void> {
    this.#live();
    if (offset % 4096 !== 0 || offset < 0) throw new RangeError("Unaligned data extent");
    const end = offset + bytes.length;
    const copy = exact(bytes);
    await this.#lease.write(["data"], "relaxed", (tx) => {
      const data = tx.store("data");
      clearRange(data, offset, end, () => {
        data.put(offset, record(offset, copy));
      });
    });
    this.#dataLength = Math.max(this.#dataLength, end);
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
    this.#live();
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
          const bytes = (value: unknown) =>
            value instanceof ArrayBuffer ? new Uint8Array(value) : null;
          headers = [bytes(first), bytes(second)];
        });
      });
    });
    return headers;
  }

  async appendLog(bytes: Uint8Array): Promise<number> {
    this.#live();
    const offset = this.#logLength;
    this.#logLength += bytes.length;
    const copy = exact(bytes);
    try {
      await this.#lease.write(["log"], "relaxed", (tx) => {
        tx.store("log").put(offset, record(offset, copy));
      });
    } catch (error) {
      if (this.#logLength === offset + bytes.length) this.#logLength = offset;
      throw error;
    }
    return offset;
  }

  async writeLogHeader(slot: 0 | 1, bytes: Uint8Array): Promise<void> {
    this.#live();
    if (bytes.length > LOG_HEADER_SLOT_BYTES) throw new RangeError("Log header slot overflow");
    const padded = new Uint8Array(LOG_HEADER_SLOT_BYTES);
    padded.set(bytes);
    await this.#lease.write(["meta"], "relaxed", (tx) => {
      tx.store("meta").put(`header-${String(slot)}`, padded.buffer);
    });
  }

  async flushLog(): Promise<void> {
    this.#live();
    const length = this.#logLength;
    await this.#lease.write(["meta"], "strict", (tx) => {
      tx.store("meta").put("flush", length);
    });
  }

  async truncateLog(length: number): Promise<void> {
    this.#live();
    if (length < 0 || length > this.#logLength) throw new RangeError("Truncate outside the log");
    await this.#lease.write(["log"], "strict", (tx) => {
      clearRange(tx.store("log"), length, Infinity, () => undefined);
    });
    this.#logLength = length;
  }

  async resetLog(): Promise<void> {
    this.#live();
    await this.#lease.write(["log"], "strict", (tx) => {
      tx.store("log").clear();
      tx.store("meta").delete("header-0");
      tx.store("meta").delete("header-1");
    });
    this.#logLength = 0;
  }

  async close(): Promise<void> {
    this.#closed = true;
  }
}

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
              epoch = (typeof stored === "number" ? stored : 0) + 1;
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
      return new IdbSession(owned, dataLength, logLength);
    },
    scratch(lease: OwnerLease): ScratchSpace {
      return new IdbScratch(own(lease));
    },
  };
}
