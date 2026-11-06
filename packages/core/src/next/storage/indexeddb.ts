/**
 * The IndexedDB backend: the store contract (contract.ts) for browsers without OPFS (Safari and
 * Firefox private browsing, storage probe 2026-11-05), over an `IdbDatabaseLike` (idb-like.ts):
 * the platform's IndexedDB in browsers, the simulated one (sim-idb.ts) in the storage kit.
 *
 * ## Fence (`epoch`)
 * Acquisition takes the Web Lock, then bumps a durable epoch record in one strict transaction.
 * Every transaction, reads included, reads the epoch first, inside itself, and aborts with
 * `OwnershipLostError` if it moved, so a stale owner that resumes changes nothing, whatever it
 * staged, and reads nothing a newer owner may have rearranged (review B1-p2d finding 1: OPFS
 * closes a fenced owner's handles, so its reads fail the same way). A stolen lock also fails the
 * lease at once.
 *
 * ## Layout (one database `minnow-core:<name>`, eight object stores)
 * - `meta`: `epoch`, the two log header slots (`header-0`, `header-1`) and `flush` (written by
 *   each `flushLog`, so the strict commit has something to commit).
 * - `roots`: the two root slots (`slot-0`, `slot-1`) in root-slot.ts's encoding. A publish puts
 *   one in a strict transaction; the other keeps the previous root.
 * - `data`, `log`, `spill`: byte spaces (idb-bytes.ts): disjoint records of ≤ 256 KiB keyed by
 *   offset, each with an entry in `data-index`, `log-index` or `spill-index` (offset → length).
 *   Writes find overlaps in an in-memory copy of the index loaded (and validated) at open; reads
 *   select records from the stored index inside their transaction. Gaps read as zeros. Freed space
 *   is deleted (`releaseData`), which returns it to the platform: unlike OPFS, IndexedDB does give
 *   quota back.
 *
 * ## Durability and batching
 * Relaxed writes are queued and committed together, in call order (`IdbSession`); `publish` and
 * `flushLog` commit the queue and themselves in one strict transaction, and a strict
 * transaction always writes something (`flushLog` its marker), since an engine need not flush one
 * that changed nothing. `releaseData`, `truncateLog` and `resetLog` are barriers too (relaxed,
 * strict, strict): every transaction that touches the in-memory indexes runs on the one commit
 * chain, in call order. Range lists (a release of many extents) are driven iteratively
 * (`inOrder`), never one stack frame per range.
 *
 * ## Log records
 * The log's last record is kept in memory while it is shorter than `LOG_COALESCE_BYTES` (the
 * tail): an append that continues it is copied into it, and the next commit rewrites that one
 * record whole (a range delete of the old record and one put, no get). Records therefore number
 * about log bytes / 64 KiB however small the group commits; a commit rewrites at most 64 KiB.
 * A crash during a rewrite leaves the previous complete record (the transaction is atomic). The
 * tail's buffer is charged to the backend's memory registration while the session holds it.
 * `truncateLog` and `resetLog` move the logical end when called, in call order with appends, and
 * forget the tail; if their transaction then fails, the session fails (later appends were placed
 * after the new end). The platforms keep one write-ahead log per database and commit in order, so a strict
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
  ReleaseUnsettledError,
  StoreClosedError,
  StoreCorruptionError,
  type AcquireOptions,
  type DataRange,
  type OwnerLease,
  type ReleaseReport,
  type ScratchSpace,
  type StorageBackend,
  type StorageObserver,
  type StoreSession,
  type WriteDataOptions,
} from "./contract.js";
import { acquireFenced, type HeldLock, type LockManagerLike } from "./fence.js";
import { ByteStore, inOrder } from "./idb-bytes.js";
import type { CacheMemory } from "./extents.js";
import type { IdbDatabaseLike, IdbTransactionLike } from "./idb-like.js";
import { chooseRoot, encodeRootSlot } from "./root-slot.js";

export interface IndexedDbBackendOptions {
  /**
   * Opens a connection to the database: called once per acquisition, which closes it at release
   * (an object returned more than once, to several acquisitions or backends, keeps one serialized
   * chain and is closed when its last acquisition releases).
   */
  readonly database: () => Promise<IdbDatabaseLike> | IdbDatabaseLike;
  /** The database name; the lock is `minnow:<name>`. */
  readonly name: string;
  readonly locks: LockManagerLike;
  /** Queue relaxed writes into batches (the default); false commits each alone (benchmarks). */
  readonly batching?: boolean;
  /**
   * Run every transaction of the connection through one serialized chain (the default).
   * WebKit can leave IndexedDB blocked for the browser session when a worker dies with two
   * independent chains of transactions in flight on its connection (DECISIONS.md "Step-down");
   * one chain never did (0 of 45 kills in the plain repro, against 3 of 45 with two). False runs
   * independent transactions concurrently (benchmarks).
   */
  readonly serializeTransactions?: boolean;
  /**
   * How long a lease release waits for in-flight transactions before aborting them. Default
   * `IDB_CLOSE_DRAIN_MS` (500 ms).
   */
  readonly releaseDrainMs?: number;
  /**
   * How long a release then waits for the transactions it aborted to end before giving up with
   * `ReleaseUnsettledError`. Default `IDB_ABORT_SETTLE_MS` (1 s).
   */
  readonly releaseSettleMs?: number;
  /** Told about every transaction once created, and when it ends (test instrumentation). */
  readonly observe?: StorageObserver;
  /**
   * Charged with the resident record indexes of the data and log spaces (idb-bytes.ts, "Cost"),
   * growing and shrinking with them, released at close: the owner registers it with the memory
   * governor. None by default.
   */
  readonly memory?: CacheMemory;
}

/** Consecutive log appends in one batch are joined into records of up to this many bytes. */
export const LOG_COALESCE_BYTES = 64 * 1024;

/**
 * A release of more ranges than this is split into transactions of at most this many, with a task
 * between them (review B1-p2e finding 2: one transaction of 12,000 range deletes held the commit
 * chain). Safe to split: released space is free (no durable root, previous root or pin reaches it,
 * and the caller reuses it only once the release resolves), so a crash between chunks leaves some
 * of it deleted and some not; what remains only occupies free space until it is reused, and never
 * a byte a root names.
 */
export const RELEASE_CHUNK = 1024;

/** Lets other tasks run (between chunks of a long release). */
const yieldTask = () =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });

/** `ranges` in consecutive chunks of at most `RELEASE_CHUNK`. */
function chunksOf<T>(ranges: readonly T[]): T[][] {
  const chunks: T[][] = [];
  for (let at = 0; at < ranges.length; at += RELEASE_CHUNK)
    chunks.push(ranges.slice(at, at + RELEASE_CHUNK));
  return chunks;
}

/** A copy the caller cannot change afterwards (it may reuse its buffer once a call resolves). */
const copyOf = (bytes: Uint8Array): Uint8Array => bytes.slice();

/**
 * Runs `body` in the lease's fenced read-write transaction and settles the byte stores' in-memory
 * indexes with it: kept when it commits, undone when it does not.
 */
async function transact(
  lease: IdbLease,
  spaces: readonly ByteStore[],
  durability: "strict" | "relaxed",
  body: (tx: IdbTransactionLike) => void,
  extra: readonly string[] = [],
): Promise<void> {
  const stores = [...spaces.flatMap((space) => [space.name, space.indexName]), ...extra];
  try {
    await lease.write(stores, durability, body);
  } catch (error) {
    for (const space of spaces) space.rollback();
    throw error;
  }
  for (const space of spaces) space.commit();
}

/** How long a lease release waits for in-flight transactions before aborting them. */
export const IDB_CLOSE_DRAIN_MS = 500;

/**
 * How long a release waits, after aborting, for those transactions to end before giving up with
 * `ReleaseUnsettledError` (review B1-p2h-2 finding 2: a missing terminal event held the lock).
 */
export const IDB_ABORT_SETTLE_MS = 1_000;

const settle = () => undefined;

/** Resolves true when `work` settles within `ms`, false when the bound runs out first. */
async function within(work: Promise<unknown>, ms: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const settled = await Promise.race([
    work.then(() => true),
    new Promise<boolean>((resolve) => {
      timer = setTimeout(() => {
        resolve(false);
      }, ms);
    }),
  ]);
  clearTimeout(timer);
  return settled;
}

/**
 * What every tracker over one inner connection object shares: how many use it (it closes when the
 * last one closes) and, when serialized, the connection's one chain. An `options.database` that
 * hands several acquisitions, or several backends, the same object (tests do) still gets one chain
 * on it (review B1-p2h-2 finding 1: a tracker's own chain let a stealing acquisition's fence start
 * beside its predecessor's transaction, two chains on one connection).
 */
interface SharedConnection {
  users: number;
  tail: Promise<void>;
}
const shared = new WeakMap<IdbDatabaseLike, SharedConnection>();

/** A transaction issued through a tracker. */
interface Issued {
  readonly stores: readonly string[];
  readonly mode: "readonly" | "readwrite";
  readonly durability: "strict" | "relaxed";
  /** The platform transaction, once created (its body ran). */
  tx: IdbTransactionLike | null;
  /** Ends the caller's promise when the release gives up on the transaction. */
  giveUp: (error: Error) => void;
  /** Reports the end of the platform transaction to the observer. */
  ended: () => void;
}

/**
 * One acquisition's connection (review B1-p2h-1 finding 2: a stolen lease's release closed the
 * connection its successor shared), tracking every transaction so a release can settle them
 * before closing it (`drainAndClose`).
 *
 * WebKit (Playwright's 26.5) leaves IndexedDB blocked for the browser session when a worker is
 * terminated with transactions of two independent chains in flight on its connection (12.5% of
 * such kills in a plain-IndexedDB repro); closing the connection immediately before the kill does
 * not prevent it, letting the in-flight transactions finish first does (0 of 40). So a release
 * waits for them, aborts any still running when the wait runs out and waits for their abort
 * events, then closes: the lock is released only after (review B1-p2h-1 finding 1). The owner
 * awaits `lease.release()` before it terminates its worker.
 *
 * Admission, cancellation and accounting are per tracker (releasing one acquisition never cancels
 * another's work); the chain is per inner connection object (`SharedConnection`).
 */
class TrackedConnection implements IdbDatabaseLike {
  readonly #inner: IdbDatabaseLike;
  readonly #shared: SharedConnection;
  readonly #serialize: boolean;
  readonly #observe: StorageObserver | null;
  /** Issued and not settled. */
  readonly #inFlight = new Map<Promise<void>, Issued>();
  #closing = false;
  #innerClosed = false;
  #released: Promise<ReleaseReport> | null = null;
  /** Set when a release stopped waiting: transactions still running abort with it. */
  #stopped: StoreClosedError | null = null;
  /** Transactions that ended with `#stopped` (aborted, or refused before they began). */
  #aborted = 0;

  constructor(inner: IdbDatabaseLike, serialize: boolean, observe: StorageObserver | null = null) {
    this.#inner = inner;
    this.#serialize = serialize;
    this.#observe = observe;
    let entry = shared.get(inner);
    if (entry === undefined) {
      entry = { users: 0, tail: Promise.resolve() };
      shared.set(inner, entry);
    }
    entry.users += 1;
    this.#shared = entry;
  }

  transaction(
    stores: readonly string[],
    mode: "readonly" | "readwrite",
    durability: "strict" | "relaxed",
    body: (tx: IdbTransactionLike) => void,
  ): Promise<void> {
    if (this.#closing) return Promise.reject(new StoreClosedError("The connection is closing"));
    const issued: Issued = { stores, mode, durability, tx: null, giveUp: settle, ended: settle };
    const start = (): Promise<void> => {
      // Queued behind the chain when the release stopped waiting: never created.
      if (this.#stopped !== null) return Promise.reject(this.#stopped);
      const created = this.#inner.transaction(stores, mode, durability, (tx) => {
        issued.tx = tx;
        if (this.#observe !== null)
          issued.ended = this.#observe({
            target: stores.filter((store) => store !== "meta").join("+") || "meta",
            op: `${mode}/${durability}`,
          });
        if (this.#stopped !== null) tx.abort(this.#stopped);
        else body(tx);
      });
      // The platform's own end (complete or abort), even after a release gave up on it.
      if (this.#observe !== null) {
        const ended = () => {
          issued.ended();
        };
        created.then(ended, ended);
      }
      return created;
    };
    const run = new Promise<void>((resolve, reject) => {
      issued.giveUp = reject;
      // One chain per connection: data, log, spill and fence transactions alike, of every
      // acquisition using this connection object, wait for the previous one to settle.
      (this.#serialize ? this.#shared.tail.then(start) : start()).then(resolve, reject);
    });
    if (this.#serialize) this.#shared.tail = run.then(settle, settle);
    this.#inFlight.set(run, issued);
    run.then(
      () => {
        this.#inFlight.delete(run);
      },
      (error: unknown) => {
        this.#inFlight.delete(run);
        if (error !== null && error === this.#stopped) this.#aborted += 1;
      },
    );
    return run;
  }

  /** Closes at once (a fence that was opened but not handed out has nothing in flight). */
  close(): void {
    this.#closing = true;
    this.#closeInner();
  }

  /**
   * Starts no new transaction and waits up to `drainMs` for those issued. If some are left, aborts
   * those the platform created and refuses those it did not (both with `StoreClosedError`), and
   * waits up to `settleMs` for the created ones to end. Then closes. If some never ended, their operations and this call
   * reject with `ReleaseUnsettledError` (the close is still requested). Idempotent: every call
   * gets the first call's outcome.
   */
  drainAndClose(drainMs: number, settleMs: number): Promise<ReleaseReport> {
    return (this.#released ??= this.#drainAndClose(drainMs, settleMs));
  }

  async #drainAndClose(drainMs: number, settleMs: number): Promise<ReleaseReport> {
    const started = Date.now();
    this.#closing = true;
    const runs = [...this.#inFlight.keys()];
    const settled = Promise.allSettled(runs);
    if (!(await within(settled, drainMs))) {
      this.#stopped = new StoreClosedError(
        "The lease was released before the transaction finished",
      );
      for (const issued of this.#inFlight.values())
        if (issued.tx !== null) issued.tx.abort(this.#stopped);
        // Never created (queued behind the chain, possibly behind a transaction that will not
        // end): its outcome is known, refused now (review B1-p2h-3). If the platform was asked
        // for it, its body aborts itself when it runs.
        else issued.giveUp(this.#stopped);
      // The platform fires a terminal event (complete or abort) for every transaction it created;
      // one that was already committing completes and counts as drained.
      if (!(await within(settled, settleMs))) {
        // Only created transactions are left: their outcome is unknown.
        const left = [...this.#inFlight.values()];
        const error = new ReleaseUnsettledError({
          drained: runs.length - this.#aborted - left.length,
          aborted: this.#aborted,
          unsettled: left.length,
          transactions: left.map(({ stores, mode, durability }) => ({ stores, mode, durability })),
          waitedMs: Date.now() - started,
        });
        // Outcome unknown: never acknowledged, never reported as aborted.
        for (const issued of left) issued.giveUp(error);
        this.#closeInner();
        throw error;
      }
    }
    this.#closeInner();
    return { drained: runs.length - this.#aborted, aborted: this.#aborted };
  }

  #closeInner(): void {
    if (this.#innerClosed) return;
    this.#innerClosed = true;
    this.#shared.users -= 1;
    if (this.#shared.users === 0) {
      shared.delete(this.#inner);
      this.#inner.close();
    }
  }
}

class IdbLease implements OwnerLease {
  readonly epoch: number;
  readonly database: IdbDatabaseLike;
  readonly #lock: HeldLock;
  readonly #lost = new AbortController();

  readonly #closeConnection: () => Promise<ReleaseReport>;

  constructor(
    lock: HeldLock,
    database: IdbDatabaseLike,
    epoch: number,
    closeConnection: () => Promise<ReleaseReport>,
  ) {
    this.#lock = lock;
    this.database = database;
    this.epoch = epoch;
    this.#closeConnection = closeConnection;
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

  /**
   * One read-only transaction over `stores` (plus `meta`) whose body runs only if the durable epoch
   * is still this lease's: a fenced owner's reads reject with `OwnershipLostError`.
   */
  read(stores: readonly string[], body: (tx: IdbTransactionLike) => void): Promise<void> {
    this.guard();
    return this.database.transaction(
      [...new Set([...stores, "meta"])],
      "readonly",
      "relaxed",
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

  /**
   * Fences this lease, settles its connection's transactions (they finish within
   * `releaseDrainMs`, or are aborted), closes the connection, then releases the lock (see
   * `TrackedConnection`). An aborted strict commit rejects with `StoreClosedError`: not durable.
   * If aborted transactions never end (`releaseSettleMs`), the lock is still released and this
   * rejects with `ReleaseUnsettledError`.
   */
  async release(): Promise<ReleaseReport> {
    this.lose();
    try {
      return await this.#closeConnection();
    } finally {
      this.#lock.release();
    }
  }
}

/** One queued relaxed write: issues its requests, then calls `next` (so writes keep their order). */
type QueuedWrite = (tx: IdbTransactionLike, next: () => void) => void;

/** A write that ends a batch: committed after everything queued, in the same transaction. */
interface Barrier {
  readonly durability: "strict" | "relaxed";
  readonly spaces: readonly ByteStore[];
  readonly extra: readonly string[];
  readonly write: QueuedWrite;
}

/** The log's last record while it is under `LOG_COALESCE_BYTES`, with its bytes. */
interface LogTail {
  readonly offset: number;
  readonly buffer: Uint8Array;
  length: number;
  /** The batch whose queue holds its rewrite (later appends in that batch just extend it). */
  queued: number;
}

/** A batch of relaxed writes is committed once it holds this many bytes or operations. */
export const IDB_BATCH_BYTES = 4 * 1024 * 1024;
export const IDB_BATCH_OPERATIONS = 256;

/**
 * The session. Relaxed writes (`writeData`, `appendLog`, `writeLogHeader`) are queued and committed
 * together, in call order, in one transaction (review B1-p2c finding 6): at the next macrotask,
 * when the batch reaches `IDB_BATCH_BYTES` or `IDB_BATCH_OPERATIONS` (then the call waits for it:
 * backpressure), before any read, and inside the same transaction as a barrier (`publish`,
 * `flushLog`), so a barrier commits everything queued before it, atomically and in order. A
 * queued write's failure (quota, a moved epoch) fails the session: every later mutation rejects
 * with it, and nothing of the batch applied; reads go on. Releases, truncation and reset are
 * barriers on the same chain. The queue holds copies (at most one batch, ≤ 4 MiB, plus the batch
 * in flight) and the log's tail record (≤ 64 KiB).
 */
class IdbSession implements StoreSession {
  readonly epoch: number;
  /** Relaxed writes are queued until their batch commits (`transfer` saves the copy). */
  readonly keepsWrites = true;
  readonly #lease: IdbLease;
  readonly #data: ByteStore;
  readonly #log: ByteStore;
  readonly #batching: boolean;
  /** The data length including queued writes. */
  /**
   * The ends of data writes not yet committed: queued, and per batch in flight. The data length is
   * the stored end or any of these (a release may shrink the stored end meanwhile).
   */
  #queuedDataEnd = 0;
  readonly #pendingDataEnds = new Set<{ readonly end: number }>();
  #logLength: number;
  #closed = false;
  #queue: QueuedWrite[] = [];
  #queuedSpaces = new Set<ByteStore>();
  #queuedExtra = new Set<string>();
  #queuedBytes = 0;
  #scheduled = false;
  /** Counts drained batches (`LogTail.queued`). */
  #batch = 0;
  #logTail: LogTail | null = null;
  readonly #memory: CacheMemory | null;
  #tailCharged = false;
  /** Every commit runs after the previous one (call order). */
  #committing: Promise<void> = Promise.resolve();
  #failure: Error | null = null;

  constructor(
    lease: IdbLease,
    data: ByteStore,
    log: ByteStore,
    batching: boolean,
    memory: CacheMemory | null,
  ) {
    this.#lease = lease;
    this.epoch = lease.epoch;
    this.#data = data;
    this.#log = log;
    this.#batching = batching;
    this.#logLength = log.end;
    this.#memory = memory;
  }

  /**
   * Sets the log tail. Its 64 KiB buffer is charged to the memory registration while the session
   * holds one (review B1-p2e finding 3); a refused charge is not retried (the buffer is bounded).
   */
  #setTail(tail: LogTail | null): void {
    this.#logTail = tail;
    if (tail !== null && !this.#tailCharged && this.#memory !== null)
      this.#tailCharged = this.#memory.charge(LOG_COALESCE_BYTES);
    else if (tail === null && this.#tailCharged) {
      this.#memory?.release(LOG_COALESCE_BYTES);
      this.#tailCharged = false;
    }
  }

  #live(): void {
    if (this.#closed) throw new StoreClosedError();
  }

  /** Mutations also refuse once a batch failed (reads go on: the extent space still reads). */
  #mutable(): void {
    this.#live();
    if (this.#failure !== null) throw this.#failure;
  }

  /** Queues a relaxed write; waits only when the batch is full (or batching is off). */
  async #enqueue(
    spaces: readonly ByteStore[],
    extra: readonly string[],
    bytes: number,
    write: QueuedWrite,
  ): Promise<void> {
    this.#mutable();
    this.#lease.guard();
    this.#queue.push(write);
    for (const space of spaces) this.#queuedSpaces.add(space);
    for (const store of extra) this.#queuedExtra.add(store);
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
   * Commits everything queued, with `barrier` last in the same transaction when given (its
   * durability then applies). Resolves when that transaction completes.
   */
  #commit(barrier: Barrier | null = null): Promise<void> {
    const writes = this.#queue;
    const spaces = [...new Set([...this.#queuedSpaces, ...(barrier?.spaces ?? [])])];
    const extra = [...this.#queuedExtra, ...(barrier?.extra ?? [])];
    this.#queue = [];
    this.#queuedSpaces = new Set();
    this.#queuedExtra = new Set();
    this.#queuedBytes = 0;
    this.#batch += 1;
    const pending = { end: this.#queuedDataEnd };
    this.#queuedDataEnd = 0;
    this.#pendingDataEnds.add(pending);
    const all = barrier === null ? writes : [...writes, barrier.write];
    const run = this.#committing.then(async () => {
      if (this.#failure !== null) throw this.#failure;
      if (all.length === 0) return;
      try {
        await transact(
          this.#lease,
          spaces,
          barrier?.durability ?? "relaxed",
          (tx) => {
            inOrder(
              all.length,
              (index, done) => {
                all[index]?.(tx, done);
              },
              () => undefined,
            );
          },
          extra,
        );
      } catch (error) {
        this.#failure ??= error instanceof Error ? error : new Error(String(error));
        throw this.#failure;
      }
    });
    const settled = run.catch(() => undefined);
    void settled.then(() => this.#pendingDataEnds.delete(pending));
    this.#committing = settled;
    return run;
  }

  /**
   * A read: everything queued, and every mutation called before it, commits first, so it sees
   * every earlier write; its transaction checks the epoch (`IdbLease.read`).
   */
  async #read(stores: readonly string[], body: (tx: IdbTransactionLike) => void): Promise<void> {
    this.#live();
    if (this.#failure === null) await this.#commit();
    await this.#lease.read(stores, body);
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
      durability: "strict",
      spaces: [],
      extra: ["roots"],
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

  async writeData(offset: number, bytes: Uint8Array, options?: WriteDataOptions): Promise<void> {
    this.#mutable();
    if (offset % 4096 !== 0 || offset < 0) throw new RangeError("Unaligned data extent");
    if (bytes.length === 0) return;
    // A transferred standalone buffer is the caller's no longer: queue it as it is (no copy).
    const whole = bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength;
    const copy = options?.transfer === true && whole ? bytes : copyOf(bytes);
    this.#queuedDataEnd = Math.max(this.#queuedDataEnd, offset + bytes.length);
    await this.#enqueue([this.#data], [], bytes.length, (tx, next) => {
      this.#data.write(tx, offset, copy, next);
    });
  }

  #currentDataLength(): number {
    let length = Math.max(this.#data.end, this.#queuedDataEnd);
    for (const { end } of this.#pendingDataEnds) length = Math.max(length, end);
    return length;
  }

  #checkData(offset: number, length: number): void {
    if (offset < 0 || length < 0 || offset + length > this.#currentDataLength())
      throw new StoreCorruptionError("Read outside the data space");
  }

  async readData(ranges: readonly DataRange[]): Promise<Uint8Array[]> {
    this.#live();
    for (const { offset, length } of ranges) this.#checkData(offset, length);
    const results = ranges.map(({ length }) => new Uint8Array(length));
    await this.#read([this.#data.name, this.#data.indexName], (tx) => {
      ranges.forEach(({ offset }, index) => {
        this.#data.read(tx, offset, results[index] ?? new Uint8Array(0), () => undefined);
      });
    });
    return results;
  }

  async readDataInto(offset: number, target: Uint8Array): Promise<void> {
    this.#live();
    this.#checkData(offset, target.length);
    await this.#read([this.#data.name, this.#data.indexName], (tx) => {
      this.#data.read(tx, offset, target, () => undefined);
    });
  }

  /**
   * Deletes the ranges' records (trimming straddlers) on the commit chain. Releasing the tail
   * shrinks the data length, but never below a write still queued or in flight.
   */
  async releaseData(ranges: readonly DataRange[]): Promise<void> {
    this.#mutable();
    this.#lease.guard();
    // Every chunk joins the commit chain now, in call order; a task runs between chunks.
    const runs = chunksOf(ranges.map(({ offset, length }) => ({ offset, length }))).map(
      (chunk, index) => {
        if (index > 0) this.#committing = this.#committing.then(yieldTask);
        return this.#commit({
          durability: "relaxed",
          spaces: [this.#data],
          extra: [],
          write: (tx, done) => {
            inOrder(
              chunk.length,
              (at, next) => {
                const range = chunk[at];
                if (range === undefined) next();
                else this.#data.clear(tx, range.offset, range.offset + range.length, next);
              },
              done,
            );
          },
        });
      },
    );
    await Promise.all(runs);
  }

  async dataLength(): Promise<number> {
    this.#live();
    return this.#currentDataLength();
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
    await this.#read([this.#log.name, this.#log.indexName], (tx) => {
      this.#log.read(tx, offset, target, () => undefined);
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

  /** Queues the bytes; a small append that continues the tail record joins it ("Log records"). */
  async appendLog(bytes: Uint8Array): Promise<number> {
    this.#mutable();
    const offset = this.#logLength;
    if (bytes.length === 0) return offset;
    this.#logLength += bytes.length;
    const tail = this.#logTail;
    if (
      tail !== null &&
      tail.offset + tail.length === offset &&
      tail.length + bytes.length <= tail.buffer.length
    ) {
      this.#lease.guard();
      tail.buffer.set(bytes, tail.length);
      tail.length += bytes.length;
      if (tail.queued === this.#batch) {
        this.#queuedBytes += bytes.length;
        return offset;
      }
      tail.queued = this.#batch;
      await this.#enqueue([this.#log], [], bytes.length, this.#rewrite(tail));
      return offset;
    }
    if (bytes.length >= LOG_COALESCE_BYTES) {
      this.#setTail(null);
      const copy = copyOf(bytes);
      await this.#enqueue([this.#log], [], bytes.length, (tx, next) => {
        this.#log.write(tx, offset, copy, next);
      });
      return offset;
    }
    const started: LogTail = {
      offset,
      buffer: new Uint8Array(LOG_COALESCE_BYTES),
      length: bytes.length,
      queued: this.#batch,
    };
    started.buffer.set(bytes);
    this.#setTail(started);
    await this.#enqueue([this.#log], [], bytes.length, this.#rewrite(started));
    return offset;
  }

  /** Writes the tail record whole, as long as it is when the transaction runs. */
  #rewrite(tail: LogTail): QueuedWrite {
    return (tx, next) => {
      this.#log.write(tx, tail.offset, tail.buffer.slice(0, tail.length), next);
    };
  }

  async writeLogHeader(slot: 0 | 1, bytes: Uint8Array): Promise<void> {
    this.#mutable();
    if (bytes.length > LOG_HEADER_SLOT_BYTES) throw new RangeError("Log header slot overflow");
    const padded = new Uint8Array(LOG_HEADER_SLOT_BYTES);
    padded.set(bytes);
    await this.#enqueue([], [], LOG_HEADER_SLOT_BYTES, (tx, next) => {
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
      durability: "strict",
      spaces: [],
      extra: [],
      write: (tx, next) => {
        tx.store("meta").put("flush", length);
        next();
      },
    });
  }

  async truncateLog(length: number): Promise<void> {
    this.#mutable();
    if (length < 0 || length > this.#logLength) throw new RangeError("Truncate outside the log");
    this.#lease.guard();
    // The logical end moves now, in call order with appends; a failure below fails the session.
    this.#logLength = length;
    this.#setTail(null);
    await this.#commit({
      durability: "strict",
      spaces: [this.#log],
      extra: [],
      write: (tx, next) => {
        this.#log.clear(tx, length, Infinity, next);
      },
    });
  }

  async resetLog(): Promise<void> {
    this.#mutable();
    this.#lease.guard();
    this.#logLength = 0;
    this.#setTail(null);
    await this.#commit({
      durability: "strict",
      spaces: [this.#log],
      extra: [],
      write: (tx, next) => {
        tx.store("meta").delete("header-0");
        tx.store("meta").delete("header-1");
        this.#log.reset(tx, next);
      },
    });
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    await this.#commit().catch(() => undefined);
    this.#closed = true;
    this.#data.close();
    this.#log.close();
    this.#setTail(null);
  }
}

const slotBytes = (value: unknown) => (value instanceof ArrayBuffer ? new Uint8Array(value) : null);

/** The spill store under the epoch fence: a byte space in `spill`, relaxed, never recovered. */
class IdbScratch implements ScratchSpace {
  readonly #lease: IdbLease;
  #space: Promise<ByteStore> | null = null;
  /** Mutations run one after another, so the in-memory index changes in call order. */
  #chain: Promise<void> = Promise.resolve();
  /** The spill service's registration, charged with the record index (`chargeTo`). */
  #memory: CacheMemory | null = null;

  constructor(lease: IdbLease) {
    this.#lease = lease;
  }

  #spill(): Promise<ByteStore> {
    this.#space ??= ByteStore.load(this.#lease.database, "spill", this.#memory);
    return this.#space;
  }

  /** Charges the spill record index to `memory` from now on (before first use). */
  chargeTo(memory: CacheMemory): void {
    if (this.#space !== null) throw new TypeError("chargeTo after the spill space was opened");
    this.#memory = memory;
  }

  /**
   * Drops the resident index and returns its charge once every mutation has settled; the next
   * use loads it again (charged again). The spill service calls this at sweep and close.
   */
  async dispose(): Promise<void> {
    const disposing = this.#chain.then(async () => {
      const space = this.#space;
      this.#space = null;
      if (space !== null) (await space.catch(() => null))?.close();
    });
    this.#chain = disposing.catch(() => undefined);
    await disposing;
  }

  #mutate(body: (spill: ByteStore, tx: IdbTransactionLike) => void): Promise<void> {
    const run = this.#chain.then(async () => {
      const spill = await this.#spill();
      await transact(this.#lease, [spill], "relaxed", (tx) => {
        body(spill, tx);
      });
    });
    this.#chain = run.catch(() => undefined);
    return run;
  }

  async write(offset: number, bytes: Uint8Array): Promise<void> {
    if (offset % 4096 !== 0 || offset < 0) throw new RangeError("Unaligned scratch extent");
    const copy = copyOf(bytes);
    await this.#mutate((spill, tx) => {
      spill.write(tx, offset, copy, () => undefined);
    });
  }

  async readInto(offset: number, target: Uint8Array): Promise<void> {
    this.#lease.guard();
    const spill = await this.#spill();
    if (offset < 0 || offset + target.length > spill.end)
      throw new StoreCorruptionError("Read outside the scratch space");
    await this.#lease.read([spill.name, spill.indexName], (tx) => {
      spill.read(tx, offset, target, () => undefined);
    });
  }

  /** Chunked like `releaseData` (`RELEASE_CHUNK`, a task between transactions). */
  async release(ranges: readonly DataRange[]): Promise<void> {
    const runs = chunksOf(ranges.map(({ offset, length }) => ({ offset, length }))).map(
      (chunk, index) => {
        if (index > 0) this.#chain = this.#chain.then(yieldTask);
        return this.#mutate((spill, tx) => {
          inOrder(
            chunk.length,
            (at, next) => {
              const range = chunk[at];
              if (range === undefined) next();
              else spill.clear(tx, range.offset, range.offset + range.length, next);
            },
            () => undefined,
          );
        });
      },
    );
    await Promise.all(runs);
  }

  async truncate(size: number): Promise<void> {
    await this.#mutate((spill, tx) => {
      spill.clear(tx, size, Infinity, () => undefined);
    });
  }
}

/** The IndexedDB `StorageBackend` (epoch fence, spill store). */
export function createIndexedDbBackend(options: IndexedDbBackendOptions): StorageBackend {
  const leases = new WeakSet<IdbLease>();
  const serialize = options.serializeTransactions ?? true;
  const drainMs = options.releaseDrainMs ?? IDB_CLOSE_DRAIN_MS;
  const settleMs = options.releaseSettleMs ?? IDB_ABORT_SETTLE_MS;
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
          // Every acquisition its own connection: releasing an obsolete lease closes only its own.
          const db = new TrackedConnection(
            await options.database(),
            serialize,
            options.observe ?? null,
          );
          let epoch = 0;
          // The fence: one strict transaction moves the durable epoch past every older owner's.
          await db
            .transaction(["meta"], "readwrite", "strict", (tx) => {
              const meta = tx.store("meta");
              meta.get("epoch", (stored) => {
                // Missing: a new store. Anything else must be a counter that can still increase
                // (review B1-p2c finding 1): a malformed or exhausted one would let two owners share
                // an epoch, so ownership is refused instead.
                if (stored === undefined) epoch = 1;
                else if (
                  typeof stored !== "number" ||
                  !Number.isSafeInteger(stored) ||
                  stored < 1
                ) {
                  tx.abort(new StoreCorruptionError("The epoch record is malformed"));
                  return;
                } else if (!Number.isSafeInteger(stored + 1)) {
                  tx.abort(new StoreCorruptionError("The epoch counter is exhausted"));
                  return;
                } else epoch = stored + 1;
                meta.put("epoch", epoch);
              });
            })
            .catch((error: unknown) => {
              db.close();
              throw error;
            });
          return { db, epoch };
        },
        (fence) => {
          fence.db.close();
        },
        acquire,
      );
      const connection = fence.db;
      const lease = new IdbLease(lock, connection, fence.epoch, () =>
        connection.drainAndClose(drainMs, settleMs),
      );
      leases.add(lease);
      return lease;
    },
    async open(lease: OwnerLease): Promise<StoreSession> {
      const owned = own(lease);
      owned.guard();
      // The record indexes of both byte spaces, loaded and validated once (16 bytes per
      // ≤ 256 KiB record), charged to the owner's memory registration.
      const memory = options.memory ?? null;
      const data = await ByteStore.load(owned.database, "data", memory);
      const log = await ByteStore.load(owned.database, "log", memory).catch((error: unknown) => {
        data.close();
        throw error;
      });
      return new IdbSession(owned, data, log, options.batching ?? true, memory);
    },
    scratch(lease: OwnerLease): ScratchSpace {
      return new IdbScratch(own(lease));
    },
  };
}
