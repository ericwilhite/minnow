/**
 * A simulated disk, lock manager and backend for tests: the in-memory store, and a physical
 * fault model under the real store logic (file-store.ts).
 *
 * ## What it models
 * - **Files and handles.** Handles have `FileSystemSyncAccessHandle`'s shape. An exclusive open
 *   of a file another live handle holds throws a `NoModificationAllowedError`-named error (or,
 *   with `busyName: "InvalidStateError"`, WebKit's name). A closed or crashed handle throws
 *   `InvalidStateError`.
 * - **Web Locks.** FIFO grants; `steal` aborts the holder's `lost` signal and grants at once.
 * - **Crash (`powerLoss`)** at any moment or at a planned physical step (`crashAtStep` counts every
 *   write, truncate and flush on any file). Per file, the post-crash image is the image at its
 *   last flush plus a seeded choice for each later operation, in order: a write is kept, lost or
 *   torn (512-byte sectors mixed old/new, one sector cut mid-way); a truncate is applied or not.
 *   Handles die and locks are released.
 * - **Flush modes.** `honest`: a completed flush is durable. `lying`: a completed flush only
 *   queues its writes; at a crash each file keeps an in-order prefix of the queued writes and
 *   loses the rest, plus everything after (a write-back cache lost at power-off).
 *
 * ## What it does not model
 * Metadata-level corruption (a file vanishing), media errors on read, quota exhaustion, or
 * IndexedDB transaction semantics. These states are a subset of what the contract allows; the
 * contract's guarantee 4 is wider (any byte may revert independently).
 */
import { FileScratch, FileStoreSession, FILE_NAMES, type FileSet } from "./file-store.js";
import { acquireFenced, type HeldLock, type LockManagerLike } from "./fence.js";
import {
  OwnershipLostError,
  type AcquireOptions,
  type OwnerLease,
  type ScratchSpace,
  type StorageBackend,
  type StoreSession,
  type SyncFile,
} from "./contract.js";

export class SimulatedCrash extends Error {
  constructor() {
    super("Simulated crash");
    this.name = "SimulatedCrash";
  }
}

function namedError(name: string, message: string): Error {
  const error = new Error(message);
  error.name = name;
  return error;
}

/** mulberry32, stream-identical to testing/seeds.ts (published code cannot import that file). */
function generator(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296;
  };
}

type Operation =
  | { readonly kind: "write"; readonly at: number; readonly bytes: Uint8Array }
  | { readonly kind: "truncate"; readonly size: number };

interface FileState {
  current: Uint8Array;
  durable: Uint8Array;
  pending: Operation[];
  /** Spare capacity belongs only to the current append lineage, never a recovered branch. */
  append: Uint8Array | null;
  appendEnd: number;
  /** Lying mode: flushed but not yet durable. */
  queued: Operation[];
  readonly handles: Set<SimFile>;
}

function resized(bytes: Uint8Array, size: number): Uint8Array {
  if (size === bytes.length) return bytes;
  const out = new Uint8Array(size);
  out.set(bytes.subarray(0, Math.min(size, bytes.length)));
  return out;
}

function apply(image: Uint8Array, operation: Operation): Uint8Array {
  if (operation.kind === "truncate") return resized(image, operation.size);
  const end = operation.at + operation.bytes.length;
  const out = end > image.length ? resized(image, end) : image.slice();
  out.set(operation.bytes, operation.at);
  return out;
}

export interface SimDiskOptions {
  readonly seed?: number;
  readonly crashAtStep?: number;
  /** Called at the planned crash, before the crashing call throws (a test snapshots its state). */
  readonly onCrash?: () => void;
  readonly flush?: "honest" | "lying";
  /** The busy error name exclusive opens throw (WebKit uses InvalidStateError). */
  readonly busyName?: "NoModificationAllowedError" | "InvalidStateError";
}

export class SimDisk {
  readonly files = new Map<string, FileState>();
  /** IndexedDB-style durable epoch record (the epoch fence). */
  epoch = 0;
  steps = 0;
  /** Power losses so far: a simulated IndexedDB reloads its state from disk after each. */
  crashes = 0;
  readonly #options: SimDiskOptions;
  readonly #random: () => number;
  #armed = true;

  constructor(options: SimDiskOptions = {}) {
    this.#options = options;
    this.#random = generator(options.seed ?? 1);
  }

  #state(name: string): FileState {
    let state = this.files.get(name);
    if (state === undefined) {
      state = {
        current: new Uint8Array(0),
        durable: new Uint8Array(0),
        pending: [],
        append: null,
        appendEnd: 0,
        queued: [],
        handles: new Set(),
      };
      this.files.set(name, state);
    }
    return state;
  }

  open(name: string, exclusive: boolean): SimFile {
    const state = this.#state(name);
    const busy = exclusive
      ? state.handles.size > 0
      : [...state.handles].some((handle) => handle.exclusive);
    if (busy)
      throw namedError(this.#options.busyName ?? "NoModificationAllowedError", `${name} is open`);
    const handle = new SimFile(this, state, exclusive);
    state.handles.add(handle);
    return handle;
  }

  /** Counts one physical mutation; at the planned step, records it and crashes. */
  step(state: FileState, operation: Operation | null): void {
    const step = this.steps++;
    if (operation !== null) {
      if (operation.kind === "write" && operation.at >= state.current.length) {
        const end = operation.at + operation.bytes.length;
        let capacity = state.append;
        if (
          capacity?.buffer !== state.current.buffer ||
          state.current.byteOffset !== 0 ||
          state.appendEnd !== state.current.length ||
          end > capacity.length
        ) {
          capacity = new Uint8Array(Math.max(end, 4096, state.current.length * 2));
          capacity.set(state.current);
        }
        // Only bytes beyond every earlier view's end may change. Flushed prefixes and
        // previously returned file images remain immutable, as with copy-on-write.
        capacity.fill(0, state.current.length, operation.at);
        capacity.set(operation.bytes, operation.at);
        state.current = capacity.subarray(0, end);
        state.append = capacity;
        state.appendEnd = end;
      } else {
        state.current = apply(state.current, operation);
        state.append = null;
        state.appendEnd = 0;
      }
      state.pending.push(operation);
    }
    if (this.#armed && step === this.#options.crashAtStep) {
      this.#options.onCrash?.();
      this.powerLoss();
      throw new SimulatedCrash();
    }
  }

  flushFile(state: FileState): void {
    this.step(state, null);
    if (this.#options.flush === "lying") state.queued.push(...state.pending);
    else state.durable = state.current;
    state.pending = [];
  }

  /**
   * Crash now: resolve every file to one allowed post-crash image, kill handles. A power loss
   * consumes the planned crash: the next process's own writes (an OPFS acquisition writes its
   * epoch) never crash again.
   */
  powerLoss(): void {
    this.#armed = false;
    this.crashes += 1;
    const random = this.#random;
    for (const state of this.files.values()) {
      // Recovery forks from an older prefix. Its spare suffix can still belong to a
      // longer pre-crash image: never reuse it, even if every pending write is lost.
      state.append = null;
      state.appendEnd = 0;
      let image = state.durable;
      let survived = true;
      if (state.queued.length > 0) {
        const kept = Math.floor(random() * (state.queued.length + 1));
        for (const operation of state.queued.slice(0, kept)) image = apply(image, operation);
        survived = kept === state.queued.length;
      }
      if (survived)
        for (const operation of state.pending) {
          const outcome = random();
          if (operation.kind === "truncate") {
            if (outcome < 0.5) image = apply(image, operation);
          } else if (outcome < 2 / 3) {
            image = apply(image, outcome < 1 / 3 ? operation : this.#tear(image, operation));
          }
        }
      state.durable = image;
      state.current = image;
      state.pending = [];
      state.queued = [];
      for (const handle of state.handles) handle.dead = true;
      state.handles.clear();
    }
  }

  #tear(image: Uint8Array, operation: Extract<Operation, { kind: "write" }>): Operation {
    const before = new Uint8Array(operation.bytes.length);
    before.set(image.subarray(operation.at, operation.at + operation.bytes.length));
    const mixed = before.slice();
    const sectors = Math.ceil(operation.bytes.length / 512);
    const torn = Math.floor(this.#random() * sectors);
    for (let sector = 0; sector < sectors; sector++) {
      const start = sector * 512;
      const end = Math.min(operation.bytes.length, start + 512);
      if (sector === torn) {
        const cut = start + Math.floor(this.#random() * (end - start));
        mixed.set(operation.bytes.subarray(start, cut), start);
      } else if (this.#random() < 0.5) mixed.set(operation.bytes.subarray(start, end), start);
    }
    return { kind: "write", at: operation.at, bytes: mixed };
  }

  /** Detaches a handle (close, or the owner dropping it on lock loss). */
  detach(state: FileState, handle: SimFile): void {
    state.handles.delete(handle);
  }
}

export class SimFile implements SyncFile {
  dead = false;
  constructor(
    readonly disk: SimDisk,
    readonly state: FileState,
    readonly exclusive: boolean,
  ) {}

  #alive(): FileState {
    if (this.dead) throw namedError("InvalidStateError", "The handle is closed");
    return this.state;
  }

  read(buffer: Uint8Array, options: { at: number }): number {
    const current = this.#alive().current;
    const available = Math.max(0, Math.min(buffer.length, current.length - options.at));
    buffer.set(current.subarray(options.at, options.at + available));
    return available;
  }

  write(buffer: Uint8Array, options: { at: number }): number {
    this.disk.step(this.#alive(), { kind: "write", at: options.at, bytes: buffer.slice() });
    return buffer.length;
  }

  truncate(size: number): void {
    this.disk.step(this.#alive(), { kind: "truncate", size });
  }

  getSize(): number {
    return this.#alive().current.length;
  }

  flush(): void {
    this.disk.flushFile(this.#alive());
  }

  close(): void {
    if (this.dead) return;
    this.dead = true;
    this.disk.detach(this.state, this);
  }
}

interface LockState {
  holder: { readonly controller: AbortController } | null;
  readonly queue: Array<{ readonly grant: () => void }>;
}

/** An in-process Web Locks model: FIFO, steal, cancellable waits. */
export class SimLocks implements LockManagerLike {
  readonly #locks = new Map<string, LockState>();

  #state(name: string): LockState {
    let state = this.#locks.get(name);
    if (state === undefined) {
      state = { holder: null, queue: [] };
      this.#locks.set(name, state);
    }
    return state;
  }

  request(name: string, options: { steal?: boolean; signal?: AbortSignal }): Promise<HeldLock> {
    const state = this.#state(name);
    const make = (): HeldLock => {
      const holder = { controller: new AbortController() };
      state.holder = holder;
      return {
        lost: holder.controller.signal,
        release: () => {
          if (state.holder !== holder) return;
          state.holder = null;
          state.queue.shift()?.grant();
        },
      };
    };
    if (options.steal === true) {
      state.holder?.controller.abort(namedError("AbortError", "Lock stolen"));
      return Promise.resolve(make());
    }
    if (state.holder === null && state.queue.length === 0) return Promise.resolve(make());
    return new Promise((resolve, reject) => {
      const waiter = {
        grant: () => {
          options.signal?.removeEventListener("abort", cancel);
          resolve(make());
        },
      };
      const cancel = () => {
        const index = state.queue.indexOf(waiter);
        if (index >= 0) state.queue.splice(index, 1);
        reject(namedError("AbortError", "Lock request aborted"));
      };
      options.signal?.addEventListener("abort", cancel, { once: true });
      state.queue.push(waiter);
    });
  }

  /** Every holder vanishes (process restart after a crash). */
  reset(): void {
    for (const state of this.#locks.values()) {
      state.holder?.controller.abort(namedError("AbortError", "Holder died"));
      state.holder = null;
      while (state.queue.length > 0) state.queue.shift()?.grant();
    }
  }
}

/** A simulated owner's lease. `freeze` models a frozen tab: its loss handling waits for `thaw`. */
export class SimLease implements OwnerLease {
  readonly epoch: number;
  readonly files: FileSet;
  /** The spill scratch file, opened under the same fence (`StorageBackend.scratch`). */
  readonly scratch: SyncFile;
  readonly #lock: HeldLock;
  readonly #lost = new AbortController();
  readonly #disk: SimDisk;
  readonly #fence: "handle" | "epoch";
  #frozen = false;
  #lossPending = false;

  constructor(
    disk: SimDisk,
    fence: "handle" | "epoch",
    lock: HeldLock,
    files: FileSet,
    scratch: SyncFile,
    epoch: number,
  ) {
    this.#disk = disk;
    this.#fence = fence;
    this.#lock = lock;
    this.files = files;
    this.scratch = scratch;
    this.epoch = epoch;
    // An owner whose lock is stolen closes its handles immediately (probe: the handle is the fence).
    lock.lost.addEventListener("abort", () => {
      if (this.#frozen) this.#lossPending = true;
      else this.#lose();
    });
  }

  get signal(): AbortSignal {
    return this.#lost.signal;
  }

  #lose(): void {
    if (this.#lost.signal.aborted) return;
    this.#lost.abort(new OwnershipLostError(this.epoch));
    this.#close();
  }

  #close(): void {
    for (const file of [
      this.files.log,
      this.files.data,
      ...this.files.roots,
      this.scratch,
      this.files.metadata,
      this.files.metadataIndex,
    ])
      file?.close();
  }

  /** The ownership check every mutation runs in the step that applies its effect. */
  guard(): void {
    if (this.#lost.signal.aborted) throw new OwnershipLostError(this.epoch);
    if (this.#fence === "epoch" && this.#disk.epoch !== this.epoch) {
      this.#lose();
      throw new OwnershipLostError(this.epoch);
    }
  }

  freeze(): void {
    this.#frozen = true;
  }

  thaw(): void {
    this.#frozen = false;
    if (this.#lossPending) this.#lose();
  }

  async release(): Promise<void> {
    this.#close();
    if (!this.#lost.signal.aborted) this.#lost.abort(new OwnershipLostError(this.epoch));
    this.#lock.release();
  }
}

/** A storage backend over a simulated disk, with either fence. */
export class SimBackend implements StorageBackend {
  readonly locks: SimLocks;
  readonly name: string;
  readonly #openDelay: (() => Promise<void>) | null;
  readonly #leases = new WeakSet<SimLease>();

  /**
   * `openDelay` makes the fence open asynchronous, as `createSyncAccessHandle()` and an IndexedDB
   * epoch transaction are: it runs before the files open.
   */
  constructor(
    readonly disk: SimDisk,
    readonly fence: "handle" | "epoch" = "handle",
    options: { locks?: SimLocks; name?: string; openDelay?: () => Promise<void> } = {},
  ) {
    this.locks = options.locks ?? new SimLocks();
    this.name = options.name ?? "minnow:db";
    this.#openDelay = options.openDelay ?? null;
  }

  #openFiles(exclusive: boolean): { files: FileSet; scratch: SimFile } {
    const opened: SimFile[] = [];
    try {
      for (const name of [...FILE_NAMES, "spill", "space-meta", "space-meta-index"])
        opened.push(this.disk.open(name, exclusive));
    } catch (error) {
      for (const file of opened) file.close();
      throw error;
    }
    const [log, data, root0, root1, scratch, metadata, metadataIndex] = opened as [
      SimFile,
      SimFile,
      SimFile,
      SimFile,
      SimFile,
      SimFile,
      SyncFile,
    ];
    return { files: { log, data, roots: [root0, root1], metadata, metadataIndex }, scratch };
  }

  async acquire(options: AcquireOptions = {}): Promise<SimLease> {
    const exclusive = this.fence === "handle";
    const { lock, fence } = await acquireFenced(
      this.locks,
      this.name,
      async () => {
        if (this.#openDelay !== null) await this.#openDelay();
        const { files, scratch } = this.#openFiles(exclusive);
        // The epoch fence's durable bump (IndexedDB: one transaction); informational for handles.
        return { files, scratch, epoch: ++this.disk.epoch };
      },
      ({ files, scratch }) => {
        for (const file of [
          files.log,
          files.data,
          ...files.roots,
          scratch,
          files.metadata,
          files.metadataIndex,
        ])
          file?.close();
      },
      options,
    );
    const lease = new SimLease(
      this.disk,
      this.fence,
      lock,
      fence.files,
      fence.scratch,
      fence.epoch,
    );
    this.#leases.add(lease);
    return lease;
  }

  async open(lease: OwnerLease): Promise<StoreSession> {
    if (!(lease instanceof SimLease) || !this.#leases.has(lease))
      throw new TypeError("The lease was not acquired from this backend");
    lease.guard();
    return new FileStoreSession(lease.files, lease.epoch, () => {
      lease.guard();
    });
  }

  /** The `spill` file under the lease's guard: the handle fence, or the epoch check (IndexedDB). */
  scratch(lease: OwnerLease): ScratchSpace | null {
    if (!(lease instanceof SimLease) || !this.#leases.has(lease))
      throw new TypeError("The lease was not acquired from this backend");
    return new FileScratch(lease.scratch, () => {
      lease.guard();
    });
  }

  /** Process restart after `disk.powerLoss()`: every lock holder is gone. */
  restart(): void {
    this.locks.reset();
  }
}
