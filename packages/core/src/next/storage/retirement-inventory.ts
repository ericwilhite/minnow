/** Replays B1/B2's immutable MetadataJournal inventory through one charged window.
 * Generic preparation uses captureAudit's cooperative producer boundary, not a second store.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import { DATA_ALIGN, StoreCorruptionError } from "./contract.js";
import { ReadDeniedError, type ByteBudget } from "./extents.js";
import type { RetirementInventory, RetirementCursor } from "./retirement-contract.js";
import { METADATA_PAYLOAD_BYTES, type ChainPosition } from "./metadata-journal.js";
import { validateExtentPair } from "./metadata-extents.js";
export const RETIREMENT_OWNER_BYTES = 4096;
export const RETIREMENT_CURSOR_BYTES = METADATA_PAYLOAD_BYTES + 4096;
export interface InventoryContext {
  readonly owner: object;
  /** Only a scalar position escapes; native frames retire before this resolves. */
  read(
    handle: number,
    position: ChainPosition | undefined,
    target: Uint8Array,
    work: TaskWork,
  ): Promise<{ readonly position: ChainPosition; readonly bytes: number }>;
  dispose(handle: number): Promise<void>;
  finished(inventory: RetirementInventory): void;
}
export class RetirementClosedError extends Error {
  readonly code = "MINNOW_RETIREMENT_CLOSED";
  constructor() {
    super("The retirement inventory is closed");
    this.name = "RetirementClosedError";
  }
}
class Cursor implements RetirementCursor {
  offset = 0;
  length = 0;
  #window: Uint8Array | undefined;
  #view: DataView | undefined;
  #at = 0;
  #end = 0;
  #position: ChainPosition | undefined;
  #closed = false;
  #freed = false;
  #pending: Promise<boolean> | null = null;
  #closing: Promise<void> | null = null;
  #last = -1;
  #lastLength = 0;
  #steps = 0;
  #consumed = 0;
  readonly #stop = new AbortController();
  readonly #signal: AbortSignal;
  constructor(
    readonly inventory: Inventory,
    readonly budget: ByteBudget,
    signal?: AbortSignal,
  ) {
    // The factory charges BEFORE constructing this cursor, its controller or window.
    // The prepaid allowance remains the maximum. Small immutable inventories
    // cannot fill a page: avoid allocating a full frame for every staged part.
    this.#window = new Uint8Array(Math.min(METADATA_PAYLOAD_BYTES, inventory.count * 16));
    this.#view = new DataView(this.#window.buffer);
    this.#signal = AbortSignal.any(
      signal === undefined
        ? [this.#stop.signal, inventory.signal]
        : [signal, this.#stop.signal, inventory.signal],
    );
  }
  next(work: TaskWork = {}): boolean | Promise<boolean> {
    if (this.#closed) return false;
    this.#signal.throwIfAborted();
    work.signal?.throwIfAborted();
    if (this.#pending !== null) throw new TypeError("Concurrent retirement cursor advances");
    const pace =
      this.#steps++ % 128 === 0 ? pauseIfDue({ ...work, signal: this.#signal }) : undefined;
    const result = pace === undefined ? this.#advance(work) : pace.then(() => this.#advance(work));
    if (typeof result === "boolean") return result;
    const pending = result.finally(() => {
      this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  #advance(work: TaskWork): boolean | Promise<boolean> {
    work.signal?.throwIfAborted();
    this.#signal.throwIfAborted();
    if (this.#closed) throw new RetirementClosedError();
    if (this.#at < this.#end) return this.#consume();
    if (this.inventory.count === 0 || this.#position?.remaining === 0) {
      if (this.#consumed !== this.inventory.count)
        throw new StoreCorruptionError("Short retirement inventory");
      this.#closed = true;
      this.#free();
      return false;
    }
    return this.#read(work);
  }
  async #read(work: TaskWork): Promise<boolean> {
    {
      const signal =
        work.signal === undefined || work.signal === this.#signal
          ? this.#signal
          : AbortSignal.any([work.signal, this.#signal]);
      if (this.#window === undefined) throw new RetirementClosedError();
      const loaded = await this.inventory.context.read(
        this.inventory.handle,
        this.#position,
        this.#window,
        { ...work, signal },
      );
      signal.throwIfAborted();
      if (this.#closed) throw new RetirementClosedError();
      if (
        !Number.isSafeInteger(loaded.bytes) ||
        loaded.bytes < 0 ||
        loaded.bytes > METADATA_PAYLOAD_BYTES ||
        loaded.bytes % 16 !== 0
      )
        throw new StoreCorruptionError("Invalid retirement window length");
      this.#position = loaded.position;
      this.#at = 0;
      this.#end = loaded.bytes;
      if (this.#end === 0) {
        if (this.#consumed !== this.inventory.count)
          throw new StoreCorruptionError("Short retirement inventory");
        this.#closed = true;
        this.#free();
        return false;
      }
      return this.#consume();
    }
  }
  #consume(): boolean {
    if (this.#view === undefined) throw new RetirementClosedError();
    this.offset = this.#view.getFloat64(this.#at, true);
    this.length = this.#view.getFloat64(this.#at + 8, true);
    this.#at += 16;
    validateExtentPair(this.offset, this.length);
    if (this.offset < this.#last + Math.ceil(this.#lastLength / DATA_ALIGN) * DATA_ALIGN)
      throw new StoreCorruptionError("Unordered retirement inventory");
    if (++this.#consumed > this.inventory.count)
      throw new StoreCorruptionError("Retirement inventory exceeds its count");
    this.#last = this.offset;
    this.#lastLength = this.length;
    return true;
  }
  #free(): void {
    if (this.#freed) return;
    this.#freed = true;
    this.#view = undefined;
    this.#window = undefined;
    this.#position = undefined;
    this.inventory.cursorClosed(this);
    this.budget.free(RETIREMENT_CURSOR_BYTES);
  }
  close(): Promise<void> {
    if (this.#closing !== null) return this.#closing;
    this.#closed = true;
    this.#stop.abort(new RetirementClosedError());
    return (this.#closing = (async () => {
      try {
        await this.#pending;
      } catch {
        /* the pending next owns its error */
      } finally {
        this.#free();
      }
    })());
  }
}
class Inventory implements RetirementInventory {
  readonly #cursors = new Set<Cursor>();
  readonly #stop = new AbortController();
  get signal(): AbortSignal {
    return this.#stop.signal;
  }
  #publications = 0;
  #closed = false;
  #closing: Promise<void> | null = null;
  #published: Promise<void> | null = null;
  #finishPublished: (() => void) | null = null;
  constructor(
    readonly context: InventoryContext,
    readonly handle: number,
    readonly count: number,
    readonly budget: ByteBudget,
  ) {}
  #accepting(): void {
    if (this.#closed) throw new RetirementClosedError();
  }
  cursor(options: {
    readonly budget: ByteBudget;
    readonly signal?: AbortSignal;
  }): RetirementCursor {
    this.#accepting();
    options.signal?.throwIfAborted();
    if (!options.budget.allocate(RETIREMENT_CURSOR_BYTES))
      throw new ReadDeniedError(RETIREMENT_CURSOR_BYTES);
    try {
      this.#accepting();
      options.signal?.throwIfAborted();
      const cursor = new Cursor(this, options.budget, options.signal);
      this.#cursors.add(cursor);
      return cursor;
    } catch (error) {
      options.budget.free(RETIREMENT_CURSOR_BYTES);
      throw error;
    }
  }
  cursorClosed(cursor: Cursor): void {
    this.#cursors.delete(cursor);
  }
  borrow(owner: object): void {
    if (this.context.owner !== owner) throw new TypeError("Foreign retirement inventory");
    if (this.#closed) throw new RetirementClosedError();
    this.#publications++;
  }
  returned(): void {
    if (this.#publications === 0) throw new TypeError("Retirement publication returned twice");
    if (--this.#publications === 0) {
      this.#finishPublished?.();
      this.#finishPublished = null;
      this.#published = null;
    }
  }
  release(): Promise<void> {
    if (this.#closing !== null) return this.#closing;
    let resolve!: () => void, reject!: (error: unknown) => void;
    const closing = new Promise<void>((yes, no) => {
      resolve = yes;
      reject = no;
    });
    this.#closing = closing;
    this.#closed = true;
    this.#stop.abort(new RetirementClosedError());
    void this.#drain().then(resolve, reject);
    return closing;
  }
  async #drain(): Promise<void> {
    let first: Error | undefined;
    try {
      for (;;) {
        const cursor = this.#cursors.values().next().value;
        if (cursor === undefined) break;
        try {
          await cursor.close();
        } catch (error) {
          first ??= error instanceof Error ? error : new Error(String(error));
        }
      }
      if (this.#publications !== 0)
        await (this.#published ??= new Promise<void>((resolve) => {
          this.#finishPublished = resolve;
        }));
      try {
        await this.context.dispose(this.handle);
      } catch (error) {
        first ??= error instanceof Error ? error : new Error(String(error));
      }
      if (first !== undefined) throw first;
    } finally {
      this.context.finished(this);
      this.budget.free(RETIREMENT_OWNER_BYTES);
    }
  }
}
/** Factory caller already owns RETIREMENT_OWNER_BYTES and the immutable journal reference. */
export function makeRetirementInventory(
  context: InventoryContext,
  handle: number,
  count: number,
  budget: ByteBudget,
): RetirementInventory {
  if (!Number.isSafeInteger(count) || count < 0)
    throw new StoreCorruptionError("Invalid retirement inventory count");
  return new Inventory(context, handle, count, budget);
}
/** One queued publication's descriptor; separately funded before allocation. */
export const RETIREMENT_BORROW_BYTES = 256;
export interface RetirementBorrow {
  readonly handle: number;
  release(): void;
}
class Borrow implements RetirementBorrow {
  constructor(
    private current: Inventory | undefined,
    private readonly budget: ByteBudget,
  ) {}
  get handle(): number {
    if (!this.current) throw new RetirementClosedError();
    return this.current.handle;
  }
  release(): void {
    if (!this.current) return;
    this.current.returned();
    this.current = undefined;
    this.budget.free(RETIREMENT_BORROW_BYTES);
  }
}
/** Acquire synchronously before a publication queues; release() drains this accepted use. */
export function borrowRetirementInventory(
  value: RetirementInventory,
  owner: object,
  budget: ByteBudget,
): RetirementBorrow {
  if (!(value instanceof Inventory)) throw new TypeError("Unprepared retirement inventory");
  if (!budget.allocate(RETIREMENT_BORROW_BYTES)) throw new ReadDeniedError(RETIREMENT_BORROW_BYTES);
  try {
    value.borrow(owner);
    return new Borrow(value, budget);
  } catch (error) {
    budget.free(RETIREMENT_BORROW_BYTES);
    throw error;
  }
}
