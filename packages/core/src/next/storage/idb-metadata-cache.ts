/** Owner-private writeback: at most 32 entries and 1 MiB, including control/entry credit.
 * Never used for durable database records. Buffer charges are exact byte lengths; fixed
 * bookkeeping uses a conservative 256 bytes per entry plus 2048 bytes for the directory.
 * Cache buffers are standalone exact-length copies; reserve buffer + entry credit before copying.
 * Refused credit writes/reads through the caller's prepaid frame. Dirty eviction retains its
 * charge until the native write settles, then wakes the governor by returning that credit.
 */
import { StoreClosedError, type MetadataStore } from "./contract.js";
import type { CacheMemory, EvictableCacheLike } from "./extents.js";

export const IDB_METADATA_CACHE_ENTRIES = 32;
export const IDB_METADATA_CACHE_BYTES = 1024 * 1024;
export const IDB_METADATA_ENTRY_BYTES = 256;
export const IDB_METADATA_CONTROL_BYTES = 2048;

export interface MetadataCacheBacking extends MetadataStore {
  validate(page: number, bytes: Uint8Array, writing: boolean): void;
  /** Fills the full caller frame, returning the exact stored (possibly compact) byte length. */
  load(page: number, target: Uint8Array): Promise<number>;
  /** Native terminal callback can outlive the rejected logical promise on ReleaseUnsettledError. */
  writeRetained(page: number, bytes: Uint8Array, nativeSettled: () => void): Promise<void>;
}
interface Entry {
  readonly page: number;
  readonly bytes: Uint8Array;
  dirty: boolean;
  touched: number;
  pins: number;
  retired: boolean;
}

export class IdbMetadataCache implements MetadataStore, EvictableCacheLike {
  readonly #memory: CacheMemory;
  #entries: Map<number, Entry> | null = null;
  #bytes = 0;
  #detached = 0;
  #retiring = false;
  #unregistered = false;
  #queue: Promise<unknown> = Promise.resolve();
  #failure: Error | null = null;
  #evicting = false;
  #closing: Promise<void> | null = null;
  #closed = false;
  readonly #onLost: () => void;

  constructor(
    readonly backing: MetadataCacheBacking,
    register: (cache: EvictableCacheLike) => CacheMemory,
    readonly guard: () => void,
    readonly signal: AbortSignal,
  ) {
    this.#memory = register(this);
    this.#onLost = () => {
      void this.close().catch(() => undefined);
    };
    if (signal.aborted) this.#onLost();
    else signal.addEventListener("abort", this.#onLost, { once: true });
  }
  get residentBytes(): number {
    return this.#bytes;
  }
  get residentEntries(): number {
    return (this.#entries?.size ?? 0) + this.#detached;
  }
  get evictableBytes(): number {
    return this.#closed || this.#evicting ? 0 : this.#bytes;
  }
  get oldestUnusedAt(): number | undefined {
    return this.#entries?.values().next().value?.touched;
  }

  #live(): void {
    this.guard();
    if (this.#closed) throw new StoreClosedError("Metadata cache closed");
  }
  #serial<T>(work: () => Promise<T>, retry = false, sticky = false): Promise<T> {
    const run = this.#queue.then(async () => {
      if (!retry && this.#failure !== null) throw this.#failure;
      try {
        // Accepted work may have waited behind I/O while the lease was fenced.
        this.guard();
        return await work();
      } catch (error) {
        if (sticky) this.#failure = error instanceof Error ? error : new Error(String(error));
        throw error;
      }
    });
    this.#queue = run.catch(() => undefined);
    return run;
  }
  #oldest(): Entry | undefined {
    return this.#entries?.values().next().value;
  }
  #touch(entry: Entry): void {
    this.#entries?.delete(entry.page);
    this.#entries?.set(entry.page, entry);
    entry.touched = performance.now();
  }
  #finishMemory(): void {
    if (this.#retiring && this.#bytes === 0 && !this.#unregistered) {
      this.#unregistered = true;
      this.#memory.unregister?.();
    }
  }
  #releaseEntry(entry: Entry): void {
    let released = entry.bytes.byteLength + IDB_METADATA_ENTRY_BYTES;
    this.#bytes -= released;
    if (this.#bytes === IDB_METADATA_CONTROL_BYTES) {
      released += IDB_METADATA_CONTROL_BYTES;
      this.#bytes = 0;
    }
    this.#memory.release(released);
    this.#finishMemory();
  }
  #retire(entry: Entry): void {
    if (entry.retired) return;
    entry.retired = true;
    if (entry.pins === 0) this.#releaseEntry(entry);
    else this.#detached++;
  }
  #remove(entry: Entry): void {
    const entries = this.#entries;
    if (entries?.get(entry.page) !== entry) return;
    entries.delete(entry.page);
    if (entries.size === 0) this.#entries = null;
    this.#retire(entry);
  }
  #discard(): void {
    const entries = this.#entries;
    this.#entries = null;
    if (entries !== null) for (const entry of entries.values()) this.#retire(entry);
  }
  async #flush(entry: Entry): Promise<void> {
    if (!entry.dirty) return;
    entry.pins++;
    let held = true;
    await this.backing.writeRetained(entry.page, entry.bytes, () => {
      if (!held) return;
      held = false;
      entry.pins--;
      if (entry.retired && entry.pins === 0) {
        this.#detached--;
        this.#releaseEntry(entry);
      }
    });
    entry.dirty = false;
  }
  /** Returns synchronously freed bytes. A dirty victim starts bounded asynchronous progress. */
  evict(bytes: number): number {
    if (this.#closed || this.#evicting || bytes <= 0) return 0;
    const entry = this.#oldest();
    if (entry === undefined) return 0;
    if (!entry.dirty) {
      const before = this.#bytes;
      this.#remove(entry);
      return before - this.#bytes;
    }
    this.#evicting = true;
    void this.#serial(
      async () => {
        const victim = this.#oldest();
        if (victim !== undefined) {
          await this.#flush(victim);
          this.#remove(victim);
        }
      },
      false,
      true,
    ).then(
      () => {
        this.#evicting = false;
        this.#memory.release(0);
      },
      () => {
        // Native failure is sticky. The failed buffer is no longer borrowed by a transaction.
        this.#evicting = false;
        this.#discard();
        this.#memory.release(0);
      },
    );
    return 0;
  }
  async #retain(page: number, bytes: Uint8Array, dirty: boolean): Promise<boolean> {
    const current = this.#entries?.get(page);
    if (current?.bytes.byteLength === bytes.byteLength) {
      current.bytes.set(bytes);
      current.dirty = dirty;
      this.#touch(current);
      return true;
    }
    // Persist the acknowledged old version before replacing its allocation. A refused copy
    // may write through, and that native write can fail without losing the earlier value.
    if (current !== undefined) {
      await this.#flush(current);
      this.#remove(current);
    }
    const cost = bytes.byteLength + IDB_METADATA_ENTRY_BYTES;
    while (
      this.residentEntries >= IDB_METADATA_CACHE_ENTRIES ||
      this.#bytes + cost + (this.#bytes === 0 ? IDB_METADATA_CONTROL_BYTES : 0) >
        IDB_METADATA_CACHE_BYTES
    ) {
      const victim = this.#oldest();
      if (victim === undefined || (!dirty && victim.dirty)) return false;
      await this.#flush(victim);
      this.#remove(victim);
    }
    const charge = cost + (this.#bytes === 0 ? IDB_METADATA_CONTROL_BYTES : 0);
    if (!this.#memory.charge(charge)) return false;
    try {
      const owned = new Uint8Array(bytes.byteLength);
      owned.set(bytes);
      this.#entries ??= new Map();
      this.#entries.set(page, {
        page,
        bytes: owned,
        dirty,
        touched: performance.now(),
        pins: 0,
        retired: false,
      });
      this.#bytes += charge;
      return true;
    } catch (error) {
      if (this.#entries?.size === 0) this.#entries = null;
      this.#memory.release(charge);
      throw error;
    }
  }
  async read(page: number, target: Uint8Array): Promise<void> {
    this.#live();
    this.backing.validate(page, target, false);
    await this.#serial(async () => {
      const entry = this.#entries?.get(page);
      if (entry !== undefined) {
        target.fill(0);
        target.set(entry.bytes);
        this.#touch(entry);
        return;
      }
      const length = await this.backing.load(page, target);
      await this.#retain(page, target.subarray(0, length), false);
    });
  }
  async write(page: number, bytes: Uint8Array): Promise<void> {
    this.#live();
    this.backing.validate(page, bytes, true);
    await this.#serial(async () => {
      if (!(await this.#retain(page, bytes, true))) await this.backing.write(page, bytes);
    });
  }
  async reset(): Promise<void> {
    this.#live();
    await this.#serial(
      async () => {
        this.#discard();
        await this.backing.reset();
        this.#failure = null;
      },
      true,
      true,
    );
  }
  async settled(): Promise<void> {
    if (this.signal.aborted) {
      await this.close();
      return;
    }
    await this.#queue;
    if (this.#failure !== null) throw this.#failure;
  }

  /** Last session closed. Keep the empty lease-scoped control available for a concurrent open;
   * its next operations queue behind this flush/drop, so they cannot read an older native page. */
  async idle(): Promise<void> {
    if (this.signal.aborted) {
      await this.close();
      return;
    }
    await this.#serial(
      async () => {
        try {
          if (this.#entries !== null)
            for (const entry of this.#entries.values()) await this.#flush(entry);
        } finally {
          this.#discard();
        }
      },
      false,
      true,
    );
  }

  close(): Promise<void> {
    if (this.#closing !== null) return this.#closing;
    this.#closed = true;
    this.signal.removeEventListener("abort", this.#onLost);
    this.#closing = this.#queue.then(async () => {
      try {
        if (!this.signal.aborted) {
          if (this.#failure !== null) throw this.#failure;
          if (this.#entries !== null)
            for (const entry of this.#entries.values()) await this.#flush(entry);
        }
      } catch (error) {
        if (!this.signal.aborted) throw error;
      } finally {
        this.#retiring = true;
        this.#discard();
        this.#finishMemory();
      }
    });
    return this.#closing;
  }
}
