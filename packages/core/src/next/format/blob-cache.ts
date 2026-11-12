/**
 * A cache of values decoded from verified blobs, keyed by the blob (`(address, length,
 * checksum)`): parsed chunk directories (ruling R15.7), inflated gzip pages (R15.5), and any
 * other decoded metadata a holder wants to keep across readers (group roots, directory pages).
 *
 * Accounting follows the space's lease rule (R5):
 * - An entry a holder uses is **pinned**: its size is charged to that holder's budget (the holder
 *   charges it, before it decodes on a miss, or when a pin succeeds on a hit), not to the cache,
 *   and it cannot be evicted.
 * - When the last holder lets go, the cache charges the entry through its governor handle
 *   (`CacheMemory.charge`); if the governor has no room, the entry is dropped.
 * - Unpinned entries are evictable (`EvictableCacheLike`), least recently released first; the
 *   governor evicts them before any grant grows or any operator spills, across every registered
 *   cache by `oldestUnusedAt` (memory/governor.ts rule 6).
 * One holder may pin an entry several times; each `pin` or `admit` is undone by one `unpin`.
 *
 * A one-pass scan's pin (`{ once: true }`, storage/extents.ts `ReadOptions.once`) is not a use
 * (review B2-p2h #2): while every pin of an entry is one, the entry keeps its place in the
 * eviction order and its `usedAt` (it is skipped while pinned), and the last such unpin charges it
 * again where it is. An ordinary pin meanwhile counts as usual. Unpin with the same option.
 */
import {
  blobAddress,
  type BlobRef,
  type CacheMemory,
  type EvictableCacheLike,
} from "../storage/extents.js";

interface Entry<T> {
  readonly value: T;
  readonly size: number;
  pins: number;
  /** Pins held by one-pass scans; while all pins are, the entry stays in the eviction order. */
  oncePins: number;
  /** Charged to the cache (only while unpinned). */
  charged: boolean;
  /** When its last holder let go (`performance.now()`). */
  usedAt: number;
}

/** Options of `pin` and `unpin`: a one-pass scan's pin is not a use. */
export interface PinOptions {
  readonly once?: boolean;
}

/** A pinned cache entry: its value and the bytes its holder must have charged. */
export interface Pinned<T> {
  readonly value: T;
  readonly size: number;
}

export const blobKey = (ref: BlobRef): string =>
  `${String(blobAddress(ref))}:${String(ref.length)}:${String(ref.checksum)}`;

export class BlobCache<T> implements EvictableCacheLike {
  readonly #memory: CacheMemory;
  readonly #entries = new Map<string, Entry<T>>();
  /**
   * The evictable entries in release order, plus those pinned only by one-pass scans, which keep
   * their place here (skipped by eviction while pinned).
   */
  readonly #unpinned = new Map<string, Entry<T>>();
  #evictable = 0;
  #resident = 0;
  hits = 0;
  misses = 0;

  /** `register` is the governor's `registerCache` (memory/governor.ts). */
  constructor(
    register: (cache: EvictableCacheLike) => CacheMemory,
    readonly key: (ref: BlobRef) => string = blobKey,
  ) {
    this.#memory = register(this);
  }

  get evictableBytes(): number {
    return this.#evictable;
  }

  /** Bytes resident, pinned or not. */
  get residentBytes(): number {
    return this.#resident;
  }

  get entryCount(): number {
    return this.#entries.size;
  }

  /** When the entry `evict` takes next was released (the governor's cross-cache order). */
  get oldestUnusedAt(): number | undefined {
    for (const entry of this.#unpinned.values()) if (entry.pins === 0) return entry.usedAt;
    return undefined;
  }

  evict(bytes: number): number {
    let freed = 0;
    for (const [key, entry] of this.#unpinned) {
      if (freed >= bytes) break;
      if (entry.pins > 0) continue;
      this.#unpinned.delete(key);
      this.#entries.delete(key);
      this.#resident -= entry.size;
      this.#evictable -= entry.size;
      freed += entry.size;
      // Unpinned entries are always charged (unpin drops the ones the governor refused). Released
      // last: the governor may call back into `evict`, which then sees a consistent cache.
      this.#memory.release(entry.size);
    }
    return freed;
  }

  /** The entry for `ref`, pinned for the caller, or null. */
  pin(ref: BlobRef, options: PinOptions = {}): Pinned<T> | null {
    const key = this.key(ref);
    const entry = this.#entries.get(key);
    if (entry === undefined) {
      this.misses++;
      return null;
    }
    this.hits++;
    this.#take(key, entry, options.once === true);
    return { value: entry.value, size: entry.size };
  }

  /**
   * One more pin; the first takes the entry out of the cache's charge and, unless it is a
   * one-pass pin, off the eviction order. An ordinary pin takes it off the order in any case.
   */
  #take(key: string, entry: Entry<T>, once: boolean): void {
    if (entry.pins === 0 && entry.charged) {
      entry.charged = false;
      this.#evictable -= entry.size;
      this.#memory.release(entry.size);
    }
    if (!once) this.#unpinned.delete(key);
    entry.pins++;
    if (once) entry.oncePins++;
  }

  /**
   * Adds a value the caller just decoded (its `size` already charged to the caller), pinned for
   * it, and returns the cached value: an earlier entry for the same blob if another holder won
   * the race (the caller's value is then garbage; its charge, of the same size, stands).
   */
  admit(ref: BlobRef, value: T, size: number): T {
    const key = this.key(ref);
    const existing = this.#entries.get(key);
    if (existing !== undefined) {
      this.#take(key, existing, false);
      return existing.value;
    }
    this.#entries.set(key, { value, size, pins: 1, oncePins: 0, charged: false, usedAt: 0 });
    this.#resident += size;
    return value;
  }

  /**
   * The caller lets go (it frees its own charge): the entry is charged here, or dropped. `once`
   * as it was pinned.
   */
  unpin(ref: BlobRef, options: PinOptions = {}): void {
    const key = this.key(ref);
    const entry = this.#entries.get(key);
    if (entry === undefined || entry.pins === 0) throw new RangeError("unpin of an unpinned entry");
    const once = options.once === true;
    if (once && entry.oncePins === 0) throw new RangeError("once unpin of an ordinary pin");
    entry.pins--;
    if (once) entry.oncePins--;
    const listed = this.#unpinned.has(key);
    if (entry.pins > 0) {
      // The last ordinary pin went while one-pass pins remain: placed in the order now, as this
      // release would place it; charged when the last pin goes.
      if (!once && !listed && entry.pins === entry.oncePins) {
        entry.usedAt = performance.now();
        this.#unpinned.set(key, entry);
      }
      return;
    }
    if (this.#memory.charge(entry.size)) {
      entry.charged = true;
      if (!listed) {
        entry.usedAt = performance.now();
        this.#unpinned.set(key, entry);
      }
      this.#evictable += entry.size;
    } else {
      this.#unpinned.delete(key);
      this.#entries.delete(key);
      this.#resident -= entry.size;
    }
  }

  /** Drops every unpinned entry and returns its memory. */
  clear(): void {
    this.evict(Number.POSITIVE_INFINITY);
  }
}
