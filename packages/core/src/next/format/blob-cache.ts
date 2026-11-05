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
 * - Unpinned entries are evictable (`EvictableCacheLike`), least recently used first; the
 *   governor evicts them before any grant grows or any operator spills.
 * One holder may pin an entry several times; each `pin` or `admit` is undone by one `unpin`.
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
  /** Charged to the cache (only while unpinned). */
  charged: boolean;
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
  /** Insertion order is recency order (an entry is re-inserted when used). */
  readonly #entries = new Map<string, Entry<T>>();
  #evictable = 0;
  #resident = 0;
  hits = 0;
  misses = 0;

  /** `register` is the governor's `registerCache` (memory/governor.ts). */
  constructor(register: (cache: EvictableCacheLike) => CacheMemory) {
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

  evict(bytes: number): number {
    let freed = 0;
    for (const [key, entry] of this.#entries) {
      if (freed >= bytes) break;
      if (entry.pins > 0) continue;
      this.#entries.delete(key);
      this.#resident -= entry.size;
      // Unpinned entries are always charged (unpin drops the ones the governor refused).
      this.#memory.release(entry.size);
      this.#evictable -= entry.size;
      freed += entry.size;
    }
    return freed;
  }

  /** The entry for `ref`, pinned for the caller, or null. */
  pin(ref: BlobRef): Pinned<T> | null {
    const key = blobKey(ref);
    const entry = this.#entries.get(key);
    if (entry === undefined) {
      this.misses++;
      return null;
    }
    this.hits++;
    if (entry.pins++ === 0 && entry.charged) {
      entry.charged = false;
      this.#evictable -= entry.size;
      this.#memory.release(entry.size);
    }
    this.#entries.delete(key);
    this.#entries.set(key, entry);
    return { value: entry.value, size: entry.size };
  }

  /**
   * Adds a value the caller just decoded (its `size` already charged to the caller), pinned for
   * it, and returns the cached value: an earlier entry for the same blob if another holder won
   * the race (the caller's value is then garbage; its charge, of the same size, stands).
   */
  admit(ref: BlobRef, value: T, size: number): T {
    const key = blobKey(ref);
    const existing = this.#entries.get(key);
    if (existing !== undefined) {
      this.#entries.delete(key);
      this.#entries.set(key, existing);
      if (existing.pins++ === 0 && existing.charged) {
        existing.charged = false;
        this.#evictable -= existing.size;
        this.#memory.release(existing.size);
      }
      return existing.value;
    }
    this.#entries.set(key, { value, size, pins: 1, charged: false });
    this.#resident += size;
    return value;
  }

  /** The caller lets go (it frees its own charge): the entry is charged here, or dropped. */
  unpin(ref: BlobRef): void {
    const key = blobKey(ref);
    const entry = this.#entries.get(key);
    if (entry === undefined || entry.pins === 0) throw new RangeError("unpin of an unpinned entry");
    if (--entry.pins > 0) return;
    if (this.#memory.charge(entry.size)) {
      entry.charged = true;
      this.#evictable += entry.size;
    } else {
      this.#entries.delete(key);
      this.#resident -= entry.size;
    }
  }

  /** Drops every unpinned entry and returns its memory. */
  clear(): void {
    this.evict(Number.POSITIVE_INFINITY);
  }
}
