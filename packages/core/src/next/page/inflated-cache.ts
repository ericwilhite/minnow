/**
 * The inflated-page cache (ruling R15.5): gzip pages kept inflated (still page-encoded, never
 * decoded), so a page inflates once per cache residency instead of once per load. The space's
 * cache holds the stored (gzip) blobs; this one holds what they inflate to, keyed by the same
 * `(address, length, checksum)`.
 *
 * Accounting, the space's lease rule (R5) applied to inflated bytes:
 * - An entry a reader holds is **pinned**: its bytes are charged to that reader's budget (the
 *   reader charges them before it inflates), not to the cache, and it cannot be evicted.
 * - When the last reader lets go, the cache charges the entry through its governor handle
 *   (`CacheMemory.charge`); if the governor has no room the entry is dropped.
 * - Unpinned entries are evictable (`EvictableCacheLike`), least recently used first; the
 *   governor evicts them before any grant grows or any operator spills.
 */
import {
  blobAddress,
  type BlobRef,
  type CacheMemory,
  type EvictableCacheLike,
} from "../storage/extents.js";

interface Entry {
  readonly bytes: Uint8Array;
  pins: number;
  /** Charged to the cache (true only while unpinned). */
  charged: boolean;
}

const keyOf = (ref: BlobRef): string =>
  `${String(blobAddress(ref))}:${String(ref.length)}:${String(ref.checksum)}`;

export class InflatedPageCache implements EvictableCacheLike {
  readonly #memory: CacheMemory;
  /** Insertion order is recency order (an entry is re-inserted when used). */
  readonly #entries = new Map<string, Entry>();
  #evictable = 0;
  hits = 0;
  misses = 0;

  /** `register` is the governor's `registerCache` (memory/governor.ts). */
  constructor(register: (cache: EvictableCacheLike) => CacheMemory) {
    this.#memory = register(this);
  }

  get evictableBytes(): number {
    return this.#evictable;
  }

  /** Bytes resident (pinned or not). */
  get residentBytes(): number {
    let total = 0;
    for (const entry of this.#entries.values()) total += entry.bytes.length;
    return total;
  }

  evict(bytes: number): number {
    let freed = 0;
    for (const [key, entry] of this.#entries) {
      if (freed >= bytes) break;
      if (entry.pins > 0) continue;
      this.#entries.delete(key);
      if (entry.charged) {
        this.#memory.release(entry.bytes.length);
        this.#evictable -= entry.bytes.length;
        freed += entry.bytes.length;
      }
    }
    return freed;
  }

  /**
   * The inflated page for `ref`, pinned for the caller, or null. The caller charges its budget
   * for the bytes (and unpins on refusal) before using them.
   */
  pin(ref: BlobRef): Uint8Array | null {
    const key = keyOf(ref);
    const entry = this.#entries.get(key);
    if (entry === undefined) {
      this.misses++;
      return null;
    }
    this.hits++;
    if (entry.pins++ === 0 && entry.charged) {
      entry.charged = false;
      this.#evictable -= entry.bytes.length;
      this.#memory.release(entry.bytes.length);
    }
    this.#entries.delete(key);
    this.#entries.set(key, entry);
    return entry.bytes;
  }

  /**
   * Adds a page the caller just inflated (its bytes already charged to the caller), pinned for
   * it, and returns the cached bytes: an earlier entry for the same page if another reader won
   * the race (the caller's buffer is then garbage, its charge unchanged).
   */
  admit(ref: BlobRef, bytes: Uint8Array): Uint8Array {
    const existing = this.pin(ref);
    if (existing !== null) {
      this.misses--; // it was a race, not a lookup
      return existing;
    }
    this.#entries.set(keyOf(ref), { bytes, pins: 1, charged: false });
    return bytes;
  }

  /** The caller lets go (it frees its own charge): the entry is charged here, or dropped. */
  unpin(ref: BlobRef): void {
    const key = keyOf(ref);
    const entry = this.#entries.get(key);
    if (entry === undefined || entry.pins === 0) throw new RangeError("unpin of an unpinned page");
    if (--entry.pins > 0) return;
    if (this.#memory.charge(entry.bytes.length)) {
      entry.charged = true;
      this.#evictable += entry.bytes.length;
    } else this.#entries.delete(key);
  }

  /** Drops every unpinned entry and returns its memory. */
  clear(): void {
    this.evict(Number.POSITIVE_INFINITY);
  }
}
