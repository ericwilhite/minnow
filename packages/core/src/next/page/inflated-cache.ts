/**
 * The inflated-page cache (ruling R15.5): gzip pages kept inflated (still page-encoded, never
 * decoded), so a page inflates once per cache residency instead of once per load. The space's
 * cache holds the stored (gzip) blobs; this one holds what they inflate to, keyed by the same
 * blob. It is a `BlobCache` of bytes (format/blob-cache.ts) and follows its accounting: a pinned
 * page is charged to the reader holding it, an unpinned one to the cache, which the governor
 * evicts first.
 */
import { BlobCache } from "../format/blob-cache.js";
import type { BlobRef, CacheMemory, EvictableCacheLike } from "../storage/extents.js";

export class InflatedPageCache implements EvictableCacheLike {
  readonly #cache: BlobCache<Uint8Array>;

  /** `register` is the governor's `registerCache` (memory/governor.ts). */
  constructor(register: (cache: EvictableCacheLike) => CacheMemory) {
    this.#cache = new BlobCache<Uint8Array>(register);
  }

  get evictableBytes(): number {
    return this.#cache.evictableBytes;
  }
  get residentBytes(): number {
    return this.#cache.residentBytes;
  }
  get hits(): number {
    return this.#cache.hits;
  }
  get misses(): number {
    return this.#cache.misses;
  }

  evict(bytes: number): number {
    return this.#cache.evict(bytes);
  }

  /** The inflated page, pinned for the caller (who charges its length), or null. */
  pin(ref: BlobRef): Uint8Array | null {
    return this.#cache.pin(ref)?.value ?? null;
  }

  /** Adds a page the caller just inflated (charged to it), pinned; returns the cached bytes. */
  admit(ref: BlobRef, bytes: Uint8Array): Uint8Array {
    return this.#cache.admit(ref, bytes, bytes.length);
  }

  unpin(ref: BlobRef): void {
    this.#cache.unpin(ref);
  }

  clear(): void {
    this.#cache.clear();
  }
}
