/** Snapshot of the byte-bounded artifact cache's lifetime counters. */
export interface ArtifactCacheStats {
  limitBytes: number;
  usedBytes: number;
  entries: number;
  hits: number;
  misses: number;
  evictions: number;
}

/** One resident artifact on the recency list; `previous` is older, `next` is newer. */
interface ArtifactCacheEntry {
  key: string;
  payload: unknown;
  bytes: number;
  previous: ArtifactCacheEntry | undefined;
  next: ArtifactCacheEntry | undefined;
}

/**
 * Byte-bounded LRU for decoded blocks, vectors, derived results, and memoized query results.
 * Payloads are immutable by convention; callers own the byte estimates for their artifact type.
 *
 * Recency lives on an intrusive doubly-linked list rather than Map insertion order: a hit
 * relinks four pointers instead of deleting and re-inserting the key, which re-hashed the
 * long block-identity keys twice per touch and was the single hottest frame of a cached point
 * lookup.
 */
export class ArtifactCache {
  readonly #limitBytes: number;
  readonly #entries = new Map<string, ArtifactCacheEntry>();
  /** Least recently used end of the recency list. */
  #oldest: ArtifactCacheEntry | undefined;
  /** Most recently used end of the recency list. */
  #newest: ArtifactCacheEntry | undefined;
  #usedBytes = 0;
  #hits = 0;
  #misses = 0;
  #evictions = 0;

  constructor(limitBytes: number) {
    if (!Number.isSafeInteger(limitBytes) || limitBytes < 0) {
      throw new RangeError("Prepare cache bytes must be a non-negative whole number");
    }
    this.#limitBytes = limitBytes;
  }

  get enabled(): boolean {
    return this.#limitBytes > 0;
  }

  #unlink(entry: ArtifactCacheEntry): void {
    if (entry.previous !== undefined) entry.previous.next = entry.next;
    else this.#oldest = entry.next;
    if (entry.next !== undefined) entry.next.previous = entry.previous;
    else this.#newest = entry.previous;
    entry.previous = undefined;
    entry.next = undefined;
  }

  #appendNewest(entry: ArtifactCacheEntry): void {
    entry.previous = this.#newest;
    entry.next = undefined;
    if (this.#newest !== undefined) this.#newest.next = entry;
    this.#newest = entry;
    this.#oldest ??= entry;
  }

  get(key: string): unknown {
    if (!this.enabled) return undefined;
    const entry = this.#entries.get(key);
    if (entry === undefined) {
      this.#misses += 1;
      return undefined;
    }
    this.#hits += 1;
    if (this.#newest !== entry) {
      this.#unlink(entry);
      this.#appendNewest(entry);
    }
    return entry.payload;
  }

  put(key: string, payload: unknown, bytes: number): void {
    if (!this.enabled || bytes > this.#limitBytes) return;
    if (!Number.isSafeInteger(bytes) || bytes < 0) {
      throw new RangeError("Artifact cache entry bytes must be a non-negative whole number");
    }
    const existing = this.#entries.get(key);
    if (existing !== undefined) {
      this.#usedBytes -= existing.bytes;
      existing.payload = payload;
      existing.bytes = bytes;
      this.#usedBytes += bytes;
      if (this.#newest !== existing) {
        this.#unlink(existing);
        this.#appendNewest(existing);
      }
    } else {
      const entry: ArtifactCacheEntry = {
        key,
        payload,
        bytes,
        previous: undefined,
        next: undefined,
      };
      this.#entries.set(key, entry);
      this.#appendNewest(entry);
      this.#usedBytes += bytes;
    }
    let oldest = this.#oldest;
    while (this.#usedBytes > this.#limitBytes && oldest !== undefined) {
      const next = oldest.next;
      if (oldest.key !== key) {
        this.#unlink(oldest);
        this.#entries.delete(oldest.key);
        this.#usedBytes -= oldest.bytes;
        this.#evictions += 1;
      }
      oldest = next;
    }
  }

  /** Releases every retained payload while preserving lifetime counters for final diagnostics. */
  clear(): void {
    this.#entries.clear();
    this.#oldest = undefined;
    this.#newest = undefined;
    this.#usedBytes = 0;
  }

  stats(): ArtifactCacheStats {
    return {
      limitBytes: this.#limitBytes,
      usedBytes: this.#usedBytes,
      entries: this.#entries.size,
      hits: this.#hits,
      misses: this.#misses,
      evictions: this.#evictions,
    };
  }
}
