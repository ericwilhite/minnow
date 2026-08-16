/** Snapshot of the byte-bounded artifact cache's lifetime counters. */
export interface ArtifactCacheStats {
  limitBytes: number;
  usedBytes: number;
  entries: number;
  hits: number;
  misses: number;
  evictions: number;
}

/**
 * Byte-bounded LRU for decoded blocks, vectors, derived results, and memoized query results.
 * Payloads are immutable by convention; callers own the byte estimates for their artifact type.
 */
export class ArtifactCache {
  readonly #limitBytes: number;
  readonly #entries = new Map<string, { payload: unknown; bytes: number }>();
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

  get(key: string): unknown {
    if (!this.enabled) return undefined;
    const entry = this.#entries.get(key);
    if (entry === undefined) {
      this.#misses += 1;
      return undefined;
    }
    this.#hits += 1;
    this.#entries.delete(key);
    this.#entries.set(key, entry);
    return entry.payload;
  }

  put(key: string, payload: unknown, bytes: number): void {
    if (!this.enabled || bytes > this.#limitBytes) return;
    if (!Number.isSafeInteger(bytes) || bytes < 0) {
      throw new RangeError("Artifact cache entry bytes must be a non-negative whole number");
    }
    const existing = this.#entries.get(key);
    if (existing !== undefined) {
      this.#entries.delete(key);
      this.#usedBytes -= existing.bytes;
    }
    this.#entries.set(key, { payload, bytes });
    this.#usedBytes += bytes;
    for (const [oldestKey, entry] of this.#entries) {
      if (this.#usedBytes <= this.#limitBytes) break;
      if (oldestKey === key) continue;
      this.#entries.delete(oldestKey);
      this.#usedBytes -= entry.bytes;
      this.#evictions += 1;
    }
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
