/** Snapshot of the byte-bounded artifact cache's lifetime counters. */
interface ArtifactCacheStats {
  limitBytes: number;
  usedBytes: number;
  entries: number;
  hits: number;
  misses: number;
  evictions: number;
}

/**
 * Per-block artifact families. Each family is its own key space, keyed by the bare block id:
 * a lookup then hashes a string the catalog already holds (its hash is cached in the string)
 * instead of building and hashing a fresh `prefix + id` string per touch, which was the
 * dominant cost of a cache hit on every scan and point read.
 */
export const ArtifactFamily = {
  /** Free-form string keys: memoized results, derived tables, overlay states. */
  keyed: 0,
  /** A decoded physical block. */
  decodedBlock: 1,
  /** A block's decoded column vector. */
  blockVector: 2,
  /** A block's header description (row count, zone map), read without decoding. */
  zoneDescription: 3,
  /** A string block's reverse dictionary, value to code. */
  reverseDictionary: 4,
  /** A segment's key locator table, keyed by segment id. */
  segmentKeyLocators: 5,
} as const;

export type ArtifactFamily = (typeof ArtifactFamily)[keyof typeof ArtifactFamily];

const FAMILY_COUNT = 6;

/**
 * What a family key is charged beyond its id: the short prefix the flat key space used to
 * spell out, so a family entry costs the residency budget what it always has.
 */
const FAMILY_KEY_PREFIX_CHARACTERS = 4;

/** One resident artifact on the recency list; `previous` is older, `next` is newer. */
interface ArtifactCacheEntry {
  family: ArtifactFamily;
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
 * lookup. One recency list and one byte budget span every family.
 */
export class ArtifactCache {
  readonly #limitBytes: number;
  readonly #families: Array<Map<string, ArtifactCacheEntry>> = Array.from(
    { length: FAMILY_COUNT },
    () => new Map<string, ArtifactCacheEntry>(),
  );
  /** Least recently used end of the recency list. */
  #oldest: ArtifactCacheEntry | undefined;
  /** Most recently used end of the recency list. */
  #newest: ArtifactCacheEntry | undefined;
  #usedBytes = 0;
  #entryCount = 0;
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
    return this.getIn(ArtifactFamily.keyed, key);
  }

  /** The payload under `key` in one family's key space, renewing its recency. */
  getIn(family: ArtifactFamily, key: string): unknown {
    if (!this.enabled) return undefined;
    const entry = this.#families[family]?.get(key);
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
    this.putIn(ArtifactFamily.keyed, key, payload, bytes);
  }

  /** Stores `payload` under `key` in one family's key space, evicting to the byte limit. */
  putIn(family: ArtifactFamily, key: string, payload: unknown, bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes < 0) {
      throw new RangeError("Artifact cache entry bytes must be a non-negative whole number");
    }
    const entries = this.#families[family];
    if (entries === undefined) throw new RangeError("Unknown artifact cache family");
    // Payload estimates belong to callers; keys and the LRU/Map entry belong to the cache.
    // Tiny results with long SQL/plan keys must not escape the residency budget.
    const keyCharacters =
      key.length + (family === ArtifactFamily.keyed ? 0 : FAMILY_KEY_PREFIX_CHARACTERS);
    bytes += 96 + keyCharacters * 2;
    if (!this.enabled || !Number.isSafeInteger(bytes) || bytes > this.#limitBytes) return;
    const existing = entries.get(key);
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
        family,
        key,
        payload,
        bytes,
        previous: undefined,
        next: undefined,
      };
      entries.set(key, entry);
      this.#entryCount += 1;
      this.#appendNewest(entry);
      this.#usedBytes += bytes;
    }
    let oldest = this.#oldest;
    while (this.#usedBytes > this.#limitBytes && oldest !== undefined) {
      const next = oldest.next;
      if (oldest.family !== family || oldest.key !== key) {
        this.#unlink(oldest);
        this.#families[oldest.family]?.delete(oldest.key);
        this.#entryCount -= 1;
        this.#usedBytes -= oldest.bytes;
        this.#evictions += 1;
      }
      oldest = next;
    }
  }

  /**
   * Keeps `payload` resident under `key` for a caller still using it: renews its recency, or
   * puts it back if it was evicted. A key that now holds a different payload — a newer artifact
   * for the same identity — is left alone. Counts as neither a hit nor a miss.
   */
  retain(key: string, payload: unknown, bytes: number): void {
    const existing = this.#families[ArtifactFamily.keyed]?.get(key);
    if (existing === undefined) {
      this.put(key, payload, bytes);
      return;
    }
    if (existing.payload !== payload || this.#newest === existing) return;
    this.#unlink(existing);
    this.#appendNewest(existing);
  }

  /** Releases every retained payload while preserving lifetime counters for final diagnostics. */
  clear(): void {
    for (const entries of this.#families) entries.clear();
    this.#entryCount = 0;
    this.#oldest = undefined;
    this.#newest = undefined;
    this.#usedBytes = 0;
  }

  stats(): ArtifactCacheStats {
    return {
      limitBytes: this.#limitBytes,
      usedBytes: this.#usedBytes,
      entries: this.#entryCount,
      hits: this.#hits,
      misses: this.#misses,
      evictions: this.#evictions,
    };
  }
}
