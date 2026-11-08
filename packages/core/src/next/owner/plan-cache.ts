/**
 * Retained statement plans: bounded by bytes and entries, LRU across templates, refusal markers
 * and raw plans. Keys and the retained data graph are charged to the governor (rule 6); a plan
 * that cannot be retained is still returned to its caller. Estimates use the same object,
 * string, bigint and backing-buffer accounting as compiled rules, never JSON serialization.
 */
import {
  OBJECT_OVERHEAD_BYTES,
  stringBytes,
  type EvictableCache,
  type MemoryGovernor,
} from "../memory/governor.js";
import { retainedBytes } from "./rule-cache.js";

export const DEFAULT_PLAN_CACHE_ENTRIES = 512;
export const DEFAULT_PLAN_CACHE_BYTES = 4 * 1024 * 1024;
type Handle = ReturnType<MemoryGovernor["registerCache"]>;
interface Entry<T> {
  readonly value: T;
  readonly bytes: number;
  usedAt: number;
}

export class PlanCache<T> {
  readonly #entries = new Map<string, Entry<T>>();
  readonly #governor: MemoryGovernor;
  readonly #entryLimit: number;
  readonly #byteLimit: number;
  #handle: Handle | null = null;
  #bytes = 0;
  #closed = false;

  constructor(
    governor: MemoryGovernor,
    entries = DEFAULT_PLAN_CACHE_ENTRIES,
    bytes = DEFAULT_PLAN_CACHE_BYTES,
  ) {
    if (!Number.isFinite(entries) || !Number.isFinite(bytes))
      throw new RangeError("Plan-cache limits must be finite");
    this.#governor = governor;
    this.#entryLimit = Math.max(0, Math.floor(entries));
    this.#byteLimit = Math.max(0, Math.floor(bytes));
  }
  get size(): number {
    return this.#entries.size;
  }
  get bytes(): number {
    return this.#bytes;
  }

  get(key: string): T | undefined {
    const entry = this.#entries.get(key);
    if (entry === undefined) return undefined;
    this.#entries.delete(key);
    entry.usedAt = performance.now();
    this.#entries.set(key, entry);
    return entry.value;
  }

  #registered(): Handle {
    if (this.#handle !== null) return this.#handle;
    const source: EvictableCache = { evictableBytes: 0, evict: (bytes) => this.#evict(bytes) };
    Object.defineProperty(source, "evictableBytes", { get: () => this.#bytes });
    Object.defineProperty(source, "oldestUnusedAt", {
      get: () => this.#entries.values().next().value?.usedAt,
    });
    return (this.#handle = this.#governor.registerCache(source));
  }

  set(key: string, value: T): boolean {
    this.delete(key);
    if (this.#closed || this.#entryLimit === 0 || this.#byteLimit === 0) return false;
    // Map node + entry, its key, and the reachable plan/marker/template graph. Shared objects
    // within an entry count once; charging sharing between entries again is conservative.
    const bytes = 2 * OBJECT_OVERHEAD_BYTES + stringBytes(key) + retainedBytes(value);
    if (bytes > this.#byteLimit) return false;
    while (this.#entries.size >= this.#entryLimit || this.#bytes + bytes > this.#byteLimit) {
      const oldest = this.#entries.keys().next();
      if (oldest.done === true) break;
      this.delete(oldest.value);
    }
    if (!this.#registered().charge(bytes)) return false;
    this.#entries.set(key, { value, bytes, usedAt: performance.now() });
    this.#bytes += bytes;
    return true;
  }

  delete(key: string): void {
    const entry = this.#entries.get(key);
    if (entry === undefined) return;
    this.#entries.delete(key);
    this.#bytes -= entry.bytes;
    this.#handle?.release(entry.bytes);
  }
  #evict(bytes: number): number {
    let freed = 0;
    while (freed < bytes) {
      const next = this.#entries.entries().next();
      if (next.done === true) break;
      freed += next.value[1].bytes;
      this.delete(next.value[0]);
    }
    return freed;
  }
  clear(): void {
    for (const key of [...this.#entries.keys()]) this.delete(key);
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.clear();
    this.#handle?.unregister?.();
    this.#handle = null;
  }
}
