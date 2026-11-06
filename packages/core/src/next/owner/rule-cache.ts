/**
 * Compiled table rules (exec/rules.ts `compileTableRules` over lowering's `lowerTableRules`),
 * cached by the identity of the catalog they were compiled against (B5-p2c reviews 1 and 2).
 *
 * - **Key: the catalog view object and the table id.** A `CatalogView` is immutable
 *   (access/catalog.ts): a DDL produces a new view. Two transactional histories that reach the
 *   same version number are two different views, so they never share rules. A table's definition
 *   alone is not a sufficient key: rule expressions resolve other catalog entries (a default's
 *   `nextval('s')` names a sequence).
 * - **Two lifetimes.** The engine's cache holds rules for the committed catalog only, and drops
 *   every entry of an older committed view when a newer one is first used (no writer can reach an
 *   older committed view: writes run against the newest). A transaction with its own schema
 *   changes keeps its rules in a session-local cache that is cleared when the transaction ends
 *   and when it rolls back to a savepoint.
 * - **Bounded and charged.** At most `limit` entries (least recently used first out). The cache is
 *   one of the governor's evictable caches (memory/governor.ts `registerCache`, rule 6): every
 *   entry is charged at its size, and the governor evicts it, oldest unused across all caches,
 *   before it refuses growth or makes an operator spill. An entry the governor refuses is
 *   compiled and returned, not kept. `close()` releases everything and unregisters.
 * - **Sized, not serialized.** `ruleBytes` walks the expressions: an object header per object,
 *   typed arrays by their bytes, BigInts by `bigintBytes`, strings by length. (JSON would throw on
 *   the BigInt of a large int64 literal.)
 */
import type { CatalogView } from "../access/catalog.js";
import type { TableRules } from "../access/modify.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import {
  OBJECT_OVERHEAD_BYTES,
  bigintBytes,
  stringBytes,
  type EvictableCache,
  type MemoryGovernor,
} from "../memory/governor.js";

/** Entries per cache, as the statement-plan cache's 512. */
export const DEFAULT_RULE_CACHE_ENTRIES = 512;

/**
 * An estimate of a value's retained size: `OBJECT_OVERHEAD_BYTES` per object or array (shared ones
 * once), the backing bytes of typed arrays (each buffer once), 8 per number, `bigintBytes` per
 * BigInt, `stringBytes` per string.
 */
export function retainedBytes(value: unknown, seen = new Set<object>()): number {
  switch (typeof value) {
    case "number":
      return 8;
    case "bigint":
      return bigintBytes(value);
    case "string":
      return stringBytes(value);
    case "object": {
      if (value === null) return 0;
      if (ArrayBuffer.isView(value)) {
        // The same view once; another view over a buffer already counted, its header only.
        if (seen.has(value)) return 0;
        seen.add(value);
        if (seen.has(value.buffer)) return OBJECT_OVERHEAD_BYTES;
        seen.add(value.buffer);
        return OBJECT_OVERHEAD_BYTES + value.buffer.byteLength;
      }
      if (seen.has(value)) return 0;
      seen.add(value);
      let bytes = OBJECT_OVERHEAD_BYTES;
      const items: Iterable<unknown> = Array.isArray(value)
        ? (value as unknown[])
        : Object.values(value as Record<string, unknown>);
      for (const item of items) bytes += retainedBytes(item, seen);
      return bytes;
    }
    default:
      return 0;
  }
}

/** A compiled rule set's retained size: its expressions, plus the compiled object. */
export function ruleBytes(expressions: TableRuleExpressions): number {
  return retainedBytes(expressions) + OBJECT_OVERHEAD_BYTES;
}

interface Entry {
  readonly rules: TableRules;
  readonly bytes: number;
  readonly view: number;
  usedAt: number;
}

/** One view's identity: a number per view object, never reused. */
const viewIds = new WeakMap<CatalogView, number>();
let nextViewId = 1;
function viewId(view: CatalogView): number {
  let id = viewIds.get(view);
  if (id === undefined) {
    id = nextViewId++;
    viewIds.set(view, id);
  }
  return id;
}

type CacheHandle = ReturnType<MemoryGovernor["registerCache"]>;

export class RuleCache {
  readonly #limit: number;
  /** In use order: the first entry is the least recently used. */
  readonly #entries = new Map<string, Entry>();
  readonly #governor: MemoryGovernor | null;
  #handle: CacheHandle | null = null;
  #bytes = 0;
  #closed = false;

  constructor(governor: MemoryGovernor | null, limit = DEFAULT_RULE_CACHE_ENTRIES) {
    this.#governor = governor;
    this.#limit = limit;
  }

  /** Entries held (tests). */
  get size(): number {
    return this.#entries.size;
  }

  /** Bytes charged to the governor (tests). */
  get bytes(): number {
    return this.#bytes;
  }

  /** Registered with the governor on first use (a session that never compiles never registers). */
  #registered(): CacheHandle | null {
    if (this.#handle !== null || this.#governor === null || this.#closed) return this.#handle;
    const source: EvictableCache = {
      evictableBytes: 0,
      evict: (bytes: number) => this.#evict(bytes),
    };
    Object.defineProperty(source, "evictableBytes", { get: () => this.#bytes });
    Object.defineProperty(source, "oldestUnusedAt", {
      get: () => this.#entries.values().next().value?.usedAt,
    });
    this.#handle = this.#governor.registerCache(source);
    return this.#handle;
  }

  /**
   * The rules of `tableId` at `view`, compiled by `compile` on a miss. `only`: drop every entry of
   * another view first (the engine's cache, when the committed view moved on).
   */
  get(
    view: CatalogView,
    tableId: number,
    compile: () => { readonly rules: TableRules; readonly expressions: TableRuleExpressions },
    only = false,
  ): TableRules {
    const id = viewId(view);
    if (only) for (const [key, entry] of this.#entries) if (entry.view !== id) this.#drop(key);
    const key = `${String(id)}:${String(tableId)}`;
    const hit = this.#entries.get(key);
    if (hit !== undefined) {
      this.#entries.delete(key);
      hit.usedAt = performance.now();
      this.#entries.set(key, hit);
      return hit.rules;
    }
    const { rules, expressions } = compile();
    if (this.#closed) return rules;
    const bytes = ruleBytes(expressions);
    while (this.#entries.size >= this.#limit) {
      const oldest = this.#entries.keys().next();
      if (oldest.done === true) break;
      this.#drop(oldest.value);
    }
    const handle = this.#registered();
    if (this.#governor !== null && handle?.charge(bytes) !== true) return rules;
    this.#entries.set(key, { rules, bytes, view: id, usedAt: performance.now() });
    this.#bytes += bytes;
    return rules;
  }

  /** Drops everything (a transaction ended or rolled back to a savepoint). */
  clear(): void {
    for (const key of [...this.#entries.keys()]) this.#drop(key);
  }

  /** Releases everything and leaves the governor (the engine or session is done with it). */
  close(): void {
    this.clear();
    this.#closed = true;
    this.#handle?.unregister?.();
    this.#handle = null;
  }

  #drop(key: string): void {
    const entry = this.#entries.get(key);
    if (entry === undefined) return;
    this.#entries.delete(key);
    this.#bytes -= entry.bytes;
    this.#handle?.release(entry.bytes);
  }

  /** The governor's eviction (rule 6): least recently used first, until `bytes` are freed. */
  #evict(bytes: number): number {
    let freed = 0;
    for (const key of [...this.#entries.keys()]) {
      if (freed >= bytes) break;
      freed += this.#entries.get(key)?.bytes ?? 0;
      this.#drop(key);
    }
    return freed;
  }
}
