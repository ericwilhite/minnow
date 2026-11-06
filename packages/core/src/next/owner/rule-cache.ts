/**
 * Compiled table rules (exec/rules.ts `compileTableRules` over lowering's `lowerTableRules`),
 * cached by the identity of the catalog they were compiled against (B5-p2c review 1, findings 2
 * and 3).
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
 * - **Bounded and charged.** At most `limit` entries (least recently used first out), each
 *   allocated at an estimate of its size from one spillable governor grant, which the cache gives
 *   back by evicting when the governor asks (`onReclaimRequest`, rule 7). An entry the governor
 *   refuses is compiled and returned, not kept. (Not `registerCache`: the governor has one
 *   evictable cache, the page cache.)
 */
import type { CatalogView } from "../access/catalog.js";
import type { TableRules } from "../access/modify.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import {
  OBJECT_OVERHEAD_BYTES,
  stringBytes,
  type Grant,
  type MemoryGovernor,
} from "../memory/governor.js";

/** Entries per cache, as the statement-plan cache's 512. */
export const DEFAULT_RULE_CACHE_ENTRIES = 512;

/** An estimate of a compiled rule set's retained size: its expressions, plus one object each. */
export function ruleBytes(expressions: TableRuleExpressions): number {
  const parts =
    expressions.checks.length + expressions.generated.length + (expressions.defaults?.length ?? 0);
  return stringBytes(JSON.stringify(expressions)) + OBJECT_OVERHEAD_BYTES * (2 + parts);
}

interface Entry {
  readonly rules: TableRules;
  readonly bytes: number;
  readonly view: number;
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

export class RuleCache {
  readonly #limit: number;
  /** In use order: the first entry is the least recently used. */
  readonly #entries = new Map<string, Entry>();
  readonly #governor: MemoryGovernor | null;
  readonly #label: string;
  #grant: Grant | null = null;
  #bytes = 0;

  constructor(governor: MemoryGovernor | null, label: string, limit = DEFAULT_RULE_CACHE_ENTRIES) {
    this.#governor = governor;
    this.#label = label;
    this.#limit = limit;
  }

  /** The cache's grant, admitted on first use (null while the governor has admissions queued). */
  #charged(): Grant | null {
    if (this.#grant !== null || this.#governor === null) return this.#grant;
    this.#grant = this.#governor.tryAdmit({ label: this.#label, minimumBytes: 0, spillable: true });
    this.#grant?.onReclaimRequest((bytes) => {
      this.#evict(bytes);
    });
    return this.#grant;
  }

  /** Entries held (tests). */
  get size(): number {
    return this.#entries.size;
  }

  /** Bytes charged to the governor (tests). */
  get bytes(): number {
    return this.#bytes;
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
      this.#entries.set(key, hit);
      return hit.rules;
    }
    const { rules, expressions } = compile();
    const bytes = ruleBytes(expressions);
    while (this.#entries.size >= this.#limit) {
      const oldest = this.#entries.keys().next();
      if (oldest.done === true) break;
      this.#drop(oldest.value);
    }
    if (this.#governor !== null && this.#charged()?.allocate(bytes) !== true) return rules;
    this.#entries.set(key, { rules, bytes, view: id });
    this.#bytes += bytes;
    return rules;
  }

  /** Drops everything (a transaction ended or rolled back to a savepoint). */
  clear(): void {
    for (const key of [...this.#entries.keys()]) this.#drop(key);
  }

  /** Drops everything and returns the grant (the engine or session is done with the cache). */
  close(): void {
    this.clear();
    this.#grant?.close();
    this.#grant = null;
  }

  #drop(key: string): void {
    const entry = this.#entries.get(key);
    if (entry === undefined) return;
    this.#entries.delete(key);
    this.#bytes -= entry.bytes;
    if (this.#grant !== null) {
      this.#grant.free(entry.bytes);
      this.#grant.release(entry.bytes);
    }
  }

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
