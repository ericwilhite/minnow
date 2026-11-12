/** Query-local validated scan facts. Complete-root APIs remain complete. */
import { BlobCache } from "../format/blob-cache.js";
import type { ColumnChunk, GroupRoot } from "../format/manifest.js";
import { MetadataReader, type MaybePromise } from "../format/metadata-reader.js";
import { parseCharge, ParseBudgetError } from "../format/manifest-codec.js";
import type { Held } from "../format/reachable.js";
import type { MemoryGovernor } from "../memory/governor.js";
import type { BlobRef, ByteBudget, ExtentReader } from "../storage/extents.js";

export interface RootColumns {
  readonly tableId: number;
  readonly columns: readonly number[];
  readonly domainColumn?: number;
}
/** Deliberately not GroupRoot: row-id metadata and unused columns are not retained. */
export interface ScanRootFacts {
  readonly rowCount: number;
  readonly columns: readonly ColumnChunk[];
}
export const PREPARE_GROUP = Symbol("prepare query scan columns");
export interface PreparedGroup {
  [PREPARE_GROUP]?: (columns: readonly number[]) => MaybePromise<void>;
}
export function prepareGroup(
  group: PreparedGroup & { root(): MaybePromise<unknown> },
  columns: readonly number[],
): MaybePromise<unknown> {
  const prepare = group[PREPARE_GROUP];
  return prepare === undefined ? group.root() : prepare.call(group, columns);
}

const TABLE = Symbol("query root table");
type RootKey = BlobRef & { readonly [TABLE]?: number };

export class QueryRoots {
  readonly roots: BlobCache<ScanRootFacts>;
  readonly #unregister: (() => void) | undefined;
  #closed = false;
  constructor(
    governor: MemoryGovernor,
    readonly sources: readonly RootColumns[],
  ) {
    let unregister: (() => void) | undefined;
    this.roots = new BlobCache<ScanRootFacts>(
      (cache) => {
        const memory = governor.registerCache(cache);
        unregister = memory.unregister?.bind(memory);
        return memory;
      },
      (ref) =>
        `${String((ref as RootKey)[TABLE])}:${String(ref.extent.offset)}:${String(ref.extent.length)}:${String(ref.offset)}:${String(ref.length)}:${String(ref.checksum)}`,
    );
    this.#unregister = unregister;
  }
  #wanted(tableId: number, id: number): boolean {
    return this.sources.some(
      (source) =>
        source.tableId === tableId && (source.columns.includes(id) || source.domainColumn === id),
    );
  }
  covers(tableId: number, columns: readonly number[]): boolean {
    return columns.every((id) => this.#wanted(tableId, id));
  }
  read(
    tableId: number,
    ref: BlobRef,
    pages: ExtentReader,
    budget: ByteBudget,
    columns: readonly number[],
  ): MaybePromise<Held<ScanRootFacts>> {
    const metadata = new MetadataReader(pages);
    // Unplanned consumers retain the complete-root behavior; never mistake omitted for missing.
    if (this.#closed || !this.covers(tableId, columns)) return metadata.groupRoot(ref, budget);
    const key: RootKey = { ...ref, [TABLE]: tableId };
    const hit = this.roots.pin(key);
    if (hit !== null) {
      if (!budget.allocate(hit.size)) {
        this.roots.unpin(key);
        throw new ParseBudgetError(hit.size);
      }
      const size = hit.size;
      return this.#held(key, hit.value, budget, size);
    }
    const select = (full: Held<GroupRoot>): Held<ScanRootFacts> => {
      const scratch = 256 + 8 * full.value.columns.length;
      if (this.#closed || !budget.allocate(scratch)) return full;
      try {
        const facts = {
          rowCount: full.value.rowCount,
          columns: full.value.columns.filter((c) => this.#wanted(tableId, c.columnId)),
        };
        const size = parseCharge(ref.length);
        const value = this.roots.admit(key, facts, size);
        // Move the already acquired full parse charge to the selected hold. The callback
        // captures only that charge, never the discarded complete parsed graph.
        const release = full.release.bind(undefined);
        return this.#held(key, value, budget, size, release);
      } catch (error) {
        full.release();
        throw error;
      } finally {
        budget.free(scratch);
      }
    };
    const full = metadata.groupRoot(ref, budget);
    return full instanceof Promise ? full.then(select) : select(full);
  }
  #held(
    ref: BlobRef,
    value: ScanRootFacts,
    budget: ByteBudget,
    size: number,
    release = () => budget.free(size),
  ): Held<ScanRootFacts> {
    let released = false;
    return {
      value,
      release: () => {
        if (released) return;
        released = true;
        release();
        this.roots.unpin(ref);
      },
    };
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.roots.clear();
    this.#unregister?.();
  }
}
