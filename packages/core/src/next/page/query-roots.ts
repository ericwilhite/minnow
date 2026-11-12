/** Query-local validated scan facts. Complete-root APIs remain complete. */
import { BlobCache } from "../format/blob-cache.js";
import type { ColumnChunk, GroupRoot } from "../format/manifest.js";
import {
  MetadataReader,
  metadataCachesFor,
  type MaybePromise,
  type MetadataCaches,
} from "../format/metadata-reader.js";
import {
  parseCharge,
  ParseBudgetError,
  selectGroupRootColumns,
  groupRootColumnCount,
  isValidatedGroupRoot,
  optionalParseAllocation,
} from "../format/manifest-codec.js";
import type { Held } from "../format/reachable.js";
import type { MemoryGovernor } from "../memory/governor.js";
import type { BlobRef, ByteBudget, ExtentReader } from "../storage/extents.js";
import { QueryDirectoryCache, withQueryDirectories } from "./directory-cache.js";

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
  /** Keep format cache ownership behind the page layer used by the executor. */
  static forReader(
    governor: MemoryGovernor,
    sources: readonly RootColumns[],
    pages: ExtentReader,
  ): QueryRoots {
    return new QueryRoots(governor, sources, metadataCachesFor(pages));
  }
  readonly roots: BlobCache<ScanRootFacts>;
  readonly #unregister: (() => void) | undefined;
  readonly #unregisterDirectories: (() => void) | undefined;
  readonly directories: QueryDirectoryCache;
  #closed = false;
  constructor(
    governor: MemoryGovernor,
    readonly sources: readonly RootColumns[],
    readonly shared?: MetadataCaches,
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
    let unregisterDirectories: (() => void) | undefined;
    try {
      this.directories = new QueryDirectoryCache((cache) => {
        const memory = governor.registerCache(cache);
        unregisterDirectories = memory.unregister?.bind(memory);
        return {
          charge: (bytes) => !this.#closed && memory.charge(bytes),
          release: (bytes) => memory.release(bytes),
        };
      });
    } catch (error) {
      this.roots.clear();
      this.#unregister?.();
      throw error;
    }
    this.#unregisterDirectories = unregisterDirectories;
  }
  reader(pages: ExtentReader): ExtentReader {
    return this.#closed ? pages : withQueryDirectories(pages, this.directories);
  }
  /** Optional construction workspace, never part of the returned execution minimum. */
  sizingBytes(length: number): number {
    return this.shared?.validatedRoots === true
      ? 2 * parseCharge(length) + 2 * length + 4096
      : parseCharge(length) + length;
  }
  #wanted(tableId: number, id: number): boolean {
    for (const source of this.sources)
      if (source.tableId === tableId && (source.columns.includes(id) || source.domainColumn === id))
        return true;
    return false;
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
    const metadata = new MetadataReader(pages, this.shared);
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
    const select = (full: Held<GroupRoot>): MaybePromise<Held<ScanRootFacts>> => {
      const scratch = 256 + 8 * groupRootColumnCount(full.value);
      const size = parseCharge(ref.length);
      // A compact owner view produces NEW refs/stat bounds. Fund those independently of
      // the owner's possible complete graph before materializing, then hand off that credit.
      const separate = isValidatedGroupRoot(full.value) ? size : 0;
      if (this.#closed) return full;
      if (!optionalParseAllocation(budget, scratch + separate)) {
        if (separate === 0) return full;
        full.release();
        return new MetadataReader(pages).groupRoot(ref, budget);
      }
      let transferred = false;
      try {
        const facts = {
          rowCount: full.value.rowCount,
          // A predicate avoids an unbounded concatenation of IDs from repeated table aliases.
          columns: selectGroupRootColumns(full.value, (id) => this.#wanted(tableId, id)),
        };
        const value = this.roots.admit(key, facts, size);
        if (separate !== 0) {
          transferred = true;
          full.release();
          return this.#held(key, value, budget, size);
        }
        // Move the already acquired full parse charge to the selected hold. The callback
        // captures only that charge, never the discarded complete parsed graph.
        const release = full.release.bind(undefined);
        return this.#held(key, value, budget, size, release);
      } catch (error) {
        full.release();
        throw error;
      } finally {
        budget.free(scratch + (transferred ? 0 : separate));
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
    this.directories.clear();
    this.#unregisterDirectories?.();
    this.#unregister?.();
  }
}
