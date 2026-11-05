/**
 * The reachability walk (manifest.ts "Reclaim"): every data-space range a manifest root
 * references, so the owner can rebuild its free-space map on open and free what no durable or
 * pinned root reaches. Crash orphans need no records: they are simply absent from every walk.
 *
 * What a root reaches:
 * - the catalog; each table's statistics blob;
 * - each table's group directory (`groups`) and the group directories of staged large
 *   transactions (`staged[].groups`);
 * - through each `GroupRef`: its group root and its deletion bitmap;
 * - through each `GroupRoot`: its row-id column (merged groups) and every column chunk's data
 *   extent, page directory and distinct sketch (both usually inside the data extent:
 *   page/writer.ts);
 * - every index run of the key index and the secondary indexes: entries, fences, Bloom filter.
 *
 * The walk reads only metadata (group directories and group roots), through a `ManifestReader`
 * that decodes those blobs and checks their checksums (verify once). Pages are never read.
 * Several chunks, groups and roots may share one extent (manifest.ts "Immutability and
 * sharing"): `occupied` merges the list into disjoint `DATA_ALIGN`-rounded ranges.
 *
 * Cost: one read per table group directory and one per group root, issued in parallel batches
 * of `concurrency`; O(groups × columns) memory for the result, never O(rows).
 */
import type { DataRange } from "../storage/contract.js";
import { DATA_ALIGN } from "../storage/contract.js";
import type {
  BlobRef,
  ExtentRef,
  GroupRef,
  GroupRoot,
  IndexRoot,
  ManifestRoot,
} from "./manifest.js";

/** Decodes (and verifies) the metadata blobs the walk descends through. */
export interface ManifestReader {
  /** The `GroupRef[]` a group directory blob holds. */
  groups(ref: BlobRef): Promise<readonly GroupRef[]>;
  /** The `GroupRoot` a group root blob holds. */
  groupRoot(ref: BlobRef): Promise<GroupRoot>;
}

export type ReachableKind =
  | "catalog"
  | "table-statistics"
  | "group-directory"
  | "staged-group-directory"
  | "group-root"
  | "deletions"
  | "row-ids"
  | "chunk-data"
  | "page-directory"
  | "distinct-sketch"
  | "index-entries"
  | "index-fences"
  | "index-bloom";

export interface ReachableRange extends DataRange {
  readonly kind: ReachableKind;
  /** The table it belongs to (null for the catalog). */
  readonly tableId: number | null;
  /** Page directories and sketches: the chunk extent they lie in (page/writer.ts puts them there). */
  readonly within?: ExtentRef;
}

export interface ReachableOptions {
  /** Group roots read at once (default 32). */
  readonly concurrency?: number;
}

/**
 * Every range `root` references, in walk order, with duplicates (a shared extent appears once per
 * reference). Rejects with whatever the reader throws (a checksum failure is corruption: the
 * caller must not compute free space from a partial walk).
 */
export async function reachable(
  root: ManifestRoot,
  reader: ManifestReader,
  options: ReachableOptions = {},
): Promise<ReachableRange[]> {
  const concurrency = Math.max(1, options.concurrency ?? 32);
  const out: ReachableRange[] = [];
  const add = (
    ref: BlobRef | ExtentRef | null,
    kind: ReachableKind,
    tableId: number | null,
    within?: ExtentRef,
  ): void => {
    if (ref === null || ref.length <= 0) return;
    const range = { offset: ref.offset, length: ref.length, kind, tableId };
    out.push(within === undefined ? range : { ...range, within });
  };
  add(root.catalog, "catalog", null);
  for (const table of root.tables) {
    const tableId = table.tableId;
    add(table.statistics, "table-statistics", tableId);
    for (const index of [table.keyIndex, ...table.secondaryIndexes]) addIndex(index, tableId, add);
    const directories: Array<{ ref: BlobRef; kind: ReachableKind }> = [
      { ref: table.groups, kind: "group-directory" },
      ...table.staged.map((staged) => ({
        ref: staged.groups,
        kind: "staged-group-directory" as const,
      })),
    ];
    for (const { ref, kind } of directories) {
      if (ref.length === 0) continue;
      add(ref, kind, tableId);
      const groups = await reader.groups(ref);
      for (let start = 0; start < groups.length; start += concurrency) {
        const batch = groups.slice(start, start + concurrency);
        const roots = await Promise.all(batch.map((group) => reader.groupRoot(group.root)));
        batch.forEach((group, index) => {
          add(group.root, "group-root", tableId);
          add(group.deletions, "deletions", tableId);
          const groupRoot = roots[index];
          if (groupRoot === undefined) return;
          if (groupRoot.rowIds.kind === "column") add(groupRoot.rowIds.ref, "row-ids", tableId);
          for (const chunk of groupRoot.columns) {
            add(chunk.data, "chunk-data", tableId);
            add(chunk.directory, "page-directory", tableId, chunk.data);
            add(chunk.distinctSketch, "distinct-sketch", tableId, chunk.data);
          }
        });
      }
    }
  }
  return out;
}

function addIndex(
  index: IndexRoot | null,
  tableId: number,
  add: (ref: BlobRef, kind: ReachableKind, tableId: number) => void,
): void {
  if (index === null) return;
  for (const run of index.runs) {
    add(run.entries, "index-entries", tableId);
    add(run.fences, "index-fences", tableId);
    add(run.bloom, "index-bloom", tableId);
  }
}

/**
 * The disjoint, ascending ranges the given references occupy, each widened to `DATA_ALIGN`
 * boundaries (the allocator's unit), with overlapping and touching ranges merged. Everything
 * else below the data length is free.
 */
export function occupied(ranges: readonly DataRange[]): DataRange[] {
  const aligned = ranges
    .filter((range) => range.length > 0)
    .map((range) => {
      const start = Math.floor(range.offset / DATA_ALIGN) * DATA_ALIGN;
      const end = Math.ceil((range.offset + range.length) / DATA_ALIGN) * DATA_ALIGN;
      return { offset: start, end };
    })
    .sort((a, b) => a.offset - b.offset);
  const out: DataRange[] = [];
  let current: { offset: number; end: number } | null = null;
  for (const range of aligned) {
    if (current !== null && range.offset <= current.end)
      current.end = Math.max(current.end, range.end);
    else {
      if (current !== null)
        out.push({ offset: current.offset, length: current.end - current.offset });
      current = { ...range };
    }
  }
  if (current !== null) out.push({ offset: current.offset, length: current.end - current.offset });
  return out;
}

// ── The storage track's open walk (storage/extents.ts `ReachableExtents`) ─────────────────────

/** A grant's suballocator (storage/extents.ts `ByteBudget`). */
export interface WalkBudget {
  allocate(bytes: number): boolean;
  free(bytes: number): void;
}

/** The extent space's reader (storage/extents.ts `ExtentReader`), stated structurally. */
export interface WalkReader {
  read(
    refs: readonly BlobRef[],
    options: { readonly budget: WalkBudget },
  ): Promise<{ readonly bytes: readonly Uint8Array[]; release(): void }>;
}

/** The byte codecs of the metadata the walk descends through (format track, to come). */
export interface ManifestCodecs {
  decodeRoot(bytes: Uint8Array): ManifestRoot;
  decodeGroups(bytes: Uint8Array): readonly GroupRef[];
  decodeGroupRoot(bytes: Uint8Array): GroupRoot;
}

/**
 * `reachable` in the shape the extent space opens with: root bytes in, extents out. Metadata
 * blobs are read through the space (verified, cached), each lease charged to `budget` only while
 * it is decoded.
 *
 * Extents, not blobs: a chunk's data extent covers its page directory and sketch, so those are
 * listed only when they lie outside it. Every other blob the manifest names (catalog, group
 * directories, group roots, deletion bitmaps, row-id columns, statistics, index entries, fences,
 * Bloom filters) is taken to be **its own extent**, as `ExtentWriter.write([blob])` produces it:
 * the manifest stores `BlobRef`s only, so the write path must write each such blob alone.
 */
export function reachableExtents(
  codecs: ManifestCodecs,
): (root: Uint8Array, reader: WalkReader, budget: WalkBudget) => AsyncIterable<ExtentRef> {
  return async function* walk(root, reader, budget) {
    const decode = async <T>(ref: BlobRef, decoder: (bytes: Uint8Array) => T): Promise<T> => {
      const lease = await reader.read([ref], { budget });
      try {
        return decoder(lease.bytes[0] ?? new Uint8Array(0));
      } finally {
        lease.release();
      }
    };
    const ranges = await reachable(codecs.decodeRoot(root), {
      groups: (ref) => decode(ref, (bytes) => codecs.decodeGroups(bytes)),
      groupRoot: (ref) => decode(ref, (bytes) => codecs.decodeGroupRoot(bytes)),
    });
    const seen = new Set<string>();
    for (const range of ranges) {
      const inside =
        range.within !== undefined &&
        range.offset >= range.within.offset &&
        range.offset + range.length <= range.within.offset + range.within.length;
      if (inside) continue;
      const key = `${String(range.offset)}:${String(range.length)}`;
      if (seen.has(key)) continue;
      seen.add(key);
      yield { offset: range.offset, length: range.length };
    }
  };
}
