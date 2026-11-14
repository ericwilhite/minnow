/**
 * The full reachability walk (manifest.ts "Reclaim"), used by audit and test-build open checks.
 * Startup uses listedRootExtents instead: it reads list pages, never group roots. Crash orphans
 * need no records: they are absent from the roots' reachability sources.
 *
 * What a root reaches:
 * - extent-list pages and the catalog; each table's statistics blob;
 * - each table's group directory (`groups`: an index blob and its pages) and, for every staged
 *   large transaction, its group directory (`staged[].groups`) and the index runs built for it
 *   (`staged[].indexRuns`);
 * - through each `GroupRef`: its group root and its deletion bitmap;
 * - through each `GroupRoot`: its row-id column (merged groups) and every column chunk's data
 *   extent, page directory and distinct sketch;
 * - every index run (key index, secondary indexes, staged runs): entries, fences, Bloom filter.
 *
 * Units (ruling R15.1): a `BlobRef` carries the extent it lives in, and so does a chunk's data
 * range (`ChunkData`, which has no checksum); the space takes the extent and never maps an address
 * back to one. Duplicates are allowed (packed and shared extents); `reachableExtents` drops only a
 * repeat of the extent it yielded last, so its memory stays bounded.
 *
 * The walk checks what it reads against what references it (review B2-p2a #6) and throws
 * `ManifestCorruptionError` on a contradiction, since free space computed from a wrong directory
 * frees live data:
 * - a directory index entry against its page: group count, first and last row id, row and live
 *   row totals;
 * - a group against its group root: row count, and for dense row ids, the row-id range;
 * - a table's directory against the table: group count and live rows (`TableRoot.groupCount`,
 *   `liveRowCount`), and every row id below `nextRowId`;
 * - a staged directory against its entry: stored rows (`rowCount`), from `firstRowId` on.
 *
 * The walk streams: per table it reads the group directory's index, then one directory page at a
 * time (manifest-codec.ts "paged group directory"), then that page's group roots in batches of
 * `concurrency`, yielding as it goes. It holds one index, one page and one batch of group roots at
 * a time, each released (its parse charge returned) when the walk moves past it. Data pages are
 * never read.
 */
import { walkCatalogTree, type CatalogTreeReader } from "./catalog-tree.js";
import {
  decodeCatalogRoot,
  decodeCatalogPage,
  type CatalogRoot,
  type CatalogPage,
} from "./catalog-pages.js";
import { sortedListedExtents } from "../storage/extent-list.js";
import type {
  BlobRef,
  ByteBudget,
  ExtentReader,
  ExtentRef,
  ReachableExtents,
} from "../storage/extents.js";
import { valid } from "./bytes.js";
import {
  decodeGroupIndex,
  decodeGroupPage,
  decodeGroupRoot,
  decodeManifestRoot,
  isEmptyDirectory,
  type GroupPageEntry,
  type Parsed,
} from "./manifest-codec.js";
import type {
  ChunkData,
  GroupRef,
  GroupRoot,
  IndexRoot,
  ManifestRoot,
  RunRef,
} from "./manifest.js";

/** A decoded blob the walk holds while it uses it; `release` returns its parse charge. */
export interface Held<T> {
  readonly value: T;
  release(): void;
}

/** Reads and decodes the metadata blobs the walk descends through (manifest-codec.ts). */
export interface ManifestReader extends CatalogTreeReader {
  /** The page entries of a group directory's index blob (`TableRoot.groups`). */
  groupIndex(ref: BlobRef): Promise<Held<readonly GroupPageEntry[]>>;
  /** The `GroupRef`s of one directory page. */
  groupPage(ref: BlobRef): Promise<Held<readonly GroupRef[]>>;
  /** The `GroupRoot` a group root blob holds. */
  groupRoot(ref: BlobRef): Promise<Held<GroupRoot>>;
}

export type ReachableKind =
  | "catalog"
  | "catalog-index"
  | "catalog-page"
  | "extent-list-page"
  | "table-statistics"
  | "group-directory"
  | "staged-group-directory"
  | "group-directory-page"
  | "group-root"
  | "deletions"
  | "row-ids"
  | "chunk-data"
  | "page-directory"
  | "distinct-sketch"
  | "index-entries"
  | "index-fences"
  | "index-bloom";

export interface Reached {
  /** A blob, or a column chunk's data range (`ChunkData`: inside its extent, no checksum). */
  readonly ref: BlobRef | ChunkData;
  readonly kind: ReachableKind;
  /** The table it belongs to (null for the root-owned catalog and extent-list pages). */
  readonly tableId: number | null;
}

export interface ReachableOptions {
  /** Group roots read at once (default 32). */
  readonly concurrency?: number;
  /** A table-only retirement walk must not read root-owned metadata. */
  readonly skipCatalog?: boolean;
}

function* runsOf(
  index: IndexRoot | { readonly runs: readonly RunRef[] } | null,
  tableId: number,
): Generator<Reached> {
  if (index === null) return;
  for (const run of index.runs) {
    yield { ref: run.entries, kind: "index-entries", tableId };
    yield { ref: run.fences, kind: "index-fences", tableId };
    yield { ref: run.bloom, kind: "index-bloom", tableId };
  }
}

/**
 * Every reference `root` reaches, streamed in walk order. Rejects with whatever the reader throws
 * (a checksum failure is corruption: a caller must not compute free space from a partial walk).
 */
export async function* walkReachable(
  root: ManifestRoot,
  reader: ManifestReader,
  options: ReachableOptions = {},
): AsyncGenerator<Reached> {
  const concurrency = Math.max(1, options.concurrency ?? 32);
  if (!options.skipCatalog)
    for await (const event of walkCatalogTree(root.catalog, reader))
      yield {
        ref: event.ref,
        kind:
          event.kind === "catalog-page"
            ? "catalog-page"
            : event.ref === root.catalog
              ? "catalog"
              : "catalog-index",
        tableId: null,
      };
  for (const page of root.extentList)
    yield { ref: page.blob, kind: "extent-list-page", tableId: null };
  for (const table of root.tables) {
    const tableId = table.tableId;
    if (table.statistics !== null)
      yield { ref: table.statistics, kind: "table-statistics", tableId };
    for (const index of [table.keyIndex, ...table.secondaryIndexes]) yield* runsOf(index, tableId);
    const directories: Directory[] = [
      {
        ref: table.groups,
        kind: "group-directory",
        expect: (totals) => {
          valid(totals.groupCount === table.groupCount, "Table group count disagrees");
          valid(totals.liveRowCount === table.liveRowCount, "Table live row count disagrees");
        },
      },
    ];
    for (const staged of table.staged) {
      directories.push({
        ref: staged.groups,
        kind: "staged-group-directory",
        expect: (totals) => {
          valid(totals.rowCount === staged.rowCount, "Staged row count disagrees");
          valid(
            totals.firstRowId === null || totals.firstRowId >= staged.firstRowId,
            "Staged rows before the staged range",
          );
        },
      });
      for (const index of staged.indexRuns) yield* runsOf(index, tableId);
    }
    for (const { ref, kind, expect } of directories) {
      const totals: Totals = { groupCount: 0, rowCount: 0, liveRowCount: 0, firstRowId: null };
      if (!isEmptyDirectory(ref)) {
        yield { ref, kind, tableId };
        const index = await reader.groupIndex(ref);
        try {
          for (const page of index.value) {
            yield { ref: page.ref, kind: "group-directory-page", tableId };
            const groups = await reader.groupPage(page.ref);
            try {
              checkPage(page, groups.value);
              yield* groupsOf(groups.value, reader, concurrency, tableId);
            } finally {
              groups.release();
            }
            totals.groupCount += page.groupCount;
            totals.rowCount += page.rowCount;
            totals.liveRowCount += page.liveRowCount;
            totals.firstRowId ??= page.firstRowId;
            valid(page.lastRowId < table.nextRowId, "A row id at or past the table's next row id");
          }
        } finally {
          index.release();
        }
      }
      expect(totals);
    }
  }
}

interface Totals {
  groupCount: number;
  rowCount: number;
  liveRowCount: number;
  firstRowId: number | null;
}

interface Directory {
  readonly ref: BlobRef;
  readonly kind: ReachableKind;
  /** Throws `ManifestCorruptionError` unless the directory's totals match what references it. */
  readonly expect: (totals: Totals) => void;
}

/** A directory index entry describes the page it references. */
function checkPage(entry: GroupPageEntry, groups: readonly GroupRef[]): void {
  let rowCount = 0;
  let liveRowCount = 0;
  for (const group of groups) {
    rowCount += group.rowCount;
    liveRowCount += group.liveRowCount;
  }
  valid(
    groups.length === entry.groupCount &&
      groups[0]?.firstRowId === entry.firstRowId &&
      groups[groups.length - 1]?.lastRowId === entry.lastRowId &&
      rowCount === entry.rowCount &&
      liveRowCount === entry.liveRowCount,
    "A directory index entry disagrees with its page",
  );
}

/** A group's reference describes its group root. */
function checkRoot(group: GroupRef, root: GroupRoot): void {
  valid(root.rowCount === group.rowCount, "A group root's row count disagrees with its group");
  valid(
    root.rowIds.kind !== "dense" || group.lastRowId - group.firstRowId + 1 === root.rowCount,
    "A dense group's row-id range disagrees with its row count",
  );
}

async function* groupsOf(
  groups: readonly GroupRef[],
  reader: ManifestReader,
  concurrency: number,
  tableId: number,
): AsyncGenerator<Reached> {
  for (let start = 0; start < groups.length; start += concurrency) {
    const batch = groups.slice(start, start + concurrency);
    const settled = await Promise.allSettled(batch.map((group) => reader.groupRoot(group.root)));
    const roots = settled.flatMap((result) =>
      result.status === "fulfilled" ? [result.value] : [],
    );
    try {
      const failed = settled.find((result) => result.status === "rejected");
      if (failed !== undefined) throw failed.reason;
      for (const [index, group] of batch.entries()) {
        yield { ref: group.root, kind: "group-root", tableId };
        if (group.deletions !== null) yield { ref: group.deletions, kind: "deletions", tableId };
        const groupRoot = roots[index]?.value;
        if (groupRoot === undefined) continue;
        checkRoot(group, groupRoot);
        if (groupRoot.rowIds.kind === "column")
          yield { ref: groupRoot.rowIds.ref, kind: "row-ids", tableId };
        for (const chunk of groupRoot.columns) {
          yield { ref: chunk.data, kind: "chunk-data", tableId };
          yield { ref: chunk.directory, kind: "page-directory", tableId };
          if (chunk.distinctSketch !== null)
            yield { ref: chunk.distinctSketch, kind: "distinct-sketch", tableId };
        }
      }
    } finally {
      for (const root of roots) root.release();
    }
  }
}

/** The whole walk as a list (tests and audits; the open uses the stream). */
export async function reachable(
  root: ManifestRoot,
  reader: ManifestReader,
  options: ReachableOptions = {},
): Promise<Reached[]> {
  const out: Reached[] = [];
  for await (const reached of walkReachable(root, reader, options)) out.push(reached);
  return out;
}

/** The decoders the walk uses (manifest-codec.ts by default; tests substitute their own). */
export interface ManifestCodecs {
  decodeCatalogRoot(bytes: Uint8Array, budget: ByteBudget): Parsed<CatalogRoot>;
  decodeCatalogPage(bytes: Uint8Array, budget: ByteBudget): Parsed<CatalogPage>;
  decodeRoot(bytes: Uint8Array, budget: ByteBudget): Parsed<ManifestRoot>;
  decodeGroupIndex(bytes: Uint8Array, budget: ByteBudget): Parsed<readonly GroupPageEntry[]>;
  decodeGroupPage(bytes: Uint8Array, budget: ByteBudget): Parsed<readonly GroupRef[]>;
  decodeGroupRoot(bytes: Uint8Array, budget: ByteBudget): Parsed<GroupRoot>;
}

export const MANIFEST_CODECS: ManifestCodecs = {
  decodeCatalogRoot,
  decodeCatalogPage,
  decodeRoot: decodeManifestRoot,
  decodeGroupIndex,
  decodeGroupPage,
  decodeGroupRoot,
};

/**
 * `walkReachable` in the shape the extent space opens with (storage/extents.ts
 * `ReachableExtents`): root bytes in, blobs and extents out, streamed. Metadata blobs are read
 * through the space (verified, cached); each lease is held only while its blob is decoded, and
 * each decoded value's parse charge only while the walk uses it, both on `budget`. The space
 * takes a blob's extent; a chunk's data range has no checksum, so it is given as its extent. A
 * reference in the same extent as the one yielded just before it is not repeated.
 */
export function reachableExtents(
  codecs: ManifestCodecs = MANIFEST_CODECS,
  options: ReachableOptions = {},
): (
  root: Uint8Array,
  reader: ExtentReader,
  budget: ByteBudget,
) => AsyncIterable<BlobRef | ExtentRef> {
  return async function* walk(root, reader, budget) {
    const decode = async <T>(
      ref: BlobRef,
      decoder: (bytes: Uint8Array, budget: ByteBudget) => Parsed<T>,
    ): Promise<Held<T>> => {
      const lease = await reader.read([ref], { budget });
      let parsed: Parsed<T>;
      try {
        parsed = decoder(lease.bytes[0] ?? new Uint8Array(0), budget);
      } finally {
        lease.release();
      }
      let released = false;
      return {
        value: parsed.value,
        release: () => {
          if (released) return;
          released = true;
          budget.free(parsed.charge);
        },
      };
    };
    const manifest = codecs.decodeRoot(root, budget);
    try {
      let last: ExtentRef | null = null;
      for await (const { ref } of walkReachable(
        manifest.value,
        {
          catalogRoot: (blob) => decode(blob, (bytes, b) => codecs.decodeCatalogRoot(bytes, b)),
          catalogPage: (blob) => decode(blob, (bytes, b) => codecs.decodeCatalogPage(bytes, b)),
          groupIndex: (blob) => decode(blob, (bytes, b) => codecs.decodeGroupIndex(bytes, b)),
          groupPage: (blob) => decode(blob, (bytes, b) => codecs.decodeGroupPage(bytes, b)),
          groupRoot: (blob) => decode(blob, (bytes, b) => codecs.decodeGroupRoot(bytes, b)),
        },
        options,
      )) {
        const extent = ref.extent;
        if (last !== null && last.offset === extent.offset && last.length === extent.length)
          continue;
        last = extent;
        yield "checksum" in ref ? ref : extent;
      }
    } finally {
      budget.free(manifest.charge);
    }
  };
}

/**
 * Startup's source of what a root reaches (ruling R25), in the shape the extent space opens with:
 * the root's extent list, read page by page (storage/extent-list.ts `listedExtents`). No group
 * root is read. `reachableExtents` is the full walk the audit checks it against.
 */
export function listedRootExtents(
  codecs: Pick<ManifestCodecs, "decodeRoot"> = MANIFEST_CODECS,
): ReachableExtents {
  const listed: ReachableExtents = async function* (root, reader, budget, work) {
    const manifest = codecs.decodeRoot(root, budget);
    try {
      yield* sortedListedExtents(manifest.value.extentList, reader, budget, work);
    } finally {
      budget.free(manifest.charge);
    }
  };
  return Object.assign(listed, {
    ordered: true,
    openList(root: Uint8Array, budget: ByteBudget) {
      const { value, charge } = codecs.decodeRoot(root, budget);
      let held = true;
      return {
        list: value.extentList,
        release() {
          if (held) {
            held = false;
            budget.free(charge);
          }
        },
      };
    },
  });
}
