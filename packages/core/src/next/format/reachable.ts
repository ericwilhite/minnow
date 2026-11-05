/**
 * The reachability walk (manifest.ts "Reclaim"): everything a manifest root references, so the
 * extent space can rebuild its free map at open and audit it (storage/extents.ts
 * `ReachableExtents`). Crash orphans need no records: they are simply absent from every walk.
 *
 * What a root reaches:
 * - the catalog; each table's statistics blob;
 * - each table's group directory (`groups`) and, for every staged large transaction, its group
 *   directory (`staged[].groups`) and the index runs built for it (`staged[].indexRuns`);
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
 * The walk streams: it reads one group directory, then its group roots in batches of
 * `concurrency`, yielding as it goes. It holds one directory's `GroupRef[]` and one batch of
 * group roots at a time, and every metadata lease only while decoding. Pages are never read.
 */
import type { BlobRef, ByteBudget, ExtentReader, ExtentRef } from "../storage/extents.js";
import type {
  ChunkData,
  GroupRef,
  GroupRoot,
  IndexRoot,
  ManifestRoot,
  RunRef,
} from "./manifest.js";

/** Decodes the metadata blobs the walk descends through. */
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

export interface Reached {
  /** A blob, or a column chunk's data range (`ChunkData`: inside its extent, no checksum). */
  readonly ref: BlobRef | ChunkData;
  readonly kind: ReachableKind;
  /** The table it belongs to (null for the catalog). */
  readonly tableId: number | null;
}

export interface ReachableOptions {
  /** Group roots read at once (default 32). */
  readonly concurrency?: number;
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
  yield { ref: root.catalog, kind: "catalog", tableId: null };
  for (const table of root.tables) {
    const tableId = table.tableId;
    if (table.statistics !== null)
      yield { ref: table.statistics, kind: "table-statistics", tableId };
    for (const index of [table.keyIndex, ...table.secondaryIndexes]) yield* runsOf(index, tableId);
    const directories: Array<{ ref: BlobRef; kind: ReachableKind }> = [
      { ref: table.groups, kind: "group-directory" },
    ];
    for (const staged of table.staged) {
      directories.push({ ref: staged.groups, kind: "staged-group-directory" });
      for (const index of staged.indexRuns) yield* runsOf(index, tableId);
    }
    for (const { ref, kind } of directories) {
      if (ref.length === 0) continue;
      yield { ref, kind, tableId };
      const groups = await reader.groups(ref);
      for (let start = 0; start < groups.length; start += concurrency) {
        const batch = groups.slice(start, start + concurrency);
        const roots = await Promise.all(batch.map((group) => reader.groupRoot(group.root)));
        for (const [index, group] of batch.entries()) {
          yield { ref: group.root, kind: "group-root", tableId };
          if (group.deletions !== null) yield { ref: group.deletions, kind: "deletions", tableId };
          const groupRoot = roots[index];
          if (groupRoot === undefined) continue;
          if (groupRoot.rowIds.kind === "column")
            yield { ref: groupRoot.rowIds.ref, kind: "row-ids", tableId };
          for (const chunk of groupRoot.columns) {
            yield { ref: chunk.data, kind: "chunk-data", tableId };
            yield { ref: chunk.directory, kind: "page-directory", tableId };
            if (chunk.distinctSketch !== null)
              yield { ref: chunk.distinctSketch, kind: "distinct-sketch", tableId };
          }
        }
      }
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

/** The byte codecs of the metadata the walk descends through (format track, Phase 2: R15.3). */
export interface ManifestCodecs {
  decodeRoot(bytes: Uint8Array): ManifestRoot;
  decodeGroups(bytes: Uint8Array): readonly GroupRef[];
  decodeGroupRoot(bytes: Uint8Array): GroupRoot;
}

/**
 * `walkReachable` in the shape the extent space opens with (storage/extents.ts
 * `ReachableExtents`): root bytes in, blobs and extents out, streamed. Metadata blobs are read
 * through the space (verified, cached), each lease charged to `budget` only while it is decoded.
 * A reference in the same extent as the one yielded just before it (a chunk's directory and
 * sketch, the next chunk of a pack) is not repeated.
 */
export function reachableExtents(
  codecs: ManifestCodecs,
  options: ReachableOptions = {},
): (
  root: Uint8Array,
  reader: ExtentReader,
  budget: ByteBudget,
) => AsyncIterable<BlobRef | ExtentRef> {
  return async function* walk(root, reader, budget) {
    const decode = async <T>(ref: BlobRef, decoder: (bytes: Uint8Array) => T): Promise<T> => {
      const lease = await reader.read([ref], { budget });
      try {
        return decoder(lease.bytes[0] ?? new Uint8Array(0));
      } finally {
        lease.release();
      }
    };
    const manifest = codecs.decodeRoot(root);
    let last: ExtentRef | null = null;
    // The space takes a blob's extent; a chunk's data range has no checksum, so it is given as
    // its extent (the unit the space reclaims).
    for await (const { ref } of walkReachable(
      manifest,
      {
        groups: (blob) => decode(blob, (bytes) => codecs.decodeGroups(bytes)),
        groupRoot: (blob) => decode(blob, (bytes) => codecs.decodeGroupRoot(bytes)),
      },
      options,
    )) {
      const extent = ref.extent;
      if (last !== null && last.offset === extent.offset && last.length === extent.length) continue;
      last = extent;
      yield "checksum" in ref ? ref : extent;
    }
  };
}
