/**
 * Gate A's streaming export, page-format half: `SELECT *` over every row of a table as one
 * sequential decode stream. Shared by the Node bench over files (scripts/export-bench.mts) and the
 * OPFS bench in a browser worker (packages/core/browser/next-export-worker.ts), so both measure
 * the same loop.
 *
 * Per group, each column's chunk is opened (directory only); the stream then emits batches of
 * 2,048 rows: for each column, release the pages behind the batch, load the pages it needs plus
 * the next ones within `readahead` stored bytes (`ChunkReader.pagesAhead`, one call so the space
 * coalesces neighbours), and decode the batch into that column's reused buffer (dictionary pages
 * as codes). Nothing is materialized for a whole group. With `once` the reads are a one-pass scan
 * (storage/extents.ts `ReadOptions.once`): nothing they read is kept in the page cache.
 */
import type { BlobCache } from "../../../packages/core/src/next/format/blob-cache.js";
import type { ChunkDirectory } from "../../../packages/core/src/next/page/directory.js";
import { ChunkReader } from "../../../packages/core/src/next/page/reader.js";
import { targetFor } from "../../../packages/core/src/next/page/test-support.js";
import {
  PageEncoding,
  type ColumnChunk,
  type PageEntry,
} from "../../../packages/core/src/next/format/manifest.js";
import type {
  BlobRef,
  ByteBudget,
  ExtentReader,
  ExtentRef,
} from "../../../packages/core/src/next/storage/extents.js";
import { ENTRY_OVERHEAD } from "../../../packages/core/src/next/storage/page-cache.js";
import type { FlatVector } from "../../../packages/core/src/next/vector/vector.js";

export const EXPORT_BATCH = 2048;

export interface ExportGroup {
  readonly rows: number;
  readonly chunks: readonly ColumnChunk[];
}

/** Exclusive wall phases of the page-stream bench, not SQL execution/result transport. */
export interface ExportPhases {
  active: boolean;
  readMs: number;
  readCalls: number;
  decodeMs: number;
  decodeCalls: number;
  gatherCalls: number;
  compressedPages: number;
  assemblyMs: number;
  otherLoadMs: number;
  /** This bench does range decode only; no gathers, decompression or row transfers. */
  gatherMs: number;
  decompressMs: number;
  transferMs: number;
}

export function exportPhases(): ExportPhases {
  return {
    active: false,
    readMs: 0,
    readCalls: 0,
    decodeMs: 0,
    decodeCalls: 0,
    gatherCalls: 0,
    compressedPages: 0,
    assemblyMs: 0,
    otherLoadMs: 0,
    gatherMs: 0,
    decompressMs: 0,
    transferMs: 0,
  };
}

export interface ExportOptions {
  readonly budget: ByteBudget;
  readonly directories: BlobCache<ChunkDirectory>;
  readonly once: boolean;
  /**
   * Per-column readahead: stored bytes of pages a column loads ahead of its batch (0: only the
   * batch's pages). Ignored when `windowBytes` is set.
   */
  readonly readahead: number;
  /**
   * Row-window readahead (increment 9): every column loads the same window of rows at once,
   * about `windowBytes` stored bytes across all columns, in one `ChunkReader.loadTogether` in
   * the order of their data in the file, then the stream decodes the window's batches with no
   * loads. A group is then read as
   * a few hundred large, nearly sequential reads instead of a strided read per column per
   * batch, which the OS's own readahead can stream. 0 or absent: per-column readahead.
   */
  readonly windowBytes?: number;
  /** Called every 8 batches (memory sampling). */
  readonly sample?: () => void;
  readonly phases?: ExportPhases;
}

export interface ExportResult {
  readonly rows: number;
  readonly ms: number;
  /** Time spent waiting for page loads (the storage half). */
  readonly loadMs: number;
  /** Time spent opening chunks (directories). */
  readonly openMs: number;
  /** Load calls issued (each coalesces neighbouring pages into platform reads). */
  readonly loads: number;
}

interface Buffer {
  target: FlatVector | null;
  readonly codes: Int32Array;
  readonly validity: Uint32Array;
}

/** One reused batch buffer per column, kept across passes (the executor reuses its own). */
const buffers = new Map<number, Buffer>();

function profileReader(reader: ChunkReader, phases: ExportPhases): void {
  const timed =
    <Args extends unknown[]>(fn: (...args: Args) => void, gather: boolean) =>
    (...args: Args): void => {
      const start = performance.now();
      try {
        fn(...args);
      } finally {
        const elapsed = performance.now() - start;
        if (gather) {
          phases.gatherMs += elapsed;
          phases.gatherCalls++;
        } else {
          phases.decodeMs += elapsed;
          phases.decodeCalls++;
        }
      }
    };
  reader.decodeRange = timed(reader.decodeRange.bind(reader), false);
  reader.decodeCodes = timed(reader.decodeCodes.bind(reader), false);
  reader.gather = timed(reader.gather.bind(reader), true);
  reader.gatherCodes = timed(reader.gatherCodes.bind(reader), true);
}

export async function exportStream(
  space: ExtentReader,
  groups: readonly ExportGroup[],
  options: ExportOptions,
): Promise<ExportResult> {
  const started = performance.now();
  let loadMs = 0;
  let openMs = 0;
  let loads = 0;
  let rows = 0;
  let batches = 0;
  const open = { budget: options.budget, directories: options.directories, once: options.once };
  const windowBytes = options.windowBytes ?? 0;
  for (const group of groups) {
    let t = performance.now();
    const readers: ChunkReader[] = [];
    for (const chunk of group.chunks) {
      const reader = await ChunkReader.open(space, chunk, open);
      if (options.phases) profileReader(reader, options.phases);
      readers.push(reader);
    }
    if (options.phases) {
      for (const reader of readers)
        for (const page of reader.directory.pages)
          if ((page.encoding & PageEncoding.gzip) !== 0) options.phases.compressedPages++;
      for (const reader of readers)
        if (
          reader.directory.dictionary &&
          (reader.directory.dictionary.encoding & PageEncoding.gzip) !== 0
        )
          options.phases.compressedPages++;
      if (options.phases.compressedPages > 0)
        throw new Error("Phase profiling expects uncompressed flushed pages");
    }
    openMs += performance.now() - t;
    const loadedFrom = readers.map(() => 0);
    /** Pages behind `from` were emitted: let them go. */
    const releaseBehind = (column: number, reader: ChunkReader, from: number, to: number) => {
      const [firstPage] = reader.pagesInRange(from, to);
      const behind = loadedFrom[column] ?? 0;
      if (firstPage > behind) {
        reader.release(Array.from({ length: firstPage - behind }, (_, k) => behind + k));
        loadedFrom[column] = firstPage;
      }
    };
    // The row window: whole batches covering about `windowBytes` of this group's stored bytes.
    const groupBytes = group.chunks.reduce((sum, chunk) => sum + chunk.data.length, 0);
    const windowRows =
      windowBytes > 0
        ? Math.min(
            group.rows,
            Math.max(
              1,
              Math.floor((windowBytes * group.rows) / Math.max(1, groupBytes) / EXPORT_BATCH),
            ) * EXPORT_BATCH,
          )
        : 0;
    /** Columns in the order of their data in the file. */
    const byAddress = readers
      .map((reader, column) => ({ reader, column }))
      .sort(
        (a, b) =>
          a.reader.chunk.data.extent.offset +
          a.reader.chunk.data.offset -
          (b.reader.chunk.data.extent.offset + b.reader.chunk.data.offset),
      );
    for (let from = 0; from < group.rows; from += EXPORT_BATCH) {
      const to = Math.min(group.rows, from + EXPORT_BATCH);
      if (windowRows > 0 && from % windowRows === 0) {
        // Every column's pages of the window, in one shared load (ChunkReader.loadTogether):
        // the space coalesces neighbouring columns' pages into large reads.
        const end = Math.min(group.rows, from + windowRows);
        const loadsOf: Array<{ reader: ChunkReader; pages: number[] }> = [];
        for (const { reader, column } of byAddress) {
          releaseBehind(column, reader, from, to);
          const [firstPage, endPage] = reader.pagesInRange(from, end);
          const pages: number[] = [];
          for (let page = firstPage; page < endPage; page++)
            if (!reader.isLoaded(page)) pages.push(page);
          if (pages.length > 0) loadsOf.push({ reader, pages });
        }
        if (loadsOf.length > 0) {
          t = performance.now();
          await ChunkReader.loadTogether(loadsOf);
          loadMs += performance.now() - t;
          loads++;
        }
      }
      for (const [column, reader] of readers.entries()) {
        if (windowRows === 0) {
          const [firstPage, endPage] = reader.pagesInRange(from, to);
          releaseBehind(column, reader, from, to);
          if (!reader.isLoaded(Math.max(firstPage, endPage - 1))) {
            const missing = reader
              .pagesAhead(from, to, options.readahead)
              .filter((page) => !reader.isLoaded(page));
            if (missing.length > 0) {
              t = performance.now();
              await reader.load(missing);
              loadMs += performance.now() - t;
              loads++;
            }
          }
        }
        let buffer = buffers.get(column);
        if (buffer === undefined) {
          buffer = {
            target: null,
            codes: new Int32Array(EXPORT_BATCH),
            validity: new Uint32Array(EXPORT_BATCH / 32),
          };
          buffers.set(column, buffer);
        }
        if (reader.isDictionaryRange(from, to)) {
          reader.decodeCodes(from, to, buffer.codes, buffer.validity);
        } else {
          const bytes = reader.varlenBytes(from, to);
          const kept = buffer.target;
          // A buffer of another column's type (a reused slot) or too small for this batch.
          if (
            kept === null ||
            kept.type.kind !== reader.chunk.type.kind ||
            (kept.data.layout === "varlen" && kept.data.bytes.length < bytes)
          )
            buffer.target = targetFor(reader, EXPORT_BATCH, Math.max(bytes, 256 * 1024));
          reader.decodeRange(from, to, buffer.target ?? fail(), 0);
        }
      }
      rows += to - from;
      if ((++batches & 7) === 0) options.sample?.();
    }
    for (const reader of readers) reader.close();
  }
  const ms = performance.now() - started;
  if (options.phases) {
    options.phases.otherLoadMs = openMs + loadMs - options.phases.readMs;
    options.phases.assemblyMs =
      ms - openMs - loadMs - options.phases.decodeMs - options.phases.gatherMs;
  }
  return { rows, ms, loadMs, openMs, loads };
}

function fail(): never {
  throw new Error("expected a batch buffer");
}

/**
 * The bench's measured working set (review B2-p2g #2): a hot set, the first page of 30 columns of
 * `outside` (a group the export does not read), used twice (protected); and every page of those
 * columns in the export's first group, used once (probation), which the export then hits. A scan
 * that promoted its hits would grow protected past the hot set and push it out of the cache.
 */
export interface WorkingSet {
  readonly hot: readonly BlobRef[];
  readonly scanned: readonly BlobRef[];
  /** What the hot set is charged unpinned (bytes and entry overhead). */
  readonly hotBytes: number;
}

const pageRef = (chunk: ColumnChunk, entry: PageEntry): BlobRef => ({
  extent: chunk.data.extent,
  offset: chunk.data.offset + entry.offset,
  length: entry.length,
  checksum: entry.checksum,
});

export async function establishWorkingSet(
  space: ExtentReader,
  first: ExportGroup,
  outside: ExportGroup,
  options: Pick<ExportOptions, "budget" | "directories">,
): Promise<WorkingSet> {
  const pagesOf = async (chunk: ColumnChunk, all: boolean): Promise<BlobRef[]> => {
    const reader = await ChunkReader.open(space, chunk, options);
    const entries = all ? reader.directory.pages : reader.directory.pages.slice(0, 1);
    reader.close();
    return entries.map((entry) => pageRef(chunk, entry));
  };
  const hot: BlobRef[] = [];
  const scanned: BlobRef[] = [];
  for (const chunk of outside.chunks.slice(0, 30)) hot.push(...(await pagesOf(chunk, false)));
  for (const chunk of first.chunks.slice(0, 30)) scanned.push(...(await pagesOf(chunk, true)));
  for (let use = 0; use < 2; use++) (await space.read(hot, { budget: options.budget })).release();
  for (let at = 0; at < scanned.length; at += 32)
    (await space.read(scanned.slice(at, at + 32), { budget: options.budget })).release();
  const hotBytes = hot.reduce((sum, ref) => sum + ref.length + ENTRY_OVERHEAD, 0);
  return { hot, scanned, hotBytes };
}

/**
 * Evicts everything but the hot set's size from `cache` (destructive: run after the last pass)
 * and counts what survived. An export that promoted nothing leaves the hot set whole.
 */
export function survivors(
  space: ExtentReader,
  cache: { readonly evictableBytes: number; evict(bytes: number): number },
  set: WorkingSet,
): { readonly hot: number; readonly scanned: number } {
  cache.evict(cache.evictableBytes - set.hotBytes);
  const resident = (ref: BlobRef): boolean => space.peek(ref, { once: true }) !== null;
  return { hot: set.hot.filter(resident).length, scanned: set.scanned.filter(resident).length };
}

/**
 * `chunks` with every extent reference moved as `moved` says (by old offset): the chunks of a
 * byte copy of a group's extents written at new addresses (the export builders make a large table
 * from a few generated groups this way; encoding is not what an export measures).
 */
export function rebaseExtents(
  chunks: readonly ColumnChunk[],
  moved: ReadonlyMap<number, ExtentRef>,
): ColumnChunk[] {
  const rebase = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(rebase);
    if (value === null || typeof value !== "object" || value instanceof Uint8Array) return value;
    const out: Record<string, unknown> = {};
    for (const [key, field] of Object.entries(value as Record<string, unknown>)) {
      if (key === "extent" && field !== null && typeof field === "object") {
        const extent = field as ExtentRef;
        out[key] = moved.get(extent.offset) ?? extent;
      } else out[key] = rebase(field);
    }
    return out;
  };
  return rebase(chunks) as ColumnChunk[];
}
