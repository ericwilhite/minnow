/**
 * How the executor's `scan` and `fetch` operators read a table: over an access/ snapshot
 * (track B3: segments, statistics, deletions, memory chunks, the pin's verified page reads) and
 * the page track's chunk readers (track B2, `page/reader.ts` `ChunkReader`) for group pages. The
 * executor implements the operators and the production `RowFetcher` (exec/fetch.ts); this file
 * fixes what it calls beyond access/.
 *
 * ## Reading a table
 * - `snapshot.table(tableId, { columns, budget })` (sync when the group directory is cached):
 *   `columns` are every column id the query reads from it (scan, filter and fetch columns);
 *   `budget` is the scan's spillable grant, which pays for the write path's copy-on-pressure of
 *   retained memtables (ruling R5).
 * - **Pruning first.** `segment.statistics(columnId)` and `segment.pages(columnId)` are sound
 *   bounds (types.ts `ColumnStatistics`): segments and pages that cannot match are skipped before
 *   any page is read, and exact int64-family bounds prove the f64 `safe` representation.
 * - **Live rows.** `segment.deletions()` and `liveRows` turn each batch range into the live
 *   selection before any column is decoded.
 * - **Memory segments** (exec/memory-scan.ts, ruling R16.3). Production scans first size a
 *   publication-stable common prefix with readHint, then fill caller-owned native buffers via
 *   the bounded chunk step, charging all output and decoder memory inside admission.
 *   Direct legacy operator fixtures may instead borrow at most one chunk
 *   of a memory segment at a time: `chunk(columnId, k, out)` lends chunk k (one row range, all its
 *   columns) until the scan's next `chunk()` call on that segment or the snapshot's release, and
 *   the scan asks for chunk k + 1 only after the batch built from chunk k was released downstream.
 *   While resident the call returns a borrowed, immutable vector synchronously (zero copy); after
 *   the write path re-points a slow reader to the published group it decodes into `out` and may
 *   return a Promise, which the scan's `next()` passes on.
 * - **Groups.** `root()`, then `chunk(columnId)`: null means the group predates the column, read
 *   as `ColumnDef.missing` (a constant vector). A chunk is read with
 *   `ChunkReads.open(snapshot.pages, chunk, budget)`.
 * - **Late materialization.** Filter columns first: decode their pages for the batch range
 *   (`decodeRange`, or `decodeCodes` when `isDictionaryRange`), evaluate the filter, then
 *   `pagesForRows` + `load` + `gather` / `gatherCodes` only the survivors of the other columns.
 *   Dictionary pages stay codes, as a `DictionaryVector` over `ChunkPages.dictionary`, whose bytes
 *   are charged to the query that decoded it (R5), never to the cache.
 *
 * ## Memory and I/O
 * - Every target buffer is the scan's, from its grant. Page bytes leased from the space are
 *   charged to the `budget` passed to `open` while the reader holds them, and dropped with
 *   `release` when the scan moves past them; the space's cache is evicted by the governor first.
 * - `load` names every page a batch range needs at once, ascending, so the space can coalesce
 *   neighbours into large reads.
 *
 * ## Synchronous when cached (ruling R6)
 * Every call returns a value, not a Promise, when what it needs is resident: `open` when the
 * chunk's directory is cached (`SpaceReader.peek`), `load` when every named page is. A point read
 * whose index blocks and pages are cached thus runs with no await (exec/executor.ts "Running").
 *
 * ## Assumed from the other tracks (to reconcile when they land)
 * - B2: `ChunkReader` satisfies `ChunkPages`, opened over access/space.ts `SpaceReader` (lease +
 *   peek) instead of `PageSource`, with `load` and `loadDictionary` returning `MaybePromise`
 *   (today they are `async`).
 * - B3: access/ as of rebuild/write 1562cd9, with ruling R16.3's borrow rule read as "one row
 *   chunk across the declared columns": the vectors of chunk k of several columns stay valid
 *   together until a call for another chunk index.
 */
import type { TaskWork } from "../task.js";
import type { MaybePromise } from "../access/key-index.js";
import type { GroupSegment } from "../access/snapshot.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import type { ColumnPredicate, CompiledPredicate } from "../page/predicate.js";
import type { FlatVector, Validity } from "../vector/vector.js";
import { ChunkReader } from "../page/reader.js";

/** A column chunk as a group segment names it (format/manifest.ts `ColumnChunk`). */
export type ColumnChunkRef = NonNullable<ReturnType<GroupSegment["chunk"]>>;

/** Row positions inside a group, ascending. */
export type GroupRows = Uint16Array | Uint32Array | Int32Array;

/** What a scan or fetch calls on one column chunk (B2's `ChunkReader`). */
export interface ChunkPages {
  readonly rowCount: number;
  /** Page indices covering rows `[from, to)`, as `[first, end)`. */
  pagesInRange(from: number, to: number): [number, number];
  /** The distinct pages holding `rows[start … end)`. */
  pagesForRows(rows: GroupRows, start?: number, end?: number): number[];
  isLoaded(page: number): boolean;
  /** Reads, verifies and parses the named pages not loaded yet; sync when all are cached. */
  load(pages: readonly number[]): MaybePromise<void>;
  /** Drops loaded pages (all, or the named ones). */
  release(pages?: readonly number[]): void;
  /** The chunk's dictionary once loaded (immutable for the pin), else null. */
  readonly dictionary: FlatVector | null;
  loadDictionary(): MaybePromise<FlatVector | null>;
  /** Whether every page over `[from, to)` is dictionary-encoded. */
  isDictionaryRange(from: number, to: number): boolean;
  /** Whether int64-family values over pages `[first, end)` fit the f64 `safe` representation. */
  isSafe(first?: number, end?: number): boolean;
  /** Optional proof from verified, loaded page masks; false/absent keeps an output validity mask. */
  isAllValid?(from: number, to: number): boolean;
  /** Optional shared, verified header facts (page/reader.ts DecodeFact); invalidated on load/release. */
  decodeFacts?(from: number, to: number): number;
  /**
   * Out-of-line values (page/large-value.ts): a decode reaching such a row needs it loaded first,
   * for a range or for the selected rows; values stay leased until `releaseValues` (all, or the
   * named rows), `release` of their page or `close`.
   */
  loadValues(from: number, to: number): MaybePromise<void>;
  loadValuesOf(rows: GroupRows, start?: number, end?: number): MaybePromise<void>;
  releaseValues(rows?: readonly number[]): void;
  /**
   * Bytes `loadValues` / `loadValuesOf` charge for the out-of-line values of a range or of the
   * selected rows (pages loaded), so a scan can reserve them before the load.
   */
  largeValueBytes(from: number, to: number): number;
  largeValueBytesOf(rows: GroupRows, start?: number, end?: number): number;
  /** What a decimal target needs: the chunk's one scale (null: per-slot scales), … */
  readonly directory: { readonly scale: number | null };
  /** … 128-bit coefficients, … */
  readonly needsInt128: boolean;
  /** … and NaN / ±Infinity flags. */
  readonly hasDecimalSpecials: boolean;
  /** Releases everything the reader holds (pages, values, the parsed directory). */
  close(): void;
  /**
   * Read-side pushdown (page/predicate.ts, page/select.ts; next/README "Predicates on encoded
   * pages"): a one-column predicate compiled for this chunk (null when not pushed down for its
   * type), the pages over a range it cannot rule out, and its qualifying positions, refinement or
   * 0/1 mask over loaded pages, without materializing values.
   */
  compilePredicate(predicate: ColumnPredicate): CompiledPredicate | null;
  pagesMatching(compiled: CompiledPredicate, from?: number, to?: number): number[];
  select(
    compiled: CompiledPredicate,
    from: number,
    to: number,
    out: Uint16Array,
    at?: number,
  ): number;
  refine(
    compiled: CompiledPredicate,
    rows: GroupRows,
    start: number,
    end: number,
    out: Uint16Array,
    at?: number,
  ): number;
  selectMask(
    compiled: CompiledPredicate,
    from: number,
    to: number,
    mask: Uint8Array,
    at?: number,
  ): number;
  /** Varlen bytes a target needs for rows `[from, to)`, or for the selected rows (full value
   * lengths, out-of-line values included, so a batch can be sized before anything is loaded). */
  varlenBytes(from: number, to: number): number;
  gatherBytes(rows: GroupRows, start?: number, end?: number): number;
  /** Decoding is synchronous over loaded pages, into the caller's target from slot `at`. */
  decodeRange(from: number, to: number, target: FlatVector, at?: number): void;
  gather(rows: GroupRows, target: FlatVector, at?: number, start?: number, end?: number): void;
  decodeCodes(
    from: number,
    to: number,
    codes: Int32Array,
    validity: Validity | null,
    at?: number,
  ): void;
  gatherCodes(
    rows: GroupRows,
    codes: Int32Array,
    validity: Validity | null,
    at?: number,
    start?: number,
    end?: number,
  ): void;
}

/** Opens B2's chunk readers over a pin's page reads (the owner supplies one per database). */
export interface ChunkReads {
  /** Verifies the chunk's directory on first read; sync when it is cached. */
  open(
    pages: SpaceReader,
    chunk: ColumnChunkRef,
    options: {
      readonly budget: ByteBudget;
      readonly signal?: AbortSignal;
      readonly once?: boolean;
      readonly task?: TaskWork;
    },
  ): MaybePromise<ChunkPages>;
  /** Coalesce a full scan's row window across columns; leases still belong to each reader. */
  loadTogether?(
    loads: ReadonlyArray<{ readonly reader: ChunkPages; readonly pages: readonly number[] }>,
  ): MaybePromise<void>;
}

/** Ordinary B2 readers, including shared one-pass slabs for full-scan row windows. */
export const chunkReads: ChunkReads = {
  open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options),
  loadTogether(loads) {
    return ChunkReader.loadTogether(
      loads.map(({ reader, pages }) => {
        if (!(reader instanceof ChunkReader)) throw new TypeError("Expected a B2 chunk reader");
        return { reader, pages };
      }),
    );
  },
};
