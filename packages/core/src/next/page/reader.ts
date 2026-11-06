/**
 * Reads one column chunk: its directory, then only the pages a row range or a selection needs.
 * This is the executor's `ChunkPages` (exec/scan.ts, ruling R14.4), opened over the extent
 * space's reader (storage/extents.ts `ExtentReader`: `read` with a lease, `peek` for hits).
 *
 * - **Verify once.** The space checks every blob's xxHash32 when its bytes come from storage and
 *   caches verified encoded blobs; the reader does not hash again. It parses each page (the
 *   structural checks of decode.ts) when it loads it; a loaded page decodes with no checks.
 * - **Synchronous when cached (rulings R6, R14.4).** `open`, `load` and `loadDictionary` return
 *   a value, not a Promise, when every blob they need is resident: `peek` for stored pages, the
 *   inflated-page cache (`options.inflated`, R15.5) for gzip pages.
 * - **One load per page.** Concurrent `load`s of the same page (or of the dictionary) share one
 *   read: a page is in flight at most once, so no lease or charge is taken twice.
 * - **Memory.** Every byte the reader holds is charged to `options.budget`: leased bytes by the
 *   space, peeked and inflated bytes by the reader (inflated pages before they are allocated),
 *   and the parsed directory at `open`. All of it is returned by `release` (pages) and `close`.
 *   A refused charge of the reader's own is `PageBudgetError(bytes)`; the space's is
 *   `ReadDeniedError`. Either way nothing is held.
 * - **close.** `close` releases everything at once. A load still in flight then completes by
 *   releasing what it read and rejecting with `ReaderClosedError`; it installs nothing.
 * - **Only what is needed.** `load` reads just the named pages, ascending, in calls of at most
 *   `MAX_READ_BYTES`, plus the dictionary page when one of them is dictionary-encoded.
 * - **Late materialization.** `gather` decodes only the selected rows: plain and dictionary pages
 *   index directly, FOR pages too, delta pages walk at most one 128-row block per run of
 *   selected rows. `gatherCodes` does the same for dictionary codes.
 *
 * - **Out-of-line values (R15.4, large-value.ts).** A page slot may hold a pointer to a value of
 *   up to 64 MiB stored as ≤ 1 MiB parts in the chunk's data. `load` reads pages only; a decode
 *   that reaches such a row needs its value loaded first (`loadValues` for a range,
 *   `loadValuesOf` for a selection; `ValueNotLoadedError` otherwise). Loading charges the parts
 *   to the budget before it reads them (the space's lease), in reads of at most `MAX_READ_BYTES`,
 *   and keeps them leased (no reassembly copy) until `releaseValues`, `release` of their page or
 *   `close`; values read together share one lease, charged until the last of them is released.
 *   A release while a load is in flight wins: the load gives what it read back. A decode copies
 *   the parts straight into the target. A consumer that can stream (`valueParts`) holds one
 *   verified part at a time; `close` ends every stream, and a stream that fails is wholly
 *   invalid. Pointers tile their page's large-value region (directory.ts, checked when the page
 *   is parsed), so a value can alias neither another value nor a page.
 *
 * Decoding is synchronous over loaded pages; every target buffer is the caller's.
 */
import type { ChunkData, ColumnChunk, PageEntry } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import {
  MAX_BLOB_BYTES,
  MAX_READ_BYTES,
  type BlobRef,
  type ByteBudget,
  type ExtentReader,
  type ReadLease,
} from "../storage/extents.js";
import type { FlatVector, Validity } from "../vector/vector.js";
import { provenSafe } from "../vector/int64.js";
import {
  decodeCodes,
  decodeRange,
  dictionaryVector,
  gatherCodes,
  gatherRows,
  isLarge,
  largeLength,
  largePointer,
  parseDictionary,
  parsePage,
  varlenBytesOf,
  type LargeResolver,
  type PageDictionary,
  type PageView,
} from "./decode.js";
import { partsOf } from "./large-value.js";
import {
  compilePredicate,
  pageMayMatch,
  type ColumnPredicate,
  type CompiledPredicate,
} from "./predicate.js";
import {
  dictionaryMatches,
  newScratch,
  scratchBytes,
  selectPage,
  type SelectScratch,
} from "./select.js";
import {
  checkDirectoryWithin,
  pageIndexOf,
  parseDirectory,
  type ChunkDirectory,
} from "./directory.js";
import { check, chunkTagOf, PageCorruptionError, ValueTag } from "./format.js";
import { inflatedLength, inflateInto } from "./gzip.js";
import type { InflatedPageCache } from "./inflated-cache.js";
import type { BlobCache } from "../format/blob-cache.js";

export { MAX_BLOB_BYTES, MAX_READ_BYTES };

/**
 * Selection-aware decode: below this fraction of a span's rows, `gather` of the selected rows
 * beats `decodeRange` of the span; at or above it, decode the span densely and keep the
 * selection over it. Measured by scripts/pushdown-bench.mts on the W1M table's 30 projected
 * columns (gather / dense: 0.31 at 10 %, 0.74 at 30 %, 1.1–1.25 at 50–60 %, 2.2 at 70 % and up).
 */
export const DENSE_SELECTION = 0.4;

/** Whether `selected` rows of a `span`-row range should be decoded densely (`DENSE_SELECTION`). */
export const prefersDense = (selected: number, span: number): boolean =>
  selected >= span * DENSE_SELECTION;

type Rows = Uint16Array | Uint32Array | Int32Array;
export type MaybePromise<T> = T | Promise<T>;

export interface ChunkReadOptions {
  /** Charged for every byte the reader holds (ruling R5). */
  readonly budget: ByteBudget;
  readonly signal?: AbortSignal;
  /** Where gzip pages stay inflated across loads and readers (R15.5); without it they inflate per load. */
  readonly inflated?: InflatedPageCache;
  /**
   * Parsed chunk directories shared across readers (R15.7): a hit opens with no parse and no
   * read. Without it each `open` parses the directory.
   */
  readonly directories?: BlobCache<ChunkDirectory>;
}

/** The reader could not charge bytes it must hold (a peeked, inflated or parsed structure). */
export class PageBudgetError extends Error {
  readonly code = "MINNOW_PAGE_BUDGET";
  constructor(readonly bytes: number) {
    super(`The chunk reader needs ${String(bytes)} bytes the budget does not have`);
    this.name = "PageBudgetError";
  }
}

/** A load that was still in flight when its reader was closed. */
export class ReaderClosedError extends Error {
  readonly code = "MINNOW_READER_CLOSED";
  constructor() {
    super("The chunk reader was closed");
    this.name = "ReaderClosedError";
  }
}

/** Bytes charged for a parsed directory: one entry object (with its statistics) per page. */
export function directoryCharge(directory: ChunkDirectory): number {
  return 256 + (directory.pages.length + (directory.dictionary === null ? 0 : 1)) * 192;
}

/** The smallest stored page entry (directory.ts): bounds a corrupt header's page count. */
const MIN_ENTRY_BYTES = 26;

/**
 * `directoryCharge` read from the blob's header, before parsing: the page count (bounded by what
 * the blob could hold) and the dictionary flag. Equal to `directoryCharge` of a valid directory.
 */
export function directoryChargeOf(bytes: Uint8Array): number {
  if (bytes.length < 24) return 256;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const pages = Math.min(view.getUint32(12, true), Math.floor(bytes.length / MIN_ENTRY_BYTES));
  return 256 + (pages + ((view.getUint8(6) & 1) !== 0 ? 1 : 0)) * 192;
}

/** `value`, or a `RangeError` (an index the caller just validated). */
function at<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined) throw new RangeError("Index outside the list");
  return value;
}
/** `at`, for methods whose `at` parameter shadows it. */
const at_ = at;

const isDictionary = (entry: PageEntry): boolean =>
  (entry.encoding & 0x7f) === PageEncoding.dictionary;
const isGzip = (entry: PageEntry): boolean => (entry.encoding & PageEncoding.gzip) !== 0;

/** What keeps one loaded page's (or the dictionary's) bytes alive, and what to give back. */
interface Held {
  /** Bytes the reader charged to its budget (peeked or inflated). */
  readonly charged: number;
  /** The lease its stored bytes came in, shared with the other pages of that read. */
  readonly lease: SharedLease | null;
  /** The inflated-page cache entry it pins. */
  readonly inflated: BlobRef | null;
}

class SharedLease {
  #holders: number;
  constructor(
    readonly lease: ReadLease,
    holders: number,
  ) {
    this.#holders = holders;
  }
  drop(): void {
    if (--this.#holders === 0) this.lease.release();
  }
}

/** The data-space address of a chunk's data range (its pages are at this + `PageEntry.offset`). */
export function dataAddress(data: ChunkData): number {
  return data.extent.offset + data.offset;
}

/** A page's blob: its offset is relative to the chunk's data range (manifest.ts "Chunks"). */
function blobOf(chunk: ColumnChunk, entry: PageEntry): BlobRef {
  return {
    extent: chunk.data.extent,
    offset: chunk.data.offset + entry.offset,
    length: entry.length,
    checksum: entry.checksum,
  };
}

function sized(bytes: Uint8Array | undefined, length: number): Uint8Array {
  if (bytes?.length !== length)
    throw new PageCorruptionError("A blob read returned the wrong length");
  return bytes;
}

const noop = (): void => undefined;

/** One loaded out-of-line value: its verified parts, and what keeps them alive. */
interface LoadedValue {
  readonly parts: readonly Uint8Array[];
  /** Leases its parts came in (shared with other values of a read), or none when peeked. */
  readonly leases: readonly SharedLease[];
  /** Bytes the reader charged (peeked parts). */
  readonly charged: number;
}

/** A value to load: its chunk row, its parts as blobs, and the row's release generation. */
interface ValueJob {
  readonly row: number;
  readonly refs: readonly BlobRef[];
  readonly bytes: number;
  readonly generation: number;
}

/** A `valueParts` stream in progress: the part it holds, until it moves on or the reader closes. */
interface Stream {
  live: boolean;
  lease: ReadLease | null;
}

export class ChunkReader {
  readonly chunk: ColumnChunk;
  readonly directory: ChunkDirectory;
  readonly #space: ExtentReader;
  readonly #options: ChunkReadOptions;
  readonly #pages: Array<PageView | undefined>;
  readonly #held: Array<Held | undefined>;
  readonly #pending = new Map<number, Promise<void>>();
  readonly #directoryCharge: number;
  /** The directory cache entry this reader pins, or null. */
  readonly #directoryPin: BlobRef | null;
  #dictionaryPending: Promise<void> | null = null;
  #dictionary: PageDictionary | null = null;
  #dictionaryHeld: Held | null = null;
  #dictionaryVector: FlatVector | null = null;
  /** Loaded out-of-line values by chunk row. */
  readonly #values = new Map<number, LoadedValue>();
  readonly #valuesPending = new Map<number, Promise<void>>();
  /**
   * Bumped when a row's pending load is released (review B2-p2b #3): a load that completes for
   * an older generation gives its holds back instead of installing.
   */
  readonly #generations = new Map<number, number>();
  /** `valueParts` streams not finished: `close` releases what they hold (review B2-p2b #2). */
  readonly #streams = new Set<Stream>();
  /** Each compiled predicate's matching dictionary codes, charged until `close`. */
  readonly #codes = new Map<CompiledPredicate, Uint32Array>();
  #scratch: SelectScratch | null = null;
  #scratchCharge = 0;
  #closed = false;

  private constructor(
    space: ExtentReader,
    chunk: ColumnChunk,
    directory: ChunkDirectory,
    options: ChunkReadOptions,
    directoryBytes: number,
    directoryPin: BlobRef | null,
  ) {
    this.#space = space;
    this.#options = options;
    this.chunk = chunk;
    this.directory = directory;
    this.#directoryCharge = directoryBytes;
    this.#directoryPin = directoryPin;
    this.#pages = new Array<PageView | undefined>(directory.pages.length);
    this.#held = new Array<Held | undefined>(directory.pages.length);
  }

  /** Reads the chunk's directory (no pages): synchronously when it is cached. */
  static open(
    space: ExtentReader,
    chunk: ColumnChunk,
    options: ChunkReadOptions,
  ): MaybePromise<ChunkReader> {
    const ref = chunk.directory;
    const cache = options.directories;
    const hit = cache?.pin(ref) ?? null;
    if (cache !== undefined && hit !== null) {
      // Cached and parsed already: charged to this reader while it holds it, unpinned at close.
      try {
        check(
          hit.value.tag === chunkTagOf(chunk.type),
          "Page directory tag differs from the column type",
        );
        // The entry was parsed for whichever chunk read it first (review B2-p2a #1).
        checkDirectoryWithin(hit.value, chunk.data.length);
      } catch (error) {
        cache.unpin(ref);
        throw error;
      }
      if (!options.budget.allocate(hit.size)) {
        cache.unpin(ref);
        throw new PageBudgetError(hit.size);
      }
      return new ChunkReader(space, chunk, hit.value, options, hit.size, ref);
    }
    const build = (bytes: Uint8Array): ChunkReader => {
      // Charged from the header's page count before the parse builds anything (B2-2 #4).
      const charge = directoryChargeOf(sized(bytes, ref.length));
      if (!options.budget.allocate(charge)) throw new PageBudgetError(charge);
      let directory: ChunkDirectory;
      try {
        directory = parseDirectory(bytes, chunk.data.length);
        check(
          directory.tag === chunkTagOf(chunk.type),
          "Page directory tag differs from the column type",
        );
      } catch (error) {
        options.budget.free(charge);
        throw error;
      }
      if (cache === undefined)
        return new ChunkReader(space, chunk, directory, options, charge, null);
      // Pinned for this reader; another reader's earlier entry wins a race (same bytes, same charge).
      const shared = cache.admit(ref, directory, charge);
      return new ChunkReader(space, chunk, shared, options, charge, ref);
    };
    const cached = space.peek(ref);
    if (cached !== null) return build(cached);
    return space.read([ref], options).then((lease) => {
      try {
        return build(lease.bytes[0] ?? new Uint8Array(0));
      } finally {
        lease.release();
      }
    });
  }

  get rowCount(): number {
    return this.directory.rowCount;
  }

  /** Page indices covering rows `[from, to)`, as `[first, end)`. */
  pagesInRange(from: number, to: number): [number, number] {
    if (to <= from) return [0, 0];
    const pages = this.directory.pages;
    return [pageIndexOf(pages, from), pageIndexOf(pages, to - 1) + 1];
  }

  /** The distinct pages holding `rows[start … end)` (group positions, ascending). */
  pagesForRows(rows: Rows, start = 0, end = rows.length): number[] {
    const pages = this.directory.pages;
    const out: number[] = [];
    let page = -1;
    let pageEnd = 0;
    for (let k = start; k < end; k++) {
      const row = rows[k] ?? 0;
      if (page >= 0 && row < pageEnd) continue;
      page = pageIndexOf(pages, row);
      const entry = pages[page];
      pageEnd = entry === undefined ? 0 : entry.firstRow + entry.rowCount;
      if (out[out.length - 1] !== page) out.push(page);
    }
    return out;
  }

  isLoaded(page: number): boolean {
    return this.#pages[page] !== undefined;
  }

  /**
   * Loads (parses) the named pages that are not loaded yet, plus the dictionary when one of them
   * needs it. Synchronous when all are cached; a page already in flight is awaited, not re-read.
   */
  load(pageIndices: readonly number[]): MaybePromise<void> {
    if (this.#closed) throw new ReaderClosedError();
    const pages = this.directory.pages;
    const waits = new Set<Promise<void>>();
    const missing: number[] = [];
    let needsDictionary = false;
    for (const index of [...new Set(pageIndices)].sort((a, b) => a - b)) {
      const entry = pages[index];
      if (entry === undefined) throw new RangeError("Page index outside the chunk");
      if (isDictionary(entry)) needsDictionary = true;
      if (this.#pages[index] !== undefined) continue;
      const pending = this.#pending.get(index);
      if (pending !== undefined) waits.add(pending);
      else missing.push(index);
    }
    let wantsDictionary = false;
    if (needsDictionary && this.#dictionary === null) {
      if (this.#dictionaryPending !== null) waits.add(this.#dictionaryPending);
      else wantsDictionary = true;
    }
    if (missing.length === 0 && !wantsDictionary)
      return waits.size === 0 ? undefined : Promise.all(waits).then(noop);
    if (waits.size === 0 && this.#loadCached(missing, wantsDictionary)) return;
    waits.add(this.#start(missing, wantsDictionary));
    return waits.size === 1 ? [...waits][0] : Promise.all(waits).then(noop);
  }

  /** Starts one read for `missing` (and the dictionary) and registers it as their pending load. */
  #start(missing: readonly number[], wantsDictionary: boolean): Promise<void> {
    const run = this.#loadRead(missing, wantsDictionary);
    for (const index of missing) this.#pending.set(index, run);
    if (wantsDictionary) this.#dictionaryPending = run;
    const settle = (): void => {
      for (const index of missing)
        if (this.#pending.get(index) === run) this.#pending.delete(index);
      if (this.#dictionaryPending === run) this.#dictionaryPending = null;
    };
    run.then(settle, settle);
    return run;
  }

  /** The sync path: every blob resident (peek, or the inflated cache). All or nothing. */
  #loadCached(missing: readonly number[], wantsDictionary: boolean): boolean {
    const pages = this.directory.pages;
    const jobs: Array<{ entry: PageEntry; page: number | null }> = missing.map((page) => ({
      entry: at(pages, page),
      page,
    }));
    const dictionaryEntry = wantsDictionary ? this.directory.dictionary : null;
    if (dictionaryEntry !== null) jobs.push({ entry: dictionaryEntry, page: null });
    const inflated = this.#options.inflated;
    const found: Array<{ bytes: Uint8Array; pinned: BlobRef | null }> = [];
    const unpin = (): void => {
      for (const { pinned } of found) if (pinned !== null) inflated?.unpin(pinned);
    };
    for (const { entry } of jobs) {
      const ref = blobOf(this.chunk, entry);
      if (isGzip(entry)) {
        const bytes = inflated?.pin(ref) ?? null;
        if (bytes === null) {
          unpin();
          return false;
        }
        found.push({ bytes, pinned: ref });
      } else {
        const bytes = this.#space.peek(ref);
        if (bytes === null) {
          unpin();
          return false;
        }
        found.push({ bytes: sized(bytes, entry.length), pinned: null });
      }
    }
    const total = found.reduce((sum, { bytes }) => sum + bytes.length, 0);
    // Peeked and cached-inflated bytes are uncharged by their caches: the reader charges them.
    if (!this.#options.budget.allocate(total)) {
      unpin();
      return false;
    }
    let installed = 0;
    try {
      for (const [k, { entry, page }] of jobs.entries()) {
        const { bytes, pinned } = at(found, k);
        this.#install(entry, page, bytes, { charged: bytes.length, lease: null, inflated: pinned });
        installed++;
      }
    } catch (error) {
      // `#install` gave back the hold that failed (one owner per hold, B2-2 #3); the rest are ours.
      for (let k = installed + 1; k < found.length; k++) {
        const { bytes, pinned } = at(found, k);
        this.#options.budget.free(bytes.length);
        if (pinned !== null) inflated?.unpin(pinned);
      }
      throw error;
    }
    return true;
  }

  async #loadRead(missing: readonly number[], wantsDictionary: boolean): Promise<void> {
    const pages = this.directory.pages;
    const jobs: Array<{ entry: PageEntry; page: number | null }> = [];
    const all: Array<{ entry: PageEntry; page: number | null }> = missing.map((page) => ({
      entry: at(pages, page),
      page,
    }));
    const dictionaryEntry = wantsDictionary ? this.directory.dictionary : null;
    if (dictionaryEntry !== null) all.push({ entry: dictionaryEntry, page: null });
    const inflated = this.#options.inflated;
    // gzip pages already inflated in the cache need no read.
    for (const job of all) {
      const ref = blobOf(this.chunk, job.entry);
      const bytes = isGzip(job.entry) ? (inflated?.pin(ref) ?? null) : null;
      if (bytes === null) {
        jobs.push(job);
        continue;
      }
      if (!this.#options.budget.allocate(bytes.length)) {
        inflated?.unpin(ref);
        throw new PageBudgetError(bytes.length);
      }
      this.#install(job.entry, job.page, bytes, {
        charged: bytes.length,
        lease: null,
        inflated: ref,
      });
    }
    let start = 0;
    while (start < jobs.length) {
      let end = start;
      let bytes = 0;
      while (end < jobs.length && bytes + at(jobs, end).entry.length <= MAX_READ_BYTES)
        bytes += at(jobs, end++).entry.length;
      if (end === start) end++;
      const batch = jobs.slice(start, end);
      if (this.#closed) throw new ReaderClosedError();
      const lease = await this.#space.read(
        batch.map(({ entry }) => blobOf(this.chunk, entry)),
        this.#options,
      );
      const shared = new SharedLease(lease, batch.length);
      // `#settle` takes over one hold on the lease per job, success or failure.
      for (let k = 0; k < batch.length; k++) {
        const { entry, page } = at(batch, k);
        try {
          await this.#settle(entry, page, lease.bytes[k], shared);
        } catch (error) {
          for (let rest = k + 1; rest < batch.length; rest++) shared.drop();
          throw error;
        }
      }
      start = end;
    }
  }

  /** Installs one page read through a lease (inflating a gzip page into charged memory first). */
  async #settle(
    entry: PageEntry,
    page: number | null,
    stored: Uint8Array | undefined,
    lease: SharedLease,
  ): Promise<void> {
    // A reader closed meanwhile is refused by `#install`, which gives the lease and charge back.
    if (!isGzip(entry)) {
      let bytes: Uint8Array;
      try {
        bytes = sized(stored, entry.length);
      } catch (error) {
        lease.drop();
        throw error;
      }
      this.#install(entry, page, bytes, { charged: 0, lease, inflated: null });
      return;
    }
    // gzip: charge the inflated size before allocating it, inflate, let the stored bytes go.
    let length: number;
    try {
      length = inflatedLength(sized(stored, entry.length));
    } catch (error) {
      lease.drop();
      throw error;
    }
    if (!this.#options.budget.allocate(length)) {
      lease.drop();
      throw new PageBudgetError(length);
    }
    let raw: Uint8Array = new Uint8Array(length);
    try {
      await inflateInto(sized(stored, entry.length), raw);
    } catch (error) {
      this.#options.budget.free(length);
      throw error;
    } finally {
      lease.drop();
    }
    // If the reader was closed while this page inflated, `#install` gives the charge back.
    const cache = this.#options.inflated;
    const ref = blobOf(this.chunk, entry);
    if (cache !== undefined) raw = cache.admit(ref, raw);
    this.#install(entry, page, raw, {
      charged: length,
      lease: null,
      inflated: cache === undefined ? null : ref,
    });
  }

  /** Parses and installs one page or the dictionary; on failure, gives `held` back. */
  #install(entry: PageEntry, page: number | null, bytes: Uint8Array, held: Held): void {
    try {
      if (this.#closed) throw new ReaderClosedError();
      if (page === null) {
        const dictionary = parseDictionary(bytes, entry, this.directory.tag);
        this.#dictionary = dictionary;
        this.#dictionaryHeld = held;
        this.#dictionaryVector = dictionaryVector(dictionary, this.chunk.type);
      } else {
        // Validated against the chunk, its dictionary's domain included: every consumer
        // (select, refine, gather, decode) relies on this one check (review B2-p2d #1).
        this.#pages[page] = parsePage(
          bytes,
          entry,
          this.directory.tag,
          this.directory.scale,
          this.directory.dictionary?.rowCount ?? 0,
        );
        this.#held[page] = held;
      }
    } catch (error) {
      this.#drop(held);
      throw error;
    }
  }

  #drop(held: Held | undefined | null): void {
    if (held === undefined || held === null) return;
    if (held.charged > 0) this.#options.budget.free(held.charged);
    held.lease?.drop();
    if (held.inflated !== null) this.#options.inflated?.unpin(held.inflated);
  }

  /** Loads the dictionary page on its own (for a dictionary vector before any page). */
  loadDictionary(): MaybePromise<FlatVector | null> {
    if (this.#closed) throw new ReaderClosedError();
    if (this.directory.dictionary === null) return null;
    if (this.#dictionaryVector !== null) return this.#dictionaryVector;
    const pending = this.#dictionaryPending;
    if (pending !== null) return pending.then(() => this.#dictionaryVector);
    if (this.#loadCached([], true)) return this.#dictionaryVector;
    return this.#start([], true).then(() => this.#dictionaryVector);
  }

  /** The chunk's dictionary as an immutable flat vector, once loaded. */
  get dictionary(): FlatVector | null {
    return this.#dictionaryVector;
  }

  /** Drops loaded pages (all, or the named ones) and their bytes. The dictionary stays. */
  release(pageIndices?: readonly number[]): void {
    if (pageIndices === undefined) this.releaseValues();
    const indices = pageIndices ?? this.#pages.map((_, index) => index);
    for (const index of indices) {
      if (this.#pages[index] === undefined) continue;
      this.#pages[index] = undefined;
      this.#drop(this.#held[index]);
      this.#held[index] = undefined;
      const entry = this.directory.pages[index];
      if (entry === undefined) continue;
      const inPage = (row: number): boolean =>
        row >= entry.firstRow && row < entry.firstRow + entry.rowCount;
      for (const [row, value] of this.#values)
        if (inPage(row)) {
          this.#values.delete(row);
          this.#dropValue(value);
        }
      for (const row of [...this.#valuesPending.keys()]) if (inPage(row)) this.#invalidate(row);
    }
  }

  /**
   * Releases every page, the dictionary and the directory's charge. Final: later calls throw
   * `ReaderClosedError`, and loads in flight reject with it after giving back what they read.
   */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const stream of this.#streams) {
      stream.live = false;
      const lease = stream.lease;
      stream.lease = null;
      lease?.release();
    }
    this.#streams.clear();
    for (const codes of this.#codes.values()) this.#options.budget.free(codes.byteLength);
    this.#codes.clear();
    this.#options.budget.free(this.#scratchCharge);
    this.#scratchCharge = 0;
    this.#scratch = null;
    this.release();
    this.#drop(this.#dictionaryHeld);
    this.#dictionaryHeld = null;
    this.#dictionary = null;
    this.#dictionaryVector = null;
    this.#options.budget.free(this.#directoryCharge);
    if (this.#directoryPin !== null) this.#options.directories?.unpin(this.#directoryPin);
  }

  // ── Predicates on encoded pages (predicate.ts, select.ts) ─────────────────────────────

  /**
   * `predicate` compiled for this chunk, or null when it is not pushed down for its type (the
   * caller evaluates it on decoded values). Compile once per chunk and reuse it for every batch.
   */
  compilePredicate(predicate: ColumnPredicate): CompiledPredicate | null {
    return compilePredicate(this.chunk.type, this.directory.scale, predicate);
  }

  /**
   * The pages over `[from, to)` that may hold a qualifying row (the others are skipped by their
   * statistics, without being read): load these before `select`.
   */
  pagesMatching(compiled: CompiledPredicate, from = 0, to = this.rowCount): number[] {
    const [first, end] = this.pagesInRange(from, to);
    const out: number[] = [];
    for (let page = first; page < end; page++)
      if (pageMayMatch(compiled, at(this.directory.pages, page))) out.push(page);
    return out;
  }

  /**
   * Writes the group positions of the rows of `[from, to)` that satisfy `compiled` to
   * `out[at …]`, ascending, and returns how many. Pages its statistics rule out are skipped
   * unread; every other page in the range must be loaded (and the dictionary, which `load`
   * brings with a dictionary page). `out` needs room for `to − from` positions.
   */
  select(compiled: CompiledPredicate, from: number, to: number, out: Uint16Array, at = 0): number {
    this.#live();
    if (out.length - at < to - from) throw new RangeError("The selection buffer is too small");
    const [first, end] = this.pagesInRange(from, to);
    let count = 0;
    for (let page = first; page < end; page++) {
      const entry = at_(this.directory.pages, page);
      if (!pageMayMatch(compiled, entry)) continue;
      const start = Math.max(from, entry.firstRow) - entry.firstRow;
      const stop = Math.min(to, entry.firstRow + entry.rowCount) - entry.firstRow;
      count += selectPage(this.#context(compiled, page), null, start, stop, out, at + count);
    }
    return count;
  }

  /**
   * Keeps the rows of the selection `rows[start … end)` (group positions, ascending) that also
   * satisfy `compiled`, writing them to `out[at …]` (which may be `rows` itself when `at ≤
   * start`); returns how many.
   */
  refine(
    compiled: CompiledPredicate,
    rows: Rows,
    start: number,
    end: number,
    out: Uint16Array,
    at = 0,
  ): number {
    this.#live();
    if (out.length - at < end - start) throw new RangeError("The selection buffer is too small");
    const pages = this.directory.pages;
    let count = 0;
    let k = start;
    while (k < end) {
      const page = pageIndexOf(pages, rows[k] ?? 0);
      const entry = at_(pages, page);
      const limit = entry.firstRow + entry.rowCount;
      let stop = k + 1;
      while (stop < end && (rows[stop] ?? 0) < limit) stop++;
      if (pageMayMatch(compiled, entry))
        count += selectPage(this.#context(compiled, page), rows, k, stop, out, at + count);
      k = stop;
    }
    return count;
  }

  #context(compiled: CompiledPredicate, page: number) {
    const view = this.#view(page);
    const entry = at_(this.directory.pages, page);
    let codes: Uint32Array | null = null;
    if (view.kind === PageEncoding.dictionary && compiled.nulls === null) {
      codes = this.#codes.get(compiled) ?? null;
      if (codes === null) {
        const dictionary = this.#dictionary;
        if (dictionary === null)
          throw new RangeError("A dictionary page needs its dictionary loaded");
        const bytes = ((dictionary.size + 31) >>> 5) * 4;
        if (!this.#options.budget.allocate(bytes)) throw new PageBudgetError(bytes);
        codes = dictionaryMatches(dictionary, compiled);
        this.#codes.set(compiled, codes);
      }
    }
    return {
      view,
      entry,
      compiled,
      codes,
      dictionary: this.#dictionary,
      scratch: this.#selectScratch(),
      values: this.#resolver(view, entry),
      type: this.chunk.type,
    };
  }

  /** Decode scratch for pages compared after decoding: one page's rows, charged until close. */
  #selectScratch(): SelectScratch {
    if (this.#scratch !== null) return this.#scratch;
    let rows = 1;
    for (const page of this.directory.pages) rows = Math.max(rows, page.rowCount);
    rows = Math.min(rows, 4096); // decoded in chunks of this many rows
    const bytes = scratchBytes(rows);
    if (!this.#options.budget.allocate(bytes)) throw new PageBudgetError(bytes);
    this.#scratchCharge = bytes;
    this.#scratch = newScratch(rows);
    return this.#scratch;
  }

  // ── Out-of-line values ──────────────────────────────────────────────────────────────────

  /** The loaded values of a page's out-of-line rows, for its decoder (null: the page has none). */
  #resolver(view: PageView, entry: PageEntry): LargeResolver | null {
    if (view.large === null) return null;
    const first = entry.firstRow;
    return (row) => this.#values.get(first + row)?.parts ?? null;
  }

  /** The out-of-line rows (chunk positions, ascending) over `[from, to)`; pages must be loaded. */
  largeRows(from: number, to: number): number[] {
    const out: number[] = [];
    const [first, end] = this.pagesInRange(from, to);
    for (let page = first; page < end; page++) {
      const view = this.#view(page);
      if (view.large === null) continue;
      const entry = at(this.directory.pages, page);
      const start = Math.max(from, entry.firstRow);
      const stop = Math.min(to, entry.firstRow + entry.rowCount);
      for (let row = start; row < stop; row++)
        if (isLarge(view, row - entry.firstRow)) out.push(row);
    }
    return out;
  }

  /** The out-of-line rows among the selected `rows[start … end)`; pages must be loaded. */
  largeRowsOf(rows: Rows, start = 0, end = rows.length): number[] {
    const out: number[] = [];
    this.#forPages(rows, start, end, (view, entry, k0, k1) => {
      if (view.large === null) return;
      for (let k = k0; k < k1; k++) {
        const row = rows[k] ?? 0;
        if (isLarge(view, row - entry.firstRow) && out[out.length - 1] !== row) out.push(row);
      }
    });
    return out;
  }

  /** Bytes of the out-of-line values over `[from, to)` (what `loadValues` charges). */
  largeValueBytes(from: number, to: number): number {
    let total = 0;
    for (const row of this.largeRows(from, to)) total += this.#largeLength(row);
    return total;
  }

  /**
   * Loads the out-of-line values of rows `[from, to)` (their pages must be loaded), so a decode
   * of those rows can copy them. Synchronous when there are none, they are loaded, or every part
   * is resident.
   */
  loadValues(from: number, to: number): MaybePromise<void> {
    if (this.#closed) throw new ReaderClosedError();
    return this.#loadValues(this.largeRows(from, to));
  }

  /** `loadValues` for the selected `rows[start … end)`. */
  loadValuesOf(rows: Rows, start = 0, end = rows.length): MaybePromise<void> {
    if (this.#closed) throw new ReaderClosedError();
    return this.#loadValues(this.largeRowsOf(rows, start, end));
  }

  /**
   * Drops out-of-line values: all, or those of the named chunk rows, loaded or still loading (a
   * load in flight then gives what it reads back; a later `loadValues` reads again).
   */
  releaseValues(rows?: readonly number[]): void {
    const named = rows ?? [...new Set([...this.#values.keys(), ...this.#valuesPending.keys()])];
    for (const row of named) {
      if (this.#valuesPending.has(row)) this.#invalidate(row);
      const value = this.#values.get(row);
      if (value === undefined) continue;
      this.#values.delete(row);
      this.#dropValue(value);
    }
  }

  /** A pending load of `row` no longer installs; the next load starts afresh. */
  #invalidate(row: number): void {
    this.#generations.set(row, this.#generation(row) + 1);
    this.#valuesPending.delete(row);
  }

  #generation(row: number): number {
    return this.#generations.get(row) ?? 0;
  }

  /** Whether row `row`'s out-of-line value is loaded. */
  isValueLoaded(row: number): boolean {
    return this.#values.has(row);
  }

  /**
   * Streams the value of chunk row `row` (its page must be loaded): an inline value as one
   * array, an out-of-line one part by part (≤ 1 MiB each, verified, charged while it is held).
   * Each yielded array is valid until the next step; nothing is yielded for null. Memory: one
   * part, whatever the value's size.
   *
   * **A failed stream is wholly invalid** (review B2-p2b #4). Every part is verified before it
   * is yielded, but a later part may still fail (`StoreCorruptionError`, a refused budget, an
   * abort, `ReaderClosedError`): the consumer must then discard everything the stream gave it,
   * not keep a prefix. `loadValues` (and so `decodeRange` / `gather`) reads and verifies every
   * part before anything is decoded.
   *
   * `close` ends every stream: what a suspended stream holds is released at once, and its next
   * step throws `ReaderClosedError`.
   */
  async *valueParts(row: number): AsyncGenerator<Uint8Array> {
    const stream: Stream = { live: true, lease: null };
    this.#live();
    this.#streams.add(stream);
    try {
      yield* this.#streamParts(row, stream);
    } finally {
      const lease = stream.lease;
      stream.lease = null;
      lease?.release();
      this.#streams.delete(stream);
    }
  }

  async *#streamParts(row: number, stream: Stream): AsyncGenerator<Uint8Array> {
    const live = (): void => {
      if (!stream.live || this.#closed) throw new ReaderClosedError();
    };
    const page = pageIndexOf(this.directory.pages, row);
    const entry = at(this.directory.pages, page);
    const view = this.#view(page);
    const local = row - entry.firstRow;
    if (local < 0 || local >= entry.rowCount) throw new RangeError("Row outside the chunk");
    if (view.validity !== null && (((view.validity[local >>> 5] ?? 0) >>> (local & 31)) & 1) === 0)
      return;
    if (view.allNull) return;
    if (!isLarge(view, local)) {
      const offsets = view.unsigned;
      const data = view.data;
      if (
        view.tag !== ValueTag.varlen ||
        view.kind !== PageEncoding.plain ||
        offsets === null ||
        data === null
      )
        throw new TypeError("valueParts streams plain varlen pages");
      yield data.subarray(offsets[local] ?? 0, offsets[local + 1] ?? 0);
      return;
    }
    const refs = this.#valueRefs(view, local);
    for (const [index, ref] of refs.entries()) {
      live();
      // A loaded value's parts are held by it (and released by `releaseValues` or `close`).
      const loaded = this.#values.get(row);
      if (loaded !== undefined) {
        yield at(loaded.parts, index);
        continue;
      }
      const lease = await this.#space.read([ref], this.#options);
      if (!stream.live || this.#closed) {
        lease.release();
        throw new ReaderClosedError();
      }
      stream.lease = lease;
      const bytes = sized(lease.bytes[0], ref.length);
      yield bytes;
      // Resumed: give the part back unless `close` already did.
      if (stream.lease === lease) {
        stream.lease = null;
        lease.release();
      }
    }
  }

  #live(): void {
    if (this.#closed) throw new ReaderClosedError();
  }

  #largeLength(row: number): number {
    const page = pageIndexOf(this.directory.pages, row);
    return largeLength(this.#view(page), row - at(this.directory.pages, page).firstRow);
  }

  /** The parts of a page row's pointer as blobs, checked against the chunk's data range. */
  #valueRefs(view: PageView, local: number): BlobRef[] {
    const pointer = largePointer(view, local);
    check(
      pointer.offset + pointer.length <= this.chunk.data.length,
      "Large value outside its chunk's data",
    );
    return partsOf(pointer).map((part) => ({
      extent: this.chunk.data.extent,
      offset: this.chunk.data.offset + part.offset,
      length: part.length,
      checksum: part.checksum,
    }));
  }

  #loadValues(rows: readonly number[]): MaybePromise<void> {
    const waits = new Set<Promise<void>>();
    const jobs: ValueJob[] = [];
    for (const row of rows) {
      if (this.#values.has(row)) continue;
      const pending = this.#valuesPending.get(row);
      if (pending !== undefined) {
        waits.add(pending);
        continue;
      }
      const page = pageIndexOf(this.directory.pages, row);
      const entry = at(this.directory.pages, page);
      const refs = this.#valueRefs(this.#view(page), row - entry.firstRow);
      jobs.push({
        row,
        refs,
        bytes: refs.reduce((sum, ref) => sum + ref.length, 0),
        generation: this.#generation(row),
      });
    }
    if (jobs.length === 0) return waits.size === 0 ? undefined : Promise.all(waits).then(noop);
    if (waits.size === 0 && this.#valuesCached(jobs)) return;
    const run = this.#readValues(jobs);
    for (const job of jobs) this.#valuesPending.set(job.row, run);
    const settle = (): void => {
      for (const job of jobs)
        if (this.#valuesPending.get(job.row) === run) this.#valuesPending.delete(job.row);
    };
    run.then(settle, settle);
    waits.add(run);
    return waits.size === 1 ? run : Promise.all(waits).then(noop);
  }

  /** The sync path: every part resident. All or nothing; the reader charges what it peeked. */
  #valuesCached(jobs: readonly ValueJob[]): boolean {
    const found: Uint8Array[][] = [];
    let total = 0;
    for (const job of jobs) {
      const parts: Uint8Array[] = [];
      for (const ref of job.refs) {
        const bytes = this.#space.peek(ref);
        if (bytes === null) return false;
        parts.push(sized(bytes, ref.length));
      }
      found.push(parts);
      total += job.bytes;
    }
    if (!this.#options.budget.allocate(total)) return false;
    jobs.forEach((job, k) => {
      this.#values.set(job.row, { parts: at(found, k), leases: [], charged: job.bytes });
    });
    return true;
  }

  /**
   * Reads values in calls of at most `MAX_READ_BYTES`: whole values grouped while they fit, a
   * larger value in reads of its own. Each value installs when its last part is in.
   */
  async #readValues(jobs: readonly ValueJob[]): Promise<void> {
    let k = 0;
    while (k < jobs.length) {
      // Whole values that fit one read together, or one value split across reads.
      const group: ValueJob[] = [];
      let bytes = 0;
      while (k < jobs.length && bytes + at(jobs, k).bytes <= MAX_READ_BYTES) {
        bytes += at(jobs, k).bytes;
        group.push(at(jobs, k++));
      }
      if (group.length > 0) {
        await this.#readGroup(group);
        continue;
      }
      await this.#readSplit(at(jobs, k++));
    }
  }

  /** Values whose parts fit one read: one lease, shared by them. */
  async #readGroup(group: readonly ValueJob[]): Promise<void> {
    if (this.#closed) throw new ReaderClosedError();
    const lease = await this.#space.read(
      group.flatMap((job) => job.refs),
      this.#options,
    );
    const shared = new SharedLease(lease, group.length);
    let next = 0;
    let installed = 0;
    try {
      for (const job of group) {
        const parts = job.refs.map((ref) => sized(lease.bytes[next++], ref.length));
        // `#installValue` owns this hold from here, on success or failure.
        installed++;
        this.#installValue(job, { parts, leases: [shared], charged: 0 });
      }
    } finally {
      for (let rest = installed; rest < group.length; rest++) shared.drop();
    }
  }

  /** One value over several reads: it holds every lease until it is released. */
  async #readSplit(job: ValueJob): Promise<void> {
    const leases: SharedLease[] = [];
    const parts: Uint8Array[] = [];
    try {
      let start = 0;
      while (start < job.refs.length) {
        let end = start;
        let bytes = 0;
        while (end < job.refs.length && bytes + at(job.refs, end).length <= MAX_READ_BYTES)
          bytes += at(job.refs, end++).length;
        if (end === start) end++;
        if (this.#closed) throw new ReaderClosedError();
        // Released while it loads: stop reading (what was read goes back below).
        if (this.#generation(job.row) !== job.generation) break;
        const refs = job.refs.slice(start, end);
        const lease = await this.#space.read(refs, this.#options);
        leases.push(new SharedLease(lease, 1));
        refs.forEach((ref, index) => parts.push(sized(lease.bytes[index], ref.length)));
        start = end;
      }
    } catch (error) {
      for (const lease of leases) lease.drop();
      throw error;
    }
    this.#installValue(job, { parts, leases, charged: 0 });
  }

  /**
   * Installs a loaded value; gives it back instead (one owner per hold) when the reader closed
   * (and refuses), when the row was released since the load began (review B2-p2b #3), or when
   * another load installed it first.
   */
  #installValue(job: ValueJob, value: LoadedValue): void {
    const stale = this.#generation(job.row) !== job.generation;
    if (this.#closed || stale || this.#values.has(job.row)) {
      this.#dropValue(value);
      if (this.#closed) throw new ReaderClosedError();
      return;
    }
    this.#values.set(job.row, value);
  }

  #dropValue(value: LoadedValue): void {
    if (value.charged > 0) this.#options.budget.free(value.charged);
    for (const lease of value.leases) lease.drop();
  }

  #view(index: number): PageView {
    const view = this.#pages[index];
    if (view === undefined) throw new RangeError(`Page ${String(index)} is not loaded`);
    return view;
  }

  /** Whether every page over `[from, to)` is dictionary-encoded (decode as codes). */
  isDictionaryRange(from: number, to: number): boolean {
    const [first, end] = this.pagesInRange(from, to);
    for (let page = first; page < end; page++) {
      const entry = this.directory.pages[page];
      if (entry === undefined || !isDictionary(entry)) return false;
    }
    return true;
  }

  /**
   * Whether int64-family values over the loaded pages `[first, end)` (default: the whole chunk,
   * by its statistics) fit the f64 `safe` representation.
   */
  isSafe(first?: number, end?: number): boolean {
    if (first === undefined || end === undefined) {
      const { min, max } = this.chunk.statistics;
      if (typeof min === "bigint" && typeof max === "bigint") return provenSafe(min, max);
      if (this.chunk.statistics.nullCount === this.rowCount) return true;
      first = 0;
      end = this.directory.pages.length;
    }
    for (let page = first; page < end; page++) {
      const view = this.#pages[page];
      if (view === undefined) return false;
      if (!view.safe && !view.allNull) return false;
    }
    return true;
  }

  #checkTarget(target: FlatVector): void {
    const data = target.data;
    // Each page checks its own scales against the target (decode.ts `decodeScales`).
    if (data.layout === "decimal" && data.scale === null && data.scales === null)
      throw new RangeError("A decimal target needs a scale or per-slot scales");
  }

  /** Bytes a varlen target needs for rows `[from, to)` (pages must be loaded). */
  varlenBytes(from: number, to: number): number {
    const [first, end] = this.pagesInRange(from, to);
    let total = 0;
    for (let page = first; page < end; page++) {
      const view = this.#view(page);
      const entry = this.directory.pages[page];
      if (entry === undefined) continue;
      const start = Math.max(from, entry.firstRow) - entry.firstRow;
      const stop = Math.min(to, entry.firstRow + entry.rowCount) - entry.firstRow;
      total += varlenBytesOf(view, this.#dictionary, null, start, stop);
    }
    return total;
  }

  /** Bytes a varlen target needs for the selected rows (pages must be loaded). */
  gatherBytes(rows: Rows, start = 0, end = rows.length): number {
    let total = 0;
    this.#forPages(rows, start, end, (view, entry, k0, k1) => {
      total += varlenBytesOf(view, this.#dictionary, rows, k0, k1, entry.firstRow);
    });
    return total;
  }

  /** Decodes rows `[from, to)` into `target` slots from `at` (loaded pages only). */
  decodeRange(from: number, to: number, target: FlatVector, at = 0): void {
    this.#checkTarget(target);
    const [first, end] = this.pagesInRange(from, to);
    let out = at;
    for (let page = first; page < end; page++) {
      const entry = this.directory.pages[page];
      if (entry === undefined) continue;
      const start = Math.max(from, entry.firstRow) - entry.firstRow;
      const stop = Math.min(to, entry.firstRow + entry.rowCount) - entry.firstRow;
      const view = this.#view(page);
      decodeRange(view, start, stop, target, out, this.#dictionary, this.#resolver(view, entry));
      out += stop - start;
    }
  }

  #forPages(
    rows: Rows,
    start: number,
    end: number,
    each: (view: PageView, entry: PageEntry, from: number, to: number) => void,
  ): void {
    const pages = this.directory.pages;
    let k = start;
    while (k < end) {
      const page = pageIndexOf(pages, rows[k] ?? 0);
      const entry = pages[page];
      if (entry === undefined) throw new RangeError("Selected row outside the chunk");
      const limit = entry.firstRow + entry.rowCount;
      let stop = k + 1;
      while (stop < end && (rows[stop] ?? 0) < limit) stop++;
      each(this.#view(page), entry, k, stop);
      k = stop;
    }
  }

  /** Decodes the selected rows `rows[start … end)` (group positions, ascending) into `target`. */
  gather(rows: Rows, target: FlatVector, at = 0, start = 0, end = rows.length): void {
    this.#checkTarget(target);
    this.#forPages(rows, start, end, (view, entry, k0, k1) => {
      gatherRows(
        view,
        rows,
        k0,
        k1,
        entry.firstRow,
        target,
        at + k0 - start,
        this.#dictionary,
        this.#resolver(view, entry),
      );
    });
  }

  /** Dictionary codes of rows `[from, to)` (every page must be dictionary-encoded). */
  decodeCodes(
    from: number,
    to: number,
    codes: Int32Array,
    validity: Validity | null,
    at = 0,
  ): void {
    const [first, end] = this.pagesInRange(from, to);
    let out = at;
    for (let page = first; page < end; page++) {
      const entry = this.directory.pages[page];
      if (entry === undefined) continue;
      const start = Math.max(from, entry.firstRow) - entry.firstRow;
      const stop = Math.min(to, entry.firstRow + entry.rowCount) - entry.firstRow;
      decodeCodes(this.#view(page), start, stop, codes, validity, out);
      out += stop - start;
    }
  }

  /** Dictionary codes of the selected rows. */
  gatherCodes(
    rows: Rows,
    codes: Int32Array,
    validity: Validity | null,
    at = 0,
    start = 0,
    end = rows.length,
  ): void {
    this.#forPages(rows, start, end, (view, entry, k0, k1) => {
      gatherCodes(view, rows, k0, k1, entry.firstRow, codes, validity, at + k0 - start);
    });
  }

  /** Whether decoding needs a `specials` array in a decimal target. */
  get hasDecimalSpecials(): boolean {
    return this.directory.tag === ValueTag.dec64 && this.directory.hasSpecials;
  }

  /** Whether any page of the chunk stores 128-bit coefficients (decode into the int128 tier). */
  get needsInt128(): boolean {
    if (this.directory.tag !== ValueTag.dec64) return false;
    const { min, max } = this.chunk.statistics;
    if (typeof min === "bigint" && typeof max === "bigint")
      return min < -(2n ** 63n) || max > 2n ** 63n - 1n;
    return true;
  }
}
