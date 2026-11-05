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
 * Decoding is synchronous over loaded pages; every target buffer is the caller's.
 */
import type { ColumnChunk, PageEntry } from "../format/manifest.js";
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
  parseDictionary,
  parsePage,
  varlenBytesOf,
  type PageDictionary,
  type PageView,
} from "./decode.js";
import { pageIndexOf, parseDirectory, type ChunkDirectory } from "./directory.js";
import { check, chunkTagOf, PageCorruptionError, ValueTag } from "./format.js";
import { inflatedLength, inflateInto } from "./gzip.js";
import type { InflatedPageCache } from "./inflated-cache.js";

export { MAX_BLOB_BYTES, MAX_READ_BYTES };

type Rows = Uint16Array | Uint32Array | Int32Array;
export type MaybePromise<T> = T | Promise<T>;

export interface ChunkReadOptions {
  /** Charged for every byte the reader holds (ruling R5). */
  readonly budget: ByteBudget;
  readonly signal?: AbortSignal;
  /** Where gzip pages stay inflated across loads and readers (R15.5); without it they inflate per load. */
  readonly inflated?: InflatedPageCache;
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

/** `value`, or a `RangeError` (an index the caller just validated). */
function at<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined) throw new RangeError("Index outside the list");
  return value;
}

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

function blobOf(chunk: ColumnChunk, entry: PageEntry): BlobRef {
  return {
    extent: chunk.data,
    offset: entry.offset,
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

export class ChunkReader {
  readonly chunk: ColumnChunk;
  readonly directory: ChunkDirectory;
  readonly #space: ExtentReader;
  readonly #options: ChunkReadOptions;
  readonly #pages: Array<PageView | undefined>;
  readonly #held: Array<Held | undefined>;
  readonly #pending = new Map<number, Promise<void>>();
  readonly #directoryCharge: number;
  #dictionaryPending: Promise<void> | null = null;
  #dictionary: PageDictionary | null = null;
  #dictionaryHeld: Held | null = null;
  #dictionaryVector: FlatVector | null = null;
  #closed = false;

  private constructor(
    space: ExtentReader,
    chunk: ColumnChunk,
    directory: ChunkDirectory,
    options: ChunkReadOptions,
    directoryBytes: number,
  ) {
    this.#space = space;
    this.#options = options;
    this.chunk = chunk;
    this.directory = directory;
    this.#directoryCharge = directoryBytes;
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
    const build = (bytes: Uint8Array): ChunkReader => {
      const directory = parseDirectory(sized(bytes, ref.length), chunk.data.length);
      check(
        directory.tag === chunkTagOf(chunk.type),
        "Page directory tag differs from the column type",
      );
      const charge = directoryCharge(directory);
      if (!options.budget.allocate(charge)) throw new PageBudgetError(charge);
      return new ChunkReader(space, chunk, directory, options, charge);
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
      for (let k = installed; k < found.length; k++) {
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
        this.#pages[page] = parsePage(bytes, entry, this.directory.tag, this.directory.scale);
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
    const indices = pageIndices ?? this.#pages.map((_, index) => index);
    for (const index of indices) {
      if (this.#pages[index] === undefined) continue;
      this.#pages[index] = undefined;
      this.#drop(this.#held[index]);
      this.#held[index] = undefined;
    }
  }

  /**
   * Releases every page, the dictionary and the directory's charge. Final: later calls throw
   * `ReaderClosedError`, and loads in flight reject with it after giving back what they read.
   */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.release();
    this.#drop(this.#dictionaryHeld);
    this.#dictionaryHeld = null;
    this.#dictionary = null;
    this.#dictionaryVector = null;
    this.#options.budget.free(this.#directoryCharge);
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
      decodeRange(this.#view(page), start, stop, target, out, this.#dictionary);
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
      gatherRows(view, rows, k0, k1, entry.firstRow, target, at + k0 - start, this.#dictionary);
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
