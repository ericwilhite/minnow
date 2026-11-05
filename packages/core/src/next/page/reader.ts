/**
 * Reads one column chunk: its directory, then only the pages a row range or a selection needs.
 * This is the executor's `ChunkPages` (exec/scan.ts, ruling R14.4), opened over the extent
 * space's reader (`PageSpace`: `read` with a lease, `peek` for cache hits).
 *
 * - **Verify once.** The space checks every blob's xxHash32 when its bytes come from storage and
 *   caches verified encoded blobs; the reader does not hash again. It parses each page (the
 *   structural checks of decode.ts) when it loads it; a loaded page decodes with no checks.
 * - **Synchronous when cached (ruling R6).** `open`, `load` and `loadDictionary` return a value,
 *   not a Promise, when every blob they need is resident (`peek`) and needs no inflating; they
 *   go through `read` otherwise.
 * - **Memory.** Every byte a loaded page holds is charged to the `budget` given to `open`: leased
 *   bytes by the space itself, peeked and inflated bytes by the reader, until `release` (or
 *   `close`, which also drops the dictionary). A refused charge on the read path is the space's
 *   `ReadDeniedError` (borrow a loan, retry); on the reader's own charges it is `PageBudgetError`.
 *   Directories are parsed to O(pages) objects, not held as bytes.
 * - **Only what is needed.** `pagesInRange` and `pagesForRows` name the pages; `load` reads just
 *   those, ascending (the space coalesces neighbours), in calls of at most `MAX_READ_BYTES`, plus
 *   the dictionary page when one of them is dictionary-encoded.
 * - **Late materialization.** `gather` decodes only the selected rows: plain and dictionary pages
 *   index directly, FOR pages too, delta pages walk at most one 128-row block per run of
 *   selected rows. `gatherCodes` does the same for dictionary codes.
 *
 * Decoding is synchronous over loaded pages; every target buffer is the caller's.
 */
import type { BlobRef, ColumnChunk, PageEntry } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import type { ByteCharge } from "../vector/varlen.js";
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
import { inflatePage } from "./gzip.js";

type Rows = Uint16Array | Uint32Array | Int32Array;
export type MaybePromise<T> = T | Promise<T>;

/** Every blob is at most this (storage ruling R8); pages and directories are blobs. */
export const MAX_BLOB_BYTES = 1024 * 1024;
/** The most bytes one `PageSpace.read` call may ask for (storage ruling R8). */
export const MAX_READ_BYTES = 4 * MAX_BLOB_BYTES;

/** A grant's suballocator (memory/governor.ts `Grant`; the space's `ByteBudget`). */
export type PageBudget = ByteCharge;

/** A lease on verified blobs (storage/extents.ts `ReadLease`). */
export interface PageLease {
  readonly bytes: readonly Uint8Array[];
  release(): void;
}

/**
 * The extent space's reader (storage/extents.ts `ExtentReader`, access/space.ts `SpaceReader`),
 * stated structurally: those modules are not on the contracts branch yet.
 */
export interface PageSpace {
  read(
    refs: readonly BlobRef[],
    options: { readonly budget: PageBudget; readonly signal?: AbortSignal },
  ): Promise<PageLease>;
  peek(ref: BlobRef): Uint8Array | null;
}

export interface ChunkReadOptions {
  /** Charged for every byte the reader holds (ruling R5). */
  readonly budget: PageBudget;
  readonly signal?: AbortSignal;
}

/** The reader could not charge bytes it must hold (a peeked or inflated page). */
export class PageBudgetError extends Error {
  readonly code = "MINNOW_PAGE_BUDGET";
  constructor(readonly bytes: number) {
    super(`Loading pages needs ${String(bytes)} bytes the budget does not have`);
    this.name = "PageBudgetError";
  }
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

/** One loaded page or dictionary: its parsed view and what keeps its bytes alive. */
interface Held {
  /** Bytes the reader itself charged (peeked or inflated). */
  readonly charged: number;
  /** The lease its stored bytes came in, shared with the other pages of that read. */
  readonly lease: SharedLease | null;
}

class SharedLease {
  #holders: number;
  constructor(
    readonly lease: PageLease,
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

export class ChunkReader {
  readonly chunk: ColumnChunk;
  readonly directory: ChunkDirectory;
  readonly #space: PageSpace;
  readonly #options: ChunkReadOptions;
  readonly #pages: Array<PageView | undefined>;
  readonly #held: Array<Held | undefined>;
  #dictionary: PageDictionary | null = null;
  #dictionaryHeld: Held | null = null;
  #dictionaryVector: FlatVector | null = null;

  private constructor(
    space: PageSpace,
    chunk: ColumnChunk,
    directory: ChunkDirectory,
    options: ChunkReadOptions,
  ) {
    this.#space = space;
    this.#options = options;
    this.chunk = chunk;
    this.directory = directory;
    this.#pages = new Array<PageView | undefined>(directory.pages.length);
    this.#held = new Array<Held | undefined>(directory.pages.length);
  }

  /** Reads the chunk's directory (no pages): synchronously when it is cached. */
  static open(
    space: PageSpace,
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
      return new ChunkReader(space, chunk, directory, options);
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
   * needs it. Synchronous when all are cached and none is gzipped.
   */
  load(pageIndices: readonly number[]): MaybePromise<void> {
    const pages = this.directory.pages;
    const missing = [...new Set(pageIndices)]
      .filter((index) => {
        if (pages[index] === undefined) throw new RangeError("Page index outside the chunk");
        return this.#pages[index] === undefined;
      })
      .sort((a, b) => a - b);
    const wantsDictionary =
      this.#dictionary === null && missing.some((index) => isDictionary(at(pages, index)));
    if (missing.length === 0 && !wantsDictionary) return;
    if (this.#loadCached(missing, wantsDictionary)) return;
    return this.#loadRead(missing, wantsDictionary);
  }

  /** The sync path: every blob resident and plain. All or nothing. */
  #loadCached(missing: readonly number[], wantsDictionary: boolean): boolean {
    const pages = this.directory.pages;
    const entries = missing.map((index) => at(pages, index));
    const dictionaryEntry = wantsDictionary ? this.directory.dictionary : null;
    if (dictionaryEntry !== null) entries.push(dictionaryEntry);
    if (entries.some(isGzip)) return false;
    const found: Uint8Array[] = [];
    for (const entry of entries) {
      const bytes = this.#space.peek(blobOf(this.chunk, entry));
      if (bytes === null) return false;
      found.push(bytes);
    }
    const total = entries.reduce((sum, entry) => sum + entry.length, 0);
    // Peeked bytes are uncharged by the space: the reader charges them while it holds them.
    if (!this.#options.budget.allocate(total)) return false;
    let charged = total;
    try {
      missing.forEach((index, k) => {
        const entry = at(entries, k);
        this.#pages[index] = parsePage(
          sized(found[k], entry.length),
          entry,
          this.directory.tag,
          this.directory.scale,
        );
        this.#held[index] = { charged: entry.length, lease: null };
        charged -= entry.length;
      });
      if (dictionaryEntry !== null) {
        const bytes = sized(found[missing.length], dictionaryEntry.length);
        this.#setDictionary(parseDictionary(bytes, dictionaryEntry, this.directory.tag), {
          charged: dictionaryEntry.length,
          lease: null,
        });
        charged -= dictionaryEntry.length;
      }
    } finally {
      if (charged > 0) this.#options.budget.free(charged);
    }
    return true;
  }

  async #loadRead(missing: readonly number[], wantsDictionary: boolean): Promise<void> {
    const pages = this.directory.pages;
    const jobs: Array<{ entry: PageEntry; page: number | null }> = missing.map((page) => ({
      entry: at(pages, page),
      page,
    }));
    const dictionaryEntry = wantsDictionary ? this.directory.dictionary : null;
    if (dictionaryEntry !== null) jobs.push({ entry: dictionaryEntry, page: null });
    // Batches of at most MAX_READ_BYTES (every blob is ≤ MAX_BLOB_BYTES, so each fits).
    let start = 0;
    while (start < jobs.length) {
      let end = start;
      let bytes = 0;
      while (end < jobs.length && bytes + (jobs[end]?.entry.length ?? 0) <= MAX_READ_BYTES)
        bytes += jobs[end++]?.entry.length ?? 0;
      if (end === start) end++;
      const batch = jobs.slice(start, end);
      const lease = await this.#space.read(
        batch.map(({ entry }) => blobOf(this.chunk, entry)),
        this.#options,
      );
      const shared = new SharedLease(lease, batch.length);
      // `#install` takes over one hold on the lease per job, success or failure.
      for (let k = 0; k < batch.length; k++) {
        const { entry, page } = at(batch, k);
        try {
          await this.#install(entry, page, lease.bytes[k], shared);
        } catch (error) {
          for (let rest = k + 1; rest < batch.length; rest++) shared.drop();
          throw error;
        }
      }
      start = end;
    }
  }

  async #install(
    entry: PageEntry,
    page: number | null,
    stored: Uint8Array | undefined,
    lease: SharedLease,
  ): Promise<void> {
    let bytes: Uint8Array;
    let charged = 0;
    let holder: SharedLease | null = lease;
    try {
      bytes = sized(stored, entry.length);
      if (isGzip(entry)) {
        // The inflated page is the reader's: charge it; the stored bytes go back with the lease.
        bytes = await inflatePage(bytes);
        if (!this.#options.budget.allocate(bytes.length)) throw new PageBudgetError(bytes.length);
        charged = bytes.length;
        holder = null;
        lease.drop();
      }
    } catch (error) {
      if (holder !== null) lease.drop();
      throw error;
    }
    const held: Held = { charged, lease: holder };
    try {
      if (page === null)
        this.#setDictionary(parseDictionary(bytes, entry, this.directory.tag), held);
      else {
        this.#pages[page] = parsePage(bytes, entry, this.directory.tag, this.directory.scale);
        this.#held[page] = held;
      }
    } catch (error) {
      this.#drop(held);
      throw error;
    }
  }

  #setDictionary(dictionary: PageDictionary, held: Held): void {
    this.#dictionary = dictionary;
    this.#dictionaryHeld = held;
    this.#dictionaryVector = dictionaryVector(dictionary, this.chunk.type);
  }

  #drop(held: Held | undefined | null): void {
    if (held === undefined || held === null) return;
    if (held.charged > 0) this.#options.budget.free(held.charged);
    held.lease?.drop();
  }

  /** Loads the dictionary page on its own (for a dictionary vector before any page). */
  loadDictionary(): MaybePromise<FlatVector | null> {
    if (this.directory.dictionary === null) return null;
    if (this.#dictionaryVector !== null) return this.#dictionaryVector;
    if (this.#loadCached([], true)) return this.#dictionaryVector;
    return this.#loadRead([], true).then(() => this.#dictionaryVector);
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

  /** Releases every page and the dictionary. The reader can load again afterwards. */
  close(): void {
    this.release();
    this.#drop(this.#dictionaryHeld);
    this.#dictionaryHeld = null;
    this.#dictionary = null;
    this.#dictionaryVector = null;
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
