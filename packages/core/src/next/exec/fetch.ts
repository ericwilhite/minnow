/**
 * The row fetcher: how the IR's `fetch` operator turns row locators (vector/locator.ts) into
 * column values (late materialization). Frozen interface; the storage-facing implementation reads
 * pages through the page cache, `MemoryRowFetcher` is the reference and test double.
 *
 * ## Contract
 * - **Snapshot.** Every request names the snapshot pin its locators came from. A released pin is
 *   refused (`SnapshotReleasedError`): locators are meaningless outside their pin.
 * - **Rows.** The request's active rows are `selection` (or 0 … length), starting at active index
 *   `from`. Output slot k receives the row of the (from + k)-th active locator: outputs are dense,
 *   in active-row order, with no selection. Repeated locators fill each slot with the same value;
 *   a null locator (validity bit clear: an outer join's null-extended row) fills null.
 * - **Buffers.** The caller owns and sizes every output (`output[c]` for each column, with a
 *   validity bitmap) and supplies one `VarlenBuilder` per variable-width column, built on the
 *   caller's grant. The fetcher allocates nothing per row and never resizes outputs.
 * - **Partial completion.** If a varlen builder answers `full` (the batch reached its byte target)
 *   or `denied` (the grant cannot supply the bytes), the fetcher stops and returns
 *   `{ done: false, filled, reason }`: slots 0 … filled − 1 are complete. The caller emits them (or,
 *   for `denied`, borrows a progress loan) and calls again with `from + filled`.
 * - **I/O.** One call reads each needed page at most once (locators are visited sorted by segment
 *   and page, values scattered back to their slots), awaiting page reads between pages.
 * - **Cancellation.** If `signal` aborts, the call rejects with its reason between page reads; the
 *   outputs are unspecified and nothing is retained.
 */
import type { Grant } from "../memory/governor.js";
import { locatorPosition, locatorSegment } from "../vector/locator.js";
import { isValid, setValid } from "../vector/validity.js";
import type { VarlenBuilder } from "../vector/varlen.js";
import type { FlatVector, Selection } from "../vector/vector.js";

export interface SnapshotPin {
  readonly rootSequence: number;
  readonly released: boolean;
  release(): void;
}

export interface FetchRequest {
  readonly snapshot: SnapshotPin;
  readonly tableId: number;
  /** A locator vector (type `locator`), `length` physical rows. */
  readonly locators: FlatVector;
  readonly length: number;
  readonly selection: Selection | null;
  /** Active index to start at (0, or a previous result's `from + filled`). */
  readonly from: number;
  /** Catalog column ids to fetch, one output each. */
  readonly columns: readonly number[];
  readonly output: readonly FlatVector[];
  /** A builder per varlen output column, null for fixed-width ones. */
  readonly varlen: ReadonlyArray<VarlenBuilder | null>;
  /** The caller's grant: the fetcher's own page buffers are charged here. */
  readonly grant: Grant;
  readonly signal?: AbortSignal;
}

export type FetchResult =
  | { readonly done: true; readonly filled: number }
  | { readonly done: false; readonly filled: number; readonly reason: "full" | "denied" };

export interface RowFetcher {
  fetch(request: FetchRequest): Promise<FetchResult>;
}

export class SnapshotReleasedError extends Error {
  readonly code = "MINNOW_SNAPSHOT_RELEASED";
  constructor() {
    super("The snapshot these locators belong to has been released");
    this.name = "SnapshotReleasedError";
  }
}

/** One segment of the reference fetcher: per column id, int32 numbers or strings. */
export type MemorySegment = ReadonlyMap<number, ReadonlyArray<number | string | null>>;

/** The reference implementation over in-memory segments, with simulated paged reads. */
export class MemoryRowFetcher implements RowFetcher {
  /** Page reads performed (for the read-each-page-once rule). */
  pageReads = 0;

  constructor(
    readonly segments: readonly MemorySegment[],
    readonly pageRows = 1024,
    readonly readPage: () => Promise<void> = () => Promise.resolve(),
  ) {}

  async fetch(request: FetchRequest): Promise<FetchResult> {
    if (request.snapshot.released) throw new SnapshotReleasedError();
    const { locators, selection, output, varlen } = request;
    const data = locators.data;
    if (data.layout !== "f64") throw new TypeError("Locators must be an f64 locator vector");
    const total = (selection === null ? request.length : selection.count) - request.from;
    // Visit in (segment, page) order; scatter to dense slots.
    const order = Array.from({ length: total }, (_, k) => k);
    const rowOf = (k: number) =>
      selection === null ? request.from + k : (selection.rows[request.from + k] ?? 0);
    const sortKey = (k: number) => {
      const row = rowOf(k);
      return isValid(locators.validity, row) ? (data.values[row] ?? 0) : -1;
    };
    order.sort((a, b) => sortKey(a) - sortKey(b));
    const pageBytes = 8 * this.pageRows;
    if (!request.grant.allocate(pageBytes)) return { done: false, filled: 0, reason: "denied" };
    try {
      let currentPage = "";
      // Values are produced in page order but committed to builders in slot order, so varlen
      // columns are filled after the fixed-width pass, slot by slot.
      const values: Array<Array<number | string | null>> = output.map(() =>
        new Array<number | string | null>(total).fill(null),
      );
      for (const k of order) {
        const row = rowOf(k);
        if (!isValid(locators.validity, row)) {
          for (const column of values) column[k] = null;
          continue;
        }
        const locator = data.values[row] ?? 0;
        const segment = locatorSegment(locator);
        const position = locatorPosition(locator);
        const page = `${String(segment)}:${String(Math.floor(position / this.pageRows))}`;
        if (page !== currentPage) {
          request.signal?.throwIfAborted();
          await this.readPage();
          request.signal?.throwIfAborted();
          this.pageReads += 1;
          currentPage = page;
        }
        request.columns.forEach((columnId, c) => {
          const column = values[c];
          if (column !== undefined)
            column[k] = this.segments[segment]?.get(columnId)?.[position] ?? null;
        });
      }
      for (let k = 0; k < total; k++) {
        for (const [c, vector] of output.entries()) {
          const value = values[c]?.[k] ?? null;
          const builder = varlen[c] ?? null;
          if (builder !== null) {
            const result =
              value === null ? builder.appendEmpty() : builder.appendString(String(value));
            if (result !== "ok") {
              // Roll slot k back out of the builders already appended, so slots < k are complete.
              for (const earlier of varlen.slice(0, c)) earlier?.truncate(k);
              return { done: false, filled: k, reason: result };
            }
          } else if (vector.data.layout === "i32")
            vector.data.values[k] = (value as number | null) ?? 0;
          if (vector.validity !== null) setValid(vector.validity, k, value !== null);
          vector.length = k + 1;
        }
      }
      return { done: true, filled: total };
    } finally {
      request.grant.free(pageBytes);
    }
  }
}
