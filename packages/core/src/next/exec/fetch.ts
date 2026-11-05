/**
 * The row fetcher: how the IR's `fetch` operator turns row locators (vector/locator.ts) into
 * column values (late materialization). Frozen interface; the storage-facing implementation reads
 * pages through the page cache, `MemoryRowFetcher` is the reference and test double.
 *
 * ## Contract
 * - **Snapshot.** Every request names the snapshot its locators came from (access/snapshot.ts:
 *   the storage-facing fetcher reads its segments). A released snapshot is refused
 *   (`SnapshotReleasedError`): locators are meaningless outside their pin.
 * - **Rows.** The request's active rows are `selection` (or 0 … length), starting at active index
 *   `from`. Output slot k receives the row of the (from + k)-th active locator: outputs are dense,
 *   in active-row order, with no selection. Repeated locators fill each slot with the same value;
 *   a null locator (validity bit clear: an outer join's null-extended row) fills null.
 * - **Capacity.** `capacity` is the most slots one call may fill; every output must hold at least
 *   that many (checked: `RangeError`). A call never fills more, and never reports `done` while
 *   active rows remain beyond it: it returns `{ done: false, reason: "capacity" }` instead.
 * - **Buffers.** The caller owns every output (`output[c]` for each column, with a validity bitmap)
 *   and supplies one `VarlenBuilder` per variable-width column, built on the caller's grant. The
 *   fetcher's own staging (the visit order and one page buffer, for at most `capacity` rows) is
 *   allocated from `grant` before any read; if refused, the call returns `denied` having read
 *   nothing. It allocates nothing per row and never resizes outputs.
 * - **Partial completion.** If a varlen builder answers `full` (the batch reached its byte target)
 *   or `denied` (the grant cannot supply the bytes), the fetcher stops and returns
 *   `{ done: false, filled, reason }`: slots 0 … filled − 1 are complete. The caller emits them (or,
 *   for `denied`, borrows a progress loan) and calls again with `from + filled`.
 * - **I/O.** One call reads each needed page of its (at most `capacity`) rows at most once:
 *   locators are visited sorted by segment and page, values scattered back to their slots. Pages
 *   are not retained across calls (beyond the shared page cache), so a resumed call may read a page
 *   an earlier call already read.
 * - **Cancellation.** If `signal` aborts, the call rejects with its reason between page reads; the
 *   outputs are unspecified and nothing is retained.
 */
import type { MaybePromise } from "../access/key-index.js";
import { SnapshotReleasedError, type Snapshot } from "../access/snapshot.js";
import type { Grant } from "../memory/governor.js";
import { locatorPosition, locatorSegment } from "../vector/locator.js";
import { isValid, setValid } from "../vector/validity.js";
import type { VarlenBuilder } from "../vector/varlen.js";
import type { FlatVector, Selection } from "../vector/vector.js";

export interface FetchRequest {
  readonly snapshot: Snapshot;
  readonly tableId: number;
  /** A locator vector (type `locator`), `length` physical rows. */
  readonly locators: FlatVector;
  readonly length: number;
  readonly selection: Selection | null;
  /** Active index to start at (0, or a previous result's `from + filled`). */
  readonly from: number;
  /** The most output slots this call may fill (every output holds at least this many). */
  readonly capacity: number;
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
  | {
      readonly done: false;
      readonly filled: number;
      readonly reason: "capacity" | "full" | "denied";
    };

/**
 * Synchronous when every page it needs is cached (ruling R6: a cached point read never awaits);
 * the caller awaits only an actual Promise. The production fetcher serves access/ snapshots over
 * B2's chunk readers (exec/scan.ts).
 */
export interface RowFetcher {
  fetch(request: FetchRequest): MaybePromise<FetchResult>;
}

/** Slots a flat output vector can hold. */
function slots(vector: FlatVector): number {
  const data = vector.data;
  switch (data.layout) {
    case "varlen":
      return data.offsets.length - 1;
    case "fixed16":
      return data.bytes.length / 16;
    case "decimal":
      return data.coefficients.repr === "int128"
        ? data.coefficients.limbs.length / 4
        : data.coefficients.values.length;
    case "interval":
      return data.months.length;
    case "int64":
      return data.values.values.length;
    default:
      return data.values.length;
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
    const { locators, selection, output, varlen, capacity } = request;
    const data = locators.data;
    if (data.layout !== "f64") throw new TypeError("Locators must be an f64 locator vector");
    for (const vector of output)
      if (slots(vector) < capacity) throw new RangeError("An output is smaller than the capacity");
    const remaining = (selection === null ? request.length : selection.count) - request.from;
    const total = Math.min(remaining, capacity);
    // Staging for this call only: visit order + one value per column per row + one page buffer.
    const stagingBytes = total * 8 * (1 + output.length) + 8 * this.pageRows;
    if (!request.grant.allocate(stagingBytes)) return { done: false, filled: 0, reason: "denied" };
    try {
      const rowOf = (k: number) =>
        selection === null ? request.from + k : (selection.rows[request.from + k] ?? 0);
      const sortKey = (k: number) => {
        const row = rowOf(k);
        return isValid(locators.validity, row) ? (data.values[row] ?? 0) : -1;
      };
      const order = Array.from({ length: total }, (_, k) => k).sort(
        (a, b) => sortKey(a) - sortKey(b),
      );
      const values: Array<Array<number | string | null>> = output.map(() =>
        new Array<number | string | null>(total).fill(null),
      );
      let currentPage = "";
      for (const k of order) {
        const row = rowOf(k);
        if (!isValid(locators.validity, row)) continue;
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
      return total < remaining
        ? { done: false, filled: total, reason: "capacity" }
        : { done: true, filled: total };
    } finally {
      request.grant.free(stagingBytes);
    }
  }
}
