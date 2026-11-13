/**
 * The memory a heavy operator (hash join, sort) uses for spill progress (track B7):
 *
 * - `Charge`: the operator's charge on its grant, tracking the peak it held.
 * - `Pool`: a sub-budget for transient buffers (staging, page reads, the page encoder, output
 *   chunks) inside the admitted minimum, with progress loans that the pool itself holds.
 *
 * Rules (memory/governor.ts rules 4-5): a step that neither the pool nor the grant can hold
 * borrows exactly its shortfall; the pool allocates the loaned bytes, so no other buffer can take
 * them; the room goes back (the loan repaid) once the step has allocated and its usage has left.
 * One step at a time, so at most one loan at a time.
 */
import type { SpillPageRef } from "../spill/contract.js";
import type { Grant, Loan } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { Vector, FlatVector } from "../vector/vector.js";
import type { ColumnStore } from "./column-store.js";
import { SORT_ROW_VIEW_BYTES, sortRowView } from "./sort-row.js";
import {
  decodePage,
  type DecodedPage,
  PageWriter,
  SpillClosedError,
  type SpillFiles,
  type SpilledRun,
} from "./spill-pages.js";

/** An operator's charge on its grant, tracking the peak it held. */
export class Charge implements ByteCharge {
  peak = 0;
  constructor(readonly grant: Grant) {}
  allocate(bytes: number): boolean {
    if (!this.grant.allocate(bytes)) return false;
    if (this.grant.held > this.peak) this.peak = this.grant.held;
    return true;
  }
  free(bytes: number): void {
    this.grant.free(bytes);
  }
}

/**
 * The pool: a sub-budget for transient buffers, allocated from the grant inside the admitted
 * minimum. Past it, allocations try the grant. When the grant refuses, `need` makes room for one
 * bounded step (one row, one bounded page) with a progress loan (memory/governor.ts rule 5) that
 * the pool itself allocates, so no other buffer can take those bytes; the pool repays it as soon
 * as its usage falls back below its own size. One step at a time, so one loan at a time.
 */
export class Pool implements ByteCharge {
  #used = 0;
  #overflow = 0;
  #size: number;
  readonly #base: number;
  #loan: Loan | null = null;
  /** Bytes added to the pool for the current step (from the grant or the loan). */
  #extra = 0;
  /** The step allocated since it made room (until then, its room is held, not given back). */
  #consumed = false;
  #steps = 0;
  #pins = 0;
  loans = 0;
  largestLoan = 0;
  constructor(
    readonly charge: ByteCharge,
    size: number,
    /** Shared emit workspace charges only the part outside its prepaid envelope. */
    readonly splitOverflow = false,
  ) {
    if (!charge.allocate(size)) throw new RangeError("A join's spill pool must be admitted");
    this.#size = size;
    this.#base = size;
  }

  /** The pool holds a step's extra bytes: shrink buffers when they are reset. */
  get owes(): boolean {
    return this.#extra > 0;
  }

  get loaned(): boolean {
    return this.#loan !== null;
  }

  /** Live suballocations, including ordinary overflow outside the prepaid pool. */
  get used(): number {
    return this.#used + this.#overflow;
  }

  /** Bytes of the pool's own room not in use. */
  get room(): number {
    return this.#size - this.#used;
  }

  /**
   * Takes `bytes` from the pool's room, keeping `keep` bytes of it for other steps; past that
   * from the grant.
   */
  allocate(bytes: number, keep = 0): boolean {
    if (this.#used + bytes <= this.#size - keep) {
      this.#used += bytes;
      if (this.#extra > 0) this.#consumed = true;
      return true;
    }
    if (this.#extra === 0) {
      const pooled = this.splitOverflow ? Math.max(0, this.#size - keep - this.#used) : 0;
      const overflow = bytes - pooled;
      if (this.charge.allocate(overflow)) {
        this.#used += pooled;
        this.#overflow += overflow;
        return true;
      }
    }
    return false;
  }

  free(bytes: number): void {
    const fromOverflow = Math.min(bytes, this.#overflow);
    if (fromOverflow > 0) {
      this.charge.free(fromOverflow);
      this.#overflow -= fromOverflow;
    }
    this.#used -= bytes - fromOverflow;
    this.settle();
  }

  /**
   * Room for `bytes` more in the pool for the next step: from the grant if it has them, else a
   * progress loan of exactly the shortfall (rule 5's one bounded step).
   */
  async need(
    bytes: number,
    grant: Grant,
    signal?: AbortSignal,
    privateStep = false,
  ): Promise<void> {
    if (this.#used + bytes <= this.#size) return;
    this.settle();
    const shortfall = this.#used + bytes - this.#size;
    if (shortfall <= 0) return;
    // Within one step the room may grow again (a row, then its page): from the grant, or one
    // loan; everything goes back together when the step's usage has left.
    if (!privateStep && this.charge.allocate(shortfall)) {
      this.#size += shortfall;
      this.#beginStep(shortfall);
      return;
    }
    if (this.#loan !== null)
      throw new RangeError("A join step needed a second progress loan before repaying its first");
    const loan = await grant.borrow(shortfall, signal);
    this.loans++;
    this.largestLoan = Math.max(this.largestLoan, shortfall);
    if (!this.charge.allocate(shortfall)) {
      loan.repay();
      throw new RangeError("A progress loan could not be held");
    }
    this.#loan = loan;
    this.#size += shortfall;
    this.#beginStep(shortfall);
  }

  #beginStep(extra: number): void {
    this.#extra += extra;
    this.#consumed = false;
  }
  /** A synchronous compound transition may free an old buffer before allocating its
   * replacement. Keep its prepaid step until the entire transition has finished.
   */
  withStep<T>(work: () => T): T {
    this.#steps++;
    try {
      return work();
    } finally {
      this.#steps--;
      this.settle();
    }
  }

  /** Pin a pre-sized multi-buffer step until every simultaneous borrower has left. */
  pinStep(): () => void {
    this.#pins++;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      if (this.#pins > 0) this.#pins--;
      this.settle();
    };
  }

  /**
   * Returns extra bytes once the step has allocated and all live pool usage fits without
   * them. Fixed buffers first created during the step may remain inside the admitted pool.
   */
  settle(): void {
    if (
      this.#steps !== 0 ||
      this.#pins > 0 ||
      this.#extra === 0 ||
      !this.#consumed ||
      this.#used > this.#size - this.#extra
    )
      return;
    this.charge.free(this.#extra);
    this.#size -= this.#extra;
    this.#extra = 0;
    const loan = this.#loan;
    this.#loan = null;
    loan?.repay();
  }

  close(): void {
    if (this.#overflow > 0) this.charge.free(this.#overflow);
    this.#overflow = 0;
    this.charge.free(this.#size);
    this.#size = this.#base;
    this.#pins = 0;
    this.#extra = 0;
    this.#consumed = false;
    this.#loan = null;
  }
}

/**
 * Spill page I/O in bounded steps over a pool (shared by the hash join and the sort): page reads
 * charged while held, decoding into views, staging stores flushed to pages, one oversized row at
 * a time.
 */
export class SpillIo {
  constructor(
    readonly pool: Pool,
    readonly files: SpillFiles,
    readonly pages: PageWriter,
    readonly context: {
      readonly grant: Grant;
      readonly queryId: number;
      readonly signal?: AbortSignal;
    },
    readonly checkAborted: () => void,
    readonly pace?: () => Promise<void> | null,
  ) {}

  /** Called with each staging chunk before it is encoded as a page (the sort measures pages). */
  onFlush: ((staging: ColumnStore) => void) | null = null;
  /** Scalar page statistics only; does not disable borrowed oversized-row writes. */
  onPage:
    | ((
        run: SpilledRun,
        columns: readonly FlatVector[],
        rows: number,
        header: { run: number; partition: number },
      ) => void)
    | null = null;

  /** Room in the pool for one bounded step of `bytes` (see `Pool`). */
  need(bytes: number): Promise<void> {
    return this.pool.need(bytes, this.context.grant, this.context.signal);
  }

  /** Resets an output or staging store, shrinking its arenas while the pool holds a step. */
  reset(store: ColumnStore): void {
    store.reset(this.pool.owes);
    this.pool.settle();
  }

  /** A close or cancellation can arrive at any asynchronous read/decode resumption. */
  #checkOpen(): void {
    this.checkAborted();
    if (this.files.isClosed) throw new SpillClosedError();
  }

  /** Reads and verifies a page (a bounded step), charged to the pool while the caller holds it. */
  async readPage(run: SpilledRun, index: number): Promise<{ bytes: Uint8Array; charged: number }> {
    this.#checkOpen();
    const ref = run.pages[index];
    if (ref === undefined) throw new RangeError("No such spill page");
    return this.readRef(run, ref);
  }
  /** Read a ref held by an on-disk tape directory, through the same charge/drain boundary. */
  async readRef(
    run: SpilledRun,
    ref: SpillPageRef,
  ): Promise<{ bytes: Uint8Array; charged: number }> {
    this.#checkOpen();
    const file = run.file;
    if (file === null) throw new RangeError("No such spill page");
    if (!this.pool.allocate(ref.length)) {
      await this.need(ref.length);
      this.#checkOpen();
      if (!this.pool.allocate(ref.length)) throw new RangeError("A spill page could not be read");
    }
    let end: (() => void) | undefined;
    let bytes: Uint8Array | undefined;
    try {
      end = this.files.beginRead(file);
      bytes = await file.read(ref);
      // The backend may return after cancellation. Never hand that page to a decoder.
      this.#checkOpen();
      return { bytes, charged: ref.length };
    } catch (error) {
      // The cancelled result must not remain owned when its charge is released.
      // eslint-disable-next-line no-useless-assignment
      bytes = undefined;
      this.pool.free(ref.length);
      this.checkAborted();
      throw error;
    } finally {
      // Keep the pool/grant alive until failed/cancelled results gave their bytes back.
      end?.();
    }
  }

  releasePage(page: { charged: number }): void {
    this.pool.free(page.charged);
  }

  /** Decodes a held page into views of its bytes (copies charged to the pool). */
  async decode(
    page: { bytes: Uint8Array; charged: number },
    types: readonly PhysicalType[],
  ): Promise<DecodedPage> {
    this.#checkOpen();
    let decoded = decodePage(page.bytes, types, this.pool);
    if ("needed" in decoded) {
      await this.need(decoded.needed);
      this.#checkOpen();
      decoded = decodePage(page.bytes, types, this.pool);
      if ("needed" in decoded) throw new RangeError("A spill page could not be decoded");
    }
    if (decoded.alignedBytes !== undefined) {
      // Decoder views now own the aligned copy. Drop the original before giving its charge
      // back, so a merge never pins both representations throughout the following output.
      const previous = page.bytes.byteLength;
      page.bytes = decoded.alignedBytes;
      page.charged -= previous;
      this.pool.free(previous);
    }
    page.charged += decoded.charged;
    return decoded;
  }

  /** Encodes and appends one staging chunk as a page of `run`. */
  async flush(
    staging: ColumnStore,
    run: SpilledRun,
    header: { run: number; partition: number },
  ): Promise<void> {
    if (staging.rows === 0) return;
    this.onFlush?.(staging);
    const columns = staging.vectors();
    this.onPage?.(run, columns, staging.rows, header);
    const oversized =
      staging.rows === 1 && PageWriter.encodedBytes(columns, 1) > staging.targetBytes;
    const pageHeader = {
      queryId: this.context.queryId,
      run: header.run,
      partition: header.partition,
      sequence: run.pageCount,
    };
    let page = this.pages.encode(pageHeader, columns, staging.rows);
    if (page === null) {
      await this.need(this.pages.growthFor(columns, staging.rows));
      page = this.pages.encode(pageHeader, columns, staging.rows);
      if (page === null) throw new RangeError("A spill page could not be encoded");
    }
    if (run.write) await run.write(page, staging.rows);
    else await this.files.append(run, page, staging.rows);
    this.pages.shrinkIf(oversized || this.pool.owes);
    this.reset(staging);
  }

  /** Appends rows to a staging store, flushing it as it fills; one oversized row at a time. */
  async stage(
    staging: ColumnStore,
    run: SpilledRun,
    header: { run: number; partition: number },
    vectors: readonly Vector[],
    ids: ArrayLike<number> | null,
    from: number,
    count: number,
  ): Promise<void> {
    let done = 0;
    while (done < count) {
      this.checkAborted();
      const paced = this.pace?.();
      if (paced != null) await paced;
      const row = ids === null ? from + done : (ids[from + done] ?? 0);
      if (
        this.onFlush === null &&
        staging.appendNeed(vectors, row) > Math.max(staging.targetBytes, 64 * 1024)
      ) {
        await this.flush(staging, run, header);
        // The input's payload is already owned. One loan funds the borrowed-row
        // views and page replacement together; no staging arena is copied first.
        const metadata = staging.types.length * SORT_ROW_VIEW_BYTES;
        await this.need(staging.appendNeed(vectors, row) + metadata + 64 * 1024);
        const release = this.pool.pinStep();
        try {
          if (!this.pool.allocate(metadata)) throw new RangeError("Spill row views not admitted");
          try {
            // Checkpoint inputs can retain hidden sort keys beyond this schema.
            // Match ColumnStore.append: only the declared prefix is serialized.
            const views = vectors
              .slice(0, staging.types.length)
              .map((vector) => sortRowView(vector, row));
            this.onPage?.(run, views, 1, header);
            const page = this.pages.encode(
              { queryId: this.context.queryId, ...header, sequence: run.pageCount },
              views,
              1,
            );
            if (page === null) throw new RangeError("Prepaid spill row could not be encoded");
            if (run.write) await run.write(page, 1);
            else await this.files.append(run, page, 1);
            this.pages.shrinkIf(true);
          } finally {
            this.pool.free(metadata);
          }
        } finally {
          release();
        }
        done++;
        continue;
      }
      done += staging.append(vectors, ids, from + done, count - done);
      // An oversized row may fit without a loan. Do not retain its arena in one partition
      // while filling the others: together they can pin a loan past this row-sized step.
      if (
        staging.rows === 1 &&
        PageWriter.encodedBytes(staging.vectors(), 1) > staging.targetBytes
      ) {
        await this.flush(staging, run, header);
        continue;
      }
      if (done >= count) break;
      if (staging.rows > 0) {
        await this.flush(staging, run, header);
        continue;
      }
      // Not even one row fits an empty staging chunk: room for exactly that row, which then
      // goes out as a page of its own before the next step.
      // The step is the row and the page it becomes: room for both at once.
      const rowBytes = staging.appendNeed(vectors, row);
      await this.need(2 * rowBytes + 64 * 1024);
      const one = staging.append(vectors, ids, from + done, 1);
      if (one === 0) throw new RangeError("A spill staging row could not be allocated");
      done += one;
      await this.flush(staging, run, header);
    }
  }
}
