/**
 * The scan source over a table's memory segments (access/snapshot.ts `MemorySegment`): one batch
 * per `MEMORY_CHUNK_ROWS` chunk (or per part of one, below), deletions applied as the batch's
 * selection, columns emitted as the vectors the write path lends (zero copy) or decodes into the
 * scan's buffers after a re-point. Group segments are read through chunk readers (exec/scan.ts),
 * not here.
 *
 * ## One borrowed chunk at a time (ruling R16.3)
 * - A memory segment lends one chunk index at a time: the vectors `chunk()` returned for that
 *   index (every column) stay valid until `chunk()` is called on that segment with another index,
 *   or the snapshot's release (access/snapshot.ts "Borrow"). The scan reads every column of chunk
 *   k (one row range), builds the batch, and asks for chunk k + 1 only in its next `next()` call,
 *   which the driver makes after the batch's borrow ended (exec/pipeline.ts). So it never holds
 *   two chunks of a segment, and never reads a vector the write path has taken back.
 *
 * ## Synchronous when resident, async after a re-point
 * `deletions()` and `chunk()` return values while the memtable is resident, so the scan's
 * `next()` returns a value and the pipeline runs without awaiting. After the write path re-points
 * a slow reader to the published group, `chunk()` may return a Promise (it decodes into the
 * scan's `out` buffers through the page cache); `next()` then returns a Promise for that batch.
 *
 * ## Partial chunks resume
 * A re-pointed decode stops early when a column's varlen builder is `full` or `denied`
 * (`complete` false). The batch then covers chunk rows `start … end − 1`, where `end` is the
 * smallest `firstRow + rowCount` over the columns; the scan's next `next()` resets the builders
 * and reads every column again from `startRow = end` (the same chunk index, so the borrow rule
 * holds). Columns that decoded past `end` are decoded again from there: simple, and bounded by
 * one chunk. Every column of one batch must share one `firstRow`; slot k is chunk row
 * `firstRow + k`, and rows before `start` are left out of the selection.
 * A `denied` part with no row at all would need a progress loan, which this scan has no grant to
 * borrow; it fails with `UnsupportedPlanError` until the executor wires loans into it.
 */
import type {
  ChunkOutput,
  ChunkResult,
  DeletionVector,
  MemorySegment,
} from "../access/snapshot.js";
import { MEMORY_CHUNK_ROWS, resumeRow } from "../access/snapshot.js";
import type { MaybePromise } from "../access/key-index.js";
import { allocateSelection } from "../vector/validity.js";
import type { Selection, Vector } from "../vector/vector.js";
import { UnsupportedPlanError } from "./errors.js";
import { isPromise, type BatchSource, type SourceItem } from "./pipeline.js";

export class MemorySegmentScan implements BatchSource {
  readonly #segments: readonly MemorySegment[];
  readonly #columnIds: readonly number[];
  readonly #out: readonly ChunkOutput[];
  readonly #vectors: Vector[];
  readonly #results: Array<ChunkResult | undefined>;
  readonly #selection: Selection = allocateSelection(MEMORY_CHUNK_ROWS);
  #segment = 0;
  /** The chunk index the next read continues, or starts when `#start` is 0. */
  #chunk = 0;
  /** Chunk-relative row the next read of `#chunk` starts at; 0 at a chunk's start. */
  #start = 0;
  /** The current segment's deletions; undefined until read. */
  #deletions: DeletionVector | null | undefined = undefined;
  #aborted = false;
  /** Where the last batch came from: its segment and the position of its slot 0. */
  #lastSegment = -1;
  #lastStart = 0;

  /**
   * `columnIds` are declared columns of the table snapshot; `out[c]` are the scan's own buffers
   * (from its grant) that a re-pointed chunk decodes into, one per column.
   */
  constructor(
    segments: readonly MemorySegment[],
    columnIds: readonly number[],
    out: readonly ChunkOutput[],
  ) {
    if (out.length !== columnIds.length) throw new RangeError("One output per column");
    this.#segments = segments;
    this.#columnIds = columnIds;
    this.#out = out;
    this.#vectors = new Array<Vector>(columnIds.length);
    this.#results = new Array<ChunkResult | undefined>(columnIds.length);
  }

  next(): MaybePromise<SourceItem | null> {
    for (;;) {
      if (this.#aborted) return null;
      const segment = this.#segments[this.#segment];
      if (segment === undefined) return null;
      if (this.#chunk * MEMORY_CHUNK_ROWS >= segment.rowCount) {
        this.#segment += 1;
        this.#chunk = 0;
        this.#start = 0;
        this.#deletions = undefined;
        continue;
      }
      if (this.#deletions === undefined) {
        const deletions = segment.deletions();
        if (isPromise(deletions))
          return deletions.then((value) => {
            this.#deletions = value;
            return this.next();
          });
        this.#deletions = deletions;
      }
      const length = Math.min(
        MEMORY_CHUNK_ROWS,
        segment.rowCount - this.#chunk * MEMORY_CHUNK_ROWS,
      );
      if (this.#start === 0 && this.#deletions !== null) {
        // Skip a chunk with no live row before reading any of it.
        this.#deletions.liveRows(this.#chunk * MEMORY_CHUNK_ROWS, length, null, this.#selection);
        if (this.#selection.count === 0) {
          this.#chunk += 1;
          continue;
        }
      }
      // The previous batch's borrow has ended: its varlen bytes may be reused.
      for (const out of this.#out) out.varlen?.reset();
      return this.#read(segment, length, 0);
    }
  }

  #read(segment: MemorySegment, length: number, from: number): MaybePromise<SourceItem> {
    for (let c = from; c < this.#columnIds.length; c++) {
      const out = this.#out[c];
      if (out === undefined) throw new RangeError("Missing output");
      const result = segment.chunk(this.#columnIds[c] ?? -1, this.#chunk, out, this.#start);
      if (isPromise(result))
        return result.then((value) => {
          this.#results[c] = value;
          return this.#read(segment, length, c + 1);
        });
      this.#results[c] = result;
    }
    return this.#emit(length);
  }

  /** Builds the batch of chunk rows `#start … end − 1` and advances to what follows it. */
  #emit(length: number): SourceItem {
    const start = this.#start;
    let firstRow = start;
    let end = length;
    let denied = false;
    for (let c = 0; c < this.#results.length; c++) {
      const result = this.#results[c];
      if (result === undefined) throw new RangeError("Missing chunk result");
      this.#results[c] = undefined;
      if (c === 0) firstRow = result.firstRow;
      if (result.firstRow !== firstRow || firstRow > start)
        throw new RangeError(
          `Memory chunk ${String(this.#chunk)}: the columns do not share a first row at or before row ${String(start)}`,
        );
      const reached = resumeRow(result) ?? length;
      if (reached < end) end = reached;
      if (result.reason === "denied" && reached <= start) denied = true;
      this.#vectors[c] = result.vector;
    }
    if (end <= start) {
      if (denied)
        throw new UnsupportedPlanError(
          "a re-pointed memory chunk denied before its first row (needs a progress loan)",
        );
      throw new RangeError(
        `Memory chunk ${String(this.#chunk)} made no progress at row ${String(start)}`,
      );
    }
    const selection = this.#select(start - firstRow, end - start, length);
    this.#lastSegment = this.#segments[this.#segment]?.index ?? -1;
    this.#lastStart = this.#chunk * MEMORY_CHUNK_ROWS + firstRow;
    if (end === length) {
      this.#chunk += 1;
      this.#start = 0;
    } else this.#start = end;
    return { batch: { length: end - firstRow, columns: this.#vectors, selection } };
  }

  /**
   * The live rows among chunk rows `#start … #start + count − 1`, as batch slots (`offset` is the
   * slot of `#start`), or null when every slot of the batch is live.
   */
  #select(offset: number, count: number, length: number): Selection | null {
    const deletions = this.#deletions ?? null;
    const selection = this.#selection;
    if (deletions === null) {
      if (offset === 0) return null;
      for (let k = 0; k < count; k++) selection.rows[k] = offset + k;
      selection.count = count;
      return selection;
    }
    // `next()` already computed the whole chunk's live rows when it starts at row 0.
    if (!(this.#start === 0 && count === length))
      deletions.liveRows(this.#chunk * MEMORY_CHUNK_ROWS + this.#start, count, null, selection);
    if (offset === 0 && selection.count === count) return null;
    if (offset !== 0)
      for (let k = 0; k < selection.count; k++)
        selection.rows[k] = (selection.rows[k] ?? 0) + offset;
    return selection;
  }

  /** The segment (`Segment.index`) the last batch came from (locators). */
  get lastSegment(): number {
    return this.#lastSegment;
  }

  /** The segment position of the last batch's slot 0: slot k is position `lastStart + k`. */
  get lastStart(): number {
    return this.#lastStart;
  }

  abort(): void {
    this.#aborted = true;
  }
}
