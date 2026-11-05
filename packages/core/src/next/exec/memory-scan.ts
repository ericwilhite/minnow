/**
 * The scan source over a table's memory segments (access/snapshot.ts `MemorySegment`): one batch
 * per `MEMORY_CHUNK_ROWS` chunk, deletions applied as the batch's selection, columns emitted as
 * the vectors the write path lends (zero copy) or decodes into the scan's buffers after a
 * re-point. Group segments are read through chunk readers (exec/scan.ts), not here.
 *
 * ## One borrowed chunk at a time (ruling R16.3)
 * - A memory segment lends one chunk at a time: what `chunk()` returns stays valid until the
 *   reader's next `chunk()` call on that segment, or the snapshot's release. The scan reads
 *   every column of chunk k (one row range), builds the batch, and asks for chunk k + 1 only in
 *   its next `next()` call, which the driver makes after the batch's borrow ended
 *   (exec/pipeline.ts). So it never holds two chunks of a segment, and never reads a vector the
 *   write path has taken back.
 * - Assumption (to confirm with B3): "one chunk" means one row chunk across the declared columns;
 *   the vectors of chunk k for several columns are valid together until a call for another
 *   chunk index.
 *
 * ## Synchronous when resident, async after a re-point
 * `deletions()` and `chunk()` return values while the memtable is resident, so the scan's
 * `next()` returns a value and the pipeline runs without awaiting. After the write path re-points
 * a slow reader to the published group, `chunk()` may return a Promise (it decodes into the
 * scan's `out` buffers through the page cache); `next()` then returns a Promise for that batch.
 * A `{ partial }` result (a varlen builder full or denied) cannot be resumed through access/ yet
 * (`ChunkOutput` has no start row), so it fails the query with `UnsupportedPlanError`: an open
 * question for B3.
 */
import type {
  ChunkOutput,
  ChunkResult,
  DeletionVector,
  MemorySegment,
} from "../access/snapshot.js";
import { MEMORY_CHUNK_ROWS } from "../access/snapshot.js";
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
  readonly #selection: Selection = allocateSelection(MEMORY_CHUNK_ROWS);
  #segment = 0;
  #chunk = 0;
  /** The current segment's deletions; undefined until read. */
  #deletions: DeletionVector | null | undefined = undefined;
  #aborted = false;

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
  }

  next(): MaybePromise<SourceItem | null> {
    for (;;) {
      if (this.#aborted) return null;
      const segment = this.#segments[this.#segment];
      if (segment === undefined) return null;
      if (this.#chunk * MEMORY_CHUNK_ROWS >= segment.rowCount) {
        this.#segment += 1;
        this.#chunk = 0;
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
      const chunk = this.#chunk++;
      const from = chunk * MEMORY_CHUNK_ROWS;
      const length = Math.min(MEMORY_CHUNK_ROWS, segment.rowCount - from);
      let selection: Selection | null = null;
      if (this.#deletions !== null) {
        this.#deletions.liveRows(from, length, null, this.#selection);
        if (this.#selection.count === 0) continue;
        if (this.#selection.count < length) selection = this.#selection;
      }
      return this.#read(segment, chunk, length, selection, 0);
    }
  }

  #read(
    segment: MemorySegment,
    chunk: number,
    length: number,
    selection: Selection | null,
    from: number,
  ): MaybePromise<SourceItem> {
    for (let c = from; c < this.#columnIds.length; c++) {
      const out = this.#out[c];
      if (out === undefined) throw new RangeError("Missing output");
      const result = segment.chunk(this.#columnIds[c] ?? -1, chunk, out);
      if (isPromise(result))
        return result.then((value) => {
          this.#take(c, value);
          return this.#read(segment, chunk, length, selection, c + 1);
        });
      this.#take(c, result);
    }
    return { batch: { length, columns: this.#vectors, selection } };
  }

  #take(column: number, result: ChunkResult): void {
    if ("partial" in result)
      throw new UnsupportedPlanError("a partial memory chunk (access/ ChunkOutput cannot resume)");
    this.#vectors[column] = result;
  }

  abort(): void {
    this.#aborted = true;
  }
}
