/** A private one-row view for conflict reads. It never emits physical locators.
 * Unlike a sparse scan mask, this bounds native sizing/decoding to the selected row.
 * Underlying hints retain their exact source origin and physical coordinates. */
import type { ChunkResult, MemorySegment } from "../access/snapshot.js";
import { MEMORY_CHUNK_ROWS } from "../access/snapshot.js";

export class PointMemorySegment implements MemorySegment {
  readonly kind = "memory";
  readonly index = 0;
  readonly rowCount = 1;
  readonly liveRowCount = 1;
  readonly #chunk: number;
  readonly #row: number;
  constructor(
    readonly source: MemorySegment,
    position: number,
  ) {
    if (!Number.isInteger(position) || position < 0 || position >= source.rowCount)
      throw new RangeError("Point fetch is outside its source segment");
    this.#chunk = Math.floor(position / MEMORY_CHUNK_ROWS);
    this.#row = position % MEMORY_CHUNK_ROWS;
  }
  readHint(...[request, context]: Parameters<MemorySegment["readHint"]>) {
    if (request.chunk !== 0 || request.startRow !== 0)
      throw new RangeError("Point fetch has one logical row");
    return this.source.readHint(
      { ...request, chunk: this.#chunk, startRow: this.#row, maxRows: 1 },
      context,
    );
  }
  chunk(...[column, chunk, out, start = 0, step]: Parameters<MemorySegment["chunk"]>) {
    if (chunk !== 0 || start !== 0 || step === undefined)
      throw new TypeError("Point fetch requires its bounded one-row read");
    const project = (result: ChunkResult): ChunkResult => {
      if (result.firstRow !== this.#row || result.rowCount > 1)
        throw new RangeError("Point fetch returned an unexpected native interval");
      return {
        ...result,
        firstRow: 0,
        complete: result.rowCount === 1,
        reason: result.rowCount === 1 ? null : result.reason,
        ...(result.need === undefined ? {} : { need: { ...result.need, row: 0 } }),
      };
    };
    const result = this.source.chunk(column, this.#chunk, out, this.#row, step);
    return result instanceof Promise ? result.then(project) : project(result);
  }
  deletions() {
    // A locator identifies the captured physical version, including a deleted version.
    return null;
  }
  statistics() {
    return null;
  }
  pages(column: number) {
    return this.source.pages(column);
  }
}
