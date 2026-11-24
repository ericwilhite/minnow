/**
 * One statement's reusable fetch of single rows by locator: ON CONFLICT DO UPDATE reads each
 * conflicting row, one proposal at a time, with it (exec/modify-upsert.ts).
 *
 * A row of a memory segment is read with the segment's bounded one-row contract directly
 * (`readHint`, then `chunk` per column), not through a `TableScan` built per row. The reads share
 * one `ReaderMemo` (access/reader-memo.ts): a row of a published memtable reuses the group root
 * and column readers the previous fetch opened on the same root, so consecutive conflicting rows
 * cost one page decode each instead of a root parse and a reader open per column per read.
 *
 * Memory: the memo is charged to `memoBudget` (the statement grant's charge, outside the pool's
 * step room, so it never holds a progress loan open) and given back by `close`, which the writer
 * calls at statement end, on abort and on error. A fetch the memo's budget refuses closes the memo
 * and reads again without it. The one-row outputs are charged to `budget` for one fetch only.
 * A row of a group segment keeps the one-row scan.
 */
import { MemoDeclinedError, ReaderMemo } from "../access/reader-memo.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import type {
  MemoryReadContext,
  MemorySegment,
  Segment,
  TableSnapshot,
} from "../access/snapshot.js";
import { MEMORY_CHUNK_ROWS } from "../access/snapshot.js";
import type { Grant } from "../memory/governor.js";
import { locatorPosition, locatorSegment } from "../vector/locator.js";
import type { PhysicalType } from "../types.js";
import type { Vector } from "../vector/vector.js";
import type { ColumnStore } from "./column-store.js";
import type { CompileContext } from "./kernels/compile.js";
import { selectedSegment } from "./late-fetch.js";
import { isPromise } from "./pipeline.js";
import { MemoryOutputs } from "./memory-output.js";
import type { ChunkReads } from "./scan.js";
import type { Pool } from "./spill-pool.js";
import { TableScan } from "./table-scan.js";

/** The one-row read's byte target (as the one-row scan it replaces). */
const POINT_TARGET_BYTES = 16384;

export interface PointFetcherOptions {
  /** Catalog column ids to read, in output order, and their types. */
  readonly columns: readonly number[];
  readonly types: readonly PhysicalType[];
  readonly pool: Pool;
  readonly grant: Grant;
  /** The statement's signal: every read and the memo stop with it. */
  readonly signal: AbortSignal;
  readonly reads: ChunkReads;
  readonly context: CompileContext;
  /** What the memo holds is charged here (default: the pool's own charge on the grant). */
  readonly memoBudget?: ByteBudget;
}

export class PointFetcher {
  readonly memo: ReaderMemo;
  readonly #vectors: Vector[];
  /** Fetches read with the memory contract, and those the memo's budget refused. */
  memoryFetches = 0;
  declines = 0;
  constructor(readonly options: PointFetcherOptions) {
    if (options.columns.length !== options.types.length)
      throw new RangeError("One type per fetched column");
    this.memo = new ReaderMemo(options.memoBudget ?? options.pool.charge, options.signal);
    this.#vectors = new Array<Vector>(options.columns.length);
  }

  /** Appends the row `locator` names in `table` to `into` (one row). */
  async fetch(
    table: TableSnapshot,
    pages: SpaceReader,
    locator: number,
    into: ColumnStore,
  ): Promise<void> {
    this.options.signal.throwIfAborted();
    const source = table.segments[locatorSegment(locator)];
    const position = locatorPosition(locator);
    if (!source || position >= source.rowCount)
      throw new RangeError("Conflict locator is outside statement snapshot");
    if (source.kind !== "memory") return this.#scan(table, source, position, pages, into);
    this.memoryFetches += 1;
    const chunk = Math.floor(position / MEMORY_CHUNK_ROWS),
      row = position % MEMORY_CHUNK_ROWS;
    const plain: MemoryReadContext = {
      pages,
      budget: this.options.pool,
      signal: this.options.signal,
    };
    const outputs = new MemoryOutputs(this.options.types, this.options.pool);
    try {
      let vectors: readonly Vector[];
      if (this.memo.closed) vectors = await this.#read(source, chunk, row, plain, outputs);
      else
        try {
          vectors = await this.#read(source, chunk, row, { ...plain, readers: this.memo }, outputs);
        } catch (error) {
          if (!(error instanceof MemoDeclinedError)) throw error;
          // The memo is optional: give back what it holds and read as an ordinary read.
          this.declines += 1;
          this.memo.close();
          vectors = await this.#read(source, chunk, row, plain, outputs);
        }
      await this.#append(vectors, null, 0, into);
    } finally {
      outputs.close();
    }
  }

  async #read(
    source: MemorySegment,
    chunk: number,
    row: number,
    context: MemoryReadContext,
    outputs: MemoryOutputs,
  ): Promise<readonly Vector[]> {
    const ids = this.options.columns;
    const pending = source.readHint(
      { columns: ids, chunk, startRow: row, maxRows: 1, targetBytes: POINT_TARGET_BYTES },
      context,
    );
    const hint = isPromise(pending) ? await pending : pending;
    if (hint.rowCount !== 1) throw new RangeError("Conflict row disappeared");
    const out = outputs.prepare(hint.outputs);
    const vectors = this.#vectors;
    for (let c = 0; c < ids.length; c++) {
      const target = out[c];
      if (target === undefined) throw new RangeError("Missing point output");
      const read = source.chunk(ids[c] ?? -1, chunk, target, row, { hint, context });
      const result = isPromise(read) ? await read : read;
      if (result.firstRow !== row || result.rowCount !== 1)
        throw new RangeError("Conflict row disappeared");
      vectors[c] = result.vector;
    }
    return vectors;
  }

  async #append(
    vectors: readonly Vector[],
    rows: Uint16Array | null,
    row: number,
    into: ColumnStore,
  ): Promise<void> {
    const { pool, grant, signal } = this.options;
    await pool.need(2 * into.appendNeed(vectors, row) + 65536, grant, signal);
    if (into.append(vectors, rows, 0, 1) !== 1)
      throw new RangeError("Conflict row copy was not funded");
  }

  /** A group segment's row: the one-row scan. */
  async #scan(
    original: TableSnapshot,
    source: Segment,
    position: number,
    pages: SpaceReader,
    into: ColumnStore,
  ): Promise<void> {
    const { pool, grant, signal } = this.options;
    const scan = new TableScan({
      table: {
        tableId: original.tableId,
        schemaVersion: original.schemaVersion,
        columns: original.columns,
        liveRowCount: 1,
        segments: [selectedSegment(source, 0, new Uint16Array([position]))],
      },
      columns: this.options.columns,
      filter: null,
      emitLocator: false,
      context: { ...this.options.context, input: this.options.types },
      budget: pool,
      pages,
      reads: this.options.reads,
      boundedMemory: true,
      targetBytes: POINT_TARGET_BYTES,
      rowsNeeded: () => 1,
      loans: {
        borrow: async (bytes) => {
          await pool.need(bytes, grant, signal);
          return { bytes, repay: () => undefined };
        },
      },
    });
    try {
      const item = await scan.next();
      if (!item || (item.batch.selection?.count ?? item.batch.length) !== 1)
        throw new RangeError("Conflict row disappeared");
      try {
        const row = item.batch.selection?.rows[0] ?? 0;
        await this.#append(item.batch.columns, item.batch.selection?.rows ?? null, row, into);
      } finally {
        item.transfer?.release();
      }
    } finally {
      scan.abort();
    }
  }

  /** Gives back everything the memo holds; later fetches read without it. Idempotent. */
  close(): void {
    this.memo.close();
  }
}
