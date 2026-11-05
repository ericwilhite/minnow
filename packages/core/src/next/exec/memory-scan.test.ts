/**
 * The memory-segment scan (exec/memory-scan.ts) under the pipeline driver: at most one borrowed
 * chunk per segment (the double poisons a chunk's vectors as soon as another chunk is asked for),
 * deletions as the selection, and re-pointed segments whose `chunk()` is async.
 */
import { describe, expect, it } from "vitest";
import {
  MEMORY_CHUNK_ROWS,
  type ChunkOutput,
  type ChunkResult,
  type DeletionVector,
  type MemorySegment,
} from "../access/snapshot.js";
import type { PhysicalType } from "../types.js";
import { decodeUtf8, VarlenBuilder, type ByteCharge } from "../vector/varlen.js";
import {
  allocateFlat,
  type Batch,
  type FlatVector,
  type Selection,
  type Vector,
} from "../vector/vector.js";
import { UnsupportedPlanError } from "./errors.js";
import { MemorySegmentScan } from "./memory-scan.js";
import { drive, type Operator, type PushResult } from "./pipeline.js";

const INT = { kind: "int32", bits: 32 } as const;
const TEXT: PhysicalType = { kind: "text" };
const FROZEN = { now: () => 0 };

/** Deletes every position divisible by `every`. */
function deleting(every: number): DeletionVector {
  return {
    deletedCount: 0,
    isDeleted: (position) => position % every === 0,
    liveRows(from, length, input, out: Selection) {
      out.count = 0;
      for (let k = 0; k < length; k++)
        if ((input === null || input.rows.includes(k)) && (from + k) % every !== 0)
          out.rows[out.count++] = k;
    },
  };
}

/**
 * A memory segment of `rows` rows whose column c holds `c × 1,000,000 + position`. It lends one
 * chunk at a time: asking for another chunk index poisons (fills with −1) every vector it lent
 * for the previous one. `repointed`: chunks decode into `out` behind a Promise instead.
 */
class LendingSegment implements MemorySegment {
  readonly kind = "memory";
  readonly liveRowCount: number;
  lentChunk = -1;
  readonly #lent: FlatVector[] = [];
  calls = 0;
  constructor(
    readonly index: number,
    readonly rowCount: number,
    readonly repointed = false,
    readonly removed: DeletionVector | null = null,
  ) {
    this.liveRowCount = rowCount;
  }
  deletions(): DeletionVector | null {
    return this.removed;
  }
  statistics() {
    return null;
  }
  pages() {
    return [];
  }
  chunk(
    columnId: number,
    chunk: number,
    out: ChunkOutput,
    startRow = 0,
  ): ChunkResult | Promise<ChunkResult> {
    this.calls += 1;
    if (chunk !== this.lentChunk) {
      for (const vector of this.#lent)
        if (vector.data.layout === "i32") vector.data.values.fill(-1);
      this.#lent.length = 0;
      this.lentChunk = chunk;
    }
    const from = chunk * MEMORY_CHUNK_ROWS;
    const length = Math.min(MEMORY_CHUNK_ROWS, this.rowCount - from);
    if (this.repointed) {
      // Decodes rows startRow … into out (slot k = chunk row startRow + k).
      const target = out.flat;
      if (target.data.layout !== "i32") throw new TypeError("int32");
      for (let row = startRow; row < length; row++)
        target.data.values[row - startRow] = columnId * 1_000_000 + from + row;
      target.length = length - startRow;
      return Promise.resolve(whole(target, startRow, length - startRow));
    }
    const target = allocateFlat(INT, length);
    if (target.data.layout !== "i32") throw new TypeError("int32");
    for (let k = 0; k < length; k++) target.data.values[k] = columnId * 1_000_000 + from + k;
    target.length = length;
    this.#lent.push(target);
    return whole(target, 0, length);
  }
}

function whole(vector: Vector, firstRow: number, rowCount: number): ChunkResult {
  return { vector, firstRow, rowCount, complete: true, reason: null };
}

/**
 * A re-pointed segment of `rowCount` rows with an int32 column (id 1: the position) and a text
 * column (id 2: `"v" + position` padded to `width` bytes), whose text decode stops when the caller's
 * varlen builder is full. Records every `chunk()` call as `[columnId, chunk, startRow]`.
 */
class PartialTextSegment implements MemorySegment {
  readonly kind = "memory";
  readonly liveRowCount: number;
  readonly calls: Array<[number, number, number]> = [];
  constructor(
    readonly index: number,
    readonly rowCount: number,
    readonly removed: DeletionVector | null = null,
    readonly width = 8,
  ) {
    this.liveRowCount = rowCount;
  }
  deletions(): DeletionVector | null {
    return this.removed;
  }
  statistics() {
    return null;
  }
  pages() {
    return [];
  }
  chunk(columnId: number, chunk: number, out: ChunkOutput, startRow = 0): Promise<ChunkResult> {
    this.calls.push([columnId, chunk, startRow]);
    const from = chunk * MEMORY_CHUNK_ROWS;
    const length = Math.min(MEMORY_CHUNK_ROWS, this.rowCount - from);
    if (!(startRow >= 0 && startRow < length)) throw new RangeError("startRow out of range");
    if (columnId === 1) {
      const target = out.flat;
      if (target.data.layout !== "i32") throw new TypeError("int32");
      for (let row = startRow; row < length; row++) target.data.values[row - startRow] = from + row;
      target.length = length - startRow;
      return Promise.resolve(whole(target, startRow, length - startRow));
    }
    const builder = out.varlen;
    if (builder === null) throw new TypeError("text needs a varlen builder");
    let row = startRow;
    let reason: "full" | "denied" | null = null;
    for (; row < length; row++) {
      const appended = builder.appendString(text(from + row, this.width));
      if (appended !== "ok") {
        reason = appended;
        break;
      }
    }
    const rowCount = row - startRow;
    const vector: FlatVector = {
      form: "flat",
      type: TEXT,
      length: rowCount,
      validity: null,
      data: builder.data(),
    };
    return Promise.resolve({
      vector,
      firstRow: startRow,
      rowCount,
      complete: reason === null,
      reason,
    });
  }
}

const text = (position: number, width = 8) => `v${String(position).padStart(width - 1, "0")}`;

/** Copies every active value as it arrives (and checks it was not poisoned). */
class Copying implements Operator {
  readonly rows: Array<Array<number | string>> = [];
  readonly batches: number[] = [];
  push(batch: Batch): PushResult {
    const active = batch.selection;
    const count = active === null ? batch.length : active.count;
    this.batches.push(count);
    for (let k = 0; k < count; k++) {
      const row = active === null ? k : (active.rows[k] ?? 0);
      this.rows.push(
        batch.columns.map((column) =>
          column.form !== "flat"
            ? 0
            : column.data.layout === "i32"
              ? (column.data.values[row] ?? 0)
              : column.data.layout === "varlen"
                ? decodeUtf8(column.data, row)
                : 0,
        ),
      );
    }
    return "ready";
  }
  ready(): Promise<void> {
    return Promise.resolve();
  }
  finish(): void {
    // nothing buffered
  }
  abort(): void {
    // nothing held
  }
}

function outputs(count: number): ChunkOutput[] {
  return Array.from({ length: count }, () => ({
    flat: allocateFlat(INT, MEMORY_CHUNK_ROWS),
    codes: new Int32Array(MEMORY_CHUNK_ROWS),
    varlen: null,
  }));
}

function expected(
  rows: number,
  columns: readonly number[],
  keep: (position: number) => boolean = () => true,
) {
  const out: number[][] = [];
  for (let position = 0; position < rows; position++)
    if (keep(position)) out.push(columns.map((column) => column * 1_000_000 + position));
  return out;
}

describe("MemorySegmentScan", () => {
  it("reads resident chunks synchronously, one borrowed chunk per segment", () => {
    const segments = [new LendingSegment(0, 5000), new LendingSegment(1, 100)];
    const sink = new Copying();
    // A frozen clock: no yield is due, so only a real Promise could make the run async.
    const run = drive(new MemorySegmentScan(segments, [1, 2], outputs(2)), sink, undefined, FROZEN);
    expect(run).toBeUndefined(); // no await anywhere
    expect(sink.rows).toEqual([...expected(5000, [1, 2]), ...expected(100, [1, 2])]);
    expect(sink.rows.flat().includes(-1)).toBe(false); // never read after the lender took it back
    expect(segments[0]?.calls).toBe(3 * 2);
  });

  it("applies deletions as the batch selection", () => {
    const segment = new LendingSegment(0, 3000, false, deleting(3));
    const sink = new Copying();
    expect(
      drive(new MemorySegmentScan([segment], [4], outputs(1)), sink, undefined, FROZEN),
    ).toBeUndefined();
    expect(sink.rows).toEqual(expected(3000, [4], (position) => position % 3 !== 0));
  });

  it("handles re-pointed segments whose chunk() is async", async () => {
    const segments = [new LendingSegment(0, 2100), new LendingSegment(1, 3000, true)];
    const sink = new Copying();
    const run = drive(new MemorySegmentScan(segments, [1, 7], outputs(2)), sink);
    expect(run).toBeInstanceOf(Promise);
    await run;
    expect(sink.rows).toEqual([...expected(2100, [1, 7]), ...expected(3000, [1, 7])]);
  });

  it("resumes a partial re-pointed chunk at its resume row, within the same chunk index", async () => {
    // 3,000 rows (two chunks), every 5th deleted; 8-byte values under a 64-byte batch target, so
    // each text decode stops (`full`) after 8 rows and the scan resumes at the next row.
    const segment = new PartialTextSegment(0, 3000, deleting(5));
    const allowance: ByteCharge = { allocate: () => true, free: () => undefined };
    const out: ChunkOutput[] = [
      {
        flat: allocateFlat(INT, MEMORY_CHUNK_ROWS),
        codes: new Int32Array(MEMORY_CHUNK_ROWS),
        varlen: null,
      },
      {
        flat: allocateFlat(TEXT, MEMORY_CHUNK_ROWS),
        codes: new Int32Array(MEMORY_CHUNK_ROWS),
        varlen: new VarlenBuilder(MEMORY_CHUNK_ROWS, allowance, 64, 64),
      },
    ];
    const sink = new Copying();
    await drive(new MemorySegmentScan([segment], [1, 2], out), sink);
    const want: Array<[number, string]> = [];
    for (let position = 0; position < 3000; position++)
      if (position % 5 !== 0) want.push([position, text(position)]);
    expect(sink.rows).toEqual(want);
    // Every batch is one 8-row part (the last part of a chunk may be shorter), deletions applied.
    expect(Math.max(...sink.batches)).toBeLessThanOrEqual(8);
    // Each part reads both columns from the same start row; chunk indices never go back.
    const starts = segment.calls
      .filter(([column]) => column === 2)
      .map(([, chunk, start]) => [chunk, start]);
    expect(starts).toHaveLength(Math.ceil(2048 / 8) + Math.ceil(952 / 8));
    expect(starts.slice(0, 3)).toEqual([
      [0, 0],
      [0, 8],
      [0, 16],
    ]);
    expect(starts.at(256)).toEqual([1, 0]);
    expect(
      segment.calls.filter(([column]) => column === 1).map(([, chunk, start]) => [chunk, start]),
    ).toEqual(starts);
    const chunks = segment.calls.map(([, chunk]) => chunk);
    expect(chunks).toEqual([...chunks].sort((a, b) => a - b));
  });

  it("refuses a denied part with no row (no progress loan to borrow)", async () => {
    // 32-byte values: the first one already needs more than the builder's 16-byte minimum arena.
    const segment = new PartialTextSegment(0, 10, null, 32);
    // The grant admits the builder's minimum, then refuses every growth.
    let admitting = true;
    const refusing: ByteCharge = { allocate: () => admitting, free: () => undefined };
    const varlen = new VarlenBuilder(MEMORY_CHUNK_ROWS, refusing, 64, 4);
    admitting = false;
    const out: ChunkOutput[] = [
      {
        flat: allocateFlat(INT, MEMORY_CHUNK_ROWS),
        codes: new Int32Array(MEMORY_CHUNK_ROWS),
        varlen: null,
      },
      {
        flat: allocateFlat(TEXT, MEMORY_CHUNK_ROWS),
        codes: new Int32Array(MEMORY_CHUNK_ROWS),
        varlen,
      },
    ];
    await expect(
      drive(new MemorySegmentScan([segment], [1, 2], out), new Copying()),
    ).rejects.toThrow(UnsupportedPlanError);
  });
});
