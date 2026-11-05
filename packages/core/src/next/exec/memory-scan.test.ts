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
import { allocateFlat, type Batch, type FlatVector, type Selection } from "../vector/vector.js";
import { MemorySegmentScan } from "./memory-scan.js";
import { drive, type Operator, type PushResult } from "./pipeline.js";

const INT = { kind: "int32", bits: 32 } as const;
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
  chunk(columnId: number, chunk: number, out: ChunkOutput): ChunkResult | Promise<ChunkResult> {
    this.calls += 1;
    if (chunk !== this.lentChunk) {
      for (const vector of this.#lent)
        if (vector.data.layout === "i32") vector.data.values.fill(-1);
      this.#lent.length = 0;
      this.lentChunk = chunk;
    }
    const from = chunk * MEMORY_CHUNK_ROWS;
    const length = Math.min(MEMORY_CHUNK_ROWS, this.rowCount - from);
    const target = this.repointed ? out.flat : allocateFlat(INT, length);
    if (target.data.layout !== "i32") throw new TypeError("int32");
    for (let k = 0; k < length; k++) target.data.values[k] = columnId * 1_000_000 + from + k;
    target.length = length;
    if (this.repointed) return Promise.resolve(target);
    this.#lent.push(target);
    return target;
  }
}

/** Copies every active value as it arrives (and checks it was not poisoned). */
class Copying implements Operator {
  readonly rows: number[][] = [];
  push(batch: Batch): PushResult {
    const active = batch.selection;
    const count = active === null ? batch.length : active.count;
    for (let k = 0; k < count; k++) {
      const row = active === null ? k : (active.rows[k] ?? 0);
      this.rows.push(
        batch.columns.map((column) =>
          column.form === "flat" && column.data.layout === "i32"
            ? (column.data.values[row] ?? 0)
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
});
