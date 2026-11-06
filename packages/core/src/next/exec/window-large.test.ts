/**
 * One huge window partition under a 64 MiB grant, for each frame class (ruling on review
 * B7-p2d): ROWS frames and running aggregates; RANGE / GROUPS frames (peers plus the frame
 * extent); whole-partition frames (two passes, the count first). The partition streams
 * (window-stream.ts): memory is bounded by the frame, not by the partition's 10 M rows. A
 * checking sink compares every row with values computed on the fly, so the test holds no rows.
 */
import { describe, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import type { Batch } from "../vector/vector.js";
import { BIG, DBL, INT, rowsOf } from "./join-fixtures.js";
import type { Operator, PushResult } from "./pipeline.js";
import { batchesOf } from "./join-fixtures.js";
import type { SortColumn } from "./sort-keys.js";
import { WindowOperator, windowMinimumBytes, type WindowSpec } from "./window.js";

const ROWS = Number(process.env.WINDOW_LARGE_ROWS ?? 10_000_000);
const GRANT = 64 << 20;
const asc = (column: number): SortColumn => ({
  column,
  direction: "asc",
  nulls: "last",
  collation: null,
});

/** A sink that checks each row as it arrives (in order) and keeps nothing. */
class Check implements Operator {
  rows = 0;
  finished = false;
  constructor(readonly check: (row: readonly unknown[], index: number) => void) {}
  push(batch: Batch): PushResult {
    for (const row of rowsOf(batch)) this.check(row, this.rows++);
    return "ready";
  }
  ready(): Promise<void> {
    return Promise.resolve();
  }
  finish(): void {
    this.finished = true;
  }
  abort(): void {
    this.finished = false;
  }
}

/** Feeds `ROWS` rows built by `row(i)` in batches (generated lazily: no 10 M-row array). */
async function run(spec: WindowSpec, row: (i: number) => unknown[], sink: Check) {
  const minimum = windowMinimumBytes(spec.types, spec.functions.length);
  const governor = new Governor(GRANT);
  const spill = new MemorySpillService();
  const grant = await governor.admit({ label: "window", minimumBytes: minimum, spillable: true });
  const op = new WindowOperator(spec, { grant, spill, queryId: 9 }, sink);
  // Rows arrive shuffled in blocks, so the sort really sorts (and spills).
  const block = 2048;
  const blocks = Math.ceil(ROWS / block);
  for (let b = 0; b < blocks; b++) {
    const at = ((b * 7919) % blocks) * block;
    const rows = [];
    for (let i = at; i < Math.min(ROWS, at + block); i++) rows.push(row(i));
    for (const batch of batchesOf(spec.types, rows as never))
      if (op.push(batch) === "blocked") await op.ready();
  }
  await op.finish();
  await op.closed;
  expect(sink.finished).toBe(true);
  expect(sink.rows).toBe(ROWS);
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
  return op.profile;
}

const x = (i: number) => (i % 1000) / 8;

describe(`one ${String(ROWS)}-row partition under a 64 MiB grant`, () => {
  it("ROWS frames and running aggregates: row_number, running sum, moving average, lag, lead", async () => {
    const types: PhysicalType[] = [INT, INT, DBL];
    const spec: WindowSpec = {
      types,
      partitionBy: [0],
      orderBy: [asc(1)],
      functions: [
        { fn: "row_number", arg: null, frame: null, type: BIG },
        {
          fn: "sum",
          arg: 2,
          frame: {
            unit: "rows",
            start: { kind: "unbounded-preceding", offset: null },
            end: { kind: "current-row", offset: null },
            exclude: "no-others",
          },
          type: DBL,
        },
        {
          fn: "avg",
          arg: 2,
          frame: {
            unit: "rows",
            start: { kind: "preceding", offset: 9 },
            end: { kind: "current-row", offset: null },
            exclude: "no-others",
          },
          type: DBL,
        },
        { fn: "lag", arg: 1, frame: null, type: INT },
        { fn: "lead", arg: 1, frame: null, type: INT },
      ],
    };
    let running = 0;
    const profile = await run(
      spec,
      (i) => [0, i, x(i)],
      new Check((row, i) => {
        running += x(i);
        let moving = 0;
        for (let j = Math.max(0, i - 9); j <= i; j++) moving += x(j);
        const width = Math.min(10, i + 1);
        if (
          row[1] !== i ||
          row[3] !== BigInt(i + 1) ||
          row[4] !== running ||
          row[5] !== moving / width ||
          row[6] !== (i > 0 ? i - 1 : null) ||
          row[7] !== (i + 1 < ROWS ? i + 1 : null)
        )
          throw new Error(`row ${String(i)}: ${JSON.stringify(row.map(String))}`);
      }),
    );
    expect(profile.streamedPartitions).toBe(1);
    expect(profile.sort.runs).toBeGreaterThan(0);
  }, 1_800_000);

  it("RANGE and GROUPS frames: rank, a RANGE ±2 count, a GROUPS ±1 sum over peers of 4", async () => {
    const types: PhysicalType[] = [INT, INT, INT];
    const g = (i: number) => Math.floor(i / 4);
    const spec: WindowSpec = {
      types,
      partitionBy: [0],
      orderBy: [asc(1)],
      functions: [
        { fn: "rank", arg: null, frame: null, type: BIG },
        {
          fn: "count_star",
          arg: null,
          frame: {
            unit: "range",
            start: { kind: "preceding", offset: 2 },
            end: { kind: "following", offset: 2 },
            exclude: "no-others",
          },
          type: BIG,
        },
        {
          fn: "sum",
          arg: 2,
          frame: {
            unit: "groups",
            start: { kind: "preceding", offset: 1 },
            end: { kind: "following", offset: 1 },
            exclude: "no-others",
          },
          type: BIG,
        },
      ],
    };
    const groups = Math.ceil(ROWS / 4);
    const rowsIn = (a: number, b: number) =>
      Math.max(0, Math.min(ROWS - 1, 4 * b + 3) - Math.max(0, 4 * a) + 1);
    const sumIn = (a: number, b: number) => {
      const lo = Math.max(0, 4 * a);
      const hi = Math.min(ROWS - 1, 4 * b + 3);
      return hi < lo ? 0 : ((lo + hi) * (hi - lo + 1)) / 2;
    };
    const profile = await run(
      spec,
      (i) => [0, g(i), i],
      new Check((row, i) => {
        const k = g(i);
        const range = rowsIn(Math.max(0, k - 2), Math.min(groups - 1, k + 2));
        const sum = sumIn(Math.max(0, k - 1), Math.min(groups - 1, k + 1));
        if (
          row[1] !== k ||
          row[3] !== BigInt(4 * k + 1) ||
          row[4] !== BigInt(range) ||
          row[5] !== BigInt(sum)
        )
          throw new Error(`row ${String(i)}: ${JSON.stringify(row.map(String))}`);
      }),
    );
    expect(profile.streamedPartitions).toBe(1);
  }, 1_800_000);

  it("whole-partition frames: ntile, percent_rank, cume_dist, a whole count, a suffix sum", async () => {
    const types: PhysicalType[] = [INT, INT];
    const spec: WindowSpec = {
      types,
      partitionBy: [0],
      orderBy: [asc(1)],
      functions: [
        { fn: "ntile", arg: null, amount: 4, frame: null, type: INT },
        { fn: "percent_rank", arg: null, frame: null, type: DBL },
        { fn: "cume_dist", arg: null, frame: null, type: DBL },
        {
          fn: "count_star",
          arg: null,
          frame: {
            unit: "rows",
            start: { kind: "unbounded-preceding", offset: null },
            end: { kind: "unbounded-following", offset: null },
            exclude: "no-others",
          },
          type: BIG,
        },
        {
          fn: "sum",
          arg: 1,
          frame: {
            unit: "rows",
            start: { kind: "current-row", offset: null },
            end: { kind: "unbounded-following", offset: null },
            exclude: "no-others",
          },
          type: BIG,
        },
      ],
    };
    const n = ROWS;
    const size = Math.floor(n / 4);
    const larger = n % 4;
    const boundary = larger * (size + 1);
    const total = ((n - 1) * n) / 2;
    const profile = await run(
      spec,
      (i) => [0, i],
      new Check((row, i) => {
        const ntile =
          i < boundary
            ? Math.floor(i / (size + 1)) + 1
            : larger + Math.floor((i - boundary) / Math.max(size, 1)) + 1;
        const suffix = total - ((i - 1) * i) / 2;
        if (
          row[1] !== i ||
          row[2] !== ntile ||
          row[3] !== (n <= 1 ? 0 : i / (n - 1)) ||
          row[4] !== (i + 1) / n ||
          row[5] !== BigInt(n) ||
          row[6] !== BigInt(suffix)
        )
          throw new Error(`row ${String(i)}: ${JSON.stringify(row.map(String))}`);
      }),
    );
    expect(profile.streamedPartitions).toBe(1);
  }, 1_800_000);
});
