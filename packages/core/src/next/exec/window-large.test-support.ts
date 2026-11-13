/**
 * One huge window partition under a 64 MiB grant, for each frame class (ruling on review
 * B7-p2d): ROWS frames and running aggregates; RANGE / GROUPS frames (peers plus the frame
 * extent); whole-partition frames (two passes, the count first). The partition streams
 * (window-stream.ts): memory is bounded by the frame, not by the partition's 10 M rows. A
 * checking sink compares every row with values computed on the fly, so the test holds no rows.
 *
 * Shared by window-large-rows, -range and -whole.test.ts, one frame class per file so parallel
 * runners can run them side by side; each case is the one the single file ran.
 */
import { expect } from "vitest";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { Batch } from "../vector/vector.js";
import { rowsOf } from "./join-fixtures.js";
import type { Operator, PushResult } from "./pipeline.js";
import { batchesOf } from "./join-fixtures.js";
import type { SortColumn } from "./sort-keys.js";
import { WindowOperator, windowMinimumBytes, type WindowSpec } from "./window.js";

export const ROWS = Number(process.env.WINDOW_LARGE_ROWS ?? 10_000_000);
export const GRANT = 64 << 20;
export const asc = (column: number): SortColumn => ({
  column,
  direction: "asc",
  nulls: "last",
  collation: null,
});

/** A sink that checks each row as it arrives (in order) and keeps nothing. */
export class Check implements Operator {
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
export async function run(spec: WindowSpec, row: (i: number) => unknown[], sink: Check) {
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

export const x = (i: number) => (i % 1000) / 8;
