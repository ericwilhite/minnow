/**
 * Sizing summaries are charged before they exist (write/memtable.ts: "every array is charged
 * before it is allocated"). Law: a memtable that has no row allocates nothing for summaries, even
 * when its charge refuses everything; with its first row every summary byte is inside
 * `memtable.bytes`; publication's `release` returns them, leaving nothing charged. Measured by
 * counting the bytes of every Int32Array and Float64Array constructed (the summary's types; a
 * chunk's offsets are Int32Arrays too, and those are charged as well).
 */
import { afterEach, expect, it, vi } from "vitest";
import type { ColumnDef } from "../access/catalog.js";
import { Memtable } from "./memtable.js";
import { column, INT, ints, TEXT, texts } from "./test-support.js";

afterEach(() => vi.unstubAllGlobals());

/** Counts the bytes of every Int32Array and Float64Array constructed from now on. */
function countTypedArrays(): { bytes: () => number } {
  let bytes = 0;
  for (const name of ["Int32Array", "Float64Array"] as const) {
    const Original = globalThis[name];
    vi.stubGlobal(
      name,
      new Proxy(Original, {
        construct(target, args) {
          const value = Reflect.construct(target, args) as Int32Array | Float64Array;
          bytes += value.byteLength;
          return value;
        },
      }),
    );
  }
  return { bytes: () => bytes };
}

// Stored columns as the catalog defines them (no expression dependencies on these).
const columns: ColumnDef[] = [column("id", INT, false), column("a", TEXT), column("b", TEXT)].map(
  ({ defaultDependencies, generatedDependencies, ...definition }, id) => {
    if (defaultDependencies !== undefined || generatedDependencies !== undefined)
      throw new Error("unexpected expression dependencies");
    return { ...definition, id };
  },
);

it("allocates nothing for summaries before the first row, even with a refusing charge", () => {
  const allocate = vi.fn(() => false);
  const counted = countTypedArrays();
  const memtable = new Memtable(1, 0, 1, columns, [], { allocate, free: () => undefined });
  expect(counted.bytes(), "typed-array bytes of an empty memtable").toBe(0);
  expect(allocate).not.toHaveBeenCalled();
  expect(memtable.bytes).toBe(0);
  memtable.releaseAll();
});

it("charges every summary byte with the first row and returns them at release", () => {
  let charged = 0;
  const charge = {
    allocate: (n: number) => {
      charged += n;
      return true;
    },
    free: (n: number) => {
      charged -= n;
    },
  };
  const input = [ints([1, 2]), texts(["x", null]), texts(["yy", "z"])];
  const counted = countTypedArrays();
  const memtable = new Memtable(1, 0, 1, columns, [], charge, 4_096);
  expect(counted.bytes()).toBe(0);
  memtable.append(input, null, 0, 2);
  // Everything constructed is charged: the chunks' arrays and the summaries'.
  expect(memtable.bytes).toBe(charged);
  expect(counted.bytes(), "typed-array bytes beyond the charge").toBeLessThanOrEqual(
    memtable.bytes,
  );
  // The summary itself: a column mapping and a maximum and sum per varlen column and chunk.
  const summaryBytes = columns.length * 4 + 2 * 2 * Math.ceil(4_096 / 2_048) * 8;
  expect(memtable.valueSummary(1, 0)).toEqual({ max: 1, sum: 1 });
  expect(memtable.valueSummary(2, 0)).toEqual({ max: 2, sum: 3 });
  expect(memtable.bytes).toBeGreaterThanOrEqual(summaryBytes);
  memtable.release();
  expect(memtable.valueSummary(1, 0), "released rows have no summary").toBeNull();
  expect(memtable.bytes, "nothing stays charged after release").toBe(0);
  expect(charged).toBe(0);
  memtable.releaseAll();
});
