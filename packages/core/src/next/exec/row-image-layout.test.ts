/** Fixed image minima fund the actual scalar arena, while payload copying shares one root step. */
import { expect, it } from "vitest";
import { OutputArena } from "./kernels/vectors.js";
import { SORT_ROW_VIEW_BYTES } from "./sort-row.js";
import {
  rowImageMinimumBytes,
  rowImageProgressBytes,
  ROW_IMAGE_FIXED_BYTES,
  ROW_IMAGE_HELPER_SLOTS,
} from "./row-image-layout.js";
import type { PhysicalType } from "../types.js";
it("funds two complete scalar image layouts without a nested helper", () => {
  const types: PhysicalType[] = [
    { kind: "int32", bits: 32 },
    { kind: "text" },
    { kind: "bytes" },
    { kind: "int64" },
    { kind: "decimal", precision: 30, scale: 4 },
    { kind: "timestamp", zoned: false },
  ];
  const minimum = rowImageMinimumBytes(types);
  let held = 0;
  const arena = new OutputArena(
    {
      allocate: (n) => {
        if (held + n > minimum) return false;
        held += n;
        return true;
      },
      free: (n) => {
        held -= n;
      },
    },
    1,
  );
  try {
    arena.take(ROW_IMAGE_FIXED_BYTES + 2 * types.length * SORT_ROW_VIEW_BYTES);
    for (let side = 0; side < 2; side++) for (const type of types) arena.flat(type);
    expect(held).toBeLessThanOrEqual(minimum);
    expect(ROW_IMAGE_HELPER_SLOTS).toBe(1);
  } finally {
    arena.close();
  }
  expect(held).toBe(0);
});
it("reserves exact payload and unique binding copy room separately from fragment I/O", () => {
  expect(rowImageProgressBytes(600000, 200000)).toBe(800000);
  expect(rowImageProgressBytes(600000)).toBe(1200000);
  for (const bad of [-1, 0.5, Infinity, NaN])
    expect(() => rowImageProgressBytes(bad)).toThrow(RangeError);
  expect(() => rowImageProgressBytes(Number.MAX_SAFE_INTEGER, 1)).toThrow(RangeError);
});
