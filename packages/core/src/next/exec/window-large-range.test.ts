/** RANGE and GROUPS frames over one huge window partition under a 64 MiB grant: one of the three frame
 * classes of window-large.test-support.ts (ruling on review B7-p2d), in a file of its own so
 * parallel runners can run them side by side. */
import { describe, expect, it } from "vitest";
import { Check, ROWS, asc, run } from "./window-large.test-support.js";
import type { PhysicalType } from "../types.js";
import { BIG, INT } from "./join-fixtures.js";
import { type WindowSpec } from "./window.js";

describe(`one ${String(ROWS)}-row partition under a 64 MiB grant`, () => {
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
});
