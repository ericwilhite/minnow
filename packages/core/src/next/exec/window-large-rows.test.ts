/** ROWS frames and running aggregates over one huge window partition under a 64 MiB grant: one of the three frame
 * classes of window-large.test-support.ts (ruling on review B7-p2d), in a file of its own so
 * parallel runners can run them side by side. */
import { describe, expect, it } from "vitest";
import { Check, ROWS, asc, run, x } from "./window-large.test-support.js";
import type { PhysicalType } from "../types.js";
import { BIG, DBL, INT } from "./join-fixtures.js";
import { type WindowSpec } from "./window.js";

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
});
