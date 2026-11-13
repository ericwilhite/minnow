/** whole-partition frames over one huge window partition under a 64 MiB grant: one of the three frame
 * classes of window-large.test-support.ts (ruling on review B7-p2d), in a file of its own so
 * parallel runners can run them side by side. */
import { describe, expect, it } from "vitest";
import { Check, ROWS, asc, run } from "./window-large.test-support.js";
import type { PhysicalType } from "../types.js";
import { BIG, DBL, INT } from "./join-fixtures.js";
import { type WindowSpec } from "./window.js";

describe(`one ${String(ROWS)}-row partition under a 64 MiB grant`, () => {
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
