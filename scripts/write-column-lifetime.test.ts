/** Borrowed input lifetime, independently observed while copied chunks remain strongly live. */
import { execFileSync } from "node:child_process";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, WRITE_COLUMN_BORROW_STATES } from "../packages/core/src/testing/reach.js";
import type { LifetimeCase } from "./write-column-lifetime-probe.mts";

const reach = createReach("memtable source borrow lifetime", WRITE_COLUMN_BORROW_STATES);
for (const seed of seedsFor("next-write-column-borrow", [0xb3d1]))
  it(`releases copied source buffers on every exit (seed ${String(seed)})`, async () => {
    const cases: LifetimeCase[] = (
      ["bool", "i32", "f32", "f64", "int64", "decimal", "varlen", "fixed16", "interval"] as const
    ).flatMap((family) =>
      (["flat", "constant", "dictionary"] as const).flatMap((form) =>
        (["copied", "full", "denied"] as const).map((outcome) => ({
          family,
          form,
          outcome,
          rows: 1,
          selected: false,
          nullable: false,
          offset: 0,
        })),
      ),
    );
    await fc.assert(
      fc.asyncProperty(
        fc.shuffledSubarray(cases, { minLength: cases.length, maxLength: cases.length }),
        fc.constantFrom(1, 7, 2048),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 1000 }),
        async (plan, rows, selected, nullable, offset) => {
          const histories = plan.map((s) => ({ ...s, rows, selected, nullable, offset }));
          const result = JSON.parse(
            execFileSync(
              process.execPath,
              ["--expose-gc", "scripts/write-column-lifetime-probe.mts", JSON.stringify(histories)],
              {
                encoding: "utf8",
                timeout: 60000,
                maxBuffer: 4 * 1024 * 1024,
              },
            ),
          ) as { cases: number; retainedInputBytes: number };
          expect(result.cases).toBe(81);
          expect(result.retainedInputBytes).toBe(0);
          for (const s of histories) {
            reach.beginHistory();
            reach.reach(s.family);
            reach.reach(s.form);
            reach.reach(s.outcome);
            reach.reach(s.selected ? "selected rows" : "dense rows");
            reach.reach(s.nullable ? "nullable source" : "all valid source");
            reach.reach(
              s.rows === 1 ? "single row" : s.rows === 2048 ? "full chunk" : "partial chunk",
            );
            if (s.outcome === "denied" && s.form !== "constant" && s.rows > 1)
              reach.reach("failed after copied prefix");
          }
        },
      ),
      { seed, numRuns: 12, endOnFailure: true },
    );
    reach.assertReached();
  }, 120_000);
