import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_ORDERING_HARNESS_REACH } from "../../testing/reach.js";
import {
  controllerOrderingFactory,
  orderingDifferential,
} from "./trigger-ordering.test-support.js";

const prefixes = fc.array(fc.constantFrom("A", "Z", "a", "z", "\ue000", "\u{10000}"), {
  minLength: 2,
  maxLength: 4,
});
for (const seed of seedsFor("next-trigger-ordering-harness", [0xb7e0, 0xb7e1]))
  it(`compares generated controller programs with PG ordering seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("trigger ordering harness", TRIGGER_ORDERING_HARNESS_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 19 }),
          fc.boolean(),
          prefixes,
          prefixes,
          fc.boolean(),
          fc.boolean(),
          async (rows, update, before, after, twoBodies, reverseCreation) => {
            reach.beginHistory();
            const history = {
                rows,
                event: update ? ("update" as const) : ("delete" as const),
                beforeNames: before.map((p, i) => `${p}_b${String(i)}`),
                afterNames: after.map((p, i) => `${p}_a${String(i)}`),
                twoBodies,
                reverseCreation,
              },
              result = await orderingDifferential(pg, history, controllerOrderingFactory);
            expect(result.actual, "PGlite generated trigger ordering").toEqual(result.expected);
            reach.reach(update ? "UPDATE" : "DELETE");
            reach.reach(rows === 0 ? "empty root" : "row work");
            reach.reach(reverseCreation ? "reverse creation" : "forward creation");
            reach.reach(twoBodies ? "two body commands" : "one body command");
            if (before.length > 1) reach.reach("multiple BEFORE names");
            if ([...before, ...after].some((p) => (p.codePointAt(0) ?? 0) > 127))
              reach.reach("UTF8 names");
            if (rows * (after.length + 1) >= 64) reach.reach("spill sized parent queue");
            reach.reach("strict PG trace");
            reach.reach("adapter cleanup fenced");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [0, false, ["z", "A"], ["z", "A"], false, true],
            [19, false, ["z", "A"], ["z", "A", "\ue000", "\u{10000}"], true, true],
            [3, true, ["a", "Z", "\u{10000}"], ["z", "A"], true, false],
            [2, true, ["a", "Z"], ["\ue000", "\u{10000}", "z"], false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
