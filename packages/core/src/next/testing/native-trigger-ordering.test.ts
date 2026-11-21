import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_ORDERING_REACH } from "../../testing/reach.js";
import { orderingDifferential, orderingScript } from "./trigger-ordering.test-support.js";
import { nativeOrderingFactory } from "./native-trigger-ordering.test-support.js";

const prefixes = fc.array(fc.constantFrom("A", "Z", "a", "z", "\ue000", "\u{10000}"), {
  minLength: 2,
  maxLength: 4,
});
for (const seed of seedsFor("next-native-trigger-ordering", [0xb7e2, 0xb7e3])) {
  const reach = createReach("native trigger ordering", NATIVE_TRIGGER_ORDERING_REACH);
  const ledger = new Map<number, { signature: string; completed: number }>();
  const attempts = [0, 0];
  // Replay the SAME seed/arbitraries/examples in both bounded bodies. Row parity assigns
  // each original history to exactly one body; the final audit proves none was lost or doubled.
  // Original twelve-run/four-example configuration stays unchanged, including large histories.
  for (const parity of [0, 1] as const)
    it(`executes generated native programs with PG ordering seed ${String(seed)}, parity ${String(parity)}`, async () => {
      const pg = new PGlite();
      let ordinal = 0;
      try {
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 0, max: 19 }),
            fc.boolean(),
            prefixes,
            prefixes,
            fc.boolean(),
            fc.boolean(),
            fc.boolean(),
            async (rows, update, before, after, twoBodies, reverseCreation, published) => {
              const at = ordinal++,
                signature = JSON.stringify([
                  rows,
                  update,
                  before,
                  after,
                  twoBodies,
                  reverseCreation,
                  published,
                ]);
              let record = ledger.get(at);
              if (record)
                expect(record.signature, "same generated history in each case group").toBe(
                  signature,
                );
              else {
                record = { signature, completed: 0 };
                ledger.set(at, record);
              }
              if (rows % 2 !== parity) return;
              reach.beginHistory();
              const history = {
                  rows,
                  event: update ? ("update" as const) : ("delete" as const),
                  beforeNames: before.map((p, i) => `${p}_b${String(i)}`),
                  afterNames: after.map((p, i) => `${p}_a${String(i)}`),
                  twoBodies,
                  reverseCreation,
                  published,
                },
                result = await orderingDifferential(
                  pg,
                  history,
                  nativeOrderingFactory,
                  orderingScript(history, "accumulator"),
                );
              expect(result.actual, "PGlite generated trigger ordering").toEqual(result.expected);
              expect(result.states).toHaveLength(4);
              for (const state of result.states)
                expect(state.actual, `PGlite native rows ${state.sql}`).toEqual(state.expected);
              reach.reach("exact parent and child rows");
              reach.reach("exact nested body rows");
              reach.reach(update ? "UPDATE" : "DELETE");
              reach.reach(rows === 0 ? "empty root" : "row work");
              reach.reach(reverseCreation ? "reverse creation" : "forward creation");
              reach.reach(twoBodies ? "two body commands" : "one body command");
              if (before.length > 1) reach.reach("multiple BEFORE names");
              if ([...before, ...after].some((p) => (p.codePointAt(0) ?? 0) > 127))
                reach.reach("UTF8 names");
              if (rows * (after.length + 1) >= 64) reach.reach("spill sized parent queue");
              reach.reach(published ? "published source" : "resident source");
              reach.reach("strict PG trace");
              reach.reach("adapter cleanup fenced");
              record.completed++;
            },
          ),
          {
            seed,
            numRuns: 12,
            examples: [
              [0, false, ["z", "A"], ["z", "A"], false, true, false],
              [19, false, ["z", "A"], ["z", "A", "\ue000", "\u{10000}"], true, true, true],
              [3, true, ["a", "Z", "\u{10000}"], ["z", "A"], true, false, true],
              [2, true, ["a", "Z"], ["\ue000", "\u{10000}", "z"], false, true, false],
            ],
          },
        );
        attempts[parity] = ordinal;
      } finally {
        await pg.close();
      }
    }, 120_000);

  it(`executes every native ordering history once and meets Reach seed ${String(seed)}`, () => {
    expect(attempts[0]).toBeGreaterThan(0);
    expect(attempts[1]).toBe(attempts[0]);
    expect(ledger.size).toBe(attempts[0]);
    for (const entry of ledger.values())
      expect(entry.completed, "every original history exactly once").toBe(1);
    reach.assertReached();
  });
}
