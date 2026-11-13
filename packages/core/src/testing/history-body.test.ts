import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "./seeds.js";
import { historyBody } from "./history-body.test-support.js";

for (const seed of seedsFor("history-body-routing", [0xb52a])) {
  it(`executes the original examples and seeded stream once across bodies (seed ${String(seed)})`, async () => {
    const number = fc.integer({ min: -1000, max: 1000 });
    const flag = fc.boolean();
    const cases: Array<{ numRuns: number; examples: Array<[number, boolean]> }> = [
      {
        numRuns: 12,
        examples: [
          [999, true],
          [-999, false],
        ],
      },
      { numRuns: 16, examples: [] },
      { numRuns: 24, examples: Array.from({ length: 12 }, (_, i) => [i * 17, i % 2 === 0]) },
      {
        numRuns: 30,
        examples: [
          [999, true],
          [-999, false],
        ],
      },
    ];
    for (const { numRuns, examples } of cases) {
      const original: Array<[number, boolean]> = [];
      await fc.assert(
        fc.asyncProperty(number, flag, async (n, b) => {
          original.push([n, b]);
        }),
        { seed, numRuns, examples },
      );
      expect(original).toHaveLength(numRuns);
      expect(original.slice(0, examples.length)).toEqual(examples);
      const routed: Array<[number, boolean]> = [];
      for (let ordinal = 0; ordinal < numRuns; ordinal++) {
        const enter = historyBody(ordinal);
        const body: Array<[number, boolean]> = [];
        await fc.assert(
          fc.asyncProperty(number, flag, async (n, b) =>
            enter(async () => {
              body.push([n, b]);
            }),
          ),
          { seed, numRuns, examples },
        );
        expect(body, "each test body executes exactly one original history").toEqual([
          original[ordinal],
        ]);
        routed.push(...body);
      }
      expect(routed, "no example or generated history is lost, repeated or changed").toEqual(
        original,
      );
    }
  });

  it(`checks shrink candidates after its selected history fails (seed ${String(seed)})`, async () => {
    const number = fc.noBias(fc.integer({ min: 0, max: 1000 }));
    const original: number[] = [];
    await fc.assert(
      fc.asyncProperty(number, async (n) => {
        original.push(n);
      }),
      { seed, numRuns: 12 },
    );
    let shrunk = false;
    for (let ordinal = 0; ordinal < 12; ordinal++) {
      const enter = historyBody(ordinal);
      const result = await fc.check(
        fc.asyncProperty(number, async (n) =>
          enter(async () => {
            if (n >= 5) throw new Error("failing value " + String(n));
          }),
        ),
        { seed, numRuns: 12 },
      );
      const first = original[ordinal];
      if (first === undefined) throw new Error("Missing original history");
      expect(result.failed).toBe(first >= 5);
      if (first >= 5) {
        expect(result.counterexample, "routing must not bypass shrinking").toEqual([5]);
        shrunk ||= result.numShrinks > 0;
      }
    }
    expect(shrunk).toBe(true);
  });
}

it("refuses invalid history ordinals", () => {
  for (const ordinal of [-1, 0.5, NaN, Infinity])
    expect(() => historyBody(ordinal)).toThrow(RangeError);
});
