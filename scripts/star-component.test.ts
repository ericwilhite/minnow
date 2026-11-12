/** Every component lifetime admits, executes and retires a complete native query. */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { starFixture } from "./lib/star-fixture.mts";
import { nativeStar } from "./lib/star-native.mts";
import { lightStar } from "./lib/star-light.mts";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, STAR_COMPONENT_REACH } from "../packages/core/src/testing/reach.js";

afterEach(() => vi.restoreAllMocks());

it.each(seedsFor("next-star-component", [0xb765, 0xb766]))(
  "executes and retires all native/coded lifetimes (seed %i)",
  async (seed) => {
    const reach = createReach("star component lifetimes", STAR_COMPONENT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 257 }),
        fc.integer({ min: 2, max: 5 }),
        async (n, lifetimes) => {
          reach.beginHistory();
          const root = fileURLToPath(new URL("../", import.meta.url)),
            fixture = await starFixture(root, n);
          expect(lightStar(fixture)).toEqual(fixture.expected);
          const { HashJoin } = (await import(
            pathToFileURL(join(root, "packages/core/dist/next/exec/join.js")).href
          )) as typeof import("../packages/core/dist/next/exec/join.js");
          const finish = vi.spyOn(HashJoin.prototype, "finishProbe");
          try {
            for (const encoded of [false, true]) {
              finish.mockClear();
              const state = await (await nativeStar(root, fixture, encoded)).prepare();
              try {
                for (let i = 0; i < lifetimes; i++)
                  expect(await state.run()).toEqual(fixture.expected);
                expect(finish).toHaveBeenCalledTimes(3 * lifetimes);
                reach.reach(encoded ? "dictionary IDs" : "native text");
                reach.reach("complete repeated lifetimes");
                if (fixture.expected.length === 0) reach.reach("empty result");
                else reach.reach("nonempty result");
              } finally {
                state.close();
                reach.reach("zero charges");
              }
            }
          } finally {
            finish.mockRestore();
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [0, 2],
          [1, 2],
          [7, 3],
          [65, 5],
          [257, 2],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
