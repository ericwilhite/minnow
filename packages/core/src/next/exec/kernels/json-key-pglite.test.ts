/** Key cursors compare decoded UTF8 bytes, including distinct spellings of the same key. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, JSONB_KEY_CURSOR_REACH } from "../../../testing/reach.js";
import { Slice, runToEnd } from "../slices.js";
import { JsonOutput } from "./json-output.js";
import { canonicalJsonb } from "./jsonb-canonical.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const KEYS = [
  JSON.stringify("é"),
  '"\\u00e9"',
  JSON.stringify("😀"),
  '"\\uD83D\\uDE00"',
  JSON.stringify("汉"),
  JSON.stringify("한"),
  JSON.stringify("zz"),
  JSON.stringify("aaaa"),
  JSON.stringify("abc"),
  JSON.stringify("\t"),
  JSON.stringify("\u0001"),
  JSON.stringify("x"),
  JSON.stringify('"\\'),
  JSON.stringify("bb"),
  '"b\\u0062"',
];
it.each(seedsFor("next-jsonb-key-cursor", [0xb6e4, 0xb6e5]))(
  "matches decoded key ordering and last duplicates without full key buffers (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB UTF8 key cursor", JSONB_KEY_CURSOR_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.shuffledSubarray(KEYS, { minLength: KEYS.length, maxLength: KEYS.length }),
        fc.integer({ min: -1000, max: 1000 }),
        fc.integer({ min: 0, max: 128 }),
        async (keys, n, padding) => {
          reach.beginHistory();
          const suffix = "x".repeat(padding),
            text =
              "{" +
              keys.map((key, i) => key.slice(0, -1) + suffix + '":' + String(n + i)).join(",") +
              "}",
            input = new TextEncoder().encode(text),
            slice = new Slice(Infinity);
          let used = 0,
            peak = 0;
          const memory = {
            // eslint-disable-next-line require-yield -- pre-admitted synchronous kernel test
            *take(bytes: number) {
              used += bytes;
              peak = Math.max(peak, used);
            },
            free(bytes: number) {
              used -= bytes;
              expect(used).toBeGreaterThanOrEqual(0);
            },
          };
          const count = new JsonOutput(null, slice);
          runToEnd(canonicalJsonb(input, count, memory, slice));
          const output = new Uint8Array(count.position);
          runToEnd(canonicalJsonb(input, new JsonOutput(output, slice), memory, slice));
          const expected = (await pg.query<{ j: string }>("SELECT $1::jsonb::text j", [text]))
            .rows[0]?.j;
          expect(new TextDecoder().decode(output)).toBe(expected);
          expect(used).toBe(0);
          expect(peak).toBeLessThanOrEqual(1024);
          for (const label of JSONB_KEY_CURSOR_REACH) reach.reach(label);
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [KEYS, 0, 0],
          [[...KEYS].reverse(), -1, 128],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
