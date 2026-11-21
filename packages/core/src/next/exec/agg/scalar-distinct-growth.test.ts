/** Growth is an upper-bound contract (growth.ts); PostgreSQL owns DISTINCT equality. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { B9_SCALAR_DISTINCT_GROWTH_REACH, createReach } from "../../../testing/reach.js";
import { Governor } from "../../memory/governor.js";
import type { Vector } from "../../vector/vector.js";
import { TEXT, dictionary, texts } from "../kernels/test-support.js";
import { HashHeadroomError } from "./hash-workspace.js";
import { ScalarByteDistinct } from "./scalar-byte-distinct.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});

it.each(seedsFor("b9-scalar-distinct-growth", [0xb9a1, 0xb9a2]))(
  "funds every buffer through real load boundaries under a small cap (seed %i)",
  async (seed) => {
    const reach = createReach("B9 scalar DISTINCT growth", B9_SCALAR_DISTINCT_GROWTH_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 8, max: 40 }),
        fc.integer({ min: -10000, max: 10000 }),
        async (width, salt) => {
          reach.beginHistory();
          for (const boundary of [512, 2048])
            for (const encoded of [false, true]) {
              const governor = new Governor(8 << 20);
              const grant = await governor.admit({
                label: "complete DISTINCT test workspace",
                minimumBytes: 4 << 20,
                spillable: true,
              });
              let held = 0,
                ceiling = Infinity,
                peak = 0;
              const set = new ScalarByteDistinct(TEXT, {
                allocate(bytes) {
                  if (held + bytes > ceiling) throw new HashHeadroomError(held + bytes, ceiling);
                  if (!grant.allocate(bytes)) throw new Error("Complete test workspace exhausted");
                  held += bytes;
                  peak = Math.max(peak, held);
                  return true;
                },
                free(bytes) {
                  grant.free(bytes);
                  held -= bytes;
                },
              });
              const out = { rows: new Uint16Array(2048), count: 0 },
                gids = new Int32Array(2048);
              const key = (i: number) => `${String(salt)}:${String(i)}:${"x".repeat(width)}`;
              const prefix = Array.from({ length: boundary }, (_, i) => key(i));
              const offered: Array<string | null> = [...prefix, key(boundary), null, key(0)];
              const expected = await pg.query<{ n: number }>(
                "SELECT count(DISTINCT k)::int AS n FROM unnest($1::text[]) AS t(k)",
                [offered],
              );
              const fund = (vector: Vector, rows: number) => {
                ceiling = held + set.growthBytes(vector, rows);
              };
              try {
                const start = texts(prefix);
                fund(start, boundary);
                set.filter(start, gids, boundary, null, true, out);
                expect(set.count).toBe(boundary);
                const values = [key(boundary), null, "UNSELECTED", key(0)];
                const probe = encoded
                  ? dictionary(texts([key(boundary), key(0), "UNSELECTED"]), [0, null, 2, 1])
                  : texts(values);
                const selection = { rows: Uint16Array.from([0, 1, 3]), count: 3 };
                fund(probe, selection.count);
                set.filter(probe, gids, 4, selection, true, out);
                expect(out.count).toBe(1);
                expect(out.rows[0]).toBe(0);
                expect(set.count).toBe(expected.rows[0]?.n);
                reach.reach(boundary === 512 ? "first slot growth" : "later slot growth");
                reach.reach(encoded ? "dictionary" : "flat");
                reach.reach("NULL");
                reach.reach("selection");
                reach.reach("PostgreSQL count");
                expect(peak).toBeLessThan(4 << 20);
                reach.reach("bounded charge");
              } finally {
                set.close();
                grant.close();
              }
              expect(held).toBe(0);
              expect(governor.usedBytes).toBe(0);
              reach.reach("released");
            }
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
);
