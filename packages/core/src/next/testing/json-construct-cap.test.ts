/** Two serialized-row reservations + the owned binding/fixed recipe, never six payload copies.
 * Use plain ASCII here: cap/re-admission/ownership states depend on wire length, not escape CPU.
 * Generated spelling/pressure/nesting suites separately exercise six-byte control expansion. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_CONSTRUCT_CAP_REACH } from "../../testing/reach.js";
import { ResultBuffers } from "../exec/result-buffer.js";
import {
  JsonCampaign,
  admitGroup,
  allocateResultBytes,
} from "./json-construct-history.test.fixture.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const query = (binary: boolean) =>
  `SELECT ${binary ? "jsonb_build_object" : "json_build_object"}('v',$1::text)::text j`;
it.each(seedsFor("next-json-construction-cap", [0xb6d8, 0xb6d9]))(
  "executes the largest fitting value and refuses the first above its complete minimum (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON result-owned boundary", JSON_CONSTRUCT_CAP_REACH);
    const campaign = new JsonCampaign();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 12, max: 20 }),
          fc.boolean(),
          async (sizeCase, binary) => {
            reach.beginHistory();
            // Keep every seed/run/example tuple. The two explicit `16` examples retain
            // the DEFAULT 1 MiB result window and >600 KiB payloads, for both JSON kinds.
            // Other histories use the public 4 KiB window and ~3.125 MiB caps: they
            // exercise the same exact two-copy cap boundary without repeated MiB scans.
            const large = sizeCase === 16;
            const window = large ? 1 << 20 : 4096;
            const cap = large
              ? (5 << 20) + (sizeCase - 12) * (1 << 14)
              : (25 << 17) + (sizeCase - 12) * 1024;
            const history = await campaign.history(cap),
              { core, governor } = history,
              sql = query(binary),
              p = core.prepare(sql);
            let lastMinimum = 0,
              lastResultMinimum = 0;
            history.observe(
              vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
                if (options.label.endsWith("/fixed-buffers")) {
                  lastMinimum = options.grants.reduce((n, g) => n + g.minimumBytes, 0);
                  lastResultMinimum =
                    options.grants.find((g) => g.label.endsWith("/result/minimum"))?.minimumBytes ??
                    0;
                }
                return admitGroup.call(governor, options);
              }),
            );
            const buffers = new Set<ArrayBufferLike>();
            history.observe(
              vi.spyOn(ResultBuffers.prototype, "bytes").mockImplementation(function (
                this: ResultBuffers,
                n,
              ) {
                const b = allocateResultBytes.call(this, n);
                buffers.add(b.buffer);
                return b;
              }),
            );
            const prepared = history.wrap({
              ...p,
              execute: (request) => p.execute({ ...request, resultWindowBytes: window }),
            });
            async function run(length: number, compare = true) {
              const value = "a".repeat(length),
                expected = compare ? (await pg.query<{ j: string }>(sql, [value])).rows : [];
              history.guard();
              const run = core.start(prepared, [value]);
              const { execution } = run;
              const release = history.ownRelease(run.release);
              let rows = 0;
              try {
                for (;;) {
                  const frame = await execution.results.next();
                  if (!frame) break;
                  try {
                    const column = frame.payload.columns[0];
                    if (column?.form !== "flat" || column.data.layout !== "varlen")
                      throw new Error("Expected flat JSON text");
                    expect(buffers.has(column.data.bytes.buffer)).toBe(true);
                    expect(new TextDecoder().decode(column.data.bytes)).toBe(expected[rows]?.j);
                    rows++;
                  } finally {
                    frame.release();
                  }
                }
                await execution.executed;
                await execution.delivered;
                expect(rows).toBe(1);
              } catch (e) {
                execution.cancel(e);
                await Promise.allSettled([execution.executed, execution.delivered]);
                throw e;
              } finally {
                release();
                buffers.clear();
                expect(governor.usedBytes).toBe(0);
                expect(core.spill.bytesInUse).toBe(0);
              }
            }
            try {
              // Large histories must exceed the default floor; small ones the explicit window.
              const anchor = large ? 600000 : 16384;
              await run(anchor);
              const overhead = binary ? 9 : 10;
              const output = anchor + overhead;
              expect(lastResultMinimum).toBe(2 * (output + 4096));
              expect(lastResultMinimum).toBeGreaterThan(window);
              if (large) {
                expect(lastResultMinimum).toBeGreaterThan(1 << 20);
                reach.reach("above result window floor");
              } else {
                expect(lastResultMinimum).toBeLessThan(1 << 20);
                reach.reach("small result window");
              }
              // The fixed cost is measured from the real complete recipe; the independent linear law
              // accounts two wire rows and one already-owned TEXT parameter, not an opaque multiplier.
              const fixed = lastMinimum - 2 * output - anchor;
              expect(fixed).toBeGreaterThan(0);
              expect(fixed).toBeLessThan(4 << 20);
              const last = Math.floor((cap - fixed - 2 * overhead) / 3);
              expect(last).toBeGreaterThan(anchor);
              await run(last);
              expect(lastMinimum).toBe(fixed + 3 * last + 2 * overhead);
              expect(lastMinimum).toBeLessThanOrEqual(cap);
              reach.reach("largest fit");
              reach.reach("result buffer identity");
              reach.reach("two copies");
              reach.reach(binary ? "JSONB streaming canonicalization" : "JSON construction");
              reach.reach("exact PostgreSQL");
              await expect(run(last + 1, false)).rejects.toMatchObject({
                sqlState: "53200",
                cause: {
                  requestedBytes: fixed + 3 * (last + 1) + 2 * overhead,
                  admittedBytes: cap,
                },
              });
              reach.reach("first cannot fit");
              expect(governor.peakBytes).toBeLessThanOrEqual(cap);
            } finally {
              await history.close();
            }
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero resources");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [16, false],
            [16, true],
            [12, false],
            [12, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await campaign.close();
    }
  },
  120000,
);
