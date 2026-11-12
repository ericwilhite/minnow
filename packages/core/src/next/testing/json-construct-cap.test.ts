/** Two serialized-row reservations + the owned binding/fixed recipe, never six payload copies.
 * Use plain ASCII here: cap/re-admission/ownership states depend on wire length, not escape CPU.
 * Generated spelling/pressure/nesting suites separately exercise six-byte control expansion. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_CONSTRUCT_CAP_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { ResultBuffers } from "../exec/result-buffer.js";
import { StorageCore } from "./storage-core.js";
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
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 12, max: 20 }), fc.boolean(), async (mib, binary) => {
        reach.beginHistory();
        const core = await new StorageCore().open(),
          cap = mib << 20,
          governor = new Governor(cap),
          sql = query(binary),
          p = core.prepare(sql),
          admit = governor.admitGroup.bind(governor);
        let lastMinimum = 0,
          lastResultMinimum = 0;
        const reservations = vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
          if (options.label.endsWith("/fixed-buffers")) {
            lastMinimum = options.grants.reduce((n, g) => n + g.minimumBytes, 0);
            lastResultMinimum =
              options.grants.find((g) => g.label.endsWith("/result/minimum"))?.minimumBytes ?? 0;
          }
          return admit(options);
        });
        const buffers = new Set<ArrayBufferLike>();
        // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked below with actual receiver
        const allocate = ResultBuffers.prototype.bytes;
        const allocations = vi.spyOn(ResultBuffers.prototype, "bytes").mockImplementation(function (
          this: ResultBuffers,
          n,
        ) {
          const b = allocate.call(this, n);
          buffers.add(b.buffer);
          return b;
        });
        const prepared = {
          ...p,
          execute: (r: Parameters<typeof p.execute>[0]) => p.execute({ ...r, governor }),
        };
        async function run(length: number, compare = true) {
          const value = "a".repeat(length),
            expected = compare ? (await pg.query<{ j: string }>(sql, [value])).rows : [];
          const { execution, release } = core.start(prepared, [value]);
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
          // Keep the anchor above the ordinary 1 MiB result-window floor.
          const anchor = 600000;
          await run(anchor);
          const overhead = binary ? 9 : 10;
          const output = anchor + overhead;
          expect(lastResultMinimum).toBe(2 * (output + 4096));
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
            cause: { requestedBytes: fixed + 3 * (last + 1) + 2 * overhead, admittedBytes: cap },
          });
          reach.reach("first cannot fit");
          expect(governor.peakBytes).toBeLessThanOrEqual(cap);
        } finally {
          allocations.mockRestore();
          reservations.mockRestore();
          await core.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero resources");
      }),
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
  },
  120000,
);
