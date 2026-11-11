/** The growth certificate must fund every allocation before a synchronous key transition. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, INT64_GROWTH_REACH } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { Governor } from "../../memory/governor.js";
import { I64, bigints, numbers } from "../kernels/test-support.js";
import { GroupTable } from "./groups.js";
import { HashWorkspace } from "./hash-workspace.js";

const reach = createReach("int64 complete key growth", INT64_GROWTH_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-int64-key-growth", [0xb4f3, 0xb4f4]))
  it(`funds half-full rehash boundaries before assigning keys (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          count: fc.integer({ min: 17000, max: 20000 }),
          batchRows: fc.constantFrom(128, 512, 2048),
          wide: fc.boolean(),
          selected: fc.boolean(),
          nullable: fc.boolean(),
        }),
        async ({ count, batchRows, wide, selected, nullable }) => {
          reach.beginHistory();
          const governor = new Governor(32 << 20);
          const grant = await governor.admit({
            label: "keys",
            minimumBytes: 64 << 10,
            spillable: true,
          });
          const workspace = new HashWorkspace(grant, grant.held);
          const table = new GroupTable([I64], workspace);
          const ids = new Int32Array(2048);
          let next = 0;
          try {
            for (let from = 0; from < count; from += batchRows) {
              const length = Math.min(batchRows, count - from);
              const values = Array.from({ length }, (_, i) =>
                nullable && from + i === 0 ? null : from + i,
              );
              const vector = wide
                ? bigints(
                    I64,
                    values.map((n) => (n === null ? null : 2n ** 60n + BigInt(n))),
                  )
                : numbers(I64, values);
              const rows = selected
                ? { rows: Uint16Array.from({ length }, (_, i) => length - i - 1), count: length }
                : null;
              const peak = workspace.used + table.growthBytes([vector], length);
              expect(await workspace.prepare(peak)).toBe(true);
              table.assign([vector], length, rows, ids);
              expect(workspace.used).toBeLessThanOrEqual(peak);
              for (let i = 0; i < length; i++) expect(ids[rows?.rows[i] ?? i]).toBe(next++);
              reach.reach("funded transition");
              if (table.groupCount > 8192) reach.reach("half-full boundary above 8192");
              if (table.groupCount > 16384) reach.reach("second half-full boundary");
            }
            expect(table.groupCount).toBe(count);
            reach.reach(wide ? "wide int64" : "safe int64");
            if (selected) reach.reach("selected input");
            if (nullable) reach.reach("NULL group");
          } finally {
            table.close();
            workspace.close();
            expect(grant.allocated).toBe(0);
            grant.close();
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero charges");
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [{ count: 18000, batchRows: 512, wide: false, selected: false, nullable: false }],
          [{ count: 18000, batchRows: 2048, wide: true, selected: true, nullable: true }],
        ],
      },
    );
  }, 60000);
