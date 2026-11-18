/** The actual native exporter advertises and consumes its complete work grant. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_NATIVE_ADMISSION_REACH } from "../../testing/reach.js";
import { snapshotOwner } from "./snapshot-owner-support.js";

for (const seed of seedsFor("next-snapshot-native-admission", [0xb2f5, 0xb2f6]))
  it(`native snapshot work shares transport admission ${String(seed)}`, async () => {
    const reach = createReach("native snapshot admission", SNAPSHOT_NATIVE_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 999 }), async (value) => {
        reach.beginHistory();
        for (const kind of ["OPFS", "IndexedDB"] as const) {
          const owner = await snapshotOwner(kind, "admission");
          try {
            const client = await owner.current.connect(),
              operations = owner.current.engine.snapshots;
            expect(operations?.exportMinimumBytes).toBe(8 << 20);
            await client.query("CREATE TABLE t (v INT)");
            await client.query("INSERT INTO t VALUES ($1)", [value]);
            const admit = vi.spyOn(owner.current.governor, "admit"),
              group = vi.spyOn(owner.current.governor, "admitGroup");
            try {
              const archive = await client.exportSnapshot();
              expect(archive.length).toBeGreaterThan(64);
              expect(
                group.mock.calls.some(
                  ([options]) =>
                    options.includesProgress === true &&
                    options.grants.some(
                      (g) => g.label === "portable export" && g.minimumBytes === 8 << 20,
                    ) &&
                    options.grants.reduce((sum, g) => sum + g.minimumBytes, 0) === (9 << 20) + 1024,
                ),
              ).toBe(true);
              expect(
                admit.mock.calls.filter(([options]) => options.label === "portable export"),
              ).toHaveLength(0);
            } finally {
              admit.mockRestore();
              group.mockRestore();
            }
            expect((await client.query("SELECT v FROM t")).rows()).toEqual([[value]]);
            reach.reach(kind);
            reach.reach("native complete group");
            reach.reach("no nested admission");
          } finally {
            await owner.close();
          }
        }
      }),
      { seed, numRuns: 8, endOnFailure: true },
    );
    reach.assertReached();
  }, 120_000);
