/** Even a post-publication source reclaim error sweeps scratch and recovers a complete target. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_OWNER_CLEANUP_REACH } from "../../testing/reach.js";
import { snapshotOwner } from "./snapshot-owner-support.js";
for (const seed of seedsFor("next-snapshot-owner-cleanup", [0xb2ed, 0xb2ee]))
  it(`post-publication cleanup failure leaves a complete reopenable generation ${String(seed)}`, async () => {
    const reach = createReach("snapshot owner cleanup", SNAPSHOT_OWNER_CLEANUP_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 999 }), async (value) => {
        reach.beginHistory();
        for (const kind of ["OPFS", "IndexedDB"] as const) {
          const source = await snapshotOwner(kind, "cleanup-source"),
            target = await snapshotOwner(kind, "cleanup-target");
          try {
            const from = await source.current.connect(),
              into = await target.current.connect();
            await from.query("CREATE TABLE t (v INT)");
            await from.query("INSERT INTO t VALUES ($1)", [value]);
            const bytes = await from.exportSnapshot();
            await into.query("CREATE TABLE t (v INT)");
            await into.query("INSERT INTO t VALUES ($1)", [-value]);
            const before = target.current.engine.databaseId;
            target.failReclaim();
            await expect(into.importSnapshot(bytes)).rejects.toMatchObject({
              code: "MINNOW_STORE_QUOTA",
            });
            expect(target.current.engine.databaseId).not.toBe(before);
            expect(target.cleanupSweeps).toBeGreaterThan(0);
            reach.reach("sweep after reclaim failure");
            expect(target.fatalEvents).toBe(1);
            expect(target.current.server.steppingDown).toBe("closing");
            reach.reach("host notified");
            const reopened = await target.restart(),
              fresh = await reopened.connect();
            expect((await fresh.query("SELECT v FROM t")).rows()).toEqual([[value]]);
            expect((await from.query("SELECT v FROM t")).rows()).toEqual([[value]]);
            reach.reach("complete target reopens");
            reach.reach("source intact");
            reach.reach(kind);
          } finally {
            try {
              await target.close();
            } finally {
              await source.close();
            }
          }
        }
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 180000);
