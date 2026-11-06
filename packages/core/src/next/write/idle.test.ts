/**
 * Idle maintenance (ruling R10) and the level-0 stall. On an idle database whose retired space
 * waits past its bound, one no-change root is published so the space is reusable, then nothing
 * stays armed; an open transaction defers it. Writers wait while an index has `stallRuns`
 * level-0 runs, until merges bring it below.
 */
import { describe, expect, it } from "vitest";
import type { WritePathImpl } from "./engine.js";
import { autocommit, batch, column, Harness, INT, ints, tableInput } from "./test-support.js";

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function table(path: WritePathImpl): Promise<number> {
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  return t;
}

async function load(path: WritePathImpl, t: number, from: number, rows: number): Promise<void> {
  for (let start = from; start < from + rows; start += 8) {
    const ids = Array.from({ length: Math.min(8, from + rows - start) }, (_, k) => start + k);
    await autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
  }
}

describe("idle publish (ruling R10)", () => {
  it("publishes one no-change root when merge-retired space waits, then stays quiet", async () => {
    const harness = new Harness("strict", "memory", 8, undefined, {
      merges: { fanIn: 2 },
      idleGarbageBytes: 1,
    });
    const path = await harness.open();
    const t = await table(path);
    await load(path, t, 0, 40);
    await path.quiesce();
    expect(path.maintenance.merges).toBeGreaterThan(0);
    // Retired runs wait for the next publish until the idle publish makes it.
    await pause(400);
    expect(path.maintenance.idlePublishes).toBeGreaterThanOrEqual(1);
    expect(harness.space.stats.waitingBytes).toBe(0);
    const published = path.maintenance.idlePublishes;
    await pause(400);
    expect(path.maintenance.idlePublishes).toBe(published); // no timer left, no wakeup
    await harness.space.audit();
    await harness.close();
  });

  it("does nothing under the garbage bound, and waits while a transaction is open", async () => {
    const quiet = new Harness("strict", "memory", 8, undefined, { merges: { fanIn: 2 } });
    let path = await quiet.open();
    let t = await table(path);
    await load(path, t, 0, 40);
    await path.quiesce();
    await pause(300);
    expect(path.maintenance.idlePublishes).toBe(0); // 16 MiB floor: far below
    await quiet.close();
    const busy = new Harness("strict", "memory", 8, undefined, {
      merges: { fanIn: 2 },
      idleGarbageBytes: 1,
    });
    path = await busy.open();
    t = await table(path);
    await load(path, t, 0, 40);
    await path.quiesce();
    // A writer holds the slot from before the first idle check until well after it.
    const held = await path.begin();
    await pause(300);
    expect(path.maintenance.idlePublishes).toBe(0);
    held.rollback();
    await pause(400);
    expect(path.maintenance.idlePublishes).toBeGreaterThanOrEqual(1);
    await busy.close();
  });
});

describe("level-0 stall", () => {
  it("makes a writer wait while an index has stallRuns level-0 runs, until a merge lands", async () => {
    const harness = new Harness("relaxed", "memory", 8, undefined, {
      // Slow merges (a task yield per entry) so level 0 reaches the stall bound.
      merges: { fanIn: 3, stallRuns: 3, sliceEntries: 1 },
    });
    const path = await harness.open();
    const t = await table(path);
    let waited = false;
    for (let start = 0; start < 400 && !waited; start += 8) {
      if (path.stats().backpressure) {
        const before = path.maintenance.merges;
        expect(path.stats().levelZeroRuns).toBeGreaterThanOrEqual(3);
        const tx = await path.begin();
        // The writer got the slot only once a merge brought level 0 below the bound.
        expect(path.maintenance.merges).toBeGreaterThan(before);
        expect(path.stats().levelZeroRuns).toBeLessThan(3);
        tx.rollback();
        waited = true;
        continue;
      }
      const ids = Array.from({ length: 8 }, (_, k) => start + k);
      const tx = await path.begin();
      const s = tx.statement({ rules: () => ({ check: () => null, generate: () => undefined }) });
      await s.table(t).insert(batch(ints(ids)), null);
      await s.end();
      await tx.commit(null);
      await path.flush();
      // Let the flush of the frozen memtable publish (merges are slow, flushes are not).
      for (let k = 0; k < 5; k++) await new Promise((resolve) => setImmediate(resolve));
    }
    expect(waited).toBe(true);
    await harness.close();
  });
});
