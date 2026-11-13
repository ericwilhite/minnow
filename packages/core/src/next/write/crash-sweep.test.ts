/**
 * The exhaustive crash sweep (review B3-p2a-1): one scenario crossing commits, memtable flushes,
 * publishes (with retired extents), a CREATE INDEX build and a reopen, crashed at every physical
 * step of the simulated disk in turn (honest flush). After each crash the store reopens and must
 * hold every acknowledged change and nothing that was never attempted; the indexes must agree with
 * the rows; the extent space's audit must pass; and writing must continue across another reopen.
 *
 * `MINNOW_CRASH_STEP=n` starts the sweep at step n (to replay a failure first).
 *
 * The index-merge and large-transaction sweeps, the longest cases, are in crash-sweep-merges.test.ts
 * and crash-sweep-staged.test.ts so parallel runners can start them beside these; the scenarios and
 * checks are in crash-sweep.test-support.ts.
 */
import { describe, expect, it } from "vitest";
import type { SimDisk } from "../storage/sim-disk.js";
import { MEMTABLE_ROWS, type Outcome, scenario, sweep } from "./crash-sweep.test-support.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
} from "./test-support.js";

describe("crash sweep", () => {
  it("a publish after replay records the first unflushed row id (found by the sweep)", async () => {
    const harness = new Harness("strict", "sim", MEMTABLE_ROWS);
    let path = await harness.open();
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", INT)]),
          })
        ).id ?? 0;
    });
    // Two commits fill most of one memtable; the third starts the next one.
    for (let k = 0; k < 3; k++) {
      const ids = Array.from({ length: 12 }, (_, j) => k * 12 + j);
      await autocommit(path, (s) => s.table(t).insert(batch(ints(ids), ints(ids)), null));
    }
    // Recovery replays all three; the flush publishes the first memtable only.
    path = await harness.crash();
    await path.quiesce();
    path = await harness.reopen();
    const view = path.pin();
    expect((await rowsOf(view, t)).length).toBe(36);
    view.release();
    await harness.close();
  });

  it("keeps an unspent fault from a shorter replay out of recovery", async () => {
    let reference = true;
    let completedReplays = 0;
    const run = async (harness: Harness, outcome: Outcome) => {
      const path = await harness.open();
      let t = 0;
      await autocommit(path, async (s) => {
        t =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [column("id", INT, false), column("v", INT)]),
            })
          ).id ?? 0;
      });
      for (const id of [1, 2, 3, 4]) {
        outcome.uncertain.set(id, ["absent", id * 10]);
        await autocommit(path, (s) => s.table(t).insert(batch(ints([id]), ints([id * 10])), null));
        outcome.uncertain.delete(id);
        outcome.acked.set(id, id * 10);
      }
      await path.quiesce();
      await harness.close();
      if (reference) {
        // Different task interleavings can publish different numbers of roots. Model
        // a longer reference deterministically, without depending on timer speed.
        const padding = harness.disk.open("reference-only-work", true);
        for (let step = 0; step < 8; step++) padding.truncate(0);
        padding.close();
        reference = false;
      } else completedReplays += 1;
    };
    await sweep((disk) => new Harness("strict", "sim", 1, disk, { merges: { mode: "off" } }), run);
    expect(completedReplays, "a replay ends before its planned fault").toBeGreaterThan(0);
  }, 1_800_000);

  it("survives a crash at every physical step of commit, flush and publish", async () => {
    await sweep(
      (disk) => new Harness("strict", "sim", MEMTABLE_ROWS, disk, { merges: { mode: "off" } }),
    );
  }, 1_800_000);

  it("survives a crash at every physical step of log recycling", async () => {
    // A small log bound: the scenario recycles several times, each with its flushes, the
    // switching root, the reset and the first frames of the new incarnation.
    const make = (disk?: SimDisk) =>
      new Harness("strict", "sim", MEMTABLE_ROWS, disk, {
        logRecycleBytes: 400,
        merges: { mode: "off" },
      });
    const probe = make();
    await scenario(probe, { acked: new Map(), uncertain: new Map() });
    const path = await probe.open();
    expect(path.state().root.log.firstLsn).toBeGreaterThan(1);
    await probe.close();
    await sweep(make);
  }, 1_800_000);
});
