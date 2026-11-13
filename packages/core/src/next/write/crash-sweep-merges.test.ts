/**
 * The exhaustive crash sweep over index merges (review B3-p2a-1): crash-sweep.test.ts's scenario
 * with every second flush merging, crashed at every physical step. It is the sweep's longest case,
 * in a file of its own only so a parallel runner can start it beside the others.
 *
 * `MINNOW_CRASH_STEP=n` starts the sweep at step n (to replay a failure first).
 */
import { describe, expect, it } from "vitest";
import type { SimDisk } from "../storage/sim-disk.js";
import { MEMTABLE_ROWS, scenario, sweep } from "./crash-sweep.test-support.js";
import { Harness } from "./test-support.js";

describe("crash sweep", () => {
  it("survives a crash at every physical step of index merges", async () => {
    // Fan-in 2: every second flush merges, at every level; slices by entry count keep the
    // interleaving of merges with commits the same in every run.
    const make = (disk?: SimDisk) =>
      new Harness("strict", "sim", MEMTABLE_ROWS / 2, disk, {
        merges: { fanIn: 2, sliceEntries: 4 },
      });
    // The clean run merges at more than one level, and its publications retire merged runs.
    const probe = make();
    await scenario(probe, { acked: new Map(), uncertain: new Map() });
    const path = await probe.open();
    const t = path.catalog.tableByName("t")?.id ?? 0;
    const levels = [...(path.state().tables.get(t)?.runs.values() ?? [])].flatMap((runs) =>
      runs.map((run) => run.level),
    );
    expect(Math.max(...levels)).toBeGreaterThanOrEqual(2);
    await probe.close();
    await sweep(make);
  }, 1_800_000);
});
