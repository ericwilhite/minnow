/**
 * The exhaustive crash sweep over large transactions (review B3-p2a-1): crash-sweep.test.ts's
 * staged scenario (staging, commit, fold) crashed at every physical step. It is one of the sweep's
 * longest cases, in a file of its own only so a parallel runner can start it beside the others.
 *
 * `MINNOW_CRASH_STEP=n` starts the sweep at step n (to replay a failure first).
 */
import { describe, expect, it } from "vitest";
import type { SimDisk } from "../storage/sim-disk.js";
import { stagedScenario, sweep } from "./crash-sweep.test-support.js";
import { Harness } from "./test-support.js";

describe("crash sweep", () => {
  it("survives a crash at every physical step of large transactions (staging, commit, fold)", async () => {
    const make = (disk?: SimDisk) =>
      new Harness("strict", "sim", 8, disk, { merges: { mode: "off" } });
    // The clean run stages, folds, and drops a rolled-back staging.
    const clean = make();
    await stagedScenario(clean, { acked: new Map(), uncertain: new Map(), atomic: [] });
    const path = await clean.open();
    expect(path.state().root.tables.every((table) => table.staged.length === 0)).toBe(true);
    await clean.close();
    await sweep(make, stagedScenario);
  }, 1_800_000);
});
