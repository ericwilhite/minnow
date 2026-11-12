/** R23.5: a merge spends the same task budget as work before the merge began. */
import { expect, it, vi } from "vitest";
import * as pipeline from "../exec/pipeline.js";
import { createReach } from "../../testing/reach.js";
import { DEFAULT_MERGE_POLICY, mergeRuns } from "./merge.js";
import { encodeRun, RunEncoder, RunReader, runRef } from "./runs.js";
import { Harness } from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };
const reach = createReach("next-write-merge-task-clock", ["fresh task", "prior task work"]);

it.each([0, 3])("merge uses the worker budget (prior work %i ms)", async (priorWork) => {
  const h = new Harness("strict", "memory");
  await h.open();
  const writer = h.space.beginWrite("merge clock");
  const grant = await h.governor.admit({
    label: "merge clock",
    minimumBytes: 8 * 1024 * 1024,
    spillable: false,
  });
  let handle: ReturnType<typeof setImmediate> | undefined;
  try {
    const encoded = encodeRun([
      {
        key: Uint8Array.of(1),
        entries: Array.from({ length: 2048 }, (_, k) => ({ rowId: k + 1, tombstone: false })),
      },
    ]);
    if (encoded === null) throw new Error("Missing input run");
    const ref = runRef(encoded, (await writer.write(encoded.parts)).blobs);
    const reader = await RunReader.open(h.space, ref, FREE, FREE);
    let clock = 0;
    let count = 0;
    let task = false;
    let progressed = false;
    const yields: number[] = [];
    // eslint-disable-next-line @typescript-eslint/unbound-method -- rebound to the actual encoder with call below.
    const add = RunEncoder.prototype.add;
    vi.spyOn(RunEncoder.prototype, "add").mockImplementation(function (
      this: RunEncoder,
      ...args: Parameters<RunEncoder["add"]>
    ) {
      clock += 0.005;
      count++;
      progressed ||= task;
      return add.call(this, ...args);
    });
    const yieldTask = pipeline.yieldTask;
    vi.spyOn(pipeline, "yieldTask").mockImplementation(async () => {
      yields.push(count);
      await yieldTask();
    });
    vi.spyOn(performance, "now").mockReturnValue(0);
    vi.spyOn(pipeline.taskClock, "now").mockImplementation(() => clock);
    pipeline.beginTask();
    clock += priorWork;
    handle = setImmediate(() => {
      task = true;
    });
    reach.beginHistory();
    reach.reach(priorWork === 0 ? "fresh task" : "prior task work");
    const output = await mergeRuns({
      space: h.space,
      writer,
      grant,
      outputBudget: grant,
      readers: [reader],
      level: 1,
      bottom: true,
      policy: DEFAULT_MERGE_POLICY,
      stopped: () => false,
    });
    expect(output.entriesOut).toBe(2048);
    expect(progressed, "a queued task runs before the merge finishes").toBe(true);
    // Checks occur every 256 entries. 3 ms already spent leaves at most one chunk;
    // a fresh task reaches 4 ms after 1024 entries at the injected rate.
    expect(yields[0]).toBe(priorWork === 0 ? 1024 : 256);
  } finally {
    if (handle !== undefined) clearImmediate(handle);
    vi.restoreAllMocks();
    await writer.abort();
    grant.close();
    await h.close();
  }
});

it("reached every merge-clock state", () => {
  reach.assertReached();
});
