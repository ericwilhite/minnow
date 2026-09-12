/**
 * A leader that stalls while alive: it keeps its handles, hears nothing, and resumes. This is
 * what a synchronous checkpoint or recovery of that length looks like to a request that arrives
 * after it started, for which no `hold` was ever posted. The follower's dispatch budget is a
 * count of attempts, not a wall-clock limit; it must outlast a stall of a few seconds rather
 * than report `leader-unavailable` while the leader is merely busy.
 */
import { expect, it, vi } from "vitest";
import { heavyTestTimeout } from "../../engine/storage-test-helpers.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { opener, outcome, table } from "./coordination-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(30_000) });

it("a follower's read and write outlive a leader that stalls for three seconds without releasing its handles", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "stall", { rpcTimeoutMs: 1_000 });
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("seed"));
    await follower.getCurrentManifestVersion();
    const resume = leader._pauseCoordinationForTests();
    const timer = setTimeout(resume, 3_000);
    const read = outcome(follower.listTables());
    const write = outcome(follower.addTable(table("during-stall")));
    const results = { read: await read, write: await write };
    clearTimeout(timer);
    resume();
    expect(results).toEqual({ read: "ok", write: "ok" });
    const names = (await follower.listTables()).map((record) => record.name).sort();
    expect(names).toEqual(["during-stall", "seed"]);
  } finally {
    leader.close();
    follower.close();
  }
});
