/**
 * A successor whose recovery is slow must not fail the other followers. Recovery in a browser
 * pays real file opens (the WAL, two checkpoint slots, the extent tail), so an election can take
 * seconds on a large database; followers whose requests arrive during it wait for the new leader
 * rather than reporting `leader-unavailable`.
 */
import { expect, it, vi } from "vitest";
import { heavyTestTimeout } from "../../engine/storage-test-helpers.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsBlockStore } from "./store.js";
import { delaySyncHandleOpens, table } from "./coordination-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(30_000) });

it("followers outlive a successor whose recovery takes several seconds after the leader dies", async () => {
  const shim = new MemoryOpfs();
  const open = () => OpfsBlockStore.open({ name: "slow-recovery", root: shim.root });
  const leader = await open();
  const followers = await Promise.all([open(), open(), open()]);
  // Only the extent files pay: the WAL open is the lock itself, and losers must fail it fast.
  const restore = await delaySyncHandleOpens(
    shim,
    (name) => !["wal", "checkpoint-a", "checkpoint-b"].includes(name),
    3_000,
  );
  try {
    await leader.addTable(table("seed"));
    await Promise.all(followers.map((follower) => follower.getCurrentManifestVersion()));
    leader._crashForTests();
    const outcomes = await Promise.all(
      followers.map((follower) =>
        follower.listTables().then(
          (tables) => tables.map((record) => record.name).join(","),
          (error: unknown) => (error instanceof Error ? error.name : String(error)),
        ),
      ),
    );
    expect(outcomes).toEqual(["seed", "seed", "seed"]);
  } finally {
    restore();
    for (const follower of followers) follower.close();
  }
});
