/**
 * The IndexedDB backend's serialized chain (indexeddb.ts, `TrackedConnection`): by default every
 * transaction of a connection, from the data, log, spill and fence paths alike, waits for the
 * previous one to settle before it is created, so at most one is in flight (WebKit wedges
 * IndexedDB when a worker dies with two independent chains in flight: DECISIONS.md "Step-down").
 * A failed transaction does not stop the chain. `serializeTransactions: false` (benchmarks) lets
 * them overlap, which shows the probe can see an overlap.
 */
import { describe, expect, it } from "vitest";
import type { IdbDatabaseLike } from "./idb-like.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";

const tick = () =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, 1);
  });

/**
 * A connection over the simulated IndexedDB whose transactions take a task before and after the
 * simulator runs them (as a platform transaction is in flight across tasks), counting how many
 * are in flight at once. While `failSpill` is set, the next transaction over the spill store fails
 * before it begins.
 */
function countingConnection() {
  const inner = simIndexedDb(new SimDisk());
  const probe = { live: 0, peak: 0, failSpill: false, failed: 0 };
  const database: IdbDatabaseLike = {
    transaction: async (stores, mode, durability, body) => {
      probe.live += 1;
      probe.peak = Math.max(probe.peak, probe.live);
      try {
        await tick();
        if (probe.failSpill && stores.includes("spill")) {
          probe.failSpill = false;
          probe.failed += 1;
          throw new Error("injected spill failure");
        }
        await inner.transaction(stores, mode, durability, body);
        await tick();
      } finally {
        probe.live -= 1;
      }
    },
    close: () => undefined,
  };
  return { probe, database };
}

/** Data, log, spill and root work issued at once, as an owner's lanes would. */
async function mixedWorkload(serializeTransactions: boolean) {
  const { probe, database } = countingConnection();
  const backend = createIndexedDbBackend({
    database: () => database,
    locks: new SimLocks(),
    name: "chain",
    serializeTransactions,
  });
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  const scratch = backend.scratch(lease);
  if (scratch === null) throw new Error("no spill store");
  await scratch.write(0, new Uint8Array(4096).fill(1));
  probe.peak = 0;
  probe.failSpill = true;
  const settled = await Promise.allSettled([
    session.writeData(0, new Uint8Array(4096).fill(2)),
    session.appendLog(new Uint8Array(100).fill(3)).then(() => session.flushLog()),
    scratch.write(4096, new Uint8Array(4096).fill(4)),
    session.publish(new Uint8Array([5])),
    scratch.write(8192, new Uint8Array(4096).fill(6)),
    session.readRoot(),
  ]);
  const peak = probe.peak;
  // After the failure the chain still runs: everything else committed and reads back.
  const after = {
    root: await session.readRoot(),
    data: (await session.readData([{ offset: 0, length: 4096 }]))[0]?.[0],
    log: (await session.readLog(0, 100))[0],
  };
  await session.close();
  await lease.release();
  return { peak, failed: probe.failed, settled, after };
}

describe("every transaction of a connection runs through one serialized chain", () => {
  it("at most one transaction is in flight, and a failure does not stop the chain", async () => {
    const { peak, failed, settled, after } = await mixedWorkload(true);
    expect(peak).toBe(1);
    expect(failed).toBe(1);
    // Exactly the operation whose transaction failed rejects; the rest resolve.
    expect(settled.filter((outcome) => outcome.status === "rejected")).toHaveLength(1);
    expect(after).toEqual({ root: new Uint8Array([5]), data: 2, log: 3 });
  });

  it("without it, the same workload overlaps transactions (the probe sees overlap)", async () => {
    const { peak } = await mixedWorkload(false);
    expect(peak).toBeGreaterThan(1);
  });
});
