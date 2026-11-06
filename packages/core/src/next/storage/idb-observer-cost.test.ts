import { expect, it } from "vitest";
import type { IdbDatabaseLike } from "./idb-like.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";

it("registers native completion once when observation is disabled", async () => {
  const inner = simIndexedDb(new SimDisk());
  const registrations: number[] = [];
  const database: IdbDatabaseLike = {
    close: () => undefined,
    transaction(...args) {
      const promise = inner.transaction(...args);
      const then = promise.then.bind(promise);
      const index = registrations.push(0) - 1;
      promise.then = (...callbacks) => {
        registrations[index] = (registrations[index] ?? 0) + 1;
        return then(...callbacks);
      };
      return promise;
    },
  };
  const backend = createIndexedDbBackend({
    database: () => database,
    locks: new SimLocks(),
    name: "observer-cost",
  });
  const lease = await backend.acquire();
  try {
    const session = await backend.open(lease);
    registrations.length = 0;
    await session.publish(new Uint8Array([1]));
    expect(registrations).toEqual([1]);
  } finally {
    await lease.release();
  }
});
