/** Publication and stealing acquisition form one serializable compatibility decision.
 * The gate is coordination, never mutation authority. A refused opener cannot abort a live owner.
 */
import "fake-indexeddb/auto";
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ROOT_GATE_RACE_REACH } from "../../testing/reach.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { IDB_STORES, openIndexedDb } from "../storage/idb-like.js";
import { SimLocks } from "../storage/sim-disk.js";
import { acquireFenced, publicationGate, type LockManagerLike } from "../storage/fence.js";
import type { StorageBackend, StoreSession } from "../storage/contract.js";
import { RootMemory } from "../storage/root-memory.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";
import { DurableFormatError } from "../durable-format.js";
import { xxHash32 } from "../hash/xxhash32.js";

const ownerName = "minnow:gate-race";
const gateName = `minnow-gate:${ownerName}`;
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
};

async function fixture(kind: "IDB" | "OPFS", locks: LockManagerLike) {
  if (kind === "OPFS") {
    const shim = new MemoryOpfs();
    return {
      backend: () =>
        createOpfsBackend({
          name: "gate-race",
          locks,
          // Native Chromium/Firefox permit Blob snapshots while a SAH is held. The strict
          // cross-browser shim refuses them, so model that independently probed native API here.
          directory: {
            keys: () => shim.root.keys(),
            getFileHandle: async (name, options) => {
              const handle = await shim.root.getFileHandle(name, options);
              return {
                createSyncAccessHandle: () => handle.createSyncAccessHandle(),
                getFile: async () =>
                  new Blob([new Uint8Array(shim.readFileBytes(name) ?? []).buffer]),
              };
            },
          },
        }),
      image: async () => {
        const entries: Array<readonly [string, Uint8Array | undefined]> = [];
        for await (const name of shim.root.keys()) entries.push([name, shim.readFileBytes(name)]);
        return entries.sort(([a], [b]) => a.localeCompare(b));
      },
    };
  }
  const factory = new IDBFactory();
  return {
    backend: () =>
      createIndexedDbBackend({
        name: "gate-race",
        locks,
        database: () => openIndexedDb(factory, "gate-race"),
      }),
    image: async () => {
      const db = await openIndexedDb(factory, "gate-race");
      const image: Record<string, unknown> = {};
      try {
        await db.transaction(IDB_STORES, "readonly", "relaxed", (tx) => {
          for (const name of IDB_STORES) {
            tx.store(name).getAllKeys({}, (keys) => {
              image[`${name}:keys`] = keys;
            });
            tx.store(name).getAll({}, (values) => {
              image[`${name}:values`] = values;
            });
          }
        });
        return image;
      } finally {
        db.close();
      }
    },
  };
}

async function futurePublication(
  session: StoreSession,
  field: "reader" | "writer",
  future: number,
) {
  const memory = countingMemory(1 << 20),
    budget = countingBudget(1 << 20);
  const roots = RootMemory.create(() => memory);
  const frame = await roots.copy(new Uint8Array(32).fill(19), budget);
  try {
    frame.prepare(session, 1, 2);
    const view = new DataView(
      frame.slotBytes.buffer,
      frame.slotBytes.byteOffset,
      frame.slotBytes.length,
    );
    view.setUint32(field === "reader" ? 16 : 20, future, true);
    view.setUint32(
      frame.slotBytes.length - 4,
      xxHash32(frame.slotBytes, 0, 0, frame.slotBytes.length - 4),
      true,
    );
    // A prepared frame simulates a later writer publishing through the SAME production path.
    await session.publish(frame.bytes);
  } finally {
    frame.release();
    roots.close();
    expect(memory.used).toBe(0);
    expect(budget.used).toBe(0);
  }
}

const reach = createReach("root publication versus steal", ROOT_GATE_RACE_REACH);
afterAll(() => reach.assertReached());
for (const kind of ["IDB", "OPFS"] as const)
  for (const seed of seedsFor("root-publication-steal", [0xb2c9, 0xb2ca]))
    it(`${kind}: serializes publication versus steal without aborting a refused opener's live owner seed ${String(seed)}`, async () => {
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 9, max: 0xffffffff }), async (future) => {
          reach.beginHistory();
          for (const field of ["reader", "writer"] as const)
            for (const order of [
              "publication first",
              "probe-steal interval",
              "steal first",
            ] as const) {
              const locks = new SimLocks(),
                queued = deferred();
              let hook: (() => Promise<void>) | undefined;
              let watchingPublication = false,
                gateHeld = false;
              const source = await fixture(kind, {
                request: async (name, options) => {
                  if (name === gateName) {
                    expect(options.steal).not.toBe(true);
                    if (watchingPublication && gateHeld) queued.resolve();
                  }
                  if (name === ownerName && options.steal === true) await hook?.();
                  const lock = await locks.request(name, options);
                  if (name !== gateName) return lock;
                  gateHeld = true;
                  return {
                    lost: lock.lost,
                    release: () => {
                      gateHeld = false;
                      lock.release();
                    },
                  };
                },
              });
              const backend = source.backend(),
                owner = await backend.acquire(),
                session = await backend.open(owner);
              let acquired: Awaited<ReturnType<StorageBackend["acquire"]>> | undefined;
              let publication: Promise<unknown> | undefined;
              let before: unknown;
              try {
                await session.publish(new Uint8Array(32).fill(7));
                await session.appendLog(new Uint8Array([5, 11, 17]));
                await session.flushLog();
                const publish = () => futurePublication(session, field, future);
                if (order === "publication first") {
                  await publish();
                  before = await source.image();
                } else if (order === "probe-steal interval") {
                  hook = async () => {
                    watchingPublication = true;
                    publication = publish().then(
                      () => {
                        queued.resolve();
                      },
                      (error: unknown) => {
                        queued.resolve();
                        return error;
                      },
                    );
                    // Deterministic barrier: either published (old implementation), or waiting
                    // for the opener's gate (new). Never wait for a blocked publisher to finish.
                    await queued.promise;
                  };
                }
                let refusal: unknown;
                try {
                  acquired = await backend.acquire({ steal: true, timeoutMs: 2000 });
                } catch (error) {
                  refusal = error;
                }
                if (order === "publication first" || refusal !== undefined) {
                  expect(refusal).toBeInstanceOf(DurableFormatError);
                  expect(refusal).toMatchObject({ code: "MINNOW_FORMAT_UNSUPPORTED" });
                  expect(owner.signal.aborted).toBe(false);
                  if (before !== undefined) expect(await source.image()).toEqual(before);
                  reach.reach("refused live owner survives");
                  // The live owner retains real write authority, not merely a non-aborted signal.
                  await session.appendLog(new Uint8Array([29]));
                  await session.flushLog();
                  reach.reach("live owner still writes");
                } else {
                  expect(acquired).toBeDefined();
                  expect(owner.signal.aborted).toBe(true);
                  const image = await source.image();
                  if (order === "steal first")
                    publication = publish().catch((error: unknown) => error);
                  expect(await publication).toMatchObject({ code: "MINNOW_OWNERSHIP_LOST" });
                  expect(await source.image()).toEqual(image);
                  reach.reach("stolen queued publication refuses");
                }
                reach.reach(kind);
                reach.reach(field);
                reach.reach(order);
              } finally {
                await publication;
                await session.close();
                await owner.release();
                await acquired?.release();
              }
            }
        }),
        { seed, numRuns: 4, endOnFailure: true },
      );
    }, 60000);

for (const kind of ["IDB", "OPFS"] as const) {
  it(`${kind}: a queued ordinary opener cannot block a live owner's gate publication`, async () => {
    const locks = new SimLocks(),
      requested = deferred();
    let waiting = false;
    const source = await fixture(kind, {
      request: (name, options) => {
        if (waiting && name === ownerName) requested.resolve();
        return locks.request(name, options);
      },
    });
    const backend = source.backend(),
      owner = await backend.acquire(),
      session = await backend.open(owner);
    let queued: Promise<unknown> | undefined;
    try {
      await session.publish(new Uint8Array(32).fill(7));
      waiting = true;
      queued = backend.acquire({ timeoutMs: 2000 }).catch((error: unknown) => error);
      await requested.promise;
      await futurePublication(session, "writer", 9);
      const before = await source.image();
      expect(owner.signal.aborted).toBe(false);
      await session.close();
      await owner.release();
      expect(await queued).toMatchObject({ code: "MINNOW_FORMAT_UNSUPPORTED" });
      expect(await source.image()).toEqual(before);
    } finally {
      await session.close();
      await owner.release();
      await queued;
    }
  });

  it(`${kind}: a timed-out gate waiter never steals later and does not strand publication`, async () => {
    const locks = new SimLocks(),
      source = await fixture(kind, locks),
      backend = source.backend();
    const owner = await backend.acquire(),
      session = await backend.open(owner);
    const held = await locks.request(gateName, {});
    try {
      await expect(backend.acquire({ steal: true, timeoutMs: 5 })).rejects.toMatchObject({
        code: "MINNOW_OWNER_UNRESPONSIVE",
      });
      expect(owner.signal.aborted).toBe(false);
      held.release();
      await futurePublication(session, "writer", 9);
      expect(owner.signal.aborted).toBe(false);
      await expect(backend.acquire({ steal: true })).rejects.toMatchObject({
        code: "MINNOW_FORMAT_UNSUPPORTED",
      });
    } finally {
      held.release();
      await session.close();
      await owner.release();
    }
  });
}

it("keeps publication excluded through fencing even if the old worker receives loss late", async () => {
  const locks = new SimLocks(),
    old = await locks.request(ownerName, {});
  const delayedLoss = new AbortController(),
    fencing = deferred(),
    finishFence = deferred();
  let writes = 0;
  let publication: Promise<unknown> | undefined;
  const publish = publicationGate(locks, ownerName, delayedLoss.signal);
  const acquiring = acquireFenced(
    locks,
    ownerName,
    async () => {
      // The underlying lock is stolen but the old worker has not handled the loss message.
      expect(old.lost.aborted).toBe(true);
      publication = publish(() => {
        writes++;
      }).catch((error: unknown) => error);
      fencing.resolve();
      await finishFence.promise;
      // Model the actual epoch/handle fence, then delivery of the old worker's loss notification.
      delayedLoss.abort();
      return 1;
    },
    () => undefined,
    { steal: true },
    async () => undefined,
  );
  await fencing.promise;
  await Promise.resolve();
  await Promise.resolve();
  const beforeFence = writes;
  finishFence.resolve();
  const acquired = await acquiring;
  try {
    expect(beforeFence).toBe(0);
    expect(await publication).toMatchObject({ code: "MINNOW_OWNERSHIP_LOST" });
    expect(writes).toBe(0);
  } finally {
    acquired.lock.release();
    old.release();
  }
});
