/** Publication and stealing acquisition form one serializable compatibility decision.
 * The gate is coordination, never mutation authority. A refused opener cannot abort a live owner.
 */
import "fake-indexeddb/auto";
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ROOT_GATE_RACE_REACH } from "../../testing/reach.js";
import { SimLocks } from "../storage/sim-disk.js";
import { acquireNative, NativeAuthority } from "../storage/native-fence.js";
import type { StorageBackend, StoreSession } from "../storage/contract.js";
import { RootMemory } from "../storage/root-memory.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";
import {
  DURABLE_READER_VERSION,
  DURABLE_WRITER_VERSION,
  DurableFormatError,
} from "../durable-format.js";
import { xxHash32 } from "../hash/xxhash32.js";

import {
  identityOwnerName,
  identityGateName,
  requireIdentity,
  type OwnerIdentity,
} from "../storage/owner-identity.js";
const initial = {
  epoch: 1,
  generation: 1,
  reader: DURABLE_READER_VERSION,
  writer: DURABLE_WRITER_VERSION,
};
const ownerName = identityOwnerName("gate-race", initial);
const gateName = identityGateName("gate-race", initial);
import { deferred, fixture } from "./root-gate.test-support.js";

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

const futureCapability = fc.integer({ min: DURABLE_READER_VERSION + 1, max: 0xffffffff });

const reach = createReach("root publication versus steal", ROOT_GATE_RACE_REACH);
afterAll(() => reach.assertReached());
for (const kind of ["IDB", "OPFS"] as const)
  for (const seed of seedsFor("root-publication-steal", [0xb2c9, 0xb2ca]))
    it(`${kind}: serializes publication versus steal without aborting a refused opener's live owner seed ${String(seed)}`, async () => {
      await fc.assert(
        fc.asyncProperty(futureCapability, async (future) => {
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
      await futurePublication(session, "writer", DURABLE_WRITER_VERSION + 1);
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
      await futurePublication(session, "writer", DURABLE_WRITER_VERSION + 1);
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
  let identity: OwnerIdentity = initial;
  const authority = new NativeAuthority(locks, "gate-race", identity, {
    lost: delayedLoss.signal,
    release: () => old.release(),
  });
  const publish = authority.publication(
    async (expected, next) => {
      requireIdentity(identity, expected);
      identity = next;
    },
    async (expected) => requireIdentity(identity, expected),
  );
  const acquiring = acquireNative(
    locks,
    "gate-race",
    async () => identity,
    async (expected, next) => {
      // The underlying lock is stolen but the old worker has not handled the loss message.
      expect(old.lost.aborted).toBe(true);
      publication = publish(() => {
        writes++;
      }).catch((error: unknown) => error);
      fencing.resolve();
      await finishFence.promise;
      // Model the actual epoch/handle fence, then delivery of the old worker's loss notification.
      requireIdentity(identity, expected);
      identity = next;
      delayedLoss.abort();
      return 1;
    },
    () => undefined,
    { steal: true },
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
    acquired.authority.release();
    old.release();
  }
});
