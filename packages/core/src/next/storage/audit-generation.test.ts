/** R8 audit must compare roots and allocator state from one publication generation. */
import { expect, it, vi } from "vitest";
import { MemoryBackend } from "./memory-store.js";
import { openExtentSpace } from "./space.js";
import { countingBudget, countingMemory } from "./space-test-support.js";

it("serializes the reviewer's previous-root enumeration/publication race", async () => {
  const backend = new MemoryBackend();
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  await session.publish(Uint8Array.of(1));
  await session.publish(Uint8Array.of(2));
  session.dataLength = () => Promise.resolve(8192);
  let armed = false;
  let entered!: () => void;
  let release!: () => void;
  const enumerating = new Promise<void>((r) => {
    entered = r;
  });
  const gate = new Promise<void>((r) => {
    release = r;
  });
  const reachable = Object.assign(
    async function* (root: Uint8Array) {
      yield { offset: root[0] === 1 ? 0 : 4096, length: 4096 };
      if (armed && root[0] === 1) {
        armed = false;
        entered();
        await gate;
      }
    },
    { ordered: true },
  );
  const space = await openExtentSpace(session, {
    reachable,
    budget: countingBudget(1 << 20),
    registerCache: () => countingMemory(Infinity),
  });
  let auditing: Promise<void> | undefined;
  let publishing: Promise<number> | undefined;
  try {
    armed = true;
    auditing = space.audit();
    void auditing.catch(() => undefined);
    await enumerating;
    // Same publication as the review probe; coordinate externally so a serialized
    // implementation is not forced to re-enter its own publication lock.
    vi.useFakeTimers();
    publishing = space.publish(Uint8Array.of(3), { writers: [], retired: [] });
    await vi.advanceTimersByTimeAsync(0);
    release();
    await vi.runAllTimersAsync();
    await expect(auditing).resolves.toBeUndefined();
    await publishing;
    vi.useRealTimers();
    await space.audit();
  } finally {
    release();
    vi.useRealTimers();
    await Promise.allSettled([auditing, publishing]);
    await space.close();
    await session.close();
    await lease.release();
  }
});
