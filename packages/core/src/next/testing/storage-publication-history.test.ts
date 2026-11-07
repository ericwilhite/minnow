/** Enumerate every actual final-phase native-I/O/task suspension in a baseline, then inject
 * monotonic reservations and queued removals/transitions at each one. No wall-time guessing.
 */
import { afterAll, expect, it, vi } from "vitest";
import { createReach, STORAGE_FINAL_HISTORY_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { taskClock, beginTask } from "../task.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";
import {
  countingBudget,
  countingMemory,
  jsonReachable,
  rootOf,
} from "../storage/space-test-support.js";
import type { ExtentWriter } from "../storage/extents.js";

const reach = createReach("final publication suspension histories", STORAGE_FINAL_HISTORY_REACH);
async function history(
  seed: number,
  target: number | null,
): Promise<{ trace: string[]; checkpoints: number[] }> {
  const backend = new MemoryBackend(),
    lease = await backend.acquire(),
    session = await backend.open(lease),
    memory = countingMemory(Infinity),
    budget = countingBudget(32 << 20);
  const nativeMetadata = session.metadata?.();
  if (nativeMetadata === undefined || nativeMetadata === null)
    throw new Error("Missing test backing");
  const trace: string[] = [],
    checkpoints: number[] = [];
  let publisher: ExtentWriter | null = null,
    armed = false,
    acted = false,
    abortRejected = false;
  let action: (kind: string) => void = () => undefined;
  const marker = (kind: string) => {
    if (!armed || publisher === null) return;
    trace.push(kind);
    if (publisher.state === "publishing") checkpoints.push(trace.length - 1);
    if (target !== null && trace.length - 1 === target) {
      acted = true;
      action(kind);
    }
  };
  session.metadata = () => ({
    reset: () => nativeMetadata.reset(),
    read: async (page, bytes) => {
      await nativeMetadata.read(page, bytes);
      marker("metadata read");
    },
    write: async (page, bytes) => {
      await nativeMetadata.write(page, bytes);
      marker("metadata write");
    },
  });
  const nativePublish = session.publish.bind(session);
  session.publish = async (root) => {
    marker("native publish before");
    await nativePublish(root);
    marker("native publish after");
  };
  const space = await openExtentSpace(session, {
    reachable: jsonReachable,
    budget,
    registerCache: () => memory,
  });
  const pending: Array<Promise<unknown>> = [],
    now = taskClock.now,
    immediate = setImmediate;
  let pin: ReturnType<typeof space.pin> | null = null;
  try {
    const initial = space.beginWrite("initial"),
      a = await initial.write([new Uint8Array([1])]),
      b = await initial.write([new Uint8Array([2])]);
    await space.publish(rootOf([...a.blobs, ...b.blobs]), { writers: [initial], retired: [] });
    await (space as unknown as { settled(): Promise<void> }).settled();
    pin = space.pin("previous root");
    const victim = space.beginWrite("abort"),
      victimExtent = await victim.write([new Uint8Array([3])]);
    victim.open(4096);
    const discarded = space.beginWrite("discard"),
      discardedExtent = await discarded.write([new Uint8Array([4])]);
    const sealBudget = countingBudget(4096),
      sealing = space.beginWrite("seal", { budget: sealBudget }),
      builder = sealing.open(4096);
    await builder.append([new Uint8Array([5])]);
    const reservations = space.beginWrite("monotonic"),
      grows = reservations.open(4096);
    publisher = space.beginWrite("publisher");
    const next = await publisher.write([new Uint8Array([6])]);
    const publishing = publisher;
    const operations = {
      abort: () => victim.abort(),
      discard: () => discarded.discard(discardedExtent.extent),
      seal: () => builder.seal(),
    };
    const orders = [
      ["abort", "discard", "seal"],
      ["abort", "seal", "discard"],
      ["discard", "abort", "seal"],
      ["discard", "seal", "abort"],
      ["seal", "abort", "discard"],
      ["seal", "discard", "abort"],
    ] as const;
    const order = orders[(seed + (target ?? 0)) % orders.length];
    if (order === undefined) throw new Error("Missing transition order");
    const actions = order.map((name) => operations[name]);
    const hole = space.beginWrite("hole"),
      holeExtent = await hole.write([new Uint8Array([7])]);
    const tail = space.beginWrite("tail guard");
    await tail.write([new Uint8Array([8])]);
    await hole.abort();
    action = (kind) => {
      // Additions consume only tracked holes/EOF. Supplied publishing writers are immutable.
      const tracked = reservations.open(4096);
      // Append/seal later would reveal placement, but during final validation only additions
      // are allowed. EOF must not move for this first exact-fit tracked-hole reservation.
      const beforeExtend = space.stats.dataBytes;
      grows.extend(4096);
      expect(tracked.capacity).toBe(4096);
      expect(beforeExtend).toBe(holeExtent.extent.offset + 8192);
      reach.reach("tracked hole consumed");
      expect(() => publishing.open(1)).toThrow(/publishing/u);
      pending.push(
        publishing.abort().catch((error: unknown) => {
          expect(error).toBeInstanceOf(TypeError);
          abortRejected = true;
        }),
      );
      for (const operation of actions) pending.push(operation());
      expect(victim.state).toBe("open");
      expect(victim.extents).toEqual([victimExtent.extent]);
      expect(discarded.extents).toEqual([discardedExtent.extent]);
      expect(sealing.extents.length).toBe(0);
      reach.reach("monotonic open and extend");
      reach.reach("publishing writer rejects additions");
      reach.reach("queued owners retain records");
      if (kind === "task yield") reach.reach("task suspension");
      if (kind.startsWith("metadata")) reach.reach("metadata suspension");
      if (kind.startsWith("native publish")) reach.reach("native publish suspension");
    };
    let tick = 0;
    taskClock.now = () => ++tick;
    vi.stubGlobal("setImmediate", (callback: (...args: unknown[]) => void, ...args: unknown[]) => {
      marker("task yield");
      return immediate(callback, ...args);
    });
    beginTask();
    armed = true;
    await space.publish(rootOf([...b.blobs, ...next.blobs]), {
      writers: [publishing],
      retired: [a.extent],
    });
    armed = false;
    await Promise.all(pending);
    taskClock.now = now;
    beginTask();
    vi.unstubAllGlobals();
    if (target !== null) {
      expect(acted, `unreached final checkpoint ${String(target)}`).toBe(true);
      expect(abortRejected).toBe(true);
      expect(victim.state).toBe("aborted");
      expect(discarded.extents).toEqual([]);
      expect(sealing.extents.length).toBe(1);
      expect(sealBudget.used).toBe(0);
      const sealed = sealing.extents[0];
      if (sealed === undefined) throw new Error("Missing sealed output");
      await sealing.abort();
      await discarded.abort();
      await reservations.abort();
      await tail.abort();
      await space.audit();
      reach.reach("audit after all orderings");
    } else {
      await victim.abort();
      await discarded.abort();
      await sealing.abort();
      await reservations.abort();
      await tail.abort();
    }
    const read = await space.read([...a.blobs, ...b.blobs, ...next.blobs], { budget });
    try {
      expect(read.bytes.map((bytes) => bytes[0])).toEqual([1, 2, 6]);
    } finally {
      read.release();
    }
    pin.release();
    pin = null;
    await space.publish(rootOf([...b.blobs, ...next.blobs]), { writers: [], retired: [] });
    await (space as unknown as { settled(): Promise<void> }).settled();
    await space.audit();
    return { trace, checkpoints };
  } finally {
    armed = false;
    taskClock.now = now;
    beginTask();
    vi.unstubAllGlobals();
    pin?.release();
    await Promise.allSettled(pending);
    await space.close();
    await session.close();
    await lease.release();
    expect(memory.used).toBe(0);
    expect(budget.used).toBe(0);
  }
}
for (const seed of seedsFor("storage-final-history", [1601, 1602, 1603, 1604]))
  it(`interleaves reservations, abort, discard and seal at every final suspension (${String(seed)})`, async () => {
    reach.beginHistory();
    const baseline = await history(seed, null);
    expect(baseline.checkpoints.length).toBeGreaterThan(20);
    for (const at of baseline.checkpoints) {
      const result = await history(seed, at);
      expect(result.trace).toEqual(baseline.trace);
    }
  }, 120_000);
afterAll(() => reach.assertReached());
