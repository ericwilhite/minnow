/** Completion under a caller-held cap; virtual large roots isolate storage metadata, and the
 * write-path integration campaign separately supplies real encoded data and recovery.
 */
import { afterAll, expect, it, vi } from "vitest";
import { createReach, STORAGE_PRESSURE_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { ROOT_BANK_BYTES } from "../storage/root-memory.js";
import { METADATA_WORKSPACE_BYTES } from "../storage/metadata-journal.js";
import { MAX_RUN_ARENA_BYTES } from "../storage/bounded-free-space.js";
import { openExtentSpace } from "../storage/space.js";
import {
  jsonReachable,
  rootOf,
  countingBudget,
  countingMemory,
} from "../storage/space-test-support.js";
import { TaskClock } from "../task.js";
import type { ReachableExtents } from "../storage/extents.js";

const reach = createReach("storage publication pressure", STORAGE_PRESSURE_REACH);
for (const seed of seedsFor("storage-pressure", [1591, 1592, 1593, 1594]))
  it(`publishes and aborts while its caller holds every ordinary byte (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      cap = (seed % 2 === 0 ? 4 : 8) << 20,
      governor = new Governor(cap);
    const backend = new MemoryBackend(),
      lease = await backend.acquire(),
      session = await backend.open(lease);
    const startup = await governor.admit({
      label: "startup",
      minimumBytes: 768 * 1024,
      spillable: true,
    });
    const space = await openExtentSpace(session, {
      reachable: jsonReachable,
      budget: startup,
      registerCache: (cache) => governor.registerCache(cache),
    });
    startup.close();
    const pinned = space.pin("old reader");
    const caller = await governor.admit({
      label: "caller closes only after publication",
      minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
      spillable: false,
    });
    const loan = seed % 2 === 0 ? await caller.borrow(64 * 1024) : null;
    const admission = vi.spyOn(governor, "admit").mockImplementation(() => {
      throw new Error("Publication waited for its caller's admission");
    });
    let current: import("../storage/extents.js").ExtentRef | null = null;
    try {
      for (let step = 0; step < 8; step++) {
        const writer = space.beginWrite(`publish ${String(step)}`),
          written = await writer.write([new Uint8Array([step + 1])]);
        await space.publish(rootOf(written.blobs), {
          writers: [writer],
          retired: current === null ? [] : [current],
        });
        current = written.extent;
        const aborted = space.beginWrite("abort at full cap");
        const open = aborted.open(4096);
        await open.append([new Uint8Array([Math.floor(random() * 255)])]);
        await aborted.abort();
        expect(aborted.state).toBe("aborted");
      }
      expect(space.stats.waitingBytes).toBe(7 * 4096);
      reach.reach("pinned retirement backlog");
      expect(admission).not.toHaveBeenCalled();
      reach.reach("no publication admission");
      expect(governor.usedBytes).toBe(
        cap - governor.reserveBytes + (loan === null ? 0 : 64 * 1024),
      );
      if (loan !== null) {
        expect(caller.loaned).toBe(64 * 1024);
        reach.reach("caller loan held");
      }
      reach.reach("abort completes at full cap");
      pinned.release();
      await (space as unknown as { settled(): Promise<void> }).settled();
      // The newest retirement still waits for its next durable root.
      expect(space.stats.waitingBytes).toBe(4096);
      await space.publish(rootOf([], current === null ? [] : [current]), {
        writers: [],
        retired: [],
      });
      await (space as unknown as { settled(): Promise<void> }).settled();
      expect(space.stats.waitingBytes).toBe(0);
      await space.audit();
      reach.reach("audit after reclamation");
    } finally {
      admission.mockRestore();
      pinned.release();
      loan?.repay();
      caller.close();
      await space.close();
      await session.close();
      await lease.release();
    }
    expect(governor.usedBytes).toBe(0);
  });

it("opens100k previous-only extents with a capped arena and a disk ticket under4MiB", async () => {
  const count = 100_000,
    governor = new Governor(4 << 20),
    backend = new MemoryBackend(),
    lease = await backend.acquire(),
    session = await backend.open(lease);
  await session.publish(new Uint8Array([1]));
  await session.publish(new Uint8Array([2]));
  // Virtual persistent data is deliberately outside this metadata-only governor fixture.
  session.dataLength = async () => count * 8192;
  session.releaseData = async () => undefined;
  const reachable: ReachableExtents = Object.assign(
    function* (root: Uint8Array) {
      if (root[0] === 1) for (let i = 0; i < count; i++) yield { offset: i * 8192, length: 1 };
    },
    { ordered: true },
  );
  const startup = await governor.admit({
    label: "startup",
    minimumBytes: 768 * 1024,
    spillable: true,
  });
  const space = await openExtentSpace(session, {
    reachable,
    budget: startup,
    registerCache: (cache) => governor.registerCache(cache),
  });
  try {
    startup.close();
    expect(space.stats.waitingBytes).toBe(count * 4096);
    expect(space.stats.retainedBytes).toBe(
      METADATA_WORKSPACE_BYTES + MAX_RUN_ARENA_BYTES + ROOT_BANK_BYTES,
    );
    expect(space.stats.untrackedFreeBytes).toBeGreaterThan(0);
  } finally {
    await space.close();
    startup.close();
    await session.close();
    await lease.release();
  }
  expect(governor.usedBytes).toBe(0);
});

it("rejects an unpublished retirement when its writer aborts at a copying yield; replacement survives", async () => {
  const backend = new MemoryBackend(),
    lease = await backend.acquire(),
    session = await backend.open(lease),
    budget = countingBudget(16 << 20),
    memory = countingMemory(Infinity);
  const space = await openExtentSpace(session, {
    reachable: jsonReachable,
    budget,
    registerCache: () => memory,
  });
  const writer = space.beginWrite("unpublished"),
    refs: Array<import("../storage/extents.js").ExtentRef> = [];
  for (let i = 0; i < 128; i++) refs.push((await writer.write([new Uint8Array([i])])).extent);
  let tick = 0,
    aborting: Promise<void> | null = null;
  try {
    await expect(
      space.publish(rootOf([]), {
        writers: [],
        retired: refs,
        clock: new TaskClock(() => (tick += 5)),
        yieldTask: async () => {
          aborting ??= writer.abort();
          await new Promise<void>((resolve) => setImmediate(resolve));
        },
      }),
    ).rejects.toBeInstanceOf(RangeError);
    await Promise.resolve(aborting);
    expect(space.root).toBeNull();
    const replacement = space.beginWrite("replacement"),
      written = await replacement.write([new Uint8Array([99])]);
    expect(written.extent.offset).toBe(0);
    await space.publish(rootOf(written.blobs), { writers: [replacement], retired: [] });
    await (space as unknown as { settled(): Promise<void> }).settled();
    await space.audit();
    const read = await space.read(written.blobs, { budget });
    try {
      expect(read.bytes[0]?.[0]).toBe(99);
    } finally {
      read.release();
    }
  } finally {
    await Promise.resolve(aborting);
    await space.close();
    await session.close();
    await lease.release();
  }
  expect(budget.used).toBe(0);
  expect(memory.used).toBe(0);
});
afterAll(() => reach.assertReached());

for (const seed of seedsFor("storage-resurrection", [1611, 1612, 1613, 1614]))
  it(`refuses to resurrect a pin-delayed older extent (${String(seed)})`, async () => {
    const backend = new MemoryBackend(),
      lease = await backend.acquire(),
      session = await backend.open(lease),
      budget = countingBudget(32 << 20),
      memory = countingMemory(Infinity);
    const space = await openExtentSpace(session, {
      reachable: jsonReachable,
      budget,
      registerCache: () => memory,
    });
    let pin: ReturnType<typeof space.pin> | null = null;
    try {
      const a = space.beginWrite("A"),
        first = await a.write([new Uint8Array([seed & 255])]);
      await space.publish(rootOf(first.blobs), { writers: [a], retired: [] });
      pin = space.pin("A remains pinned");
      const b = space.beginWrite("B"),
        second = await b.write([new Uint8Array([(seed + 1) & 255])]);
      const current = rootOf(second.blobs);
      await space.publish(current, { writers: [b], retired: [first.extent] });
      await expect(
        space.publish(rootOf(first.blobs), { writers: [], retired: [second.extent] }),
      ).rejects.toThrow(/neither current-root nor supplied sealed-writer/u);
      expect(space.root).toEqual(current);
      expect(space.failed).toBeNull();
      pin.release();
      pin = null;
      await space.publish(current, { writers: [], retired: [] });
      await (space as unknown as { settled(): Promise<void> }).settled();
      await space.audit();
      const read = await space.read(second.blobs, { budget });
      try {
        expect(read.bytes[0]?.[0]).toBe((seed + 1) & 255);
      } finally {
        read.release();
      }
    } finally {
      pin?.release();
      await space.close();
      await session.close();
      await lease.release();
    }
    expect(memory.used).toBe(0);
    expect(budget.used).toBe(0);
  });
