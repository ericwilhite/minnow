/** R25/R5: retained allocator and retirement state survives the startup grant. */
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import type { ExtentListPage } from "../storage/extent-list.js";
import { Governor } from "../memory/governor.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";
import { ReadDeniedError, type ExtentSpace, type ReachableExtents } from "../storage/extents.js";
import { FIXED_CACHE_BYTES } from "../storage/page-cache.js";

const reach = createReach("space-retained-memory", [
  "fragmented",
  "previous-only",
  "transferred",
  "retired",
  "closed",
]);
async function stored(count: number) {
  const backend = new MemoryBackend();
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  await session.publish(Uint8Array.of(1));
  await session.publish(Uint8Array.of(2));
  session.dataLength = () => Promise.resolve(count * 8192);
  const reachable: ReachableExtents = Object.assign(
    function* (root: Uint8Array) {
      if (root[0] === 1) for (let i = 0; i < count; i++) yield { offset: i * 8192, length: 4096 };
    },
    { ordered: true },
  );
  return {
    session,
    reachable,
    close: async () => {
      await session.close();
      await lease.release();
    },
  };
}
for (const seed of seedsFor("space-retained-memory", [771, 772, 773, 774]))
  it(`retains charges across startup close and previous-only reclaim (${String(seed)})`, async () => {
    reach.beginHistory();
    const count = 128 + Math.floor(mulberry32(seed)() * 256);
    const h = await stored(count);
    const governor = new Governor(8 << 20);
    const startup = await governor.admit({
      label: "startup",
      minimumBytes: 512 << 10,
      spillable: true,
    });
    let space: ExtentSpace | undefined;
    try {
      space = await openExtentSpace(h.session, {
        reachable: h.reachable,
        budget: startup,
        registerCache: (c) => governor.registerCache(c),
      });
      expect(space.stats.freeBytes).toBe((count - 1) * 4096);
      reach.reach("fragmented");
      expect(space.stats.waitingBytes).toBe(count * 4096);
      reach.reach("previous-only");
      expect(startup.allocated).toBe(0);
      startup.close();
      expect(governor.usedBytes).toBeGreaterThan(FIXED_CACHE_BYTES + count * 32);
      reach.reach("transferred");
      await space.audit();
      await space.publish(Uint8Array.of(3), { writers: [], retired: [] });
      await space.audit();
      expect(space.stats.waitingBytes).toBe(0);
      expect(space.stats.dataBytes).toBe(0);
      expect(governor.usedBytes).toBeGreaterThan(FIXED_CACHE_BYTES);
      reach.reach("retired");
      await space.close();
      expect(governor.usedBytes).toBe(0);
      expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
      reach.reach("closed");
    } finally {
      await space?.close();
      startup.close();
      await h.close();
    }
  });
it("reaches all retained-space states", () => reach.assertReached());

it("refuses 100,000 previous-only extents within a finite startup budget and cleans up", async () => {
  const h = await stored(100_000);
  const budget = countingBudget(512 << 10);
  const memory = countingMemory(Infinity);
  let space: ExtentSpace | undefined;
  try {
    const opened = openExtentSpace(h.session, {
      reachable: h.reachable,
      budget,
      registerCache: () => memory,
    });
    await expect(
      opened.then((s) => {
        space = s;
      }),
    ).rejects.toBeInstanceOf(ReadDeniedError);
    expect(budget.used).toBe(0);
    expect(memory.used).toBe(0);
  } finally {
    await space?.close();
    await h.close();
  }
});

it("asks admission before enumerating under a refusing budget", async () => {
  const h = await stored(100_000);
  let requests = 0,
    entries = 0;
  const memory = countingMemory(Infinity);
  const reachable = Object.assign(
    function* () {
      entries++;
      yield { offset: 0, length: 4096 };
    },
    { ordered: true },
  );
  let space: ExtentSpace | undefined;
  try {
    await expect(
      openExtentSpace(h.session, {
        reachable,
        budget: {
          allocate() {
            requests++;
            return false;
          },
          free() {
            /* No successful reservation exists under this refusing budget. */
          },
        },
        registerCache: () => memory,
      }).then((s) => {
        space = s;
      }),
    ).rejects.toBeInstanceOf(ReadDeniedError);
    expect(requests).toBeGreaterThan(0);
    expect(entries).toBe(0);
    expect(memory.used).toBe(0);
  } finally {
    await space?.close();
    await h.close();
  }
});

for (const reason of ["abort", "corrupt", "transfer-refusal"] as const)
  it(`cleans construction reservations after ${reason}`, async () => {
    const h = await stored(256);
    const controller = new AbortController();
    const failure = new Error("stop retained-map construction");
    const budget = countingBudget(512 << 10);
    const memory = countingMemory(
      reason === "transfer-refusal" ? FIXED_CACHE_BYTES + 512 : Infinity,
    );
    const reachable = Object.assign(
      function* (root: Uint8Array) {
        if (root[0] !== 1) return;
        for (let i = 0; i < 256; i++) {
          if (i === 65 && reason === "abort") controller.abort(failure);
          if (i === 65 && reason === "corrupt") throw failure;
          yield { offset: i * 8192, length: 4096 };
        }
      },
      { ordered: true },
    );
    let space: ExtentSpace | undefined;
    try {
      const opened = openExtentSpace(h.session, {
        reachable,
        budget,
        registerCache: () => memory,
        signal: controller.signal,
      });
      await expect(
        opened.then((s) => {
          space = s;
        }),
      ).rejects.toBeInstanceOf(reason === "transfer-refusal" ? ReadDeniedError : Error);
      expect(budget.used).toBe(0);
      expect(memory.used).toBe(0);
    } finally {
      await space?.close();
      await h.close();
    }
  });

for (const state of ["fragmented-current", "contiguous-previous"] as const)
  it(`admits ${state} state independently, before retaining its entries`, async () => {
    const h = await stored(2048);
    const budget = countingBudget(64 << 10);
    const memory = countingMemory(Infinity);
    const reachable = Object.assign(
      function* (root: Uint8Array) {
        if (root[0] !== (state === "fragmented-current" ? 2 : 1)) return;
        for (let i = 0; i < 2048; i++)
          yield { offset: i * (state === "fragmented-current" ? 8192 : 4096), length: 4096 };
      },
      { ordered: true },
    );
    let space: ExtentSpace | undefined;
    try {
      await expect(
        openExtentSpace(h.session, { reachable, budget, registerCache: () => memory }).then((s) => {
          space = s;
        }),
      ).rejects.toBeInstanceOf(ReadDeniedError);
      expect(budget.used).toBe(0);
      expect(memory.used).toBe(0);
    } finally {
      await space?.close();
      await h.close();
    }
  });

it("keeps the old free-map generation intact if growth overlap is refused", async () => {
  const { RetainedMemory, RetainedCapacity, FREE_RUN_SLOT_BYTES } =
    await import("../storage/retained-memory.js");
  const { FreeSpaceMap } = await import("../storage/free-space.js");
  const budget = countingBudget(4096);
  const memory = RetainedMemory.create(budget);
  const map = new FreeSpaceMap(4096, new RetainedCapacity(memory, FREE_RUN_SLOT_BYTES));
  try {
    for (let i = 0; i <= 32; i++) expect(map.allocateAt(i * 8192, 4096)).toBe(true);
    const before = map.runs();
    const end = map.end;
    expect(() => map.allocateAt(33 * 8192, 4096)).toThrow(ReadDeniedError);
    expect(map.runs()).toEqual(before);
    expect(map.end).toBe(end);
    expect(budget.used).toBe(1024 + 2 * 32 * FREE_RUN_SLOT_BYTES);
  } finally {
    memory.close();
  }
  expect(budget.used).toBe(0);
});

it("accounts previous-only entries decoded from actual list pages", async () => {
  const { MemoryExtentSpace } = await import("../page/memory-extents.js");
  const { encodeListPage, sortedListedExtents } = await import("../storage/extent-list.js");
  const backing = new MemoryExtentSpace();
  const writer = backing.beginWrite("durable lists");
  const list: ExtentListPage[] = [];
  for (let start = 0; start < 100_000; start += 4096) {
    const ranges = Array.from({ length: Math.min(4096, 100_000 - start) }, (_, i) => ({
      offset: 2 ** 30 + (start + i) * 8192,
      length: 4096,
    }));
    const blob = (await writer.write([encodeListPage(ranges)])).blobs[0];
    if (blob === undefined) throw new Error("missing list page");
    list.push({ blob, first: 2 ** 30 + start * 8192, count: ranges.length });
  }
  const h = await stored(100_000);
  h.session.dataLength = () => Promise.resolve(2 ** 31);
  const budget = countingBudget(512 << 10);
  const memory = countingMemory(Infinity);
  const reachable: ReachableExtents = Object.assign(
    (root: Uint8Array, _reader: unknown, b: Parameters<ReachableExtents>[2]) =>
      sortedListedExtents(root[0] === 1 ? list : [], backing, b),
    { ordered: true },
  );
  let space: ExtentSpace | undefined;
  try {
    await expect(
      openExtentSpace(h.session, { reachable, budget, registerCache: () => memory }).then((s) => {
        space = s;
      }),
    ).rejects.toBeInstanceOf(ReadDeniedError);
    expect(budget.used).toBe(0);
    expect(memory.used).toBe(0);
  } finally {
    await space?.close();
    await h.close();
  }
});
