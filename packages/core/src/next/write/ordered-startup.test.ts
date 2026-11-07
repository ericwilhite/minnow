import { MemoryExtentSpace } from "../page/memory-extents.js";
import { encodeListPage, sortedListedExtents } from "../storage/extent-list.js";
import { expect, it, vi } from "vitest";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";
import { FreeSpaceMap } from "../storage/free-space.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";
import { ReadDeniedError } from "../storage/extents.js";
import { RETAINED_BASE_BYTES, RETIRED_SLOT_BYTES } from "../storage/retained-memory.js";
import { FIXED_CACHE_BYTES } from "../storage/page-cache.js";
import type { ReachableExtents } from "../storage/extents.js";

it("stream-merges ordered roots without the unordered sort constructor", async () => {
  const backend = new MemoryBackend();
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  await session.publish(Uint8Array.of(1));
  await session.publish(Uint8Array.of(2));
  const length = 100_000 * 4096;
  vi.spyOn(session, "dataLength").mockResolvedValue(length);
  let visited = 0;
  const reachable = Object.assign(
    function* (root: Uint8Array) {
      for (let i = root[0] === 1 ? 0 : 1; i < 100_000; i += 2) {
        visited++;
        yield { offset: i * 4096, length: 4096 };
      }
    },
    { ordered: true },
  ) satisfies ReachableExtents;
  const sorted = vi.spyOn(FreeSpaceMap, "fromAllocatedAsync");
  let space;
  try {
    // Half of the union belongs only to the previous root. The old 8 MiB
    // fixture budget hid that retained state; it must now refuse and clean up.
    const refused = countingBudget(8 << 20);
    const refusedMemory = countingMemory(Infinity);
    await expect(
      openExtentSpace(session, {
        reachable,
        budget: refused,
        registerCache: () => refusedMemory,
      }),
    ).rejects.toBeInstanceOf(ReadDeniedError);
    expect(refused.used).toBe(0);
    expect(refusedMemory.used).toBe(0);
    visited = 0;
    const retainedBytes = RETAINED_BASE_BYTES + 2 * 65536 * RETIRED_SLOT_BYTES;
    const budget = countingBudget(retainedBytes);
    const memory = countingMemory(Infinity);
    space = await openExtentSpace(session, {
      reachable,
      budget,
      registerCache: () => memory,
    });
    expect(budget.used).toBe(0);
    expect(memory.used).toBe(FIXED_CACHE_BYTES + retainedBytes);
    expect(sorted).not.toHaveBeenCalled();
    expect(visited).toBe(100_000);
    expect(space.stats.dataBytes).toBe(length);
    await space.audit();
  } finally {
    sorted.mockRestore();
    await space?.close();
    await lease.release();
  }
});

it("rejects conflicting duplicates and descending ordered inputs before reclaim", async () => {
  for (const ranges of [
    [
      { offset: 0, length: 1 },
      { offset: 0, length: 2 },
    ],
    [
      { offset: 4096, length: 1 },
      { offset: 0, length: 1 },
    ],
    [
      { offset: 0, length: 8192 },
      { offset: 4096, length: 1 },
    ],
  ]) {
    const backend = new MemoryBackend();
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    await session.publish(Uint8Array.of(1));
    vi.spyOn(session, "dataLength").mockResolvedValue(8192);
    const release = vi.spyOn(session, "releaseData");
    try {
      await expect(
        openExtentSpace(session, {
          reachable: Object.assign(() => ranges, { ordered: true }),
          budget: countingBudget(8 << 20),
          registerCache: () => countingMemory(Infinity),
        }),
      ).rejects.toMatchObject({ name: "StoreCorruptionError" });
      expect(release).not.toHaveBeenCalled();
    } finally {
      await lease.release();
    }
  }
});

it("merges scattered list-page allocations into the data order and releases its workspace", async () => {
  const reader = new MemoryExtentSpace();
  const writer = reader.beginWrite("scattered pages");
  const second = (await writer.write([encodeListPage([{ offset: 2 ** 21, length: 100 }])]))
    .blobs[0];
  const first = (await writer.write([encodeListPage([{ offset: 2 ** 20, length: 100 }])])).blobs[0];
  if (first === undefined || second === undefined) throw new Error("Missing test pages");
  const budget = countingBudget(8 << 20);
  const offsets: number[] = [];
  for await (const ref of sortedListedExtents(
    [
      { blob: first, first: 2 ** 20, count: 1 },
      { blob: second, first: 2 ** 21, count: 1 },
    ],
    reader,
    budget,
  ))
    offsets.push(ref.offset);
  expect(offsets).toEqual([second.extent.offset, first.extent.offset, 2 ** 20, 2 ** 21]);
  expect(budget.used).toBe(0);
});
