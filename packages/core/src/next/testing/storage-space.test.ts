/**
 * The extent space's cache under the real memory governor (storage/page-cache.ts,
 * memory/governor.ts rules 1, 6 and 7): its fixed slabs and unpinned blobs are charged through the
 * cache handle, leased blobs to the reader's grant; growth evicts the cache before refusing; the
 * governor's `usedBytes` equals what the space and the grants account for after every step.
 */
import { describe, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import type { BlobRef } from "../storage/extents.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { ENTRY_OVERHEAD, FIXED_CACHE_BYTES } from "../storage/page-cache.js";
import { openExtentSpace } from "../storage/space.js";
import { at, bytesOf, jsonReachable } from "../storage/space-test-support.js";

const MiB = 1 << 20;

describe("extent space under the memory governor", () => {
  it("keeps exact accounting, yields the cache to growth, and never evicts a leased blob", async () => {
    const governor = new Governor(16 * MiB);
    const backend = new MemoryBackend();
    const session = await backend.open(await backend.acquire());
    const startup = governor.tryAdmit({ label: "startup", minimumBytes: 0, spillable: true });
    if (startup === null) throw new Error("no startup grant");
    const space = await openExtentSpace(session, {
      reachable: jsonReachable,
      budget: startup,
      registerCache: (cache) => governor.registerCache(cache),
    });
    expect(governor.usedBytes).toBe(FIXED_CACHE_BYTES);

    const writer = space.beginWrite("data");
    const blobs: BlobRef[] = [];
    for (let chunk = 0; chunk < 16; chunk++)
      blobs.push(
        ...(
          await writer.write(
            Array.from({ length: 16 }, (_, page) => bytesOf(32_768, chunk * 16 + page)),
          )
        ).blobs,
      );
    const query = governor.tryAdmit({ label: "query", minimumBytes: MiB, spillable: true });
    if (query === null) throw new Error("no query grant");
    const accounted = () =>
      FIXED_CACHE_BYTES +
      (space.stats.cache.residentBytes - space.stats.cache.pinnedBytes) +
      ENTRY_OVERHEAD * cachedEntries() +
      query.held +
      startup.held;
    let entries = 0;
    const cachedEntries = () => entries;

    // Read every blob once (8 MiB): the cache keeps what the cap allows.
    for (let start = 0; start < blobs.length; start += 32) {
      const lease = await space.read(blobs.slice(start, start + 32), { budget: query });
      expect(query.allocated).toBe(MiB);
      lease.release();
    }
    entries = Math.round(
      (space.stats.cache.residentBytes - space.stats.cache.pinnedBytes) / 32_768,
    );
    expect(governor.usedBytes).toBe(accounted());
    expect(governor.usedBytes).toBeLessThanOrEqual(governor.capBytes);
    const resident = space.stats.cache.residentBytes;
    expect(resident).toBeGreaterThan(4 * MiB);

    // A leased blob stays while growth evicts the rest of the cache.
    const pinned = await space.read([at(blobs, 0)], { budget: query });
    expect(query.tryGrow(10 * MiB)).toBe(true);
    expect(space.stats.cache.residentBytes).toBeLessThan(resident);
    expect(space.peek(at(blobs, 0))).toEqual(bytesOf(32_768, 0));
    entries = Math.round(
      (space.stats.cache.residentBytes - space.stats.cache.pinnedBytes) / 32_768,
    );
    expect(governor.usedBytes).toBe(accounted());
    pinned.release();
    query.close();
    startup.close();
    await space.close();
    expect(governor.usedBytes).toBe(0);
  });
});
