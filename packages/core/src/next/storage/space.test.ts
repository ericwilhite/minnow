/**
 * The extent space (space.ts) against its contract (extents.ts), over the in-memory backend:
 * writers and builders, publication, two-generation reclaim, the open walk, audit, leases and the
 * cache. Crash behaviour is in space-crash.test.ts; the real governor in
 * testing/storage-space.test.ts.
 */
import { describe, expect, it } from "vitest";
import { storeConformanceCases } from "./conformance.js";
import { DATA_ALIGN, StoreCorruptionError, type StoreSession } from "./contract.js";
import {
  MAX_BLOB_BYTES,
  MAX_READ_BYTES,
  ReadDeniedError,
  SpaceFailedError,
  type BlobRef,
  type ExtentRef,
  type ExtentSpace,
} from "./extents.js";
import { MemoryBackend } from "./memory-store.js";
import { ENTRY_OVERHEAD, FIXED_CACHE_BYTES } from "./page-cache.js";
import { BUILDER_BUFFER_BYTES, openExtentSpace } from "./space.js";
import { xxHash32 } from "../hash/xxhash32.js";
import {
  at,
  bytesOf,
  sameBytes,
  countingBudget,
  countingMemory,
  jsonReachable,
  openOver,
  rootOf,
} from "./space-test-support.js";

describe("memory backend", () => {
  for (const testCase of storeConformanceCases)
    it(`conformance: ${testCase.name}`, () => testCase.run(async () => new MemoryBackend()));
});

describe("extent space: writing, publishing and reopening", () => {
  it("writes blobs, publishes a root naming them, and reopens to the same bytes", async () => {
    const backend = new MemoryBackend();
    const { space, budget } = await openOver(backend);
    const writer = space.beginWrite("t");
    const first = await writer.write([bytesOf(10, 1), bytesOf(5000, 2)]);
    const builder = writer.open(100);
    const pending = await builder.append([bytesOf(30, 3)]);
    expect(pending).toEqual([{ offset: 0, length: 30, checksum: expect.any(Number) as number }]);
    const second = await builder.seal();
    if (second === null) throw new Error("sealed empty");
    const blobs = [...first.blobs, ...second.blobs];
    expect(first.blobs[1]).toMatchObject({ extent: first.extent, offset: 10, length: 5000 });
    expect(await space.publish(rootOf(blobs), { writers: [writer], retired: [] })).toBe(1);
    expect(writer.state).toBe("published");
    const lease = await space.read(blobs, { budget });
    expect(lease.bytes).toEqual([bytesOf(10, 1), bytesOf(5000, 2), bytesOf(30, 3)]);
    lease.release();
    await space.audit();
    await space.close();

    const again = await openOver(backend, { reuseLock: true });
    expect(again.space.root).toEqual(rootOf(blobs));
    const reread = await again.space.read(blobs, { budget: again.budget });
    expect(reread.bytes[1]).toEqual(bytesOf(5000, 2));
    reread.release();
    await again.space.audit();
  });

  it("extends in place when it can, relocates at seal when it cannot, and keeps every blob", async () => {
    const { space, budget } = await openOver(new MemoryBackend());
    const writer = space.beginWrite("t");
    const grows = writer.open(DATA_ALIGN);
    await grows.append([bytesOf(4000, 1)]);
    expect(grows.extend(3 * DATA_ALIGN)).toBe(true);
    await grows.append([bytesOf(9000, 2)]);
    const grown = await grows.seal();
    // A neighbour blocks the next one: extend must reserve elsewhere and relocate at seal.
    const blocked = writer.open(DATA_ALIGN);
    await blocked.append([bytesOf(4090, 3)]);
    const neighbour = await writer.write([bytesOf(100, 4)]);
    expect(blocked.extend(2 * DATA_ALIGN)).toBe(false);
    await blocked.append([bytesOf(20, 5), bytesOf(5000, 6)]); // the first straddles the segments
    const moved = await blocked.seal();
    if (grown === null || moved === null) throw new Error("sealed empty");
    expect(space.stats.relocations).toBe(1);
    expect(moved.extent.length).toBe(4090 + 20 + 5000);
    const blobs = [...grown.blobs, ...neighbour.blobs, ...moved.blobs];
    const lease = await space.read(blobs, { budget });
    expect(lease.bytes).toEqual([
      bytesOf(4000, 1),
      bytesOf(9000, 2),
      bytesOf(100, 4),
      bytesOf(4090, 3),
      bytesOf(20, 5),
      bytesOf(5000, 6),
    ]);
    lease.release();
    await space.publish(rootOf(blobs), { writers: [writer], retired: [] });
    await space.audit();
  });

  it("interleaves builders with unaligned pages and crosses the 1 MiB write slab", async () => {
    const { space, budget } = await openOver(new MemoryBackend());
    const writer = space.beginWrite("t");
    const builders = [writer.open(3 << 20), writer.open(64), writer.open(5000)];
    const expected: Uint8Array[][] = [[], [], []];
    const pending: Array<Array<{ offset: number; length: number; checksum: number }>> = [
      [],
      [],
      [],
    ];
    let seed = 1;
    for (let round = 0; round < 60; round++)
      for (let index = 0; index < builders.length; index++) {
        const builder = builders[index];
        if (builder === undefined) continue;
        const size = index === 0 ? 33_333 + round * 7 : 777 + round;
        const part = bytesOf(size, seed++);
        if (builder.length + size > builder.capacity) builder.extend(size + 4096);
        pending[index]?.push(...(await builder.append([part])));
        expected[index]?.push(part);
      }
    const blobs: BlobRef[] = [];
    for (const builder of builders) {
      const sealed = await builder.seal();
      if (sealed === null) throw new Error("sealed empty");
      blobs.push(...sealed.blobs);
    }
    for (let start = 0; start < blobs.length; start += 40) {
      const lease = await space.read(blobs.slice(start, start + 40), { budget });
      lease.release();
    }
    const flat = expected.flat();
    for (let index = 0; index < blobs.length; index++) {
      const lease = await space.read([at(blobs, index)], { budget });
      expect(sameBytes(lease.bytes[0], flat[index]), `blob ${String(index)}`).toBe(true);
      lease.release();
    }
    await space.publish(rootOf(blobs), { writers: [writer], retired: [] });
    await space.audit();
  });

  it("aborts and discards free their space at once; an empty seal frees its reservation", async () => {
    const { space } = await openOver(new MemoryBackend());
    const writer = space.beginWrite("t");
    const { extent } = await writer.write([bytesOf(5000, 1)]);
    await writer.discard(extent);
    expect(writer.extents).toEqual([]);
    const again = await writer.write([bytesOf(10, 2)]);
    expect(again.extent.offset).toBe(extent.offset);
    expect(await writer.open(100).seal()).toBeNull();
    writer.open(DATA_ALIGN);
    await writer.abort();
    await writer.abort();
    expect(writer.state).toBe("aborted");
    expect(() => writer.open(1)).toThrow(TypeError);
    expect(space.stats.dataBytes).toBe(0);
    const other = space.beginWrite("u");
    expect((await other.write([bytesOf(1, 1)])).extent.offset).toBe(0);
  });

  it("refuses misuse before any effect", async () => {
    const { space } = await openOver(new MemoryBackend());
    const writer = space.beginWrite("t");
    const builder = writer.open(10);
    await expect(builder.append([bytesOf(DATA_ALIGN + 1, 1)])).rejects.toThrow(RangeError);
    await expect(
      writer.open(MAX_BLOB_BYTES + 10).append([bytesOf(MAX_BLOB_BYTES + 1, 1)]),
    ).rejects.toThrow(/MAX_BLOB_BYTES/u);
    await expect(writer.write([])).rejects.toThrow(RangeError);
    await expect(space.publish(rootOf([]), { writers: [writer], retired: [] })).rejects.toThrow(
      /unsealed builder/u,
    );
    await writer.abort();
    const published = space.beginWrite("p");
    const { extent } = await published.write([bytesOf(10, 1)]);
    await space.publish(rootOf([]), { writers: [published], retired: [] });
    await expect(space.publish(rootOf([]), { writers: [published], retired: [] })).rejects.toThrow(
      /is published/u,
    );
    const free: ExtentRef = { offset: 64 * DATA_ALIGN, length: 1 };
    await expect(space.publish(rootOf([]), { writers: [], retired: [free] })).rejects.toThrow(
      /not allocated/u,
    );
    await expect(
      space.publish(rootOf([]), { writers: [], retired: [extent, extent] }),
    ).rejects.toThrow(/twice/u);
    const held = space.beginWrite("h");
    const own = await held.write([bytesOf(10, 1)]);
    await expect(space.publish(rootOf([]), { writers: [], retired: [own.extent] })).rejects.toThrow(
      /unpublished writer/u,
    );
    await space.publish(rootOf([]), { writers: [], retired: [extent] });
    await expect(space.publish(rootOf([]), { writers: [], retired: [extent] })).rejects.toThrow(
      /already retired/u,
    );
    expect(space.failed).toBeNull();
  });
});

describe("extent space: per-builder staging buffers (writer budget)", () => {
  it("keeps interleaved builders' bytes exact, charges each buffer, and returns it at seal or abort", async () => {
    const { space, budget: reader } = await openOver(new MemoryBackend());
    const charge = countingBudget(3 * BUILDER_BUFFER_BYTES);
    const writer = space.beginWrite("columns", { budget: charge });
    const builders = Array.from({ length: 4 }, () => writer.open(1 << 20));
    // Three buffers fit the budget; the fourth builder shares the slab.
    expect(charge.used).toBe(3 * BUILDER_BUFFER_BYTES);
    const expected: Uint8Array[][] = [[], [], [], []];
    let seed = 1;
    for (let round = 0; round < 40; round++)
      for (let index = 0; index < builders.length; index++) {
        const part = bytesOf(7_000 + ((round * 37 + index * 11) % 9_000), seed++);
        await at(builders, index).append([part]);
        at(expected, index).push(part);
      }
    const blobs: BlobRef[] = [];
    for (const builder of builders.slice(0, 3)) {
      const sealed = await builder.seal();
      if (sealed === null) throw new Error("sealed empty");
      blobs.push(...sealed.blobs);
    }
    expect(charge.used).toBe(0);
    for (let index = 0; index < blobs.length; index++) {
      const lease = await space.read([at(blobs, index)], { budget: reader });
      expect(
        sameBytes(lease.bytes[0], expected.slice(0, 3).flat()[index]),
        `blob ${String(index)}`,
      ).toBe(true);
      lease.release();
    }
    const aborted = space.beginWrite("aborted", { budget: charge });
    await aborted.open(DATA_ALIGN).append([bytesOf(100, 1)]);
    expect(charge.used).toBe(DATA_ALIGN);
    await aborted.abort();
    expect(charge.used).toBe(0);
    await writer.abort();
    await (space as unknown as { settled(): Promise<void> }).settled();
    await space.audit();
  });
});

describe("extent space: two-generation reclaim (ruling R8)", () => {
  async function publishOne(space: ExtentSpace, retired: ExtentRef[], seed: number) {
    const writer = space.beginWrite(`w${String(seed)}`);
    const written = await writer.write([bytesOf(DATA_ALIGN, seed)]);
    await space.publish(rootOf(written.blobs), { writers: [writer], retired });
    return written;
  }

  it("reuses retired space only after the next publish is durable", async () => {
    const { space } = await openOver(new MemoryBackend());
    const a = await publishOne(space, [], 1);
    const b = await publishOne(space, [a.extent], 2);
    await (space as unknown as { settled(): Promise<void> }).settled();
    expect(space.stats.waitingBytes).toBe(DATA_ALIGN);
    // A is still the previous root's: new space must not land on it.
    const probe = space.beginWrite("probe");
    expect((await probe.write([bytesOf(10, 9)])).extent.offset).not.toBe(a.extent.offset);
    await probe.abort();
    await publishOne(space, [b.extent], 3);
    await (space as unknown as { settled(): Promise<void> }).settled();
    const reuse = space.beginWrite("reuse");
    expect((await reuse.write([bytesOf(10, 9)])).extent.offset).toBe(a.extent.offset);
    await reuse.abort();
    await space.audit();
  });

  it("also waits for every pin older than the retiring publish", async () => {
    const { space } = await openOver(new MemoryBackend());
    const a = await publishOne(space, [], 1);
    const pin = space.pin("slow reader");
    const b = await publishOne(space, [a.extent], 2);
    await publishOne(space, [b.extent], 3);
    await (space as unknown as { settled(): Promise<void> }).settled();
    expect(space.stats.oldestPin?.label).toBe("slow reader");
    expect(space.stats.waitingBytes).toBe(2 * DATA_ALIGN);
    pin.release();
    await (space as unknown as { settled(): Promise<void> }).settled();
    // A (retired by 2) is free now; B (retired by 3) waits for the next publish.
    expect(space.stats.waitingBytes).toBe(DATA_ALIGN);
    await space.audit();
  });
});

describe("extent space: open walk and audit", () => {
  it("frees crash orphans at open and holds what only the previous root reaches until a publish", async () => {
    const backend = new MemoryBackend();
    const { space } = await openOver(backend);
    const w1 = space.beginWrite("1");
    const a = await w1.write([bytesOf(DATA_ALIGN, 1)]);
    await space.publish(rootOf(a.blobs), { writers: [w1], retired: [] });
    const w2 = space.beginWrite("2");
    const b = await w2.write([bytesOf(DATA_ALIGN, 2)]);
    await space.publish(rootOf(b.blobs), { writers: [w2], retired: [a.extent] });
    const orphan = space.beginWrite("orphan");
    await orphan.write([bytesOf(3 * DATA_ALIGN, 3)]);
    await space.close();

    const { space: reopened } = await openOver(backend, { reuseLock: true });
    expect(reopened.previousRoot).toEqual(rootOf(a.blobs));
    await reopened.audit();
    expect(reopened.stats.waitingBytes).toBe(DATA_ALIGN);
    // The orphan's space at the tail was returned; A is kept until the first publish.
    expect(reopened.stats.dataBytes).toBe(2 * DATA_ALIGN);
    const w3 = reopened.beginWrite("3");
    const c = await w3.write([bytesOf(10, 4)]);
    expect(c.extent.offset).toBe(2 * DATA_ALIGN);
    await reopened.publish(rootOf([...b.blobs, ...c.blobs]), { writers: [w3], retired: [] });
    await (reopened as unknown as { settled(): Promise<void> }).settled();
    expect(reopened.stats.waitingBytes).toBe(0);
    await reopened.audit();
  });

  it("refuses to open over roots whose extents overlap", async () => {
    const backend = new MemoryBackend();
    const { space } = await openOver(backend);
    const writer = space.beginWrite("t");
    const { extent } = await writer.write([bytesOf(2 * DATA_ALIGN, 1)]);
    const bogus: ExtentRef = { offset: extent.offset + DATA_ALIGN, length: 10 };
    await space.publish(new TextEncoder().encode(JSON.stringify({ extents: [extent, bogus] })), {
      writers: [writer],
      retired: [],
    });
    await space.close();
    await expect(openOver(backend, { reuseLock: true })).rejects.toThrow(StoreCorruptionError);
  });

  it("audit finds a published extent the root does not reach (a leak)", async () => {
    const { space } = await openOver(new MemoryBackend());
    const writer = space.beginWrite("t");
    await writer.write([bytesOf(10, 1)]);
    await space.publish(rootOf([]), { writers: [writer], retired: [] });
    await expect(space.audit()).rejects.toThrow(/unaccounted \(a leak\)/u);
  });
});

describe("extent space: a rejected publish", () => {
  it("leaves its writers indeterminate and the space failed; reads continue", async () => {
    const backend = new MemoryBackend();
    let refuse = false;
    const { space, budget } = await openOver(backend, {
      wrap: (session) =>
        new Proxy(session, {
          get(target, key, receiver) {
            if (key === "publish" && refuse) return () => Promise.reject(new Error("device gone"));
            const value: unknown = Reflect.get(target, key, receiver);
            return typeof value === "function" ? (value as () => unknown).bind(target) : value;
          },
        }),
    });
    const writer = space.beginWrite("t");
    const { blobs, extent } = await writer.write([bytesOf(100, 1)]);
    refuse = true;
    await expect(space.publish(rootOf(blobs), { writers: [writer], retired: [] })).rejects.toThrow(
      "device gone",
    );
    expect(writer.state).toBe("indeterminate");
    expect(space.failed?.message).toBe("device gone");
    expect(() => space.beginWrite("next")).toThrow(SpaceFailedError);
    await expect(writer.abort()).rejects.toThrow(TypeError);
    await expect(space.publish(rootOf([]), { writers: [], retired: [] })).rejects.toThrow(
      SpaceFailedError,
    );
    const lease = await space.read(blobs, { budget });
    expect(lease.bytes[0]).toEqual(bytesOf(100, 1));
    lease.release();
    expect(extent.offset).toBe(0);
  });
});

describe("extent space: reads, leases and the cache", () => {
  it("charges every leased byte to the budget, once per blob, and refuses past MAX_READ_BYTES", async () => {
    const { space } = await openOver(new MemoryBackend());
    const writer = space.beginWrite("t");
    const { blobs } = await writer.write([bytesOf(1000, 1), bytesOf(2000, 2)]);
    const budget = countingBudget(10_000);
    const lease = await space.read([at(blobs, 0), at(blobs, 1), at(blobs, 0)], {
      budget,
    });
    expect(budget.used).toBe(3000);
    expect(lease.bytes[0]).toBe(lease.bytes[2]);
    lease.release();
    lease.release();
    expect(budget.used).toBe(0);
    await expect(space.read(blobs, { budget: countingBudget(2999) })).rejects.toThrow(
      ReadDeniedError,
    );
    const huge: BlobRef[] = Array.from({ length: 5 }, (_, index) => ({
      extent: { offset: index * MAX_BLOB_BYTES, length: MAX_BLOB_BYTES },
      offset: 0,
      length: MAX_BLOB_BYTES,
      checksum: 0,
    }));
    await expect(space.read(huge, { budget })).rejects.toThrow(/MAX_READ_BYTES/u);
    expect(MAX_READ_BYTES).toBe(4 * MAX_BLOB_BYTES);
    const outside = { ...at(blobs, 0), offset: 2990 };
    await expect(space.read([outside], { budget })).rejects.toThrow(StoreCorruptionError);
  });

  it("verifies once: storage corruption is refused on a miss, and cached bytes are trusted", async () => {
    const backend = new MemoryBackend();
    const { space, session } = await openOver(backend);
    const writer = space.beginWrite("t");
    const { blobs } = await writer.write([bytesOf(100, 1), bytesOf(100, 2)]);
    const budget = countingBudget(1 << 20);
    const cached = await space.read([at(blobs, 0)], { budget });
    cached.release();
    await session.writeData(0, bytesOf(200, 77)); // both blobs change on storage
    const hit = await space.read([at(blobs, 0)], { budget });
    expect(hit.bytes[0]).toEqual(bytesOf(100, 1));
    hit.release();
    await expect(space.read([at(blobs, 1)], { budget })).rejects.toThrow(/failed its checksum/u);
    expect(budget.used).toBe(0);
  });

  it("drops a freed extent's blobs before its space is reused", async () => {
    const { space, budget } = await openOver(new MemoryBackend());
    const w1 = space.beginWrite("1");
    const a = await w1.write([bytesOf(100, 1)]);
    await space.publish(rootOf(a.blobs), { writers: [w1], retired: [] });
    const lease = await space.read(a.blobs, { budget });
    lease.release();
    expect(space.peek(at(a.blobs, 0))).not.toBeNull();
    for (let round = 2; round <= 3; round++) {
      const writer = space.beginWrite(String(round));
      const next = await writer.write([bytesOf(DATA_ALIGN, round)]);
      await space.publish(rootOf(next.blobs), {
        writers: [writer],
        retired: round === 2 ? [a.extent] : [],
      });
    }
    await (space as unknown as { settled(): Promise<void> }).settled();
    expect(space.peek(at(a.blobs, 0))).toBeNull();
    const writer = space.beginWrite("reuse");
    const reused = await writer.write([bytesOf(100, 9)]);
    expect(reused.extent.offset).toBe(a.extent.offset);
    const fresh = await space.read(reused.blobs, { budget });
    expect(fresh.bytes[0]).toEqual(bytesOf(100, 9));
    fresh.release();
  });

  it("charges unpinned blobs to the cache and pinned ones to the reader, and evicts on demand", async () => {
    const memory = countingMemory(Infinity);
    const { space } = await openOver(new MemoryBackend(), { memory });
    expect(memory.used).toBe(FIXED_CACHE_BYTES);
    const writer = space.beginWrite("t");
    const { blobs } = await writer.write([bytesOf(1000, 1), bytesOf(3000, 2)]);
    const budget = countingBudget(1 << 20);
    const lease = await space.read(blobs, { budget });
    expect(memory.used).toBe(FIXED_CACHE_BYTES);
    expect(space.stats.cache.pinnedBytes).toBe(4000);
    expect(memory.cache?.evictableBytes).toBe(0);
    lease.release();
    expect(memory.used).toBe(FIXED_CACHE_BYTES + 4000 + 2 * ENTRY_OVERHEAD);
    expect(memory.cache?.evict(1)).toBeGreaterThan(0);
    expect(memory.used).toBe(FIXED_CACHE_BYTES + 3000 + ENTRY_OVERHEAD);
    expect(space.peek(at(blobs, 1))).not.toBeNull();
    await space.close();
    expect(memory.used).toBe(0);
  });

  it("keeps hot blobs through a scan larger than the cache (segmented LRU)", async () => {
    const memory = countingMemory(FIXED_CACHE_BYTES + 40 * (DATA_ALIGN + ENTRY_OVERHEAD));
    const { space } = await openOver(new MemoryBackend(), { memory });
    const writer = space.beginWrite("t");
    const hot = await writer.write([bytesOf(DATA_ALIGN, 1)]);
    const cold = await writer.write(
      Array.from({ length: 200 }, (_, index) => bytesOf(DATA_ALIGN, index)),
    );
    const budget = countingBudget(1 << 22);
    for (let use = 0; use < 2; use++) (await space.read(hot.blobs, { budget })).release();
    for (const blob of cold.blobs) (await space.read([blob], { budget })).release();
    expect(space.peek(at(hot.blobs, 0))).not.toBeNull();
    expect(memory.used).toBeLessThanOrEqual(memory.limit);
    expect(space.stats.cache.residentBytes).toBeGreaterThan(30 * DATA_ALIGN);
  });

  it("coalesces misses into slab reads", async () => {
    const { space, budget } = await openOver(new MemoryBackend());
    const writer = space.beginWrite("t");
    const { blobs } = await writer.write(
      Array.from({ length: 64 }, (_, index) => bytesOf(16_384, index)),
    );
    const before = space.stats.cache.platformReads;
    const lease = await space.read(blobs.slice(0, 63), { budget });
    lease.release();
    expect(space.stats.cache.platformReads - before).toBe(1);
  });
});

describe("extent space: test support", () => {
  it("opens a fresh space over a fresh session", async () => {
    const backend = new MemoryBackend();
    const session: StoreSession = await backend.open(await backend.acquire());
    const space = await openExtentSpace(session, {
      reachable: jsonReachable,
      budget: countingBudget(Infinity),
      registerCache: () => countingMemory(Infinity),
    });
    expect(space.root).toBeNull();
    expect(space.generation).toBe(0);
  });
});

/** Each probe of review B1-p2a (161efd9), kept as a test. */
describe("review B1-p2a findings", () => {
  const options = () => ({
    reachable: jsonReachable,
    budget: countingBudget(Infinity),
    registerCache: () => countingMemory(Infinity),
  });

  it("1: refuses roots that name one offset with two lengths, before truncating anything", async () => {
    const backend = new MemoryBackend();
    const session = await backend.open(await backend.acquire());
    await session.writeData(0, bytesOf(8192, 1));
    await session.publish(rootOf([], [{ offset: 0, length: 8192 }]));
    await session.publish(rootOf([], [{ offset: 0, length: 4096 }]));
    await expect(openExtentSpace(session, options())).rejects.toThrow(/different extents at 0/u);
    // The previous root's data is untouched.
    expect(await session.dataLength()).toBe(8192);
    const [bytes] = await session.readData([{ offset: 0, length: 8192 }]);
    expect(bytes).toEqual(bytesOf(8192, 1));
  });

  it("2: reads distinct blobs at one address by full identity, zero-length ones included", async () => {
    const { space, budget } = await openOver(new MemoryBackend());
    const writer = space.beginWrite("zero");
    const written = await writer.write([new Uint8Array(0), new Uint8Array([42])]);
    const both = await space.read(written.blobs, { budget });
    expect(both.bytes.map((bytes) => [...bytes])).toEqual([[], [42]]);
    both.release();
    const bad = { ...at(written.blobs, 1), checksum: 123 };
    await expect(space.read([at(written.blobs, 1), bad], { budget })).rejects.toThrow(
      /failed its checksum/u,
    );
    const wrongEmpty = { ...at(written.blobs, 0), checksum: 7 };
    await expect(space.read([wrongEmpty], { budget })).rejects.toThrow(StoreCorruptionError);
    // A prefix blob at the same address as a longer one: both are verified and returned.
    const pair = await writer.write([new Uint8Array([1, 2])]);
    const whole = at(pair.blobs, 0);
    const prefix = { ...whole, length: 1, checksum: xxHash32(new Uint8Array([1])) };
    const mixed = await space.read([whole, prefix], { budget });
    expect(mixed.bytes.map((bytes) => [...bytes])).toEqual([[1, 2], [1]]);
    mixed.release();
    expect(budget.used).toBe(0);
    const fractional = { ...whole, offset: 0.5 };
    await expect(space.read([fractional], { budget })).rejects.toThrow(StoreCorruptionError);
  });

  it("3: an abort racing a seal frees each range once (plain, empty and relocating seals)", async () => {
    for (const kind of ["plain", "empty", "relocating"] as const) {
      const { space } = await openOver(new MemoryBackend());
      const writer = space.beginWrite(kind);
      const builder = writer.open(DATA_ALIGN);
      if (kind !== "empty") await builder.append([bytesOf(4000, 2)]);
      if (kind === "relocating") {
        await writer.write([bytesOf(10, 3)]); // blocks in-place growth
        expect(builder.extend(DATA_ALIGN)).toBe(false);
        await builder.append([bytesOf(500, 4)]);
      }
      const results = await Promise.allSettled([builder.seal(), writer.abort()]);
      expect(results[1].status, kind).toBe("fulfilled");
      if (results[0].status === "rejected")
        expect(String(results[0].reason), kind).toMatch(/aborted during the seal/u);
      await (space as unknown as { settled(): Promise<void> }).settled();
      expect(space.failed, kind).toBeNull();
      expect(space.stats.writerBytes, kind).toBe(0);
      expect(space.stats.dataBytes, kind).toBe(0);
      await space.audit();
    }
  });

  it("3: a relocation that fails leaves its ranges to the writer's abort", async () => {
    let failWrites = false;
    const { space } = await openOver(new MemoryBackend(), {
      wrap: (session) =>
        new Proxy(session, {
          get(target, key, receiver) {
            if (key === "writeData" && failWrites)
              return () => Promise.reject(new Error("device gone"));
            const value: unknown = Reflect.get(target, key, receiver);
            return typeof value === "function" ? (value as () => unknown).bind(target) : value;
          },
        }),
    });
    const writer = space.beginWrite("t");
    const builder = writer.open(DATA_ALIGN);
    await builder.append([bytesOf(4000, 1)]);
    await writer.write([bytesOf(10, 2)]);
    expect(builder.extend(DATA_ALIGN)).toBe(false);
    await builder.append([bytesOf(500, 3)]);
    failWrites = true;
    await expect(builder.seal()).rejects.toThrow("device gone");
    failWrites = false;
    await writer.abort();
    await (space as unknown as { settled(): Promise<void> }).settled();
    expect(space.stats.dataBytes).toBe(0);
    await space.audit();
  });

  it("4: refuses an extent whose exact end passes the data length, or with unsafe coordinates", async () => {
    for (const extent of [
      { offset: 0, length: 4000 },
      { offset: 0, length: 0.5 },
      { offset: 4096.25, length: 1 },
      { offset: -4096, length: 1 },
      { offset: 0, length: Number.MAX_SAFE_INTEGER + 1 },
    ]) {
      const backend = new MemoryBackend();
      const session = await backend.open(await backend.acquire());
      await session.writeData(0, new Uint8Array([1]));
      await session.publish(rootOf([], [extent]));
      await expect(openExtentSpace(session, options()), JSON.stringify(extent)).rejects.toThrow(
        StoreCorruptionError,
      );
      expect(await session.dataLength()).toBe(1);
    }
  });

  it("5: unpublished space being released is not garbage debt", async () => {
    let unblock!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => (unblock = resolve));
    const started = new Promise<void>((resolve) => (entered = resolve));
    const { space } = await openOver(new MemoryBackend(), {
      wrap: (session) =>
        new Proxy(session, {
          get(target, key) {
            if (key === "releaseData")
              return async (ranges: Parameters<StoreSession["releaseData"]>[0]) => {
                entered();
                await gate;
                return session.releaseData(ranges);
              };
            const value: unknown = Reflect.get(target, key);
            return typeof value === "function" ? (value as () => unknown).bind(target) : value;
          },
        }),
    });
    const writer = space.beginWrite("stats");
    await writer.write([bytesOf(4096, 1)]);
    const pending = writer.abort();
    await started;
    expect(space.stats.waitingBytes).toBe(0);
    expect(space.stats.releasingBytes).toBe(4096);
    unblock();
    await pending;
  });
});
