import { afterAll, expect, it, vi } from "vitest";
import { createReach, RETIREMENT_TICKETS_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import {
  MetadataJournal,
  MetadataWorkspace,
  METADATA_WORKSPACE_BYTES,
} from "../storage/metadata-journal.js";
import { RetirementTickets } from "../storage/retirement-tickets.js";
import {
  saveExtentInput,
  saveNativeList,
  disposeSnapshot,
  reconstructMap,
} from "../storage/extent-snapshot.js";
import { PageCache } from "../storage/page-cache.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { MAX_RUN_ARENA_BYTES, MIN_RUN_ARENA_BYTES } from "../storage/bounded-free-space.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";
import { ScriptedMetadataPages } from "./metadata-test-support.js";
import { METADATA_PAGE_BYTES } from "../storage/contract.js";
import { encodeListPage, type ExtentListPage } from "../storage/extent-list.js";
import { openPairs } from "../storage/metadata-extents.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { ReadDeniedError, type ExtentRef } from "../storage/extents.js";

const reach = createReach("disk-derived retirement tickets", RETIREMENT_TICKETS_REACH);
const BASE = 2 ** 40;
for (const seed of seedsFor("retirement-tickets", [1581, 1582, 1583, 1584]))
  it(`keeps a pin-delayed disk queue and reclaims in bounded batches at full pressure (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      governor = new Governor(4 << 20);
    const backend = new MemoryBackend(),
      lease = await backend.acquire(),
      session = await backend.open(lease),
      cache = new PageCache(session);
    cache.attach(governor.registerCache(cache));
    const startup = await governor.admit({
      label: "ticket workspace",
      minimumBytes: METADATA_WORKSPACE_BYTES,
      spillable: false,
    });
    const workspace = MetadataWorkspace.create(startup),
      pages = new ScriptedMetadataPages(),
      journal = new MetadataJournal(pages, workspace),
      tickets = new RetirementTickets(journal);
    workspace.transfer((owner) => governor.registerCache(owner));
    startup.close();
    const caller = await governor.admit({
      label: "caller closes only after publication",
      minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
      spillable: false,
    });
    const loan = seed % 2 === 0 ? await caller.borrow(64 * 1024) : null;
    const sourceRequests = vi.spyOn(startup, "allocate").mockImplementation(() => {
      throw new Error("Unexpected transient allocation request");
    });
    const newAdmissions = vi.spyOn(governor, "admit").mockImplementation(() => {
      throw new Error("Unexpected publication admission wait");
    });
    let frames = 0;
    const originalArray = Uint8Array;
    vi.stubGlobal(
      "Uint8Array",
      new Proxy(originalArray, {
        construct(target, args, constructor) {
          if (args[0] === METADATA_PAGE_BYTES) {
            frames++;
            expect(frames).toBeLessThanOrEqual(2);
          }
          return Reflect.construct(target, args, constructor) as Uint8Array;
        },
      }),
    );
    const originalRun = workspace.run.bind(workspace);
    const phases = vi.spyOn(workspace, "run").mockImplementation(async (phase, work) => {
      frames = 0;
      return originalRun(phase, async () => {
        expect(frames).toBe(2);
        return work();
      });
    });
    const inSlab = <T>(work: (io: { slab: Uint8Array }) => Promise<T>) =>
      cache.withReadSlab((slab) => work({ slab }));
    let model = Array.from({ length: 1200 }, (_, index) => ({
        offset: BASE + index * 8192,
        length: 1 + Math.floor(random() * 4096),
      })),
      nextIndex = 1200;
    let current = await inSlab((io) =>
      saveExtentInput(journal, model, true, Infinity, undefined, io),
    );
    const expected = new Map<number, number>(),
      retiredByGeneration = new Map<number, readonly ExtentRef[]>();
    const freed = new Set<number>();
    let batches = 0;
    const release = async (
      ranges: ReadonlyArray<{ readonly offset: number; readonly length: number }>,
    ) => {
      expect(ranges.length).toBeGreaterThan(0);
      expect(ranges.length).toBeLessThanOrEqual(256);
      expect(() => workspace.frame(2)).toThrow(); // only the worklist cursor/control release phase remains
      for (const range of ranges) {
        expect(expected.get(range.offset)).toBe(range.length);
        expect(freed.has(range.offset)).toBe(false);
        freed.add(range.offset);
      }
      batches++;
      await Promise.resolve();
    };
    try {
      for (let generation = 1; generation <= 8; generation++) {
        const retired = model.filter((_, index) => index % 4 === generation % 4);
        const kept = model.filter((_, index) => index % 4 !== generation % 4);
        model = [
          ...kept,
          ...Array.from({ length: retired.length }, () => ({
            offset: BASE + nextIndex++ * 8192,
            length: 1 + Math.floor(random() * 4096),
          })),
        ].sort((a, b) => a.offset - b.offset);
        const after = await inSlab((io) =>
          saveExtentInput(journal, model, true, Infinity, undefined, io),
        );
        for (const extent of retired) expected.set(extent.offset, 4096);
        retiredByGeneration.set(generation, retired);
        await tickets.prepare(generation, current, after, retired.length * 4096);
        tickets.commit();
        await disposeSnapshot(journal, current);
        current = after;
        await inSlab((io) => tickets.reclaim(generation, 0, release, undefined, io));
        expect(freed.size).toBe(0);
      }
      reach.reach("pin delayed disk backlog");
      reach.reach("source owners released");
      reach.reach("full ordinary capacity");
      if (loan !== null) {
        expect(caller.loaned).toBe(64 * 1024);
        reach.reach("caller loan remains held");
      }
      await inSlab((io) => tickets.reclaim(8, null, release, undefined, io));
      expect(freed.size).toBe(7 * 300);
      expect(tickets.waitingBytes).toBe(300 * 4096);
      for (const extent of retiredByGeneration.get(8) ?? [])
        expect(freed.has(extent.offset)).toBe(false);
      reach.reach("next generation guard");
      await inSlab((io) => tickets.reclaim(9, null, release, undefined, io));
      expect(freed.size).toBe(expected.size);
      expect(tickets.waitingBytes).toBe(0);
      expect(batches).toBe(16);
      reach.reach("two bounded batches per ticket");
      expect(sourceRequests).not.toHaveBeenCalled();
      expect(newAdmissions).not.toHaveBeenCalled();
      expect(governor.usedBytes).toBe((3 << 20) + (loan === null ? 0 : 64 * 1024));
      expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
      await disposeSnapshot(journal, current);
      current = 0;
    } finally {
      phases.mockRestore();
      vi.unstubAllGlobals();
      newAdmissions.mockRestore();
      sourceRequests.mockRestore();
      loan?.repay();
      caller.close();
      startup.close();
      workspace.close();
      cache.close();
      await session.close();
      await lease.release();
    }
    expect(governor.usedBytes).toBe(0);
  });

async function construction() {
  const backend = new MemoryBackend(),
    lease = await backend.acquire(),
    session = await backend.open(lease),
    cache = new PageCache(session),
    memory = countingMemory(Infinity);
  cache.attach(memory);
  const budget = countingBudget(METADATA_WORKSPACE_BYTES + MAX_RUN_ARENA_BYTES),
    workspace = MetadataWorkspace.create(budget),
    pages = new ScriptedMetadataPages(),
    journal = new MetadataJournal(pages, workspace);
  return {
    cache,
    session,
    memory,
    budget,
    workspace,
    pages,
    journal,
    async close() {
      workspace.close();
      await cache.settled();
      cache.close();
      await session.close();
      await lease.release();
      expect(budget.used).toBe(0);
      expect(memory.used).toBe(0);
    },
  };
}

it("reconstructs previous-only ownership with one arena admission, without a retirement index", async () => {
  const f = await construction();
  try {
    const current = await saveExtentInput(
      f.journal,
      (function* () {
        for (let i = 1; i < 40_000; i += 2) yield { offset: i * 4096, length: 4096 };
      })(),
      true,
      40_000 * 4096,
    );
    const previous = await saveExtentInput(
      f.journal,
      (function* () {
        for (let i = 0; i < 40_000; i++) yield { offset: i * 4096, length: 4096 };
      })(),
      true,
      40_000 * 4096,
    );
    const allocation = vi.spyOn(f.budget, "allocate");
    const map = await reconstructMap(
      f.journal,
      f.cache,
      current,
      previous,
      40_000 * 4096,
      f.budget,
    );
    try {
      expect(map.freeBytes).toBe(0);
      expect(map.untrackedFreeBytes).toBe(0);
      expect(map.end).toBe(40_000 * 4096);
      expect(map.bytes).toBe(MIN_RUN_ARENA_BYTES);
      expect(allocation.mock.calls).toEqual([[MIN_RUN_ARENA_BYTES]]);
    } finally {
      map.close();
      allocation.mockRestore();
    }
    await disposeSnapshot(f.journal, current);
    await disposeSnapshot(f.journal, previous);
  } finally {
    await f.close();
  }
});

it("keeps capped fragmentation debt and recovers it only when the rebuilt gaps coalesce", async () => {
  const f = await construction(),
    count = 20_000,
    dataLength = (count * 2 - 1) * 4096;
  try {
    const root = await saveExtentInput(
      f.journal,
      (function* () {
        for (let i = 0; i < count; i++) yield { offset: i * 8192, length: 4096 };
      })(),
      true,
      dataLength,
    );
    let debt = 0;
    for (let reopen = 0; reopen < 2; reopen++) {
      const map = await reconstructMap(f.journal, f.cache, root, root, dataLength, f.budget);
      try {
        expect(map.bytes).toBe(MAX_RUN_ARENA_BYTES);
        expect(map.runCount).toBe(map.capacity);
        expect(map.untrackedFreeBytes).toBe((count - 1 - map.capacity) * 4096);
        if (reopen === 0) debt = map.untrackedFreeBytes;
        else expect(map.untrackedFreeBytes).toBe(debt);
        const untracked = (map.capacity * 2 + 1) * 4096;
        expect(map.allocateAt(untracked, 4096)).toBe(false);
        expect(map.allocationState(untracked, 4096)).toBe("unknown");
      } finally {
        map.close();
      }
    }
    const coalesced = await saveExtentInput(
      f.journal,
      [{ offset: dataLength - 4096, length: 4096 }],
      true,
      dataLength,
    );
    const recovered = await reconstructMap(
      f.journal,
      f.cache,
      coalesced,
      coalesced,
      dataLength,
      f.budget,
    );
    try {
      expect(recovered.runCount).toBe(1);
      expect(recovered.untrackedFreeBytes).toBe(0);
      expect(recovered.freeBytes).toBe(dataLength - 4096);
    } finally {
      recovered.close();
    }
    await disposeSnapshot(f.journal, root);
    await disposeSnapshot(f.journal, coalesced);
  } finally {
    await f.close();
  }
});

it("refuses arena admission before allocating its backing and returns it after every failed native close", async () => {
  let writes = 0;
  for (let fault = 0; fault <= writes; fault++) {
    const f = await construction();
    try {
      const a = await saveExtentInput(f.journal, [{ offset: 4096, length: 4096 }], true, 8192);
      const b = await saveExtentInput(f.journal, [], true, 8192);
      if (fault === 0) {
        const denied = countingBudget(MIN_RUN_ARENA_BYTES - 1);
        await expect(reconstructMap(f.journal, f.cache, a, b, 8192, denied)).rejects.toBeInstanceOf(
          ReadDeniedError,
        );
        expect(denied.used).toBe(0);
      }
      const before = f.pages.writes;
      if (fault !== 0) f.pages.failAt = before + fault;
      if (fault === 0) {
        const map = await reconstructMap(f.journal, f.cache, a, b, 8192, f.budget);
        map.close();
        writes = f.pages.writes - before;
        expect(writes).toBeGreaterThan(0);
        await disposeSnapshot(f.journal, a);
        await disposeSnapshot(f.journal, b);
      } else {
        await expect(reconstructMap(f.journal, f.cache, a, b, 8192, f.budget)).rejects.toThrow(
          "scripted metadata write failure",
        );
        expect(f.budget.used).toBe(METADATA_WORKSPACE_BYTES);
      }
    } finally {
      await f.close();
    }
  }
});

afterAll(() => reach.assertReached());

it("saves payloads and scattered physical list extents without sorting every payload pair", async () => {
  const f = await construction();
  try {
    const pages: ExtentListPage[] = [];
    for (const [first, count, physical] of [
      [0, 4096, 69632],
      [4096, 1, 0],
    ] as const) {
      const bytes = encodeListPage(
        Array.from({ length: count }, (_, i) => ({ offset: BASE + (first + i) * 8192, length: 1 })),
      );
      pages.push({
        first: BASE + first * 8192,
        count,
        blob: {
          extent: { offset: physical, length: bytes.length },
          offset: 0,
          length: bytes.length,
          checksum: xxHash32(bytes),
        },
      });
      await f.session.writeData(physical, bytes);
    }
    const handle = await f.cache.withReadSlab((slab) =>
      saveNativeList(f.journal, f.cache, pages, BASE + 4097 * 8192, undefined, { slab }),
    );
    await f.workspace.run("source", async () => {
      const pairs = await openPairs(f.journal, handle, 1, true);
      let index = 0;
      try {
        while (await pairs.next()) {
          const expected =
            index === 0
              ? { offset: 0, length: 32 }
              : index === 1
                ? { offset: 69632, length: 65552 }
                : { offset: BASE + (index - 2) * 8192, length: 1 };
          expect({ offset: pairs.offset, length: pairs.length }).toEqual(expected);
          index++;
        }
        expect(index).toBe(4099);
      } finally {
        await pairs.close();
      }
    });
    await disposeSnapshot(f.journal, handle);
    const self = encodeListPage([{ offset: 0, length: 32 }]);
    await f.session.writeData(0, self);
    const page = {
      first: 0,
      count: 1,
      blob: { extent: { offset: 0, length: 32 }, offset: 0, length: 32, checksum: xxHash32(self) },
    };
    await expect(
      f.cache.withReadSlab((slab) =>
        saveNativeList(f.journal, f.cache, [page], 32, undefined, { slab }),
      ),
    ).rejects.toThrow("Repeated private extent pair");
    expect(f.budget.used).toBe(METADATA_WORKSPACE_BYTES);
  } finally {
    await f.close();
  }
});

it("unlinks an aborted provisional ticket and restores both snapshot references", async () => {
  const f = await construction(),
    tickets = new RetirementTickets(f.journal);
  try {
    const a = await saveExtentInput(f.journal, [{ offset: 0, length: 1 }], true, 1),
      b = await saveExtentInput(f.journal, [], true, 1);
    await tickets.prepare(0, a, b, 4096);
    tickets.commit();
    await tickets.prepare(1, b, a, 4096);
    await tickets.abort();
    expect(tickets.waitingBytes).toBe(4096);
    let visits = 0;
    await tickets.forEachRoot(async () => {
      visits++;
    });
    expect(visits).toBe(2);
    await f.workspace.run("spool", async () => {
      expect((await f.journal.describe(a)).refs).toBe(2);
      expect((await f.journal.describe(b)).refs).toBe(2);
    });
    await tickets.reclaim(1, null, async (ranges) => {
      expect(ranges).toEqual([{ offset: 0, length: 4096 }]);
    });
    await disposeSnapshot(f.journal, a);
    await disposeSnapshot(f.journal, b);
  } finally {
    await f.close();
  }
});
