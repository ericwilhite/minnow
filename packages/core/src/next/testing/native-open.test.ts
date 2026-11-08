/** R25 reopen: native lists are already immutable recovery authority. Opening must not copy
 * their payloads into private storage. Descriptor credit survives startup close, and the first
 * publication/audit can form the R8 ticket at a full ordinary cap without waiting on its caller.
 * Virtual payload coordinates isolate metadata; real native datasets are measured separately.
 */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { createReach, NATIVE_OPEN_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { Governor, type Loan } from "../memory/governor.js";
import { StoreCorruptionError, type MetadataStore } from "../storage/contract.js";
import { encodeListPage, LIST_PAGE_EXTENTS, type ExtentListPage } from "../storage/extent-list.js";
import {
  ReadDeniedError,
  type ByteBudget,
  type ExtentRef,
  type ExtentSpace,
  type ReachableExtents,
} from "../storage/extents.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { FIXED_CACHE_BYTES } from "../storage/page-cache.js";
import { openExtentSpace } from "../storage/space.js";

const reach = createReach("direct native-list reopen", NATIVE_OPEN_REACH);
const shape = fc.record({
  count: fc.integer({ min: 4097, max: 12000 }),
  stride: fc.constantFrom(8192, 12288),
  divisor: fc.constantFrom(2, 3, 5),
  high: fc.boolean(),
  loan: fc.boolean(),
  reversePages: fc.boolean(),
  first: fc.constantFrom("publish", "audit", "close"),
});
type Shape = typeof shape extends fc.Arbitrary<infer Value> ? Value : never;

async function fixture(
  s: Shape,
  perPage = LIST_PAGE_EXTENTS,
  options: { payloadBase?: number; disagreement?: boolean } = {},
) {
  const backend = new MemoryBackend(),
    lease = await backend.acquire(),
    session = await backend.open(lease);
  const base = options.payloadBase ?? (s.high ? 2 ** 40 : 1 << 20);
  const lists: ExtentListPage[][] = [[], []];
  let pageAddress = 0,
    activeLeases = 0,
    writes = 0;
  let writeHook: ((page: number, bytes: Uint8Array) => void) | null = null;
  let readHook: (() => void) | null = null;
  let virtualEnd = base + (s.count - 1) * s.stride + 4096;
  const payload = function* (root: number): Iterable<ExtentRef> {
    for (let i = 0; i < s.count; i++)
      if (root === 1 || i % s.divisor === 0)
        yield {
          offset: base + i * s.stride,
          length: 1 + (i % 257) + (options.disagreement === true && root === 2 && i === 0 ? 1 : 0),
        };
  };
  const physicalStride = perPage === 1 ? 4096 : 73728;
  for (const root of [1, 2]) {
    let batch: ExtentRef[] = [];
    const entries = root === 1 ? s.count : Math.floor((s.count - 1) / s.divisor) + 1;
    const pages = Math.ceil(entries / perPage),
      firstAddress = pageAddress;
    let pageIndex = 0;
    const flush = async () => {
      if (batch.length === 0) return;
      const bytes = encodeListPage(batch);
      const offset =
        firstAddress + (s.reversePages ? pages - 1 - pageIndex : pageIndex) * physicalStride;
      pageIndex++;
      await session.writeData(offset, bytes);
      lists[root - 1]?.push({
        first: batch[0]?.offset ?? 0,
        count: batch.length,
        blob: {
          extent: { offset, length: bytes.length },
          offset: 0,
          length: bytes.length,
          checksum: xxHash32(bytes),
        },
      });
      batch = [];
    };
    for (const extent of payload(root)) {
      batch.push(extent);
      if (batch.length === perPage) await flush();
    }
    await flush();
    pageAddress += pages * physicalStride;
    await session.publish(Uint8Array.of(root));
  }
  const originalWrite = session.writeData.bind(session);
  const originalRead = session.readDataInto.bind(session);
  session.readDataInto = async (...args) => {
    await originalRead(...args);
    readHook?.();
  };
  session.dataLength = async () => virtualEnd;
  session.releaseData = async (ranges) => {
    expect(ranges.length).toBeLessThanOrEqual(256);
    for (const range of ranges) {
      if (range.offset < base) await originalWrite(range.offset, new Uint8Array(range.length));
      else if (range.offset + range.length >= virtualEnd)
        virtualEnd = Math.min(virtualEnd, range.offset);
    }
  };
  const backing = session.metadata?.();
  if (backing === undefined || backing === null) throw new Error("Missing private metadata store");
  const metadata: MetadataStore = {
    read: (page, target) => backing.read(page, target),
    write: async (page, bytes) => {
      writes++;
      writeHook?.(page, bytes);
      await backing.write(page, bytes);
    },
    reset: () => backing.reset(),
  };
  session.metadata = () => metadata;
  const fromRoot = (root: Uint8Array) => root[0] ?? 0;
  const reachable: ReachableExtents = Object.assign(
    function* (root: Uint8Array) {
      yield* payload(fromRoot(root));
      for (const page of lists[fromRoot(root) - 1] ?? []) yield page.blob.extent;
    },
    {
      openList(root: Uint8Array, budget: ByteBudget) {
        const list = lists[fromRoot(root) - 1] ?? [];
        const charge = 256 + list.length * 256;
        if (!budget.allocate(charge)) throw new ReadDeniedError(charge);
        activeLeases++;
        let held = true;
        return {
          list,
          release() {
            if (!held) return;
            held = false;
            activeLeases--;
            budget.free(charge);
          },
        };
      },
    },
  );
  return {
    session,
    reachable,
    currentList: lists[1] ?? [],
    get writes() {
      return writes;
    },
    get activeLeases() {
      return activeLeases;
    },
    onWrite(hook: ((page: number, bytes: Uint8Array) => void) | null) {
      writeHook = hook;
    },
    onRead(hook: (() => void) | null) {
      readHook = hook;
    },
    async close() {
      await session.close();
      await lease.release();
    },
  };
}
for (const seed of seedsFor("native-open", [1751, 1752]))
  it(`defers payload copies and preserves progress with startup closed (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(shape, async (s) => {
        reach.beginHistory();
        const h = await fixture(s),
          governor = new Governor(8 << 20);
        let startup = await governor.admit({
          label: "native startup",
          minimumBytes: 1 << 20,
          spillable: true,
        });
        let space: ExtentSpace | null = null;
        let pressure: Awaited<ReturnType<Governor["admit"]>> | null = null;
        let loan: Loan | null = null;
        try {
          space = await openExtentSpace(h.session, {
            reachable: h.reachable,
            budget: startup,
            registerCache: (c) => governor.registerCache(c),
          });
          expect(h.writes, "ordinary open must not materialize native payload lists").toBe(0);
          expect(h.activeLeases).toBe(2);
          reach.reach("no payload materialization");
          expect(startup.allocated).toBe(0);
          startup.close();
          expect(governor.usedBytes).toBe(FIXED_CACHE_BYTES + space.stats.retainedBytes);
          reach.reach("startup closed with charged descriptors");
          pressure = await governor.admit({
            label: "caller closes after publication",
            minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
            spillable: false,
          });
          if (s.loan) {
            loan = await pressure.borrow(64 << 10);
            reach.reach("caller loan held");
          }
          if (s.first === "audit") {
            await space.audit();
            reach.reach("audit first");
          }
          if (s.first === "close") {
            await space.close();
            space = null;
            expect(h.writes).toBe(0);
            reach.reach("unused close has no payload copy");
          } else {
            await space.publish(Uint8Array.of(2), {
              writers: [],
              retired: [],
              list: h.currentList,
            });
            await space.audit();
            expect(space.stats.waitingBytes).toBe(0);
            expect(h.writes).toBeGreaterThan(0);
            reach.reach("first publication and reclaim");
            await space.close();
            space = null;
          }
          expect(h.activeLeases).toBe(0);
          if (loan) expect(pressure.loaned).toBe(loan.bytes);
          loan?.repay();
          loan = null;
          pressure.close();
          pressure = null;
          expect(governor.usedBytes).toBe(0);
          startup = await governor.admit({
            label: "reopen",
            minimumBytes: 1 << 20,
            spillable: true,
          });
          space = await openExtentSpace(h.session, {
            reachable: h.reachable,
            budget: startup,
            registerCache: (c) => governor.registerCache(c),
          });
          expect(space.root?.[0]).toBe(2);
          await space.audit();
          reach.reach("reopen and audit");
          if (s.high) reach.reach("full safe-integer coordinates");
          if (s.reversePages) reach.reach("unordered physical allocations");
          expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
        } finally {
          loan?.repay();
          pressure?.close();
          await space?.close();
          startup.close();
          await h.close();
        }
        expect(h.activeLeases).toBe(0);
        expect(governor.usedBytes).toBe(0);
        reach.reach("all credit returned");
      }),
      { seed, numRuns: 40 },
    );
  }, 120_000);
for (const seed of seedsFor("native-open-cleanup", [1753, 1754, 1755, 1756]))
  it(`returns descriptor credit after refusal and cancellation (${String(seed)})`, async () => {
    for (const refused of [0, 1]) {
      const s: Shape = {
        count: 4097 + (seed % 1000),
        stride: 8192,
        divisor: 2,
        high: true,
        loan: false,
        reversePages: true,
        first: "publish",
      };
      const h = await fixture(s),
        governor = new Governor(8 << 20);
      const startup = await governor.admit({
        label: "refused native open",
        minimumBytes: 1 << 20,
        spillable: true,
      });
      let descriptor = 0;
      try {
        await expect(
          openExtentSpace(h.session, {
            reachable: h.reachable,
            budget: {
              ordinarySource: () => startup,
              allocate(bytes) {
                if (bytes < 16 * 1024 && descriptor++ === refused) return false;
                return startup.allocate(bytes);
              },
              free: (bytes) => startup.free(bytes),
            },
            registerCache: (c) => governor.registerCache(c),
          }),
        ).rejects.toBeInstanceOf(ReadDeniedError);
        expect(h.activeLeases).toBe(0);
        expect(startup.allocated).toBe(0);
        expect(governor.usedBytes).toBe(startup.held);
      } finally {
        startup.close();
        await h.close();
      }
      expect(governor.usedBytes).toBe(0);
    }
    const s: Shape = {
      count: 4097 + (seed % 1000),
      stride: 8192,
      divisor: 3,
      high: true,
      loan: false,
      reversePages: true,
      first: "publish",
    };
    const h = await fixture(s),
      governor = new Governor(8 << 20);
    const startup = await governor.admit({
      label: "aborted native open",
      minimumBytes: 1 << 20,
      spillable: true,
    });
    const controller = new AbortController(),
      reason = new Error("abort native list decode");
    h.onRead(() => controller.abort(reason));
    try {
      await expect(
        openExtentSpace(h.session, {
          reachable: h.reachable,
          budget: startup,
          signal: controller.signal,
          registerCache: (c) => governor.registerCache(c),
        }),
      ).rejects.toBe(reason);
      expect(h.activeLeases).toBe(0);
      expect(startup.allocated).toBe(0);
    } finally {
      startup.close();
      await h.close();
    }
    expect(governor.usedBytes).toBe(0);
  });

for (const seed of seedsFor("native-open-handoff", [1761, 1762, 1763, 1764]))
  it(`keeps native authority until a complete lazy handoff (${String(seed)})`, async () => {
    const s: Shape = {
      count: 4097 + (seed % 1000),
      stride: 8192,
      divisor: 2,
      high: true,
      loan: false,
      reversePages: true,
      first: "publish",
    };
    const h = await fixture(s),
      governor = new Governor(8 << 20);
    const startup = await governor.admit({
      label: "lazy handoff",
      minimumBytes: 1 << 20,
      spillable: true,
    });
    let space: ExtentSpace | null = null;
    try {
      space = await openExtentSpace(h.session, {
        reachable: h.reachable,
        budget: startup,
        registerCache: (c) => governor.registerCache(c),
      });
      startup.close();
      const controller = new AbortController(),
        reason = new Error("abort lazy handoff");
      let copyingPayload = false,
        aborted = false;
      h.onRead(() => {
        copyingPayload = true;
      });
      h.onWrite(() => {
        if (copyingPayload && !aborted) {
          aborted = true;
          controller.abort(reason);
        }
      });
      await expect(
        space.publish(Uint8Array.of(2), {
          writers: [],
          retired: [],
          list: h.currentList,
          signal: controller.signal,
        }),
      ).rejects.toBe(reason);
      expect(aborted).toBe(true);
      expect(space.failed).toBeNull();
      expect(h.activeLeases).toBe(2);
      h.onRead(null);
      h.onWrite(null);
      await space.publish(Uint8Array.of(2), { writers: [], retired: [], list: h.currentList });
      await space.audit();
      expect(h.activeLeases).toBe(0);
    } finally {
      await space?.close();
      startup.close();
      await h.close();
    }
    expect(governor.usedBytes).toBe(0);
  });

for (const seed of seedsFor("native-open-write-failure", [1765, 1766]))
  it(`closes cleanly after failed private handoff writes (${String(seed)})`, async () => {
    const s: Shape = {
      count: 4097 + (seed % 1000),
      stride: 8192,
      divisor: 2,
      high: true,
      loan: false,
      reversePages: true,
      first: "publish",
    };
    const h = await fixture(s),
      governor = new Governor(8 << 20);
    const startup = await governor.admit({
      label: "failed lazy handoff",
      minimumBytes: 1 << 20,
      spillable: true,
    });
    let space: ExtentSpace | null = null;
    try {
      space = await openExtentSpace(h.session, {
        reachable: h.reachable,
        budget: startup,
        registerCache: (c) => governor.registerCache(c),
      });
      startup.close();
      let payload = false;
      const failure = new Error("native metadata write failure");
      h.onRead(() => {
        payload = true;
      });
      h.onWrite(() => {
        if (payload) throw failure;
      });
      await expect(
        space.publish(Uint8Array.of(2), { writers: [], retired: [], list: h.currentList }),
      ).rejects.toBe(failure);
      expect(space.failed).toBe(failure);
    } finally {
      h.onWrite(null);
      await space?.close();
      startup.close();
      await h.close();
    }
    expect(h.activeLeases).toBe(0);
    expect(governor.usedBytes).toBe(0);
  });
for (const refused of [0, 1])
  it(`returns both owners' credit when descriptor transfer ${String(refused)} is refused`, async () => {
    const h = await fixture({
      count: 5000,
      stride: 8192,
      divisor: 2,
      high: true,
      loan: false,
      reversePages: true,
      first: "close",
    });
    const governor = new Governor(8 << 20);
    const startup = await governor.admit({
      label: "transfer refusal",
      minimumBytes: 1 << 20,
      spillable: true,
    });
    let transfers = 0;
    try {
      await expect(
        openExtentSpace(h.session, {
          reachable: h.reachable,
          budget: startup,
          registerCache(c) {
            const handle = governor.registerCache(c);
            return {
              ...handle,
              takeFrom(source, bytes) {
                if (bytes < 16 * 1024 && transfers++ === refused) return false;
                return handle.takeFrom(source, bytes);
              },
            };
          },
        }),
      ).rejects.toBeInstanceOf(ReadDeniedError);
      expect(h.activeLeases).toBe(0);
      expect(startup.allocated).toBe(0);
      expect(governor.usedBytes).toBe(startup.held);
    } finally {
      startup.close();
      await h.close();
    }
    expect(governor.usedBytes).toBe(0);
  });

it("externally sorts only an oversized physical page directory, never payload pairs", async () => {
  const h = await fixture(
    {
      count: 12301,
      stride: 8192,
      divisor: 2,
      high: true,
      loan: false,
      reversePages: true,
      first: "close",
    },
    1,
  );
  const governor = new Governor(16 << 20);
  const startup = await governor.admit({
    label: "large physical directory",
    minimumBytes: 8 << 20,
    spillable: true,
  });
  let space: ExtentSpace | null = null;
  h.onWrite((_page, bytes) => {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    for (let at = 24; at + 8 <= bytes.length - 4; at += 8) {
      const word = view.getFloat64(at, true);
      if (Number.isSafeInteger(word)) expect(word).toBeLessThan(2 ** 40);
    }
  });
  try {
    space = await openExtentSpace(h.session, {
      reachable: h.reachable,
      budget: startup,
      registerCache: (c) => governor.registerCache(c),
    });
    expect(h.writes).toBeGreaterThan(0); // Physical pages exceed the fixed S index window.
    expect(h.activeLeases).toBe(2);
    expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
  } finally {
    await space?.close();
    startup.close();
    await h.close();
  }
  expect(h.activeLeases).toBe(0);
  expect(governor.usedBytes).toBe(0);
});

for (const cancel of [false, true])
  it(`services a timer during immediately resolved native decoding, cancel ${String(cancel)}`, async () => {
    const h = await fixture({
      count: 100000,
      stride: 8192,
      divisor: 2,
      high: true,
      loan: false,
      reversePages: true,
      first: "close",
    });
    const governor = new Governor(8 << 20);
    const startup = await governor.admit({
      label: "sliced native open",
      minimumBytes: 1 << 20,
      spillable: true,
    });
    const controller = new AbortController(),
      reason = new Error("cancel during native decoding");
    const original = Object.getOwnPropertyDescriptor(DataView.prototype, "getFloat64")?.value as (
      this: DataView,
      at: number,
      little?: boolean,
    ) => number;
    let reads = 0,
      servicedAt = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const spy = vi.spyOn(DataView.prototype, "getFloat64").mockImplementation(function (
      this: DataView,
      at,
      little,
    ) {
      reads++;
      if (reads === 2048)
        timer = setTimeout(() => {
          servicedAt = reads;
          if (cancel) controller.abort(reason);
        }, 0);
      return original.call(this, at, little);
    });
    let space: ExtentSpace | null = null;
    try {
      const pending = openExtentSpace(h.session, {
        reachable: h.reachable,
        budget: startup,
        signal: controller.signal,
        registerCache: (c) => governor.registerCache(c),
      });
      if (cancel) await expect(pending).rejects.toBe(reason);
      else space = await pending;
      expect(servicedAt).toBeGreaterThanOrEqual(2048);
      expect(servicedAt).toBeLessThan(300000);
      if (!cancel) expect(reads).toBeGreaterThan(servicedAt);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      spy.mockRestore();
      await space?.close();
      startup.close();
      await h.close();
    }
    expect(h.activeLeases).toBe(0);
    expect(governor.usedBytes).toBe(0);
  });
for (const corruption of ["self-allocation", "cross-root-length"] as const)
  it(`refuses native ${corruption} corruption before any data release`, async () => {
    const h = await fixture(
      {
        count: 5000,
        stride: 8192,
        divisor: 2,
        high: true,
        loan: false,
        reversePages: false,
        first: "close",
      },
      LIST_PAGE_EXTENTS,
      corruption === "self-allocation" ? { payloadBase: 0 } : { disagreement: true },
    );
    const governor = new Governor(8 << 20);
    const startup = await governor.admit({
      label: "corrupt native root",
      minimumBytes: 1 << 20,
      spillable: true,
    });
    const released = vi.spyOn(h.session, "releaseData");
    try {
      await expect(
        openExtentSpace(h.session, {
          reachable: h.reachable,
          budget: startup,
          registerCache: (c) => governor.registerCache(c),
        }),
      ).rejects.toBeInstanceOf(StoreCorruptionError);
      expect(released).not.toHaveBeenCalled();
      expect(h.activeLeases).toBe(0);
      expect(startup.allocated).toBe(0);
    } finally {
      released.mockRestore();
      startup.close();
      await h.close();
    }
    expect(governor.usedBytes).toBe(0);
  });
afterAll(() => reach.assertReached());
