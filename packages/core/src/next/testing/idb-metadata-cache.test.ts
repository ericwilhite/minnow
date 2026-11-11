/** MetadataStore exact-byte semantics plus the approved bounded private-cache cost contract. */
import { afterAll, expect, it } from "vitest";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { chooseBackend } from "../storage/select.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, IDB_METADATA_CACHE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import {
  METADATA_PAGE_BYTES,
  METADATA_SMALL_BYTES,
  OwnershipLostError,
  ReleaseUnsettledError,
  StoreCorruptionError,
} from "../storage/contract.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import type { EvictableCacheLike } from "../storage/extents.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { expectBytes } from "../storage/space-test-support.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import type { IdbDatabaseLike } from "../storage/idb-like.js";

// Independent credit oracle; do not import the implementation or its constants. This suite
// must collect on the parent (which has no cache module) and fail on native-call/byte laws.
const IDB_METADATA_ENTRY_BYTES = 256,
  IDB_METADATA_CONTROL_BYTES = 2048;
interface MetadataCacheControl extends EvictableCacheLike {
  readonly residentBytes: number;
  readonly residentEntries: number;
  settled(): Promise<void>;
}

const reach = createReach("IndexedDB private metadata cache", IDB_METADATA_CACHE_REACH);
function fixture(
  database?: IdbDatabaseLike,
  limits: { releaseDrainMs?: number; releaseSettleMs?: number } = {},
) {
  const disk = new SimDisk(),
    governor = new Governor(64 * 1024 * 1024);
  const inner = database ?? simIndexedDb(disk);
  let cache: MetadataCacheControl | null = null,
    transactions = 0,
    allowed = true;
  const backend = createIndexedDbBackend({
    ...limits,
    database: () => inner,
    locks: new SimLocks(),
    name: "metadata-cache",
    observe: (event) => {
      if (event.target === "meta") transactions++;
      return () => undefined;
    },
    registerCache: (control) => {
      expect(control).toHaveProperty("residentBytes");
      expect(control).toHaveProperty("residentEntries");
      cache = control as MetadataCacheControl;
      const memory = governor.registerCache(control);
      return {
        charge: (n) => allowed && memory.charge(n),
        release: (n) => memory.release(n),
        unregister: () => memory.unregister(),
      };
    },
  });
  return {
    backend,
    governor,
    disk,
    inner,
    cache: () => {
      if (cache === null) throw Error("cache was not registered");
      return cache;
    },
    get transactions() {
      return transactions;
    },
    clearTransactions() {
      transactions = 0;
    },
    allow(value: boolean) {
      allowed = value;
    },
  };
}

for (const seed of seedsFor("idb-private-metadata-cache", [1771, 1772, 1773, 1774]))
  it(`keeps exact private pages coherent within both cache caps and refused credit (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      f = fixture(),
      lease = await f.backend.acquire();
    const session = await f.backend.open(lease),
      metadata = session.metadata?.();
    if (metadata == null) throw Error("Missing metadata store");
    const target = new Uint8Array(METADATA_PAGE_BYTES),
      model = new Map<number, Uint8Array>();
    const write = async (page: number, length: number, value: number) => {
      const bytes = new Uint8Array(length).fill(value);
      await metadata.write(page, bytes);
      model.set(page, bytes.slice());
      bytes.fill(0);
    };
    const verify = async (page: number) => {
      const expected = model.get(page);
      if (expected === undefined) throw Error("Missing model page");
      await metadata.read(page, target);
      expectBytes(target.subarray(0, expected.length), expected);
      expect(target.subarray(expected.length).every((byte) => byte === 0)).toBe(true);
      target.fill(255); // returned arrays cannot mutate retained cache bytes
    };
    try {
      await metadata.reset();
      f.clearTransactions();
      for (let round = 0; round < 4; round++)
        for (let page = 0; page < 21; page++) {
          await write(
            page,
            page < 3 ? METADATA_PAGE_BYTES : METADATA_SMALL_BYTES,
            1 + Math.floor(random() * 254),
          );
          await verify(page);
        }
      // The same working set used by the full-scale load: no native read/write per hit.
      expect(f.transactions).toBe(0);
      reach.reach("coalesced private writes");
      const cache = f.cache();
      expect(cache.residentBytes).toBe(
        3 * METADATA_PAGE_BYTES +
          18 * METADATA_SMALL_BYTES +
          21 * IDB_METADATA_ENTRY_BYTES +
          IDB_METADATA_CONTROL_BYTES,
      );
      expect(f.governor.usedBytes).toBe(cache.residentBytes);
      reach.reach("exact compact charge");
      let maxEntries = cache.residentEntries;
      for (let page = 21; page < 65; page++) {
        await write(page, METADATA_PAGE_BYTES, 1 + Math.floor(random() * 254));
        maxEntries = Math.max(maxEntries, cache.residentEntries);
        expect(cache.residentEntries).toBeLessThanOrEqual(32);
        expect(cache.residentBytes).toBeLessThanOrEqual(1024 * 1024);
        expect(f.governor.usedBytes).toBe(cache.residentBytes);
      }
      expect(maxEntries).toBe(32);
      reach.reach("entry and byte cap pressure");
      for (let step = 0; step < 240; step++) {
        if (step % 80 === 79) {
          await metadata.reset();
          model.clear();
        }
        const denied = step % 17 === 0;
        if (denied) {
          for (let i = 0; i < 33 && cache.residentBytes > 0; i++) {
            cache.evict(1);
            await cache.settled();
          }
          expect(cache.residentBytes).toBe(0);
          f.allow(false);
        }
        const page = Math.floor(random() * 48),
          length = random() < 0.5 ? METADATA_SMALL_BYTES : METADATA_PAGE_BYTES;
        await write(page, length, 1 + Math.floor(random() * 254));
        await verify(page);
        if (denied) {
          expect(cache.residentBytes).toBe(0);
          f.allow(true);
          reach.reach("credit refused native fallback");
        }
        expect(cache.residentEntries).toBeLessThanOrEqual(32);
        expect(cache.residentBytes).toBeLessThanOrEqual(1024 * 1024);
        expect(f.governor.usedBytes).toBe(cache.residentBytes);
      }
      for (const page of model.keys()) await verify(page);
      await session.close();
      expect(cache.residentBytes).toBe(0);
      const reopened = await f.backend.open(lease),
        again = reopened.metadata?.();
      if (again == null) throw Error("Missing reopened metadata");
      for (const [page, bytes] of model) {
        await again.read(page, target);
        expectBytes(target.subarray(0, bytes.length), bytes);
      }
      await reopened.close();
      expect(cache.residentBytes).toBe(0);
      reach.reach("close and reopen");
    } finally {
      await session.close();
      await lease.release();
    }
    expect(f.governor.usedBytes).toBe(0);
  }, 60_000);

it("shares writes between concurrent sessions and orders reopen behind last-close invalidation", async () => {
  reach.beginHistory();
  const f = fixture(),
    lease = await f.backend.acquire();
  const a = await f.backend.open(lease),
    b = await f.backend.open(lease);
  const x = a.metadata?.(),
    y = b.metadata?.();
  if (x == null || y == null) throw Error("Missing metadata");
  const frame = new Uint8Array(METADATA_SMALL_BYTES).fill(31),
    target = new Uint8Array(METADATA_PAGE_BYTES);
  try {
    await x.write(0, frame);
    await y.read(0, target);
    expect(target[0]).toBe(31);
    frame.fill(47);
    await y.write(0, frame);
    await x.read(0, target);
    expect(target[0]).toBe(47);
    await b.close();
    await x.read(0, target);
    expect(target[0]).toBe(47);
    const closing = a.close(),
      c = await f.backend.open(lease),
      z = c.metadata?.();
    if (z == null) throw Error("Missing concurrent reopen");
    await z.read(0, target);
    expect(target[0]).toBe(47);
    await closing;
    await z.reset();
    await expect(z.read(0, target)).rejects.toThrow(StoreCorruptionError);
    await z.write(0, frame); // a missing-page read must not poison a reusable private store
    const replacement = await f.backend.acquire({ steal: true, timeoutMs: 2_000 });
    try {
      for (const action of [() => z.read(0, target), () => z.write(0, frame), () => z.reset()])
        await expect(action()).rejects.toThrow(OwnershipLostError);
      reach.reach("cached owner fenced");
    } finally {
      await c.close();
      await replacement.release();
    }
    reach.reach("concurrent sessions and reset");
  } finally {
    await a.close();
    await b.close();
    await lease.release();
  }
  expect(f.governor.usedBytes).toBe(0);
});

it("dirty eviction holds its bytes through native settlement and wakes a below-cap admission", async () => {
  reach.beginHistory();
  const inner = simIndexedDb(new SimDisk());
  let pause = false,
    began!: () => void,
    resume!: () => void;
  const started = new Promise<void>((resolve) => {
      began = resolve;
    }),
    gate = new Promise<void>((resolve) => {
      resume = resolve;
    });
  const db: IdbDatabaseLike = {
    close: () => inner.close(),
    transaction(stores, mode, durability, body) {
      if (
        pause &&
        stores.length === 1 &&
        stores[0] === "meta" &&
        mode === "readwrite" &&
        durability === "relaxed"
      ) {
        pause = false;
        began();
        return gate.then(() => inner.transaction(stores, mode, durability, body));
      }
      return inner.transaction(stores, mode, durability, body);
    },
  };
  const f = fixture(db),
    lease = await f.backend.acquire(),
    session = await f.backend.open(lease),
    metadata = session.metadata?.();
  if (metadata == null) throw Error("Missing metadata");
  const frame = new Uint8Array(METADATA_PAGE_BYTES).fill(71);
  for (let page = 0; page < 16; page++) await metadata.write(page, frame);
  const cache = f.cache(),
    before = cache.residentBytes;
  expect(f.governor.usedBytes).toBe(before);
  const blocker = await f.governor.admit({
    label: "other owner",
    minimumBytes: f.governor.capBytes - f.governor.reserveBytes - before,
    spillable: false,
  });
  pause = true;
  let granted = false;
  const admission = f.governor
    .admit({
      label: "below cap",
      minimumBytes: METADATA_PAGE_BYTES + IDB_METADATA_ENTRY_BYTES,
      spillable: true,
    })
    .then((grant) => {
      granted = true;
      return grant;
    });
  await started;
  expect(cache.residentBytes).toBe(before);
  expect(granted).toBe(false);
  resume();
  const grant = await admission;
  expect(granted).toBe(true);
  expect(cache.residentBytes).toBeLessThan(before);
  expect(f.governor.usedBytes).toBeLessThanOrEqual(f.governor.capBytes);
  grant.close();
  blocker.close();
  await session.close();
  await lease.release();
  expect(f.governor.usedBytes).toBe(0);
  reach.reach("dirty eviction wakes admission");
});

it("invalidates on close and refuses malformed native pages after reopen", async () => {
  reach.beginHistory();
  const f = fixture(),
    lease = await f.backend.acquire(),
    session = await f.backend.open(lease),
    metadata = session.metadata?.();
  if (metadata == null) throw Error("Missing metadata");
  await metadata.write(0, new Uint8Array(METADATA_SMALL_BYTES));
  await session.close();
  await f.inner.transaction(["meta"], "readwrite", "relaxed", (tx) =>
    tx.store("meta").put(-1, new ArrayBuffer(31)),
  );
  const reopened = await f.backend.open(lease),
    again = reopened.metadata?.();
  if (again == null) throw Error("Missing reopened metadata");
  await expect(again.read(0, new Uint8Array(METADATA_PAGE_BYTES))).rejects.toThrow(
    StoreCorruptionError,
  );
  await reopened.close();
  await lease.release();
  expect(f.governor.usedBytes).toBe(0);
  reach.reach("malformed native page refused");
});

it("a refused replacement whose native write fails preserves the acknowledged old version", async () => {
  reach.beginHistory();
  const inner = simIndexedDb(new SimDisk());
  let failAt = 0;
  const db: IdbDatabaseLike = {
    close: () => inner.close(),
    transaction(stores, mode, durability, body) {
      return inner.transaction(stores, mode, durability, (tx) => {
        body(tx);
        if (
          failAt > 0 &&
          stores.length === 1 &&
          stores[0] === "meta" &&
          mode === "readwrite" &&
          durability === "relaxed" &&
          --failAt === 0
        )
          tx.abort(new Error("replacement write interrupted"));
      });
    },
  };
  const f = fixture(db),
    lease = await f.backend.acquire(),
    session = await f.backend.open(lease),
    metadata = session.metadata?.();
  if (metadata == null) throw Error("Missing metadata");
  const original = new Uint8Array(METADATA_PAGE_BYTES).fill(19),
    replacement = new Uint8Array(METADATA_SMALL_BYTES).fill(43),
    target = new Uint8Array(METADATA_PAGE_BYTES);
  try {
    for (const nth of [1, 2]) {
      f.allow(true);
      await metadata.reset();
      await metadata.write(0, original);
      f.allow(false);
      failAt = nth;
      await expect(metadata.write(0, replacement)).rejects.toThrow("replacement write interrupted");
      await metadata.read(0, target);
      expectBytes(target, original);
      await metadata.write(0, replacement);
      await metadata.read(0, target);
      expectBytes(target.subarray(0, replacement.length), replacement);
      expect(target.subarray(replacement.length).every((byte) => byte === 0)).toBe(true);
    }
    reach.reach("failed refused replacement keeps old bytes");
  } finally {
    await session.close();
    await lease.release();
  }
  expect(f.governor.usedBytes).toBe(0);
});

it("an asynchronous dirty-flush failure is reported until reset retries, with no retained charge", async () => {
  reach.beginHistory();
  const inner = simIndexedDb(new SimDisk());
  let fail = false;
  const db: IdbDatabaseLike = {
    close: () => inner.close(),
    transaction(stores, mode, durability, body) {
      return inner.transaction(stores, mode, durability, (tx) => {
        body(tx);
        if (
          fail &&
          stores.length === 1 &&
          stores[0] === "meta" &&
          mode === "readwrite" &&
          durability === "relaxed"
        ) {
          fail = false;
          tx.abort(new Error("dirty flush interrupted"));
        }
      });
    },
  };
  const f = fixture(db),
    lease = await f.backend.acquire(),
    session = await f.backend.open(lease),
    metadata = session.metadata?.();
  if (metadata == null) throw Error("Missing metadata");
  const frame = new Uint8Array(METADATA_SMALL_BYTES).fill(27),
    target = new Uint8Array(METADATA_PAGE_BYTES);
  try {
    await metadata.write(0, frame);
    fail = true;
    f.cache().evict(1);
    await expect(f.cache().settled()).rejects.toThrow("dirty flush interrupted");
    await expect(metadata.read(0, target)).rejects.toThrow("dirty flush interrupted");
    expect(f.cache().residentBytes).toBe(0);
    expect(f.governor.usedBytes).toBe(0);
    await metadata.reset();
    await metadata.write(0, frame);
    await metadata.read(0, target);
    expectBytes(target.subarray(0, frame.length), frame);
    reach.reach("failed dirty eviction and reset retry");
  } finally {
    await session.close();
    await lease.release();
  }
  expect(f.governor.usedBytes).toBe(0);
});

it("routes the owner governor through ordinary backend selection", async () => {
  reach.beginHistory();
  (globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;
  const governor = new Governor(64 * 1024 * 1024);
  const chosen = await chooseBackend(
    "chosen-cache",
    { indexedDB: new IDBFactory() },
    new SimLocks(),
    { registerCache: (cache) => governor.registerCache(cache) },
  );
  expect(chosen.kind).toBe("indexeddb");
  const lease = await chosen.backend.acquire(),
    session = await chosen.backend.open(lease),
    metadata = session.metadata?.();
  if (metadata == null) throw Error("Missing chosen metadata");
  try {
    await metadata.write(0, new Uint8Array(METADATA_SMALL_BYTES));
    expect(governor.usedBytes).toBe(METADATA_SMALL_BYTES + 256 + 2048);
    await session.close();
    expect(governor.usedBytes).toBe(0);
    reach.reach("governor routed through chooser");
  } finally {
    await session.close();
    await lease.release();
  }
  expect(governor.usedBytes).toBe(0);
});

for (const queued of [false, true])
  it(
    queued
      ? "fences queued work before it can mutate an unsettled native frame"
      : "keeps a detached native frame charged after logical release gives up",
    async () => {
      reach.beginHistory();
      const inner = simIndexedDb(new SimDisk());
      let borrowed: Uint8Array | null = null;
      const nativeFrame = () => {
        if (borrowed === null) throw Error("Native frame was not borrowed");
        return borrowed;
      };
      let pause = false,
        began!: () => void,
        resume!: () => void,
        nativeEnd: Promise<void> = Promise.resolve();
      const started = new Promise<void>((resolve) => {
          began = resolve;
        }),
        gate = new Promise<void>((resolve) => {
          resume = resolve;
        });
      const database: IdbDatabaseLike = {
        close: () => inner.close(),
        transaction(stores, mode, durability, body) {
          if (
            pause &&
            stores.length === 1 &&
            stores[0] === "meta" &&
            mode === "readwrite" &&
            durability === "relaxed"
          ) {
            pause = false;
            nativeEnd = inner
              .transaction(stores, mode, durability, (tx) =>
                body({
                  store: (name) => {
                    const store = tx.store(name);
                    return {
                      get: (key, next) => store.get(key, next),
                      getAll: (range, next, count) => store.getAll(range, next, count),
                      getAllKeys: (range, next, count) => store.getAllKeys(range, next, count),
                      last: (range, next) => store.last(range, next),
                      put: (key, value) => {
                        if (typeof key === "number" && key < 0 && value instanceof ArrayBuffer) {
                          borrowed = new Uint8Array(value);
                          began();
                        }
                        store.put(key, value);
                      },
                      delete: (key) => store.delete(key),
                      deleteRange: (range) => store.deleteRange(range),
                      clear: () => store.clear(),
                    };
                  },
                  abort: () => undefined,
                }),
              )
              .then(() => gate);
            return nativeEnd;
          }
          return inner.transaction(stores, mode, durability, body);
        },
      };
      const f = fixture(database, { releaseDrainMs: 1, releaseSettleMs: 1 }),
        lease = await f.backend.acquire(),
        session = await f.backend.open(lease),
        metadata = session.metadata?.();
      if (metadata == null) throw Error("Missing metadata");
      try {
        await metadata.write(0, new Uint8Array(METADATA_SMALL_BYTES).fill(67));
        const cache = f.cache(),
          before = cache.residentBytes;
        expect(before).toBeGreaterThan(0);
        pause = true;
        const pending = queued
          ? metadata.write(0, new Uint8Array(METADATA_PAGE_BYTES).fill(91))
          : Promise.resolve();
        const replacement = Promise.allSettled([pending]);
        if (!queued) cache.evict(1);
        await started;
        const target = new Uint8Array(METADATA_PAGE_BYTES).fill(211);
        const continuations = Promise.allSettled(
          queued
            ? [
                metadata.write(0, new Uint8Array(METADATA_SMALL_BYTES).fill(193)),
                metadata.read(0, target),
                metadata.reset(),
              ]
            : [],
        );
        await expect(lease.release()).rejects.toBeInstanceOf(ReleaseUnsettledError);
        const results = await continuations;
        await replacement;
        if (queued) {
          expectBytes(nativeFrame(), new Uint8Array(METADATA_SMALL_BYTES).fill(67));
          expectBytes(target, new Uint8Array(METADATA_PAGE_BYTES).fill(211));
          expect(results).toHaveLength(3);
          for (const result of results) {
            expect(result.status).toBe("rejected");
            if (result.status === "rejected")
              expect(result.reason).toBeInstanceOf(OwnershipLostError);
          }
          reach.reach("queued operations fenced before execution");
        }
        await session.close();
        expect(cache.residentBytes).toBe(before);
        expect(cache.residentEntries).toBe(1);
        expect(f.governor.usedBytes).toBe(before);
      } finally {
        resume();
        await nativeEnd;
        await session.close();
        await lease.release().catch(() => undefined);
      }
      expect(f.governor.usedBytes).toBe(0);
      reach.reach("unsettled native frame retains credit");
    },
  );

afterAll(() => reach.assertReached());
