/** Independent native-request/copy counters for the prepaid list/reclaim workspace. */
import { afterAll, expect, it, vi } from "vitest";
import {
  createReach,
  STORAGE_BOUNDED_IO_REACH,
  STORAGE_SERIAL_INDEX_REACH,
} from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { ByteStore, IDB_RECORD_BYTES } from "../storage/idb-bytes.js";
import type { IdbDatabaseLike } from "../storage/idb-like.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { SimDisk } from "../storage/sim-disk.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { PageCache, FIXED_CACHE_BYTES } from "../storage/page-cache.js";
import { countingBudget, countingMemory, expectBytes } from "../storage/space-test-support.js";
import { MAX_SLAB } from "../storage/slabs.js";
import { xxHash32 } from "../hash/xxhash32.js";
import {
  MetadataJournal,
  MetadataWorkspace,
  METADATA_WORKSPACE_BYTES,
} from "../storage/metadata-journal.js";
import { METADATA_PAGE_BYTES } from "../storage/contract.js";
import { ScriptedMetadataPages } from "./metadata-test-support.js";

const reach = createReach("storage bounded I/O", STORAGE_BOUNDED_IO_REACH);
for (const lazy of [false, true])
  for (const seed of seedsFor("storage-bounded-io", [1561, 1562, 1563, 1564]))
    it(`keeps one native boundary response and a bounded copy (${String(seed)}, lazy ${String(lazy)})`, async () => {
      reach.beginHistory();
      const physical = simIndexedDb(new SimDisk()),
        random = mulberry32(seed);
      let tracking = false,
        pending = 0,
        maxPending = 0,
        native = 0,
        peak = 0,
        maxCopy = 0;
      const observed: IdbDatabaseLike = {
        close: () => physical.close(),
        transaction: (stores, mode, durability, body) =>
          physical.transaction(stores, mode, durability, (tx) =>
            body({
              abort: (reason) => tx.abort(reason),
              store: (name) => {
                const store = tx.store(name);
                return {
                  ...store,
                  get: (key, then) => {
                    const tracked = tracking && name === "data";
                    if (tracked) {
                      pending++;
                      maxPending = Math.max(maxPending, pending);
                    }
                    store.get(key, (value) => {
                      const bytes = (value as { bytes?: unknown } | undefined)?.bytes;
                      const size = tracked && bytes instanceof ArrayBuffer ? bytes.byteLength : 0;
                      if (tracked) pending--;
                      native += size;
                      peak = Math.max(peak, native);
                      try {
                        then(value);
                      } finally {
                        native -= size;
                      }
                    });
                  },
                  put: (key, value) => {
                    const bytes = (value as { bytes?: unknown } | undefined)?.bytes;
                    if (tracking && name === "data" && bytes instanceof ArrayBuffer) {
                      maxCopy = Math.max(maxCopy, bytes.byteLength);
                      peak = Math.max(peak, native + bytes.byteLength);
                    }
                    store.put(key, value);
                  },
                };
              },
            }),
          ),
      };
      const space = lazy
        ? await ByteStore.openLazy(observed, "data")
        : await ByteStore.load(observed, "data");
      const image = new Uint8Array(3 * IDB_RECORD_BYTES);
      image.fill(seed & 0xff, 0, IDB_RECORD_BYTES);
      image.fill((seed + 1) & 0xff, 2 * IDB_RECORD_BYTES);
      await observed.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
        space.write(tx, 0, image.slice(0, IDB_RECORD_BYTES), () => {
          space.write(tx, 2 * IDB_RECORD_BYTES, image.slice(2 * IDB_RECORD_BYTES), () => undefined);
        });
      });
      space.commit();
      const start = 64 * 1024 + Math.floor(random() * 4096),
        end = 2 * IDB_RECORD_BYTES + 64 * 1024 + Math.floor(random() * 4096);
      tracking = true;
      await observed.transaction(["data", "data-index"], "readwrite", "strict", (tx) =>
        space.clear(tx, start, end, () => undefined),
      );
      space.commit();
      expect(maxPending).toBe(1);
      expect(maxCopy).toBeLessThanOrEqual(60 * 1024);
      expect(peak).toBeLessThanOrEqual(256 * 1024 + 60 * 1024);
      reach.reach("one boundary response");
      reach.reach("bounded copy");
      expect(native).toBe(0);
      expect(pending).toBe(0);
      maxPending = 0;
      const target = new Uint8Array(image.length);
      await observed.transaction(["data", "data-index"], "readonly", "relaxed", (tx) =>
        space.read(tx, 0, target, () => undefined, true),
      );
      expect(maxPending).toBe(1);
      reach.reach("serial record read");
      image.fill(0, start, end);
      expectBytes(target, image);
      reach.reach("outside bytes preserved");
      if (lazy) reach.reach("lazy");
      else reach.reach("eager");
      tracking = false;
      await ByteStore.audit(observed, "data");
      const reopened = await ByteStore.openLazy(observed, "data");
      expect(reopened.end).toBe(image.length);
      observed.close();
    });

it("admits the fixed slab before allocating it and serializes borrowed views with cache reads", async () => {
  const backend = new MemoryBackend(),
    lease = await backend.acquire(),
    session = await backend.open(lease);
  const memory = countingMemory(Infinity),
    budget = countingBudget(64 * 1024);
  const a = new Uint8Array(4096).fill(1),
    b = new Uint8Array(4096).fill(2),
    c = new Uint8Array(4096).fill(3);
  await session.writeData(0, a);
  await session.writeData(4096, b);
  await session.writeData(8192, c);
  const original = Uint8Array;
  let allocations = 0;
  vi.stubGlobal(
    "Uint8Array",
    new Proxy(original, {
      construct(target, args, constructor) {
        if (args[0] === MAX_SLAB) {
          expect(memory.used).toBeGreaterThanOrEqual(FIXED_CACHE_BYTES);
          allocations++;
        }
        return Reflect.construct(target, args, constructor) as Uint8Array;
      },
    }),
  );
  const cache = new PageCache(session);
  let resume!: () => void, enter!: () => void;
  const entered = new Promise<void>((resolve) => {
      enter = resolve;
    }),
    held = new Promise<void>((resolve) => {
      resume = resolve;
    });
  let borrowing: Promise<void> | null = null;
  let reading: ReturnType<PageCache["read"]> | null = null;
  try {
    expect(allocations).toBe(0);
    cache.attach(memory);
    expect(allocations).toBe(1);
    borrowing = cache.withReadSlab(async (slab) => {
      const first = slab.subarray(0, 4096),
        second = slab.subarray(65552, 65552 + 4096);
      await cache.readRaw(0, first);
      await cache.readRaw(4096, second);
      enter();
      await held;
      expectBytes(first, a);
      expectBytes(second, b);
    });
    await entered;
    let finished = false;
    reading = cache
      .read(
        [
          {
            extent: { offset: 8192, length: 4096 },
            offset: 0,
            length: 4096,
            checksum: xxHash32(c),
          },
        ],
        { budget },
      )
      .then((value) => {
        finished = true;
        return value;
      });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(finished).toBe(false);
    resume();
    await borrowing;
    const read = await reading;
    try {
      expectBytes(read.bytes[0], c);
    } finally {
      read.release();
    }
    await cache.settled();
    expect(allocations).toBe(1);
    expect(cache.stats.bytesRead).toBe(3 * 4096);
    const refused = new PageCache(session);
    expect(() => refused.attach(countingMemory(FIXED_CACHE_BYTES - 1))).toThrow(RangeError);
    expect(allocations).toBe(1);
    refused.close();
  } finally {
    resume();
    await borrowing;
    if (reading !== null) {
      const read = await reading;
      read.release();
    }
    await cache.settled();
    cache.close();
    vi.unstubAllGlobals();
    await session.close();
    await lease.release();
  }
  expect(memory.used).toBe(0);
  expect(budget.used).toBe(0);
});

afterAll(() => reach.assertReached());

it("borrows six metadata read frames from the paid slab and keeps native writers standalone", async () => {
  const backend = new MemoryBackend(),
    lease = await backend.acquire(),
    session = await backend.open(lease),
    memory = countingMemory(Infinity);
  const cache = new PageCache(session);
  cache.attach(memory);
  const budget = countingBudget(METADATA_WORKSPACE_BYTES),
    workspace = MetadataWorkspace.create(budget),
    journal = new MetadataJournal(new ScriptedMetadataPages(), workspace);
  try {
    await workspace.run("merge", () =>
      cache.withReadSlab(async (slab) => {
        // Two192KiB raw windows +256KiB temporary arena +six60KiB metadata views =1000KiB.
        const frames = Array.from({ length: 6 }, (_, at) =>
          slab.subarray(
            640 * 1024 + at * METADATA_PAGE_BYTES,
            640 * 1024 + (at + 1) * METADATA_PAGE_BYTES,
          ),
        );
        await workspace.withReadFrames(frames, async (first) => {
          const writer = await journal.writer(1);
          await writer.append(new Uint8Array([7, 8, 9]));
          const handle = await writer.finish();
          const cursor = await journal.cursor(handle, first + 5);
          try {
            expect(cursor.frame.buffer).toBe(slab.buffer);
            expectBytes(await cursor.next(), new Uint8Array([7, 8, 9]));
            await expect(journal.writer(first)).rejects.toThrow("borrowed read frame");
            await expect(workspace.withReadFrames(frames, async () => undefined)).rejects.toThrow(
              "scope",
            );
          } finally {
            await cursor.close();
          }
          await journal.release(handle);
        });
        expect(() => workspace.frame(4)).toThrow();
        const repeated = frames[0];
        if (repeated === undefined) throw new Error("Missing borrowed frame");
        await expect(
          workspace.withReadFrames([repeated, repeated], async () => undefined),
        ).rejects.toThrow("overlap");
      }),
    );
    await workspace.run("release", async () => {
      expect(() => workspace.frame(2)).toThrow();
    });
  } finally {
    workspace.close();
    await cache.settled();
    cache.close();
    await session.close();
    await lease.release();
  }
  expect(budget.used).toBe(0);
  expect(memory.used).toBe(0);
});

for (const lazy of [false, true])
  it(`rolls back already trimmed boundaries when the later record is corrupt (lazy ${String(lazy)})`, async () => {
    const db = simIndexedDb(new SimDisk());
    const space = lazy ? await ByteStore.openLazy(db, "data") : await ByteStore.load(db, "data");
    const original = new Uint8Array(IDB_RECORD_BYTES).fill(19);
    await db.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
      space.write(tx, 0, original, () =>
        space.write(tx, 2 * IDB_RECORD_BYTES, original, () => undefined),
      );
    });
    space.commit();
    await db.transaction(["data"], "readwrite", "strict", (tx) => {
      tx.store("data").put(2 * IDB_RECORD_BYTES, {
        offset: 2 * IDB_RECORD_BYTES,
        bytes: new Uint8Array(7).buffer,
      });
    });
    await expect(
      db.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
        space.clear(tx, 64 * 1024, 2 * IDB_RECORD_BYTES + 64 * 1024, () => undefined);
      }),
    ).rejects.toThrow("disagrees");
    space.rollback();
    const target = new Uint8Array(IDB_RECORD_BYTES);
    await db.transaction(["data", "data-index"], "readonly", "relaxed", (tx) =>
      space.read(tx, 0, target, () => undefined, true),
    );
    expectBytes(target, original);
    let entries = 0;
    await db.transaction(["data-index"], "readonly", "relaxed", (tx) =>
      tx.store("data-index").getAll({}, (found) => {
        entries = found.length;
      }),
    );
    expect(entries).toBe(2);
    db.close();
  });

const serialReach = createReach("serial native index page", STORAGE_SERIAL_INDEX_REACH);
for (const seed of seedsFor("storage-serial-index", [1661, 1662, 1663, 1664]))
  it(`bounds tiny-record index selection as well as byte responses (${String(seed)})`, async () => {
    serialReach.beginHistory();
    const physical = simIndexedDb(new SimDisk()),
      count = 1024 + (seed % 257);
    const image = new Uint8Array(count * 2);
    await physical.transaction(["data", "data-index"], "readwrite", "relaxed", (tx) => {
      for (let i = 0; i < count; i++) {
        const bytes = new Uint8Array([i & 255]);
        image[i * 2] = i & 255;
        tx.store("data").put(i * 2, { offset: i * 2, bytes: bytes.buffer });
        tx.store("data-index").put(i * 2, { offset: i * 2, length: 1 });
      }
    });
    let largest = 0,
      pages = 0;
    const observed: IdbDatabaseLike = {
      close: () => physical.close(),
      transaction: (stores, mode, durability, body) =>
        physical.transaction(stores, mode, durability, (tx) =>
          body({
            abort: (reason) => tx.abort(reason),
            store: (name) => {
              const store = tx.store(name);
              return {
                ...store,
                getAll: (range, then, limit) =>
                  store.getAll(
                    range,
                    (entries) => {
                      if (name === "data-index") {
                        largest = Math.max(largest, entries.length);
                        pages++;
                      }
                      then(entries);
                    },
                    limit,
                  ),
              };
            },
          }),
        ),
    };
    const store = await ByteStore.openLazy(observed, "data"),
      target = new Uint8Array(image.length - 2);
    await observed.transaction(["data", "data-index"], "readonly", "relaxed", (tx) =>
      store.read(tx, 1, target, () => undefined, true),
    );
    expectBytes(target, image.subarray(1, image.length - 1));
    expect(largest).toBeLessThanOrEqual(128);
    serialReach.reach("bounded native index page");
    expect(pages).toBeGreaterThan(1);
    serialReach.reach("multiple index pages");
    serialReach.reach("exact tiny-record image");
    store.close();
    observed.close();
  });

afterAll(() => serialReach.assertReached());
