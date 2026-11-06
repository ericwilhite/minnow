/**
 * Hot paths of the extent space (packages/core/src/next/storage/space.ts) over the in-memory
 * backend, in Node. It measures the space's own CPU, not a disk: OPFS adds 15–80 µs per platform
 * call (storage probe), which is why slab coalescing and the write slab matter there.
 *
 *   npx vite-node scripts/storage-space-bench.mts
 *
 * Targets it serves (PROPOSAL "Targets"): owner-side warm PK read ≤ 10 µs (the cache's `peek` and
 * a leased hit are a small part of it); cold scans bound by bytes, not calls (coalesced misses);
 * bulk load (page-at-a-time appends through the write slab).
 */
import { performance } from "node:perf_hooks";
import type { BlobRef, ExtentRef } from "../packages/core/src/next/storage/extents.ts";
import type { StorageBackend } from "../packages/core/src/next/storage/contract.ts";
import type {
  IdbDatabaseLike,
  IdbRange,
  IdbStoreLike,
} from "../packages/core/src/next/storage/idb-like.ts";
import { createIndexedDbBackend } from "../packages/core/src/next/storage/indexeddb.ts";
import { MemoryBackend } from "../packages/core/src/next/storage/memory-store.ts";
import { SimLocks } from "../packages/core/src/next/storage/sim-disk.ts";
import { openExtentSpace } from "../packages/core/src/next/storage/space.ts";
import {
  bytesOf,
  countingBudget,
  countingMemory,
  jsonReachable,
  rootOf,
} from "../packages/core/src/next/storage/space-test-support.ts";

const PAGE = 32_768;
const MiB = 1 << 20;

function report(name: string, value: number, unit: string): void {
  console.log(`${name.padEnd(46)} ${value.toFixed(value < 10 ? 2 : 0).padStart(10)} ${unit}`);
}

async function main(): Promise<void> {
  const backend = new MemoryBackend();
  const session = await backend.open(await backend.acquire());
  const space = await openExtentSpace(session, {
    reachable: jsonReachable,
    budget: countingBudget(Infinity),
    registerCache: () => countingMemory(Infinity),
  });
  const budget = countingBudget(Infinity);

  // Writes: 8 builders interleaved, one 32 KiB page per append (the group writer's pattern).
  const pages = 8 * 400; // 100 MiB
  const page = bytesOf(PAGE, 1);
  const writer = space.beginWrite("bench");
  const builders = Array.from({ length: 8 }, () => writer.open(400 * PAGE));
  let start = performance.now();
  for (let index = 0; index < pages; index++) await builders[index % 8]?.append([page]);
  const blobs: BlobRef[] = [];
  for (const builder of builders) blobs.push(...((await builder.seal())?.blobs ?? []));
  let ms = performance.now() - start;
  report(
    "append, 8 interleaved builders, 32 KiB pages",
    (pages * PAGE) / MiB / (ms / 1000),
    "MiB/s",
  );
  report("  per append", (ms * 1000) / pages, "µs");

  await space.publish(rootOf(blobs.slice(0, 1)), { writers: [writer], retired: [] });
  const publishes = 200;
  start = performance.now();
  let previous: ExtentRef | null = null;
  for (let index = 0; index < publishes; index++) {
    // Each publish retires the previous one's extent: reclaim runs on every step.
    const small = space.beginWrite("small");
    const { blobs: written, extent } = await small.write([bytesOf(200, index)]);
    await space.publish(rootOf(written), {
      writers: [small],
      retired: previous === null ? [] : [previous],
    });
    previous = extent;
  }
  report(
    "small write + publish + reclaim (memory store)",
    ((performance.now() - start) * 1000) / publishes,
    "µs",
  );

  // Cold reads: 32 pages per call, coalesced into 1 MiB slabs, verified and copied.
  const sequential = blobs
    .slice()
    .sort((a, b) => a.extent.offset + a.offset - (b.extent.offset + b.offset));
  start = performance.now();
  for (let index = 0; index < sequential.length; index += 32) {
    const lease = await space.read(sequential.slice(index, index + 32), { budget });
    lease.release();
  }
  ms = performance.now() - start;
  report("cold read (verify + copy), 32 pages/call", (pages * PAGE) / MiB / (ms / 1000), "MiB/s");
  report("  platform reads per call", space.stats.cache.platformReads / (pages / 32), "");

  // Warm: one cached 32 KiB page.
  const hot = sequential[0] as BlobRef;
  const iterations = 200_000;
  start = performance.now();
  let sink = 0;
  for (let index = 0; index < iterations; index++) sink += space.peek(hot)?.length ?? 0;
  report("peek, cached blob", ((performance.now() - start) * 1e6) / iterations, "ns");
  start = performance.now();
  for (let index = 0; index < iterations / 10; index++) {
    const lease = await space.read([hot], { budget });
    sink += lease.bytes.length;
    lease.release();
  }
  report(
    "read + release, one cached blob",
    ((performance.now() - start) * 1e6) / (iterations / 10),
    "ns",
  );
  if (sink === 0) console.log("(unreachable)");
  await space.close();
}

/**
 * An IndexedDB that answers requests in order and keeps values by reference (no structured clone,
 * no disk): the IndexedDB backend's own CPU, isolated. Never a model of IndexedDB's semantics.
 */
function nullIndexedDb(): IdbDatabaseLike {
  const stores = new Map<string, Map<number | string, unknown>>();
  const storeOf = (name: string) => {
    let store = stores.get(name);
    if (store === undefined) {
      store = new Map();
      stores.set(name, store);
    }
    return store;
  };
  const inRange = (store: Map<number | string, unknown>, range: IdbRange | null) =>
    [...store.keys()]
      .filter((key): key is number => typeof key === "number")
      .filter((key) => range?.lower === undefined || key >= range.lower)
      .filter(
        (key) =>
          range?.upper === undefined ||
          key < range.upper ||
          (key === range.upper && range.upperOpen !== true),
      )
      .sort((a, b) => a - b);
  return {
    async transaction(_names, _mode, _durability, body) {
      const requests: Array<() => void> = [];
      const wrap = (name: string): IdbStoreLike => {
        const store = storeOf(name);
        return {
          get: (key, then) => requests.push(() => then(store.get(key))),
          getAll: (range, then) =>
            requests.push(() => then(inRange(store, range).map((key) => store.get(key)))),
          getAllKeys: (range, then) => requests.push(() => then(inRange(store, range))),
          last: (range, then) =>
            requests.push(() => {
              const key = inRange(store, range).at(-1);
              then(key === undefined ? undefined : store.get(key));
            }),
          put: (key, value) => requests.push(() => store.set(key, value)),
          delete: (key) => requests.push(() => store.delete(key)),
          deleteRange: (range) =>
            requests.push(() => inRange(store, range).forEach((key) => store.delete(key))),
          clear: () => requests.push(() => store.clear()),
        };
      };
      body({
        store: wrap,
        abort: (reason) => {
          throw reason;
        },
      });
      while (requests.length > 0) {
        await Promise.resolve();
        requests.shift()?.();
      }
    },
    close: () => undefined,
  };
}

/** CPU only: 16 interleaved builders with their own buffers, 16 MiB of 32 KiB pages, a publish. */
async function builderRun(backend: StorageBackend): Promise<number> {
  const session = await backend.open(await backend.acquire());
  const space = await openExtentSpace(session, {
    reachable: jsonReachable,
    budget: countingBudget(Infinity),
    registerCache: () => countingMemory(Infinity),
  });
  const page = bytesOf(PAGE, 3);
  const start = performance.now();
  const writer = space.beginWrite("columns", { budget: countingBudget(Infinity) });
  const builders = Array.from({ length: 16 }, () => writer.open(MiB));
  for (let index = 0; index < 32; index++)
    for (const builder of builders) await builder.append([page]);
  const blobs: BlobRef[] = [];
  for (const builder of builders) blobs.push(...((await builder.seal())?.blobs ?? []));
  await space.publish(rootOf(blobs.slice(0, 1)), { writers: [writer], retired: [] });
  const ms = performance.now() - start;
  await space.close();
  return ms;
}

async function builderLane(): Promise<void> {
  const runs = Number(process.env.RUNS ?? "9");
  for (const [name, make] of [
    ["memory backend", () => new MemoryBackend()],
    [
      "IndexedDB backend over a null IndexedDB",
      () =>
        createIndexedDbBackend({ database: nullIndexedDb, name: "bench", locks: new SimLocks() }),
    ],
  ] as const) {
    const samples: number[] = [];
    for (let run = 0; run < runs; run++) samples.push(await builderRun(make()));
    samples.sort((a, b) => a - b);
    console.log(
      `16 builders x 1 MiB, 32 KiB pages, ${name}`.padEnd(64),
      `median ${(samples[Math.floor(runs / 2)] ?? 0).toFixed(1)} ms  min ${(samples[0] ?? 0).toFixed(1)} ms  (${String(runs)} runs)`,
    );
  }
}

// `LANE=builders` runs only the builder lane (the p2e before/after comparison).
if (process.env.LANE !== "builders") await main();
await builderLane();
