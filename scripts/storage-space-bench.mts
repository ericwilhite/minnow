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
import { MemoryBackend } from "../packages/core/src/next/storage/memory-store.ts";
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

await main();
