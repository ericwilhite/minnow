import fc from "fast-check";
import { seedsFor } from "../src/testing/seeds.js";
import { createReach, LISTED_REOPEN_CATALOG_REACH } from "../src/testing/reach.js";
import { decodeCatalogParts } from "../src/next/write/catalog-stream.js";
import { CatalogBuffers, catalogStorageParts } from "../src/next/write/catalog-storage.js";
import { Catalog } from "../src/next/write/catalog.js";
import { expect, it } from "vitest";
import { buildListedReopenSpace, openListedReopen } from "./next-storage-listed-reopen.js";
import { MemoryBackend } from "../src/next/storage/memory-store.js";
import {
  decodeGroupIndex,
  decodeGroupPage,
  decodeManifestRoot,
} from "../src/next/format/manifest-codec.js";
import { countingBudget } from "../src/next/storage/space-test-support.js";

function required<T>(value: T | null | undefined): T {
  if (value === null || value === undefined)
    throw new Error("Expected a stored reopen fixture value");
  return value;
}

it("builds real group directories and lists across a publication boundary, then reopens", async () => {
  const backend = new MemoryBackend();
  const built = await buildListedReopenSpace(backend, 4097, 4096);
  expect(built.groups).toBe(4097);
  const opened = await openListedReopen(backend);
  try {
    const budget = countingBudget(Infinity);
    const root = decodeManifestRoot(required(opened.space.root), budget);
    expect(root.value.sequence).toBe(2);
    expect(root.value.tables[0]?.groupCount).toBe(4097);
    expect(root.value.extentList.length).toBeGreaterThan(1);
    expect(root.value.tables[0]?.statistics).not.toBeNull();
    budget.free(root.charge);
    expect(opened.timing.bytesRead).toBeGreaterThan(4097 * 16);
    // Audit traverses the real catalog/statistics/directory/group/list references and compares
    // both kept roots with their lists; the timed open itself reads only list pages.
    await opened.space.audit();
  } finally {
    await opened.close();
  }
});

it("startup does not walk group roots; the explicit audit still refuses their corruption", async () => {
  const backend = new MemoryBackend();
  await buildListedReopenSpace(backend, 4, 4096);
  const lease = await backend.acquire();
  try {
    const session = await backend.open(lease);
    const rootBytes = required(await session.readRoot());
    const budget = countingBudget(Infinity);
    const root = decodeManifestRoot(rootBytes, budget);
    const read = async (ref: import("../src/next/storage/extents.js").BlobRef) =>
      required(
        (
          await session.readData([{ offset: ref.extent.offset + ref.offset, length: ref.length }])
        )[0],
      );
    const index = decodeGroupIndex(await read(required(root.value.tables[0]).groups), budget);
    const page = decodeGroupPage(await read(required(index.value[0]).ref), budget);
    const ref = required(page.value[0]).root;
    const damaged = (await read(ref)).slice();
    damaged[0] = (damaged[0] ?? 0) ^ 1;
    await session.writeData(ref.extent.offset + ref.offset, damaged);
    await session.publish(rootBytes);
    budget.free(root.charge + index.charge + page.charge);
    await session.close();
  } finally {
    await lease.release();
  }
  const opened = await openListedReopen(backend);
  try {
    await expect(opened.space.audit()).rejects.toThrow(/checksum/i);
  } finally {
    await opened.close();
  }
});

for (const seed of seedsFor("next-listed-reopen-catalog", [0xb77a, 0xb77b]))
  it(`persists native catalog trees through listed publications seed ${String(seed)}`, async () => {
    const reach = createReach("listed reopen catalog", LISTED_REOPEN_CATALOG_REACH);
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(1, 17, 4097), async (groups) => {
        reach.beginHistory();
        const backend = new MemoryBackend();
        await buildListedReopenSpace(backend, groups, 4096);
        const opened = await openListedReopen(backend);
        const budget = countingBudget(Infinity);
        const buffers = new CatalogBuffers(budget);
        try {
          const root = decodeManifestRoot(required(opened.space.root), budget);
          try {
            const data = await decodeCatalogParts(
              catalogStorageParts(root.value.catalog, opened.space, budget),
              buffers,
            );
            const catalog = Catalog.fromStored(data, budget);
            expect(catalog.tableByName("reopen")?.id).toBe(root.value.tables[0]?.tableId);
            expect(catalog.version).toBe(root.value.catalogVersion);
            expect(catalog.counters.nextTableId).toBe(root.value.nextTableId);
            reach.reach("native catalog");
            expect(root.value.sequence).toBe(groups > 4096 ? 2 : 1);
            reach.reach(groups > 4096 ? "multiple publications" : "single publication");
          } finally {
            budget.free(root.charge);
          }
          await opened.space.audit();
          reach.reach("audit");
        } finally {
          buffers.close();
          await opened.close();
        }
        expect(budget.used).toBe(0);
        reach.reach("no IO charge");
      }),
      { seed, numRuns: 8, examples: [[1], [17], [4097]] },
    );
    reach.assertReached();
  });
