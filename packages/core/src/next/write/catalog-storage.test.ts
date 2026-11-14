/** Exercise physical fanout boundaries with real extent IO and a bounded codec workspace. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  CATALOG_STORAGE_REACH,
  CATALOG_STORAGE_FAULT_REACH,
} from "../../testing/reach.js";
import { CATALOG_NATIVE_GRAMMAR as grammar } from "../durable-format.js";
import { Harness } from "./test-support.js";
import { Catalog } from "./catalog.js";
import { decodeCatalogRoot } from "../format/catalog-pages.js";
import {
  catalogStreamByteLength,
  decodeCatalogStream,
  type CatalogStreamData,
} from "./catalog-stream.js";
import {
  writeCatalogStorage,
  catalogStorageParts,
  catalogStorageEvents,
} from "./catalog-storage.js";
function dataFor(targetPages: number, count: number, delta: number): CatalogStreamData {
  const empty = decodeCatalogStream(Catalog.empty().encode());
  let widths = Array.from(
    { length: count },
    () => Math.floor((targetPages * grammar.pageBytes) / count) - 128,
  );
  if (count > 1) {
    widths[0] = (widths[0] ?? 0) + delta;
    widths[1] = (widths[1] ?? 0) - delta;
  }
  const build = (): CatalogStreamData => ({
    ...empty,
    views: widths.map((width, i) => ({
      id: i + 1,
      name: `v${String(i)}`,
      bodySql: `SELECT '${"x".repeat(width)}' AS v`,
    })),
    counters: { ...empty.counters, nextViewId: count + 1 },
  });
  let data = build();
  const diff = targetPages * grammar.pageBytes - 17 - catalogStreamByteLength(data);
  const last = widths.length - 1;
  widths = widths.map((w, i) => (i === last ? w + diff : w));
  data = build();
  expect(Math.ceil(catalogStreamByteLength(data) / grammar.pageBytes)).toBe(targetPages);
  return data;
}
for (const seed of seedsFor("next-catalog-storage", [0xb756, 0xb757]))
  for (const targetPages of [63, 64, 65])
    it(`native catalog ${String(targetPages)}pages seed${String(seed)}`, async () => {
      const reach = createReach<string>("native catalog IO", [
        ...CATALOG_STORAGE_REACH,
        targetPages > 64 ? "internal root" : "leaf root",
      ]);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 12, max: 20 }),
          fc.integer({ min: -1024, max: 1024 }),
          async (count, delta) => {
            reach.beginHistory();
            const harness = new Harness("relaxed", "memory");
            let allocated = 0,
              peak = 0;
            const budget = {
              allocate(n: number) {
                if (allocated + n > 6 * 1024 * 1024) return false;
                allocated += n;
                peak = Math.max(peak, allocated);
                return true;
              },
              free(n: number) {
                allocated -= n;
              },
            };
            await harness.open();
            const writer = harness.space.beginWrite("MCAT codec test");
            try {
              const data = dataFor(targetPages, count, delta),
                ref = await writeCatalogStorage(writer, data, budget);
              expect(allocated).toBe(0);
              const lease = await harness.space.read([ref], { budget });
              let root: ReturnType<typeof decodeCatalogRoot>;
              try {
                root = decodeCatalogRoot(lease.bytes[0] ?? new Uint8Array(), budget);
              } finally {
                lease.release();
              }
              try {
                expect(root.value.pages).toBe(targetPages);
                expect(root.value.depth).toBe(targetPages > grammar.fanOut ? 1 : 0);
              } finally {
                budget.free(root.charge);
              }
              const parts: Uint8Array[] = [];
              let bytes = 0;
              for await (const part of catalogStorageParts(ref, harness.space, budget)) {
                parts.push(part.slice());
                bytes += part.length;
              }
              const merged = new Uint8Array(bytes);
              let at = 0;
              for (const part of parts) {
                merged.set(part, at);
                at += part.length;
              }
              const decoded = decodeCatalogStream(merged);
              expect(decoded.views).toEqual(data.views);
              expect(decoded.counters).toEqual(data.counters);
              expect(allocated).toBe(0);
              expect(peak).toBeLessThan(6 * 1024 * 1024);
              const controller = new AbortController();
              for await (const event of catalogStorageEvents(
                ref,
                harness.space,
                budget,
                controller.signal,
              )) {
                if (event.kind === "catalog-page") {
                  controller.abort();
                  break;
                }
              }
              expect(allocated).toBe(0);
              reach.reach(targetPages > 64 ? "internal root" : "leaf root");
              for (const s of [
                "exact catalog values",
                "bounded workspace",
                "actual data pages",
                "cancelled iterator cleanup",
                "zero resources",
              ] as const)
                reach.reach(s);
            } finally {
              await writer.abort();
              await harness.close();
            }
            expect(allocated).toBe(0);
            expect(harness.governor.usedBytes).toBe(0);
          },
        ),
        { seed, numRuns: 2, examples: [[16, 0]] },
      );
      reach.assertReached();
    }, 120000);

it.each(seedsFor("next-catalog-storage-faults", [0xb758, 0xb759]))(
  "aborts catalog publication work at IO cuts (seed %i)",
  async (seed) => {
    const reach = createReach("catalog native IO faults", CATALOG_STORAGE_FAULT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 3 }), async (failAt) => {
        reach.beginHistory();
        const harness = new Harness("relaxed", "memory");
        let live = 0;
        const budget = {
          allocate(n: number) {
            live += n;
            return true;
          },
          free(n: number) {
            live -= n;
          },
        };
        await harness.open();
        const previous = harness.space.root?.slice(),
          writer = harness.space.beginWrite("catalog injected IO failure");
        const data: CatalogStreamData = {
          ...decodeCatalogStream(Catalog.empty().encode()),
          views: Array.from({ length: 4 }, (_, i) => ({
            id: i + 1,
            name: `v${String(i)}`,
            bodySql: `SELECT '${"x".repeat(65536)}'`,
          })),
          counters: { ...Catalog.empty().counters, nextViewId: 5 },
        };
        const failure = new Error("injected catalog write fault");
        let writes = 0;
        const proxy = new Proxy(writer, {
          get(target, key) {
            if (key === "write")
              return async (parts: readonly Uint8Array[]) => {
                if (++writes === failAt) throw failure;
                return target.write(parts);
              };
            const member: unknown = Reflect.get(target, key, target);
            const bound: unknown = typeof member === "function" ? member.bind(target) : member;
            return bound;
          },
        });
        try {
          await expect(writeCatalogStorage(proxy, data, budget)).rejects.toBe(failure);
          expect(writer.state).toBe("aborted");
          expect(live).toBe(0);
          expect(harness.space.root).toEqual(previous);
          reach.reach(failAt === 3 ? "index write cutoff" : "data write cutoff");
          reach.reach("old root unchanged");
          reach.reach("writer aborted");
          reach.reach("zero temporary allocations");
        } finally {
          await writer.abort();
          await harness.close();
        }
        expect(harness.governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 6, examples: [[1], [2], [3]] },
    );
    reach.assertReached();
  },
  120000,
);
