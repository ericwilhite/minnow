/** R5/R25: a parsed-directory hit cannot consume the workspace admitted for its next page.
 * Optional frozen-runtime replay checks historical candidates with the same generated law.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, NATIVE_DIRECTORY_BUDGET_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import * as pageCode from "./reader.js";
import * as rootCode from "./query-roots.js";
import * as supportCode from "./test-support.js";
import * as memoryCode from "../memory/governor.js";
import * as cacheCode from "../format/blob-cache.js";
import type { ChunkDirectory } from "./directory.js";
import type { ExtentReader, CacheMemory } from "../storage/extents.js";

async function runtime<T>(name: string, fallback: T): Promise<T> {
  const root = process.env.MINNOW_NATIVE_DIRECTORY_RUNTIME_URL;
  return root === undefined
    ? fallback
    : ((await import(
        /* @vite-ignore */ new URL(
          "packages/core/dist/next/" + name + ".js",
          root.endsWith("/") ? root : root + "/",
        ).href
      )) as T);
}
const { ChunkReader } = await runtime("page/reader", pageCode),
  { QueryRoots } = await runtime("page/query-roots", rootCode),
  { writeGroup } = await runtime("page/test-support", supportCode),
  { Governor } = await runtime("memory/governor", memoryCode),
  { BlobCache, fullBlobKey } = await runtime("format/blob-cache", cacheCode);

for (const seed of seedsFor("next-directory-native-budget", [0xb327, 0xb328]))
  it(`cached directories preserve the complete native page budget seed ${String(seed)}`, async () => {
    const reach = createReach("native directory budget", NATIVE_DIRECTORY_BUDGET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2048, max: 3072 }),
        fc.integer({ min: 2, max: 17 }),
        fc.boolean(),
        async (count, period, nullable) => {
          reach.beginHistory();
          const made = await writeGroup([
            {
              type: { kind: "float64" },
              values: Array.from({ length: count }, (_, row) =>
                nullable && row % period === 0 ? null : Math.sin(row) * Math.SQRT2,
              ),
            },
          ]);
          const chunk = made.group.columns[0];
          if (!chunk) throw new Error("chunk");
          let allocated = 0,
            peak = 0;
          const reference = await ChunkReader.open(made.space, chunk, {
            budget: {
              allocate(bytes) {
                allocated += bytes;
                peak = Math.max(peak, allocated);
                return true;
              },
              free(bytes) {
                allocated -= bytes;
              },
            },
          });
          const indices = reference.directory.pages.map((_, index) => index);
          try {
            await reference.load(indices);
          } finally {
            reference.close();
          }
          expect(allocated).toBe(0);
          reach.reach("independent native peak");
          reach.reach("wide float payload");
          reach.reach(nullable ? "sparse NULLs" : "all valid");
          const governor = new Governor(8 << 20),
            roots = new QueryRoots(governor, []);
          let handle: CacheMemory | undefined;
          const ordinary = new BlobCache<ChunkDirectory>((cache) => {
            handle = governor.registerCache(cache);
            return handle;
          }, fullBlobKey);
          // A historical prepared-root implementation supplies its own directory cache.
          // Otherwise exercise the existing R15.7 parsed cache; no history is skipped.
          const prepared = roots as typeof roots & {
            reader?: (pages: ExtentReader) => ExtentReader;
            directories?: { readonly hits: number };
          };
          const pages = prepared.reader?.(made.space) ?? made.space;
          const options = prepared.reader === undefined ? { directories: ordinary } : {};
          const rich = await governor.admit({
            label: "populate parsed directory",
            minimumBytes: 1 << 20,
            spillable: false,
          });
          try {
            (await ChunkReader.open(pages, chunk, { budget: rich, ...options })).close();
          } finally {
            rich.close();
          }
          let cached: pageCode.ChunkReader | undefined;
          try {
            cached = await ChunkReader.open(pages, chunk, {
              budget: {
                allocate(bytes) {
                  if (allocated + bytes > peak) return false;
                  allocated += bytes;
                  return true;
                },
                free(bytes) {
                  allocated -= bytes;
                },
              },
              ...options,
            });
            expect(prepared.directories?.hits ?? ordinary.hits).toBe(1);
            reach.reach("parsed cache hit");
            await cached.load(indices);
            expect(allocated).toBeLessThanOrEqual(peak);
            reach.reach("fixed native peak funds page loads");
          } finally {
            cached?.close();
            roots.close();
            ordinary.clear();
            handle?.unregister?.();
          }
          expect(allocated).toBe(0);
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero charges after cleanup");
        },
      ),
      { seed, numRuns: 12, endOnFailure: true },
    );
    reach.assertReached();
  });
