/** Fixture roots obey the same encoded metadata and lease contracts as real scan inputs. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../packages/core/src/testing/seeds.js";
import {
  BENCH_ROOT_ADMISSION_REACH,
  createReach,
} from "../../../packages/core/src/testing/reach.js";
import { MetadataReader } from "../../../packages/core/src/next/format/metadata-reader.js";
import { ParseBudgetError } from "../../../packages/core/src/next/format/manifest-codec.js";
import { MemoryExtentSpace } from "../../../packages/core/src/next/page/memory-extents.js";
import { writeGroup } from "../../../packages/core/src/next/page/test-support.js";
import { ReadDeniedError } from "../../../packages/core/src/next/storage/extents.js";
import { benchSnapshotOf } from "./scan-fixture.js";

for (const seed of seedsFor("bench-root-admission", [0xb2f6, 0xb2f7]))
  it(`supplies charged encoded roots without changing payload storage (${seed})`, async () => {
    const reach = createReach("bench root admission", BENCH_ROOT_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 3 }),
        fc.integer({ min: 1, max: 17 }),
        async (count, rows) => {
          reach.beginHistory();
          const space = new MemoryExtentSpace();
          const groups = [];
          for (let index = 0; index < count; index++) {
            const { group } = await writeGroup(
              [
                {
                  type: { kind: "int32", bits: 32 },
                  values: Array.from({ length: rows }, (_, r) => r + index),
                },
              ],
              { space },
            );
            groups.push({ rows, chunks: group.columns });
          }
          const { table, pages } = benchSnapshotOf(space, groups);
          let allocated = 0,
            capacity = Infinity;
          const budget = {
            allocate(bytes: number) {
              if (allocated + bytes > capacity) return false;
              allocated += bytes;
              return true;
            },
            free(bytes: number) {
              allocated -= bytes;
              expect(allocated).toBeGreaterThanOrEqual(0);
            },
          };
          for (const segment of table.segments) {
            if (segment.kind !== "group") throw new Error("stored fixture");
            expect(
              () => segment.group,
              "executor fixtures must expose encoded root references",
            ).not.toThrow();
            const root = segment.group.root;
            for (const cold of [false, true]) {
              const reader = new MetadataReader(cold ? { ...pages, peek: () => null } : pages);
              const held = await reader.groupRoot(root, budget);
              expect(held.value).toEqual(await segment.root());
              expect(allocated, "metadata parse must be charged").toBeGreaterThan(0);
              held.release();
              held.release();
              expect(allocated).toBe(0);
              reach.reach(cold ? "cold charged parse" : "warm charged parse");
            }
            capacity = 0;
            expect(() => new MetadataReader(pages).groupRoot(root, budget)).toThrow(
              ParseBudgetError,
            );
            expect(allocated).toBe(0);
            reach.reach("parse refusal cleanup");
            capacity = Infinity;
            const chunk = await segment.chunk(1);
            if (!chunk) throw new Error("fixture column");
            const refs = [root, chunk.directory, { ...root }];
            const lease = await pages.read(refs, { budget });
            expect(lease.bytes[0]).toEqual(pages.peek(root));
            expect(lease.bytes[2]).toEqual(lease.bytes[0]);
            expect(allocated).toBe(refs.reduce((sum, ref) => sum + ref.length, 0));
            lease.release();
            lease.release();
            expect(allocated).toBe(0);
            expect(space.leased).toBe(0);
            reach.reach("mixed lease");
            capacity = root.length;
            await expect(pages.read([root, chunk.directory], { budget })).rejects.toBeInstanceOf(
              ReadDeniedError,
            );
            expect(allocated).toBe(0);
            reach.reach("payload refusal cleanup");
            capacity = Infinity;
            const abort = new AbortController();
            abort.abort();
            await expect(
              pages.read([root], { budget, signal: abort.signal }),
            ).rejects.toMatchObject({ name: "AbortError" });
            expect(allocated).toBe(0);
            reach.reach("cancelled read");
          }
          reach.reach(count === 1 ? "single group" : "multiple groups");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [1, 3],
          [3, 5],
        ],
      },
    );
    reach.assertReached();
  });
