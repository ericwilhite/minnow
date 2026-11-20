/** The encoded-consumer borrow contract: metadata precedes payload, empty demand reads no
 * value page, live/loaded/bounds checks precede a callback, and close returns reader credit.
 */
import fc from "fast-check";
import { expect, expectTypeOf, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ENCODED_PAGE_BORROW_REACH } from "../../testing/reach.js";
import type { ByteBudget } from "../storage/extents.js";
import { ChunkReader, ReaderClosedError } from "./reader.js";
import type { EncodedPageCheck, EncodedPageKernel } from "./consume.js";
import { defined, writeGroup } from "./test-support.js";

it.each(seedsFor("next-encoded-page-borrow", [0xb4e7, 0xb4e8]))(
  "keeps metadata and empty demand outside payload lifetime (seed %i)",
  async (seed) => {
    const reach = createReach("encoded page borrow", ENCODED_PAGE_BORROW_REACH);
    expectTypeOf<ReturnType<EncodedPageKernel<null>>>().toEqualTypeOf<undefined>();
    expectTypeOf<ReturnType<EncodedPageCheck>>().toEqualTypeOf<boolean>();
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 257, max: 1024 }), fc.integer(), async (count, salt) => {
        reach.beginHistory();
        let used = 0;
        const budget: ByteBudget = {
            allocate(bytes) {
              used += bytes;
              return true;
            },
            free(bytes) {
              used -= bytes;
              expect(used).toBeGreaterThanOrEqual(0);
            },
          },
          fixture = await writeGroup(
            [
              {
                type: { kind: "int32", bits: 32 },
                values: Array.from({ length: count }, (_, row) =>
                  Math.imul(row ^ salt, 0x9e37_79b1),
                ),
              },
            ],
            { gzip: false, pageBytes: 1024, rawPageBytes: 4096 },
          ),
          reader = await ChunkReader.open(fixture.space, defined(fixture.group.columns[0]), {
            budget,
          }),
          kernel = vi.fn<EncodedPageKernel<null>>(() => undefined),
          check = vi.fn<EncodedPageCheck>(() => true),
          before = used;
        try {
          for (let page = 0; page < reader.directory.pages.length; page++) {
            const entry = reader.pageEntry(page);
            expect(reader.isLoaded(page)).toBe(false);
            expect(entry).toBe(reader.directory.pages[page]);
            reader.consumePage(page, null, 0, 0, null, kernel);
            expect(kernel).not.toHaveBeenCalled();
            expect(used).toBe(before);
            expect(() => reader.checkPage(page, check)).toThrow(RangeError);
            expect(check).not.toHaveBeenCalled();
            expect(() => reader.consumePage(page, null, 0, 1, null, kernel)).toThrow(RangeError);
            expect(kernel).not.toHaveBeenCalled();
            reach.reach("metadata before payload");
            reach.reach("empty span without payload");
            reach.reach("unloaded page refused before callback");
            await reader.load([page]);
            const held = used;
            expect(reader.checkPage(page, check)).toBe(true);
            expect(check).toHaveBeenCalledTimes(1);
            check.mockClear();
            expect(used).toBe(held);
            reach.reach("loaded eligibility without transition");
            expect(() => reader.consumePage(page, null, -1, 1, null, kernel)).toThrow(RangeError);
            expect(() =>
              reader.consumePage(page, null, 0, entry.rowCount + 1, null, kernel),
            ).toThrow(RangeError);
            expect(() =>
              reader.consumePage(page, Int32Array.of(entry.firstRow - 1), 0, 1, null, kernel),
            ).toThrow(RangeError);
            expect(() =>
              reader.consumePage(
                page,
                Uint32Array.of(entry.firstRow + entry.rowCount),
                0,
                1,
                null,
                kernel,
              ),
            ).toThrow(RangeError);
            expect(kernel).not.toHaveBeenCalled();
            expect(used).toBe(held);
            reader.consumePage(page, null, 0, 1, null, kernel);
            expect(kernel).toHaveBeenCalledTimes(1);
            expect(used).toBe(held);
            kernel.mockClear();
            reader.release([page]);
            reach.reach("span endpoints checked");
            reach.reach("loaded page borrowed without new capacity");
          }
        } finally {
          reader.close();
        }
        expect(used).toBe(0);
        expect(() => reader.pageEntry(0)).toThrow(ReaderClosedError);
        expect(() => reader.checkPage(0, check)).toThrow(ReaderClosedError);
        expect(check).not.toHaveBeenCalled();
        expect(() => reader.consumePage(0, null, 0, 0, null, kernel)).toThrow(ReaderClosedError);
        expect(kernel).not.toHaveBeenCalled();
        reach.reach("closed reader refused");
        reach.reach("close returns credit");
      }),
      { seed, numRuns: 40 },
    );
    reach.assertReached();
  },
  120_000,
);
