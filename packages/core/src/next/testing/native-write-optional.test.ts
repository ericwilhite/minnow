/** Optional resident releases and dictionaries must not turn metadata pressure into a refusal. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_WRITE_OPTIONAL_REACH,
  NATIVE_WRITE_DICTIONARY_PRESSURE_REACH,
} from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { defined, openChunk, readChunk, vectorOf } from "../page/test-support.js";
import { GroupWriter, groupWriterMinimum } from "../page/writer.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { extentsOf, ExtentListKeeper } from "../storage/extent-list.js";
import { MetadataBudget } from "../write/budget.js";
import { descriptors, fixture, openOwner, pressure } from "./retirement-test-support.js";

for (const seed of seedsFor("next-native-write-optional", [0xb7a1, 0xb7a2])) {
  it(`spools an optional metadata reservation instead of requesting larger credit (${String(seed)})`, async () => {
    const reach = createReach("metadata retirement fallback", NATIVE_WRITE_OPTIONAL_REACH);
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(4095, 4096), async (count) => {
        reach.beginHistory();
        const owner = await openOwner();
        let held: Awaited<ReturnType<typeof pressure>> | undefined;
        try {
          const { root, anchor } = await fixture(owner, count + 1, 64);
          held = await pressure(owner, 192 << 10);
          const budget = new MetadataBudget(held.grant, () => 0, owner.governor.capBytes);
          const attempt = budget.tryAllocate.bind(budget);
          let refused = 0;
          budget.tryAllocate = (bytes) => {
            const ok = attempt(bytes);
            if (!ok) refused++;
            return ok;
          };
          const keeper = new ExtentListKeeper(root.list, {
            reader: owner.space,
            budget,
            pageExtents: 64,
          });
          const writer = owner.space.beginWrite("metadata fallback", { budget });
          const bytes = held.grant.held;
          try {
            const result = await keeper.prepare(2, {
              added: [],
              released: [...descriptors(root.first, count)],
              writer,
            });
            expect(refused).toBeGreaterThan(0);
            reach.reach("preferred size declined");
            expect(await extentsOf(result.list, owner.space, budget)).toEqual([anchor.extent]);
            reach.reach("cheaper path exact");
            expect(held.grant.held).toBe(bytes);
            reach.reach("no grant growth");
            keeper.abort(2);
          } finally {
            keeper.close();
            await writer.abort();
          }
        } finally {
          held?.close();
          await owner.close();
        }
        expect(owner.governor.usedBytes).toBe(0);
        reach.reach("zero final charge");
      }),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  });

  it(`falls back to exact plain pages when optional dictionary credit is unavailable (${String(seed)})`, async () => {
    const reach = createReach(
      "metadata dictionary fallback",
      NATIVE_WRITE_DICTIONARY_PRESSURE_REACH,
    );
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 120, max: 160 }), async (width) => {
        reach.beginHistory();
        const governor = new Governor(2 << 20);
        const encoder = new TextEncoder();
        const values = Array.from({ length: 4096 }, (_, i) =>
          encoder.encode(`${String(i >> 2)}${"x".repeat(width)}`),
        );
        const bytes = values.reduce((n, value) => n + value.length, 0);
        const options = {
          columns: [
            {
              columnId: 1,
              type: { kind: "text" as const },
              valueBytes: bytes,
              maxValueBytes: width + 4,
              dictionaryValueBytes: bytes,
              largeValues: 0,
            },
          ],
          maxRows: values.length,
          rawPageBytes: 4096,
          pageBytes: 4096,
        };
        // The immutable fixture supplies its actual bounds, just like a native
        // flush. Directory entries and serialized tails are now charged; the old
        // 96KiB grant omitted that ownership. Keep the 2MiB cap and deny ALL spare
        // dictionary credit after funding the complete mandatory plain step.
        const minimum = groupWriterMinimum(options);
        expect(minimum).toBeLessThan(governor.capBytes - governor.reserveBytes);
        reach.reach("actual immutable input bounds");
        const grant = await governor.admit({
          label: "page workspace",
          minimumBytes: minimum,
          spillable: false,
        });
        const holder = await governor.admit({
          label: "competing reader",
          minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
          spillable: false,
        });
        reach.reach("complete plain workspace prepaid");
        const budget = new MetadataBudget(grant, () => 0, governor.capBytes);
        const attempt = budget.tryAllocate.bind(budget);
        let refused = 0;
        budget.tryAllocate = (bytes, reserve) => {
          const ok = attempt(bytes, reserve);
          if (!ok) refused++;
          return ok;
        };
        try {
          const space = new MemoryExtentSpace();
          const writer = new GroupWriter({
            ...options,
            writer: space.beginWrite("optional fallback"),
            charge: budget,
          });
          let group;
          try {
            const vector = vectorOf({ kind: "text" }, values);
            expect(await writer.append([vector], values.length)).toBe(values.length);
            group = await writer.finish();
          } finally {
            await writer.abort();
          }
          expect(refused).toBeGreaterThan(0);
          reach.reach("preferred size declined");
          expect(await readChunk(space, defined(group.columns[0]))).toEqual(values);
          reach.reach("cheaper path exact");
          expect(grant.held).toBe(minimum);
          const reader = await openChunk(space, defined(group.columns[0]));
          try {
            expect(reader.directory.dictionary).toBeNull();
            reach.reach("no dictionary retained by plain fallback");
          } finally {
            reader.close();
          }
          reach.reach("no grant growth");
          expect(grant.allocated).toBe(0);
        } finally {
          holder.close();
          grant.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero final charge");
      }),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  });
}
