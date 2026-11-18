/** Optional resident releases and dictionaries must not turn metadata pressure into a refusal. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_WRITE_OPTIONAL_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { defined, readChunk, writeGroup } from "../page/test-support.js";
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
    const reach = createReach("metadata dictionary fallback", NATIVE_WRITE_OPTIONAL_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 120, max: 160 }), async (width) => {
        reach.beginHistory();
        const governor = new Governor(2 << 20);
        const grant = await governor.admit({
          label: "page workspace",
          minimumBytes: 96 << 10,
          spillable: false,
        });
        const holder = await governor.admit({
          label: "competing reader",
          minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
          spillable: false,
        });
        const budget = new MetadataBudget(grant, () => 0, governor.capBytes);
        const attempt = budget.tryAllocate.bind(budget);
        let refused = 0;
        budget.tryAllocate = (bytes) => {
          const ok = attempt(bytes);
          if (!ok) refused++;
          return ok;
        };
        try {
          const encoder = new TextEncoder();
          const values = Array.from({ length: 4096 }, (_, i) =>
            encoder.encode(`${String(i >> 2)}${"x".repeat(width)}`),
          );
          const { space, group } = await writeGroup([{ type: { kind: "text" }, values }], {
            charge: budget,
            rawPageBytes: 4096,
            pageBytes: 4096,
          });
          expect(refused).toBeGreaterThan(0);
          reach.reach("preferred size declined");
          expect(await readChunk(space, defined(group.columns[0]))).toEqual(values);
          reach.reach("cheaper path exact");
          expect(grant.held).toBe(96 << 10);
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
