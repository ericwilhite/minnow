/** R25/root parse funding: one committed list may be borrowed only from its funded root. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_ROOT_LIST_FUNDING_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { ExtentListKeeper } from "../storage/extent-list.js";

for (const seed of seedsFor("next-native-root-list-funding", [0xb7a3, 0xb7a4]))
  it(`hands exact committed metadata to its funded root once (${String(seed)})`, async () => {
    const reach = createReach("native root list funding", NATIVE_ROOT_LIST_FUNDING_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 20 }), async (count) => {
        reach.beginHistory();
        const governor = new Governor(8 << 20);
        const grant = await governor.admit({
          label: "list and root funding",
          minimumBytes: 4 << 20,
          spillable: false,
        });
        const space = new MemoryExtentSpace();
        const keeper = new ExtentListKeeper([], { reader: space, budget: grant, pageExtents: 8 });
        const writer = space.beginWrite("root list");
        try {
          const prepared = await keeper.prepare(1, {
            added: Array.from({ length: count }, (_, i) => ({
              offset: (i + 1) * 8192,
              length: 1000,
            })),
            released: [],
            writer,
          });
          const pending = grant.allocated;
          expect(pending).toBeGreaterThan(0);
          expect(() => keeper.borrowCommittedList(prepared.list)).toThrow(TypeError);
          expect(grant.allocated).toBe(pending);
          reach.reach("pending funding preserved");
          keeper.commit(1, 1);
          expect(grant.allocated).toBeGreaterThan(0);
          reach.reach("standalone keeper stays funded");
          // The root is funded before it retains the exact list. It owns this charge
          // until its own last reference dies, independently of keeper close/reopen.
          const rootBytes = 256 + 256 * prepared.list.length;
          expect(grant.allocate(rootBytes)).toBe(true);
          let root: { list: typeof prepared.list } | undefined = { list: prepared.list };
          const fundedList = root.list;
          const before = grant.allocated;
          expect(() => keeper.borrowCommittedList([...fundedList])).toThrow(TypeError);
          expect(grant.allocated).toBe(before);
          reach.reach("different identity refused");
          keeper.borrowCommittedList(fundedList);
          expect(grant.allocated).toBe(rootBytes);
          keeper.borrowCommittedList(fundedList);
          expect(grant.allocated).toBe(rootBytes);
          reach.reach("one root reservation");
          await keeper.prepare(2, { added: [], released: [], writer });
          expect(() => keeper.borrowCommittedList(fundedList)).toThrow(TypeError);
          expect(grant.allocated).toBe(rootBytes);
          keeper.abort(2);
          keeper.close();
          expect(grant.allocated).toBe(rootBytes);
          reach.reach("root survives keeper close");
          root = undefined;
          grant.free(rootBytes);
          expect(grant.allocated).toBe(0);
        } finally {
          keeper.close();
          await writer.abort();
          grant.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero final charge");
      }),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  });
