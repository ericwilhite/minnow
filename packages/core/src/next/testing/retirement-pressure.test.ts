/** R25/R8: immutable exact release inventories, fixed retirement window, both roots and pins. */
import { afterAll, expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, BOUNDED_RETIREMENT_REACH } from "../../testing/reach.js";
import type { RetirementInventory } from "../storage/retirement-contract.js";
import { DATA_ALIGN } from "../storage/contract.js";
import { ExtentListKeeper, extentsOf } from "../storage/extent-list.js";
import {
  RETIREMENT_CURSOR_BYTES,
  RETIREMENT_OWNER_BYTES,
} from "../storage/retirement-inventory.js";
import {
  descriptors,
  decodeRoot,
  encodeRoot,
  fixture,
  openOwner,
  pressure,
  releasedBytes,
  trackReleases,
} from "./retirement-test-support.js";
const reach = createReach("bounded arbitrary retirement", BOUNDED_RETIREMENT_REACH);
for (const seed of seedsFor("bounded-retirement", [0xb7f1, 0xb7f2]))
  it(
    `keeps large retirement bounded and exactly once after both roots and a pin (${String(seed)})`,
    { timeout: 60_000 },
    async () => {
      reach.beginHistory();
      const owner = await openOwner();
      let held: Awaited<ReturnType<typeof pressure>> | undefined;
      let inventoryToRelease: RetirementInventory | undefined;
      let releasePin: (() => void) | undefined;
      try {
        const count = 8193 + Math.floor(mulberry32(seed)() * 64);
        const { root, anchor, firstBlob } = await fixture(owner, count);
        const ranges = trackReleases(owner.session),
          pin = owner.space.pin("old FTS generation");
        releasePin = () => pin.release();
        held = await pressure(owner);
        const budget = held.grant,
          keeper = new ExtentListKeeper(root.list, {
            reader: owner.space,
            budget,
            pageExtents: 64,
          });
        const baseline = budget.allocated;
        const inventory = await owner.space.prepareRetirements(
          descriptors(root.first, count - 1, true),
          { budget },
        );
        inventoryToRelease = inventory;
        expect(inventory.count).toBe(count - 1);
        expect(budget.allocated - baseline).toBe(RETIREMENT_OWNER_BYTES);
        reach.reach("large fixed-cap inventory");
        reach.reach("reverse source");
        const cursor = inventory.cursor({ budget });
        let at = 0;
        try {
          while (await cursor.next()) {
            expect(cursor.offset).toBe(root.first + at++ * DATA_ALIGN);
          }
        } finally {
          await cursor.close();
        }
        expect(at).toBe(count - 1);
        expect(budget.allocated - baseline).toBe(RETIREMENT_OWNER_BYTES);
        reach.reach("multiple replay windows");
        const writer = owner.space.beginWrite("large retirement", { budget });
        try {
          const prepared = await keeper.prepare(2, {
            added: writer.extents,
            released: [],
            releasedInventory: inventory,
            writer,
          });
          expect(await extentsOf(prepared.list, owner.space, budget)).toEqual([anchor.extent]);
          const next = { first: anchor.extent.offset, count: 1, list: prepared.list };
          await owner.space.publish(encodeRoot(next), {
            writers: [writer],
            retired: prepared.retire,
            retiredInventory: inventory,
            list: prepared.list,
            budget,
          });
          keeper.commit(2, 2);
          // Descriptor inventory is not a data owner: the two native roots and pin are the owners.
          await inventory.release();
          expect(releasedBytes(ranges, root.first, (count - 1) * DATA_ALIGN)).toBe(0);
          reach.reach("previous-root protection");
          await owner.space.publish(encodeRoot(next), {
            writers: [],
            retired: [],
            list: next.list,
            budget,
          });
          await owner.space.settled();
          expect(releasedBytes(ranges, root.first, (count - 1) * DATA_ALIGN)).toBe(0);
          const read = await owner.space.read([anchor, firstBlob], { budget });
          try {
            expect(read.bytes[1]?.[0]).toBe(0);
          } finally {
            read.release();
          }
          reach.reach("pin protection after both roots drain");
          pin.release();
          await owner.space.settled();
          expect(releasedBytes(ranges, root.first, (count - 1) * DATA_ALIGN)).toBe(
            (count - 1) * DATA_ALIGN,
          );
          expect(releasedBytes(ranges, anchor.extent.offset, DATA_ALIGN)).toBe(0);
          // Independent per-allocation oracle: an omitted extent plus a duplicate elsewhere
          // must not cancel out in the aggregate byte total. Linear in extents + release ranges.
          const difference = new Int32Array(count);
          for (const range of ranges) {
            const start = Math.max(root.first, range.offset),
              end = Math.min(root.first + (count - 1) * DATA_ALIGN, range.offset + range.length);
            if (start >= end) continue;
            expect((start - root.first) % DATA_ALIGN).toBe(0);
            expect((end - root.first) % DATA_ALIGN).toBe(0);
            const begin = (start - root.first) / DATA_ALIGN,
              stop = (end - root.first) / DATA_ALIGN;
            difference[begin] = (difference[begin] ?? 0) + 1;
            difference[stop] = (difference[stop] ?? 0) - 1;
          }
          let references = 0;
          for (let index = 0; index < count - 1; index++) {
            references += difference[index] ?? 0;
            expect(references).toBe(1);
          }

          reach.reach("exactly-once physical release");
          await owner.space.audit();
        } finally {
          await inventory.release();
          keeper.close();
          pin.release();
          if (writer.state === "open") await writer.abort();
        }
        expect(budget.allocated).toBe(0);
        expect(RETIREMENT_CURSOR_BYTES).toBeLessThan(70 << 10);
        held.close();
        held = undefined;
        await owner.close();
        const reopened = await openOwner(owner.backend);
        try {
          const bytes = reopened.space.root;
          if (bytes === null) throw new Error("Lost bounded retirement root on reopen");
          const root = decodeRoot(bytes),
            keeper = new ExtentListKeeper(root.list, {
              reader: reopened.space,
              budget: reopened.startup,
              pageExtents: 64,
            });
          const writer = reopened.space.beginWrite("write after bounded retirement", {
            budget: reopened.startup,
          });
          try {
            const written = await writer.write([new Uint8Array([23])]),
              extra = written.blobs[0];
            if (extra === undefined) throw new Error("Missing continued write");
            const prepared = await keeper.prepare(3, {
              added: writer.extents,
              released: [],
              writer,
            });
            const next = { ...root, extra, list: prepared.list };
            await reopened.space.publish(encodeRoot(next), {
              writers: [writer],
              retired: prepared.retire,
              list: prepared.list,
            });
            keeper.commit(3, 3);
            const read = await reopened.space.read([anchor, extra], { budget: reopened.startup });
            try {
              expect(read.bytes[1]?.[0]).toBe(23);
            } finally {
              read.release();
            }
            await reopened.space.audit();
          } finally {
            keeper.close();
            if (writer.state === "open") await writer.abort();
          }
        } finally {
          await reopened.close();
        }
        const final = await openOwner(owner.backend);
        try {
          await final.space.audit();
          expect(decodeRoot(final.space.root ?? new Uint8Array()).extra).toBeDefined();
        } finally {
          await final.close();
        }
      } finally {
        releasePin?.();
        try {
          await inventoryToRelease?.release();
        } finally {
          try {
            held?.close();
          } finally {
            await owner.close();
          }
        }
      }
    },
  );
afterAll(() => reach.assertReached());
