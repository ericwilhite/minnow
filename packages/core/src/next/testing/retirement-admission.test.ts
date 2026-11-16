/** Every private owner/window/publication descriptor is prepaid; refusal consumes no input. */
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, BOUNDED_RETIREMENT_ADMISSION_REACH } from "../../testing/reach.js";
import { ReadDeniedError, type ByteBudget } from "../storage/extents.js";
import {
  RETIREMENT_BORROW_BYTES,
  RETIREMENT_CURSOR_BYTES,
  RETIREMENT_OWNER_BYTES,
} from "../storage/retirement-inventory.js";
import { RETIREMENT_PREPARATION_BYTES } from "../storage/retirement-prepare.js";
import { descriptors, encodeRoot, fixture, openOwner } from "./retirement-test-support.js";
const reach = createReach(
  "retirement charge-before-allocation",
  BOUNDED_RETIREMENT_ADMISSION_REACH,
);
for (const seed of seedsFor("bounded-retirement-admission", [0xb7fb, 0xb7fc]))
  it(`refused prep, cursor and publication stay bounded and leave source/root untouched (${String(seed)})`, async () => {
    reach.beginHistory();
    const owner = await openOwner();
    try {
      const { root } = await fixture(owner, 4);
      for (const allowed of [0, RETIREMENT_PREPARATION_BYTES]) {
        let used = 0,
          visits = 0;
        const budget: ByteBudget = {
          allocate(bytes) {
            if (used + bytes > allowed) return false;
            used += bytes;
            return true;
          },
          free(bytes) {
            used -= bytes;
            expect(used).toBeGreaterThanOrEqual(0);
          },
        };
        function* source() {
          for (const extent of descriptors(root.first, 3)) {
            visits++;
            yield extent;
          }
        }
        await expect(owner.space.prepareRetirements(source(), { budget })).rejects.toBeInstanceOf(
          ReadDeniedError,
        );
        expect(visits).toBe(0);
        expect(used).toBe(0);
      }
      reach.reach("preparation before source");
      const budget = owner.startup,
        baseline = budget.allocated;
      const inventory = await owner.space.prepareRetirements(descriptors(root.first, 3), {
        budget,
      });
      try {
        let cursorRequest = 0;
        expect(() =>
          inventory.cursor({
            budget: {
              allocate(bytes) {
                cursorRequest = bytes;
                return false;
              },
              free() {
                throw new Error("Refused cursor must own no charge");
              },
            },
          }),
        ).toThrow(ReadDeniedError);
        expect(cursorRequest).toBe(RETIREMENT_CURSOR_BYTES);
        expect(budget.allocated).toBe(baseline + RETIREMENT_OWNER_BYTES);
        reach.reach("cursor before native read");
        let request = 0;
        await expect(
          owner.space.publish(encodeRoot(root), {
            writers: [],
            retired: [],
            retiredInventory: inventory,
            list: root.list,
            budget: {
              allocate(bytes) {
                request = bytes;
                return false;
              },
              free() {
                throw new Error("Refused publication must own no charge");
              },
            },
          }),
        ).rejects.toBeInstanceOf(ReadDeniedError);
        expect(request).toBe(RETIREMENT_BORROW_BYTES);
        expect(owner.space.generation).toBe(1);
        expect(owner.space.failed).toBeNull();
        reach.reach("publication before queue");
        const cursor = inventory.cursor({ budget });
        try {
          expect(await cursor.next()).toBe(true);
          expect(cursor.offset).toBe(root.first);
        } finally {
          await cursor.close();
        }
      } finally {
        await inventory.release();
      }
      expect(budget.allocated).toBe(baseline);
      await owner.space.audit();
    } finally {
      await owner.close();
    }
  });
afterAll(() => reach.assertReached());
