/** Producer failure and reentrant cancellation never strand descriptor credit or a metadata phase. */
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, BOUNDED_RETIREMENT_FAULT_REACH } from "../../testing/reach.js";
import type { ByteBudget } from "../storage/extents.js";
import { descriptors, fixture, openOwner } from "./retirement-test-support.js";
const reach = createReach("retirement partial preparation", BOUNDED_RETIREMENT_FAULT_REACH);
for (const seed of seedsFor("bounded-retirement-faults", [0xb7fd, 0xb7fe])) {
  it(`producer failures before, within and after a window leave a reusable space (${String(seed)})`, async () => {
    reach.beginHistory();
    const owner = await openOwner(),
      budget = owner.startup;
    try {
      const { root } = await fixture(owner, 5),
        baseline = budget.allocated;
      // Window is 61408/16=3838 records. Duplicates are legal preparation input, not extra claims.
      for (const [failAt, state] of [
        [0, "empty source failure"],
        [1, "first item failure"],
        [3837, "before window failure"],
        [3838, "at window failure"],
        [3839, "after window failure"],
        [7677, "multiple window failure"],
      ] as const) {
        async function* failing() {
          let i = 0;
          for (;;) {
            if (i++ === failAt) throw new Error("generated retirement producer failure");
            yield { offset: root.first, length: 1 };
          }
        }
        await expect(owner.space.prepareRetirements(failing(), { budget })).rejects.toThrow(
          "generated retirement producer failure",
        );
        expect(budget.allocated).toBe(baseline);
        expect(owner.space.failed).toBeNull();
        reach.reach(state);
        const inventory = await owner.space.prepareRetirements(descriptors(root.first, 3, true), {
          budget,
        });
        try {
          const cursor = inventory.cursor({ budget });
          let count = 0;
          try {
            while (await cursor.next()) count++;
          } finally {
            await cursor.close();
          }
          expect(count).toBe(3);
        } finally {
          await inventory.release();
        }
        expect(budget.allocated).toBe(baseline);
      }
      await owner.space.audit();
    } finally {
      await owner.close();
    }
  });
  for (const closeAt of [1, 2])
    it(`space close reentering preparation allocation ${String(closeAt)} is fenced (${String(seed)})`, async () => {
      reach.beginHistory();
      const owner = await openOwner();
      let closing: Promise<void> | undefined;
      try {
        let calls = 0,
          visits = 0;
        const budget: ByteBudget = {
          allocate(bytes) {
            const accepted = owner.startup.allocate(bytes);
            if (++calls === closeAt) closing = owner.space.close();
            return accepted;
          },
          free: (bytes) => owner.startup.free(bytes),
        };
        function* source() {
          visits++;
          yield { offset: 0, length: 1 };
        }
        await expect(owner.space.prepareRetirements(source(), { budget })).rejects.toBeInstanceOf(
          Error,
        );
        expect(visits).toBe(0);
        reach.reach(closeAt === 1 ? "close at preparation charge" : "close at owner charge");
        await closing;
        expect(owner.startup.allocated).toBe(0);
      } finally {
        await closing?.catch(() => undefined);
        await owner.close();
      }
    });
  it(`inventory release reentering cursor allocation stops construction (${String(seed)})`, async () => {
    reach.beginHistory();
    const owner = await openOwner();
    let closing: Promise<void> | undefined;
    try {
      const inventory = await owner.space.prepareRetirements([{ offset: 0, length: 1 }], {
        budget: owner.startup,
      });
      const budget: ByteBudget = {
        allocate(bytes) {
          const accepted = owner.startup.allocate(bytes);
          closing = inventory.release();
          return accepted;
        },
        free: (bytes) => owner.startup.free(bytes),
      };
      expect(() => inventory.cursor({ budget })).toThrow("closed");
      reach.reach("release at cursor charge");
      await closing;
      expect(owner.startup.allocated).toBe(0);
    } finally {
      await closing?.catch(() => undefined);
      await owner.close();
    }
  });
}

afterAll(() => reach.assertReached());
