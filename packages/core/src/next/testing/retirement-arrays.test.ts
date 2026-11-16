/** Resident descriptors are a bounded fast path, not a prerequisite for large publications. */
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, BOUNDED_RETIREMENT_ARRAY_REACH } from "../../testing/reach.js";
import { DATA_ALIGN } from "../storage/contract.js";
import { ExtentListKeeper, extentsOf } from "../storage/extent-list.js";
import type { ExtentRef } from "../storage/extents.js";
import { descriptors, fixture, openOwner, pressure } from "./retirement-test-support.js";

const reach = createReach("retirement source shapes", BOUNDED_RETIREMENT_ARRAY_REACH);
/** A dense, read-only caller array without retaining N descriptor objects in the fixture.
 * Both the real parent keeper and the candidate receive the same array contract. */
function releaseArray(first: number, count: number): readonly ExtentRef[] {
  return new Proxy([] as ExtentRef[], {
    get(target, key, receiver) {
      if (key === "length") return count;
      if (typeof key === "string" && /^(0|[1-9][0-9]*)$/u.test(key)) {
        const index = Number(key);
        return index < count ? { offset: first + index * DATA_ALIGN, length: 1 } : undefined;
      }
      return Reflect.get(target, key, receiver) as unknown;
    },
  });
}
for (const seed of seedsFor("bounded-retirement-arrays", [0xb7f5])) {
  for (const count of [4095, 4096, 4097, 8193] as const)
    it(
      `bounded array route at ${String(count)} releases, seed ${String(seed)}`,
      { timeout: 60_000 },
      async () => {
        reach.beginHistory();
        const owner = await openOwner();
        let held: Awaited<ReturnType<typeof pressure>> | undefined;
        try {
          const { root, anchor } = await fixture(owner, count + 1);
          held = await pressure(owner);
          const budget = held.grant,
            keeper = new ExtentListKeeper(root.list, {
              reader: owner.space,
              budget,
              pageExtents: 64,
            }),
            writer = owner.space.beginWrite("array boundary", { budget });
          try {
            const result = await keeper.prepare(2, {
              added: [],
              released: releaseArray(root.first, count),
              writer,
            });
            expect(await extentsOf(result.list, owner.space, budget)).toEqual([anchor.extent]);
            keeper.abort(2);
            reach.reach(
              (
                {
                  4095: "array-4095",
                  4096: "array-4096",
                  4097: "array-4097",
                  8193: "array-8193",
                } as const
              )[count],
            );
            expect(budget.allocated).toBe(0);
          } finally {
            keeper.close();
            await writer.abort();
          }
        } finally {
          held?.close();
          await owner.close();
        }
      },
    );
  it(`mixes an immutable source with bounded array releases, without a nested-workspace wait (${String(seed)})`, async () => {
    reach.beginHistory();
    const owner = await openOwner();
    try {
      const { root, anchor } = await fixture(owner, 17),
        budget = owner.startup;
      const inventory = await owner.space.prepareRetirements(descriptors(root.first, 8), {
        budget,
      });
      const keeper = new ExtentListKeeper(root.list, {
          reader: owner.space,
          budget,
          pageExtents: 4,
        }),
        writer = owner.space.beginWrite("mixed release sources", { budget });
      try {
        const prepared = await keeper.prepare(2, {
          added: [],
          released: [...descriptors(root.first + 8 * DATA_ALIGN, 8)],
          releasedInventory: inventory,
          writer,
        });
        expect(await extentsOf(prepared.list, owner.space, budget)).toEqual([anchor.extent]);
        keeper.abort(2);
        const cursor = inventory.cursor({ budget });
        try {
          expect(await cursor.next()).toBe(true);
        } finally {
          await cursor.close();
        }
        reach.reach("mixed-source reentry");
      } finally {
        keeper.close();
        await writer.abort();
        await inventory.release();
      }
    } finally {
      await owner.close();
    }
  });
  it(`small resident releases avoid the measurable private-inventory I/O (${String(seed)})`, async () => {
    reach.beginHistory();
    const counts: number[] = [];
    for (const forced of [false, true]) {
      const owner = await openOwner();
      try {
        const { root } = await fixture(owner, 3),
          budget = owner.startup,
          metadata = owner.session.metadata?.();
        if (metadata == null) throw new Error("Missing metadata store");
        let io = 0;
        const read = metadata.read.bind(metadata),
          write = metadata.write.bind(metadata);
        metadata.read = async (...args) => {
          io++;
          await read(...args);
        };
        metadata.write = async (...args) => {
          io++;
          await write(...args);
        };
        const writer = owner.space.beginWrite("small release I/O", { budget }),
          keeper = new ExtentListKeeper(root.list, {
            reader: owner.space,
            budget,
            pageExtents: 64,
          });
        const inventory = forced
          ? await owner.space.prepareRetirements(descriptors(root.first, 1), { budget })
          : undefined;
        try {
          await keeper.prepare(2, {
            added: [],
            released: forced ? [] : [{ offset: root.first, length: 1 }],
            ...(inventory === undefined ? {} : { releasedInventory: inventory }),
            writer,
          });
          counts.push(io);
          keeper.abort(2);
        } finally {
          keeper.close();
          await writer.abort();
          await inventory?.release();
        }
      } finally {
        await owner.close();
      }
    }
    expect(counts[1]).toBeGreaterThan(counts[0] ?? Infinity);
    reach.reach("bounded resident I/O saving");
  });
}

afterAll(() => reach.assertReached());
