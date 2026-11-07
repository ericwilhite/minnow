/** A closed index cursor never reinstalls a pending collection or writes its caller's buffer. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { autocommit, batch, column, Harness, INT, ints, tableInput } from "./test-support.js";

it.each(seedsFor("next-index-close-collect", [0xe131, 0xe132]))(
  "ends a pending collect without recreating rows (seed %i)",
  async (seed) => {
    const reach = createReach("range close", [
      "ascending",
      "descending",
      "pending",
      "released-pin",
      "unchanged-output",
      "no-rematerialization",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 32 }),
        fc.integer({ min: 1, max: 8 }),
        fc.boolean(),
        fc.boolean(),
        async (count, capacity, descending, releasePin) => {
          reach.beginHistory();
          const harness = new Harness(),
            path = await harness.open();
          let tableId = 0;
          await autocommit(path, async (s) => {
            tableId =
              (
                await s.changeCatalog({
                  kind: "createTable",
                  table: tableInput("t", [column("id", INT, false)]),
                })
              ).id ?? 0;
          });
          await autocommit(path, (s) =>
            s.table(tableId).insert(batch(ints(Array.from({ length: count }, (_, k) => k))), null),
          );
          const pin = path.pin(),
            indexId = path.catalog.table(tableId)?.primaryKey;
          if (indexId == null) throw new Error("fixture primary key");
          const index = pin.index(tableId, indexId),
            collect = index.collect.bind(index);
          let entered!: () => void, resume!: () => void;
          const arrived = new Promise<void>((resolve) => {
              entered = resolve;
            }),
            gate = new Promise<void>((resolve) => {
              resume = resolve;
            });
          vi.spyOn(index, "collect").mockImplementation(async (range) => {
            const entries = await collect(range);
            entered();
            await gate;
            return entries;
          });
          const mapped = vi.spyOn(index.table, "locator"),
            cursor = index.range({ lower: null, upper: null }, descending ? "desc" : "asc"),
            out = new Float64Array(capacity).fill(-1),
            pending = Promise.resolve(cursor.next(out));
          try {
            await arrived;
            reach.reach("pending");
            cursor.close();
            cursor.close();
            if (releasePin) {
              pin.release();
              reach.reach("released-pin");
            }
            resume();
            expect(await pending).toBe(0);
            expect([...out]).toEqual(Array.from({ length: capacity }, () => -1));
            reach.reach("unchanged-output");
            expect(mapped).not.toHaveBeenCalled();
            reach.reach("no-rematerialization");
            if (!releasePin) expect(await cursor.next(out)).toBe(0);
            reach.reach(descending ? "descending" : "ascending");
          } finally {
            resume();
            await Promise.allSettled([pending]);
            cursor.close();
            pin.release();
            vi.restoreAllMocks();
            await harness.close();
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [4, 2, false, false],
          [4, 2, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
