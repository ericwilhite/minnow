/** A closed streaming index cursor drains a pending read without writing its caller's buffer. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { Governor } from "../memory/governor.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { autocommit, batch, column, Harness, INT, ints, tableInput } from "./test-support.js";

it.each(seedsFor("next-index-close-collect", [0xe131, 0xe132]))(
  "ends a pending streaming read without recreating rows (seed %i)",
  async (seed) => {
    const reach = createReach("range close", [
      "ascending",
      "descending",
      "pending",
      "metadata",
      "block",
      "no-collect",
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
        fc.boolean(),
        fc.boolean(),
        async (count, capacity, descending, releasePin, block, cancel) => {
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
          await path.recycleLog();
          await path.quiesce();
          const pin = path.pin(),
            indexId = path.catalog.table(tableId)?.primaryKey;
          if (indexId == null) throw new Error("fixture primary key");
          const index = pin.index(tableId, indexId),
            read = harness.space.read.bind(harness.space),
            collect = vi.spyOn(index, "collect");
          const metadata = new Set(
            index.table.tableView.runs
              .get(index.def.id)
              ?.flatMap((run) => [run.entries, run.fences, run.bloom]),
          );
          vi.spyOn(harness.space, "peek").mockReturnValue(null);
          let entered!: () => void, resume!: () => void;
          const arrived = new Promise<void>((resolve) => {
              entered = resolve;
            }),
            gate = new Promise<void>((resolve) => {
              resume = resolve;
            });
          vi.spyOn(harness.space, "read").mockImplementation(async (...args) => {
            const lease = await read(...args);
            if (block && args[0].some((ref) => metadata.has(ref))) return lease;
            entered();
            await gate;
            return lease;
          });
          const plan = index.planRange({ lower: null, upper: null }, descending ? "desc" : "asc");
          const governor = new Governor(plan.minimumBytes + 1024 * 1024);
          const grant = await governor.admit({
            label: "pending cursor",
            minimumBytes: plan.minimumBytes,
            spillable: false,
          });
          const controller = new AbortController();
          const mapped = vi.spyOn(index.table, "locator"),
            cursor = plan.open({ budget: grant, signal: controller.signal }),
            out = new Float64Array(capacity).fill(-1),
            pending = Promise.resolve(
              block && count > capacity ? cursor.skip(capacity) : cursor.next(out),
            );
          void pending.catch(() => undefined);
          try {
            const reached = await Promise.race([
              arrived.then(() => true),
              pending.then(() => false),
            ]);
            expect(reached, "pending storage-read gate must be reached").toBe(true);
            reach.reach("pending");
            reach.reach(block ? "block" : "metadata");
            if (cancel) controller.abort(new Error("external cancellation"));
            cursor.close();
            cursor.close();
            if (releasePin) {
              pin.release();
              reach.reach("released-pin");
            }
            resume();
            if (cancel) await expect(pending).rejects.toThrow("external cancellation");
            else if (block && count > capacity) expect(await pending).toBeLessThanOrEqual(capacity);
            else expect(await pending).toBe(0);
            expect([...out]).toEqual(Array.from({ length: capacity }, () => -1));
            reach.reach("unchanged-output");
            expect(mapped).not.toHaveBeenCalled();
            expect(collect).not.toHaveBeenCalled();
            reach.reach("no-collect");
            reach.reach("no-rematerialization");
            if (!releasePin && !cancel) expect(await cursor.next(out)).toBe(0);
            reach.reach(descending ? "descending" : "ascending");
          } finally {
            resume();
            await Promise.allSettled([pending]);
            cursor.close();
            expect(grant.allocated).toBe(0);
            expect(grant.held).toBe(plan.minimumBytes);
            grant.close();
            expect(governor.usedBytes).toBe(0);
            pin.release();
            vi.restoreAllMocks();
            await harness.close();
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [4, 2, false, false, false, false],
          [4, 2, true, true, false, true],
          [4, 2, false, false, true, true],
          [4, 2, true, true, true, false],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
