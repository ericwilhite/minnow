/** Shipped trigger bodies have two derived write levels, including the terminal spent budget.
 * This component checks packet/frame capacity, not ordinary-owner SQL dispatch.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_BUDGET_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Charge, Pool, SpillIo } from "../exec/spill-pool.js";
import { PageWriter, SpillFiles } from "../exec/spill-pages.js";
import {
  TriggerAfterQueue,
  TRIGGER_AFTER_QUEUE_BYTES,
  TRIGGER_MAX_FRAMES,
} from "../exec/trigger-after-queue.js";

for (const seed of seedsFor("next-trigger-budget", [0xb7d6, 0xb7d7]))
  it(`keeps the shipped derived-write boundary in bounded frames seed ${String(seed)}`, async () => {
    const reach = createReach("trigger signed budget", TRIGGER_BUDGET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 4 }),
        fc.integer({ min: 1, max: 3 }),
        fc.boolean(),
        fc.boolean(),
        async (depth, rows, before, pressure) => {
          reach.beginHistory();
          const shipped = new MinnowDatabase(new MemoryBlockStore(), {
            compression: "raw",
            rowsPerBlock: 8,
          });
          try {
            for (let level = 0; level < depth; level++)
              await shipped.execute(`CREATE TABLE t${String(level)}(n INT)`);
            for (let level = 0; level < depth - 1; level++)
              await shipped.execute(
                `CREATE TRIGGER tr${String(level)} ${before ? "BEFORE" : "AFTER"} INSERT ON t${String(level)} BEGIN INSERT INTO t${String(level + 1)}(n) VALUES(NEW.n); END`,
              );
            let failure: unknown;
            try {
              await shipped.execute(
                `INSERT INTO t0(n) VALUES ${Array.from({ length: rows }, (_, i) => `(${String(i)})`).join(",")}`,
              );
            } catch (error) {
              failure = error;
            }
            expect(failure === undefined).toBe(depth <= 3);
            if (depth === 4) {
              expect(failure).toBeInstanceOf(Error);
              expect((failure as Error).message).toContain("Trigger cascade depth exceeded");
              reach.reach("third trigger rejected and rolled back");
            }
            for (let level = 0; level < depth; level++)
              expect(
                (await shipped.query(`SELECT n FROM t${String(level)} ORDER BY n`)).rows,
              ).toEqual(depth > 3 ? [] : Array.from({ length: rows }, (_, i) => ({ n: i })));
            if (depth === 3) reach.reach("two derived writes succeed");
          } finally {
            await shipped.close();
          }
          const governor = new Governor(8 << 20),
            grant = await governor.admit({
              label: "three trigger frames",
              minimumBytes: 2 << 20,
              spillable: true,
            }),
            charge = new Charge(grant),
            pool = new Pool(charge, TRIGGER_MAX_FRAMES * TRIGGER_AFTER_QUEUE_BYTES),
            service = new MemorySpillService(),
            files = new SpillFiles(service, 1, charge),
            pages = new PageWriter(pool, 16384),
            io = new SpillIo(pool, files, pages, { grant, queryId: 1 }, () => undefined),
            queues: TriggerAfterQueue[] = [];
          let competing: Awaited<ReturnType<Governor["admit"]>> | undefined;
          try {
            expect(TRIGGER_MAX_FRAMES).toBe(3);
            for (let level = 0; level < TRIGGER_MAX_FRAMES; level++)
              queues.push(new TriggerAfterQueue(io, service, { rootId: 1, frameId: level + 1 }));
            const held = grant.held;
            if (pressure) {
              const room = governor.capBytes - governor.reserveBytes - governor.usedBytes;
              competing = await governor.admit({
                label: "competing credit",
                minimumBytes: room,
                spillable: false,
              });
              expect(competing.allocate(room)).toBe(true);
              reach.reach("competing credit");
            }
            for (let level = 0; level < Math.min(depth, TRIGGER_MAX_FRAMES); level++) {
              const queue = queues[level];
              if (!queue) throw new Error("missing queue");
              const budget = 1 - level;
              for (let row = 0; row < rows; row++)
                await queue.append({
                  kind: "referential",
                  command: { rootId: 1, id: level + 1, frameId: level + 1 },
                  tableId: level + 1,
                  event: "insert",
                  images: { spoolId: 7, row },
                  budget,
                  id: row + 1,
                });
              let visited = 0;
              await queue.drain(async (event) => {
                expect(event.budget).toBe(budget);
                expect(event.images.row).toBe(visited++);
                expect(pool.owes).toBe(false);
              });
              expect(visited).toBe(rows);
              reach.reach(
                budget === 1
                  ? "root budget"
                  : budget === 0
                    ? "body zero budget"
                    : "terminal negative RI budget",
              );
              await expect(
                queue.append({
                  kind: "trigger",
                  command: { rootId: 1, id: level + 1, frameId: level + 1 },
                  tableId: 1,
                  event: "insert",
                  images: { spoolId: 7, row: 0 },
                  budget: -2,
                  id: 1,
                }),
              ).rejects.toThrow("Invalid AFTER");
            }
            expect(grant.held).toBe(held);
            reach.reach(before ? "BEFORE chain" : "AFTER chain");
            if (rows > 1) reach.reach("multiple root rows");
          } finally {
            await Promise.all(queues.map((q) => q.close()));
            await files.closeAll();
            pages.close();
            pool.close();
            grant.close();
            competing?.close();
          }
          expect(service.bytesInUse).toBe(0);
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, 1, false, false],
          [2, 2, true, true],
          [3, 3, false, true],
          [4, 2, true, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
