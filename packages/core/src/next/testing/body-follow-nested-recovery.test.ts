/** Follow's deferred USER AFTER packets belong to the statement, not the transaction.
 * Cancellation and savepoint recovery retire their charged/spilled graph before reuse. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FOLLOW_NESTED_RECOVERY_REACH } from "../../testing/reach.js";
import type { Savepoint } from "../access/write-path.js";
import { TriggerAfterQueue } from "../exec/trigger-after-queue.js";
import { NativePrimaryAdapter } from "../write/primary-command.js";
import { batch, ints, rules } from "../write/test-support.js";
import { commandRootFixture } from "./command-root.test-support.js";
import { oracleRows } from "./differential-adapter.js";

for (const seed of seedsFor("next-follow-nested-recovery", [0xb713, 0xb714]))
  it(`recovers spilled follow packets within a live transaction seed ${String(seed)}`, async () => {
    const reach = createReach("follow nested recovery", FOLLOW_NESTED_RECOVERY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 64, max: 72 }),
        fc.constantFrom("cancel", "failed", "rollback"),
        fc.boolean(),
        fc.integer({ min: 1, max: 99 }),
        async (count, mode, published, prefix) => {
          reach.beginHistory();
          const points: Savepoint[] = [];
          const invalid = mode === "failed",
            f = await commandRootFixture(
              [
                "CREATE TABLE keep(id INT PRIMARY KEY,v INT)",
                "CREATE TABLE p(id INT PRIMARY KEY)",
                `CREATE TABLE c(id INT PRIMARY KEY,pid INT UNIQUE REFERENCES p(id)${invalid ? ",g INT GENERATED ALWAYS AS(CASE WHEN pid>100 THEN 1 ELSE pid END) STORED,CONSTRAINT child_group_unique UNIQUE(g)" : ""})`,
                "CREATE TABLE audit(id INT PRIMARY KEY,v INT)",
                "CREATE TABLE trace(id INT PRIMARY KEY,v INT)",
                "INSERT INTO audit VALUES(1,0)",
                "INSERT INTO trace VALUES(1,0)",
                `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)})`).join(",")}`,
                `INSERT INTO c(id,pid) VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)})`).join(",")}`,
                "CREATE TRIGGER child_before BEFORE UPDATE ON c BEGIN UPDATE audit SET v=v+1 WHERE id=1; END",
                "CREATE TRIGGER audit_after AFTER UPDATE ON audit BEGIN UPDATE trace SET v=v+1 WHERE id=1; END",
              ],
              invalid ? "UPDATE p SET id=id+100" : `UPDATE p SET id=(id%${String(count)})+1`,
              published,
              64 << 20,
              false,
              async (tx) => {
                const s = tx.statement({ rules }),
                  table = s.catalog.tableByName("keep");
                if (!table) throw Error("keep table");
                await s.table(table.id).insert(batch(ints([0]), ints([prefix])), null);
                await s.end();
                points.push(tx.savepoint());
              },
            ),
            reason = new Error("cancel after deferred packets spilled");
          let deferred = 0,
            acceptedTrace = 0,
            activeFiles = 0,
            sawSpilledDeferred = false;
          const create = f.spill.create.bind(f.spill),
            files = vi.spyOn(f.spill, "create").mockImplementation(async (id) => {
              const file = await create(id);
              activeFiles++;
              const close = file.close.bind(file);
              let closed = false;
              file.close = async () => {
                await close();
                if (!closed) {
                  closed = true;
                  activeFiles--;
                }
              };
              return file;
            }),
            append = Reflect.get(TriggerAfterQueue.prototype, "append"),
            packets = vi
              .spyOn(TriggerAfterQueue.prototype, "append")
              .mockImplementation(async function (this: TriggerAfterQueue, event) {
                await append.call(this, event);
                if (event.kind === "deferredTrigger") deferred++;
              }),
            drain = Reflect.get(TriggerAfterQueue.prototype, "drain"),
            drains = vi.spyOn(TriggerAfterQueue.prototype, "drain").mockImplementation(function (
              this: TriggerAfterQueue,
              visit,
            ) {
              return drain.call(this, async (event) => {
                // drain flushes and reads its tape before delivering this decoded packet.
                if (event.kind === "deferredTrigger") {
                  expect(f.spill.bytesInUse).toBeGreaterThan(0);
                  sawSpilledDeferred = true;
                  reach.reach("deferred packets spilled");
                  if (mode === "cancel") f.stop.abort(reason);
                }
                await visit(event);
              });
            }),
            apply = Reflect.get(NativePrimaryAdapter.prototype, "apply"),
            writes = vi
              .spyOn(NativePrimaryAdapter.prototype, "apply")
              .mockImplementation(async function (this: NativePrimaryAdapter, ...args) {
                const out = await apply.apply(this, args);
                if (args[0].tableId === f.statement.catalog.tableByName("trace")?.id)
                  acceptedTrace++;
                return out;
              });
          try {
            expect(await f.root.run({ slots: [] })).toBe(count);
            if (mode === "rollback") {
              await f.root.end();
              expect(deferred).toBe(count);
              expect(acceptedTrace).toBe(count);
              reach.reach("successful deferred effects before rollback");
            } else {
              let error: unknown = null;
              try {
                await f.root.end();
              } catch (caught) {
                error = caught;
              }
              if (mode === "cancel") {
                expect(error).toBe(reason);
                expect(deferred).toBe(count);
                expect(sawSpilledDeferred).toBe(true);
                reach.reach("cancel after spill");
              } else {
                const child = f.statement.catalog.tableByName("c");
                expect(error).toMatchObject({
                  sqlState: "23505",
                  tableId: child?.id,
                  indexId: child?.indexes.find((i) => i.name === "child_group_unique")?.id,
                });
                expect(deferred).toBe(count);
                reach.reach("failed statement inside savepoint");
              }
              expect(acceptedTrace).toBe(0);
            }
            // Retire only the statement graph. Closing/rolling back the transaction
            // here would conceal savepoint and continuation bugs.
            await f.retireStatement();
            expect(f.statement.tx.state).toBe("open");
            expect(f.metadata.allocated).toBe(0);
            expect(f.source.allocated).toBe(0);
            expect(f.source.loaned).toBe(0);
            expect(activeFiles).toBe(0);
            expect(f.spill.bytesInUse).toBe(0);
            reach.reach("zero graph charge and spill files before continuation");
            const point = points[0];
            if (!point) throw Error("savepoint");
            f.tx.rollbackTo(point);
            f.tx.release(point);
            if (mode === "rollback")
              reach.reach("rollback to savepoint undoes successful deferred effects");
            const next = f.tx.statement({ rules }),
              keep = next.catalog.tableByName("keep");
            if (!keep) throw Error("keep table");
            await next.table(keep.id).insert(batch(ints([1]), ints([prefix + 1])), null);
            await next.end();
            const receipt = await f.tx.commit(null);
            await receipt.durable;
            for (const [sql, expected] of [
              [
                "SELECT id,v FROM keep ORDER BY id",
                [
                  { id: 0, v: prefix },
                  { id: 1, v: prefix + 1 },
                ],
              ],
              [
                "SELECT id FROM p ORDER BY id",
                Array.from({ length: count }, (_, i) => ({ id: i + 1 })),
              ],
              [
                "SELECT id,pid FROM c ORDER BY id",
                Array.from({ length: count }, (_, i) => ({ id: i + 1, pid: i + 1 })),
              ],
              ["SELECT v FROM audit", [{ v: 0 }]],
              ["SELECT v FROM trace", [{ v: 0 }]],
            ] as const) {
              const got = await f.core.execute(sql);
              if (!("batches" in got)) throw Error("rows");
              expect(oracleRows(got)).toEqual(expected);
            }
            reach.reach("prefix and later command commit");
            reach.reach(published ? "published" : "resident");
          } finally {
            packets.mockRestore();
            drains.mockRestore();
            files.mockRestore();
            writes.mockRestore();
            await f.close();
          }
          expect(f.core.harness.governor.usedBytes).toBe(0);
          expect(activeFiles).toBe(0);
          expect(f.spill.bytesInUse).toBe(0);
          reach.reach("zero native charge");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [64, "cancel", false, 1],
          [65, "failed", true, 2],
          [66, "rollback", false, 3],
          [64, "rollback", true, 4],
          [65, "cancel", true, 5],
          [66, "failed", false, 6],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
