/** Foreign buffers are temporary WRITE-account pressure, not another intrinsic floor.
 * Query pins may precede/follow the statement's locator pin; own pins cannot cause a self-wait.
 * Caller input vectors are fixture-owned, not a SQL-source admission proof. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_WRITE_STEP_BORROW_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { TableReadRecipe } from "../access/snapshot.js";
import type { StatementImpl } from "../write/transaction.js";
import { batch, ints, texts, rules, watchGrants } from "../write/test-support.js";
import { WriteCredits } from "../write/credits.js";
import { NativeWriteSteps } from "../write/native-step.js";
import { yieldTask } from "../task.js";
import { StorageCore } from "./storage-core.js";
function gate() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
for (const seed of seedsFor("next-native-write-owner-borrow", [0xb711, 0xb712]))
  it(`waits for foreign WRITE-account buffers seed ${String(seed)}`, async () => {
    const reach = createReach("native write-owner foreign borrow", NATIVE_WRITE_STEP_BORROW_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 64),
        fc.boolean(),
        fc.constantFrom("older", "newer", "self"),
        async (cap, cancel, order) => {
          reach.beginHistory();
          const entered = gate(),
            resume = gate(),
            governor = new Governor(cap * 1024 * 1024),
            core = new StorageCore(128);
          Object.defineProperty(core.harness, "governor", { value: governor });
          Object.defineProperty(core.harness, "tuning", {
            value: {
              merges: { mode: "off" },
              beforeFlushPublish: async () => {
                entered.resolve();
                await resume.promise;
              },
            },
          });
          const observed: { rows: Grant | null; rowMinimum: number; waiting: boolean } = {
            rows: null,
            rowMinimum: 0,
            waiting: false,
          };
          watchGrants(governor, (g) => {
            if (g.label === "write path") {
              observed.rows = g;
              observed.rowMinimum = g.held;
            }
          });
          await core.open();
          let snap: ReturnType<typeof core.harness.path.pin> | null = null,
            reader: Grant | null = null,
            recipe: TableReadRecipe | null = null,
            tx: Awaited<ReturnType<typeof core.harness.path.begin>> | null = null,
            s: StatementImpl | null = null,
            done: Promise<unknown> | null = null,
            waitSpy: { mockRestore(): void } | null = null,
            reclaimSpy: { mockRestore(): void } | null = null;
          const stop = new AbortController(),
            reason = new Error("foreign-buffer wait cancelled");
          const beginOwner = async () => {
            const owner = await core.harness.path.begin();
            const statement = owner.statement({ rules });
            statement.readOwn();
            return { transaction: owner, statement };
          };
          try {
            await core.execute("CREATE TABLE t(id INT PRIMARY KEY,v TEXT)");
            const table = core.harness.path.catalog.tables[0];
            if (!table) throw Error("table");
            await core.write((statement) =>
              statement
                .table(table.id)
                .insert(
                  batch(
                    ints(Array.from({ length: 128 }, (_, i) => i)),
                    texts(Array.from({ length: 128 }, () => "x".repeat(49152))),
                  ),
                  null,
                ),
            );
            await core.write((statement) =>
              statement.table(table.id).insert(batch(ints([128]), texts(["tail"])), null),
            );
            await entered.promise;
            if (order !== "older") {
              const created = await beginOwner();
              tx = created.transaction;
              s = created.statement;
            }
            if (order !== "self") {
              reader = await governor.admit({
                label: "foreign native read recipe",
                minimumBytes: 16384,
                spillable: false,
              });
              snap = core.harness.path.pin();
              recipe = snap.internalTable(table.id).prepareRead({ budget: reader });
              expect(recipe).not.toBeNull();
            }
            resume.resolve();
            await core.harness.path.quiesce();
            const borrowed = core.harness.path.stats().borrowedChunkBytes;
            if (order === "older") {
              const created = await beginOwner();
              tx = created.transaction;
              s = created.statement;
            }
            if (!s) throw Error("native statement");
            expect(typeof s.prepareNativeStep, "native write funding capability").toBe("function");
            expect(core.harness.path.shared.readers.count - s.nativeReaderCount).toBe(
              order === "self" ? 0 : 1,
            );
            const before = await s.prepareNativeStep(0, stop.signal, { includesProgress: true });
            const wait = Reflect.get(NativeWriteSteps.prototype, "wait");
            waitSpy = vi.spyOn(NativeWriteSteps.prototype, "wait").mockImplementation(function (
              this: NativeWriteSteps,
              ...args
            ) {
              if (order === "self")
                throw Error("native funding must not wait on its own locator pin");
              observed.waiting = true;
              return wait.apply(this, args);
            });
            if (order === "self") {
              expect(borrowed).toBe(0);
              const rootBytes = before.capacityBytes - before.protectedWriteBytes + 512;
              before.release();
              let attempts = 0;
              const reclaim = Reflect.get(WriteCredits.prototype, "reclaimNativeStep");
              reclaimSpy = vi
                .spyOn(WriteCredits.prototype, "reclaimNativeStep")
                .mockImplementation(function (this: WriteCredits, ...args) {
                  expect(
                    ++attempts,
                    "intrinsic native refusal has bounded funding work",
                  ).toBeLessThan(64);
                  return reclaim.apply(this, args);
                });
              await expect(
                s.prepareNativeStep(rootBytes, stop.signal, { includesProgress: true }),
              ).rejects.toMatchObject({ code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" });
              reach.reach("own locator pin is not foreign pressure");
              reach.reach("unfunded headroom refusal has bounded work");
            } else {
              expect(borrowed).toBeGreaterThan(0);
              const allocated = observed.rows?.allocated ?? 0,
                drop =
                  Math.max(observed.rowMinimum, allocated) -
                  Math.max(observed.rowMinimum, allocated - borrowed);
              expect(drop).toBeGreaterThan(0);
              const rootBytes =
                before.capacityBytes - before.protectedWriteBytes + Math.floor(drop / 2);
              before.release();
              let settled = false;
              const pending = s
                .prepareNativeStep(rootBytes, stop.signal, { includesProgress: true })
                .then(
                  (value) => ({ value, error: null }),
                  (error: unknown) => ({ value: null, error }),
                )
                .then((value) => {
                  settled = true;
                  return value;
                });
              done = pending;
              for (let turn = 0; turn < 32 && !observed.waiting; turn++) await yieldTask();
              expect(observed.waiting, "native pressure wait was reached").toBe(true);
              expect(settled).toBe(false);
              reach.reach("foreign native chunk waits");
              if (cancel) {
                stop.abort(reason);
                await yieldTask();
                await yieldTask();
                expect(settled, "cancel settles without the foreign borrower retiring").toBe(true);
                expect(core.harness.path.stats().borrowedChunkBytes).toBeGreaterThan(0);
              } else {
                recipe?.close();
                recipe = null;
              }
              const result = await pending;
              if (cancel) {
                expect(result.error).toBe(reason);
                reach.reach("cancel with borrower still live");
              } else {
                expect(result.error).toBeNull();
                const boundary = result.value;
                if (!boundary) throw Error("boundary");
                expect(boundary.protectedWriteBytes + rootBytes).toBeLessThanOrEqual(
                  governor.capBytes,
                );
                boundary.check();
                boundary.release();
                const root = await governor.admitGroup({
                  label: "after native borrow",
                  includesProgress: true,
                  grants: [{ label: "root", minimumBytes: rootBytes, spillable: false }],
                  signal: stop.signal,
                });
                try {
                  expect(root[0]?.allocate(rootBytes)).toBe(true);
                  const value = new Uint8Array(rootBytes);
                  value[0] = 11;
                  expect(value[0]).toBe(11);
                  root[0]?.free(rootBytes);
                } finally {
                  for (const g of root) g.close();
                }
                expect(snap?.released).toBe(false);
                reach.reach("borrow release sufficient before snapshot close");
              }
              reach.reach(
                order === "older"
                  ? "foreign query pin before owner"
                  : "foreign query pin after owner",
              );
            }
            reach.reach(cap === 32 ? "32 MiB" : "64 MiB");
          } finally {
            reclaimSpy?.mockRestore();
            stop.abort(reason);
            resume.resolve();
            recipe?.close();
            reader?.close();
            snap?.release();
            await done;
            waitSpy?.mockRestore();
            await s?.closeNativeCommands();
            tx?.rollback();
            await core.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("all ownership retired");
        },
      ),
      {
        seed,
        numRuns: 14,
        examples: [
          [32, false, "older"],
          [64, true, "older"],
          [32, true, "older"],
          [64, false, "older"],
          [32, false, "newer"],
          [64, true, "newer"],
          [32, true, "newer"],
          [64, false, "newer"],
          [32, false, "self"],
          [64, true, "self"],
        ],
      },
    );
    reach.assertReached();
  }, 180000);
