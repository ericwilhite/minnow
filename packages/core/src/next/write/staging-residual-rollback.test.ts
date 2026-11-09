/**
 * access/key-index.ts resolution and PostgreSQL ROLLBACK: rolling back new rows cannot undo a
 * committed DELETE, nor an earlier statement kept by a savepoint. Staging owns its rows/puts,
 * not every entry of the rowless memtable it borrowed (memtable.ts "Staged").
 */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { UniqueViolationError } from "../access/modify.js";
import { MAX_LSN, type Snapshot } from "../access/snapshot.js";
import { taskClock } from "../exec/pipeline.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { createReach, WRITE_STAGED_RESIDUAL_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { LogAppender } from "./log.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  locators,
  rowsOf,
  rules,
  tableInput,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };
const reach = createReach(
  "staging preserves unrelated index entries",
  WRITE_STAGED_RESIDUAL_STATES,
);

async function probe(view: Snapshot, table: number, index: number, ids: readonly number[]) {
  const keys = new KeyBatch(FREE);
  try {
    encodeKeyBatch(
      "equality",
      [{ vector: ints(ids), direction: "asc", nulls: "first" }],
      { length: ids.length, selection: null },
      keys,
    );
    const out = {
      probes: new Int32Array(ids.length + 1),
      locators: new Float64Array(ids.length + 1),
      count: 0,
    };
    await view.index(table, index).probe(keys, out);
    return out;
  } finally {
    keys.close();
  }
}

for (const backend of ["memory", "sim", "indexeddb"] as const)
  for (const seed of seedsFor("next-write-staged-residual-rollback", [702, 704]))
    it(`preserves residual entries across staged rollback (${backend}, seed ${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            capacity: fc.constantFrom(4, 6, 8),
            base: fc.integer({ min: 10, max: 1000 }),
            calls: fc.integer({ min: 3, max: 6 }),
            rollback: fc.constantFrom("transaction", "statement", "savepoint"),
            secondary: fc.constantFrom("none", "unique", "nonunique"),
            earlierDelete: fc.boolean(),
            snapshotBeforeDelete: fc.boolean(),
            holdFlush: fc.boolean(),
            merge: fc.boolean(),
            clockEvery: fc.constantFrom(1, 4, 7),
            pauseBeforeCommit: fc.boolean(),
          }),
          async (plan) => {
            reach.beginHistory();
            reach.reach(backend);
            reach.reach(plan.rollback);
            reach.reach(plan.secondary);
            reach.reach(plan.earlierDelete ? "earlier pending delete" : "committed delete only");
            reach.reach(
              plan.snapshotBeforeDelete ? "snapshot before delete" : "snapshot after delete",
            );
            reach.reach(plan.holdFlush ? "held residual flush" : "unheld residual flush");
            reach.reach(plan.merge ? "merges" : "no merges");
            let checks = 0;
            const clock = vi
              .spyOn(taskClock, "due")
              .mockImplementation(() => ++checks % plan.clockEvery === 0);
            // Called below with the actual appender as `this`, never as a detached function.
            // eslint-disable-next-line @typescript-eslint/unbound-method
            const durable = LogAppender.prototype.durable;
            const durableCheck = vi
              .spyOn(LogAppender.prototype, "durable")
              .mockImplementation(function (this: LogAppender, lsn) {
                // A logical guard, not a timeout: a pending sequence is never a durable LSN.
                expect(lsn, "a flush waits only for committed LSNs").toBeLessThanOrEqual(MAX_LSN);
                return durable.call(this, lsn);
              });
            let hold = false;
            let entered!: () => void, release!: () => void;
            const atFlush = new Promise<void>((resolve) => {
              entered = resolve;
            });
            const gate = new Promise<void>((resolve) => {
              release = resolve;
            });
            const h = new Harness(
              "strict",
              backend === "sim" ? "sim" : "memory",
              plan.capacity,
              undefined,
              {
                merges: { mode: plan.merge ? "auto" : "off", fanIn: 2 },
                beforeFlushPublish: async () => {
                  if (hold) {
                    entered();
                    await gate;
                  }
                },
              },
            );
            if (backend === "indexeddb")
              Object.assign(h, {
                backend: createIndexedDbBackend({
                  database: () => simIndexedDb(h.disk),
                  name: "staged-residual",
                  locks: new SimLocks(),
                  memory: h.governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
                }),
              });
            let p = await h.open(),
              table = 0,
              secondary = 0;
            let retained: Snapshot | null = null;
            const original = Array.from({ length: plan.capacity }, (_, k) => plan.base + k);
            const deleted = original[1] ?? 0,
              earlier = original[2] ?? 0;
            const values = (ids: readonly number[]) =>
              batch(ints(ids), ints(ids.map((id) => id * 3)));
            try {
              await autocommit(p, async (statement) => {
                table =
                  (
                    await statement.changeCatalog({
                      kind: "createTable",
                      table: tableInput("t", [column("id", INT, false), column("v", INT, false)]),
                    })
                  ).id ?? 0;
                if (plan.secondary !== "none")
                  secondary =
                    (
                      await statement.changeCatalog({
                        kind: "createIndex",
                        index: {
                          tableId: table,
                          name: "by_v",
                          columnIds: [statement.catalog.table(table)?.columns[1]?.id ?? 0],
                          unique: plan.secondary === "unique",
                          role: "secondary",
                        },
                      })
                    ).id ?? 0;
              });
              await autocommit(p, (statement) =>
                statement.table(table).insert(values(original), null),
              );
              // Freeze the full committed input without leaving a live row behind. The next
              // DELETE creates a rowless memtable whose committed tombstone staging must keep.
              const prime = await p.begin(),
                priming = prime.statement({ rules });
              await priming.table(table).insert(values([plan.base + 10_000]), null);
              await priming.end();
              prime.rollback();
              await p.quiesce();
              expect(p.state().tables.get(table)?.memtables.length).toBe(0);
              if (plan.snapshotBeforeDelete) retained = p.pin();
              await autocommit(p, async (statement) => {
                const out = await probe(
                  statement.view,
                  table,
                  statement.catalog.table(table)?.primaryKey ?? 0,
                  [deleted],
                );
                expect(out.count).toBe(1);
                await statement
                  .table(table)
                  .delete(batch(locators([out.locators[0] ?? 0], table)), null);
              });
              retained ??= p.pin();
              const tx = await p.begin();
              if (plan.earlierDelete) {
                const statement = tx.statement({ rules });
                const out = await probe(
                  statement.view,
                  table,
                  statement.catalog.table(table)?.primaryKey ?? 0,
                  [earlier],
                );
                expect(out.count).toBe(1);
                await statement
                  .table(table)
                  .delete(batch(locators([out.locators[0] ?? 0], table)), null);
                await statement.end();
              }
              const savepoint = tx.savepoint();
              const statement = tx.statement({ rules });
              const stagedBefore = p.maintenance.stagedResidualEntries;
              const pending: number[] = [];
              for (let call = 0; call < plan.calls; call++) {
                const fresh = Array.from(
                  { length: plan.capacity },
                  (_, k) => plan.base + 20_000 + call * plan.capacity + k,
                );
                pending.push(...fresh);
                await statement.table(table).insert(values(fresh), null);
              }
              expect(p.maintenance.stagedResidualEntries).toBeGreaterThan(stagedBefore);
              reach.reach("residual staged");
              hold = plan.holdFlush;
              if (plan.rollback === "transaction") tx.rollback();
              else {
                if (plan.rollback === "statement") statement.rollback();
                else {
                  await statement.end();
                  tx.rollbackTo(savepoint);
                }
                if (plan.pauseBeforeCommit && plan.earlierDelete && !plan.holdFlush) {
                  await p.quiesce();
                  reach.reach("pending residual waits for commit");
                }
                await (
                  await tx.commit(null)
                ).durable;
              }
              const expected = original.filter(
                (id) =>
                  id !== deleted &&
                  !(plan.earlierDelete && plan.rollback !== "transaction" && id === earlier),
              );
              const check = async (view: Snapshot, ids: readonly number[]) => {
                expect(
                  (await rowsOf(view, table))
                    .map((row) => row[0])
                    .sort((a, b) => Number(a) - Number(b)),
                ).toEqual(ids);
                const primary = view.catalog.table(table)?.primaryKey ?? 0;
                expect((await probe(view, table, primary, pending)).count).toBe(0);
                const out = await probe(view, table, primary, original);
                expect(Array.from(out.probes.subarray(0, out.count), (k) => original[k])).toEqual(
                  ids,
                );
                const cursor = view
                  .index(table, primary)
                  .range({ lower: null, upper: null }, "asc");
                try {
                  const buffer = new Float64Array(plan.capacity + 1);
                  const found: number[] = [];
                  for (;;) {
                    const count = await cursor.next(buffer);
                    if (!count) break;
                    found.push(...buffer.subarray(0, count));
                  }
                  expect(found.sort((a, b) => a - b)).toEqual(
                    Array.from(out.locators.subarray(0, out.count)).sort((a, b) => a - b),
                  );
                } finally {
                  cursor.close();
                }
                if (secondary) {
                  expect(
                    (
                      await probe(
                        view,
                        table,
                        secondary,
                        pending.map((id) => id * 3),
                      )
                    ).count,
                  ).toBe(0);
                  const found = await probe(
                    view,
                    table,
                    secondary,
                    original.map((id) => id * 3),
                  );
                  expect(
                    Array.from(found.probes.subarray(0, found.count), (k) => original[k]),
                  ).toEqual(ids);
                }
              };
              let view = p.pin();
              try {
                await check(view, expected);
              } finally {
                view.release();
              }
              const old = plan.snapshotBeforeDelete
                ? original
                : original.filter((id) => id !== deleted);
              await check(retained, old);
              if (plan.holdFlush) {
                await atFlush;
                reach.reach("residual flush blocked");
              }
              await autocommit(p, (st) => st.table(table).insert(values([deleted]), null));
              reach.reach("deleted key reused");
              // A real duplicate must still fail; disabling unique verification is not a fix.
              await expect(
                autocommit(p, (st) => st.table(table).insert(values([original[0] ?? 0]), null)),
              ).rejects.toBeInstanceOf(UniqueViolationError);
              release();
              hold = false;
              await p.quiesce();
              await check(retained, old);
              retained.release();
              retained = null;
              const final = [...expected, deleted].sort((a, b) => a - b);
              view = p.pin();
              try {
                await check(view, final);
              } finally {
                view.release();
              }
              p = await h.reopen();
              view = p.pin();
              try {
                await check(view, final);
              } finally {
                view.release();
              }
              reach.reach("reopened");
            } finally {
              release();
              retained?.release();
              try {
                await h.close();
              } finally {
                durableCheck.mockRestore();
                clock.mockRestore();
              }
            }
          },
        ),
        { seed, numRuns: 40 },
      );
    }, 300_000);

it("reaches the staged residual rollback dimensions", () => reach.assertReached());
