/** Independent INT lifetime coverage. The wide BIGINT history is kept, unchanged,
 * as its separately pinned it.fails until B5b fixes handoff/B5b-bigint-delete.md.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { createReach, GROUP_AGGREGATE_NATIVE_INT_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { readGroupAggregate, groupAggregateCoefficient } from "../page/group-aggregate.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { SimLocks } from "../storage/sim-disk.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;
for (const seed of seedsFor("next-group-aggregate-native-int", [0xb2f6, 0xb2f7]))
  it(`keeps INT facts through deletes, old pins and reopen seed ${String(seed)}`, async () => {
    const reach = createReach("A1 native INT generations", GROUP_AGGREGATE_NATIVE_INT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 33, max: 80 }),
        fc.integer({ min: 1, max: 99 }),
        async (rows, salt) => {
          reach.beginHistory();
          for (const kind of ["OPFS", "IndexedDB"] as const) {
            const core = new StorageCore(8),
              native = new MemoryOpfs(),
              factory = new IDBFactory(),
              locks = new SimLocks();
            Object.assign(core.harness, {
              backend:
                kind === "OPFS"
                  ? createOpfsBackend({ name: "a1-native-int", directory: native.root, locks })
                  : createIndexedDbBackend({
                      name: "a1-native-int",
                      locks,
                      database: () => openIndexedDb(factory, "a1-native-int"),
                    }),
            });
            Object.assign(core.harness.tuning, {
              merges: { mode: "manual", fanIn: 2, sliceEntries: 3 },
            });
            await core.open();
            try {
              await core.execute("CREATE TABLE t(k INT PRIMARY KEY,n INT)");
              const values = Array.from({ length: rows }, (_, row) => row * 10007 - salt * 40007);
              for (let from = 0; from < rows; from += 8) {
                await core.execute(
                  `INSERT INTO t VALUES ${values
                    .slice(from, from + 8)
                    .map((n, k) => `(${String(from + k)},${String(n)})`)
                    .join(",")}`,
                );
                await core.harness.path.quiesce();
              }
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              const grant = await core.harness.governor.admit({
                label: "A1 native INT metadata",
                minimumBytes: 4 << 20,
                spillable: false,
              });
              const old = core.harness.path.pin();
              try {
                const table = old.table(1, { budget: grant }),
                  segment = table.segments[0];
                expect(table.segments.length).toBeGreaterThan(1);
                if (segment?.kind !== "group") throw new Error("Source was not persisted");
                await segment.root();
                const column = segment.chunk(2);
                if (column?.aggregateSummary === null || column === null)
                  throw new Error("Missing native INT record");
                const expected = values
                  .slice(
                    segment.group.firstRowId - 1,
                    segment.group.firstRowId - 1 + segment.group.rowCount,
                  )
                  .reduce((sum, n) => sum + BigInt(n), 0n);
                const checkOld = async () => {
                  expect(segment.group.deletions).toBeNull();
                  const held = await readGroupAggregate(old.pages, segment.group, column, {
                    budget: grant,
                  });
                  if (held === null) throw new Error("Lost old pinned record");
                  try {
                    const sum = held.value.sum;
                    if (sum === null) throw new Error("Lost old pinned SUM");
                    expect(held.value.nonNullCount).toBe(segment.group.rowCount);
                    expect(groupAggregateCoefficient(sum.coefficientBytes)).toBe(expected);
                  } finally {
                    held.release();
                  }
                };
                await checkOld();
                reach.reach("real stored source");
                reach.reach("pristine record");
                const merges = core.harness.path.maintenance.merges;
                await core.harness.path.runMaintenance();
                expect(core.harness.path.maintenance.merges).toBeGreaterThan(merges);
                reach.reach("index runs merged");
                await core.execute("DELETE FROM t WHERE k=0");
                await core.harness.path.recycleLog();
                await core.harness.path.quiesce();
                await checkOld();
                reach.reach("old pin valid");
                const fresh = core.harness.path.pin();
                try {
                  const current = fresh
                    .table(1, { budget: grant })
                    .segments.find(
                      (s) => s.kind === "group" && s.group.groupId === segment.group.groupId,
                    );
                  if (current?.kind !== "group") throw new Error("Lost changed group");
                  await current.root();
                  const changed = current.chunk(2);
                  if (changed === null) throw new Error("Lost changed column");
                  expect(changed.aggregateSummary).toEqual(column.aggregateSummary);
                  reach.reach("summary refs unchanged");
                  expect(current.group.deletions).not.toBeNull();
                  const stale = await readGroupAggregate(fresh.pages, current.group, changed, {
                    budget: grant,
                  });
                  try {
                    expect(stale, "changed visibility must decline its summary").toBeNull();
                  } finally {
                    stale?.release();
                  }
                  reach.reach("delete fallback");
                } finally {
                  fresh.release();
                }
              } finally {
                old.release();
                expect(grant.allocated).toBe(0);
                grant.close();
              }
              await core.harness.space.audit();
              reach.reach("R25 inventory");
              const before = await core.execute("SELECT k,n FROM t ORDER BY k");
              if (!("batches" in before)) throw new Error("Missing source rows");
              const expectedRows = values.slice(1).map((n, row) => ({ k: row + 1, n }));
              expect(oracleRows(before)).toEqual(expectedRows);
              await core.harness.reopen();
              expect(await core.execute("SELECT k,n FROM t ORDER BY k")).toEqual(before);
              reach.reach("publication and reopen");
              await core.execute("INSERT INTO t VALUES (1000,42)");
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              const continued = await core.execute("SELECT k,n FROM t ORDER BY k");
              if (!("batches" in continued)) throw new Error("Missing continued-write rows");
              expect(oracleRows(continued)).toEqual([...expectedRows, { k: 1000, n: 42 }]);
              reach.reach("continued writes");
              reach.reach(kind);
            } finally {
              await core.close();
            }
            expect(core.harness.governor.usedBytes).toBe(0);
            reach.reach("zero charges");
          }
        },
      ),
      { seed, numRuns: 4, examples: [[33, 1]] },
    );
    reach.assertReached();
  }, 120_000);
