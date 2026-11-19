/** A1 source facts follow pinned immutable generations, not the latest global state. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, GROUP_AGGREGATE_NATIVE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { readGroupAggregate, groupAggregateCoefficient } from "../page/group-aggregate.js";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { SimLocks } from "../storage/sim-disk.js";
import { StorageCore } from "./storage-core.js";
(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

// Temporary expected failure assigned to B5b: handoff/B5b-bigint-delete.md.
// Only the exact DELETE failure is expected; any other error makes it.fails fail.
const PENDING_DELETE = "B5b: persisted wide BIGINT DELETE";
function pendingDelete(error: unknown): boolean {
  for (let depth = 0; depth < 16 && error instanceof Error; depth++, error = error.cause)
    if (error.message === PENDING_DELETE) return true;
  return false;
}
for (const seed of seedsFor("next-group-aggregate-native", [0xb2e7, 0xb2e8]))
  it.fails(
    `keeps old pinned exact facts and declines changed visibility seed ${String(seed)}`,
    async () => {
      try {
        const reach = createReach("A1 native generations", GROUP_AGGREGATE_NATIVE_REACH);
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
                      ? createOpfsBackend({ name: "a1-native", directory: native.root, locks })
                      : createIndexedDbBackend({
                          name: "a1-native",
                          locks,
                          database: () => openIndexedDb(factory, "a1-native"),
                        }),
                });
                Object.assign(core.harness.tuning, {
                  merges: { mode: "manual", fanIn: 2, sliceEntries: 3 },
                });
                await core.open();
                try {
                  const client = core;
                  await client.execute("CREATE TABLE t (k INT PRIMARY KEY, n BIGINT)");
                  const values = Array.from(
                    { length: rows },
                    (_, row) => BigInt(row - salt) * 10000000000000000n,
                  );
                  for (let from = 0; from < rows; from += 8) {
                    const part = values.slice(from, from + 8);
                    await client.execute(
                      `INSERT INTO t VALUES ${part.map((v, row) => `(${String(from + row)},${v.toString()})`).join(",")}`,
                    );
                    await core.harness.path.quiesce();
                  }
                  await core.harness.path.recycleLog();
                  await core.harness.path.quiesce();
                  const grant = await core.harness.governor.admit({
                    label: "A1 native metadata",
                    minimumBytes: 4 << 20,
                    spillable: false,
                  });
                  const old = core.harness.path.pin();
                  try {
                    const table = old.table(1, { budget: grant }),
                      segment = table.segments[0];
                    expect(table.segments.length).toBeGreaterThan(1);
                    expect(segment?.kind).toBe("group");
                    if (segment?.kind !== "group") throw new Error("A1 source was not persisted");
                    await segment.root();
                    const column = segment.chunk(2);
                    if (column === null) throw new Error("Missing native coefficient column");
                    expect(column.aggregateSummary).not.toBeNull();
                    reach.reach("real stored source");
                    const read = async () => {
                      const held = await readGroupAggregate(old.pages, segment.group, column, {
                        budget: grant,
                      });
                      if (held?.value.sum === null || held === null)
                        throw new Error("Missing native exact record");
                      try {
                        expect(held.value.nonNullCount).toBe(segment.group.rowCount);
                        expect(groupAggregateCoefficient(held.value.sum.coefficientBytes)).toBe(
                          values
                            .slice(
                              segment.group.firstRowId - 1,
                              segment.group.firstRowId - 1 + segment.group.rowCount,
                            )
                            .reduce((sum, value) => sum + value, 0n),
                        );
                      } finally {
                        held.release();
                      }
                    };
                    await read();
                    reach.reach("pristine record");
                    const merges = core.harness.path.maintenance.merges;
                    await core.harness.path.runMaintenance();
                    expect(core.harness.path.maintenance.merges).toBeGreaterThan(merges);
                    reach.reach("index runs merged");
                    const merged = core.harness.path.pin();
                    try {
                      const after = merged.table(1, { budget: grant });
                      const same = after.segments.find(
                        (next) =>
                          next.kind === "group" && next.group.groupId === segment.group.groupId,
                      );
                      if (same?.kind !== "group") throw new Error("Merge lost source group");
                      expect(same.group.root).toEqual(segment.group.root);
                      await same.root();
                      expect(same.chunk(2)?.aggregateSummary).toEqual(column.aggregateSummary);
                    } finally {
                      merged.release();
                    }
                    await read();
                    reach.reach("summary refs unchanged");
                    try {
                      await client.execute("DELETE FROM t WHERE k=0");
                    } catch (error) {
                      if (
                        error instanceof RangeError &&
                        error.message ===
                          "A page with values beyond ±(2^53 − 1) needs an int64 target"
                      )
                        throw new Error(PENDING_DELETE, { cause: error });
                      throw error;
                    }
                    await core.harness.path.recycleLog();
                    await core.harness.path.quiesce();
                    await read();
                    reach.reach("old pin valid");
                    const fresh = core.harness.path.pin();
                    try {
                      const changed = fresh.table(1, { budget: grant });
                      let declined = 0;
                      for (const next of changed.segments) {
                        if (next.kind !== "group" || next.group.groupId !== segment.group.groupId)
                          continue;
                        await next.root();
                        const source = next.chunk(2);
                        if (source === null) throw new Error("Missing updated source");
                        expect(next.group.deletions).not.toBeNull();
                        expect(
                          await readGroupAggregate(fresh.pages, next.group, source, {
                            budget: grant,
                          }),
                        ).toBeNull();
                        declined++;
                      }
                      expect(declined).toBe(1);
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
                  const before = await client.execute("SELECT k,n FROM t ORDER BY k");
                  await core.harness.reopen();
                  const after = await client.execute("SELECT k,n FROM t ORDER BY k");
                  expect(after).toEqual(before);
                  reach.reach("publication and reopen");
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
      } catch (error) {
        if (pendingDelete(error)) throw error;
        // Resolve deliberately: it.fails then reports failure, rather than accepting an
        // unrelated A1 regression as the known parent issue. Keep its diagnostic too.
        console.error("Unexpected A1 native failure (not the B5b DELETE issue)", error);
      }
    },
    120_000,
  );
