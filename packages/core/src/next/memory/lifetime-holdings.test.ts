/**
 * The write engine's lifetime holdings (GOVERNOR-WAIT-POLICY, rule 4, reviews 8 and 9). Generated
 * write histories (indexed and unindexed tables, secondary indexes, inserts, deletions of memtable
 * and published rows, spills, drops, snapshots held across them) end idle, and then:
 * - a request sized to the usable cap settles (served, or 53200 when a publication it caused
 *   grew the lifetime holdings): idle state that only a flush, publication or reclaim frees never
 *   leaves it waiting;
 * - a request held up by state a snapshot retains waits for the snapshot, never 53200: finite
 *   holders are not lifetime;
 * - once settled, the registration is exact: the whole usable cap is served and one byte more is
 *   refused with 53200 at once.
 * A stuck wait fails as a GovernorStallError naming every holder (2 s, or MINNOW_GOVERNOR_STALL_MS).
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, LIFETIME_HOLDINGS_REACH } from "../../testing/reach.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rowsOf,
  tableInput,
} from "../write/test-support.js";
import type { WritePathImpl } from "../write/engine.js";
import { Governor } from "./governor.js";

const PENDING = Symbol("pending");
const settleWithin = <T>(work: Promise<T>, ms: number) =>
  Promise.race([
    work,
    new Promise<typeof PENDING>((resolve) => {
      setTimeout(() => {
        resolve(PENDING);
      }, ms);
    }),
  ]);
/** A request's outcome. A 53200 is lawful only above the usable cap at the moment of refusal
 * (read synchronously in the rejection, before anything else can run); below it is a defect. */
const outcome = (governor: Governor, label: string, bytes: number, signal?: AbortSignal) =>
  governor
    .admit({ label, minimumBytes: bytes, spillable: false, ...(signal ? { signal } : {}) })
    .then(
      (grant) => {
        grant.close();
        return "served";
      },
      (error: unknown) => {
        const state = (error as { sqlState?: string }).sqlState;
        if (state !== "53200") return error;
        return bytes > governor.usableCapacity() ? "53200" : "53200 below the usable cap";
      },
    );

/** Row ids of every live row, through the primary key index (indexed tables). */
async function indexedRowIds(path: WritePathImpl, tableId: number): Promise<number[]> {
  const view = path.pin();
  try {
    const reader = view.index(tableId, path.catalog.table(tableId)?.primaryKey ?? 0);
    const cursor = reader.range({ lower: null, upper: null }, "asc");
    const ids: number[] = [];
    const out = new Float64Array(256);
    try {
      for (let n = await cursor.next(out); n > 0; n = await cursor.next(out))
        ids.push(...Array.from(out.subarray(0, n)));
    } finally {
      cursor.close();
    }
    return ids;
  } finally {
    view.release();
  }
}

for (const seed of seedsFor("next-governor-lifetime-holdings", [50901, 50902]))
  it(`registers only lifetime holdings and spills idle state (${String(seed)})`, async () => {
    const reach = createReach("governor lifetime holdings", LIFETIME_HOLDINGS_REACH);
    const previousStall = Governor.stallAfterMs;
    Governor.stallAfterMs = previousStall ?? 2_000;
    const deadline = Math.max(4_000, 2 * Governor.stallAfterMs);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.array(
            fc.record({
              indexed: fc.boolean(),
              secondary: fc.boolean(),
              rows: fc.constantFrom(1, 3, 40, 700),
              deletes: fc.constantFrom("none", "some", "all"),
              spillFirst: fc.boolean(),
              drop: fc.boolean(),
            }),
            { minLength: 1, maxLength: 2 },
          ),
          fc.constantFrom(16, 1 << 16),
          fc.boolean(),
          async (tables, memtableRows, holdSnapshot) => {
            reach.beginHistory();
            const h = new Harness("strict", "sim", memtableRows);
            Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
            const path = await h.open();
            const governor = h.governor;
            let snapshot: ReturnType<WritePathImpl["pin"]> | null = null;
            const indexedLive: number[] = [];
            try {
              for (const [k, spec] of tables.entries()) {
                let t = 0;
                await autocommit(path, async (s) => {
                  t =
                    (
                      await s.changeCatalog({
                        kind: "createTable",
                        table: tableInput(
                          `t${String(k)}`,
                          [column("id", INT, false), column("v", INT)],
                          spec.indexed ? {} : { primaryKey: null },
                        ),
                      })
                    ).id ?? 0;
                });
                if (spec.secondary) {
                  const v = path.catalog.table(t)?.columns[1]?.id ?? 0;
                  await autocommit(path, (s) =>
                    s.changeCatalog({
                      kind: "createIndex",
                      index: {
                        tableId: t,
                        name: `v${String(k)}`,
                        columnIds: [v],
                        unique: false,
                        role: "secondary",
                      },
                    }),
                  );
                  reach.reach("secondary index");
                }
                const ids = Array.from({ length: spec.rows }, (_, i) => i);
                await autocommit(path, (s) =>
                  s.table(t).insert(batch(ints(ids), ints(ids.map((i) => i % 7))), null),
                );
                await path.quiesce();
                if (spec.spillFirst) {
                  // An idle-spill round first: the deletions below then hit published rows.
                  expect(["served", "53200"]).toContain(
                    await settleWithin(
                      outcome(governor, "spill first", governor.usableCapacity()),
                      deadline,
                    ),
                  );
                  await path.quiesce();
                }
                if (spec.deletes !== "none") {
                  const all = spec.indexed ? await indexedRowIds(path, t) : ids;
                  const doomed = spec.deletes === "all" ? all : all.filter((_, i) => i % 3 === 0);
                  const counted = path.pin();
                  const before = (await rowsOf(counted, t)).length;
                  counted.release();
                  await autocommit(path, (s) =>
                    s.table(t).delete(batch(locators(doomed, t)), null),
                  );
                  const view = path.pin();
                  expect((await rowsOf(view, t)).length).toBe(before - doomed.length);
                  view.release();
                  reach.reach(
                    spec.spillFirst ? "deletion of published rows" : "deletion of memtable rows",
                  );
                  if (!spec.indexed && spec.spillFirst)
                    reach.reach("unindexed published deletions");
                  if (spec.deletes === "all") reach.reach("deletion-only memtable");
                }
                if (spec.drop) {
                  if (holdSnapshot && snapshot === null) {
                    snapshot = path.pin();
                    // Open the table's index runs through the snapshot: their readers stay
                    // resident for it after the drop.
                    if (spec.indexed)
                      await snapshot
                        .index(t, path.catalog.table(t)?.primaryKey ?? 0)
                        .collect({ lower: null, upper: null });
                  }
                  await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
                  reach.reach(
                    snapshot !== null ? "table dropped under a snapshot" : "table dropped",
                  );
                } else if (spec.indexed) indexedLive.push(t);
              }
              await path.quiesce();
              if (snapshot !== null) {
                // State only the snapshot retains is finite: a request it blocks waits for it.
                const stop = new AbortController();
                const held = outcome(
                  governor,
                  "behind a snapshot",
                  governor.usableCapacity(),
                  stop.signal,
                );
                const early = await settleWithin(held, 200);
                expect(early, "refused below the cap while a snapshot held finite state").not.toBe(
                  "53200 below the usable cap",
                );
                snapshot.release();
                snapshot = null;
                const settled = early === PENDING ? await settleWithin(held, deadline) : early;
                stop.abort();
                expect(settled, "the snapshot's release did not serve the request").not.toBe(
                  PENDING,
                );
                expect(settled).not.toBe("53200 below the usable cap");
                if (early === PENDING) reach.reach("waited for a snapshot, then settled");
                await path.quiesce();
              }
              const first = await settleWithin(
                outcome(governor, "idle usable cap", governor.usableCapacity()),
                deadline,
              );
              expect(
                first,
                "waited on idle state no flush, publication or reclaim was freeing",
              ).not.toBe(PENDING);
              expect(first).not.toBe("53200 below the usable cap");
              await path.quiesce();
              // Read every surviving indexed table through its primary key: the current state's
              // run readers open and stay resident after the snapshot closes, and the
              // registration must count them.
              for (const t of indexedLive) {
                const view = path.pin();
                try {
                  // A collect opens the runs' resident readers (a range cursor streams past them).
                  await view
                    .index(t, path.catalog.table(t)?.primaryKey ?? 0)
                    .collect({ lower: null, upper: null });
                } finally {
                  view.release();
                }
                if ((path.state().tables.get(t)?.runs.size ?? 0) > 0)
                  reach.reach("current run readers open");
              }
              const usable = governor.usableCapacity();
              expect(await settleWithin(outcome(governor, "above", usable + 1), 500)).toBe("53200");
              // Exact both ways: served (nothing unregistered blocks it), and at the grant every other
              // holding is exactly the registered lifetime holdings (nothing registered is
              // unheld, which would refuse requests below the true cap).
              let slack: number | null = null;
              const whole = governor
                .admit({ label: "whole usable cap", minimumBytes: usable, spillable: false })
                .then((grant) => {
                  slack = governor.capBytes - governor.reserveBytes - governor.usedBytes;
                  grant.close();
                  return "served";
                });
              expect(
                await settleWithin(whole, deadline),
                "the usable cap waited behind unregistered holdings",
              ).toBe("served");
              expect(slack, "registered lifetime holdings the writer does not hold").toBe(0);
              reach.reach("exact after publication");
            } finally {
              snapshot?.release();
              await h.close();
            }
            expect(governor.usedBytes).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            // Review 9: deletion-only memtable over published, indexed rows.
            [
              [
                {
                  indexed: true,
                  secondary: false,
                  rows: 3,
                  deletes: "all",
                  spillFirst: true,
                  drop: false,
                },
              ],
              1 << 16,
              false,
            ],
            // Review 9: retired run readers kept only by a snapshot.
            [
              [
                {
                  indexed: true,
                  secondary: false,
                  rows: 40,
                  deletes: "none",
                  spillFirst: false,
                  drop: true,
                },
              ],
              16,
              true,
            ],
            // Unindexed published deletions only (no memtable content).
            [
              [
                {
                  indexed: false,
                  secondary: false,
                  rows: 40,
                  deletes: "some",
                  spillFirst: true,
                  drop: false,
                },
              ],
              1 << 16,
              false,
            ],
          ],
        },
      );
      reach.assertReached();
    } finally {
      Governor.stallAfterMs = previousStall;
    }
  }, 300_000);
