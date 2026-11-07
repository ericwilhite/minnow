/** PROPOSAL "One memory governor": temporary reader pressure waits, never size-refuses. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_METADATA_PRESSURE_STATES } from "../../testing/reach.js";
import { MetadataBudget } from "./budget.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  rules,
  tableInput,
} from "./test-support.js";

const MiB = 1024 * 1024;
const reach = createReach("metadata under live reader pressure", WRITE_METADATA_PRESSURE_STATES);
for (const backend of ["memory", "indexeddb"] as const)
  for (const seed of seedsFor("next-write-metadata-pressure", [0xb3a1]))
    it(`waits for live reader memory through publication over ${backend} (seed ${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            capMiB: fc.constantFrom(32, 48, 64),
            spare: fc.constantFrom(1, 256, 1024),
            delay: fc.integer({ min: 2, max: 12 }),
            cycles: fc.integer({ min: 3, max: 5 }),
            stage: fc.boolean(),
          }),
          async (s) => {
            reach.beginHistory();
            reach.reach(backend);
            const h = new Harness("strict", "memory", 8, undefined, {
              merges: { fanIn: 2, stallRuns: 4, sliceEntries: 32 },
            });
            const governor = new Governor(s.capMiB * MiB);
            Object.assign(h, { governor });
            if (backend === "indexeddb")
              Object.assign(h, {
                backend: createIndexedDbBackend({
                  database: () => simIndexedDb(h.disk),
                  name: "metadata-pressure",
                  locks: new SimLocks(),
                  memory: governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
                }),
              });
            const grants: Grant[] = [];
            let reader: Grant | null = null,
              buffer: Uint8Array | null = null,
              timer: ReturnType<typeof setTimeout> | null = null;
            let waits = 0,
              failureWhileHeld: unknown = null;
            const clearTimer = () => {
              if (timer !== null) clearTimeout(timer);
            };
            const release = () => {
              if (!reader) return;
              expect(buffer?.[0]).toBe(7);
              expect(buffer?.at(-1)).toBe(7);
              reader.close();
              reader = null;
              buffer = null;
              reach.reach("reader released");
            };
            let path: Awaited<ReturnType<Harness["open"]>> | null = null;
            const schedule = () => {
              if (!reader || timer !== null) return;
              waits++;
              reach.reach("metadata waits");
              timer = setTimeout(() => {
                failureWhileHeld = path?.failure;
                release();
                timer = null;
              }, s.delay);
            };
            const admit = governor.admit.bind(governor);
            governor.admit = async (options) => {
              if (reader && options.label.startsWith("merge")) schedule();
              const g = await admit(options);
              grants.push(g);
              if (options.label === "write path metadata") {
                const grow = g.tryGrow.bind(g);
                g.tryGrow = (bytes) => {
                  const ok = grow(bytes);
                  if (!ok && reader) schedule();
                  return ok;
                };
              }
              return g;
            };
            path = await h.open();
            let table = 0,
              next = 0;
            const expected: number[][] = [];
            try {
              await autocommit(path, async (st) => {
                table =
                  (
                    await st.changeCatalog({
                      kind: "createTable",
                      table: tableInput("t", [column("id", INT, false)]),
                    })
                  ).id ?? 0;
              });
              for (let cycle = 0; cycle < s.cycles; cycle++) {
                await path.quiesce();
                for (const g of grants)
                  if (
                    g.label !== "write path" &&
                    g.label !== "write commit buffers" &&
                    g.held > g.allocated + g.loaned
                  )
                    g.release(g.held - g.allocated - g.loaned);
                reader = await governor.admit({
                  label: "concurrent reader",
                  minimumBytes: 0,
                  spillable: true,
                });
                const bytes =
                  governor.capBytes - governor.reserveBytes - governor.usedBytes - s.spare;
                expect(reader.allocate(bytes)).toBe(true);
                buffer = new Uint8Array(bytes).fill(7);
                reach.reach("reader owns real bytes");
                const before = path.maintenance.stagedRows;
                const tx = await path.begin(),
                  st = tx.statement({ rules });
                const calls = s.stage && cycle > 0 ? 3 : 1;
                for (let part = 0; part < calls; part++) {
                  const ids = Array.from({ length: calls === 1 ? 16 : 8 }, () => next++);
                  await st.table(table).insert(batch(ints(ids)), null);
                  expected.push(...ids.map((id) => [id]));
                }
                await st.end();
                await (
                  await tx.commit(null)
                ).durable;
                // The reviewer's second commit forces the first commit's frozen prefix to flush.
                const id = next++;
                await autocommit(path, (st) => st.table(table).insert(batch(ints([id])), null));
                expected.push([id]);
                await path.quiesce();
                expect(failureWhileHeld).toBeNull();
                expect(path.failure).toBeNull();
                expect(path.maintenance.mergeFailures).toBe(0);
                expect(reader).toBeNull();
                expect(path.maintenance.flushes).toBeGreaterThan(0);
                reach.reach("flush published");
                if (path.maintenance.stagedRows > before) reach.reach("staging published");
                expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              }
              expect(waits).toBeGreaterThan(0);
              expect(path.maintenance.merges).toBeGreaterThan(0);
              reach.reach("merge completed");
              path = await h.reopen();
              const view = path.pin();
              try {
                expect(await rowsOf(view, table)).toEqual(expected);
              } finally {
                view.release();
              }
              reach.reach("durable reopen");
              await h.space.audit();
            } finally {
              clearTimer();
              release();
              await h.close();
            }
            expect(governor.usedBytes).toBe(0);
          },
        ),
        { seed, numRuns: 12 },
      );
    }, 120_000);
it("rejects only an individual metadata allocation beyond the entire cap with a typed error", async () => {
  const g = new Governor(32 * MiB),
    grant = await g.admit({ label: "metadata", minimumBytes: 0, spillable: true });
  const budget = new MetadataBudget(grant, () => 0, g.capBytes);
  try {
    expect(() => budget.allocate(g.capBytes + 1)).toThrow(
      expect.objectContaining({
        code: "MINNOW_METADATA_ALLOCATION_TOO_LARGE",
        bytes: g.capBytes + 1,
        cap: g.capBytes,
      }),
    );
    expect(g.usedBytes).toBe(0);
  } finally {
    grant.close();
  }
});
it("keeps fitting metadata synchronous and cancels a wait without retaining a loan", async () => {
  const g = new Governor(32 * MiB),
    grant = await g.admit({ label: "metadata", minimumBytes: 0, spillable: true }),
    holder = await g.admit({ label: "reader", minimumBytes: 0, spillable: true }),
    budget = new MetadataBudget(grant, () => 0, g.capBytes),
    controller = new AbortController(),
    stopped = new Error("metadata cancelled");
  try {
    expect(
      budget.retry(
        () => budget.allocate(1),
        () => undefined,
      ),
    ).toBe(true);
    budget.free(1);
    grant.release(grant.held);
    expect(holder.allocate(g.capBytes - g.reserveBytes)).toBe(true);
    const pending = budget.retry(
      () => budget.allocate(1),
      () => controller.signal.throwIfAborted(),
    );
    expect(pending).toBeInstanceOf(Promise);
    controller.abort(stopped);
    await expect(pending).rejects.toBe(stopped);
    expect(grant.allocated).toBe(0);
    expect(grant.loaned).toBe(0);
  } finally {
    holder.close();
    grant.close();
  }
  expect(g.usedBytes).toBe(0);
});
it("reaches metadata pressure states", () => reach.assertReached());
