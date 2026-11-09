/**
 * PROPOSAL "One memory governor", governor rules4/5 and extents.ts "Free space":
 * freeing an owned range must complete while unused ordinary holdings fill the cap.
 * Replay review B3-p2d-5's seed598 shapes through the real write path and both stores.
 * Pressure is scoped to allocator mutation: B3's separate metadata/merge admission
 * policy is intentionally not substituted into the production write path here.
 */
import fc from "fast-check";
import { watchGrants } from "./test-support.js";
import { expect, it } from "vitest";
import { createReach, FREE_MAP_PRESSURE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor, type Grant } from "../memory/governor.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
  TEXT,
  texts,
} from "./test-support.js";

const MiB = 1024 * 1024;
const reach = createReach(
  "free-map mutation under idle ordinary holdings",
  FREE_MAP_PRESSURE_REACH,
);
const scenario = fc.record({
  capMiB: fc.constantFrom(32, 48, 64),
  rows: fc.integer({ min: 8, max: 40 }),
  columns: fc.integer({ min: 2, max: 8 }),
  valueBytes: fc.constantFrom(8, 16, 24, 32),
  cycles: fc.integer({ min: 6, max: 12 }),
  fanIn: fc.constantFrom(2, 3),
  loan: fc.boolean(),
  atMerge: fc.boolean(),
  releaseStartup: fc.boolean(),
});

/** Both the parent map and the prepaid replacement expose their native release operation. */
interface ReleaseMap {
  free?: (offset: number, bytes: number) => void;
  releaseKnownAllocated?: (offset: number, bytes: number) => void;
}
for (const backend of ["memory", "indexeddb"] as const)
  for (const seed of seedsFor("storage-free-map-pressure", [0x256]))
    it(`keeps real writes and reopen correct at free-map pressure: ${backend}/${String(seed)}`, async () => {
      await fc.assert(
        fc.asyncProperty(scenario, async (s) => {
          reach.beginHistory();
          reach.reach(backend);
          const harness = new Harness("strict", "memory", s.rows, undefined, {
            merges: { fanIn: s.fanIn, stallRuns: 2 * s.fanIn, sliceEntries: 32 },
            logRecycleBytes: 16 * MiB,
          });
          const governor = new Governor(s.capMiB * MiB);
          Object.assign(harness, { governor });
          if (backend === "indexeddb")
            Object.assign(harness, {
              backend: createIndexedDbBackend({
                database: () => simIndexedDb(harness.disk),
                name: "free-map-pressure",
                locks: new SimLocks(),
                memory: governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
              }),
            });
          const grants: Grant[] = [];
          watchGrants(governor, (grant) => grants.push(grant));
          let write: Grant | undefined;
          let pressured = 0;
          harness.onSpace = (space) => {
            const startup = grants.find((g) => g.label === "startup" && g.held > 0);
            if (s.releaseStartup && startup)
              startup.release(startup.held - startup.allocated - startup.loaned);
            reach.reach(s.releaseStartup ? "startup returned" : "startup retained");
            // This is an internal allocator seam, not a change to writer/session behaviour.
            const map = (space as unknown as { readonly map: ReleaseMap }).map;
            const key = map.releaseKnownAllocated === undefined ? "free" : "releaseKnownAllocated";
            const release = map[key]?.bind(map);
            if (release === undefined) throw new Error("Missing native free-map mutation");
            map[key] = (offset, bytes) => {
              if (!write || write.held === 0 || bytes === 0) return release(offset, bytes);
              const growth = governor.capBytes - governor.reserveBytes - governor.usedBytes - 1;
              const idle = governor.tryAdmit({
                label: "idle estimate at allocator mutation",
                minimumBytes: Math.max(0, growth),
                spillable: false,
              });
              if (idle === null) throw new Error("Fixture could not reserve ordinary headroom");
              const charged = governor.usedBytes;
              try {
                expect(governor.capBytes - governor.reserveBytes - governor.usedBytes).toBe(1);
                pressured++;
                reach.reach("one byte ordinary headroom");
                release(offset, bytes);
                // Freeing changes arena contents, never its prepaid resident capacity.
                expect(governor.usedBytes).toBe(charged);
                expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              } finally {
                idle.close();
              }
            };
          };
          let path = await harness.open();
          let scratch: Uint8Array | null = null;
          let loan: Awaited<ReturnType<Grant["borrow"]>> | null = null;
          try {
            let table = 0;
            await autocommit(path, async (statement) => {
              table =
                (
                  await statement.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [
                      column("id", INT, false),
                      ...Array.from({ length: s.columns }, (_, k) => column(`v${String(k)}`, TEXT)),
                    ]),
                  })
                ).id ?? 0;
            });
            write = grants.find((g) => g.label === "write path" && g.held > 0);
            if (!write) throw new Error("Missing write grant");
            if (!write.allocate(6 * MiB)) throw new Error("Working fixture did not fit");
            scratch = new Uint8Array(6 * MiB).fill(0x5a);
            if (s.loan) {
              loan = await write.borrow(64 * 1024);
              reach.reach("caller loan held");
            }
            const expected: unknown[][] = [];
            for (let cycle = 0; cycle < s.cycles; cycle++) {
              const ids = Array.from({ length: s.rows }, (_, k) => cycle * s.rows + k);
              const strings = ids.map((id) => `value-${String(id)}-${"x".repeat(s.valueBytes)}`);
              await autocommit(path, (statement) =>
                statement
                  .table(table)
                  .insert(
                    batch(ints(ids), ...Array.from({ length: s.columns }, () => texts(strings))),
                    null,
                  ),
              );
              expected.push(
                ...ids.map((id, k) => [id, ...Array.from({ length: s.columns }, () => strings[k])]),
              );
              if (s.atMerge) await path.flush();
              await path.quiesce();
              expect(path.failure).toBeNull();
              expect(path.maintenance.mergeFailures).toBe(0);
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              expect(write.allocated).toBeLessThanOrEqual(write.held);
              if (loan) expect(write.loaned).toBe(loan.bytes);
            }
            expect(path.maintenance.merges).toBeGreaterThan(0);
            reach.reach("real merge completed");
            // Exercise abort's reclamation too, with the same saturated holdings.
            const aborted = harness.space.beginWrite("generated low-cap abort");
            aborted.open((s.columns + 1) * 4096);
            await aborted.abort();
            reach.reach("abort completed");
            expect(pressured).toBeGreaterThan(0);
            const view = path.pin();
            try {
              expect(await rowsOf(view, table)).toEqual(expected);
            } finally {
              view.release();
            }
            expect(scratch[0]).toBe(0x5a);
            expect(scratch.at(-1)).toBe(0x5a);
            write.free(scratch.byteLength);
            scratch = null;
            loan?.repay();
            loan = null;
            write = undefined;
            path = await harness.reopen();
            const reopened = path.pin();
            try {
              expect(await rowsOf(reopened, table)).toEqual(expected);
            } finally {
              reopened.release();
            }
            await harness.space.audit();
            reach.reach("durable reopen and audit");
          } finally {
            if (scratch) write?.free(scratch.byteLength);
            loan?.repay();
            write = undefined;
            await harness.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("closed without charge");
        }),
        { seed, numRuns: 40 },
      );
    }, 120_000);
it("reaches the free-map pressure states", () => reach.assertReached());
