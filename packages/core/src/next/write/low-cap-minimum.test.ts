/** Admitted write progress must not wait for another execution's retained minimum. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_PRIVATE_MINIMUM_STATES } from "../../testing/reach.js";
import {
  Harness,
  autocommit,
  column,
  tableInput,
  INT,
  batch,
  ints,
  rowsOf,
} from "./test-support.js";

const MiB = 1048576;
const reach = createReach("prepaid write progress", WRITE_PRIVATE_MINIMUM_STATES);
for (const backend of ["memory", "indexeddb"] as const)
  for (const seed of seedsFor("next-write-private-minimum", [0xb3f1]))
    it(`makes merge progress inside prepaid credit over ${backend} (seed ${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            cap: fc.constantFrom(32, 48, 64),
            rows: fc.integer({ min: 4, max: 16 }),
            fanIn: fc.constantFrom(2, 3),
            cycles: fc.integer({ min: 5, max: 8 }),
          }),
          async (s) => {
            reach.beginHistory();
            reach.reach(backend);
            reach.reach(s.cap === 32 ? "32 MiB" : s.cap === 48 ? "48 MiB" : "64 MiB");
            reach.reach(s.fanIn === 2 ? "two input policy" : "three input policy");
            const governor = new Governor(s.cap * MiB);
            const h = new Harness("strict", "memory", s.rows, undefined, {
              merges: { mode: "manual", fanIn: s.fanIn, sliceEntries: 32 },
            });
            Object.assign(h, { governor });
            if (backend === "indexeddb")
              Object.assign(h, {
                backend: createIndexedDbBackend({
                  database: () => simIndexedDb(h.disk),
                  name: "write-private-minimum",
                  locks: new SimLocks(),
                  memory: governor.registerCache({
                    evictableBytes: 0,
                    evict: () => 0,
                  }),
                }),
              });
            let holder: Grant | undefined,
              buffer: Uint8Array | undefined,
              blocked = 0;
            const blockedAt: string[] = [];
            const release = () => {
              if (!holder) return;
              expect(buffer?.[0]).toBe(7);
              expect(buffer?.at(-1)).toBe(7);
              holder.close();
              holder = undefined;
              buffer = undefined;
            };
            const admit = governor.admit.bind(governor);
            governor.admit = async (options) => {
              const pending = admit(options);
              if (holder && options.label.startsWith("merge")) {
                const settlement = { complete: false };
                void pending.then(
                  () => {
                    settlement.complete = true;
                  },
                  () => {
                    settlement.complete = true;
                  },
                );
                // The old design's queue is observed at a semantic microtask boundary. Release
                // only for cleanup, then reject via the progress assertion; timeout is not RED.
                for (let turn = 0; turn < 8; turn++) await Promise.resolve();
                if (!settlement.complete) {
                  blocked++;
                  blockedAt.push("pending global merge admission");
                  release();
                }
              }
              const grant = await pending;
              watchMetadata(grant);
              return grant;
            };
            const watchMetadata = (grant: Grant) => {
              if (grant.label === "write path metadata") {
                const grow = grant.tryGrow.bind(grant);
                grant.tryGrow = (bytes) => {
                  const ok = grow(bytes);
                  if (!ok && holder) {
                    blocked++;
                    blockedAt.push("denied metadata growth");
                    release();
                  }
                  return ok;
                };
              }
            };
            if (typeof governor.admitGroup === "function") {
              const admitGroup = governor.admitGroup.bind(governor);
              governor.admitGroup = async (options) => {
                const grants = await admitGroup(options);
                for (const grant of grants) watchMetadata(grant);
                return grants;
              };
            }
            try {
              const path = await h.open();
              let table = 0;
              await autocommit(path, async (st) => {
                table =
                  (
                    await st.changeCatalog({
                      kind: "createTable",
                      table: tableInput("t", [column("id", INT, false)]),
                    })
                  ).id ?? 0;
              });
              const expected: number[][] = [];
              for (let cycle = 0; cycle < s.cycles; cycle++) {
                const ids = Array.from({ length: s.rows }, (_, n) => cycle * s.rows + n);
                await autocommit(path, (st) => st.table(table).insert(batch(ints(ids)), null));
                expected.push(...ids.map((id) => [id]));
                await path.quiesce();
              }
              expect(path.maintenance.flushes).toBeGreaterThanOrEqual(s.fanIn);
              reach.reach("merge debt prepared");
              const free = governor.capBytes - governor.reserveBytes - governor.usedBytes;
              expect(free).toBeGreaterThan(0);
              holder = await governor.admit({
                label: "protected execution minimum",
                minimumBytes: free,
                spillable: false,
              });
              expect(holder.allocate(free)).toBe(true);
              buffer = new Uint8Array(free).fill(7);
              reach.reach("protected holder owns real bytes");
              await path.runMaintenance();
              expect(
                blocked,
                `write maintenance must not wait for an admitted execution's protected minimum (${blockedAt.join(", ")})`,
              ).toBe(0);
              expect(holder).toBeDefined();
              expect(buffer[0]).toBe(7);
              expect(path.maintenance.merges).toBeGreaterThan(0);
              expect(path.maintenance.mergeFailures).toBe(0);
              expect(path.failure).toBeNull();
              reach.reach("merge progressed while holder retained");
              const view = path.pin();
              try {
                expect(await rowsOf(view, table)).toEqual(expected);
                reach.reach("complete current snapshot");
              } finally {
                view.release();
              }
              release();
              await h.reopen();
              const reopened = h.path.pin();
              try {
                expect(await rowsOf(reopened, table)).toEqual(expected);
                reach.reach("durable reopen");
              } finally {
                reopened.release();
              }
              await h.space.audit();
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
            } finally {
              release();
              await h.close();
            }
            expect(governor.usedBytes).toBe(0);
            reach.reach("all credit returned");
          },
        ),
        { seed, numRuns: 40 },
      );
    }, 120000);

it("reaches prepaid write progress states", () => reach.assertReached());
