/** Pending statement markers are not LSNs and must never become log-durability waits. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { MAX_LSN } from "../access/snapshot.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_PENDING_FLUSH_STATES } from "../../testing/reach.js";
import { yieldTask } from "../exec/pipeline.js";
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
  tableInput,
} from "./test-support.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";

const reach = createReach("pending delta versus flush durability", WRITE_PENDING_FLUSH_STATES);
for (const seed of seedsFor("next-write-pending-flush", [0xb3c7]))
  it(`does not wait for pending index markers (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          cap: fc.constantFrom(4, 8, 16),
          rollback: fc.boolean(),
          earlier: fc.boolean(),
          backend: fc.constantFrom("memory", "indexeddb"),
        }),
        async (s) => {
          reach.beginHistory();
          reach.reach(s.backend);
          const h = new Harness("strict", "memory", s.cap);
          if (s.backend === "indexeddb")
            Object.assign(h, {
              backend: createIndexedDbBackend({
                database: () => simIndexedDb(h.disk),
                name: "pending-flush",
                locks: new SimLocks(),
                memory: h.governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
              }),
            });
          const p = await h.open();
          let table = 0;
          try {
            await autocommit(p, async (st) => {
              table =
                (
                  await st.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false)]),
                  })
                ).id ?? 0;
            });
            const base = Array.from({ length: s.cap - 1 }, (_, k) => k);
            await autocommit(p, (st) => st.table(table).insert(batch(ints(base)), null));
            const pin = p.pin(),
              at = pin.internalTable(table).locator(1);
            pin.release();
            const invalid: number[] = [];
            // eslint-disable-next-line @typescript-eslint/unbound-method -- called with the actual appender below.
            const durable = LogAppender.prototype.durable;
            vi.spyOn(LogAppender.prototype, "durable").mockImplementation(function (
              this: LogAppender,
              lsn,
            ) {
              if (lsn > MAX_LSN) {
                invalid.push(lsn);
                return Promise.resolve();
              }
              return durable.call(this, lsn);
            });
            const tx = await p.begin();
            let st = tx.statement({
              rules: () => ({ check: () => null, generate: () => undefined }),
            });
            try {
              await st.table(table).delete(batch(locators([at], table)), null);
              if (s.earlier) {
                await st.end();
                st = tx.statement({
                  rules: () => ({ check: () => null, generate: () => undefined }),
                });
                reach.reach("earlier statement");
              } else reach.reach("same statement");
              await st.table(table).insert(batch(ints([100, 101])), null);
              await yieldTask();
              await yieldTask();
              expect(p.stats().frozenMemtables).toBeGreaterThan(0);
              reach.reach("frozen committed rows with pending tombstone");
              expect(
                invalid,
                "no durability request may name an uncommitted statement marker",
              ).toEqual([]);
              await st.end();
              if (s.rollback) {
                tx.rollback();
                reach.reach("rollback");
              } else {
                const receipt = await tx.commit(null);
                await receipt.durable;
                reach.reach("commit");
              }
            } finally {
              tx.rollback();
              vi.restoreAllMocks();
            }
            const expected = (s.rollback ? base : [...base.slice(1), 100, 101]).map((id) => [id]);
            const check = async (path: typeof p) => {
              const v = path.pin();
              try {
                expect(
                  (await rowsOf(v, table)).sort((a, b) => Number(a[0]) - Number(b[0])),
                ).toEqual(expected);
              } finally {
                v.release();
              }
            };
            await p.recycleLog();
            await check(p);
            await check(await h.reopen());
            reach.reach("publication and reopen");
          } finally {
            vi.restoreAllMocks();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("close settles");
        },
      ),
      { seed, numRuns: 40 },
    );
  }, 120_000);
it("reaches pending flush states", () => reach.assertReached());
