/** Optional range caches may shed memory; UNIQUE still probes every potentially matching layer. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { UniqueViolationError } from "../access/modify.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { createReach, WRITE_RANGE_PRESSURE_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { KeyDelta } from "./key-hash.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
} from "./test-support.js";

const reach = createReach("optional bounds under pressure", WRITE_RANGE_PRESSURE_STATES);
for (const seed of seedsFor("next-write-range-pressure", [0xb3c4]))
  it(`keeps uniqueness when nonempty layers have no funded range (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          rows: fc.constantFrom(4, 8, 16),
          batchRows: fc.constantFrom(1, 2),
          duplicate: fc.boolean(),
          backend: fc.constantFrom("memory", "indexeddb"),
        }),
        async (s) => {
          reach.beginHistory();
          reach.reach(s.backend);
          reach.reach(s.batchRows === 1 ? "single row" : "batch rows");
          let entered!: () => void, release!: () => void;
          const reached = new Promise<void>((resolve) => {
              entered = resolve;
            }),
            gate = new Promise<void>((resolve) => {
              release = resolve;
            });
          const h = new Harness("strict", "memory", s.rows, undefined, {
            beforeFlushPublish: async () => {
              entered();
              await gate;
            },
          });
          if (s.backend === "indexeddb")
            Object.assign(h, {
              backend: createIndexedDbBackend({
                database: () => simIndexedDb(h.disk),
                name: "range-pressure",
                locks: new SimLocks(),
                memory: h.governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
              }),
            });
          const p = await h.open();
          let table = 0,
            unknown = 0;
          const base = Array.from({ length: s.rows }, (_, k) => k),
            fresh = s.rows + 100;
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
            await autocommit(p, (st) => st.table(table).insert(batch(ints(base)), null));
            // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the delta receiver via call below.
            const original = KeyDelta.prototype.keyRange;
            vi.spyOn(KeyDelta.prototype, "keyRange").mockImplementation(function (this: KeyDelta) {
              if (this.keyCount > 0) {
                unknown++;
                return null;
              }
              return original.call(this);
            });
            const values = Array.from({ length: s.batchRows }, (_, k) => s.rows + 20 + k);
            if (s.duplicate) values[values.length - 1] = s.rows >>> 1;
            const attempt = autocommit(p, (st) =>
              st.table(table).insert(batch(ints(values)), null),
            );
            if (s.duplicate) {
              await expect(attempt).rejects.toBeInstanceOf(UniqueViolationError);
              reach.reach(s.batchRows === 1 ? "single conflict" : "batch conflict");
            } else {
              await attempt;
              reach.reach("accepted fresh keys");
            }
            expect(unknown).toBeGreaterThan(0);
            reach.reach("nonempty range unavailable");
            await autocommit(p, (st) => st.table(table).insert(batch(ints([fresh])), null));
            const expected = [...base, ...(s.duplicate ? [] : values), fresh]
              .sort((a, b) => a - b)
              .map((id) => [id]);
            const check = async (path: typeof p) => {
              const view = path.pin();
              try {
                expect(
                  (await rowsOf(view, table)).sort((a, b) => Number(a[0]) - Number(b[0])),
                ).toEqual(expected);
              } finally {
                view.release();
              }
            };
            await check(p);
            expect(p.failure).toBeNull();
            await reached;
            release();
            await p.quiesce();
            await check(p);
            reach.reach("publication");
            vi.restoreAllMocks();
            await check(await h.reopen());
            reach.reach("durable reopen");
            await h.space.audit();
          } finally {
            release();
            vi.restoreAllMocks();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
  }, 120_000);
it("reaches range-pressure states", () => reach.assertReached());
