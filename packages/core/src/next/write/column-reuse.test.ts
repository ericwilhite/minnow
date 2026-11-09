/** vector/vector.ts ownership: reuse only after the last borrow; cached capacities stay charged. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_COLUMN_POOL_STATES } from "../../testing/reach.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { UniqueViolationError } from "../access/modify.js";
import { ColumnChunk } from "./columns.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  TEXT,
  ints,
  texts,
  tableInput,
  rowsOf,
} from "./test-support.js";
const reach = createReach("column reuse ownership", WRITE_COLUMN_POOL_STATES);
for (const backend of ["memory", "indexeddb"] as const)
  for (const seed of seedsFor("next-write-column-pool", [0xb3a11]))
    it(`reuses released storage through rollback and reopen over ${backend} (${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            count: fc.constantFrom(8193, 12289),
            width: fc.constantFrom(31, 257),
            nullEvery: fc.integer({ min: 2, max: 11 }),
          }),
          async (s) => {
            reach.beginHistory();
            reach.reach(backend);
            const h = new Harness("strict", "memory", 2048);
            if (backend === "indexeddb")
              Object.assign(h, {
                backend: createIndexedDbBackend({
                  database: () => simIndexedDb(h.disk),
                  name: "column-pool",
                  locks: new SimLocks(),
                  memory: h.governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
                }),
              });
            let path = await h.open(),
              table = 0;
            await autocommit(path, async (st) => {
              table =
                (
                  await st.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false), column("v", TEXT)]),
                  })
                ).id ?? 0;
            });
            const seen = new Set<ArrayBufferLike>();
            let reused = 0;
            // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked below with explicit this
            const append = ColumnChunk.prototype.append;
            const spy = vi.spyOn(ColumnChunk.prototype, "append").mockImplementation(function (
              this: ColumnChunk,
              ...args
            ) {
              const empty = this.length === 0;
              append.apply(this, args);
              const buffer = this.arena?.buffer;
              if (empty && buffer) {
                if (seen.has(buffer)) reused++;
                seen.add(buffer);
              }
            });
            const ids = Array.from({ length: s.count }, (_, k) => k);
            const value = "r".repeat(s.width);
            const nullable = ids.map((k) => (k % s.nullEvery === 0 ? null : value));
            try {
              // The duplicate is last, after staging and reuse; all earlier copied rows roll back.
              const bad = [...ids];
              bad[bad.length - 1] = 0;
              await expect(
                autocommit(path, (st) =>
                  st.table(table).insert(batch(ints(bad), texts(nullable)), null),
                ),
              ).rejects.toBeInstanceOf(UniqueViolationError);
              const before = path.pin();
              try {
                expect(await rowsOf(before, table)).toEqual([]);
              } finally {
                before.release();
              }
              reach.reach("rollback after reuse");
              await autocommit(path, (st) =>
                st.table(table).insert(batch(ints(ids), texts(ids.map(() => value))), null),
              );
              expect(reused, "released text arenas must be reused").toBeGreaterThan(0);
              reach.reach("backing buffer reused");
              reach.reach("null to valid");
              spy.mockRestore();
              await h.reopen();
              path = h.path;
              const view = path.pin();
              try {
                expect(await rowsOf(view, table)).toEqual(ids.map((k) => [k, value]));
              } finally {
                view.release();
              }
              reach.reach("reopen");
            } finally {
              spy.mockRestore();
              await h.close();
            }
            expect(h.governor.usedBytes).toBe(0);
            reach.reach("zero final charge");
          },
        ),
        { seed, numRuns: 8 },
      );
    }, 120000);

it("reaches column reuse states", () => reach.assertReached());
