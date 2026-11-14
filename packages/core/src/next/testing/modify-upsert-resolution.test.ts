/** Completed asynchronous arbitration reads are results, never cache-warming retries. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_UPSERT_RESOLUTION_REACH } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { ViewIndex } from "../write/view.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

for (const seed of seedsFor("next-upsert-async-resolution", [0xb5d1]))
  it(`consumes each completed arbiter lookup once seed ${String(seed)}`, async () => {
    const reach = createReach("async arbitration", MODIFY_UPSERT_RESOLUTION_REACH),
      pg = new PGlite(),
      old = await currentEngine("memory");
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 8 }),
          fc.constantFrom("fresh", "primary", "unique", "targetless"),
          fc.boolean(),
          async (n, kind, published) => {
            reach.beginHistory();
            const core = await new StorageCore(published ? 1 : undefined).open();
            try {
              const all = async (sql: string) => {
                await core.execute(sql);
                await pg.exec(sql);
                await old.run({ kind: "sql", sql });
              };
              await pg.exec("DROP TABLE IF EXISTS async_ups");
              await old.run({ kind: "sql", sql: "DROP TABLE IF EXISTS async_ups" });
              await all(
                "CREATE TABLE async_ups(id INTEGER PRIMARY KEY,note TEXT UNIQUE,v INTEGER)",
              );
              await all(
                `INSERT INTO async_ups VALUES ${Array.from({ length: n }, (_, k) => `(${String(k + 1)},'n${String(k + 1)}',0)`).join(",")}`,
              );
              if (published) {
                await core.harness.path.recycleLog();
                await core.reopen();
              }
              const targetless = kind === "targetless",
                fresh = kind === "fresh",
                byPrimary = kind === "primary";
              const sql = `INSERT INTO async_ups VALUES ${Array.from({ length: n }, (_, k) => `(${String(byPrimary ? k + 1 : n + k + 1)},'${fresh ? "fresh" : "n"}${String(k + 1)}',${String(k + 10)})`).join(",")} ON CONFLICT ${targetless ? "" : byPrimary ? "(id)" : "(note)"} ${targetless ? "DO NOTHING" : "DO UPDATE SET v=EXCLUDED.v"} RETURNING id,v`;
              const expected = (await pg.query(sql)).rows,
                shipped = await old.run({ kind: "sql", sql });
              // eslint-disable-next-line @typescript-eslint/unbound-method -- Supplied receiver via call below.
              const original = ViewIndex.prototype.resolve;
              let calls = 0;
              const maximum = n * (targetless ? 2 : 1);
              const spy = vi.spyOn(ViewIndex.prototype, "resolve").mockImplementation(function (
                this: ViewIndex,
                key,
                out,
              ) {
                expect(
                  ++calls,
                  "a completed arbiter read must not be issued again",
                ).toBeLessThanOrEqual(maximum);
                return Promise.resolve(original.call(this, key, out));
              });
              try {
                const result = await core.execute(sql);
                if (!("returning" in result)) throw new Error("returning");
                const bag = (rows: readonly unknown[]) =>
                  [...rows].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
                expect(bag(oracleRows(result.returning))).toEqual(bag(expected));
                expect(bag(oracleRows(result.returning))).toEqual(bag(shipped.rows ?? []));
                expect(calls).toBe(maximum);
              } finally {
                spy.mockRestore();
              }
              const after = await core.execute("SELECT * FROM async_ups ORDER BY id");
              if (after.kind !== "rows" || !("batches" in after)) throw new Error("rows");
              expect(oracleRows(after)).toEqual(
                (await pg.query("SELECT * FROM async_ups ORDER BY id")).rows,
              );
              expect(oracleRows(after)).toEqual(
                (await old.run({ kind: "sql", sql: "SELECT * FROM async_ups ORDER BY id" })).rows,
              );
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach(kind);
              reach.reach(published ? "published" : "resident");
              reach.reach("completed read used");
              reach.reach("bounded lookup count");
            } finally {
              await core.close();
            }
            expect(core.harness.governor.usedBytes).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [1, "fresh", false],
            [2, "primary", true],
            [2, "unique", false],
            [2, "targetless", true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await old.close();
    }
  }, 60000);
