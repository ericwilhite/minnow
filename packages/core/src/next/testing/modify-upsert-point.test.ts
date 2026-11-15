/** A conflict fetch reads one physical row, including a publication after locator capture. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_UPSERT_POINT_REACH } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { MemoryReads } from "../write/memory-read.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-upsert-point", [0xb5d3]))(
  "bounds conflict reads to the selected physical row seed %i",
  async (seed) => {
    const reach = createReach("upsert point reads", MODIFY_UPSERT_POINT_REACH);
    const pg = new PGlite(),
      shipped = await currentEngine("memory");
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.oneof(fc.integer({ min: 2, max: 8 }), fc.constant(2050)),
          fc.nat(),
          fc.boolean(),
          fc.boolean(),
          async (n, offset, published, nullable) => {
            reach.beginHistory();
            const core = await new StorageCore(4096).open();
            try {
              const id = (offset % n) + 1;
              const all = async (sql: string) => {
                await core.execute(sql);
                await pg.exec(sql);
                await shipped.run({ kind: "sql", sql });
              };
              await pg.exec("DROP TABLE IF EXISTS point_ups");
              await shipped.run({ kind: "sql", sql: "DROP TABLE IF EXISTS point_ups" });
              await all("CREATE TABLE point_ups(id INTEGER PRIMARY KEY,v INTEGER,note TEXT)");
              for (let from = 0; from < n; from += 200) {
                await all(
                  `INSERT INTO point_ups VALUES ${Array.from(
                    { length: Math.min(200, n - from) },
                    (_, j) => {
                      const k = from + j;
                      return `(${String(k + 1)},${String(k * 13)},${nullable && k + 1 === id ? "NULL" : `'ß${String(k + 1)}'`})`;
                    },
                  ).join(",")}`,
                );
              }
              const tableId = core.harness.path.catalog.tableByName("point_ups")?.id;
              if (tableId === undefined) throw new Error("missing target");
              // eslint-disable-next-line @typescript-eslint/unbound-method -- Receiver supplied below.
              const original = MemoryReads.prototype.hint;
              let hints = 0,
                moved = false;
              const spy = vi.spyOn(MemoryReads.prototype, "hint").mockImplementation(function (
                this: MemoryReads,
                request,
                context,
              ) {
                hints++;
                expect(
                  request.maxRows,
                  "conflict fetch must not decode unrelated physical rows",
                ).toBe(1);
                const read = () => original.call(this, request, context);
                if (!published || moved) return read();
                moved = true;
                return (async () => {
                  // Freeze the committed source after capture without another SQL writer.
                  const previous = core.harness.path.liveMemtable(tableId, 0).memtable;
                  previous.state = "frozen";
                  previous.measure();
                  core.harness.path.liveMemtable(tableId, 1);
                  await core.harness.path.quiesce();
                  expect(this.source.resident(request.columns[0] ?? -1, request.chunk)).toBeNull();
                  return read();
                })();
              });
              const sql = `INSERT INTO point_ups VALUES(${String(id)},7,'!') ON CONFLICT(id) DO UPDATE SET v=point_ups.v+EXCLUDED.v,note=point_ups.note RETURNING *`;
              try {
                const actual = await core.execute(sql);
                if (!("returning" in actual)) throw new Error("returning");
                expect(oracleRows(actual.returning)).toEqual((await pg.query(sql)).rows);
                expect(oracleRows(actual.returning)).toEqual(
                  (await shipped.run({ kind: "sql", sql })).rows,
                );
                expect(hints).toBeGreaterThan(0);
                expect(moved).toBe(published);
              } finally {
                spy.mockRestore();
              }
              const after = await core.execute("SELECT * FROM point_ups ORDER BY id");
              if (after.kind !== "rows" || !("batches" in after)) throw new Error("rows");
              expect(oracleRows(after)).toEqual(
                (await pg.query("SELECT * FROM point_ups ORDER BY id")).rows,
              );
              expect(oracleRows(after)).toEqual(
                (await shipped.run({ kind: "sql", sql: "SELECT * FROM point_ups ORDER BY id" }))
                  .rows,
              );
              reach.reach(published ? "publication after capture" : "resident");
              reach.reach(id === 1 ? "first" : id === n ? "last" : "middle");
              reach.reach(nullable ? "NULL" : "text");
              reach.reach(id > 2048 ? "later physical chunk" : "first physical chunk");
              reach.reach("one physical row");
              expect(core.spill.bytesInUse).toBe(0);
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
            [4, 0, false, false],
            [4, 1, true, false],
            [4, 3, true, true],
            [2050, 2048, false, true],
            [2050, 2049, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await shipped.close();
    }
  },
  60000,
);
