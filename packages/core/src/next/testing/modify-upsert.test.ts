/** PostgreSQL INSERT ON CONFLICT: arbiters observe earlier proposals, including EXCLUDED and WHERE. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  MODIFY_UPSERT_REACH,
  MODIFY_UPSERT_KEY_LIMIT_REACH,
} from "../../testing/reach.js";
import * as keyEncoding from "../keys/key-encoding.js";
import { flatOf, slotIndex } from "../exec/kernels/vectors.js";
import { IndexKeyLimitError } from "../write/index-limits.js";
import { ViewIndex } from "../write/view.js";
import { currentEngine } from "../../testing/differential-current.js";
import { checkPlan } from "./ir-check.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-modify-upsert", [0xb5bd, 0xb5be]))(
  "arbitrates native proposals against PostgreSQL/shipped (seed %i)",
  async (seed) => {
    const core = await new StorageCore(16).open(),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    await pg.waitReady;
    const reach = createReach("native ON CONFLICT", MODIFY_UPSERT_REACH);
    const all = async (sql: string, params: ReadonlyArray<number | string | null> = []) => {
      const lowered = core.lowerer.lower(sql, core.context());
      if (lowered.kind === "modify") checkPlan(lowered.plan, core.harness.path.catalog);
      const bag = (rows: readonly unknown[] | undefined) =>
        [...(rows ?? [])].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
      const actual = await core.execute(sql, params),
        expected = await pg.query(sql, [...params]),
        old = await shipped.run({ kind: "sql", sql, params });
      if ("returning" in actual) {
        expect(bag(oracleRows(actual.returning))).toEqual(bag(expected.rows));
        expect(bag(oracleRows(actual.returning))).toEqual(bag(old.rows));
      }
      if (actual.kind === "rows" && "batches" in actual) {
        expect(oracleRows(actual)).toEqual(expected.rows);
        expect(oracleRows(actual)).toEqual(old.rows);
      }
      expect(core.spill.bytesInUse).toBe(0);
      return actual;
    };
    const compare = async () => {
      const sql = "SELECT * FROM ups ORDER BY id",
        actual = await core.execute(sql);
      if (actual.kind !== "rows" || !("batches" in actual)) throw new Error("rows");
      expect(oracleRows(actual)).toEqual((await pg.query(sql)).rows);
      expect(oracleRows(actual)).toEqual((await shipped.run({ kind: "sql", sql })).rows);
    };
    const schema =
      "CREATE TABLE ups (id INTEGER PRIMARY KEY,email TEXT UNIQUE,a INTEGER,b INTEGER,v INTEGER NOT NULL,UNIQUE(a,b))";
    try {
      await all(schema);
      await all("CREATE TABLE pkups (a INTEGER,b INTEGER,v INTEGER,PRIMARY KEY(a,b))");
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 8 }),
          fc.integer({ min: 1, max: 8 }),
          fc.boolean(),
          async (n, delta, reverse) => {
            reach.beginHistory();
            await all("DROP TABLE ups");
            await all(schema);
            await all(
              `INSERT INTO ups VALUES ${Array.from({ length: n }, (_, i) => `(${String(i + 1)},'e${String(i + 1)}',${String(i + 1)},${String(i + 2)},${String((i + 1) * 10)})`).join(",")}`,
            );
            await all(
              "INSERT INTO ups VALUES (1,'e1',1,2,$1) ON CONFLICT (id) DO UPDATE SET v=ups.v+EXCLUDED.v RETURNING id,v",
              [delta],
            );
            await all("DROP TABLE pkups");
            await all("CREATE TABLE pkups (a INTEGER,b INTEGER,v INTEGER,PRIMARY KEY(a,b))");
            await all("INSERT INTO pkups VALUES (1,2,3)");
            await all(
              `INSERT INTO pkups VALUES (1,2,${String(delta)}) ON CONFLICT(${reverse ? "b,a" : "a,b"}) DO UPDATE SET v=pkups.v+EXCLUDED.v RETURNING a,b,v`,
            );
            reach.reach("composite primary target");
            reach.reach("primary target");
            reach.reach("EXCLUDED");
            await all(
              `INSERT INTO ups VALUES (100,'e2',100,101,${String(delta)}) ON CONFLICT(email) DO UPDATE SET v=ups.v+EXCLUDED.v RETURNING id,v`,
            );
            reach.reach("unique target");
            await all(
              `INSERT INTO ups VALUES (101,'fresh',1,2,${String(delta)}) ON CONFLICT(${reverse ? "b,a" : "a,b"}) DO UPDATE SET v=EXCLUDED.v RETURNING id,v`,
            );
            reach.reach(reverse ? "target permutation" : "target order");
            for (const flag of [false, true]) {
              await all(
                `INSERT INTO ups VALUES (1,'e1',1,2,${String(delta)}) ON CONFLICT(id) DO UPDATE SET v=EXCLUDED.v+$1 WHERE EXCLUDED.v>$2 RETURNING id,v+$3 AS bound`,
                [delta, flag ? 0 : 1000, 7],
              );
              reach.reach(flag ? "WHERE true" : "WHERE false");
            }
            await all(
              "INSERT INTO ups VALUES (1,'e1',1,2,99),(900,'e2',900,901,99),(901,'fresh901',1,2,99),(902,'fresh902',902,903,99) ON CONFLICT DO NOTHING RETURNING id,v",
            );
            reach.reach("targetless nothing");
            reach.reach("multi-row proposals");
            await all(
              "INSERT INTO ups VALUES (1,'other',1000,1001,99) ON CONFLICT(id) DO NOTHING RETURNING id,v",
            );
            reach.reach("targeted nothing");
            await all(
              "INSERT INTO ups SELECT id+10000,email,a,b,v FROM ups ON CONFLICT(email) DO UPDATE SET v=EXCLUDED.v+$1 RETURNING id,v",
              [delta],
            );
            reach.reach("insert select");
            await all(
              "INSERT INTO ups VALUES (903,'fresh903',NULL,2,1),(904,'fresh904',NULL,2,2) ON CONFLICT(a,b) DO NOTHING RETURNING id,v",
            );
            reach.reach("null unique tuples");
            await compare();
            for (const proposals of [
              "(1,'e1',1,2,1),(1,'e1',1,2,2)",
              "(999,'new',999,1000,1),(999,'new',999,1000,2)",
            ]) {
              const sql = `INSERT INTO ups VALUES ${proposals} ON CONFLICT(id) DO UPDATE SET v=EXCLUDED.v RETURNING id,v`;
              await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "21000" });
              await expect(pg.query(sql)).rejects.toMatchObject({ code: "21000" });
              await expect(shipped.run({ kind: "sql", sql })).rejects.toThrow(/twice/);
              reach.reach(proposals.startsWith("(1,") ? "repeated existing" : "repeated inserted");
              await compare();
            }
            await all("INSERT INTO ups VALUES (905,'fresh905',905,906,5) ON CONFLICT DO NOTHING");
            const counted = await all(
              "INSERT INTO ups VALUES (1,'e1',1,2,1) ON CONFLICT(id) DO UPDATE SET v=EXCLUDED.v",
            );
            expect("rowCount" in counted ? counted.rowCount : -1).toBe(1);
            const repeatedUnique =
              "INSERT INTO ups VALUES (1100,'e2',1100,1101,1),(1101,'e2',1101,1102,2) ON CONFLICT(email) DO UPDATE SET v=EXCLUDED.v";
            await expect(core.execute(repeatedUnique)).rejects.toMatchObject({ sqlState: "21000" });
            await expect(pg.query(repeatedUnique)).rejects.toMatchObject({ code: "21000" });
            await expect(shipped.run({ kind: "sql", sql: repeatedUnique })).rejects.toThrow(
              /twice/,
            );
            reach.reach("repeated unique target");
            const nullProposal =
              "INSERT INTO ups VALUES (1,'e1',1,2,NULL) ON CONFLICT(id) DO NOTHING";
            await expect(core.execute(nullProposal)).rejects.toMatchObject({ sqlState: "23502" });
            await expect(pg.query(nullProposal)).rejects.toMatchObject({ code: "23502" });
            // Shipped DO NOTHING skips this validation; PGlite is the constraint authority.
            expect((await shipped.run({ kind: "sql", sql: nullProposal })).counts?.rowCount).toBe(
              0,
            );
            reach.reach("without RETURNING");
            reach.reach("proposed NOT NULL before conflict");
            const bad = "INSERT INTO ups VALUES (999,'e2',999,1000,1) ON CONFLICT(id) DO NOTHING";
            await expect(core.execute(bad)).rejects.toMatchObject({ sqlState: "23505" });
            await expect(pg.query(bad)).rejects.toMatchObject({ code: "23505" });
            await expect(shipped.run({ kind: "sql", sql: bad })).rejects.toThrow();
            reach.reach("non-arbiter unique rollback");
            await compare();
            const refused =
              "INSERT INTO ups VALUES (1,'e1',1,2,1) ON CONFLICT(id) DO UPDATE SET id=EXCLUDED.id";
            await expect(core.execute(refused)).rejects.toThrow(/cannot reassign the conflict key/);
            await expect(shipped.run({ kind: "sql", sql: refused })).rejects.toThrow(
              /cannot reassign the conflict key/,
            );
            const primaryThroughUnique =
              "INSERT INTO ups VALUES (1,'e1',1,2,1) ON CONFLICT(email) DO UPDATE SET id=EXCLUDED.id";
            await expect(core.execute(primaryThroughUnique)).rejects.toThrow(
              /cannot reassign the conflict key/,
            );
            await expect(shipped.run({ kind: "sql", sql: primaryThroughUnique })).rejects.toThrow(
              /cannot reassign the conflict key/,
            );
            await all("CREATE TABLE fk_parent (id INTEGER PRIMARY KEY,code INTEGER UNIQUE)");
            await all(
              "CREATE TABLE fk_child (id INTEGER PRIMARY KEY,ref INTEGER REFERENCES fk_parent(id) ON UPDATE CASCADE)",
            );
            await all("INSERT INTO fk_parent VALUES(1,10),(2,20)");
            await all("INSERT INTO fk_child VALUES(1,1)");
            await all(
              "INSERT INTO fk_child VALUES(1,2) ON CONFLICT(id) DO UPDATE SET ref=EXCLUDED.ref RETURNING id,ref",
            );
            await all("SELECT * FROM fk_child ORDER BY id");
            reach.reach("FK valid update");
            const missingParent =
              "INSERT INTO fk_child VALUES(1,999) ON CONFLICT(id) DO UPDATE SET ref=EXCLUDED.ref RETURNING id,ref";
            await expect(core.execute(missingParent)).rejects.toMatchObject({ sqlState: "23503" });
            await expect(pg.query(missingParent)).rejects.toMatchObject({ code: "23503" });
            await expect(shipped.run({ kind: "sql", sql: missingParent })).rejects.toThrow();
            await all("SELECT * FROM fk_child ORDER BY id");
            await all("DROP TABLE fk_child");
            await all("DROP TABLE fk_parent");
            reach.reach("FK rollback");
            reach.reach("arbiter assignment refused");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [2, 1, false],
            [3, 2, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
      await shipped.close();
      expect(core.harness.governor.usedBytes).toBe(0);
    }
  },
  60000,
);

for (const seed of seedsFor("next-upsert-key-limits", [0xb5d2]))
  it(`checks oversized proposals before encoding their arbiter keys seed ${String(seed)}`, async () => {
    const reach = createReach("upsert key limits", MODIFY_UPSERT_KEY_LIMIT_REACH),
      pg = new PGlite(),
      old = await currentEngine("memory");
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 130000, max: 180000 }),
          fc.constantFrom("x", "é", "😀"),
          fc.boolean(),
          fc.constantFrom("nothing", "targetless", "update"),
          async (size, glyph, prefix, action) => {
            reach.beginHistory();
            const core = await new StorageCore().open();
            try {
              const ddl =
                "CREATE TABLE wide_ups(id INTEGER PRIMARY KEY,note TEXT UNIQUE,v INTEGER)";
              await core.execute(ddl);
              await pg.exec("DROP TABLE IF EXISTS wide_ups");
              await pg.exec(ddl);
              await old.run({ kind: "sql", sql: "DROP TABLE IF EXISTS wide_ups" });
              await old.run({ kind: "sql", sql: ddl });
              const table = core.harness.path.catalog.tableByName("wide_ups"),
                note = table?.columns.find((c) => c.name === "note"),
                index = table?.indexes.find(
                  (i) => i.role === "unique" && i.columnIds.includes(note?.id ?? -1),
                );
              if (!index) throw new Error("note index");
              // Keep the exact UTF16 width oversized while making PG btree compression predictable.
              const value = glyph + "x".repeat(size - glyph.length),
                expected = {
                  code: "MINNOW_INDEX_KEY_TOO_LONG",
                  indexId: index.id,
                  units: value.length,
                  maximum: 16384,
                };
              const original = keyEncoding.encodeKeyBatch;
              const encoding = vi
                .spyOn(keyEncoding, "encodeKeyBatch")
                .mockImplementation((...args) => {
                  for (const column of args[1]) {
                    const data = flatOf(column.vector).data,
                      at = slotIndex(column.vector, 0);
                    if (
                      data.layout === "varlen" &&
                      (data.offsets[at + 1] ?? 0) - (data.offsets[at] ?? 0) > 65536
                    )
                      throw new Error(
                        "oversized arbiter reached key encoding before its limit check",
                      );
                  }
                  return original(...args);
                });
              const plain = "INSERT INTO wide_ups VALUES(99,$1,7)",
                sql = `INSERT INTO wide_ups VALUES ${prefix ? "(1,'prefix',2)," : ""}(2,$1,3) ON CONFLICT ${action === "targetless" ? "" : "(note)"} ${action === "update" ? "DO UPDATE SET v=EXCLUDED.v" : "DO NOTHING"} RETURNING id`;
              try {
                await expect(core.execute(plain, [value])).rejects.toBeInstanceOf(
                  IndexKeyLimitError,
                );
                await expect(core.execute(plain, [value])).rejects.toMatchObject(expected);
                await expect(core.execute(sql, [value])).rejects.toMatchObject(expected);
              } finally {
                encoding.mockRestore();
              }
              await expect(old.run({ kind: "sql", sql, params: [value] })).rejects.toThrow();
              // PG accepts these compressible keys; Minnow's documented index-path cap is stricter.
              expect((await pg.query(sql, [value])).rows).toHaveLength(prefix ? 2 : 1);
              const empty = await core.execute("SELECT * FROM wide_ups");
              if (empty.kind !== "rows" || !("batches" in empty)) throw new Error("rows");
              expect(oracleRows(empty)).toEqual([]);
              reach.reach(action);
              reach.reach(prefix ? "prefix rollback" : "empty proposal");
              reach.reach(glyph === "x" ? "ASCII" : "UTF8");
              reach.reach("before key encoding");
              reach.reach("plain identity");
              reach.reach("shipped cap PG difference");
              // Legal keys must also use bounded call arity on engines with smaller argument limits.
              const fromCharCode = String.fromCharCode;
              const arity = vi.spyOn(String, "fromCharCode").mockImplementation((...units) => {
                expect(
                  units.length,
                  "byte-key conversion uses bounded call arity",
                ).toBeLessThanOrEqual(512);
                return Reflect.apply(fromCharCode, String, units);
              });
              try {
                await core.execute(
                  "INSERT INTO wide_ups VALUES(3,$1,9) ON CONFLICT(note) DO NOTHING",
                  [glyph.repeat(1024)],
                );
                const pin = core.harness.path.pin();
                try {
                  const entries = await new ViewIndex(
                    index,
                    pin.internalTable(index.tableId),
                  ).collect({ lower: null, upper: null });
                  expect(entries).toHaveLength(1);
                  expect(entries[0]?.key.length).toBeGreaterThan(512);
                  reach.reach("range keys");
                } finally {
                  pin.release();
                }
              } finally {
                arity.mockRestore();
              }
              reach.reach("bounded arity");
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
            [150000, "x", false, "nothing"],
            [130000, "é", true, "targetless"],
            [150000, "😀", true, "update"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await old.close();
    }
  }, 120000);
