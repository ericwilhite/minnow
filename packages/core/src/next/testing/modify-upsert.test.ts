/** PostgreSQL INSERT ON CONFLICT: arbiters observe earlier proposals, including EXCLUDED and WHERE. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_UPSERT_REACH } from "../../testing/reach.js";
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
