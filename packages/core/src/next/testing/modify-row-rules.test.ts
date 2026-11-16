/** Root DML must use the same admitted rule scope as native FK actions. PostgreSQL is the error authority. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_ROW_RULES_REACH, ROOT_RULE_ORDER_REACH } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
it.each(seedsFor("next-modify-row-rules", [0xb5c1, 0xb5c2]))(
  "binds generated/CHECK rules through root completion (seed %i)",
  async (seed) => {
    const core = await new StorageCore(32).open(),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    await pg.waitReady;
    const reach = createReach("root row rules", MODIFY_ROW_RULES_REACH);
    const bag = (rows: readonly unknown[] | undefined) =>
      [...(rows ?? [])].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    const all = async (sql: string, params: readonly number[] = [], nativeSql = sql) => {
      const actual = await core.execute(nativeSql, params),
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
    };
    const schema =
      "CREATE TABLE ruled(id INTEGER PRIMARY KEY,v INTEGER NOT NULL,den INTEGER,g INTEGER GENERATED ALWAYS AS(100/den) STORED,h INTEGER GENERATED ALWAYS AS(v*2) STORED,CHECK(v>=0),CHECK(g>0))";
    try {
      await all(schema);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 3, max: 12 }),
          fc.integer({ min: 1, max: 7 }),
          async (n, delta) => {
            reach.beginHistory();
            await all("DROP TABLE IF EXISTS rule_source");
            await all("DROP TABLE IF EXISTS rule_child");
            await all("DROP TABLE IF EXISTS rule_parent");
            await all("DROP TABLE ruled");
            await all(schema);
            await all(
              `INSERT INTO ruled(id,v,den) VALUES ${Array.from({ length: n }, (_, i) => `(${String(i + 1)},${String(i + 1)},${String(1 + (i % 3))})`).join(",")} RETURNING id,v,den,g,h`,
            );
            reach.reach("insert generated");
            await all("UPDATE ruled SET v=v+$1 WHERE id%2=0 RETURNING id,v,g,h", [delta]);
            reach.reach("update generated");
            await all("UPDATE ruled SET h=DEFAULT WHERE id%2=0 RETURNING id,h");
            reach.reach("generated DEFAULT");
            await all(
              "INSERT INTO ruled(id,v,den) SELECT id+100,v+1,den FROM ruled RETURNING id,v,g,h",
            );
            reach.reach("insert select generated");
            await all(
              "INSERT INTO ruled(id,v,den) VALUES(1,5,2) ON CONFLICT(id) DO UPDATE SET v=EXCLUDED.h+$1,den=EXCLUDED.den RETURNING id,v,g,h",
              [delta],
            );
            reach.reach("excluded generated");
            await all("CREATE TABLE rule_source(id INTEGER PRIMARY KEY,link INTEGER,den INTEGER)");
            await all("INSERT INTO rule_source VALUES(1,1,1),(2,1,2)");
            await all(
              "UPDATE ruled SET den=rule_source.den FROM rule_source WHERE ruled.id=rule_source.link RETURNING ruled.id,ruled.g",
            );
            await all("DROP TABLE rule_source");
            reach.reach("duplicate valid match");

            await expect(core.execute("UPDATE ruled SET g=9")).rejects.toMatchObject({
              sqlState: "428C9",
            });
            reach.reach("generated assignment refused");
            await all("SELECT * FROM ruled ORDER BY id");
            await all("CREATE TABLE rule_parent(id INTEGER PRIMARY KEY)");
            await all(
              "CREATE TABLE rule_child(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES rule_parent(id) ON UPDATE CASCADE ON DELETE SET NULL,g INTEGER GENERATED ALWAYS AS(COALESCE(pid,0)*2) STORED CHECK(g>=0))",
              [],
              // Product omission means identity-follow; PG uses explicit CASCADE for this control.
              "CREATE TABLE rule_child(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES rule_parent(id) ON DELETE SET NULL,g INTEGER GENERATED ALWAYS AS(COALESCE(pid,0)*2) STORED CHECK(g>=0))",
            );
            await all("INSERT INTO rule_parent VALUES(1)");
            await all("INSERT INTO rule_child(id,pid) VALUES(10,1)");
            await all(`UPDATE rule_parent SET id=id+${String(delta)} RETURNING id`);
            await all("SELECT * FROM rule_child ORDER BY id");
            expect(
              core.harness.path.catalog.tableByName("rule_child")?.foreignKeys[0]?.onUpdate,
            ).toBe("follow");
            reach.reach("omitted follow policy");
            reach.reach("FK generated rekey");
            await all("DELETE FROM rule_parent RETURNING id");
            await all("SELECT * FROM rule_child ORDER BY id");
            reach.reach("FK generated set null");
            await all("DROP TABLE rule_child");
            await all("DROP TABLE rule_parent");
            const before = await core.execute("SELECT * FROM ruled ORDER BY id");
            await expect(
              core.execute("UPDATE ruled SET v=-1 WHERE id%2=0 RETURNING id,v"),
            ).rejects.toMatchObject({ sqlState: "23514" });
            await expect(
              pg.query("UPDATE ruled SET v=-1 WHERE id%2=0 RETURNING id,v"),
            ).rejects.toMatchObject({ code: "23514" });
            await expect(
              shipped.run({
                kind: "sql",
                sql: "UPDATE ruled SET v=-1 WHERE id%2=0 RETURNING id,v",
              }),
            ).rejects.toThrow();
            const after = await core.execute("SELECT * FROM ruled ORDER BY id");
            if (
              before.kind !== "rows" ||
              !("batches" in before) ||
              after.kind !== "rows" ||
              !("batches" in after)
            )
              throw new Error("rows");
            expect(oracleRows(after)).toEqual(oracleRows(before));
            reach.reach("CHECK rollback");
          },
        ),
        { seed, numRuns: 12 },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
      await shipped.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  60000,
);

it.each(seedsFor("next-root-rule-error-order", [0xb5c3, 0xb5c4]))(
  "preserves first root-row constraint before later generated errors (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    await pg.waitReady;
    const reach = createReach("root error identity", ROOT_RULE_ORDER_REACH);
    try {
      const schema =
        "CREATE TABLE root_order(id INTEGER PRIMARY KEY,v INTEGER NOT NULL,den INTEGER,g INTEGER GENERATED ALWAYS AS(10/den) STORED NOT NULL,CONSTRAINT z_bad CHECK(v>=0),CONSTRAINT a_bad CHECK(v<100))";
      await core.execute(schema);
      await pg.exec(schema);
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 1, max: 8 }), fc.boolean(), async (n, notNull) => {
          reach.beginHistory();
          const first = notNull ? "NULL" : "-1",
            sql = `INSERT INTO root_order(id,v,den) VALUES ${Array.from({ length: n }, (_, i) => `(${String(i + 1)},${first},1)`).join(",")},(${String(n + 1)},1,0) RETURNING id,g`;
          const state = notNull ? "23502" : "23514";
          const table = core.harness.path.catalog.tableByName("root_order");
          if (!table) throw new Error("rule table");
          const identity = notNull
            ? { columnId: table.columns.find((column) => column.name === "v")?.id }
            : { checkId: table.checks.find((check) => check.name === "z_bad")?.id };
          await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: state, ...identity });
          reach.reach(notNull ? "NOT NULL before generated error" : "CHECK before generated error");
          await expect(pg.query(sql)).rejects.toMatchObject({ code: state });
          const rows = await core.execute("SELECT * FROM root_order");
          if (rows.kind !== "rows" || !("batches" in rows)) throw new Error("rows");
          expect(oracleRows(rows)).toEqual([]);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("constraint identity");
          reach.reach("statement rollback");
        }),
        {
          seed,
          numRuns: 12,
          examples: [
            [1, false],
            [2, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  60000,
);
