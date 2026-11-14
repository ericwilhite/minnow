/** FTS booleans come from shipped SQL; JSON text comes independently from PG's constructor. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, FTS_JSON_REACH } from "../../testing/reach.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
for (const seed of seedsFor("next-fts-json", [0xb7fd, 0xb7fe]))
  it(`keeps FTS and JSON owned on the same resumable row (${String(seed)})`, async () => {
    const core = await new StorageCore().open(),
      db = new MinnowDatabase(new MemoryBlockStore()),
      pg = new PGlite(),
      random = mulberry32(seed);
    const reach = createReach(`FTS JSON ${String(seed)}`, FTS_JSON_REACH);
    reach.beginHistory();
    try {
      await pg.waitReady;
      const ddl = "CREATE TABLE docs(id INTEGER PRIMARY KEY,a TEXT)";
      await core.execute(ddl);
      await db.execute(ddl);
      const rows = Array.from({ length: 24 }, (_, id) => ({
        id,
        a: id % 6 === 0 ? null : random() < 0.5 ? "alpha" : "beta",
      }));
      await core.writeBatch("insertBatch", "docs", rows);
      await db.insertBatch("docs", rows);
      const values = (
        await db.query("SELECT id,a,MATCH(a) AGAINST 'alpha' m FROM docs ORDER BY id")
      ).rows;
      await pg.exec("CREATE TABLE expected(id int,a text,m boolean)");
      for (const row of values)
        await pg.query("INSERT INTO expected VALUES($1,$2,$3)", [row.id, row.a, row.m]);
      for (const [native, authority, state] of [
        [
          "SELECT id,JSON_BUILD_OBJECT('m',MATCH(a) AGAINST 'alpha') j FROM docs ORDER BY id",
          "SELECT id,json_build_object('m',m)::text j FROM expected ORDER BY id",
          "nested",
        ],
        [
          "SELECT id,MATCH(a) AGAINST 'alpha' m,JSON_BUILD_OBJECT('a',a) j FROM docs ORDER BY id",
          "SELECT id,m,json_build_object('a',a)::text j FROM expected ORDER BY id",
          "siblings",
        ],
        [
          "SELECT id,CASE WHEN id%2=0 THEN JSON_BUILD_OBJECT('m',MATCH(a) AGAINST 'alpha') ELSE NULL END j FROM docs ORDER BY id",
          "SELECT id,CASE WHEN id%2=0 THEN json_build_object('m',m)::text ELSE NULL END j FROM expected ORDER BY id",
          "CASE demand",
        ],
      ] as const) {
        const actual = await core.execute(native);
        if (!("batches" in actual)) throw Error("query");
        expect(oracleRows(actual), native).toEqual((await pg.query(authority)).rows);
        reach.reach(state);
      }
      await core.execute("CREATE TABLE wide(id INTEGER PRIMARY KEY,a TEXT)");
      const wide = rows.map((row) => ({ ...row, a: (row.a ?? "beta") + " " + "x".repeat(512) }));
      await core.writeBatch("insertBatch", "wide", wide);
      await pg.exec("CREATE TABLE wide_expected(id int,a text,m boolean)");
      for (const row of wide)
        await pg.query("INSERT INTO wide_expected VALUES($1,$2,$3)", [
          row.id,
          row.a,
          row.a.startsWith("alpha"),
        ]);
      const members = Array.from({ length: 32 }, (_, i) => `'k${String(i)}',a`).join(",");
      const sql = `SELECT id,JSON_BUILD_OBJECT(${members},'m',MATCH(a) AGAINST $1) j FROM wide ORDER BY id`;
      const params = ["alpha"],
        create = core.spill.create.bind(core.spill);
      let changed = false,
        live = true;
      core.spill.create = async (queryId) => {
        if (live) {
          changed = true;
          params[0] = "beta";
        }
        return create(queryId);
      };
      try {
        const result = await core.execute(sql, params);
        if (!("batches" in result)) throw Error("wide JSON");
        const expected = (
          await pg.query(
            `SELECT id,json_build_object(${members},'m',m)::text j FROM wide_expected ORDER BY id`,
          )
        ).rows;
        expect(oracleRows(result)).toEqual(expected);
        expect(changed).toBe(true);
        expect(core.lastProfile?.operators.some((op) => op.spillPasses > 0)).toBe(true);
        reach.reach("owned parameters across cut");
      } finally {
        live = false;
        core.spill.create = create;
      }
      expect(core.spill.bytesInUse).toBe(0);
      reach.reach("drained");
      reach.assertReached();
    } finally {
      await core.close();
      await db.close();
      await pg.close();
    }
  }, 60000);
