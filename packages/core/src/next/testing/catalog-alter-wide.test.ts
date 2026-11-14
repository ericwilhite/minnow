/** Small and multipart ALTER commits carry the same schema transition and frozen bytes. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_ALTER_WIDE_REACH, createReach } from "../../testing/reach.js";
import { catalogChangeByteLength } from "../write/catalog-stream.js";
import { SMALL_COMMIT_BYTES } from "../write/commit-buffers.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
for (const seed of seedsFor("next-catalog-alter-wide", [0xb78e, 0xb78f]))
  it(`replays large frozen missing values seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("wide ALTER WAL", CATALOG_ALTER_WIDE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 65536, max: 131072 }), async (width) => {
          reach.beginHistory();
          const core = await new StorageCore(4).open();
          try {
            await pg.exec("DROP TABLE IF EXISTS t");
            await pg.exec("CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1),(2)");
            await core.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)");
            await core.writeBatch("insertBatch", "t", [{ id: 1 }, { id: 2 }]);
            const text = "汉".repeat(width),
              sql = `ALTER TABLE t ADD COLUMN value TEXT NOT NULL DEFAULT '${text}'`;
            const lowered = core.lowerer.lower(sql, core.context());
            if (lowered.kind !== "ddl" || !lowered.change) throw new Error("Missing wide ALTER");
            const current = core.harness.path.catalog,
              logged = current.change(lowered.change, current.version + 1).logged[0];
            if (!logged) throw new Error("Missing logged ALTER");
            reach.reach(
              catalogChangeByteLength(logged) > SMALL_COMMIT_BYTES ? "multipart" : "small commit",
            );
            await pg.exec(sql);
            await core.execute(sql);
            await pg.exec("INSERT INTO t VALUES (3,'suffix')");
            await core.writeBatch("insertBatch", "t", [{ id: 3, value: "suffix" }]);
            await core.reopen();
            const query = "SELECT * FROM t ORDER BY id",
              result = await core.execute(query);
            if (!("batches" in result)) throw new Error("Missing wide recovered rows");
            expect(oracleRows(result)).toEqual((await pg.query(query)).rows);
            reach.reach("exact backfill");
            reach.reach("new-schema rows");
          } finally {
            await core.close();
          }
        }),
        { seed, numRuns: 6, examples: [[65536], [131072]] },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
