/** PostgreSQL UPDATE/DELETE statement snapshots and atomic statement failure.
 * https://www.postgresql.org/docs/current/sql-update.html
 * https://www.postgresql.org/docs/current/sql-insert.html
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, GENERAL_MODIFY_REACH } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { StorageCore } from "./storage-core.js";
import { ChunkReader } from "../page/reader.js";
import { rowsOf, rules } from "../write/test-support.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-general-modify", [0xb5b1, 0xb5b2]))(
  "executes snapshot modifications atomically against PostgreSQL and shipped (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    await pg.waitReady;
    const reach = createReach("general modify", GENERAL_MODIFY_REACH);
    const all = async (sql: string) => {
      await core.execute(sql);
      await pg.exec(sql);
      await shipped.run({ kind: "sql", sql });
    };
    const compare = async () => {
      const sql = "SELECT * FROM dml ORDER BY id";
      const next = await core.execute(sql);
      if (next.kind !== "rows" || !("batches" in next)) throw new Error("rows");
      const rows = oracleRows(next);
      expect(rows).toEqual((await pg.query(sql)).rows);
      const old = await shipped.run({ kind: "sql", sql });
      expect(old.rows).toEqual(rows);
    };
    try {
      await all("CREATE TABLE dml (id INTEGER PRIMARY KEY, v INTEGER UNIQUE)");
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 12 }),
          fc.integer({ min: 1, max: 5 }),
          async (n, delta) => {
            reach.beginHistory();
            await all("DROP TABLE dml");
            await all("CREATE TABLE dml (id INTEGER PRIMARY KEY, v INTEGER UNIQUE)");
            await all(
              `INSERT INTO dml VALUES ${Array.from({ length: n }, (_, i) => `(${String(i + 1)},${String((i + 1) * 10)})`).join(",")}`,
            );
            reach.reach("multi-row VALUES");
            await all("INSERT INTO dml SELECT id + 20000, v + 20000 FROM dml LIMIT 0");
            reach.reach("empty producer");
            await all(`UPDATE dml SET v = v + ${String(delta)} WHERE id % 2 = 0`);
            reach.reach("expression update");
            await compare();
            await all(
              `INSERT INTO dml SELECT id + 100, v + 1000 FROM dml WHERE id <= ${String(delta)}`,
            );
            reach.reach("INSERT SELECT snapshot");
            await compare();
            await all(`UPDATE dml SET id = id + 1000 WHERE id < 1000`);
            reach.reach("primary key update snapshot");
            await compare();
            // The first row writes successfully; a later row violates the primary key.
            const tx = await core.harness.path.begin();
            try {
              const statement = tx.statement({ rules });
              const lowered = core.lowerer.lower(
                "INSERT INTO dml VALUES (9999,9999),(1001,8888)",
                core.context(),
              );
              if (lowered.kind !== "modify") throw new Error("modify");
              const execution = core.executor.prepare(lowered.plan).execute({
                snapshot: null,
                statement,
                parameters: [],
                statementTime: 0n,
                governor: core.harness.governor,
                spill: core.spill,
                queryId: 999,
                reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
                sequences: null,
                sequenceSession: null,
                returning: "atExecuted",
              });
              await expect(execution.executed).rejects.toMatchObject({ sqlState: "23505" });
              await execution.delivered.catch(() => undefined);
              // The transaction stays usable; owner rollback cannot mask a missing statement rollback.
              const probe = tx.statement({ rules });
              const table = probe.catalog.tableByName("dml");
              if (!table) throw new Error("table");
              expect((await rowsOf(probe.view, table.id)).some((row) => row[0] === 9999)).toBe(
                false,
              );
              await probe.end();
              const receipt = await tx.commit(null);
              await receipt.durable;
            } catch (error) {
              tx.rollback();
              throw error;
            }
            reach.reach("failure rollback");
            await compare();
            await all(`DELETE FROM dml WHERE v % 2 = ${String(delta % 2)}`);
            reach.reach("predicate delete");
            await compare();
          },
        ),
        { seed, numRuns: 12 },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await shipped.close();
      await pg.close();
    }
  },
  60000,
);

it.each(["before", "after"])("handles cancellation %s statement completion", async (when) => {
  const core = await new StorageCore().open();
  try {
    await core.execute("CREATE TABLE cancel_dml (id INTEGER PRIMARY KEY)");
    const tx = await core.harness.path.begin();
    try {
      const real = tx.statement({ rules });
      let resume!: () => void, entered!: () => void;
      const gate = new Promise<void>((resolve) => {
        resume = resolve;
      });
      const ending = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const statement = {
        ordinal: real.ordinal,
        view: real.view,
        readOwn: real.readOwn.bind(real),
        catalog: real.catalog,
        table: real.table.bind(real),
        changeCatalog: real.changeCatalog.bind(real),
        rollback: real.rollback.bind(real),
        end: async (signal?: AbortSignal) => {
          entered();
          await gate;
          await real.end(signal);
          if (when === "after") execution.cancel("cancel completed checks");
        },
      };
      const lowered = core.lowerer.lower("INSERT INTO cancel_dml VALUES (1)", core.context());
      if (lowered.kind !== "modify") throw new Error("modify");
      const execution = core.executor.prepare(lowered.plan).execute({
        snapshot: null,
        statement,
        parameters: [],
        statementTime: 0n,
        governor: core.harness.governor,
        spill: core.spill,
        queryId: 1000,
        reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
        sequences: null,
        sequenceSession: null,
        returning: "atExecuted",
      });
      await ending;
      if (when === "before") execution.cancel("cancel deferred checks");
      resume();
      if (when === "before")
        await expect(execution.executed).rejects.toMatchObject({ sqlState: "57014" });
      else expect((await execution.executed).rowCount).toBe(1);
      await execution.delivered.catch(() => undefined);
      const probe = tx.statement({ rules });
      const table = probe.catalog.tableByName("cancel_dml");
      if (!table) throw new Error("table");
      expect(await rowsOf(probe.view, table.id)).toEqual(when === "before" ? [] : [[1]]);
      await probe.end();
    } finally {
      tx.rollback();
    }
  } finally {
    await core.close();
  }
});
