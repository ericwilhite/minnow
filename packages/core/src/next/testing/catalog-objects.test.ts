/** PG object dependency, replacement, quoted identifier and durable sequence controls. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_OBJECTS_REACH, createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const failed = async (work: () => Promise<unknown>) => {
  try {
    await work();
    return null;
  } catch (error) {
    return error && typeof error === "object"
      ? "sqlState" in error
        ? error.sqlState
        : "code" in error
          ? error.code
          : error
      : error;
  }
};
for (const seed of seedsFor("next-catalog-objects", [0xb78a, 0xb78b]))
  it(`keeps object identities and dependency rules seed ${String(seed)}`, async () => {
    const reach = createReach("catalog object DDL", CATALOG_OBJECTS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("stage2_mood", "Case", "a.b", 'a"b', "INTEGER", "integer"),
        fc.integer({ min: 1, max: 1000 }),
        fc.boolean(),
        fc.boolean(),
        async (name, restart, called, checkpoint) => {
          reach.beginHistory();
          const pg = new PGlite(),
            core = await new StorageCore().open();
          try {
            const execute = async (sql: string) => {
              await pg.exec(sql);
              await core.execute(sql);
            };
            await execute(`CREATE TYPE ${quote(name)} AS ENUM ('z','a','汉')`);
            const value = `'z'::${quote(name)}`;
            await execute(`CREATE VIEW v AS SELECT ${value} AS "a.b"`);
            const first = core.harness.path.catalog.views[0]?.id;
            await execute(`CREATE OR REPLACE VIEW v AS SELECT ${value} AS "a.b", 1 AS "Case"`);
            expect(core.harness.path.catalog.views[0]?.id).toBe(first);
            reach.reach("replace identity");
            await execute("CREATE VIEW dependent AS SELECT * FROM v");
            const drop = "DROP VIEW v";
            expect(await failed(() => core.execute(drop))).toBe(await failed(() => pg.exec(drop)));
            reach.reach("view drop dependency");
            const wrong = `CREATE OR REPLACE VIEW v AS SELECT ${value} AS wrong_name`;
            expect(await failed(() => core.execute(wrong))).toBe(
              await failed(() => pg.exec(wrong)),
            );
            reach.reach("output compatibility");
            const duplicate = "CREATE VIEW duplicate_output AS SELECT 1 AS a,2 AS a";
            expect(await failed(() => core.execute(duplicate))).toBe(
              await failed(() => pg.exec(duplicate)),
            );
            reach.reach("duplicate output");
            const typeDrop = `DROP TYPE ${quote(name)}`;
            expect(await failed(() => core.execute(typeDrop))).toBe(
              await failed(() => pg.exec(typeDrop)),
            );
            reach.reach("type dependency");
            const select = "SELECT * FROM dependent",
              result = await core.execute(select);
            if (!("batches" in result)) throw new Error("Missing view rows");
            expect(oracleRows(result)).toEqual((await pg.query(select)).rows);
            reach.reach("quoted view output");
            await execute("DROP VIEW dependent");
            await execute("DROP VIEW v");
            await execute(typeDrop);
            await execute(`DROP TYPE IF EXISTS ${quote(name)}`);
            reach.reach("drop type");
            await execute("CREATE SEQUENCE s");
            await execute("CREATE VIEW seqv AS SELECT nextval('s') AS n");
            expect(await failed(() => core.execute("DROP SEQUENCE s"))).toBe(
              await failed(() => pg.exec("DROP SEQUENCE s")),
            );
            reach.reach("sequence dependency");
            await execute("DROP VIEW seqv");
            const id = core.harness.path.catalog.sequences.find(
              (sequence) => sequence.name === "s",
            )?.id;
            if (!id) throw new Error("Missing sequence");
            await core.harness.path.sequences.set(id, BigInt(restart), called);
            await pg.query("SELECT setval('s',$1,$2)", [restart, called]);
            if (checkpoint) {
              await core.harness.path.quiesce();
              reach.reach("checkpoint restart");
            } else reach.reach("WAL restart");
            await core.reopen();
            const out = { repr: "int64", values: new BigInt64Array(1) } as const;
            await core.harness.path.sequences.next(id, 1, out, 0);
            expect(Number(out.values[0])).toBe(
              (await pg.query<{ n: number }>("SELECT nextval('s') AS n")).rows[0]?.n,
            );
            reach.reach(called ? "called restart" : "uncalled restart");
            await execute("DROP SEQUENCE s");
            await execute("DROP SEQUENCE IF EXISTS s");
            reach.reach("drop sequence");
          } finally {
            await core.close();
            await pg.close();
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          ["a.b", 4, false, false],
          ["Case", 7, true, true],
          ["INTEGER", 4, false, false],
          ["integer", 7, true, true],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);
