import fc from "fast-check";
import { Kysely, sql } from "kysely";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../../core/dist/next/owner/database.js";
import { MemoryBackend } from "../../../core/dist/next/storage/memory-store.js";
import { SimLocks } from "../../../core/dist/next/storage/sim-disk.js";
import { seedsFor } from "../../../core/dist/testing/seeds.js";
import { createReach, NATIVE_KYSELY_REACH } from "../../../core/dist/testing/reach.js";
import { NativeMinnowDialect, nativeLiveQuery } from "./index.js";
interface DB {
  t: { id: number; n: bigint; v: string | null };
}
for (const seed of seedsFor("next-native-kysely", [46956, 46957]))
  it(`executes canonical SQL and isolated Kysely sessions (${String(seed)})`, async () => {
    const reach = createReach("native Kysely", NATIVE_KYSELY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.bigInt({ min: -(1n << 60n), max: 1n << 60n }),
        fc.option(
          fc.string({ maxLength: 24 }).map((x) => x.replaceAll("\0", "�")),
          { nil: null },
        ),
        fc.integer({ min: 1, max: 3 }),
        async (n, v, chunk) => {
          reach.beginHistory();
          const native = await MinnowDatabase.open({
            backend: new MemoryBackend({ locks: new SimLocks() }),
            memoryLimitBytes: 64 << 20,
          });
          const db = new Kysely<DB>({ dialect: new NativeMinnowDialect({ driver: native }) });
          try {
            await db.schema
              .createTable("t")
              .addColumn("id", "integer", (c) => c.primaryKey())
              .addColumn("n", "bigint")
              .addColumn("v", "text")
              .execute();
            await db
              .insertInto("t")
              .values([
                { id: 1, n, v },
                { id: 2, n: n + 1n, v: null },
                { id: 3, n: n - 1n, v: "" },
              ])
              .execute();
            const expected = [
              { id: 1, n, v },
              { id: 2, n: n + 1n, v: null },
              { id: 3, n: n - 1n, v: "" },
            ];
            expect(await db.selectFrom("t").selectAll().orderBy("id").execute()).toEqual(expected);
            reach.reach("exact int8");
            reach.reach("nullable text");
            expect((await sql`SELECT 1 x,2 x`.execute(db)).rows).toEqual([{ x: 2 }]);
            const precise = await sql<{
              n: string;
              t: string;
            }>`select 12.3400::numeric n,TIMESTAMP '2000-02-29 12:34:56.123456' t`.execute(db);
            expect(precise.rows).toEqual([{ n: "12.3400", t: "2000-02-29T12:34:56.123456" }]);
            reach.reach("NUMERIC scale");
            reach.reach("timestamp microseconds");
            const streamed = [];
            for await (const row of db.selectFrom("t").selectAll().orderBy("id").stream(chunk))
              streamed.push(row);
            expect(streamed).toEqual(expected);
            reach.reach("bounded stream");
            for await (const row of db.selectFrom("t").selectAll().orderBy("id").stream(chunk)) {
              expect(row.id).toBe(1);
              break;
            }
            reach.reach("early cursor close");
            const failure = new Error("body");
            await expect(
              db.transaction().execute(async (tx) => {
                await tx.insertInto("t").values({ id: 4, n, v }).execute();
                expect(await tx.selectFrom("t").select("id").orderBy("id").execute()).toEqual([
                  { id: 1 },
                  { id: 2 },
                  { id: 3 },
                  { id: 4 },
                ]);
                expect((await native.query("SELECT id FROM t ORDER BY id")).tuples()).toEqual([
                  [1],
                  [2],
                  [3],
                ]);
                reach.reach("isolated callback");
                throw failure;
              }),
            ).rejects.toBe(failure);
            expect(await db.selectFrom("t").selectAll().orderBy("id").execute()).toEqual(expected);
            reach.reach("rollback");
            const controlled = await db.startTransaction().execute();
            const point = await controlled.savepoint("point").execute();
            await point.insertInto("t").values({ id: 4, n, v }).execute();
            await point.rollbackToSavepoint("point").execute();
            await controlled.commit().execute();
            expect(await db.selectFrom("t").selectAll().orderBy("id").execute()).toEqual(expected);
            reach.reach("savepoint recovery");
            const table = (await db.introspection.getTables()).find((t) => t.name === "t");
            expect(table?.columns.map((c) => c.dataType)).toEqual(["integer", "bigint", "text"]);
            reach.reach("catalog SQL widths");
            const decoded = new Kysely<DB>({
              dialect: new NativeMinnowDialect({
                driver: native,
                resultDecoding: { numeric: "number", json: "parse" },
              }),
            });
            try {
              expect(
                (
                  await sql`select 12.3400::numeric n, 'null'::json j, '{"a":1}'::jsonb b`.execute(
                    decoded,
                  )
                ).rows,
              ).toEqual([{ n: 12.34, j: null, b: { a: 1 } }]);
              expect((await sql`SELECT 1::numeric x,2::numeric x`.execute(decoded)).rows).toEqual([
                { x: 2 },
              ]);
            } finally {
              await decoded.destroy();
            }
            reach.reach("explicit decoding");
            const live = nativeLiveQuery({ driver: native, db });
            const window = live.window(db.selectFrom("t").selectAll().orderBy("id"), {
              key: "id",
              limit: 2,
            });
            await window.refresh();
            expect(window.getSnapshot().rows).toEqual(expected.slice(0, 2));
            await live.close();
            reach.reach("live bound");
          } finally {
            await db.destroy();
            await native.close();
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [9007199254740993n, null, 1],
          [-9007199254740993n, "水", 2],
          [0n, "", 3],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
