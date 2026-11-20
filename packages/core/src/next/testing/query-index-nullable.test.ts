/** PostgreSQL predicates must retain rows that a NULL-omitting composite index cannot cover. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, QUERY_INDEX_NULLABLE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { operators, withoutIndexes } from "./query-index.test-support.js";

for (const seed of seedsFor("next-query-index-nullable", [47809, 47810]))
  it(`covers nullable composite keys with scans or proven non-null index ranges seed${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("nullable composite query indexes", QUERY_INDEX_NULLABLE_REACH);
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 12, max: 24 }),
          fc.boolean(),
          fc.constantFrom("memory", "stored", "reopen"),
          async (n, nullable, placement) => {
            reach.beginHistory();
            const core = new StorageCore(placement === "memory" ? 256 : 8),
              governor = new Governor(32 << 20);
            Object.assign(core.harness, { governor });
            await core.open();
            let scan = false;
            const context = core.context.bind(core);
            core.context = () => {
              const c = context();
              return {
                ...c,
                catalog: scan ? withoutIndexes(c.catalog) : c.catalog,
                statistics: { table: () => ({ rowCount: 1000000, column: () => null }) },
              };
            };
            const execute = async (sql: string, params: ReadonlyArray<number | null>) => {
              const result = await core.execute(sql, params, AbortSignal.timeout(30000));
              if (!("batches" in result)) throw Error("query rows required");
              return oracleRows(result);
            };
            const check = async (
              sql: string,
              params: ReadonlyArray<number | null>,
              indexed: boolean,
            ) => {
              const expected = (await pg.query(sql, [...params])).rows;
              scan = true;
              expect(await execute(sql, params)).toEqual(expected);
              scan = false;
              const lowered = core.lowerer.lower(sql, core.context());
              if (lowered.kind !== "query") throw Error("query");
              const path = operators(lowered.plan.root);
              expect(await execute(sql, params)).toEqual(expected);
              expect(path.includes("indexRange") || path.includes("keyLookup")).toBe(indexed);
              reach.reach(indexed ? "safe index" : "scan fallback");
              reach.reach("forced scan");
              reach.reach("PGlite");
            };
            try {
              await pg.exec("DROP TABLE IF EXISTS np");
              const ddl = `CREATE TABLE np(id INT PRIMARY KEY,a INT,b INT ${nullable ? "" : "NOT NULL"},c INT ${nullable ? "" : "NOT NULL"},v INT)`;
              for (const sql of [ddl, "CREATE UNIQUE INDEX np_abc ON np(a,b,c)"]) {
                await core.execute(sql);
                await pg.exec(sql);
              }
              for (let start = 1; start <= n; start += 8) {
                const tuples = Array.from({ length: Math.min(8, n - start + 1) }, (_, k) => {
                  const id = start + k,
                    a = nullable && id === n ? "NULL" : String(1 + ((id - 1) % 3)),
                    b = nullable && (id === 1 || id % 4 === 0) ? "NULL" : String(id),
                    c = nullable && (id === 2 || id % 5 === 0) ? "NULL" : String(id % 4);
                  return `(${String(id)},${a},${b},${c},${String(id)})`;
                });
                const sql = `INSERT INTO np VALUES ${tuples.join(",")}`;
                await core.execute(sql);
                await pg.exec(sql);
              }
              if (placement !== "memory") {
                await core.harness.path.quiesce();
                await core.harness.path.recycleLog();
              }
              if (placement === "reopen") await core.reopen();
              reach.reach(placement);
              reach.reach(nullable ? "nullable suffix" : "declared nonnull suffix");
              await check("SELECT id,v FROM np WHERE a=$1 ORDER BY id", [1], !nullable);
              await check("SELECT id FROM np WHERE a>=$1 AND a<$2 ORDER BY id", [1, 4], !nullable);
              await check("SELECT id FROM np WHERE a=$1 AND b=$2 ORDER BY id", [2, 2], !nullable);
              reach.reach("partial prefixes");
              await check(
                "SELECT id FROM np WHERE a=$1 AND b=$2 AND c=$3 ORDER BY id",
                [3, 3, 3],
                true,
              );
              await check(
                "SELECT id FROM np WHERE a=$1 AND b=$2 AND c=$3 ORDER BY id",
                [1, null, 1],
                true,
              );
              reach.reach("complete point and null argument");
              await check(
                "SELECT id FROM np WHERE a=$1 AND b>=$2 AND c=$3 ORDER BY id",
                [3, 1, 3],
                true,
              );
              await check(
                "SELECT id FROM np WHERE a>=$1 AND a<$2 AND b>=$3 AND c>=$4 ORDER BY id",
                [1, 4, 1, 1],
                true,
              );
              reach.reach("strict residual proof");
              await check(
                "SELECT id FROM np WHERE a=$1 AND b IN($2,$3) AND c=$4 ORDER BY id",
                [3, 3, 7, 3],
                true,
              );
              reach.reach("IN proof");
              await check(
                "SELECT id FROM np WHERE a=$1 AND b IS NOT NULL AND c IS NOT NULL ORDER BY id",
                [1],
                true,
              );
              reach.reach("IS NOT NULL proof");
              await check(
                "SELECT id FROM np WHERE a=$1 AND b IS NULL AND c=$2 ORDER BY id",
                [1, 1],
                !nullable,
              );
              await check(
                "SELECT id FROM np WHERE a=$1 AND b IS NOT DISTINCT FROM $2 AND c=$3 ORDER BY id",
                [1, null, 1],
                !nullable,
              );
              await check(
                "SELECT id FROM np WHERE a=$1 AND (b>=$2 OR b IS NULL) AND c=$3 ORDER BY id",
                [1, 1, 1],
                !nullable,
              );
              reach.reach("no proof from NULL-safe or OR");
              await check("SELECT id FROM np WHERE b=$1 ORDER BY id", [n], false);
              if (nullable) reach.reach("null first component");
              for (const sql of [
                "UPDATE np SET v=v+1 WHERE a=$1 RETURNING id,v",
                "DELETE FROM np WHERE a=$1 RETURNING id",
              ]) {
                const actual = await core.execute(sql, [1], AbortSignal.timeout(30000));
                if (!("returning" in actual)) throw Error("returning required");
                const expected = (await pg.query(sql, [1])).rows;
                const order = (rows: readonly unknown[]) =>
                  [...rows].sort(
                    (a, b) =>
                      Number((a as Record<string, unknown>).id) -
                      Number((b as Record<string, unknown>).id),
                  );
                expect(order(oracleRows(actual.returning))).toEqual(order(expected));
                expect(actual.rowCount).toBe(expected.length);
              }
              reach.reach("UPDATE and DELETE targets");
              expect(await execute("SELECT id,v FROM np ORDER BY id", [])).toEqual(
                (await pg.query("SELECT id,v FROM np ORDER BY id")).rows,
              );
            } finally {
              await core.close();
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero ownership");
            }
          },
        ),
        {
          seed,
          numRuns: 6,
          endOnFailure: true,
          examples: [
            [12, true, "memory"],
            [16, false, "stored"],
            [20, true, "reopen"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 180000);
