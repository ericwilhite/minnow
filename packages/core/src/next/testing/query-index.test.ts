/** PostgreSQL SELECT semantics; native indexes and forced scans read the same pinned rows. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, QUERY_INDEX_REACH } from "../../testing/reach.js";
import { operators, withoutIndexes } from "./query-index.test-support.js";
import type { Rel } from "../ir/ir.js";
import type { ParameterValue } from "../exec/executor.js";
import { QueryRuntime } from "../exec/run.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
function firstIndex(rel: Rel): Rel | null {
  if (rel.op === "keyLookup" || rel.op === "indexRange") return rel;
  if ("input" in rel) return firstIndex(rel.input);
  if ("left" in rel) return firstIndex(rel.left) ?? firstIndex(rel.right);
  return null;
}
for (const seed of seedsFor("next-query-index", [47801, 47802]))
  it(`matches indexed/forced-scan/PostgreSQL queries and pinned writes seed${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("query index semantics", QUERY_INDEX_REACH);
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 12, max: 32 }),
          fc.integer({ min: 1, max: 7 }),
          fc.constantFrom("memory", "stored", "reopen"),
          fc.boolean(),
          async (n, first, placement, descending) => {
            reach.beginHistory();
            const core = new StorageCore(placement === "memory" ? 256 : 8);
            Object.assign(core.harness, { governor: new Governor(32 << 20) });
            Object.assign(core.harness.tuning, {
              merges: { mode: "manual", fanIn: 2, sliceEntries: 3 },
            });
            await core.open();
            let forceScan = false,
              direct = false;
            const original = core.context.bind(core);
            core.context = () => {
              const c = original();
              return {
                ...c,
                catalog: forceScan ? withoutIndexes(c.catalog) : c.catalog,
                statistics: { table: () => ({ rowCount: 1000000, column: () => null }) },
              };
            };
            const prepare = core.executor.prepare.bind(core.executor);
            const prepareSpy = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
              if (!direct) return prepare(plan);
              const root = firstIndex(plan.root);
              if (!root) throw Error("Missing direct index root");
              return prepare({ ...plan, root, columns: root.output });
            });
            const all = async (sql: string) => {
              await core.execute(sql);
              await pg.exec(sql);
            };
            const rows = async (sql: string, params: readonly ParameterValue[] = []) => {
              const result = await core.execute(sql, params, AbortSignal.timeout(30000));
              if (!("batches" in result)) throw Error("Rows required");
              return oracleRows(result);
            };
            const check = async (
              sql: string,
              params: ReadonlyArray<number | bigint | string | null>,
              route?: "keyLookup" | "indexRange",
              root = false,
            ) => {
              const expected = (await pg.query(sql, [...params])).rows;
              forceScan = true;
              const scan = core.lowerer.lower(sql, core.context());
              if (scan.kind !== "query") throw Error("query");
              expect(operators(scan.plan.root)).not.toContain("keyLookup");
              expect(operators(scan.plan.root)).not.toContain("indexRange");
              expect(await rows(sql, params)).toEqual(expected);
              forceScan = false;
              const indexed = core.lowerer.lower(sql, core.context());
              if (indexed.kind !== "query") throw Error("query");
              if (route) {
                expect(operators(indexed.plan.root)).toContain(route);
                reach.reach(route);
              }
              direct = root;
              try {
                expect(await rows(sql, params)).toEqual(expected);
              } finally {
                direct = false;
              }
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("forced scan");
              reach.reach("PGlite");
            };
            try {
              for (const table of ["t", "u", "cp"]) await pg.exec(`DROP TABLE IF EXISTS ${table}`);
              await all("CREATE TABLE t(id INT PRIMARY KEY,k TEXT UNIQUE,grp INT,v TEXT)");
              await all("CREATE INDEX t_grp ON t(grp)");
              await all("CREATE TABLE u(id INT PRIMARY KEY,z INT)");
              await all("CREATE TABLE cp(a INT,b TEXT,v INT,PRIMARY KEY(a,b))");
              const key = (i: number) => `k${String(i).padStart(3, "0")}`;
              for (let from = 1; from <= n; from += 8) {
                const ids = Array.from({ length: Math.min(8, n - from + 1) }, (_, i) => from + i);
                await all(
                  `INSERT INTO t VALUES ${ids.map((i) => `(${String(i)},'${key(i)}',${String(i % 4)},'row-${String(i)}')`).join(",")}`,
                );
                await all(
                  `INSERT INTO u VALUES ${ids.map((i) => `(${String(i)},${String(i * 3)})`).join(",")}`,
                );
                await all(
                  `INSERT INTO cp VALUES ${ids.map((i) => `(${String(i % 3)},'${key(i)}',${String(i)})`).join(",")}`,
                );
              }
              if (placement !== "memory") {
                await core.harness.path.recycleLog();
                await core.harness.path.quiesce();
              }
              if (placement === "reopen") await core.reopen();
              reach.reach(placement);
              await check("SELECT id,k,grp,v FROM t WHERE id=$1", [first], "keyLookup", true);
              reach.reach("root");
              await check(
                "SELECT id,k,grp,v FROM t WHERE id>=$1 AND id<$2",
                [first, first + 1],
                "indexRange",
                true,
              );
              reach.reach("range root");
              await check("SELECT id,v FROM t WHERE id=$1", [first], "keyLookup");
              reach.reach("projection");
              await check("SELECT id FROM t WHERE id=$1", [null], "keyLookup");
              reach.reach("NULL key");
              await check("SELECT id FROM t WHERE id=$1::SMALLINT", [first], "keyLookup");
              reach.reach("coerced key");
              await check("SELECT id FROM t WHERE id=$1::BIGINT", [5000000000n]);
              const widened = core.lowerer.lower(
                "SELECT id FROM t WHERE id=$1::BIGINT",
                core.context(),
              );
              if (widened.kind !== "query") throw Error("query");
              expect(operators(widened.plan.root)).not.toContain("keyLookup");
              reach.reach("widening scan");
              await check(
                "SELECT 100/(id-3) v FROM t WHERE id>=1 AND id<8 LIMIT 1",
                [],
                "indexRange",
              );
              reach.reach("limit early stop");
              await check(
                "SELECT id,v FROM t WHERE id IN($1,$2,$3,$4) ORDER BY id",
                [first, first, null, first + 1],
                "keyLookup",
              );
              reach.reach("IN duplicates");
              await check(
                "SELECT id,v FROM t WHERE id IN($1,$2,$3,$4) ORDER BY id",
                [first, first + 1, null, first],
                "keyLookup",
              );
              reach.reach("nonadjacent duplicates");
              await check(
                "SELECT id FROM t WHERE k IN($1,$2,$3) ORDER BY id",
                [key(first), key(first), key(first + 1)],
                "keyLookup",
              );
              reach.reach("text key");
              await check(
                "SELECT id,v FROM t WHERE id=$1 AND v<>$2",
                [first, "absent"],
                "keyLookup",
              );
              reach.reach("filter");
              await check(
                `SELECT id,v FROM t WHERE grp>=$1 AND grp<$2 ORDER BY v ${descending ? "DESC" : "ASC"},id LIMIT $3 OFFSET $4`,
                [1, 3, 4, 1],
                "indexRange",
              );
              reach.reach("sort");
              reach.reach("limit/offset");
              reach.reach(descending ? "descending" : "ascending");
              await check("SELECT id FROM t WHERE grp>=$1 AND grp<$2", [null, 3], "indexRange");
              reach.reach("NULL bound");
              await check(
                "SELECT id FROM t WHERE k>=$1 AND k<$2 ORDER BY id",
                [key(2), key(10)],
                "indexRange",
              );
              await check(
                "SELECT t.id,t.v,u.z FROM t JOIN u ON t.id=u.id WHERE t.id IN($1,$2,$3) AND u.id IN($1,$2,$3) ORDER BY t.id LIMIT $4",
                [first, first, first + 1, 8],
                "keyLookup",
              );
              reach.reach("join");
              await check(
                "SELECT t.id,u.z FROM t JOIN u ON t.id=u.id WHERE t.grp>=1 AND t.grp<3 AND u.id>=1 AND u.id<10 ORDER BY t.id",
                [],
                "indexRange",
              );
              reach.reach("simultaneous sources");
              const setQuery =
                "SELECT id FROM t WHERE id=$1 EXCEPT SELECT id FROM u WHERE id>=$2 AND id<$3 ORDER BY id";
              await check(setQuery, [first, 1, 4], "keyLookup");
              const setPlan = core.lowerer.lower(setQuery, core.context());
              if (setPlan.kind !== "query") throw Error("query");
              expect(operators(setPlan.plan.root)).toContain("indexRange");
              reach.reach("mixed serial set inputs");
              await check(
                "SELECT a,b,v FROM cp WHERE a=$1 AND b=$2",
                [first % 3, key(first)],
                "keyLookup",
              );
              reach.reach("composite point");
              await check(
                "SELECT a,b,v FROM cp WHERE a=$1 AND b>=$2 AND b<$3 ORDER BY b",
                [1, key(1), key(n)],
                "indexRange",
              );
              reach.reach("composite range");
              await check("SELECT a,b,v FROM cp WHERE a=$1 ORDER BY b", [1], "indexRange");
              reach.reach("composite prefix");
              await check("SELECT id FROM t WHERE id=$1 LIMIT 0", [first], "keyLookup");
              reach.reach("zero limit");
              // Pause before whole-query admission. The view is already pinned; committed
              // writes/publication must neither widen its cursor nor change its visible rows.
              const sql = "SELECT id,v FROM t WHERE id IN($1,$2) ORDER BY id",
                params = [first, first + 1];
              const expected = (await pg.query(sql, params)).rows;
              forceScan = true;
              expect(await rows(sql, params)).toEqual(expected);
              forceScan = false;
              let enter!: () => void, resume!: () => void;
              const entered = new Promise<void>((r) => (enter = r)),
                gate = new Promise<void>((r) => (resume = r));
              // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with its actual runtime below.
              const reserve = QueryRuntime.prototype.reserve;
              let armed = true;
              const pause = vi
                .spyOn(QueryRuntime.prototype, "reserve")
                .mockImplementation(function (this: QueryRuntime, entries) {
                  if (armed && this.plan.root.op !== "modify") {
                    armed = false;
                    enter();
                    return gate.then(() => reserve.call(this, entries));
                  }
                  return reserve.call(this, entries);
                });
              const pending = rows(sql, params);
              void pending.catch(() => undefined);
              try {
                await entered;
                for (const mutation of [
                  `UPDATE t SET v='changed' WHERE id=${String(first)}`,
                  `DELETE FROM t WHERE id=${String(first + 1)}`,
                  `INSERT INTO t VALUES(${String(n + 1)},'${key(n + 1)}',1,'new')`,
                ])
                  await all(mutation);
                await core.harness.path.quiesce();
                resume();
                expect(await pending).toEqual(expected);
              } finally {
                resume();
                pause.mockRestore();
                await pending.catch(() => undefined);
              }
              reach.reach("pinned concurrent writes");
              reach.reach("concurrent publication");
              await core.reopen();
              await check(
                "SELECT id,v FROM t WHERE id IN($1,$2,$3) ORDER BY id",
                [first, first + 1, n + 1],
                "keyLookup",
              );
              reach.reach("reopen after writes");
            } finally {
              prepareSpy.mockRestore();
              await core.close();
              expect(core.harness.governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero charge");
            }
          },
        ),
        {
          seed,
          numRuns: 9,
          endOnFailure: true,
          examples: [
            [16, 2, "memory", false],
            [20, 3, "stored", true],
            [24, 4, "reopen", false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 180000);
