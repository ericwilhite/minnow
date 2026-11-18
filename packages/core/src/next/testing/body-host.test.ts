/** Real compiled body inputs, row rules, accepted images and native writes under the B6
 * controller. SQL dispatch remains disabled until the joint ordering/FK campaign passes. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_BODY_ERROR_REACH,
  NATIVE_BODY_HOST_REACH,
} from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { referentialFixture } from "./referential.test-support.js";
function state(error: unknown): string | null {
  if (error === null) return null;
  if (typeof error === "object" && "sqlState" in error && typeof error.sqlState === "string")
    return error.sqlState;
  if (typeof error === "object" && "code" in error && typeof error.code === "string")
    return error.code;
  throw new Error("Unexpected body failure", { cause: error });
}
async function code(body: () => Promise<unknown>): Promise<string | null> {
  try {
    await body();
    return null;
  } catch (error) {
    return state(error);
  }
}
for (const seed of seedsFor("next-native-body-host", [0xb65b, 0xb65c]))
  it(`runs complete compiled root and bodies against PG seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("compiled native body host", NATIVE_BODY_HOST_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 3 }),
          fc.integer({ min: 0, max: 12 }),
          fc.integer({ min: 1, max: 100 }),
          fc.boolean(),
          fc.boolean(),
          async (kind, count, n, published, fail) => {
            reach.beginHistory();
            const setup = [
              "CREATE TABLE p(id INT PRIMARY KEY,v INT CHECK(v>0),g INT GENERATED ALWAYS AS(v*2) STORED)",
              "CREATE TABLE c(id INT PRIMARY KEY,v INT CHECK(v>0))",
              "CREATE TABLE audit(id INT,v INT)",
              ...(count
                ? [
                    `INSERT INTO p(id,v) VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(n)})`).join(",")}`,
                    `INSERT INTO c VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},1)`).join(",")}`,
                  ]
                : []),
            ];
            const event = kind === 0 ? "INSERT" : kind === 1 ? "UPDATE" : "DELETE",
              side = event === "DELETE" ? "OLD" : "NEW";
            const first =
              kind === 3
                ? "UPDATE c SET v=v+1 WHERE id=OLD.id"
                : `INSERT INTO audit VALUES(${side}.id,${side}.v)`;
            const second =
              kind === 3
                ? "INSERT INTO audit SELECT id,v FROM c"
                : `DELETE FROM c WHERE id=${side}.id`;
            // The shipped body grammar uses VALUES only. Its later statement can read the prefix
            // through UPDATE expressions/predicates, then append its result via a nested trigger.
            const body =
              kind === 3 ? `${first}; UPDATE c SET v=v+1 WHERE id=OLD.id` : `${first}; ${second}`;
            const native = [...setup, `CREATE TRIGGER log AFTER ${event} ON p BEGIN ${body}; END`];
            if (kind === 3)
              native.push(
                "CREATE TRIGGER child_log AFTER UPDATE ON c BEGIN INSERT INTO audit VALUES(NEW.id,NEW.v); END",
              );
            const mutation =
              kind === 0
                ? `INSERT INTO p(id,v) VALUES(99,${String(fail ? -1 : n)})`
                : kind === 1
                  ? `UPDATE p SET v=${fail ? "-1" : "v+1"}`
                  : "DELETE FROM p";
            const f = await referentialFixture(native, published, null, 256 << 20, mutation);
            try {
              await pg.exec("DROP TABLE IF EXISTS audit,c,p CASCADE");
              for (const sql of setup) await pg.exec(sql);
              await pg.exec(
                `CREATE OR REPLACE FUNCTION body_log() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body}; RETURN ${event === "DELETE" ? "OLD" : "NEW"}; END $$; CREATE TRIGGER log AFTER ${event} ON p FOR EACH ROW EXECUTE FUNCTION body_log()`,
              );
              if (kind === 3)
                await pg.exec(
                  "CREATE OR REPLACE FUNCTION child_body_log() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit VALUES(NEW.id,NEW.v); RETURN NEW; END $$; CREATE TRIGGER child_log AFTER UPDATE ON c FOR EACH ROW EXECUTE FUNCTION child_body_log()",
                );
              const expected = await code(() => pg.exec(mutation));
              const actual = await code(async () => {
                await f.nativeRoot();
                await f.end();
              });
              expect(actual).toBe(expected);
              if (actual) await f.rollback();
              await f.retire();
              for (const table of ["p", "c", "audit"]) {
                const sql = `SELECT * FROM ${table} ORDER BY id,v`;
                const observed = await f.core.execute(sql);
                if (!("batches" in observed)) throw new Error("rows");
                expect(oracleRows(observed)).toEqual((await pg.query(sql)).rows);
              }
              reach.reach(event);
              reach.reach(published ? "published" : "resident");
              reach.reach(count === 0 ? "zero rows" : "rowset");
              if (kind === 3) reach.reach("later body prefix");
              if (actual) reach.reach("rollback");
              else reach.reach("success");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        { seed, numRuns: 40 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);

for (const seed of seedsFor("next-native-body-error-order", [0xb65d, 0xb65e]))
  it(`keeps earlier body CHECK before a later source error seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native body error order", NATIVE_BODY_ERROR_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 23 }),
          fc.boolean(),
          fc.boolean(),
          async (n, failCheck, published) => {
            reach.beginHistory();
            const setup = [
                "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
                `CREATE TABLE c(id INT PRIMARY KEY,den INT,v INT CHECK(v${failCheck ? "<" : ">"}0))`,
                `INSERT INTO c VALUES(1,${String(n)},${failCheck ? "-1" : "1"}),(2,0,${failCheck ? "-1" : "1"})`,
                `INSERT INTO p VALUES(1,${String(n)})`,
              ],
              body = "UPDATE c SET v=100/den",
              mutation = "UPDATE p SET v=v+1",
              f = await referentialFixture(
                [...setup, `CREATE TRIGGER body BEFORE UPDATE ON p BEGIN ${body}; END`],
                published,
                null,
                256 << 20,
                mutation,
              );
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p CASCADE");
              for (const sql of setup) await pg.exec(sql);
              await pg.exec(
                `CREATE OR REPLACE FUNCTION body_error() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body}; RETURN NEW; END $$; CREATE TRIGGER body BEFORE UPDATE ON p FOR EACH ROW EXECUTE FUNCTION body_error()`,
              );
              const expected = await code(() => pg.exec(mutation)),
                actual = await code(async () => {
                  await f.nativeRoot();
                  await f.end();
                });
              expect(expected).toBe(failCheck ? "23514" : "22012");
              expect(actual).toBe(expected);
              reach.reach(failCheck ? "earlier CHECK" : "terminal expression");
              reach.reach(published ? "published" : "resident");
              await f.rollback();
              await f.retire();
              const result = await f.core.execute("SELECT * FROM p");
              if (!("batches" in result)) throw new Error("rows");
              expect(oracleRows(result)).toEqual([{ id: 1, v: n }]);
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        { seed, numRuns: 24 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  });
