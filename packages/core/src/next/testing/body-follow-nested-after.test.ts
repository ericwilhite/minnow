/** Lead's implicit-follow law: nested USER AFTER waits; nested RI/checks remain immediate.
 * Captured children are unique only at the complete group checkpoint, across nested writers. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FOLLOW_NESTED_AFTER_REACH } from "../../testing/reach.js";
import { NativePrimaryAdapter } from "../write/primary-command.js";
import { commandRootFixture } from "./command-root.test-support.js";
import { oracleRows } from "./differential-adapter.js";

async function failure(run: () => Promise<unknown>): Promise<unknown> {
  try {
    await run();
    return null;
  } catch (error) {
    return error;
  }
}
function state(error: unknown): string | null {
  if (error === null) return null;
  if (typeof error === "object") {
    if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
    if ("code" in error && typeof error.code === "string") return error.code;
  }
  throw new Error("untyped native failure", { cause: error });
}

for (const seed of seedsFor("next-follow-nested-after", [0xb703, 0xb704]))
  it(`defers nested user observers behind complete follow groups seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("follow nested observers", FOLLOW_NESTED_AFTER_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 7 }),
          fc.integer({ min: 0, max: 3 }),
          fc.integer({ min: 0, max: 2 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (size, route, position, published, all, explicitCandidate) => {
            reach.beginHistory();
            const count = route === 3 ? Math.max(3, size) : size,
              at =
                route >= 2
                  ? count
                  : position === 0
                    ? 1
                    : position === 1
                      ? Math.ceil(count / 2)
                      : count,
              explicit = route < 2 && explicitCandidate,
              selected = all && route < 2 ? "true" : `OLD.id=${String(at)}`,
              setup = [
                "CREATE TABLE p(id INT PRIMARY KEY)",
                "CREATE TABLE audit(id INT PRIMARY KEY,v INT)",
                "INSERT INTO audit VALUES(1,0)",
                "CREATE TABLE trace(id INT PRIMARY KEY,v TEXT,n INT)",
                "INSERT INTO trace VALUES(1,'',0)",
                ...(route >= 2
                  ? [
                      "CREATE TABLE s(id INT PRIMARY KEY)",
                      `INSERT INTO s VALUES${(route === 3
                        ? Array.from({ length: count }, (_, i) => i + 1)
                        : [1]
                      )
                        .concat(99)
                        .map((n) => `(${String(n)})`)
                        .join(",")}`,
                    ]
                  : []),
                `CREATE TABLE c(id INT PRIMARY KEY,pid INT UNIQUE REFERENCES p(id)${explicit ? " ON UPDATE CASCADE" : ""}${route === 0 ? ",g INT GENERATED ALWAYS AS(CASE WHEN pid>100 THEN 1 ELSE pid END) STORED,CONSTRAINT child_group_unique UNIQUE(g)" : ""}${route >= 2 ? `,k INT DEFAULT 99 REFERENCES s(id)${route === 2 && explicitCandidate ? "" : " ON UPDATE CASCADE"} ON DELETE SET DEFAULT,marker INT${route === 3 ? ",CONSTRAINT child_k_unique UNIQUE(k)" : ""}` : ""})`,
                `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)})`).join(",")}`,
                `INSERT INTO c(id,pid${route >= 2 ? ",k,marker" : ""}) VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)}${route >= 2 ? `,${String(route === 3 ? i + 1 : i === count - 2 ? 1 : 99)},0` : ""})`).join(",")}`,
              ],
              before =
                route < 2
                  ? `UPDATE audit SET v=v+1 WHERE ${selected}`
                  : route === 2
                    ? `UPDATE s SET id=id+10 WHERE id=1 AND ${selected}; UPDATE audit SET v=v+1 WHERE ${selected}`
                    : `DELETE FROM s WHERE id<=2 AND ${selected}; UPDATE audit SET v=v+1 WHERE ${selected}`,
              after =
                route === 1
                  ? `UPDATE trace SET v=v||CAST(NEW.v AS TEXT)||';' WHERE id=1; UPDATE c SET pid=1/(pid-${String(at)}) WHERE id=${String(at)} AND pid=${String(at)}`
                  : "UPDATE trace SET n=1/(id-1) WHERE id=1",
              triggers = [
                { name: "child_before", table: "c", timing: "BEFORE", body: before },
                ...(route !== 2
                  ? [{ name: "audit_after", table: "audit", timing: "AFTER", body: after }]
                  : []),
              ],
              mutation =
                route === 0 || explicit
                  ? "UPDATE p SET id=id+100"
                  : `UPDATE p SET id=(id%${String(count)})+1`,
              f = await commandRootFixture(
                [
                  ...setup,
                  ...triggers.map(
                    (t) =>
                      `CREATE TRIGGER ${t.name} ${t.timing} UPDATE ON ${t.table} BEGIN ${t.body}; END`,
                  ),
                ],
                mutation,
                published,
              ),
              table = f.statement.catalog.tableByName("c");
            if (!table) throw Error("child table");
            let rootCommand = 0,
              acceptedChildren = 0,
              nestedChildren = 0;
            const apply = Reflect.get(NativePrimaryAdapter.prototype, "apply"),
              writes = vi
                .spyOn(NativePrimaryAdapter.prototype, "apply")
                .mockImplementation(async function (
                  this: NativePrimaryAdapter,
                  ...args: Parameters<NativePrimaryAdapter["apply"]>
                ) {
                  const out = await apply.apply(this, args);
                  const token = args[0];
                  if (token.tableId === f.statement.catalog.tableByName("p")?.id)
                    rootCommand = token.command.id;
                  if (token.tableId === table.id) {
                    if (token.command.id === rootCommand) acceptedChildren++;
                    else nestedChildren++;
                  }
                  if (
                    route === 2 &&
                    token.tableId === f.statement.catalog.tableByName("audit")?.id
                  ) {
                    expect(nestedChildren).toBeGreaterThan(0);
                    reach.reach("nested RI visible to next body");
                  }
                  return out;
                });
            try {
              let expected: string | null = route === 0 || route === 3 ? "23505" : null;
              if (explicit) {
                await pg.exec("DROP TABLE IF EXISTS c,p,audit,trace,s CASCADE");
                for (const sql of setup) await pg.exec(sql);
                for (const [i, t] of triggers.entries())
                  await pg.exec(
                    `CREATE OR REPLACE FUNCTION nested_f${String(i)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${t.body}; RETURN NEW; END $$; CREATE TRIGGER ${t.name} ${t.timing} UPDATE ON ${t.table} FOR EACH ROW EXECUTE FUNCTION nested_f${String(i)}()`,
                  );
                expected = state(await failure(() => pg.exec(mutation)));
                expect(expected).toBe("22012");
                reach.reach("explicit PG action unchanged");
              }
              const error = await failure(async () => {
                await f.root.run({ slots: [] });
                await f.root.end();
              });
              expect(state(error)).toBe(expected);
              if (!explicit && (route === 0 || route === 3)) {
                expect(error).toMatchObject({
                  tableId: table.id,
                  indexId: table.indexes.find(
                    (i) => i.name === (route === 0 ? "child_group_unique" : "child_k_unique"),
                  )?.id,
                });
                expect(acceptedChildren).toBe(count);
                reach.reach(
                  route === 0
                    ? "group23505 before nested22012"
                    : "deferred nested versions checked",
                );
              }
              if (error) await f.retire();
              else {
                const receipt = await f.tx.commit(null);
                await receipt.durable;
                await f.retire();
              }
              const rows = await f.core.execute(
                `SELECT id,pid${route >= 2 ? ",k,marker" : ""} FROM c ORDER BY id`,
              );
              if (!("batches" in rows)) throw Error("rows");
              expect(oracleRows(rows)).toEqual(
                Array.from({ length: count }, (_, i) => ({
                  id: i + 1,
                  pid: error ? i + 1 : ((i + 1) % count) + 1,
                  ...(route >= 2
                    ? {
                        k: error
                          ? route === 3
                            ? i + 1
                            : i === count - 2
                              ? 1
                              : 99
                          : i === count - 2
                            ? 11
                            : 99,
                        marker: 0,
                      }
                    : {}),
                })),
              );
              if (!error && route === 1) {
                const trace = await f.core.execute("SELECT v FROM trace");
                if (!("batches" in trace)) throw Error("trace");
                expect(oracleRows(trace)).toEqual([
                  {
                    v: Array.from({ length: all ? count : 1 }, (_, i) => `${String(i + 1)};`).join(
                      "",
                    ),
                  },
                ]);
                reach.reach("immutable nested images FIFO");
              }
              if (route === 2) {
                expect(error).toBeNull();
                reach.reach("mid-swap RI uniqueness waits");
                reach.reach(
                  explicitCandidate ? "nested follow membership overlap" : "nested explicit RI",
                );
                reach.reach("nested RI visible to next body");
              }
              reach.reach(count === 2 ? "swap" : "rotation");
              reach.reach(published ? "published" : "resident");
              if (route < 2 && !explicit)
                reach.reach(
                  at === 1 ? "first position" : at === count ? "last position" : "middle position",
                );
            } finally {
              writes.mockRestore();
              await f.close();
            }
            reach.reach("zero native charge");
          },
        ),
        {
          seed,
          numRuns: 24,
          examples: [
            [2, 0, 0, false, false, false],
            [2, 1, 2, true, false, false],
            [3, 1, 1, false, true, false],
            [3, 2, 2, true, false, false],
            [3, 2, 2, false, false, true],
            [3, 3, 2, false, false, false],
            [2, 0, 0, true, false, true],
            [3, 1, 2, false, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
