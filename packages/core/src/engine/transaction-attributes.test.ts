/** Per-attribute admission, not isolation execution: PostgreSQL variable.c is the authority. */
import { PGlite } from "@electric-sql/pglite";
import { beforeEach, afterEach, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { createReach, TRANSACTION_ATTRIBUTE_STATES } from "../testing/reach.js";
import { MinnowDatabase } from "./database.js";
import { compileStatement } from "./query.js";

let pg: PGlite;
let databases: MinnowDatabase[];
beforeEach(async () => {
  pg = new PGlite();
  databases = [
    new MinnowDatabase(new MemoryBlockStore()),
    new MinnowDatabase(new MemoryBlockStore()),
  ];
  for (const sql of ["CREATE TABLE t (id integer PRIMARY KEY)", "INSERT INTO t VALUES (1)"]) {
    await pg.exec(sql);
    for (const db of databases) await db.execute(sql);
  }
});
afterEach(async () => {
  await pg.close();
  for (const db of databases) await db.close();
});
function failure(error: unknown): { code: string; message: string } {
  const message = error instanceof Error ? error.message : String(error);
  const code =
    typeof error === "object" && error !== null && "code" in error
      ? String(error.code)
      : /must(?: not)? be called|cannot be called|transaction read-write mode|cannot set transaction read-write mode/u.test(
            message,
          )
        ? "25001"
        : "unexpected";
  return { code, message };
}
const LEVELS = [undefined, "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ"] as const;
const CONTEXTS = [
  { name: "empty", sql: [], queried: false, savepoint: false },
  { name: "scalar read", sql: ["SELECT 1"], queried: true, savepoint: false },
  { name: "table read", sql: ["SELECT * FROM t"], queried: true, savepoint: false },
  { name: "write", sql: ["INSERT INTO t VALUES (2)"], queried: true, savepoint: false },
  { name: "savepoint", sql: ["SAVEPOINT s"], queried: false, savepoint: true },
  {
    name: "read then savepoint",
    sql: ["SELECT * FROM t", "SAVEPOINT s"],
    queried: true,
    savepoint: true,
  },
  {
    name: "savepoint then read",
    sql: ["SAVEPOINT s", "SELECT * FROM t"],
    queried: true,
    savepoint: true,
  },
  {
    name: "write then savepoint",
    sql: ["INSERT INTO t VALUES (2)", "SAVEPOINT s"],
    queried: true,
    savepoint: true,
  },
  {
    name: "nested savepoint",
    sql: ["SAVEPOINT s", "SAVEPOINT inner_s"],
    queried: false,
    savepoint: true,
  },
  {
    name: "released savepoint",
    sql: ["SAVEPOINT s", "RELEASE s"],
    queried: false,
    savepoint: false,
  },
  {
    name: "released read",
    sql: ["SAVEPOINT s", "SELECT * FROM t", "RELEASE s"],
    queried: true,
    savepoint: false,
  },
  {
    name: "rolled back read",
    sql: ["SAVEPOINT s", "SELECT * FROM t", "ROLLBACK TO s"],
    queried: true,
    savepoint: true,
  },
] as const;
const ITEMS = [
  ...LEVELS.slice(1).map((level) => [`ISOLATION LEVEL ${String(level)}`]),
  ["DEFERRABLE"],
  ["NOT DEFERRABLE"],
  ["READ ONLY"],
  ["READ WRITE"],
  ["ISOLATION LEVEL READ COMMITTED", "ISOLATION LEVEL REPEATABLE READ"],
  ["ISOLATION LEVEL REPEATABLE READ", "ISOLATION LEVEL READ COMMITTED"],
  ["ISOLATION LEVEL READ COMMITTED", "ISOLATION LEVEL READ COMMITTED"],
  ["DEFERRABLE", "NOT DEFERRABLE"],
  ["NOT DEFERRABLE", "DEFERRABLE"],
  ["READ ONLY", "ISOLATION LEVEL REPEATABLE READ", "DEFERRABLE"],
  ["DEFERRABLE", "ISOLATION LEVEL REPEATABLE READ", "READ ONLY"],
];
for (const initial of LEVELS)
  it(`matches PostgreSQL's attribute matrix from ${initial ?? "default"}`, async () => {
    const reach = createReach<string>("transaction attribute matrix", TRANSACTION_ATTRIBUTE_STATES);
    const failures: string[] = [];
    let cases = 0;
    for (const initialize of ["begin modes", "set modes"])
      for (const readOnly of [false, true])
        for (const deferrable of [false, true])
          for (const context of CONTEXTS) {
            const earlierWrite = context.sql.some((sql) => sql.startsWith("INSERT"));
            if (readOnly && earlierWrite) continue;
            for (const items of ITEMS) {
              const modes = `${initial === undefined ? "" : `ISOLATION LEVEL ${initial} `}${readOnly ? "READ ONLY" : "READ WRITE"} ${deferrable ? "DEFERRABLE" : "NOT DEFERRABLE"}`;
              const set = `SET TRANSACTION ${items.join(", ")}`;
              const history = [
                ...(initialize === "begin modes"
                  ? [`BEGIN ${modes}`]
                  : ["BEGIN", `SET TRANSACTION ${modes}`]),
                ...context.sql,
              ];
              await pg.exec("DELETE FROM t WHERE id = 2");
              for (const sql of history) await pg.exec(sql);
              let expected: ReturnType<typeof failure> | undefined;
              try {
                await pg.exec(set);
              } catch (error) {
                expected = failure(error);
              }
              if (expected !== undefined && context.savepoint) await pg.exec("ROLLBACK TO s");
              await pg.exec(expected !== undefined && !context.savepoint ? "ROLLBACK" : "COMMIT");
              const expectedRows = (await pg.query("SELECT id FROM t ORDER BY id")).rows;
              for (const [entry, db] of databases.entries()) {
                reach.beginHistory();
                await db.execute("DELETE FROM t WHERE id = 2");
                const run = (sql: string) =>
                  entry === 0 || /^(?:SELECT|INSERT)/u.test(sql)
                    ? db.execute(sql)
                    : db.runStatement(compileStatement(sql));
                for (const sql of history) await run(sql);
                let actual: ReturnType<typeof failure> | undefined;
                try {
                  await run(set);
                } catch (error) {
                  actual = failure(error);
                }
                // PG aborts a failed statement's transaction; recover at the same boundary for both.
                if (actual !== undefined && context.savepoint) await run("ROLLBACK TO s");
                await run(actual !== undefined && !context.savepoint ? "ROLLBACK" : "COMMIT");
                const rows = (await db.query("SELECT id FROM t ORDER BY id")).rows;
                if (
                  JSON.stringify(expected) !== JSON.stringify(actual) ||
                  JSON.stringify(rows) !== JSON.stringify(expectedRows)
                )
                  failures.push(
                    `${entry === 0 ? "execute" : "compiled"}: ${history.join("; ")}; ${set}\nPG ${JSON.stringify(expected)} ${JSON.stringify(expectedRows)}; Minnow ${JSON.stringify(actual)} ${JSON.stringify(rows)}`,
                  );
                else {
                  reach.reach(entry === 0 ? "execute" : "compiled");
                  reach.reach(initialize);
                  reach.reach(context.queried ? "after query" : "before query");
                  reach.reach(context.savepoint ? "savepoint" : "top level");
                  reach.reach(expected === undefined ? "accepted" : "refused");
                  if (earlierWrite) reach.reach("earlier write");
                  if (context.name.startsWith("released")) reach.reach("release");
                  if (context.name === "rolled back read") reach.reach("rollback to");
                  if (new Set(items).size !== items.length) reach.reach("duplicate item");
                  if (items.length === 3) reach.reach("mixed attributes");
                  for (const item of items) {
                    if (item.startsWith("ISOLATION"))
                      reach.reach(
                        item.endsWith(initial ?? "READ COMMITTED")
                          ? "same isolation"
                          : "changed isolation",
                      );
                    else if (item.endsWith("DEFERRABLE"))
                      reach.reach(
                        (item === "DEFERRABLE") === deferrable
                          ? "same deferrable"
                          : "changed deferrable",
                      );
                    else
                      reach.reach(
                        (item === "READ ONLY") === readOnly ? "same access" : "changed access",
                      );
                  }
                  if (expected?.message.includes("ISOLATION LEVEL"))
                    reach.reach(
                      context.queried ? "isolation query refusal" : "isolation savepoint refusal",
                    );
                  if (expected?.message.includes("[NOT] DEFERRABLE"))
                    reach.reach(
                      context.savepoint
                        ? "deferrable savepoint refusal"
                        : "deferrable query refusal",
                    );
                }
                cases++;
              }
            }
          }
    expect(cases).toBe(2464);
    expect(failures.join("\n")).toBe("");
    reach.assertReached();
  });
