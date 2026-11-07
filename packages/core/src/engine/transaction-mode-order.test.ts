/** PostgreSQL validates SET TRANSACTION mode items in order, before final modes are known. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { createReach, TRANSACTION_MODE_ORDER_STATES } from "../testing/reach.js";
import { MinnowDatabase } from "./database.js";
import { compileStatement } from "./query.js";

const pg = new PGlite();
beforeAll(async () => {
  await pg.exec("CREATE TABLE t (id integer)");
});
afterAll(async () => {
  await pg.close();
});
function state(error: unknown): string {
  if (typeof error === "object" && error !== null && "code" in error) return String(error.code);
  const message = String(error);
  if (message.includes("read-write mode")) return "25001";
  if (message.includes("in a read-only transaction")) return "25006";
  return message;
}

it("keeps compiled mode arrays ordered and duplicate items intact", () => {
  for (const command of ["BEGIN", "START TRANSACTION", "SET TRANSACTION"])
    for (const separator of [", ", " "]) {
      const sql =
        command +
        " " +
        [
          "READ ONLY",
          "READ WRITE",
          "ISOLATION LEVEL READ COMMITTED",
          "DEFERRABLE",
          "READ ONLY",
          "NOT DEFERRABLE",
          "ISOLATION LEVEL REPEATABLE READ",
        ].join(separator);
      const statement = compileStatement(sql);
      const items =
        statement.kind === "transaction"
          ? statement.modes
          : statement.kind === "set"
            ? statement.transactionModes
            : undefined;
      expect(items, sql).toEqual([
        { kind: "access", readOnly: true },
        { kind: "access", readOnly: false },
        { kind: "isolation", level: "read committed" },
        { kind: "deferrable", deferrable: true },
        { kind: "access", readOnly: true },
        { kind: "deferrable", deferrable: false },
        { kind: "isolation", level: "repeatable read" },
      ]);
    }
});

it("validates each access item in order through SQL and compiled entry points, as PostgreSQL", async () => {
  const reach = createReach<string>("transaction mode order", TRANSACTION_MODE_ORDER_STATES);
  const lists: string[][] = [];
  for (const a of ["READ ONLY", "READ WRITE"]) {
    lists.push([a]);
    for (const b of ["READ ONLY", "READ WRITE"]) {
      lists.push([a, b]);
      for (const c of ["READ ONLY", "READ WRITE"]) lists.push([a, b, c]);
    }
  }
  const databases = [
    new MinnowDatabase(new MemoryBlockStore()),
    new MinnowDatabase(new MemoryBlockStore()),
  ];
  for (const db of databases) await db.execute("CREATE TABLE t (id integer)");
  const failures: string[] = [];
  let cases = 0;
  try {
    for (const begin of ["BEGIN READ ONLY", "BEGIN READ WRITE"])
      for (const [context, prefix] of [
        ["top level", []],
        ["savepoint", ["SAVEPOINT s"]],
        ["after query", ["SELECT 1"]],
        ["already read only", ["SAVEPOINT s", "SET TRANSACTION READ ONLY"]],
      ] as const)
        for (const separator of [", ", " "])
          for (const list of lists) {
            const set = "SET TRANSACTION " + list.join(separator);
            const history = [begin, ...prefix, set, "INSERT INTO t VALUES (1)"];
            let expected = "ok";
            try {
              for (const item of history) await pg.exec(item);
            } catch (error) {
              expected = state(error);
            } finally {
              await pg.exec("ROLLBACK");
            }
            for (const [i, db] of databases.entries()) {
              reach.beginHistory();
              let actual = "ok";
              try {
                for (const item of history)
                  if (i === 0 || item.startsWith("SELECT") || item.startsWith("INSERT"))
                    await db.execute(item);
                  else await db.runStatement(compileStatement(item));
              } catch (error) {
                actual = state(error);
              } finally {
                await db.execute("ROLLBACK");
              }
              if (actual !== expected)
                failures.push(
                  `${i === 0 ? "execute" : "compiled"}: ${history.join("; ")}\nPG ${expected}; Minnow ${actual}`,
                );
              else {
                reach.reach(context);
                reach.reach(begin === "BEGIN READ ONLY" ? "begin read only" : "begin read write");
                reach.reach(i === 0 ? "execute" : "compiled");
                reach.reach(expected === "ok" ? "accepted" : "refused");
                if (new Set(list).size < list.length) reach.reach("duplicates");
              }
              cases++;
            }
          }
    expect(cases).toBe(448);
    expect(failures.join("\n")).toBe("");
    reach.assertReached();
  } finally {
    for (const db of databases) await db.close();
  }
});
