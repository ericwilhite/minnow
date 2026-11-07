// Calls with arguments their function does not take fail with a message naming the function,
// as PostgreSQL's: no overload (42883) for NOW(x) and ROW_NUMBER(x); the ordered-set aggregate
// (42809) for RANK(x) and its siblings; CURRENT_TIMESTAMP(p), which PostgreSQL accepts, is
// refused as not supported.
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

const CASES: ReadonlyArray<readonly [sql: string, message: string]> = [
  ["SELECT now(1)", "NOW takes no arguments"],
  ["SELECT row_number(1) OVER () FROM a", "ROW_NUMBER takes no arguments"],
  ["SELECT rank(1) OVER () FROM a", "WITHIN GROUP is required for ordered-set aggregate rank"],
  [
    "SELECT dense_rank(1) OVER () FROM a",
    "WITHIN GROUP is required for ordered-set aggregate dense_rank",
  ],
  [
    "SELECT percent_rank(1) OVER () FROM a",
    "WITHIN GROUP is required for ordered-set aggregate percent_rank",
  ],
  ["SELECT current_timestamp(1) IS NOT NULL AS x", "CURRENT_TIMESTAMP(precision) is not supported"],
];

describe("function argument errors", () => {
  it("name the function, as PostgreSQL's do", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)");
    await pg.exec("CREATE TABLE a (id INTEGER PRIMARY KEY)");
    for (const [sql, message] of CASES) {
      await expect(database.query(sql), sql).rejects.toThrow(message);
      let code = "ok";
      try {
        await pg.query(sql);
      } catch (error) {
        code = String((error as { code?: string }).code);
      }
      // PostgreSQL refuses the same call, or (CURRENT_TIMESTAMP(p)) accepts what is refused here.
      if (message.includes("not supported")) expect(code, sql).toBe("ok");
      else expect(["42883", "42809"], sql).toContain(code);
    }
    // The forms without arguments still work.
    expect((await database.query("SELECT now() IS NOT NULL AS x")).rows).toEqual([{ x: true }]);
    expect((await database.query("SELECT rank() OVER () AS r FROM a")).rows).toEqual([]);
    await database.close();
  });
});
