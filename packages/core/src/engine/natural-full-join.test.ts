// NATURAL FULL JOIN, which PostgreSQL accepts, is refused with a message saying so and how to
// write it, instead of failing at execution with a missing join condition; the FULL JOIN … USING
// spelling it suggests returns PostgreSQL's rows.
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

describe("NATURAL FULL JOIN", () => {
  it("is refused as not supported, and its USING spelling matches PostgreSQL", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    for (const sql of [
      "CREATE TABLE c (id integer PRIMARY KEY, name text, k integer)",
      "CREATE TABLE o (id integer PRIMARY KEY, cid integer, qty integer, k integer)",
      "INSERT INTO c VALUES (1, 'a', 7), (2, 'b', 8), (3, 'z', 9)",
      "INSERT INTO o VALUES (1, 1, 5, 7), (11, 9, 6, 1), (2, 2, 7, 3)",
    ]) {
      await database.execute(sql);
      await pg.exec(sql);
    }
    for (const sql of [
      "SELECT id, k, name, qty FROM c NATURAL FULL JOIN o ORDER BY id, k",
      "SELECT count(*) AS n FROM c NATURAL FULL JOIN o",
    ]) {
      await expect(database.query(sql), sql).rejects.toThrow("NATURAL FULL JOIN is not supported");
      await pg.query(sql);
    }
    const using = "SELECT id, k, name, qty FROM c FULL JOIN o USING (id, k) ORDER BY id, k";
    expect((await database.query(using)).rows.map((row) => Object.values(row))).toEqual(
      (await pg.query<Record<string, unknown>>(using)).rows.map((row) => Object.values(row)),
    );
    await database.close();
  });
});
