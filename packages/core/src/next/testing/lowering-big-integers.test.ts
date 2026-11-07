/**
 * Integer literals past 2^53 are bigint while they fit int8, as in PostgreSQL, and keep their
 * exact value through lowering (the shipped parser's constant pass leaves them as written under
 * lowering's scope). Types, and values as differences small enough to compare exactly, must
 * match PGlite.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { LoweredDatabase } from "./lowering-harness.js";

const TYPES: Readonly<Record<number, string>> = { 20: "bigint", 23: "integer", 1700: "numeric" };

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

describe("integer literals past 2^53", () => {
  it("type and compute as PostgreSQL", async () => {
    const next = new LoweredDatabase();
    for (const sql of [
      "CREATE TABLE big (id integer PRIMARY KEY, k bigint)",
      "INSERT INTO big VALUES (1, 9007199254740993), (2, 9223372036854775807), (3, -9007199254740993)",
    ]) {
      await pg.exec(sql);
      next.execute(sql);
    }
    for (const sql of [
      "SELECT 9007199254740993 - 9007199254740992 AS d",
      "SELECT -9007199254740993 + 9007199254740992 AS d",
      "SELECT 9223372036854775807 - 9223372036854775806 AS d",
      "SELECT k - 9007199254740992 AS d FROM big WHERE id = 1",
      "SELECT k + 9007199254740992 AS d FROM big WHERE id = 3",
      "SELECT id FROM big WHERE k = 9223372036854775807",
      "SELECT 2 * 3 AS m, 7 / 2 AS q",
    ]) {
      const expected = await pg.query<Record<string, unknown>>(sql);
      const lowered = next.lower(sql);
      if (lowered.kind !== "query") throw new Error(sql);
      expect(
        lowered.columns.map((column) => column.sqlType),
        `${sql} types`,
      ).toEqual(
        expected.fields.map((field) => TYPES[field.dataTypeID] ?? String(field.dataTypeID)),
      );
      expect(next.execute(sql), `${sql} rows`).toEqual(expected.rows);
    }
    // The literal alone is bigint (its value is past what the harness compares exactly).
    for (const sql of [
      "SELECT 9007199254740993 AS a",
      "SELECT 9223372036854775807 AS a",
      "SELECT 9223372036854775808 AS a",
    ]) {
      const expected = await pg.query(sql);
      const lowered = next.lower(sql);
      if (lowered.kind !== "query") throw new Error(sql);
      expect(
        lowered.columns.map((column) => column.sqlType),
        sql,
      ).toEqual(expected.fields.map((field) => TYPES[field.dataTypeID]));
    }
  });
});
