// An unknown name in GROUP BY, HAVING or an aggregate argument is reported as unknown, as in
// PostgreSQL (42P01 for a qualifier no source has, 42703 for a column), not as an ungrouped
// column; a `t.*` argument names a source. The current engine keeps its own wording for those
// errors; lowering's front-end path gives PostgreSQL's messages exactly.
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { LoweredDatabase } from "../next/testing/lowering-harness.js";
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

const SETUP = [
  "CREATE TABLE a (id integer PRIMARY KEY, v integer)",
  "INSERT INTO a VALUES (1, 10), (2, 20)",
];

const CASES = [
  "SELECT count(z.*) FROM a",
  "SELECT id FROM a GROUP BY z.id",
  "SELECT id FROM a GROUP BY nope",
  "SELECT id FROM a GROUP BY a.nope",
  "SELECT id FROM a GROUP BY id HAVING nope > 0",
  "SELECT id FROM a GROUP BY id HAVING z.v > 0",
  "SELECT id FROM a GROUP BY id HAVING max(nope) > 0",
];

/** The engine's name errors by the class PostgreSQL gives them. */
const ENGINE_CLASS: ReadonlyArray<readonly [RegExp, string]> = [
  [/^(Unknown table alias: |missing FROM-clause entry for table )/u, "42P01"],
  [/^(Unknown column: |Ambiguous or missing column: )/u, "42703"],
];

describe("name errors before grouping errors", () => {
  it("match PostgreSQL in the engine and in lowering", async () => {
    const engine = new MinnowDatabase(new MemoryBlockStore());
    const next = new LoweredDatabase();
    for (const sql of SETUP) {
      await engine.execute(sql);
      next.execute(sql);
      await pg.exec(sql);
    }
    for (const sql of [
      ...CASES,
      "SELECT count(a.*) AS n FROM a",
      "SELECT v AS k FROM a GROUP BY k",
    ]) {
      let expected: { code: string; message: string } | null = null;
      try {
        await pg.query(sql);
      } catch (error) {
        expected = {
          code: String((error as { code?: string }).code),
          message: (error as Error).message,
        };
      }
      let engineClass = "ok";
      try {
        await engine.query(sql);
      } catch (error) {
        const message = (error as Error).message;
        engineClass = ENGINE_CLASS.find(([pattern]) => pattern.test(message))?.[1] ?? message;
      }
      let lowered = "ok";
      try {
        next.lower(sql);
      } catch (error) {
        lowered = (error as Error).message;
      }
      expect(engineClass, `${sql} (engine)`).toBe(expected?.code ?? "ok");
      expect(lowered, `${sql} (lowering)`).toBe(expected?.message ?? "ok");
    }
    await engine.close();
  });
});
