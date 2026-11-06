// An unknown name in GROUP BY, HAVING or an aggregate argument is reported as unknown, as in
// PostgreSQL (42P01 for a qualifier no source has, 42703 for a column), not as an ungrouped
// column; a `t.*` argument names a source. The current engine keeps its own wording for those
// errors; lowering's front-end path gives PostgreSQL's messages exactly.
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { NAME_RESOLUTION_SETUP, NAME_RESOLUTION_CASES } from "../testing/conformance-source.js";
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

/** The engine's name errors by the class PostgreSQL gives them. */
const ENGINE_CLASS: ReadonlyArray<readonly [RegExp, string]> = [
  [/^(Unknown table alias: |missing FROM-clause entry for table )/u, "42P01"],
  [/^(Unknown column: |Ambiguous or missing column: )/u, "42703"],
];

describe("name errors before grouping errors", () => {
  it("match PostgreSQL in the shipped engine", async () => {
    const engine = new MinnowDatabase(new MemoryBlockStore());
    for (const sql of NAME_RESOLUTION_SETUP) {
      await engine.execute(sql);
      await pg.exec(sql);
    }
    for (const sql of NAME_RESOLUTION_CASES) {
      let expected: string | null = null;
      try {
        await pg.query(sql);
      } catch (error) {
        expected = String((error as { code?: string }).code);
      }
      let engineClass = "ok";
      try {
        await engine.query(sql);
      } catch (error) {
        const message = (error as Error).message;
        engineClass = ENGINE_CLASS.find(([pattern]) => pattern.test(message))?.[1] ?? message;
      }
      expect(engineClass, `${sql} (engine)`).toBe(expected ?? "ok");
    }
    await engine.close();
  });
});
