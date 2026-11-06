// Bracketed comments nest, as in PostgreSQL: an inner opening needs its own closing before the
// outer one ends. Every shape runs in PGlite and in the engine: both return the same rows, or
// both fail.
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

const SHAPES = [
  "SELECT 1 /* a */ AS x",
  "SELECT 1 /* a /* b */ c */ AS x",
  "SELECT 1 /* a /* b /* c */ d */ e */ AS x",
  "SELECT /* outer /* inner */ still outer */ 2 AS x",
  "SELECT '/* not a comment */' AS x",
  'SELECT 1 AS "/* x */"',
  "SELECT 1 /* a /* b */ AS x",
  "SELECT 1 /* a */ */ AS x",
  "SELECT 1 /**/ AS x",
  "SELECT 1 /* -- inside */ AS x",
  "SELECT 1 -- /* line\n AS x",
  "SELECT 1 /* a */ AS x /* trailing /* nested */ */",
];

describe("bracketed comments", () => {
  it("nest exactly as PostgreSQL", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const outcome = async (run: () => Promise<unknown[]>): Promise<string> => {
      try {
        return JSON.stringify(await run());
      } catch {
        return "error";
      }
    };
    const mismatches: string[] = [];
    let failed = 0;
    for (const sql of SHAPES) {
      const expected = await outcome(async () => (await pg.query(sql)).rows);
      const actual = await outcome(async () => (await database.query(sql)).rows);
      if (expected === "error") failed += 1;
      if (actual !== expected)
        mismatches.push(`${sql}\n  pglite: ${expected}\n  minnow: ${actual}`);
    }
    await database.close();
    expect(mismatches.join("\n")).toBe("");
    expect(failed).toBeGreaterThan(0);
  });
});
