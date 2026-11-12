/** SQL arguments are evaluated first; JSONB conversion then reports defects in pair order. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_CONSTRUCT_ERROR_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
const defects = [
  "NULL,0",
  "'{}'::json,0",
  String.raw`'v','"\u0000"'::json`,
  "'v','1e131072'::json",
  String.raw`'v','"\uD800"'::json`,
];
it.each(seedsFor("next-jsonb-construction-errors", [0xb6e8, 0xb6e9]))(
  "keeps independent conversion/key defects in PostgreSQL order (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB constructor error order", JSONB_CONSTRUCT_ERROR_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.shuffledSubarray([0, 1, 2, 3, 4], { minLength: 2, maxLength: 5 }),
        fc.boolean(),
        async (order, prefix) => {
          reach.beginHistory();
          const sql = `SELECT jsonb_build_object(${prefix ? "'ok',1," : ""}${order.map((i) => defects[i]).join(",")},'after',2)`;
          let state: string | undefined;
          try {
            await pg.query(sql);
          } catch (e) {
            state = (e as { code: string }).code;
          }
          expect(state).toBeDefined();
          await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: state });
          const precedes = (a: number, b: number) =>
            order.includes(a) && order.includes(b) && order.indexOf(a) < order.indexOf(b);
          if (precedes(2, 0)) reach.reach("Unicode before NULL key");
          if (precedes(3, 0)) reach.reach("numeric before NULL key");
          if (precedes(0, 2)) reach.reach("NULL key before Unicode");
          if (precedes(1, 2)) reach.reach("typed key before value");
          if (order.includes(4)) reach.reach("surrogate conversion");
          if (order.length > 2) reach.reach("three or more defects");
          reach.reach("exact PostgreSQL SQLSTATE");
          reach.reach("zero spill");
          expect(core.spill.bytesInUse).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [[2, 0], false],
          [[3, 0], true],
          [[0, 2], false],
          [[1, 2], false],
          [[2, 4, 0], true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
it.each([
  "SELECT json_build_object(NULL,1)",
  "SELECT jsonb_build_object(NULL,1)",
  "SELECT json_object(NULL VALUE '1'::jsonb)",
  "SELECT json_object(NULL VALUE 1)",
])("preserves constructor-specific NULL key error: %s", async (sql) => {
  let state: string | undefined;
  try {
    await pg.query(sql);
  } catch (e) {
    state = (e as { code: string }).code;
  }
  expect(state).toBeDefined();
  await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: state });
});
