/** PostgreSQL nodeLimit.c returns at count <= 0 before calling its child, even under ORDER BY.
 * https://doxygen.postgresql.org/nodeLimit_8c_source.html#l00081
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import { createExecutor } from "../exec/create.js";
import { UnsupportedPlanError } from "../exec/errors.js";

it.each(seedsFor("next-executor-ordered-zero", [0xc601, 0xc602]))(
  "does not enter a lowered sorted-zero input (seed %i)",
  async (seed) => {
    const reach = createReach("lowered ordered zero", [
      "constant",
      "parameter",
      "projected failure",
      "order failure",
      "offset",
      "descending",
    ]);
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    await pg.waitReady;
    try {
      await core.execute("CREATE TABLE z (v INTEGER)");
      await core.writeBatch("insertBatch", "z", [{ v: 0 }, { v: 1 }]);
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await pg.exec("CREATE TABLE z(v INTEGER); INSERT INTO z VALUES (0),(1)");
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 0, max: 12 }),
          fc.boolean(),
          fc.integer({ min: 1, max: 10000 }),
          async (parameter, orderFailure, offset, descending, numerator) => {
            reach.beginHistory();
            const expression = `${String(numerator)}/v`,
              projection = orderFailure ? "v AS q" : `${expression} AS q`,
              order = orderFailure ? expression : "v";
            const base = `SELECT ${projection} FROM z ORDER BY ${order}${descending ? " DESC" : ""}`;
            // Establish each failure independently before crediting its unreachable-input dimension.
            await expect(pg.query(base)).rejects.toMatchObject({ code: "22012" });
            await expect(core.execute(base)).rejects.toMatchObject({
              code: "MINNOW_DIVISION_BY_ZERO",
            });
            reach.reach(orderFailure ? "order failure" : "projected failure");
            const sql = `${base} LIMIT ${parameter ? "$1" : "0"} OFFSET ${String(offset)}`,
              args = parameter ? [0] : [];
            expect((await pg.query(sql, args)).rows).toEqual([]);
            const result = await core.execute(sql, args);
            if (!("batches" in result)) throw new Error("expected SELECT rows");
            expect(oracleRows(result)).toEqual([]);
            expect(core.lastProfile?.pipelines).toBe(0);
            reach.reach(parameter ? "parameter" : "constant");
            if (offset > 0) reach.reach("offset");
            if (descending) reach.reach("descending");
          },
        ),
        {
          seed,
          numRuns: 24,
          examples: [
            [false, false, 1, false, 1],
            [true, true, 1, true, 1],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  },
  120000,
);

it("still validates capabilities under a sorted zero limit", () => {
  expect(() =>
    createExecutor().prepare({
      root: {
        id: 2,
        op: "sort",
        input: { id: 1, op: "input", output: [] },
        keys: [],
        limit: { kind: "const", value: 0 },
        offset: null,
        withTies: false,
        output: [],
      },
      columns: [],
      parameters: [],
      placeholderCount: 0,
      clockSlot: null,
      initPlans: [],
      catalogVersion: 0,
      volatile: false,
    }),
  ).toThrow(UnsupportedPlanError);
});
