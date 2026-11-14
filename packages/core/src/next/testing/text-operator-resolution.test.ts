/** Opening output casts must not turn a different/undefined || overload into textcat. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TEXT_OPERATOR_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
for (const seed of seedsFor("text-operator", [47071, 47072]))
  it(`resolves text operands and refuses foreign concat seed${String(seed)}`, async () => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      reach = createReach("text operator", TEXT_OPERATOR_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("integer", "boolean", "jsonb"),
          fc.integer({ min: 0, max: 3 }),
          fc.integer({ min: -99, max: 99 }),
          async (type, mode, n) => {
            reach.beginHistory();
            const value =
                type === "integer"
                  ? `(${String(n)})::integer`
                  : type === "boolean"
                    ? n % 2
                      ? "TRUE"
                      : "FALSE"
                    : `'{"n":${String(n)}}'::jsonb`,
              lhs =
                mode === 0 ? "'x'::text" : mode === 3 ? (type === "jsonb" ? "'[]'" : "'x'") : value,
              rhs = mode === 1 ? "'y'::text" : value,
              sql = `SELECT ${lhs}||${rhs} v`;
            if (mode < 2 || (mode === 3 && type !== "jsonb")) {
              const expected = (await pg.query(sql)).rows,
                actual = await core.execute(sql);
              if (!("batches" in actual)) throw new Error("Rows required");
              expect(oracleRows(actual)).toEqual(expected);
              reach.reach(mode === 0 ? "left text" : mode === 1 ? "right text" : "unknown text");
            } else if (type === "jsonb") {
              await pg.query(sql);
              await expect(core.execute(sql)).rejects.toMatchObject({
                code: "MINNOW_UNSUPPORTED_PLAN",
                sqlState: "0A000",
              });
              reach.reach("JSONB stays unsupported");
            } else {
              await expect(pg.query(sql)).rejects.toMatchObject({ code: "42883" });
              await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "42883" });
              reach.reach("undefined operator");
            }
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero");
          },
        ),
        {
          seed,
          numRuns: 32,
          examples: [
            ["integer", 0, 1],
            ["integer", 1, -1],
            ["boolean", 3, 0],
            ["integer", 2, 2],
            ["jsonb", 2, 1],
            ["jsonb", 3, 1],
          ],
          endOnFailure: true,
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 60000);
