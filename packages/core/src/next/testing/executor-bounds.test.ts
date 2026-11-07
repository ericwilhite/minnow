/** An active LIMIT node validates its own OFFSET and LIMIT before returning zero rows.
 * PostgreSQL nodeLimit.c recompute_limits/ExecLimit; unreachable child nodes stay unopened.
 */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

const bounds = [
  0,
  3,
  -1,
  null,
  9007199254741009n,
  9223372036854775808n,
  -9223372036854775809n,
] as const;
async function outcome<T>(run: () => Promise<T>): Promise<{ value: T } | { error: unknown }> {
  try {
    return { value: await run() };
  } catch (error) {
    return { error };
  }
}
function state(error: unknown, property: "code" | "sqlState"): unknown {
  return typeof error === "object" && error !== null
    ? (error as Record<string, unknown>)[property]
    : undefined;
}

it.each(seedsFor("next-executor-active-bounds", [0xc607, 0xc608]))(
  "validates active bounds before the zero-input shortcut (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("active bounds", [
      "ordered",
      "unordered",
      "constant-limit",
      "parameter-limit",
      "zero",
      "zero-negative-offset",
      "negative-limit",
      "negative-offset",
      "both-negative-offset-first",
      "null-limit",
      "null-offset",
      "beyond-safe-limit",
      "beyond-safe-offset",
      "overflow-limit",
      "overflow-offset",
    ]);
    await pg.waitReady;
    try {
      await core.execute("CREATE TABLE z(v INTEGER)");
      await core.writeBatch("insertBatch", "z", [{ v: 1 }, { v: 1 }, { v: 1 }]);
      await pg.exec("CREATE TABLE z(v INTEGER);INSERT INTO z VALUES(1),(1),(1)");
      const examples: Array<[number, number, boolean, boolean]> = [
        [0, 2, true, false],
        [0, 2, true, true],
      ];
      for (const ordered of [false, true])
        for (const constant of [false, true])
          for (let offset = 0; offset < bounds.length; offset++)
            examples.push([0, offset, ordered, constant]);
      examples.push(
        [2, 2, true, false],
        [2, 2, false, false],
        [3, 3, true, true],
        [4, 0, true, false],
        [5, 0, true, false],
        [6, 0, false, false],
      );
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: bounds.length - 1 }),
          fc.integer({ min: 0, max: bounds.length - 1 }),
          fc.boolean(),
          fc.boolean(),
          async (li, oi, ordered, constant) => {
            reach.beginHistory();
            const limit = bounds[li],
              offset = bounds[oi];
            if (limit === undefined || offset === undefined) throw new Error("bound dimension");
            const literal = constant && (li === 0 || li === 1 || li === 3);
            const sql = `SELECT v FROM z${ordered ? " ORDER BY v" : ""} LIMIT ${literal ? (limit === null ? "ALL" : String(limit)) : "$1"} OFFSET ${literal ? "$1" : "$2"}`;
            const args = literal ? [offset] : [limit, offset];
            const pgResult = await outcome(() => pg.query(sql, args));
            const result = await outcome(() => core.execute(sql, args));
            if ("error" in pgResult) {
              expect("error" in result).toBe(true);
              if (!("error" in result)) throw new Error("missing bound error");
              const code = state(pgResult.error, "code");
              expect(["2201X", "2201W", "22003"]).toContain(code);
              expect(state(result.error, "sqlState")).toBe(code);
              if (limit === 0 && offset === -1) {
                expect(code).toBe("2201X");
                reach.reach("zero-negative-offset");
              }
              if (limit === -1 && offset === -1) {
                expect(code).toBe("2201X");
                reach.reach("both-negative-offset-first");
              }
              if (code === "2201W") reach.reach("negative-limit");
              if (code === "2201X") reach.reach("negative-offset");
              if (li >= 5 && oi < 5) reach.reach("overflow-limit");
              if (oi >= 5 && li < 5) reach.reach("overflow-offset");
            } else {
              expect("value" in result).toBe(true);
              if (!("value" in result) || !("batches" in result.value))
                throw new Error("expected SELECT result");
              expect(oracleRows(result.value)).toEqual(pgResult.value.rows);
              if (limit === 0) {
                expect(core.lastProfile?.pipelines).toBe(0);
                reach.reach("zero");
              }
              if (limit === null) reach.reach("null-limit");
              if (offset === null) reach.reach("null-offset");
              if (li === 4) reach.reach("beyond-safe-limit");
              if (oi === 4) reach.reach("beyond-safe-offset");
            }
            reach.reach(ordered ? "ordered" : "unordered");
            reach.reach(literal ? "constant-limit" : "parameter-limit");
            expect(core.spill.bytesInUse).toBe(0);
          },
        ),
        { seed, numRuns: 48, examples },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  },
  120000,
);

it.each(seedsFor("next-executor-unreachable-bounds", [0xc609, 0xc60a]))(
  "does not bind the validly typed bounds of an unopened child (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("unreachable bounds", [
      "ordered-child",
      "unordered-child",
      "ordered-parent",
      "unordered-parent",
      "negative-limit",
      "negative-offset",
      "parameter-zero",
      "constant-zero",
    ]);
    await pg.waitReady;
    try {
      await core.execute("CREATE TABLE z(v INTEGER)");
      await core.writeBatch("insertBatch", "z", [{ v: 1 }]);
      await pg.exec("CREATE TABLE z(v INTEGER);INSERT INTO z VALUES(1)");
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 1, max: 1000 }),
          async (childOrder, parentOrder, parameterZero, negativeLimit, amount) => {
            reach.beginHistory();
            const inner = `SELECT v FROM z${childOrder ? " ORDER BY v" : ""} LIMIT $1 OFFSET $2`;
            const sql = `SELECT v FROM (${inner}) q${parentOrder ? " ORDER BY v" : ""} LIMIT ${parameterZero ? "$3" : "0"}`;
            const args = [
              negativeLimit ? -amount : 1,
              negativeLimit ? 0 : -amount,
              ...(parameterZero ? [0] : []),
            ];
            expect((await pg.query(sql, args)).rows).toEqual([]);
            const actual = await outcome(() => core.execute(sql, args));
            expect("error" in actual ? state(actual.error, "sqlState") : null).toBe(null);
            if (!("value" in actual) || !("batches" in actual.value))
              throw new Error("SELECT result");
            expect(oracleRows(actual.value)).toEqual([]);
            expect(core.lastProfile?.pipelines).toBe(0);
            reach.reach(childOrder ? "ordered-child" : "unordered-child");
            reach.reach(parentOrder ? "ordered-parent" : "unordered-parent");
            reach.reach(parameterZero ? "parameter-zero" : "constant-zero");
            reach.reach(negativeLimit ? "negative-limit" : "negative-offset");
          },
        ),
        {
          seed,
          numRuns: 24,
          examples: [
            [true, false, false, false, 1],
            [false, true, true, true, 1],
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

it.each([false, true])(
  "keeps protocol arity errors before binding a missing OFFSET parameter (ordered %s)",
  async (ordered) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    await pg.waitReady;
    try {
      await core.execute("CREATE TABLE z(v INTEGER)");
      await pg.exec("CREATE TABLE z(v INTEGER)");
      const sql = `SELECT v FROM z${ordered ? " ORDER BY v" : ""} LIMIT 0 OFFSET $1`;
      await expect(pg.query(sql, [])).rejects.toMatchObject({ code: "08P01" });
      await expect(core.execute(sql, [])).rejects.toMatchObject({
        code: "MINNOW_PARAMETER_BIND",
        sqlState: "08P01",
      });
    } finally {
      await core.close();
      await pg.close();
    }
  },
);
