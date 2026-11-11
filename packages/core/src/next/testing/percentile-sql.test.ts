/** Hash-grouped selection retains PostgreSQL ordered-set ranks and float8_lerp order. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { createExecutor } from "../exec/create.js";
import { PercentileOperator } from "../exec/agg/percentile.js";
import { Governor } from "../memory/governor.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { OutputArena } from "../exec/kernels/vectors.js";

async function deleteThirds(core: StorageCore, rows: number): Promise<void> {
  if (rows === 0) return;
  const table = core.harness.path.catalog.tableByName("t");
  if (!table) throw new Error("Missing fixture table");
  if (table.primaryKey === null) throw new Error("Missing fixture PK");
  const index = table.primaryKey;
  const grant = await core.harness.governor.admit({
    label: "percentile/delete-fixture",
    minimumBytes: 1 << 20,
    spillable: true,
  });
  const arena = new OutputArena(grant, 2048),
    keys = new KeyBatch(grant);
  try {
    await core.write(async (statement) => {
      const count = Math.ceil(rows / 3),
        ids = arena.flat({ kind: "int32", bits: 32 }).vector,
        probes = arena.flat({ kind: "int32", bits: 32 }).vector,
        locators = arena.flat({ kind: "locator", tableId: table.id }).vector;
      if (
        ids.data.layout !== "i32" ||
        probes.data.layout !== "i32" ||
        locators.data.layout !== "f64"
      )
        throw new Error("Fixture locator layouts");
      ids.length = count;
      for (let i = 0; i < count; i++) ids.data.values[i] = 3 * i;
      encodeKeyBatch(
        "equality",
        [{ vector: ids, direction: "asc", nulls: "first" }],
        { length: count, selection: null },
        keys,
      );
      const matches = { probes: probes.data.values, locators: locators.data.values, count: 0 };
      expect(await statement.view.index(table.id, index).probe(keys, matches)).toEqual({
        done: true,
      });
      expect(matches.count).toBe(count);
      locators.length = matches.count;
      expect(
        await statement
          .table(table.id)
          .delete({ length: matches.count, selection: null, columns: [locators] }, null),
      ).toBe(count);
    });
  } finally {
    keys.close();
    arena.close();
    grant.close();
  }
}

const fractions = [0, 0.1, 0.25, 0.5, 0.9, 1, null, -1, 2, NaN] as const;
const patterns = ["general", "empty", "NULL", "zero", "special", "skew", "many", "huge"] as const;
it.each(seedsFor("next-grouped-percentiles", [0xe153, 0xe154]))(
  "selects per group exactly and falls back with bounded memory (seed %i)",
  async (seed) => {
    const pg = new PGlite(),
      core = await new StorageCore(8192).open();
    const reach = createReach("grouped percentiles", [
      "hash selection",
      "stored",
      "memory",
      "ascending",
      "descending",
      "NULL fraction",
      "range error",
      "all NULL",
      "empty",
      "signed zero",
      "NaN infinity",
      "overflow interpolation",
      "skew",
      "many groups fallback",
      "generic exact",
      "PG exact",
      "released",
      "deleted visibility",
    ]);
    const spy = vi.spyOn(PercentileOperator.prototype, "push");
    let fallback = false,
      governor = new Governor(128 << 20);
    const ordinary = createExecutor(),
      generic = createExecutor({ groupedPercentiles: false });
    core.executor.prepare = (plan) => {
      const prepared = (fallback ? generic : ordinary).prepare(plan);
      return { ...prepared, execute: (request) => prepared.execute({ ...request, governor }) };
    };
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...patterns),
          fc.integer({ min: 0, max: fractions.length - 1 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (pattern, index, descending, stored, deleted) => {
            reach.beginHistory();
            const fraction = fractions[index];
            if (fraction === undefined) throw Error("fraction");
            const ddl =
              "CREATE TABLE t(id integer PRIMARY KEY,g text,x double precision,y integer)";
            await core.execute("DROP TABLE IF EXISTS t");
            await core.execute(ddl);
            await pg.exec("DROP TABLE IF EXISTS t;" + ddl);
            const n =
              pattern === "empty"
                ? 0
                : pattern === "many"
                  ? 2201
                  : pattern === "skew"
                    ? 2305
                    : pattern === "huge"
                      ? 90
                      : 89;
            const special = [-Infinity, -2, -0, 0, 1, Infinity, NaN];
            const rows = Array.from({ length: n }, (_, id) => ({
              id,
              g:
                pattern === "huge"
                  ? "main"
                  : pattern === "many"
                    ? String(id)
                    : id % 17 === 0
                      ? null
                      : pattern === "skew" && id % 101 !== 0
                        ? "main"
                        : "g" + String(id % 7),
              x:
                pattern === "huge"
                  ? id % 2 === 0
                    ? -1e308
                    : 1e308
                  : pattern === "NULL"
                    ? null
                    : pattern === "zero"
                      ? -0
                      : pattern === "special"
                        ? (special[id % special.length] ?? 0)
                        : id % 13 === 0
                          ? null
                          : (((id * 73) % 101) - 50) / 8,
              y: id % 11 === 0 ? null : ((id * 47) % 103) - 51,
            }));
            if (n) {
              await core.writeBatch("insertBatch", "t", rows);
              for (let start = 0; start < n; start += 200) {
                const part = rows.slice(start, start + 200);
                await pg.query(
                  "INSERT INTO t VALUES " +
                    part
                      .map(
                        (_, i) =>
                          `($${String(i * 4 + 1)},$${String(i * 4 + 2)},$${String(i * 4 + 3)},$${String(i * 4 + 4)})`,
                      )
                      .join(","),
                  part.flatMap((r) => [
                    r.id,
                    r.g,
                    r.x === null ? null : Object.is(r.x, -0) ? "-0" : String(r.x),
                    r.y,
                  ]),
                );
              }
            }
            if (stored) {
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              reach.reach("stored");
            } else reach.reach("memory");
            if (deleted) {
              await deleteThirds(core, n);
              await pg.exec("DELETE FROM t WHERE id%3=0");
              reach.reach("deleted visibility");
            }
            const direction = descending ? "DESC" : "ASC";
            const sql = `SELECT g,percentile_cont($1) WITHIN GROUP(ORDER BY x ${direction}) c,percentile_disc($1) WITHIN GROUP(ORDER BY y ${direction}) d FROM t GROUP BY g ORDER BY g NULLS LAST`;
            const args = [fraction];
            let expected: unknown, code: unknown;
            try {
              expected = (await pg.query(sql, args)).rows;
            } catch (error) {
              code = (error as { code: unknown }).code;
            }
            fallback = false;
            spy.mockClear();
            governor = new Governor(128 << 20);
            if (code) {
              expect(code).toBe("22003");
              await expect(core.execute(sql, args)).rejects.toMatchObject({ sqlState: code });
              reach.reach("range error");
            } else {
              const actual = await core.execute(sql, args);
              if (!("batches" in actual)) throw new Error("Expected query rows");
              expect(oracleRows(actual)).toEqual(expected);
              if (n > 0 && pattern !== "many") expect(spy).toHaveBeenCalled();
              if (spy.mock.calls.length) reach.reach("hash selection");
              if (pattern === "many") {
                expect(spy).not.toHaveBeenCalled();
                reach.reach("many groups fallback");
              }
              fallback = true;
              const genericRows = await core.execute(sql, args);
              if (!("batches" in genericRows)) throw new Error("Expected generic rows");
              expect(oracleRows(genericRows)).toEqual(expected);
              reach.reach("generic exact");
              reach.reach("PG exact");
            }
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("released");
            reach.reach(descending ? "descending" : "ascending");
            if (fraction === null) reach.reach("NULL fraction");
            if (pattern === "NULL") reach.reach("all NULL");
            if (pattern === "empty") reach.reach("empty");
            if (pattern === "zero") reach.reach("signed zero");
            if (pattern === "special") reach.reach("NaN infinity");
            if (pattern === "huge") reach.reach("overflow interpolation");
            if (pattern === "skew") reach.reach("skew");
          },
        ),
        {
          seed,
          numRuns: 33,
          examples: [
            ["general", 3, false, true, false],
            ["general", 4, true, false, false],
            ["general", 6, false, true, false],
            ["general", 7, false, false, false],
            ["NULL", 3, false, true, false],
            ["empty", 3, true, false, false],
            ["zero", 3, false, false, false],
            ["special", 3, true, true, false],
            ["skew", 3, false, true, false],
            ["many", 3, true, true, false],
            ["huge", 3, false, true, false],
            ["general", 3, false, true, true],
            ["special", 3, true, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      spy.mockRestore();
      await core.close();
      await pg.close();
    }
  },
  120_000,
);
