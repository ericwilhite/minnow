/** PGlite evaluates constant key coercions before an indexed IN probe can satisfy LIMIT. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, QUERY_INDEX_COERCION_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { ViewIndex } from "../write/view.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { operators, withoutIndexes } from "./query-index.test-support.js";

async function outcome(run: () => Promise<unknown>) {
  try {
    return { error: null, rows: await run() };
  } catch (error) {
    let current: unknown = error;
    for (let depth = 0; depth < 4 && current instanceof Error; depth++) {
      const value = current as Error & { sqlState?: string; code?: string; cause?: unknown };
      const state = value.sqlState ?? value.code;
      if (state && /^[0-9A-Z]{5}$/u.test(state)) return { error: state, rows: null };
      current = value.cause;
    }
    throw error;
  }
}

const history = fc.record({
  n: fc.integer({ min: 4, max: 12 }),
  count: fc.integer({ min: 2, max: 8 }),
  position: fc.integer({ min: 0, max: 7 }),
  target: fc.constantFrom("SMALLINT", "INT"),
  overflow: fc.boolean(),
  firstMatch: fc.boolean(),
  nulls: fc.boolean(),
  limit: fc.integer({ min: 1, max: 3 }),
  placement: fc.constantFrom("memory", "stored", "reopen"),
});
for (const seed of seedsFor("next-query-index-coercion", [47811, 47812]))
  it(`validates streamed IN coercions before LIMIT and row rules seed${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("query index coercion", QUERY_INDEX_COERCION_REACH);
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(history, async (h) => {
          reach.beginHistory();
          const core = new StorageCore(h.placement === "memory" ? 256 : 8),
            governor = new Governor(32 << 20);
          Object.assign(core.harness, { governor });
          await core.open();
          let scan = false;
          const context = core.context.bind(core);
          core.context = () => {
            const c = context();
            return { ...c, catalog: scan ? withoutIndexes(c.catalog) : c.catalog };
          };
          try {
            await pg.exec("DROP TABLE IF EXISTS qcoerce");
            for (const sql of [
              "CREATE TABLE qcoerce(id INT PRIMARY KEY,v INT,CONSTRAINT positive_v CHECK(v>=0))",
              `INSERT INTO qcoerce VALUES ${Array.from({ length: h.n }, (_, i) => `(${String(i + 1)},${String(i + 1)})`).join(",")}`,
            ]) {
              await pg.exec(sql);
              await core.execute(sql);
            }
            if (h.placement !== "memory") {
              await core.harness.path.quiesce();
              await core.harness.path.recycleLog();
            }
            if (h.placement === "reopen") await core.reopen();
            const position = h.position % h.count;
            const params = Array.from({ length: h.count }, (_, i) =>
              i === position
                ? h.overflow
                  ? h.target === "SMALLINT"
                    ? 40000
                    : 5000000000
                  : 1 + (i % h.n)
                : h.nulls && i > 0 && i % 3 === 0
                  ? null
                  : h.firstMatch
                    ? 1 + (i % h.n)
                    : 1000 + i,
            );
            const keys = params.map((_, i) =>
              i === position ? `$${String(i + 1)}::NUMERIC::${h.target}` : `$${String(i + 1)}`,
            );
            const predicate = `id IN(${keys.join(",")})`;
            const sql = `SELECT id,v FROM qcoerce WHERE ${predicate}${h.overflow ? "" : " ORDER BY id"} LIMIT ${String(h.limit)}`;
            const expected = await outcome(async () => (await pg.query(sql, params)).rows);
            expect(expected.error).toBe(h.overflow ? "22003" : null);
            for (const forced of [true, false]) {
              scan = forced;
              const lowered = core.lowerer.lower(sql, core.context());
              if (lowered.kind !== "query") throw Error("query required");
              expect(operators(lowered.plan.root).includes("keyLookup")).toBe(!forced);
              const probe =
                h.overflow && !forced ? vi.spyOn(ViewIndex.prototype, "planProbe") : null;
              try {
                const actual = await outcome(async () => {
                  const result = await core.execute(sql, params, AbortSignal.timeout(30000));
                  if (!("batches" in result)) throw Error("rows required");
                  return oracleRows(result);
                });
                expect(actual).toEqual(expected);
                if (probe) {
                  expect(probe).not.toHaveBeenCalled();
                  reach.reach("before any key probe");
                }
              } finally {
                probe?.mockRestore();
              }
              reach.reach(forced ? "forced scan" : "index path");
            }
            scan = false;
            if (h.overflow) {
              const modify = `UPDATE qcoerce SET v=-1 WHERE ${predicate} RETURNING id`;
              const pgError = await outcome(async () => pg.query(modify, params));
              expect(pgError.error).toBe("22003");
              const actual = await outcome(async () => core.execute(modify, params));
              expect(actual.error).toBe("22003");
              reach.reach("modify error order");
              const rows = await core.execute("SELECT id,v FROM qcoerce ORDER BY id");
              if (!("batches" in rows)) throw Error("rows required");
              expect(oracleRows(rows)).toEqual(
                (await pg.query("SELECT id,v FROM qcoerce ORDER BY id")).rows,
              );
              reach.reach("unchanged rows after error");
            } else reach.reach("ordered result");
            reach.reach(position === 0 ? "cast first" : "cast later");
            reach.reach(h.target === "SMALLINT" ? "smallint" : "integer");
            reach.reach(h.overflow ? "22003" : "success");
            reach.reach(h.firstMatch ? "matching prefix" : "no early match");
            if (h.nulls) reach.reach("NULL keys");
            reach.reach(h.placement);
            reach.reach("LIMIT");
            reach.reach("PGlite");
          } finally {
            await core.close();
            expect(governor.usedBytes).toBe(0);
            expect(governor.loanedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero ownership");
          }
        }),
        {
          seed,
          numRuns: 8,
          endOnFailure: true,
          examples: [
            [
              {
                n: 6,
                count: 2,
                position: 1,
                target: "SMALLINT",
                overflow: true,
                firstMatch: true,
                nulls: false,
                limit: 1,
                placement: "memory",
              },
            ],
            [
              {
                n: 8,
                count: 3,
                position: 2,
                target: "INT",
                overflow: true,
                firstMatch: true,
                nulls: false,
                limit: 2,
                placement: "reopen",
              },
            ],
            [
              {
                n: 5,
                count: 4,
                position: 0,
                target: "SMALLINT",
                overflow: false,
                firstMatch: false,
                nulls: true,
                limit: 1,
                placement: "stored",
              },
            ],
            [
              {
                n: 7,
                count: 5,
                position: 3,
                target: "INT",
                overflow: false,
                firstMatch: true,
                nulls: false,
                limit: 3,
                placement: "memory",
              },
            ],
            [
              {
                n: 6,
                count: 2,
                position: 0,
                target: "INT",
                overflow: true,
                firstMatch: false,
                nulls: false,
                limit: 1,
                placement: "stored",
              },
            ],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 180000);
