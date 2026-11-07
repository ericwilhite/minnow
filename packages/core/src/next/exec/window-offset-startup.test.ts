/** RANGE invokes in_range only for non-NULL comparisons, on both evaluation paths. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { createReach, WINDOW_OFFSET_STARTUP_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf, INT, DBL, BIG, type Row } from "./join-fixtures.js";
import { drive } from "./pipeline.js";
import { WindowOperator, windowMinimumBytes, type WindowSpec } from "./window.js";

const reach = createReach("window offset startup", WINDOW_OFFSET_STARTUP_REACH, { minimum: 2 });
for (const seed of seedsFor("next-window-offset-startup", [0x7751, 0x7752]))
  it(`matches PG on NULL-only RANGE keys, seed ${String(seed)}`, async () => {
    const pg = new PGlite();
    await pg.waitReady;
    const random = mulberry32(seed);
    reach.beginHistory();
    try {
      for (const streamed of [false, true])
        for (const empty of [false, true])
          for (const offset of [-1 - Math.floor(random() * 4), NaN, null])
            for (const following of [false, true])
              for (const fn of ["count_star", "lag"]) {
                const rows: Row[] = empty
                  ? []
                  : Array.from({ length: 3 + Math.floor(random() * 5) }, (_, i) => [i, null]);
                const bounds = following
                  ? "CURRENT ROW AND $1 FOLLOWING"
                  : "$1 PRECEDING AND CURRENT ROW";
                const sql = `SELECT id,${fn === "lag" ? "lag(x)" : "count(*)"}OVER(ORDER BY x RANGE BETWEEN ${bounds})AS n FROM (SELECT i AS id,NULL::float8 AS x FROM generate_series(0,${String(rows.length - 1)})t(i))t ORDER BY id`;
                let authority;
                try {
                  authority = {
                    values: (await pg.query<{ n: unknown }>(sql, [offset])).rows.map((r) =>
                      String(r.n),
                    ),
                  };
                } catch (error) {
                  authority = { code: (error as { code: string }).code };
                }
                const spec: WindowSpec = {
                  types: [INT, DBL],
                  partitionBy: [],
                  orderBy: [{ column: 1, direction: "asc", nulls: "last", collation: null }],
                  streamRows: streamed ? 1 : 100000,
                  functions: [
                    {
                      fn,
                      arg: fn === "lag" ? 1 : null,
                      type: fn === "lag" ? DBL : BIG,
                      frame: {
                        unit: "range",
                        start: following
                          ? { kind: "current-row", offset: null }
                          : { kind: "preceding", offset },
                        end: following
                          ? { kind: "following", offset }
                          : { kind: "current-row", offset: null },
                        exclude: "no-others",
                      },
                    },
                  ],
                };
                const governor = new Governor(64 << 20);
                const grant = await governor.admit({
                  label: "offset startup",
                  minimumBytes: windowMinimumBytes(spec.types),
                  spillable: true,
                });
                const spill = new MemorySpillService();
                const sink = new CollectSink();
                const op = new WindowOperator(spec, { grant, spill, queryId: 1 }, sink);
                let actual;
                try {
                  await drive(new ArraySource(batchesOf(spec.types, rows, { batchRows: 2 })), op);
                  actual = {
                    values: sink.rows
                      .sort((a, b) => Number(a[0]) - Number(b[0]))
                      .map((r) => String(r[2])),
                  };
                } catch (error) {
                  actual = { code: (error as { code: string }).code };
                } finally {
                  await op.closed;
                }
                expect(actual, sql).toEqual(authority);
                expect(governor.usedBytes).toBe(0);
                expect(spill.bytesInUse).toBe(0);
                if (streamed && !empty && offset !== null)
                  expect(op.profile.streamedPartitions).toBeGreaterThan(0);
                reach.reach(streamed ? "streamed" : "resident");
                reach.reach(empty ? "empty" : "NULL only");
                reach.reach(
                  offset === null
                    ? "NULL 22004"
                    : Number.isNaN(offset)
                      ? "NaN accepted"
                      : "negative accepted",
                );
                reach.reach(following ? "FOLLOWING" : "PRECEDING");
                reach.reach(fn === "lag" ? "navigation" : "aggregate");
                reach.reach("released");
              }
    } finally {
      await pg.close();
    }
  }, 180_000);
it("reaches both offset evaluation paths", () => reach.assertReached());
