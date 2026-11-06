/** Float RANGE law: PostgreSQL float.c in_range_float8_float8; NULL handling is separate. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf, INT, DBL, BIG, type Row } from "./join-fixtures.js";
import { drive } from "./pipeline.js";
import { WindowOperator, windowMinimumBytes, type WindowSpec } from "./window.js";
import type { Frame } from "./window-frames.js";

const families = [
  "NaN peers",
  "positive infinity NULL adjacency",
  "negative infinity NULL adjacency",
  "mixed non-finites",
] as const;
const states = [
  ...families,
  "ascending",
  "descending",
  "NULLS FIRST",
  "NULLS LAST",
  "infinite offset",
  "non-finite FOLLOWING across ring slides",
  "prefix retained before peer start",
] as const;
const reach = createReach(
  "next-window-float-range",
  (["resident", "streamed"] as const).flatMap((path) => states.map((state) => `${path}: ${state}`)),
);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});

for (const seed of seedsFor("next-window-float-range", [0x572, 0x9a1]))
  it(`non-finite RANGE vs PostgreSQL seed ${String(seed)}`, async () => {
    const random = mulberry32(seed);
    for (const streamed of [false, true])
      for (const family of families)
        for (const direction of ["asc", "desc"] as const)
          for (const nulls of ["first", "last"] as const)
            for (const offset of [0, 1, Infinity]) {
              const path = streamed ? "streamed" : "resident";
              const spine: Array<number | null> =
                family === "NaN peers"
                  ? [1, NaN, NaN]
                  : family === "positive infinity NULL adjacency"
                    ? [Infinity, null, Infinity]
                    : family === "negative infinity NULL adjacency"
                      ? [-Infinity, null, -Infinity]
                      : [null, -Infinity, -1, -0, 0, 1, Infinity, NaN, NaN, null];
              const pool = [...spine];
              for (let i = 0, n = 1 + Math.floor(random() * 20); i < n; i++)
                spine.push(pool[Math.floor(random() * pool.length)] ?? null);
              const data: Row[] = spine.map((x, id) => [id, x]);
              const exclude =
                (["no-others", "current-row", "group", "ties"] as const)[
                  Math.floor(random() * 4)
                ] ?? "no-others";
              const frame: Frame = {
                unit: "range",
                start: { kind: "preceding", offset },
                end: { kind: "following", offset },
                exclude,
              };
              const spec: WindowSpec = {
                types: [INT, DBL],
                partitionBy: [],
                orderBy: [{ column: 1, direction, nulls, collation: null }],
                streamRows: streamed ? 1 : 100000,
                functions: [{ fn: "count_star", arg: null, type: BIG, frame }],
              };
              const values = data
                .map(
                  (row) =>
                    `(${String(row[0])},${row[1] === null ? "NULL::float8" : `'${String(row[1])}'::float8`})`,
                )
                .join(",");
              const bound = offset === Infinity ? "('Infinity'::float8)" : String(offset);
              const exclusion =
                exclude === "no-others" ? "NO OTHERS" : exclude.replace("-", " ").toUpperCase();
              const expected = (
                await pg.query<{ id: number; v: unknown }>(
                  `SELECT id, count(*) OVER (ORDER BY x ${direction} NULLS ${nulls} RANGE BETWEEN ${bound} PRECEDING AND ${bound} FOLLOWING EXCLUDE ${exclusion}) AS v FROM (VALUES ${values}) t(id,x) ORDER BY id`,
                )
              ).rows.map((row) => String(row.v));
              const governor = new Governor(64 << 20);
              const grant = await governor.admit({
                label: "range",
                minimumBytes: windowMinimumBytes(spec.types),
                spillable: true,
              });
              const spill = new MemorySpillService();
              const sink = new CollectSink();
              const op = new WindowOperator(spec, { grant, spill, queryId: 1 }, sink);
              await drive(new ArraySource(batchesOf(spec.types, data, { batchRows: 3 })), op);
              await op.closed;
              expect(
                sink.rows.sort((a, b) => Number(a[0]) - Number(b[0])).map((row) => String(row[2])),
                `${path} ${family} ${direction} ${nulls} ${String(offset)} ${exclude}`,
              ).toEqual(expected);
              expect(governor.usedBytes).toBe(0);
              expect(spill.bytesInUse).toBe(0);
              reach.beginHistory();
              reach.reach(`${path}: ${family}`);
              reach.reach(`${path}: ${direction === "asc" ? "ascending" : "descending"}`);
              reach.reach(`${path}: NULLS ${nulls === "first" ? "FIRST" : "LAST"}`);
              if (offset === Infinity) reach.reach(`${path}: infinite offset`);
            }
    for (const streamed of [false, true])
      for (const value of [NaN, Infinity, -Infinity]) {
        const count = 4097 + Math.floor(random() * 1024);
        const direction = random() < 0.5 ? "asc" : "desc";
        const nulls = Number.isNaN(value) ? "first" : random() < 0.5 ? "first" : "last";
        const data: Row[] = Array.from({ length: count + 2 }, (_, id) => [
          id,
          id === 0 || id === count + 1 ? null : value,
        ]);
        const spec: WindowSpec = {
          types: [INT, DBL],
          partitionBy: [],
          orderBy: [{ column: 1, direction, nulls, collation: null }],
          streamRows: streamed ? 1 : 100000,
          functions: [
            {
              fn: "count_star",
              arg: null,
              type: BIG,
              frame: {
                unit: "range",
                start: { kind: "following", offset: 1 },
                end: { kind: "unbounded-following", offset: null },
                exclude: "no-others",
              },
            },
            {
              fn: "count",
              arg: 0,
              type: BIG,
              frame: {
                unit: "range",
                start: { kind: "following", offset: 1 },
                end: { kind: "unbounded-following", offset: null },
                exclude: "no-others",
              },
            },
          ],
        };
        const expected = (
          await pg.query<{ id: number; v: unknown; c: unknown }>(
            `SELECT id,count(*) OVER (ORDER BY x ${direction} NULLS ${nulls} RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS v,count(id) OVER (ORDER BY x ${direction} NULLS ${nulls} RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS c FROM (SELECT id,CASE WHEN id IN (0,${String(count + 1)}) THEN NULL::float8 ELSE '${String(value)}'::float8 END AS x FROM generate_series(0,${String(count + 1)}) t(id)) t ORDER BY id`,
          )
        ).rows.map((row) => [String(row.v), String(row.c)]);
        const governor = new Governor(64 << 20);
        const grant = await governor.admit({
          label: "range",
          minimumBytes: windowMinimumBytes(spec.types, spec.functions.length),
          spillable: true,
        });
        const sink = new CollectSink();
        const spill = new MemorySpillService();
        const op = new WindowOperator(spec, { grant, spill, queryId: 1 }, sink);
        await drive(new ArraySource(batchesOf(spec.types, data, { batchRows: 97 })), op);
        await op.closed;
        expect(
          sink.rows
            .sort((a, b) => Number(a[0]) - Number(b[0]))
            .map((row) => [String(row[2]), String(row[3])]),
        ).toEqual(expected);
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
        if (streamed) expect(op.profile.streamedPartitions).toBeGreaterThan(0);
        reach.beginHistory();
        reach.reach(
          `${streamed ? "streamed" : "resident"}: non-finite FOLLOWING across ring slides`,
        );
        if (nulls === "first")
          reach.reach(`${streamed ? "streamed" : "resident"}: prefix retained before peer start`);
      }
  }, 600_000);
