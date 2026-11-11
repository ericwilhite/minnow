/** PostgreSQL ranks count rows before a peer group; bounded selection must keep every peer. */
import { PGlite } from "@electric-sql/pglite";
import { beforeAll, afterAll, expect, it } from "vitest";
import { createReach, WINDOW_TOPK_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import {
  ArraySource,
  BIG,
  DBL,
  TEXT,
  INT,
  CollectSink,
  batchesOf,
  type Row,
} from "./join-fixtures.js";
import { drive } from "./pipeline.js";
import { WindowOperator, windowMinimumBytes, type WindowSpec } from "./window.js";

const reach = createReach("bounded window ranking", WINDOW_TOPK_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
function canonical(v: unknown): string {
  if (v === null) return "NULL";
  if (Object.is(v, -0)) return "-0";
  if (
    typeof v === "number" ||
    typeof v === "string" ||
    typeof v === "bigint" ||
    typeof v === "boolean" ||
    v === undefined
  )
    return String(v);
  throw new TypeError("expected scalar SQL value");
}
const sqlValue = (v: unknown, type: string): string =>
  v === null ? "NULL" : `'${canonical(v).replaceAll("'", "''")}'::${type}`;

for (const seed of seedsFor("next-window-topk", [0xb710, 0xb711]))
  it(`bounded ranking versus PostgreSQL (${String(seed)})`, async () => {
    const random = mulberry32(seed);
    for (const [type, sqlType, pool] of [
      [DBL, "float8", [null, NaN, Infinity, -Infinity, -0, 0, 1, 2, 3]],
      [
        BIG,
        "bigint",
        [
          null,
          -(1n << 63n),
          -(1n << 63n) + 1n,
          -(1n << 53n) - 1n,
          0n,
          (1n << 53n) + 1n,
          (1n << 53n) + 2n,
          (1n << 62n) + 1n,
          (1n << 62n) + 2n,
          (1n << 63n) - 1n,
        ],
      ],
    ] as const)
      for (const large of [false, true]) {
        const n = large ? 70000 : 67;
        const types = [TEXT, type, INT];
        const rows: Row[] = Array.from({ length: n }, (_, id) => [
          [null, "", "é", "😀"][id % 4] ?? null,
          large && type.kind === "int64"
            ? (1n << 53n) + BigInt(id)
            : (pool[Math.floor(random() * pool.length)] ?? null),
          id,
        ]);
        await pg.exec(
          `DROP TABLE IF EXISTS wt; CREATE TABLE wt(p text COLLATE "C", k ${sqlType}, id int)`,
        );
        for (let at = 0; at < rows.length; at += 1000)
          await pg.exec(
            `INSERT INTO wt VALUES ${rows
              .slice(at, at + 1000)
              .map(([p, k, id]) => `(${sqlValue(p, "text")},${sqlValue(k, sqlType)},${String(id)})`)
              .join(",")}`,
          );
        for (const fn of ["row_number", "rank"] as const)
          for (const direction of ["asc", "desc"] as const)
            for (const nulls of ["first", "last"] as const) {
              reach.beginHistory();
              const k = large ? 2 : 3;
              const noOrder = !large && fn === "rank" && direction === "asc" && nulls === "first";
              // ROW_NUMBER has a total order. RANK deliberately has large boundary peer groups.
              const orderBy = noOrder
                ? []
                : [
                    { column: 1, direction, nulls, collation: null },
                    ...(fn === "row_number"
                      ? [
                          {
                            column: 2,
                            direction: "asc" as const,
                            nulls: "last" as const,
                            collation: null,
                          },
                        ]
                      : []),
                  ];
              const expected = (
                await pg.query(
                  `SELECT p,k,id,r FROM (SELECT *, ${fn}() OVER(PARTITION BY p ${noOrder ? "" : `ORDER BY k ${direction} NULLS ${nulls}${fn === "row_number" ? ",id" : ""}`}) r FROM wt) t WHERE r<=${String(k)} ORDER BY id`,
                  [],
                  { rowMode: "array" },
                )
              ).rows;
              const governor = new Governor(128 << 20),
                spill = new MemorySpillService();
              const grant = await governor.admit({
                label: "bounded ranking",
                minimumBytes: windowMinimumBytes(types),
                spillable: true,
              });
              const sink = new CollectSink();
              const actor = new WindowOperator(
                {
                  topK: k,
                  types,
                  partitionBy: [0],
                  orderBy,
                  functions: [{ fn, arg: null, frame: null, type: BIG }],
                },
                { grant, spill, queryId: seed },
                sink,
              );
              try {
                await drive(
                  new ArraySource(
                    batchesOf(types, rows, {
                      batchRows: 256,
                      dictionary: true,
                      int64: "int64",
                      skip: 3,
                    }),
                  ),
                  actor,
                );
                await actor.closed;
                expect(actor.profile.rowsIn).toBe(n);
                expect(governor.usedBytes).toBe(0);
                expect(spill.bytesInUse).toBe(0);
                const actual = sink.rows.sort((a, b) => Number(a[2]) - Number(b[2]));
                expect(actual.length).toBe(expected.length);
                const mismatch = actual.findIndex((row, i) =>
                  row.some(
                    (v, c) =>
                      canonical(v) !== canonical((expected[i] as unknown[] | undefined)?.[c]),
                  ),
                );
                expect(
                  mismatch < 0
                    ? null
                    : { at: mismatch, actual: actual[mismatch], expected: expected[mismatch] },
                ).toBeNull();
                expect(actor.profile.boundedRanking).toBe(true);
                if (noOrder) reach.reach("all peers without ORDER BY");
                if (actor.profile.sort.compactions > 0) reach.reach("compaction");
                if (actor.profile.sort.rejected > 0) reach.reach("raw rejection");
                reach.reach(fn);
                reach.reach(direction === "asc" ? "ascending" : "descending");
                reach.reach(nulls === "first" ? "NULLS FIRST" : "NULLS LAST");
                reach.reach(type.kind === "float64" ? "non-finite keys" : "wide int64");
                reach.reach("NULL keys");
                reach.reach("C text partitions");
                reach.reach("selected dictionary");
                if (fn === "rank") {
                  reach.reach("boundary peers");
                }
              } finally {
                actor.abort(new Error("cleanup"));
                await actor.closed;
                expect(governor.usedBytes).toBe(0);
                expect(spill.bytesInUse).toBe(0);
                reach.reach("zero resources");
              }
            }
      }
  }, 120000);

for (const seed of seedsFor("next-window-topk-fallback", [0xb712, 0xb713]))
  it(`empty, global, framed and pressured ranking (${String(seed)})`, async () => {
    for (const [n, partition, framed, pressure] of [
      [0, false, false, false],
      [23, false, false, false],
      [10, false, false, false],
      [23, true, true, false],
      [5000, true, false, true],
    ] as const) {
      reach.beginHistory();
      const k = n === 10 ? 5 : 2,
        ties = 3 + (seed % 2);
      const types = [INT, DBL, TEXT],
        rows: Row[] = Array.from({ length: n }, (_, i) => [
          i % 3,
          pressure
            ? seed % 2 === 0
              ? NaN
              : Infinity
            : n === 10
              ? i < ties
                ? 0
                : i - ties + 1
              : i % 7 === 0
                ? NaN
                : i % 5 === 0
                  ? null
                  : i % 4,
          "x".repeat(pressure ? 512 : 2),
        ]);
      await pg.exec("DROP TABLE IF EXISTS wf;CREATE TABLE wf(p int,k float8,t text)");
      for (let at = 0; at < n; at += 500)
        await pg.exec(
          `INSERT INTO wf VALUES ${rows
            .slice(at, at + 500)
            .map(([p, k, t]) => `(${String(p)},${sqlValue(k, "float8")},${sqlValue(t, "text")})`)
            .join(",")}`,
        );
      const expected = (
        await pg.query(
          `SELECT * FROM (SELECT *,rank() OVER(${partition ? "PARTITION BY p " : ""}ORDER BY k RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING) r FROM wf) t WHERE r<=${String(k)}`,
          [],
          { rowMode: "array" },
        )
      ).rows;
      const spec: WindowSpec = {
        topK: k,
        types,
        partitionBy: partition ? [0] : [],
        orderBy: [{ column: 1, direction: "asc", nulls: "last", collation: null }],
        functions: [
          {
            fn: "rank",
            arg: null,
            type: BIG,
            frame: framed
              ? {
                  unit: "range",
                  start: { kind: "preceding", offset: 0 },
                  end: { kind: "following", offset: 0 },
                  exclude: "no-others",
                }
              : null,
          },
        ],
      };
      const governor = new Governor(64 << 20),
        spill = new MemorySpillService(),
        busy = await governor.admit({ label: "competitor", minimumBytes: 0, spillable: true }),
        grant = await governor.admit({
          label: "ranking",
          minimumBytes: windowMinimumBytes(types),
          spillable: true,
        });
      const sink = new CollectSink(),
        actor = new WindowOperator(spec, { grant, spill, queryId: seed }, sink);
      if (pressure)
        expect(busy.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes)).toBe(
          true,
        );
      try {
        await drive(
          new ArraySource(batchesOf(types, rows, { batchRows: 127, dictionary: true })),
          actor,
        );
        await actor.closed;
        const bag = (r: ReadonlyArray<readonly unknown[]>) =>
          r.map((row) => row.map(canonical).join("|")).sort();
        const actual = framed ? sink.rows.filter((row) => Number(row[3]) <= k) : sink.rows;
        expect(bag(actual)).toEqual(bag(expected as unknown[][]));
        if (n === 10) {
          expect(actual.some((row) => Number(row[3]) === ties + 1)).toBe(true);
          reach.reach("rank gaps");
        }
        expect(actor.profile.boundedRanking).toBe(!framed);
        if (n === 0) reach.reach("empty input");
        if (!partition) reach.reach("unpartitioned");
        if (framed) reach.reach("frame fallback");
        if (pressure) {
          expect(actor.profile.sort.runs).toBeGreaterThan(0);
          reach.reach("spill");
        }
      } finally {
        actor.abort(new Error("cleanup"));
        await actor.closed;
        busy.close();
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
      }
    }
  }, 120000);
