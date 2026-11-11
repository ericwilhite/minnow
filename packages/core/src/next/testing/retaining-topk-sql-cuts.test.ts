/** SQL-derived producer values keep their bounded parent through real native cuts. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, RETAINING_TOPK_SQL_CUT_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { WindowOperator, windowRetainingMinimumBytes } from "../exec/window.js";
import type { WindowCheckpoint } from "../exec/window-checkpoint.js";
import {
  INT,
  DBL,
  TEXT,
  CollectSink,
  batchesOf,
  type Value,
  type Row,
} from "../exec/join-fixtures.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
function scalar(value: unknown): Value {
  if (
    value === null ||
    typeof value === "number" ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  )
    return value;
  throw Error("Unexpected SQL-derived scalar");
}
function compare(a: Value | undefined, b: Value | undefined): number {
  if (a === b) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  if (typeof a === "number" && typeof b === "number") return a - b;
  if (typeof a === "string" && typeof b === "string") return a < b ? -1 : 1;
  throw Error("Unexpected ordering key");
}
for (const seed of seedsFor("next-retaining-topk-sql-cuts", [0xb7d4, 0xb7d5]))
  it(`keeps SQL-derived bounded state across forced cuts (${String(seed)})`, async () => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach(
      `SQL-derived top-k cuts ${String(seed)}`,
      RETAINING_TOPK_SQL_CUT_REACH,
    );
    const random = mulberry32(seed);
    try {
      await pg.waitReady;
      const ddl =
        "CREATE TABLE source(id INTEGER,p INTEGER,k INTEGER,v DOUBLE PRECISION,t TEXT,doc TEXT)";
      await core.execute(ddl);
      await pg.exec(ddl);
      const data = Array.from({ length: 40 }, (_, id) => ({
        id,
        p: id % 11 === 0 ? null : id % 3,
        k: id % 7 === 0 ? null : Math.floor(random() * 4),
        v: [1e16, 1, -1e16, 2][id % 4] ?? 0,
        t: `日本-${String(id)}`,
        doc: JSON.stringify({ p: id % 11 === 0 ? null : id % 3 }),
      }));
      await core.writeBatch("insertBatch", "source", data);
      for (const row of data)
        await pg.query("INSERT INTO source VALUES($1,$2,$3,$4,$5,$6)", [
          row.id,
          row.p,
          row.k,
          row.v,
          row.t,
          row.doc,
        ]);
      for (const producer of ["hybrid", "JSONB"] as const) {
        const source =
          producer === "hybrid"
            ? "SELECT id,p,k,sum(v) v,min(t) t FROM source GROUP BY id,p,k"
            : "SELECT id,doc::jsonb->>'p' p,k,v,t FROM source";
        const input = `SELECT id,p,k,v,t FROM(${source})q ORDER BY id`;
        const result = await core.execute(input);
        if (!("batches" in result)) throw Error("No source rows");
        const sourceRows = oracleRows(result);
        expect(sourceRows).toEqual((await pg.query(input)).rows);
        const rows: Row[] = sourceRows.map((row) => [
          scalar(row.id),
          scalar(row.p),
          scalar(row.k),
          scalar(row.v),
          scalar(row.t),
        ]);
        for (const fn of ["rank", "row_number"] as const)
          for (const cut of [0, 1, 20, 39, 40]) {
            reach.beginHistory();
            reach.reach(producer);
            reach.reach(fn);
            reach.reach(cut === 0 ? "empty cut" : "nonempty cut");
            const types = [INT, producer === "JSONB" ? TEXT : INT, INT, DBL, TEXT];
            const governor = new Governor(64 << 20),
              spill = new MemorySpillService(),
              sink = new CollectSink(2);
            const order = [
              { column: 2, direction: "asc" as const, nulls: "last" as const, collation: null },
              ...(fn === "row_number"
                ? [
                    {
                      column: 0,
                      direction: "asc" as const,
                      nulls: "last" as const,
                      collation: null,
                    },
                  ]
                : []),
            ];
            const make = async (extra = 0) =>
              new WindowOperator(
                {
                  checkpointable: true,
                  topK: 3,
                  types,
                  partitionBy: [1],
                  orderBy: order,
                  functions: [{ fn, arg: null, frame: null, type: { kind: "int64" } }],
                },
                {
                  grant: await governor.admit({
                    label: "SQL-derived top-k",
                    minimumBytes: windowRetainingMinimumBytes(types) + extra,
                    spillable: true,
                  }),
                  spill,
                  queryId: 1,
                },
                sink,
              );
            let actor = await make(),
              saved: WindowCheckpoint | undefined;
            const push = async (from: number, to: number) => {
              for (const batch of batchesOf(types, rows.slice(from, to), {
                dictionary: true,
                skip: 3,
                batchRows: 9,
              }))
                if (actor.push(batch) === "blocked") await actor.ready();
            };
            const rotate = async () => {
              expect(sink.rows).toHaveLength(0);
              saved = await actor.checkpoint();
              actor.abort(Error("retire"));
              await actor.closed;
              actor = await make(saved.minimumBytes);
              await actor.restore(saved);
              saved = undefined;
              expect(actor.profile.boundedRanking).toBe(true);
              reach.reach("bounded after restore");
            };
            try {
              await push(0, cut);
              await rotate();
              const second = Math.min(rows.length, cut + 1);
              await push(cut, second);
              await rotate();
              reach.reach("two cuts");
              await push(second, rows.length);
              await actor.finish();
              await actor.closed;
              const actual = sink.rows.map((row) =>
                row.map((value) => (typeof value === "bigint" ? Number(value) : value)),
              );
              actual.sort(
                (a, b) => compare(a[1], b[1]) || compare(a[2], b[2]) || compare(a[0], b[0]),
              );
              const sql = `SELECT id,p,k,v,t,r FROM(SELECT id,p,k,v,t,${fn}() OVER(PARTITION BY p ORDER BY k NULLS LAST${fn === "row_number" ? ",id" : ""}) r FROM(${source})s)w WHERE r<=3 ORDER BY p NULLS LAST,k NULLS LAST,id`;
              expect(actual).toEqual((await pg.query(sql, [], { rowMode: "array" })).rows);
              reach.reach("exact PostgreSQL");
            } finally {
              actor.abort(Error("cleanup"));
              await actor.closed;
              await saved?.close();
              expect(governor.usedBytes).toBe(0);
              expect(spill.bytesInUse).toBe(0);
              reach.reach("zero resources");
            }
          }
      }
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
