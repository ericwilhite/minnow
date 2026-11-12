/** Ordinary write -> flushed scan -> lowering -> NUMERIC kernel -> result frames vs PostgreSQL. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NUMERIC_FIXED_SQL_REACH } from "../../testing/reach.js";
const kinds = [
  "small",
  "int64 fit",
  "positive overflow",
  "negative overflow",
  "cancelling overflow",
] as const;
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
function decimal(c: bigint, scale: number): string {
  const s = (c < 0n ? -c : c).toString().padStart(scale + 1, "0");
  return (c < 0n ? "-" : "") + (scale === 0 ? s : `${s.slice(0, -scale)}.${s.slice(-scale)}`);
}
it.each(seedsFor("next-numeric-fixed-sql", [0xb753, 0xb754]))(
  "executes exact stored coefficients and checked fallback (seed %i)",
  async (seed) => {
    const reach = createReach("fixed NUMERIC SQL", NUMERIC_FIXED_SQL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...kinds),
        fc.integer({ min: 1, max: 1000 }),
        fc.boolean(),
        fc.constantFrom(65, 2049),
        async (kind, tail, selected, n) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open();
          try {
            await pg.exec("DROP TABLE IF EXISTS t");
            const typ = kind === "small" ? "numeric(12,2)" : "numeric(18,2)";
            const ddl = `CREATE TABLE t(id INTEGER,g INTEGER,v ${typ})`;
            await core.execute(ddl);
            await pg.exec(ddl);
            const rows = Array.from({ length: n }, (_, i) => {
              let c: bigint;
              if (kind === "small") c = BigInt((i % 2 === 0 ? 1 : -1) * (tail + (i % 11)));
              else {
                c = 900000000000000000n + BigInt(tail);
                if (
                  kind === "negative overflow" ||
                  (kind === "int64 fit" && Math.floor(i / 3) % 2 !== 0) ||
                  (kind === "cancelling overflow" && Math.floor(i / 36) % 2 !== 0)
                )
                  c = -c;
              }
              return {
                id: i,
                g: i % 3,
                v: i % 17 === 0 ? null : decimal(c, 2),
              };
            });
            await core.writeBatch("insertBatch", "t", rows);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            for (let at = 0; at < n; at += 256) {
              const part = rows.slice(at, at + 256);
              await pg.query(
                `INSERT INTO t VALUES ${part.map((_, i) => `($${String(i * 3 + 1)},$${String(i * 3 + 2)},$${String(i * 3 + 3)})`).join(",")}`,
                part.flatMap((r) => [r.id, r.g, r.v]),
              );
            }
            for (const grouped of [false, true]) {
              const sql = `SELECT ${grouped ? "g," : ""}sum(v) s,avg(v) a,min(v) m,max(v) x FROM t${selected ? " WHERE id%5<>1" : ""}${grouped ? " GROUP BY g ORDER BY g" : ""}`;
              const actual = await core.execute(sql);
              if (!("columns" in actual && "batches" in actual))
                throw new Error("Missing numeric rows");
              // The adapter renders exact decimal coefficients/scales; PG text output is the
              // independent authority. Native numeric-to-text CAST is outside this kernel.
              const oracle = sql.replace(/(sum|avg|min|max)\(v\)/g, "$&::text");
              expect(oracleRows(actual)).toEqual((await pg.query(oracle)).rows);
              reach.reach(grouped ? "grouped" : "scalar");
            }
            reach.reach(kind);
            if (selected) reach.reach("selection");
            if (n > 2048) reach.reach("multi-batch");
            reach.reach("flushed scan");
            reach.reach("PGlite exact");
          } finally {
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
            reach.reach("zero resources");
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          ...kinds.map(
            (k) => [k, 17, true, 65] as [(typeof kinds)[number], number, boolean, number],
          ),
          ["small", 3, false, 2049],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
