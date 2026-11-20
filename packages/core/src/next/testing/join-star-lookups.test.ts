/** Ordinary SQL must reach the lookup sink and preserve both PostgreSQL values
 * and native transition bits. Integer-quarter payloads make the PG sums exact.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  B9_STAR_LOOKUP_SQL_REACH,
  B9_STAR_FLOAT_DEMAND_REACH,
} from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { createExecutor } from "../exec/create.js";
import { StarLookups } from "../exec/join-star-lookups.js";
import { ExecutionError } from "../exec/errors.js";
const sql =
  "SELECT a.tier AS tier_a,b.tier AS tier_b,c.label AS label_c,COUNT(*) AS n,SUM(f.v) AS s FROM sf f JOIN sa a ON a.k=f.a JOIN sb b ON b.k=f.b JOIN sc c ON c.k=f.c WHERE a.active AND b.weight>1 AND c.tier='t1' GROUP BY a.tier,b.tier,c.label ORDER BY s DESC,tier_a,tier_b,label_c LIMIT 50";
let pg: PGlite;
interface PgStarRow {
  tier_a: string | null;
  tier_b: string | null;
  label_c: string | null;
  n: number | string | bigint;
  s: number | null;
}
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
function bits(rows: ReturnType<typeof oracleRows>) {
  return rows.map((row) =>
    Object.entries(row).map(([name, v]) => [
      name,
      typeof v === "number" ? Buffer.from(new Float64Array([v]).buffer).toString("hex") : v,
    ]),
  );
}
async function read(core: StorageCore, statement = sql) {
  const result = await core.execute(statement);
  if (!("batches" in result)) throw new Error("No star result");
  return oracleRows(result);
}
it.each(seedsFor("b9-star-lookup-sql", [0xb9a23, 0xb9a24]))(
  "SQL star uses one fact sink with exact native bits and PGlite groups (seed %i)",
  async (seed) => {
    const reach = createReach("B9 star SQL", B9_STAR_LOOKUP_SQL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 7 }),
        fc.integer({ min: 64, max: 128 }),
        fc.boolean(),
        async (cardinality, n, reversed) => {
          reach.beginHistory();
          const core = await new StorageCore(32).open();
          const dimensions = ["sa", "sb", "sc"].map((name, d) => ({
              name,
              rows: Array.from({ length: cardinality }, (_, i) => {
                const id = reversed ? cardinality - 1 - i : i;
                return {
                  k: `key-${String(id)}`,
                  tier: id % 3 === 0 ? null : id % 2 === 0 ? "t1" : "t0",
                  label: id % 3 === 0 ? null : (["", "λ", "\ufeffx"][id % 3] ?? ""),
                  weight: id % 4,
                  active: d !== 0 || id % 4 !== 0,
                };
              }),
            })),
            facts = Array.from({ length: n }, (_, id) => ({
              id,
              a: id % 17 === 0 ? null : `key-${String(id % (cardinality + 1))}`,
              b: `key-${String(Math.floor(id / 3) % (cardinality + 1))}`,
              c: `key-${String(Math.floor(id / 5) % (cardinality + 1))}`,
              v: id % 11 === 0 ? null : ((id % 31) - 15) / 4,
            }));
          try {
            await pg.exec("DROP TABLE IF EXISTS sf,sa,sb,sc");
            const ddl =
              "CREATE TABLE sf(id INTEGER PRIMARY KEY,a TEXT,b TEXT,c TEXT,v DOUBLE PRECISION)";
            await core.execute(ddl);
            await pg.exec(ddl);
            for (const d of dimensions) {
              const statement = `CREATE TABLE ${d.name}(k TEXT PRIMARY KEY,tier TEXT,label TEXT,weight DOUBLE PRECISION,active BOOLEAN)`;
              await core.execute(statement);
              await pg.exec(statement);
              await core.writeBatch("insertBatch", d.name, d.rows);
              await pg.query(
                `INSERT INTO ${d.name} SELECT * FROM json_to_recordset($1::json) x(k text,tier text,label text,weight double precision,active boolean)`,
                [JSON.stringify(d.rows)],
              );
            }
            await core.writeBatch("insertBatch", "sf", facts);
            await pg.query(
              "INSERT INTO sf SELECT * FROM json_to_recordset($1::json) x(id int,a text,b text,c text,v double precision)",
              [JSON.stringify(facts)],
            );
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const control = createExecutor({ starLookups: false }),
              prepare = vi
                .spyOn(core.executor, "prepare")
                .mockImplementation((plan) => control.prepare(plan));
            let before: Awaited<ReturnType<typeof read>>;
            try {
              before = await read(core);
            } finally {
              prepare.mockRestore();
            }
            const replay = vi.spyOn(StarLookups.prototype, "probe");
            let after: Awaited<ReturnType<typeof read>>;
            try {
              after = await read(core);
              expect(replay).toHaveBeenCalledTimes(1);
            } finally {
              replay.mockRestore();
            }
            expect(bits(after)).toEqual(bits(before));
            // PGlite's JS decoder strips a leading BOM. Read PostgreSQL UTF-8
            // bytes independently of that decoder, as the byte-key suites do.
            const reference = await pg.query<PgStarRow>(`SELECT
              encode(convert_to(q.tier_a,'UTF8'),'hex') AS tier_a,
              encode(convert_to(q.tier_b,'UTF8'),'hex') AS tier_b,
              encode(convert_to(q.label_c,'UTF8'),'hex') AS label_c,q.n,q.s
              FROM (${sql}) q ORDER BY q.s DESC,q.tier_a COLLATE "C",
              q.tier_b COLLATE "C",q.label_c COLLATE "C"`),
              text = (value: unknown) => {
                if (value === null) return null;
                if (typeof value !== "string") throw new Error("Missing PostgreSQL byte result");
                return Buffer.from(value, "hex").toString("utf8");
              },
              expected = reference.rows.map((row) => ({
                tier_a: text(row.tier_a),
                tier_b: text(row.tier_b),
                label_c: text(row.label_c),
                n: Number(row.n),
                s: row.s,
              }));
            expect(after).toEqual(expected);
            expect(bits(await read(core))).toEqual(bits(after));
            const unordered = sql.slice(0, sql.indexOf(" ORDER BY")),
              native = vi
                .spyOn(core.executor, "prepare")
                .mockImplementation((plan) => control.prepare(plan));
            let unsorted: Awaited<ReturnType<typeof read>>;
            try {
              unsorted = await read(core, unordered);
            } finally {
              native.mockRestore();
            }
            const unorderedReplay = vi.spyOn(StarLookups.prototype, "probe");
            try {
              expect(bits(await read(core, unordered))).toEqual(bits(unsorted));
              expect(unorderedReplay).toHaveBeenCalledTimes(1);
            } finally {
              unorderedReplay.mockRestore();
            }
            reach.reach("native unordered row order");
            reach.reach("ordinary SQL sink reached");
            reach.reach("native float bits");
            reach.reach("PostgreSQL groups");
            reach.reach(reversed ? "reversed source ordinals" : "forward source ordinals");
            if (
              after.some(
                (row) => row.tier_a === null || row.tier_b === null || row.label_c === null,
              )
            )
              reach.reach("NULL grouping values");
          } finally {
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
            reach.reach("all ownership returned");
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [7, 128, false],
          [7, 128, true],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  },
  180000,
);

it.each(seedsFor("b9-star-float-demand", [0xb9a29, 0xb9a2a]))(
  "native float bits and demanded errors survive lookup fusion (seed %i)",
  async (seed) => {
    const payloads = [
      [Number.MAX_VALUE, -Number.MAX_VALUE, Number.MAX_VALUE],
      [Number.MAX_VALUE, Number.MAX_VALUE, -Number.MAX_VALUE],
      [NaN, 1, -1],
      [Infinity, -Infinity, null],
      [-0, -0, null],
      [null, null, null],
    ] as const;
    const reach = createReach("B9 star float demand", B9_STAR_FLOAT_DEMAND_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 0, max: payloads.length - 1 }), async (index) => {
        reach.beginHistory();
        const core = await new StorageCore(32).open();
        try {
          await core.execute(
            "CREATE TABLE sf(id INTEGER PRIMARY KEY,a TEXT,b TEXT,c TEXT,v DOUBLE PRECISION)",
          );
          for (const name of ["sa", "sb", "sc"]) {
            await core.execute(
              `CREATE TABLE ${name}(k TEXT PRIMARY KEY,tier TEXT,label TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
            );
            await core.writeBatch(
              "insertBatch",
              name,
              Array.from({ length: 3 }, (_, id) => ({
                k: `k${String(id)}`,
                tier: "t1",
                label: "g",
                weight: 2,
                active: true,
              })),
            );
          }
          const values = payloads[index];
          if (!values) throw new Error("Missing generated payloads");
          await core.writeBatch(
            "insertBatch",
            "sf",
            Array.from({ length: 96 }, (_, id) => ({
              id,
              a: `k${String(id % 3)}`,
              b: `k${String(Math.floor(id / 3) % 3)}`,
              c: `k${String(Math.floor(id / 9) % 3)}`,
              v: values[id % values.length] ?? null,
            })),
          );
          await core.harness.path.recycleLog();
          await core.harness.path.quiesce();
          const outcome = async () => {
            try {
              return { rows: bits(await read(core)) };
            } catch (error) {
              if (!(error instanceof ExecutionError)) throw error;
              return {
                error: { code: error.code, sqlState: error.sqlState, message: error.message },
              };
            }
          };
          const control = createExecutor({ starLookups: false }),
            prepare = vi
              .spyOn(core.executor, "prepare")
              .mockImplementation((plan) => control.prepare(plan));
          let before: Awaited<ReturnType<typeof outcome>>;
          try {
            before = await outcome();
          } finally {
            prepare.mockRestore();
          }
          const replay = vi.spyOn(StarLookups.prototype, "probe");
          try {
            expect(await outcome()).toEqual(before);
            expect(replay).toHaveBeenCalledTimes(1);
          } finally {
            replay.mockRestore();
          }
          expect(await outcome()).toEqual(before);
          reach.reach("native bits or exact error");
          reach.reach("same plan deterministic");
          reach.reach(
            index < 2
              ? "finite extremes"
              : index === 2
                ? "NaN"
                : index === 3
                  ? "infinities"
                  : index === 4
                    ? "signed zero"
                    : "all NULL",
          );
        } finally {
          await core.close();
          expect(core.harness.governor.usedBytes).toBe(0);
        }
      }),
      { seed, numRuns: 8, examples: payloads.map((_, i) => [i]), endOnFailure: true },
    );
    reach.assertReached();
  },
  180000,
);
