/**
 * Join output demand must count every column an expression reads, wherever the expression sits
 * and however deeply the column is nested. PostgreSQL owns the result. An inner join omits the
 * build columns its consumer does not read (exec/plan.ts output demand), so a reference the
 * demand walker misses becomes a NULL or a lost match here.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, JOIN_DEMAND_EXPRESSION_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { joinGatherObserver } from "../exec/join.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

type Shape = (typeof JOIN_DEMAND_EXPRESSION_REACH)[number];
/** Each shape wraps an inner expression `x` (a build column, or another shape). */
const SHAPES: ReadonlyArray<readonly [Shape, (x: string) => string]> = [
  ["searched CASE", (x) => `CASE WHEN a.v > 0 THEN ${x} ELSE -1 END`],
  ["simple CASE", (x) => `CASE b.label WHEN 'x' THEN ${x} WHEN 'y' THEN 2 ELSE NULL END`],
  ["CASE condition", (x) => `CASE WHEN ${x} > 0 THEN 1 ELSE 0 END`],
  ["COALESCE", (x) => `COALESCE(${x}, a.v, 0)`],
  ["NULLIF", (x) => `NULLIF(${x}, 1)`],
  ["nested call", (x) => `COALESCE(NULLIF(-(${x}) * 2, 0), 7) - 1`],
  ["IN list", (x) => `CASE WHEN ${x} IN (1, 2, -1) THEN 1 ELSE 0 END`],
  ["cast", (x) => `CAST(${x} AS INTEGER)`],
  ["arithmetic", (x) => `${x} + a.v * 2`],
];
type Placement = (typeof JOIN_DEMAND_EXPRESSION_REACH)[number];
const PLACEMENTS: ReadonlyArray<readonly [Placement, (e: string) => string]> = [
  [
    "projection",
    (e) =>
      `SELECT a.id AS aid, b.id AS bid, ${e} AS x FROM a JOIN b ON a.k = b.k ORDER BY aid, bid`,
  ],
  [
    "residual",
    (e) =>
      `SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND ${e} > 0 ORDER BY aid, bid`,
  ],
  [
    "parent join key",
    (e) =>
      `SELECT a.id AS aid, d.id AS did FROM a JOIN b ON a.k = b.k JOIN d ON d.k = ${e} ORDER BY aid, did`,
  ],
  [
    "ORDER BY",
    (e) => `SELECT a.id AS aid FROM a JOIN b ON a.k = b.k ORDER BY ${e} NULLS FIRST, a.id, b.id`,
  ],
  [
    "GROUP BY",
    (e) =>
      `SELECT ${e} AS g, COUNT(*) AS n FROM a JOIN b ON a.k = b.k GROUP BY 1 ORDER BY g NULLS FIRST`,
  ],
  [
    "window argument",
    (e) =>
      `SELECT a.id AS aid, b.id AS bid, SUM(${e}) OVER (PARTITION BY a.k ORDER BY a.id, b.id) AS w FROM a JOIN b ON a.k = b.k ORDER BY aid, bid`,
  ],
];

const canonical = (rows: ReadonlyArray<Record<string, unknown>>, columns: readonly string[]) =>
  rows.map((row) =>
    columns.map((column) => {
      const value = row[column];
      if (value === null || value === undefined) return null;
      if (typeof value === "string") return value;
      return Number(value);
    }),
  );

let core: StorageCore;
let pg: PGlite;

beforeAll(async () => {
  core = await new StorageCore(8192).open();
  pg = new PGlite();
  await pg.waitReady;
  const random = mulberry32(0x7d3a);
  for (const name of ["a", "b", "d"]) {
    const ddl = `CREATE TABLE ${name} (id DOUBLE PRECISION PRIMARY KEY, k DOUBLE PRECISION, v DOUBLE PRECISION, label TEXT)`;
    await core.execute(ddl);
    await pg.exec(ddl);
    const rows = Array.from({ length: 12 }, (_, id) => ({
      id,
      // d's keys cover the values the shapes produce, so parent-key matches occur.
      k: id % 5 === 0 ? null : name === "d" ? (id % 9) - 3 : id % 3,
      v: id % 4 === 0 ? null : Math.floor(random() * 7) - 3,
      label: id % 6 === 0 ? null : id % 2 === 0 ? "x" : "y",
    }));
    await core.writeBatch("insertBatch", name, rows);
    await pg.exec(
      `INSERT INTO ${name} VALUES ${rows
        .map(
          (r) =>
            `(${[r.id, r.k, r.v].map((v) => (v === null ? "NULL" : String(v))).join(",")},${r.label === null ? "NULL" : `'${r.label}'`})`,
        )
        .join(",")}`,
    );
  }
});
afterAll(async () => {
  await core.close();
  await pg.close();
});

async function same(sql: string): Promise<void> {
  const expected = await pg.query<Record<string, unknown>>(sql);
  const columns = expected.fields.map((field) => field.name);
  const result = await core.execute(sql);
  if (result.kind !== "rows" || !("batches" in result)) throw new Error(`No rows for ${sql}`);
  expect(canonical(oracleRows(result), columns), sql).toEqual(canonical(expected.rows, columns));
}

// Reviewer repros: a build column read only inside a CASE arm, in the projection and as the
// parent join's key.
it("keeps a build column read only inside a CASE arm of the projection", async () => {
  await same(
    "SELECT CASE WHEN a.v > 0 THEN b.v ELSE -1 END AS x, a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k ORDER BY aid, bid",
  );
});
it("keeps a build column read only inside a CASE parent join key", async () => {
  await same(
    "SELECT a.id AS aid, d.v AS dv FROM a JOIN b ON a.k = b.k JOIN d ON d.k = CASE WHEN b.v > 0 THEN b.v ELSE -1 END ORDER BY aid, dv NULLS FIRST",
  );
});

/** Every join's gathered build columns while `sql` runs (and its result still matches). */
async function gathered(sql: string): Promise<number[][]> {
  const seen: number[][] = [];
  joinGatherObserver.observe = (_relId, columns) => seen.push([...columns]);
  try {
    await same(sql);
  } finally {
    joinGatherObserver.observe = null;
  }
  return seen;
}

// The optimisation itself, pinned: losing output demand fails here, not only in a benchmark.
it("gathers only the build column a projection over the join reads", async () => {
  // b builds k (its key, reused from the probe), label (read only by the scan's filter) and v.
  // Only v is gathered for matched pairs.
  expect(
    await gathered(
      "SELECT a.id AS aid, b.v AS bv FROM a JOIN b ON a.k = b.k WHERE b.label = 'x' ORDER BY aid, bv NULLS FIRST",
    ),
  ).toEqual([[2]]);
});

for (const seed of seedsFor("next-join-demand-expressions", [0x7d3b, 0x7d3c]))
  it(`keeps every column an expression reads, in every placement (seed ${String(seed)})`, async () => {
    const reach = createReach("join demand expressions", JOIN_DEMAND_EXPRESSION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: SHAPES.length - 1 }),
        fc.integer({ min: -1, max: SHAPES.length - 1 }),
        fc.integer({ min: 0, max: PLACEMENTS.length - 1 }),
        async (outer, inner, placement) => {
          reach.beginHistory();
          const [outerName, wrap] = SHAPES[outer] ?? SHAPES[0] ?? ["searched CASE", (x) => x];
          const nested = inner >= 0 ? SHAPES[inner] : undefined;
          const expr = wrap(nested === undefined ? "b.v" : nested[1]("b.v"));
          const [where, build] = PLACEMENTS[placement] ?? PLACEMENTS[0] ?? ["projection", (e) => e];
          await same(build(expr));
          reach.reach(outerName);
          if (nested !== undefined) {
            reach.reach(nested[0]);
            reach.reach("nested shapes");
          }
          reach.reach(where);
          reach.reach("PG exact");
        },
      ),
      {
        seed,
        numRuns: 60,
        examples: SHAPES.flatMap((_, s) =>
          PLACEMENTS.map((_, p) => [s, -1, p] as [number, number, number]),
        ),
      },
    );
    reach.assertReached();
  }, 180_000);
