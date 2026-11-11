/** UNION ALL fixes a bag, not a set; native work tables preserve level/input order through spill. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RECURSIVE_SQL_REACH, RECURSIVE_SPILL_REACH } from "../../testing/reach.js";
import { SpillFiles } from "../exec/spill-pages.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { SimBackend, SimDisk } from "../storage/sim-disk.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
  await core.execute("CREATE TABLE recursive_seed(id INTEGER PRIMARY KEY,v DOUBLE PRECISION)");
  await pg.exec("CREATE TABLE recursive_seed(id INTEGER PRIMARY KEY,v DOUBLE PRECISION)");
  const rows = Array.from({ length: 6000 }, (_, id) => ({
    id,
    v: [1e16, 1, -1e16, 1][id % 4] ?? 0,
  }));
  await core.writeBatch("insertBatch", "recursive_seed", rows);
  for (let at = 0; at < rows.length; at += 500) {
    const part = rows.slice(at, at + 500);
    await pg.query(
      `INSERT INTO recursive_seed VALUES ${part.map((_, i) => `($${String(2 * i + 1)},$${String(2 * i + 2)})`).join(",")}`,
      part.flatMap((row) => [row.id, row.v]),
    );
  }
  await core.harness.path.recycleLog();
  await core.harness.path.quiesce();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
it.each(seedsFor("next-recursive-sql", [0xb66c, 0xb66d]))(
  "keeps duplicate/NULL seeds and every bounded step exactly (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Recursive SQL bags", RECURSIVE_SQL_REACH);
    const examples: Array<[Array<number | null>, number, number, number, number]> = [
      [[1, 1, null], 3, 1, -1, -5],
      [[], 2, 1, -1, -5],
      [[0], 0, 1, -1, -5],
      [[null, null], 2, -1, -1, -5],
      [[3, 1, -1], 6, 2, -1, -5],
      [[0, 1, 2, 3, 4, 5], 3, 1, 8, 3],
      [[null, 1], 2, 1, 0, 0],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.oneof(fc.integer({ min: -5, max: 5 }), fc.constant(null)), { maxLength: 8 }),
        fc.integer({ min: 0, max: 10 }),
        fc.integer({ min: -3, max: 3 }),
        fc.integer({ min: -1, max: 35 }),
        fc.integer({ min: -5, max: 5 }),
        async (values, depth, step, limit, threshold) => {
          reach.beginHistory();
          const base =
            values.length === 0
              ? "SELECT NULL::integer AS n,0 AS d,0 AS ord WHERE false"
              : `SELECT n,d,ord FROM(VALUES ${values.map((value, index) => `(${value === null ? "NULL::integer" : String(value)},0,${String(index)})`).join(",")}) q(n,d,ord)`;
          const source = `WITH RECURSIVE r(n,d,ord) AS(${base} UNION ALL SELECT n+(${String(step)}),d+1,ord FROM r WHERE d<${String(depth)} AND (n IS NULL OR n>=${String(threshold)})) SELECT n,d FROM r`;
          const sql = source + (limit < 0 ? " ORDER BY d,n NULLS LAST" : ` LIMIT ${String(limit)}`);
          // The work-table contract is level/input order. Pin PG to the explicit VALUES ordinal
          // only for a limited native stream; keep the ordinary unordered bag check unchanged.
          const authority = limit < 0 ? sql : source + ` ORDER BY d,ord LIMIT ${String(limit)}`;
          const expected = (await pg.query(authority)).rows,
            result = await core.execute(sql);
          if (!("batches" in result)) throw new Error("Expected recursive rows");
          expect(oracleRows(result), sql).toEqual(expected);
          reach.reach(depth === 0 ? "base only" : "step rows");
          if (values.length === 0) reach.reach("empty base");
          if (values.includes(null)) reach.reach("NULL seed");
          if (new Set(values).size < values.length) reach.reach("duplicate seed");
          if (values.length > 1) reach.reach("multiple seeds");
          if (step < 0) reach.reach("negative step");
          if (step === 0) reach.reach("zero value step");
          if (limit >= 0) reach.reach("limited demand");
          if (threshold > -5) reach.reach("filtered step");
          if (values.join(",") === "0,1,2,3,4,5" && limit === 8 && threshold === 3)
            reach.reach("later selected slot");
          reach.reach("exact PG");
        },
      ),
      { seed, numRuns: 50 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);
it.each(seedsFor("next-recursive-spill", [0xb66e, 0xb66f]))(
  "keeps FIFO rows and float SUM through prepaid spill under pressure (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Recursive spill", RECURSIVE_SPILL_REACH);
    const examples: Array<[number, number, boolean]> = [
      [3001, 2, false],
      [4096, 1, true],
      [5001, 3, true],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 3001, max: 5001 }),
        fc.integer({ min: 1, max: 3 }),
        fc.boolean(),
        async (count, depth, scratch) => {
          reach.beginHistory();
          const cap = 8 * 1024 * 1024,
            governor = new Governor(cap),
            resident = await governor.admit({
              label: "competing",
              minimumBytes: 0,
              spillable: true,
            });
          const backend = scratch ? new SimBackend(new SimDisk()) : null,
            lease = await backend?.acquire(),
            space = lease === undefined ? null : backend?.scratch(lease),
            spill = space ? new ScratchSpillService(space) : new MemorySpillService();
          const original = core.executor.prepare.bind(core.executor);
          const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
            const prepared = original(plan);
            return {
              ...prepared,
              execute: (request) => prepared.execute({ ...request, governor, spill }),
            };
          });
          let pressured = false;
          const appendPage = SpillFiles.prototype.append; // eslint-disable-line @typescript-eslint/unbound-method
          const append = vi.spyOn(SpillFiles.prototype, "append").mockImplementation(function (
            this: SpillFiles,
            ...args
          ) {
            if (!pressured) {
              // Leave 64 KiB for the existing aggregate output buffer; the recursive
              // working set and scratch write batching still have no optional growth.
              const remaining = Math.max(
                0,
                cap - governor.reserveBytes - governor.usedBytes - 65536,
              );
              expect(resident.allocate(remaining)).toBe(true);
              pressured = true;
            }
            return appendPage.call(this, ...args);
          });
          const source = `WITH RECURSIVE r(id,d,v) AS(SELECT id,0 AS d,v FROM recursive_seed WHERE id%6001<${String(count)} UNION ALL SELECT id,d+1,v FROM r WHERE d<${String(depth)}) `;
          try {
            const rows = await core.execute(source + "SELECT id,d,v FROM r");
            if (!("batches" in rows)) throw new Error("Expected recursive rows");
            // Native's documented work-table order is level then stored input order. Pin PG to that
            // order explicitly; unordered recursive SQL is not an accumulation-order authority.
            expect(oracleRows(rows)).toEqual(
              (await pg.query(source + "SELECT id,d,v FROM r ORDER BY d,id")).rows,
            );
            const profile = core.lastProfile?.operators.find(
              (operator) => operator.op === "recursive",
            );
            expect(profile?.spilledBytes).toBeGreaterThan(0);
            expect(profile?.spillPasses).toBeGreaterThan(0);
            expect(pressured).toBe(true);
            resident.free(resident.allocated);
            resident.release(resident.held);
            pressured = false;
            const sum = await core.execute(source + "SELECT SUM(v) AS s FROM r");
            if (!("batches" in sum)) throw new Error("Expected recursive sum");
            expect(oracleRows(sum)).toEqual(
              (await pg.query(source + "SELECT SUM(v ORDER BY d,id) AS s FROM r")).rows,
            );
            expect(pressured).toBe(true);
            reach.reach("competing pressure");
            reach.reach("FIFO exact rows");
            reach.reach("ordered PG SUM");
            reach.reach("spill");
          } finally {
            append.mockRestore();
            prepare.mockRestore();
            resident.close();
            if (spill instanceof ScratchSpillService) await spill.close();
            await lease?.release();
          }
          expect(spill.bytesInUse).toBe(0);
          expect(governor.usedBytes).toBe(0);
          expect(governor.peakBytes).toBeLessThanOrEqual(cap);
          reach.reach(scratch ? "scratch service" : "memory service");
          reach.reach("small cap");
          reach.reach("returned all bytes");
        },
      ),
      { seed, numRuns: 6 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);
it("has no shipped recursion-depth cap and honors demand at a selected work-table slot", async () => {
  const sql =
    "WITH RECURSIVE r(n) AS(SELECT 0 UNION ALL SELECT n+1 FROM r WHERE n<100005) SELECT COUNT(*) AS n FROM r";
  const result = await core.execute(sql);
  if (!("batches" in result)) throw new Error("Expected count");
  expect(oracleRows(result).map((row) => ({ n: String(row.n) }))).toEqual(
    (await pg.query("SELECT n::text AS n FROM(" + sql + ") q")).rows,
  );
  const prefix =
    "WITH RECURSIVE r(n,d) AS(SELECT n,0 AS d FROM(VALUES (0),(1),(2),(3)) q(n) UNION ALL SELECT n+10,d+1 FROM r WHERE d<3) SELECT n,d FROM r LIMIT 7";
  const actual = await core.execute(prefix);
  if (!("batches" in actual)) throw new Error("Expected prefix");
  const filtered =
    "WITH RECURSIVE r(n,d) AS(SELECT n,0 AS d FROM(VALUES (0),(1),(2),(3),(4),(5)) q(n) UNION ALL SELECT n+10,d+1 FROM r WHERE d=0 AND n>=3) SELECT n,d FROM r LIMIT 8";
  const sliced = await core.execute(filtered);
  if (!("batches" in sliced)) throw new Error("Expected filtered prefix");
  expect(oracleRows(sliced)).toEqual((await pg.query(filtered)).rows);
  // PG's implementation emits the same level/input order for this explicit VALUES seed.
  expect(oracleRows(actual)).toEqual((await pg.query(prefix)).rows);
}, 120000);
