/** End-to-end exact hash COUNT(DISTINCT), including selected bitmap and pair controls.
 * Law: PG nodeAgg.c "Spilling To Disk" and the lead's hybrid-hash ruling. Raw
 * distinct-bitmap.test.ts separately proves the bitmap/pair memory-saving relationship.
 */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { HybridAggregateOperator } from "../exec/agg/hybrid.js";
import { DistinctSet } from "../exec/agg/distinct.js";
import { createExecutor } from "../exec/create.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-executor-distinct-bitmap", [0xe245, 0xe246]))(
  "matches PG with word boundaries, filters and late groups (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite(),
      current = await currentEngine("memory");
    const reach = createReach("SQL distinct bitmap", [
      "memory",
      "stored",
      "late-groups",
      "empty",
      "filter",
      "numeric",
      "pair-control",
      "resident hybrid hash",
      "bitmap selected",
      "pairs selected",
    ]);
    const hybrid = vi.spyOn(HybridAggregateOperator.prototype, "finish"),
      modes: number[] = [];
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with .apply(this).
    const filter = DistinctSet.prototype.filter;
    const membership = vi.spyOn(DistinctSet.prototype, "filter").mockImplementation(function (
      this: DistinctSet,
      ...args
    ) {
      const result = filter.apply(this, args);
      modes.push(this.checkpointMode);
      return result;
    });
    await pg.waitReady;
    try {
      for (const name of ["s", "m", "x", "e"]) {
        const sql = `CREATE TABLE ${name}(g INTEGER,t TEXT,v DOUBLE PRECISION,d NUMERIC(12,2))`;
        await core.execute(sql);
        await pg.exec(sql);
        await current.run({ kind: "sql", sql });
        if (name === "e") continue;
        const rows = Array.from({ length: 5121 }, (_, i) => ({
          g: i % 31 === 0 ? null : name === "x" && i >= 4096 ? i % 100 : i % 10,
          t: i % 13 === 0 ? null : `k${String(i % 257)}-α`,
          v: i % 17 === 0 ? null : i % 257,
          d: i % 19 === 0 ? null : `${String(i % 257)}.${i % 2 ? "50" : "5"}`,
        }));
        await core.writeBatch("insertBatch", name, rows);
        await current.run({ kind: "insertBatch", table: name, rows });
        for (let from = 0; from < rows.length; from += 128) {
          const part = rows.slice(from, from + 128);
          await pg.query(
            `INSERT INTO ${name} VALUES ${part.map((_, k) => `($${String(k * 4 + 1)},$${String(k * 4 + 2)},$${String(k * 4 + 3)},$${String(k * 4 + 4)})`).join(",")}`,
            part.flatMap((r) => [r.g, r.t, r.v, r.d]),
          );
        }
        if (name === "s") {
          await core.harness.path.recycleLog();
          await core.harness.path.quiesce();
        }
      }
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          async (state, filtered, numeric) => {
            reach.beginHistory();
            const name = ["m", "s", "x", "e"][state];
            if (!name) throw new Error("fixture");
            const sql = `SELECT g,COUNT(DISTINCT t) AS nt,COUNT(DISTINCT v) AS nv${numeric ? ",COUNT(DISTINCT d) AS nd" : ""}${filtered ? ",COUNT(DISTINCT t) FILTER (WHERE v>100) AS nf" : ""} FROM ${name} GROUP BY g ORDER BY g NULLS FIRST`;
            const expected = (await pg.query<Record<string, unknown>>(sql)).rows;
            const old = await current.run({ kind: "sql", sql });
            expect(old.kind).toBe("rows");
            expect(old.rows).toEqual(expected);
            const hybridBefore = hybrid.mock.calls.length,
              modeBefore = modes.length;
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("SELECT");
            expect(hybrid.mock.calls.length).toBeGreaterThan(hybridBefore);
            expect(
              core.lastProfile?.operators.find((op) => op.op === "aggregate")?.spilledBytes,
            ).toBe(0);
            if (state !== 3) {
              expect(modes.slice(modeBefore)).toContain(1);
              reach.reach("bitmap selected");
            }
            const modeAfter = modes.length;
            reach.reach("resident hybrid hash");
            expect(oracleRows(result)).toEqual(expected);
            const candidate = core.lastProfile?.operators.find(
              (o) => o.op === "aggregate",
            )?.peakHeldBytes;
            const saved = core.executor.prepare.bind(core.executor);
            try {
              const legacyExecutor = createExecutor({ groupedDistinctBitmap: false });
              core.executor.prepare = legacyExecutor.prepare.bind(legacyExecutor);
              const control = await core.execute(sql);
              if (!("batches" in control)) throw new Error("control SELECT");
              expect(oracleRows(control)).toEqual(expected);
              const before = core.lastProfile?.operators.find(
                (o) => o.op === "aggregate",
              )?.peakHeldBytes;
              if (state < 2) {
                expect(candidate).toBeDefined();
                expect(before).toBeDefined();
                // Hash strategies legitimately reserve different state. Both stay
                // resident here; the control must actually select pair membership.
                expect(modes.length).toBeGreaterThan(modeAfter);
                expect(modes.slice(modeAfter).every((mode) => mode === 0)).toBe(true);
                expect(
                  core.lastProfile?.operators.find((op) => op.op === "aggregate")?.spilledBytes,
                ).toBe(0);
                reach.reach("pairs selected");
              }
            } finally {
              core.executor.prepare = saved;
            }
            reach.reach((["memory", "stored", "late-groups", "empty"] as const)[state] ?? "empty");
            if (filtered) reach.reach("filter");
            if (numeric) reach.reach("numeric");
            reach.reach("pair-control");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [0, true, true],
            [1, false, true],
            [2, true, false],
            [3, false, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      hybrid.mockRestore();
      membership.mockRestore();
      await current.close();
      await pg.close();
      await core.close();
      expect(core.harness.governor.usedBytes).toBe(0);
      expect(core.spill.bytesInUse).toBe(0);
    }
  },
  120000,
);
