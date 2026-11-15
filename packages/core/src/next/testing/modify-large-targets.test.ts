/** Large DML uses statement snapshots and charged/spilled repeat identities, at a bounded cap. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_LARGE_TARGETS_REACH } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { Governor } from "../memory/governor.js";
import { StatementImpl } from "../write/transaction.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
it.each(seedsFor("next-modify-large-targets", [0xb5c7]))(
  "spills repeated targets at64MiB (seed %i)",
  async (seed) => {
    const reach = createReach("large modify targets", MODIFY_LARGE_TARGETS_REACH);
    const scenario = async (n: number, reference: boolean) => {
      if (!reference) reach.beginHistory();
      const started = performance.now();
      const deadline = AbortSignal.timeout(120000);
      const core = new StorageCore(256),
        governor = new Governor(64 * 1024 * 1024);
      Object.defineProperty(core.harness, "governor", { value: governor });
      const admit = governor.tryAdmit.bind(governor);
      governor.tryAdmit = (options) => {
        if (options.label.startsWith("query#") && options.label.endsWith("/fixed-buffers"))
          expect(
            options.minimumBytes + governor.usedBytes,
            "unfunded large-modify minimum",
          ).toBeLessThanOrEqual(governor.capBytes);
        return admit(options);
      };
      await core.open();
      const pg = reference ? new PGlite() : null,
        old = reference ? await currentEngine("memory") : null;
      await pg?.waitReady;
      let delta = 0;
      const touched = vi.spyOn(StatementImpl.prototype, "touched", "get").mockImplementation(() => {
        throw new Error("unbounded repeat Set reached");
      });
      // eslint-disable-next-line @typescript-eslint/unbound-method -- Bound through call below.
      const originalEnd = StatementImpl.prototype.end;
      const ending = vi.spyOn(StatementImpl.prototype, "end").mockImplementation(function (
        this: StatementImpl,
        signal?: AbortSignal,
      ) {
        const rows = this.tx.undo.filter(
          (entry) => entry.kind === "rows" && entry.statement === this.ordinal,
        );
        const tables = new Set(
          rows.map((entry) => (entry.kind === "rows" ? entry.memtable : null)),
        );
        expect(rows.length, "one row undo range per active writer/memtable").toBeLessThanOrEqual(
          tables.size,
        );
        if (!reference) reach.reach("coalesced row undo");
        return originalEnd.call(this, signal);
      });
      const bag = (rows: readonly unknown[] | undefined) =>
        [...(rows ?? [])].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
      const all = async (sql: string) => {
        const a = await core.execute(sql, [], deadline);
        if ("returning" in a) {
          const rows = oracleRows(a.returning).sort((x, y) => Number(x.id) - Number(y.id));
          expect(rows).toEqual(
            Array.from({ length: n }, (_, k) => ({ id: k + 1, v: k + 1 + delta })),
          );
          if (pg && old) {
            const b = await pg.query(sql),
              c = await old.run({ kind: "sql", sql });
            expect(bag(rows)).toEqual(bag(b.rows));
            expect(bag(rows)).toEqual(bag(c.rows));
          }
        } else if (pg && old) {
          await pg.query(sql);
          await old.run({ kind: "sql", sql });
        }
        expect(core.spill.bytesInUse).toBe(0);
      };
      try {
        await all("CREATE TABLE big_target(id INTEGER PRIMARY KEY,v INTEGER)");
        await all("CREATE TABLE big_source(sid INTEGER PRIMARY KEY,tid INTEGER,v INTEGER)");
        for (let start = 0; start < n; start += 200) {
          const size = Math.min(200, n - start);
          await all(
            `INSERT INTO big_target VALUES ${Array.from({ length: size }, (_, k) => `(${String(start + k + 1)},0)`).join(",")}`,
          );
          await all(
            `INSERT INTO big_source VALUES ${Array.from({ length: size }, (_, k) => `(${String(start + k + 1)},${String(start + k + 1)},${String(start + k + 1)})`).join(",")}`,
          );
        }
        for (let start = 0; start < n; start += 200)
          await all(
            `INSERT INTO big_source VALUES ${Array.from({ length: Math.min(200, n - start) }, (_, k) => `(${String(n + start + k + 1)},${String(start + k + 1)},${String(-start - k - 1)})`).join(",")}`,
          );
        await core.harness.path.quiesce();
        await all(
          "UPDATE big_target SET v=big_source.v FROM big_source WHERE big_target.id=big_source.tid RETURNING big_target.id,big_target.v",
        );
        if (!reference) reach.reach("joined first match");
        delta = 1;
        await all(
          `INSERT INTO big_target SELECT sid,v FROM big_source WHERE sid<=${String(n)} ON CONFLICT(id) DO UPDATE SET v=EXCLUDED.v+1 RETURNING id,v`,
        );
        if (!reference) reach.reach("large upsert");
        await all(
          "DELETE FROM big_target USING big_source WHERE big_target.id=big_source.tid RETURNING big_target.id,big_target.v",
        );
        if (!reference) reach.reach("joined delete");
        const empty = await core.execute("SELECT * FROM big_target", [], deadline);
        if (empty.kind !== "rows" || !("batches" in empty)) throw new Error("rows");
        expect(oracleRows(empty)).toEqual([]);
        expect(touched).not.toHaveBeenCalled();
        expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
        if (!reference) reach.reach("no native repeat Set");
        if (!reference) reach.reach("64 MiB");
        if (!reference) reach.reach("complete cleanup");
      } finally {
        touched.mockRestore();
        ending.mockRestore();
        await core.close();
        await pg?.close();
        await old?.close();
      }
      expect(governor.usedBytes).toBe(0);
      expect(
        performance.now() - started,
        "one large history must complete within120s",
      ).toBeLessThan(120000);
      if (!reference) reach.reach("120s per history");
    };
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 8193, max: 22000 }), async (n) => {
        await scenario(4 + (n % 37), true);
        await scenario(n, false);
      }),
      { seed, numRuns: 2, examples: [[20000]], endOnFailure: true },
    );
    reach.assertReached();
  },
  // Complete histories retain their individual 120s cancellation/cleanup bound.
  // The outer allowance also includes their seeding and PostgreSQL/shipped comparisons.
  600000,
);
