/** access/modify.ts memory contract: one statement, bounded charged input, no producer/writer hold-and-wait. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_PROGRESS_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { TableWriterImpl, StatementImpl } from "../write/transaction.js";
import { watchGrants } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-modify-progress", [0xb5ba]))(
  "completes large modifications at 64 MiB with retired producer reservations (seed %i)",
  async (seed) => {
    const reach = createReach("modification progress", MODIFY_PROGRESS_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 3 }), async (delta) => {
        reach.beginHistory();
        const core = new StorageCore(256);
        Object.defineProperty(core.harness, "governor", { value: new Governor(64 * 1024 * 1024) });
        const grants: Grant[] = [];
        watchGrants(core.harness.governor, (grant) => grants.push(grant));
        const tryAdmit = core.harness.governor.tryAdmit.bind(core.harness.governor);
        core.harness.governor.tryAdmit = (options) => {
          const grant = tryAdmit(options);
          if (grant) grants.push(grant);
          return grant;
        };
        await core.open();
        let checking = false;
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound with call in the spy.
        const originalUpdate = TableWriterImpl.prototype.update;
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound with call in the spy.
        const originalDelete = TableWriterImpl.prototype.delete;
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound with call in the spy.
        const originalEnd = StatementImpl.prototype.end;
        const check = () => {
          if (!checking) return;
          const producers = grants.filter(
            (grant) =>
              grant.label.includes("query#") &&
              /\/(scan|project|filter|values)#/u.test(grant.label),
          );
          expect(producers.length).toBeGreaterThan(0);
          expect(
            producers.every((grant) => grant.held === 0),
            "producer must retire before native writer entry",
          ).toBe(true);
          reach.reach("read grants retired");
        };
        const update = vi.spyOn(TableWriterImpl.prototype, "update").mockImplementation(function (
          this: TableWriterImpl,
          batch,
          written,
        ) {
          check();
          return originalUpdate.call(this, batch, written);
        });
        const remove = vi.spyOn(TableWriterImpl.prototype, "delete").mockImplementation(function (
          this: TableWriterImpl,
          batch,
          written,
        ) {
          check();
          return originalDelete.call(this, batch, written);
        });
        const end = vi.spyOn(StatementImpl.prototype, "end").mockImplementation(function (
          this: StatementImpl,
          signal,
        ) {
          if (checking) {
            // Reading touched is a test observation. Single-table plans must not retain IDs at all.
            expect([...this.touched.values()].reduce((n, set) => n + set.size, 0)).toBe(0);
          }
          return originalEnd.call(this, signal);
        });
        try {
          await core.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");
          for (let from = 0; from < 20000; from += 200)
            await core.execute(
              `INSERT INTO t VALUES ${Array.from({ length: 200 }, (_, j) => `(${String(from + j)},${String(from + j)})`).join(",")}`,
            );
          checking = true;
          const start = performance.now();
          const updated = await core.execute(`UPDATE t SET v = v + ${String(delta)}`);
          expect("rowCount" in updated ? updated.rowCount : null).toBe(20000);
          expect(performance.now() - start).toBeLessThan(30000);
          reach.reach("single UPDATE no target set");
          reach.reach("20000 updated");
          checking = false;
          const rows = await core.execute("SELECT COUNT(*) AS n FROM t WHERE v <> id");
          if (rows.kind !== "rows" || !("batches" in rows)) throw new Error("rows");
          expect(oracleRows(rows)).toEqual([{ n: 20000 }]);
          checking = true;
          const deleted = await core.execute("DELETE FROM t");
          expect("rowCount" in deleted ? deleted.rowCount : null).toBe(20000);
          reach.reach("single DELETE no target set");
          reach.reach("20000 deleted");
          expect(core.spill.bytesInUse).toBe(0);
          expect(core.harness.governor.peakBytes).toBeLessThanOrEqual(
            core.harness.governor.capBytes,
          );
          reach.reach("64 MiB");
          reach.reach("bounded spill workspace");
        } finally {
          update.mockRestore();
          remove.mockRestore();
          end.mockRestore();
          await core.close();
        }
        expect(core.harness.governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  },
  120000,
);
