/** access/modify.ts memory contract: one statement, bounded charged input, no producer/writer hold-and-wait. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_PROGRESS_REACH } from "../../testing/reach.js";
import { WriteCredits } from "../write/credits.js";
import { Governor, type Grant } from "../memory/governor.js";
import { TableWriterImpl, StatementImpl } from "../write/transaction.js";
import { watchGrants } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

const historyPaths = ["0", "1"] as const;
const campaigns = seedsFor("next-modify-progress", [0xb5ba]).map((seed) => ({
  seed,
  reach: createReach("modification progress", MODIFY_PROGRESS_REACH),
  completed: new Map<string, { count: number; delta: number }>(),
}));

afterAll(() => {
  for (const { seed, completed, reach } of campaigns) {
    for (const path of historyPaths)
      expect(
        completed.get(path)?.count,
        `one full history per 120000ms test budget: seed ${String(seed)}, path ${path}`,
      ).toBe(1);
    expect(completed.size, "both original generated histories must run").toBe(2);
    expect([...completed.keys()].sort(), "both original replay paths must run").toEqual(["0", "1"]);
    reach.assertReached();
  }
});

for (const path of historyPaths)
  for (const campaign of campaigns)
    it(`completes large modifications at 64 MiB with retired producer reservations (seed ${String(campaign.seed)}, history ${path})`, async () => {
      const { seed, reach, completed } = campaign;
      const property = fc.asyncProperty(fc.integer({ min: 1, max: 3 }), async (delta) => {
        reach.beginHistory();
        const core = new StorageCore(256);
        Object.defineProperty(core.harness, "governor", { value: new Governor(64 * 1024 * 1024) });
        // Labels are immutable. Remember exactly the accounts each assertion observes,
        // rather than retaining every native account and filtering it on every writer call.
        const producers: Grant[] = [],
          mergeGrants: Grant[] = [];
        const observeGrant = (grant: Grant) => {
          if (
            grant.label.includes("query#") &&
            /\/(scan|project|filter|values)#/u.test(grant.label)
          )
            producers.push(grant);
          if (grant.label.startsWith("merge t")) mergeGrants.push(grant);
        };
        watchGrants(core.harness.governor, observeGrant);
        const tryAdmit = core.harness.governor.tryAdmit.bind(core.harness.governor);
        core.harness.governor.tryAdmit = (options) => {
          const grant = tryAdmit(options);
          if (grant) observeGrant(grant);
          return grant;
        };
        await core.open();
        let checking = false,
          verifying = false,
          updateInputRows = 0;
        let protocolFailure: Error | undefined;
        const verifyStop = new AbortController();
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with the same native owner.
        const originalMetadata = WriteCredits.prototype.prepareMetadata;
        // Plain observers keep no mock call/result/context history or borrowed batches.
        const checkMerge = () => {
          if (verifying && core.harness.path.stats().levelZeroRuns === 0) {
            if (mergeGrants.some((g) => g.held > 0)) {
              protocolFailure = new Error(
                "Completed merge retained working credit across future metadata admission",
              );
              verifyStop.abort(protocolFailure);
              throw protocolFailure;
            }
            reach.reach("finished merge credit retired");
          }
        };
        WriteCredits.prototype.prepareMetadata = function (
          this: WriteCredits,
          bytes,
          signal,
          includesProgress,
        ) {
          checkMerge();
          return originalMetadata.call(this, bytes, signal, includesProgress);
        };
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with the same native owner.
        const originalNative = WriteCredits.prototype.prepareNativeMinimum;
        WriteCredits.prototype.prepareNativeMinimum = function (
          this: WriteCredits,
          bytes,
          signal,
          includesProgress,
        ) {
          checkMerge();
          return originalNative.call(this, bytes, signal, includesProgress);
        };
        // Completion is the ownership boundary even when an already-funded
        // verification query needs no later metadata preparation at all.
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with the same native owner.
        const originalRelease = WriteCredits.prototype.releaseMerge;
        WriteCredits.prototype.releaseMerge = function (this: WriteCredits, grant) {
          originalRelease.call(this, grant);
          if (grant.held !== 0 || grant.allocated !== 0 || grant.loaned !== 0) {
            // Record a background protocol failure without throwing into maintenance;
            // the verification query and final check report it on the test's own stack.
            protocolFailure = new Error("Completed merge did not return all working credit");
            verifyStop.abort(protocolFailure);
          } else reach.reach("finished merge credit retired");
        };
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound with call in the observer.
        const originalUpdate = TableWriterImpl.prototype.update;
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound with call in the observer.
        const originalDelete = TableWriterImpl.prototype.delete;
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound with call in the observer.
        const originalEnd = StatementImpl.prototype.end;
        const check = () => {
          if (!checking) return;
          expect(producers.length).toBeGreaterThan(0);
          expect(
            producers.every((grant) => grant.held === 0),
            "producer must retire before native writer entry",
          ).toBe(true);
          reach.reach("read grants retired");
        };
        TableWriterImpl.prototype.update = function (this: TableWriterImpl, batch, written) {
          check();
          if (checking) {
            updateInputRows += batch.selection?.count ?? batch.length;
            expect(
              updateInputRows,
              "one snapshot pass bounds native UPDATE work",
            ).toBeLessThanOrEqual(20000);
          }
          return originalUpdate.call(this, batch, written);
        };
        TableWriterImpl.prototype.delete = function (this: TableWriterImpl, batch, written) {
          check();
          return originalDelete.call(this, batch, written);
        };
        StatementImpl.prototype.end = function (this: StatementImpl, signal) {
          if (checking) {
            // Reading touched is a test observation. Single-table plans must not retain IDs at all.
            expect([...this.touched.values()].reduce((n, set) => n + set.size, 0)).toBe(0);
          }
          return originalEnd.call(this, signal);
        };
        try {
          await core.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");
          for (let from = 0; from < 20000; from += 200)
            await core.execute(
              `INSERT INTO t VALUES ${Array.from({ length: 200 }, (_, j) => `(${String(from + j)},${String(from + j)})`).join(",")}`,
            );
          checking = true;
          const updated = await core.execute(`UPDATE t SET v = v + ${String(delta)}`);
          expect("rowCount" in updated ? updated.rowCount : null).toBe(20000);
          // Progress is bounded native work, independent of host scheduling or V8 coverage.
          expect(updateInputRows, "all snapshot rows reach the writer exactly once").toBe(20000);
          reach.reach("single UPDATE no target set");
          reach.reach("20000 updated");
          checking = false;
          verifying = true;
          let rows;
          try {
            rows = await core.execute(
              "SELECT COUNT(*) AS n FROM t WHERE v <> id",
              [],
              verifyStop.signal,
            );
          } catch (error) {
            throw protocolFailure ?? error;
          } finally {
            verifying = false;
          }
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
          WriteCredits.prototype.prepareMetadata = originalMetadata;
          WriteCredits.prototype.prepareNativeMinimum = originalNative;
          WriteCredits.prototype.releaseMerge = originalRelease;
          TableWriterImpl.prototype.update = originalUpdate;
          TableWriterImpl.prototype.delete = originalDelete;
          StatementImpl.prototype.end = originalEnd;
          await core.close();
        }
        if (protocolFailure) throw protocolFailure;
        expect(core.harness.governor.usedBytes).toBe(0);
        expect(
          core.harness.path.maintenance.lastMergeFailure,
          "healthy completed merges must finalize without ownership errors",
        ).toBeNull();
        expect(core.harness.path.maintenance.merges).toBeGreaterThan(0);
        reach.reach("healthy merge completion");
        const previous = completed.get(path);
        completed.set(path, { count: (previous?.count ?? 0) + 1, delta });
        reach.reach(path === "0" ? "first seeded history" : "second seeded history");
      });
      await fc.assert(property, { seed, numRuns: 1, path });
      // Replay paths partition the original two draws without changing their RNG or shrinking.
      const expected = fc.sample(property, { seed, numRuns: 2 })[Number(path)];
      expect(completed.get(path)?.delta, "the original seeded draw is preserved").toBe(
        expected?.[0],
      );
    }, 120000);
