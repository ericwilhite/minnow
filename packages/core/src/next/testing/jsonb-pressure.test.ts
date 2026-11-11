/** Re-admission must queue after all producer credit retires; cancellation owns saved files. */
import { expect, it, vi, beforeAll, afterAll } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_PRESSURE_REACH } from "../../testing/reach.js";
import { PGlite } from "@electric-sql/pglite";
import { Governor, type Grant } from "../memory/governor.js";
import type { Execution } from "../exec/executor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
function deferred() {
  let resolve: () => void = () => {
    throw new Error("Not initialized");
  };
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
it.each(seedsFor("next-jsonb-pressure", [0xb686, 0xb687]))(
  "retires JSON producer before admission wait (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB phase pressure", JSONB_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer({ min: 300, max: 3000 }),
        fc.boolean(),
        fc.boolean(),
        async (cancel, exponent, except, largeKey) => {
          reach.beginHistory();
          // Every native history still owns a fresh core and governor. The PG authority
          // runs only read-only SELECTs, so it needs no database reset or repeated WASM boot.
          const core = await new StorageCore().open();
          const governor = new Governor(32 << 20),
            native = governor.admitGroup.bind(governor);
          const reached = deferred();
          let holder: Grant | undefined,
            execution: Execution | undefined,
            count = 0;
          governor.admitGroup = async (options) => {
            if (
              options.label.endsWith("fixed-buffers") &&
              options.idleCredit !== undefined &&
              ++count === 1
            ) {
              expect(options.idleCredit).toBeDefined();
              expect(governor.usedBytes).toBe(options.idleCredit.held);
              [holder] = await native({
                label: "competing native execution",
                idleCredit: options.idleCredit,
                includesProgress: true,
                grants: [{ label: "whole cap", minimumBytes: governor.capBytes, spillable: false }],
              });
              const waiting = native(options);
              reached.resolve();
              return waiting;
            }
            return native(options);
          };
          const original = core.executor.prepare.bind(core.executor);
          const spy = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
            const prepared = original(plan);
            return {
              ...prepared,
              execute: (request) => {
                execution = prepared.execute({ ...request, governor });
                return execution;
              },
            };
          });
          let sql = `SELECT ('{"n":1e${String(exponent)},"a":null,"pad":1e5000}'::jsonb -> $1::text)::text AS n`;
          if (except) sql = `SELECT '${"1" + "0".repeat(exponent)}'::text AS n EXCEPT (${sql})`;
          const key = largeKey ? "汉".repeat(400000) : "n";
          const parameters = [key];
          const work = core.execute(sql, parameters);
          void work.catch(() => undefined);
          try {
            await Promise.race([
              reached.promise,
              work.then(() => {
                throw new Error("No JSON admission checkpoint");
              }),
            ]);
            parameters[0] = largeKey ? "n" : "a";
            if (largeKey) reach.reach("large parameter fragments");
            reach.reach("owned parameters");
            reach.reach("complete producer retirement");
            reach.reach("FIFO wait");
            reach.reach("spill checkpoint");
            reach.reach(except ? "EXCEPT right" : "unary");
            expect(core.spill.bytesInUse).toBeGreaterThan(0);
            expect(governor.usedBytes).toBe(governor.capBytes);
            if (cancel) {
              reach.reach("queued cancellation");
              const reason = new Error("cancel queued JSON continuation");
              execution?.cancel(reason);
              await expect(work).rejects.toMatchObject({
                code: "MINNOW_QUERY_CANCELLED",
                sqlState: "57014",
              });
              expect(governor.usedBytes).toBe(holder?.held);
              holder?.close();
              const recovery = await core.execute(sql, [key]);
              reach.reach("recovery");
              if (!("batches" in recovery)) throw new Error("Expected recovery rows");
              expect(oracleRows(recovery)).toEqual((await pg.query(sql, [key])).rows);
            } else {
              holder?.close();
              reach.reach("resume");
              const result = await work;
              if (!("batches" in result)) throw new Error("Expected JSON result");
              expect(oracleRows(result)).toEqual((await pg.query(sql, [key])).rows);
            }
          } finally {
            holder?.close();
            execution?.cancel();
            await work.catch(() => undefined);
            spy.mockRestore();
            expect(core.spill.bytesInUse).toBe(0);
            expect(governor.usedBytes).toBe(0);
            reach.reach("returned files and grants");
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [false, 1000, false, false],
          [true, 1000, false, false],
          [false, 600, true, false],
          [true, 600, true, false],
          [false, 1000, false, true],
          [true, 1000, false, true],
          [false, 600, true, true],
          [true, 600, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
