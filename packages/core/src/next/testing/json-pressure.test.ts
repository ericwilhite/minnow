/** Input-sized extraction must prepay result progress even when its document is a literal. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_LITERAL_PROGRESS_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { ResultSink } from "../exec/result-sink.js";
import { texts } from "../exec/kernels/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
  await core.execute("CREATE TABLE json_dummy (id INTEGER PRIMARY KEY)");
  await core.writeBatch("insertBatch", "json_dummy", [{ id: 1 }]);
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
it.each(seedsFor("next-json-literal-progress", [0xb652, 0xb653]))(
  "reserves large literal result copies under competing pressure (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON literal progress", JSON_LITERAL_PROGRESS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 355000, max: 375000 }),
        fc.boolean(),
        fc.constantFrom(1, 5),
        async (length, stored, columns) => {
          reach.beginHistory();
          const value = "汉".repeat(length),
            document = JSON.stringify({ n: value });
          const sql =
            "SELECT '{\"n\":\"small\"}'::json ->> 'n' AS j" + (stored ? " FROM json_dummy" : "");
          const lowered = core.lowerer.lower(sql, core.context());
          if (lowered.kind !== "query" || lowered.plan.root.op !== "project")
            throw new Error("Expected literal JSON project");
          const root = lowered.plan.root,
            expr = root.exprs[0];
          if (expr?.op !== "call" || expr.args[0]?.op !== "literal")
            throw new Error("Expected JSON getter literal");
          // Replace only the literal vector in a real lowered plan: large SQL text itself would
          // test parser performance rather than the executor's immutable IR input bound.
          const literal = expr.args[0],
            firstColumn = lowered.plan.columns[0],
            firstOutput = root.output[0];
          if (firstColumn === undefined || firstOutput === undefined)
            throw new Error("Missing JSON column");
          const plan = {
            ...lowered.plan,
            columns: Array.from({ length: columns }, (_, i) => ({
              ...firstColumn,
              name: "j" + String(i),
            })),
            root: {
              ...root,
              output: Array.from({ length: columns }, (_, i) => ({
                ...firstOutput,
                name: "j" + String(i),
              })),
              exprs: Array.from({ length: columns }, () => ({
                ...expr,
                args: [
                  { ...literal, value: texts([document], literal.type) },
                  ...expr.args.slice(1),
                ],
              })),
            },
          };
          const governor = new Governor(64 * 1024 * 1024);
          const resident = await governor.admit({
            label: "competing-query",
            minimumBytes: 0,
            spillable: true,
          });
          const originalPrepare = core.executor.prepare.bind(core.executor);
          const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((input) => {
            const prepared = originalPrepare(input);
            return {
              ...prepared,
              execute: (request) => prepared.execute({ ...request, governor }),
            };
          });
          const lower = vi.spyOn(core.lowerer, "lower").mockReturnValue({ ...lowered, plan });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked below with the actual receiver via .call
          const originalPush = ResultSink.prototype.push;
          let pressured = false;
          const push = vi.spyOn(ResultSink.prototype, "push").mockImplementation(function (
            this: ResultSink,
            batch,
            transfer,
          ) {
            if (!pressured) {
              pressured = true;
              const room = governor.capBytes - governor.reserveBytes - governor.usedBytes;
              expect(resident.allocate(Math.max(0, room))).toBe(true);
            }
            return originalPush.call(this, batch, transfer);
          });
          try {
            const expected = (
              await pg.query(
                "SELECT " +
                  Array.from(
                    { length: columns },
                    (_, i) => "$1::json ->> 'n' AS j" + String(i),
                  ).join(","),
                [document],
              )
            ).rows;
            const actual = await core.execute(sql);
            if (!("batches" in actual)) throw new Error("Expected JSON result");
            expect(oracleRows(actual)).toEqual(expected);
            expect(pressured).toBe(true);
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          } finally {
            push.mockRestore();
            lower.mockRestore();
            prepare.mockRestore();
            resident.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach(stored ? "stored source plus literal" : "literal only");
          reach.reach("UTF8 over private default");
          reach.reach("competing pressure");
          reach.reach("returned all grants");
          reach.reach(columns === 1 ? "one output" : "many outputs");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [355000, false, 1],
          [355000, false, 5],
          [375000, true, 1],
          [375000, true, 5],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
