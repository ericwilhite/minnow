/** Bounded fragments, rather than expanded-workspace multiples, fund wide row delivery. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_WIDE_REACH } from "../../testing/reach.js";
import type { ScalarExpr, Rel } from "../ir/ir.js";
import { Governor } from "../memory/governor.js";
import { ResultSink } from "../exec/result-sink.js";
import { texts } from "../exec/kernels/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let core: StorageCore, pg: PGlite;
beforeAll(async () => {
  core = await new StorageCore().open();
  pg = new PGlite();
  await pg.waitReady;
  await core.execute("CREATE TABLE jsonb_dummy(id INTEGER PRIMARY KEY)");
  await core.writeBatch("insertBatch", "jsonb_dummy", [{ id: 1 }]);
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
it.each(seedsFor("next-jsonb-wide", [0xb688, 0xb689]))(
  "funds exact UTF8 fan-out under pressure (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB bounded row delivery", JSONB_WIDE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 345000, max: 355000 }),
        fc.boolean(),
        fc.constantFrom(1, 5),
        fc.boolean(),
        async (length, stored, columns, largeKey) => {
          reach.beginHistory();
          const document = JSON.stringify({ n: "汉".repeat(length) });
          const sql =
            `SELECT '{"n":"small"}'::jsonb ->> $1::text AS j` + (stored ? " FROM jsonb_dummy" : "");
          const lowered = core.lowerer.lower(sql, core.context());
          if (lowered.kind !== "query" || lowered.plan.root.op !== "project")
            throw new Error("Expected JSONB projection");
          const root = lowered.plan.root;
          const replace = (expr: ScalarExpr): ScalarExpr => {
            if (
              expr.op === "literal" &&
              expr.type.kind === "text" &&
              expr.value.data.layout === "varlen" &&
              expr.value.data.bytes.length > 5
            )
              return { ...expr, value: texts([document], expr.type) };
            if (expr.op === "cast") return { ...expr, input: replace(expr.input) };
            if (expr.op === "call") return { ...expr, args: expr.args.map(replace) };
            return expr;
          };
          const first = root.exprs[0],
            output = root.output[0],
            column = lowered.plan.columns[0];
          if (!first || !output || !column) throw new Error("Missing JSONB output");
          const plan = {
            ...lowered.plan,
            root: {
              ...root,
              exprs: Array.from({ length: columns }, () => replace(first)),
              output: Array.from({ length: columns }, (_, i) => ({
                ...output,
                name: `j${String(i)}`,
              })),
            } satisfies Rel,
            columns: Array.from({ length: columns }, (_, i) => ({
              ...column,
              name: `j${String(i)}`,
            })),
          };
          const key = largeKey ? "k".repeat(8 << 20) : "n";
          const governor = new Governor((largeKey ? 20 : 32) << 20),
            resident = await governor.admit({
              label: "competing execution",
              minimumBytes: 0,
              spillable: true,
            });
          const prepare = core.executor.prepare.bind(core.executor);
          const prep = vi.spyOn(core.executor, "prepare").mockImplementation((input) => {
            const p = prepare(input);
            return { ...p, execute: (request) => p.execute({ ...request, governor }) };
          });
          const lower = vi.spyOn(core.lowerer, "lower").mockReturnValue({ ...lowered, plan });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- receiver supplied with call
          const push = ResultSink.prototype.push;
          let pressured = false;
          const sink = vi.spyOn(ResultSink.prototype, "push").mockImplementation(function (
            this: ResultSink,
            batch,
            transfer,
          ) {
            if (!pressured) {
              pressured = true;
              expect(
                resident.allocate(
                  Math.max(0, governor.capBytes - governor.reserveBytes - governor.usedBytes),
                ),
              ).toBe(true);
            }
            return push.call(this, batch, transfer);
          });
          try {
            const actual = await core.execute(sql, [key]);
            if (!("batches" in actual)) throw new Error("Expected JSONB rows");
            const expected = await pg.query(
              "SELECT " +
                Array.from(
                  { length: columns },
                  (_, i) => `$1::jsonb ->> $2::text AS j${String(i)}`,
                ).join(","),
              [document, key],
            );
            expect(oracleRows(actual)).toEqual(expected.rows);
            expect(pressured).toBe(true);
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          } finally {
            sink.mockRestore();
            lower.mockRestore();
            prep.mockRestore();
            resident.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach(stored ? "stored source" : "zero source");
          reach.reach(columns === 1 ? "one output" : "five outputs");
          if (largeKey) reach.reach("large parameter at cap");
          reach.reach("UTF8 megabyte");
          reach.reach("competing pressure");
          reach.reach("returned files and grants");
        },
      ),
      {
        seed,
        numRuns: 9,
        examples: [
          [345000, false, 1, false],
          [345000, false, 5, false],
          [355000, true, 1, false],
          [355000, true, 5, false],
          [345000, false, 5, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
