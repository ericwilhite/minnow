/** Bounded fragments, rather than expanded-workspace multiples, fund wide row delivery. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_WIDE_REACH } from "../../testing/reach.js";
import type { ScalarExpr, Rel } from "../ir/ir.js";
import { Governor } from "../memory/governor.js";
import { ResultSink } from "../exec/result-sink.js";
import { JsonScan } from "../exec/kernels/json-scan.js";
import { JsonNode } from "../exec/kernels/json.js";
import { MemorySpillFile } from "../spill/memory-spill.js";
import type { Execution } from "../exec/executor.js";
import { jsonbByteProgress, jsonbWideDeadline } from "./jsonb-wide-deadline.test.fixture.js";
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
        fc.integer({ min: 24000, max: 76000 }),
        fc.boolean(),
        fc.constantFrom(1, 5),
        fc.boolean(),
        async (length, stored, columns, largeKey) => {
          reach.beginHistory();
          const document = JSON.stringify({ n: "汉".repeat(length) });
          const valueBytes = new TextEncoder().encode(document).byteLength - 8;
          expect(valueBytes, "the fixture must exercise multi-byte UTF8").toBeGreaterThan(length);
          reach.reach("multi-byte UTF8");
          // Keep the single-value MiB boundary and multi-column MiB fan-out, without
          // scanning five independent MiB documents in every instrumented history.
          if (columns === 5)
            expect(
              valueBytes,
              "coverage fixture repeats at most 256 KiB per extractor",
            ).toBeLessThanOrEqual(256 << 10);
          if (valueBytes >= 1 << 20) reach.reach("UTF8 megabyte");
          if (!largeKey && columns * valueBytes >= 1 << 20) reach.reach("UTF8 fan-out megabyte");
          expect(valueBytes).toBeGreaterThan(65536);
          reach.reach("multiple payload fragments");
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
          const executions = new Set<Execution>();
          const activity = new Set<() => void>();
          let byteAdvances = 0,
            fragmentAdvances = 0;
          const advance = () => {
            for (const notify of activity) notify();
          };
          const prepare = core.executor.prepare.bind(core.executor);
          const prep = vi.spyOn(core.executor, "prepare").mockImplementation((input) => {
            const p = prepare(input);
            return {
              ...p,
              execute: (request) => {
                const execution = p.execute({ ...request, governor });
                executions.add(execution);
                return execution;
              },
            };
          });
          const lower = vi.spyOn(core.lowerer, "lower").mockReturnValue({ ...lowered, plan });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- receiver supplied with call
          const scan = JsonScan.prototype.string;
          const scanning = vi.spyOn(JsonScan.prototype, "string").mockImplementation(function* (
            this: JsonScan,
          ) {
            yield* jsonbByteProgress(
              scan.call(this),
              () => this.at,
              () => {
                byteAdvances++;
                advance();
              },
            );
          });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- receiver supplied with apply
          const run = JsonNode.prototype.run;
          const evaluating = vi.spyOn(JsonNode.prototype, "run").mockImplementation(function* (
            this: JsonNode,
            ...args
          ) {
            const output = yield* run.apply(this, args);
            advance();
            return output;
          });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- receiver supplied with call
          const append = MemorySpillFile.prototype.append;
          const writing = vi
            .spyOn(MemorySpillFile.prototype, "append")
            .mockImplementation(async function (this: MemorySpillFile, page) {
              const ref = await append.call(this, page);
              fragmentAdvances++;
              advance();
              return ref;
            });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- receiver supplied with call
          const read = MemorySpillFile.prototype.read;
          const reading = vi
            .spyOn(MemorySpillFile.prototype, "read")
            .mockImplementation(async function (this: MemorySpillFile, ref) {
              const bytes = await read.call(this, ref);
              fragmentAdvances++;
              advance();
              return bytes;
            });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- receiver supplied with call
          const push = ResultSink.prototype.push;
          let pressured = false,
            pressureBytes = 0;
          const sink = vi.spyOn(ResultSink.prototype, "push").mockImplementation(function (
            this: ResultSink,
            batch,
            transfer,
          ) {
            if (!pressured) {
              pressured = true;
              pressureBytes = Math.max(
                0,
                governor.capBytes - governor.reserveBytes - governor.usedBytes,
              );
              expect(resident.allocate(pressureBytes)).toBe(true);
              expect(resident.allocated).toBe(pressureBytes);
            }
            advance();
            return push.call(this, batch, transfer);
          });
          let work: ReturnType<StorageCore["execute"]> | undefined;
          try {
            work = core.execute(sql, [key]);
            const actual = await jsonbWideDeadline(work, activity, (error) => {
              resident.close();
              for (const execution of executions) execution.cancel(error);
            });
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
            expect(byteAdvances).toBeGreaterThan(0);
            expect(fragmentAdvances).toBeGreaterThan(0);
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          } finally {
            resident.close();
            for (const execution of executions) execution.cancel();
            if (work) await Promise.allSettled([work]);
            scanning.mockRestore();
            evaluating.mockRestore();
            writing.mockRestore();
            reading.mockRestore();
            sink.mockRestore();
            lower.mockRestore();
            prep.mockRestore();
          }
          expect(governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach(stored ? "stored source" : "zero source");
          reach.reach(columns === 1 ? "one output" : "five outputs");
          if (largeKey) reach.reach("large parameter at cap");
          if (pressureBytes > 0) reach.reach("competing pressure");
          reach.reach("returned files and grants");
          reach.reach("observed byte progress");
          reach.reach("observed fragment progress");
        },
      ),
      {
        seed,
        numRuns: 9,
        examples: [
          [355000, false, 1, false],
          [76000, false, 5, false],
          [25000, true, 1, false],
          [76000, true, 5, false],
          [25000, false, 5, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
