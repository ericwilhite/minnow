/** Inline progress floors are not cell-width certificates for aggregate admission. */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, HASH_SOURCE_VALUE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import * as reads from "../page/read-bounds.js";
import { QueryRuntime } from "../exec/run.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
const boundSource = reads.boundSource;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const aggregateProgress = QueryRuntime.prototype.aggregateProgress;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("hash-source-value-bound", [0xb7c5, 0xb7c6]))(
  "keeps snapshot width facts above every decoded cell seed%i",
  async (seed) => {
    const core = await new StorageCore().open();
    const reach = createReach("hash source value", HASH_SOURCE_VALUE_REACH);
    let history = 0;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 32000, max: 120000 }),
          fc.constantFrom(2, 16),
          fc.boolean(),
          fc.boolean(),
          async (width, rows, unicode, multiple) => {
            reach.beginHistory();
            const name = `bound_${String(history++)}`;
            const value = unicode ? "汉".repeat(Math.floor(width / 3)) : "x".repeat(width);
            const input = Array.from({ length: rows }, (_, i) => ({
              a: value + String(i % 2),
              b: "b" + value + String(i % 2),
            }));
            await core.execute(`CREATE TABLE ${name}(a TEXT,b TEXT)`);
            await core.writeBatch("insertBatch", name, input);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const columns = multiple ? "a,b" : "a";
            const result = await core.execute(`SELECT ${columns} FROM ${name}`);
            if (result.kind !== "rows" || !("batches" in result))
              throw new Error("missing source rows");
            const canonical = (values: ReadonlyArray<Record<string, unknown>>) =>
              values.map((r) => JSON.stringify(r)).sort();
            expect(canonical(oracleRows(result))).toEqual(
              canonical(input.map((row) => (multiple ? row : { a: row.a }))),
            );
            // Written UTF-8 lengths are the authority; ordinary decode independently
            // checks the same snapshot before its metadata funds a hash transition.
            const actual = [
              Math.max(...input.map((row) => new TextEncoder().encode(row.a).length)),
              ...(multiple
                ? [Math.max(...input.map((row) => new TextEncoder().encode(row.b).length))]
                : []),
            ];
            const governor = new Governor(128 << 20),
              bounds: reads.ReadBound[] = [];
            const prepare = core.executor.prepare.bind(core.executor);
            let seen = false;
            try {
              // Isolate the row-bounded planning state without inventing byte facts.
              // The competing-cap route and enforced TableScan limit are proved by
              // executor-result-progress; this uses the real reader and real planner.
              vi.spyOn(reads, "boundSource").mockImplementation((view, source, ...rest) => {
                const record = (bound: reads.ReadBound) => {
                  bounds.push(bound);
                  return bound;
                };
                const bound = boundSource(view, { ...source, batchRows: 1 }, ...rest);
                return bound instanceof Promise ? bound.then(record) : record(bound);
              });
              vi.spyOn(QueryRuntime.prototype, "aggregateProgress").mockImplementation(function (
                this: QueryRuntime,
                ...a
              ) {
                if (this.request.governor === governor) {
                  expect(bounds.some((b) => b.batchRows === 1)).toBe(true);
                  for (const [index, key] of a[2].keys.entries()) {
                    if (key.op !== "bytes") throw new Error("missing certified source width");
                    expect(key.bytes).toBeGreaterThanOrEqual(actual[index] ?? 0);
                  }
                  seen = true;
                }
                return aggregateProgress.apply(this, a);
              });
              vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                const p = prepare(plan);
                return {
                  ...p,
                  execute: (r) => p.execute({ ...r, governor, signal: AbortSignal.timeout(30000) }),
                };
              });
              const grouped = await core.execute(
                `SELECT count(*) n FROM ${name} GROUP BY ${columns}`,
              );
              if (grouped.kind !== "rows" || !("batches" in grouped))
                throw new Error("missing grouped rows");
              expect(oracleRows(grouped).map((r) => r.n)).toEqual([rows / 2, rows / 2]);
              expect(seen).toBe(true);
              if (bounds.some((b) => b.progress === 65536 && actual.some((width) => width > 16384)))
                reach.reach("progress floor insufficient");
              if (Math.max(...actual) > 65536) reach.reach("large values");
              if (unicode) reach.reach("UTF8");
              if (rows === 16) reach.reach("repeated rows");
              if (multiple) reach.reach("multiple columns");
              expect(governor.peakBytes).toBeGreaterThan(0);
              reach.reach("charged sizing");
            } finally {
              vi.restoreAllMocks();
              expect(governor.usedBytes).toBe(0);
            }
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero");
          },
        ),
        {
          seed,
          numRuns: 14, // Four pinned boundaries plus ten random histories.
          endOnFailure: true,
          examples: [
            [64000, 2, false, false],
            [64000, 2, true, true],
            [60000, 16, false, true],
            [80000, 2, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
  },
  90000,
);
