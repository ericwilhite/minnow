/** A replay never combines float states or changes the row order inside a key class. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { Rel } from "../../ir/ir.js";
import type { Batch } from "../../vector/vector.js";
import { F64, I32, col, numbers, valueAt, sqlLiteral } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import type { Operator } from "../pipeline.js";
import { AggregateOperator } from "./aggregate.js";
import { GroupPartitionProbe } from "./partition-probe.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const rel: Extract<Rel, { op: "aggregate" }> = {
  op: "aggregate",
  id: 1,
  output: [],
  input: { op: "values", id: 0, output: [], rows: [] },
  groupBy: [0],
  groupingSets: null,
  aggregates: ["sum", "avg"].map((fn) => ({
    fn: fn as "sum" | "avg",
    args: [col(1, F64)],
    distinct: false,
    filter: null,
    orderBy: [],
    type: F64,
  })),
};
const bits = (value: number): string => {
  const b = new ArrayBuffer(8),
    d = new DataView(b);
  d.setFloat64(0, value, false);
  return d.getBigUint64(0, false).toString(16);
};

function at<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined) throw new Error("Missing generated partition value");
  return value;
}

it.each(seedsFor("aggregate-replay-pglite-order", [0xb495, 0xb496]))(
  "matches single-pass and PG float transitions bit for bit (seed %i)",
  async (seed) => {
    const reach = createReach("aggregate replay PG order", [
      "multiple passes",
      "cancellation-sensitive values",
      "selection",
      "PG bits",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: -100, max: 100 }), { minLength: 72, maxLength: 96 }),
        fc.integer({ min: 1, max: 6 }),
        async (values, shift) => {
          reach.beginHistory();
          let bytes = 0;
          const charge = {
            allocate(n: number) {
              bytes += n;
              return true;
            },
            free(n: number) {
              bytes -= n;
            },
          };
          const probe = new GroupPartitionProbe([I32], 0, charge, 2);
          const batches: Batch[] = [];
          const sql: string[] = [];
          for (let start = 0; start < values.length; start += 12) {
            const part = values.slice(start, start + 12);
            const keys = part.map((_, i) => (start + i) % 7);
            // Alternate cancellation and tiny tails; a state merge or row reorder changes bits.
            const floats = part.map((v, i) =>
              (start + i) % 3 === 0 ? 2 ** 50 : (start + i) % 3 === 1 ? -(2 ** 50) : v / 10,
            );
            const rows = Uint16Array.from(
              part.flatMap((_, i) => ((start + i + shift) % 11 === 0 ? [] : [i])),
            );
            const batch: Batch = {
              length: part.length,
              columns: [numbers(I32, keys), numbers(F64, floats)],
              selection: { rows, count: rows.length },
            };
            expect(probe.offer([at(batch.columns, 0)], [], batch.length, batch.selection)).toBe(
              true,
            );
            batches.push(batch);
            for (const row of rows)
              sql.push(`(${String(keys[row])},${sqlLiteral("float8", floats[row] ?? 0)})`);
          }
          const run = async (passes: number): Promise<Map<number, readonly string[]>> => {
            const found = new Map<number, readonly string[]>();
            for (let pass = 0; pass < passes; pass++) {
              const arena = new OutputArena(charge, 2048);
              const out: Operator = {
                push(batch) {
                  for (let i = 0; i < batch.length; i++) {
                    const key = Number(valueAt(at(batch.columns, 0), i));
                    expect(found.has(key)).toBe(false);
                    found.set(key, [
                      bits(Number(valueAt(at(batch.columns, 1), i))),
                      bits(Number(valueAt(at(batch.columns, 2), i))),
                    ]);
                  }
                  return "ready";
                },
                ready: () => Promise.resolve(),
                finish: () => undefined,
                abort: () => undefined,
              };
              const op = new AggregateOperator(
                rel,
                { arena, input: [I32, F64], params: { slots: [] } },
                charge,
                out,
              );
              try {
                for (const batch of batches) {
                  const selection = { rows: new Uint16Array(2048), count: 0 };
                  if (passes > 1)
                    probe.select(
                      [at(batch.columns, 0)],
                      batch.length,
                      batch.selection,
                      pass,
                      selection,
                    );
                  op.push({ ...batch, selection: passes > 1 ? selection : batch.selection });
                }
                await op.finish();
              } finally {
                op.abort(new Error("test cleanup"));
                arena.close();
              }
            }
            return found;
          };
          try {
            const bounds = probe.seal();
            if (!bounds) throw new Error("Missing bounded replay plan");
            expect(bounds.passes).toBeGreaterThan(1);
            const single = await run(1),
              replay = await run(bounds.passes);
            expect(replay).toEqual(single);
            const result = await pg.query<{ k: number; s: string; a: string }>(
              `SELECT k, sum(v)::text s, avg(v)::text a FROM (VALUES ${sql.join(",")}) t(k,v) GROUP BY k`,
            );
            expect(
              new Map(result.rows.map((r) => [r.k, [bits(Number(r.s)), bits(Number(r.a))]])),
            ).toEqual(single);
            reach.reach("multiple passes");
            reach.reach("cancellation-sensitive values");
            reach.reach("selection");
            reach.reach("PG bits");
          } finally {
            probe.close();
          }
          expect(bytes).toBe(0);
          reach.reach("released");
        },
      ),
      { seed, numRuns: 10 },
    );
    reach.assertReached();
  },
  60000,
);
