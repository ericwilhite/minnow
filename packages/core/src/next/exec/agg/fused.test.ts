/**
 * The fused scalar pass (aggregate.ts `#pushFused`, states.ts `fusedSums`) and shared SUM/AVG
 * state against PostgreSQL (PGlite): no GROUP BY, SUM and AVG over one to six NUMERIC(12, 2)
 * columns (so fused loops of every width run), batches that qualify (no NULL, every row) mixed
 * with batches that do not (NULLs, a selection), several batches per run. PostgreSQL's answer is
 * the law. Declared reach: a fused loop of each width, a SUM and an AVG sharing a column, and a
 * non-qualifying batch between fused ones.
 */
import { PGlite } from "@electric-sql/pglite";
import v8 from "node:v8";
import vm from "node:vm";
import fc from "fast-check";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { AggregateCall, OutputColumn } from "../../ir/ir.js";
import type { Batch } from "../../vector/vector.js";
import { UNCHARGED, col, decimal, decimals, valueAt } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import type { Operator, PushResult } from "../pipeline.js";
import { AggregateOperator } from "./aggregate.js";

const SEEDS = seedsFor("next-aggregate-fused", [0xf05, 0x9]);
const N12 = decimal(12, 2);
const D = decimal(null, null);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

/** NUMERIC(12, 2) text of coefficient c (scale 2). */
function text(c: bigint): string {
  const negative = c < 0n;
  const digits = (negative ? -c : c).toString().padStart(3, "0");
  return `${negative ? "-" : ""}${digits.slice(0, -2)}.${digits.slice(-2)}`;
}

/** Our NUMERIC output as PostgreSQL's text. */
function render(value: unknown): string {
  if (value === null) return "NULL";
  const [c, s] = value as [bigint, number];
  const negative = c < 0n;
  const digits = (negative ? -c : c).toString().padStart(s + 1, "0");
  return `${negative ? "-" : ""}${s === 0 ? digits : `${digits.slice(0, digits.length - s)}.${digits.slice(digits.length - s)}`}`;
}

describe("fused scalar NUMERIC sums and shared SUM/AVG state against PostgreSQL", () => {
  it.each(SEEDS)(
    "SUM / AVG over 1–6 NUMERIC(12,2) columns (seed %i)",
    async (seed) => {
      const reach = createReach("next-aggregate-fused", [
        "fused width 2",
        "fused width 3",
        "fused width 4",
        "fused width 5 or more",
        "shared SUM and AVG",
        "non-qualifying batch",
      ] as const);
      const coefficient = fc.oneof(
        fc.bigInt({ min: -(10n ** 11n), max: 10n ** 11n }),
        fc.bigInt({ min: -1000n, max: 1000n }),
      );
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 6 }),
          fc.array(fc.constantFrom("sum", "avg", "both"), { minLength: 6, maxLength: 6 }),
          fc.array(
            fc.record({
              rows: fc.integer({ min: 1, max: 2048 }),
              nulls: fc.boolean(),
              selected: fc.boolean(),
              seed: fc.integer(),
            }),
            { minLength: 1, maxLength: 5 },
          ),
          async (width, fns, shapes) => {
            reach.beginHistory();
            // Columns' values per batch (null where `nulls`), and which rows are active.
            const data: Batch[] = [];
            const all: Array<Array<bigint | null>> = Array.from({ length: width }, () => []);
            let qualifying = 0;
            let other = 0;
            for (const shape of shapes) {
              const columns = Array.from({ length: width }, (_, c) =>
                fc.sample(fc.option(coefficient, { freq: shape.nulls ? 4 : 1_000_000 }), {
                  seed: shape.seed + c,
                  numRuns: shape.rows,
                }),
              );
              const picked = shape.selected
                ? Array.from({ length: shape.rows }, (_, r) => r).filter((r) => r % 3 !== 1)
                : null;
              const active = picked ?? Array.from({ length: shape.rows }, (_, r) => r);
              for (let c = 0; c < width; c++)
                for (const r of active) all[c]?.push(columns[c]?.[r] ?? null);
              if (!shape.selected && columns.every((values) => values.every((v) => v !== null)))
                qualifying += 1;
              else other += 1;
              data.push({
                length: shape.rows,
                selection:
                  picked === null ? null : { rows: Uint16Array.from(picked), count: picked.length },
                columns: columns.map((values) =>
                  decimals(
                    N12,
                    values.map((v) => (v === null ? null : ([v, 2] as const))),
                    "safe",
                    2,
                  ),
                ),
              });
            }
            const calls: AggregateCall[] = [];
            const sql: string[] = [];
            let shared = false;
            for (let c = 0; c < width; c++) {
              const fn = fns[c] ?? "sum";
              for (const f of fn === "both" ? (["sum", "avg"] as const) : [fn]) {
                calls.push({
                  fn: f,
                  args: [col(c, N12)],
                  distinct: false,
                  filter: null,
                  orderBy: [],
                  type: D,
                });
                sql.push(`${f}(c${String(c)})::text`);
              }
              if (fn === "both") shared = true;
            }
            if (qualifying > 0) {
              const fusedWidth = width;
              if (fusedWidth === 2) reach.reach("fused width 2");
              if (fusedWidth === 3) reach.reach("fused width 3");
              if (fusedWidth === 4) reach.reach("fused width 4");
              if (fusedWidth >= 5) reach.reach("fused width 5 or more");
            }
            if (shared) reach.reach("shared SUM and AVG");
            if (qualifying > 0 && other > 0) reach.reach("non-qualifying batch");
            // Ours.
            const rowsOut: unknown[][] = [];
            const sink: Operator = {
              push(batch: Batch): PushResult {
                for (let r = 0; r < batch.length; r++)
                  rowsOut.push(batch.columns.map((column) => valueAt(column, r)));
                return "ready";
              },
              ready: () => Promise.resolve(),
              finish: () => undefined,
              abort: () => undefined,
            };
            const output: OutputColumn[] = calls.map((c) => ({
              name: c.fn,
              type: c.type,
              nullable: true,
            }));
            const op = new AggregateOperator(
              {
                id: 1,
                op: "aggregate",
                output,
                input: { id: 0, op: "values", output: [], rows: [] },
                groupBy: [],
                groupingSets: null,
                aggregates: calls,
              },
              {
                arena: new OutputArena(UNCHARGED, 2048),
                input: Array.from({ length: width }, () => N12),
                params: { slots: [] },
              },
              UNCHARGED,
              sink,
            );
            for (const batch of data) op.push(batch);
            await op.finish();
            const ours = (rowsOut[0] ?? []).map(render);
            // PostgreSQL over the same active rows (one VALUES row per active row).
            const count = all[0]?.length ?? 0;
            const tuples = Array.from(
              { length: count },
              (_, r) =>
                `(${all
                  .map((values) => {
                    const v = values[r] ?? null;
                    return v === null ? "null::numeric(12,2)" : `${text(v)}::numeric(12,2)`;
                  })
                  .join(", ")})`,
            );
            const names = Array.from({ length: width }, (_, c) => `c${String(c)}`).join(", ");
            const result = await pg.query<Record<string, string | null>>(
              `select ${sql.map((s, k) => `${s} as a${String(k)}`).join(", ")} from (values ${tuples.join(", ")}) t(${names})`,
            );
            const theirs = Object.values(result.rows[0] ?? {}).map((v) => v ?? "NULL");
            expect(ours).toEqual(theirs);
          },
        ),
        { seed, numRuns: 40 },
      );
      reach.assertReached();
    },
    180_000,
  );

  it("keeps no reference to a fused batch's coefficients once push returns (review B4-p2c-1)", async () => {
    // A forced collection: the WeakRef clears only if nothing (the operator included) holds it.
    v8.setFlagsFromString("--expose-gc");
    const gc = vm.runInNewContext("gc") as () => void;
    const op = new AggregateOperator(
      {
        id: 1,
        op: "aggregate",
        output: [],
        input: { id: 0, op: "values", output: [], rows: [] },
        groupBy: [],
        groupingSets: null,
        aggregates: [0, 1].map((c) => ({
          fn: "sum" as const,
          args: [col(c, N12)],
          distinct: false,
          filter: null,
          orderBy: [],
          type: D,
        })),
      },
      { arena: new OutputArena(UNCHARGED, 2048), input: [N12, N12], params: { slots: [] } },
      UNCHARGED,
      {
        push: () => "ready",
        ready: () => Promise.resolve(),
        finish: () => undefined,
        abort: () => undefined,
      },
    );
    const refs: Array<WeakRef<Float64Array>> = [];
    const push = () => {
      const columns = [0, 1].map(() =>
        decimals(
          N12,
          Array.from({ length: 2048 }, (_, i) => [BigInt(i), 2] as const),
          "safe",
          2,
        ),
      );
      for (const column of columns)
        if (column.data.layout === "decimal" && column.data.coefficients.repr === "safe")
          refs.push(new WeakRef(column.data.coefficients.values));
      op.push({ length: 2048, selection: null, columns });
    };
    push();
    for (let k = 0; k < 5; k++) {
      await new Promise((resolve) => setImmediate(resolve));
      gc();
    }
    expect(refs.every((ref) => ref.deref() === undefined)).toBe(true);
    await op.finish();
  });

  it("charges every buffer the operator builds, the fused totals included (review B4-p2c-1)", () => {
    const counting = () => {
      const c = {
        held: 0,
        allocate: (n: number) => ((c.held += n), true),
        free: (n: number) => void (c.held -= n),
      };
      return c;
    };
    const arenaCharge = counting();
    const stateCharge = counting();
    const types = [
      Uint8Array,
      Uint16Array,
      Uint32Array,
      Int32Array,
      Float64Array,
      BigInt64Array,
    ] as const;
    const real: unknown[] = types.map((T) => (globalThis as Record<string, unknown>)[T.name]);
    let built = 0;
    types.forEach((T) => {
      (globalThis as Record<string, unknown>)[T.name] = new Proxy(T, {
        construct(target, args: unknown[]) {
          const made = Reflect.construct(target, args) as { byteLength: number };
          built += made.byteLength;
          return made;
        },
      });
    });
    let arena: OutputArena;
    try {
      arena = new OutputArena(arenaCharge, 2048);
      const before = built;
      new AggregateOperator(
        {
          id: 1,
          op: "aggregate",
          output: [],
          input: { id: 0, op: "values", output: [], rows: [] },
          groupBy: [],
          groupingSets: null,
          aggregates: [0, 1, 2].map((c) => ({
            fn: "sum" as const,
            args: [col(c, N12)],
            distinct: false,
            filter: null,
            orderBy: [],
            type: D,
          })),
        },
        { arena, input: [N12, N12, N12], params: { slots: [] } },
        stateCharge,
        {
          push: () => "ready",
          ready: () => Promise.resolve(),
          finish: () => undefined,
          abort: () => undefined,
        },
      );
      expect(built - before).toBeLessThanOrEqual(arenaCharge.held + stateCharge.held);
    } finally {
      types.forEach((T, k) => {
        (globalThis as Record<string, unknown>)[T.name] = real[k];
      });
    }
  });
});
