/** PostgreSQL is the authority for grouping, float8 SUM transitions and top-N order. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import type { Rel } from "../../ir/ir.js";
import type { ByteCharge } from "../../vector/varlen.js";
import { OBJECT_OVERHEAD_BYTES } from "../../memory/governor.js";
import type { Batch, Vector } from "../../vector/vector.js";
import { taskClock, beginTask, YIELD_INTERVAL_MS, type Operator } from "../pipeline.js";
import { OutputArena } from "../kernels/vectors.js";
import {
  I32,
  I64,
  F64,
  col,
  numbers,
  dictionary,
  constant,
  valueAt,
  sqlLiteral,
} from "../kernels/test-support.js";
import { DomainCountSumOperator } from "./domain-count-sum.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const rel: Extract<Rel, { op: "aggregate" }> = {
  id: 1,
  op: "aggregate",
  input: { id: 0, op: "values", rows: [], output: [] },
  groupBy: [0],
  groupingSets: null,
  aggregates: [
    { fn: "count_star", args: [], type: I64, distinct: false, orderBy: [], filter: null },
    { fn: "sum", args: [col(1, F64)], type: F64, distinct: false, orderBy: [], filter: null },
  ],
  output: [
    { name: "k", type: I32, nullable: true },
    { name: "n", type: I64, nullable: false },
    { name: "s", type: F64, nullable: true },
  ],
};
const options = {
  outputTopN: {
    count: 100,
    keys: [
      { column: 1, direction: "desc" as const, nulls: "last" as const, collation: null },
      { column: 0, direction: "asc" as const, nulls: "last" as const, collation: null },
    ],
  },
};
type Row = Array<number | null>;
function deferred() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function compare(a: Row, b: Row): number {
  return (
    Number(b[1]) - Number(a[1]) ||
    (a[0] === null ? (b[0] === null ? 0 : 1) : b[0] === null ? -1 : Number(a[0]) - Number(b[0]))
  );
}
function rowsOf(batch: Batch): Row[] {
  return Array.from({ length: batch.selection?.count ?? batch.length }, (_, k) => {
    const row = batch.selection?.rows[k] ?? k;
    return batch.columns.map((v) => {
      const value = valueAt(v, row);
      if (value === null) return null;
      if (typeof value !== "number" && typeof value !== "bigint")
        throw new TypeError("Number expected");
      return Number(value);
    });
  });
}
async function expected(batches: readonly Batch[]): Promise<Row[]> {
  const rows = batches.flatMap(rowsOf);
  const input =
    rows.length === 0
      ? "SELECT 0 AS ord,0::int4 AS k,0::float8 AS v WHERE false"
      : `VALUES ${rows.map((r, i) => `(${String(i)},${sqlLiteral("int4", r[0] ?? null)},${sqlLiteral("float8", r[1] ?? null)})`).join(",")}`;
  return (
    await pg.query<{ k: string | null; n: string; s: string | null }>(
      `WITH inp(ord,k,v) AS (${input}) SELECT k::text,count(*)::text AS n,sum(v ORDER BY ord)::text AS s FROM inp GROUP BY k ORDER BY count(*) DESC,inp.k NULLS LAST LIMIT 100`,
    )
  ).rows.map((r) => [
    r.k === null ? null : Number(r.k),
    Number(r.n),
    r.s === null ? null : Number(r.s),
  ]);
}
async function run(batches: readonly Batch[], narrow = false, deny = false) {
  let held = 0,
    refused = 0;
  const charge: ByteCharge = {
    allocate(n) {
      if (deny && n === 8192 + 2 * OBJECT_OVERHEAD_BYTES) {
        refused++;
        return false;
      }
      held += n;
      return true;
    },
    free(n) {
      held -= n;
      expect(held).toBeGreaterThanOrEqual(0);
    },
  };
  const arena = new OutputArena(charge, 2048),
    rows: Row[] = [];
  let borrowed: Batch | null = null;
  const sink: Operator = {
    push(b) {
      borrowed = b;
      return "blocked";
    },
    ready: async () => {
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
      if (borrowed !== null) rows.push(...rowsOf(borrowed));
      borrowed = null;
    },
    finish: () => undefined,
    abort: () => undefined,
  };
  const operator = new DomainCountSumOperator(
    rel,
    { input: [I32, F64], arena, params: { slots: [] } },
    charge,
    sink,
    options,
  );
  try {
    for (const batch of batches) operator.probe(batch);
    if (narrow || deny)
      operator.bounds(
        0,
        511,
        batches.reduce((n, b) => n + (b.selection?.count ?? b.length), 0),
      );
    operator.seal();
    for (const batch of batches) operator.push(batch);
    await operator.finish();
    return { rows: rows.sort(compare).slice(0, 100), direct: operator.directRows, refused };
  } finally {
    operator.abort(undefined);
    arena.close();
    expect(held).toBe(0);
  }
}

it.each(seedsFor("domain-count-sum", [0xb491, 0xb492]))(
  "keeps exact groups across masks, encodings and domain fallback (seed %i)",
  async (seed) => {
    const reach = createReach("domain COUNT/SUM", [
      "direct",
      "nullable",
      "dictionary",
      "constant",
      "selection",
      "wide fallback",
      "range guard",
      "refused direct state",
      "PG",
      "release",
      "backpressure",
      "native endpoint domains",
      "exceptional transitions",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 160, max: 320 }),
        fc.array(fc.integer({ min: -10000, max: 10000 }), { minLength: 32, maxLength: 32 }),
        async (n, values) => {
          reach.beginHistory();
          for (const mode of [
            "dense",
            "nullable",
            "dictionary",
            "constant",
            "wide",
            "guard",
            "deny",
            "min",
            "max",
            "exceptional",
          ] as const) {
            const batches: Batch[] = [];
            for (let phase = 0; phase < 3; phase++) {
              let keys: Vector = numbers(
                I32,
                Array.from({ length: n }, (_, i) =>
                  mode === "wide" && i === 0
                    ? -2147483648
                    : mode === "wide" && i === 1
                      ? 2147483647
                      : mode === "guard" && i % 5 === 0
                        ? 10000
                        : mode === "nullable" && i % 11 === 0
                          ? null
                          : (mode === "min" ? -2147483648 : mode === "max" ? 2147483391 : 0) +
                            (i % 257),
                ),
              );
              let sums: Vector = numbers(
                F64,
                Array.from({ length: n }, (_, i) =>
                  mode === "nullable" && (phase === 0 || i % 13 === 0)
                    ? null
                    : mode === "exceptional"
                      ? ([NaN, Infinity, -Infinity, -0, 1e154, -1e154][(i + phase) % 6] ?? 0)
                      : (values[(i + phase) % 32] ?? 0) / 8,
                ),
                Infinity,
              );
              if (mode === "dictionary" && phase === 1) {
                keys = dictionary(
                  numbers(I32, [-3, 8, 511]),
                  Array.from({ length: n }, (_, i) => (i % 7 === 0 ? null : (i + phase) % 3)),
                );
                sums = dictionary(
                  numbers(F64, [-0, -11, 7]),
                  Array.from({ length: n }, (_, i) => (i % 5 === 0 ? null : i % 3)),
                );
                reach.reach("dictionary");
              }
              if (mode === "constant" && phase === 1) {
                keys = constant(numbers(I32, [-3]), n);
                sums = constant(numbers(F64, [null], Infinity), n);
                reach.reach("constant");
              }
              const selection =
                phase === 2
                  ? {
                      rows: Uint16Array.from({ length: Math.ceil(n / 2) }, (_, i) => i * 2),
                      count: Math.ceil(n / 2),
                    }
                  : null;
              batches.push({ length: n, columns: [keys, sums], selection });
            }
            const want = await expected(batches),
              got = await run(batches, mode === "guard", mode === "deny");
            expect(got.rows).toEqual(want);
            if (mode === "dense") {
              expect(got.direct).toBeGreaterThan(0);
              reach.reach("direct");
            }
            if (mode === "nullable") reach.reach("nullable");
            if (mode === "wide") {
              expect(got.direct).toBe(0);
              reach.reach("wide fallback");
            }
            if (mode === "guard") {
              expect(got.direct).toBeGreaterThan(0);
              expect(want.some((r) => r[0] === 10000)).toBe(true);
              reach.reach("range guard");
            }
            if (mode === "deny") {
              expect(got.refused).toBeGreaterThan(0);
              expect(got.direct).toBe(0);
              reach.reach("refused direct state");
            }
            if (mode === "min" || mode === "max") {
              expect(got.direct).toBeGreaterThan(0);
              reach.reach("native endpoint domains");
            }
            if (mode === "exceptional") {
              expect(got.direct).toBeGreaterThan(0);
              reach.reach("exceptional transitions");
            }
            reach.reach("selection");
            reach.reach("PG");
            reach.reach("release");
            reach.reach("backpressure");
          }
        },
      ),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
  },
  60000,
);

it("retains empty input, signed zero, nonfinite SUM and incremental overflow", async () => {
  for (const values of [
    [],
    [-0, -0],
    [1e154, -1e154, 1e154],
    [NaN, Infinity, -Infinity],
    [Infinity, -Infinity],
  ]) {
    const batches: Batch[] = [
      {
        length: values.length,
        columns: [
          numbers(
            I32,
            values.map(() => -3),
          ),
          numbers(F64, values),
        ],
        selection: null,
      },
    ];
    expect((await run(batches)).rows).toEqual(await expected(batches));
  }
  const overflow: Batch[] = [
    {
      length: 3,
      columns: [numbers(I32, [2, 2, 2]), numbers(F64, [1e308, 1e308, -1e308])],
      selection: null,
    },
  ];
  await expect(expected(overflow)).rejects.toMatchObject({ code: "22003" });
  await expect(run(overflow)).rejects.toMatchObject({ sqlState: "22003" });
});

it.each(seedsFor("domain-count-sum-cancellation", [0xb493]))(
  "releases independent instances before finish and across a blocked output (seed %i)",
  async (seed) => {
    const reach = createReach("domain cancellation", [
      "before finish",
      "blocked output",
      "independent",
      "release",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 64 }), async (n) => {
        reach.beginHistory();
        for (const blocked of [false, true]) {
          let held = 0;
          const charge: ByteCharge = {
            allocate(bytes) {
              held += bytes;
              return true;
            },
            free(bytes) {
              held -= bytes;
              expect(held).toBeGreaterThanOrEqual(0);
            },
          };
          const arena = new OutputArena(charge, 2048),
            otherArena = new OutputArena(charge, 2048);
          const requested = deferred(),
            ready = deferred();
          const sink: Operator = {
            push() {
              requested.resolve();
              return "blocked";
            },
            ready: () => ready.promise,
            finish: () => undefined,
            abort: () => undefined,
          };
          const context = { input: [I32, F64], arena, params: { slots: [] } };
          const operator = new DomainCountSumOperator(rel, context, charge, sink, {
            ...options,
            now: () => 0,
          });
          const other = new DomainCountSumOperator(
            rel,
            { ...context, arena: otherArena },
            charge,
            {
              push: () => "ready",
              ready: () => Promise.resolve(),
              finish: () => undefined,
              abort: () => undefined,
            },
            { ...options, now: () => 0 },
          );
          const batch: Batch = {
            length: n,
            columns: [
              numbers(
                I32,
                Array.from({ length: n }, (_, i) => i),
              ),
              numbers(
                F64,
                Array.from({ length: n }, () => 3),
              ),
            ],
            selection: null,
          };
          const reason = new Error("cancel direct-domain execution");
          try {
            for (const op of [operator, other]) {
              op.probe(batch);
              op.seal();
              op.push(batch);
            }
            let finishing: Promise<void> | null = null;
            if (blocked) {
              finishing = Promise.resolve(operator.finish());
              await requested.promise;
            }
            operator.abort(reason);
            ready.resolve();
            if (finishing !== null) await expect(finishing).rejects.toBe(reason);
            else await expect(Promise.resolve().then(() => operator.finish())).rejects.toBe(reason);
            reach.reach(blocked ? "blocked output" : "before finish");
            await other.finish();
            expect(other.directRows).toBe(n);
            reach.reach("independent");
          } finally {
            ready.resolve();
            operator.abort(reason);
            other.abort(undefined);
            arena.close();
            otherArena.close();
            expect(held).toBe(0);
            reach.reach("release");
          }
        }
      }),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
  },
  60000,
);

it("uses the worker's existing task deadline while ranking empty domain slots", async () => {
  const saved = taskClock.now;
  let now = 0,
    held = 0,
    pushed = false;
  taskClock.now = () => now;
  beginTask();
  const charge: ByteCharge = {
    allocate(bytes) {
      held += bytes;
      return true;
    },
    free(bytes) {
      held -= bytes;
    },
  };
  const arena = new OutputArena(charge, 2048);
  const operator = new DomainCountSumOperator(
    rel,
    { input: [I32, F64], arena, params: { slots: [] } },
    charge,
    {
      push() {
        pushed = true;
        return "ready";
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    },
    options,
  );
  try {
    const batch: Batch = {
      length: 1,
      columns: [numbers(I32, [-3]), numbers(F64, [2])],
      selection: null,
    };
    operator.bounds(-8192, 8191, 1);
    operator.seal();
    operator.push(batch);
    now = YIELD_INTERVAL_MS;
    const finishing = operator.finish();
    expect(pushed).toBe(false);
    expect(finishing).toBeInstanceOf(Promise);
    now = 0;
    await finishing;
    expect(pushed).toBe(true);
  } finally {
    operator.abort(undefined);
    arena.close();
    expect(held).toBe(0);
    taskClock.now = saved;
    beginTask();
  }
});
