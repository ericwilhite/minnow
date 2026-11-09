/** PostgreSQL float8pl/float8_accum and the Accumulator contract are the authorities. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import type { Vector } from "../../vector/vector.js";
import {
  BOOL,
  I32,
  col,
  call,
  lit,
  F64,
  I64,
  UNCHARGED,
  numbers,
  dictionary,
  constant,
  valueAt,
} from "../kernels/test-support.js";
import { AggregateOperator } from "./aggregate.js";
import { OutputArena } from "../kernels/vectors.js";
import type { AggregateCall } from "../../ir/ir.js";
import { accumulatorFor, updateGroupedPair } from "./states.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});
type Mode = "count" | "sums" | "average";
type Row = readonly [number, number | null, number | null];
function literal(value: number | null): string {
  if (value === null) return "null::float8";
  return `'${Object.is(value, -0) ? "-0" : String(value)}'::float8`;
}
function vector(raw: ReadonlyArray<number | null>, form: string): Vector {
  if (form === "constant") return constant(numbers(F64, [raw[0] ?? null]), raw.length);
  if (form === "dictionary")
    return dictionary(
      numbers(
        F64,
        raw.map((v) => v ?? 0),
      ),
      raw.map((v, i) => (v === null ? null : i)),
    );
  const result = numbers(F64, raw);
  if (result.data.layout !== "f64") throw new Error("builder");
  // A NULL slot is unconstrained storage. Multiplying its payload by a mask is unsound.
  const values = result.data.values;
  raw.forEach((v, i) => {
    if (v === null) values[i] = i % 2 ? Infinity : NaN;
  });
  return result;
}
function code(error: unknown): string {
  if (typeof error === "object" && error !== null) {
    if ("sqlState" in error) return String(error.sqlState);
    if ("code" in error) return String(error.code);
  }
  throw error;
}

it.each(seedsFor("next-agg-grouped-pairs", [0xa731, 0xa732]))(
  "keeps independent states across forms, NULL masks, selection, growth and exceptions (seed %i)",
  async (seed) => {
    const reach = createReach("grouped float pairs", [
      "count fusion",
      "sum fusion",
      "average fallback",
      "null fallback",
      "dictionary fallback",
      "constant fallback",
      "selection",
      "resume",
      "growth",
      "nonfinite",
      "overflow",
      "Sxx overflow",
      "negative zero",
      "released",
    ] as const);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom<Mode>("count", "sums", "average"),
        fc.constantFrom("flat", "dictionary", "constant"),
        fc.boolean(),
        fc.integer({ min: 0, max: 6 }),
        fc.integer({ min: 1, max: 8 }),
        fc.array(
          fc.tuple(
            fc.integer({ min: 0, max: 3 }),
            fc.option(fc.integer({ min: -1000000, max: 1000000 })),
            fc.option(fc.integer({ min: -1000000, max: 1000000 })),
          ),
          { minLength: 4, maxLength: 24 },
        ),
        async (mode, form, selected, scenario, split, generated) => {
          reach.beginHistory();
          const make = () => ({
            first: accumulatorFor(
              mode === "count" ? "count_star" : "sum",
              mode === "count" ? I64 : F64,
              mode === "count" ? null : F64,
              UNCHARGED,
            ),
            second: accumulatorFor(mode === "average" ? "avg" : "sum", F64, F64, UNCHARGED),
          });
          const candidate = make(),
            control = make(),
            active: Row[] = [],
            groups = new Set<number>();
          let max = 0,
            fallback = false,
            ours: string | undefined,
            old: string | undefined;
          const apply = (data: readonly Row[], encoding: string, pick: boolean) => {
            const rawA = data.map((r) => r[1]),
              rawB = data.map((r) => r[2]);
            if (encoding === "constant") {
              rawA.fill(rawA[0] ?? null);
              rawB.fill(rawB[0] ?? null);
            }
            const a = vector(rawA, encoding),
              b = vector(rawB, encoding),
              gids = Int32Array.from(data, (r) => r[0]),
              rows = Uint16Array.from(data.map((_, i) => i).filter((i) => !pick || i % 3 !== 1));
            for (const i of rows) {
              const g = gids[i] ?? 0;
              max = Math.max(max, g + 1);
              groups.add(g);
              active.push([g, rawA[i] ?? null, rawB[i] ?? null]);
            }
            for (const pair of [candidate, control]) {
              pair.first.grow(max);
              pair.second.grow(max);
            }
            if (max > 2048) reach.reach("growth");
            if (pick) reach.reach("selection");
            try {
              const paired = updateGroupedPair(
                candidate.first,
                candidate.second,
                mode === "count" ? null : a,
                b,
                gids,
                rows,
                rows.length,
              );
              if (paired) {
                reach.reach(mode === "count" ? "count fusion" : "sum fusion");
                if (fallback) reach.reach("resume");
              } else {
                fallback = true;
                if (mode === "average") reach.reach("average fallback");
                if (encoding === "dictionary") reach.reach("dictionary fallback");
                if (encoding === "constant") reach.reach("constant fallback");
                if (rawA.includes(null) || rawB.includes(null)) reach.reach("null fallback");
                candidate.first.update(mode === "count" ? null : a, gids, rows, rows.length, false);
                candidate.second.update(b, gids, rows, rows.length, false);
              }
            } catch (error) {
              ours = code(error);
            }
            try {
              control.first.update(mode === "count" ? null : a, gids, rows, rows.length, false);
              control.second.update(b, gids, rows, rows.length, false);
            } catch (error) {
              old = code(error);
            }
            expect(ours).toBe(old);
            if (!ours)
              for (let first = 0; first < max; first += 2048) {
                const n = Math.min(2048, max - first);
                expect(candidate.first.emit(first, n)).toEqual(control.first.emit(first, n));
                expect(candidate.second.emit(first, n)).toEqual(control.second.emit(first, n));
              }
          };
          try {
            // Every history has a dense prefix, a possible fallback, and dense input after it.
            apply(
              [
                [0, 1, 2],
                [1, -3, 4],
              ],
              "flat",
              false,
            );
            for (let first = 0; first < generated.length; first += split)
              apply(generated.slice(first, first + split), form, selected);
            apply(
              [
                [2048, 7, 11],
                [2048, -2, 13],
              ],
              "flat",
              false,
            );
            const tails: ReadonlyArray<readonly Row[]> = [
              [[701, -0, -0]],
              [
                [702, 1, Infinity],
                [702, 2, -Infinity],
              ],
              [
                [702, 1, NaN],
                [702, 2, Infinity],
              ],
              [
                [702, 1, 1e308],
                [702, 2, 1e308],
              ],
              [
                [702, 1, 1e154],
                [702, 2, -1e154],
              ],
              [
                [702, 1e308, 1],
                [702, 1e308, 2],
              ],
              // A second SUM overflows earlier in the batch; first SUM's suffix is still evaluated.
              [
                [702, 1, 1e308],
                [702, 2, 1e308],
                [702, 1e308, 1],
                [702, 1e308, 1],
              ],
            ];
            const tail = tails[scenario];
            if (!tail) throw new Error("scenario");
            apply(tail, "flat", false);
            const sql = `select g, ${mode === "count" ? "count(*)" : "sum(a)"}::text as x, ${mode === "average" ? "avg" : "sum"}(b)::text as y from (values ${active.map(([g, a, b]) => `(${String(g)},${literal(a)},${literal(b)})`).join(",")}) t(g,a,b) group by g order by g`;
            let theirs: string | undefined;
            let expected: Array<{ g: number; x: string | null; y: string | null }> = [];
            try {
              expected = (await pg.query<{ g: number; x: string | null; y: string | null }>(sql))
                .rows;
            } catch (error) {
              theirs = code(error);
            }
            expect(ours).toBe(theirs);
            if (theirs === "22003") {
              reach.reach("overflow");
              if (mode === "average" && scenario === 4) reach.reach("Sxx overflow");
            }
            if (!theirs) {
              const actual = [...groups]
                .sort((a, b) => a - b)
                .map((g) => [
                  g,
                  valueAt(candidate.first.emit(g, 1), 0),
                  valueAt(candidate.second.emit(g, 1), 0),
                ]);
              const rendered = expected.map((r) => [
                r.g,
                r.x === null ? null : Number(r.x),
                r.y === null ? null : Number(r.y),
              ]);
              for (const row of actual) if (typeof row[1] === "bigint") row[1] = Number(row[1]);
              expect(actual).toEqual(rendered);
              if (scenario === 0) {
                expect(Object.is(actual.find((r) => r[0] === 701)?.[2], -0)).toBe(
                  mode !== "average",
                );
                reach.reach("negative zero");
              }
              if (scenario === 1 || scenario === 2) reach.reach("nonfinite");
            }
          } finally {
            for (const pair of [candidate, control]) {
              pair.first.close();
              pair.second.close();
            }
            reach.reach("released");
          }
        },
      ),
      { seed, numRuns: 120 },
    );
    reach.assertReached();
  },
  180_000,
);

it.each(seedsFor("next-agg-grouped-pair-operator", [0xa741, 0xa742]))(
  "keeps FILTER, DISTINCT and expression arguments out of the pair kernel (seed %i)",
  async (seed) => {
    const reach = createReach("grouped pair operator", [
      "count",
      "sums",
      "plain",
      "filter",
      "distinct",
      "expression",
      "selection",
      "released",
    ] as const);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.constantFrom("plain", "filter", "distinct", "expression"),
        fc.boolean(),
        fc.array(
          fc.tuple(
            fc.integer({ min: 0, max: 3 }),
            fc.integer({ min: -9, max: 9 }),
            fc.option(fc.integer({ min: -3, max: 3 })),
          ),
          { minLength: 16, maxLength: 40 },
        ),
        async (star, modifier, selected, generated) => {
          reach.beginHistory();
          const first: AggregateCall = {
            fn: star ? "count_star" : "sum",
            args: star ? [] : [col(1, F64)],
            type: star ? I64 : F64,
            filter: null,
            distinct: false,
            orderBy: [],
          };
          const second: AggregateCall = {
            fn: "sum",
            args: [
              modifier === "expression"
                ? call("add", F64, col(2, F64), lit(numbers(F64, [11])))
                : col(2, F64),
            ],
            type: F64,
            filter: modifier === "filter" ? col(3, BOOL) : null,
            distinct: modifier === "distinct",
            orderBy: [],
          };
          const raw = generated.map(([g, a, b], i) => [g, a, b, i % 2] as const);
          const prefix: Array<readonly [number, number, number | null, number]> = [
            [0, 1, 2, 1],
            [1, 3, 4, 0],
          ];
          const all = [...prefix, ...raw.filter((_, i) => !selected || i % 3 !== 1)];
          const data = [prefix, raw];
          const results: unknown[][][] = [];
          for (const fusion of [false, true]) {
            let used = 0;
            const charge = {
                allocate(n: number) {
                  used += n;
                  return true;
                },
                free(n: number) {
                  used -= n;
                  expect(used).toBeGreaterThanOrEqual(0);
                },
              },
              arena = new OutputArena(charge, 2048),
              rows: unknown[][] = [];
            const op = new AggregateOperator(
              {
                id: 1,
                op: "aggregate",
                output: [],
                input: { id: 0, op: "values", output: [], rows: [] },
                groupBy: [0],
                groupingSets: null,
                aggregates: [first, second],
              },
              { arena, input: [I32, F64, F64, BOOL], params: { slots: [] } },
              charge,
              {
                push(batch) {
                  for (let i = 0; i < batch.length; i++)
                    rows.push(batch.columns.map((c) => valueAt(c, i)));
                  return "ready";
                },
                ready: () => Promise.resolve(),
                finish: () => undefined,
                abort: () => undefined,
              },
              { groupedUpdateFusion: fusion },
            );
            try {
              for (const [index, input] of data.entries()) {
                const picked =
                  index === 1 && selected
                    ? Uint16Array.from(input.map((_, i) => i).filter((i) => i % 3 !== 1))
                    : null;
                op.push({
                  length: input.length,
                  selection: picked === null ? null : { rows: picked, count: picked.length },
                  columns: [
                    numbers(
                      I32,
                      input.map((r) => r[0]),
                    ),
                    numbers(
                      F64,
                      input.map((r) => r[1]),
                    ),
                    vector(
                      input.map((r) => r[2]),
                      "flat",
                    ),
                    numbers(
                      BOOL,
                      input.map((r) => r[3]),
                    ),
                  ],
                });
              }
              await op.finish();
              results.push(
                rows
                  .map((row) => row.map((v) => (typeof v === "bigint" ? Number(v) : v)))
                  .sort((a, b) => Number(a[0]) - Number(b[0])),
              );
            } finally {
              op.abort(undefined);
              arena.close();
              expect(used).toBe(0);
            }
          }
          const rhs = modifier === "expression" ? "b+11::float8" : "b",
            sql = `select g,${star ? "count(*)" : "sum(a)"}::text as x,sum(${modifier === "distinct" ? "distinct " : ""}${rhs})${modifier === "filter" ? " filter(where flag)" : ""}::text as y from (values ${all.map(([g, a, b, flag]) => `(${String(g)},${literal(a)},${literal(b)},${flag === 1 ? "true" : "false"})`).join(",")})t(g,a,b,flag) group by g order by g`;
          const expected = (
            await pg.query<{ g: number; x: string | null; y: string | null }>(sql)
          ).rows.map((r) => [
            r.g,
            r.x === null ? null : Number(r.x),
            r.y === null ? null : Number(r.y),
          ]);
          expect(results[0]).toEqual(expected);
          expect(results[1]).toEqual(expected);
          reach.reach(star ? "count" : "sums");
          reach.reach(modifier);
          if (selected) reach.reach("selection");
          reach.reach("released");
        },
      ),
      { seed, numRuns: 80 },
    );
    reach.assertReached();
  },
  180_000,
);
