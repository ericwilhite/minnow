/** PostgreSQL float8_accum order, exact integer transitions and SQL NULL semantics. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import type { AggregateCall } from "../../ir/ir.js";
import type { Batch, Vector } from "../../vector/vector.js";
import {
  I32,
  I64,
  F64,
  col,
  numbers,
  bigints,
  dictionary,
  constant,
  valueAt,
} from "../kernels/test-support.js";
import { IDENTITY } from "../kernels/vectors.js";
import { accumulatorFor } from "./states.js";
import { TypedLayout, typedLayoutCalls } from "./typed-layout.js";

const input = [
  ...Array.from({ length: 5 }, () => I32),
  ...Array.from({ length: 7 }, () => F64),
  I32,
  I32,
  F64,
  F64,
  I32,
  I32,
  I64,
];
const call = (fn: AggregateCall["fn"], column: number, type = F64): AggregateCall => ({
  fn,
  type,
  args: column < 0 ? [] : [col(column, input[column] ?? I32)],
  filter: null,
  distinct: false,
  orderBy: [],
});
const baseCalls = [
  call("count_star", -1, I64),
  ...Array.from({ length: 5 }, (_, i) => call("sum", i, I64)),
  ...Array.from({ length: 5 }, (_, i) => call("avg", i + 5)),
  call("min", 10),
  call("max", 11),
  call("min", 12, I32),
  call("max", 13, I32),
  call("sum", 14),
  call("sum", 15),
  call("avg", 16),
  call("count", 17, I64),
  call("max", 18, I64),
];
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const numeric = (v: unknown) => (v === null ? null : Number(v));
function literal(v: number | null, kind: string): string {
  if (v === null) return "NULL";
  return kind === "float64" ? `'${Object.is(v, -0) ? "-0" : String(v)}'::float8` : String(v);
}
it.each(seedsFor("typed-layout-transitions", [0xb481, 0xb482]))(
  "restores numeric records before every uncertified batch (seed %i)",
  async (seed) => {
    const reach = createReach("numeric record promotion", [
      "packed",
      "NULL",
      "dictionary",
      "constant",
      "magnitude",
      "groups",
      "int64",
      "zero",
      "borrowed",
      "PG",
      "released",
      "Sxx",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: -10000, max: 10000 }), { minLength: 24, maxLength: 24 }),
        fc.boolean(),
        fc.integer({ min: 0, max: 19 }),
        async (base, selected, rotation) => {
          const calls = [...baseCalls.slice(rotation), ...baseCalls.slice(0, rotation)];
          reach.beginHistory();
          for (const mode of [
            "NULL",
            "dictionary",
            "constant",
            "magnitude",
            "groups",
            "int64",
            "zero",
          ] as const) {
            let held = 0;
            const charge = {
              allocate(n: number) {
                held += n;
                return true;
              },
              free(n: number) {
                held -= n;
              },
            };
            const members = typedLayoutCalls(calls, input);
            if (!members) throw new Error("Missing numeric layout");
            const byIndex = new Map(members.map((m, slot) => [m.index, slot]));
            const averageColumn = members.find((m) => m.offset === 6)?.column ?? 5;
            const bank = new TypedLayout(members, input, charge),
              ordinary = calls.map((c) =>
                accumulatorFor(c.fn, c.type, c.args[0]?.type ?? null, charge),
              );
            const records: string[] = [];
            let expectedN = 0,
              expectedSx = 0,
              expectedQ = 0;
            try {
              for (let step = 0; step < 3; step++) {
                const values = input.map((t, c) =>
                  Array.from({ length: 8 }, (_, i) => {
                    const v = base[step * 8 + i] ?? 0;
                    if (step === 2 && mode === "NULL" && c === 5 && i % 2 === 0) return null;
                    if (step === 2 && mode === "magnitude" && c === 5) return 1e130;
                    if (step === 2 && mode === "int64" && c === 18) return v * 2 ** 40;
                    if (c === 17 && i % 3 === 0) return null;
                    return t.kind === "float64"
                      ? mode === "zero"
                        ? -0
                        : v / 8 + (c % 3) * 0.1
                      : v;
                  }),
                );
                if (step === 2 && mode === "constant") for (const a of values) a.fill(a[0] ?? 0);
                const columns: Vector[] = values.map((a, c) => numbers(input[c] ?? I32, a));
                if (step === 2 && mode === "zero")
                  columns[0] = dictionary(
                    need(columns, 0) as ReturnType<typeof numbers>,
                    [0, 1, 2, 3, 4, 5, 6, 7],
                  );
                if (step === 2 && mode === "int64")
                  columns[18] = bigints(
                    I64,
                    need(values, 18).map((v) => (v === null ? null : BigInt(v))),
                  );
                if (step === 2 && mode === "dictionary")
                  for (let c = 0; c < columns.length; c++) {
                    const v = columns[c];
                    if (v?.form === "flat")
                      columns[c] = dictionary(
                        v,
                        Array.from({ length: 8 }, (_, i) => (values[c]?.[i] === null ? null : i)),
                      );
                  }
                if (step === 2 && mode === "constant")
                  for (let c = 0; c < columns.length; c++) {
                    const a = values[c];
                    if (a) columns[c] = constant(numbers(input[c] ?? I32, [a[0] ?? null]), 8);
                  }
                const rows = selected ? Uint16Array.of(0, 2, 3, 5, 7) : IDENTITY,
                  count = selected ? 5 : 8;
                const gids = new Int32Array(8);
                if (step === 2 && mode === "groups") gids.fill(1024);
                const groups = step === 2 && mode === "groups" ? 1025 : 1;
                for (const m of bank.members) m.accumulator.grow(groups);
                for (const a of ordinary) a.grow(groups);
                const batch: Batch = {
                  length: 8,
                  columns,
                  selection: selected ? { rows, count } : null,
                };
                const fused = bank.tryUpdate(batch, gids, rows, count);
                if (step < 2) {
                  expect(fused).toBe(true);
                  reach.reach("packed");
                } else {
                  expect(fused).toBe(false);
                  reach.reach(mode);
                }
                for (let k = 0; k < count; k++) {
                  const r = rows[k] ?? 0,
                    g = gids[r] ?? 0;
                  records.push(
                    `(${String(g)},${values.map((a, c) => literal(a[r] ?? null, input[c]?.kind ?? "int32")).join(",")})`,
                  );
                  if (step < 2) {
                    const v = values[averageColumn]?.[r] ?? 0,
                      n = expectedN + 1,
                      sx = expectedSx + v,
                      tmp = v * n - sx;
                    if (expectedN > 0) expectedQ += (tmp * tmp) / (n * expectedN);
                    expectedN = n;
                    expectedSx = sx;
                  }
                }
                if (step < 2) {
                  expect(bank.transition(0, 0)).toEqual([expectedN, expectedSx, expectedQ]);
                  reach.reach("Sxx");
                }
                for (let i = 0; i < calls.length; i++) {
                  const c = calls[i],
                    arg = c?.args[0],
                    v = arg?.op === "column" ? (columns[arg.index] ?? null) : null;
                  if (!fused)
                    bank.members[byIndex.get(i) ?? -1]?.accumulator.update(
                      v,
                      gids,
                      rows,
                      count,
                      false,
                    );
                  ordinary[i]?.update(v, gids, rows, count, false);
                }
                // Reader-owned vectors may be reused immediately after push.
                for (const v of columns)
                  if (v.form === "flat" && (v.data.layout === "i32" || v.data.layout === "f64"))
                    v.data.values.fill(999);
                reach.reach("borrowed");
              }
              const expressions = calls.map((c) => {
                const arg = c.args[0],
                  name = arg?.op === "column" ? `c${String(arg.index)}` : "*";
                return c.fn === "count_star"
                  ? "count(*)"
                  : c.fn === "avg" && arg?.op === "column" && input[arg.index]?.kind === "int32"
                    ? `sum(${name})::float8/count(${name})::float8`
                    : `${c.fn}(${name})`;
              });
              const result = await pg.query(
                `SELECT g,${expressions.map((e, i) => `${e} AS a${String(i)}`).join(",")} FROM (VALUES ${records.join(",")}) t(g,${input.map((_, i) => `c${String(i)}`).join(",")}) GROUP BY g ORDER BY g`,
              );
              for (const r of result.rows as Array<Record<string, unknown>>) {
                const g = Number(r.g);
                for (let i = 0; i < calls.length; i++) {
                  const actual = valueAt(
                      need(bank.members, byIndex.get(i) ?? -1).accumulator.emit(g, 1),
                      0,
                    ),
                    expected = valueAt(need(ordinary, i).emit(g, 1), 0);
                  expect(actual).toEqual(expected);
                  // JSON from PG does not retain negative zero; ordinary state comparison does.
                  expect(
                    numeric(actual) === 0 ? 0 : numeric(actual),
                    `${mode} aggregate ${String(i)}`,
                  ).toEqual(numeric(r[`a${String(i)}`]) === 0 ? 0 : numeric(r[`a${String(i)}`]));
                }
              }
              reach.reach("PG");
            } finally {
              for (const m of bank.members) m.accumulator.close();
              for (const a of ordinary) a.close();
            }
            expect(held).toBe(0);
            reach.reach("released");
          }
        },
      ),
      { seed, numRuns: 20 },
    );
    reach.assertReached();
  },
  60000,
);

function need<T>(values: readonly T[], index: number): T {
  const v = values[index];
  if (v === undefined) throw new Error("Missing test fixture member");
  return v;
}

/** This queries a single float8_accum independently, including its hidden Sxx. */
async function pgTransition(
  values: ReadonlyArray<number | null>,
): Promise<readonly number[] | string> {
  try {
    const result = await pg.query<{
      s: string;
    }>(`WITH RECURSIVE raw(ord,x) AS (VALUES ${values.map((v, i) => `(${String(i)},${literal(v, "float64")})`).join(",")}),
      inp AS (SELECT row_number() OVER (ORDER BY ord) rn,x FROM raw WHERE x IS NOT NULL),
      walk(rn,s) AS (SELECT 0::bigint,ARRAY[0,0,0]::float8[] UNION ALL SELECT inp.rn,float8_accum(walk.s,inp.x) FROM walk JOIN inp ON inp.rn=walk.rn+1)
      SELECT s::text s FROM walk ORDER BY rn DESC LIMIT 1`);
    return need(result.rows, 0).s.slice(1, -1).split(",").map(Number);
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error)) throw error;
    return String(error.code);
  }
}
it.each(seedsFor("typed-layout-exceptions", [0xb483, 0xb484]))(
  "keeps PG Sxx and exceptional transitions after record promotion (seed %i)",
  async (seed) => {
    const reach = createReach("numeric record exceptions", [
      "packed",
      "mask",
      "boundary",
      "special",
      "overflow",
      "independent PG",
      "Sxx",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: -100, max: 100 }), { minLength: 12, maxLength: 12 }),
        async (base) => {
          reach.beginHistory();
          const calls = baseCalls,
            selected = typedLayoutCalls(calls, input);
          if (!selected) throw new Error("Missing numeric layout");
          for (const mode of ["mask", "boundary", "special", "overflow"] as const)
            for (let c = 0; c < 5; c++) {
              const bank = new TypedLayout(selected, input, {
                allocate: () => true,
                free: () => undefined,
              });
              const groups = new Int32Array(12),
                prefix = input.map((t) =>
                  numbers(
                    t,
                    base.map((v) => (t.kind === "float64" ? v / 8 : v)),
                  ),
                );
              for (const m of bank.members) m.accumulator.grow(1);
              try {
                expect(
                  bank.tryUpdate(
                    { columns: prefix, length: 12, selection: null },
                    groups,
                    IDENTITY,
                    12,
                  ),
                ).toBe(true);
                reach.reach("packed");
                const tail = Array.from({ length: 4 }, (_, i) =>
                  mode === "mask"
                    ? i % 2 === 0
                      ? null
                      : 3
                    : mode === "boundary"
                      ? (i % 2 ? -1 : 1) * 2 ** 400 * (1 + Number.EPSILON)
                      : mode === "special"
                        ? ([NaN, Infinity, -Infinity, -0, 0][c] ?? 0)
                        : (i % 2 ? -1 : 1) * 1e154,
                );
                const columns: Vector[] = [...prefix];
                columns[5 + c] = numbers(F64, tail);
                if (mode === "special") columns[0] = dictionary(need(prefix, 0), [0, 1, 2, 3]);
                const batch: Batch = { columns, length: 4, selection: null };
                expect(bank.tryUpdate(batch, groups, IDENTITY, 4)).toBe(false);
                const slot = selected.findIndex((m) => m.offset === 6 + c),
                  member = need(bank.members, slot);
                let failure: string | undefined;
                try {
                  member.accumulator.update(columns[5 + c] ?? null, groups, IDENTITY, 4, false);
                } catch (error) {
                  if (!(error instanceof Error) || !("sqlState" in error)) throw error;
                  failure = String(error.sqlState);
                }
                const pgState = await pgTransition([...base.map((v) => v / 8), ...tail]);
                reach.reach("independent PG");
                if (typeof pgState === "string") {
                  expect(pgState).toBe("22003");
                  expect(failure).toBe("22003");
                  reach.reach("overflow");
                } else {
                  expect(failure).toBeUndefined();
                  expect(bank.transition(c, 0)).toEqual(pgState);
                  if ((pgState[2] ?? 0) > 0) reach.reach("Sxx");
                  reach.reach(mode);
                }
              } finally {
                for (const m of bank.members) m.accumulator.close();
              }
            }
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("typed-layout-prefix-limit", [0xb485, 0xb486]))(
  "promotes at the exact prefix row bound and releases failed growth (seed %i)",
  (seed) => {
    const reach = createReach("numeric record bounds", [
      "row bound",
      "restore",
      "growth refusal",
      "promotion refusal",
      "release",
      "integer carry",
    ]);
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 3 }), (extra) => {
        reach.beginHistory();
        const selected = typedLayoutCalls(baseCalls, input);
        if (!selected) throw new Error("Missing numeric layout");
        let bytes = 0,
          cap = Infinity;
        const bank = new TypedLayout(selected, input, {
          allocate(n) {
            if (bytes + n > cap) return false;
            bytes += n;
            return true;
          },
          free(n) {
            bytes -= n;
          },
        });
        const value = seed % 2 === 0 ? -0x80000000 : 0x7fffffff;
        const columns = input.map((type) =>
            numbers(
              type,
              Array.from({ length: 2048 }, () => value),
            ),
          ),
          gids = new Int32Array(2048),
          batch: Batch = { columns, length: 2048, selection: null };
        try {
          for (const m of bank.members) m.accumulator.grow(1);
          for (let n = 0; n < 2 ** 21; n += 2048)
            expect(bank.tryUpdate(batch, gids, IDENTITY, 2048)).toBe(true);
          expect(bank.tryUpdate({ ...batch, length: extra }, gids, IDENTITY, extra)).toBe(false);
          reach.reach("row bound");
          for (const m of bank.members) {
            const arg = baseCalls[m.index]?.args[0];
            m.accumulator.update(
              arg?.op === "column" ? (columns[arg.index] ?? null) : null,
              gids,
              IDENTITY,
              extra,
              false,
            );
          }
          const count = need(
            bank.members,
            selected.findIndex((m) => m.offset === 0),
          );
          expect(valueAt(count.accumulator.emit(0, 1), 0)).toBe(BigInt(2 ** 21 + extra));
          const sum = need(
            bank.members,
            selected.findIndex((m) => m.offset === 1),
          );
          expect(valueAt(sum.accumulator.emit(0, 1), 0)).toBe(
            BigInt(2 ** 21 + extra) * BigInt(value),
          );
          const avg = need(
            bank.members,
            selected.findIndex((m) => m.offset === 22),
          );
          expect(valueAt(avg.accumulator.emit(0, 1), 0)).toBe(value);
          reach.reach("integer carry");
          reach.reach("restore");
        } finally {
          for (const m of bank.members) m.accumulator.close();
        }
        expect(bytes).toBe(0);
        reach.reach("release");
        for (const phase of ["growth", "promotion"] as const) {
          const other = new TypedLayout(selected, input, {
            allocate(n) {
              if (bytes + n > cap) return false;
              bytes += n;
              return true;
            },
            free(n) {
              bytes -= n;
            },
          });
          try {
            if (phase === "growth") {
              cap = 0;
              expect(() => need(other.members, 0).accumulator.grow(1)).toThrow(
                "aggregation growth was refused",
              );
              reach.reach("growth refusal");
            } else {
              cap = Infinity;
              for (const m of other.members) m.accumulator.grow(1);
              expect(other.tryUpdate(batch, gids, IDENTITY, 1)).toBe(true);
              cap = bytes;
              const member = need(
                other.members,
                selected.findIndex((m) => m.offset === 6),
              );
              expect(() =>
                member.accumulator.update(columns[5] ?? null, gids, IDENTITY, 1, false),
              ).toThrow("aggregation growth was refused");
              reach.reach("promotion refusal");
            }
          } finally {
            for (const m of other.members) m.accumulator.close();
          }
          expect(bytes).toBe(0);
          cap = Infinity;
        }
      }),
      { seed, numRuns: 3 },
    );
    reach.assertReached();
  },
  60000,
);
