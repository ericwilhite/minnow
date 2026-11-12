/** Certified batch folding must preserve exact NUMERIC transitions, including after a cut.
 * Authority: PostgreSQL NUMERIC SUM/MIN/MAX and Accumulator's charged-growth contract.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import {
  createReach,
  NUMERIC_FIXED_REACH,
  NUMERIC_COEFFICIENT_BOUND_REACH,
  NUMERIC_FIXED_CHARGE_REACH,
} from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { Governor } from "../../memory/governor.js";
import type { Vector } from "../../vector/vector.js";
import {
  decimals,
  dictionary,
  decimal,
  sqlLiteral,
  valueAt,
  type DecimalValue,
} from "../kernels/test-support.js";
import { PageWriter, decodePage } from "../spill-pages.js";
import { StateRow, StateReader } from "./state-row.js";
import { accumulatorFor, fusedSums } from "./states.js";
import { IDENTITY } from "../kernels/vectors.js";
import { GroupValues } from "./values.js";

const D = decimal(null, null);
const CASES = [
  "fixed",
  "selected",
  "nulls",
  "unsafe",
  "int64",
  "int64 negative",
  "int64 exact",
  "int64 boundaries",
  "int64 carry overflow",
  "int128",
  "wide",
  "mixed",
  "special",
  "dictionary",
  "constant",
  "many groups",
  "full batch",
] as const;
type Kind = (typeof CASES)[number];
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});
function display(value: unknown): string | null {
  if (value === null) return null;
  if (typeof value === "string") return value;
  if (!Array.isArray(value)) throw new TypeError("Not a NUMERIC result");
  const c = value[0] as bigint,
    s = value[1] as number;
  const digits = (c < 0n ? -c : c).toString().padStart(s + 1, "0");
  return `${c < 0n ? "-" : ""}${s === 0 ? digits : `${digits.slice(0, -s)}.${digits.slice(-s)}`}`;
}
it.each(seedsFor("next-numeric-fixed", [0xb721, 0xb722]))(
  "certifies exact fixed-scale folds and falls back across representations (seed %i)",
  async (seed) => {
    const reach = createReach("fixed-scale NUMERIC", NUMERIC_FIXED_REACH);
    const examples: Array<[Kind, number, number]> = CASES.map((kind) => [kind, 17, 2]);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...CASES),
        fc.integer({ min: -999999, max: 999999 }),
        fc.integer({ min: 0, max: 5 }),
        async (kind, coefficient, baseScale) => {
          reach.beginHistory();
          const groups = kind === "many groups" ? 1025 : kind === "constant" ? 1 : 3;
          const length =
            kind === "int64 boundaries"
              ? 3
              : kind === "full batch"
                ? 2048
                : kind === "many groups"
                  ? 1025
                  : 65;
          const gov = new Governor(32 << 20);
          const grant = await gov.admit({
            label: "numeric fold",
            minimumBytes: 24 << 20,
            spillable: true,
          });
          const writer = new PageWriter(grant);
          const continuous = ["sum", "min", "max", "avg"].map((fn) =>
            accumulatorFor(fn, D, D, grant),
          );
          const restored = ["sum", "min", "max", "avg"].map((fn) =>
            accumulatorFor(fn, D, D, grant),
          );
          const expectedRows: string[] = [];
          let ordinal = 0;
          const set = vi.spyOn(GroupValues.prototype, "set");
          const fills = vi.spyOn(Uint32Array.prototype, "fill");
          try {
            for (const acc of [...continuous, ...restored]) acc.grow(groups);
            for (let part = 0; part < 3; part++) {
              const scale = kind === "mixed" ? baseScale + part : baseScale;
              const tier = kind.startsWith("int64")
                ? "int64"
                : kind === "int128"
                  ? "int128"
                  : kind === "wide"
                    ? "wide"
                    : "safe";
              const values: DecimalValue[] = Array.from({ length }, (_, i) => {
                if (kind === "nulls" || (kind === "selected" && i % 7 === 0)) return null;
                if (kind === "special" && i % 11 === 0)
                  return ["NaN", "Infinity", "-Infinity"][i % 3] as
                    "NaN" | "Infinity" | "-Infinity";
                if (kind === "int64 boundaries")
                  return [[(1n << 63n) - 1n, -(1n << 63n), -1n][i % 3] ?? 0n, scale];
                if (kind === "int64 carry overflow") {
                  const edge = (1n << 63n) - 1n;
                  return [[edge, 1n, -1n, -edge - 1n, -1n, 1n][Math.floor(i / 3) % 6] ?? 0n, scale];
                }
                const magnitude = kind.startsWith("int64")
                  ? (kind === "int64 negative" || (kind === "int64 exact" && i % 2 !== 0)
                      ? -1n
                      : 1n) *
                    (1n << 60n)
                  : kind === "int128"
                    ? 1n << 100n
                    : kind === "wide"
                      ? 1n << 150n
                      : 0n;
                return [
                  magnitude +
                    BigInt(
                      kind === "selected"
                        ? Math.abs(coefficient) + 1 + (i % 5)
                        : (i % 2 === 0 ? 1 : -1) * coefficient + (i % 5),
                    ),
                  scale,
                ];
              });
              if (kind === "unsafe") {
                const sequence = [2n ** 52n, 2n ** 52n, 1n, -(2n ** 52n), -(2n ** 52n)];
                for (let i = 0; i < values.length; i++)
                  values[i] = [sequence[Math.floor(i / 3) % sequence.length] ?? 0n, scale];
              }
              let vector: Vector = decimals(D, values, tier, kind === "mixed" ? null : scale);
              if (kind === "dictionary")
                vector = dictionary(
                  vector,
                  Array.from({ length }, (_, i) => length - 1 - i),
                );
              if (kind === "constant")
                vector = {
                  form: "constant",
                  type: D,
                  length,
                  value: decimals(D, [values[0] ?? null], tier, scale),
                };
              const gids = Int32Array.from({ length }, (_, i) => i % groups);
              const active = Array.from({ length }, (_, i) => i).filter(
                (i) => kind !== "selected" || i % 3 !== 1,
              );
              if (kind === "selected") active.reverse();
              const rows = Uint16Array.from(active);
              for (const i of active)
                expectedRows.push(
                  `(${String(gids[i])},${String(ordinal++)},${sqlLiteral("numeric", valueAt(vector, i) as DecimalValue)})`,
                );
              const before = set.mock.calls.length;
              fills.mockClear();
              for (const acc of continuous) acc.update(vector, gids, rows, rows.length, false);
              if (kind.startsWith("int64") && part === 0) {
                expect(
                  fills.mock.calls.length,
                  "int64 batch scratch was attempted",
                ).toBeGreaterThanOrEqual(2);
                reach.reach("int64 batch attempted");
              }
              if ((kind === "fixed" || kind === "full batch") && part === 0) {
                expect(set.mock.calls.length - before).toBeLessThanOrEqual(2 * groups);
                reach.reach("batch winners once per group");
              }
              if (part === 0) {
                for (let a = 0; a < continuous.length; a++)
                  for (let g = 0; g < groups; g++) {
                    const row = new StateRow(grant);
                    try {
                      continuous[a]?.save?.(g, row);
                      const encoded = writer.encode(
                        { queryId: 1, run: 0, partition: 0, sequence: g },
                        row.columns,
                        1,
                      );
                      if (!encoded) throw new Error("unfunded state encode");
                      const decoded = decodePage(
                        encoded,
                        row.columns.map((c) => c.type),
                        grant,
                      );
                      if ("needed" in decoded) throw new Error("unfunded state decode");
                      try {
                        restored[a]?.restore?.(g, new StateReader(decoded.columns, 0));
                      } finally {
                        grant.free(decoded.charged);
                      }
                    } finally {
                      row.close();
                    }
                  }
                reach.reach("saved prefix before suffix");
              } else for (const acc of restored) acc.update(vector, gids, rows, rows.length, false);
            }
            const query = await pg.query<{
              g: number;
              s: string | null;
              m: string | null;
              x: string | null;
              a: string | null;
            }>(
              `SELECT g,SUM(v)::text s,MIN(v)::text m,MAX(v)::text x,AVG(v)::text a FROM (VALUES ${expectedRows.join(",")}) t(g,i,v) GROUP BY g ORDER BY g`,
            );
            for (let a = 0; a < continuous.length; a++) {
              const l = continuous[a],
                r = restored[a];
              if (!l || !r) throw new Error("missing accumulator");
              const bytes = l.outputBytes(0, groups) + r.outputBytes(0, groups);
              expect(grant.allocate(bytes)).toBe(true);
              try {
                const left = l.emit(0, groups),
                  right = r.emit(0, groups);
                for (let g = 0; g < groups; g++) {
                  expect(valueAt(right, g)).toEqual(valueAt(left, g));
                  const expected = query.rows.find((row) => row.g === g);
                  expect(display(valueAt(left, g))).toBe(
                    expected ? [expected.s, expected.m, expected.x, expected.a][a] : null,
                  );
                }
              } finally {
                grant.free(bytes);
              }
            }
            reach.reach(kind);
            reach.reach("PGlite exact");
          } finally {
            fills.mockRestore();
            set.mockRestore();
            for (const acc of [...continuous, ...restored]) acc.close();
            writer.close();
            expect(grant.allocated).toBe(0);
            grant.close();
            expect(gov.usedBytes).toBe(0);
            reach.reach("zero charges");
          }
        },
      ),
      { seed, numRuns: examples.length + 8, examples },
    );
    reach.assertReached();
  },
  120_000,
);

it.each(seedsFor("next-numeric-coefficient-bound", [0xb723, 0xb724]))(
  "certifies the stored coefficient rather than the declared negative scale (seed %i)",
  async (seed) => {
    const reach = createReach("NUMERIC stored coefficient bound", NUMERIC_COEFFICIENT_BOUND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: -3, max: -1 }),
        fc.integer({ min: 1, max: 1000 }),
        async (scale, tail) => {
          reach.beginHistory();
          const input = decimal(12, scale);
          const coefficient = (10n ** 12n - BigInt(tail)) * 10n ** BigInt(-scale);
          const vector = decimals(
            input,
            Array.from({ length: 2048 }, () => [coefficient, 0] as const),
            "safe",
            0,
          );
          const gov = new Governor(8 << 20),
            grant = await gov.admit({
              label: "stored precision",
              minimumBytes: 4 << 20,
              spillable: true,
            });
          try {
            const answer = await pg.query<{ s: string }>(
              `SELECT SUM(v)::text s FROM (SELECT ${sqlLiteral(`numeric(12,${String(scale)})`, [coefficient, 0])} v FROM generate_series(1,2048)) t`,
            );
            for (const fused of [false, true]) {
              const acc = accumulatorFor("sum", D, input, grant);
              try {
                acc.grow(1);
                const values = fused ? acc.fusedValues?.(vector, IDENTITY, 2048) : null;
                if (values) {
                  const out = new Float64Array(1);
                  fusedSums([values], 1, 2048, out);
                  acc.addFused?.(out[0] ?? 0, 2048);
                } else acc.update(vector, new Int32Array(2048), IDENTITY, 2048, true);
                expect(display(valueAt(acc.emit(0, 1), 0))).toBe(answer.rows[0]?.s);
                reach.reach(fused ? "fused admission" : "ordinary update");
              } finally {
                acc.close();
              }
            }
            reach.reach("negative scale");
            reach.reach("PGlite exact");
          } finally {
            expect(grant.allocated).toBe(0);
            grant.close();
            expect(gov.usedBytes).toBe(0);
            reach.reach("zero charges");
          }
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [-3, 1],
          [-2, 3],
          [-1, 17],
        ],
      },
    );
    reach.assertReached();
  },
  60_000,
);

it.each(seedsFor("next-numeric-fixed-charge", [0xb725, 0xb726]))(
  "funds every new backing buffer before construction (seed %i)",
  (seed) => {
    const reach = createReach("NUMERIC buffer funding", NUMERIC_FIXED_CHARGE_REACH);
    // Prime the immutable shared NULL descriptor before observing per-state allocation.
    const prime = accumulatorFor("min", D, D, { allocate: () => true, free: () => undefined });
    prime.grow(1);
    prime.close();
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 1024 }), (groups) => {
        reach.beginHistory();
        for (const fn of ["sum", "min", "max"] as const) {
          let unconsumed = 0,
            held = 0,
            constructions = 0;
          const charge = {
            allocate: (bytes: number) => {
              unconsumed += bytes;
              held += bytes;
              return true;
            },
            free: (bytes: number) => {
              held -= bytes;
            },
          };
          const state = accumulatorFor(fn, D, D, charge);
          const types = [Uint8Array, Uint16Array, Uint32Array, Int32Array, Float64Array] as const;
          try {
            for (const Type of types)
              (globalThis as Record<string, unknown>)[Type.name] = new Proxy(Type, {
                construct(target, args: unknown[]) {
                  const length = args[0];
                  if (typeof length !== "number")
                    throw new Error("Unexpected constructor form during state growth");
                  const bytes = length * target.BYTES_PER_ELEMENT;
                  expect(unconsumed, "unfunded NUMERIC backing buffer").toBeGreaterThanOrEqual(
                    bytes,
                  );
                  unconsumed -= bytes;
                  constructions += 1;
                  return Reflect.construct(target, args) as object;
                },
              });
            state.grow(groups);
            expect(constructions).toBeGreaterThan(0);
            expect(unconsumed).toBe(0);
            reach.reach(fn);
            reach.reach("charged before construction");
          } finally {
            for (const Type of types) (globalThis as Record<string, unknown>)[Type.name] = Type;
            state.close();
            expect(held).toBe(0);
            reach.reach("zero charges");
          }
        }
      }),
      { seed, numRuns: 12, examples: [[1], [20], [1024]] },
    );
    reach.assertReached();
  },
);
