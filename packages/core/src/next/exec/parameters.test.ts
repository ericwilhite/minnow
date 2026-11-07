import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { afterAll, afterEach, beforeAll, expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import { PG_EPOCH_UNIX_MICROS } from "../temporal.js";
import { int64At } from "../vector/int64.js";
import { int128At } from "../vector/int128.js";
import { decodeUtf8 } from "../vector/varlen.js";
import { ParameterBindError, UnsupportedPlanError } from "./errors.js";
import { bindParameter as bindChargedParameter, checkParameterType } from "./parameters.js";

import { Governor, type Grant } from "../memory/governor.js";
import type { ParameterValue } from "./executor.js";
const cellGrants: Grant[] = [];
afterEach(() => {
  for (const grant of cellGrants.splice(0)) grant.close();
});
function bindParameter(type: PhysicalType, value: ParameterValue, slot: number) {
  const grant = new Governor(8 << 20).tryAdmit({
    label: "test/parameter",
    minimumBytes: 0,
    spillable: true,
  });
  if (grant === null) throw new Error("parameter admission failed");
  cellGrants.push(grant);
  return bindChargedParameter(type, value, slot, grant);
}
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  // The rebuild core's session time zone is UTC (types.ts), independent of the host's TZ.
  await pg.exec("SET TIME ZONE 'UTC'");
}, 60_000);
afterAll(async () => {
  await pg.close();
});

function decimalText(type: PhysicalType, source: string): string {
  const vector = bindParameter(type, source, 0);
  if (vector.data.layout !== "decimal") throw new Error("decimal expected");
  const data = vector.data;
  const special = data.specials?.[0] ?? 0;
  if (special !== 0) return special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity";
  const coefficients = data.coefficients;
  const c =
    coefficients.repr === "wide"
      ? (coefficients.values[0] ?? 0n)
      : coefficients.repr === "int128"
        ? int128At(coefficients.limbs, 0)
        : int64At(coefficients, 0);
  const scale = data.scale ?? data.scales?.[0] ?? 0;
  const digits = (c < 0n ? -c : c).toString().padStart(scale + 1, "0");
  const text = scale === 0 ? digits : `${digits.slice(0, -scale)}.${digits.slice(-scale)}`;
  return c < 0n ? `-${text}` : text;
}

const numericCase = fc.record({
  coefficient: fc.bigInt({ min: -(10n ** 18n), max: 10n ** 18n }),
  units: fc.integer({ min: -1000, max: 1000 }),
  scale: fc.integer({ min: 0, max: 8 }),
  notation: fc.boolean(),
  tie: fc.boolean(),
  typmod: fc.oneof(
    fc.constant(null),
    fc.record({
      precision: fc.integer({ min: 1, max: 20 }),
      scale: fc.integer({ min: -3, max: 8 }),
    }),
  ),
});

for (const seed of seedsFor("next-exec-bind-numeric", [0xb101, 0xb102]))
  it(`numeric client text binds like PostgreSQL's target cast (${String(seed)})`, async () => {
    const reach = createReach(`bind numeric ${String(seed)}`, [
      "positive",
      "negative",
      "exponent",
      "plain",
      "unconstrained",
      "typmod",
      "tie",
      "negativeScale",
      "refused",
    ] as const);
    await fc.assert(
      fc.asyncProperty(numericCase, async (data) => {
        reach.beginHistory();
        let coefficient = data.coefficient,
          scale = data.scale;
        const mod = data.typmod === null ? null : { ...data.typmod };
        if (data.tie && mod !== null) {
          mod.precision = 15;
          scale = Math.max(0, mod.scale + 1);
          coefficient =
            (BigInt(data.units) * 10n + (data.units < 0 ? -5n : 5n)) *
            10n ** BigInt(Math.max(0, -mod.scale - 1));
          reach.reach("tie");
        }
        reach.reach(coefficient < 0n ? "negative" : "positive");
        reach.reach(data.notation ? "exponent" : "plain");
        reach.reach(mod === null ? "unconstrained" : "typmod");
        if (mod !== null && mod.scale < 0) reach.reach("negativeScale");
        const digits = (coefficient < 0n ? -coefficient : coefficient)
          .toString()
          .padStart(scale + 1, "0");
        const source = data.notation
          ? `${String(coefficient)}e-${String(scale)}`
          : `${coefficient < 0n ? "-" : ""}${scale === 0 ? digits : `${digits.slice(0, -scale)}.${digits.slice(-scale)}`}`;
        const type: PhysicalType = {
          kind: "decimal",
          precision: mod?.precision ?? null,
          scale: mod?.scale ?? null,
        };
        const sqlType =
          mod === null ? "numeric" : `numeric(${String(mod.precision)},${String(mod.scale)})`;
        let expected: string;
        try {
          const result = await pg.query<{ v: string }>(`SELECT ($1::${sqlType})::text AS v`, [
            source,
          ]);
          expected = result.rows[0]?.v ?? "missing";
        } catch (error) {
          expect((error as { code?: string }).code).toBe("22003");
          expect(() => bindParameter(type, source, 0)).toThrow(ParameterBindError);
          reach.reach("refused");
          return;
        }
        expect(decimalText(type, source)).toBe(expected);
      }),
      { seed, numRuns: 100 },
    );
    reach.assertReached();
  });

it("keeps int2/int4/int8 binding exact and reports the offending slot", () => {
  for (const bits of [16, 32] as const) {
    const type: PhysicalType = { kind: "int32", bits };
    const min = -(2 ** (bits - 1)),
      max = 2 ** (bits - 1) - 1;
    for (const value of [min, max])
      expect(bindParameter(type, BigInt(value), 3).data).toMatchObject({
        layout: "i32",
        values: Int32Array.of(value),
      });
    for (const bad of [min - 1, max + 1, 1.5, "1", true]) {
      try {
        bindParameter(type, bad, 3);
        throw new Error("accepted invalid integer");
      } catch (error) {
        expect(error).toBeInstanceOf(ParameterBindError);
        expect((error as ParameterBindError).slot).toBe(3);
      }
    }
  }
  for (const value of [-(1n << 63n), (1n << 63n) - 1n, (1n << 53n) + 1n]) {
    const result = bindParameter({ kind: "int64" }, value, 0);
    if (result.data.layout !== "int64") throw new Error("int64 expected");
    expect(int64At(result.data.values, 0)).toBe(value);
  }
  expect(() => bindParameter({ kind: "int64" }, 2 ** 53, 0)).toThrow(ParameterBindError);
  expect(() => bindParameter({ kind: "int64" }, 1n << 63n, 0)).toThrow(ParameterBindError);
});

it("copies byte parameters and keeps NULL distinct from false and zero", () => {
  const source = Uint8Array.of(1, 2, 3);
  const bound = bindParameter({ kind: "bytes" }, source, 0);
  source.fill(0);
  if (bound.data.layout !== "varlen") throw new Error("bytes expected");
  expect([...bound.data.bytes]).toEqual([1, 2, 3]);
  expect(bindParameter({ kind: "bool" }, null, 0).validity?.[0]).toBe(0);
  expect(bindParameter({ kind: "bool" }, false, 0).validity).toBeNull();
  const text = bindParameter({ kind: "text" }, "O'Brien", 0);
  if (text.data.layout !== "varlen") throw new Error("text expected");
  expect(decodeUtf8(text.data, 0)).toBe("O'Brien");
});

it("normalizes timestamp offsets exactly, with PostgreSQL's zoned/unzoned distinction", async () => {
  const samples = [
    "2025-01-02",
    "2025-01-02 03:04:05.123456+05:30",
    "2025-01-02 03:04:05-08",
    "2025-01-02T03:04:05Z",
    "0001-01-02 03:04:05 BC",
    "12345-01-02 03:04:05+00",
  ];
  for (const zoned of [true, false])
    for (const source of samples) {
      const sqlType = zoned ? "timestamptz" : "timestamp";
      const result = await pg.query<{ v: string }>(
        `SELECT (EXTRACT(EPOCH FROM $1::${sqlType}) * 1000000)::bigint::text AS v`,
        [source],
      );
      const value = bindParameter({ kind: "timestamp", zoned }, source, 0);
      if (value.data.layout !== "int64") throw new Error("timestamp expected");
      expect(int64At(value.data.values, 0), `${source}, zoned=${String(zoned)}`).toBe(
        BigInt(result.rows[0]?.v ?? "") - PG_EPOCH_UNIX_MICROS,
      );
    }
});

it("refuses huge numeric exponents before materializing a coefficient", () => {
  const mock = vi.spyOn(globalThis, "BigInt").mockImplementation(() => {
    throw new Error("coefficient was materialized");
  });
  try {
    expect(() =>
      bindParameter({ kind: "decimal", precision: null, scale: null }, "1e131072", 0),
    ).toThrow("numeric input is out of range");
    expect(mock).not.toHaveBeenCalled();
  } finally {
    mock.mockRestore();
  }
});

it("refuses unavailable parameter families during preparation", () => {
  expect(() => checkParameterType({ kind: "array", element: { kind: "text" } })).toThrow(
    UnsupportedPlanError,
  );
  expect(() => checkParameterType({ kind: "text", maxLength: 3 })).toThrow(UnsupportedPlanError);
});
