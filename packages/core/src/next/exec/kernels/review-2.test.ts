/**
 * Review B4-p2a-2: each finding's probe as a committed regression test.
 *
 * 1. A cast's compiled node carries the cast's target type, so a call over a widened argument
 *    binds the overload lowering chose (R19): every implicit coercion lowering emits, composed with
 *    a call or comparison on the target type, plus `SELECT $1 + 40000` lowered for real.
 * 2. More than 32 (scale, scale) pairs in one batch stay in the limb tiers.
 * 3. DATE → TIMESTAMP(TZ) is 22008 outside PostgreSQL's finite timestamp range.
 */
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import { IMPLICIT_CASTS, physicalTypeOfSql, type SqlType } from "../../ir/functions.js";
import type { ScalarExpr } from "../../ir/ir.js";
import { samePhysicalType, type PhysicalType } from "../../types.js";
import type { FlatVector } from "../../vector/vector.js";
import { DatetimeOverflowError, UnsupportedPlanError } from "../errors.js";
import { FUNCTION_BINDINGS, castRefusal, overloadKey } from "./bindings.js";
import {
  I16,
  I32,
  batchOf,
  call,
  cmp,
  col,
  compileFor,
  decimal,
  decimals,
  numbers,
  texts,
  valueAt,
} from "./test-support.js";

/** A one-row vector of `type` holding a small value every cast of the table accepts. */
function one(type: PhysicalType): FlatVector {
  switch (type.kind) {
    case "decimal":
      return decimals(type, [[7n, 0]]);
    case "json":
      return texts(["1"], type);
    default:
      return numbers(type, [7]);
  }
}

/** Whether `fn(T, T)` over `sql` has a kernel binding. */
function hasKernel(fn: string, sql: SqlType): boolean {
  const entry = FUNCTION_BINDINGS[fn];
  if (entry === undefined) return false;
  const binding =
    "overloads" in entry ? entry.overloads[overloadKey({ args: [sql, sql], result: sql })] : entry;
  return binding !== undefined && "kernel" in binding;
}

const PAIRS = Object.entries(IMPLICIT_CASTS).flatMap(([from, targets]) =>
  targets.map((to) => [`${from} → ${to}`, from as SqlType, to] as const),
);

describe("review B4-p2a-2", () => {
  it.each(PAIRS)(
    "1. an implicit %s cast composes with calls on its target type",
    (_n, from, to) => {
      const source = physicalTypeOfSql(from);
      const target = physicalTypeOfSql(to);
      const cast: ScalarExpr = { op: "cast", input: col(0, source), type: target };
      if (castRefusal(from, to) !== null) {
        // Not implemented yet: refused (construct "cast"), never compiled with the wrong type.
        expect(() => compileFor(cast, [source])).toThrow(UnsupportedPlanError);
        return;
      }
      const compiled = compileFor(cast, [source]);
      expect(samePhysicalType(compiled.type, target)).toBe(true);
      const value = compiled.evaluate(batchOf([one(source)]), null);
      expect(samePhysicalType(value.type, target)).toBe(true);
      // A call on the target type binds from the cast's type, never the source's.
      const expr: ScalarExpr = hasKernel("add", to)
        ? call("add", target, cast, col(1, target))
        : cmp("=", cast, col(1, target));
      const out = compileFor(expr, [source, target]).evaluate(
        batchOf([one(source), one(target)]),
        null,
      );
      expect(out.length).toBe(1);
    },
  );

  it("1. widened int2 arguments bind as int4 (40000 + 1 does not overflow int2)", () => {
    const cast: ScalarExpr = { op: "cast", input: col(0, I16), type: I32 };
    const compiled = compileFor(call("add", I32, cast, col(1, I32)), [I16, I32]);
    const out = compiled.evaluate(batchOf([numbers(I16, [1]), numbers(I32, [40000])]), null);
    expect(valueAt(out, 0)).toBe(40001);
  });

  it("1. lowered SELECT $1 + 40000 with a SMALLINT parameter compiles and evaluates", async () => {
    const { createLowerer } = await import("../../lower/create.js");
    const { MemoryCatalog } = await import("../../lower/memory-catalog.js");
    const lowered = createLowerer().lower("SELECT $1 + 40000", {
      catalog: new MemoryCatalog().view,
      parameterTypes: [I16],
      statistics: { table: () => ({ rowCount: 1, column: () => null }) },
      costs: {
        decodePerValue: 1,
        filterPerRow: 1,
        hashBuildPerRow: 1,
        hashProbePerRow: 1,
        aggregatePerRow: 1,
        sortPerRowLog: 1,
        emitPerValue: 1,
        spillBytesPerSecond: 1,
      },
    });
    if (lowered.kind !== "query" || lowered.plan.root.op !== "project")
      throw new Error("a projection plan expected");
    const expr = lowered.plan.root.exprs[0];
    if (expr === undefined) throw new Error("missing projection");
    const compiled = compileFor(expr, [], { slots: [numbers(I16, [1])] });
    expect(valueAt(compiled.evaluate({ length: 1, columns: [], selection: null }, null), 0)).toBe(
      40001,
    );
  }, 120_000);

  it("2. 33 scale pairs with int64-size coefficients stay in the limb tiers", () => {
    const D = decimal(null, null);
    const a = decimals(
      D,
      Array.from({ length: 33 }, (_, i) => [2n ** 60n, i] as const),
      "int64",
    );
    const b = decimals(
      D,
      Array.from({ length: 33 }, (_, i) => [1n, i] as const),
      "int64",
    );
    const out = compileFor(call("add", D, col(0, D), col(1, D)), [D, D]).evaluate(
      batchOf([a, b]),
      null,
    );
    if (out.form !== "flat" || out.data.layout !== "decimal") throw new Error("decimal expected");
    expect(out.data.coefficients.repr).toBe("int64");
    for (let i = 0; i < 33; i++) expect(valueAt(out, i)).toEqual([2n ** 60n + 1n, i]);
  });

  it("2. 2,048 rows over more than 1,900 distinct scale pairs stay in the limb tiers", () => {
    const D = decimal(null, null);
    const n = 2048;
    // Pairs (sa, sa + d) with |d| ≤ 10: thousands of distinct pairs, every aligned value and
    // result within 128 bits (|c| < 2^41 · 10^10).
    const sa = (i: number) => Math.floor(i / 21);
    const sb = (i: number) => Math.max(0, sa(i) + (i % 21) - 10);
    const a = decimals(
      D,
      Array.from({ length: n }, (_, i) => [2n ** 40n + BigInt(i), sa(i)] as const),
      "int64",
    );
    const b = decimals(
      D,
      Array.from({ length: n }, (_, i) => [BigInt(i), sb(i)] as const),
      "int64",
    );
    const pairs = new Set(Array.from({ length: n }, (_, i) => `${String(sa(i))}/${String(sb(i))}`));
    expect(pairs.size).toBeGreaterThan(1900);
    const out = compileFor(call("sub", D, col(0, D), col(1, D)), [D, D]).evaluate(
      batchOf([a, b]),
      null,
    );
    if (out.form !== "flat" || out.data.layout !== "decimal") throw new Error("decimal expected");
    expect(out.data.coefficients.repr).not.toBe("wide");
    for (let i = 0; i < n; i++) {
      const scale = Math.max(sa(i), sb(i));
      const expected =
        (2n ** 40n + BigInt(i)) * 10n ** BigInt(scale - sa(i)) -
        BigInt(i) * 10n ** BigInt(scale - sb(i));
      expect(valueAt(out, i)).toEqual([expected, scale]);
    }
  });

  it("3. DATE → TIMESTAMP(TZ) is 22008 past PostgreSQL's finite range, as PostgreSQL", async () => {
    const pg = new PGlite();
    try {
      const bounds = await pg.query<{ last: number; first: number }>(
        "select date '294276-12-31' - date '2000-01-01' as last, date '4714-11-24 BC' - date '2000-01-01' as first",
      );
      const row = bounds.rows[0];
      if (row === undefined) throw new Error("bounds expected");
      for (const target of ["timestamp", "timestamptz"]) {
        await expect(pg.query(`select date '294277-01-01'::${target}`)).rejects.toMatchObject({
          code: "22008",
        });
      }
      const date = { kind: "date" } as const;
      for (const zoned of [false, true]) {
        const type = { kind: "timestamp", zoned } as const;
        const cast = compileFor({ op: "cast", input: col(0, date), type }, [date]);
        const inRange = cast.evaluate(batchOf([numbers(date, [row.last, row.first])]), null);
        expect(valueAt(inRange, 0)).toBe(BigInt(row.last) * 86_400_000_000n);
        expect(valueAt(inRange, 1)).toBe(BigInt(row.first) * 86_400_000_000n);
        for (const days of [row.last + 1, row.first - 1, 2_000_000_000])
          expect(() => cast.evaluate(batchOf([numbers(date, [days])]), null)).toThrow(
            DatetimeOverflowError,
          );
      }
    } finally {
      await pg.close();
    }
  }, 60_000);
});
