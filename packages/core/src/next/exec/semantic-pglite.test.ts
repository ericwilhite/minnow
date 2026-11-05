/**
 * JSONB order, equality and parsing against PostgreSQL itself (PGlite, C collation): the law is
 * PostgreSQL's behaviour, so the generated cases are judged by it, not by this module.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { compareDecimals, compareJsonb, jsonbEqualityKey, parseJsonb } from "./semantic.js";

const SEEDS = seedsFor("next-jsonb-pglite", [0x4a5b]);
let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

async function postgresCompare(a: string, b: string): Promise<number> {
  const result = await pg.query<{ c: number }>(
    "select case when $1::jsonb < $2::jsonb then -1 when $1::jsonb = $2::jsonb then 0 else 1 end as c",
    [a, b],
  );
  return result.rows[0]?.c ?? NaN;
}

async function postgresAccepts(text: string): Promise<boolean> {
  try {
    await pg.query("select $1::jsonb", [text]);
    return true;
  } catch {
    return false;
  }
}

function accepts(text: string): boolean {
  try {
    parseJsonb(text);
    return true;
  } catch {
    return false;
  }
}

const space = fc.constantFrom("", "", " ", "\t", "\n", "\r", "  ");
const keyText = fc.constantFrom("a", "b", "aa", "ab", "B", "z", "é", "😀", "", "bb", "aaa", "a b");
const stringText = fc.oneof(
  keyText.map((s) => JSON.stringify(s)),
  // The same characters as \u escapes, surrogate pairs included.
  keyText.map(
    (s) =>
      `"${s
        .split("")
        .map((unit) => `\\u${unit.charCodeAt(0).toString(16).padStart(4, "0")}`)
        .join("")}"`,
  ),
  keyText.map(
    (s) =>
      `"${Array.from(s)
        .map((c) => ((c.codePointAt(0) ?? 0) > 0xffff ? `\\ud83d\\ude00` : c))
        .join("")}"`,
  ),
);
const numberText = fc.oneof(
  fc.integer({ min: -20, max: 20 }).map(String),
  fc.constantFrom(
    "1.0",
    "1.50",
    "0.1",
    "-0",
    "1e2",
    "1E-2",
    "10",
    "2",
    "100.000",
    "0.10e1",
    "-1.5e+3",
  ),
  fc.double({ noNaN: true, noDefaultInfinity: true }).map(String),
);
const json: fc.Arbitrary<string> = fc.letrec((tie) => ({
  node: fc.oneof(
    { depthSize: "small" },
    fc.constantFrom("null", "true", "false"),
    numberText,
    stringText,
    fc
      .tuple(fc.array(fc.tuple(space, tie("node"), space), { maxLength: 3 }), space)
      .map(
        ([items, inner]) =>
          `[${inner}${(items as Array<[string, string, string]>).map(([a, v, b]) => a + v + b).join(",")}]`,
      ),
    fc
      .array(fc.tuple(space, keyText, space, tie("node")), { maxLength: 3 })
      .map(
        (pairs) =>
          `{${(pairs as Array<[string, string, string, string]>).map(([a, k, b, v]) => `${a}${JSON.stringify(k)}${b}:${v}`).join(",")}}`,
      ),
  ),
})).node;
const padded = fc.tuple(space, json, space).map(([a, v, b]) => a + v + b);

describe("JSONB against PostgreSQL (review A1-3 finding 5)", () => {
  it("matches PostgreSQL on the review's probes", async () => {
    for (const [a, b] of [
      ['{"b":0}', '{"aa":0}'],
      ["[[]]", "[null]"],
      ["[]", "{}"],
      ["[]", '"x"'],
      ["1", "[1]"],
      ['{"aa":1,"b":2}', '{"ab":0,"b":2}'],
    ] as const)
      expect(compareJsonb(parseJsonb(a), parseJsonb(b)), `${a} vs ${b}`).toBe(
        await postgresCompare(a, b),
      );
    for (const text of [
      "01",
      " 1",
      '"\\ud800"',
      '"\\udc00x"',
      '"\\u0000"',
      " 1",
      "\t1\n",
      "1.",
      ".5",
      "+1",
      "[1,]",
      "NaN",
    ])
      expect(accepts(text), JSON.stringify(text)).toBe(await postgresAccepts(text));
    // A raw lone surrogate cannot even reach PostgreSQL as UTF-8; it is refused here.
    expect(accepts('"\ud800"')).toBe(false);
  });

  it("accepts exactly PostgreSQL's NUMERIC range (review A1-4 follow-up)", async () => {
    const nines = (count: number) => "9".repeat(count);
    for (const text of [
      "1e131072",
      "1e131071",
      "1e-16384",
      "1e-16383",
      "1e9999999999999999999999999999",
      '{"n":[1e-16384]}',
      "0e1001",
      "0e-1001",
      "0e-16383",
      "0e-16384",
      "0e2147483647",
      "0e1073741822",
      "0e1073741823",
      "1.0e-16382",
      "1.00e-16382",
      "0.000e5",
      "-0.0",
      nines(131072),
      nines(131073),
      `${nines(131000)}e72`,
      `${nines(131000)}e73`,
      `0.${"1".repeat(16383)}`,
      `0.${"1".repeat(16384)}`,
      `1.${"0".repeat(16384)}`,
      `0.${"1".repeat(16000)}e-383`,
      `0.${"1".repeat(16000)}e-384`,
    ])
      expect(
        accepts(text),
        text.length > 40 ? `${text.slice(0, 20)}…(${String(text.length)})` : text,
      ).toBe(await postgresAccepts(text));
  }, 60_000);

  const numberEdges = fc
    .tuple(
      fc.constantFrom("0", "1", "9.99", "0.5", "-12.3400", "1.0"),
      fc.oneof(
        fc.integer({ min: -16_400, max: -16_370 }),
        fc.integer({ min: 131_060, max: 131_080 }),
        fc.integer({ min: -2000, max: 2000 }),
        fc.constantFrom(1_073_741_822, 1_073_741_823, -1_073_741_823, 2_147_483_648),
      ),
    )
    .map(([mantissa, exponent]) => `${mantissa}e${String(exponent)}`);

  it.each(SEEDS)(
    "agrees with PostgreSQL on numbers near NUMERIC's limits (seed %i)",
    async (seed) => {
      await fc.assert(
        fc.asyncProperty(numberEdges, async (text) => {
          expect(accepts(text), text).toBe(await postgresAccepts(text));
        }),
        { seed, numRuns: 300 },
      );
    },
    60_000,
  );

  it.each(SEEDS)(
    "orders and equates generated values exactly as PostgreSQL does (seed %i)",
    async (seed) => {
      await fc.assert(
        fc.asyncProperty(padded, padded, async (a, b) => {
          expect(compareJsonb(parseJsonb(a), parseJsonb(b))).toBe(await postgresCompare(a, b));
        }),
        { seed, numRuns: 400 },
      );
    },
    60_000,
  );

  it("compares a zero with a huge exponent at once, in bounded memory (review A1-5 finding 1)", async () => {
    const started = performance.now();
    for (const [a, b] of [
      ["0e1073741823", "0"],
      ["-0e1073741823", "0.00"],
      ["[0e1073741823]", "[0e-16383]"],
      ['{"n":0e1073741823}', '{"n":-0.0e5}'],
      ["0e1073741823", "1e-16383"],
      ["-1e-16383", "0e1073741823"],
    ] as const) {
      const x = parseJsonb(a);
      const y = parseJsonb(b);
      const expected = await postgresCompare(a, b);
      expect(compareJsonb(x, y), `${a} vs ${b}`).toBe(expected);
      expect(Buffer.from(jsonbEqualityKey(x)).equals(Buffer.from(jsonbEqualityKey(y)))).toBe(
        expected === 0,
      );
    }
    // Materializing 10^1073741823 takes seconds and gigabytes; the PostgreSQL round trips above
    // dominate this budget.
    expect(performance.now() - started).toBeLessThan(2_000);
  });

  it("orders decimals by sign, point position and digits (small values, against BigInt)", () => {
    const decimal = fc.record({
      coefficient: fc.bigInt({ min: -(10n ** 12n), max: 10n ** 12n }),
      scale: fc.integer({ min: -6, max: 6 }),
    });
    const exact = (d: { coefficient: bigint; scale: number }, scale: number) =>
      d.coefficient * 10n ** BigInt(scale - d.scale);
    // Independent pairs, the same value spelled with more trailing zeros, and neighbours.
    const pair = fc.oneof(
      fc.tuple(decimal, decimal),
      fc
        .tuple(decimal, fc.integer({ min: 0, max: 4 }))
        .map(([a, k]) => [
          a,
          { coefficient: a.coefficient * 10n ** BigInt(k), scale: a.scale + k },
        ]),
      fc
        .tuple(decimal, fc.integer({ min: 0, max: 4 }), fc.constantFrom(-1n, 1n))
        .map(([a, k, step]) => [
          a,
          { coefficient: a.coefficient * 10n ** BigInt(k) + step, scale: a.scale + k },
        ]),
    );
    for (const seed of SEEDS)
      fc.assert(
        fc.property(pair, ([a, b]) => {
          const x = exact(a, 12);
          const y = exact(b, 12);
          expect(compareDecimals(a, b)).toBe(x === y ? 0 : x < y ? -1 : 1);
        }),
        { seed, numRuns: 2000 },
      );
  });

  // Numbers at NUMERIC's limits on both sides of a comparison: zeros of every sign and exponent,
  // the smallest and largest magnitudes, inside containers. Each side is first checked to be
  // accepted exactly when PostgreSQL accepts it; refused pairs are counted and must stay a minority.
  const limitNumber = fc
    .tuple(
      fc.constantFrom(
        ...["0", "-0", "0.000", "-0.0", "1", "-1", "9.99", "-12.3400", "1.0", "0.5"],
        ...["10", "1.00", "0.10", "-1.0", "9.990", "100"],
      ),
      fc.oneof(
        fc.constantFrom(0, 1_073_741_823, -16_383, -16_382, 131_071, 131_070, 131_069, 1_000_000),
        fc.integer({ min: -16_384, max: -16_370 }),
        fc.integer({ min: 131_060, max: 131_073 }),
        fc.integer({ min: -3, max: 3 }),
        fc.integer({ min: -3, max: 3 }),
      ),
    )
    .map(([mantissa, exponent]) => `${mantissa}e${String(exponent)}`);
  // Zeros with exponents far from every other value's scale: aligning scales would build
  // 10^1073741823.
  const zeroNumber = fc
    .tuple(
      fc.constantFrom("0", "-0", "0.000", "-0.0"),
      fc.constantFrom(1_073_741_823, 1_000_000, 131_073, 5, 0, -16_383, -16_380),
    )
    .map(([mantissa, exponent]) => `${mantissa}e${String(exponent)}`);
  const edgeNumber = fc.oneof(limitNumber, zeroNumber);
  // Both sides in the same position, so the numbers (not the structure) decide.
  const limitPair = fc
    .tuple(
      fc.constantFrom(
        (n: string) => n,
        (n: string) => `[${n}]`,
        (n: string) => `{"a":[true,${n}]}`,
      ),
      edgeNumber,
      edgeNumber,
    )
    .map(([wrap, a, b]) => [wrap(a), wrap(b)] as const);
  const hugeZero = /(^|[^\d.])-?0(\.0+)?e(1073741823|1000000)/u;

  it.each(SEEDS)(
    "orders and equates numbers at NUMERIC's limits as PostgreSQL does, keys included (seed %i)",
    async (seed) => {
      const reach = { compared: 0, refused: 0, hugeZeroVsOtherScale: 0, equalSpellings: 0 };
      await fc.assert(
        fc.asyncProperty(limitPair, async ([a, b]) => {
          const [okA, okB] = [accepts(a), accepts(b)];
          expect(okA, a.slice(0, 40)).toBe(await postgresAccepts(a));
          expect(okB, b.slice(0, 40)).toBe(await postgresAccepts(b));
          if (!okA || !okB) {
            reach.refused += 1;
            return;
          }
          reach.compared += 1;
          if (hugeZero.test(a) !== hugeZero.test(b)) reach.hugeZeroVsOtherScale += 1;
          const expected = await postgresCompare(a, b);
          if (expected === 0 && a !== b) reach.equalSpellings += 1;
          const [x, y] = [parseJsonb(a), parseJsonb(b)];
          expect(compareJsonb(x, y), `${a} vs ${b}`).toBe(expected);
          expect(
            Buffer.from(jsonbEqualityKey(x)).equals(Buffer.from(jsonbEqualityKey(y))),
            `${a} key ${b}`,
          ).toBe(expected === 0);
        }),
        { seed, numRuns: 300 },
      );
      // Declared reach: most pairs are valid and compared; a huge-exponent zero meets another
      // scale in the same position; equal values with different spellings occur.
      expect(reach.compared).toBeGreaterThan(reach.refused);
      expect(reach.hugeZeroVsOtherScale).toBeGreaterThanOrEqual(20);
      expect(reach.equalSpellings).toBeGreaterThanOrEqual(10);
    },
    120_000,
  );

  const nearJson = fc.oneof(
    padded,
    fc
      .tuple(
        padded,
        fc.constantFrom("0", " ", " ", "﻿", "\v", "\f", ",", ".", "+", "e", "\\u0000", "]"),
      )
      .map(([text, junk]) => {
        // Insert between code points: a split surrogate pair would reach PostgreSQL as U+FFFD.
        const points = Array.from(text);
        const at = Math.floor(points.length / 2);
        return [...points.slice(0, at), junk, ...points.slice(at)].join("");
      }),
    fc.constantFrom(
      '"\\ud800"',
      '"\\udbff\\u0041"',
      '"a\tb"',
      "tru",
      "-",
      "1e",
      "00",
      "-01",
      "[01]",
      '{"a" 1}',
    ),
  );

  it.each(SEEDS)(
    "accepts exactly what PostgreSQL accepts (seed %i)",
    async (seed) => {
      await fc.assert(
        fc.asyncProperty(nearJson, async (text) => {
          expect(accepts(text), JSON.stringify(text)).toBe(await postgresAccepts(text));
        }),
        { seed, numRuns: 400 },
      );
    },
    60_000,
  );
});
