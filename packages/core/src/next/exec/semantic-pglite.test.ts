/**
 * JSONB order, equality and parsing against PostgreSQL itself (PGlite, C collation): the law is
 * PostgreSQL's behaviour, so the generated cases are judged by it, not by this module.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { compareJsonb, parseJsonb } from "./semantic.js";

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
