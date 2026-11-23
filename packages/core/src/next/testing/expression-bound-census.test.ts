/**
 * The input-size bound of a cast to text never under-states its value
 * (exec/input-size-plan.ts `castBytes`).
 *
 * Law (the contract of `columnInputBytes`/`expressionInputBytes`: "per-value admission
 * provenance"): a bound is an upper bound on the bytes of every value the expression produces.
 * A bound below a real value lets a statement reserve less than its rows need (a refusal part
 * way, the class #53 and #56 fixed). Before, a cast to text had no bound at all: its column fell
 * back to the source and parameter envelope.
 * - Fixed-width types: every value's text, through the formatters the cast kernel uses, at the
 *   extremes of each type's stored domain (wider than its SQL literals), is within the bound.
 * - NUMERIC with a declared precision and scale: generated through SQL, at full precision.
 * - The planned bound of each cast that runs from a table covers what it returns.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { columnInputBytes, FIXED_TEXT_BYTES } from "../exec/input-size-plan.js";
import { jsonFloatText } from "../exec/kernels/json-float.js";
import { sqlScalarText } from "../exec/sql-array-text.js";
import { formatDate } from "../temporal.js";
import { formatSqlInterval, formatSqlTime, formatSqlTimestamp } from "../temporal-text.js";
import { createReach, EXPRESSION_BOUND_CENSUS_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const utf8 = new TextEncoder();
const bytes = (value: unknown) =>
  value === null || value === undefined
    ? 0
    : utf8.encode(typeof value === "string" ? value : JSON.stringify(value)).length;

/** The planned bound of the first output column of `sql`, and the values it returns. */
async function census(core: StorageCore, sql: string) {
  const lowered = core.lowerer.lower(sql, core.context());
  if (lowered.kind !== "query") throw new Error(`${sql}: a query`);
  const bound = columnInputBytes(lowered.plan.root, 0, { parameter: () => 0, source: () => null });
  const result = await core.execute(sql);
  if (!("batches" in result)) throw new Error(`${sql}: rows`);
  return { bound, values: oracleRows(result).map((row) => Object.values(row)[0]) };
}

function within(sql: string, bound: number | null, values: readonly unknown[]) {
  expect(bound, `${sql}: bounded`).not.toBeNull();
  for (const value of values)
    expect(bytes(value), `${sql}: ${JSON.stringify(value)}`).toBeLessThanOrEqual(bound ?? -1);
}

/** Formats `format(value)`, or nothing when the value is outside what the kernel formats. */
function text(format: () => string): string | null {
  try {
    return format();
  } catch {
    return null;
  }
}

it("bounds the text of every fixed-width type at the extremes of its stored domain", () => {
  const int32 = [-2147483648, -2147483647, -1, 0, 1, 2147483646, 2147483647];
  const int64 = [-(2n ** 63n), -(2n ** 63n) + 1n, -1n, 0n, 1n, 2n ** 63n - 2n, 2n ** 63n - 1n];
  const texts: string[] = [];
  const add = (value: string | null) => {
    if (value !== null) texts.push(value);
  };
  for (const days of int32) add(text(() => formatDate(days)));
  for (const micros of int64)
    for (const zoned of [false, true]) add(text(() => formatSqlTimestamp(micros, zoned)));
  for (const micros of [0n, 1n, 86_399_999_999n, 86_400_000_000n])
    add(text(() => formatSqlTime(micros)));
  for (const months of int32)
    for (const days of int32)
      for (const micros of int64) add(text(() => formatSqlInterval(months, days, micros)));
  for (const value of [
    -Number.MAX_VALUE,
    -Number.MIN_VALUE,
    Number.NaN,
    -Infinity,
    -1.2345678e-300,
  ])
    add(text(() => jsonFloatText(value, false)));
  // REAL holds float32 values only.
  for (const value of [-3.4028234663852886e38, -1.401298464324817e-45, -1.1754942e-38, -1.2345678])
    add(text(() => jsonFloatText(Math.fround(value), true)));
  for (const value of [...int64, -32768, 2147483647]) add(String(value));
  add(sqlScalarText(true, { kind: "bool" }));
  add(sqlScalarText(false, { kind: "bool" }));
  add(sqlScalarText(new Uint8Array(16).fill(255), { kind: "uuid" }));
  expect(texts.length, "the formatters ran").toBeGreaterThan(100);
  const longest = texts.reduce((most, value) => (bytes(value) > bytes(most) ? value : most), "");
  expect(bytes(longest), `the longest: ${longest}`).toBeLessThanOrEqual(FIXED_TEXT_BYTES);
});

for (const seed of seedsFor("next-expression-bound-census", [0x6001, 0x6002]))
  it(`bounds a NUMERIC's text from its declared precision and scale (seed ${String(seed)})`, async () => {
    const reach = createReach("expression bound census", EXPRESSION_BOUND_CENSUS_REACH);
    const core = new StorageCore();
    await core.open();
    let table = 0;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 38 }).chain((precision) =>
            fc.tuple(
              fc.constant(precision),
              fc.integer({ min: 0, max: precision }),
              fc.array(fc.integer({ min: 0, max: 9 }), {
                minLength: precision,
                maxLength: precision,
              }),
              fc.boolean(),
            ),
          ),
          async ([precision, scale, digits, negative]) => {
            reach.beginHistory();
            const whole = digits.slice(0, precision - scale).join("") || "0";
            const fraction = digits.slice(precision - scale).join("");
            const value = `${negative ? "-" : ""}${whole}${fraction === "" ? "" : `.${fraction}`}`;
            const name = `n${String(++table)}`;
            await core.execute(
              `CREATE TABLE ${name}(id INT PRIMARY KEY, v NUMERIC(${String(precision)}, ${String(scale)}))`,
            );
            await core.execute(`INSERT INTO ${name} VALUES (1, ${value})`);
            const sql = `SELECT CAST(v AS TEXT) FROM ${name}`;
            const { bound, values } = await census(core, sql);
            within(sql, bound, values);
            reach.reach(scale === precision ? "numeric below one" : "numeric with a whole part");
            if (negative) reach.reach("negative numeric");
          },
        ),
        {
          seed,
          numRuns: 60,
          examples: [
            [[5, 5, [1, 2, 3, 4, 5], true]],
            [[38, 10, Array.from({ length: 38 }, () => 9), true]],
            [[1, 0, [0], false]],
          ],
        },
      );
    } finally {
      await core.close();
    }
    reach.assertReached();
  }, 300_000);

it("bounds what each cast to text from a table column returns", async () => {
  const core = new StorageCore();
  await core.open();
  try {
    await core.execute("CREATE TYPE mood AS ENUM ('ok', 'un très très long libellé 😀')");
    await core.execute(
      "CREATE TABLE v(id INT PRIMARY KEY, i BIGINT, s SMALLINT, f DOUBLE PRECISION, r REAL, b BOOLEAN, d DATE, ts TIMESTAMP, iv INTERVAL, n NUMERIC(38, 10), m mood)",
    );
    await core.execute(
      "INSERT INTO v VALUES (1, -9223372036854775808, -32768, -1.7976931348623157e308, -3.4028235e38, false, DATE '0001-01-01', TIMESTAMP '0001-01-01 00:00:00 BC', INTERVAL '-1 microseconds', -9999999999999999999999999999.9999999999, 'un très très long libellé 😀')",
    );
    for (const column of ["i", "s", "f", "r", "b", "d", "ts", "iv", "n", "m"]) {
      const sql = `SELECT CAST(${column} AS TEXT) FROM v`;
      const { bound, values } = await census(core, sql);
      within(sql, bound, values);
    }
  } finally {
    await core.close();
  }
}, 60_000);
