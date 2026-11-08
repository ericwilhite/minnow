/** DATE_TRUNC is PostgreSQL's exact calendar truncation, in the core's fixed UTC session. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, DATE_TRUNC_REACH } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import {
  TIMESTAMP_MIN,
  TIMESTAMP_END,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
  formatTimestamp,
} from "../../temporal.js";
import type { PhysicalType } from "../../types.js";
import type { Vector } from "../../vector/vector.js";
import { compileExpr } from "./compile.js";
import { OutputArena } from "./vectors.js";
import {
  TEXT,
  batchOf,
  bigints,
  call,
  col,
  constant,
  dictionary,
  numbers,
  texts,
  valueAt,
} from "./test-support.js";
const UNITS = [
  "microseconds",
  "milliseconds",
  "second",
  "minute",
  "hour",
  "day",
  "week",
  "month",
  "quarter",
  "year",
  "decade",
  "century",
  "millennium",
] as const;
const ALIASES = [
  "us",
  "usec",
  "useconds",
  "usecs",
  "microsecon",
  "microsecondsXYZ",
  "ms",
  "msec",
  "msecond",
  "mseconds",
  "msecs",
  "millisecon",
  "s",
  "sec",
  "seconds",
  "secs",
  "m",
  "min",
  "mins",
  "minutes",
  "h",
  "hours",
  "hr",
  "hrs",
  "d",
  "days",
  "w",
  "weeks",
  "mon",
  "mons",
  "months",
  "qtr",
  "y",
  "years",
  "yr",
  "yrs",
  "dec",
  "decades",
  "decs",
  "c",
  "cent",
  "centuries",
  "mil",
  "millennia",
  "mils",
];
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("SET TIME ZONE 'UTC'");
}, 60_000);
afterAll(async () => {
  await pg.close();
});
function wrap(vector: ReturnType<typeof bigints>, form: number): Vector {
  if (form === 2)
    return constant(bigints(vector.type, [valueAt(vector, 0) as bigint]), vector.length);
  if (form === 1)
    return dictionary(
      vector,
      Array.from({ length: vector.length }, (_, i) => (valueAt(vector, i) === null ? null : i)),
    );
  return vector;
}
async function check(
  micros: bigint,
  unit: string,
  zoned: boolean,
  form: number,
  selected: boolean,
  reach: ReturnType<typeof createReach<string>>,
): Promise<void> {
  reach.beginHistory();
  const safe = form >= 3;
  form %= 3;
  if (safe) micros %= 9007199254740991n;
  const type: PhysicalType = { kind: "timestamp", zoned };
  const values: Array<bigint | null> = [
    micros,
    -1n,
    null,
    safe ? 0n : TIMESTAMP_POS_INFINITY,
    safe ? 1n : TIMESTAMP_NEG_INFINITY,
    123n,
  ];
  if (selected) values.push(123n);
  const vector = safe
    ? numbers(
        type,
        values.map((v) => (v === null ? null : Number(v))),
      )
    : bigints(type, values);
  const input = wrap(vector, form);
  const fieldValues = [unit, unit, "garbage", unit, unit, null];
  if (selected) fieldValues.push("garbage");
  const fields =
    form === 2
      ? constant(texts([unit]), values.length)
      : form === 1
        ? dictionary(
            texts([unit, "garbage"]),
            fieldValues.map((v) => (v === null ? null : v === unit ? 0 : 1)),
          )
        : texts(fieldValues);
  const active = selected ? [0, 2, 4, 5] : [0, 1, 2, 3, 4, 5];
  const parameters = active.flatMap((row) => [
    valueAt(fields, row),
    valueAt(input, row) === null ? null : formatTimestamp(valueAt(input, row) as bigint, zoned),
  ]);
  // Ask PG for its integer epoch offset, avoiding JS Date's millisecond precision and range.
  const sql = `SELECT CASE WHEN isfinite(v) THEN (extract(epoch FROM (v - ${zoned ? "timestamptz" : "timestamp"} '2000-01-01 00:00:00+00'))*1000000)::numeric(30,0)::text ELSE v::text END AS v FROM (SELECT date_trunc(u,t) AS v FROM (VALUES ${active.map((_, i) => `($${String(2 * i + 1)}::text,$${String(2 * i + 2)}::${zoned ? "timestamptz" : "timestamp"})`).join(",")}) d(u,t)) q`;
  // formatTimestamp uses astronomical ISO years; PG input uses BC notation.
  for (let i = 1; i < parameters.length; i += 2) {
    const text = parameters[i];
    if (typeof text === "string" && /^(-\d{6}|0000)-/u.test(text)) {
      const year = Number(text.slice(0, text.startsWith("-") ? 7 : 4));
      parameters[i] =
        `${String(1 - year).padStart(4, "0")}${text.slice(text.startsWith("-") ? 7 : 4).replace(/Z$/u, "")} BC`;
    } else if (typeof text === "string" && text.startsWith("+")) parameters[i] = text.slice(1);
  }
  let expected: Array<bigint | null> | undefined, state: string | undefined;
  try {
    expected = (await pg.query<{ v: string | null }>(sql, parameters)).rows.map(({ v }) =>
      v === null
        ? null
        : v === "infinity"
          ? TIMESTAMP_POS_INFINITY
          : v === "-infinity"
            ? TIMESTAMP_NEG_INFINITY
            : BigInt(v),
    );
  } catch (error) {
    state = (error as { code: string }).code;
    expect(["22008", "22023", "0A000"]).toContain(state);
  }
  let charged = 0;
  const arena = new OutputArena(
    {
      allocate(bytes) {
        charged += bytes;
        return true;
      },
      free(bytes) {
        charged -= bytes;
      },
    },
    values.length,
  );
  try {
    const compiled = compileExpr(call("date_trunc", type, col(0, TEXT), col(1, type)), {
      arena,
      input: [TEXT, type],
      params: { slots: [] },
    });
    const batch = batchOf(
      [fields, input],
      selected ? { rows: Uint16Array.from(active), count: active.length } : null,
    );
    const run = () => compiled.evaluate(batch, batch.selection);
    if (state !== undefined) {
      expect(run, `${unit}/${micros.toString()}`).toThrow(
        expect.objectContaining({ sqlState: state }),
      );
      reach.reach(state);
    } else {
      const result = run();
      expect(
        active.map((row) => valueAt(result, row)),
        `${unit}/${micros.toString()}`,
      ).toEqual(expected);
      expect(active.map((row) => valueAt(run(), row))).toEqual(expected);
      reach.reach("rows");
    }
    reach.reach(zoned ? "timestamptz" : "timestamp");
    reach.reach(["flat", "dictionary", "constant"][form] ?? "missing");
    reach.reach(selected ? "selected" : "all rows");
    if (form !== 2) {
      reach.reach("null garbage");
      reach.reach("NULL unit");
      if (!safe) reach.reach("infinity");
      if (selected) reach.reach("unselected garbage");
    }
    reach.reach(safe ? "safe tier" : "int64 tier");
    if (micros < 0n) reach.reach("before epoch");
    if (micros > 9007199254740991n || micros < -9007199254740991n) reach.reach("int64 precision");
    if (UNITS.includes(unit as (typeof UNITS)[number])) reach.reach(unit);
    else reach.reach("aliases or invalid units");
  } finally {
    arena.close();
    expect(charged).toBe(0);
  }
}
it.each(seedsFor("next-date-trunc", [0xb613, 0xb614]))(
  "matches PG units, calendar boundaries and vector forms (seed %i)",
  async (seed) => {
    const reach = createReach<string>("DATE_TRUNC", DATE_TRUNC_REACH);
    const examples: Array<[bigint, string, boolean, number, boolean]> = [];
    for (const unit of [
      ...UNITS,
      ...ALIASES,
      "MONTH",
      "timezone",
      "timezone_hour",
      "epoch",
      "garbage",
      " month ",
    ])
      for (const zoned of [false, true]) examples.push([755920123456789n, unit, zoned, 0, false]);
    for (const micros of [
      TIMESTAMP_MIN,
      TIMESTAMP_END - 1n,
      -63082281600000001n,
      -63113904000000001n,
      -63082368000000000n,
      -1n,
      0n,
      9007199254740993n,
    ])
      for (const unit of UNITS) examples.push([micros, unit, false, 1, true]);
    for (let form = 0; form < 6; form++)
      examples.push([755920123456789n, "month", true, form, false]);
    await fc.assert(
      fc.asyncProperty(
        fc.bigInt({ min: TIMESTAMP_MIN, max: TIMESTAMP_END - 1n }),
        fc.constantFrom(...UNITS),
        fc.boolean(),
        fc.integer({ min: 0, max: 5 }),
        fc.boolean(),
        (micros, unit, zoned, form, selected) => check(micros, unit, zoned, form, selected, reach),
      ),
      { seed, numRuns: examples.length + 120, examples },
    );
    reach.assertReached();
  },
  120_000,
);
