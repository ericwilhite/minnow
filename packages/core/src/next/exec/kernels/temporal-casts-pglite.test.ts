/** SQL temporal output text, checked against independently constructed PostgreSQL values. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { createReach, TEMPORAL_TEXT_REACH } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import {
  DATE_MIN,
  DATE_MAX,
  DATE_POS_INFINITY,
  DATE_NEG_INFINITY,
  TIMESTAMP_MIN,
  TIMESTAMP_END,
  TIMESTAMP_POS_INFINITY,
  TIMESTAMP_NEG_INFINITY,
} from "../../temporal.js";
import { compileExpr } from "./compile.js";
import { OutputArena } from "./vectors.js";
import { TEXT, batchOf, col, valueAt } from "./test-support.js";
import {
  TEMPORAL_TYPES,
  temporalVector,
  temporalValue,
  pgTemporal,
  type TemporalValue,
} from "./temporal-test-support.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("SET TIME ZONE 'UTC';SET DateStyle='ISO';SET IntervalStyle='postgres'");
}, 60_000);
afterAll(async () => {
  await pg.close();
});
interface Shape {
  kind: number;
  date: number;
  timestamp: bigint;
  months: number;
  days: number;
  micros: bigint;
  form: number;
  selected: boolean;
  safe: boolean;
  nullFirst: boolean;
}
async function check(shape: Shape, reach: ReturnType<typeof createReach<string>>): Promise<void> {
  reach.beginHistory();
  const type = TEMPORAL_TYPES[shape.kind];
  if (type === undefined) throw new Error("Missing type");
  const safe = shape.safe && type.kind === "timestamp";
  const timestamp = safe ? shape.timestamp % 9007199254740991n : shape.timestamp;
  const first: TemporalValue = shape.nullFirst
    ? null
    : type.kind === "date"
      ? shape.date
      : type.kind === "interval"
        ? { months: shape.months, days: shape.days, micros: shape.micros }
        : timestamp;
  const values: TemporalValue[] = [
    first,
    null,
    type.kind === "interval"
      ? { months: -13, days: 1, micros: -1n }
      : type.kind === "date"
        ? 0
        : -1n,
  ];
  if (type.kind === "date") values.push(DATE_POS_INFINITY, DATE_NEG_INFINITY);
  else if (type.kind === "timestamp" && !safe)
    values.push(TIMESTAMP_POS_INFINITY, TIMESTAMP_NEG_INFINITY);
  if (shape.selected) {
    const poison =
      type.kind === "interval"
        ? { months: 2147483647, days: 2147483647, micros: 9223372036854775807n }
        : type.kind === "date"
          ? DATE_MAX + 1
          : TIMESTAMP_END;
    values[2] = poison;
    values.push(poison);
  }
  const input = temporalVector(type, values, safe, shape.form);
  const active = Array.from(
    { length: values.length - (shape.selected ? 1 : 0) },
    (_, i) => i,
  ).filter((i) => !shape.selected || i !== 2);
  if (shape.selected) active.reverse();
  const sql = `SELECT v::text AS s FROM (VALUES ${active.map((i) => `(${pgTemporal(type, temporalValue(input, i))})`).join(",")}) d(v)`;
  const expected = (await pg.query<{ s: string | null }>(sql)).rows.map((r) => r.s);
  let charged = 0;
  const arena = new OutputArena(
    {
      allocate(n) {
        charged += n;
        return true;
      },
      free(n) {
        charged -= n;
      },
    },
    values.length,
  );
  try {
    const compiled = compileExpr(
      { op: "cast", input: col(0, type), type: TEXT },
      { arena, input: [type], params: { slots: [] } },
    );
    const batch = batchOf(
      [input],
      shape.selected
        ? { rows: Uint16Array.from([...active, 2, values.length - 1]), count: active.length }
        : null,
    );
    const out = compiled.evaluate(batch, batch.selection);
    expect(
      active.map((i) => valueAt(out, i)),
      sql,
    ).toEqual(expected);
    expect(active.map((i) => valueAt(compiled.evaluate(batch, batch.selection), i))).toEqual(
      expected,
    );
    reach.reach(type.kind === "timestamp" ? (type.zoned ? "timestamptz" : "timestamp") : type.kind);
    reach.reach(["flat", "dictionary", "constant"][shape.form] ?? "missing");
    if (shape.form === 2 && shape.nullFirst) reach.reach("constant NULL");
    reach.reach(shape.selected ? "selected" : "all rows");
    if (shape.selected) reach.reach("sparse nonascending selection");
    if (shape.form !== 2) reach.reach("NULL garbage");
    if (shape.selected && shape.form !== 2) reach.reach("unselected garbage");
    if (type.kind === "timestamp") reach.reach(safe ? "safe timestamp" : "wide timestamp");
    if (type.kind === "timestamp" && !safe && shape.form !== 2) reach.reach("infinities");
    if (type.kind === "date" && first !== null && shape.date < -730119) reach.reach("BC");
    if (type.kind === "interval" && shape.form !== 2 && shape.months < 0 && shape.days > 0)
      reach.reach("mixed signs");
  } finally {
    arena.close();
    expect(charged).toBe(0);
  }
}
it.each(seedsFor("next-temporal-output-text", [0xb623, 0xb624]))(
  "matches PG text for all vector forms and calendar ranges (seed %i)",
  async (seed) => {
    const reach = createReach<string>("SQL temporal text", TEMPORAL_TEXT_REACH);
    const base: Shape = {
      kind: 0,
      date: DATE_MIN,
      timestamp: -1n,
      months: -13,
      days: 1,
      micros: -1n,
      form: 0,
      selected: false,
      safe: false,
      nullFirst: false,
    };
    const examples: Array<[Shape]> = [];
    for (let kind = 0; kind < 4; kind++)
      for (let form = 0; form < 3; form++)
        for (const selected of [false, true]) examples.push([{ ...base, kind, form, selected }]);
    for (const timestamp of [
      TIMESTAMP_MIN,
      TIMESTAMP_END - 1n,
      0n,
      1n,
      1000n,
      123000n,
      9007199254740993n,
    ])
      for (const kind of [1, 2]) examples.push([{ ...base, kind, timestamp }]);
    for (const date of [DATE_MIN, DATE_MAX, -730119, 0, 8825]) examples.push([{ ...base, date }]);
    examples.push([{ ...base, kind: 1, safe: true }]);
    for (let kind = 0; kind < 4; kind++)
      examples.push([{ ...base, kind, form: 2, nullFirst: true }]);
    for (const micros of [-9223372036854775808n, 9223372036854775807n, 0n, 86400000001n])
      examples.push([{ ...base, kind: 3, micros }]);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          kind: fc.integer({ min: 0, max: 3 }),
          date: fc.integer({ min: DATE_MIN, max: DATE_MAX }),
          timestamp: fc.bigInt({ min: TIMESTAMP_MIN, max: TIMESTAMP_END - 1n }),
          months: fc.integer(),
          days: fc.integer(),
          micros: fc.bigInt({ min: -9223372036854775808n, max: 9223372036854775807n }),
          form: fc.integer({ min: 0, max: 2 }),
          selected: fc.boolean(),
          safe: fc.boolean(),
          nullFirst: fc.boolean(),
        }),
        (shape) => check(shape, reach),
      ),
      { seed, numRuns: examples.length + 80, examples },
    );
    reach.assertReached();
  },
  120_000,
);

it("keeps bounded temporal text casts and interval infinities explicitly refused", async () => {
  for (const type of TEMPORAL_TYPES) {
    const value: TemporalValue =
      type.kind === "date" ? 0 : type.kind === "interval" ? { months: 0, days: 0, micros: 0n } : 0n;
    expect(
      (await pg.query(`SELECT CAST(${pgTemporal(type, value)} AS varchar(3)) AS s`)).rows,
    ).toBeDefined();
    const arena = new OutputArena({ allocate: () => true, free: () => undefined }, 1);
    try {
      expect(() =>
        compileExpr(
          { op: "cast", input: col(0, type), type: { kind: "text", maxLength: 3 } },
          { arena, input: [type], params: { slots: [] } },
        ),
      ).toThrow(
        expect.objectContaining({
          name: "UnsupportedPlanError",
          stage: "execute",
          construct: "cast",
        }),
      );
    } finally {
      arena.close();
    }
  }
  const type = TEMPORAL_TYPES[3];
  if (type === undefined) throw new Error("Missing interval type");
  for (const negative of [false, true]) {
    const value = {
      months: negative ? -2147483648 : 2147483647,
      days: negative ? -2147483648 : 2147483647,
      micros: negative ? -9223372036854775808n : 9223372036854775807n,
    };
    const text = negative ? "-infinity" : "infinity";
    expect(
      (await pg.query<{ s: string }>(`SELECT ${pgTemporal(type, value)}::text AS s`)).rows,
    ).toEqual([{ s: text }]);
    // The PG native tuple is observed only to reject unsupported values; no new writer encoding.
    const hex = negative ? "80000000000000008000000080000000" : "7fffffffffffffff7fffffff7fffffff";
    expect(
      (await pg.query<{ v: string }>(`SELECT encode(interval_send(INTERVAL '${text}'),'hex') AS v`))
        .rows,
    ).toEqual([{ v: hex }]);
    const input = temporalVector(type, [value], false, 0),
      arena = new OutputArena({ allocate: () => true, free: () => undefined }, 1);
    try {
      const node = compileExpr(
        { op: "cast", input: col(0, type), type: TEXT },
        { arena, input: [type], params: { slots: [] } },
      );
      expect(() => node.evaluate(batchOf([input]), null)).toThrow(
        expect.objectContaining({
          name: "UnsupportedPlanError",
          sqlState: "0A000",
          stage: "execute",
          construct: "cast",
        }),
      );
    } finally {
      arena.close();
    }
  }
});
