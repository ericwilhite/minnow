/** Date difference and timestamp difference: PG fields and errors, without rendering aliases. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { createReach, TEMPORAL_SUB_REACH } from "../../../testing/reach.js";
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
import type { PhysicalType } from "../../types.js";
import { compileExpr } from "./compile.js";
import { OutputArena } from "./vectors.js";
import { I32, batchOf, call, col, valueAt } from "./test-support.js";
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
  await pg.exec("SET TIME ZONE 'UTC';SET IntervalStyle='postgres'");
}, 60_000);
afterAll(async () => {
  await pg.close();
});
const MODES = [
  "finite",
  "same infinity",
  "infinite interval",
  "opposite infinity",
  "overflow",
  "NULL",
] as const;
interface PgDifference {
  v: number | null;
  text: string | null;
  months: number | null;
  days: number | null;
  micros: string | null;
}
interface Shape {
  kind: number;
  dateA: number;
  dateB: number;
  a: bigint;
  b: bigint;
  left: number;
  right: number;
  selected: boolean;
  safe: boolean;
  negative: boolean;
  mode: (typeof MODES)[number];
}
async function check(shape: Shape, reach: ReturnType<typeof createReach<string>>): Promise<void> {
  reach.beginHistory();
  const type = TEMPORAL_TYPES[shape.kind];
  if (type === undefined) throw new Error("Missing type");
  const date = type.kind === "date",
    safe = !date && shape.safe && shape.mode === "finite";
  let a: TemporalValue = date ? shape.dateA : safe ? shape.a % 9007199254740991n : shape.a;
  let b: TemporalValue = date ? shape.dateB : safe ? shape.b % 9007199254740991n : shape.b;
  const infinity = date
    ? shape.negative
      ? DATE_NEG_INFINITY
      : DATE_POS_INFINITY
    : shape.negative
      ? TIMESTAMP_NEG_INFINITY
      : TIMESTAMP_POS_INFINITY;
  if (shape.mode === "same infinity") {
    a = infinity;
    b = infinity;
  }
  if (shape.mode === "infinite interval") {
    a = infinity;
    b = date ? 0 : 0n;
  }
  if (shape.mode === "opposite infinity") {
    a = infinity;
    b = date
      ? shape.negative
        ? DATE_POS_INFINITY
        : DATE_NEG_INFINITY
      : shape.negative
        ? TIMESTAMP_POS_INFINITY
        : TIMESTAMP_NEG_INFINITY;
  }
  if (shape.mode === "overflow") {
    a = date ? DATE_MAX : TIMESTAMP_END - 1n;
    b = date ? DATE_MIN : TIMESTAMP_MIN;
  }
  if (shape.mode === "NULL") {
    a = null;
    b = infinity;
  }
  const lhs: TemporalValue[] = shape.mode === "finite" ? [a, b, null, a] : [a];
  const rhs: TemporalValue[] = shape.mode === "finite" ? [b, a, a, null] : [b];
  if (shape.selected) {
    const poison = date ? DATE_MAX + 1 : TIMESTAMP_END;
    if (shape.mode === "finite") {
      lhs[1] = poison;
      rhs[1] = date ? 0 : 0n;
    }
    lhs.push(poison);
    rhs.push(date ? 0 : 0n);
  }
  const l = temporalVector(type, lhs, safe, shape.left),
    r = temporalVector(type, rhs, safe, shape.right);
  const active = Array.from({ length: lhs.length - (shape.selected ? 1 : 0) }, (_, i) => i).filter(
    (i) => !shape.selected || shape.mode !== "finite" || i !== 1,
  );
  if (shape.selected) active.reverse();
  const pairs = active
    .map(
      (i) => `(${pgTemporal(type, temporalValue(l, i))},${pgTemporal(type, temporalValue(r, i))})`,
    )
    .join(",");
  const sql = date
    ? `SELECT a-b AS v FROM (VALUES ${pairs}) d(a,b)`
    : `SELECT v::text AS text,CASE WHEN isfinite(v) THEN (extract(year FROM v)*12+extract(month FROM v))::int4 END AS months,CASE WHEN isfinite(v) THEN extract(day FROM v)::int4 END AS days,CASE WHEN isfinite(v) THEN ((extract(hour FROM v)*3600+extract(minute FROM v)*60+extract(second FROM v))*1000000)::text END AS micros FROM(SELECT a-b AS v FROM(VALUES ${pairs}) d(a,b)) q`;
  let expected: PgDifference[] | undefined, state: string | undefined;
  try {
    expected = (await pg.query<PgDifference>(sql)).rows;
  } catch (error) {
    state = (error as { code: string }).code;
    expect(state).toBe("22008");
  }
  const infiniteResult =
    expected?.some((row) => row.text === "infinity" || row.text === "-infinity") ?? false;
  const output: PhysicalType = date ? I32 : { kind: "interval" };
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
    lhs.length,
  );
  try {
    const compiled = compileExpr(call("sub", output, col(0, type), col(1, type)), {
      arena,
      input: [type, type],
      params: { slots: [] },
    });
    const batch = batchOf(
      [l, r],
      shape.selected
        ? { rows: Uint16Array.from([...active, lhs.length - 1]), count: active.length }
        : null,
    );
    const run = () => compiled.evaluate(batch, batch.selection);
    if (infiniteResult) {
      expect(run).toThrow(
        expect.objectContaining({
          name: "UnsupportedPlanError",
          sqlState: "0A000",
          stage: "execute",
          construct: "function",
        }),
      );
      reach.reach("infinite interval refused");
      if (expected?.some((row) => row.text === "infinity"))
        reach.reach("positive infinity refused");
      if (expected?.some((row) => row.text === "-infinity"))
        reach.reach("negative infinity refused");
    } else if (state !== undefined) {
      expect(run, sql).toThrow(expect.objectContaining({ sqlState: state }));
      reach.reach("22008");
    } else {
      const wanted = (expected ?? []).map((row) =>
        date
          ? row.v
          : row.micros === null
            ? null
            : {
                months: row.months,
                days: row.days,
                micros: BigInt(row.micros.replace(/\.0+$/u, "")),
              },
      );
      const read = () => {
        const result = run();
        return active.map((i) => (date ? valueAt(result, i) : temporalValue(result, i)));
      };
      expect(read(), sql).toEqual(wanted);
      expect(read(), sql).toEqual(wanted);
      reach.reach("rows");
      if (wanted.some((v) => v === null)) reach.reach("strict NULL");
    }
    reach.reach(
      date ? "date" : type.kind === "timestamp" && type.zoned ? "timestamptz" : "timestamp",
    );
    reach.reach(["flat", "dictionary", "constant"][shape.left] ?? "missing");
    reach.reach(shape.selected ? "selected" : "all rows");
    if (!date && shape.mode === "finite") reach.reach(safe ? "safe timestamp" : "wide timestamp");
    if (shape.selected && shape.left !== 2) reach.reach("unselected garbage");
    if (shape.mode === "NULL" && shape.left === 2) reach.reach("constant NULL");
  } finally {
    arena.close();
    expect(charged).toBe(0);
  }
}
it.each(seedsFor("next-temporal-subtract", [0xb625, 0xb626]))(
  "matches PG differences and preserves exact interval fields (seed %i)",
  async (seed) => {
    const reach = createReach<string>("temporal subtraction", TEMPORAL_SUB_REACH);
    const base: Shape = {
      kind: 0,
      dateA: DATE_MAX,
      dateB: DATE_MIN,
      a: -1n,
      b: 0n,
      left: 0,
      right: 0,
      selected: false,
      safe: false,
      negative: false,
      mode: "finite",
    };
    const examples: Array<[Shape]> = [];
    for (let kind = 0; kind < 3; kind++)
      for (const mode of MODES)
        for (let left = 0; left < 3; left++)
          for (const negative of [false, true])
            examples.push([
              { ...base, kind, mode, left, right: left, selected: left === 1, negative },
            ]);
    for (const a of [
      -1n,
      86400000001n,
      -86400000001n,
      9007199254740993n,
      TIMESTAMP_END - 1n,
      TIMESTAMP_MIN,
    ])
      for (const safe of [false, true]) examples.push([{ ...base, kind: 1, a, b: 0n, safe }]);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          kind: fc.integer({ min: 0, max: 2 }),
          dateA: fc.integer({ min: DATE_MIN, max: DATE_MAX }),
          dateB: fc.integer({ min: DATE_MIN, max: DATE_MAX }),
          a: fc.bigInt({ min: TIMESTAMP_MIN, max: TIMESTAMP_END - 1n }),
          b: fc.bigInt({ min: TIMESTAMP_MIN, max: TIMESTAMP_END - 1n }),
          left: fc.integer({ min: 0, max: 2 }),
          right: fc.integer({ min: 0, max: 2 }),
          selected: fc.boolean(),
          safe: fc.boolean(),
          negative: fc.boolean(),
          mode: fc.constantFrom(...MODES),
        }),
        (shape) => check(shape, reach),
      ),
      { seed, numRuns: examples.length + 80, examples },
    );
    reach.assertReached();
  },
  120_000,
);
