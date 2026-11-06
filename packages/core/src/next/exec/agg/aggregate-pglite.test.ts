/**
 * The aggregate operator against PostgreSQL (PGlite): generated rows, grouped by an integer, a
 * text (dictionary-encoded or flat) or a NUMERIC key, with COUNT(*), COUNT, SUM, AVG, MIN, MAX,
 * BOOL_AND/OR, COUNT(DISTINCT), SUM(DISTINCT) and FILTER, over int4, int8 (past 2^53), float8 (−0, NaN, ±∞) and
 * NUMERIC (mixed scales, every tier, NaN and infinities). PostgreSQL's answer is the law; rows
 * are compared as sets (group order is unspecified).
 *
 * Deliberate, documented differences (DECISIONS.md C11): AVG of integers and floats is float64
 * here, NUMERIC in PostgreSQL; those columns are compared numerically within float rounding.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { AggregateCall, OutputColumn, ScalarExpr } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Vector } from "../../vector/vector.js";
import { OutputArena } from "../kernels/vectors.js";
import {
  BOOL,
  F64,
  I32,
  I64,
  TEXT,
  UNCHARGED,
  bigints,
  cmp,
  col,
  decimal,
  decimals,
  dictionary,
  lit,
  numbers,
  texts,
  valueAt,
  sqlLiteral,
} from "../kernels/test-support.js";
import { drive, type BatchSource, type Operator, type PushResult } from "../pipeline.js";
import { AggregateOperator } from "./aggregate.js";

/** `xs[i]`, which must exist. */
function at<T>(xs: readonly T[], i: number): T {
  const x = xs[i];
  if (x === undefined) throw new RangeError(`no element ${String(i)}`);
  return x;
}

const SEEDS = seedsFor("next-aggregate-pglite", [0xa66, 0x5]);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

const D = decimal(null, null);
const D12 = decimal(12, 2);
type Num = readonly [bigint, number] | "NaN" | "Infinity" | "-Infinity" | null;
interface Row {
  readonly k: number | null;
  readonly t: string | null;
  readonly i: number | null;
  readonly b: bigint | null;
  readonly f: number | null;
  readonly n: Num;
  readonly m: Num; // NUMERIC(12,2): finite, scale 2
  readonly flag: boolean | null;
}

const numArb: fc.Arbitrary<Num> = fc.oneof(
  {
    weight: 8,
    arbitrary: fc.tuple(
      fc.bigInt({ min: -(10n ** 14n), max: 10n ** 14n }),
      fc.integer({ min: 0, max: 5 }),
    ),
  },
  {
    weight: 2,
    arbitrary: fc.tuple(
      fc.bigInt({ min: -(10n ** 30n), max: 10n ** 30n }),
      fc.integer({ min: 0, max: 8 }),
    ),
  },
  {
    // Near ±2^127 (the int128 tier) and past it (the wide tier). Positive ones at scale 0:
    // two in one group pass 2^127 through the carry (aligned values that each fit 128 bits).
    weight: 3,
    arbitrary: fc.oneof(
      fc.tuple(fc.bigInt({ min: 2n ** 126n, max: 2n ** 127n - 1n }), fc.constant(0)),
      fc.tuple(
        fc.oneof(
          fc.bigInt({ min: 2n ** 126n, max: 2n ** 127n - 1n }).chain((v) => fc.constantFrom(v, -v)),
          fc.constantFrom(
            2n ** 127n - 1n,
            -(2n ** 127n),
            2n ** 127n,
            -(2n ** 127n) - 1n,
            2n ** 127n + 12345n,
            -(2n ** 130n),
          ),
        ),
        fc.integer({ min: 0, max: 2 }),
      ),
    ),
  },
  {
    // Few values at several scales (1.5, 1.50, 1.500): ties whose display scale differs.
    weight: 3,
    arbitrary: fc
      .tuple(fc.integer({ min: -3, max: 3 }), fc.integer({ min: 0, max: 3 }))
      .map(([v, s]) => [BigInt(v) * 10n ** BigInt(s), s] as const),
  },
  {
    weight: 1,
    arbitrary: fc.constantFrom("NaN" as const, "Infinity" as const, "-Infinity" as const, null),
  },
);
/**
 * The text column's values, as rows and as the dictionary entries of encoded batches. "O'Brien":
 * an apostrophe, which the SQL side must escape.
 */
const TEXT_DOMAIN = ["a", "b", "c", "dd", "é", "O'Brien"] as const;

const rowArb: fc.Arbitrary<Row> = fc.record({
  k: fc.oneof(fc.constant(null), fc.integer({ min: 0, max: 4 })),
  t: fc.oneof(fc.constant(null), fc.constantFrom(...TEXT_DOMAIN)),
  i: fc.oneof(
    fc.constant(null),
    fc.integer({ min: -(2 ** 31), max: 2 ** 31 - 1 }),
    // The int4 extremes: unquoted, PostgreSQL's −2147483648::int4 fails on the literal.
    fc.constantFrom(-(2 ** 31), 2 ** 31 - 1),
  ),
  b: fc.oneof(
    fc.constant(null),
    fc.bigInt({ min: -(2n ** 63n), max: 2n ** 63n - 1n }),
    fc.constantFrom(-(2n ** 63n), 2n ** 63n - 1n),
    fc.bigInt({ min: -1000n, max: 1000n }),
    // Safe-tier values whose sums leave ±2^52 (the f64 part spills into the carry).
    fc.bigInt({ min: 2n ** 51n, max: 2n ** 53n - 1n }).map((v) => (v % 3n === 0n ? -v : v)),
  ),
  f: fc.oneof(
    { weight: 30, arbitrary: fc.constant(null) },
    { weight: 60, arbitrary: fc.integer({ min: -1000, max: 1000 }).map((v) => v / 8) },
    { weight: 10, arbitrary: fc.constantFrom(0, -0) },
    { weight: 10, arbitrary: fc.constantFrom(NaN, Infinity, -Infinity) },
    // Rare (about 30% of 40-row cases): finite values whose sum overflows (22003, which ends the
    // whole query, so it must not hide the other aggregates in most cases).
    { weight: 1, arbitrary: fc.constantFrom(1e308, -1e308) },
  ),
  n: numArb,
  m: fc.oneof(
    fc.constant(null),
    fc.bigInt({ min: -(10n ** 11n), max: 10n ** 11n }).map((c) => [c, 2] as const),
  ),
  flag: fc.oneof(fc.constant(null), fc.boolean()),
});

function numText(value: Num): string {
  if (value === null) return "null";
  if (typeof value === "string") return `'${value}'`;
  const [c, s] = value;
  const negative = c < 0n;
  const digits = (negative ? -c : c).toString().padStart(s + 1, "0");
  return `'${negative ? "-" : ""}${s === 0 ? digits : `${digits.slice(0, digits.length - s)}.${digits.slice(digits.length - s)}`}'`;
}

/** The narrowest coefficient tier that holds every value. */
function tierOf(values: readonly Num[]): "safe" | "int128" | "wide" {
  const magnitudes = values.map((v): bigint => {
    if (!Array.isArray(v)) return 0n;
    const c = (v as readonly [bigint, number])[0];
    return c < 0n ? -c : c;
  });
  if (magnitudes.every((m) => m < 2n ** 53n)) return "safe";
  return magnitudes.every((m) => m < 2n ** 127n) ? "int128" : "wide";
}

/** Tiers of the NUMERIC batches the last `batches` call built (declared reach). */
const tiers = new Set<string>();

/** The batches: rows split into batches of `split` rows; text as a dictionary when `encode`. */
function batches(rows: readonly Row[], split: number, encode: boolean): Batch[] {
  const out: Batch[] = [];
  for (let start = 0; start < rows.length; start += split) {
    const part = rows.slice(start, start + split);
    const entries: readonly string[] = TEXT_DOMAIN;
    const textColumn: Vector = encode
      ? dictionary(
          texts(entries),
          part.map((r) => {
            if (r.t === null) return null;
            const code = entries.indexOf(r.t);
            if (code < 0) throw new RangeError(`No dictionary entry for ${r.t}`);
            return code;
          }),
        )
      : texts(part.map((r) => r.t));
    const wide = part.some(
      (r) => r.b !== null && (r.b > 2n ** 53n - 1n || r.b < -(2n ** 53n - 1n)),
    );
    const nTier = tierOf(part.map((r) => r.n));
    tiers.add(nTier);
    out.push({
      length: part.length,
      selection: null,
      columns: [
        numbers(
          I32,
          part.map((r) => r.k),
        ),
        textColumn,
        numbers(
          I32,
          part.map((r) => r.i),
        ),
        wide
          ? bigints(
              I64,
              part.map((r) => r.b),
            )
          : numbers(
              I64,
              part.map((r) => (r.b === null ? null : Number(r.b))),
            ),
        numbers(
          F64,
          part.map((r) => r.f),
        ),
        decimals(
          D,
          part.map((r) => r.n),
          nTier,
        ),
        decimals(
          D12,
          part.map((r) => r.m),
          "safe",
          2,
        ),
        numbers(
          BOOL,
          part.map((r) => (r.flag === null ? null : r.flag ? 1 : 0)),
        ),
      ],
    });
  }
  return out;
}

const INPUT: readonly PhysicalType[] = [I32, TEXT, I32, I64, F64, D, D12, BOOL];
const COLUMN_SQL = ["k", "t", "i", "b", "f", "n", "m", "flag"];

interface Case {
  readonly sql: string;
  readonly call: AggregateCall;
  /** How to compare: exact text, or float within rounding (AVG of ints/floats, C11). */
  readonly compare: "exact" | "float";
}

const call = (
  fn: AggregateCall["fn"],
  type: PhysicalType,
  args: ScalarExpr[],
  extra: Partial<AggregateCall> = {},
): AggregateCall => ({
  fn,
  args,
  distinct: false,
  filter: null,
  orderBy: [],
  type,
  ...extra,
});

const CASES: readonly Case[] = [
  { sql: "count(*)", call: call("count_star", I64, []), compare: "exact" },
  { sql: "count(n)", call: call("count", I64, [col(5, D)]), compare: "exact" },
  { sql: "sum(i)", call: call("sum", I64, [col(2, I32)]), compare: "exact" },
  { sql: "sum(b)", call: call("sum", decimal(null, 0), [col(3, I64)]), compare: "exact" },
  { sql: "sum(f)", call: call("sum", F64, [col(4, F64)]), compare: "exact" },
  { sql: "sum(n)", call: call("sum", D, [col(5, D)]), compare: "exact" },
  { sql: "sum(m)", call: call("sum", D, [col(6, D12)]), compare: "exact" },
  { sql: "avg(n)", call: call("avg", D, [col(5, D)]), compare: "exact" },
  { sql: "avg(m)", call: call("avg", D, [col(6, D12)]), compare: "exact" },
  { sql: "avg(i)", call: call("avg", F64, [col(2, I32)]), compare: "float" },
  { sql: "avg(f)", call: call("avg", F64, [col(4, F64)]), compare: "float" },
  { sql: "min(n)", call: call("min", D, [col(5, D)]), compare: "exact" },
  { sql: "max(b)", call: call("max", I64, [col(3, I64)]), compare: "exact" },
  { sql: "max(t)", call: call("max", TEXT, [col(1, TEXT)]), compare: "exact" },
  { sql: "max(n)", call: call("max", D, [col(5, D)]), compare: "exact" },
  { sql: "min(f)", call: call("min", F64, [col(4, F64)]), compare: "exact" },
  { sql: "max(f)", call: call("max", F64, [col(4, F64)]), compare: "exact" },
  { sql: "min(i)", call: call("min", I32, [col(2, I32)]), compare: "exact" },
  { sql: "bool_and(flag)", call: call("bool_and", BOOL, [col(7, BOOL)]), compare: "exact" },
  { sql: "bool_or(flag)", call: call("bool_or", BOOL, [col(7, BOOL)]), compare: "exact" },
  {
    sql: "count(distinct t)",
    call: call("count", I64, [col(1, TEXT)], { distinct: true }),
    compare: "exact",
  },
  {
    sql: "count(distinct n)",
    call: call("count", I64, [col(5, D)], { distinct: true }),
    compare: "exact",
  },
  {
    sql: "sum(distinct i)",
    call: call("sum", I64, [col(2, I32)], { distinct: true }),
    compare: "exact",
  },
  {
    sql: "sum(m) filter (where i > 0)",
    call: call("sum", D, [col(6, D12)], { filter: cmp(">", col(2, I32), lit(numbers(I32, [0]))) }),
    compare: "exact",
  },
];

/**
 * PostgreSQL feeds a query's aggregates in input order (Values Scan → Agg / HashAggregate) unless
 * one of them is DISTINCT: then it sorts the input (presorted aggregates, or Sort → GroupAggregate,
 * not stable). MIN/MAX ties and float sums depend on that order, so the DISTINCT cases run in a
 * query of their own.
 */
const IN_ORDER = CASES.filter((c) => !c.call.distinct);
const SORTED = CASES.filter((c) => c.call.distinct);
/** SUM(float8) alone: its 22003 is not masked by AVG's (which overflows on more inputs). */
const FLOAT_SUM = CASES.filter((c) => c.sql === "sum(f)");
/** SUM(float8) with COUNT(*): two row-layout members, so its 22003 is judged in that layout too. */
const FLOAT_SUM_COUNT = CASES.filter((c) => c.sql === "sum(f)" || c.sql === "count(*)");
/** COUNT and float SUM/AVG together (AVG's 22003 masks SUM's, which FLOAT_SUM_COUNT judges). */
const EXTREME_FLOAT_CASES = CASES.filter((c) =>
  ["count(*)", "count(n)", "sum(f)", "avg(f)"].includes(c.sql),
);

/** Our value as PostgreSQL's text (numbers by value; floats within rounding for "float"). */
function render(type: PhysicalType, value: unknown): string {
  if (value === null) return "NULL";
  if (type.kind === "bool") return value === true ? "true" : "false";
  if (type.kind === "decimal") {
    if (typeof value === "string") return value;
    const [c, s] = value as [bigint, number];
    return numText([c, s]).slice(1, -1);
  }
  const plain = value as string | number | bigint;
  if (type.kind === "float64") return Object.is(value, -0) ? "-0" : String(plain);
  return String(plain);
}

class Collect implements Operator {
  readonly rows: string[][] = [];
  constructor(readonly types: readonly PhysicalType[]) {}
  push(batch: Batch): PushResult {
    for (let r = 0; r < batch.length; r++)
      this.rows.push(
        batch.columns.map((column, c) => render(at(this.types, c), valueAt(column, r))),
      );
    return "ready";
  }
  ready(): Promise<void> {
    return Promise.resolve();
  }
  finish(): void {
    // collected
  }
  abort(): void {
    // nothing held
  }
}

async function postgres(sql: string): Promise<string[][] | { error: string }> {
  try {
    const result = await pg.query<Record<string, string | null>>(sql);
    return result.rows.map((row) => Object.values(row).map((v) => v ?? "NULL"));
  } catch (error) {
    return { error: (error as { code?: string }).code ?? String(error) };
  }
}

function close(a: string, b: string): boolean {
  if (a === b) return true;
  if (a === "-0" || b === "-0") return false; // a sign of zero is display, not rounding
  const x = Number(a);
  const y = Number(b);
  return (
    Number.isFinite(x) && Number.isFinite(y) && Math.abs(x - y) <= 1e-9 * Math.max(1, Math.abs(y))
  );
}

async function judge(
  rows: readonly Row[],
  key: number | null,
  cases: readonly Case[],
  split: number,
  encode: boolean,
): Promise<void> {
  const keyTypes = key === null ? [] : [at(INPUT, key)];
  const output: OutputColumn[] = [
    ...keyTypes.map((type) => ({ name: "key", type, nullable: true })),
    ...cases.map((c) => ({ name: c.sql, type: c.call.type, nullable: true })),
  ];
  const rel = {
    id: 1,
    op: "aggregate" as const,
    output,
    input: { id: 0, op: "values" as const, output: [], rows: [] },
    groupBy: key === null ? [] : [key],
    groupingSets: null,
    aggregates: cases.map((c) => c.call),
  };
  const sink = new Collect(output.map((o) => o.type));
  const context = {
    arena: new OutputArena(UNCHARGED, 2048),
    params: { slots: [] as FlatVector[] },
    input: INPUT,
  };
  let ours: string[][] | { error: string };
  try {
    const operator = new AggregateOperator(rel, context, UNCHARGED, sink);
    const data = batches(rows, split, encode);
    let next = 0;
    const source: BatchSource = {
      next: () => (next < data.length ? { batch: at(data, next++) } : null),
      abort: () => undefined,
    };
    await drive(source, operator);
    ours = sink.rows;
  } catch (error) {
    ours = { error: (error as { sqlState?: string }).sqlState ?? String(error) };
  }
  // Every value quoted and cast (sqlLiteral): no literal may fail on its own.
  const values = rows
    .map(
      (r) =>
        `(${[
          sqlLiteral("int4", r.k),
          sqlLiteral("text", r.t),
          sqlLiteral("int4", r.i),
          sqlLiteral("int8", r.b),
          sqlLiteral("float8", r.f),
          sqlLiteral("numeric", r.n),
          sqlLiteral("numeric(12,2)", r.m),
          sqlLiteral("bool", r.flag),
        ].join(", ")})`,
    )
    .join(", ");
  // An empty input: one typed NULL row, filtered out (VALUES cannot be empty).
  const empty =
    "(null::int4, null::text, null::int4, null::int8, null::float8, null::numeric, null::numeric(12,2), null::bool)";
  const keySql = key === null ? "" : `${COLUMN_SQL[key] ?? ""}::text as key, `;
  const sql = `select ${keySql}${cases.map((c, k) => `(${c.sql})::text as a${String(k)}`).join(", ")} from (values ${rows.length === 0 ? empty : values}) v(${COLUMN_SQL.join(", ")})${rows.length === 0 ? " where false" : ""}${key === null ? "" : ` group by ${COLUMN_SQL[key] ?? ""}`}`;
  const theirs = await postgres(sql);
  if ("error" in theirs) {
    // An error is the aggregates' only if the input relation stands on its own: every column
    // cast and output (PostgreSQL may prune what a query does not use, so not a COUNT).
    const input = `select ${COLUMN_SQL.map((c) => `${c}::text`).join(", ")} from (values ${rows.length === 0 ? empty : values}) v(${COLUMN_SQL.join(", ")})`;
    const alone = await postgres(input);
    if ("error" in alone)
      throw new Error(
        `Harness bug: the input itself fails in PostgreSQL (${alone.error}): ${input}`,
      );
  }
  if ("error" in theirs || "error" in ours) {
    expect(ours, sql).toEqual(theirs);
    return;
  }
  // A NUMERIC group's key is one of its values (1.5 or 1.50): compare keys by value.
  const byValue = (text: string) =>
    key === 5 || key === 6
      ? text.includes(".")
        ? text.replace(/0+$/u, "").replace(/\.$/u, "")
        : text
      : text;
  const order = (table: string[][]) =>
    table
      .map((row) => (keyTypes.length === 0 ? row : [byValue(row[0] ?? ""), ...row.slice(1)]))
      .sort((a, b) => (a[0] ?? "").localeCompare(b[0] ?? ""));
  const a = order(ours);
  const b = order(theirs);
  expect(a.length, sql).toBe(b.length);
  for (let r = 0; r < a.length; r++)
    for (let c = 0; c < (a[r]?.length ?? 0); c++) {
      const kind = c < keyTypes.length ? "exact" : (cases[c - keyTypes.length]?.compare ?? "exact");
      const x = a[r]?.[c] ?? "";
      const y = b[r]?.[c] ?? "";
      if (kind === "float")
        expect(close(x, y), `${sql}\nrow ${String(r)} col ${String(c)}: ${x} vs ${y}`).toBe(true);
      else expect(x, `${sql}\nrow ${String(r)} col ${String(c)}`).toBe(y);
    }
}

type FloatEvent = "overflow" | "Sxx-only overflow" | "opposite infinities" | "NaN with Infinity";

/**
 * What PostgreSQL's SUM(float8) does over a group's values in input order (float8pl, the first
 * value being the state): 22003 for an infinite sum of finite operands (then it stops), NaN from
 * opposite infinities, NaN with an infinity.
 */
function sumEvents(values: ReadonlyArray<number | null>): Set<FloatEvent> {
  const events = new Set<FloatEvent>();
  const infinite = (x: number) => x === Infinity || x === -Infinity;
  let started = false;
  let state = 0;
  for (const v of values) {
    if (v === null) continue;
    if (!started) {
      started = true;
      state = v;
      continue;
    }
    const r = state + v;
    if (infinite(r) && !infinite(state) && !infinite(v)) {
      events.add("overflow");
      return events;
    }
    if (infinite(state) && infinite(v) && state !== v) events.add("opposite infinities");
    if ((Number.isNaN(state) && infinite(v)) || (infinite(state) && Number.isNaN(v)))
      events.add("NaN with Infinity");
    state = r;
  }
  return events;
}

/**
 * What PostgreSQL's AVG(float8) does over a group's values in input order (float8_accum,
 * Youngs–Cramer): 22003 when Sx or Sxx becomes infinite from a finite state and a finite value
 * ("Sxx-only" when Sx stays finite), then it stops.
 */
function avgEvents(values: ReadonlyArray<number | null>): Set<FloatEvent> {
  const events = new Set<FloatEvent>();
  const infinite = (x: number) => x === Infinity || x === -Infinity;
  let n = 0;
  let sx = 0;
  let sxx = 0;
  for (const v of values) {
    if (v === null) continue;
    const n1 = n + 1;
    const sx1 = sx + v;
    let sxx1 = sxx;
    if (n > 0) {
      const t = v * n1 - sx1;
      sxx1 = sxx + (t * t) / (n1 * n);
      if (infinite(sx1) || infinite(sxx1)) {
        if (!infinite(sx) && !infinite(v)) {
          events.add("overflow");
          if (!infinite(sx1)) events.add("Sxx-only overflow");
          return events;
        }
        sxx1 = NaN;
      }
    } else if (Number.isNaN(v) || infinite(v)) sxx1 = NaN;
    n = n1;
    sx = sx1;
    sxx = sxx1;
  }
  return events;
}

describe("aggregation against PostgreSQL (PGlite, generated)", () => {
  it.each(SEEDS)(
    "every aggregate, grouped by int / text / none, any batching (seed %i)",
    async (seed) => {
      const reach = createReach("next-aggregate-pglite", [
        "int128 batch",
        "wide batch",
        "safe and int128 batches",
        "NUMERIC sum past 2^127",
        "int4 minimum or maximum",
        "int8 minimum or maximum",
        "apostrophe in text",
      ] as const);
      await fc.assert(
        fc.asyncProperty(
          fc.array(rowArb, { minLength: 0, maxLength: 40 }),
          fc.constantFrom(null, 0, 1, 6),
          fc.integer({ min: 1, max: 16 }),
          fc.boolean(),
          async (rows, key, split, encode) => {
            reach.beginHistory();
            for (const r of rows) {
              if (r.i === -(2 ** 31) || r.i === 2 ** 31 - 1) reach.reach("int4 minimum or maximum");
              if (r.b === -(2n ** 63n) || r.b === 2n ** 63n - 1n)
                reach.reach("int8 minimum or maximum");
              if (r.t?.includes("'") === true) reach.reach("apostrophe in text");
            }
            tiers.clear();
            batches(rows, split, encode);
            if (tiers.has("int128")) reach.reach("int128 batch");
            if (tiers.has("wide")) reach.reach("wide batch");
            if (tiers.has("safe") && tiers.has("int128")) reach.reach("safe and int128 batches");
            // A group's exact NUMERIC sum past 128 bits (PostgreSQL keeps it exact), and one that
            // gets there from aligned values that each fit 128 bits (the carry overflow path).
            const groups = new Map<string, Array<readonly [bigint, number]>>();
            for (const r of rows)
              if (Array.isArray(r.n)) {
                const g =
                  key === null
                    ? ""
                    : String((r as unknown as Record<string, unknown>)[COLUMN_SQL[key] ?? ""]);
                groups.set(g, [...(groups.get(g) ?? []), r.n as readonly [bigint, number]]);
              }
            for (const values of groups.values()) {
              const scale = Math.max(...values.map(([, s]) => s));
              const aligned = values.map(([c, s]) => c * 10n ** BigInt(scale - s));
              const sum = aligned.reduce((a, b) => a + b, 0n);
              if (sum >= 2n ** 127n || sum < -(2n ** 127n)) {
                reach.reach("NUMERIC sum past 2^127");
              }
            }
            await judge(rows, key, IN_ORDER, split, encode);
            await judge(rows, key, SORTED, split, encode);
            await judge(rows, key, FLOAT_SUM, split, encode);
          },
        ),
        { seed, numRuns: 120 },
      );
      reach.assertReached();
    },
    180_000,
  );

  it("review B4-p2d-1: an int4 minimum beside an overflowing SUM: the 22003 is the SUM's", async () => {
    // Unquoted, −2147483648::int4 failed in PostgreSQL itself (22003, integer out of range), and
    // the suite took that for SUM(f)'s 22003: a false accept. Quoted, the input stands
    // (SUM(i) alone has a value), and SUM(f)'s 22003 is PostgreSQL's answer.
    const rows: Row[] = [0, 1].map(() => ({
      k: 1,
      t: "a",
      i: -(2 ** 31),
      b: 1n,
      f: 1e308,
      n: null,
      m: null,
      flag: true,
    }));
    const values = `(values ${rows.map(() => `(${sqlLiteral("int4", -(2 ** 31))}, ${sqlLiteral("float8", 1e308)})`).join(", ")}) v(i, f)`;
    expect(await postgres(`select sum(i)::text from ${values}`)).toEqual([["-4294967296"]]);
    expect(await postgres(`select sum(f)::text from ${values}`)).toEqual({ error: "22003" });
    await judge(
      rows,
      1,
      CASES.filter((c) => c.sql === "sum(i)" || c.sql === "sum(f)"),
      1,
      false,
    );
  });

  it.each(SEEDS)(
    "COUNT and float SUM/AVG over extreme float8 values, grouped, against PostgreSQL (seed %i)",
    async (seed) => {
      // f is mostly ±1e308, ±1e154, ±Infinity, NaN and −0 in two or three groups, so float8pl's
      // and float8_accum's 22003 rules (including Sxx's own overflow around 1e154, where the sum
      // stays finite) and SUM's −0 identity are judged per group; the main property rarely puts
      // such values together in one group. Reach marks what PostgreSQL's transitions actually
      // did, in input order (sumEvents, avgEvents), and a result state only when no group's
      // 22003 hid the query's results.
      const reach = createReach("next-aggregate-pglite-extreme-float", [
        "float SUM overflow in a group",
        "AVG overflow",
        "Sxx-only overflow (finite sum)",
        "opposite infinities (NaN)",
        "NaN with Infinity",
        "infinite input",
        "negative-zero-only group",
      ] as const);
      const extreme = fc.constantFrom(
        1e308,
        -1e308,
        1e154,
        -1e154,
        1.5e154,
        Infinity,
        -Infinity,
        NaN,
        -0,
        0,
        1.5,
        null,
      );
      await fc.assert(
        fc.asyncProperty(
          fc.array(fc.tuple(rowArb, extreme, fc.constantFrom("a", "O'Brien", null)), {
            minLength: 1,
            maxLength: 12,
          }),
          fc.integer({ min: 1, max: 6 }),
          async (triples, split) => {
            reach.beginHistory();
            const rows = triples.map(([r, f, t]) => ({ ...r, f, t }));
            const groups = new Map<string | null, Array<number | null>>();
            for (const r of rows) groups.set(r.t, [...(groups.get(r.t) ?? []), r.f]);
            const sums = [...groups.values()].map(sumEvents);
            const avgs = [...groups.values()].map(avgEvents);
            const sumFails = sums.some((e) => e.has("overflow"));
            const avgFails = avgs.some((e) => e.has("overflow"));
            if (sumFails) reach.reach("float SUM overflow in a group");
            if (avgFails) reach.reach("AVG overflow");
            if (avgs.some((e) => e.has("Sxx-only overflow")))
              reach.reach("Sxx-only overflow (finite sum)");
            // COUNT(*) + SUM(f) shows its results unless a SUM failed.
            if (!sumFails) {
              if (sums.some((e) => e.has("opposite infinities")))
                reach.reach("opposite infinities (NaN)");
              if (sums.some((e) => e.has("NaN with Infinity"))) reach.reach("NaN with Infinity");
              if (rows.some((r) => r.f === Infinity || r.f === -Infinity))
                reach.reach("infinite input");
              for (const values of groups.values())
                if (values.length > 0 && values.every((v) => Object.is(v, -0)))
                  reach.reach("negative-zero-only group");
            }
            await judge(rows, 1, EXTREME_FLOAT_CASES, split, false);
            await judge(rows, 1, FLOAT_SUM_COUNT, split, false);
          },
        ),
        { seed, numRuns: 120 },
      );
      reach.assertReached();
    },
    180_000,
  );

  it.each(SEEDS)(
    "NUMERIC SUM / AVG / MIN / MAX around ±2^127 at one scale, any batching (seed %i)",
    async (seed) => {
      const reach = createReach("next-aggregate-int128-boundary", [
        "carry overflow from 128-bit values",
        "sum back inside 128 bits after an overflow",
        "safe and int128 batches",
      ] as const);
      const big = fc.bigInt({ min: 2n ** 125n, max: 2n ** 127n - 1n });
      const value = fc.oneof(
        { weight: 3, arbitrary: big },
        { weight: 1, arbitrary: big.map((v) => -v) },
        { weight: 2, arbitrary: fc.bigInt({ min: -1000n, max: 1000n }) },
        { weight: 1, arbitrary: fc.constant(null) },
      );
      const cases = CASES.filter((c) =>
        ["sum(n)", "avg(n)", "min(n)", "max(n)", "count(n)"].includes(c.sql),
      );
      await fc.assert(
        fc.asyncProperty(
          fc.array(fc.tuple(fc.integer({ min: 0, max: 2 }), value), {
            minLength: 1,
            maxLength: 24,
          }),
          fc.integer({ min: 0, max: 3 }),
          fc.integer({ min: 1, max: 8 }),
          fc.constantFrom(null, 0),
          async (pairs, scale, split, key) => {
            reach.beginHistory();
            const rows: Row[] = pairs.map(([k, n]) => ({
              k,
              t: null,
              i: null,
              b: null,
              f: null,
              n: n === null ? null : ([n, scale] as const),
              m: null,
              flag: null,
            }));
            tiers.clear();
            batches(rows, split, false);
            if (tiers.has("safe") && tiers.has("int128")) reach.reach("safe and int128 batches");
            const groups = new Map<number, bigint[]>();
            for (const [k, n] of pairs)
              if (n !== null) {
                const g = key === null ? 0 : k;
                groups.set(g, [...(groups.get(g) ?? []), n]);
              }
            for (const values of groups.values()) {
              let running = 0n;
              let overflowed = false;
              for (const v of values) {
                running += v;
                if (running >= 2n ** 127n || running < -(2n ** 127n)) overflowed = true;
              }
              if (overflowed) {
                if (running >= 2n ** 127n || running < -(2n ** 127n))
                  reach.reach("carry overflow from 128-bit values");
                else reach.reach("sum back inside 128 bits after an overflow");
              }
            }
            await judge(rows, key, cases, split, false);
          },
        ),
        { seed, numRuns: 80 },
      );
      reach.assertReached();
    },
    180_000,
  );

  it("groups NUMERIC keys by value (1.5 = 1.50) and floats with −0 = 0", async () => {
    const rows: Row[] = [[150n, 2], [15n, 1], [2n, 0], null, [200n, 2]].map((n) => ({
      k: 1,
      t: "a",
      i: 1,
      b: 1n,
      f: 0,
      n: n as Num,
      m: null,
      flag: true,
    }));
    await judge(rows, 5, [at(CASES, 0)], 2, false);
  });

  it("raises a float SUM's overflow even when ±∞ follows in the same batch", async () => {
    const base = { k: 1, t: "a", i: 1, b: 1n, n: null, m: null, flag: true } as const;
    const rows: Row[] = [1e308, 1e308, Infinity, 1].map((f) => ({ ...base, f }));
    await judge(rows, null, FLOAT_SUM, 16, false);
    await judge(rows, 0, FLOAT_SUM, 16, false);
  });
});
