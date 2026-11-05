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
const rowArb: fc.Arbitrary<Row> = fc.record({
  k: fc.oneof(fc.constant(null), fc.integer({ min: 0, max: 4 })),
  t: fc.oneof(fc.constant(null), fc.constantFrom("a", "b", "c", "dd", "é")),
  i: fc.oneof(fc.constant(null), fc.integer({ min: -(2 ** 31), max: 2 ** 31 - 1 })),
  b: fc.oneof(
    fc.constant(null),
    fc.bigInt({ min: -(2n ** 63n), max: 2n ** 63n - 1n }),
    fc.bigInt({ min: -1000n, max: 1000n }),
    // Safe-tier values whose sums leave ±2^52 (the f64 part spills into the carry).
    fc.bigInt({ min: 2n ** 51n, max: 2n ** 53n - 1n }).map((v) => (v % 3n === 0n ? -v : v)),
  ),
  f: fc.oneof(
    { weight: 3, arbitrary: fc.constant(null) },
    { weight: 6, arbitrary: fc.integer({ min: -1000, max: 1000 }).map((v) => v / 8) },
    { weight: 1, arbitrary: fc.constantFrom(0, -0) },
    { weight: 1, arbitrary: fc.constantFrom(NaN, Infinity, -Infinity) },
    // Rare: finite values whose sum overflows (22003), alone or before an infinity.
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

/** The batches: rows split into batches of `split` rows; text as a dictionary when `encode`. */
function batches(rows: readonly Row[], split: number, encode: boolean): Batch[] {
  const out: Batch[] = [];
  for (let start = 0; start < rows.length; start += split) {
    const part = rows.slice(start, start + split);
    const entries = ["a", "b", "c", "dd", "é"];
    const textColumn: Vector = encode
      ? dictionary(
          texts(entries),
          part.map((r) => (r.t === null ? null : entries.indexOf(r.t))),
        )
      : texts(part.map((r) => r.t));
    const wide = part.some(
      (r) => r.b !== null && (r.b > 2n ** 53n - 1n || r.b < -(2n ** 53n - 1n)),
    );
    const nSafe = part.every((r) => !Array.isArray(r.n) || Math.abs(Number(r.n[0])) < 2 ** 53);
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
          nSafe ? "safe" : "int128",
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
  const values = rows
    .map(
      (r) =>
        `(${r.k === null ? "null::int4" : String(r.k)}, ${r.t === null ? "null::text" : `'${r.t}'`}, ${r.i === null ? "null::int4" : `${String(r.i)}::int4`}, ${r.b === null ? "null::int8" : `${r.b.toString()}::int8`}, ${r.f === null ? "null::float8" : `'${Object.is(r.f, -0) ? "-0" : String(r.f)}'::float8`}, ${numText(r.n)}::numeric, ${r.m === null ? "null::numeric(12,2)" : `${numText(r.m)}::numeric(12,2)`}, ${r.flag === null ? "null::bool" : String(r.flag)})`,
    )
    .join(", ");
  // An empty input: one typed NULL row, filtered out (VALUES cannot be empty).
  const empty =
    "(null::int4, null::text, null::int4, null::int8, null::float8, null::numeric, null::numeric(12,2), null::bool)";
  const keySql = key === null ? "" : `${COLUMN_SQL[key] ?? ""}::text as key, `;
  const sql = `select ${keySql}${cases.map((c, k) => `(${c.sql})::text as a${String(k)}`).join(", ")} from (values ${rows.length === 0 ? empty : values}) v(${COLUMN_SQL.join(", ")})${rows.length === 0 ? " where false" : ""}${key === null ? "" : ` group by ${COLUMN_SQL[key] ?? ""}`}`;
  const theirs = await postgres(sql);
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

describe("aggregation against PostgreSQL (PGlite, generated)", () => {
  it.each(SEEDS)(
    "every aggregate, grouped by int / text / none, any batching (seed %i)",
    async (seed) => {
      await fc.assert(
        fc.asyncProperty(
          fc.array(rowArb, { minLength: 0, maxLength: 40 }),
          fc.constantFrom(null, 0, 1, 6),
          fc.integer({ min: 1, max: 16 }),
          fc.boolean(),
          async (rows, key, split, encode) => {
            await judge(rows, key, IN_ORDER, split, encode);
            await judge(rows, key, SORTED, split, encode);
            await judge(rows, key, FLOAT_SUM, split, encode);
          },
        ),
        { seed, numRuns: 120 },
      );
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
