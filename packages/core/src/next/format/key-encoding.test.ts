import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { TIMESTAMP_NEG_INFINITY, TIMESTAMP_POS_INFINITY } from "../temporal.js";
import { INT32_MAX, INT32_MIN, INT64_MAX, INT64_MIN, type PhysicalType } from "../types.js";
import {
  encodeEqualityKey,
  encodeSortKey,
  type DecimalKey,
  type IntervalKey,
  type KeyValue,
  type SortKeyPart,
} from "./key-encoding.js";

const SEEDS = seedsFor("next-key-encoding", [0x6b65, 0x79]);
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");
const utf8 = (text: string) => new TextEncoder().encode(text);
const MOOD: PhysicalType = { kind: "enum", name: "mood", labels: ["sad", "ok", "happy"] };
const asc = (type: PhysicalType, value: KeyValue | null): SortKeyPart => ({
  type,
  value,
  direction: "asc",
  nulls: "last",
});

/** Frozen bytes: changing any of these is a stored-format change. */
describe("key encoding golden vectors", () => {
  it.each<[string, SortKeyPart[], string]>([
    ["int32 5", [asc({ kind: "int32", bits: 32 }, 5)], "0180000005"],
    [
      "int32 −1 DESC NULLS FIRST",
      [{ type: { kind: "int32", bits: 32 }, value: -1, direction: "desc", nulls: "first" }],
      "0180000000",
    ],
    ["null ASC NULLS LAST", [asc({ kind: "int32", bits: 32 }, null)], "02"],
    [
      "null DESC NULLS FIRST",
      [{ type: { kind: "text" }, value: null, direction: "desc", nulls: "first" }],
      "00",
    ],
    ["date −infinity", [asc({ kind: "date" }, INT32_MIN)], "0100000000"],
    ["text a\\0b", [asc({ kind: "text" }, new Uint8Array([0x61, 0, 0x62]))], "016100ff620001"],
    [
      "decimal 1.5",
      [asc({ kind: "decimal", precision: null, scale: null }, { coefficient: 15n, scale: 1 })],
      "0104800000011000",
    ],
    [
      "decimal −1.5",
      [asc({ kind: "decimal", precision: null, scale: null }, { coefficient: -15n, scale: 1 })],
      "01027ffffffeefff",
    ],
    [
      "decimal 0",
      [asc({ kind: "decimal", precision: null, scale: null }, { coefficient: 0n, scale: 4 })],
      "0103",
    ],
    [
      "decimal NaN",
      [
        asc(
          { kind: "decimal", precision: null, scale: null },
          { coefficient: 0n, scale: 0, special: "NaN" },
        ),
      ],
      "0106",
    ],
    ["float 1.0", [asc({ kind: "float64" }, 1)], "01bff0000000000000"],
    ["float −1.0", [asc({ kind: "float64" }, -1)], "01400fffffffffffff"],
    ["float −0", [asc({ kind: "float64" }, -0)], "018000000000000000"],
    ["float NaN", [asc({ kind: "float64" }, Number.NaN)], "01fff8000000000000"],
    [
      "timestamp +infinity",
      [asc({ kind: "timestamp", zoned: true }, TIMESTAMP_POS_INFINITY)],
      "01ffffffffffffffff",
    ],
    [
      "timestamp −infinity",
      [asc({ kind: "timestamp", zoned: false }, TIMESTAMP_NEG_INFINITY)],
      "010000000000000000",
    ],
    [
      "interval 1 mon",
      [asc({ kind: "interval" }, { months: 1, days: 0, micros: 0n })],
      "0180000000000000000000025b7f3d4000",
    ],
    ["enum ok (sort: rank)", [asc(MOOD, utf8("ok"))], "010001"],
    [
      "(1 ASC, 'x' DESC)",
      [
        asc({ kind: "int32", bits: 32 }, 1),
        { type: { kind: "text" }, value: utf8("x"), direction: "desc", nulls: "last" },
      ],
      "01800000010187fffe",
    ],
  ])("%s", (_name, parts, expected) => {
    expect(hex(encodeSortKey(parts))).toBe(expected);
  });

  it("equality keys: scale-free NUMERIC, normalized INTERVAL, label-text ENUM, null-safe nulls", () => {
    const decimal: PhysicalType = { kind: "decimal", precision: null, scale: null };
    const eq = (type: PhysicalType, value: KeyValue | null) =>
      hex(encodeEqualityKey([{ type, value }]));
    expect(eq(decimal, { coefficient: 15n, scale: 1 })).toBe(
      eq(decimal, { coefficient: 1500n, scale: 3 }),
    );
    expect(eq(decimal, { coefficient: 15n, scale: 1 })).toBe("0104800000011000");
    expect(eq({ kind: "interval" }, { months: 1, days: 0, micros: 0n })).toBe(
      eq({ kind: "interval" }, { months: 0, days: 30, micros: 0n }),
    );
    expect(eq(MOOD, utf8("ok"))).toBe("016f6b0001");
    expect(eq({ kind: "int32", bits: 32 }, null)).toBe("00");
    expect(() => encodeSortKey([asc(MOOD, utf8("angry"))])).toThrow(RangeError);
    expect(() => encodeSortKey([asc({ kind: "locator", tableId: 1 }, 1)])).toThrow(TypeError);
  });
});

// ---- Generated consistency: order, equality and hash against reference semantics ----

type Kind =
  | "bool"
  | "int32"
  | "date"
  | "int64"
  | "timestamp"
  | "float64"
  | "float32"
  | "decimal"
  | "text"
  | "uuid"
  | "enum"
  | "interval";

const TYPES: Record<Kind, PhysicalType> = {
  bool: { kind: "bool" },
  int32: { kind: "int32", bits: 32 },
  date: { kind: "date" },
  int64: { kind: "int64" },
  timestamp: { kind: "timestamp", zoned: true },
  float64: { kind: "float64" },
  float32: { kind: "float32" },
  decimal: { kind: "decimal", precision: null, scale: null },
  text: { kind: "text" },
  uuid: { kind: "uuid" },
  enum: MOOD,
  interval: { kind: "interval" },
};

const smallBytes = fc
  .uint8Array({ maxLength: 4 })
  .map((bytes) => bytes.map((b) => [0, 1, 0x61, 0xff][b % 4] ?? 0));

const values: Record<Kind, fc.Arbitrary<KeyValue>> = {
  bool: fc.boolean(),
  int32: fc.oneof(
    fc.integer({ min: -3, max: 3 }),
    fc.integer(),
    fc.constantFrom(INT32_MIN, INT32_MAX),
  ),
  date: fc.oneof(fc.integer({ min: -3, max: 3 }), fc.constantFrom(INT32_MIN, INT32_MAX)),
  int64: fc.oneof(fc.bigInt({ min: -3n, max: 3n }), fc.bigInt({ min: INT64_MIN, max: INT64_MAX })),
  timestamp: fc.oneof(
    fc.bigInt({ min: -3n, max: 3n }),
    fc.constantFrom(TIMESTAMP_NEG_INFINITY, TIMESTAMP_POS_INFINITY),
  ),
  float64: fc.oneof(
    fc.double(),
    fc.constantFrom(Number.NaN, -0, 0, Infinity, -Infinity, 1, -1, 1.5),
  ),
  float32: fc.float().map((value) => Math.fround(value)),
  decimal: fc.oneof(
    fc.record({
      coefficient: fc.bigInt({ min: -1200n, max: 1200n }),
      scale: fc.integer({ min: -3, max: 5 }),
    }),
    fc.record({
      coefficient: fc.bigInt({ min: -(10n ** 30n), max: 10n ** 30n }),
      scale: fc.integer({ min: -40, max: 40 }),
    }),
    fc.constantFrom<DecimalKey>(
      { coefficient: 0n, scale: 0, special: "NaN" },
      { coefficient: 0n, scale: 0, special: "Infinity" },
      { coefficient: 0n, scale: 0, special: "-Infinity" },
    ),
  ),
  text: smallBytes,
  uuid: fc.uint8Array({ minLength: 16, maxLength: 16 }).map((bytes) => bytes.map((b) => b % 3)),
  enum: fc.constantFrom("sad", "ok", "happy").map(utf8),
  interval: fc.record({
    months: fc.integer({ min: -2, max: 2 }),
    days: fc.integer({ min: -60, max: 60 }),
    micros: fc.constantFrom(0n, 1n, -1n, 86_400_000_000n),
  }),
};

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index++) {
    const difference = (left[index] ?? 0) - (right[index] ?? 0);
    if (difference !== 0) return Math.sign(difference);
  }
  return Math.sign(left.length - right.length);
}

function decimalRank(value: DecimalKey): number {
  if (value.special === "-Infinity") return 0;
  if (value.special === "Infinity") return 2;
  if (value.special === "NaN") return 3;
  return 1;
}

const bigSign = (value: bigint) => (value > 0n ? 1 : value < 0n ? -1 : 0);

/** SQL comparison by PostgreSQL's rules, independent of the encoder. */
function reference(kind: Kind, a: KeyValue, b: KeyValue): number {
  switch (kind) {
    case "bool":
      return Math.sign(Number(a) - Number(b));
    case "int32":
    case "date":
      return Math.sign((a as number) - (b as number));
    case "int64":
    case "timestamp":
      return bigSign((a as bigint) - (b as bigint));
    case "float64":
    case "float32": {
      const x = a as number;
      const y = b as number;
      if (Number.isNaN(x) || Number.isNaN(y))
        return Number(Number.isNaN(x)) - Number(Number.isNaN(y));
      return x === y ? 0 : x < y ? -1 : 1;
    }
    case "decimal": {
      const x = a as DecimalKey;
      const y = b as DecimalKey;
      const rank = decimalRank(x) - decimalRank(y);
      if (rank !== 0 || decimalRank(x) !== 1) return Math.sign(rank);
      const scale = Math.max(x.scale, y.scale);
      const align = (v: DecimalKey) => v.coefficient * 10n ** BigInt(scale - v.scale);
      return bigSign(align(x) - align(y));
    }
    case "text":
    case "uuid":
      return compareBytes(a as Uint8Array, b as Uint8Array);
    case "enum": {
      const labels = (MOOD as { labels: readonly string[] }).labels;
      const decode = (v: KeyValue) => labels.indexOf(new TextDecoder().decode(v as Uint8Array));
      return Math.sign(decode(a) - decode(b));
    }
    case "interval": {
      const span = (v: IntervalKey) =>
        (BigInt(v.months) * 30n + BigInt(v.days)) * 86_400_000_000n + v.micros;
      return bigSign(span(a as IntervalKey) - span(b as IntervalKey));
    }
  }
}

interface Column {
  readonly kind: Kind;
  readonly direction: "asc" | "desc";
  readonly nulls: "first" | "last";
}

const columnArb: fc.Arbitrary<Column> = fc.record({
  kind: fc.constantFrom(...(Object.keys(TYPES) as Kind[])),
  direction: fc.constantFrom("asc" as const, "desc" as const),
  nulls: fc.constantFrom("first" as const, "last" as const),
});

const tupleArb = (columns: readonly Column[]) =>
  fc.tuple(...columns.map((column) => fc.option(values[column.kind], { freq: 5 })));

const caseArb = fc
  .array(columnArb, { minLength: 1, maxLength: 3 })
  .chain((columns) => fc.tuple(fc.constant(columns), tupleArb(columns), tupleArb(columns)));

function referenceTuple(
  columns: readonly Column[],
  a: Array<KeyValue | null>,
  b: Array<KeyValue | null>,
): number {
  for (const [index, column] of columns.entries()) {
    const x = a[index] ?? null;
    const y = b[index] ?? null;
    let order: number;
    if (x === null || y === null) {
      if (x === y) continue;
      const nullFirst = column.nulls === "first";
      order = x === null ? (nullFirst ? -1 : 1) : nullFirst ? 1 : -1;
      return order;
    }
    order = reference(column.kind, x, y) * (column.direction === "desc" ? -1 : 1);
    if (order !== 0) return order;
  }
  return 0;
}

const sortKey = (columns: readonly Column[], tuple: Array<KeyValue | null>) =>
  encodeSortKey(
    columns.map((column, index) => ({
      type: TYPES[column.kind],
      value: tuple[index] ?? null,
      direction: column.direction,
      nulls: column.nulls,
    })),
  );

const equalityKey = (columns: readonly Column[], tuple: Array<KeyValue | null>) =>
  encodeEqualityKey(
    columns.map((column, index) => ({ type: TYPES[column.kind], value: tuple[index] ?? null })),
  );

describe("key encoding consistency (generated)", () => {
  it.each(SEEDS)("byte order, equality and hashing agree with SQL semantics (seed %i)", (seed) => {
    fc.assert(
      fc.property(caseArb, ([columns, a, b]) => {
        const expected = referenceTuple(columns, a, b);
        expect(compareBytes(sortKey(columns, a), sortKey(columns, b))).toBe(expected);
        const equalA = equalityKey(columns, a);
        const equalB = equalityKey(columns, b);
        expect(compareBytes(equalA, equalB) === 0).toBe(expected === 0);
        if (expected === 0) expect(xxHash32(equalA)).toBe(xxHash32(equalB));
      }),
      { seed, numRuns: 4000 },
    );
  });
});
