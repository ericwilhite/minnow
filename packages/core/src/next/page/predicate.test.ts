/**
 * Predicate pushdown (predicate.ts, select.ts, ChunkReader.select / refine / pagesMatching)
 * against a plain model: decode every row, evaluate the SQL comparison on the decoded value,
 * keep the true ones. Generated over every pushed-down type and every page codec, with nulls,
 * NaN, −0, specials, truncated byte bounds and constants on and around the stored values.
 */
import fc from "fast-check";
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { ColumnChunk } from "../format/manifest.js";
import type { PhysicalType } from "../types.js";
import { PageEncoding } from "../format/manifest.js";
import { type MemoryExtentSpace, UNLIMITED } from "./memory-extents.js";
import {
  chunkMayMatch,
  type ColumnPredicate,
  type DecimalConstant,
  type PredicateConstant,
} from "./predicate.js";
import { ChunkReader, DENSE_SELECTION, prefersDense } from "./reader.js";
import { defined, writeGroup, type DecimalValue, type TestValue } from "./test-support.js";

const SEEDS = seedsFor("next-predicate-pushdown", [0x9e11, 0x2a, 0x77d1]);
const RUNS = 600;

type Family =
  | "int32"
  | "date"
  | "int64"
  | "timestamp"
  | "float64"
  | "float32"
  | "bool"
  | "decimal"
  | "decimal128"
  | "text"
  | "bytes"
  | "uuid"
  | "enum"
  | "interval"
  | "mixed";

const TYPES: Record<Family, PhysicalType> = {
  int32: { kind: "int32", bits: 32 },
  date: { kind: "date" },
  int64: { kind: "int64" },
  timestamp: { kind: "timestamp", zoned: false },
  float64: { kind: "float64" },
  float32: { kind: "float32" },
  bool: { kind: "bool" },
  decimal: { kind: "decimal", precision: 18, scale: 2 },
  decimal128: { kind: "decimal", precision: null, scale: 2 },
  text: { kind: "text" },
  bytes: { kind: "bytes" },
  uuid: { kind: "uuid" },
  enum: { kind: "enum", name: "mood", labels: ["sad", "ok", "happy"] },
  interval: { kind: "interval" },
  mixed: { kind: "decimal", precision: null, scale: null },
};

const OPS = ["=", "<>", "<", "<=", ">", ">=", "in", "between", "isNull", "isNotNull"] as const;
type Op = (typeof OPS)[number];
const CODECS = ["constant", "plain", "for", "delta", "dictionary", "bitmap", "allNull"] as const;
type Codec = (typeof CODECS)[number];

const reachStates: string[] = [];
const codecOps: Record<Codec, readonly Op[]> = {
  constant: OPS,
  plain: OPS,
  for: OPS,
  delta: OPS,
  dictionary: OPS,
  bitmap: OPS,
  allNull: OPS,
};
for (const codec of CODECS) for (const op of codecOps[codec]) reachStates.push(`${codec}:${op}`);
for (const op of OPS) reachStates.push(`pruned:${op}`, `kept:${op}`);
for (const codec of CODECS)
  if (codec !== "allNull") reachStates.push(`sparse:${codec}`, `dense:${codec}`);
const reach = createReach("next-predicate-pushdown", reachStates);
afterAll(() => {
  reach.assertReached();
});

const encoder = new TextEncoder();

// ── Values ───────────────────────────────────────────────────────────────────────────────────

/** A column's values: a pool of distinct values (few → dictionary/FOR, many → plain). */
function values(family: Family): fc.Arbitrary<TestValue[]> {
  const rows = fc.integer({ min: 1, max: 700 });
  const column = (value: fc.Arbitrary<TestValue>, shape: "pool" | "wide" | "sequence") =>
    rows.chain((count) =>
      shape === "pool"
        ? fc
            .array(value, { minLength: 1, maxLength: 6 })
            .chain((pool) =>
              fc.array(fc.constantFrom(...pool), { minLength: count, maxLength: count }),
            )
        : fc.array(value, { minLength: count, maxLength: count }),
    );
  const shapes = (value: fc.Arbitrary<TestValue>, sequence?: fc.Arbitrary<TestValue[]>) =>
    fc.oneof(
      { weight: 1, arbitrary: column(value, "pool") },
      { weight: 1, arbitrary: column(value, "wide") },
      ...(sequence === undefined ? [] : [{ weight: 2, arbitrary: sequence }]),
    );
  const ints32 = fc.oneof(
    fc.integer({ min: -3, max: 3 }),
    fc.integer({ min: 0, max: 300 }),
    fc.integer(),
  );
  const seq32 = rows.chain((count) =>
    fc
      .integer({ min: -1000, max: 1000 })
      .map((start) => Array.from({ length: count }, (_, k) => start + Math.floor(k / 3))),
  );
  const ints64 = fc.oneof(
    fc.bigInt({ min: -3n, max: 3n }),
    fc.bigInt({ min: 0n, max: 70_000n }),
    fc.bigInt({ min: -(2n ** 63n), max: 2n ** 63n - 1n }),
    fc.constantFrom(2n ** 53n + 1n, -(2n ** 63n), 2n ** 63n - 1n),
  );
  const seq64 = rows.chain((count) =>
    fc
      .bigInt({ min: -(2n ** 60n), max: 2n ** 60n })
      .map((start) => Array.from({ length: count }, (_, k) => start + BigInt(k * 7))),
  );
  const floats = fc.oneof(
    { weight: 1, arbitrary: fc.constant(Number.NaN) },
    fc.constantFrom(
      Number.NaN,
      -0,
      0,
      1,
      -1,
      0.5,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
    ),
    fc.double({ noDefaultInfinity: true }),
  );
  const coefficient = (wide: boolean): fc.Arbitrary<TestValue> =>
    fc.oneof(
      {
        weight: 8,
        arbitrary: fc.record({
          coefficient: fc.bigInt({ min: -500n, max: 500n }),
          special: fc.constant(0),
        }),
      },
      {
        weight: 3,
        arbitrary: fc.record({
          coefficient: wide
            ? fc.bigInt({ min: -(2n ** 120n), max: 2n ** 120n })
            : fc.bigInt({ min: -(10n ** 17n), max: 10n ** 17n }),
          special: fc.constant(0),
        }),
      },
      {
        weight: 1,
        arbitrary: fc.record({ coefficient: fc.constant(0n), special: fc.constantFrom(1, 2, 3) }),
      },
    );
  const words = fc.constantFrom(
    "",
    "a",
    "ab",
    "abc",
    "b",
    "ba",
    "z",
    "zz",
    "x".repeat(40),
    `${"x".repeat(40)}y`,
    "ÿ",
    "é",
  );
  const text = fc.oneof(
    words.map((word) => encoder.encode(word)),
    fc.string({ maxLength: 50 }).map((word) => encoder.encode(word)),
  );
  const bytes = fc.oneof(
    text,
    fc.uint8Array({ maxLength: 40 }),
    fc.constant(new Uint8Array(36).fill(0xff)),
  );
  switch (family) {
    case "int32":
    case "date":
      return shapes(ints32, seq32);
    case "int64":
    case "timestamp":
      return shapes(ints64, seq64);
    case "float64":
      return shapes(floats);
    case "float32":
      return shapes(floats.map((value) => Math.fround(value)));
    case "bool":
      return shapes(fc.boolean());
    case "decimal":
      return shapes(coefficient(false));
    case "decimal128":
      return shapes(coefficient(true));
    case "text":
      return shapes(text);
    case "bytes":
      return shapes(bytes);
    case "uuid":
      return shapes(
        fc.uint8Array({ minLength: 16, maxLength: 16 }).map((value) => {
          value[0] = (value[0] ?? 0) & 3;
          return value;
        }),
      );
    case "enum":
      return shapes(fc.constantFrom("sad", "ok", "happy").map((label) => encoder.encode(label)));
    case "interval":
      return shapes(
        fc.record({
          months: fc.integer({ min: -2, max: 2 }),
          days: fc.constant(0),
          micros: fc.constant(0n),
        }),
      );
    case "mixed":
      return shapes(
        fc.record({
          coefficient: fc.bigInt({ min: -50n, max: 50n }),
          special: fc.constant(0),
          scale: fc.constantFrom(0, 1, 3),
        }),
      );
  }
}

/** Constants on and around the stored values. */
function constantFor(family: Family, pool: readonly TestValue[]): fc.Arbitrary<PredicateConstant> {
  const stored = pool.filter((value) => value !== null);
  const pick = stored.length === 0 ? null : fc.constantFrom(...stored);
  const around = <T>(
    make: (value: TestValue) => fc.Arbitrary<T>,
    other: fc.Arbitrary<T>,
  ): fc.Arbitrary<T> => (pick === null ? other : fc.oneof(pick.chain(make), other));
  switch (family) {
    case "int32":
    case "date":
      return around(
        (value) =>
          fc.constantFrom(
            value as number,
            (value as number) + 1,
            (value as number) - 1,
            (value as number) + 0.5,
            (value as number) - 0.5,
          ),
        fc.oneof(fc.integer(), fc.constantFrom(2 ** 31, -(2 ** 31) - 1, 1e300, -1e300)),
      );
    case "int64":
    case "timestamp":
      return around(
        (value) =>
          fc.constantFrom<PredicateConstant>(
            value as bigint,
            (value as bigint) + 1n,
            (value as bigint) - 1n,
          ),
        fc.oneof(
          fc.bigInt({ min: -(2n ** 64n), max: 2n ** 64n }),
          fc.constantFrom<PredicateConstant>(0.5, -2.5, 2n ** 63n),
        ),
      );
    case "float64":
    case "float32":
      return around(
        (value) => fc.constantFrom(value as number, (value as number) + 0.25),
        fc.constantFrom(
          Number.NaN,
          -0,
          0,
          1,
          Number.POSITIVE_INFINITY,
          Number.NEGATIVE_INFINITY,
          0.1,
        ),
      );
    case "bool":
      return fc.boolean();
    case "decimal":
    case "decimal128":
      return around(
        (value) => {
          const { coefficient, special } = value as DecimalValue;
          if (special !== 0)
            return fc.constant<PredicateConstant>({
              coefficient: 0n,
              scale: 0,
              special: special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity",
            });
          return fc.constantFrom<PredicateConstant>(
            { coefficient, scale: 2 },
            { coefficient: coefficient * 10n, scale: 3 },
            { coefficient: coefficient * 10n + 1n, scale: 3 },
            { coefficient: coefficient * 10n - 5n, scale: 3 },
            { coefficient: coefficient / 100n, scale: 0 },
          );
        },
        fc.oneof(
          fc.record({
            coefficient: fc.bigInt({ min: -1000n, max: 1000n }),
            scale: fc.constantFrom(0, 1, 2, 4),
          }),
          fc.constantFrom<PredicateConstant>(
            { coefficient: 0n, scale: 0, special: "NaN" },
            { coefficient: 0n, scale: 0, special: "Infinity" },
            { coefficient: 0n, scale: 0, special: "-Infinity" },
          ),
        ),
      );
    case "text":
    case "bytes":
    case "uuid":
    case "enum":
    case "interval":
    case "mixed":
      return around(
        (value) => {
          const bytes = value instanceof Uint8Array ? value : new Uint8Array(0);
          const next = bytes.slice();
          if (next.length > 0) next[next.length - 1] = ((next[next.length - 1] ?? 0) + 1) & 0xff;
          return fc.constantFrom<PredicateConstant>(
            bytes,
            bytes.subarray(0, Math.max(0, bytes.length - 1)),
            Uint8Array.from([...bytes, 0]),
            next,
          );
        },
        fc.oneof(fc.uint8Array({ maxLength: 40 }), fc.constant(new Uint8Array(0))),
      );
  }
}

// ── The model ────────────────────────────────────────────────────────────────────────────────

function floatOrder(a: number, b: number): number {
  if (Number.isNaN(a)) return Number.isNaN(b) ? 0 : 1;
  if (Number.isNaN(b)) return -1;
  return a === b ? 0 : a < b ? -1 : 1; // −0 === 0
}

const RANK = { finite: 1, NaN: 3, Infinity: 2, "-Infinity": 0 } as const;

/** −1, 0 or 1: the stored value against the constant, exactly. */
function compare(family: Family, value: TestValue, constant: PredicateConstant): number {
  switch (family) {
    case "int32":
    case "date":
    case "int64":
    case "timestamp": {
      const v = typeof value === "bigint" ? value : BigInt(value as number);
      if (typeof constant === "bigint") return v < constant ? -1 : v > constant ? 1 : 0;
      const c = constant as number;
      if (Number.isInteger(c)) {
        const big = BigInt(c);
        return v < big ? -1 : v > big ? 1 : 0;
      }
      // A fraction: v is never equal; compare with its floor.
      return v <= BigInt(Math.floor(c)) ? -1 : 1;
    }
    case "float64":
    case "float32":
      return floatOrder(value as number, constant as number);
    case "bool":
      return Number(value) - Number(constant);
    case "decimal":
    case "decimal128": {
      const v = value as DecimalValue;
      const c = constant as DecimalConstant;
      const vr =
        v.special === 0
          ? RANK.finite
          : v.special === 1
            ? RANK.NaN
            : v.special === 2
              ? RANK.Infinity
              : RANK["-Infinity"];
      const cr = c.special === undefined ? RANK.finite : RANK[c.special];
      if (vr !== cr || vr !== RANK.finite) return vr < cr ? -1 : vr > cr ? 1 : 0;
      // coefficient / 10^2 against c.coefficient / 10^c.scale.
      const left = v.coefficient * 10n ** BigInt(c.scale);
      const right = c.coefficient * 100n;
      return left < right ? -1 : left > right ? 1 : 0;
    }
    default:
      return Buffer.compare(Buffer.from(value as Uint8Array), Buffer.from(constant as Uint8Array));
  }
}

function modelMatches(family: Family, value: TestValue, predicate: ColumnPredicate): boolean {
  if (predicate.op === "isNull") return value === null;
  if (predicate.op === "isNotNull") return value !== null;
  if (value === null) return false;
  switch (predicate.op) {
    case "in":
      return predicate.values.some((constant) => compare(family, value, constant) === 0);
    case "between":
      return (
        compare(family, value, predicate.low) >= 0 && compare(family, value, predicate.high) <= 0
      );
    default: {
      const order = compare(family, value, predicate.value);
      switch (predicate.op) {
        case "=":
          return order === 0;
        case "<>":
          return order !== 0;
        case "<":
          return order < 0;
        case "<=":
          return order <= 0;
        case ">":
          return order > 0;
        case ">=":
          return order >= 0;
      }
    }
  }
}

/** Whether the type and operator are pushed down (predicate.ts module comment). */
function pushedDown(family: Family, op: Op): boolean {
  if (op === "isNull" || op === "isNotNull") return true;
  if (family === "interval" || family === "mixed") return false;
  if (family === "enum") return op === "=" || op === "<>" || op === "in";
  return true;
}

// ── The property ─────────────────────────────────────────────────────────────────────────────

const KIND_NAMES: Record<number, Codec> = {
  [PageEncoding.plain]: "plain",
  [PageEncoding.constant]: "constant",
  [PageEncoding.bitmap]: "bitmap",
  [PageEncoding.frameOfReference]: "for",
  [PageEncoding.dictionary]: "dictionary",
};

function codecOf(space: MemoryExtentSpace, chunk: ColumnChunk, offset: number): Codec {
  const at = chunk.data.extent.offset + chunk.data.offset + offset;
  const flags = space.bytes[at + 2] ?? 0;
  if ((flags & 2) !== 0) return "allNull";
  if ((flags & 4) !== 0) return "delta";
  return KIND_NAMES[space.bytes[at] ?? 0] ?? "plain";
}

const scenario = fc
  .constantFrom(...(Object.keys(TYPES) as Family[]))
  .chain((family) =>
    fc.record({
      family: fc.constant(family),
      values: values(family),
      nullRate: fc.constantFrom(0, 0, 0.1, 0.5, 1),
      nullSeed: fc.nat(),
      pageBytes: fc.constantFrom(64, 512, 32 * 1024),
      op: fc.constantFrom(...OPS),
      subsetSeed: fc.nat(),
    }),
  )
  .chain((base) => {
    const constant = constantFor(base.family, base.values);
    const predicate: fc.Arbitrary<ColumnPredicate> =
      base.op === "isNull" || base.op === "isNotNull"
        ? fc.constant<ColumnPredicate>(
            base.op === "isNull" ? { op: "isNull" } : { op: "isNotNull" },
          )
        : base.op === "in"
          ? fc
              .array(constant, { maxLength: 6 })
              .map((list) => ({ op: "in" as const, values: list }))
          : base.op === "between"
            ? fc
                .tuple(constant, constant)
                .map(([low, high]) => ({ op: "between" as const, low, high }))
            : constant.map((value) => ({ op: base.op as "=", value }));
    return predicate.map((p) => ({ ...base, predicate: p }));
  });

describe.each(SEEDS)("predicate pushdown matches decode-then-filter (seed %i)", (seed) => {
  it("selects, refines and prunes exactly", async () => {
    await fc.assert(
      fc.asyncProperty(
        scenario,
        async ({ family, values: raw, nullRate, nullSeed, pageBytes, predicate, subsetSeed }) => {
          let state = nullSeed | 1;
          const random = (): number => {
            state ^= state << 13;
            state ^= state >>> 17;
            state ^= state << 5;
            return (state >>> 0) / 2 ** 32;
          };
          const values = raw.map((value) => (random() < nullRate ? null : value));
          const type = TYPES[family];
          const vector =
            family === "decimal128"
              ? { scale: 2, tier: "int128" as const }
              : family === "decimal"
                ? { scale: 2 }
                : undefined;
          const { space, group } = await writeGroup(
            [{ type, values, ...(vector === undefined ? {} : { vector }) }],
            { pageBytes },
          );
          const chunk = defined(group.columns[0]);
          const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
          const compiled = reader.compilePredicate(predicate);
          expect(compiled !== null).toBe(pushedDown(family, predicate.op));
          if (compiled === null) {
            reader.close();
            return;
          }
          const expected = values.flatMap((value, row) =>
            modelMatches(family, value, predicate) ? [row] : [],
          );
          // Group pruning is sound.
          if (expected.length > 0)
            expect(chunkMayMatch(compiled, values.length, chunk.statistics)).toBe(true);
          // Page pruning is sound; select reads only the unpruned pages.
          const kept = reader.pagesMatching(compiled);
          await reader.load(kept);
          const out = new Uint16Array(values.length);
          const count = reader.select(compiled, 0, values.length, out);
          expect(Array.from(out.subarray(0, count))).toEqual(expected);
          // Refine a subset (in place).
          const subset = Uint16Array.from(
            values.flatMap((_, row) =>
              ((row * 2654435761 + subsetSeed) >>> 0) % 3 === 0 ? [row] : [],
            ),
          );
          await reader.load(reader.pagesForRows(subset));
          const wanted = Array.from(subset).filter((row) =>
            modelMatches(family, values[row] ?? null, predicate),
          );
          const refined = reader.refine(compiled, subset, 0, subset.length, subset);
          expect(Array.from(subset.subarray(0, refined))).toEqual(wanted);

          // Declared reach.
          reach.beginHistory();
          const op = predicate.op;
          const keptSet = new Set(kept);
          reader.directory.pages.forEach((entry, page) => {
            const codec = codecOf(space, chunk, entry.offset);
            reach.reach(`${codec}:${op}`);
            reach.reach(keptSet.has(page) ? `kept:${op}` : `pruned:${op}`);
            const inPage = expected.filter(
              (row) => row >= entry.firstRow && row < entry.firstRow + entry.rowCount,
            ).length;
            if (codec !== "allNull" && entry.rowCount >= 8) {
              if (inPage > 0 && inPage * 4 < entry.rowCount) reach.reach(`sparse:${codec}`);
              if (inPage * 4 > entry.rowCount * 3) reach.reach(`dense:${codec}`);
            }
          });
          reader.close();
        },
      ),
      { seed, numRuns: RUNS },
    );
  }, 300_000);
});

describe("predicate pushdown: fixed cases", () => {
  it("never prunes a page whose bound equals the constant, nor one holding NaN for a NaN test", async () => {
    const { space, group } = await writeGroup(
      [{ type: { kind: "float64" }, values: [1, 2, Number.NaN, -0, 3] }],
      { pageBytes: 16 },
    );
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget: UNLIMITED });
    for (const [predicate, rows] of [
      [{ op: "=", value: Number.NaN }, [2]],
      [{ op: ">", value: 3 }, [2]],
      [{ op: "=", value: 0 }, [3]],
      [{ op: "<=", value: -0 }, [3]],
      [{ op: "<>", value: Number.NaN }, [0, 1, 3, 4]],
    ] as Array<[ColumnPredicate, number[]]>) {
      const compiled = defined(reader.compilePredicate(predicate) ?? undefined);
      await reader.load(reader.pagesMatching(compiled));
      const out = new Uint16Array(5);
      expect(
        Array.from(out.subarray(0, reader.select(compiled, 0, 5, out))),
        JSON.stringify(predicate),
      ).toEqual(rows);
    }
    reader.close();
  });

  it("prefers a dense decode from DENSE_SELECTION of a span (the measured crossover)", () => {
    expect(DENSE_SELECTION).toBe(0.4);
    expect([
      prefersDense(0, 100),
      prefersDense(39, 100),
      prefersDense(40, 100),
      prefersDense(100, 100),
    ]).toEqual([false, false, true, true]);
  });

  it("keeps an all-NaN page for every test NaN satisfies", async () => {
    const values = [Number.NaN, Number.NaN, Number.NaN, null, 1, 2];
    const { space, group } = await writeGroup([{ type: { kind: "float64" }, values }], {
      pageBytes: 16,
    });
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget: UNLIMITED });
    expect(
      reader.directory.pages.some((page) => Number.isNaN(page.min) && Number.isNaN(page.max)),
    ).toBe(true);
    for (const [predicate, rows] of [
      [{ op: "=", value: Number.NaN }, [0, 1, 2]],
      [{ op: ">", value: 1 }, [0, 1, 2, 5]],
      [{ op: "<>", value: 1 }, [0, 1, 2, 5]],
      [{ op: ">=", value: Number.NaN }, [0, 1, 2]],
      [{ op: "in", values: [2, Number.NaN] }, [0, 1, 2, 5]],
      [{ op: "<", value: Number.NaN }, [4, 5]],
    ] as Array<[ColumnPredicate, number[]]>) {
      const compiled = defined(reader.compilePredicate(predicate) ?? undefined);
      await reader.load(reader.pagesMatching(compiled));
      const out = new Uint16Array(values.length);
      expect(
        Array.from(out.subarray(0, reader.select(compiled, 0, values.length, out))),
        JSON.stringify(predicate),
      ).toEqual(rows);
    }
    reader.close();
  });

  it("charges its scratch and code bitmaps, and returns them at close", async () => {
    const { space, group } = await writeGroup([
      {
        type: { kind: "text" },
        values: Array.from({ length: 3000 }, (_, k) => encoder.encode(`v${String(k % 7)}`)),
      },
      {
        type: { kind: "int64" },
        values: Array.from({ length: 3000 }, (_, k) => BigInt(k) * 1000n),
      },
    ]);
    let held = 0;
    const budget = {
      allocate: (bytes: number) => ((held += bytes), true),
      free: (bytes: number) => void (held -= bytes),
    };
    for (const chunk of group.columns) {
      const reader = await ChunkReader.open(space, chunk, { budget });
      const compiled = defined(
        reader.compilePredicate(
          chunk.type.kind === "text"
            ? { op: "in", values: [encoder.encode("v3")] }
            : { op: ">", value: 5000n },
        ) ?? undefined,
      );
      await reader.load(reader.pagesMatching(compiled));
      const out = new Uint16Array(3000);
      expect(reader.select(compiled, 0, 3000, out)).toBeGreaterThan(0);
      reader.close();
    }
    expect(held).toBe(0);
  });
});
