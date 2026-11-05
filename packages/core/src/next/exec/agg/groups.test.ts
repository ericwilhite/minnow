/**
 * GroupTable against a model (a Map from each key's SQL-equality class to its first-seen id):
 * every key kind (int4, int8 past 2^53, float8 with NaN and −0, text, NUMERIC at mixed scales
 * and NaN), single and composite keys, flat / dictionary / constant vectors, selections, NULLs,
 * and enough distinct keys to grow every table several times. The SQL GROUP BY equality is the
 * law (PostgreSQL: NULLs group together, −0 = 0, NaN = NaN, NUMERIC by value).
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../../testing/seeds.js";
import type { PhysicalType } from "../../types.js";
import type { FlatVector, Selection, Vector } from "../../vector/vector.js";
import {
  F64,
  I32,
  I64,
  TEXT,
  UNCHARGED,
  bigints,
  constant,
  decimal,
  decimalValue,
  decimals,
  dictionary,
  numbers,
  texts,
  valueAt,
} from "../kernels/test-support.js";
import { GroupTable } from "./groups.js";

/** `xs[i]`, which must exist. */
function at<T>(xs: readonly T[], i: number): T {
  const x = xs[i];
  if (x === undefined) throw new RangeError(`no element ${String(i)}`);
  return x;
}

const SEEDS = seedsFor("next-group-table", [0x60, 0x61]);
const NUM = decimal(null, null);

type Kind = "int4" | "int8" | "float8" | "text" | "numeric";
const TYPES: Record<Kind, PhysicalType> = {
  int4: I32,
  int8: I64,
  float8: F64,
  text: TEXT,
  numeric: NUM,
};

/** Key value `index` (of `card` distinct classes; `card` itself is NULL) with a variant `v`. */
function keyValue(kind: Kind, index: number, card: number, v: number): unknown {
  if (index === card) return null;
  const centred = index - (card >> 1);
  switch (kind) {
    case "int4":
      return centred * 7919;
    case "int8":
      return index % 3 === 0 ? BigInt(centred) * 2n ** 60n + 1n : BigInt(centred);
    case "float8":
      if (index === 0) return v % 2 === 0 ? -0 : 0;
      if (index === 1) return NaN;
      return centred / 4;
    case "text":
      return `${"x".repeat(index % 37)}k${String(index)}`;
    case "numeric": {
      if (index === 0) return "NaN";
      const scale = v % 3;
      return [BigInt(centred) * 10n ** BigInt(scale), scale] as const;
    }
  }
}

/** The SQL-equality class of a key value (what GROUP BY merges). */
function canonical(kind: Kind, value: unknown): string {
  if (value === null) return "null";
  if (kind === "float8") return Number.isNaN(value) ? "NaN" : String((value as number) + 0);
  if (kind === "numeric") return decimalValue(value);
  if (typeof value === "string" || typeof value === "number" || typeof value === "bigint")
    return String(value);
  throw new TypeError("unexpected key value");
}

function flatOf(kind: Kind, values: readonly unknown[]): FlatVector {
  switch (kind) {
    case "int4":
    case "float8":
      return numbers(TYPES[kind], values as Array<number | null>);
    case "int8":
      return bigints(I64, values as Array<bigint | null>);
    case "text":
      return texts(values as Array<string | null>);
    case "numeric":
      return decimals(NUM, values as never, "int64");
  }
}

/** A batch column in one of the three forms (constant only when every row has one value). */
function vectorOf(kind: Kind, values: readonly unknown[], form: number): Vector {
  const keys = values.map((value) => canonical(kind, value));
  if (form === 2 && keys.every((key) => key === keys[0]) && values[0] !== undefined) {
    const one = flatOf(kind, [values[0]]);
    return constant(one, values.length);
  }
  if (form === 1) {
    // Dictionary: the distinct values (first variant) as entries; NULL rows by validity.
    const entries: unknown[] = [];
    const codeOf = new Map<string, number>();
    const codes = values.map((value, row) => {
      if (value === null) return null;
      const key = at(keys, row);
      let code = codeOf.get(key);
      if (code === undefined) {
        code = entries.length;
        entries.push(value);
        codeOf.set(key, code);
      }
      return code;
    });
    return dictionary(flatOf(kind, entries.length === 0 ? [null] : entries), codes);
  }
  return flatOf(kind, values);
}

interface Scenario {
  readonly kinds: readonly Kind[];
  readonly card: number;
  readonly rows: number;
  readonly seed: number;
}

function check({ kinds, card, rows, seed }: Scenario): void {
  const random = mulberry32(seed);
  const table = new GroupTable(
    kinds.map((kind) => TYPES[kind]),
    UNCHARGED,
  );
  const model = new Map<string, number>();
  const firstValue: unknown[][] = [];
  const gids = new Int32Array(2048);
  for (let start = 0; start < rows;) {
    const length = Math.min(rows - start, 1 + Math.floor(random() * 2048));
    start += length;
    // A low-cardinality batch now and then, so dictionary and constant forms get exercised.
    const narrow = random() < 0.2 ? 1 + Math.floor(random() * 3) : card + 1;
    const values = kinds.map(() => {
      const base = Math.floor(random() * (card + 1));
      return Array.from({ length }, () => {
        const index =
          narrow > card
            ? Math.floor(random() * (card + 1))
            : (base + Math.floor(random() * narrow)) % (card + 1);
        return index;
      });
    });
    const decoded = kinds.map((kind, c) =>
      at(values, c).map((index) => keyValue(kind, index, card, Math.floor(random() * 6))),
    );
    const vectors = kinds.map((kind, c) =>
      vectorOf(kind, at(decoded, c), Math.floor(random() * 3)),
    );
    let selection: Selection | null = null;
    if (random() < 0.5) {
      const picked: number[] = [];
      for (let row = 0; row < length; row++) if (random() < 0.6) picked.push(row);
      selection = { rows: Uint16Array.from(picked), count: picked.length };
    }
    gids.fill(-7);
    table.assign(vectors, length, selection, gids);
    const active =
      selection === null
        ? Array.from({ length }, (_, row) => row)
        : Array.from(selection.rows.subarray(0, selection.count));
    for (const row of active) {
      const key = kinds.map((kind, c) => canonical(kind, valueAt(at(vectors, c), row))).join("|");
      let id = model.get(key);
      if (id === undefined) {
        id = model.size;
        model.set(key, id);
        firstValue.push(kinds.map((_, c) => valueAt(at(vectors, c), row)));
      }
      expect(gids[row], `row ${String(row)} key ${key}`).toBe(id);
    }
  }
  expect(table.groupCount).toBe(model.size);
  // Emitted keys, in chunks: each group's first-seen value (by SQL equality).
  for (let from = 0; from < model.size; from += 2048) {
    const count = Math.min(2048, model.size - from);
    kinds.forEach((kind, c) => {
      const emitted = table.emitKey(c, from, count);
      for (let k = 0; k < count; k++)
        expect(canonical(kind, valueAt(emitted, k))).toBe(
          canonical(kind, at(firstValue, from + k)[c]),
        );
    });
  }
  table.close();
}

const KINDS: readonly Kind[] = ["int4", "int8", "float8", "text", "numeric"];

describe("GroupTable against a GROUP BY model (generated)", () => {
  it.each(SEEDS)(
    "one key of every kind, growing past several doublings (seed %i)",
    (seed) => {
      fc.assert(
        fc.property(
          fc.constantFrom(...KINDS),
          fc.constantFrom(3, 700, 5000),
          fc.integer({ min: 1, max: 12_000 }),
          fc.integer(),
          (kind, card, rows, inner) => {
            check({ kinds: [kind], card, rows, seed: inner });
          },
        ),
        { seed, numRuns: 40 },
      );
    },
    180_000,
  );

  it.each(SEEDS)(
    "composite keys of mixed kinds (seed %i)",
    (seed) => {
      fc.assert(
        fc.property(
          fc.array(fc.constantFrom(...KINDS), { minLength: 2, maxLength: 3 }),
          fc.constantFrom(2, 30, 200),
          fc.integer({ min: 1, max: 8_000 }),
          fc.integer(),
          (kinds, card, rows, inner) => {
            check({ kinds, card, rows, seed: inner });
          },
        ),
        { seed, numRuns: 30 },
      );
    },
    180_000,
  );
});
