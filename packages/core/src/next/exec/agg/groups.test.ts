/**
 * GroupTable against a model (a Map from each key's SQL-equality class to its first-seen id):
 * every key kind (int4, int8 past 2^53, float8 with NaN and −0, text, NUMERIC at mixed scales
 * and NaN), single and composite keys, flat / dictionary / constant vectors, selections, NULLs,
 * and enough distinct keys to grow every table several times. The SQL GROUP BY equality is the
 * law (PostgreSQL: NULLs group together, −0 = 0, NaN = NaN, NUMERIC by value).
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
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
import { ByteArena, Charged, GroupTable, type Int32KeyEvent } from "./groups.js";

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

/** Returns whether the composite table was still direct-mapped at the end. */
function check({ kinds, card, rows, seed }: Scenario): boolean {
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
  const direct = table.directMapped;
  table.close();
  return direct;
}

const KINDS: readonly Kind[] = ["int4", "int8", "float8", "text", "numeric"];

interface IntScenario {
  /** Key k is `base + stride × k` (int32-clamped), k < card; NULL at times. */
  readonly base: number;
  readonly stride: number;
  readonly card: number;
  readonly rows: number;
  /** Chance per batch that the keys drift past the range seen (widening a direct array). */
  readonly drift: number;
  /** Chance per batch of one far key (moving a direct array to the hash table). */
  readonly outlier: number;
  readonly seed: number;
}

/** One int4 key against the GROUP BY model; returns what the key column did. */
function checkInts(scenario: IntScenario): ReadonlySet<Int32KeyEvent> {
  const random = mulberry32(scenario.seed);
  const table = new GroupTable([I32], UNCHARGED);
  // Keyed by the generated value itself (NULL as null): each row's value is in `values`, which
  // the vector forms only encode differently.
  const model = new Map<number | null, number>();
  const firstValue: Array<number | null> = [];
  const gids = new Int32Array(2048);
  const clamp = (x: number): number => Math.max(-0x8000_0000, Math.min(0x7fff_ffff, x));
  let reach = scenario.card;
  for (let start = 0; start < scenario.rows;) {
    const length = Math.min(scenario.rows - start, 1 + Math.floor(random() * 2048));
    start += length;
    if (random() < scenario.drift) reach += 1 + Math.floor(random() * scenario.card);
    const far = random() < scenario.outlier ? Math.floor(random() * length) : -1;
    const values = Array.from({ length }, (_, row) => {
      if (random() < 0.03) return null;
      if (row === far) return clamp(scenario.base + (random() < 0.5 ? -1 : 1) * 2 ** 30);
      return clamp(scenario.base + scenario.stride * Math.floor(random() * reach));
    });
    const vector = vectorOf("int4", values, Math.floor(random() * 3));
    let selection: Selection | null = null;
    if (random() < 0.3) {
      const picked: number[] = [];
      for (let row = 0; row < length; row++) if (random() < 0.6) picked.push(row);
      selection = { rows: Uint16Array.from(picked), count: picked.length };
    }
    gids.fill(-7);
    table.assign([vector], length, selection, gids);
    const active =
      selection === null
        ? Array.from({ length }, (_, row) => row)
        : Array.from(selection.rows.subarray(0, selection.count));
    for (const row of active) {
      const key = values[row] ?? null;
      let id = model.get(key);
      if (id === undefined) {
        id = model.size;
        model.set(key, id);
        firstValue.push(key);
      }
      // One cheap comparison per row; the assertion (and its message) only on a mismatch.
      if (gids[row] !== id) expect(gids[row], `row ${String(row)} key ${String(key)}`).toBe(id);
    }
  }
  expect(table.groupCount).toBe(model.size);
  for (let from = 0; from < model.size; from += 2048) {
    const count = Math.min(2048, model.size - from);
    const emitted = table.emitKey(0, from, count);
    for (let k = 0; k < count; k++) {
      const value = valueAt(emitted, k);
      if (value !== firstValue[from + k]) expect(value).toBe(firstValue[from + k]);
    }
  }
  const history = new Set(table.keyHistory(0));
  table.close();
  return history;
}

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
      // Direct-mapped while the radices fit 2^16 slots, hashed past that (TupleKeys).
      const reach = createReach("next-group-table-composite", ["direct-mapped", "hashed"] as const);
      fc.assert(
        fc.property(
          fc.array(fc.constantFrom(...KINDS), { minLength: 2, maxLength: 3 }),
          fc.constantFrom(2, 30, 200),
          fc.integer({ min: 1, max: 8_000 }),
          fc.integer(),
          (kinds, card, rows, inner) => {
            reach.beginHistory();
            reach.reach(check({ kinds, card, rows, seed: inner }) ? "direct-mapped" : "hashed");
          },
        ),
        { seed, numRuns: 30 },
      );
      reach.assertReached();
    },
    180_000,
  );

  it.each(SEEDS)(
    "int4 keys over dense and sparse ranges: direct-mapped, widened, hashed (seed %i)",
    (seed) => {
      // Law: GROUP BY's equality classes, first-seen group ids, and emitted keys are the same in
      // either mode and across every move between them (Int32Keys contract).
      const reach = createReach("next-group-table-int32", [
        "direct",
        "hashed",
        "widened",
        "hash to direct",
        "direct to hash",
      ] as const);
      fc.assert(
        fc.property(
          fc.record({
            base: fc.constantFrom(0, -5_000, 2 ** 31 - 100_000, -(2 ** 31) + 3),
            stride: fc.constantFrom(1, 1, 3, 7919),
            card: fc.constantFrom(50, 3_000, 60_000, 300_000),
            rows: fc.integer({ min: 1, max: 400_000 }),
            drift: fc.constantFrom(0, 0.05),
            outlier: fc.constantFrom(0, 0, 0.01),
            seed: fc.integer(),
          }),
          (scenario) => {
            reach.beginHistory();
            for (const event of checkInts(scenario)) reach.reach(event);
          },
        ),
        // A failure reports its counterexample at once (shrinking 400k-row cases takes minutes);
        // MINNOW_SEED replays it.
        { seed, numRuns: 40, endOnFailure: true },
      );
      reach.assertReached();
    },
    300_000,
  );

  it("direct-mapped int4 keys at the edges of their range, then past it", () => {
    // Keys 0 … 99: a direct array over [−25, 125) (a quarter's slack each way). Then the keys
    // just inside, just outside (widening), and 2^30 away (back to hashing).
    const table = new GroupTable([I32], UNCHARGED);
    const gids = new Int32Array(2048);
    const seen = new Map<number, number>();
    const batches = [
      Array.from({ length: 100 }, (_, k) => k),
      // One past the top alone (the bottom edge stays covered), then one past the bottom.
      [-25, 124, 125, 124, 125],
      [-26, -25, 125],
      [2 ** 30, 0, 125, -26, 2 ** 30, -(2 ** 30)],
    ];
    for (const keys of batches) {
      table.assign([numbers(I32, keys)], keys.length, null, gids);
      keys.forEach((key, row) => {
        if (!seen.has(key)) seen.set(key, seen.size);
        expect(gids[row], `key ${String(key)}`).toBe(seen.get(key));
      });
    }
    expect([...table.keyHistory(0)]).toEqual(["direct", "widened", "direct to hash", "hashed"]);
    table.close();
  });

  it("switches a full direct-mapped composite table (65,536 groups) to hashing, keeping ids", () => {
    const table = new GroupTable([I32, I32], UNCHARGED);
    const gids = new Int32Array(2048);
    const all = Array.from({ length: 65_536 }, (_, t) => [t >>> 8, t & 255] as const);
    const assign = (tuples: ReadonlyArray<readonly [number, number]>): number[] => {
      const out: number[] = [];
      for (let start = 0; start < tuples.length; start += 2048) {
        const part = tuples.slice(start, start + 2048);
        table.assign(
          [
            numbers(
              I32,
              part.map(([a]) => a),
            ),
            numbers(
              I32,
              part.map(([, b]) => b),
            ),
          ],
          part.length,
          null,
          gids,
        );
        out.push(...gids.subarray(0, part.length));
      }
      return out;
    };
    const first = assign(all);
    expect(first).toEqual(Array.from({ length: 65_536 }, (_, t) => t));
    expect(table.directMapped).toBe(true);
    // A 257th value of the first column needs a 17th bit: hash from now on.
    expect(assign([[256, 0]])).toEqual([65_536]);
    expect(table.directMapped).toBe(false);
    expect(assign(all)).toEqual(first);
    expect(table.groupCount).toBe(65_537);
    table.close();
  });
});

describe("byte keys copied and compared four bytes at a time (generated)", () => {
  const BYTE_SEEDS = seedsFor("next-group-table-bytes", [0x62, 0x63]);
  it.each(BYTE_SEEDS)(
    "keys of 0–80 bytes at any source offset match a byte-by-byte model (seed %i)",
    (seed) => {
      // Law: GROUP BY over text is byte equality (C collation); ids first-seen; the emitted key
      // is the first-seen value's bytes (ByteKeys / ByteArena contract). The model compares and
      // keeps bytes one at a time; the table copies and compares them four at a time through
      // DataViews, so tail lengths (0–3 left over) and unaligned offsets are what it must get
      // right. Each batch's bytes start 0–7 bytes into their buffer.
      const reach = createReach("next-group-table-bytes", [
        "tail of 1–3 bytes",
        "unaligned source buffer",
        "keys differing only in the tail",
        "empty key",
        "key over 64 bytes",
      ] as const);
      fc.assert(
        fc.property(fc.integer(), fc.integer({ min: 1, max: 6 }), (inner, batches) => {
          reach.beginHistory();
          const random = mulberry32(inner);
          const table = new GroupTable([TEXT], UNCHARGED);
          const model = new Map<string, number>();
          const first: string[] = [];
          const gids = new Int32Array(2048);
          // A pool of keys sharing long prefixes, so many probes compare equal heads.
          const pool = Array.from({ length: 40 }, () => {
            const length = Math.floor(random() * 81);
            const prefix = "p".repeat(Math.min(length, Math.floor(random() * 4) * 16));
            let key = prefix;
            while (key.length < length)
              key += String.fromCharCode(0x21 + Math.floor(random() * 90));
            return key;
          });
          for (let b = 0; b < batches; b++) {
            const length = 1 + Math.floor(random() * 500);
            const keys = Array.from(
              { length },
              () => pool[Math.floor(random() * pool.length)] ?? "",
            );
            const pad = Math.floor(random() * 8);
            const total = keys.reduce((n, k) => n + k.length, 0);
            const buffer = new Uint8Array(pad + total + 8);
            const bytes = buffer.subarray(pad, pad + total);
            const offsets = new Int32Array(length + 1);
            let at = 0;
            keys.forEach((key, k) => {
              offsets[k] = at;
              for (let c = 0; c < key.length; c++) bytes[at + c] = key.charCodeAt(c);
              at += key.length;
            });
            offsets[length] = at;
            if (pad % 4 !== 0) reach.reach("unaligned source buffer");
            const vector: FlatVector = {
              form: "flat",
              type: TEXT,
              length,
              validity: null,
              data: { layout: "varlen", bytes, offsets },
            };
            gids.fill(-7);
            table.assign([vector], length, null, gids);
            for (let r = 0; r < length; r++) {
              const key = keys[r] ?? "";
              if (key.length % 4 !== 0) reach.reach("tail of 1–3 bytes");
              if (key.length === 0) reach.reach("empty key");
              if (key.length > 64) reach.reach("key over 64 bytes");
              let id = model.get(key);
              if (id === undefined) {
                id = model.size;
                model.set(key, id);
                first.push(key);
              }
              if (gids[r] !== id) expect(gids[r], `row ${String(r)} key ${key}`).toBe(id);
            }
          }
          // Distinct keys that share all but their last 1–3 bytes.
          const seen = [...model.keys()];
          if (
            seen.some((a) =>
              seen.some(
                (b) =>
                  a !== b &&
                  a.length === b.length &&
                  a.length % 4 !== 0 &&
                  a.slice(0, a.length - (a.length % 4)) === b.slice(0, b.length - (b.length % 4)),
              ),
            )
          )
            reach.reach("keys differing only in the tail");
          const out = table.emitKey(0, 0, model.size);
          for (let k = 0; k < model.size; k++) expect(valueAt(out, k)).toBe(first[k]);
          table.close();
        }),
        { seed, numRuns: 60 },
      );
      reach.assertReached();
    },
    180_000,
  );
});

describe("ByteArena.equals four bytes at a time (generated)", () => {
  const ARENA_SEEDS = seedsFor("next-byte-arena", [0x64, 0x65]);
  it.each(ARENA_SEEDS)(
    "equals is byte equality for keys of 0–80 bytes at any offset (seed %i)",
    (seed) => {
      // The table reaches `equals` only after a hash match, so keys differing in one byte rarely
      // get there: this compares it directly with byte equality, the difference (if any) in the
      // head, a word or the 1–3 byte tail.
      const reach = createReach("next-byte-arena", [
        "differ in the tail",
        "differ in a word",
        "equal",
        "unaligned source",
      ] as const);
      fc.assert(
        fc.property(
          fc.array(
            fc.record({
              length: fc.integer({ min: 0, max: 80 }),
              pad: fc.integer({ min: 0, max: 7 }),
              at: fc.integer({ min: -1, max: 80 }),
              seed: fc.integer(),
            }),
            { minLength: 1, maxLength: 30 },
          ),
          (cases) => {
            reach.beginHistory();
            const arena = new ByteArena(new Charged(UNCHARGED));
            cases.forEach(({ length, pad, at, seed: s }, id) => {
              const random = mulberry32(s);
              const key = Uint8Array.from({ length }, () => Math.floor(random() * 256));
              const buffer = new Uint8Array(pad + 2 * length + 8);
              const source = buffer.subarray(pad);
              source.set(key, 0);
              arena.append(id, source, 0, length, new DataView(source.buffer, pad));
              // The probe: the same bytes, or one byte changed at `at` (−1: none).
              const probe = buffer.subarray(pad);
              probe.set(key, length);
              const change = at >= 0 && at < length;
              if (change) probe[length + at] = ((probe[length + at] ?? 0) + 1) & 255;
              if (pad % 4 !== 0) reach.reach("unaligned source");
              if (!change) reach.reach("equal");
              else if (at >= length - (length % 4)) reach.reach("differ in the tail");
              else reach.reach("differ in a word");
              const view = new DataView(probe.buffer, probe.byteOffset, probe.byteLength);
              expect(arena.equals(id, probe, length, 2 * length, view)).toBe(!change);
              expect(arena.equals(id, probe, length, 2 * length)).toBe(!change);
            });
          },
        ),
        { seed, numRuns: 200 },
      );
      reach.assertReached();
    },
    120_000,
  );
});
