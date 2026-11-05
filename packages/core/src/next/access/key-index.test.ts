import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { encodeEqualityKey, type EqualityKeyPart } from "../keys/key-encoding.js";
import type { PhysicalType } from "../types.js";
import {
  comparisonRange,
  IndexCorruptionError,
  inKeyRange,
  prefixRange,
  resolveVersions,
  type IndexEntry,
} from "./key-index.js";
import { pendingSeq, readBound, statementBound } from "./snapshot.js";

const SEEDS = seedsFor("next-access-key-range", [0x6b72, 0x6e67]);
const INT: PhysicalType = { kind: "int32", bits: 32 };
const TEXT: PhysicalType = { kind: "text" };
const utf8 = (text: string) => new TextEncoder().encode(text);
const part = (type: PhysicalType, value: number | string): EqualityKeyPart => ({
  type,
  value: typeof value === "string" ? utf8(value) : value,
});
const key = (...parts: EqualityKeyPart[]) => encodeEqualityKey(parts);

/** SQL order of the values (C collation: UTF-8 bytes for text). */
function compareValues(left: number | string, right: number | string): number {
  if (typeof left === "number" && typeof right === "number") return Math.sign(left - right);
  return Math.sign(Buffer.compare(utf8(String(left)), utf8(String(right))));
}

const CMP = ["=", "<", "<=", ">", ">="] as const;
type Cmp = (typeof CMP)[number];
const holds = (cmp: Cmp, order: number) =>
  cmp === "="
    ? order === 0
    : cmp === "<"
      ? order < 0
      : cmp === "<="
        ? order <= 0
        : cmp === ">"
          ? order > 0
          : order >= 0;

describe("key ranges", () => {
  it("encodes a composite key as its components back to back", () => {
    const whole = key(part(INT, 7), part(TEXT, "b"), part(INT, -1));
    const pieces = [key(part(INT, 7)), key(part(TEXT, "b")), key(part(INT, -1))];
    expect(Buffer.concat(pieces).equals(Buffer.from(whole))).toBe(true);
  });

  it("separates a prefix from keys that extend it", () => {
    const prefix = key(part(INT, 7), part(TEXT, "b"));
    const range = comparisonRange(key(part(INT, 7)), ">", key(part(TEXT, "b")));
    expect(inKeyRange(range, key(part(INT, 7), part(TEXT, "b"), part(INT, 99)))).toBe(false);
    expect(inKeyRange(range, key(part(INT, 7), part(TEXT, "ba"), part(INT, 0)))).toBe(true);
    expect(inKeyRange(range, key(part(INT, 8), part(TEXT, "a"), part(INT, 0)))).toBe(false);
    expect(inKeyRange(prefixRange(prefix), key(part(INT, 7), part(TEXT, "b"), part(INT, 99)))).toBe(
      true,
    );
    expect(inKeyRange({ lower: null, upper: null }, new Uint8Array())).toBe(true);
  });

  // The law: for an index on (k1 int, k2 text, k3 int), `k1 = a AND k2 cmp b` and `k1 cmp a` as
  // byte ranges select exactly the tuples the SQL predicate selects (C collation).
  it.each(SEEDS)("byte ranges select what SQL comparisons select (seed %i)", (seed) => {
    const int = fc.integer({ min: -3, max: 3 });
    const text = fc.constantFrom("", "a", "b", "ba", "b\u0000", "é", "\u{1F600}", "￿");
    const tuple = fc.tuple(int, text, int);
    fc.assert(
      fc.property(
        fc.constantFrom(...CMP),
        fc.boolean(),
        tuple,
        tuple,
        (cmp, onSecond, probe, row) => {
          const rowKey = key(part(INT, row[0]), part(TEXT, row[1]), part(INT, row[2]));
          const range = onSecond
            ? comparisonRange(key(part(INT, probe[0])), cmp, key(part(TEXT, probe[1])))
            : comparisonRange(new Uint8Array(), cmp, key(part(INT, probe[0])));
          const expected = onSecond
            ? row[0] === probe[0] && holds(cmp, compareValues(row[1], probe[1]))
            : holds(cmp, compareValues(row[0], probe[0]));
          expect(inKeyRange(range, rowKey)).toBe(expected);
        },
      ),
      { seed, numRuns: 3000 },
    );
  });
});

describe("version resolution", () => {
  const put = (rowId: number, seq = 0): IndexEntry => ({ rowId, seq, tombstone: false });
  const tomb = (rowId: number, seq = 0): IndexEntry => ({ rowId, seq, tombstone: true });

  it("hides a put behind a visible tombstone only", () => {
    const layers = [[tomb(1, 5), put(2, 5)], [put(1)]];
    expect(resolveVersions(layers, readBound(4), true)).toEqual([1]);
    expect(resolveVersions(layers, readBound(5), true)).toEqual([2]);
  });

  it("shows pending changes to later statements only", () => {
    const layers = [[tomb(1, pendingSeq(2)), put(9, pendingSeq(2))], [put(1)]];
    expect(resolveVersions(layers, readBound(100), true)).toEqual([1]);
    expect(resolveVersions(layers, statementBound(2), true)).toEqual([1]);
    expect(resolveVersions(layers, statementBound(3), true)).toEqual([9]);
  });

  it("returns every live row of a non-unique key in row-id order", () => {
    const layers = [[put(7, 3), tomb(4, 2)], [put(4), put(5)], [put(2)]];
    expect(resolveVersions(layers, readBound(3), false)).toEqual([2, 5, 7]);
  });

  it("stops at the newest live put of a unique key, and verify catches a second", () => {
    const layers = [[put(8, 1)], [put(3)]];
    expect(resolveVersions(layers, readBound(1), true)).toEqual([8]);
    expect(() => resolveVersions(layers, readBound(1), true, true)).toThrow(IndexCorruptionError);
  });
});
