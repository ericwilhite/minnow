/** Shape reuse must preserve the page/vector exactness contract (types.ts and column-target.ts).
 * Compare scalar memos with uncached verified descriptors, and both decoders with source rows.
 * Histories change residency and selections independently of their endpoint pages.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, SCAN_SHAPE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ChunkReader, DecodeFact } from "../page/reader.js";
import { defined, sameValue, valueAt, writeGroup, type TestValue } from "../page/test-support.js";
import { countingBudget } from "../storage/space-test-support.js";
import type { PhysicalType } from "../types.js";
import { isValid } from "../vector/validity.js";
import type { Vector } from "../vector/vector.js";
import { ColumnTarget, type DecodeSpec } from "./column-target.js";
import type { ChunkPages } from "./scan.js";

function rows(vector: Vector): TestValue[] {
  if (vector.form !== "flat" && vector.form !== "dictionary") throw new Error("Unexpected shape");
  return Array.from({ length: vector.length }, (_, row) => {
    if (!isValid(vector.validity, row)) return null;
    return vector.form === "flat"
      ? valueAt(vector, row)
      : valueAt(vector.dictionary, vector.codes[row] ?? 0);
  });
}

/** Fast byte/value equality on success; retain Vitest's full diff on a mismatch. */
function equal(actual: readonly TestValue[], expected: readonly TestValue[]): void {
  if (
    actual.length !== expected.length ||
    !actual.every((value, row) => sameValue(value, expected[row] ?? null))
  )
    expect(actual).toEqual(expected);
}

/** Bind private-field methods to the reader, with the optional memo capability hidden. */
function uncached(reader: ChunkReader): ChunkPages {
  return new Proxy(reader, {
    get(target, key) {
      if (key === "decodeFacts") return undefined;
      const value: unknown = Reflect.get(target, key, target);
      if (typeof value !== "function") return value;
      const bound: unknown = value.bind(target);
      return bound;
    },
  });
}

function facts(reader: ChunkReader, from: number, to: number): void {
  const [first, end] = reader.pagesInRange(from, to);
  const expected =
    (reader.isDictionaryRange(from, to) ? DecodeFact.dictionary : 0) |
    (reader.isSafe(first, end) ? DecodeFact.safeInt64 : 0) |
    (reader.isAllValid(from, to) ? DecodeFact.allValid : 0);
  expect(reader.decodeFacts(from, to)).toBe(expected);
}

it.each(seedsFor("next-column-shape", [0xb2d1, 0xb2d2]))(
  "shape reuse preserves decoded output and charges (seed %i)",
  async (seed) => {
    const reach = createReach("next-column-shape", SCAN_SHAPE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 300, max: 512 }),
        fc.integer({ min: 1, max: 19 }),
        fc.constantFrom(512, 2048),
        async (count, salt, rawPageBytes) => {
          reach.beginHistory();
          reach.reach(rawPageBytes === 512 ? "512 B raw page" : "2 KiB raw page");
          const encoder = new TextEncoder();
          const types: PhysicalType[] = [
            { kind: "bool" },
            { kind: "int32", bits: 32 },
            { kind: "float32" },
            { kind: "float64" },
            { kind: "int64" },
            { kind: "interval" },
            { kind: "uuid" },
            { kind: "text" },
            { kind: "bytes" },
            { kind: "decimal", precision: 18, scale: 2 },
            { kind: "decimal", precision: null, scale: null },
            { kind: "int64" },
          ];
          const columns = types.map((type, column) => ({
            type,
            vector: { scale: 2, tier: column === 10 ? ("int128" as const) : ("int64" as const) },
            values: Array.from({ length: count }, (_, row): TestValue => {
              if (column === 11 || (column % 3 === 0 && (row + salt) % 7 === 0)) return null;
              const integer =
                BigInt(row < 256 ? row * salt : 2 ** 53) + (row < 256 ? 0n : BigInt(row));
              switch (type.kind) {
                case "bool":
                  return row % 2 === 0;
                case "int32":
                  return row * salt;
                case "float32":
                  return Math.fround(row * 1.25);
                case "float64":
                  return row % 5 === 0 ? -0 : row * 1.25;
                case "int64":
                  return integer;
                case "interval":
                  return { months: row % 12, days: row % 30, micros: integer };
                case "uuid":
                  return Uint8Array.from({ length: 16 }, (_, k) => (row + k + salt) % 256);
                case "text":
                  return encoder.encode(`v${String(row % 3)}`);
                case "bytes":
                  return Uint8Array.from(
                    { length: 8 + ((row * salt) % 53) },
                    (_, k) => (row + k) % 256,
                  );
                case "decimal":
                  return {
                    coefficient: column === 10 ? 2n ** 80n + BigInt(row) : integer,
                    special: 0,
                    ...(column === 10 ? { scale: row % 5 } : {}),
                  };
                default:
                  throw new Error("Fixture type");
              }
            }),
          }));
          const { space, group } = await writeGroup(columns, {
            rawPageBytes,
            inlineBytes: 0,
          });
          for (let column = 0; column < columns.length; column++) {
            const source = defined(columns[column]),
              budget = countingBudget(Infinity);
            const reader = await ChunkReader.open(
              space.view({ cache: false }),
              defined(group.columns[column]),
              { budget },
            );
            let targetCharge = 0;
            const memory = {
              take(n: number) {
                targetCharge += n;
              },
              give(n: number) {
                targetCharge -= n;
              },
            };
            const target = new ColumnTarget(source.type, count, memory),
              reference = new ColumnTarget(source.type, count, memory);
            try {
              const indices = reader.directory.pages.map((_, page) => page);
              expect(indices.length).toBeGreaterThan(0);
              facts(reader, 0, count); // unloaded pages cannot prove safe/all-valid
              await reader.load(indices);
              await reader.loadValues(0, count);
              facts(reader, 0, count);
              facts(reader, 1, count - 1); // same endpoint pages, different exact span
              reach.reach("same page span");
              const first = defined(reader.directory.pages[0]);
              facts(reader, 0, first.rowCount); // different endpoint pages must recompute
              reach.reach("narrower page span");
              const gap = Math.floor(indices.length / 2);
              reader.release([gap]);
              facts(reader, 0, count);
              reach.reach("release gap");
              if (gap > 0 && gap < indices.length - 1) {
                const selected = { rows: Uint16Array.of(0, count - 1), count: 2 };
                const sparse = target.decodeOwned(reader, selected, false);
                equal(rows(sparse.vector), [
                  source.values[0] ?? null,
                  source.values[count - 1] ?? null,
                ]);
                memory.give(sparse.bytes);
                reach.reach("gather across unloaded gap");
              }
              await reader.load([gap]);
              await reader.loadValues(0, count);
              facts(reader, 0, count);
              reach.reach("reload gap");
              const positions = [
                Uint16Array.of(0, 1, count - 1),
                Uint16Array.of(0, count - 2, count - 1),
              ];
              const specs: DecodeSpec[] = [
                { from: 0, to: count },
                { from: 1, to: Math.min(count, first.rowCount) },
                { from: Math.floor(count / 2), to: count },
                ...positions.map((rows) => ({ rows, count: rows.length })),
              ];
              for (const spec of specs) {
                const expected =
                  "from" in spec
                    ? source.values.slice(spec.from, spec.to)
                    : Array.from(
                        spec.rows.subarray(0, spec.count),
                        (row) => source.values[row] ?? null,
                      );
                for (const codes of [true, false]) {
                  const a = target.outputBytes(reader, spec, codes, true),
                    b = reference.outputBytes(uncached(reader), spec, codes, true);
                  expect(a).toBe(b);
                  expect(target.growthFor(reader, spec)).toBe(
                    reference.growthFor(uncached(reader), spec),
                  );
                  const actual = target.decode(reader, spec),
                    old = reference.decode(uncached(reader), spec);
                  equal(rows(actual), expected);
                  equal(rows(actual), rows(old));
                  const owned = target.decodeOwned(reader, spec, codes),
                    legacy = reference.decodeOwned(uncached(reader), spec, codes);
                  equal(rows(owned.vector), expected);
                  equal(rows(owned.vector), rows(legacy.vector));
                  expect(owned.bytes).toBe(legacy.bytes);
                  expect(owned.bytes).toBeLessThanOrEqual(a);
                  memory.give(owned.bytes + legacy.bytes);
                  if (actual.form === "dictionary") reach.reach("dictionary");
                }
              }
              reach.reach("owned and borrowed");
              reach.reach("gather same endpoints");
              if (column === 4) {
                expect(reader.isSafe(0, 1)).toBe(true);
                expect(reader.isSafe(indices.length - 1, indices.length)).toBe(false);
                reach.reach("mixed safe integers");
              }
              if (column === 8) reach.reach("plain varlen");
              if (column === 11) reach.reach("all null");
              if (column === 1) reach.reach("all valid");
              if (column === 10) reach.reach("int128 mixed scales");
            } finally {
              target.close();
              reference.close();
              reader.close();
            }
            expect(targetCharge).toBe(0);
            expect(budget.used).toBe(0);
          }
        },
      ),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
    console.log(
      "SCAN_SHAPE_REACH",
      JSON.stringify({ seed, states: Object.fromEntries(reach.counts()) }),
    );
  },
);
