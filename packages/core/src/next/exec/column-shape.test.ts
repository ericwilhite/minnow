/** Shape reuse must preserve the page/vector exactness contract (types.ts and column-target.ts).
 * Compare scalar memos with legacy verified descriptors and independent source byte counts.
 * Histories change residency and selections independently of their endpoint pages.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, SCAN_SHAPE_REACH, OUTPUT_CLASS_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ChunkReader, DecodeFact } from "../page/reader.js";
import { defined, sameValue, valueAt, writeGroup, type TestValue } from "../page/test-support.js";
import type { ByteBudget } from "../access/space.js";
import type { PhysicalType } from "../types.js";
import { isValid } from "../vector/validity.js";
import type { Vector } from "../vector/vector.js";
import { ReturnPool } from "./return-pool.js";
import { Governor } from "../memory/governor.js";
import { ColumnTarget, type DecodeSpec } from "./column-target.js";
import { referenceCapacity } from "./output-class-test-support.js";
import { sourceSizedPages } from "./column-shape-test-support.js";

function countingBudget(): ByteBudget & { used: number } {
  const budget = {
    used: 0,
    allocate(bytes: number) {
      budget.used += bytes;
      return true;
    },
    free(bytes: number) {
      budget.used -= bytes;
    },
  };
  return budget;
}

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
    const reach = createReach("next-column-shape", [...SCAN_SHAPE_REACH, ...OUTPUT_CLASS_REACH]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 300, max: 512 }),
        fc.integer({ min: 1, max: 19 }),
        fc.constantFrom(512, 2048),
        fc.boolean(),
        fc.boolean(),
        async (count, salt, rawPageBytes, sparseEveryLayout, bucketed) => {
          reach.beginHistory();
          reach.reach(bucketed ? "bucketed output" : "exact output");
          reach.reach(rawPageBytes === 512 ? "512 B raw page" : "2 KiB raw page");
          if (sparseEveryLayout) reach.reach("sparse NULLs in every layout");
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
              if (
                column === 11 ||
                ((column % 3 === 0 || sparseEveryLayout) && (row + salt) % 7 === 0)
              )
                return null;
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
              budget = countingBudget(),
              pool = new ReturnPool(new Governor(8 * 1048576), 4 * 1048576);
            const reader = await ChunkReader.open(
              space.view({ cache: false }),
              defined(group.columns[column]),
              { budget },
            );
            let targetCharge = 0;
            const memory = {
              ...(bucketed ? { outputSize: referenceCapacity } : {}),
              reuse: (bytes: number) => pool.take(bytes),
              take: (n: number) => {
                targetCharge += n;
              },
              give: (n: number) => {
                targetCharge -= n;
              },
            };
            const target = new ColumnTarget(source.type, count, memory),
              reference = new ColumnTarget(source.type, count, {
                take: memory.take,
                give: memory.give,
              });
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
                    b = reference.outputBytes(
                      sourceSizedPages(reader, source.values),
                      spec,
                      codes,
                      true,
                    );
                  expect(a).toBe(b);
                  expect(target.growthFor(reader, spec)).toBe(
                    reference.growthFor(sourceSizedPages(reader, source.values), spec),
                  );
                  const actual = target.decode(reader, spec),
                    old = reference.decode(sourceSizedPages(reader, source.values), spec);
                  equal(rows(actual), expected);
                  equal(rows(actual), rows(old));
                  const chargedBefore = targetCharge;
                  const owned = target.decodeOwned(reader, spec, codes),
                    legacy = reference.decodeOwned(
                      sourceSizedPages(reader, source.values),
                      spec,
                      codes,
                    );
                  equal(rows(owned.vector), expected);
                  equal(rows(owned.vector), rows(legacy.vector));
                  expect(targetCharge - chargedBefore).toBe(owned.bytes + legacy.bytes);
                  expect(owned.bytes).toBe(owned.buffers.reduce((sum, b) => sum + b.byteLength, 0));
                  expect(owned.bytes).toBeLessThanOrEqual(
                    target.outputBytes(reader, spec, codes, true, memory.outputSize),
                  );
                  if (owned.bytes > legacy.bytes) reach.reach("charged zero padding");
                  const expectedBytes = legacy.buffers.map((buffer) => {
                    const bytes = new Uint8Array(
                      bucketed ? referenceCapacity(buffer.byteLength) : buffer.byteLength,
                    );
                    bytes.set(new Uint8Array(buffer));
                    return bytes;
                  });
                  const ids = pool.issue(0, owned.buffers);
                  const received = structuredClone(owned.buffers, { transfer: [...owned.buffers] });
                  // A consumer can modify its exclusive storage before relinquishing it.
                  for (const buffer of received) new Uint8Array(buffer).fill(0xa5);
                  const returned = structuredClone(received, { transfer: [...received] });
                  expect(received.every((b) => b.byteLength === 0)).toBe(true);
                  pool.release(
                    0,
                    returned.map((buffer, i) => ({ id: ids[i] ?? -1, buffer })),
                  );
                  expect(owned.buffers.every((buffer) => buffer.byteLength === 0)).toBe(true);
                  memory.give(owned.bytes);
                  const hitsBefore = pool.hits;
                  const again = target.decodeOwned(reader, spec, codes);
                  equal(rows(again.vector), expected);
                  if (expectedBytes.some((bytes) => bytes.byteLength > 0)) {
                    expect(pool.hits).toBeGreaterThan(hitsBefore);
                    reach.reach("recycled native bytes");
                  }
                  const actualBytes = again.buffers.map((buffer) => new Uint8Array(buffer));
                  if (
                    actualBytes.length !== expectedBytes.length ||
                    !actualBytes.every((bytes, i) => {
                      const referenceBytes = expectedBytes[i];
                      if (referenceBytes === undefined) return false;
                      return (
                        bytes.byteLength === referenceBytes.byteLength &&
                        bytes.every((value, byte) => value === referenceBytes[byte])
                      );
                    })
                  )
                    expect(actualBytes).toEqual(expectedBytes);
                  memory.give(again.bytes + legacy.bytes);
                  if (actual.form === "dictionary") reach.reach("dictionary");
                }
              }
              reach.reach("owned and borrowed");
              reach.reach("gather same endpoints");
              if (column === 4) {
                expect(reader.isSafe(0, 1)).toBe(true);
                const lastPage = defined(reader.directory.pages.at(-1));
                const hasWide = source.values
                  .slice(count - lastPage.rowCount)
                  .some(
                    (value) => typeof value === "bigint" && value > BigInt(Number.MAX_SAFE_INTEGER),
                  );
                expect(reader.isSafe(indices.length - 1, indices.length)).toBe(!hasWide);
                if (hasWide) reach.reach("mixed safe integers");
              }
              if (column === 8) reach.reach("plain varlen");
              if (column === 11) reach.reach("all null");
              if (column === 1 && !sparseEveryLayout) {
                expect(source.values.every((value) => value !== null)).toBe(true);
                reach.reach("all valid");
              }
              if (column === 10) reach.reach("int128 mixed scales");
            } finally {
              pool.close();
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
