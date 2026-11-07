/** Small-page writes must preserve source values, even when out-of-bounds scalar stores
 * would silently disappear. Cases run independently: a throwing wide copy cannot hide a
 * completed, wrong safe-interval or decimal write (B2-p2m review #1).
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  createReach,
  SMALL_PAGE_REPRESENTATION_REACH,
  SMALL_PAGE_NULL_STATES,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import { newDictionaryId, type Vector } from "../vector/vector.js";
import { MemoryExtentSpace } from "./memory-extents.js";
import {
  defined,
  openChunk,
  readChunk,
  sameValue,
  vectorOf,
  type TestValue,
  type VectorOptions,
} from "./test-support.js";
import { GroupWriter } from "./writer.js";

interface Case {
  readonly name: string;
  readonly type: PhysicalType;
  readonly width: 8 | 16;
  readonly options: VectorOptions;
  value(row: number, salt: number): TestValue;
}
const decimal = { kind: "decimal", precision: null, scale: 2 } as const;
const CASES: readonly Case[] = [
  {
    name: "safe interval",
    type: { kind: "interval" },
    width: 16,
    options: { safe: true },
    value: (row, salt) => ({ months: row, days: row % 30, micros: BigInt(row * salt + 1) }),
  },
  {
    name: "wide interval",
    type: { kind: "interval" },
    width: 16,
    options: { safe: false },
    value: (row, salt) => ({
      months: row,
      days: row % 30,
      micros: 2n ** 60n + BigInt(row * salt + 1),
    }),
  },
  {
    name: "safe decimal",
    type: decimal,
    width: 16,
    options: { safe: true, scale: 2 },
    value: (row, salt) => ({ coefficient: BigInt(row * salt + 1), special: 0 }),
  },
  {
    name: "int64 decimal",
    type: decimal,
    width: 16,
    options: { safe: false, scale: 2 },
    value: (row, salt) => ({ coefficient: 2n ** 60n + BigInt(row * salt + 1), special: 0 }),
  },
  {
    name: "int128 decimal",
    type: decimal,
    width: 16,
    options: { tier: "int128", scale: 2 },
    value: (row, salt) => ({ coefficient: 2n ** 80n + BigInt(row * salt + 1), special: 0 }),
  },
  {
    name: "safe int64",
    type: { kind: "int64" },
    width: 8,
    options: { safe: true },
    value: (row, salt) => BigInt(row * salt + 1),
  },
  {
    name: "wide int64",
    type: { kind: "int64" },
    width: 8,
    options: { safe: false },
    value: (row, salt) => 2n ** 60n + BigInt(row * salt + 1),
  },
];

describe.each(
  CASES.flatMap((test) =>
    ["flat", "dictionary"].flatMap((form) =>
      SMALL_PAGE_NULL_STATES.map((mask) => ({
        test,
        form,
        mask,
        name: `${test.name}, ${form}, ${mask}`,
      })),
    ),
  ),
)("small pages: $name", ({ test, form, mask }) => {
  const sparse = mask === "sparse nulls";
  it.each(seedsFor("next-small-page-representations", [0xb2d7, 0xb2d8]))(
    "preserves every source row after completed writes (seed %i)",
    async (seed) => {
      const reach = createReach(`next-small-pages/${test.name}/${form}/${mask}`, [
        ...SMALL_PAGE_REPRESENTATION_REACH,
        mask,
      ]);
      await fc
        .assert(
          fc.asyncProperty(
            fc.constantFrom(16, 32, 48, 63),
            fc.integer({ min: 129, max: 320 }),
            fc.integer({ min: 1, max: 31 }),
            async (capacity, count, salt) => {
              reach.beginHistory();
              const source = Array.from({ length: count }, (_, row) =>
                sparse && (row + salt) % 11 === 0 ? null : test.value(row, salt),
              );
              const flat = vectorOf(test.type, source, test.options);
              const vector: Vector =
                form === "flat"
                  ? flat
                  : {
                      form: "dictionary",
                      type: test.type,
                      length: count,
                      dictionary: flat,
                      dictionaryId: newDictionaryId(),
                      codes: Int32Array.from({ length: count }, (_, row) => row),
                      validity: flat.validity,
                    };
              reach.reach(sparse ? "sparse nulls" : "all valid");
              for (const rawPageBytes of [capacity * test.width, 2048]) {
                const space = new MemoryExtentSpace();
                const writer = new GroupWriter({
                  columns: [{ columnId: 1, type: test.type }],
                  writer: space.beginWrite("independent representation"),
                  rawPageBytes,
                  pageBytes: 128,
                });
                expect(await writer.append([vector], count)).toBe(count);
                const chunk = defined((await writer.finish()).columns[0]);
                reach.reach("write completed");
                const actual = await readChunk(space, chunk);
                reach.reach("ordinary read completed");
                expect(actual.length).toBe(source.length);
                // Deliberately before page-count assertions: a completed silent write must fail value equality.
                for (let row = 0; row < count; row++)
                  if (!sameValue(actual[row] ?? null, source[row] ?? null))
                    expect(
                      actual[row],
                      `source equality after successful write/read: ${test.name}/${form}/${sparse ? "sparse" : "all-valid"}, raw=${String(rawPageBytes)}, row=${String(row)}`,
                    ).toEqual(source[row]);
                reach.reach("source equality");
                const reader = await openChunk(space, chunk);
                try {
                  expect(reader.directory.pages.length).toBeGreaterThan(1);
                  expect(
                    reader.directory.pages.every(
                      (page) => page.rowCount <= Math.floor(rawPageBytes / test.width),
                    ),
                  ).toBe(true);
                } finally {
                  reader.close();
                }
                reach.reach("second flush");
                reach.reach(
                  rawPageBytes / test.width < 64 ? "capacity below 64" : "capacity at least 64",
                );
              }
            },
          ),
          { seed, numRuns: 24 },
        )
        .catch((error: unknown) => {
          const cause = error instanceof Error ? error.cause : undefined;
          const detail = cause instanceof Error ? cause.message : String(cause ?? error);
          const replay = error instanceof Error ? error.message : String(error);
          throw new Error(`${test.name}/${form}/${mask}: ${detail}\n${replay}`, { cause: error });
        });
      reach.assertReached();
      console.log(
        "SMALL_PAGE_REPRESENTATION_REACH",
        JSON.stringify({ seed, case: test.name, form, states: Object.fromEntries(reach.counts()) }),
      );
    },
  );
});
