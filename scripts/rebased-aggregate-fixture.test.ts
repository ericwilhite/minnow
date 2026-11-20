/** Copied diagnostic fixtures cannot carry facts bound to old source addresses.
 * Authority: MAGS1's full-source identity contract; absence preserves scan fallback.
 * These are codec/ref fixtures, not a native publication or throughput campaign.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, GROUP_AGGREGATE_REBASE_REACH } from "../packages/core/src/testing/reach.js";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import type { ColumnChunk, ExtentRef } from "../packages/core/src/next/format/manifest.js";
import {
  decodeGroupAggregate,
  tryEncodeGroupAggregate,
} from "../packages/core/src/next/format/group-aggregate.js";
import { xxHash32 } from "../packages/core/src/next/hash/xxhash32.js";
import { rebaseExtents } from "./browser-perf/page/export-stream.js";

for (const seed of seedsFor("aggregate-fixture-rebase", [0xb2f4, 0xb2f5]))
  it(`omits source-bound facts when copying a source seed ${String(seed)}`, () => {
    const reach = createReach("A1 copied diagnostic bindings", GROUP_AGGREGATE_REBASE_REACH);
    fc.assert(
      fc.property(
        fc.array(fc.integer({ min: -10000, max: 10000 }), { minLength: 1, maxLength: 128 }),
        fc.boolean(),
        fc.integer({ min: 1, max: 99 }),
        (input, nullable, salt) => {
          reach.beginHistory();
          const values = input.filter((_, row) => !nullable || row % 7 !== 0);
          let held = 0;
          const budget = {
            allocate(bytes: number) {
              held += bytes;
              return true;
            },
            free(bytes: number) {
              held -= bytes;
              expect(held).toBeGreaterThanOrEqual(0);
            },
          };
          for (const sum of [false, true]) {
            const data = { offset: salt * 65536, length: 4096 },
              directory = { offset: salt * 65536 + 16384, length: 4096 },
              record = { offset: salt * 65536 + 32768, length: 8192 };
            const source: ColumnChunk = {
              columnId: salt,
              type: { kind: "int32", bits: 32 },
              data: { extent: data, offset: 0, length: 1024 },
              directory: { extent: directory, offset: 0, length: 128, checksum: salt },
              statistics: {
                nullCount: input.length - values.length,
                min: null,
                max: null,
                dictionarySize: null,
              },
              distinctSketch: null,
              aggregateSummary: null,
            };
            const expected = {
              nonNullCount: values.length,
              sum: sum
                ? {
                    coefficient: values.reduce((total, n) => total + BigInt(n), 0n),
                    coefficientScale: 0,
                    resultScale: 0,
                    nanCount: 0,
                    positiveInfinityCount: 0,
                    negativeInfinityCount: 0,
                  }
                : null,
            };
            const encoded = tryEncodeGroupAggregate(expected, input.length, source, budget);
            if (encoded === null) throw new Error("Missing bounded fixture summary");
            try {
              const attached: ColumnChunk = {
                ...source,
                aggregateSummary: {
                  extent: record,
                  offset: 0,
                  length: encoded.bytes.length,
                  checksum: xxHash32(encoded.bytes),
                },
              };
              for (const present of [false, true]) {
                const original = present ? attached : source,
                  before = structuredClone(original);
                for (const shape of [
                  "data moved",
                  "directory moved",
                  "source length changed",
                  "record moved",
                  "identity unchanged",
                  "no-op mapping",
                ] as const) {
                  const moved = new Map<number, ExtentRef>();
                  if (shape !== "no-op mapping") {
                    const extent =
                      shape === "data moved" || shape === "identity unchanged"
                        ? data
                        : shape === "directory moved" || shape === "source length changed"
                          ? directory
                          : record;
                    moved.set(extent.offset, {
                      ...extent,
                      length: extent.length + (shape === "source length changed" ? 4096 : 0),
                      offset:
                        extent.offset +
                        (shape === "identity unchanged" || shape === "source length changed"
                          ? 0
                          : 1048576),
                    });
                  }
                  const [copied] = rebaseExtents([original], moved);
                  if (copied === undefined) throw new Error("Copy lost a column");
                  if (
                    !present ||
                    shape === "data moved" ||
                    shape === "directory moved" ||
                    shape === "source length changed"
                  )
                    expect(
                      copied.aggregateSummary,
                      "copied source must omit its bound summary",
                    ).toBeNull();
                  else {
                    expect(copied.aggregateSummary).not.toBeNull();
                    const parsed = decodeGroupAggregate(
                      encoded.bytes,
                      input.length,
                      copied,
                      budget,
                    );
                    try {
                      expect(parsed.value).toEqual(expected);
                    } finally {
                      budget.free(parsed.charge);
                    }
                    reach.reach("kept record validates");
                  }
                  expect(original).toEqual(before);
                  expect(copied.statistics).toEqual(original.statistics);
                  expect(copied.data.extent).toEqual(moved.get(data.offset) ?? data);
                  expect(copied.directory.extent).toEqual(moved.get(directory.offset) ?? directory);
                  reach.reach(shape);
                }
                reach.reach(present ? "present summary" : "absent summary");
                reach.reach("source unchanged");
              }
              reach.reach(sum ? "sum record" : "count record");
            } finally {
              budget.free(encoded.charge);
            }
          }
          expect(held).toBe(0);
          reach.reach("zero charges");
        },
      ),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
  });
