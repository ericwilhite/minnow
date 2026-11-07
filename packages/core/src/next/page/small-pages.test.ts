/** GroupWriter's raw buffer is a hard capacity (writer.ts ColumnWriter.capacity/room).
 * Density-based page sizing must respect it even when it holds fewer than 64 rows.
 * Every accepted small-page write must round-trip through the ordinary reader.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, SMALL_PAGE_CAPACITY_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import {
  defined,
  openChunk,
  readChunk,
  sameValue,
  writeGroup,
  type TestValue,
} from "./test-support.js";

it.each(seedsFor("next-small-page-capacity", [0xb2d5, 0xb2d6]))(
  "adaptive pages never exceed the allocated row capacity (seed %i)",
  async (seed) => {
    const reach = createReach("next-small-page-capacity", SMALL_PAGE_CAPACITY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(512, 768, 1024, 2048),
        fc.integer({ min: 129, max: 320 }),
        fc.integer({ min: 1, max: 31 }),
        async (rawPageBytes, count, salt) => {
          reach.beginHistory();
          const types: PhysicalType[] = [
            { kind: "interval" },
            { kind: "uuid" },
            { kind: "decimal", precision: null, scale: 2 },
          ];
          const columns = types.map((type) => ({
            type,
            vector: { tier: "int128" as const, scale: 2 },
            values: Array.from({ length: count }, (_, row): TestValue => {
              if ((row + salt) % 11 === 0) return null;
              switch (type.kind) {
                case "interval":
                  return {
                    months: row % 12,
                    days: row % 30,
                    micros: (row % 2 === 0 ? 1n : -1n) * (2n ** 60n + BigInt(row)),
                  };
                case "uuid":
                  return Uint8Array.from({ length: 16 }, (_, k) => (row * salt + k) % 256);
                case "decimal":
                  return { coefficient: 2n ** 80n + BigInt(row * salt), special: 0 };
                default:
                  throw new Error("Fixture type");
              }
            }),
          }));
          const { space, group } = await writeGroup(columns, { rawPageBytes, pageBytes: 128 });
          const capacity = Math.floor(rawPageBytes / 16);
          for (let column = 0; column < columns.length; column++) {
            const chunk = defined(group.columns[column]),
              reader = await openChunk(space, chunk);
            try {
              expect(reader.directory.pages.length).toBeGreaterThan(1);
              expect(reader.directory.pages.every((page) => page.rowCount <= capacity)).toBe(true);
              reach.reach("second flush");
            } finally {
              reader.close();
            }
            const actual = await readChunk(space, chunk),
              source = defined(columns[column]);
            expect(actual.length).toBe(count);
            expect(actual.every((value, row) => sameValue(value, source.values[row] ?? null))).toBe(
              true,
            );
            reach.reach(
              source.type.kind === "uuid"
                ? "fixed16"
                : source.type.kind === "decimal"
                  ? "decimal128"
                  : "interval",
            );
          }
          reach.reach(capacity < 64 ? "capacity below 64" : "capacity at least 64");
        },
      ),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
    console.log(
      "SMALL_PAGE_CAPACITY_REACH",
      JSON.stringify({ seed, states: Object.fromEntries(reach.counts()) }),
    );
  },
);
