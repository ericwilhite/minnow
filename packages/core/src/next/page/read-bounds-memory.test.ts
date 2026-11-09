/** Lead's interim legacy interval limit: emitting fewer rows does not bound chunk() decode. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { Governor, MemoryAdmissionError } from "../memory/governor.js";
import { INT32_MAX, type PhysicalType } from "../types.js";
import { boundSource } from "./read-bounds.js";

it.each(seedsFor("next-legacy-memory-minimum", [0xc41d, 0xc41e]))(
  "conservatively sizes the legacy interval without probing its unbounded decoder (seed %i)",
  async (seed) => {
    const reach = createReach("legacy memory minimum", [
      "empty",
      "fixed",
      "variable",
      "limit-prefix",
      "fits",
      "over-cap",
      "no-probe",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(0, 1, 2048, 4096),
        fc.integer({ min: 1, max: 8 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (rows, columns, variable, prefix, largeCap) => {
          reach.beginHistory();
          let touched = false;
          const kinds: PhysicalType[] = Array.from({ length: columns }, () =>
            variable ? { kind: "text" } : { kind: "int32", bits: 32 },
          );
          const view = {
            pages: {
              peek() {
                touched = true;
                throw new Error("sizing must not probe a memory value");
              },
              read() {
                touched = true;
                throw new Error("sizing must not read a memory value");
              },
            },
            table: () => ({
              columns: kinds.map((_, id) => ({ id })),
              segments: [{ kind: "memory" as const, rowCount: rows, liveRowCount: rows }],
            }),
          };
          const gov = new Governor(largeCap ? 2 ** 47 : 16 * 1024 * 1024),
            source = {
              id: 1,
              tableId: 1,
              columns: kinds.map((_, id) => id),
              types: kinds,
              windowBytes: 0,
              stagingBytes: 4096,
              rows: prefix ? 1 : null,
            };
          let bound: Awaited<ReturnType<typeof boundSource>> | null = null;
          try {
            bound = await boundSource(view, source, gov, new AbortController().signal, 4096);
          } catch (error) {
            expect(error).toBeInstanceOf(MemoryAdmissionError);
            if (!(error instanceof MemoryAdmissionError)) throw error;
            expect(error.label).toBe("read-step/legacy-memory-interval");
            expect(error.bytes).toBeGreaterThan(error.capacity);
            reach.reach("over-cap");
          }
          if (bound !== null) {
            expect(Number.isSafeInteger(bound.bytes)).toBe(true);
            expect(bound.bytes).toBeLessThanOrEqual(gov.capBytes);
            if (rows > 0) {
              expect(bound.bytes).toBeGreaterThanOrEqual(columns * 64 * 1024 * 1024);
              if (variable)
                expect(
                  bound.bytes,
                  "legacy minimum must count full logical values, not one blob",
                ).toBeGreaterThanOrEqual(8 * Math.min(2048, rows) * columns * INT32_MAX);
            } else {
              expect(bound.bytes).toBe(0);
              reach.reach("empty");
            }
            reach.reach("fits");
          }
          expect(touched).toBe(false);
          expect(gov.usedBytes).toBe(0);
          reach.reach("no-probe");
          reach.reach(variable ? "variable" : "fixed");
          if (prefix) reach.reach("limit-prefix");
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [0, 1, true, true, false],
          [2048, 1, true, true, false],
          [2048, 1, false, false, true],
          [2048, 1, true, true, true],
        ],
      },
    );
    reach.assertReached();
  },
);
