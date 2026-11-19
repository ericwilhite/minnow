/** GroupWriter's dictionary fallback must not disable plentiful-memory certified routes. */
import fc from "fast-check";
import { expect, it, onTestFinished } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_DICTIONARY_PREFLIGHT_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { GroupWriter, groupWriterMinimum } from "../page/writer.js";
import { readChunk, sameValue, vectorOf } from "../page/test-support.js";
import { MetadataBudget } from "./budget.js";
import { TEXT } from "./test-support.js";

for (const seed of seedsFor("next-write-dictionary-preflight", [47239, 47240]))
  it(`prepays optional dictionaries or falls back before emitting (seed ${String(seed)})`, async () => {
    const reach = createReach("native dictionary preflight", WRITE_DICTIONARY_PREFLIGHT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 64, max: 4096 }),
        fc.integer({ min: 1, max: 8 }),
        fc.integer({ min: 8, max: 32 }),
        fc.constantFrom("plenty", "blocked", "reclaimed"),
        async (rows, distinct, width, mode) => {
          const pressure = mode !== "plenty";
          reach.beginHistory();
          const encoder = new TextEncoder();
          const values = Array.from({ length: rows }, (_, i) =>
            i % 11 === 0
              ? null
              : encoder.encode(`é🚀:${String(i % distinct)}:${"x".repeat(width)}`),
          );
          const options = {
            columns: [
              {
                columnId: 1,
                type: TEXT,
                valueBytes: values.reduce((n, v) => n + (v?.length ?? 0), 0),
                maxValueBytes: Math.max(...values.map((v) => v?.length ?? 0)),
                dictionaryValueBytes: values.reduce((n, v) => n + (v?.length ?? 0), 0),
                largeValues: 0,
              },
            ],
            maxRows: rows,
          };
          const required = groupWriterMinimum(options),
            governor = new Governor(16 << 20),
            grant = await governor.admit({
              label: "dictionary group",
              minimumBytes: required,
              spillable: true,
            }),
            budget = new MetadataBudget(grant, () => 0, governor.capBytes),
            space = new MemoryExtentSpace(),
            writer = space.beginWrite("dictionary preflight");
          let competitor: Grant | undefined,
            retained: Uint8Array | undefined,
            group: GroupWriter | undefined,
            emitting = false,
            firstAllocation = true,
            closing: Promise<void> | undefined;
          const optional = budget.tryAllocate.bind(budget);
          budget.tryAllocate = (bytes, reserve) => {
            const accepted = optional(bytes, reserve);
            if (accepted) {
              const remaining = grant.held - grant.allocated;
              // Cleanup aid for the reserve-stealing mutant: no new dictionary array
              // exists yet. Refund this rejected charge before its assertion.
              if (remaining < reserve) budget.free(bytes);
              expect(
                remaining,
                "optional allocation consumed required future workspace",
              ).toBeGreaterThanOrEqual(reserve);
              reach.reach("required future workspace retained");
            }
            return accepted;
          };
          const grow = grant.tryGrow.bind(grant);
          grant.tryGrow = (bytes) => {
            expect(emitting, "emit never requests new dictionary credit").toBe(false);
            return grow(bytes);
          };
          const allocate = grant.allocate.bind(grant);
          grant.allocate = (bytes) => {
            if (firstAllocation) {
              firstAllocation = false;
              expect(grant.held).toBeGreaterThanOrEqual(required);
              expect(grant.held - required).toBeLessThanOrEqual(1 << 20);
              if (mode !== "blocked")
                expect(
                  grant.held,
                  "plentiful dictionary headroom precedes buffers",
                ).toBeGreaterThan(required);
              reach.reach("preflight precedes every group buffer");
            }
            return allocate(bytes);
          };
          const close = () =>
            (closing ??= (async () => {
              emitting = false;
              await group?.abort();
              await writer.abort();
              if (retained) expect(retained.at(-1)).toBe(67);
              retained = undefined;
              competitor?.free(competitor.allocated);
              competitor?.close();
              competitor = undefined;
              expect(grant.allocated).toBe(0);
              grant.close();
            })());
          onTestFinished(close);
          try {
            if (mode === "blocked") {
              const free = governor.capBytes - governor.reserveBytes - governor.usedBytes;
              competitor = await governor.admit({
                label: "retained reader",
                minimumBytes: free,
                spillable: false,
              });
              expect(competitor.allocate(free)).toBe(true);
              retained = new Uint8Array(free).fill(67);
              reach.reach("competing live allocation");
            }
            group = new GroupWriter({ ...options, writer, charge: budget });
            if (mode === "reclaimed") {
              const spare = grant.held - required;
              expect(spare).toBeGreaterThan(0);
              grant.release(spare);
              const free = governor.capBytes - governor.reserveBytes - governor.usedBytes;
              competitor = await governor.admit({
                label: "reader after preflight",
                minimumBytes: free,
                spillable: false,
              });
              expect(competitor.allocate(free)).toBe(true);
              retained = new Uint8Array(free).fill(67);
              reach.reach("unused optional prepayment reclaimed");
              reach.reach("competing live allocation");
            }
            const prepaid = grant.held;
            emitting = true;
            await group.append([vectorOf(TEXT, values)], rows);
            const output = await group.finish();
            emitting = false;
            expect(grant.held).toBe(prepaid);
            expect(group.peakWorkingBytes).toBeLessThanOrEqual(required);
            reach.reach("no emit growth or minimum increase");
            const chunk = output.columns[0];
            if (!chunk) throw new Error("Missing dictionary output");
            if (pressure) {
              expect(chunk.statistics.dictionarySize).toBeNull();
              reach.reach("bounded plain fallback");
            } else {
              expect(
                chunk.statistics.dictionarySize,
                "profitable repeated values retain dictionary pages",
              ).toBeGreaterThan(0);
              reach.reach("plentiful dictionary retained");
            }
            const actual = await readChunk(space, chunk);
            expect(actual.length).toBe(values.length);
            expect(actual.every((value, i) => sameValue(value, values[i] ?? null))).toBe(true);
            reach.reach("exact NULL and UTF8 rows");
          } finally {
            await close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 14,
        examples: [
          [89, 8, 8, "plenty"],
          [89, 8, 8, "blocked"],
          [89, 8, 8, "reclaimed"],
          [4096, 4, 32, "plenty"],
          [4096, 4, 32, "blocked"],
          [4096, 4, 32, "reclaimed"],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
