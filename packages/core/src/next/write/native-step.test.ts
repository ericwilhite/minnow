/** NativeWriteStepBoundary/WriteCredits contract: account the actual joint owner once,
 * preserve prepaid primary progress, and reject a stale witness before root allocation. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_WRITE_STEP_CREDIT_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { WriteCredits, WRITE_ROW_STEP_BYTES, type PreparedWriteCredit } from "./credits.js";
import { NativeWriteSteps, NATIVE_WRITE_STEP_BYTES } from "./native-step.js";
import type { NativeWriteStepBoundary } from "../access/native-write-step.js";
const KiB = 1024,
  minimum = { rows: 64 * KiB, metadata: 64 * KiB, commits: 64 * KiB, merge: 64 * KiB };
for (const seed of seedsFor("next-native-write-owner-credit", [0xb715, 0xb716]))
  it(`counts native safe-step accounts once seed ${String(seed)}`, async () => {
    const reach = createReach("native write-owner credit", NATIVE_WRITE_STEP_CREDIT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 16 }),
        fc.boolean(),
        fc.boolean(),
        async (units, active, privateProgress) => {
          reach.beginHistory();
          const governor = new Governor(4 << 20),
            credits = await WriteCredits.open(governor, minimum),
            owner = {},
            stop = new AbortController();
          const pressure: { buffer: Uint8Array | null; injected: boolean } = {
            buffer: null,
            injected: false,
          };
          let steps: NativeWriteSteps | null = null,
            primary: PreparedWriteCredit | null = null,
            boundary: NativeWriteStepBoundary | null = null,
            merge: Awaited<ReturnType<WriteCredits["acquireMerge"]>> | null = null;
          try {
            const denied = {
                allocate: () => false,
                free: () => {
                  throw Error("unfunded controller freed memory");
                },
              },
              scope = {
                governor,
                check: (s: object) => {
                  expect(s).toBe(owner);
                },
                protectedBytes: () => credits.protectedBytes(),
                stabilize: async () => {
                  if (pressure.buffer) {
                    credits.metadata.free(pressure.buffer.byteLength);
                    pressure.buffer = null;
                    steps?.changed();
                  }
                  credits.reclaimNativeStep();
                  if (!pressure.injected) {
                    pressure.injected = true;
                    // Change lands between native preparation and witness publication.
                    // A fresh preparation retires it, without replaying SQL.
                    queueMicrotask(() => {
                      expect(credits.metadata.allocate(units * 64)).toBe(true);
                      pressure.buffer = new Uint8Array(units * 64);
                      pressure.buffer[0] = 97;
                      steps?.changed();
                    });
                  }
                },
              };
            expect(() => new NativeWriteSteps({ ...scope, charge: denied })).toThrow(
              "must be admitted",
            );
            reach.reach("control charged before retention");
            steps = new NativeWriteSteps({ ...scope, charge: credits.rows });
            const controller = steps;
            credits.onNativeStepChange((growth) => controller.changed(growth));
            const payload = 32 * KiB + units * 4096,
              progress = minimum.rows + units * 1024,
              metadata = units * 1024,
              headroom = minimum.metadata + units * 2048;
            expect(credits.rows.tryGrow(payload + progress + minimum.rows)).toBe(true);
            expect(credits.rows.allocate(payload)).toBe(true);
            const rows = new Uint8Array(payload);
            rows[0] = 61;
            primary = credits.protectRows(progress);
            await credits.prepareMetadata(headroom);
            if (credits.metadata.held < metadata + headroom)
              await credits.growTo(credits.metadata, metadata + headroom);
            expect(credits.metadata.allocate(metadata)).toBe(true);
            const meta = new Uint8Array(metadata);
            meta[0] = 73;
            await credits.prepareMerge(4 * minimum.merge + units * 1024);
            if (active) {
              merge = await credits.acquireMerge(
                "active safe-step merge",
                2 * minimum.merge + units * 1024,
              );
              expect(merge.allocate(merge.held)).toBe(true);
            }
            const rowFloor = payload + NATIVE_WRITE_STEP_BYTES + WRITE_ROW_STEP_BYTES + progress,
              mergeFloor = active ? 2 * minimum.merge + units * 1024 : minimum.merge,
              expected = rowFloor + metadata + headroom + minimum.commits + mergeFloor;
            boundary = await steps.prepare(owner, units * 1024, stop.signal, {
              includesProgress: privateProgress,
            });
            expect(
              pressure.buffer,
              "handoff pressure is restabilized before witness publication",
            ).toBeNull();
            reach.reach("handoff changes restabilized");
            expect(boundary.protectedWriteBytes).toBe(expected);
            expect(credits.rows.held).toBe(rowFloor);
            expect(credits.metadata.held).toBe(metadata + headroom);
            expect(credits.merge.held).toBe(active ? 0 : minimum.merge);
            primary.check();
            expect(rows[0]).toBe(61);
            expect(meta[0]).toBe(73);
            reach.reach("primary floor survives reclaim");
            reach.reach("metadata and commit buffers once");
            reach.reach(active ? "active merge once" : "future merge forecast reclaimed");
            const newer = headroom + units * 4096;
            await credits.prepareMetadata(newer, stop.signal);
            expect(boundary.invalidated.aborted).toBe(true);
            expect(() => boundary?.check()).toThrow("footprint changed");
            reach.reach("stale witness rejected before allocation");
            boundary.release();
            boundary = null;
            boundary = await steps.prepare(owner, units * 1024, stop.signal, {
              includesProgress: privateProgress,
            });
            boundary.check();
            expect(boundary.protectedWriteBytes).toBe(expected + newer - headroom);
            await credits.prepareMetadata(headroom, stop.signal);
            expect(boundary.invalidated.aborted).toBe(true);
            expect(() => boundary?.check()).toThrow("footprint changed");
            reach.reach("decrease invalidates for reclaim");
            primary.check();
            reach.reach("fresh boundary after invalidation");
            boundary.release();
            boundary = null;
            credits.rows.free(payload);
            credits.metadata.free(metadata);
          } finally {
            boundary?.release();
            if (pressure.buffer) {
              credits.metadata.free(pressure.buffer.byteLength);
              pressure.buffer = null;
            }
            primary?.release();
            if (merge) {
              merge.free(merge.allocated);
              credits.releaseMerge(merge);
            }
            await steps?.close();
            credits.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [1, false, false],
          [2, true, true],
          [3, false, true],
          [4, true, false],
        ],
      },
    );
    reach.assertReached();
  });
