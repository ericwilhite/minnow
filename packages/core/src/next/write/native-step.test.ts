/** NativeWriteStepBoundary/WriteCredits contract: account the actual joint owner once,
 * preserve prepaid primary progress, and reject a stale witness before root allocation. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
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
        fc.boolean(),
        async (units, active, privateProgress, donated) => {
          reach.beginHistory();
          const governor = new Governor(4 << 20),
            credits = await WriteCredits.open(governor, minimum),
            owner = {},
            stop = new AbortController();
          const pressure: { buffer: Uint8Array | null; injected: boolean } = {
            buffer: null,
            injected: false,
          };
          let nextHeadroom = 0;
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
                // The engine's native protocol (write/engine.ts): a native step never merges, so it
                // protects and keeps no merge minimum; an active merge keeps its own floor.
                protectedBytes: () => credits.protectedBytes(0),
                stabilize: async () => {
                  if (pressure.buffer) {
                    credits.metadata.free(pressure.buffer.byteLength);
                    pressure.buffer = null;
                    steps?.changed();
                  }
                  await credits.prepareNativeMinimum(nextHeadroom, stop.signal, privateProgress);
                  credits.reclaimNativeStep(0);
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
            nextHeadroom = headroom;
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
              activeMerge = active ? 2 * minimum.merge + units * 1024 : 0,
              base = rowFloor + metadata + headroom + minimum.commits,
              expected = base + activeMerge;
            if (donated) {
              credits.reclaimIdleForecasts();
              // An ordinary step's prospective minimum still keeps the merge minimum.
              expect(
                credits.protectedBytes(minimum.merge, headroom),
                "the prospective minimum includes donated WAL credit",
              ).toBe(base + Math.max(minimum.merge, activeMerge));
              reach.reach("native funding restored after idle donation");
            }
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
            // Active: the merge's credit moved to its own grant. Idle: the forecast is reclaimed.
            expect(credits.merge.held).toBe(0);
            primary.check();
            expect(rows[0]).toBe(61);
            expect(meta[0]).toBe(73);
            reach.reach("primary floor survives reclaim");
            reach.reach("metadata and commit buffers once");
            reach.reach(active ? "active merge once" : "future merge forecast reclaimed");
            const newer = headroom + units * 4096;
            nextHeadroom = newer;
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
            // Keep the native metadata step live and inject a non-waiting growth
            // refusal, forcing an unchanged-floor WAL transfer on the cold pass.
            credits.bindWorkingMinimum(() => ({ metadata: credits.metadata.held }));
            const immediate = vi.spyOn(credits.rows, "tryGrow").mockReturnValue(false);
            try {
              await credits.growTo(credits.rows, credits.rows.held + units * 64, stop.signal);
            } finally {
              immediate.mockRestore();
              credits.bindWorkingMinimum(() => ({}));
            }
            expect(
              boundary.invalidated.aborted,
              "transferred credit invalidates even an unchanged protected floor",
            ).toBe(true);
            reach.reach("unchanged-floor transfer invalidates witness");
            boundary.release();
            boundary = await steps.prepare(owner, units * 1024, stop.signal, {
              includesProgress: privateProgress,
            });
            boundary.check();
            expect(boundary.protectedWriteBytes).toBe(expected + newer - headroom);
            nextHeadroom = headroom;
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
          [1, false, false, false],
          [2, true, true, true],
          [3, false, true, true],
          [4, true, false, false],
        ],
      },
    );
    reach.assertReached();
  });
