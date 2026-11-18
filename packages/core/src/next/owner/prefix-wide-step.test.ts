/** A wide row that fits the cap is ordinary read progress, not a reserve-sized value.
 * Decoder/input/pin ownership retires before retry admission. No mutation occurs here. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_PREFIX_WIDE_STEP_REACH,
  NATIVE_PREFIX_WIDE_LIFETIME_REACH,
} from "../../testing/reach.js";
import { releaseIdle } from "../write/budget.js";
import { batch, ints, texts } from "../write/test-support.js";
import { decimal, decimals } from "../exec/kernels/test-support.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { prefixFixture } from "./prefix-images.test-support.js";
for (const seed of seedsFor("next-native-prefix-wide-step", [0xb687, 0xb688]))
  it(`funds complete wide native reads at a fresh boundary seed ${String(seed)}`, async () => {
    const reach = createReach("native wide source step", NATIVE_PREFIX_WIDE_STEP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 14, max: 20 }),
        fc.boolean(),
        fc.constantFrom(64, 128),
        async (mib, published, cap) => {
          reach.beginHistory();
          const width = Math.floor((mib * (1 << 20)) / 3),
            pending = cap === 128 && !published,
            f = await prefixFixture(pending ? 0 : 1, width, published, false, false, cap << 20),
            borrow = vi.spyOn(f.source, "borrow"),
            readers = f.statement.tx.hooks.shared.readers,
            baseReaders = readers.count;
          let preparations = 0;
          f.setPrepare(async () => {
            preparations++;
            if (preparations > 1)
              expect(f.source.held, "unused producer credit must return before admission").toBe(0);
            expect(readers.count, "old prefix pin must retire before admission").toBe(baseReaders);
            expect(f.source.allocated, "decoder allocations must retire before admission").toBe(0);
            expect(f.source.loaned).toBe(0);
          });
          try {
            let id = f.ids[0],
              expected = f.payloads[0];
            if (pending) {
              expected = "雪".repeat(width) + ":0";
              await f.statement
                .table(f.tableId)
                .insert(
                  batch(
                    ints([1]),
                    decimals(decimal(38, 0), [[1n, 0]], "wide", 0),
                    texts([expected]),
                  ),
                  null,
                );
              const current = f.statement.readOwn();
              try {
                id = current.internalTable(f.tableId).rowId(0);
              } finally {
                current.release();
              }
            }
            if (id === undefined) throw new Error("missing source row");
            const pin = f.statement.readOwn();
            let memory = false;
            try {
              const table = pin.internalTable(f.tableId);
              memory = table.segments[Math.floor(table.locator(id) / 65536)]?.kind === "memory";
            } finally {
              pin.release();
            }
            const ref = await f.reader.capture(f.tableId, id, f.stop.signal);
            expect(preparations).toBeGreaterThan(1);
            expect(readers.count).toBe(baseReaders);
            expect(borrow).not.toHaveBeenCalled();
            expect(f.source.allocated).toBe(0);
            expect(f.source.loaned).toBe(0);
            // The source has ended. Fund the complete image+binding-copy envelope atomically
            // before borrowing it; no producer or image is kept across this admission.
            releaseIdle(f.source);
            const wanted = f.root.allocated + 2 * (3 * width + 2) + (256 << 10),
              missing = Math.max(0, wanted - f.root.held),
              [credit] = await f.h.governor.admitGroup({
                label: "wide image observation",
                grants: [
                  {
                    label: "whole image observation progress",
                    minimumBytes: missing,
                    spillable: false,
                  },
                ],
                includesProgress: true,
                signal: f.stop.signal,
              });
            if (!credit) throw new Error("missing observation credit");
            try {
              expect(f.h.governor.transferCredit(credit, f.root, missing)).toBe(true);
            } finally {
              credit.close();
            }
            const lease = await f.images.readRowImages(ref, f.stop.signal);
            try {
              const payload = lease.old?.columns[2];
              if (!payload) throw new Error("missing payload");
              expect(
                valueAt(payload, 0) === expected,
                "exact wide UTF8 payload survives retries",
              ).toBe(true);
            } finally {
              lease.release();
            }
            expect(f.h.governor.peakBytes).toBeLessThanOrEqual(cap << 20);
            reach.reach(memory ? "memory source" : "stored source");
            reach.reach(cap === 64 ? "64MiB" : "128MiB");
            reach.reach("complete ordinary retry");
            reach.reach("pin and decoder retired");
            reach.reach("no reserve loan");
            reach.reach("exact payload");
          } finally {
            borrow.mockRestore();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [20, true, 64],
          [20, false, 128],
          [14, false, 64],
        ],
      },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-native-prefix-wide-lifetime", [0xb689, 0xb68a]))
  it(`fences cancelled wide-read admission without a decoder or pin seed ${String(seed)}`, async () => {
    const reach = createReach("wide prefix admission lifetime", NATIVE_PREFIX_WIDE_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (cancel) => {
        reach.beginHistory();
        const f = await prefixFixture(
            1,
            Math.floor((14 * (1 << 20)) / 3),
            true,
            false,
            false,
            64 << 20,
          ),
          readers = f.statement.tx.hooks.shared.readers,
          base = readers.count;
        let preparations = 0,
          enter: () => void = () => undefined,
          resume: () => void = () => undefined,
          closed = false;
        const entered = new Promise<void>((r) => {
            enter = r;
          }),
          gate = new Promise<void>((r) => {
            resume = r;
          });
        f.setPrepare(async () => {
          preparations++;
          if (preparations === 1) return;
          expect(readers.count).toBe(base);
          expect(f.source.allocated).toBe(0);
          expect(f.source.loaned).toBe(0);
          expect(f.source.held).toBe(0);
          enter();
          await gate;
        });
        try {
          const id = f.ids[0];
          if (id === undefined) throw new Error("missing source row");
          const pending = f.reader.capture(f.tableId, id, f.stop.signal);
          void pending.catch(() => undefined);
          await Promise.race([
            entered,
            pending.then(
              () => {
                throw new Error("missing wider-read admission");
              },
              (error: unknown) => {
                throw error;
              },
            ),
          ]);
          const paid = f.root.allocated;
          if (cancel) f.stop.abort(new Error("cancel wide admission"));
          const closing = f.reader.close().then(() => {
            closed = true;
          });
          for (let i = 0; i < 8; i++) await Promise.resolve();
          expect(closed).toBe(false);
          expect(f.root.allocated).toBe(paid);
          resume();
          await expect(pending).rejects.toBeInstanceOf(Error);
          await closing;
          reach.reach(cancel ? "cancel" : "close");
          reach.reach("no producer pin or credit");
          reach.reach("metadata fenced through admission");
        } finally {
          resume();
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 4, examples: [[true], [false]] },
    );
    reach.assertReached();
  }, 120000);
