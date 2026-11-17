/** Small ordinal cuts stay resident; ownership survives creator close and drains gated reads. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITER_RESIDENT_REACH,
  WRITER_RESIDENT_DEFER_REACH,
} from "../../testing/reach.js";
import { openOwner, pressure } from "./retirement-test-support.js";
import {
  captureWriterExtents,
  capturedWriterReferences,
  type CapturedWriterExtents,
} from "../storage/writer-extents.js";
import {
  borrowRetirementInventory,
  type InventoryContext,
} from "../storage/retirement-inventory.js";
import { TaskClock } from "../task.js";
import type { ByteBudget, ExtentRef, WrittenExtent } from "../storage/extents.js";

for (const seed of seedsFor("next-writer-resident-input", [0xb94b, 0xb94c]))
  it(
    `retains bounded resident cuts under pressure seed ${String(seed)}`,
    async (context) => {
      const stop = new AbortController();
      let cleanup: (() => Promise<void>) | undefined;
      context.onTestFinished(async () => {
        stop.abort();
        await cleanup?.();
      });
      const reach = createReach("resident writer input", WRITER_RESIDENT_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            prefix: fc.constantFrom(0, 32, 63, 64, 65, 96),
            count: fc.integer({ min: 2, max: 17 }),
            reverse: fc.boolean(),
            discard: fc.boolean(),
          }),
          async ({ prefix, count, reverse, discard }) => {
            stop.signal.throwIfAborted();
            reach.beginHistory();
            const owner = await openOwner(),
              fixed = await pressure(owner, 128 << 10),
              writer = owner.space.beginWrite("resident input source", {
                ownershipBudget: fixed.grant,
              }),
              backing = owner.session.metadata?.();
            if (!backing) throw new Error("Native metadata store missing");
            const originalWrite = backing.write.bind(backing);
            let writes = 0;
            backing.write = async (...args) => {
              writes++;
              await originalWrite(...args);
            };
            let captured: CapturedWriterExtents | undefined,
              releaseGate: (() => void) | undefined,
              closing: Promise<void> | undefined;
            const close = () =>
              (closing ??= (async () => {
                releaseGate?.();
                backing.write = originalWrite;
                try {
                  await captured?.release();
                } finally {
                  try {
                    if (writer.state === "open") await writer.abort();
                  } finally {
                    fixed.close();
                    await owner.close();
                  }
                }
              })());
            cleanup = close;
            try {
              for (let i = 0; i < prefix; i++) await writer.write([Uint8Array.of(i & 255)]);
              const builders = Array.from({ length: count }, () => writer.open(1));
              for (let i = 0; i < count; i++)
                await builders[i]?.append([Uint8Array.of((i + 17) & 255)]);
              const sealed: WrittenExtent[] = [];
              for (let k = 0; k < count; k++) {
                const index = reverse ? count - k - 1 : k,
                  result = await builders[index]?.seal();
                if (!result) throw new Error("Generated nonempty seal missing");
                sealed.push(result);
              }
              if (discard) {
                const first = sealed.shift();
                if (!first) throw new Error("Missing discard");
                await writer.discard(first.extent);
              }
              const oracle = sealed.map((s) => s.extent).sort((a, b) => a.offset - b.offset),
                total = prefix + count,
                tail = ((total - 1) % 64) + 1,
                resident = prefix >= total - tail,
                before = writes,
                work = { signal: stop.signal, clock: new TaskClock(() => 0) };
              captured = await captureWriterExtents(writer, fixed.grant, work, prefix);
              if (resident) {
                expect(writes).toBe(before);
                reach.reach("resident zero journal writes");
              } else {
                expect(writes).toBeGreaterThan(before);
                reach.reach("crossing cut uses disk fallback");
              }
              if (prefix >= 64 && resident) reach.reach("resident suffix after earlier spill");
              if (discard) reach.reach("ordinal holes preserved");
              const inventory = captured.addedInventory;
              if (!inventory) throw new Error("Retained input inventory missing");
              if (resident) {
                expect(() =>
                  borrowRetirementInventory(inventory, owner.space, owner.startup),
                ).toThrow("Foreign");
                reach.reach("input is not retirement authority");
              }
              expect(fixed.grant.allocated).toBe(0);
              fixed.grant.close();
              reach.reach("creator closes after ownership transfer");
              const actual: ExtentRef[] = [];
              for await (const ref of capturedWriterReferences(captured, owner.startup, work))
                actual.push(ref);
              expect(actual).toEqual(oracle);
              reach.reach("exact sealed receipts");
              await writer.abort();
              // Gate an accepted context response after its buffer is copied. The unchanged
              // inventory lifecycle must retain both source/cursor credit until settlement.
              let markEntered!: () => void;
              const entered = new Promise<void>((resolve) => {
                markEntered = resolve;
              });
              const native = inventory as unknown as {
                  context: InventoryContext & { residentBytes?: number };
                  budget: ByteBudget;
                },
                read = native.context.read.bind(native.context),
                gate = new Promise<void>((resolve) => {
                  releaseGate = resolve;
                });
              native.context.read = (...args) =>
                read(...args).then(async (value) => {
                  markEntered();
                  await gate;
                  return value;
                });
              let payloadAtRefund = -1,
                refunds = 0;
              const refund = native.budget.free.bind(native.budget);
              if (resident)
                native.budget.free = (bytes) => {
                  payloadAtRefund = native.context.residentBytes ?? -1;
                  refunds++;
                  refund(bytes);
                };
              const cursor = inventory.cursor({ budget: owner.startup }),
                reading = Promise.resolve(cursor.next(work)).then(
                  () => false,
                  () => true,
                );
              await entered;
              let released = false,
                spaceSettled = false;
              const releasing = inventory.release().then(
                  () => {
                    released = true;
                    return undefined;
                  },
                  (error: unknown) => {
                    released = true;
                    return error;
                  },
                ),
                spaceClosing = owner.space.close().then(
                  () => {
                    spaceSettled = true;
                    return undefined;
                  },
                  (error: unknown) => {
                    spaceSettled = true;
                    return error;
                  },
                );
              await new Promise<void>((resolve) => setImmediate(resolve));
              expect.soft(released).toBe(false);
              expect.soft(spaceSettled).toBe(false);
              releaseGate?.();
              expect(await reading).toBe(true);
              await cursor.close();
              expect(await releasing).toBeUndefined();
              expect(await spaceClosing).toBeUndefined();
              if (resident) {
                expect(payloadAtRefund).toBe(0);
                expect(refunds).toBe(1);
                expect(() => native.context.read(0, undefined, new Uint8Array(0), work)).toThrow(
                  "closed",
                );
                reach.reach("payload cleared before one refund");
              }
              native.budget.free = refund;
              reach.reach("release fences accepted response");
            } finally {
              await close();
            }
            reach.reach("all ownership returned");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [{ prefix: 0, count: 17, reverse: true, discard: false }],
            [{ prefix: 63, count: 17, reverse: false, discard: true }],
            [{ prefix: 64, count: 17, reverse: true, discard: true }],
            [{ prefix: 96, count: 2, reverse: false, discard: false }],
          ],
        },
      );
      reach.assertReached();
    },
    process.env.CI ? 120_000 : 40_000,
  );

for (const seed of seedsFor("next-writer-resident-defer", [0xb94f, 0xb950]))
  it(`refunds a deferred copy before disk fallback seed ${String(seed)}`, async (context) => {
    const stop = new AbortController();
    let cleanup: (() => Promise<void>) | undefined;
    context.onTestFinished(async () => {
      stop.abort();
      await cleanup?.();
    });
    const reach = createReach(
      "resident copy defers without consuming credit",
      WRITER_RESIDENT_DEFER_REACH,
    );
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 17, max: 32 }), async (count) => {
        stop.signal.throwIfAborted();
        reach.beginHistory();
        const owner = await openOwner(),
          writer = owner.space.beginWrite("clock-deferred resident copy"),
          fixed = await pressure(owner, 4096),
          backing = owner.session.metadata?.();
        if (!backing) throw new Error("Missing native metadata store");
        const write = backing.write.bind(backing);
        let writes = 0;
        backing.write = async (...args) => {
          writes++;
          await write(...args);
        };
        let captured: CapturedWriterExtents | undefined, closing: Promise<void> | undefined;
        const close = () =>
          (closing ??= (async () => {
            backing.write = write;
            try {
              await captured?.release();
            } finally {
              try {
                if (writer.state === "open") await writer.abort();
              } finally {
                fixed.close();
                await owner.close();
              }
            }
          })());
        cleanup = close;
        try {
          const expected: ExtentRef[] = [];
          for (let i = 0; i < count; i++)
            expected.push((await writer.write([Uint8Array.of(i)])).extent);
          let reads = 0;
          // Start=0; entry=1; copy entry=2; first chunk=3; next chunk=4 (due).
          // The later task stays at4, so the journal route completes normally.
          const work = { signal: stop.signal, clock: new TaskClock(() => Math.min(reads++, 4)) };
          captured = await captureWriterExtents(writer, fixed.grant, work, 0);
          expect(writes).toBeGreaterThan(0);
          reach.reach("copy deferred to sliced disk route");
          expect(fixed.grant.allocated).toBe(0);
          reach.reach("single admitted owner transferred once");
          const actual: ExtentRef[] = [];
          for await (const ref of capturedWriterReferences(captured, owner.startup, work))
            actual.push(ref);
          expect(actual).toEqual(expected);
          reach.reach("exact suffix after clock deferral");
        } finally {
          await close();
        }
        reach.reach("all bytes released");
      }),
      { seed, numRuns: 4, examples: [[17], [32]] },
    );
    reach.assertReached();
  });
