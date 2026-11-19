/** Governor rules2/4/7: surrender dormant forecasts and admit the whole remaining step. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_WRITE_GROWTH_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { yieldTask } from "../task.js";
import { WriteCredits } from "./credits.js";

const MiB = 1 << 20;
for (const seed of seedsFor("next-native-write-growth", [0xb7a5, 0xb7a6]))
  it(
    "waits once for net new native credit and remains cancellable (seed " + String(seed) + ")",
    async () => {
      const reach = createReach("native write growth", NATIVE_WRITE_GROWTH_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("rows" as const, "metadata" as const),
          fc.boolean(),
          fc.integer({ min: 1, max: 3 }),
          async (kind, cancel, idleMiB) => {
            reach.beginHistory();
            const governor = new Governor(32 * MiB);
            const credits = await WriteCredits.open(governor, {
              rows: 4 * MiB,
              metadata: MiB,
              commits: MiB / 4,
              merge: MiB / 4,
            });
            credits.rows.onReclaimRequest(() => credits.reclaimRows());
            credits.metadata.onReclaimRequest(() => credits.reclaimMetadata());
            const target = kind === "rows" ? credits.rows : credits.metadata;
            const liveBytes = 64 << 10,
              delta = MiB / 4;
            let paidLive = 0;
            let live: Uint8Array | undefined,
              head: Uint8Array | undefined,
              tail: Uint8Array | undefined;
            let holder: Grant | undefined;
            const otherBuffers = new Map<Grant, Uint8Array>();
            let pending: Promise<unknown> | undefined;
            let restore: () => void = () => undefined;
            const stop = new AbortController(),
              reason = new Error("cancel native growth");
            try {
              expect(target.allocate(liveBytes)).toBe(true);
              paidLive = liveBytes;
              live = new Uint8Array(liveBytes).fill(3);
              await credits.prepareMetadata(MiB);
              // Rules2/4/7 make other accounts' forecasts revocable. Give them
              // real buffers so this unchanged requested step actually needs
              // external credit; never count a future estimate as live input.
              for (const account of [
                credits.rows,
                credits.metadata,
                credits.commits,
                credits.merge,
              ]) {
                if (account === target) continue;
                const bytes = account.held;
                expect(account.allocate(bytes)).toBe(true);
                otherBuffers.set(account, new Uint8Array(bytes).fill(41));
              }
              reach.reach("other native accounts own real buffers");
              const floor = kind === "rows" ? credits.minimum.rows : liveBytes + MiB;
              const idle = idleMiB * MiB,
                wanted = floor + idle + delta;
              expect(target.tryGrow(floor + idle - target.held)).toBe(true);
              const spare = governor.capBytes - governor.reserveBytes - governor.usedBytes;
              holder = await governor.admit({
                label: "live competing read",
                minimumBytes: spare,
                spillable: false,
              });
              expect(holder.allocate(spare)).toBe(true);
              head = new Uint8Array(delta).fill(7);
              tail = new Uint8Array(spare - delta).fill(7);
              reach.reach("real competing buffers");
              let entered!: () => void;
              const ready = new Promise<void>((resolve) => {
                entered = resolve;
              });
              const requests: number[] = [];
              const admit = governor.admitGroup.bind(governor);
              const observed = vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
                requests.push(options.grants.reduce((sum, part) => sum + part.minimumBytes, 0));
                // A broken immediate-credit loop must fail a work bound, never exhaust V8
                // while starving the timer that could cancel the operation.
                if (requests.length > 2) throw new Error("Native growth recycled its own credit");
                entered();
                return admit(options);
              });
              restore = () => observed.mockRestore();
              let settled = false;
              pending = credits.growTo(target, wanted, stop.signal).then(
                () => {
                  settled = true;
                  return null;
                },
                (error: unknown) => {
                  settled = true;
                  return error;
                },
              );
              await Promise.race([
                ready,
                pending.then(() => {
                  throw new Error("Growth completed before competing credit was released");
                }),
              ]);
              expect(requests).toEqual([wanted - liveBytes]);
              expect(target.held).toBe(liveBytes);
              expect(target.allocated).toBe(liveBytes);
              for (const buffer of otherBuffers.values()) expect(buffer.at(-1)).toBe(41);
              reach.reach("unused working floors surrendered");
              reach.reach("complete deficit after surrender");
              await yieldTask();
              expect(settled).toBe(false);
              expect(live.at(-1)).toBe(3);
              reach.reach("task runs while growth waits");
              if (cancel) stop.abort(reason);
              else {
                expect(head.at(-1)).toBe(7);
                head = undefined;
                holder.free(delta);
                holder.release(delta);
              }
              expect(await pending).toBe(cancel ? reason : null);
              if (!cancel) expect(target.held).toBeGreaterThanOrEqual(wanted);
              expect(requests).toHaveLength(1);
              expect(target.loaned).toBe(0);
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              reach.reach(kind);
              reach.reach(
                cancel ? "cancellation without competitor release" : "net new credit completes",
              );
            } finally {
              stop.abort(reason);
              await pending;
              restore();
              if (tail !== undefined) expect(tail.at(-1)).toBe(7);
              // eslint-disable-next-line no-useless-assignment -- drop owned buffers before refund
              head = tail = undefined;
              holder?.free(holder.allocated);
              holder?.close();
              // eslint-disable-next-line no-useless-assignment -- drop the live buffer before refund
              live = undefined;
              target.free(paidLive);
              for (const [account, buffer] of otherBuffers) {
                const bytes = buffer.byteLength;
                expect(buffer.at(-1)).toBe(41);
                otherBuffers.delete(account);
                account.free(bytes);
              }
              credits.close();
            }
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero final charge");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            ["rows", false, 1],
            ["rows", true, 3],
            ["metadata", false, 2],
            ["metadata", true, 3],
          ],
          endOnFailure: true,
        },
      );
      reach.assertReached();
    },
  );
