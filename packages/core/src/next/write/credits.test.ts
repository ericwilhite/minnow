/** Native write steps keep their minimum while another admitted execution retains its credit. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { seedsFor } from "../../testing/seeds.js";
import { WriteCredits } from "./credits.js";
import { createReach, WRITE_CREDIT_STATES } from "../../testing/reach.js";
const reach = createReach("native write minimum", WRITE_CREDIT_STATES);

const KiB = 1024;
const minimum = {
  rows: 64 * KiB,
  metadata: 64 * KiB,
  commits: 64 * KiB,
  merge: 64 * KiB,
};
for (const seed of seedsFor("next-write-native-credit", [0xb3c1]))
  it(`funds native steps and returns each holding once (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 16 * KiB }), async (bytes) => {
        reach.beginHistory();
        const governor = new Governor(4 * 1024 * KiB);
        const credits = await WriteCredits.open(governor, minimum);
        reach.reach("atomic owner admitted");
        let movedIntoMerge = 0;
        const transfer = governor.transferCredit.bind(governor);
        governor.transferCredit = (source, target, bytes) => {
          const moved = transfer(source, target, bytes);
          if (moved && source === credits.rows && target.label === "merge test")
            movedIntoMerge += bytes;
          return moved;
        };
        let holder: Awaited<ReturnType<Governor["admit"]>> | undefined;
        try {
          expect(credits.rows.allocate(bytes)).toBe(true);
          const row = new Uint8Array(bytes).fill(7);
          expect(credits.rows.tryGrow(1024 * KiB)).toBe(true);
          const spare = governor.capBytes - governor.reserveBytes - governor.usedBytes;
          holder = await governor.admit({
            label: "another complete execution",
            minimumBytes: spare,
            spillable: false,
          });
          expect(holder.allocate(spare)).toBe(true);
          const retained = new Uint8Array(spare).fill(9);
          const active = await credits.acquireMerge("merge test", minimum.merge);
          try {
            expect(active.allocate(minimum.merge)).toBe(true);
            const merge = new Uint8Array(minimum.merge).fill(3);
            expect(credits.merge.held).toBe(0);
            expect(credits.rows.allocated).toBe(bytes);
            expect(row[0]).toBe(7);
            expect(retained[0]).toBe(9);
            expect(merge[0]).toBe(3);
            reach.reach("merge while protected holder retained");
            expect(movedIntoMerge).toBeGreaterThan(0);
            reach.reach("idle row credit transferred");
            active.free(minimum.merge);
          } finally {
            credits.releaseMerge(active);
          }
          expect(credits.merge.held).toBe(minimum.merge);
          reach.reach("merge credit reused");
          // Metadata uses the same native accounts: fitting preparations stay synchronous.
          expect(credits.prepareMetadata(bytes)).toBeUndefined();
          expect(credits.metadata.allocate(bytes)).toBe(true);
          const metadata = new Uint8Array(bytes).fill(4);
          expect(metadata[0]).toBe(4);
          credits.metadata.free(bytes);
          reach.reach("metadata step retained and returned");
          credits.rows.free(row.byteLength);
        } finally {
          holder?.close();
          credits.close();
          credits.close();
        }
        expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
        expect(governor.usedBytes).toBe(0);
        reach.reach("all accounts returned once");
      }),
      { seed, numRuns: 40 },
    );
  });

for (const seed of seedsFor("next-write-native-credit-cancel", [0xb3c2]))
  it(`cancels queued growth before closing its inputs (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.tuple(fc.integer({ min: 1, max: 16 * KiB }), fc.boolean()),
        async ([bytes, local]) => {
          reach.beginHistory();
          const governor = new Governor(4 * 1024 * KiB);
          const credits = await WriteCredits.open(governor, minimum);
          expect(credits.rows.allocate(credits.rows.held)).toBe(true);
          const row = new Uint8Array(credits.rows.allocated).fill(7);
          const spare = governor.capBytes - governor.reserveBytes - governor.usedBytes;
          const holder = await governor.admit({
            label: "protected reader",
            minimumBytes: spare,
            spillable: false,
          });
          expect(holder.allocate(spare)).toBe(true);
          const retained = new Uint8Array(spare).fill(9);
          let settled = false;
          const reason = new Error("write close cancelled growth");
          const controller = new AbortController();
          const pending = credits
            .growTo(
              credits.metadata,
              credits.metadata.held + bytes,
              local ? controller.signal : undefined,
            )
            .then(
              () => {
                settled = true;
                return null;
              },
              (error: unknown) => {
                settled = true;
                return error;
              },
            );
          try {
            for (let turn = 0; turn < 8; turn++) await Promise.resolve();
            expect(settled).toBe(false);
            reach.reach("growth waits on real retained bytes");
            expect(row[0]).toBe(7);
            expect(retained[0]).toBe(9);
            if (local) controller.abort(reason);
            else credits.cancel(reason);
            expect(await pending).toBe(reason);
            reach.reach(local ? "operation cancellation" : "owner cancellation");
            if (local) expect(credits.prepareMerge(minimum.merge)).toBeUndefined();
          } finally {
            credits.cancel(reason);
            await pending;
            credits.rows.free(row.byteLength);
            holder.close();
            credits.close();
          }
          expect(governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
  });

it("reaches native write credit states", () => reach.assertReached());
