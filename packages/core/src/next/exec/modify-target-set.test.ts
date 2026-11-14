import type { SpillService } from "../spill/contract.js";
/** Repeat identity is exact after eviction; all retained state fits a charged fixed envelope. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  MODIFY_TARGET_SET_REACH,
  MODIFY_TARGET_CANCEL_REACH,
} from "../../testing/reach.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ModifyTargetSet, MODIFY_TARGET_SET_BYTES } from "./modify-target-set.js";
it.each(seedsFor("next-modify-target-set", [0xb5c6]))(
  "keeps exact spilled target identities (seed %i)",
  async (seed) => {
    const reach = createReach("spilled target membership", MODIFY_TARGET_SET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.tuple(
            fc.integer({ min: 0, max: 3 }),
            fc.integer({ min: 0, max: 31 }),
            fc.integer({ min: 0, max: 1023 }),
            fc.constantFrom(0, 2 ** 40, 2 ** 52),
          ),
          { minLength: 40, maxLength: 180 },
        ),
        async (cells) => {
          reach.beginHistory();
          const spill = new MemorySpillService();
          let charged = 0,
            peak = 0;
          const tracker = new ModifyTargetSet(
            {
              allocate: (n) => {
                if (charged + n > MODIFY_TARGET_SET_BYTES) return false;
                charged += n;
                peak = Math.max(peak, charged);
                return true;
              },
              free: (n) => {
                charged -= n;
              },
            },
            spill,
            1,
            new AbortController().signal,
          );
          const seen = new Set<string>();
          const rows = [
            ...Array.from({ length: 24 }, (_, block) => [1, block, 1023, 0] as const),
            ...Array.from({ length: 24 }, (_, block) => [1, block, 1023, 2 ** 52] as const),
            ...cells,
            ...cells,
            ...Array.from({ length: 24 }, (_, block) => [1, block, 1023, 0] as const),
          ];
          try {
            for (const [table, block, offset, base] of rows) {
              const row = base + block * 1024 + offset,
                key = `${String(table)}:${String(row)}`;
              expect(await tracker.testAndSet(table, row)).toBe(seen.has(key));
              seen.add(key);
            }
            expect(spill.bytesInUse).toBeGreaterThan(0);
            expect(peak).toBe(MODIFY_TARGET_SET_BYTES);
            reach.reach("evicted leaf");
            reach.reach("repeated target");
            reach.reach("separate tables");
            reach.reach("wide row identity");
            reach.reach("fixed charged envelope");
          } finally {
            await tracker.close();
          }
          expect(charged).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("complete cleanup");
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-modify-target-cancel", [0xb5c9]))(
  "fences cancelled repeat spill I/O (seed %i)",
  async (seed) => {
    const reach = createReach("repeat spill cancellation", MODIFY_TARGET_CANCEL_REACH);
    await fc.assert(
      fc.asyncProperty(fc.constantFrom("create", "append", "read"), async (phase) => {
        reach.beginHistory();
        const backing = new MemorySpillService(),
          controller = new AbortController();
        let charged = 0,
          armed = false;
        let release!: () => void, entered!: () => void;
        const gate = new Promise<void>((resolve) => {
            release = resolve;
          }),
          started = new Promise<void>((resolve) => {
            entered = resolve;
          });
        const pause = async (name: string) => {
          if (armed && name === phase) {
            armed = false;
            entered();
            await gate;
          }
        };
        const spill: SpillService = {
          get bytesInUse() {
            return backing.bytesInUse;
          },
          sweep: () => backing.sweep(),
          create: async (queryId) => {
            await pause("create");
            const file = await backing.create(queryId);
            return {
              get queryId() {
                return file.queryId;
              },
              get length() {
                return file.length;
              },
              close: () => file.close(),
              read: (ref) => file.read(ref),
              readInto: async (ref, target) => {
                await pause("read");
                await file.readInto(ref, target);
              },
              append: async (bytes) => {
                await pause("append");
                return file.append(bytes);
              },
            };
          },
        };
        const tracker = new ModifyTargetSet(
          {
            allocate: (n) => {
              charged += n;
              return true;
            },
            free: (n) => {
              charged -= n;
            },
          },
          spill,
          1,
          controller.signal,
        );
        const initial = phase === "read" ? 20 : 8;
        for (let k = 0; k < initial; k++) await tracker.testAndSet(1, k * 1024);
        armed = true;
        const pending = tracker.testAndSet(1, phase === "read" ? 0 : 8192);
        await started;
        const reason = new Error("cancel pending repeat spill");
        controller.abort(reason);
        const closing = tracker.close();
        expect(charged).toBe(MODIFY_TARGET_SET_BYTES);
        release();
        await expect(pending).rejects.toBe(reason);
        await closing;
        await tracker.close();
        expect(charged).toBe(0);
        expect(backing.bytesInUse).toBe(0);
        reach.reach(phase);
        reach.reach("borrow fenced");
        reach.reach("idempotent cleanup");
      }),
      { seed, numRuns: 6, examples: [["create"], ["append"], ["read"]] },
    );
    reach.assertReached();
  },
  60000,
);
