/** Node CPU regression property for non-unique hot keys; the core remains browser-only. */
import fc from "fast-check";
import { cpuUsage } from "node:process";
import { expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { KeyDelta } from "../packages/core/src/next/write/key-hash.js";

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)] ?? 0;
for (const seed of seedsFor("next-write-hot-key-cpu", [0xb3b2]))
  it(`does not multiply the legacy sort CPU on generated hot keys (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.uint8Array({ minLength: 1, maxLength: 12 }), async (key) => {
        const delta = new KeyDelta();
        for (let k = 0; k < 16384; k++) delta.add(key, 0, key.length, k + 1, 1, false);
        const stream = async () => {
          let count = 0;
          await delta.forEachSorted(Infinity, () => {
            count++;
          });
          expect(count).toBe(16384);
        };
        const legacy = () => {
          expect(delta.sorted(Infinity)[0]?.entries.length).toBe(16384);
        };
        const cpu = { stream: [] as number[], legacy: [] as number[] };
        try {
          for (let round = -1; round < 5; round++) {
            const order: ReadonlyArray<keyof typeof cpu> =
              round % 2 === 0 ? ["stream", "legacy"] : ["legacy", "stream"];
            for (const label of order) {
              const before = cpuUsage();
              for (let repeat = 0; repeat < 2; repeat++)
                if (label === "stream") await stream();
                else legacy();
              const used = cpuUsage(before);
              if (round >= 0) cpu[label].push(used.user + used.system);
            }
          }
          expect(median(cpu.stream), JSON.stringify(cpu)).toBeLessThan(
            median(cpu.legacy) * 8 + 2000,
          );
        } finally {
          delta.releaseCharge();
        }
      }),
      { seed, numRuns: 3, endOnFailure: true },
    );
  }, 120_000);
