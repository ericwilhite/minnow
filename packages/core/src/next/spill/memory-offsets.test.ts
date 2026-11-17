/** Scratch lookup must be exact and sublinear as bounded registries spill more pages. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  MEMORY_SPILL_OFFSET_REACH,
  MEMORY_SPILL_NATIVE_OFFSET_REACH,
} from "../../testing/reach.js";
import { SpillCorruptionError } from "./contract.js";
import { findSpillOffset, MemorySpillService, MemorySpillFile } from "./memory-spill.js";
for (const seed of seedsFor("next-memory-spill-offsets", [0xb645, 0xb646]))
  it(`finds exact spilled offsets with logarithmic probes ${String(seed)}`, () => {
    const reach = createReach("memory spill offset probes", MEMORY_SPILL_OFFSET_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 65536 }),
        fc.integer({ min: 1, max: 4096 }),
        fc.nat(65536),
        fc.constantFrom("present", "outside", "hole"),
        (count, stride, position, kind) => {
          reach.beginHistory();
          const offsets = Array.from({ length: count }, (_, i) => i * stride),
            missing = kind !== "present",
            hole = kind === "hole" && count > 0 && stride > 1,
            wanted = hole
              ? (position % count) * stride + 1
              : missing
                ? count * stride + 1
                : count === 0
                  ? 0
                  : (position % count) * stride;
          let probes = 0;
          const observed = new Proxy(offsets, {
            get(target, key, receiver) {
              if (typeof key === "string" && /^\d+$/.test(key)) probes++;
              return Reflect.get(target, key, receiver) as unknown;
            },
          });
          expect(findSpillOffset(observed, wanted)).toBe(offsets.indexOf(wanted));
          expect(probes, "logarithmic scratch metadata probes").toBeLessThanOrEqual(
            Math.ceil(Math.log2(count + 1)),
          );
          reach.reach(count === 0 ? "empty" : count > 1024 ? "many pages" : "few pages");
          reach.reach(missing ? "absent" : "present");
          if (hole) reach.reach("between pages");
          reach.reach("exact offset");
        },
      ),
      {
        seed,
        numRuns: 30,
        examples: [
          [0, 1, 0, "present"],
          [1, 8, 0, "present"],
          [65536, 264, 32768, "present"],
          [65536, 2080, 65535, "outside"],
          [1024, 264, 512, "hole"],
        ],
      },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-memory-spill-native-offsets", [0xb647, 0xb648]))
  it(`uses bounded lookup for actual native scratch pages ${String(seed)}`, async () => {
    const reach = createReach("native memory spill offsets", MEMORY_SPILL_NATIVE_OFFSET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 48 }),
        fc.nat(65536),
        fc.boolean(),
        async (count, pick, corrupt) => {
          reach.beginHistory();
          const spill = new MemorySpillService(),
            file = await spill.create(1),
            refs = [],
            pages = [];
          try {
            for (let i = 0; i < count; i++) {
              const bytes = Uint8Array.from({ length: 1 + (i % 17) }, (_, k) => (i + k) % 256);
              pages.push(bytes);
              refs.push(await file.append(bytes));
            }
            const index = pick % count,
              ref = refs[index],
              bytes = pages[index];
            if (!ref || !bytes) throw new Error("Missing scratch fixture");
            if (corrupt) {
              if (!(file instanceof MemorySpillFile))
                throw new Error("Missing native scratch file");
              file.corrupt(ref, 0);
            }
            const old = vi.spyOn(Array.prototype, "indexOf").mockImplementation(() => {
              throw new Error("linear scratch offset scan");
            });
            let pending: Promise<Uint8Array>;
            try {
              pending = file.read(ref);
            } finally {
              old.mockRestore();
            }
            if (corrupt) await expect(pending).rejects.toBeInstanceOf(SpillCorruptionError);
            else expect(await pending).toEqual(bytes);
            reach.reach(corrupt ? "checksum" : "payload");
            reach.reach(count > 16 ? "many pages" : "few pages");
          } finally {
            await file.close();
            expect(spill.bytesInUse).toBe(0);
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [48, 20, false],
          [1, 0, true],
        ],
      },
    );
    reach.assertReached();
  });
