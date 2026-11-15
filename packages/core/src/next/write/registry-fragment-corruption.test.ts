/** Bad temporary image links fail before delivery, with bounded traversal and complete cleanup. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, REGISTRY_FRAGMENT_CORRUPTION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { SpillService } from "../spill/contract.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import {
  RegistryFragments,
  REGISTRY_FRAGMENT_BYTES,
  type RegistryPayload,
} from "./registry-fragments.js";

it.each(seedsFor("next-registry-fragment-corruption", [0xb5dd]))(
  "refuses corrupt image chains with bounded reads seed %i",
  async (seed) => {
    const reach = createReach("native fragment corruption", REGISTRY_FRAGMENT_CORRUPTION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("checksum", "header", "cycle", "truncated", "oversized", "fractional"),
        fc.integer({ min: 2, max: 4 }),
        async (kind, count) => {
          reach.beginHistory();
          const backing = new MemorySpillService();
          let charged = 0,
            reads = 0,
            armed = false;
          const spill: SpillService = {
            get bytesInUse() {
              return backing.bytesInUse;
            },
            sweep: () => backing.sweep(),
            create: async (queryId) => {
              const file = await backing.create(queryId);
              return {
                queryId,
                get length() {
                  return file.length;
                },
                append: (page) => file.append(page),
                read: (ref) => file.read(ref),
                readInto: async (ref, into) => {
                  reads++;
                  await file.readInto(ref, into);
                  if (armed && (kind === "header" || kind === "cycle")) {
                    armed = false;
                    const view = new DataView(into.buffer, into.byteOffset, into.byteLength);
                    if (kind === "header") view.setUint32(0, 0, true);
                    else {
                      view.setFloat64(8, ref.offset, true);
                      view.setUint32(16, ref.length, true);
                      view.setUint32(20, ref.checksum, true);
                    }
                  }
                },
                close: () => file.close(),
              };
            },
          };
          const fragments = new RegistryFragments(
            {
              allocate: (bytes) => {
                if (charged + bytes > REGISTRY_FRAGMENT_BYTES) return false;
                charged += bytes;
                return true;
              },
              free: (bytes) => {
                charged -= bytes;
              },
            },
            spill,
            1,
            new AbortController().signal,
            () => undefined,
          );
          try {
            let payload: RegistryPayload = await fragments.append(count * 65504 + 19, (into) => {
              into.fill(123);
            });
            const tail = payload.tail;
            if (!tail) throw new Error("Missing nonempty test payload");
            if (kind === "checksum")
              payload = {
                ...payload,
                tail: { ...tail, checksum: (tail.checksum ^ 1) >>> 0 },
              };
            if (kind === "truncated") payload = { ...payload, length: payload.length + 65504 };
            if (kind === "oversized") payload = { ...payload, tail: { ...tail, length: 65537 } };
            if (kind === "fractional") payload = { ...payload, tail: { ...tail, checksum: 0.5 } };
            armed = true;
            let failure: unknown;
            try {
              await fragments.readInto(payload, new Uint8Array(payload.length));
            } catch (error) {
              failure = error;
            }
            expect(failure).toMatchObject({ name: "SpillCorruptionError" });
            if (kind === "checksum") expect((failure as Error).message).toContain("verification");
            if (kind === "oversized" || kind === "fractional") expect(reads).toBe(0);
            else expect(reads).toBeLessThanOrEqual(count + 1);
            reach.reach(kind);
            reach.reach("bounded reads");
          } finally {
            await fragments.close();
          }
          expect(charged).toBe(0);
          expect(backing.bytesInUse).toBe(0);
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          ["checksum", 2],
          ["header", 3],
          ["cycle", 4],
          ["truncated", 2],
          ["oversized", 2],
          ["fractional", 2],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
