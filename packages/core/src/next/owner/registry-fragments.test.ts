/** Root image payloads spill without a resident fragment list or whole-value encode copy. */
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  createReach,
  REGISTRY_FRAGMENT_REACH,
  REGISTRY_FRAGMENT_IO_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { SpillService } from "../spill/contract.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { RegistryFragments, REGISTRY_FRAGMENT_BYTES } from "./registry-fragments.js";

class Budget {
  used = 0;
  allocate(n: number): boolean {
    if (this.used + n > REGISTRY_FRAGMENT_BYTES) return false;
    this.used += n;
    return true;
  }
  free(n: number): void {
    this.used -= n;
    expect(this.used).toBeGreaterThanOrEqual(0);
  }
}
const value = (position: number, salt: number) => (position * 31 + salt) & 255;

it.each(seedsFor("next-registry-fragments", [0xb5db]))(
  "streams exact image fragments at fixed admission seed %i",
  async (seed) => {
    const reach = createReach("native image fragments", REGISTRY_FRAGMENT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 11 }),
        fc.integer({ min: 0, max: 65503 }),
        fc.integer({ min: 0, max: 255 }),
        async (chunks, tail, salt) => {
          reach.beginHistory();
          const budget = new Budget(),
            backing = new MemorySpillService();
          let opened = 0,
            clock = 0,
            largestWrite = 0,
            largestRead = 0,
            largestCopy = 0;
          const spill: SpillService = {
            get bytesInUse() {
              return backing.bytesInUse;
            },
            sweep: () => backing.sweep(),
            create: async (queryId) => {
              opened++;
              const file = await backing.create(queryId);
              return {
                queryId,
                get length() {
                  return file.length;
                },
                append: (page) => {
                  largestWrite = Math.max(largestWrite, page.length);
                  return file.append(page);
                },
                read: (ref) => file.read(ref),
                readInto: (ref, into) => {
                  largestRead = Math.max(largestRead, into.length);
                  return file.readInto(ref, into);
                },
                close: () => file.close(),
              };
            },
          };
          const fragments = new RegistryFragments(
            budget,
            spill,
            1,
            new AbortController().signal,
            () => {
              clock++;
            },
          );
          const length = chunks * 65504 + tail;
          const fill = (extra: number) => (into: Uint8Array, position: number) => {
            largestCopy = Math.max(largestCopy, into.length);
            for (let i = 0; i < into.length; i++) into[i] = value(position + i, salt + extra);
          };
          try {
            const a = await fragments.append(length, fill(0));
            if (length === 0) {
              expect(a.tail).toBeNull();
              expect(opened).toBe(0);
              reach.reach("empty");
            }
            const b = await fragments.append(Math.floor(length / 2) + 3, fill(1));
            for (const [payload, extra] of [
              [a, 0],
              [b, 1],
              [a, 0],
            ] as const) {
              const output = new Uint8Array(payload.length);
              await fragments.readInto(payload, output);
              for (let i = 0; i < output.length; i++)
                if (output[i] !== value(i, salt + extra))
                  throw new Error(`Wrong fragment byte at ${String(i)}`);
            }
            expect(largestWrite).toBeLessThanOrEqual(65536);
            expect(largestRead).toBeLessThanOrEqual(65536);
            expect(largestCopy).toBeLessThanOrEqual(65504);
            expect(clock).toBeGreaterThanOrEqual(
              2 * (Math.ceil(a.length / 65504) + Math.ceil(b.length / 65504)),
            );
            expect(opened).toBe(1);
            expect(budget.used).toBe(REGISTRY_FRAGMENT_BYTES);
            reach.reach("bounded copy and I/O");
            reach.reach("task clock per fragment");
            reach.reach("one file across payloads");
            reach.reach("replay without ref list");
            if (length > REGISTRY_FRAGMENT_BYTES) reach.reach("payload larger than scratch");
            if (length === 65504) reach.reach("exact fragment");
            if (length > 65504 && length % 65504 !== 0) reach.reach("ragged fragment");
          } finally {
            await fragments.close();
          }
          await fragments.close();
          expect(budget.used).toBe(0);
          expect(backing.bytesInUse).toBe(0);
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [0, 0, 0],
          [1, 0, 255],
          [2, 19, 3],
          [11, 65503, 17],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-fragment-io", [0xb5dc]))(
  "keeps image fragment scratch through cancellation fences seed %i",
  async (seed) => {
    const reach = createReach("native image fragment I/O", REGISTRY_FRAGMENT_IO_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("create", "append", "read", "fill", "clock"),
        fc.boolean(),
        async (phase, cancel) => {
          reach.beginHistory();
          const budget = new Budget(),
            backing = new MemorySpillService(),
            controller = new AbortController();
          let armed = false,
            release!: () => void,
            entered!: () => void;
          const gate = new Promise<void>((resolve) => {
            release = resolve;
          });
          const enteredGate = new Promise<void>((resolve) => {
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
                queryId,
                get length() {
                  return file.length;
                },
                append: async (page) => {
                  await pause("append");
                  return file.append(page);
                },
                read: (ref) => file.read(ref),
                readInto: async (ref, into) => {
                  await pause("read");
                  await file.readInto(ref, into);
                },
                close: () => file.close(),
              };
            },
          };
          const fragments = new RegistryFragments(budget, spill, 1, controller.signal, () =>
            pause("clock"),
          );
          const fill = async (into: Uint8Array) => {
            await pause("fill");
            into.fill(19);
          };
          const saved = phase === "read" ? await fragments.append(131111, fill) : null;
          armed = true;
          const pending = saved
            ? fragments.readInto(saved, new Uint8Array(saved.length))
            : fragments.append(131111, fill);
          void pending.catch(() => undefined);
          await enteredGate;
          if (cancel) controller.abort(new Error("cancelled fragments"));
          let finished = false;
          const closing = fragments.close().then(() => {
            finished = true;
          });
          for (let i = 0; i < 8; i++) await Promise.resolve();
          const early = finished,
            charged = budget.used;
          release();
          let error: unknown;
          try {
            await pending;
          } catch (caught) {
            error = caught;
          }
          await closing;
          expect(early, "fragment close must fence pending I/O and source work").toBe(false);
          expect(charged, "pending fragment scratch remains charged").toBe(REGISTRY_FRAGMENT_BYTES);
          expect(error).toBeInstanceOf(Error);
          expect((error as Error).message).toContain(cancel ? "cancelled fragments" : "closed");
          expect(budget.used).toBe(0);
          expect(backing.bytesInUse).toBe(0);
          reach.reach(phase);
          reach.reach(cancel ? "external cancellation" : "close cancellation");
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          ["create", false],
          ["append", true],
          ["read", false],
          ["fill", true],
          ["clock", false],
          ["read", true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
