/** Frozen command boundary: scalar IDs, bounded charged metadata and fenced scratch I/O. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  REGISTRY_DIRECTORY_REACH,
  REGISTRY_DIRECTORY_IO_REACH,
  REGISTRY_DIRECTORY_FAULT_REACH,
  REGISTRY_DIRECTORY_ALLOCATION_REACH,
  REGISTRY_DIRECTORY_OWNER_REACH,
} from "../../testing/reach.js";
import type { SpillService } from "../spill/contract.js";
import { MemorySpillFile, MemorySpillService } from "../spill/memory-spill.js";
import {
  RegistryDirectory,
  REGISTRY_DIRECTORY_BYTES,
  REGISTRY_RECORD_BYTES,
} from "./registry-directory.js";

class Budget {
  used = 0;
  peak = 0;
  allocate(n: number): boolean {
    if (this.used + n > REGISTRY_DIRECTORY_BYTES) return false;
    this.used += n;
    this.peak = Math.max(this.peak, this.used);
    return true;
  }
  free(n: number): void {
    this.used -= n;
    expect(this.used).toBeGreaterThanOrEqual(0);
  }
}
const key = (kind: number, id: number) => `${String(kind)}:${String(id)}`;

it.each(seedsFor("next-registry-directory", [0xb5d6]))(
  "keeps exact bounded registry records seed %i",
  async (seed) => {
    const reach = createReach("native registry directory", REGISTRY_DIRECTORY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.tuple(
            fc.constantFrom(0, 1, 255),
            fc.constantFrom(0, 2 ** 32, 2 ** 52, Number.MAX_SAFE_INTEGER - 65535),
            fc.integer({ min: 0, max: 65535 }),
            fc.integer(),
          ),
          { minLength: 40, maxLength: 100 },
        ),
        fc.integer({ min: 192, max: 384 }),
        async (random, blocks) => {
          reach.beginHistory();
          const budget = new Budget(),
            backing = new MemorySpillService();
          let opened = 0;
          const spill: SpillService = {
            get bytesInUse() {
              return backing.bytesInUse;
            },
            sweep: () => backing.sweep(),
            create: (id) => {
              opened++;
              return backing.create(id);
            },
          };
          const directory = new RegistryDirectory(budget, spill, 1, new AbortController().signal);
          const input = new Uint8Array(REGISTRY_RECORD_BYTES),
            output = new Uint8Array(REGISTRY_RECORD_BYTES);
          const expected = new Map<string, Uint8Array>();
          const records = [
            ...Array.from({ length: blocks }, (_, k) => [0, 0, k * 16, k] as const),
            ...Array.from({ length: blocks }, (_, k) => [0, 2 ** 52, k * 16, k + 1] as const),
            ...Array.from({ length: blocks }, (_, k) => [255, 2 ** 52, k * 16, -k] as const),
            ...random,
            ...random.map(([kind, base, offset, value]) => [kind, base, offset, ~value] as const),
            [255, Number.MAX_SAFE_INTEGER - 65535, 65535, 17] as const,
          ];
          try {
            expect(await directory.get(1, 1, output)).toBe(false);
            expect(output.every((v) => v === 0)).toBe(true);
            for (const [kind, base, offset, value] of records) {
              input.fill(value & 255);
              new DataView(input.buffer).setInt32(4, value, true);
              await directory.put(kind, base + offset, input);
              expected.set(key(kind, base + offset), input.slice());
            }
            for (const [kind, base, offset] of [...records].reverse()) {
              expect(await directory.get(kind, base + offset, output)).toBe(true);
              expect(output).toEqual(expected.get(key(kind, base + offset)));
            }
            expect(opened, "over-budget registry descriptors must spill").toBe(1);
            expect(backing.bytesInUse).toBeGreaterThan(0);
            expect(expected.size * REGISTRY_RECORD_BYTES).toBeGreaterThan(REGISTRY_DIRECTORY_BYTES);
            expect(budget.used).toBe(REGISTRY_DIRECTORY_BYTES);
            expect(budget.peak).toBe(REGISTRY_DIRECTORY_BYTES);
            for (const state of [
              "evicted records",
              "overwritten records",
              "separate namespaces",
              "wide IDs",
              "max safe ID",
              "missing record",
              "fixed metadata",
              "one spill file",
              "descriptor count above cap",
            ] as const)
              reach.reach(state);
          } finally {
            await directory.close();
          }
          await directory.close();
          expect(budget.used).toBe(0);
          expect(backing.bytesInUse).toBe(0);
          reach.reach("zero ownership");
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-directory-owners", [0xb5da]))(
  "separates exact table and row provenance keys seed %i",
  async (seed) => {
    const reach = createReach("registry owner keys", REGISTRY_DIRECTORY_OWNER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(2 ** 32, 2 ** 52),
        fc.integer({ min: 192, max: 256 }),
        async (wide, count) => {
          reach.beginHistory();
          const budget = new Budget(),
            spill = new MemorySpillService();
          const directory = new RegistryDirectory(budget, spill, 1, new AbortController().signal);
          const bytes = new Uint8Array(REGISTRY_RECORD_BYTES);
          const owners = [0, wide, Number.MAX_SAFE_INTEGER];
          try {
            for (const [tag, owner] of owners.entries())
              for (let row = 0; row < count; row++) {
                bytes.fill((tag + row) & 255);
                await directory.put(1, row, bytes, owner);
              }
            for (const [tag, owner] of [...owners.entries()].reverse())
              for (let row = count - 1; row >= 0; row--) {
                expect(await directory.get(1, row, bytes, owner)).toBe(true);
                expect(bytes.every((value) => value === ((tag + row) & 255))).toBe(true);
              }
            expect(owners.length * count * REGISTRY_RECORD_BYTES).toBeGreaterThan(
              REGISTRY_DIRECTORY_BYTES,
            );
            expect(budget.peak).toBe(REGISTRY_DIRECTORY_BYTES);
            expect(spill.bytesInUse).toBeGreaterThan(0);
            reach.reach("same row different tables");
            reach.reach("wide table ID");
            reach.reach("max safe table ID");
            reach.reach("over-cap descriptors spill");
          } finally {
            await directory.close();
          }
          expect(budget.used).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("zero ownership");
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-directory-io", [0xb5d7]))(
  "fences registry close against pending I/O seed %i",
  async (seed) => {
    const reach = createReach("native registry I/O", REGISTRY_DIRECTORY_IO_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("create", "append", "read"),
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
          const started = new Promise<void>((resolve) => {
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
          const directory = new RegistryDirectory(budget, spill, 1, controller.signal);
          const bytes = new Uint8Array(REGISTRY_RECORD_BYTES);
          for (let i = 0; i < (phase === "read" ? 24 : 8); i++)
            await directory.put(0, i * 16, bytes);
          armed = true;
          const pending =
            phase === "read" ? directory.get(0, 0, bytes) : directory.put(0, 128, bytes);
          void pending.catch(() => undefined);
          await started;
          if (cancel) controller.abort(new Error("cancelled registry"));
          let ended = false;
          const closing = directory.close().then(() => {
            ended = true;
          });
          for (let i = 0; i < 8; i++) await Promise.resolve();
          const endedWhilePending = ended,
            chargedWhilePending = budget.used;
          release();
          let pendingError: unknown;
          try {
            await pending;
          } catch (error) {
            pendingError = error;
          }
          await closing;
          expect(endedWhilePending, "registry close must fence pending I/O").toBe(false);
          expect(chargedWhilePending, "registry I/O must retain paid scratch").toBe(
            REGISTRY_DIRECTORY_BYTES,
          );
          expect(pendingError).toBeInstanceOf(Error);
          expect((pendingError as Error).message).toContain(
            cancel ? "cancelled registry" : "closed",
          );
          expect(budget.used).toBe(0);
          expect(backing.bytesInUse).toBe(0);
          reach.reach(phase);
          reach.reach(cancel ? "external cancellation" : "close cancellation");
          reach.reach("charge survives I/O");
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          ["create", false],
          ["append", true],
          ["read", false],
          ["create", true],
          ["append", false],
          ["read", true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-directory-faults", [0xb5d8]))(
  "cleans failed registry metadata and rejects corrupt pages seed %i",
  async (seed) => {
    const reach = createReach("registry faults", REGISTRY_DIRECTORY_FAULT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("create", "append", "read", "checksum", "structure"),
        fc.boolean(),
        async (phase, failClose) => {
          reach.beginHistory();
          const budget = new Budget(),
            backing = new MemorySpillService();
          const ioError = new Error("registry I/O fault"),
            closeError = new Error("registry close fault");
          let armed = false,
            closed = 0;
          const fail = (name: string) => {
            if (armed && name === phase) {
              armed = false;
              throw ioError;
            }
          };
          const spill: SpillService = {
            get bytesInUse() {
              return backing.bytesInUse;
            },
            sweep: () => backing.sweep(),
            create: async (queryId) => {
              fail("create");
              const file = await backing.create(queryId);
              return {
                queryId,
                get length() {
                  return file.length;
                },
                append: (page) => {
                  fail("append");
                  return file.append(page);
                },
                read: (ref) => file.read(ref),
                readInto: async (ref, into) => {
                  fail("read");
                  if (armed && phase === "checksum") {
                    armed = false;
                    if (!(file instanceof MemorySpillFile)) throw new Error("Expected test file");
                    file.corrupt(ref, 0);
                  }
                  await file.readInto(ref, into);
                  if (armed && phase === "structure") {
                    armed = false;
                    into[0] = (into[0] ?? 0) ^ 255;
                  }
                },
                close: async () => {
                  closed++;
                  await file.close();
                  if (failClose) throw closeError;
                },
              };
            },
          };
          const directory = new RegistryDirectory(budget, spill, 1, new AbortController().signal);
          const bytes = new Uint8Array(REGISTRY_RECORD_BYTES);
          const read = phase === "read" || phase === "checksum" || phase === "structure";
          for (let i = 0; i < (read ? 24 : 8); i++) await directory.put(0, i * 16, bytes);
          armed = true;
          const pending = read ? directory.get(0, 0, bytes) : directory.put(0, 128, bytes);
          let failure: unknown;
          try {
            await pending;
          } catch (error) {
            failure = error;
          }
          if (phase === "checksum" || phase === "structure")
            expect(failure).toMatchObject({ name: "SpillCorruptionError" });
          else expect(failure).toBe(ioError);
          expect(() => directory.get(0, 0, bytes)).toThrow(failure);
          const closing = directory.close();
          if (failClose && phase !== "create") {
            await expect(closing).rejects.toBe(closeError);
            await expect(directory.close()).rejects.toBe(closeError);
            reach.reach("close failure");
          } else {
            await closing;
            await directory.close();
          }
          expect(closed).toBe(phase === "create" ? 0 : 1);
          expect(budget.used).toBe(0);
          expect(backing.bytesInUse).toBe(0);
          reach.reach(phase);
          reach.reach("failure poisons further work");
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
          ["checksum", true],
          ["structure", false],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-directory-allocation", [0xb5d9]))(
  "charges before buffers and unwinds partial construction seed %i",
  async (seed) => {
    const reach = createReach("registry construction", REGISTRY_DIRECTORY_ALLOCATION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 9 }), async (nth) => {
        reach.beginHistory();
        const budget = new Budget(),
          spill = new MemorySpillService();
        const native = globalThis.Uint8Array,
          failure = new Error("registry allocation fault");
        let allocations = 0;
        vi.stubGlobal(
          "Uint8Array",
          new Proxy(native, {
            construct(target, args, newTarget) {
              if (budget.used !== REGISTRY_DIRECTORY_BYTES)
                throw new Error("Registry buffers allocated without admitted metadata");
              if (++allocations === nth) throw failure;
              return Reflect.construct(target, args, newTarget) as Uint8Array;
            },
          }),
        );
        let error: unknown;
        try {
          new RegistryDirectory(budget, spill, 1, new AbortController().signal);
        } catch (caught) {
          error = caught;
        } finally {
          vi.stubGlobal("Uint8Array", native);
        }
        expect(error).toBe(failure);
        expect(allocations).toBe(nth);
        expect(budget.used).toBe(0);
        expect(spill.bytesInUse).toBe(0);
        reach.reach(nth === 1 ? "first allocation" : "partial construction");
        reach.reach("zero ownership");
        let deniedBuffers = 0;
        vi.stubGlobal(
          "Uint8Array",
          new Proxy(native, {
            construct(target, args, newTarget) {
              deniedBuffers++;
              return Reflect.construct(target, args, newTarget) as Uint8Array;
            },
          }),
        );
        try {
          expect(
            () =>
              new RegistryDirectory(
                {
                  allocate: () => false,
                  free: () => {
                    throw new Error("Unowned charge");
                  },
                },
                spill,
                1,
                new AbortController().signal,
              ),
          ).toThrow(/admitted/);
        } finally {
          vi.stubGlobal("Uint8Array", native);
        }
        expect(deniedBuffers).toBe(0);
        reach.reach("denied before allocation");
      }),
      { seed, numRuns: 12, examples: [[1], [2], [3], [4], [5], [6], [7], [8], [9]] },
    );
    reach.assertReached();
  },
  60000,
);
