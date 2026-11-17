import fc from "fast-check";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, IDB_ENTRY_ALLOCATION_REACH } from "../../testing/reach.js";
import { ByteStore, SERIAL_READ_PAGE } from "./idb-bytes.js";
import { openIndexedDb } from "./idb-like.js";
import { StoreCorruptionError } from "./contract.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

for (const seed of seedsFor("next-idb-entry-allocation", [0xb2d9, 0xb2da]))
  it(`closed persisted index shapes without per-record key arrays ${String(seed)}`, async () => {
    const reach = createReach("IDB entry allocation", IDB_ENTRY_ALLOCATION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: SERIAL_READ_PAGE + 17 }),
        fc.constantFrom("extra", "toString", "constructor", "__proto__"),
        async (count, extra) => {
          reach.beginHistory();
          const db = await openIndexedDb(new IDBFactory(), "entry-allocation");
          try {
            await db.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
              for (let i = 0; i < count; i++) {
                tx.store("data").put(i * 2, {
                  offset: i * 2,
                  bytes: new Uint8Array([(i % 251) + 1]).buffer,
                });
                tx.store("data-index").put(i * 2, { offset: i * 2, length: 1 });
              }
            });
            const space = await ByteStore.openLazy(db, "data");
            for (const serial of [false, true]) {
              const target = new Uint8Array(count * 2);
              let arrays = 0;
              const observe = (value: unknown) => {
                if (
                  typeof value === "object" &&
                  value !== null &&
                  Object.hasOwn(value, "offset") &&
                  Object.hasOwn(value, "length")
                )
                  arrays++;
              };
              // Instrument the key-array APIs, not RSS/GC or an implementation snapshot.
              // Only persisted index-entry arguments count; restore before assertions.
              const ownKeys = Reflect.ownKeys,
                keys = Object.keys,
                names = Object.getOwnPropertyNames,
                symbols = Object.getOwnPropertySymbols;
              const spies = [
                vi.spyOn(Reflect, "ownKeys").mockImplementation((value) => {
                  observe(value);
                  return ownKeys(value);
                }),
                vi.spyOn(Object, "keys").mockImplementation((value) => {
                  observe(value);
                  return keys(value);
                }),
                vi.spyOn(Object, "getOwnPropertyNames").mockImplementation((value) => {
                  observe(value);
                  return names(value);
                }),
                vi.spyOn(Object, "getOwnPropertySymbols").mockImplementation((value) => {
                  observe(value);
                  return symbols(value);
                }),
              ];
              try {
                await db.transaction(["data", "data-index"], "readonly", "relaxed", (tx) => {
                  space.read(tx, 0, target, () => undefined, serial);
                });
              } finally {
                for (const spy of spies) spy.mockRestore();
              }
              expect(
                arrays,
                `key-array calls over ${String(count)} records; serial=${String(serial)}`,
              ).toBe(0);
              for (let i = 0; i < count; i++) {
                expect(target[i * 2]).toBe((i % 251) + 1);
                expect(target[i * 2 + 1]).toBe(0);
              }
              reach.reach(serial ? "serial read" : "parallel read");
            }
            reach.reach(count === 1 ? "one record" : "multiple records");
            if (count > SERIAL_READ_PAGE) reach.reach("serial page boundary");
            reach.reach("no key arrays");
            await db.transaction(["data-index"], "readwrite", "strict", (tx) => {
              tx.store("data-index").put(0, { offset: 0, length: 1, [extra]: 7 });
            });
            for (const serial of [false, true])
              await expect(
                db.transaction(["data", "data-index"], "readonly", "relaxed", (tx) => {
                  space.read(tx, 0, new Uint8Array(count * 2), () => undefined, serial);
                }),
              ).rejects.toBeInstanceOf(StoreCorruptionError);
            reach.reach("extra field refused on read");
          } finally {
            db.close();
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [1, "extra"],
          [SERIAL_READ_PAGE + 1, "__proto__"],
        ],
      },
    );
    reach.assertReached();
  });
