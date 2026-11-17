import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, STORE_NAME_REACH } from "../../testing/reach.js";
import { startOwnerHost } from "../owner/host.js";
import { OwnerConnector } from "../owner/connector.js";
import { chooseBackend, type StoragePlatform } from "./select.js";
import { InvalidStoreNameError, validateStoreName, encodeStoreName } from "./store-name.js";

for (const seed of seedsFor("next-store-name", [0xb2d5, 0xb2d6]))
  it(`refuses invalid names before platform or connector activity ${String(seed)}`, async () => {
    const reach = createReach("store names", STORE_NAME_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 0, max: 79 }), async (n) => {
        reach.beginHistory();
        for (const [kind, name] of [
          ["empty", ""],
          ["high surrogate", "a".repeat(n) + "\ud800"],
          ["low surrogate", "\udfff" + "a".repeat(n)],
          ["too long", "x".repeat(81 + n)],
          ["UTF-8 bound", "é".repeat(41 + n)],
          ["UTF-8 bound", "€".repeat(27)],
          ["UTF-8 bound", "😀".repeat(21)],
        ] as const) {
          const platform = new Proxy(
            {},
            {
              get() {
                throw new Error("platform touched");
              },
            },
          ) as StoragePlatform;
          await expect(chooseBackend(name, platform)).rejects.toBeInstanceOf(InvalidStoreNameError);
          const options = new Proxy(
            { database: name },
            {
              get(target, key) {
                if (key === "database") return target.database;
                throw new Error("connector activity");
              },
            },
          );
          expect(() => new OwnerConnector(options as never)).toThrow(InvalidStoreNameError);
          const untouched = new Proxy(
            {},
            {
              get() {
                throw new Error("owner storage activity");
              },
            },
          );
          await expect(
            startOwnerHost(options as never, untouched as never, untouched as never),
          ).rejects.toBeInstanceOf(InvalidStoreNameError);
          reach.reach(kind);
        }
        for (const name of [
          "x".repeat(80),
          "é".repeat(40),
          "€".repeat(26) + "xx",
          "😀".repeat(20),
          "é",
          "e\u0301",
        ]) {
          validateStoreName(name);
          expect(
            (
              await chooseBackend(
                name,
                {},
                {
                  request() {
                    throw new Error("unexpected lock");
                  },
                },
              )
            ).kind,
          ).toBe("memory");
        }
        expect(new TextEncoder().encode("é")).not.toEqual(new TextEncoder().encode("e\u0301"));
        expect(encodeStoreName(".")).toBe("%2E");
        expect(encodeStoreName("..")).toBe("%2E%2E");
        expect(encodeStoreName("é")).not.toBe(encodeStoreName("e\u0301"));
        reach.reach("valid boundary");
      }),
      { seed, numRuns: 32 },
    );
    reach.assertReached();
  });
