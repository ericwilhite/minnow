import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { MemoryOpfs } from "../../../packages/core/src/testing/opfs-shim.js";
import { seedsFor } from "../../../packages/core/src/testing/seeds.js";
import { createReach, NATIVE_FIXTURE_REACH } from "../../../packages/core/src/testing/reach.js";
import { createOpfsBackend } from "../../../packages/core/src/next/storage/opfs.js";
import { encodeRootSlot } from "../../../packages/core/src/next/storage/root-slot.js";
import { SimLocks } from "../../../packages/core/src/next/storage/sim-disk.js";
import { installFileStoreFixture } from "./opfs-fixture.js";

const reach = createReach("native file-store fixture import", NATIVE_FIXTURE_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("native-export-fixture", [0xb2d3, 0xb2d4]))
  it(`installs exact source bytes behind a target-local native fence seed ${String(seed)}`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.uint8Array({ minLength: 1, maxLength: 128 }), async (data) => {
        reach.beginHistory();
        for (const split of [1, 17]) {
          const shim = new MemoryOpfs(),
            locks = new SimLocks(),
            root = new TextEncoder().encode('{"extents":[]}');
          const source = new Map<string, Uint8Array>([
            ["data", data],
            ["log", new Uint8Array()],
            ["root-0", encodeRootSlot(1, root)],
            ["root-1", new Uint8Array()],
          ]);
          const original = [...source].map(([name, bytes]) => [name, bytes.slice()] as const);
          let reads = 0;
          const options = {
            directory: shim.root,
            name: "fixture",
            locks,
            files: [...source].map(([name, bytes]) => ({ name, bytes: bytes.length })),
            read: async (name: string) => {
              reads++;
              const bytes = source.get(name);
              if (bytes === undefined) throw new Error("missing source");
              return new Response(
                new ReadableStream<Uint8Array>({
                  start(controller) {
                    for (let at = 0; at < bytes.length; at += split)
                      controller.enqueue(bytes.subarray(at, at + split));
                    controller.close();
                  },
                }),
              );
            },
          };
          await installFileStoreFixture(options);
          expect(reads).toBe(4);
          for (const [name, bytes] of original) {
            expect(shim.readFileBytes(name)).toEqual(bytes);
            expect(source.get(name)).toEqual(bytes);
          }
          const backend = createOpfsBackend({ directory: shim.root, name: "fixture", locks });
          const lease = await backend.acquire();
          expect(lease.epoch).toBe(2);
          const session = await backend.open(lease);
          expect(await session.readRoot()).toEqual(root);
          const target = new Uint8Array(data.length);
          await session.readDataInto(0, target);
          expect(target).toEqual(data);
          await session.close();
          await lease.release();
          const before = new Map<string, Uint8Array | undefined>();
          for await (const name of shim.root.keys()) before.set(name, shim.readFileBytes(name));
          await expect(installFileStoreFixture(options)).rejects.toThrow(
            "Fixture target must be empty",
          );
          expect(reads).toBe(4);
          const after = new Map<string, Uint8Array | undefined>();
          for await (const name of shim.root.keys()) after.set(name, shim.readFileBytes(name));
          expect(after).toEqual(before);
          reach.reach(split === 1 ? "one-byte chunks" : "split chunks");
          reach.reach("ordinary native open and exact bytes");
          reach.reach("target-local epoch advances");
          reach.reach("source unchanged");
          reach.reach("occupied target refused unchanged");
        }
      }),
      { seed, numRuns: 4, endOnFailure: true },
    );
  });
