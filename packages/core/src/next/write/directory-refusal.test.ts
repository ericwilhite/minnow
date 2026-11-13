/** No supported next/ predecessor: archived roots/writers refuse, never silently upgrade. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, DIRECTORY_REFUSAL_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { SimLocks } from "../storage/sim-disk.js";
import { DirectoryFiles } from "./directory-fixture-files.js";
import { openDirectory } from "./directory-compat-api.js";
import { currentDirectoryFixture } from "./directory-current.test-support.js";
import { frozenDirectory, readDirectoryFixture } from "./directory-frozen.js";
/** Exact bytes via a reversible encoding: generic typed-array matchers expand huge fixtures. */
async function nativeImage(native: MemoryOpfs): Promise<Map<string, string>> {
  const image = new Map<string, string>();
  for await (const file of native.root.keys()) {
    const bytes = native.readFileBytes(file);
    if (bytes === undefined) throw new Error("Enumerated native file is missing");
    image.set(file, Buffer.from(bytes).toString("base64"));
  }
  return image;
}
const reach = createReach("development directory refusal", DIRECTORY_REFUSAL_REACH);
for (const seed of seedsFor("next-directory-development-refusal", [0xb508]))
  it(`refuses archived formats and preserves payloads (seed ${String(seed)})`, async () => {
    const old = await frozenDirectory();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("packed-0", "packed-4095", "packed-4099"),
          async (name) => {
            reach.beginHistory();
            reach.reach(name);
            const image = await readDirectoryFixture(name),
              disk = DirectoryFiles.load(image);
            // Native acquisition must refuse the intact outer selector before opening
            // writable handles or advancing the epoch. SimBackend below is only the fixture
            // bridge; its informational epoch is not the native pre-fence contract.
            const native = new MemoryOpfs();
            for (const [file, contents] of Object.entries(image.files))
              native.writeFileBytes(file, Uint8Array.from(Buffer.from(contents, "base64")));
            const before = await nativeImage(native);
            let writes = 0,
              handles = 0;
            native.setWriteFault(() => {
              writes++;
            });
            const backend = createOpfsBackend({
              name: "archived-directory",
              locks: new SimLocks(),
              directory: {
                keys: () => native.root.keys(),
                getFileHandle: async (file, options) => {
                  const handle = await native.root.getFileHandle(file, options);
                  return {
                    getFile: () => handle.getFile(),
                    createSyncAccessHandle: () => {
                      handles++;
                      return handle.createSyncAccessHandle();
                    },
                  };
                },
              },
            });
            // The registry's root-slot gate precedes the inner manifest decoder.
            await expect(backend.acquire()).rejects.toMatchObject({
              name: "DurableFormatError",
              code: "MINNOW_FORMAT_UNSUPPORTED",
            });
            expect(writes, "no mutation before format refusal").toBe(0);
            expect(handles).toBe(0);
            const afterNative = await nativeImage(native);
            expect(afterNative).toEqual(before);
            reach.reach("native refusal before fencing");
            reach.reach("zero native writes");
            reach.reach("entire native image unchanged");
            await expect(openDirectory(disk, new SimLocks())).rejects.toMatchObject({
              name: "DurableFormatError",
              code: "MINNOW_FORMAT_UNSUPPORTED",
            });
            const unchanged = disk.image();
            for (const key of ["data", "root-0", "root-1"])
              expect(unchanged.files[key]).toBe(image.files[key]);
            reach.reach("old development root refused");
            const current = await currentDirectoryFixture(name),
              host = DirectoryFiles.load(current);
            await expect(old.open(host, new SimLocks())).rejects.toMatchObject({
              name: "ManifestCorruptionError",
              code: "MINNOW_MANIFEST_CORRUPT",
            });
            const after = host.image();
            for (const key of ["data", "root-0", "root-1"])
              expect(after.files[key]).toBe(current.files[key]);
            reach.reach("old development writer refused");
            reach.reach("payload bytes unchanged");
          },
        ),
        { seed, numRuns: 12, examples: [["packed-0"], ["packed-4095"], ["packed-4099"]] },
      );
    } finally {
      await old.dispose();
    }
    reach.assertReached();
  }, 120_000);
