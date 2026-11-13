/** No supported next/ predecessor: archived roots/writers refuse, never silently upgrade. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { SimLocks } from "../storage/sim-disk.js";
import { DirectoryFiles } from "./directory-fixture-files.js";
import { openDirectory } from "./directory-compat-api.js";
import { currentDirectoryFixture } from "./directory-current.test-support.js";
import { frozenDirectory, readDirectoryFixture } from "./directory-frozen.js";
const reach = createReach("development directory refusal", [
  "old development root refused",
  "old development writer refused",
  "payload bytes unchanged",
] as const);
for (const seed of seedsFor("next-directory-development-refusal", [0xb508]))
  it(`refuses archived formats and preserves payloads (seed ${String(seed)})`, async () => {
    const old = await frozenDirectory();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("packed-0", "packed-4095", "packed-4099"),
          async (name) => {
            reach.beginHistory();
            const image = await readDirectoryFixture(name),
              disk = DirectoryFiles.load(image);
            await expect(openDirectory(disk, new SimLocks())).rejects.toMatchObject({
              name: "UnsupportedEncodingError",
              format: "manifest",
              version: 2,
            });
            const unchanged = disk.image();
            for (const key of ["data", "root-0", "root-1"])
              expect(unchanged.files[key]).toBe(image.files[key]);
            reach.reach("old development root refused");
            const current = await currentDirectoryFixture(name),
              host = DirectoryFiles.load(current);
            await expect(old.open(host, new SimLocks())).rejects.toMatchObject({
              name: "ManifestCorruptionError",
            });
            const after = host.image();
            for (const key of ["data", "root-0", "root-1"])
              expect(after.files[key]).toBe(current.files[key]);
            reach.reach("old development writer refused");
            reach.reach("payload bytes unchanged");
          },
        ),
        { seed, numRuns: 12 },
      );
    } finally {
      await old.dispose();
    }
    reach.assertReached();
  }, 120_000);
