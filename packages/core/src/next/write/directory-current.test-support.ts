/** Current-root bootstrap over unchanged directory payloads, not a next-to-next upgrade.
 * Archived root slots are discarded. Current and independently bundled current writers own
 * the continuing-writer tests; archived writers/roots are used only for explicit refusal. */
import { build } from "esbuild";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { SimBackend, SimLocks, type SimDisk } from "../storage/sim-disk.js";
import { encodeManifestRoot } from "../format/manifest-codec.js";
import { readDirectoryFixture } from "./directory-frozen.js";
import { DirectoryFiles, type DirectoryImage } from "./directory-fixture-files.js";
import type { openDirectory } from "./directory-compat-api.js";
import { need } from "./bytes.js";
/** Current-format test roots over unchanged packed group/page payloads.
 * Never loaded as a migration; old root slots are discarded before these are freshly encoded. */
import type { ManifestRoot } from "../format/manifest.js";
export const DIRECTORY_RECIPES = {
  "packed-0": {
    formatVersion: 3,
    sequence: 2,
    epoch: 1,
    log: {
      salt: 378410529,
      firstLsn: 2,
      rootSequence: 2,
    },
    globalLsn: 1,
    catalogVersion: 2,
    catalog: {
      extent: {
        offset: 8192,
        length: 679,
      },
      offset: 0,
      length: 679,
      checksum: 2957321151,
    },
    nextTableId: 2,
    sequences: [],
    tables: [
      {
        tableId: 1,
        schemaVersion: 1,
        nextRowId: 1,
        flushedThroughLsn: 1,
        liveRowCount: 0,
        groups: {
          extent: {
            offset: 0,
            length: 0,
          },
          offset: 0,
          length: 0,
          checksum: 0,
        },
        groupCount: 0,
        staged: [],
        keyIndex: null,
        secondaryIndexes: [],
        statistics: null,
      },
    ],
    clients: [],
    extentList: [
      {
        blob: {
          extent: {
            offset: 12288,
            length: 32,
          },
          offset: 0,
          length: 32,
          checksum: 613664295,
        },
        first: 8192,
        count: 1,
      },
    ],
  },
  "packed-4095": {
    formatVersion: 3,
    sequence: 3,
    epoch: 1,
    log: {
      salt: 2069787747,
      firstLsn: 3,
      rootSequence: 3,
    },
    globalLsn: 2,
    catalogVersion: 2,
    catalog: {
      extent: {
        offset: 33648640,
        length: 679,
      },
      offset: 0,
      length: 679,
      checksum: 2957321151,
    },
    nextTableId: 2,
    sequences: [],
    tables: [
      {
        tableId: 1,
        schemaVersion: 1,
        nextRowId: 4096,
        flushedThroughLsn: 2,
        liveRowCount: 4095,
        groups: {
          extent: {
            offset: 33640448,
            length: 32,
          },
          offset: 0,
          length: 32,
          checksum: 3338744856,
        },
        groupCount: 4095,
        staged: [],
        keyIndex: null,
        secondaryIndexes: [],
        statistics: {
          extent: {
            offset: 33644544,
            length: 34,
          },
          offset: 0,
          length: 34,
          checksum: 1396776289,
        },
      },
    ],
    clients: [],
    extentList: [
      {
        blob: {
          extent: {
            offset: 33652736,
            length: 65552,
          },
          offset: 0,
          length: 65552,
          checksum: 3070741974,
        },
        first: 8192,
        count: 4096,
      },
      {
        blob: {
          extent: {
            offset: 33722368,
            length: 65552,
          },
          offset: 0,
          length: 65552,
          checksum: 3550003529,
        },
        first: 16785408,
        count: 4096,
      },
      {
        blob: {
          extent: {
            offset: 33792000,
            length: 48,
          },
          offset: 0,
          length: 48,
          checksum: 1679828674,
        },
        first: 33644544,
        count: 2,
      },
    ],
  },
  "packed-4099": {
    formatVersion: 3,
    sequence: 3,
    epoch: 1,
    log: {
      salt: 2809723960,
      firstLsn: 3,
      rootSequence: 3,
    },
    globalLsn: 2,
    catalogVersion: 2,
    catalog: {
      extent: {
        offset: 33681408,
        length: 679,
      },
      offset: 0,
      length: 679,
      checksum: 2957321151,
    },
    nextTableId: 2,
    sequences: [],
    tables: [
      {
        tableId: 1,
        schemaVersion: 1,
        nextRowId: 4100,
        flushedThroughLsn: 2,
        liveRowCount: 4099,
        groups: {
          extent: {
            offset: 33673216,
            length: 47,
          },
          offset: 0,
          length: 47,
          checksum: 3160694728,
        },
        groupCount: 4099,
        staged: [],
        keyIndex: null,
        secondaryIndexes: [],
        statistics: {
          extent: {
            offset: 33677312,
            length: 34,
          },
          offset: 0,
          length: 34,
          checksum: 1102165933,
        },
      },
    ],
    clients: [],
    extentList: [
      {
        blob: {
          extent: {
            offset: 33685504,
            length: 65552,
          },
          offset: 0,
          length: 65552,
          checksum: 3070741974,
        },
        first: 8192,
        count: 4096,
      },
      {
        blob: {
          extent: {
            offset: 33755136,
            length: 65552,
          },
          offset: 0,
          length: 65552,
          checksum: 2615667190,
        },
        first: 16785408,
        count: 4096,
      },
      {
        blob: {
          extent: {
            offset: 33824768,
            length: 176,
          },
          offset: 0,
          length: 176,
          checksum: 2211226632,
        },
        first: 33562624,
        count: 10,
      },
    ],
  },
} satisfies Record<string, ManifestRoot>;
const ready = new Map<string, Promise<DirectoryImage>>();
export function currentDirectoryFixture(
  name: keyof typeof DIRECTORY_RECIPES,
): Promise<DirectoryImage> {
  let image = ready.get(name);
  if (image !== undefined) return image;
  image = (async () => {
    const source = await readDirectoryFixture(name);
    const files = DirectoryFiles.load({
      ...source,
      files: Object.fromEntries(
        Object.entries(source.files).filter(([key]) => key !== "root-0" && key !== "root-1"),
      ),
    });
    const backend = new SimBackend(files as unknown as SimDisk, "handle", {
      locks: new SimLocks(),
    });
    const lease = await backend.acquire();
    try {
      const session = await backend.open(lease);
      try {
        const root = DIRECTORY_RECIPES[name];
        // Both slots are fresh current publications. No old root parser or converter runs.
        for (let step = 1; step <= 2; step++)
          await session.publish(
            encodeManifestRoot({ ...root, epoch: lease.epoch, sequence: root.sequence + step }),
          );
      } finally {
        await session.close();
      }
    } finally {
      await lease.release();
    }
    return files.image();
  })();
  ready.set(name, image);
  return image;
}
/** Independent current module graph: only native files/locks cross its boundary. */
export async function directoryPeer() {
  const scratch = await mkdtemp(join(tmpdir(), "minnow-directory-current-"));
  try {
    const result = await build({
      entryPoints: [fileURLToPath(new URL("./directory-compat-api.ts", import.meta.url))],
      platform: "node",
      target: "node22",
      format: "esm",
      bundle: true,
      write: false,
    });
    const file = join(scratch, "current.mjs");
    await writeFile(file, need(result.outputFiles[0]).contents);
    const current = (await import(/* @vite-ignore */ pathToFileURL(file).href)) as {
      openDirectory: typeof openDirectory;
    };
    return {
      open: current.openDirectory,
      dispose: () => rm(scratch, { recursive: true, force: true }),
    };
  } catch (error) {
    await rm(scratch, { recursive: true, force: true });
    throw error;
  }
}
