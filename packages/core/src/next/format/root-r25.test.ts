/** Frozen writer bytes are loaded through ordinary backends, opened, written and reopened. */
import { describe, expect, it } from "vitest";
import fixture from "./root-r25.fixture.json";
import development from "./root-before-r25.fixture.json";
import {
  decodeManifestRoot,
  encodeManifestRoot,
  ManifestCorruptionError,
} from "./manifest-codec.js";
import { listedRootExtents, reachableExtents } from "./reachable.js";
import { ExtentListKeeper } from "../storage/extent-list.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { StoreCorruptionError, type StorageBackend } from "../storage/contract.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { openExtentSpace } from "../storage/space.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";

function required<T>(value: T | null | undefined): T {
  if (value === null || value === undefined)
    throw new Error("Expected a stored reopen fixture value");
  return value;
}

const bytes = (hex: string) => Uint8Array.from(Buffer.from(hex, "hex"));
const options = () => ({
  reachable: listedRootExtents(),
  walk: reachableExtents(),
  verifyAtOpen: true,
  budget: countingBudget(Infinity),
  registerCache: () => countingMemory(Infinity),
});
function backend(kind: "opfs" | "indexeddb"): StorageBackend {
  const disk = new SimDisk();
  const locks = new SimLocks();
  return kind === "indexeddb"
    ? createIndexedDbBackend({ name: "fixture", locks, database: () => simIndexedDb(disk) })
    : createOpfsBackend({
        name: "fixture",
        locks,
        directory: {
          getFileHandle: async (name) => ({
            createSyncAccessHandle: async () => disk.open(name, true),
          }),
        },
      });
}
async function seed(store: StorageBackend, corrupt = false) {
  const lease = await store.acquire();
  try {
    const session = await store.open(lease);
    for (const [index, payload] of fixture.payloads.entries()) {
      const data = bytes(payload.hex);
      if (corrupt && index === 1) data[0] = (data[0] ?? 0) ^ 1;
      await session.writeData(payload.offset, data);
    }
    await session.publish(bytes(fixture.rootHex));
    await session.close();
  } finally {
    await lease.release();
  }
}

for (const kind of ["opfs", "indexeddb"] as const)
  describe(`frozen manifest 2 on ${kind}`, () => {
    it("preserves its catalog, continues publication, and reopens both roots through their lists", async () => {
      const store = backend(kind);
      await seed(store);
      const lease = await store.acquire();
      try {
        const session = await store.open(lease);
        const space = await openExtentSpace(session, options());
        const budget = countingBudget(Infinity);
        const parsed = decodeManifestRoot(required(space.root), budget);
        const root = parsed.value;
        budget.free(parsed.charge);
        const read = await space.read([root.catalog], { budget });
        const catalogBytes = required(read.bytes[0]).slice();
        read.release();
        expect(catalogBytes).toEqual(bytes(required(fixture.payloads[0]).hex));
        const keeper = new ExtentListKeeper(root.extentList, { reader: space, budget });
        const writer = space.beginWrite("continue frozen writer");
        const catalog = required((await writer.write([catalogBytes])).blobs[0]);
        const retired = [root.catalog.extent];
        const prepared = await keeper.prepare(2, {
          added: [...writer.extents],
          released: retired,
          writer,
        });
        await space.publish(
          encodeManifestRoot({
            ...root,
            sequence: 2,
            epoch: lease.epoch,
            catalog,
            extentList: prepared.list,
          }),
          { writers: [writer], retired: [...retired, ...prepared.retire] },
        );
        keeper.commit(2, 2);
        await space.audit();
        await space.close();
        await session.close();
      } finally {
        await lease.release();
      }
      const next = await store.acquire();
      try {
        const session = await store.open(next);
        const space = await openExtentSpace(session, options());
        const budget = countingBudget(Infinity);
        const root = decodeManifestRoot(required(space.root), budget);
        expect(root.value.sequence).toBe(2);
        const read = await space.read([root.value.catalog], { budget });
        expect(read.bytes[0]).toEqual(bytes(required(fixture.payloads[0]).hex));
        read.release();
        budget.free(root.charge);
        await space.audit();
        await space.close();
        await session.close();
      } finally {
        await next.release();
      }
    });
    it("refuses a damaged frozen list page without repairing it", async () => {
      const store = backend(kind);
      await seed(store, true);
      for (let attempt = 0; attempt < 2; attempt++) {
        const lease = await store.acquire();
        try {
          const session = await store.open(lease);
          await expect(openExtentSpace(session, options())).rejects.toBeInstanceOf(
            StoreCorruptionError,
          );
          await session.close();
        } finally {
          await lease.release();
        }
      }
    });
  });

for (const kind of ["opfs", "indexeddb"] as const)
  it(`refuses the frozen pre-R25 development root through ordinary ${kind} open`, async () => {
    const store = backend(kind);
    const seed = await store.acquire();
    try {
      const session = await store.open(seed);
      for (const payload of development.payloads)
        await session.writeData(payload.offset, bytes(payload.hex));
      await session.publish(bytes(development.rootHex));
      await session.close();
    } finally {
      await seed.release();
    }
    for (let attempt = 0; attempt < 2; attempt++) {
      const lease = await store.acquire();
      try {
        const session = await store.open(lease);
        await expect(openExtentSpace(session, options())).rejects.toBeInstanceOf(
          ManifestCorruptionError,
        );
        expect(await session.readRoot()).toEqual(bytes(development.rootHex));
        await session.close();
      } finally {
        await lease.release();
      }
    }
  });
