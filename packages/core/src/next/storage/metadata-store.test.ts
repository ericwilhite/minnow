/** Owner-private metadata must survive the spill sweep, but never a fenced writer. */
import { afterAll, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, METADATA_STORE_REACH } from "../../testing/reach.js";
import {
  METADATA_PAGE_BYTES,
  METADATA_SMALL_BYTES,
  OwnershipLostError,
  StoreCorruptionError,
  type MetadataStore,
  type StorageBackend,
} from "./contract.js";
import { MemoryBackend } from "./memory-store.js";
import { SimBackend, SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { createOpfsBackend } from "./opfs.js";

const factories: ReadonlyArray<readonly [string, () => StorageBackend]> = [
  ["memory", () => new MemoryBackend()],
  ["file handle", () => new SimBackend(new SimDisk(), "handle")],
  ["file epoch", () => new SimBackend(new SimDisk(), "epoch")],
  [
    "IndexedDB",
    () => {
      const disk = new SimDisk();
      return createIndexedDbBackend({
        database: () => simIndexedDb(disk),
        name: "db",
        locks: new SimLocks(),
      });
    },
  ],
  [
    "OPFS",
    () => {
      const disk = new SimDisk();
      return createOpfsBackend({
        name: "db",
        locks: new SimLocks(),
        directory: {
          getFileHandle: async (name) => ({
            createSyncAccessHandle: async () => disk.open(name, true),
          }),
        },
      });
    },
  ],
];
const reach = createReach("metadata store", METADATA_STORE_REACH);

for (const [name, factory] of factories)
  for (const seed of seedsFor("metadata-store", [1511, 1512, 1513]))
    it(`${name}: isolated fixed pages and fenced operations (${String(seed)})`, async () => {
      reach.beginHistory();
      const random = mulberry32(seed),
        backend = factory();
      const lease = await backend.acquire();
      const session = await backend.open(lease);
      const metadata = session.metadata?.();
      if (metadata == null) throw new Error("No metadata backing");
      const frame = new Uint8Array(METADATA_PAGE_BYTES),
        target = new Uint8Array(METADATA_PAGE_BYTES);
      const values = new Uint8Array(9),
        lengths = new Uint32Array(9);
      try {
        await metadata.reset();
        await session.writeData(0, new Uint8Array([7, 8, 9]));
        await session.publish(new Uint8Array([2]));
        await session.appendLog(new Uint8Array([3, 4]));
        const scratch = backend.scratch(lease);
        for (let step = 0; step < 27; step++) {
          const page =
            step < 2 * values.length ? step % values.length : Math.floor(random() * values.length);
          const value = 1 + Math.floor(random() * 254);
          frame.fill(value);
          const length =
            step < values.length || (step >= 2 * values.length && step % 2 === 0)
              ? METADATA_PAGE_BYTES
              : METADATA_SMALL_BYTES;
          const input = length === frame.length ? frame : frame.slice(0, length);
          await metadata.write(page, input);
          values[page] = value;
          lengths[page] = length;
          input.fill(0);
          // Reuse is permitted only after the write settles.
          frame.fill(0);
          if (scratch !== null) {
            await scratch.write(0, new Uint8Array([5]));
            await scratch.truncate(0);
          }
          await metadata.read(page, target);
          expect(target.every((byte, index) => byte === (index < length ? value : 0))).toBe(true);
        }
        reach.reach("rewrite");
        reach.reach("spill sweep isolation");
        for (let page = 0; page < values.length; page++) {
          await metadata.read(page, target);
          expect(
            target.every(
              (byte, index) => byte === (index < (lengths[page] ?? 0) ? values[page] : 0),
            ),
          ).toBe(true);
        }
        if (scratch !== null) await scratch.write(0, new Uint8Array([5]));
        await metadata.reset();
        await expect(metadata.read(0, target)).rejects.toThrow(StoreCorruptionError);
        expect(await session.readRoot()).toEqual(new Uint8Array([2]));
        expect((await session.readData([{ offset: 0, length: 3 }]))[0]).toEqual(
          new Uint8Array([7, 8, 9]),
        );
        expect(session.logLength()).toBe(2);
        if (scratch !== null) {
          const one = new Uint8Array(1);
          await scratch.readInto(0, one);
          expect(one[0]).toBe(5);
        }
        reach.reach("metadata reset isolation");
        frame.fill(11);
        await metadata.write(0, frame);
        const replacement = await backend.acquire({ steal: true, timeoutMs: 2_000 });
        try {
          for (const action of [
            () => metadata.read(0, target),
            () => metadata.write(0, frame),
            () => metadata.reset(),
          ])
            await expect(action()).rejects.toThrow(OwnershipLostError);
          reach.reach("fenced");
          const fresh = (await backend.open(replacement)).metadata?.();
          if (fresh == null) throw new Error("No replacement metadata");
          // Reopen discards owner-private contents before interpreting any page.
          await fresh.reset();
          await fresh.write(0, frame);
          await fresh.read(0, target);
          expect(target).toEqual(frame);
          reach.reach("replacement reset");
        } finally {
          await replacement.release();
        }
      } finally {
        await session.close();
        await lease.release();
      }
    });

for (const [name, factory] of factories)
  it(`${name}: validates page/frame bounds before I/O`, async () => {
    const backend = factory(),
      lease = await backend.acquire();
    try {
      const session = await backend.open(lease);
      const metadata: MetadataStore | null | undefined = session.metadata?.();
      if (metadata == null) throw new Error("No metadata backing");
      const frame = new Uint8Array(METADATA_PAGE_BYTES);
      for (const page of [-1, 0.5, Number.MAX_SAFE_INTEGER, Number.NaN]) {
        await expect(metadata.write(page, frame)).rejects.toThrow(RangeError);
        await expect(metadata.read(page, frame)).rejects.toThrow(RangeError);
      }
      await expect(metadata.write(0, frame.subarray(1))).rejects.toThrow(RangeError);
      await expect(metadata.read(0, frame.subarray(1))).rejects.toThrow(RangeError);
    } finally {
      await lease.release();
    }
  });

it("IndexedDB refuses a view that would clone a larger backing buffer", async () => {
  const factory = factories.find(([name]) => name === "IndexedDB")?.[1];
  if (factory === undefined) throw new Error("Missing IndexedDB factory");
  const backend = factory(),
    lease = await backend.acquire();
  try {
    const metadata = (await backend.open(lease)).metadata?.();
    if (metadata == null) throw new Error("No metadata backing");
    await expect(
      metadata.write(0, new Uint8Array(METADATA_PAGE_BYTES * 2).subarray(METADATA_PAGE_BYTES)),
    ).rejects.toThrow("standalone prepaid frame");
  } finally {
    await lease.release();
  }
});

afterAll(() => reach.assertReached());
