/** Verified bytes still need endpoint/version checks; native CRC failures are never answers. */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { createReach, OVERFLOW_INTEGRITY_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { StoreSession } from "../storage/contract.js";
import type { EvictableCache } from "../memory/governor.js";
import type { RunRef, ManifestRoot } from "../format/manifest.js";
import { encodeManifestRoot } from "../format/manifest-codec.js";
import { UnsupportedEncodingError, type RunEncoding } from "../format/encoding.js";
import { KEY_DIRECTORY_MAGIC, readKeyPointer } from "../format/key-pointer.js";
import { ByteReader as PointerReader } from "../format/bytes.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { ByteReader, need, UNCHARGED } from "./bytes.js";
import { RunBuilder, RunReader, runRef } from "./runs.js";
import { Harness } from "./test-support.js";
const reach = createReach("overflow integrity", OVERFLOW_INTEGRITY_STATES);
afterAll(() => reach.assertReached());
function root(ref: RunRef, catalog: ManifestRoot["catalog"]): ManifestRoot {
  return {
    formatVersion: 3,
    sequence: 1,
    epoch: 1,
    log: { salt: 1, firstLsn: 1, rootSequence: 1 },
    globalLsn: 0,
    catalogVersion: 1,
    catalog,
    nextTableId: 2,
    sequences: [],
    clients: [],
    extentList: [],
    tables: [
      {
        tableId: 1,
        schemaVersion: 1,
        nextRowId: 7,
        flushedThroughLsn: 0,
        liveRowCount: 6,
        groups: catalog,
        groupCount: 0,
        staged: [],
        keyIndex: { indexId: 1, columnIds: [1], unique: true, runs: [ref] },
        secondaryIndexes: [],
        statistics: null,
      },
    ],
  };
}
for (const seed of seedsFor("next-overflow-summary-corruption", [0xb50d]))
  it(`refuses contradictory bounds and impossible capacities (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(2049, 49155, 65537),
        fc.boolean(),
        fc.constantFrom(0, 3, 255),
        async (width, shared, version) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory");
          await h.open();
          const writer = h.space.beginWrite("summary corruption"),
            builder = new RunBuilder(false, false, UNCHARGED, 2);
          let summary: ReturnType<RunBuilder["finish"]> | undefined;
          const grant = await h.governor.admit({
            label: "corrupt summary",
            minimumBytes: 4 * 1024 * 1024,
            spillable: false,
          });
          try {
            for (let i = 0; i < 6; i++) {
              const k = new Uint8Array(width).fill(65);
              new DataView(k.buffer).setUint32(shared ? width - 4 : 0, i, false);
              builder.add(k, i, false);
            }
            summary = need(builder.finish());
            const ref = runRef(summary, (await writer.write(summary.parts)).blobs),
              storage = need(ref.keyStorage);
            const changes: RunRef[] = [
              { ...ref, minKey: ref.maxKey, keyStorage: { ...storage, minimum: storage.maximum } },
              { ...ref, maxKey: ref.minKey, keyStorage: { ...storage, maximum: storage.minimum } },
            ];
            for (let i = 0; i < changes.length; i++) {
              await expect(
                RunReader.open(h.space, need(changes[i]), grant, grant),
              ).rejects.toMatchObject({ code: "MINNOW_STORE_CORRUPT" });
              expect(grant.allocated).toBe(0);
              reach.reach(i === 0 ? "minimum contradiction" : "maximum contradiction");
            }
            const impossible = {
              ...ref,
              keyStorage: { ...storage, maxKeyBytes: ref.entries.offset + 1 },
            };
            await expect(
              Promise.resolve().then(() => RunReader.open(h.space, impossible, grant, grant)),
            ).rejects.toMatchObject({ code: "MINNOW_MANIFEST_CORRUPT" });
            expect(() => encodeManifestRoot(root(impossible, ref.entries))).toThrow(/key capacity/);
            reach.reach("capacity outside data");
            const cross = {
              ...ref,
              fences: {
                ...ref.fences,
                extent: { ...ref.entries.extent, offset: ref.entries.extent.offset + 4096 },
              },
            };
            expect(() => encodeManifestRoot(root(cross, ref.entries))).toThrow(/crosses extents/);
            reach.reach("cross-extent refusal");
            expect(() =>
              encodeManifestRoot(root({ ...ref, encoding: version as RunEncoding }, ref.entries)),
            ).toThrow(UnsupportedEncodingError);
            reach.reach("unknown run version");
            reach.reach(shared ? "shared-prefix corruption" : "different-prefix corruption");
          } finally {
            summary?.release();
            builder.close();
            grant.close();
            await writer.abort();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 12 },
    );
  }, 120_000);

for (const seed of seedsFor("next-overflow-native-corruption", [0xb50e]))
  it(`refuses corrupt payloads and checksummed future directories (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(2049, 49155, 65537), fc.boolean(), async (width, future) => {
        reach.beginHistory();
        const h = new Harness("strict", "memory"),
          caches: EvictableCache[] = [];
        let session: StoreSession | undefined;
        const open = h.backend.open.bind(h.backend),
          register = h.governor.registerCache.bind(h.governor);
        const opening = vi.spyOn(h.backend, "open").mockImplementation(async (lease) => {
          session = await open(lease);
          return session;
        });
        const registration = vi.spyOn(h.governor, "registerCache").mockImplementation((cache) => {
          caches.push(cache);
          return register(cache);
        });
        await h.open();
        const writer = h.space.beginWrite("native overflow corruption"),
          builder = new RunBuilder(false, false, UNCHARGED, 2);
        const grant = await h.governor.admit({
          label: "native corrupt reader",
          minimumBytes: 4 * 1024 * 1024,
          spillable: false,
        });
        let summary: ReturnType<RunBuilder["finish"]> | undefined, reader: RunReader | undefined;
        try {
          const key = new Uint8Array(width).fill(65);
          builder.add(key, 1, false);
          summary = need(builder.finish());
          let ref = runRef(summary, (await writer.write(summary.parts)).blobs);
          const native = need(session),
            parts = new Map<number, Uint8Array>();
          let at = 0;
          for (const p of summary.parts) {
            parts.set(at, p);
            at += p.length;
          }
          const index = new ByteReader(need(summary.parts.at(-3)));
          expect(index.varuint()).toBe(1);
          const offset = index.varuint(),
            length = index.varuint();
          index.u32();
          index.end();
          const block = need(parts.get(offset));
          expect(block.length).toBe(length);
          const p = readKeyPointer(new PointerReader(block.subarray(4, block.length - 7)), offset);
          if (p.kind !== "overflow") throw new Error("overflow fixture expected");
          const extent = ref.entries.extent;
          const raw = need((await native.readData([extent]))[0]).slice();
          if (future) {
            const directory = need(parts.get(p.directory.offset)).slice();
            expect(new DataView(directory.buffer).getUint32(0, true)).toBe(KEY_DIRECTORY_MAGIC);
            directory[4] = 255;
            const checksum = xxHash32(directory);
            const change = (bytes: Uint8Array, at: number, n: number) => {
              const out = bytes.slice();
              new DataView(out.buffer).setUint32(at, n, true);
              return out;
            };
            const changedBlock = change(block, block.length - 11, checksum);
            const changedIndex = change(
              need(summary.parts.at(-3)),
              need(summary.parts.at(-3)).length - 4,
              xxHash32(changedBlock),
            );
            const fences = need(summary.parts.at(-2)),
              changedFences = change(fences, fences.length - 4, checksum);
            const storage = need(ref.keyStorage);
            ref = {
              ...ref,
              entries: { ...ref.entries, checksum: xxHash32(changedIndex) },
              fences: { ...ref.fences, checksum: xxHash32(changedFences) },
              keyStorage: {
                ...storage,
                minimum: change(storage.minimum, storage.minimum.length - 4, checksum),
                maximum: change(storage.maximum, storage.maximum.length - 4, checksum),
              },
            };
            for (const [where, bytes] of [
              [p.directory.offset, directory],
              [offset, changedBlock],
              [ref.entries.offset, changedIndex],
              [ref.fences.offset, changedFences],
            ] as const)
              raw.set(bytes, where);
          } else {
            raw[0] = (raw[0] ?? 0) ^ 1;
          }
          await native.writeData(extent.offset, raw);
          for (const cache of caches) cache.evict(Number.MAX_SAFE_INTEGER);
          reader = await RunReader.open(h.space, ref, grant, grant);
          await expect(reader.lookup(h.space, key, [], grant)).rejects.toMatchObject({
            code: future ? "MINNOW_UNSUPPORTED_ENCODING" : "MINNOW_STORE_CORRUPT",
          });
          expect(grant.allocated).toBe(reader.bytes);
          reach.reach(future ? "checksummed future directory" : "native payload CRC");
        } finally {
          if (reader) grant.free(reader.bytes);
          expect(grant.allocated).toBe(0);
          summary?.release();
          builder.close();
          grant.close();
          await writer.abort();
          await h.close();
          opening.mockRestore();
          registration.mockRestore();
        }
        expect(h.governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 8 },
    );
  }, 120_000);
