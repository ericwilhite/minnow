/** Registry law: unknown durable identities/tags/reserved bits never mean empty or decoded data. */
import fc from "fast-check";
import {
  discoverDurableSources,
  DURABLE_EMISSIONS,
  DURABLE_REFERENCES,
} from "./durable-inventory.js";
import { expect, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import {
  createReach,
  DURABLE_REGISTRY_REACH,
  DURABLE_FIELD_REACH,
  DURABLE_NATIVE_REACH,
  DURABLE_ALIAS_REACH,
  DURABLE_CATALOG_REACH,
  DURABLE_READ_REACH,
  DURABLE_STARTUP_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import {
  DURABLE_FORMATS,
  DURABLE_READER_VERSION,
  MANIFEST_FORMAT_VERSION,
  type DurableFormatName,
  type FormatField,
} from "../durable-format.js";
import { ByteReader as MetadataReader } from "../format/bytes.js";
import { decodeDeletionBitmap } from "../format/deletion-bitmap.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { decodeRootSlot, encodeRootSlot } from "../storage/root-slot.js";
import { encodeEpochSlot, createOpfsBackend, type OpfsDirectoryLike } from "../storage/opfs.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { decodeListPage, encodeListPage } from "../storage/extent-list.js";
import type { ByteBudget, ExtentReader } from "../storage/extents.js";
import {
  decodeLogHeader,
  encodeLogHeader,
  encodeFrame,
  scanLog,
  scanLogIncremental,
} from "../format/log-frame.js";
import {
  decodeGroupRoot,
  tryDecodeGroupRootView,
  decodeManifestRoot,
  decodeGroupPage,
  decodeGroupIndex,
  decodeTableStatistics,
  encodeGroupRoot,
  encodeManifestRoot,
  encodeGroupPage,
  encodeGroupIndex,
  encodeTableStatistics,
  type Parsed,
} from "../format/manifest-codec.js";
import type { ManifestRoot, GroupRoot, BlobRef, PageEntry } from "../format/manifest.js";
import { parsePage, parseDictionary } from "../page/decode.js";
import { encodeDirectory, parseDirectory } from "../page/directory.js";
import { decodePointer, encodePointer, LARGE_VALUE_BYTES } from "../page/large-value.js";
import { DistinctSketch } from "../page/sketch.js";
import { encodeBody, decodeBody } from "../write/body.js";
import { encodeColumn, decodeColumn } from "../write/columns.js";
import { Catalog, decodeLogged, encodeLogged } from "../write/catalog.js";
import { encodeJson } from "../write/bytes.js";
import { encodeRun, runRef, RunReader, blockEntries } from "../write/runs.js";
import { encodeArrayValue, decodeArrayValue } from "../vector/array-value.js";
import type { FlatVector } from "../vector/vector.js";

const budget = (): ByteBudget & { held: number } => ({
  held: 0,
  allocate(n) {
    this.held += n;
    return true;
  },
  free(n) {
    this.held -= n;
  },
});
const blob: BlobRef = {
  extent: { offset: 4096, length: 4096 },
  offset: 0,
  length: 16,
  checksum: 7,
};
const manifest: ManifestRoot = {
  formatVersion: MANIFEST_FORMAT_VERSION,
  sequence: 1,
  epoch: 1,
  log: { salt: 7, firstLsn: 1, rootSequence: 1 },
  globalLsn: 0,
  catalogVersion: 0,
  catalog: blob,
  nextTableId: 1,
  tables: [],
  sequences: [],
  clients: [],
  extentList: [],
};
const root: GroupRoot = {
  rowCount: 2,
  rowIds: { kind: "dense" },
  columns: [
    {
      columnId: 1,
      type: { kind: "int32", bits: 32 },
      data: { extent: blob.extent, offset: 32, length: 8 },
      directory: blob,
      statistics: { nullCount: 0, min: 3, max: 9, dictionarySize: null },
      distinctSketch: null,
    },
  ],
};
const group = {
  groupId: 1,
  firstRowId: 1,
  lastRowId: 2,
  rowCount: 2,
  liveRowCount: 2,
  root: blob,
  deletions: null,
};
const pageEntry: PageEntry = {
  offset: 0,
  length: 24,
  firstRow: 0,
  rowCount: 2,
  nullCount: 0,
  checksum: 0,
  encoding: 1,
  min: null,
  max: null,
};
const rawPage = new Uint8Array(24);
rawPage.set([1, 2, 0, 4]);
new DataView(rawPage.buffer).setUint32(4, 2, true);
new Int32Array(rawPage.buffer, 16, 2).set([3, 9]);
const directory = {
  tag: 2,
  rowCount: 2,
  scale: 0,
  hasSpecials: false,
  pages: [pageEntry],
  dictionary: null,
  sketch: null,
};
const integer: FlatVector = {
  form: "flat",
  type: { kind: "int32", bits: 32 },
  length: 2,
  validity: null,
  data: { layout: "i32", values: new Int32Array([3, 9]) },
};
interface Adapter {
  bytes: Uint8Array;
  decode: (bytes: Uint8Array, funding: ReturnType<typeof budget>) => unknown;
  repair?(bytes: Uint8Array): void;
  also?: ReadonlyArray<(bytes: Uint8Array, funding: ReturnType<typeof budget>) => unknown>;
}
const parsed =
  (decode: (bytes: Uint8Array, b: ByteBudget) => Parsed<unknown> | null) =>
  (bytes: Uint8Array, b: ReturnType<typeof budget>) => {
    const result = decode(bytes, b);
    if (result === null) throw new Error("Unexpected optional parse refusal");
    b.free(result.charge);
    return result.value;
  };
const checksum = (bytes: Uint8Array, end = bytes.length - 4, seed = 0) =>
  new DataView(bytes.buffer, bytes.byteOffset).setUint32(end, xxHash32(bytes, seed, 0, end), true);
async function epoch(bytes: Uint8Array) {
  const shim = new MemoryOpfs();
  shim.writeFileBytes("epoch", bytes);
  // Unknown identities must be refused before even creating another native file.
  const unsupported =
    new DataView(bytes.buffer, bytes.byteOffset).getUint32(0, true) !==
      DURABLE_FORMATS.epochSlot.magic ||
    (new DataView(bytes.buffer, bytes.byteOffset).getUint32(8, true) & 0xffe00000) !== 0;
  if (unsupported)
    shim.setWriteFault(() => {
      throw new Error("mutation before format refusal");
    });
  const backend = createOpfsBackend({
    name: "registry",
    directory: shim.root,
    locks: new SimLocks(),
  });
  const lease = await backend.acquire();
  await lease.release();
  return 1;
}
function runAdapter(
  part: "runBlock" | "runBloom" | "runIndex" | "runFences" | "runBloomDirectory",
): Adapter {
  const run = encodeRun([{ key: new Uint8Array([97]), entries: [{ rowId: 1, tombstone: false }] }]);
  if (run === null) throw new Error("Missing run");
  let offset = 0;
  const extent = { offset: 4096, length: 4096 };
  const refs = run.parts.map((bytes) => {
    const ref = { extent, offset, length: bytes.length, checksum: xxHash32(bytes) };
    offset += bytes.length;
    return ref;
  });
  const target =
    part === "runBlock"
      ? 0
      : part === "runBloom"
        ? 1
        : part === "runIndex"
          ? run.parts.length - 3
          : part === "runFences"
            ? run.parts.length - 2
            : run.parts.length - 1;
  const bytes = run.parts[target];
  if (bytes === undefined) throw new Error("Missing run part");
  const spaceFor = (changed: Uint8Array): ExtentReader => ({
    peek: (ref) => {
      const index = refs.findIndex((r) => r.offset === ref.offset);
      return index === target ? changed : (run.parts[index] ?? null);
    },
    read: () => Promise.reject(new Error("Resident run unexpectedly read storage")),
  });
  const visit = async (changed: Uint8Array, b: ReturnType<typeof budget>, cursor: boolean) => {
    const space = spaceFor(changed),
      reader = await RunReader.open(space, runRef(run, refs), b, b);
    const out: Array<{ rowId: number; tombstone: boolean }> = [];
    try {
      if (cursor)
        await reader.cursor(space, b, (_key, rowId, tombstone) => {
          out.push({ rowId, tombstone });
          return false;
        });
      else await reader.lookup(space, new Uint8Array([97]), out, b);
      expect(out).toHaveLength(1); // Proves Bloom/bounds did not bypass the block.
      return out;
    } finally {
      b.free(reader.bytes);
    }
  };
  return {
    bytes,
    also:
      part === "runBlock"
        ? [(changed, b) => visit(changed, b, false), (changed, b) => visit(changed, b, true)]
        : [],
    decode: (changed, b) => {
      if (part === "runBlock") {
        const out: Array<{ key: Uint8Array; rowId: number; tombstone: boolean }> = [];
        blockEntries(changed, out);
        return out;
      }
      const space = spaceFor(changed);
      const reader = RunReader.open(space, runRef(run, refs), b, b);
      if (reader instanceof Promise) throw new Error("Unexpected asynchronous resident run");
      b.free(reader.bytes);
      return reader;
    },
  };
}
function adapters(): Record<
  Exclude<DurableFormatName, "snapshot" | "snapshotFrame" | "indexedDb">,
  Adapter
> {
  return {
    rootSlot: {
      bytes: encodeRootSlot(1, new Uint8Array([1, 2, 3])),
      decode: (bytes) => decodeRootSlot(bytes),
      repair: checksum,
    },
    epochSlot: { bytes: encodeEpochSlot(1), decode: epoch, repair: checksum },
    logHeader: {
      bytes: encodeLogHeader({
        salt: 7,
        generation: 1,
        ackEnd: 0,
        ackChain: 7,
        ackLsn: 0,
        rootSequence: 1,
      }),
      decode: decodeLogHeader,
      repair: (bytes) => checksum(bytes, 48),
    },
    logFrame: {
      bytes: encodeFrame(
        { salt: 7, lsn: 1, partIndex: 0, partCount: 1 },
        encodeBody({ request: null, effects: [] }),
        7,
      ).bytes,
      decode: (bytes) => scanLog(bytes, [null, null], manifest.log),
      repair: (bytes) => checksum(bytes, bytes.length - 4, 7),
      also: [
        async (bytes, b) => {
          let delivered = 0;
          try {
            return await scanLogIncremental(
              {
                length: bytes.length,
                read: (offset, length) => Promise.resolve(bytes.subarray(offset, offset + length)),
              },
              [null, null],
              manifest.log,
              {
                part: () => {
                  delivered++;
                },
                commit: () => undefined,
                abandon: () => undefined,
              },
              b,
            );
          } catch (error) {
            expect(delivered).toBe(0);
            throw error;
          }
        },
      ],
    },
    manifest: { bytes: encodeManifestRoot(manifest), decode: parsed(decodeManifestRoot) },
    groupRoot: {
      bytes: encodeGroupRoot(root),
      decode: parsed(decodeGroupRoot),
      also: [parsed(tryDecodeGroupRootView)],
    },
    deletionBitmap: {
      bytes: new Uint8Array([1, 0, 0, 0]),
      decode: (bytes) => decodeDeletionBitmap(bytes, 2),
    },
    groupPage: { bytes: encodeGroupPage([group]), decode: parsed(decodeGroupPage) },
    groupIndex: {
      bytes: encodeGroupIndex([
        { ref: blob, firstRowId: 1, lastRowId: 2, groupCount: 1, rowCount: 2, liveRowCount: 2 },
      ]),
      decode: parsed(decodeGroupIndex),
    },
    tableStatistics: {
      bytes: encodeTableStatistics({
        groupCount: 1,
        columns: [
          { columnId: 1, nullCount: 0, values: 2, bytes: 8, unbounded: false, min: 3, max: 9 },
        ],
      }),
      decode: parsed(decodeTableStatistics),
    },
    extentList: {
      bytes: encodeListPage([{ offset: 4096, length: 4096 }]),
      decode: (bytes) => decodeListPage(bytes, { blob, first: 4096, count: 1 }),
    },
    pageDirectory: {
      bytes: encodeDirectory(directory),
      decode: (bytes) => parseDirectory(bytes, 4096),
    },
    page: { bytes: rawPage, decode: (bytes) => parsePage(bytes, pageEntry, 2) },
    largeValue: {
      bytes: encodePointer({ offset: 0, length: LARGE_VALUE_BYTES + 1, checksums: [7] }),
      decode: (bytes) => decodePointer(bytes, 0, bytes.length),
    },
    sketch: {
      bytes: new DistinctSketch().serialize(),
      decode: (bytes) => DistinctSketch.parse(bytes),
    },
    commitBody: { bytes: encodeBody({ request: null, effects: [] }), decode: decodeBody },
    column: {
      bytes: encodeColumn(integer, 0, 2),
      decode: (bytes) => decodeColumn(bytes, integer.type),
    },
    catalog: { bytes: Catalog.empty().encode(), decode: (bytes) => Catalog.decode(bytes) },
    catalogChange: { bytes: encodeLogged({ op: "dropTable", tableId: 1 }), decode: decodeLogged },
    runBlock: runAdapter("runBlock"),
    runIndex: runAdapter("runIndex"),
    runFences: runAdapter("runFences"),
    runBloomDirectory: runAdapter("runBloomDirectory"),
    runBloom: runAdapter("runBloom"),
    arrayValue: {
      bytes: encodeArrayValue(
        { kind: "bool" },
        { dimensions: [1], lowerBounds: [1], elements: [true] },
      ),
      decode: (bytes) => decodeArrayValue({ kind: "bool" }, bytes),
    },
  };
}
function fieldValue(view: DataView, field: FormatField, value: number): void {
  if (field.width === 1) view.setUint8(field.offset, value);
  else if (field.width === 2) view.setUint16(field.offset, value, true);
  else view.setUint32(field.offset, value, true);
}
async function refused(adapter: Adapter, bytes: Uint8Array, label: string): Promise<void> {
  const before = bytes.slice();
  for (const decode of [adapter.decode, ...(adapter.also ?? [])]) {
    const b = budget();
    let failure: unknown;
    try {
      await decode(bytes, b);
    } catch (error) {
      failure = error;
    }
    expect(failure, label).toBeInstanceOf(Error);
    const code = (failure as { code?: unknown }).code;
    expect(typeof code, label).toBe("string");
    expect(code, label).toMatch(/^MINNOW_/u);
    expect(b.held, label).toBe(0);
    expect(bytes, label).toEqual(before);
  }
}
for (const seed of seedsFor("next-durable-registry", [0xb221, 0xb222]))
  it(`sweeps every registered decoder with unknown identity/tag and reserved bits seed ${String(seed)}`, async () => {
    const all = adapters();
    const names = Object.keys(all) as Array<keyof typeof all>;
    const reach = createReach("durable decoder registry", DURABLE_REGISTRY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 255 }),
        fc.integer({ min: 0, max: 31 }),
        async (unknown, bit) => {
          reach.beginHistory();
          for (const name of names) {
            const adapter = all[name],
              b = budget();
            await adapter.decode(adapter.bytes, b);
            for (const decode of adapter.also ?? []) await decode(adapter.bytes, b);
            expect(b.held, name).toBe(0);
            reach.reach(name);
            for (const field of DURABLE_FORMATS[name].fields as readonly FormatField[]) {
              const original = new DataView(adapter.bytes.buffer, adapter.bytes.byteOffset);
              const old =
                field.width === 1
                  ? original.getUint8(field.offset)
                  : field.width === 2
                    ? original.getUint16(field.offset, true)
                    : original.getUint32(field.offset, true);
              const values: number[] = [];
              if (field.values !== undefined) {
                let probe =
                  field.name === "magic" ? (Number(field.values[0]) ^ (1 << bit)) >>> 0 : unknown;
                while (field.values.includes(probe)) probe = (probe + 1) >>> 0;
                const maximum = 2 ** (8 * field.width) - 1;
                for (const value of [
                  probe,
                  0,
                  1,
                  (old | 256) >>> 0,
                  (old | 65536) >>> 0,
                  (old | 16777216) >>> 0,
                ])
                  if (value <= maximum && !field.values.includes(value)) values.push(value);
              } else
                for (let bit = 0; bit < field.width * 8; bit++)
                  if (((field.mask ?? 0) & (1 << bit)) === 0) values.push((old | (1 << bit)) >>> 0);
              for (const value of new Set(values)) {
                const bytes = adapter.bytes.slice();
                fieldValue(new DataView(bytes.buffer), field, value);
                adapter.repair?.(bytes);
                await refused(adapter, bytes, `${name}.${field.name}:${String(value)}`);
              }
            }
            // Exercise the actual named parent, not an unrelated version gate.
            const format = DURABLE_FORMATS[name];
            if (format.versionIn === "parent") {
              const parentName = format.parent as keyof typeof all;
              const parent = all[parentName];
              const field = DURABLE_FORMATS[parentName].fields.find((f) => f.name === "version");
              if (field === undefined) throw new Error(`Missing parent version: ${parentName}`);
              const bytes = parent.bytes.slice();
              fieldValue(new DataView(bytes.buffer), field, unknown);
              parent.repair?.(bytes);
              await refused(
                {
                  bytes: adapter.bytes,
                  decode: (child, b) => {
                    parent.decode(bytes, b);
                    return adapter.decode(child, b);
                  },
                },
                adapter.bytes.slice(),
                `${name}.${parentName} version`,
              );
            }
          }
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-durable-aliases", [0xb227, 0xb228]))
  it(`sweeps lazy roots and dictionary parsing independently seed ${String(seed)}`, async () => {
    const reach = createReach("durable aliases", DURABLE_ALIAS_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 255 }), async (unknown) => {
        reach.beginHistory();
        const manifestBytes = encodeManifestRoot(manifest),
          metadata = new MetadataReader(manifestBytes);
        metadata.u32();
        metadata.u8();
        const extents = metadata.count(2);
        for (let i = 0; i < extents; i++) {
          metadata.uint();
          metadata.uint();
        }
        for (const version of [0, 1, MANIFEST_FORMAT_VERSION + 1 + (unknown % 124)]) {
          const bytes = manifestBytes.slice();
          bytes[metadata.at] = version;
          await refused(
            { bytes, decode: parsed(decodeManifestRoot) },
            bytes,
            "logical manifest version",
          );
        }
        reach.reach("logical manifest");
        const rootBytes = encodeGroupRoot(root);
        rootBytes[4] = unknown;
        await refused(
          { bytes: rootBytes, decode: parsed(tryDecodeGroupRootView) },
          rootBytes,
          "lazy root version",
        );
        reach.reach("lazy root");
        const bytes = new Uint8Array(27);
        bytes.set([1, 6, 0, 4]);
        const view = new DataView(bytes.buffer);
        view.setUint32(4, 1, true);
        view.setUint32(8, 3, true);
        view.setUint32(20, 3, true);
        bytes.set([97, 98, 99], 24);
        const entry = { ...pageEntry, rowCount: 1, length: bytes.length };
        const adapter: Adapter = { bytes, decode: (bytes) => parseDictionary(bytes, entry, 6) };
        adapter.decode(bytes, budget());
        for (const field of DURABLE_FORMATS.page.fields) {
          const mutated = bytes.slice();
          fieldValue(new DataView(mutated.buffer), field, field.mask === 0 ? 1 : 255);
          await refused(adapter, mutated, `dictionary.${field.name}`);
        }
        reach.reach("dictionary");
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  });

it("has a real adapter for every implemented byte decoder and keeps snapshot design-only", () => {
  const all = adapters();
  expect(DURABLE_READER_VERSION).toBeGreaterThanOrEqual(1);
  expect(DURABLE_READER_VERSION).toBeLessThanOrEqual(8);
  expect(MANIFEST_FORMAT_VERSION).toBeGreaterThanOrEqual(2);
  expect(MANIFEST_FORMAT_VERSION).toBeLessThanOrEqual(257);
  const discovered = discoverDurableSources();
  expect([...discovered.sites.keys()].sort()).toEqual(Object.keys(DURABLE_EMISSIONS).sort());
  for (const [site, count] of discovered.sites)
    expect(count, site).toBe(DURABLE_EMISSIONS[site]?.count);
  expect(discovered.refs).toEqual(Object.keys(DURABLE_REFERENCES).sort());
  const emitted = new Set(Object.values(DURABLE_EMISSIONS).flatMap((site) => site.kinds));
  for (const kinds of Object.values(DURABLE_REFERENCES))
    for (const kind of kinds) expect(emitted.has(kind), `unemitted reference ${kind}`).toBe(true);
  expect(
    Object.keys(DURABLE_FORMATS)
      .filter(
        (name) =>
          DURABLE_FORMATS[name as DurableFormatName].state === "implemented" &&
          name !== "indexedDb",
      )
      .sort(),
  ).toEqual([...emitted].filter((kind) => kind !== "indexedDb").sort());
  expect(Object.keys(all).sort()).toEqual(
    [...emitted].filter((kind) => kind !== "indexedDb").sort(),
  );
  expect([...DURABLE_REGISTRY_REACH].sort()).toEqual(Object.keys(all).sort());
  expect(DURABLE_FORMATS.snapshot.state).toBe("design");
  expect(Object.isFrozen(DURABLE_FORMATS.page.fields)).toBe(true);
}, 60_000); // The source inventory builds a TypeScript program, independently of decoder execution.

for (const seed of seedsFor("next-durable-fields", [0xb223, 0xb224]))
  it(`refuses embedded discriminators before returning decoded values seed ${String(seed)}`, async () => {
    const reach = createReach("embedded durable fields", DURABLE_FIELD_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 255 }), async (unknown) => {
        reach.beginHistory();
        const body = encodeBody({ request: null, effects: [] });
        body[1] = unknown;
        await refused({ bytes: body, decode: decodeBody }, body, "commit request flag");
        reach.reach("request flag");
        const column = encodeColumn(integer, 0, 2);
        column[2] = unknown;
        await refused(
          { bytes: column, decode: (bytes) => decodeColumn(bytes, integer.type) },
          column,
          "column validity flag",
        );
        reach.reach("column flag");
        // NULL payloads are ignored (vector contract); the same live byte is a strict boolean.
        const boolean: FlatVector = {
          form: "flat",
          type: { kind: "bool" },
          length: 1,
          validity: new Uint32Array([0]),
          data: { layout: "bool", values: new Uint8Array([unknown]) },
        };
        expect(decodeColumn(encodeColumn(boolean, 0, 1), boolean.type).validity).toEqual(
          boolean.validity,
        );
        reach.reach("NULL payload");
        const live = encodeColumn({ ...boolean, validity: null }, 0, 1);
        await refused(
          { bytes: live, decode: (bytes) => decodeColumn(bytes, boolean.type) },
          live,
          "live boolean payload",
        );
        reach.reach("column boolean");
        const array = encodeArrayValue(
          { kind: "bool" },
          { dimensions: [1], lowerBounds: [1], elements: [true] },
        );
        array[array.length - 1] = unknown;
        await refused(
          { bytes: array, decode: (bytes) => decodeArrayValue({ kind: "bool" }, bytes) },
          array,
          "array boolean",
        );
        reach.reach("array boolean");
        const change = encodeJson({ op: `future-${String(unknown)}`, tableId: 1 });
        await refused({ bytes: change, decode: decodeLogged }, change, "catalog operation");
        reach.reach("catalog operation");
        const run = adapters().runBlock.bytes.slice();
        run[run.length - 1] = unknown | 2;
        await refused(runAdapter("runBlock"), run, "run flags");
        reach.reach("run flags");
        const nullArray = encodeArrayValue(
          { kind: "bool" },
          { dimensions: [1], lowerBounds: [1], elements: [null] },
        );
        nullArray[12] = (nullArray[12] ?? 0) | 128;
        await refused(
          { bytes: nullArray, decode: (bytes) => decodeArrayValue({ kind: "bool" }, bytes) },
          nullArray,
          "array bitmap reserved bits",
        );
        reach.reach("array bitmap");
        const padding = new Uint8Array(32);
        padding.set([1, 2, 1, 4]);
        new DataView(padding.buffer).setUint32(4, 2, true);
        padding[16] = 1;
        padding[20] = 1; // bit words end20; bytes20..23 reserved alignment.
        await refused(
          {
            bytes: padding,
            decode: (bytes) => parsePage(bytes, { ...pageEntry, nullCount: 1, length: 32 }, 2),
          },
          padding,
          "page alignment padding",
        );
        reach.reach("page padding");
        const allNull = new Uint8Array(16);
        const family = 11 + (unknown % 245);
        allNull.set([2, family, 2, 0]);
        new DataView(allNull.buffer).setUint32(4, 1, true);
        await refused(
          {
            bytes: allNull,
            decode: (bytes) =>
              parsePage(
                bytes,
                { ...pageEntry, length: 16, encoding: 2, rowCount: 1, nullCount: 1 },
                family,
              ),
          },
          allNull,
          "unknown all-null family",
        );
        const frame = adapters().logFrame;
        const invalidPadding = frame.bytes.slice();
        invalidPadding[invalidPadding.length - 5] = unknown;
        frame.repair?.(invalidPadding);
        await refused(frame, invalidPadding, "frame alignment padding");
      }),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-durable-idb", [0xb225, 0xb226]))
  it(`refuses future native IDB versions without changing the database seed ${String(seed)}`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 3, max: 100 }), async (version) => {
        const factory = new IDBFactory();
        await new Promise<void>((resolve, reject) => {
          const request = factory.open("future", version);
          request.onupgradeneeded = () =>
            request.result.createObjectStore("meta").put("kept", "witness");
          request.onsuccess = () => {
            request.result.close();
            resolve();
          };
          request.onerror = () => reject(request.error ?? new Error("fixture"));
        });
        let failure: unknown;
        try {
          const db = await openIndexedDb(factory, "future");
          db.close();
        } catch (error) {
          failure = error;
        }
        expect(failure).toBeInstanceOf(Error);
        expect(typeof (failure as { code?: unknown }).code).toBe("string");
        expect((failure as { code?: unknown }).code).toMatch(/^MINNOW_/u);
        expect(await factory.databases()).toEqual([{ name: "future", version }]);
      }),
      { seed, numRuns: 8 },
    );
  });

for (const seed of seedsFor("next-durable-native-open", [0xb229, 0xb22a]))
  it(`refuses unknown native roots and log headers before writes seed ${String(seed)}`, async () => {
    const reach = createReach("durable native", DURABLE_NATIVE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 255 }), async (unknown) => {
        reach.beginHistory();
        const all = adapters();
        for (const name of ["rootSlot", "logHeader"] as const) {
          reach.reach(name);
          const adapter = all[name],
            bytes = adapter.bytes.slice();
          const view = new DataView(bytes.buffer);
          if (name === "rootSlot")
            view.setUint32(0, (DURABLE_FORMATS.rootSlot.magic ^ unknown) >>> 0, true);
          else view.setUint32(4, unknown, true);
          adapter.repair?.(bytes);
          const shim = new MemoryOpfs();
          const file = name === "rootSlot" ? "root-0" : "log";
          shim.writeFileBytes(file, bytes);
          let writes = 0;
          shim.setWriteFault(() => {
            writes++;
            throw new Error("native mutation before format refusal");
          });
          const backend = createOpfsBackend({
            name: "future",
            directory: shim.root,
            locks: new SimLocks(),
          });
          let failure: unknown;
          try {
            const lease = await backend.acquire();
            await lease.release();
          } catch (error) {
            failure = error;
          }
          expect(failure).toBeInstanceOf(Error);
          expect(typeof (failure as { code?: unknown }).code).toBe("string");
          expect((failure as { code?: unknown }).code).toMatch(/^MINNOW_/u);
          expect(writes).toBe(0);
          const names: string[] = [];
          for await (const name of shim.root.keys()) names.push(name);
          expect(names).toEqual([file]);
          expect(shim.readFileBytes(file)).toEqual(bytes);
          reach.reach("OPFS");
          const factory = new IDBFactory();
          const raw = await openIndexedDb(factory, "future");
          await raw.transaction(["meta", "roots"], "readwrite", "strict", (tx) => {
            tx.store("meta").put("epoch", 7);
            tx.store(name === "rootSlot" ? "roots" : "meta").put(
              name === "rootSlot" ? "slot-0" : "header-0",
              bytes.slice().buffer,
            );
          });
          const idb = createIndexedDbBackend({
            name: "future",
            database: () => raw,
            locks: new SimLocks(),
          });
          let idbFailure: unknown;
          try {
            const lease = await idb.acquire();
            await lease.release();
          } catch (error) {
            idbFailure = error;
          }
          expect(idbFailure).toBeInstanceOf(Error);
          expect(typeof (idbFailure as { code?: unknown }).code).toBe("string");
          expect((idbFailure as { code?: unknown }).code).toMatch(/^MINNOW_/u);
          const verify = await openIndexedDb(factory, "future");
          try {
            await verify.transaction(["meta", "roots"], "readonly", "relaxed", (tx) => {
              tx.store("meta").get("epoch", (value) => expect(value).toBe(7));
              tx.store(name === "rootSlot" ? "roots" : "meta").get(
                name === "rootSlot" ? "slot-0" : "header-0",
                (value) => expect(new Uint8Array(value as ArrayBuffer)).toEqual(bytes),
              );
            });
          } finally {
            verify.close();
          }
          reach.reach("IDB");
        }
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-durable-catalog", [0xb22b, 0xb22c]))
  it(`refuses unknown catalog and scalar tags before publishing a catalog seed ${String(seed)}`, async () => {
    const reach = createReach("durable catalog", DURABLE_CATALOG_REACH);
    const source = Catalog.empty().change(
      {
        kind: "createTable",
        table: {
          name: "registry",
          columns: [
            {
              name: "id",
              type: { kind: "int32", bits: 32 },
              declared: { type: "number" },
              nullable: false,
              defaultSql: null,
              generatedSql: null,
              identitySequence: null,
              missing: null,
            },
          ],
          primaryKey: [0],
          unique: [],
          foreignKeys: [
            {
              name: "self",
              columns: [0],
              parentTableId: "self",
              parentIndexId: "primary",
              onDelete: "restrict",
              enforced: true,
            },
          ],
          checks: [],
        },
      },
      1,
    ).catalog;
    const text = new TextDecoder().decode(source.encode());
    const clone = () =>
      JSON.parse(text) as {
        version: unknown;
        tables: Array<{
          columns: Array<{ type: { kind: string }; missing: unknown }>;
          indexes: Array<{ kind: string; role: string }>;
          foreignKeys: Array<{ onDelete: string }>;
        }>;
      };
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 255 }), async (unknown) => {
        reach.beginHistory();
        for (const kind of DURABLE_CATALOG_REACH) {
          reach.reach(kind);
          const data = clone(),
            table = data.tables[0],
            column = table?.columns[0],
            index = table?.indexes[0],
            foreign = table?.foreignKeys[0];
          if (
            table === undefined ||
            column === undefined ||
            index === undefined ||
            foreign === undefined
          )
            throw new Error("Incomplete genuine catalog fixture");
          const future = `future-${String(unknown)}`;
          if (kind === "physical") column.type.kind = future;
          if (kind === "index kind") index.kind = future;
          if (kind === "index role") index.role = future;
          if (kind === "foreign action") foreign.onDelete = future;
          if (kind === "scalar number") data.version = { $number: future };
          if (kind === "missing form") column.missing = { ...integer, length: 1, form: future };
          if (kind === "missing layout")
            column.missing = { ...integer, length: 1, data: { ...integer.data, layout: future } };
          const bytes = encodeJson(data);
          await refused(
            { bytes, decode: (bytes) => Catalog.decode(bytes) },
            bytes,
            `catalog ${kind}`,
          );
        }
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-durable-header-read", [0xb22d, 0xb22e]))
  it(`never treats an unread epoch header as an empty store seed ${String(seed)}`, async () => {
    const reach = createReach("durable read", DURABLE_READ_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (blocked) => {
        reach.beginHistory();
        reach.reach(blocked ? "blocked" : "progress");
        const shim = new MemoryOpfs();
        for (const name of [
          "log",
          "data",
          "root-0",
          "root-1",
          "spill",
          "epoch",
          "space-meta",
          "space-meta-index",
          "spill-meta",
        ])
          shim.writeFileBytes(name, name === "epoch" ? encodeEpochSlot(7) : new Uint8Array());
        const original = shim.readFileBytes("epoch");
        let writes = 0;
        shim.setWriteFault(() => {
          writes++;
        });
        if (blocked)
          shim.setTransferLimit((path, operation) =>
            path === "epoch" && operation === "read" ? 0 : undefined,
          );
        const backend = createOpfsBackend({
          name: "header",
          directory: shim.root,
          locks: new SimLocks(),
        });
        let failure: unknown;
        try {
          const lease = await backend.acquire();
          expect(lease.epoch).toBe(8);
          await lease.release();
        } catch (error) {
          failure = error;
        }
        if (blocked) {
          expect(writes).toBe(0);
          expect(shim.readFileBytes("epoch")).toEqual(original);
          expect(failure).toBeInstanceOf(Error);
          expect((failure as { code?: unknown }).code).toBe("MINNOW_STORE_CORRUPT");
        } else {
          expect(failure).toBeUndefined();
          expect(writes).toBeGreaterThan(0);
        }
      }),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  });

// First supported1.0 root envelope: newer inner codecs must refuse BEFORE fencing.
for (const seed of seedsFor("next-durable-before-fence", [0xb22f, 0xb230]))
  it(`refuses a newer root before all native mutations seed ${String(seed)}`, async () => {
    const reach = createReach("durable startup", DURABLE_STARTUP_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: DURABLE_READER_VERSION + 1, max: 8 }), async (future) => {
        reach.beginHistory();
        for (const field of ["minimum reader", "manifest format"] as const)
          for (const slot of ["root-0", "root-1"] as const)
            for (const [complete, stale] of [
              [false, false],
              [false, true],
              [true, false],
              [true, true],
            ] as const) {
              reach.reach(stale ? "stale snapshot" : "fresh snapshot");
              reach.reach(field);
              reach.reach(slot);
              reach.reach(complete ? "complete tree" : "partial tree");
              const bytes = encodeRootSlot(9, encodeManifestRoot(manifest));
              const header = new DataView(bytes.buffer);
              if (field === "minimum reader")
                header.setUint8(10, (header.getUint8(10) & 31) | ((future - 1) << 5));
              else header.setUint8(11, future - 1); // manifest version future+1, biased by2.
              checksum(bytes);
              const shim = new MemoryOpfs();
              // Good older slot must not conceal a future current/previous slot.
              const image = new Map<string, Uint8Array>([
                [slot, bytes],
                [slot === "root-0" ? "root-1" : "root-0", encodeRootSlot(8, new Uint8Array([9]))],
                ["epoch", encodeEpochSlot(7)],
              ]);
              if (complete)
                for (const name of [
                  "log",
                  "data",
                  "spill",
                  "space-meta",
                  "space-meta-index",
                  "spill-meta",
                ])
                  image.set(name, new Uint8Array([19, 31, 47]));
              for (const [name, contents] of image) shim.writeFileBytes(name, contents);
              const directory: OpfsDirectoryLike = {
                keys: () => shim.root.keys(),
                getFileHandle: async (name, options) => {
                  const handle = await shim.root.getFileHandle(name, options);
                  return {
                    createSyncAccessHandle: () => handle.createSyncAccessHandle(),
                    getFile: async () => {
                      if (stale && name === slot) {
                        // Snapshot the older root, then let the departing writer publish the
                        // future root before the incoming owner obtains its exclusive handle.
                        const before = new Blob([
                          encodeRootSlot(8, new Uint8Array([9])).slice().buffer,
                        ]);
                        return before;
                      }
                      return handle.getFile();
                    },
                  };
                },
              };
              let opfsWrites = 0;
              shim.setWriteFault(() => {
                opfsWrites++;
              });
              await expect(
                createOpfsBackend({
                  name: "future",
                  directory,
                  locks: new SimLocks(),
                }).acquire(),
              ).rejects.toMatchObject({ code: "MINNOW_FORMAT_UNSUPPORTED" });
              expect(opfsWrites).toBe(0);
              const after = new Map<string, Uint8Array | undefined>();
              for await (const name of shim.root.keys()) after.set(name, shim.readFileBytes(name));
              expect(after).toEqual(image);
              reach.reach("OPFS unchanged");

              const factory = new IDBFactory();
              const raw = await openIndexedDb(factory, "future");
              const witness = new Uint8Array([7, 13, 23]).buffer;
              await raw.transaction(
                ["meta", "roots", "log", "data"],
                "readwrite",
                "strict",
                (tx) => {
                  tx.store("meta").put("epoch", 7);
                  tx.store("roots").put("slot-0", bytes.slice().buffer);
                  for (const name of ["log", "data", "meta"]) tx.store(name).put(0, witness);
                },
              );
              let idbWrites = 0;
              const backend = createIndexedDbBackend({
                name: "future",
                locks: new SimLocks(),
                database: () => ({
                  close: () => raw.close(),
                  transaction: (stores, mode, durability, body) =>
                    raw.transaction(stores, mode, durability, (tx) =>
                      body({
                        abort: (error) => tx.abort(error),
                        store: (name) => {
                          const store = tx.store(name);
                          return {
                            ...store,
                            put: (key, value) => {
                              idbWrites++;
                              store.put(key, value);
                            },
                            delete: (key) => {
                              idbWrites++;
                              store.delete(key);
                            },
                          };
                        },
                      }),
                    ),
                }),
              });
              await expect(backend.acquire()).rejects.toMatchObject({
                code: "MINNOW_FORMAT_UNSUPPORTED",
              });
              expect(idbWrites).toBe(0);
              const verify = await openIndexedDb(factory, "future");
              try {
                await verify.transaction(
                  ["meta", "roots", "log", "data"],
                  "readonly",
                  "relaxed",
                  (tx) => {
                    tx.store("meta").get("epoch", (value) => expect(value).toBe(7));
                    tx.store("roots").get("slot-0", (value) =>
                      expect(new Uint8Array(value as ArrayBuffer)).toEqual(bytes),
                    );
                    for (const name of ["log", "data", "meta"])
                      tx.store(name).get(0, (value) => expect(value).toEqual(witness));
                  },
                );
              } finally {
                verify.close();
              }
              reach.reach("IDB unchanged");
              reach.reach("zero writes");
            }
      }),
      { seed, numRuns: 9 },
    );
    reach.assertReached();
  });
