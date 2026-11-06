/**
 * Gate A's reopen, at the storage layer (next-storage.spec.ts, opt-in): a space holding a million
 * blobs is built once, then reopened in fresh workers, timing each part: the owner's fence
 * (`acquire`), the store session (`backend.open`: on IndexedDB this loads and validates the data
 * and log record indexes) and the extent space (`openExtentSpace`: the walk of every extent the
 * current and previous roots reach, and the free map built from them).
 *
 * The root names manifest blobs, each a binary list of extents (like an engine's catalog naming
 * its column chunks), so the walk reads the manifests and yields every extent, as an engine's
 * would; blobs inside an extent need no entry of their own.
 *
 * Layouts:
 * - `packed`: 1,000 blobs of 1 KiB per extent (an engine's page-sized blobs in column-chunk
 *   extents). Few extents, and on IndexedDB few records (about data bytes / 256 KiB).
 * - `scattered`: every blob its own extent, written alone (each one record on IndexedDB): the
 *   worst case for the record index and for the walk.
 */
import type { StorageBackend } from "../src/next/storage/contract.js";
import type { BlobRef, ExtentRef, ReachableExtents } from "../src/next/storage/extents.js";
import { ByteStore } from "../src/next/storage/idb-bytes.js";
import type { IdbDatabaseLike, IdbTransactionLike } from "../src/next/storage/idb-like.js";
import { openExtentSpace } from "../src/next/storage/space.js";
import { bytesOf, countingBudget, countingMemory } from "../src/next/storage/space-test-support.js";

export type ReopenLayout = "packed" | "scattered";

/** Extents per manifest blob (16 bytes each: under the 1 MiB blob cap). */
const PER_MANIFEST = 60_000;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** The root: the manifests. */
const encodeRoot = (manifests: readonly BlobRef[]) => encoder.encode(JSON.stringify({ manifests }));

function encodeManifest(extents: readonly ExtentRef[]): Uint8Array {
  const values = new Float64Array(extents.length * 2);
  extents.forEach((extent, index) => {
    values[2 * index] = extent.offset;
    values[2 * index + 1] = extent.length;
  });
  return new Uint8Array(values.buffer);
}

/** Every manifest, then every extent each manifest lists. */
export const manifestReachable: ReachableExtents = async function* (root, reader, budget) {
  const { manifests } = JSON.parse(decoder.decode(root)) as { manifests: BlobRef[] };
  yield* manifests;
  for (const manifest of manifests) {
    const lease = await reader.read([manifest], { budget });
    const [bytes] = lease.bytes;
    if (bytes === undefined) throw new Error("manifest missing");
    const values = new Float64Array(bytes.slice().buffer);
    lease.release();
    for (let at = 0; at < values.length; at += 2)
      yield { offset: values[at] ?? 0, length: values[at + 1] ?? 0 };
  }
};

export interface ReopenBuild {
  readonly layout: ReopenLayout;
  readonly blobs: number;
  readonly extents: number;
  readonly dataBytes: number;
  readonly buildMs: number;
}

/** Builds the space: `blobs` blobs in `layout`, one root naming them through manifests. */
export async function buildReopenSpace(
  backend: StorageBackend,
  layout: ReopenLayout,
  blobs: number,
): Promise<ReopenBuild> {
  const started = performance.now();
  const lease = await backend.acquire({ timeoutMs: 30_000 });
  const session = await backend.open(lease);
  const space = await openExtentSpace(session, {
    reachable: manifestReachable,
    budget: countingBudget(Infinity),
    registerCache: () => countingMemory(64 << 20),
  });
  const extents: ExtentRef[] = [];
  const writer = space.beginWrite(
    "reopen",
    layout === "scattered" ? { budget: countingBudget(Infinity) } : {},
  );
  if (layout === "packed") {
    const page = bytesOf(1024, 1);
    for (let written = 0; written < blobs; written += 1_000) {
      const parts = Array.from({ length: Math.min(1_000, blobs - written) }, () => page);
      extents.push((await writer.write(parts)).extent);
    }
  } else {
    const blob = bytesOf(100, 2);
    for (let written = 0; written < blobs; written++)
      extents.push((await writer.write([blob])).extent);
  }
  const manifests: BlobRef[] = [];
  for (let at = 0; at < extents.length; at += PER_MANIFEST)
    manifests.push(
      ...(await writer.write([encodeManifest(extents.slice(at, at + PER_MANIFEST))])).blobs,
    );
  await space.publish(encodeRoot(manifests), { writers: [writer], retired: [] });
  const dataBytes = space.stats.dataBytes;
  await space.close();
  await session.close();
  await lease.release();
  return {
    layout,
    blobs,
    extents: extents.length,
    dataBytes,
    buildMs: Math.round(performance.now() - started),
  };
}

export interface ReopenTiming {
  readonly acquireMs: number;
  /** The store session: on IndexedDB, loading and validating the record indexes. */
  readonly sessionMs: number;
  /** The extent space: the walk of both roots and the free map. */
  readonly spaceMs: number;
  readonly totalMs: number;
  readonly dataBytes: number;
}

/** Reopens the space and times each part (call it in a fresh worker: a new process). */
export async function measureReopen(backend: StorageBackend): Promise<ReopenTiming> {
  const t0 = performance.now();
  const lease = await backend.acquire({ timeoutMs: 30_000 });
  const t1 = performance.now();
  const session = await backend.open(lease);
  const t2 = performance.now();
  const space = await openExtentSpace(session, {
    reachable: manifestReachable,
    budget: countingBudget(Infinity),
    registerCache: () => countingMemory(64 << 20),
  });
  const t3 = performance.now();
  const dataBytes = space.stats.dataBytes;
  await space.close();
  await session.close();
  await lease.release();
  return {
    acquireMs: Math.round(t1 - t0),
    sessionMs: Math.round(t2 - t1),
    spaceMs: Math.round(t3 - t2),
    totalMs: Math.round(t3 - t0),
    dataBytes,
  };
}

/**
 * The IndexedDB requests open makes, timed alone (each in its own readonly transaction, whole
 * range): the record index's entries (`getAll`), its keys, and the record store's keys.
 */
export async function measureIdbOpenRequests(db: IdbDatabaseLike): Promise<Record<string, number>> {
  const time = async (
    stores: string[],
    body: (tx: IdbTransactionLike, done: () => void) => void,
  ) => {
    const started = performance.now();
    await db.transaction(stores, "readonly", "relaxed", (tx) => {
      body(tx, () => undefined);
    });
    return Math.round(performance.now() - started);
  };
  let entries = 0;
  const indexEntriesMs = await time(["data-index"], (tx) => {
    tx.store("data-index").getAll({}, (values) => {
      entries = values.length;
    });
  });
  const indexKeysMs = await time(["data-index"], (tx) => {
    tx.store("data-index").getAllKeys({}, () => undefined);
  });
  const recordKeysMs = await time(["data"], (tx) => {
    tx.store("data").getAllKeys({}, () => undefined);
  });
  const started = performance.now();
  const store = await ByteStore.load(db, "data");
  const loadMs = Math.round(performance.now() - started);
  return { entries, indexEntriesMs, indexKeysMs, recordKeysMs, loadMs, records: store.records };
}
