import { crc32 } from "@minnowdb/core/block-format";
import { IndexedDbBlockStore } from "@minnowdb/core/storage/indexeddb";
import {
  manifestRecordRetainedReservationBytes,
  type Manifest,
  type StorageIntegrityReport,
} from "@minnowdb/core/storage/contracts";

const NOW = "2026-09-14T12:00:00.000Z";

interface IntegrityIssueSummary {
  code: string;
  location: string;
  message: string;
}

interface FreshPrefixResult {
  discoveryPages: [number, number];
  concurrentlyPrunedVersions: number[];
  prunedBoundaryIntegrity: boolean;
  firstRemoval: number;
  versionsAfterFirstRemoval: number[];
  firstIntegrity: boolean;
  reopenedIntegrity: boolean;
  finalRemoval: number;
  finalVersions: number[];
  finalIntegrity: boolean;
}

interface LegacyPrefixResult {
  initialIntegrity: boolean;
  extraGapIssue: IntegrityIssueSummary | null;
  malformedMarkerIssue: IntegrityIssueSummary | null;
  missingBoundaryIssue: IntegrityIssueSummary | null;
  missingBoundaryError: { name: string; message: string } | null;
  firstRemoval: number;
  versionsAfterFirstRemoval: number[];
  reopenedRemovals: [number, number, number];
  finalVersions: number[];
  finalIntegrity: boolean;
}

function manifests(versions: readonly number[], prunedBelow: number): Manifest[] {
  return versions.map((version) => ({
    version,
    previousVersion: version === 0 ? null : version - 1,
    liveBlockCount: 0,
    liveBlockBytes: 0,
    changedTableIds: [],
    createdAt: NOW,
    ...(version < prunedBelow ? { prunedAt: NOW } : {}),
  }));
}

function recordLedger(records: readonly Manifest[]) {
  const manifestBytes = records.reduce(
    (total, record) => total + manifestRecordRetainedReservationBytes(record),
    0,
  );
  const manifestCount = records.length;
  const segmentCount = 0;
  const segmentBytes = 0;
  return {
    manifestCount,
    manifestBytes,
    segmentCount,
    segmentBytes,
    checksum: crc32(
      new TextEncoder().encode(
        `${String(manifestCount)}:${String(manifestBytes)}:${String(segmentCount)}:${String(segmentBytes)}`,
      ),
    ),
  };
}

async function mutate(
  name: string,
  stores: string | string[],
  write: (transaction: IDBTransaction) => void,
): Promise<void> {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("Native IndexedDB open failed"));
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = database.transaction(stores, "readwrite");
      write(transaction);
      transaction.oncomplete = () => resolve();
      transaction.onabort = () =>
        reject(transaction.error ?? new Error("Native IndexedDB mutation aborted"));
      transaction.onerror = () =>
        reject(transaction.error ?? new Error("Native IndexedDB mutation failed"));
    });
  } finally {
    database.close();
  }
}

async function seed(
  name: string,
  records: readonly Manifest[],
  currentVersion: number,
  cleanup?: { phase: "delete"; safeBelow: number; beforeVersion: number },
): Promise<void> {
  await mutate(name, ["catalog", "manifests", "statistics"], (transaction) => {
    const manifestStore = transaction.objectStore("manifests");
    for (const record of records) manifestStore.put(record, record.version);
    const catalog = transaction.objectStore("catalog");
    catalog.put(currentVersion, "manifest/current");
    if (cleanup !== undefined) catalog.put(cleanup, "manifest/prune-cleanup");
    transaction.objectStore("statistics").put(recordLedger(records), "resource/records");
  });
}

async function versions(store: IndexedDbBlockStore): Promise<number[]> {
  return (await store.listManifestPage(null, 32)).records.map(({ version }) => version);
}

function exactIssue(
  report: StorageIntegrityReport,
  code: string,
  location: string,
): IntegrityIssueSummary | null {
  const found = report.issues.find((issue) => issue.code === code && issue.location === location);
  return found === undefined
    ? null
    : { code: found.code, location: found.location, message: found.message };
}

function serializedError(error: unknown): { name: string; message: string } {
  return error instanceof Error
    ? { name: error.name, message: error.message }
    : { name: "NonError", message: String(error) };
}

export async function runNativeIndexedDbFreshManifestPrefix(): Promise<FreshPrefixResult> {
  const name = `manifest-prefix-idb-fresh-${crypto.randomUUID()}`;
  let store: IndexedDbBlockStore | undefined = await IndexedDbBlockStore.open({ name, indexedDB });
  try {
    const records = manifests(
      Array.from({ length: 10 }, (_, version) => version),
      8,
    );
    await seed(name, records, 9);
    const discoveryPages: [number, number] = [
      await store.removePrunedManifestRecords(5),
      await store.removePrunedManifestRecords(5),
    ];
    const concurrentCollection = await store.createGarbageCollectionJob({
      id: "prune-scanned-boundary",
      candidateManifestVersions: [8],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: NOW,
      createdAt: NOW,
    });
    const concurrentStep = await store.runGarbageCollectionStep({
      jobId: concurrentCollection.id,
      expectedRevision: concurrentCollection.revision,
      maxItems: 1,
      updatedAt: NOW,
    });
    const concurrentlyPrunedVersions = concurrentStep.prunedManifestVersions;
    const prunedBoundaryIntegrity = (await store.checkIntegrity()).ok;
    const firstRemoval = await store.removePrunedManifestRecords(5);
    const versionsAfterFirstRemoval = await versions(store);
    const firstIntegrity = (await store.checkIntegrity()).ok;
    store.close();
    store = undefined;

    store = await IndexedDbBlockStore.open({ name, indexedDB });
    const reopenedIntegrity = (await store.checkIntegrity()).ok;
    const finalRemoval = await store.removePrunedManifestRecords(5);
    const finalVersions = await versions(store);
    const finalIntegrity = (await store.checkIntegrity()).ok;
    return {
      discoveryPages,
      concurrentlyPrunedVersions,
      prunedBoundaryIntegrity,
      firstRemoval,
      versionsAfterFirstRemoval,
      firstIntegrity,
      reopenedIntegrity,
      finalRemoval,
      finalVersions,
      finalIntegrity,
    };
  } finally {
    store?.close();
  }
}

export async function runNativeIndexedDbLegacyManifestPrefix(): Promise<LegacyPrefixResult> {
  const name = `manifest-prefix-idb-legacy-${crypto.randomUUID()}`;
  let store: IndexedDbBlockStore | undefined = await IndexedDbBlockStore.open({ name, indexedDB });
  try {
    const records = manifests([0, 1, 2, 3, 4, 5, 8, 9], 8);
    const cleanup = { phase: "delete", safeBelow: 8, beforeVersion: 6 } as const;
    await seed(name, records, 9, cleanup);
    const initialIntegrity = (await store.checkIntegrity()).ok;

    const version4 = records.find(({ version }) => version === 4);
    const version8 = records.find(({ version }) => version === 8);
    if (version4 === undefined || version8 === undefined) {
      throw new Error("Legacy cleanup fixture is incomplete");
    }
    await mutate(name, "manifests", (transaction) => {
      transaction.objectStore("manifests").delete(4);
    });
    const extraGapIssue = exactIssue(await store.checkIntegrity(), "invalid-manifest", "5");
    await mutate(name, "manifests", (transaction) => {
      transaction.objectStore("manifests").put(version4, 4);
    });

    await mutate(name, "catalog", (transaction) => {
      transaction
        .objectStore("catalog")
        .put({ phase: "delete", safeBelow: 8, beforeVersion: 9 }, "manifest/prune-cleanup");
    });
    const malformedMarkerIssue = exactIssue(
      await store.checkIntegrity(),
      "invalid-catalog-record",
      "manifest/prune-cleanup",
    );
    await mutate(name, "catalog", (transaction) => {
      transaction.objectStore("catalog").put(cleanup, "manifest/prune-cleanup");
    });

    await mutate(name, "manifests", (transaction) => {
      transaction.objectStore("manifests").delete(8);
    });
    const missingBoundaryIssue = exactIssue(
      await store.checkIntegrity(),
      "invalid-manifest-prune-cleanup",
      "manifest/prune-cleanup",
    );
    let missingBoundaryError: { name: string; message: string } | null = null;
    try {
      await store.removePrunedManifestRecords(8);
    } catch (error) {
      missingBoundaryError = serializedError(error);
    }
    await mutate(name, "manifests", (transaction) => {
      transaction.objectStore("manifests").put(version8, 8);
    });

    const firstRemoval = await store.removePrunedManifestRecords(2);
    const versionsAfterFirstRemoval = await versions(store);
    store.close();
    store = undefined;

    store = await IndexedDbBlockStore.open({ name, indexedDB });
    const reopenedRemovals: [number, number, number] = [
      await store.removePrunedManifestRecords(2),
      await store.removePrunedManifestRecords(2),
      await store.removePrunedManifestRecords(2),
    ];
    const finalVersions = await versions(store);
    const finalIntegrity = (await store.checkIntegrity()).ok;
    return {
      initialIntegrity,
      extraGapIssue,
      malformedMarkerIssue,
      missingBoundaryIssue,
      missingBoundaryError,
      firstRemoval,
      versionsAfterFirstRemoval,
      reopenedRemovals,
      finalVersions,
      finalIntegrity,
    };
  } finally {
    store?.close();
  }
}
