import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { crc32, encodeBlock } from "../block-format/index.js";
import {
  decodeSnapshotMetadataItems,
  extendSnapshotFrameStreamChecksum,
  snapshotFrameEnvelopeParts,
  snapshotFrameStreamHeaderIdentity,
} from "./snapshot-stream.js";
import {
  IndexedDbBlockStore,
  BlockReadBatchTooLargeError,
  LeaseExpiredError,
  LeaseOwnerConflictError,
  MAX_MAINTENANCE_BATCH_ITEMS,
  MAX_MANIFEST_BLOCK_PRESENCE_IDS,
  MAX_MANIFEST_RECORDS,
  MAX_MANIFEST_RETAINED_BYTES,
  MAX_BLOCK_READ_BATCH_BYTES,
  MAX_COMPLETED_GARBAGE_COLLECTION_JOB_RECORDS,
  MAX_CATALOG_RECORDS,
  MAX_CATALOG_RETAINED_BYTES,
  MAX_GLOBAL_STAGED_BLOCKS,
  MAX_PINNED_MANIFEST_VERSION_LAG,
  MAX_PINNED_RETIRED_BLOCKS,
  MAX_PINNED_RETIRED_BYTES,
  MAX_RETIRED_HISTORY_BYTES,
  MAX_SEGMENT_RECORDS,
  MAX_SEGMENT_RETAINED_BYTES,
  MAX_TERMINAL_COMPACTION_JOB_RECORDS,
  MAX_TERMINAL_TRANSACTION_RECORDS,
  MAX_STORAGE_BULK_READ_ITEMS,
  MAX_STORAGE_DATABASE_NAME_CHARACTERS,
  MAX_STORAGE_ID_CHARACTERS,
  MAX_TEMP_RUN_BATCH_BYTES,
  MAX_TEMP_RUN_PAGE_BYTES,
  MAX_TEMP_RUN_PAGES_PER_BATCH,
  MAX_TRANSACTION_PENDING_BLOCKS,
  MAX_TRANSACTION_STAGE_BLOCKS,
  MAX_TRANSACTION_STAGE_SEGMENTS,
  SchemaConflictError,
  SnapshotImportConflictError,
  StorageCorruptionError,
  StorageResourceLimitError,
  UniqueKeyConflictError,
  UniqueKeyBuildConflictError,
  catalogRecordRetainedBytes,
  manifestRecordRetainedReservationBytes,
  secondaryUniqueKeyNamespace,
  segmentRecordRetainedBytes,
  TableInUseError,
  type CompactionJobRecord,
  type IndexedDbBlockStoreOptions,
  type SegmentRecord,
  type TransactionRecord,
  type SnapshotFrame,
  type SnapshotFrameFooter,
  type SnapshotFrameStreamHeader,
  type TableRecord,
} from "./index.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(300_000) });

const NOW = "2026-08-24T12:00:00.000Z";

function catalogLedger(recordCount: number, retainedBytes: number) {
  return {
    recordCount,
    retainedBytes,
    checksum: crc32(new TextEncoder().encode(`${String(recordCount)}:${String(retainedBytes)}`)),
  };
}
const COPY_COMPACTION_FIELDS = {
  rewritePlan: { kind: "copy-v1" },
  outputCursor: null,
  memoryBudgetBytes: 0,
  minimumMemoryBytes: 0,
  level0SourceStoredBytes: 0,
  anchorSourceStoredBytes: 0,
  peakWorkingBytes: 0,
  outputLogicalBytes: 0,
} as const satisfies Pick<
  CompactionJobRecord,
  | "rewritePlan"
  | "outputCursor"
  | "memoryBudgetBytes"
  | "minimumMemoryBytes"
  | "level0SourceStoredBytes"
  | "anchorSourceStoredBytes"
  | "peakWorkingBytes"
  | "outputLogicalBytes"
>;

async function openStore(indexedDB: IDBFactory, name = crypto.randomUUID()) {
  return IndexedDbBlockStore.open({ name, indexedDB } satisfies IndexedDbBlockStoreOptions);
}

interface ExportedSnapshotFrames {
  header: SnapshotFrameStreamHeader;
  frames: SnapshotFrame[];
  footer: SnapshotFrameFooter;
}

async function exportSnapshotFrames(source: IndexedDbBlockStore): Promise<ExportedSnapshotFrames> {
  const ownerId = `snapshot-export-${crypto.randomUUID()}`;
  const session = await source.beginSnapshotFrameExport({
    ownerId,
    createdAt: NOW,
    expiresAt: "2026-08-24T12:30:00.000Z",
  });
  const frameCount = Object.values(session.header.kinds).reduce(
    (total, summary) => total + summary.frameCount,
    0,
  );
  const frames: SnapshotFrame[] = [];
  let checksum = 0;
  let itemCount = 0;
  let storedBytes = 0;
  try {
    for (let sequence = 0; sequence < frameCount; sequence += 1) {
      const frame = await source.readSnapshotExportFrame({
        sessionId: session.sessionId,
        ownerId,
        sequence,
        expiresAtCutoff: NOW,
        expiresAt: "2026-08-24T12:30:00.000Z",
      });
      if (frame === undefined) throw new Error(`Snapshot frame ${String(sequence)} is missing`);
      frames.push(frame);
      checksum = extendSnapshotFrameStreamChecksum(checksum, snapshotFrameEnvelopeParts(frame));
      itemCount += frame.itemCount;
      storedBytes += frame.payload.byteLength;
    }
    return {
      header: session.header,
      frames,
      footer: { frameCount, itemCount, storedBytes, checksum },
    };
  } finally {
    await source.closeSnapshotFrameExport({ sessionId: session.sessionId, ownerId });
  }
}

async function importSnapshotFrames(
  target: IndexedDbBlockStore,
  snapshot: ExportedSnapshotFrames,
): Promise<void> {
  const ownerId = `snapshot-import-${crypto.randomUUID()}`;
  const identity = snapshotFrameStreamHeaderIdentity(snapshot.header);
  await target.beginSnapshotFrameImport({
    identity,
    ownerId,
    createdAt: NOW,
    expiresAt: "2026-08-24T12:30:00.000Z",
    header: snapshot.header,
  });
  for (let start = 0; start < snapshot.frames.length; start += 4) {
    await target.appendSnapshotImportFrames({
      identity,
      ownerId,
      expiresAtCutoff: NOW,
      expiresAt: "2026-08-24T12:30:00.000Z",
      frames: snapshot.frames.slice(start, start + 4),
    });
  }
  await target.finishSnapshotFrameImport({
    identity,
    ownerId,
    expiresAtCutoff: NOW,
    footer: snapshot.footer,
  });
}

function abortNextColumnDropAfterSegmentWrite(indexedDB: IDBFactory): () => void {
  let armed = false;
  const originalOpen = indexedDB.open.bind(indexedDB);
  Object.defineProperty(indexedDB, "open", {
    configurable: true,
    value: (name: string, version?: number) => {
      const request = version === undefined ? originalOpen(name) : originalOpen(name, version);
      request.addEventListener("success", () => {
        const database = request.result;
        const originalTransaction = database.transaction.bind(database);
        Object.defineProperty(database, "transaction", {
          configurable: true,
          value: (
            stores: string | string[],
            mode?: IDBTransactionMode,
            options?: IDBTransactionOptions,
          ) => {
            const transaction = originalTransaction(stores, mode, options);
            const storeNames = typeof stores === "string" ? [stores] : stores;
            if (
              !armed ||
              mode !== "readwrite" ||
              !storeNames.includes("catalog") ||
              !storeNames.includes("manifests") ||
              !storeNames.includes("segments")
            ) {
              return transaction;
            }
            armed = false;
            const segmentStore = transaction.objectStore("segments");
            const originalPut = segmentStore.put.bind(segmentStore);
            Object.defineProperty(segmentStore, "put", {
              configurable: true,
              value: (value: unknown, key?: IDBValidKey) => {
                const putRequest = key === undefined ? originalPut(value) : originalPut(value, key);
                transaction.abort();
                return putRequest;
              },
            });
            return transaction;
          },
        });
      });
      return request;
    },
  });
  return () => {
    armed = true;
  };
}

function abortNextSnapshotTakeoverAfterClear(indexedDB: IDBFactory): () => void {
  let armed = false;
  const originalOpen = indexedDB.open.bind(indexedDB);
  Object.defineProperty(indexedDB, "open", {
    configurable: true,
    value: (name: string, version?: number) => {
      const request = version === undefined ? originalOpen(name) : originalOpen(name, version);
      request.addEventListener("success", () => {
        const database = request.result;
        const originalTransaction = database.transaction.bind(database);
        Object.defineProperty(database, "transaction", {
          configurable: true,
          value: (
            stores: string | string[],
            mode?: IDBTransactionMode,
            options?: IDBTransactionOptions,
          ) => {
            const transaction = originalTransaction(stores, mode, options);
            const storeNames = typeof stores === "string" ? [stores] : stores;
            if (
              !armed ||
              mode !== "readwrite" ||
              !storeNames.includes("snapshotHeaders") ||
              !storeNames.includes("statistics")
            ) {
              return transaction;
            }
            armed = false;
            const statistics = transaction.objectStore("statistics");
            const originalPut = statistics.put.bind(statistics);
            Object.defineProperty(statistics, "put", {
              configurable: true,
              value: (value: unknown, key?: IDBValidKey) => {
                const putRequest = key === undefined ? originalPut(value) : originalPut(value, key);
                if (key === "resource/global") transaction.abort();
                return putRequest;
              },
            });
            return transaction;
          },
        });
      });
      return request;
    },
  });
  return () => {
    armed = true;
  };
}

async function mutate(
  indexedDB: IDBFactory,
  name: string,
  stores: string | string[],
  write: (transaction: IDBTransaction) => void,
): Promise<void> {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("raw IndexedDB open failed"));
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = database.transaction(stores, "readwrite");
      write(transaction);
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error("raw mutation aborted"));
      transaction.onerror = () => reject(transaction.error ?? new Error("raw mutation failed"));
    });
  } finally {
    database.close();
  }
}

function adjustRawRecordLedger(
  transaction: IDBTransaction,
  delta: {
    manifestCount?: number;
    manifestBytes?: number;
    segmentCount?: number;
    segmentBytes?: number;
  },
): void {
  const statistics = transaction.objectStore("statistics");
  const request = statistics.get("resource/records");
  request.onsuccess = () => {
    const current = request.result as {
      manifestCount: number;
      manifestBytes: number;
      segmentCount: number;
      segmentBytes: number;
    };
    const next = {
      manifestCount: current.manifestCount + (delta.manifestCount ?? 0),
      manifestBytes: current.manifestBytes + (delta.manifestBytes ?? 0),
      segmentCount: current.segmentCount + (delta.segmentCount ?? 0),
      segmentBytes: current.segmentBytes + (delta.segmentBytes ?? 0),
    };
    putRawRecordLedger(transaction, next);
  };
}

function putRawRecordLedger(
  transaction: IDBTransaction,
  record: {
    manifestCount: number;
    manifestBytes: number;
    segmentCount: number;
    segmentBytes: number;
  },
): void {
  transaction.objectStore("statistics").put(
    {
      ...record,
      checksum: crc32(
        new TextEncoder().encode(
          `${String(record.manifestCount)}:${String(record.manifestBytes)}:${String(record.segmentCount)}:${String(record.segmentBytes)}`,
        ),
      ),
    },
    "resource/records",
  );
}

async function readRawValue(
  indexedDB: IDBFactory,
  name: string,
  storeName: string,
  key: IDBValidKey,
): Promise<unknown> {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("raw IndexedDB open failed"));
  });
  try {
    const transaction = database.transaction(storeName, "readonly");
    const value = await new Promise<unknown>((resolve, reject) => {
      const request = transaction.objectStore(storeName).get(key);
      request.onsuccess = () => resolve(request.result as unknown);
      request.onerror = () => reject(request.error ?? new Error("raw IndexedDB read failed"));
    });
    await new Promise<void>((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error("raw read aborted"));
      transaction.onerror = () => reject(transaction.error ?? new Error("raw read failed"));
    });
    return value;
  } finally {
    database.close();
  }
}

function countBlockPayloadReads(indexedDB: IDBFactory): {
  count: () => number;
  reset: () => void;
} {
  let reads = 0;
  const originalOpen = indexedDB.open.bind(indexedDB);
  Object.defineProperty(indexedDB, "open", {
    configurable: true,
    value: (name: string, version?: number) => {
      const request = version === undefined ? originalOpen(name) : originalOpen(name, version);
      request.addEventListener("success", () => {
        const database = request.result;
        const originalTransaction = database.transaction.bind(database);
        Object.defineProperty(database, "transaction", {
          configurable: true,
          value: (
            stores: string | string[],
            mode?: IDBTransactionMode,
            options?: IDBTransactionOptions,
          ) => {
            const transaction = originalTransaction(stores, mode, options);
            const storeNames = typeof stores === "string" ? [stores] : stores;
            if (!storeNames.includes("blocks")) return transaction;
            const blocks = transaction.objectStore("blocks");
            const originalGet = blocks.get.bind(blocks);
            Object.defineProperty(blocks, "get", {
              configurable: true,
              value: (key: IDBValidKey) => {
                reads += 1;
                return originalGet(key);
              },
            });
            return transaction;
          },
        });
      });
      return request;
    },
  });
  return {
    count: () => reads,
    reset: () => {
      reads = 0;
    },
  };
}

function countManifestAndBlockGets(indexedDB: IDBFactory): {
  counts: () => { blocks: number; manifests: number };
  reset: () => void;
} {
  let blocks = 0;
  let manifests = 0;
  const originalOpen = indexedDB.open.bind(indexedDB);
  Object.defineProperty(indexedDB, "open", {
    configurable: true,
    value: (name: string, version?: number) => {
      const request = version === undefined ? originalOpen(name) : originalOpen(name, version);
      request.addEventListener("success", () => {
        const database = request.result;
        const originalTransaction = database.transaction.bind(database);
        Object.defineProperty(database, "transaction", {
          configurable: true,
          value: (
            stores: string | string[],
            mode?: IDBTransactionMode,
            options?: IDBTransactionOptions,
          ) => {
            const transaction = originalTransaction(stores, mode, options);
            for (const storeName of ["blocks", "manifests"] as const) {
              const storeNames = typeof stores === "string" ? [stores] : stores;
              if (!storeNames.includes(storeName)) continue;
              const store = transaction.objectStore(storeName);
              const originalGet = store.get.bind(store);
              Object.defineProperty(store, "get", {
                configurable: true,
                value: (key: IDBValidKey) => {
                  if (storeName === "blocks") blocks += 1;
                  else manifests += 1;
                  return originalGet(key);
                },
              });
            }
            return transaction;
          },
        });
      });
      return request;
    },
  });
  return {
    counts: () => ({ blocks, manifests }),
    reset: () => {
      blocks = 0;
      manifests = 0;
    },
  };
}

function countDatabaseTransactions(indexedDB: IDBFactory): {
  count: () => number;
  reset: () => void;
} {
  let count = 0;
  const originalOpen = indexedDB.open.bind(indexedDB);
  Object.defineProperty(indexedDB, "open", {
    configurable: true,
    value: (name: string, version?: number) => {
      const request = version === undefined ? originalOpen(name) : originalOpen(name, version);
      request.addEventListener("success", () => {
        const database = request.result;
        const originalTransaction = database.transaction.bind(database);
        Object.defineProperty(database, "transaction", {
          configurable: true,
          value: (
            stores: string | string[],
            mode?: IDBTransactionMode,
            options?: IDBTransactionOptions,
          ) => {
            count += 1;
            return originalTransaction(stores, mode, options);
          },
        });
      });
      return request;
    },
  });
  return {
    count: () => count,
    reset: () => {
      count = 0;
    },
  };
}

function countCatalogDeletes(indexedDB: IDBFactory): { count: () => number; reset: () => void } {
  let deletes = 0;
  const originalOpen = indexedDB.open.bind(indexedDB);
  Object.defineProperty(indexedDB, "open", {
    configurable: true,
    value: (name: string, version?: number) => {
      const request = version === undefined ? originalOpen(name) : originalOpen(name, version);
      request.addEventListener("success", () => {
        const database = request.result;
        const originalTransaction = database.transaction.bind(database);
        Object.defineProperty(database, "transaction", {
          configurable: true,
          value: (
            stores: string | string[],
            mode?: IDBTransactionMode,
            options?: IDBTransactionOptions,
          ) => {
            const transaction = originalTransaction(stores, mode, options);
            const storeNames = typeof stores === "string" ? [stores] : stores;
            if (!storeNames.includes("catalog")) return transaction;
            const catalog = transaction.objectStore("catalog");
            const originalDelete = catalog.delete.bind(catalog);
            Object.defineProperty(catalog, "delete", {
              configurable: true,
              value: (key: IDBValidKey) => {
                deletes += 1;
                return originalDelete(key);
              },
            });
            return transaction;
          },
        });
      });
      return request;
    },
  });
  return {
    count: () => deletes,
    reset: () => {
      deletes = 0;
    },
  };
}

function countTempCursorEvents(indexedDB: IDBFactory): {
  count: () => number;
  reset: () => void;
} {
  let events = 0;
  const originalOpen = indexedDB.open.bind(indexedDB);
  Object.defineProperty(indexedDB, "open", {
    configurable: true,
    value: (name: string, version?: number) => {
      const request = version === undefined ? originalOpen(name) : originalOpen(name, version);
      request.addEventListener("success", () => {
        const database = request.result;
        const originalTransaction = database.transaction.bind(database);
        Object.defineProperty(database, "transaction", {
          configurable: true,
          value: (
            stores: string | string[],
            mode?: IDBTransactionMode,
            options?: IDBTransactionOptions,
          ) => {
            const transaction = originalTransaction(stores, mode, options);
            const storeNames = typeof stores === "string" ? [stores] : stores;
            if (!storeNames.includes("temp")) return transaction;
            const temp = transaction.objectStore("temp");
            const originalOpenCursor = temp.openCursor.bind(temp);
            Object.defineProperty(temp, "openCursor", {
              configurable: true,
              value: (query?: IDBValidKey | IDBKeyRange | null, direction?: IDBCursorDirection) => {
                const cursorRequest = originalOpenCursor(query, direction);
                cursorRequest.addEventListener("success", () => {
                  events += 1;
                });
                return cursorRequest;
              },
            });
            return transaction;
          },
        });
      });
      return request;
    },
  });
  return {
    count: () => events,
    reset: () => {
      events = 0;
    },
  };
}

function overrideCommittedTransactionCount(indexedDB: IDBFactory, count: number): void {
  const originalOpen = indexedDB.open.bind(indexedDB);
  Object.defineProperty(indexedDB, "open", {
    configurable: true,
    value: (name: string, version?: number) => {
      const request = version === undefined ? originalOpen(name) : originalOpen(name, version);
      request.addEventListener("success", () => {
        const database = request.result;
        const originalTransaction = database.transaction.bind(database);
        Object.defineProperty(database, "transaction", {
          configurable: true,
          value: (
            stores: string | string[],
            mode?: IDBTransactionMode,
            options?: IDBTransactionOptions,
          ) => {
            const transaction = originalTransaction(stores, mode, options);
            const storeNames = typeof stores === "string" ? [stores] : stores;
            if (!storeNames.includes("transactions")) return transaction;
            const store = transaction.objectStore("transactions");
            const originalIndex = store.index.bind(store);
            Object.defineProperty(store, "index", {
              configurable: true,
              value: (indexName: string) => {
                const index = originalIndex(indexName);
                if (indexName !== "byStatus") return index;
                const originalCount = index.count.bind(index);
                Object.defineProperty(index, "count", {
                  configurable: true,
                  value: (query?: IDBValidKey | IDBKeyRange | null) => {
                    const countRequest = query == null ? originalCount() : originalCount(query);
                    if (query === "committed") {
                      countRequest.addEventListener("success", () => {
                        Object.defineProperty(countRequest, "result", {
                          configurable: true,
                          value: count,
                        });
                      });
                    }
                    return countRequest;
                  },
                });
                return index;
              },
            });
            return transaction;
          },
        });
      });
      return request;
    },
  });
}

async function injectBlocks(
  indexedDB: IDBFactory,
  name: string,
  blocks: ReadonlyArray<{ id: string; bytes: Uint8Array }>,
): Promise<void> {
  await mutate(indexedDB, name, ["blocks", "catalog"], (transaction) => {
    const blockStore = transaction.objectStore("blocks");
    const catalog = transaction.objectStore("catalog");
    for (const block of blocks) {
      blockStore.add(block.bytes, block.id);
      catalog.add(
        { byteLength: block.bytes.byteLength, checksum: crc32(block.bytes) },
        `block-metadata/${block.id}`,
      );
    }
  });
}

async function injectSegments(
  indexedDB: IDBFactory,
  name: string,
  segments: readonly SegmentRecord[],
): Promise<void> {
  await mutate(indexedDB, name, ["segments", "statistics"], (transaction) => {
    const store = transaction.objectStore("segments");
    for (const record of segments) store.add(record, record.id);
    adjustRawRecordLedger(transaction, {
      segmentCount: segments.length,
      segmentBytes: segments.reduce(
        (total, record) => total + segmentRecordRetainedBytes(record),
        0,
      ),
    });
  });
}

function activeTransaction(id: string, snapshotVersion: number | null): TransactionRecord {
  return {
    id,
    ownerId: `owner-${id}`,
    expiresAt: "2026-08-24T12:30:00.000Z",
    snapshotVersion,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: NOW,
    updatedAt: NOW,
    committedVersion: null,
  };
}

function segment(id: string, transactionId: string, blockId: string): SegmentRecord {
  return {
    id,
    tableId: "events",
    transactionId,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { value: [blockId] },
    kind: "insert",
    level: 0,
    logicalOrder: 0,
    commitOrdinal: 0,
    rowIdSpans: [],
    createdAt: NOW,
  };
}

async function publishManifest(
  store: IndexedDbBlockStore,
  input: { expectedVersion: number | null; blockIds: readonly string[]; createdAt: string },
) {
  const current = await store.getCurrentManifest();
  const currentIds = new Set(
    current === undefined ? [] : await readManifestBlockIds(store, current.version),
  );
  const nextIds = new Set(input.blockIds);
  const additions = [...nextIds].filter((id) => !currentIds.has(id));
  const removals = [...currentIds].filter((id) => !nextIds.has(id));
  const transaction = activeTransaction(`publish-${crypto.randomUUID()}`, input.expectedVersion);
  await store.createTransaction(transaction);
  const staged =
    additions.length === 0
      ? transaction
      : await store.updateTransaction(transaction.id, 0, {
          pendingBlockIds: additions,
          updatedAt: input.createdAt,
        });
  await store.commitTransaction({
    transactionId: transaction.id,
    expectedTransactionRevision: staged.revision,
    expectedManifestVersion: input.expectedVersion,
    removedBlockIds: removals,
    committedAt: input.createdAt,
  });
  const published = await store.getCurrentManifest();
  if (published === undefined) throw new Error("Commit did not publish a manifest");
  return published;
}

async function readManifestBlockIds(
  store: IndexedDbBlockStore,
  version: number,
): Promise<string[]> {
  const ids: string[] = [];
  let cursor: string | null = null;
  do {
    const page = await store.listManifestBlockPage({ version, afterBlockId: cursor, limit: 256 });
    ids.push(...page.records.map((record) => record.blockId));
    cursor = page.nextCursor;
  } while (cursor !== null);
  return ids;
}

describe("IndexedDB corruption hardening", () => {
  it("persists structural epochs and rejects an old-schema journal after reopen", async () => {
    const indexedDB = new IDBFactory();
    const name = "schema-epoch-reopen";
    let store = await IndexedDbBlockStore.open({ name, indexedDB });
    await store.addTable({
      id: "schema-epoch-table",
      name: "schema_epoch_table",
      managed: false,
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    const begun = await store.beginTransaction({
      record: {
        id: "schema-epoch-transaction",
        ownerId: "schema-epoch-owner",
        expiresAt: "2026-08-24T12:30:00.000Z",
        pendingBlockIds: [],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: NOW,
        updatedAt: NOW,
        committedVersion: null,
      },
    });
    const staged = await store.stageTransactionArtifacts({
      transactionId: begun.record.id,
      expectedRevision: begun.record.revision,
      blocks: [{ id: "schema-epoch-block", bytes: Uint8Array.of(1) }],
      segments: [
        {
          id: "schema-epoch-segment",
          tableId: "schema-epoch-table",
          transactionId: begun.record.id,
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { value: ["schema-epoch-block"] },
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          createdAt: NOW,
        },
      ],
      updatedAt: NOW,
    });
    await store.updateTable("schema-epoch-table", 0, {
      columns: [
        { id: "value", name: "value", type: "number", nullable: false },
        { id: "note", name: "note", type: "string", nullable: true },
      ],
    });
    const beforeClose = await store.getCatalogProbe();
    store.close();

    store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect((await store.getCatalogProbe()).schemaEpoch).toBe(beforeClose.schemaEpoch);
    await expect(
      store.commitTransaction({
        transactionId: staged.id,
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [{ tableId: "schema-epoch-table", limit: 4096 }],
        committedAt: "2026-08-24T12:00:01.000Z",
      }),
    ).rejects.toBeInstanceOf(SchemaConflictError);
    expect(await store.getCurrentManifestVersion()).toBeNull();
    expect(await store.getTransaction(staged.id)).toMatchObject({ status: "active" });
    store.close();
  });

  it("validates the database name before issuing an IndexedDB open request", async () => {
    const indexedDB = new IDBFactory();
    let opens = 0;
    const originalOpen = indexedDB.open.bind(indexedDB);
    Object.defineProperty(indexedDB, "open", {
      configurable: true,
      value: (name: string, version?: number) => {
        opens += 1;
        return version === undefined ? originalOpen(name) : originalOpen(name, version);
      },
    });
    for (const name of ["", "   ", "x".repeat(MAX_STORAGE_DATABASE_NAME_CHARACTERS + 1)]) {
      await expect(IndexedDbBlockStore.open({ name, indexedDB })).rejects.toBeInstanceOf(TypeError);
    }
    expect(opens).toBe(0);
    const boundary = await IndexedDbBlockStore.open({
      name: "x".repeat(MAX_STORAGE_DATABASE_NAME_CHARACTERS),
      indexedDB,
    });
    expect(opens).toBe(1);
    boundary.close();
  });

  it("uses strict durability by default and keeps relaxed mode an explicit opt-in", async () => {
    const observed: IDBTransactionDurability[] = [];
    const instrument = (factory: IDBFactory): void => {
      const originalOpen = factory.open.bind(factory);
      Object.defineProperty(factory, "open", {
        configurable: true,
        value: (name: string, version?: number) => {
          const request = version === undefined ? originalOpen(name) : originalOpen(name, version);
          request.addEventListener("success", () => {
            const database = request.result;
            const originalTransaction = database.transaction.bind(database);
            Object.defineProperty(database, "transaction", {
              configurable: true,
              value: (
                stores: string | string[],
                mode?: IDBTransactionMode,
                options?: IDBTransactionOptions,
              ) => {
                if (options?.durability !== undefined) observed.push(options.durability);
                return originalTransaction(stores, mode, options);
              },
            });
          });
          return request;
        },
      });
    };

    const strictFactory = new IDBFactory();
    instrument(strictFactory);
    const strict = await openStore(strictFactory);
    await strict.createTransaction(activeTransaction("strict", null));
    strict.close();

    const relaxedFactory = new IDBFactory();
    instrument(relaxedFactory);
    const relaxed = await IndexedDbBlockStore.open({
      name: crypto.randomUUID(),
      indexedDB: relaxedFactory,
      durability: "relaxed",
    });
    await relaxed.createTransaction(activeTransaction("relaxed", null));
    relaxed.close();
    expect(observed).toEqual(["strict", "relaxed"]);
  });

  it("rejects oversized public identities and bulk reads before opening a transaction", async () => {
    const indexedDB = new IDBFactory();
    const transactions = countDatabaseTransactions(indexedDB);
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    const tooMany = Array.from(
      { length: MAX_STORAGE_BULK_READ_ITEMS + 1 },
      (_, index) => `id/${String(index)}`,
    );
    const tooLong = "x".repeat(MAX_STORAGE_ID_CHARACTERS + 1);

    transactions.reset();
    await expect(store.getBlocks(tooMany)).rejects.toBeInstanceOf(RangeError);
    await expect(store.getTransactions(tooMany)).rejects.toBeInstanceOf(RangeError);
    await expect(store.getExistingUniqueKeys("table", tooMany)).rejects.toBeInstanceOf(RangeError);
    await expect(store.getBlock(tooLong)).rejects.toBeInstanceOf(TypeError);
    await expect(store.readManifestBlock(0, tooLong)).rejects.toBeInstanceOf(TypeError);
    expect(transactions.count()).toBe(0);

    const boundary = "x".repeat(MAX_STORAGE_ID_CHARACTERS);
    await expect(store.getBlock(boundary)).resolves.toBeUndefined();
    expect(transactions.count()).toBe(1);
    store.close();

    await mutate(indexedDB, name, "catalog", (transaction) => {
      transaction.objectStore("catalog").put(
        {
          id: tooLong,
          name: "oversized",
          columns: [],
          revision: 0,
          createdAt: NOW,
        },
        `table/id/${tooLong}`,
      );
    });
    await expect(openStore(indexedDB, name)).rejects.toMatchObject({
      name: "StorageCorruptionError",
    });
  });

  it("preflights aggregate block bytes from metadata and fails closed on metadata drift", async () => {
    const indexedDB = new IDBFactory();
    const reads = countBlockPayloadReads(indexedDB);
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await injectBlocks(indexedDB, name, [
      { id: "batch/a", bytes: Uint8Array.of(1) },
      { id: "batch/b", bytes: Uint8Array.of(2) },
    ]);
    const declared = Math.floor(MAX_BLOCK_READ_BATCH_BYTES / 2) + 1;
    await mutate(indexedDB, name, "catalog", (transaction) => {
      const catalog = transaction.objectStore("catalog");
      catalog.put({ byteLength: declared, checksum: 0 }, "block-metadata/batch/a");
      catalog.put({ byteLength: declared, checksum: 0 }, "block-metadata/batch/b");
    });

    reads.reset();
    const oversized = store.getBlocks(["batch/a", "batch/b"]);
    await expect(oversized).rejects.toBeInstanceOf(BlockReadBatchTooLargeError);
    await expect(oversized).rejects.toMatchObject({
      requestedBytes: declared * 2,
      limitBytes: MAX_BLOCK_READ_BATCH_BYTES,
    });
    expect(reads.count()).toBe(0);

    await mutate(indexedDB, name, "catalog", (transaction) => {
      const catalog = transaction.objectStore("catalog");
      catalog.put(
        { byteLength: MAX_BLOCK_READ_BATCH_BYTES, checksum: 0 },
        "block-metadata/batch/a",
      );
      catalog.put({ byteLength: 1, checksum: 0 }, "block-metadata/batch/b");
    });
    reads.reset();
    await expect(store.getBlocks(["batch/a"])).rejects.toBeInstanceOf(StorageCorruptionError);
    expect(reads.count()).toBe(1);

    await mutate(indexedDB, name, "catalog", (transaction) => {
      transaction.objectStore("catalog").delete("block-metadata/batch/a");
    });
    reads.reset();
    await expect(store.getBlocks(["batch/a"])).rejects.toBeInstanceOf(StorageCorruptionError);
    expect(reads.count()).toBe(0);

    await mutate(indexedDB, name, ["blocks", "catalog"], (transaction) => {
      transaction.objectStore("blocks").delete("batch/b");
      transaction
        .objectStore("catalog")
        .put({ byteLength: 1, checksum: 0 }, "block-metadata/batch/b");
    });
    reads.reset();
    await expect(store.getBlocks(["batch/b"])).rejects.toBeInstanceOf(StorageCorruptionError);
    expect(reads.count()).toBe(1);
    store.close();
  });

  it("rejects shared-memory payloads before every block and temp-page mutation", async () => {
    const indexedDB = new IDBFactory();
    const store = await openStore(indexedDB);
    const shared = new Uint8Array(new SharedArrayBuffer(4));

    await store.createTransaction(activeTransaction("stage-shared", null));
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "stage-shared",
        expectedRevision: 0,
        blocks: [
          { id: "ordinary-before-shared", bytes: Uint8Array.of(1) },
          { id: "stage-shared-block", bytes: shared },
        ],
        segments: [],
        updatedAt: NOW,
      }),
    ).rejects.toBeInstanceOf(TypeError);
    expect(await store.getBlock("ordinary-before-shared")).toBeUndefined();
    expect(await store.getBlock("stage-shared-block")).toBeUndefined();
    expect(await store.getTransaction("stage-shared")).toMatchObject({ revision: 0 });

    const { snapshotVersion: _snapshotVersion, ...fresh } = activeTransaction("write-shared", null);
    void _snapshotVersion;
    await expect(
      store.writeTransaction({
        transaction: { record: fresh },
        blocks: [{ id: "write-shared-block", bytes: shared }],
        segments: [],
        expectedManifestVersion: null,
        committedAt: NOW,
      }),
    ).rejects.toBeInstanceOf(TypeError);
    expect(await store.getBlock("write-shared-block")).toBeUndefined();
    expect(await store.getTransaction("write-shared")).toBeUndefined();

    await expect(
      store.putTempRunPages([
        { ownerId: "owner", runId: "run", pageIndex: 0, bytes: Uint8Array.of(1) },
        { ownerId: "owner", runId: "run", pageIndex: 1, bytes: shared },
      ]),
    ).rejects.toBeInstanceOf(TypeError);
    expect(await store.getTempRunPage("owner", "run", 0)).toBeUndefined();
    expect(await store.getTempRunPage("owner", "run", 1)).toBeUndefined();
    store.close();
  });

  it("bounds atomic artifact batches and the durable transaction journal", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await store.createTransaction(activeTransaction("bounded-stage", null));
    const tooManyBlocks = Array.from({ length: MAX_TRANSACTION_STAGE_BLOCKS + 1 }, (_, index) => ({
      id: `too-many-block-${String(index)}`,
      bytes: Uint8Array.of(index),
    }));
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "bounded-stage",
        expectedRevision: 0,
        blocks: tooManyBlocks,
        segments: [],
        updatedAt: NOW,
      }),
    ).rejects.toThrow(/exceeds 64 blocks/);
    const tooManySegments = Array.from({ length: MAX_TRANSACTION_STAGE_SEGMENTS + 1 }, (_, index) =>
      segment(`too-many-segment-${String(index)}`, "bounded-stage", "missing"),
    );
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "bounded-stage",
        expectedRevision: 0,
        blocks: [],
        segments: tooManySegments,
        updatedAt: NOW,
      }),
    ).rejects.toThrow(/exceeds 64 segments/);
    expect(await store.getTransaction("bounded-stage")).toMatchObject({ revision: 0 });
    expect(await store.getBlock("too-many-block-0")).toBeUndefined();

    const { snapshotVersion: _snapshotVersion, ...freshBlocks } = activeTransaction(
      "bounded-write-blocks",
      null,
    );
    void _snapshotVersion;
    await expect(
      store.writeTransaction({
        transaction: { record: freshBlocks },
        blocks: tooManyBlocks,
        segments: [],
        expectedManifestVersion: null,
        committedAt: NOW,
      }),
    ).rejects.toThrow(/exceeds 64 blocks/);
    const { snapshotVersion: _secondSnapshot, ...freshSegments } = activeTransaction(
      "bounded-write-segments",
      null,
    );
    void _secondSnapshot;
    await expect(
      store.writeTransaction({
        transaction: { record: freshSegments },
        blocks: [],
        segments: tooManySegments.map((entry) => ({
          ...entry,
          transactionId: "bounded-write-segments",
        })),
        expectedManifestVersion: null,
        committedAt: NOW,
      }),
    ).rejects.toThrow(/exceeds 64 segments/);
    expect(await store.getTransaction("bounded-write-blocks")).toBeUndefined();
    expect(await store.getTransaction("bounded-write-segments")).toBeUndefined();

    let revision = 0;
    for (
      let start = 0;
      start < MAX_TRANSACTION_STAGE_BLOCKS * 2;
      start += MAX_TRANSACTION_STAGE_BLOCKS
    ) {
      const updated = await store.stageTransactionArtifacts({
        transactionId: "bounded-stage",
        expectedRevision: revision,
        blocks: Array.from({ length: MAX_TRANSACTION_STAGE_BLOCKS }, (_, offset) => ({
          id: `journal-block-${String(start + offset)}`,
          bytes: Uint8Array.of((start + offset) & 0xff),
        })),
        segments: [],
        updatedAt: NOW,
      });
      revision = updated.revision;
    }
    const journalIds = Array.from(
      { length: MAX_TRANSACTION_PENDING_BLOCKS },
      (_, index) => `journal-block-${String(index)}`,
    ).sort();
    const alreadyStaged = new Set(
      Array.from(
        { length: MAX_TRANSACTION_STAGE_BLOCKS * 2 },
        (_, index) => `journal-block-${String(index)}`,
      ),
    );
    await injectBlocks(
      indexedDB,
      name,
      journalIds
        .filter((id) => !alreadyStaged.has(id))
        .map((id) => ({
          id,
          bytes: Uint8Array.of(1),
        })),
    );
    const bounded = await store.getTransaction("bounded-stage");
    if (bounded === undefined) throw new Error("Missing bounded transaction fixture");
    await mutate(indexedDB, name, "transactions", (transaction) => {
      transaction
        .objectStore("transactions")
        .put({ ...bounded, pendingBlockIds: journalIds }, bounded.id);
    });
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "bounded-stage",
        expectedRevision: revision,
        blocks: [{ id: "journal-overflow", bytes: Uint8Array.of(1) }],
        segments: [],
        updatedAt: NOW,
      }),
    ).rejects.toThrow(/journal exceeds 4096 pending blocks/);
    const capped = await store.getTransaction("bounded-stage");
    expect(capped).toMatchObject({ revision });
    expect(capped?.pendingBlockIds).toContain("journal-block-0");
    expect(capped?.pendingBlockIds).toContain("journal-block-4095");
    expect(capped?.pendingBlockIds).toHaveLength(MAX_TRANSACTION_PENDING_BLOCKS);
    expect(await store.getBlock("journal-overflow")).toBeUndefined();
    store.close();
  });

  it("rejects oversized temp-page batches before any IndexedDB mutation", async () => {
    const indexedDB = new IDBFactory();
    const store = await openStore(indexedDB);
    const page = (pageIndex: number, bytes = Uint8Array.of(pageIndex)) => ({
      ownerId: "bounded-temp",
      runId: "run",
      pageIndex,
      bytes,
    });

    await expect(
      store.putTempRunPages(
        Array.from({ length: MAX_TEMP_RUN_PAGES_PER_BATCH + 1 }, (_, index) => page(index)),
      ),
    ).rejects.toThrow(/exceeds 64 pages/);
    expect(await store.getTempRunPage("bounded-temp", "run", 0)).toBeUndefined();

    await expect(
      store.putTempRunPage(page(0, new Uint8Array(MAX_TEMP_RUN_PAGE_BYTES + 1))),
    ).rejects.toThrow(/Temp run page exceeds/);
    expect(await store.getTempRunPage("bounded-temp", "run", 0)).toBeUndefined();

    const aggregate = new Uint8Array(MAX_TEMP_RUN_BATCH_BYTES + 1);
    const split = Math.ceil(aggregate.byteLength / 2);
    await expect(
      store.putTempRunPages([
        page(0, aggregate.subarray(0, split)),
        page(1, aggregate.subarray(split)),
      ]),
    ).rejects.toThrow(/page batch exceeds .* bytes/);
    expect(await store.getTempRunPage("bounded-temp", "run", 0)).toBeUndefined();
    expect(await store.getTempRunPage("bounded-temp", "run", 1)).toBeUndefined();
    store.close();
  });

  it("deletes one temp-run prefix without scanning unrelated owners", async () => {
    const indexedDB = new IDBFactory();
    const cursorEvents = countTempCursorEvents(indexedDB);
    const store = await openStore(indexedDB);
    const unrelated = Array.from({ length: 128 }, (_, index) => ({
      ownerId: `other-${String(index).padStart(3, "0")}`,
      runId: "run",
      pageIndex: 0,
      bytes: Uint8Array.of(index),
    }));
    for (const ownerId of [...unrelated.map((page) => page.ownerId), "target"]) {
      await store.createTempOwner({
        ownerId,
        createdAt: NOW,
        expiresAt: "2026-08-24T12:30:00.000Z",
        revision: 0,
      });
    }
    for (let index = 0; index < unrelated.length; index += MAX_TEMP_RUN_PAGES_PER_BATCH) {
      await store.putTempRunPages(unrelated.slice(index, index + MAX_TEMP_RUN_PAGES_PER_BATCH));
    }
    await store.putTempRunPages([
      { ownerId: "target", runId: "selected", pageIndex: 0, bytes: Uint8Array.of(1) },
      { ownerId: "target", runId: "selected", pageIndex: 1, bytes: Uint8Array.of(2) },
      { ownerId: "target", runId: "retained", pageIndex: 0, bytes: Uint8Array.of(3) },
    ]);
    cursorEvents.reset();
    await store.removeTempRun("target", "selected");
    // First record + two matching pages + first key after the prefix. A full scan would visit
    // every unrelated page before reaching this owner.
    expect(cursorEvents.count()).toBeLessThanOrEqual(4);
    expect(await store.getTempRunPage("target", "selected", 0)).toBeUndefined();
    expect(await store.getTempRunPage("target", "selected", 1)).toBeUndefined();
    expect(await store.getTempRunPage("target", "retained", 0)).toEqual(Uint8Array.of(3));
    expect(await store.getTempRunPage("other-000", "run", 0)).toEqual(Uint8Array.of(0));
    store.close();
  });

  it("rejects orphan hot-path references and stale full-text writers before mutation", async () => {
    const indexedDB = new IDBFactory();
    const store = await openStore(indexedDB);
    await store.createTransaction(activeTransaction("owner", null));
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "owner",
        expectedRevision: 0,
        blocks: [{ id: "orphan-block", bytes: Uint8Array.of(1) }],
        segments: [segment("orphan-segment", "owner", "orphan-block")],
        updatedAt: NOW,
      }),
    ).rejects.toThrow(/missing table/);
    expect(await store.getBlock("orphan-block")).toBeUndefined();
    expect(await store.getSegment("orphan-segment")).toBeUndefined();
    await expect(store.reserveRowIds("missing", 1)).rejects.toThrow(/has no table/);
    await expect(store.reserveAutoIncrement("missing", "id", 1)).rejects.toThrow(
      /has no declared column/,
    );

    await store.addTable({
      managed: false,
      id: "articles",
      name: "articles",
      columns: [{ id: "body", name: "body", type: "string", nullable: false }],
      ftsColumns: {
        body: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "ready",
          buildFromVersion: 0,
        },
      },
      revision: 0,
      createdAt: NOW,
    });
    await expect(store.removeFtsColumn("articles", "body")).rejects.toThrow(/still active/);
    expect((await store.getTable("articles"))?.ftsColumns?.body?.state).toBe("ready");
    await store.updateTable("articles", 0, { ftsColumns: null });
    await expect(store.removeFtsColumn("articles", "body")).resolves.toBeUndefined();
    await expect(
      store.writeFtsBase("articles", "body", {
        coversVersion: -1,
        chunks: [[{ term: "stale", rowIds: [1n], tf: [1] }]],
        totalTokens: 1,
      }),
    ).rejects.toThrow(/no longer active/);
    expect(
      await store.readFtsCandidates("articles", "body", [{ term: "stale", prefix: false }], -1),
    ).toMatchObject({ hasBase: false });
    store.close();
  });

  it("reclaims abandoned FTS base builds in bounded reopen pages", async () => {
    const indexedDB = new IDBFactory();
    const deletes = countCatalogDeletes(indexedDB);
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "cleanup-articles",
      name: "cleanup_articles",
      columns: [{ id: "body", name: "body", type: "string", nullable: false }],
      ftsColumns: {
        body: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "ready",
          buildFromVersion: 0,
        },
      },
      revision: 0,
      createdAt: NOW,
    });
    await store.writeFtsBase("cleanup-articles", "body", {
      coversVersion: 0,
      chunks: [[{ term: "completed", rowIds: [1n], tf: [1] }]],
      totalTokens: 1,
    });
    const buildOwnerId = "abandoned-build-owner";
    const buildExpiresAt = "2026-08-24T12:30:00.000Z";
    await store.beginFtsBaseBuild({
      tableId: "cleanup-articles",
      columnId: "body",
      buildId: "abandoned-build",
      ownerId: buildOwnerId,
      createdAt: NOW,
      expiresAt: buildExpiresAt,
    });
    for (let ordinal = 0; ordinal < 130; ordinal += 1) {
      await store.writeFtsBaseBuildChunk({
        tableId: "cleanup-articles",
        columnId: "body",
        buildId: "abandoned-build",
        ownerId: buildOwnerId,
        expiresAtCutoff: NOW,
        expiresAt: buildExpiresAt,
        updatedAt: NOW,
        ordinal,
        chunk: [
          {
            term: `term-${String(ordinal).padStart(3, "0")}`,
            rowIds: [BigInt(ordinal + 1)],
            tf: [1],
          },
        ],
      });
    }
    await mutate(indexedDB, name, "catalog", (transaction) => {
      const catalog = transaction.objectStore("catalog");
      const request = catalog.get("fts-base-build/cleanup-articles/body");
      request.onsuccess = () => {
        catalog.put(
          {
            ...(request.result as object),
            createdAt: "1999-12-31T23:30:00.000Z",
            expiresAt: "2000-01-01T00:00:00.000Z",
            ftsBuildExpiry: "2000-01-01T00:00:00.000Z",
            updatedAt: "1999-12-31T23:30:00.000Z",
          },
          "fts-base-build/cleanup-articles/body",
        );
      };
    });
    store.close();

    deletes.reset();
    store = await openStore(indexedDB, name);
    expect(deletes.count()).toBe(64);
    expect(
      await readRawValue(indexedDB, name, "catalog", "fts-base-build/cleanup-articles/body"),
    ).toMatchObject({ cleanupIndex: 64 });
    expect(
      await readRawValue(
        indexedDB,
        name,
        "catalog",
        "fts-base/cleanup-articles/body/generation/abandoned-build/000000",
      ),
    ).toBeUndefined();
    expect(
      await readRawValue(
        indexedDB,
        name,
        "catalog",
        "fts-base/cleanup-articles/body/generation/abandoned-build/000064",
      ),
    ).toBeDefined();
    expect(
      await store.readFtsCandidates(
        "cleanup-articles",
        "body",
        [{ term: "completed", prefix: false }],
        0,
      ),
    ).toMatchObject({ hasBase: true, rowIdsByTerm: [[1n]] });
    store.close();

    deletes.reset();
    store = await openStore(indexedDB, name);
    expect(deletes.count()).toBe(64);
    expect(
      await readRawValue(indexedDB, name, "catalog", "fts-base-build/cleanup-articles/body"),
    ).toMatchObject({ cleanupIndex: 128 });
    store.close();

    deletes.reset();
    store = await openStore(indexedDB, name);
    expect(deletes.count()).toBe(3);
    expect(
      await readRawValue(indexedDB, name, "catalog", "fts-base-build/cleanup-articles/body"),
    ).toBeUndefined();
    expect(
      await store.readFtsCandidates(
        "cleanup-articles",
        "body",
        [{ term: "completed", prefix: false }],
        0,
      ),
    ).toMatchObject({ hasBase: true, rowIdsByTerm: [[1n]] });
    expect(await store.checkIntegrity()).toMatchObject({ ok: true });
    store.close();
  });

  it("replays an acknowledged postings chunk exactly without double-accounting", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "replay-articles",
      name: "replay_articles",
      columns: [{ id: "body", name: "body", type: "string", nullable: false }],
      ftsColumns: {
        body: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "ready",
          buildFromVersion: 0,
        },
      },
      revision: 0,
      createdAt: NOW,
    });
    const chunk = [{ term: "exact", rowIds: [1n], tf: [1] }];
    // Keep the lease live across the deliberate close/reopen without tying the test to a
    // calendar date. Static timestamps eventually turn this recovery test into an expiry test.
    const buildStartedAt = Date.now();
    const timestamp = (offsetMs: number) => new Date(buildStartedAt + offsetMs).toISOString();
    const buildCreatedAt = timestamp(0);
    const initialExpiresAt = timestamp(50 * 60_000);
    const replayedAt = timestamp(60_000);
    const replayExpiresAt = timestamp(51 * 60_000);
    const conflictAt = timestamp(2 * 60_000);
    const conflictExpiresAt = timestamp(52 * 60_000);
    await store.beginFtsBaseBuild({
      tableId: "replay-articles",
      columnId: "body",
      buildId: "replayed-build",
      ownerId: "replayed-owner",
      createdAt: buildCreatedAt,
      expiresAt: initialExpiresAt,
    });
    await store.writeFtsBaseBuildChunk({
      tableId: "replay-articles",
      columnId: "body",
      buildId: "replayed-build",
      ownerId: "replayed-owner",
      expiresAtCutoff: buildCreatedAt,
      expiresAt: initialExpiresAt,
      updatedAt: buildCreatedAt,
      ordinal: 0,
      chunk,
    });
    const before = await readRawValue(
      indexedDB,
      name,
      "catalog",
      "fts-base-build/replay-articles/body",
    );
    await store.writeFtsBaseBuildChunk({
      tableId: "replay-articles",
      columnId: "body",
      buildId: "replayed-build",
      ownerId: "replayed-owner",
      expiresAtCutoff: replayedAt,
      expiresAt: replayExpiresAt,
      updatedAt: replayedAt,
      ordinal: 0,
      chunk,
    });
    expect(
      await readRawValue(indexedDB, name, "catalog", "fts-base-build/replay-articles/body"),
    ).toMatchObject({
      boundaries: (before as { boundaries: unknown }).boundaries,
      totalTokens: (before as { totalTokens: unknown }).totalTokens,
      retainedBytes: (before as { retainedBytes: unknown }).retainedBytes,
      retainedEntries: (before as { retainedEntries: unknown }).retainedEntries,
      expiresAt: replayExpiresAt,
    });
    const afterReplay = await readRawValue(
      indexedDB,
      name,
      "catalog",
      "fts-base-build/replay-articles/body",
    );
    await expect(
      store.writeFtsBaseBuildChunk({
        tableId: "replay-articles",
        columnId: "body",
        buildId: "replayed-build",
        ownerId: "replayed-owner",
        expiresAtCutoff: conflictAt,
        expiresAt: conflictExpiresAt,
        updatedAt: conflictAt,
        ordinal: 0,
        chunk: [{ term: "changed", rowIds: [1n], tf: [1] }],
      }),
    ).rejects.toMatchObject({ name: "PostingBuildConflictError" });
    expect(
      await readRawValue(indexedDB, name, "catalog", "fts-base-build/replay-articles/body"),
    ).toEqual(afterReplay);
    store.close();

    store = await openStore(indexedDB, name);
    await store.finishFtsBaseBuild({
      tableId: "replay-articles",
      columnId: "body",
      buildId: "replayed-build",
      ownerId: "replayed-owner",
      expiresAtCutoff: conflictAt,
      coversVersion: 0,
      chunkCount: 1,
      totalTokens: 1,
      completedAt: conflictAt,
    });
    expect(
      await store.readFtsCandidates(
        "replay-articles",
        "body",
        [{ term: "exact", prefix: false }],
        0,
      ),
    ).toMatchObject({ hasBase: true, rowIdsByTerm: [[1n]] });
    store.close();
  });

  it("atomically refuses table removal while transactions or compactions own its records", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await injectBlocks(indexedDB, name, [
      { id: "source", bytes: Uint8Array.of(1) },
      { id: "other-table", bytes: Uint8Array.of(2) },
    ]);
    await publishManifest(store, {
      expectedVersion: null,
      blockIds: ["source", "other-table"],
      createdAt: NOW,
    });
    await store.createTransaction(activeTransaction("writer", 0));
    await injectSegments(indexedDB, name, [segment("source-segment", "writer", "source")]);
    await expect(
      store.dropTable({
        tableId: "events",
        expectedTableRevision: 0,
        expectedManifestVersion: null,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: NOW,
      }),
    ).rejects.toThrow(/Manifest changed/);
    await expect(
      store.dropTable({
        tableId: "events",
        expectedTableRevision: 1,
        expectedManifestVersion: 0,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: NOW,
      }),
    ).rejects.toThrow(/changed/);
    await expect(
      store.dropTable({
        tableId: "events",
        expectedTableRevision: 0,
        expectedManifestVersion: 0,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: NOW,
      }),
    ).rejects.toBeInstanceOf(TableInUseError);
    expect(await store.getTable("events")).toBeDefined();
    expect(await store.getSegment("source-segment")).toBeDefined();
    expect(await store.getCurrentManifestVersion()).toBe(0);

    await store.updateTransaction("writer", 0, {
      pendingBlockIds: ["source"],
      pendingSegmentIds: ["source-segment"],
      status: "aborted",
      updatedAt: NOW,
    });
    const job: CompactionJobRecord = {
      ...COPY_COMPACTION_FIELDS,
      id: "busy-job",
      tableId: "events",
      sourceManifestVersion: 0,
      sourceSegmentIds: ["source-segment"],
      sourceBlockIds: ["source"],
      outputBlockIds: [],
      cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
      processedRows: 0,
      sourceStoredBytes: 0,
      outputStoredBytes: 0,
      logicalBytes: 0,
      targetLevel: 1,
      state: "planned",
      transactionId: null,
      outputSegmentId: null,
      publishedVersion: null,
      revision: 0,
      createdAt: NOW,
      updatedAt: NOW,
    };
    await store.createCompactionJob(job);
    await expect(
      store.dropTable({
        tableId: "events",
        expectedTableRevision: 0,
        expectedManifestVersion: 0,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: NOW,
      }),
    ).rejects.toBeInstanceOf(TableInUseError);
    expect(await store.getTable("events")).toBeDefined();
    await expect(store.removeCompactionJob("busy-job")).rejects.toThrow(/not terminal/);
    expect(await store.getCompactionJob("busy-job")).toBeDefined();
    await injectBlocks(indexedDB, name, [{ id: "orphan-output", bytes: Uint8Array.of(9) }]);
    await store.createTransaction(activeTransaction("compactor", 0));
    const outputCheckpoint = await store.updateCompactionJob("busy-job", 0, {
      state: "running",
      transactionId: "compactor",
      outputBlockIds: ["orphan-output"],
      updatedAt: NOW,
    });
    await store.cancelCompactionJob("busy-job", outputCheckpoint.revision, NOW);
    expect(await store.removeCompactionJob("busy-job")).toBe(false);
    const cleanup = await store.createGarbageCollectionJob({
      id: "orphan-output-cleanup",
      candidateManifestVersions: [],
      candidateSegmentIds: [],
      candidateBlockIds: ["orphan-output"],
      candidateTransactionIds: [],
      leaseCutoff: NOW,
      createdAt: NOW,
    });
    await store.runGarbageCollectionStep({
      jobId: cleanup.id,
      expectedRevision: cleanup.revision,
      maxItems: 1,
      updatedAt: NOW,
    });
    expect(await store.getBlock("orphan-output")).toBeUndefined();
    expect(await store.removeCompactionJob("busy-job")).toBe(true);
    expect(await store.getCompactionJob("busy-job")).toBeUndefined();
    expect(
      await store.dropTable({
        tableId: "events",
        expectedTableRevision: 0,
        expectedManifestVersion: 0,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: NOW,
      }),
    ).toMatchObject({ version: 1, changedTableIds: ["events"] });
    expect(await readManifestBlockIds(store, 1)).toEqual(["other-table"]);
    expect(await store.getTable("events")).toBeUndefined();
    expect(await store.getSegment("source-segment")).toBeUndefined();
    expect(await store.getBlock("source")).toEqual(Uint8Array.of(1));
    expect(await store.getBlock("other-table")).toEqual(Uint8Array.of(2));
    store.close();
  });

  it("releases only owner-matched leases and terminal maintenance jobs", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await store.createLease({
      id: "reader",
      kind: "reader",
      manifestVersion: null,
      ownerId: "owner-a",
      createdAt: NOW,
      expiresAt: "2026-08-24T12:30:00.000Z",
      revision: 0,
    });
    await expect(store.removeLease({ id: "reader", ownerId: "owner-b" })).rejects.toBeInstanceOf(
      LeaseOwnerConflictError,
    );
    expect(await store.getLease("reader")).toMatchObject({ ownerId: "owner-a" });
    expect(await store.removeLease({ id: "reader", ownerId: "owner-a" })).toBe(true);
    expect(await store.removeLease({ id: "reader", ownerId: "owner-a" })).toBe(false);

    await injectBlocks(indexedDB, name, [{ id: "gc-block", bytes: Uint8Array.of(1) }]);
    await publishManifest(store, { expectedVersion: null, blockIds: ["gc-block"], createdAt: NOW });
    await mutate(indexedDB, name, ["catalog", "manifests"], (tx) => {
      tx.objectStore("manifests").put(
        {
          version: 1,
          previousVersion: 0,
          liveBlockCount: 0,
          liveBlockBytes: 0,
          changedTableIds: [],
          createdAt: NOW,
        },
        1,
      );
      const catalog = tx.objectStore("catalog");
      const provenance = catalog.get(["manifest-block", "gc-block"]);
      provenance.onsuccess = () => {
        catalog.put({ ...(provenance.result as object), removedVersion: 1 }, [
          "manifest-block",
          "gc-block",
        ]);
      };
      catalog.put(1, "manifest/current");
    });
    const gc = await store.createGarbageCollectionJob({
      id: "gc",
      candidateManifestVersions: [],
      candidateSegmentIds: [],
      candidateBlockIds: ["gc-block"],
      candidateTransactionIds: [],
      leaseCutoff: NOW,
      createdAt: NOW,
    });
    await expect(store.removeGarbageCollectionJob(gc.id)).rejects.toThrow(/not completed/);
    expect(await store.getGarbageCollectionJob(gc.id)).toBeDefined();
    await store.runGarbageCollectionStep({
      jobId: gc.id,
      expectedRevision: gc.revision,
      maxItems: 1,
      updatedAt: NOW,
    });
    await store.removeGarbageCollectionJob(gc.id);
    expect(await store.getGarbageCollectionJob(gc.id)).toBeUndefined();
    store.close();
  });

  it("CAS-appends bounded garbage-collection discovery pages before reclamation", async () => {
    const store = await openStore(new IDBFactory());
    const discovery = {
      phase: "manifests" as const,
      currentManifestVersion: null,
      retainAboveVersion: -1,
      retainAfter: -1,
      maxPlanningItems: 2,
      manifestCursor: null,
      segmentCursor: null,
      transactionCursor: null,
      compactionCursor: null,
      visitedRecords: 0,
    };
    const job = await store.createGarbageCollectionJob({
      id: "bounded-discovery",
      candidateManifestVersions: [],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: NOW,
      createdAt: NOW,
      discovery,
    });

    await expect(
      store.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: job.revision,
        maxItems: MAX_MAINTENANCE_BATCH_ITEMS + 1,
        updatedAt: NOW,
      }),
    ).rejects.toThrow(/cannot exceed 1024 items/);
    expect(await store.getGarbageCollectionJob(job.id)).toEqual(job);

    await expect(
      store.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: job.revision,
        maxItems: 1,
        updatedAt: NOW,
      }),
    ).rejects.toThrow(/discovery must complete/);
    expect(await store.getGarbageCollectionJob(job.id)).toEqual(job);

    const page = await store.updateGarbageCollectionPlanning({
      jobId: job.id,
      expectedRevision: job.revision,
      discovery: { ...discovery, phase: "segments", manifestCursor: 0, visitedRecords: 1 },
      updatedAt: NOW,
    });
    expect(page).toMatchObject({
      revision: 1,
      discovery: { phase: "segments", visitedRecords: 1 },
    });
    await expect(
      store.updateGarbageCollectionPlanning({
        jobId: job.id,
        expectedRevision: job.revision,
        discovery: { ...discovery, phase: "complete", visitedRecords: 1 },
        updatedAt: NOW,
      }),
    ).rejects.toMatchObject({ name: "GarbageCollectionJobConflictError" });
    expect(await store.getGarbageCollectionJob(job.id)).toEqual(page);

    const completed = await store.updateGarbageCollectionPlanning({
      jobId: job.id,
      expectedRevision: page.revision,
      discovery: { ...discovery, phase: "complete", manifestCursor: 0, visitedRecords: 1 },
      updatedAt: NOW,
    });
    expect(completed).toMatchObject({ state: "completed", revision: 2 });
    store.close();
  });

  it("atomically limits active maintenance ownership across IndexedDB connections", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const first = await openStore(indexedDB, name);
    await first.addTable({
      managed: false,
      id: "maintenance-events",
      name: "maintenance_events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await injectBlocks(indexedDB, name, [{ id: "maintenance-source", bytes: Uint8Array.of(1) }]);
    await publishManifest(first, {
      expectedVersion: null,
      blockIds: ["maintenance-source"],
      createdAt: NOW,
    });
    await first.createTransaction(activeTransaction("maintenance-owner", 0));
    await first.updateTransaction("maintenance-owner", 0, { status: "aborted", updatedAt: NOW });
    await injectSegments(indexedDB, name, [
      {
        ...segment("maintenance-segment", "maintenance-owner", "maintenance-source"),
        tableId: "maintenance-events",
      },
    ]);
    const compaction = (id: string): CompactionJobRecord => ({
      ...COPY_COMPACTION_FIELDS,
      id,
      tableId: "maintenance-events",
      sourceManifestVersion: 0,
      sourceSegmentIds: ["maintenance-segment"],
      sourceBlockIds: ["maintenance-source"],
      outputBlockIds: [],
      cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
      processedRows: 0,
      sourceStoredBytes: 0,
      outputStoredBytes: 0,
      logicalBytes: 0,
      targetLevel: 1,
      state: "planned",
      transactionId: null,
      outputSegmentId: null,
      publishedVersion: null,
      revision: 0,
      createdAt: NOW,
      updatedAt: NOW,
    });
    const second = await openStore(indexedDB, name);
    const compactionAttempts = await Promise.allSettled([
      first.createCompactionJob(compaction("maintenance-race-a")),
      second.createCompactionJob(compaction("maintenance-race-b")),
    ]);
    expect(compactionAttempts.filter((attempt) => attempt.status === "fulfilled")).toHaveLength(1);
    expect(compactionAttempts.filter((attempt) => attempt.status === "rejected")).toHaveLength(1);
    const activeCompaction =
      (await first.getCompactionJob("maintenance-race-a")) ??
      (await first.getCompactionJob("maintenance-race-b"));
    expect(activeCompaction).toBeDefined();
    await first.cancelCompactionJob(activeCompaction?.id ?? "", 0, NOW);
    const rejectedCompactionId =
      activeCompaction?.id === "maintenance-race-a" ? "maintenance-race-b" : "maintenance-race-a";
    await second.createCompactionJob(compaction(rejectedCompactionId));

    const createGc = (store: Awaited<ReturnType<typeof openStore>>, id: string) =>
      store.createGarbageCollectionJob({
        id,
        candidateManifestVersions: [],
        candidateSegmentIds: [],
        candidateBlockIds: ["maintenance-source"],
        candidateTransactionIds: [],
        leaseCutoff: NOW,
        createdAt: NOW,
      });
    const gcAttempts = await Promise.allSettled([
      createGc(first, "maintenance-gc-a"),
      createGc(second, "maintenance-gc-b"),
    ]);
    expect(gcAttempts.filter((attempt) => attempt.status === "fulfilled")).toHaveLength(1);
    expect(gcAttempts.filter((attempt) => attempt.status === "rejected")).toHaveLength(1);
    const activeGc =
      (await first.getGarbageCollectionJob("maintenance-gc-a")) ??
      (await first.getGarbageCollectionJob("maintenance-gc-b"));
    expect(activeGc).toBeDefined();
    let gcStep = await first.runGarbageCollectionStep({
      jobId: activeGc?.id ?? "",
      expectedRevision: 0,
      maxItems: 1,
      updatedAt: NOW,
    });
    if (gcStep.job.state !== "completed") {
      gcStep = await first.runGarbageCollectionStep({
        jobId: activeGc?.id ?? "",
        expectedRevision: gcStep.job.revision,
        maxItems: 1,
        updatedAt: NOW,
      });
    }
    expect(gcStep.job.state).toBe("completed");
    expect(
      await readRawValue(indexedDB, name, "gc", "maintenance/active-garbage-collection"),
    ).toBeUndefined();
    const rejectedGcId =
      activeGc?.id === "maintenance-gc-a" ? "maintenance-gc-b" : "maintenance-gc-a";
    await createGc(second, rejectedGcId);
    expect(await first.checkIntegrity()).toMatchObject({ ok: true });
    first.close();
    second.close();
  });

  it("enforces the level-zero ceiling atomically on every publishing commit", async () => {
    const store = await openStore(new IDBFactory());
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "string", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await store.createTransaction(activeTransaction("first-writer", null));
    const firstStaged = await store.stageTransactionArtifacts({
      transactionId: "first-writer",
      expectedRevision: 0,
      blocks: [{ id: "first-block", bytes: Uint8Array.of(1) }],
      segments: [segment("first-segment", "first-writer", "first-block")],
      updatedAt: NOW,
    });
    await store.commitTransaction({
      transactionId: "first-writer",
      expectedTransactionRevision: firstStaged.revision,
      expectedManifestVersion: null,
      removedBlockIds: [],
      levelZeroSegmentLimits: [{ tableId: "events", limit: 1 }],
      committedAt: NOW,
    });

    await store.createTransaction(activeTransaction("second-writer", 0));
    const secondStaged = await store.stageTransactionArtifacts({
      transactionId: "second-writer",
      expectedRevision: 0,
      blocks: [{ id: "second-block", bytes: Uint8Array.of(2) }],
      segments: [segment("second-segment", "second-writer", "second-block")],
      updatedAt: NOW,
    });
    const commit = {
      transactionId: "second-writer",
      expectedTransactionRevision: secondStaged.revision,
      expectedManifestVersion: 0,
      removedBlockIds: [],
      committedAt: NOW,
    } as const;
    await expect(store.commitTransaction(commit)).rejects.toThrow(/level-zero segment limits/i);
    await expect(
      store.commitTransaction({
        ...commit,
        levelZeroSegmentLimits: [{ tableId: "other", limit: 1 }],
      }),
    ).rejects.toThrow(/no pending table/);
    await expect(
      store.commitTransaction({
        ...commit,
        levelZeroSegmentLimits: [{ tableId: "events", limit: 4097 }],
      }),
    ).rejects.toThrow(/4096/);
    await expect(
      store.commitTransaction({
        ...commit,
        levelZeroSegmentLimits: [{ tableId: "events", limit: 1 }],
      }),
    ).rejects.toMatchObject({ name: "CompactionBacklogError" });
    expect(await store.getCurrentManifestVersion()).toBe(0);
    expect(await store.getTransaction("second-writer")).toMatchObject({
      status: "active",
      revision: secondStaged.revision,
    });
    expect(await store.getSegment("second-segment")).toBeDefined();
    store.close();
  });

  it("bounds durable owner lifetimes and pages only expired owners in expiry order", async () => {
    const store = await openStore(new IDBFactory());
    const cutoff = "2026-08-24T12:10:00.000Z";
    for (const [id, expiresAt] of [
      ["lease-b", "2026-08-24T12:05:00.000Z"],
      ["lease-a", "2026-08-24T12:05:00.000Z"],
      ["lease-live", "2026-08-24T12:20:00.000Z"],
    ] as const) {
      await store.createLease({
        id,
        kind: "reader",
        manifestVersion: null,
        ownerId: id,
        createdAt: NOW,
        expiresAt,
        revision: 0,
      });
    }
    const firstLeasePage = await store.listExpiredLeasePage(cutoff, null, 1);
    expect(firstLeasePage.records.map((record) => record.id)).toEqual(["lease-a"]);
    expect(
      (await store.listExpiredLeasePage(cutoff, firstLeasePage.nextCursor, 2)).records.map(
        (record) => record.id,
      ),
    ).toEqual(["lease-b"]);
    await expect(
      store.renewLease({
        id: "lease-a",
        expectedRevision: 0,
        expiresAtCutoff: "2026-08-24T12:05:00.000Z",
        expiresAt: "2026-08-24T12:15:00.000Z",
      }),
    ).rejects.toBeInstanceOf(LeaseExpiredError);
    await expect(
      store.moveLease({
        id: "lease-a",
        expectedRevision: 0,
        manifestVersion: null,
        expiresAtCutoff: "2026-08-24T12:05:00.000Z",
        expiresAt: "2026-08-24T12:15:00.000Z",
      }),
    ).rejects.toBeInstanceOf(LeaseExpiredError);
    expect(await store.getLease("lease-a")).toMatchObject({ revision: 0 });
    await expect(
      store.createLease({
        id: "unbounded-lease",
        kind: "reader",
        manifestVersion: null,
        ownerId: "unbounded",
        createdAt: NOW,
        expiresAt: "2026-08-24T13:00:00.001Z",
        revision: 0,
      }),
    ).rejects.toThrow(/maximum TTL/);

    for (const [ownerId, expiresAt] of [
      ["temp-b", "2026-08-24T12:04:00.000Z"],
      ["temp-a", "2026-08-24T12:04:00.000Z"],
      ["temp-live", "2026-08-24T12:30:00.000Z"],
    ] as const) {
      await store.createTempOwner({ ownerId, createdAt: NOW, expiresAt, revision: 0 });
    }
    const firstTempPage = await store.listExpiredTempOwnerPage(cutoff, null, 1);
    expect(firstTempPage.records).toEqual(["temp-a"]);
    expect(
      (await store.listExpiredTempOwnerPage(cutoff, firstTempPage.nextCursor, 2)).records,
    ).toEqual(["temp-b"]);
    await expect(
      store.renewTempOwner({
        ownerId: "temp-a",
        expectedRevision: 0,
        expiresAtCutoff: "2026-08-24T12:04:00.000Z",
        expiresAt: "2026-08-24T12:20:00.000Z",
      }),
    ).rejects.toThrow(/expired/);
    expect(await store.getTempOwner("temp-a")).toMatchObject({ revision: 0 });
    await expect(
      store.createTempOwner({
        ownerId: "unbounded-temp",
        createdAt: NOW,
        expiresAt: "2026-08-24T13:00:00.001Z",
        revision: 0,
      }),
    ).rejects.toThrow(/maximum TTL/);
    store.close();
  });

  it("atomically drops a column, preserves aliases and zero-column row segments across reopen and snapshot", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    const columnA = { id: "a", name: "a", type: "number" as const, nullable: false };
    const columnB = { id: "b", name: "b", type: "number" as const, nullable: true };
    await store.addTable({
      managed: false,
      id: "evolution",
      name: "evolution",
      columns: [columnA],
      revision: 0,
      createdAt: NOW,
    });
    await store.addTable({
      managed: false,
      id: "other",
      name: "other",
      columns: [{ id: "x", name: "x", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await store.reserveRowIds("evolution", 1);
    await store.reserveRowIds("other", 1);
    const onlyABytes = await encodeBlock({ type: "number", values: [1] });
    const aliasBytes = await encodeBlock({ type: "number", values: [2] });
    await store.createTransaction(activeTransaction("evolution-writer", null));
    const evolutionJournal = await store.stageTransactionArtifacts({
      transactionId: "evolution-writer",
      expectedRevision: 0,
      blocks: [
        { id: "only-a", bytes: onlyABytes },
        { id: "alias", bytes: aliasBytes },
      ],
      segments: [
        {
          id: "evolution-segment",
          tableId: "evolution",
          transactionId: "evolution-writer",
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { a: ["alias", "only-a"] },
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          createdAt: NOW,
        },
      ],
      updatedAt: NOW,
    });
    await store.commitTransaction({
      transactionId: "evolution-writer",
      expectedTransactionRevision: evolutionJournal.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "evolution", limit: 4096 }],
      committedAt: NOW,
    });
    await store.createTransaction(activeTransaction("other-writer", 0));
    const otherBlockJournal = await store.updateTransaction("other-writer", 0, {
      pendingBlockIds: ["alias"],
      updatedAt: NOW,
    });
    const otherJournal = await store.stageTransactionArtifacts({
      transactionId: "other-writer",
      expectedRevision: otherBlockJournal.revision,
      blocks: [],
      segments: [
        {
          id: "other-segment",
          tableId: "other",
          transactionId: "other-writer",
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { x: ["alias"] },
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          createdAt: NOW,
        },
      ],
      updatedAt: NOW,
    });
    await store.commitTransaction({
      transactionId: "other-writer",
      expectedTransactionRevision: otherJournal.revision,
      expectedManifestVersion: 0,
      levelZeroSegmentLimits: [{ tableId: "other", limit: 4096 }],
      committedAt: NOW,
    });
    await store.updateTable("evolution", 0, { columns: [columnA, columnB] });
    const ownerBefore = await store.getTransaction("evolution-writer");
    expect(
      await store.dropTableColumn({
        tableId: "evolution",
        columnId: "a",
        expectedTableRevision: 1,
        expectedManifestVersion: 1,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: NOW,
      }),
    ).toMatchObject({ version: 2, changedTableIds: ["evolution"] });
    expect(await readManifestBlockIds(store, 2)).toEqual(["alias"]);
    expect(await store.getTable("evolution")).toMatchObject({
      columns: [columnB],
      revision: 2,
    });
    expect(await store.getSegment("evolution-segment")).toMatchObject({ columnBlockIds: {} });
    expect(await store.getTransaction("evolution-writer")).toEqual(ownerBefore);
    expect(await store.getBlock("only-a")).toEqual(onlyABytes);
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true, issues: [] });
    store.close();

    store = await openStore(indexedDB, name);
    expect(await store.getSegment("evolution-segment")).toMatchObject({ columnBlockIds: {} });
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true, issues: [] });
    const snapshot = await exportSnapshotFrames(store);
    const imported = await openStore(new IDBFactory());
    await importSnapshotFrames(imported, snapshot);
    expect(await imported.getSegment("evolution-segment")).toMatchObject({ columnBlockIds: {} });
    expect((await imported.getTable("evolution"))?.columns).toEqual([columnB]);
    expect(await imported.checkIntegrity({ mode: "full" })).toMatchObject({
      ok: true,
      issues: [],
    });
    imported.close();
    store.close();
  });

  it("rolls back every column-drop family when IndexedDB aborts after a segment write", async () => {
    const indexedDB = new IDBFactory();
    const armAbort = abortNextColumnDropAfterSegmentWrite(indexedDB);
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    const columns = [
      { id: "a", name: "a", type: "number" as const, nullable: false },
      { id: "b", name: "b", type: "number" as const, nullable: true },
    ];
    await store.addTable({
      managed: false,
      id: "atomic-drop",
      name: "atomic_drop",
      columns,
      revision: 0,
      createdAt: NOW,
    });
    const aBytes = await encodeBlock({ type: "number", values: [1] });
    const bBytes = await encodeBlock({ type: "number", values: [2] });
    await store.createTransaction(activeTransaction("atomic-writer", null));
    const journal = await store.stageTransactionArtifacts({
      transactionId: "atomic-writer",
      expectedRevision: 0,
      blocks: [
        { id: "atomic-a", bytes: aBytes },
        { id: "atomic-b", bytes: bBytes },
      ],
      segments: [
        {
          id: "atomic-segment",
          tableId: "atomic-drop",
          transactionId: "atomic-writer",
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { a: ["atomic-a"], b: ["atomic-b"] },
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          createdAt: NOW,
        },
      ],
      updatedAt: NOW,
    });
    await store.commitTransaction({
      transactionId: "atomic-writer",
      expectedTransactionRevision: journal.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "atomic-drop", limit: 4096 }],
      committedAt: NOW,
    });
    const beforeProbe = await store.getCatalogProbe();
    armAbort();
    await expect(
      store.dropTableColumn({
        tableId: "atomic-drop",
        columnId: "a",
        expectedTableRevision: 0,
        expectedManifestVersion: 0,
        expectedCatalogEpoch: beforeProbe.catalogEpoch,
        committedAt: NOW,
      }),
    ).rejects.toThrow();
    expect(await store.getTable("atomic-drop")).toMatchObject({ columns, revision: 0 });
    expect(await store.getSegment("atomic-segment")).toMatchObject({
      columnBlockIds: { a: ["atomic-a"], b: ["atomic-b"] },
    });
    expect(await store.getCurrentManifest()).toMatchObject({ version: 0 });
    expect(await readManifestBlockIds(store, 0)).toEqual(["atomic-a", "atomic-b"]);
    expect(await store.getCatalogProbe()).toEqual(beforeProbe);
    expect(await store.getBlock("atomic-a")).toEqual(aBytes);
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true, issues: [] });
    store.close();

    store = await openStore(indexedDB, name);
    expect(await store.getTable("atomic-drop")).toMatchObject({ columns, revision: 0 });
    expect(await store.getSegment("atomic-segment")).toMatchObject({
      columnBlockIds: { a: ["atomic-a"], b: ["atomic-b"] },
    });
    expect(await store.getCurrentManifest()).toMatchObject({ version: 0 });
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true, issues: [] });
    store.close();
  });

  it("throws typed corruption for malformed control records and never drops malformed tables", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await mutate(indexedDB, name, ["blocks", "catalog", "leases", "transactions"], (tx) => {
      tx.objectStore("catalog").put("not-a-version", "manifest/current");
      tx.objectStore("catalog").put(
        {
          id: "broken-table",
          name: "broken",
          columns: [{}],
          revision: 0,
          createdAt: NOW,
        },
        "table/id/broken-table",
      );
      tx.objectStore("catalog").put("broken-table", "table/name/broken");
      tx.objectStore("transactions").put(
        { ...activeTransaction("broken-transaction", null), status: "committed" },
        "broken-transaction",
      );
      tx.objectStore("leases").put(
        {
          id: "broken-lease",
          kind: "reader",
          manifestVersion: null,
          ownerId: "owner",
          expiresAt: "not-a-date",
          revision: 0,
        },
        "broken-lease",
      );
      tx.objectStore("blocks").put("not-bytes", "broken-block");
    });

    await expect(store.getCurrentManifestVersion()).rejects.toBeInstanceOf(StorageCorruptionError);
    await expect(store.listTables()).rejects.toBeInstanceOf(StorageCorruptionError);
    await expect(store.getTransaction("broken-transaction")).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
    await expect(store.getLease("broken-lease")).rejects.toBeInstanceOf(StorageCorruptionError);
    await expect(store.getBlock("broken-block")).rejects.toBeInstanceOf(StorageCorruptionError);
    const report = await store.checkIntegrity({ maxIssues: 3 });
    expect(report.ok).toBe(false);
    expect(report.issueCount).toBeGreaterThanOrEqual(5);
    expect(report.issues).toHaveLength(3);
    store.close();
  });

  it("rejects unknown fields on top-level and nested v1 control records", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    const job = await store.createGarbageCollectionJob({
      id: "strict-job",
      candidateManifestVersions: [],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: NOW,
      createdAt: NOW,
    });
    expect(job.id).toBe("strict-job");
    const table = {
      id: "strict-table",
      name: "strict_table",
      columns: [{ id: "id", name: "id", type: "number", nullable: false }],
      uniqueKeyColumnId: "id",
      revision: 0,
      createdAt: NOW,
    };
    await mutate(
      indexedDB,
      name,
      ["catalog", "gc", "leases", "manifests", "segments", "transactions"],
      (tx) => {
        const catalog = tx.objectStore("catalog");
        catalog.put({ ...table, future: true }, "table/id/unknown-top");
        catalog.put(
          { ...table, id: "unknown-nested", columns: [{ ...table.columns[0], future: true }] },
          "table/id/unknown-nested",
        );
        catalog.put({ versions: [], hasBase: false, future: true }, [
          "unique-key-chunk-index",
          "strict-table",
        ]);
        tx.objectStore("transactions").put(
          { ...activeTransaction("unknown-transaction", null), future: true },
          "unknown-transaction",
        );
        tx.objectStore("leases").put(
          {
            id: "unknown-lease",
            kind: "reader",
            manifestVersion: null,
            ownerId: "owner",
            expiresAt: NOW,
            revision: 0,
            future: true,
          },
          "unknown-lease",
        );
        tx.objectStore("segments").put(
          { ...segment("unknown-segment", "unknown-transaction", "unused"), future: true },
          "unknown-segment",
        );
        tx.objectStore("manifests").put(
          {
            version: 0,
            previousVersion: null,
            blockIds: [],
            deltaDepth: 0,
            createdAt: NOW,
            future: true,
          },
          0,
        );
      },
    );

    await expect(store.getTable("unknown-top")).rejects.toBeInstanceOf(StorageCorruptionError);
    await expect(store.getTable("unknown-nested")).rejects.toBeInstanceOf(StorageCorruptionError);
    await expect(store.getTransaction("unknown-transaction")).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
    await expect(store.getLease("unknown-lease")).rejects.toBeInstanceOf(StorageCorruptionError);
    await expect(store.getSegment("unknown-segment")).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
    await expect(store.getManifest(0)).rejects.toBeInstanceOf(StorageCorruptionError);
    await expect(store.getExistingUniqueKeys("strict-table", ["x"])).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );

    await mutate(indexedDB, name, "gc", (tx) => {
      const request = tx.objectStore("gc").get("garbage-collection-job/strict-job");
      request.onsuccess = () => {
        const envelope = request.result as { kind: string; record: Record<string, unknown> };
        tx.objectStore("gc").put(
          { ...envelope, record: { ...envelope.record, future: true } },
          "garbage-collection-job/strict-job",
        );
      };
    });
    await expect(store.getGarbageCollectionJob("strict-job")).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
    store.close();
  });

  it("bounds manifest resolution and rejects key, predecessor, and depth corruption", async () => {
    for (const record of [
      {
        version: 2,
        previousVersion: null,
        blockIds: [],
        deltaDepth: 0,
        createdAt: NOW,
      },
      {
        version: 1,
        previousVersion: 1,
        addedBlockIds: [],
        removedBlockIds: [],
        deltaDepth: 1,
        createdAt: NOW,
      },
      {
        version: 1,
        previousVersion: 0,
        addedBlockIds: [],
        removedBlockIds: [],
        deltaDepth: 2,
        createdAt: NOW,
      },
    ]) {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      const store = await openStore(indexedDB, name);
      await mutate(indexedDB, name, ["catalog", "manifests"], (tx) => {
        tx.objectStore("catalog").put(record.version === 2 ? 0 : 1, "manifest/current");
        tx.objectStore("manifests").put(record, record.version === 2 ? 0 : 1);
      });
      await expect(store.getCurrentManifest()).rejects.toBeInstanceOf(StorageCorruptionError);
      store.close();
    }
  });

  it("reads exact-version block membership without touching unrelated payloads and fails closed", async () => {
    const indexedDB = new IDBFactory();
    const requests = countManifestAndBlockGets(indexedDB);
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    const bytes = await encodeBlock({ type: "number", values: [42] });
    const blocks = Array.from({ length: 200 }, (_, index) => ({
      id: `membership/${String(index).padStart(4, "0")}`,
      bytes,
    }));
    await injectBlocks(indexedDB, name, blocks);
    await publishManifest(store, {
      expectedVersion: null,
      blockIds: blocks.map((block) => block.id),
      createdAt: NOW,
    });
    expect(await store.checkIntegrity()).toMatchObject({ ok: true });
    expect(store._residentStateForTests().manifestCacheBlockIds).toBe(0);

    requests.reset();
    await expect(
      store.hasManifestBlocks(0, [
        blocks[137]?.id ?? "",
        "membership/missing",
        blocks[137]?.id ?? "",
      ]),
    ).resolves.toEqual([true, false, true]);
    expect(requests.counts()).toEqual({ blocks: 0, manifests: 1 });

    requests.reset();
    await expect(store.readManifestBlock(0, blocks[137]?.id ?? "")).resolves.toEqual(bytes);
    expect(requests.counts()).toEqual({ blocks: 1, manifests: 1 });

    requests.reset();
    await expect(store.hasManifestBlocks(null, [blocks[0]?.id ?? ""])).resolves.toEqual([false]);
    await expect(store.readManifestBlock(null, blocks[0]?.id ?? "")).resolves.toBeUndefined();
    await expect(store.hasManifestBlocks(999, [blocks[0]?.id ?? ""])).resolves.toEqual([false]);
    expect(requests.counts()).toEqual({ blocks: 0, manifests: 1 });

    requests.reset();
    await expect(
      store.hasManifestBlocks(
        0,
        Array.from(
          { length: MAX_MANIFEST_BLOCK_PRESENCE_IDS + 1 },
          (_, index) => `id/${String(index)}`,
        ),
      ),
    ).rejects.toBeInstanceOf(RangeError);
    await expect(store.hasManifestBlocks(-1, [])).rejects.toBeInstanceOf(RangeError);
    await expect(store.readManifestBlock(0, "")).rejects.toBeInstanceOf(TypeError);
    expect(requests.counts()).toEqual({ blocks: 0, manifests: 0 });

    const liveId = blocks[137]?.id ?? "";
    await mutate(indexedDB, name, "blocks", (transaction) => {
      transaction.objectStore("blocks").delete(liveId);
    });
    await expect(store.readManifestBlock(0, liveId)).rejects.toBeInstanceOf(StorageCorruptionError);
    await mutate(indexedDB, name, "blocks", (transaction) => {
      transaction.objectStore("blocks").put("not-bytes", liveId);
    });
    await expect(store.readManifestBlock(0, liveId)).rejects.toBeInstanceOf(StorageCorruptionError);

    await mutate(indexedDB, name, "manifests", (transaction) => {
      const manifests = transaction.objectStore("manifests");
      const request = manifests.get(0);
      request.onsuccess = () => {
        manifests.put({ ...(request.result as object), prunedAt: NOW }, 0);
      };
    });
    requests.reset();
    await expect(store.hasManifestBlocks(0, [liveId])).resolves.toEqual([false]);
    await expect(store.readManifestBlock(0, liveId)).resolves.toBeUndefined();
    expect(requests.counts()).toEqual({ blocks: 0, manifests: 2 });
    store.close();
  });

  it("bounds tombstone cleanup, resumes its durable cursor, and preserves readable deltas", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    const manifestRecords = Array.from({ length: 70 }, (_, version) => ({
      version,
      previousVersion: version === 0 ? null : version - 1,
      liveBlockCount: 0,
      liveBlockBytes: 0,
      changedTableIds: [],
      createdAt: NOW,
      ...(version < 40 ? { prunedAt: NOW } : {}),
    }));
    await mutate(indexedDB, name, ["catalog", "manifests", "statistics"], (tx) => {
      const manifests = tx.objectStore("manifests");
      for (const record of manifestRecords) manifests.put(record, record.version);
      tx.objectStore("catalog").put(69, "manifest/current");
      adjustRawRecordLedger(tx, {
        manifestCount: manifestRecords.length,
        manifestBytes: manifestRecords.reduce(
          (total, record) => total + manifestRecordRetainedReservationBytes(record),
          0,
        ),
      });
    });

    for (let call = 0; call < 4; call += 1) {
      expect(await store.removePrunedManifestRecords(5)).toBeLessThanOrEqual(5);
    }
    store.close();
    store = await openStore(indexedDB, name);
    let removed = 0;
    for (let call = 0; call < 32; call += 1) {
      const page = await store.removePrunedManifestRecords(5);
      expect(page).toBeLessThanOrEqual(5);
      removed += page;
      if ((await store.getManifest(0)) === undefined && page === 0) break;
    }
    expect(removed).toBe(40);
    expect(await store.getManifest(39)).toBeUndefined();
    expect(await store.getManifest(40)).toMatchObject({ version: 40, liveBlockCount: 0 });
    expect(await store.getCurrentManifest()).toMatchObject({ version: 69, liveBlockCount: 0 });
    store.close();
  });

  it("prunes a manifest while its reservation ledger is at the exact byte ceiling", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await publishManifest(store, { expectedVersion: null, blockIds: [], createdAt: NOW });
    await publishManifest(store, { expectedVersion: 0, blockIds: [], createdAt: NOW });
    await mutate(indexedDB, name, "statistics", (transaction) => {
      putRawRecordLedger(transaction, {
        manifestCount: 2,
        manifestBytes: MAX_MANIFEST_RETAINED_BYTES,
        segmentCount: 0,
        segmentBytes: 0,
      });
    });
    const job = await store.createGarbageCollectionJob({
      id: "manifest-byte-ceiling",
      candidateManifestVersions: [0],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: NOW,
      createdAt: NOW,
    });
    const result = await store.runGarbageCollectionStep({
      jobId: job.id,
      expectedRevision: job.revision,
      maxItems: 1,
      updatedAt: NOW,
    });
    expect(result.prunedManifestVersions).toEqual([0]);
    await expect(
      readRawValue(indexedDB, name, "statistics", "resource/records"),
    ).resolves.toMatchObject({
      manifestCount: 2,
      manifestBytes: MAX_MANIFEST_RETAINED_BYTES,
    });
    await expect(store.getManifest(0)).resolves.toMatchObject({ version: 0, prunedAt: NOW });
    store.close();
  });

  it("pages historical provenance after its pruned manifest summary is deleted", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    const manifestRecords = [
      {
        version: 0,
        previousVersion: null,
        liveBlockCount: 100,
        liveBlockBytes: 100,
        changedTableIds: [],
        createdAt: NOW,
        prunedAt: NOW,
      },
      {
        version: 1,
        previousVersion: 0,
        liveBlockCount: 0,
        liveBlockBytes: 0,
        changedTableIds: [],
        createdAt: NOW,
      },
    ];
    await mutate(indexedDB, name, ["catalog", "manifests", "statistics"], (tx) => {
      for (const record of manifestRecords) {
        tx.objectStore("manifests").put(record, record.version);
      }
      const catalog = tx.objectStore("catalog");
      catalog.put(1, "manifest/current");
      for (let index = 0; index < 100; index += 1) {
        const blockId = `retired-${String(index).padStart(3, "0")}`;
        catalog.put(
          {
            blockId,
            byteLength: 1,
            checksum: index,
            addedVersion: 0,
            removedVersion: 1,
          },
          ["manifest-block", blockId],
        );
      }
      adjustRawRecordLedger(tx, {
        manifestCount: manifestRecords.length,
        manifestBytes: manifestRecords.reduce(
          (total, record) => total + manifestRecordRetainedReservationBytes(record),
          0,
        ),
      });
    });
    expect(await store.removePrunedManifestRecords(64)).toBe(1);
    expect(await store.getManifest(0)).toBeUndefined();

    const first = await store.listManifestBlockPage({
      version: 0,
      afterBlockId: null,
      limit: 64,
    });
    expect(first.records).toHaveLength(64);
    expect(first.nextCursor).toBe(first.records.at(-1)?.blockId);
    const second = await store.listManifestBlockPage({
      version: 0,
      afterBlockId: first.nextCursor,
      limit: 64,
    });
    expect(second.records).toHaveLength(36);
    expect(second.nextCursor).toBeNull();
    const retiredFirst = await store.listRetiredManifestBlockPage({
      removedThroughVersion: 1,
      afterBlockId: null,
      limit: 64,
    });
    expect(retiredFirst.records).toHaveLength(64);
    const retiredSecond = await store.listRetiredManifestBlockPage({
      removedThroughVersion: 1,
      afterBlockId: retiredFirst.nextCursor,
      limit: 64,
    });
    expect(retiredSecond.records).toHaveLength(36);
    expect(retiredSecond.nextCursor).toBeNull();
    store.close();
  });

  it("treats manifest cleanup on a never-published database as already complete", async () => {
    const store = await openStore(new IDBFactory());
    await expect(store.removePrunedManifestRecords(64)).resolves.toBe(0);
    await expect(store.removePrunedManifestRecords(64)).resolves.toBe(0);
    expect(await store.getCurrentManifestVersion()).toBeNull();
    store.close();
  });

  it("degrades incomplete base and delta accelerators to full scans and invalid snapshot state", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "articles",
      name: "articles",
      columns: [{ id: "body", name: "body", type: "string", nullable: false }],
      ftsColumns: {
        body: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "ready",
          buildFromVersion: 0,
        },
      },
      revision: 0,
      createdAt: NOW,
    });
    await publishManifest(store, { expectedVersion: null, blockIds: [], createdAt: NOW });
    await store.writeFtsBase("articles", "body", {
      coversVersion: 0,
      chunks: [
        [{ term: "alpha", rowIds: [1n], tf: [1] }],
        [{ term: "omega", rowIds: [2n], tf: [1] }],
      ],
      totalTokens: 2,
    });
    await expect(
      store.readFtsCandidates("articles", "body", [{ lower: "alpha", upper: "omega" }], 0, 1),
    ).resolves.toMatchObject({ rowIdsByTerm: [[]], overflow: true });
    await expect(
      store.readFtsCandidates("articles", "body", [{ term: "alpha", prefix: false }], 0, 0),
    ).rejects.toBeInstanceOf(RangeError);
    await mutate(indexedDB, name, "catalog", (tx) => {
      const catalog = tx.objectStore("catalog");
      const toc = catalog.get("fts-base-index/articles/body");
      toc.onsuccess = () => {
        const generation = (toc.result as { generation: string }).generation;
        catalog.delete(
          `fts-base/articles/body/generation/${encodeURIComponent(generation)}/000000`,
        );
      };
    });
    expect(
      await store.readFtsCandidates("articles", "body", [{ term: "alpha", prefix: false }], 0),
    ).toMatchObject({ hasBase: false });
    const exported = await exportSnapshotFrames(store);
    const imported = await openStore(new IDBFactory());
    await importSnapshotFrames(imported, exported);
    expect((await imported.getTable("articles"))?.ftsColumns?.body?.state).toBe("invalid");
    expect(
      await imported.readFtsCandidates("articles", "body", [{ term: "alpha", prefix: false }], 0),
    ).toMatchObject({ hasBase: false });
    imported.close();

    await store.writeFtsBase("articles", "body", {
      coversVersion: 0,
      chunks: [[{ term: "alpha", rowIds: [1n], tf: [1] }]],
      totalTokens: 1,
    });
    await store.createTransaction(activeTransaction("delta-transaction", 0));
    await store.commitTransaction({
      transactionId: "delta-transaction",
      expectedTransactionRevision: 0,
      expectedManifestVersion: 0,
      ftsChanges: [
        {
          tableId: "articles",
          columns: [
            {
              columnId: "body",
              postings: [{ term: "beta", rowIds: [2n], tf: [1] }],
              totalTokens: 1,
            },
          ],
        },
      ],
      committedAt: NOW,
    });
    await mutate(indexedDB, name, "catalog", (tx) => {
      tx.objectStore("catalog").delete("fts-chunk/articles/body/1");
    });
    store.close();
    store = await openStore(indexedDB, name);
    expect(
      await store.readFtsCandidates("articles", "body", [{ term: "beta", prefix: false }], 1),
    ).toMatchObject({ hasBase: false });
    store.close();
  });

  it("omits an oversized posting generation atomically and restores it invalid", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const source = await openStore(indexedDB, name);
    await source.addTable({
      managed: false,
      id: "oversized-fts",
      name: "oversized_fts",
      columns: [{ id: "body", name: "body", type: "string", nullable: false }],
      ftsColumns: {
        body: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "ready",
          buildFromVersion: 0,
        },
      },
      revision: 0,
      createdAt: NOW,
    });
    await publishManifest(source, { expectedVersion: null, blockIds: [], createdAt: NOW });
    const postings = Array.from({ length: 257 }, (_, index) => ({
      term: `${String(index).padStart(4, "0")}:${"x".repeat(65_531)}`,
      rowIds: [BigInt(index + 1)],
      tf: [1],
    }));
    await mutate(indexedDB, name, "catalog", (transaction) => {
      const catalog = transaction.objectStore("catalog");
      catalog.put(
        {
          coversVersion: 0,
          totalTokens: postings.length,
          boundaries: [{ first: postings[0]?.term, last: postings.at(-1)?.term }],
          generation: "oversized-generation",
        },
        "fts-base-index/oversized-fts/body",
      );
      catalog.put({ versions: [] }, "fts-chunk/index/oversized-fts/body");
      catalog.put(postings, "fts-base/oversized-fts/body/generation/oversized-generation/000000");
    });

    const snapshot = await exportSnapshotFrames(source);
    expect(snapshot.header.kinds["posting-page"]).toMatchObject({
      frameCount: 0,
      itemCount: 0,
      storedBytes: 0,
    });
    const catalogItems = snapshot.frames
      .filter((frame) => frame.kind === "catalog-page")
      .flatMap((frame) => decodeSnapshotMetadataItems("catalog-page", frame.payload));
    expect(catalogItems).toHaveLength(1);
    expect(catalogItems[0]).toMatchObject({
      kind: "table",
      record: { ftsColumns: { body: { state: "invalid" } } },
    });

    const restored = await openStore(new IDBFactory());
    await importSnapshotFrames(restored, snapshot);
    expect((await restored.getTable("oversized-fts"))?.ftsColumns?.body?.state).toBe("invalid");
    expect(
      await restored.readFtsCandidates(
        "oversized-fts",
        "body",
        [{ term: postings[0]?.term ?? "", prefix: false }],
        0,
      ),
    ).toMatchObject({ hasBase: false });
    restored.close();
    source.close();
  });
  it("never treats missing, legacy, partition, or tail UNIQUE membership as empty", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "users",
      name: "users",
      columns: [{ id: "email", name: "email", type: "string", nullable: false }],
      uniqueKeyColumnId: "email",
      primaryKeyColumnIds: ["email"],
      uniqueKeyLookupReady: true,
      revision: 0,
      createdAt: NOW,
    });
    await mutate(indexedDB, name, "catalog", (tx) => {
      tx.objectStore("catalog").delete(["unique-key-chunk-index", "users"]);
    });
    await store.createTransaction(activeTransaction("missing-index", null));
    await expect(
      store.commitTransaction({
        transactionId: "missing-index",
        expectedTransactionRevision: 0,
        expectedManifestVersion: null,
        uniqueKeyChanges: [
          { tableId: "users", keyTokens: ["string:a@example.com"], requireAbsent: true },
        ],
        committedAt: NOW,
      }),
    ).rejects.toBeInstanceOf(StorageCorruptionError);
    expect(await store.getCurrentManifestVersion()).toBeNull();

    await mutate(indexedDB, name, "catalog", (tx) => {
      tx.objectStore("catalog").put([], ["unique-key-chunk-index", "users"]);
    });
    await expect(store.getExistingUniqueKeys("users", ["x"])).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
    await mutate(indexedDB, name, "catalog", (tx) => {
      tx.objectStore("catalog").put({ versions: [], hasBase: true, partitions: 1, tokenCount: 1 }, [
        "unique-key-chunk-index",
        "users",
      ]);
    });
    await expect(store.getExistingUniqueKeys("users", ["x"])).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
    await mutate(indexedDB, name, "catalog", (tx) => {
      tx.objectStore("catalog").put({ versions: [0], hasBase: false }, [
        "unique-key-chunk-index",
        "users",
      ]);
    });
    store.close();
    store = await openStore(indexedDB, name);
    await expect(store.getExistingUniqueKeys("users", ["x"])).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
    store.close();
  });

  it("stages only owned, unique segments and refuses the whole batch atomically", async () => {
    const indexedDB = new IDBFactory();
    const store = await openStore(indexedDB);
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await store.createTransaction(activeTransaction("owner", null));
    const duplicate = segment("duplicate", "owner", "new-block");
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "owner",
        expectedRevision: 0,
        blocks: [{ id: "new-block", bytes: Uint8Array.of(1) }],
        segments: [duplicate, duplicate],
        updatedAt: NOW,
      }),
    ).rejects.toThrow(/already exists/);
    expect(await store.getBlock("new-block")).toBeUndefined();
    expect(await store.getTransaction("owner")).toMatchObject({ revision: 0 });
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "owner",
        expectedRevision: 0,
        blocks: [],
        segments: [segment("foreign", "someone-else", "unused")],
        updatedAt: NOW,
      }),
    ).rejects.toThrow(/another transaction/);
    expect(await store.getSegment("foreign")).toBeUndefined();
    store.close();
  });

  it("renews only live ownership and aborts expiry atomically without losing journals", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    const cutoff = "2026-08-24T12:10:00.000Z";
    const oldExpiry = "2026-08-24T12:20:00.000Z";
    const newExpiry = "2026-08-24T12:30:00.000Z";
    await injectBlocks(indexedDB, name, [{ id: "journal-block", bytes: Uint8Array.of(1) }]);
    await store.createTransaction({
      ...activeTransaction("leased", null),
      ownerId: "writer-a",
      expiresAt: oldExpiry,
    });
    await store.updateTransaction("leased", 0, {
      pendingBlockIds: ["journal-block"],
      updatedAt: NOW,
    });
    expect(
      await store.renewTransaction({
        transactionId: "leased",
        ownerId: "writer-b",
        expiresAtCutoff: cutoff,
        expiresAt: newExpiry,
      }),
    ).toBe(false);
    expect(
      await store.renewTransaction({
        transactionId: "leased",
        ownerId: "writer-a",
        expiresAtCutoff: cutoff,
        expiresAt: newExpiry,
      }),
    ).toBe(true);
    expect(await store.getTransaction("leased")).toMatchObject({
      revision: 1,
      expiresAt: newExpiry,
      pendingBlockIds: ["journal-block"],
    });
    expect(
      await store.abortTransactionIfExpired({
        transactionId: "leased",
        expectedOwnerId: "writer-a",
        expiresAtCutoff: oldExpiry,
        updatedAt: newExpiry,
      }),
    ).toBeUndefined();
    const aborted = await store.abortTransactionIfExpired({
      transactionId: "leased",
      expectedOwnerId: "writer-a",
      expiresAtCutoff: newExpiry,
      updatedAt: newExpiry,
    });
    expect(aborted).toMatchObject({
      status: "aborted",
      revision: 2,
      pendingBlockIds: ["journal-block"],
      expiresAt: newExpiry,
    });
    store.close();
    store = await openStore(indexedDB, name);
    expect(
      await store.renewTransaction({
        transactionId: "leased",
        ownerId: "writer-a",
        expiresAtCutoff: cutoff,
        expiresAt: "2026-08-24T12:40:00.000Z",
      }),
    ).toBe(false);
    expect(await store.getTransaction("leased")).toMatchObject({
      status: "aborted",
      revision: 2,
    });

    await store.createTransaction({
      ...activeTransaction("already-expired", null),
      ownerId: "writer-a",
      expiresAt: cutoff,
    });
    expect(
      await store.renewTransaction({
        transactionId: "already-expired",
        ownerId: "writer-a",
        expiresAtCutoff: cutoff,
        expiresAt: newExpiry,
      }),
    ).toBe(false);
    expect(await store.getTransaction("already-expired")).toMatchObject({ expiresAt: cutoff });
    store.close();
  });

  it("refuses row-id, auto-increment, revision, epoch, and manifest overflow atomically", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    const rowEnd = 1n << 64n;
    const autoEnd = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    await store.addTable({
      managed: false,
      id: "overflow-row",
      name: "overflow_row",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await store.addTable({
      managed: false,
      id: "overflow-auto",
      name: "overflow_auto",
      columns: [
        {
          id: "id",
          name: "id",
          type: "number",
          nullable: false,
          defaultValue: { kind: "autoincrement" },
        },
      ],
      uniqueKeyColumnId: "id",
      revision: 0,
      createdAt: NOW,
    });
    await mutate(indexedDB, name, ["catalog", "manifests", "transactions"], (tx) => {
      const catalog = tx.objectStore("catalog");
      catalog.put(rowEnd, "row-id/overflow-row");
      catalog.put(autoEnd, "auto-increment/overflow-auto/id");
      catalog.put(Number.MAX_SAFE_INTEGER, "catalog/epoch");
      catalog.put(Number.MAX_SAFE_INTEGER, "manifest/current");
      tx.objectStore("manifests").put(
        {
          version: Number.MAX_SAFE_INTEGER,
          previousVersion: Number.MAX_SAFE_INTEGER - 1,
          liveBlockCount: 0,
          liveBlockBytes: 0,
          changedTableIds: [],
          createdAt: NOW,
        },
        Number.MAX_SAFE_INTEGER,
      );
      tx.objectStore("transactions").put(
        {
          ...activeTransaction("overflow-transaction", null),
          revision: Number.MAX_SAFE_INTEGER,
          schemaEpochGuard: Number.MAX_SAFE_INTEGER,
        },
        "overflow-transaction",
      );
    });

    await expect(store.reserveRowIds("overflow-row", 1)).rejects.toBeInstanceOf(RangeError);
    await expect(store.reserveRowIds("overflow-row", 1)).rejects.toBeInstanceOf(RangeError);
    await expect(store.reserveAutoIncrement("overflow-auto", "id", 1)).rejects.toBeInstanceOf(
      RangeError,
    );
    await expect(
      store.updateTransaction("overflow-transaction", Number.MAX_SAFE_INTEGER, { updatedAt: NOW }),
    ).rejects.toBeInstanceOf(RangeError);
    expect(await store.getTransaction("overflow-transaction")).toMatchObject({
      revision: Number.MAX_SAFE_INTEGER,
      status: "active",
    });
    await expect(
      publishManifest(store, {
        expectedVersion: Number.MAX_SAFE_INTEGER,
        blockIds: [],
        createdAt: NOW,
      }),
    ).rejects.toBeInstanceOf(RangeError);
    expect(await store.getCurrentManifestVersion()).toBe(Number.MAX_SAFE_INTEGER);
    await expect(
      store.addTable({
        managed: false,
        id: "epoch-overflow",
        name: "epoch_overflow",
        columns: [{ id: "id", name: "id", type: "number", nullable: false }],
        revision: 0,
        createdAt: NOW,
      }),
    ).rejects.toBeInstanceOf(RangeError);
    expect(await store.getTable("epoch-overflow")).toBeUndefined();
    store.close();
  });

  it("rolls artifacts back atomically and refuses published, foreign, or retained roots", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await store.createTransaction(activeTransaction("happy", null));
    const staged = await store.stageTransactionArtifacts({
      transactionId: "happy",
      expectedRevision: 0,
      blocks: [{ id: "happy-block", bytes: Uint8Array.of(1) }],
      segments: [segment("happy-segment", "happy", "happy-block")],
      updatedAt: NOW,
    });
    const rolledBack = await store.rollbackTransactionArtifacts({
      transactionId: "happy",
      expectedRevision: staged.revision,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      removeBlockIds: ["happy-block"],
      removeSegmentIds: ["happy-segment"],
      updatedAt: NOW,
    });
    expect(rolledBack).toMatchObject({ pendingBlockIds: [], pendingSegmentIds: [], revision: 2 });
    expect(await store.getBlock("happy-block")).toBeUndefined();
    expect(await store.getSegment("happy-segment")).toBeUndefined();

    await injectBlocks(indexedDB, name, [{ id: "historical-live", bytes: Uint8Array.of(2) }]);
    await publishManifest(store, {
      expectedVersion: null,
      blockIds: ["historical-live"],
      createdAt: NOW,
    });
    await mutate(indexedDB, name, ["catalog", "manifests"], (tx) => {
      tx.objectStore("manifests").put(
        {
          version: 1,
          previousVersion: 0,
          liveBlockCount: 0,
          liveBlockBytes: 0,
          changedTableIds: [],
          createdAt: NOW,
        },
        1,
      );
      const provenance = tx.objectStore("catalog").get(["manifest-block", "historical-live"]);
      provenance.onsuccess = () => {
        tx.objectStore("catalog").put({ ...(provenance.result as object), removedVersion: 1 }, [
          "manifest-block",
          "historical-live",
        ]);
      };
      tx.objectStore("catalog").put(1, "manifest/current");
    });
    await store.createTransaction(activeTransaction("corrupt-journal", 1));
    const corruptJournal = await store.getTransaction("corrupt-journal");
    expect(corruptJournal).toBeDefined();
    if (corruptJournal === undefined) throw new Error("Expected corrupt-journal transaction");
    await mutate(indexedDB, name, "transactions", (tx) => {
      tx.objectStore("transactions").put(
        { ...corruptJournal, pendingBlockIds: ["historical-live"] },
        "corrupt-journal",
      );
    });
    await expect(
      store.rollbackTransactionArtifacts({
        transactionId: "corrupt-journal",
        expectedRevision: 0,
        pendingBlockIds: [],
        pendingSegmentIds: [],
        removeBlockIds: ["historical-live"],
        removeSegmentIds: [],
        updatedAt: NOW,
      }),
    ).rejects.toBeInstanceOf(StorageCorruptionError);
    expect(await store.getBlock("historical-live")).toEqual(Uint8Array.of(2));
    expect(await store.getTransaction("corrupt-journal")).toMatchObject({
      revision: 0,
      pendingBlockIds: ["historical-live"],
    });
    store.close();

    const foreignName = crypto.randomUUID();
    store = await openStore(indexedDB, foreignName);
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await injectBlocks(indexedDB, foreignName, [{ id: "foreign-block", bytes: Uint8Array.of(3) }]);
    await injectSegments(indexedDB, foreignName, [
      segment("foreign-segment", "foreign-owner", "foreign-block"),
    ]);
    await store.createTransaction(activeTransaction("victim", null));
    const victim = await store.getTransaction("victim");
    expect(victim).toBeDefined();
    if (victim === undefined) throw new Error("Expected victim transaction");
    await mutate(indexedDB, foreignName, "transactions", (tx) => {
      tx.objectStore("transactions").put(
        { ...victim, pendingSegmentIds: ["foreign-segment"] },
        "victim",
      );
    });
    await expect(
      store.rollbackTransactionArtifacts({
        transactionId: "victim",
        expectedRevision: 0,
        pendingBlockIds: [],
        pendingSegmentIds: [],
        removeBlockIds: [],
        removeSegmentIds: ["foreign-segment"],
        updatedAt: NOW,
      }),
    ).rejects.toBeInstanceOf(StorageCorruptionError);
    expect(await store.getSegment("foreign-segment")).toBeDefined();
    expect(await store.getTransaction("victim")).toMatchObject({ revision: 0 });

    await injectBlocks(indexedDB, foreignName, [{ id: "retained-root", bytes: Uint8Array.of(4) }]);
    await injectSegments(indexedDB, foreignName, [
      segment("retained-segment", "retainer", "retained-root"),
    ]);
    await store.createTransaction(activeTransaction("block-victim", null));
    const blockVictim = await store.updateTransaction("block-victim", 0, {
      pendingBlockIds: ["retained-root"],
      updatedAt: NOW,
    });
    await expect(
      store.rollbackTransactionArtifacts({
        transactionId: "block-victim",
        expectedRevision: blockVictim.revision,
        pendingBlockIds: [],
        pendingSegmentIds: [],
        removeBlockIds: ["retained-root"],
        removeSegmentIds: [],
        updatedAt: NOW,
      }),
    ).rejects.toBeInstanceOf(StorageCorruptionError);
    expect(await store.getBlock("retained-root")).toEqual(Uint8Array.of(4));
    store.close();
  });

  it("binds every manifest retirement to the exact ready compaction and refuses aliases atomically", async () => {
    const prepare = async (prefix: string) => {
      const store = await openStore(new IDBFactory());
      await store.addTable({
        managed: false,
        id: "events",
        name: "events",
        columns: [{ id: "value", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: NOW,
      });
      const sourceTransactionId = `${prefix}-source-owner`;
      const sourceBlockId = `${prefix}-source-block`;
      const sourceSegmentId = `${prefix}-source-segment`;
      await store.createTransaction(activeTransaction(sourceTransactionId, null));
      const stagedSource = await store.stageTransactionArtifacts({
        transactionId: sourceTransactionId,
        expectedRevision: 0,
        blocks: [{ id: sourceBlockId, bytes: Uint8Array.of(1) }],
        segments: [segment(sourceSegmentId, sourceTransactionId, sourceBlockId)],
        updatedAt: NOW,
      });
      await store.commitTransaction({
        transactionId: sourceTransactionId,
        expectedTransactionRevision: stagedSource.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [{ tableId: "events", limit: 4096 }],
        committedAt: NOW,
      });
      const transactionId = `${prefix}-compactor`;
      const jobId = `${prefix}-job`;
      const outputBlockId = `${jobId}/output/segment/000000/column/000000/part/000000`;
      const outputSegmentId = `${prefix}-output-segment`;
      await store.createTransaction(activeTransaction(transactionId, 0));
      const stagedOutput = await store.stageTransactionArtifacts({
        transactionId,
        expectedRevision: 0,
        blocks: [{ id: outputBlockId, bytes: Uint8Array.of(2) }],
        segments: [{ ...segment(outputSegmentId, transactionId, outputBlockId), level: 1 }],
        updatedAt: NOW,
      });
      const job: CompactionJobRecord = {
        ...COPY_COMPACTION_FIELDS,
        id: jobId,
        tableId: "events",
        sourceManifestVersion: 0,
        sourceSegmentIds: [sourceSegmentId],
        sourceBlockIds: [sourceBlockId],
        outputBlockIds: [outputBlockId],
        cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
        processedRows: 1,
        sourceStoredBytes: 1,
        level0SourceStoredBytes: 1,
        outputStoredBytes: 1,
        logicalBytes: 1,
        outputLogicalBytes: 1,
        targetLevel: 1,
        state: "ready",
        transactionId,
        outputSegmentId,
        publishedVersion: null,
        revision: 0,
        createdAt: NOW,
        updatedAt: NOW,
      };
      await store.createCompactionJob(job);
      return { store, job, sourceBlockId, stagedOutput, transactionId, outputBlockId };
    };

    const valid = await prepare("exact");
    await expect(
      valid.store.commitTransaction({
        transactionId: valid.transactionId,
        expectedTransactionRevision: valid.stagedOutput.revision,
        expectedManifestVersion: 0,
        removedBlockIds: [valid.sourceBlockId],
        committedAt: NOW,
      }),
    ).rejects.toThrow(/requires a compaction job/);
    expect(await valid.store.getCurrentManifest()).toMatchObject({ version: 0 });
    expect(await readManifestBlockIds(valid.store, 0)).toEqual([valid.sourceBlockId]);
    expect(await valid.store.getTransaction(valid.transactionId)).toMatchObject({
      status: "active",
      revision: valid.stagedOutput.revision,
    });
    await expect(
      valid.store.commitTransaction({
        transactionId: valid.transactionId,
        expectedTransactionRevision: valid.stagedOutput.revision,
        expectedManifestVersion: 0,
        compactionJobId: "missing-job",
        removedBlockIds: [valid.sourceBlockId],
        committedAt: NOW,
      }),
    ).rejects.toThrow(/does not exist/);
    expect(
      await valid.store.commitTransaction({
        transactionId: valid.transactionId,
        expectedTransactionRevision: valid.stagedOutput.revision,
        expectedManifestVersion: 0,
        compactionJobId: valid.job.id,
        removedBlockIds: [valid.sourceBlockId],
        changedTableIds: ["events"],
        committedAt: NOW,
      }),
    ).toMatchObject({ version: 1, changedTableIds: ["events"] });
    expect(await readManifestBlockIds(valid.store, 1)).toEqual([valid.outputBlockId]);
    valid.store.close();

    const aliased = await prepare("alias");
    await aliased.store.createTransaction(activeTransaction("alias-adopter", 0));
    await aliased.store.updateTransaction("alias-adopter", 0, {
      pendingBlockIds: [aliased.sourceBlockId],
      updatedAt: NOW,
    });
    await aliased.store.stageTransactionArtifacts({
      transactionId: "alias-adopter",
      expectedRevision: 1,
      blocks: [],
      segments: [segment("alias-segment", "alias-adopter", aliased.sourceBlockId)],
      updatedAt: NOW,
    });
    await expect(
      aliased.store.commitTransaction({
        transactionId: aliased.transactionId,
        expectedTransactionRevision: aliased.stagedOutput.revision,
        expectedManifestVersion: 0,
        compactionJobId: aliased.job.id,
        removedBlockIds: [aliased.sourceBlockId],
        committedAt: NOW,
      }),
    ).rejects.toThrow(/also references it/);
    expect(await aliased.store.getCurrentManifest()).toMatchObject({ version: 0 });
    expect(await readManifestBlockIds(aliased.store, 0)).toEqual([aliased.sourceBlockId]);
    expect(await aliased.store.getTransaction(aliased.transactionId)).toMatchObject({
      status: "active",
      revision: aliased.stagedOutput.revision,
    });
    expect(await aliased.store.getBlock(aliased.sourceBlockId)).toEqual(Uint8Array.of(1));
    aliased.store.close();
  });

  it("removes only unpublished segments owned by an aborted transaction and unjournals them", async () => {
    const indexedDB = new IDBFactory();
    const store = await openStore(indexedDB);
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await store.createTransaction(activeTransaction("aborted-owner", null));
    const stagedAborted = await store.stageTransactionArtifacts({
      transactionId: "aborted-owner",
      expectedRevision: 0,
      blocks: [{ id: "aborted-block", bytes: Uint8Array.of(1) }],
      segments: [segment("aborted-segment", "aborted-owner", "aborted-block")],
      updatedAt: NOW,
    });
    const aborted = await store.updateTransaction("aborted-owner", stagedAborted.revision, {
      status: "aborted",
      updatedAt: NOW,
    });
    await expect(store.removeAbortedSegment("aborted-segment", "wrong-owner")).rejects.toThrow(
      /not owned/,
    );
    expect(await store.getSegment("aborted-segment")).toBeDefined();
    await store.createTransaction(activeTransaction("block-adopter", null));
    await store.updateTransaction("block-adopter", 0, {
      pendingBlockIds: ["aborted-block"],
      updatedAt: NOW,
    });
    // Another active transaction may safely adopt the immutable bytes. Removing this aborted
    // owner's segment metadata must neither reject that recovery path nor delete the payload.
    expect(await store.removeAbortedSegment("aborted-segment", "aborted-owner")).toBe(true);
    expect(await store.getSegment("aborted-segment")).toBeUndefined();
    expect(await store.getTransaction("aborted-owner")).toMatchObject({
      revision: aborted.revision + 1,
      pendingSegmentIds: [],
      pendingBlockIds: ["aborted-block"],
    });
    expect(await store.getBlock("aborted-block")).toEqual(Uint8Array.of(1));
    expect(await store.getTransaction("block-adopter")).toMatchObject({
      status: "active",
      pendingBlockIds: ["aborted-block"],
    });
    expect(await store.removeAbortedSegment("aborted-segment", "aborted-owner")).toBe(false);

    await store.createTransaction(activeTransaction("active-owner", null));
    await store.stageTransactionArtifacts({
      transactionId: "active-owner",
      expectedRevision: 0,
      blocks: [{ id: "active-block", bytes: Uint8Array.of(2) }],
      segments: [segment("active-segment", "active-owner", "active-block")],
      updatedAt: NOW,
    });
    await expect(store.removeAbortedSegment("active-segment", "active-owner")).rejects.toThrow(
      /must be aborted/,
    );
    expect(await store.getSegment("active-segment")).toBeDefined();

    await store.createTransaction(activeTransaction("published-owner", null));
    const stagedPublished = await store.stageTransactionArtifacts({
      transactionId: "published-owner",
      expectedRevision: 0,
      blocks: [{ id: "published-block", bytes: Uint8Array.of(3) }],
      segments: [segment("published-segment", "published-owner", "published-block")],
      updatedAt: NOW,
    });
    await store.updateTransaction("published-owner", stagedPublished.revision, {
      status: "aborted",
      updatedAt: NOW,
    });
    await publishManifest(store, {
      expectedVersion: null,
      blockIds: ["published-block"],
      createdAt: NOW,
    });
    await expect(
      store.removeAbortedSegment("published-segment", "published-owner"),
    ).rejects.toThrow(/published by manifest/);
    expect(await store.getSegment("published-segment")).toBeDefined();
    store.close();
  });

  it("atomically adopts an exact aborted compaction output into its authorized replacement", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await store.createTransaction(activeTransaction("aborted-output-owner", null));
    const staged = await store.stageTransactionArtifacts({
      transactionId: "aborted-output-owner",
      expectedRevision: 0,
      blocks: [{ id: "adopted-output-block", bytes: Uint8Array.of(1) }],
      segments: [
        {
          ...segment("adopted-output-segment", "aborted-output-owner", "adopted-output-block"),
          level: 1,
        },
      ],
      updatedAt: NOW,
    });
    const aborted = await store.updateTransaction("aborted-output-owner", staged.revision, {
      status: "aborted",
      updatedAt: NOW,
    });
    await store.createTransaction(activeTransaction("replacement-output-owner", null));
    const replacement = await store.updateTransaction("replacement-output-owner", 0, {
      pendingBlockIds: ["adopted-output-block"],
      updatedAt: NOW,
    });
    const job: CompactionJobRecord = {
      ...COPY_COMPACTION_FIELDS,
      id: "adoption-job",
      tableId: "events",
      sourceManifestVersion: 0,
      sourceSegmentIds: ["unrelated-source-segment"],
      sourceBlockIds: ["unrelated-source-block"],
      outputBlockIds: ["adopted-output-block"],
      cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
      processedRows: 0,
      sourceStoredBytes: 0,
      outputStoredBytes: 0,
      logicalBytes: 0,
      targetLevel: 1,
      state: "running",
      transactionId: "replacement-output-owner",
      outputSegmentId: "adopted-output-segment",
      publishedVersion: null,
      revision: 0,
      createdAt: NOW,
      updatedAt: NOW,
    };
    await mutate(indexedDB, name, "gc", (transaction) => {
      transaction
        .objectStore("gc")
        .add({ kind: "compaction-job", record: job }, "compaction-job/adoption-job");
    });
    const desired = {
      ...segment("adopted-output-segment", "replacement-output-owner", "adopted-output-block"),
      level: 1,
    };

    await expect(
      store.adoptAbortedSegment({
        segment: { ...desired, createdAt: "2026-08-24T12:00:01.000Z" },
        expectedAbortedTransactionId: aborted.id,
        expectedAbortedTransactionRevision: aborted.revision,
        replacementTransactionId: replacement.id,
        expectedReplacementTransactionRevision: replacement.revision,
        compactionJobId: job.id,
        updatedAt: NOW,
      }),
    ).rejects.toThrow(/does not match the immutable adoption record/);
    expect(await store.getSegment(desired.id)).toMatchObject({
      transactionId: aborted.id,
      rowCount: 1,
    });
    expect(await store.getTransaction(aborted.id)).toMatchObject({ revision: aborted.revision });
    expect(await store.getTransaction(replacement.id)).toMatchObject({
      revision: replacement.revision,
      pendingSegmentIds: [],
    });

    await mutate(indexedDB, name, "gc", (transaction) => {
      transaction.objectStore("gc").add(
        {
          kind: "compaction-job",
          record: { ...job, id: "foreign-adoption-job" },
        },
        "compaction-job/foreign-adoption-job",
      );
    });
    await expect(
      store.adoptAbortedSegment({
        segment: desired,
        expectedAbortedTransactionId: aborted.id,
        expectedAbortedTransactionRevision: aborted.revision,
        replacementTransactionId: replacement.id,
        expectedReplacementTransactionRevision: replacement.revision,
        compactionJobId: job.id,
        updatedAt: NOW,
      }),
    ).rejects.toThrow(/foreign compaction job/);
    expect(await store.getSegment(desired.id)).toMatchObject({ transactionId: aborted.id });
    expect(await store.getTransaction(aborted.id)).toMatchObject({ revision: aborted.revision });
    expect(await store.getTransaction(replacement.id)).toMatchObject({
      revision: replacement.revision,
      pendingSegmentIds: [],
    });
    await mutate(indexedDB, name, "gc", (transaction) => {
      transaction.objectStore("gc").delete("compaction-job/foreign-adoption-job");
    });

    const updated = await store.adoptAbortedSegment({
      segment: desired,
      expectedAbortedTransactionId: aborted.id,
      expectedAbortedTransactionRevision: aborted.revision,
      replacementTransactionId: replacement.id,
      expectedReplacementTransactionRevision: replacement.revision,
      compactionJobId: job.id,
      updatedAt: NOW,
    });
    expect(updated).toMatchObject({
      id: replacement.id,
      revision: replacement.revision + 1,
      pendingSegmentIds: [desired.id],
    });
    expect(await store.getSegment(desired.id)).toMatchObject({ transactionId: replacement.id });
    expect(await store.getTransaction(aborted.id)).toMatchObject({
      revision: aborted.revision + 1,
      pendingSegmentIds: [],
    });
    store.close();
  });

  it("round-trips the canonical framed snapshot with one verified payload read", async () => {
    const indexedDB = new IDBFactory();
    const source = await openStore(indexedDB);
    await source.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    const block = await encodeBlock({ type: "number", values: [42] });
    await source.createTransaction(activeTransaction("snapshot-writer", null));
    const staged = await source.stageTransactionArtifacts({
      transactionId: "snapshot-writer",
      expectedRevision: 0,
      blocks: [{ id: "snapshot-block", bytes: block }],
      segments: [segment("snapshot-segment", "snapshot-writer", "snapshot-block")],
      updatedAt: NOW,
    });
    await source.commitTransaction({
      transactionId: "snapshot-writer",
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      removedBlockIds: [],
      levelZeroSegmentLimits: [{ tableId: "events", limit: 1 }],
      committedAt: NOW,
    });

    const exported = await source.beginSnapshotFrameExport({
      ownerId: "snapshot-exporter",
      createdAt: NOW,
      expiresAt: "2026-08-24T12:30:00.000Z",
    });
    const frameCount = Object.values(exported.header.kinds).reduce(
      (total, summary) => total + summary.frameCount,
      0,
    );
    const frames: SnapshotFrame[] = [];
    let checksum = 0;
    let itemCount = 0;
    let storedBytes = 0;
    for (let sequence = 0; sequence < frameCount; sequence += 1) {
      const frame = await source.readSnapshotExportFrame({
        sessionId: exported.sessionId,
        ownerId: "snapshot-exporter",
        sequence,
        expiresAtCutoff: NOW,
        expiresAt: "2026-08-24T12:30:00.000Z",
      });
      expect(frame?.sequence).toBe(sequence);
      if (frame === undefined) throw new Error("Snapshot frame is missing");
      frames.push(frame);
      checksum = extendSnapshotFrameStreamChecksum(checksum, snapshotFrameEnvelopeParts(frame));
      itemCount += frame.itemCount;
      storedBytes += frame.payload.byteLength;
    }
    expect(frames.filter((frame) => frame.kind === "block")).toHaveLength(1);
    await expect(
      source.readSnapshotExportFrame({
        sessionId: exported.sessionId,
        ownerId: "snapshot-exporter",
        sequence: frameCount,
        expiresAtCutoff: NOW,
        expiresAt: "2026-08-24T12:30:00.000Z",
      }),
    ).resolves.toBeUndefined();
    expect(
      await source.closeSnapshotFrameExport({
        sessionId: exported.sessionId,
        ownerId: "snapshot-exporter",
      }),
    ).toBe(true);

    const target = await openStore(indexedDB);
    const identity = snapshotFrameStreamHeaderIdentity(exported.header);
    await target.beginSnapshotFrameImport({
      identity,
      ownerId: "snapshot-importer",
      createdAt: NOW,
      expiresAt: "2026-08-24T12:30:00.000Z",
      header: exported.header,
    });
    for (let start = 0; start < frames.length; start += 4) {
      await target.appendSnapshotImportFrames({
        identity,
        ownerId: "snapshot-importer",
        expiresAtCutoff: NOW,
        expiresAt: "2026-08-24T12:30:00.000Z",
        frames: frames.slice(start, start + 4),
      });
    }
    await target.finishSnapshotFrameImport({
      identity,
      ownerId: "snapshot-importer",
      expiresAtCutoff: NOW,
      footer: { frameCount, itemCount, storedBytes, checksum },
    });
    await target.beginSnapshotFrameImport({
      identity,
      ownerId: "snapshot-replay",
      createdAt: NOW,
      expiresAt: "2026-08-24T12:30:00.000Z",
      header: exported.header,
    });
    await target.appendSnapshotImportFrames({
      identity,
      ownerId: "snapshot-replay",
      expiresAtCutoff: NOW,
      expiresAt: "2026-08-24T12:30:00.000Z",
      frames,
    });
    await target.finishSnapshotFrameImport({
      identity,
      ownerId: "snapshot-replay",
      expiresAtCutoff: NOW,
      footer: { frameCount, itemCount, storedBytes, checksum },
    });
    expect(await target.getCurrentManifestVersion()).toBe(0);
    expect(await target.getTable("events")).toMatchObject({ name: "events" });
    expect(await target.getSegment("snapshot-segment")).toMatchObject({ tableId: "events" });
    expect(await target.getBlock("snapshot-block")).toEqual(block);
    expect(await target.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    source.close();
    target.close();
  });

  it("enforces the catalog record ceiling during framed import without advancing the prefix", async () => {
    const source = await openStore(new IDBFactory());
    await source.addTable({
      managed: false,
      id: "catalog-cap",
      name: "catalog_cap",
      columns: [{ id: "value", name: "value", type: "number", nullable: true }],
      revision: 0,
      createdAt: NOW,
    });
    await publishManifest(source, { expectedVersion: null, blockIds: [], createdAt: NOW });
    const snapshot = await exportSnapshotFrames(source);
    const catalogFrame = snapshot.frames.find((frame) => frame.kind === "catalog-page");
    if (catalogFrame === undefined) throw new Error("Catalog frame is missing");

    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const target = await openStore(indexedDB, name);
    const identity = snapshotFrameStreamHeaderIdentity(snapshot.header);
    await target.beginSnapshotFrameImport({
      identity,
      ownerId: "catalog-cap-owner",
      createdAt: NOW,
      expiresAt: "2026-08-24T12:30:00.000Z",
      header: snapshot.header,
    });
    await mutate(indexedDB, name, "statistics", (transaction) => {
      transaction
        .objectStore("statistics")
        .put(catalogLedger(MAX_CATALOG_RECORDS, 0), "resource/catalog");
    });
    await expect(
      target.appendSnapshotImportFrames({
        identity,
        ownerId: "catalog-cap-owner",
        expiresAtCutoff: NOW,
        expiresAt: "2026-08-24T12:30:00.000Z",
        frames: [catalogFrame],
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "catalog record",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(
      readRawValue(indexedDB, name, "catalog", "table/id/catalog-cap"),
    ).resolves.toBeUndefined();
    await expect(
      readRawValue(indexedDB, name, "catalog", "snapshot/frame-import"),
    ).resolves.toMatchObject({ nextSequence: 0, frameCount: 0, itemCount: 0 });

    await mutate(indexedDB, name, "statistics", (transaction) => {
      transaction.objectStore("statistics").put(catalogLedger(0, 0), "resource/catalog");
    });
    await expect(
      target.appendSnapshotImportFrames({
        identity,
        ownerId: "catalog-cap-owner",
        expiresAtCutoff: NOW,
        expiresAt: "2026-08-24T12:30:00.000Z",
        frames: [catalogFrame],
      }),
    ).resolves.toMatchObject({ nextSequence: 1 });
    target.close();
    source.close();
  });

  it("atomically discards every staged record before a foreign expired import takeover", async () => {
    const sourceA = await openStore(new IDBFactory());
    await sourceA.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    const block = await encodeBlock({ type: "number", values: [7] });
    await sourceA.createTransaction(activeTransaction("takeover-writer", null));
    const staged = await sourceA.stageTransactionArtifacts({
      transactionId: "takeover-writer",
      expectedRevision: 0,
      blocks: [{ id: "takeover-block", bytes: block }],
      segments: [segment("takeover-segment", "takeover-writer", "takeover-block")],
      updatedAt: NOW,
    });
    await sourceA.commitTransaction({
      transactionId: "takeover-writer",
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      removedBlockIds: [],
      levelZeroSegmentLimits: [{ tableId: "events", limit: 1 }],
      committedAt: NOW,
    });
    const snapshotA = await exportSnapshotFrames(sourceA);

    const sourceB = await openStore(new IDBFactory());
    await sourceB.addTable({
      managed: false,
      id: "replacement",
      name: "replacement",
      columns: [{ id: "value", name: "value", type: "number", nullable: true }],
      revision: 0,
      createdAt: NOW,
    });
    await publishManifest(sourceB, { expectedVersion: null, blockIds: [], createdAt: NOW });
    const snapshotB = await exportSnapshotFrames(sourceB);

    const indexedDB = new IDBFactory();
    const armAbort = abortNextSnapshotTakeoverAfterClear(indexedDB);
    const name = crypto.randomUUID();
    const target = await openStore(indexedDB, name);
    const identityA = snapshotFrameStreamHeaderIdentity(snapshotA.header);
    const identityB = snapshotFrameStreamHeaderIdentity(snapshotB.header);
    await target.beginSnapshotFrameImport({
      identity: identityA,
      ownerId: "owner-a",
      createdAt: NOW,
      expiresAt: "2026-08-24T12:01:00.000Z",
      header: snapshotA.header,
    });
    await target.appendSnapshotImportFrames({
      identity: identityA,
      ownerId: "owner-a",
      expiresAtCutoff: NOW,
      expiresAt: "2026-08-24T12:01:00.000Z",
      frames: snapshotA.frames,
    });

    armAbort();
    await expect(
      target.beginSnapshotFrameImport({
        identity: identityB,
        ownerId: "owner-b",
        createdAt: "2026-08-24T12:02:00.000Z",
        expiresAt: "2026-08-24T12:30:00.000Z",
        header: snapshotB.header,
      }),
    ).rejects.toBeDefined();
    await expect(
      readRawValue(indexedDB, name, "catalog", "snapshot/frame-import"),
    ).resolves.toMatchObject({ identity: identityA, nextSequence: snapshotA.frames.length });
    await expect(readRawValue(indexedDB, name, "blocks", "takeover-block")).resolves.toEqual(block);
    await expect(
      readRawValue(indexedDB, name, "segments", "takeover-segment"),
    ).resolves.toBeDefined();

    await target.beginSnapshotFrameImport({
      identity: identityB,
      ownerId: "owner-b",
      createdAt: "2026-08-24T12:02:00.000Z",
      expiresAt: "2026-08-24T12:30:00.000Z",
      header: snapshotB.header,
    });
    await expect(
      readRawValue(indexedDB, name, "blocks", "takeover-block"),
    ).resolves.toBeUndefined();
    await expect(
      readRawValue(indexedDB, name, "segments", "takeover-segment"),
    ).resolves.toBeUndefined();
    await expect(
      readRawValue(indexedDB, name, "transactions", "takeover-writer"),
    ).resolves.toBeUndefined();
    await expect(
      readRawValue(indexedDB, name, "catalog", "table/id/events"),
    ).resolves.toBeUndefined();
    await expect(
      readRawValue(indexedDB, name, "catalog", "block-metadata/takeover-block"),
    ).resolves.toBeUndefined();
    await expect(
      readRawValue(indexedDB, name, "catalog", ["manifest-block", "takeover-block"]),
    ).resolves.toBeUndefined();
    await expect(
      readRawValue(indexedDB, name, "snapshotHeaders", ["import", identityA, 0]),
    ).resolves.toBeUndefined();
    await expect(readRawValue(indexedDB, name, "statistics", "resource/global")).resolves.toEqual({
      stagedBlockCount: 0,
      stagedSegmentCount: 0,
      stagedBytes: 0,
      retiredHistoryBytes: 0,
    });
    await expect(readRawValue(indexedDB, name, "statistics", "resource/catalog")).resolves.toEqual(
      catalogLedger(0, 0),
    );
    await expect(
      readRawValue(indexedDB, name, "statistics", "resource/records"),
    ).resolves.toMatchObject({
      manifestCount: 0,
      manifestBytes: 0,
      segmentCount: 0,
      segmentBytes: 0,
    });
    await expect(
      readRawValue(indexedDB, name, "catalog", "snapshot/frame-import"),
    ).resolves.toMatchObject({ identity: identityB, nextSequence: 0 });
    target.close();
    sourceA.close();
    sourceB.close();
  });
  it("keeps framed snapshot planning to one retained semantic item at high cardinality", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: true }],
      revision: 0,
      createdAt: NOW,
    });
    await publishManifest(store, { expectedVersion: null, blockIds: [], createdAt: NOW });
    const recordCount = 2_000;
    await mutate(indexedDB, name, ["segments", "transactions"], (transaction) => {
      const segments = transaction.objectStore("segments");
      const transactions = transaction.objectStore("transactions");
      for (let index = 0; index < recordCount; index += 1) {
        const suffix = String(index).padStart(4, "0");
        const transactionId = `snapshot-owner-${suffix}`;
        transactions.add(
          {
            ...activeTransaction(transactionId, 0),
            status: "committed",
            revision: 1,
            committedVersion: 0,
          },
          transactionId,
        );
        segments.add(
          {
            ...segment(`snapshot-segment-${suffix}`, transactionId, "unused"),
            columnBlockIds: {},
          },
          `snapshot-segment-${suffix}`,
        );
      }
    });
    const session = await store.beginSnapshotFrameExport({
      ownerId: "high-cardinality-export",
      createdAt: NOW,
      expiresAt: "2026-08-24T12:30:00.000Z",
    });
    expect(session.header.kinds["segment-page"]).toMatchObject({
      frameCount: recordCount,
      itemCount: recordCount,
    });
    expect(session.header.kinds["transaction-page"]).toMatchObject({
      frameCount: recordCount + 1,
      itemCount: recordCount + 1,
    });
    expect(store._residentStateForTests().snapshotPeakRetainedItems).toBe(1);
    await store.closeSnapshotFrameExport({
      sessionId: session.sessionId,
      ownerId: "high-cardinality-export",
    });
    store.close();
  });
  it("streams canonical UNIQUE membership in fixed parts and restores enforcement", async () => {
    const indexedDB = new IDBFactory();
    const source = await openStore(indexedDB);
    await source.addTable({
      managed: false,
      id: "users",
      name: "users",
      columns: [{ id: "email", name: "email", type: "string", nullable: false }],
      primaryKeyColumnIds: ["email"],
      uniqueKeyColumnId: "email",
      uniqueKeyLookupReady: true,
      revision: 0,
      createdAt: NOW,
    });
    const tokens = Array.from(
      { length: 5_000 },
      (_, index) => `string:${String(index).padStart(5, "0")}:${"x".repeat(900)}`,
    );
    await source.createTransaction(activeTransaction("unique-snapshot-writer", null));
    await source.commitTransaction({
      transactionId: "unique-snapshot-writer",
      expectedTransactionRevision: 0,
      expectedManifestVersion: null,
      uniqueKeyChanges: [{ tableId: "users", keyTokens: tokens, requireAbsent: true }],
      committedAt: NOW,
    });

    const first = await exportSnapshotFrames(source);
    const second = await exportSnapshotFrames(source);
    expect(second.header).toEqual(first.header);
    expect(second.frames).toEqual(first.frames);
    expect(second.footer).toEqual(first.footer);
    const uniqueItems = first.frames
      .filter((frame) => frame.kind === "unique-page")
      .flatMap((frame) => decodeSnapshotMetadataItems("unique-page", frame.payload));
    const descriptor = uniqueItems.find((item) => item.kind === "unique-generation");
    const chunks = uniqueItems.filter((item) => item.kind === "unique-chunk");
    expect(descriptor).toMatchObject({ tokenCount: tokens.length, chunkCount: chunks.length });
    expect(chunks.length).toBeGreaterThan(2);
    expect(chunks.every((chunk) => chunk.keyTokens.length <= 2_048)).toBe(true);
    expect(source._residentStateForTests().snapshotPeakRetainedBytes).toBeLessThanOrEqual(
      4 * 1024 * 1024,
    );

    const restored = await openStore(new IDBFactory());
    await importSnapshotFrames(restored, first);
    await expect(
      restored.getExistingUniqueKeys("users", [tokens[0] ?? "", tokens.at(-1) ?? "", "missing"]),
    ).resolves.toEqual([tokens[0], tokens.at(-1)]);
    await restored.createTransaction(activeTransaction("duplicate-after-restore", 0));
    await expect(
      restored.commitTransaction({
        transactionId: "duplicate-after-restore",
        expectedTransactionRevision: 0,
        expectedManifestVersion: 0,
        uniqueKeyChanges: [
          { tableId: "users", keyTokens: [tokens[2_500] ?? ""], requireAbsent: true },
        ],
        committedAt: NOW,
      }),
    ).rejects.toBeInstanceOf(UniqueKeyConflictError);
    expect(await restored.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    restored.close();
    source.close();
  });
  it("accounts catalog bytes atomically at the hard ceiling and fails closed on drift", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    const table: TableRecord = {
      managed: false,
      id: "catalog-bytes",
      name: "catalog_bytes",
      columns: [{ id: "value", name: "value", type: "number", nullable: true }],
      revision: 0,
      createdAt: NOW,
    };
    const retainedBytes = catalogRecordRetainedBytes(table);
    await mutate(indexedDB, name, "statistics", (transaction) => {
      transaction
        .objectStore("statistics")
        .put(catalogLedger(0, MAX_CATALOG_RETAINED_BYTES - retainedBytes), "resource/catalog");
    });

    await store.addTable(table);
    await expect(readRawValue(indexedDB, name, "statistics", "resource/catalog")).resolves.toEqual(
      catalogLedger(1, MAX_CATALOG_RETAINED_BYTES),
    );
    await expect(
      store.addTable({ ...table, id: "catalog-overflow", name: "catalog_overflow" }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "catalog byte",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getTable("catalog-overflow")).resolves.toBeUndefined();
    await expect(
      store.updateTable(table.id, 0, { view: { sql: "SELECT 1", managed: false } }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "catalog byte",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getTable(table.id)).resolves.toEqual(table);
    await expect(readRawValue(indexedDB, name, "statistics", "resource/catalog")).resolves.toEqual(
      catalogLedger(1, MAX_CATALOG_RETAINED_BYTES),
    );

    // Restore a fully consistent baseline to prove ordinary reopen, integrity, and decrement.
    await mutate(indexedDB, name, "statistics", (transaction) => {
      transaction
        .objectStore("statistics")
        .put(catalogLedger(1, retainedBytes), "resource/catalog");
    });
    store.close();
    store = await openStore(indexedDB, name);
    await expect(store.checkIntegrity({ mode: "full" })).resolves.toMatchObject({ ok: true });
    await store.removeTable(table.id, 0);
    await expect(readRawValue(indexedDB, name, "statistics", "resource/catalog")).resolves.toEqual(
      catalogLedger(0, 0),
    );

    await mutate(indexedDB, name, "statistics", (transaction) => {
      transaction.objectStore("statistics").put(catalogLedger(0, 1), "resource/catalog");
    });
    const drift = await store.checkIntegrity({ mode: "full" });
    expect(drift.ok).toBe(false);
    expect(drift.issues.some((issue) => issue.code === "catalog-resource-ledger-mismatch")).toBe(
      true,
    );
    store.close();
    await expect(openStore(indexedDB, name)).rejects.toMatchObject({
      name: "StorageCorruptionError",
      location: "statistics/resource/catalog",
    });
    await mutate(indexedDB, name, "statistics", (transaction) => {
      transaction
        .objectStore("statistics")
        .put(catalogLedger(MAX_CATALOG_RECORDS + 1, 0), "resource/catalog");
    });
    await expect(openStore(indexedDB, name)).rejects.toMatchObject({
      name: "StorageCorruptionError",
      location: "statistics/resource/catalog",
    });
  });

  it("enforces manifest and segment record ledgers at exact count and byte ceilings", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    const manifest0 = await publishManifest(store, {
      expectedVersion: null,
      blockIds: [],
      createdAt: NOW,
    });
    const manifest1 = {
      version: 1,
      previousVersion: 0,
      liveBlockCount: 0,
      liveBlockBytes: 0,
      changedTableIds: [],
      createdAt: NOW,
    };
    await store.createTransaction(activeTransaction("manifest-at-limit", 0));
    await mutate(indexedDB, name, "statistics", (transaction) => {
      putRawRecordLedger(transaction, {
        manifestCount: MAX_MANIFEST_RECORDS - 1,
        manifestBytes:
          MAX_MANIFEST_RETAINED_BYTES - manifestRecordRetainedReservationBytes(manifest1),
        segmentCount: 0,
        segmentBytes: 0,
      });
    });
    await store.commitTransaction({
      transactionId: "manifest-at-limit",
      expectedTransactionRevision: 0,
      expectedManifestVersion: 0,
      removedBlockIds: [],
      committedAt: NOW,
    });
    await expect(
      readRawValue(indexedDB, name, "statistics", "resource/records"),
    ).resolves.toMatchObject({
      manifestCount: MAX_MANIFEST_RECORDS,
      manifestBytes: MAX_MANIFEST_RETAINED_BYTES,
    });

    await store.createTransaction(activeTransaction("manifest-count-refusal", 1));
    await expect(
      store.commitTransaction({
        transactionId: "manifest-count-refusal",
        expectedTransactionRevision: 0,
        expectedManifestVersion: 1,
        removedBlockIds: [],
        committedAt: NOW,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "manifest record",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getCurrentManifestVersion()).resolves.toBe(1);
    await expect(store.getTransaction("manifest-count-refusal")).resolves.toMatchObject({
      revision: 0,
      status: "active",
    });

    const manifestBytes =
      manifestRecordRetainedReservationBytes(manifest0) +
      manifestRecordRetainedReservationBytes(manifest1);
    await mutate(indexedDB, name, "statistics", (transaction) => {
      putRawRecordLedger(transaction, {
        manifestCount: 2,
        manifestBytes: MAX_MANIFEST_RETAINED_BYTES,
        segmentCount: 0,
        segmentBytes: 0,
      });
    });
    await expect(
      store.commitTransaction({
        transactionId: "manifest-count-refusal",
        expectedTransactionRevision: 0,
        expectedManifestVersion: 1,
        removedBlockIds: [],
        committedAt: NOW,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "manifest byte",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getCurrentManifestVersion()).resolves.toBe(1);

    const block = await encodeBlock({ type: "number", values: [1] });
    const stagedSegment = segment("segment-at-limit", "segment-at-limit-owner", "segment/block");
    await store.createTransaction(activeTransaction("segment-at-limit-owner", 1));
    await mutate(indexedDB, name, "statistics", (transaction) => {
      putRawRecordLedger(transaction, {
        manifestCount: 2,
        manifestBytes,
        segmentCount: MAX_SEGMENT_RECORDS - 1,
        segmentBytes: MAX_SEGMENT_RETAINED_BYTES - segmentRecordRetainedBytes(stagedSegment),
      });
    });
    await store.stageTransactionArtifacts({
      transactionId: "segment-at-limit-owner",
      expectedRevision: 0,
      blocks: [{ id: "segment/block", bytes: block }],
      segments: [stagedSegment],
      updatedAt: NOW,
    });
    await expect(
      readRawValue(indexedDB, name, "statistics", "resource/records"),
    ).resolves.toMatchObject({
      segmentCount: MAX_SEGMENT_RECORDS,
      segmentBytes: MAX_SEGMENT_RETAINED_BYTES,
    });

    await store.createTransaction(activeTransaction("segment-refusal-owner", 1));
    const refusedSegment = segment(
      "segment-refused",
      "segment-refusal-owner",
      "segment/refused-block",
    );
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "segment-refusal-owner",
        expectedRevision: 0,
        blocks: [{ id: "segment/refused-block", bytes: block }],
        segments: [refusedSegment],
        updatedAt: NOW,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "segment record",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getBlock("segment/refused-block")).resolves.toBeUndefined();
    await expect(store.getTransaction("segment-refusal-owner")).resolves.toMatchObject({
      revision: 0,
    });

    await mutate(indexedDB, name, "statistics", (transaction) => {
      putRawRecordLedger(transaction, {
        manifestCount: 2,
        manifestBytes,
        segmentCount: 1,
        segmentBytes: MAX_SEGMENT_RETAINED_BYTES,
      });
    });
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "segment-refusal-owner",
        expectedRevision: 0,
        blocks: [{ id: "segment/refused-block", bytes: block }],
        segments: [refusedSegment],
        updatedAt: NOW,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "segment byte",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getSegment("segment-refused")).resolves.toBeUndefined();

    await mutate(indexedDB, name, "statistics", (transaction) => {
      putRawRecordLedger(transaction, {
        manifestCount: 2,
        manifestBytes,
        segmentCount: 1,
        segmentBytes: segmentRecordRetainedBytes(stagedSegment),
      });
    });
    await store.rollbackTransactionArtifacts({
      transactionId: "segment-at-limit-owner",
      expectedRevision: 1,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      removeBlockIds: ["segment/block"],
      removeSegmentIds: ["segment-at-limit"],
      updatedAt: NOW,
    });
    await expect(
      readRawValue(indexedDB, name, "statistics", "resource/records"),
    ).resolves.toMatchObject({
      manifestCount: 2,
      manifestBytes,
      segmentCount: 0,
      segmentBytes: 0,
    });
    await store.createTransaction(activeTransaction("gc-segment-owner", 1));
    await store.updateTransaction("gc-segment-owner", 0, { status: "aborted", updatedAt: NOW });
    const gcSegment = segment("gc-ledger-segment", "gc-segment-owner", "already-reclaimed");
    await injectSegments(indexedDB, name, [gcSegment]);
    const gc = await store.createGarbageCollectionJob({
      id: "record-ledger-gc",
      candidateManifestVersions: [],
      candidateSegmentIds: [gcSegment.id],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: NOW,
      createdAt: NOW,
    });
    const gcStep = await store.runGarbageCollectionStep({
      jobId: gc.id,
      expectedRevision: gc.revision,
      maxItems: 1,
      updatedAt: NOW,
    });
    expect(gcStep.reclaimedSegmentIds).toEqual([gcSegment.id]);
    await expect(
      readRawValue(indexedDB, name, "statistics", "resource/records"),
    ).resolves.toMatchObject({
      manifestCount: 2,
      manifestBytes,
      segmentCount: 0,
      segmentBytes: 0,
    });
    store.close();
    store = await openStore(indexedDB, name);
    await expect(store.checkIntegrity({ mode: "full" })).resolves.toMatchObject({ ok: true });

    await mutate(indexedDB, name, "statistics", (transaction) => {
      transaction.objectStore("statistics").put(
        {
          manifestCount: 2,
          manifestBytes,
          segmentCount: 0,
          segmentBytes: 0,
          checksum: 0,
        },
        "resource/records",
      );
    });
    await expect(store.checkIntegrity({ mode: "full" })).resolves.toMatchObject({ ok: false });
    store.close();
    await expect(openStore(indexedDB, name)).rejects.toMatchObject({
      name: "StorageCorruptionError",
      location: "statistics/resource/records",
    });
    await mutate(indexedDB, name, "statistics", (transaction) => {
      putRawRecordLedger(transaction, {
        manifestCount: MAX_MANIFEST_RECORDS + 1,
        manifestBytes: 0,
        segmentCount: 0,
        segmentBytes: 0,
      });
    });
    await expect(openStore(indexedDB, name)).rejects.toMatchObject({
      name: "StorageCorruptionError",
      location: "statistics/resource/records",
    });
  });

  it("accounts staged transaction resources atomically across rollback, reopen, and corruption", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    const bytes = await encodeBlock({ type: "number", values: [1, 2, 3] });
    await store.createTransaction(activeTransaction("ledger-owner", null));
    await store.stageTransactionArtifacts({
      transactionId: "ledger-owner",
      expectedRevision: 0,
      blocks: [{ id: "ledger/block", bytes }],
      segments: [],
      updatedAt: NOW,
    });
    await expect(readRawValue(indexedDB, name, "statistics", "resource/global")).resolves.toEqual({
      stagedBlockCount: 1,
      stagedSegmentCount: 0,
      stagedBytes: bytes.byteLength,
      retiredHistoryBytes: 0,
    });
    await expect(
      readRawValue(indexedDB, name, "statistics", "resource/transaction/ledger-owner"),
    ).resolves.toEqual({ blockCount: 1, segmentCount: 0, retainedBytes: bytes.byteLength });

    store.close();
    store = await openStore(indexedDB, name);
    await store.rollbackTransactionArtifacts({
      transactionId: "ledger-owner",
      expectedRevision: 1,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      removeBlockIds: ["ledger/block"],
      removeSegmentIds: [],
      updatedAt: NOW,
    });
    await expect(readRawValue(indexedDB, name, "statistics", "resource/global")).resolves.toEqual({
      stagedBlockCount: 0,
      stagedSegmentCount: 0,
      stagedBytes: 0,
      retiredHistoryBytes: 0,
    });
    await expect(
      readRawValue(indexedDB, name, "statistics", "resource/transaction/ledger-owner"),
    ).resolves.toBeUndefined();
    await expect(store.getBlock("ledger/block")).resolves.toBeUndefined();

    await store.createTransaction(activeTransaction("ledger-refusal", null));
    await mutate(indexedDB, name, "statistics", (transaction) => {
      transaction.objectStore("statistics").put(
        {
          stagedBlockCount: MAX_GLOBAL_STAGED_BLOCKS,
          stagedSegmentCount: 0,
          stagedBytes: 0,
          retiredHistoryBytes: 0,
        },
        "resource/global",
      );
    });
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "ledger-refusal",
        expectedRevision: 0,
        blocks: [{ id: "ledger/refused", bytes }],
        segments: [],
        updatedAt: NOW,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "staged block",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getTransaction("ledger-refusal")).resolves.toMatchObject({ revision: 0 });
    await expect(store.getBlock("ledger/refused")).resolves.toBeUndefined();

    store.close();
    store = await openStore(indexedDB, name);
    const report = await store.checkIntegrity({ mode: "full", maxIssues: 20 });
    expect(report.ok).toBe(false);
    expect(report.issues.map((issue) => issue.code)).toContain("resource-ledger-mismatch");
    store.close();
  });

  it("refuses pinned manifest lag and pinned retired bytes without creating a lease", async () => {
    const lagIndexedDB = new IDBFactory();
    const lagName = crypto.randomUUID();
    let lagStore = await openStore(lagIndexedDB, lagName);
    await publishManifest(lagStore, { expectedVersion: null, blockIds: [], createdAt: NOW });
    const distantVersion = MAX_PINNED_MANIFEST_VERSION_LAG + 1;
    const distantManifest = {
      version: distantVersion,
      previousVersion: 0,
      liveBlockCount: 0,
      liveBlockBytes: 0,
      changedTableIds: [],
      createdAt: NOW,
    };
    await mutate(lagIndexedDB, lagName, ["catalog", "manifests", "statistics"], (transaction) => {
      transaction.objectStore("manifests").add(distantManifest, distantVersion);
      transaction.objectStore("catalog").put(distantVersion, "manifest/current");
      adjustRawRecordLedger(transaction, {
        manifestCount: 1,
        manifestBytes: manifestRecordRetainedReservationBytes(distantManifest),
      });
    });
    await expect(
      lagStore.createLease({
        id: "too-old",
        kind: "reader",
        manifestVersion: 0,
        ownerId: "reader",
        createdAt: NOW,
        expiresAt: "2026-08-24T12:30:00.000Z",
        revision: 0,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "pinned manifest version lag",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(lagStore.getLease("too-old")).resolves.toBeUndefined();
    lagStore.close();
    lagStore = await openStore(lagIndexedDB, lagName);
    await expect(lagStore.getLease("too-old")).resolves.toBeUndefined();
    lagStore.close();

    const retiredIndexedDB = new IDBFactory();
    const retiredName = crypto.randomUUID();
    let retiredStore = await openStore(retiredIndexedDB, retiredName);
    await publishManifest(retiredStore, { expectedVersion: null, blockIds: [], createdAt: NOW });
    const provenanceBytes = MAX_BLOCK_READ_BATCH_BYTES;
    const provenanceCount = Math.floor(MAX_PINNED_RETIRED_BYTES / provenanceBytes) + 1;
    const retiredManifest = {
      version: 1,
      previousVersion: 0,
      liveBlockCount: 0,
      liveBlockBytes: 0,
      changedTableIds: [],
      createdAt: NOW,
    };
    await mutate(
      retiredIndexedDB,
      retiredName,
      ["catalog", "manifests", "statistics"],
      (transaction) => {
        transaction.objectStore("manifests").add(retiredManifest, 1);
        const catalog = transaction.objectStore("catalog");
        catalog.put(1, "manifest/current");
        for (let index = 0; index < provenanceCount; index += 1) {
          const blockId = `retired/${String(index).padStart(2, "0")}`;
          catalog.add(
            {
              blockId,
              byteLength: provenanceBytes,
              checksum: 0,
              addedVersion: 0,
              removedVersion: 1,
            },
            ["manifest-block", blockId],
          );
        }
        adjustRawRecordLedger(transaction, {
          manifestCount: 1,
          manifestBytes: manifestRecordRetainedReservationBytes(retiredManifest),
        });
      },
    );
    await expect(
      retiredStore.createLease({
        id: "pins-too-much",
        kind: "reader",
        manifestVersion: 0,
        ownerId: "reader",
        createdAt: NOW,
        expiresAt: "2026-08-24T12:30:00.000Z",
        revision: 0,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "pinned retired byte",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(retiredStore.getLease("pins-too-much")).resolves.toBeUndefined();
    retiredStore.close();
    retiredStore = await openStore(retiredIndexedDB, retiredName);
    await expect(retiredStore.getLease("pins-too-much")).resolves.toBeUndefined();
    retiredStore.close();
  });

  it("accepts the pinned-retired block ceiling and refuses the next record atomically", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    await publishManifest(store, { expectedVersion: null, blockIds: [], createdAt: NOW });
    const retiredManifest = {
      version: 1,
      previousVersion: 0,
      liveBlockCount: 0,
      liveBlockBytes: 0,
      changedTableIds: [],
      createdAt: NOW,
    };
    await mutate(indexedDB, name, ["catalog", "manifests", "statistics"], (transaction) => {
      transaction.objectStore("manifests").add(retiredManifest, 1);
      const catalog = transaction.objectStore("catalog");
      catalog.put(1, "manifest/current");
      for (let index = 0; index < MAX_PINNED_RETIRED_BLOCKS; index += 1) {
        const blockId = `pinned-count/${String(index).padStart(5, "0")}`;
        catalog.add({ blockId, byteLength: 1, checksum: 0, addedVersion: 0, removedVersion: 1 }, [
          "manifest-block",
          blockId,
        ]);
      }
      adjustRawRecordLedger(transaction, {
        manifestCount: 1,
        manifestBytes: manifestRecordRetainedReservationBytes(retiredManifest),
      });
    });
    await store.createLease({
      id: "at-block-limit",
      kind: "reader",
      manifestVersion: 0,
      ownerId: "reader",
      createdAt: NOW,
      expiresAt: "2026-08-24T12:30:00.000Z",
      revision: 0,
    });
    await mutate(indexedDB, name, "catalog", (transaction) => {
      const blockId = "pinned-count/overflow";
      transaction
        .objectStore("catalog")
        .add({ blockId, byteLength: 1, checksum: 0, addedVersion: 0, removedVersion: 1 }, [
          "manifest-block",
          blockId,
        ]);
    });
    await expect(
      store.renewLease({
        id: "at-block-limit",
        expectedRevision: 0,
        expiresAtCutoff: NOW,
        expiresAt: "2026-08-24T12:45:00.000Z",
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "pinned retired block",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getLease("at-block-limit")).resolves.toMatchObject({
      revision: 0,
      expiresAt: "2026-08-24T12:30:00.000Z",
    });
    store.close();
    store = await openStore(indexedDB, name);
    await expect(store.getLease("at-block-limit")).resolves.toMatchObject({
      revision: 0,
      expiresAt: "2026-08-24T12:30:00.000Z",
    });
    store.close();
  });
  it("refuses retired-history and terminal-maintenance growth atomically across reopen", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    const bytes = await encodeBlock({ type: "number", values: [7] });
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await store.createTransaction(activeTransaction("history-writer", null));
    await store.stageTransactionArtifacts({
      transactionId: "history-writer",
      expectedRevision: 0,
      blocks: [{ id: "history/block", bytes }],
      segments: [segment("history-segment", "history-writer", "history/block")],
      updatedAt: NOW,
    });
    await store.commitTransaction({
      transactionId: "history-writer",
      expectedTransactionRevision: 1,
      expectedManifestVersion: null,
      removedBlockIds: [],
      levelZeroSegmentLimits: [{ tableId: "events", limit: 4096 }],
      committedAt: NOW,
    });
    await mutate(indexedDB, name, "statistics", (transaction) => {
      transaction.objectStore("statistics").put(
        {
          stagedBlockCount: 0,
          stagedSegmentCount: 0,
          stagedBytes: 0,
          retiredHistoryBytes: MAX_RETIRED_HISTORY_BYTES,
        },
        "resource/global",
      );
    });
    await expect(
      store.dropTable({
        tableId: "events",
        expectedTableRevision: 0,
        expectedManifestVersion: 0,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: NOW,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "retired history byte",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getCurrentManifestVersion()).resolves.toBe(0);
    await expect(store.getTable("events")).resolves.toBeDefined();
    await expect(store.getSegment("history-segment")).resolves.toBeDefined();

    const job: CompactionJobRecord = {
      ...COPY_COMPACTION_FIELDS,
      id: "terminal-limit",
      tableId: "events",
      sourceManifestVersion: 0,
      sourceSegmentIds: ["history-segment"],
      sourceBlockIds: ["history/block"],
      outputBlockIds: [],
      cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
      processedRows: 0,
      sourceStoredBytes: 0,
      outputStoredBytes: 0,
      logicalBytes: 0,
      targetLevel: 1,
      state: "planned",
      transactionId: null,
      outputSegmentId: null,
      publishedVersion: null,
      revision: 0,
      createdAt: NOW,
      updatedAt: NOW,
    };
    await store.createCompactionJob(job);
    await mutate(indexedDB, name, "gc", (transaction) => {
      transaction.objectStore("gc").put(
        {
          activeCompactionJobs: 1,
          terminalCompactionJobs: MAX_TERMINAL_COMPACTION_JOB_RECORDS,
          activeGarbageCollectionJobs: 0,
          completedGarbageCollectionJobs: 0,
        },
        "maintenance/quota",
      );
    });
    await expect(store.cancelCompactionJob(job.id, 0, NOW)).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "terminal compaction job",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getCompactionJob(job.id)).resolves.toMatchObject({
      state: "planned",
      revision: 0,
    });
    const gc = await store.createGarbageCollectionJob({
      id: "completed-limit",
      candidateManifestVersions: [],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: ["history-writer"],
      leaseCutoff: NOW,
      createdAt: NOW,
    });
    await mutate(indexedDB, name, "gc", (transaction) => {
      transaction.objectStore("gc").put(
        {
          activeCompactionJobs: 1,
          terminalCompactionJobs: MAX_TERMINAL_COMPACTION_JOB_RECORDS,
          activeGarbageCollectionJobs: 1,
          completedGarbageCollectionJobs: MAX_COMPLETED_GARBAGE_COLLECTION_JOB_RECORDS,
        },
        "maintenance/quota",
      );
    });
    await expect(
      store.runGarbageCollectionStep({
        jobId: gc.id,
        expectedRevision: gc.revision,
        maxItems: 1,
        updatedAt: NOW,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "completed garbage collection job",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getGarbageCollectionJob(gc.id)).resolves.toMatchObject({
      state: "planned",
      revision: 0,
    });
    store.close();
    store = await openStore(indexedDB, name);
    await expect(store.getCurrentManifestVersion()).resolves.toBe(0);
    await expect(store.getTable("events")).resolves.toBeDefined();
    await expect(store.getCompactionJob(job.id)).resolves.toMatchObject({
      state: "planned",
      revision: 0,
    });
    await expect(store.getGarbageCollectionJob(gc.id)).resolves.toMatchObject({
      state: "planned",
      revision: 0,
    });
    store.close();
  });

  it("refuses terminal transaction history before publishing or changing its journal", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await openStore(indexedDB, name);
    await store.createTransaction(activeTransaction("terminal-limit", null));
    store.close();
    overrideCommittedTransactionCount(indexedDB, MAX_TERMINAL_TRANSACTION_RECORDS);
    store = await openStore(indexedDB, name);
    await expect(
      store.commitTransaction({
        transactionId: "terminal-limit",
        expectedTransactionRevision: 0,
        expectedManifestVersion: null,
        removedBlockIds: [],
        levelZeroSegmentLimits: [],
        committedAt: NOW,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "terminal transaction",
    } satisfies Partial<StorageResourceLimitError>);
    await expect(store.getCurrentManifestVersion()).resolves.toBeNull();
    await expect(store.getTransaction("terminal-limit")).resolves.toMatchObject({
      status: "active",
      revision: 0,
      committedVersion: null,
      pendingBlockIds: [],
      pendingSegmentIds: [],
    });
    store.close();
    store = await openStore(indexedDB, name);
    await expect(store.getCurrentManifestVersion()).resolves.toBeNull();
    await expect(store.getTransaction("terminal-limit")).resolves.toMatchObject({
      status: "active",
      revision: 0,
      committedVersion: null,
    });
    store.close();
  });

  it("streams storage statistics and full block verification with bounded issue output", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    const live = await encodeBlock({ type: "number", values: [1, 2, 3] });
    const obsolete = await encodeBlock({ type: "string", values: ["old"] });
    await injectBlocks(indexedDB, name, [
      { id: "live", bytes: live },
      { id: "obsolete", bytes: obsolete },
    ]);
    await publishManifest(store, { expectedVersion: null, blockIds: ["live"], createdAt: NOW });
    expect(await store.getStorageStats()).toMatchObject({
      backend: "indexeddb",
      liveBlockBytes: live.byteLength,
      obsoleteBlockBytes: obsolete.byteLength,
      manifestCount: 1,
    });
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({
      ok: true,
      checkedBlocks: 2,
      checkedBytes: live.byteLength + obsolete.byteLength,
      issueCount: 0,
    });
    const damaged = live.slice();
    damaged[damaged.length - 1] = (damaged[damaged.length - 1] ?? 0) ^ 0xff;
    await mutate(indexedDB, name, "blocks", (tx) => {
      tx.objectStore("blocks").put(damaged, "live");
    });
    const report = await store.checkIntegrity({ mode: "full", maxIssues: 1 });
    expect(report.ok).toBe(false);
    expect(report.issueCount).toBeGreaterThan(0);
    expect(report.issues).toHaveLength(1);
    expect(report.issues[0]?.code).toBe("invalid-block");
    await expect(exportSnapshotFrames(store)).rejects.toBeInstanceOf(StorageCorruptionError);
    store.close();
  });

  it("reports orphan counters and conflicting active-transaction artifact ownership", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: NOW,
    });
    await injectBlocks(indexedDB, name, [
      { id: "live", bytes: Uint8Array.of(1) },
      { id: "owned", bytes: Uint8Array.of(2) },
    ]);
    await publishManifest(store, { expectedVersion: null, blockIds: ["live"], createdAt: NOW });
    await store.createTransaction(activeTransaction("live-journal", 0));
    await store.updateTransaction("live-journal", 0, {
      pendingBlockIds: ["live"],
      updatedAt: NOW,
    });
    await injectSegments(indexedDB, name, [segment("owned-segment", "missing-journal", "owned")]);
    await store.createTransaction(activeTransaction("missing-journal", 0));
    await mutate(indexedDB, name, "catalog", (tx) => {
      tx.objectStore("catalog").put(2n, "row-id/missing-table");
      tx.objectStore("catalog").put(2n, "auto-increment/events/value");
    });

    const report = await store.checkIntegrity({ maxIssues: 20 });
    expect(report.ok).toBe(false);
    expect(report.issues.map((entry) => entry.code)).toEqual(
      expect.arrayContaining([
        "orphan-row-id-counter",
        "orphan-auto-increment-counter",
        "invalid-transaction",
      ]),
    );
    expect(report.issues.filter((entry) => entry.code === "invalid-transaction")).toHaveLength(2);
    store.close();
  });
});

describe("IndexedDB UNIQUE build recovery hardening", () => {
  function uniqueBuildTable(id: string, buildId: string): TableRecord {
    return {
      id,
      name: id.replaceAll("-", "_"),
      managed: false,
      revision: 0,
      columns: [{ id: "value", name: "value", type: "string", nullable: false }],
      secondaryIndexes: {
        value_unique: {
          name: `${id}_value_unique`,
          columnId: "value",
          columnIds: ["value"],
          directions: ["asc"],
          unique: true,
          termEncoding: "tuple-v1",
          storage: "postings-v1",
          storageColumnId: `${id}-unique-storage`,
          locator: "row-id",
          state: "building",
          buildId,
          buildFromVersion: -1,
        },
      },
      createdAt: NOW,
    };
  }

  it("publishes an ordered generation exactly once and rejects every stale or changed replay", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    const table = uniqueBuildTable("unique-users", "users-build");
    const namespaceId = secondaryUniqueKeyNamespace(table.id, "value_unique");
    await store.addTable(table);

    const begin = {
      buildId: "users-build",
      tableId: table.id,
      indexId: "value_unique",
      namespaceId,
      ownerId: "builder-a",
      createdAt: "2026-08-24T12:00:00.000Z",
      expiresAt: "2026-08-24T12:30:00.000Z",
    } as const;
    await expect(store.beginUniqueKeyBuild(begin)).resolves.toMatchObject({
      state: "active",
      nextOrdinal: 0,
      retainedBytes: 0,
    });
    await expect(store.beginUniqueKeyBuild(begin)).resolves.toMatchObject({
      buildId: begin.buildId,
      ownerId: begin.ownerId,
    });
    await expect(
      store.beginUniqueKeyBuild({ ...begin, ownerId: "builder-b" }),
    ).rejects.toBeInstanceOf(UniqueKeyBuildConflictError);
    await expect(
      store.beginUniqueKeyBuild({
        ...begin,
        buildId: "unowned-build",
        ownerId: "builder-b",
      }),
    ).rejects.toBeInstanceOf(UniqueKeyBuildConflictError);

    await expect(
      store.renewUniqueKeyBuild({
        buildId: begin.buildId,
        ownerId: begin.ownerId,
        expiresAtCutoff: "2026-08-24T12:05:00.000Z",
        expiresAt: "2026-08-24T12:45:00.000Z",
        updatedAt: "2026-08-24T12:05:00.000Z",
      }),
    ).resolves.toMatchObject({ expiresAt: "2026-08-24T12:45:00.000Z" });
    await expect(
      store.renewUniqueKeyBuild({
        buildId: begin.buildId,
        ownerId: "builder-b",
        expiresAtCutoff: "2026-08-24T12:05:00.000Z",
        expiresAt: "2026-08-24T12:45:00.000Z",
        updatedAt: "2026-08-24T12:05:00.000Z",
      }),
    ).rejects.toBeInstanceOf(UniqueKeyBuildConflictError);

    const firstChunk = {
      buildId: begin.buildId,
      ownerId: begin.ownerId,
      expiresAtCutoff: "2026-08-24T12:06:00.000Z",
      ordinal: 0,
      keyTokens: ["alpha", "beta"],
      updatedAt: "2026-08-24T12:06:00.000Z",
    } as const;
    await expect(store.appendUniqueKeyBuildChunk(firstChunk)).resolves.toMatchObject({
      nextOrdinal: 1,
      tokenCount: 2,
    });
    await expect(store.appendUniqueKeyBuildChunk(firstChunk)).resolves.toMatchObject({
      nextOrdinal: 1,
      tokenCount: 2,
    });
    await expect(
      store.appendUniqueKeyBuildChunk({ ...firstChunk, keyTokens: ["alpha", "changed"] }),
    ).rejects.toBeInstanceOf(UniqueKeyBuildConflictError);
    await expect(
      store.appendUniqueKeyBuildChunk({ ...firstChunk, ordinal: 2, keyTokens: ["zeta"] }),
    ).rejects.toBeInstanceOf(UniqueKeyBuildConflictError);
    await expect(
      store.appendUniqueKeyBuildChunk({ ...firstChunk, ordinal: 1, keyTokens: ["beta"] }),
    ).rejects.toThrow("globally ordered");
    await expect(
      store.appendUniqueKeyBuildChunk({
        ...firstChunk,
        ordinal: 1,
        keyTokens: ["delta", "charlie"],
      }),
    ).rejects.toThrow("strict lexical order");
    await expect(
      store.appendUniqueKeyBuildChunk({
        ...firstChunk,
        ordinal: 1,
        keyTokens: ["delta", "omega"],
        updatedAt: "2026-08-24T12:07:00.000Z",
      }),
    ).resolves.toMatchObject({ nextOrdinal: 2, tokenCount: 4 });

    const finish = {
      buildId: begin.buildId,
      ownerId: begin.ownerId,
      expiresAtCutoff: "2026-08-24T12:08:00.000Z",
      expectedTableRevision: 0,
      expectedManifestVersion: null,
      chunkCount: 2,
      coversVersion: -1,
      completedAt: "2026-08-24T12:08:00.000Z",
    } as const;
    await expect(store.finishUniqueKeyBuild({ ...finish, chunkCount: 1 })).rejects.toBeInstanceOf(
      UniqueKeyBuildConflictError,
    );
    const ready = await store.finishUniqueKeyBuild(finish);
    expect(ready).toMatchObject({
      revision: 1,
      secondaryIndexes: { value_unique: { state: "ready", uniqueEnforced: true } },
    });
    await expect(store.finishUniqueKeyBuild(finish)).resolves.toEqual(ready);
    await expect(
      store.getExistingUniqueKeys(namespaceId, ["missing", "omega", "alpha"]),
    ).resolves.toEqual(["alpha", "omega"]);
    await expect(
      store.abortUniqueKeyBuild({
        buildId: begin.buildId,
        ownerId: begin.ownerId,
        expiresAtCutoff: "2026-08-24T12:09:00.000Z",
      }),
    ).rejects.toBeInstanceOf(UniqueKeyBuildConflictError);
    store.close();

    const reopened = await openStore(indexedDB, name);
    await expect(reopened.getUniqueKeyBuild(begin.buildId)).resolves.toMatchObject({
      state: "completed",
      retainedBytes: 0,
      tokenCount: 4,
    });
    reopened.close();
  });

  it("allows only the owner to abort a live generation and reclaims every staged token", async () => {
    const indexedDB = new IDBFactory();
    const store = await openStore(indexedDB);
    const table = uniqueBuildTable("abort-users", "abort-build");
    const namespaceId = secondaryUniqueKeyNamespace(table.id, "value_unique");
    await store.addTable(table);
    await store.beginUniqueKeyBuild({
      buildId: "abort-build",
      tableId: table.id,
      indexId: "value_unique",
      namespaceId,
      ownerId: "abort-owner",
      createdAt: "2026-08-24T12:00:00.000Z",
      expiresAt: "2026-08-24T12:30:00.000Z",
    });
    await store.appendUniqueKeyBuildChunk({
      buildId: "abort-build",
      ownerId: "abort-owner",
      expiresAtCutoff: "2026-08-24T12:01:00.000Z",
      ordinal: 0,
      keyTokens: ["one", "two"],
      updatedAt: "2026-08-24T12:01:00.000Z",
    });
    await expect(
      store.abortUniqueKeyBuild({
        buildId: "abort-build",
        ownerId: "intruder",
        expiresAtCutoff: "2026-08-24T12:02:00.000Z",
      }),
    ).rejects.toBeInstanceOf(UniqueKeyBuildConflictError);
    await expect(
      store.abortUniqueKeyBuild({
        buildId: "abort-build",
        ownerId: "abort-owner",
        expiresAtCutoff: "2026-08-24T12:02:00.000Z",
      }),
    ).resolves.toBe(true);
    await expect(store.getUniqueKeyBuild("abort-build")).resolves.toBeUndefined();
    await expect(
      store.abortUniqueKeyBuild({
        buildId: "abort-build",
        ownerId: "abort-owner",
        expiresAtCutoff: "2026-08-24T12:02:00.000Z",
      }),
    ).resolves.toBe(false);
    await store.beginUniqueKeyBuild({
      buildId: "abort-build",
      tableId: table.id,
      indexId: "value_unique",
      namespaceId,
      ownerId: "replacement-owner",
      createdAt: "2026-08-24T12:03:00.000Z",
      expiresAt: "2026-08-24T12:30:00.000Z",
    });
    await expect(
      store.appendUniqueKeyBuildChunk({
        buildId: "abort-build",
        ownerId: "replacement-owner",
        expiresAtCutoff: "2026-08-24T12:04:00.000Z",
        ordinal: 0,
        keyTokens: ["one"],
        updatedAt: "2026-08-24T12:04:00.000Z",
      }),
    ).resolves.toMatchObject({ nextOrdinal: 1, tokenCount: 1 });
    await expect(
      store.abortUniqueKeyBuild({
        buildId: "abort-build",
        ownerId: "replacement-owner",
        expiresAtCutoff: "2026-08-24T12:05:00.000Z",
      }),
    ).resolves.toBe(true);
    store.close();
  });
});

describe("IndexedDB postings build recovery hardening", () => {
  it("renews one owner, replays exact chunks, and publishes only a complete generation", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await store.addTable({
      id: "search-articles",
      name: "search_articles",
      managed: false,
      revision: 0,
      columns: [{ id: "title", name: "title", type: "string", nullable: false }],
      ftsColumns: {
        title: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "building",
          buildFromVersion: -1,
        },
      },
      createdAt: NOW,
    });
    const begin = {
      tableId: "search-articles",
      columnId: "title",
      buildId: "search-build",
      ownerId: "search-owner",
      createdAt: "2026-08-24T12:00:00.000Z",
      expiresAt: "2026-08-24T12:30:00.000Z",
    } as const;
    await expect(store.beginFtsBaseBuild(begin)).resolves.toBeUndefined();
    await expect(store.beginFtsBaseBuild(begin)).resolves.toBeUndefined();
    await expect(
      store.beginFtsBaseBuild({
        ...begin,
        buildId: "competing-build",
        ownerId: "competing-owner",
      }),
    ).rejects.toThrow("owned by another caller");
    await expect(
      store.renewFtsBaseBuild({
        ...begin,
        ownerId: "competing-owner",
        expiresAtCutoff: "2026-08-24T12:01:00.000Z",
        expiresAt: "2026-08-24T12:40:00.000Z",
        updatedAt: "2026-08-24T12:01:00.000Z",
      }),
    ).rejects.toThrow("ownership is absent or expired");
    await expect(
      store.renewFtsBaseBuild({
        ...begin,
        expiresAtCutoff: "2026-08-24T12:01:00.000Z",
        expiresAt: "2026-08-24T12:40:00.000Z",
        updatedAt: "2026-08-24T12:01:00.000Z",
      }),
    ).resolves.toBeUndefined();

    const first: Parameters<IndexedDbBlockStore["writeFtsBaseBuildChunk"]>[0] = {
      ...begin,
      expiresAtCutoff: "2026-08-24T12:02:00.000Z",
      expiresAt: "2026-08-24T12:40:00.000Z",
      updatedAt: "2026-08-24T12:02:00.000Z",
      ordinal: 0,
      chunk: [
        { term: "alpha", rowIds: [1n, 3n], tf: [1, 2] },
        { term: "beta", rowIds: [2n], tf: [1] },
      ],
    };
    await expect(store.writeFtsBaseBuildChunk({ ...first, ordinal: -1 })).rejects.toThrow(
      "ordinal is outside",
    );
    await expect(store.writeFtsBaseBuildChunk(first)).resolves.toBeUndefined();
    await expect(store.writeFtsBaseBuildChunk(first)).resolves.toBeUndefined();
    await expect(
      store.writeFtsBaseBuildChunk({
        ...first,
        chunk: [{ term: "changed", rowIds: [1n], tf: [1] }],
      }),
    ).rejects.toThrow("chunk replay changed");
    await expect(
      store.writeFtsBaseBuildChunk({
        ...first,
        ordinal: 2,
        chunk: [{ term: "omega", rowIds: [4n], tf: [1] }],
      }),
    ).rejects.toThrow("out of order");
    await expect(
      store.writeFtsBaseBuildChunk({
        ...first,
        ordinal: 1,
        chunk: [{ term: "omega", rowIds: [4n], tf: [1] }],
      }),
    ).resolves.toBeUndefined();

    const finish = {
      tableId: begin.tableId,
      columnId: begin.columnId,
      buildId: begin.buildId,
      ownerId: begin.ownerId,
      expiresAtCutoff: "2026-08-24T12:03:00.000Z",
      completedAt: "2026-08-24T12:03:00.000Z",
      coversVersion: 0,
      chunkCount: 2,
      totalTokens: 5,
    } as const;
    await expect(store.finishFtsBaseBuild({ ...finish, chunkCount: 1 })).rejects.toThrow(
      "incomplete",
    );
    await expect(store.finishFtsBaseBuild({ ...finish, totalTokens: 4 })).rejects.toThrow(
      "does not match",
    );
    await expect(store.finishFtsBaseBuild(finish)).resolves.toBeUndefined();
    await expect(
      store.readFtsCandidates(
        begin.tableId,
        begin.columnId,
        [
          { term: "alpha", prefix: false },
          { lower: "beta", lowerInclusive: true, upper: "omega", upperInclusive: true },
        ],
        0,
      ),
    ).resolves.toMatchObject({
      rowIdsByTerm: [
        [1n, 3n],
        [2n, 4n],
      ],
      coversVersion: 0,
      totalTokens: 5,
    });
    await expect(
      store.abortFtsBaseBuild({
        tableId: begin.tableId,
        columnId: begin.columnId,
        buildId: begin.buildId,
        ownerId: begin.ownerId,
        expiresAtCutoff: "2026-08-24T12:04:00.000Z",
      }),
    ).resolves.toBeUndefined();
    store.close();

    const reopened = await openStore(indexedDB, name);
    await expect(
      reopened.readFtsCandidates(
        begin.tableId,
        begin.columnId,
        [{ term: "omega", prefix: false }],
        0,
      ),
    ).resolves.toMatchObject({ rowIdsByTerm: [[4n]], coversVersion: 0 });
    reopened.close();
  });
});

describe("IndexedDB interrupted snapshot recovery hardening", () => {
  it("inspects, renews, aborts, cancels, and safely cancels a completed replay", async () => {
    const sourceFactory = new IDBFactory();
    const sourceName = crypto.randomUUID();
    const source = await openStore(sourceFactory, sourceName);
    await source.addTable({
      id: "snapshot-events",
      name: "snapshot_events",
      managed: false,
      revision: 0,
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      createdAt: NOW,
    });
    const block = await encodeBlock({ type: "number", values: [1, 2, 3] });
    await injectBlocks(sourceFactory, sourceName, [{ id: "snapshot/value/0", bytes: block }]);
    await publishManifest(source, {
      expectedVersion: null,
      blockIds: ["snapshot/value/0"],
      createdAt: NOW,
    });
    const snapshot = await exportSnapshotFrames(source);
    const identity = snapshotFrameStreamHeaderIdentity(snapshot.header);
    source.close();

    const targetFactory = new IDBFactory();
    const targetName = crypto.randomUUID();
    const target = await openStore(targetFactory, targetName);
    const begin = {
      identity,
      ownerId: "interrupted-owner",
      createdAt: NOW,
      expiresAt: "2026-08-24T12:30:00.000Z",
      header: snapshot.header,
    } as const;
    await target.beginSnapshotFrameImport(begin);
    await expect(
      target.renewSnapshotFrameImport({
        identity,
        ownerId: "foreign-owner",
        expiresAtCutoff: "2026-08-24T12:01:00.000Z",
        expiresAt: "2026-08-24T12:40:00.000Z",
      }),
    ).rejects.toBeInstanceOf(SnapshotImportConflictError);
    await expect(
      target.renewSnapshotFrameImport({
        identity,
        ownerId: begin.ownerId,
        expiresAtCutoff: "2026-08-24T12:01:00.000Z",
        expiresAt: "2026-08-24T12:40:00.000Z",
      }),
    ).resolves.toMatchObject({ expiresAt: "2026-08-24T12:40:00.000Z" });
    await target.appendSnapshotImportFrames({
      identity,
      ownerId: begin.ownerId,
      expiresAtCutoff: "2026-08-24T12:02:00.000Z",
      expiresAt: "2026-08-24T12:40:00.000Z",
      frames: snapshot.frames,
    });
    await expect(target.inspectInterruptedImport()).resolves.toMatchObject({
      identity,
      version: 0,
      stagedBlockCount: 1,
    });
    await expect(target.abortInterruptedImport("other-snapshot")).rejects.toThrow(
      "identity does not match",
    );
    await expect(target.abortInterruptedImport(identity)).resolves.toEqual({
      identity,
      removedBlockCount: 1,
      removedBytes: snapshot.footer.storedBytes,
    });
    await expect(target.inspectInterruptedImport()).resolves.toBeNull();
    await expect(target.getCurrentManifestVersion()).resolves.toBeNull();
    await expect(target.getBlock("snapshot/value/0")).resolves.toBeUndefined();

    await target.beginSnapshotFrameImport(begin);
    await target.appendSnapshotImportFrames({
      identity,
      ownerId: begin.ownerId,
      expiresAtCutoff: "2026-08-24T12:02:00.000Z",
      expiresAt: "2026-08-24T12:40:00.000Z",
      frames: snapshot.frames,
    });
    await expect(
      target.cancelSnapshotFrameImport({ identity, ownerId: "foreign-owner" }),
    ).rejects.toBeInstanceOf(SnapshotImportConflictError);
    await expect(
      target.cancelSnapshotFrameImport({ identity, ownerId: begin.ownerId }),
    ).resolves.toEqual({
      identity,
      removedBlockCount: 1,
      removedBytes: block.byteLength,
    });
    await expect(target.inspectInterruptedImport()).resolves.toBeNull();

    await importSnapshotFrames(target, snapshot);
    await expect(target.getCurrentManifestVersion()).resolves.toBe(0);
    await expect(target.getBlock("snapshot/value/0")).resolves.toEqual(block);
    await target.beginSnapshotFrameImport({ ...begin, ownerId: "replay-owner" });
    await expect(
      target.cancelSnapshotFrameImport({ identity, ownerId: "replay-owner" }),
    ).resolves.toEqual({ identity, removedBlockCount: 0, removedBytes: 0 });
    await expect(target.getCurrentManifestVersion()).resolves.toBe(0);
    await expect(target.getBlock("snapshot/value/0")).resolves.toEqual(block);
    target.close();
  });
});

describe("IndexedDB integrity family isolation", () => {
  it("reports independent malformed catalog families without stopping the bounded scan", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await mutate(indexedDB, name, "catalog", (transaction) => {
      const catalog = transaction.objectStore("catalog");
      const stringRecords: ReadonlyArray<readonly [string, unknown]> = [
        ["manifest/current", "invalid"],
        ["catalog/epoch", -1],
        ["catalog/schema-epoch", -1],
        ["table/id/broken", null],
        ["table/name/broken", ""],
        ["secondary-index/name/", null],
        ["secondary-index/name/broken", null],
        ["trigger/name/broken", null],
        ["trigger/id/broken", null],
        ["block-metadata/", null],
        ["block-metadata/missing", null],
        ["row-id/", 1n],
        ["auto-increment/", 1n],
        ["fts-base-index/broken", null],
        ["fts-base-build/broken", null],
        ["fts-retirement/broken", null],
        ["unique-key-build/broken", null],
        ["fts-base/broken", null],
        ["fts-chunk/index/broken", null],
        ["fts-chunk/broken/0", null],
      ];
      for (const [key, value] of stringRecords) catalog.put(value, key);
      const structuredRecords: ReadonlyArray<readonly [IDBValidKey, unknown]> = [
        [1, null],
        [["unique-key-build-chunk", "build", -1], null],
        [["unique-key-chunk-index", "namespace", "extra"], null],
        [["unique-key-base-part", "generation", 1], null],
        [["unique-key-chunk", "namespace", -1, "boundary"], null],
        [["unknown-structured-family", "namespace"], null],
      ];
      for (const [key, value] of structuredRecords) catalog.put(value, key);
    });

    const report = await store.checkIntegrity({ maxIssues: 100 });
    expect(report.ok).toBe(false);
    expect(report.issueCount).toBeGreaterThanOrEqual(24);
    expect(
      report.issues.filter((issue) => issue.code === "invalid-catalog-record").length,
    ).toBeGreaterThanOrEqual(20);
    await expect(store.checkIntegrity({ mode: "invalid" as "metadata" })).rejects.toThrow(
      "Integrity mode is invalid",
    );
    await expect(store.checkIntegrity({ maxIssues: -1 })).rejects.toThrow(
      "must be a non-negative whole number",
    );
    store.close();
  });
});
