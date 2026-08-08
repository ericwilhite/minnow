import {
  type CommitTransactionInput,
  type CompactionJobRecord,
  CompactionJobConflictError,
  type CompactionJobRecordUpdate,
  createManifest,
  storeNames,
  type BlockStore,
  type BlockWrite,
  type LeaseRecord,
  LeaseConflictError,
  type Manifest,
  type PublishManifestInput,
  type RowIdRange,
  type SegmentRecord,
  type TableRecord,
  type TransactionRecord,
  TransactionRecordConflictError,
  type TransactionRecordUpdate,
  UniqueKeyConflictError,
  normalizeCompactionJobRecord,
  updateCompactionJobRecord,
  updateTransactionRecord,
  WriteConflictError,
} from "./types.js";

const SCHEMA_VERSION = 1;
const CURRENT_MANIFEST_KEY = "manifest/current";
const TABLE_ID_PREFIX = "table/id/";
const TABLE_NAME_PREFIX = "table/name/";
const ROW_ID_PREFIX = "row-id/";
const UNIQUE_KEY_CHUNK_INDEX = "unique-key-chunk-index";
const UNIQUE_KEY_CHUNK = "unique-key-chunk";
const COMPACTION_JOB_KEY_PREFIX = "compaction-job/";
const storageTextEncoder = new TextEncoder();
const storageTextBuffer = new Uint8Array(1_024);

interface UniqueKeyChunk {
  addedTokens: string[];
  removedTokens: string[];
}

interface CompactionJobEnvelope {
  kind: "compaction-job";
  record: CompactionJobRecord;
}

export interface IndexedDbBlockStoreOptions {
  name: string;
  durability?: IDBTransactionDurability;
  indexedDB?: IDBFactory;
}

export class IndexedDbBlockStore implements BlockStore {
  readonly #db: IDBDatabase;
  readonly #durability: IDBTransactionDurability;

  private constructor(db: IDBDatabase, durability: IDBTransactionDurability) {
    this.#db = db;
    this.#durability = durability;
  }

  static async open(options: IndexedDbBlockStoreOptions): Promise<IndexedDbBlockStore> {
    const factory = options.indexedDB ?? getGlobalIndexedDb();
    if (factory === undefined) throw new Error("IndexedDB is unavailable");
    const request = factory.open(options.name, SCHEMA_VERSION);
    request.addEventListener("upgradeneeded", () => {
      for (const storeName of storeNames) {
        if (!request.result.objectStoreNames.contains(storeName)) {
          request.result.createObjectStore(storeName);
        }
      }
    });
    const db = await requestResult(request);
    return new IndexedDbBlockStore(db, options.durability ?? "relaxed");
  }

  async addBlock(id: string, bytes: Uint8Array): Promise<void> {
    return this.addBlocks([{ id, bytes }]);
  }

  async addBlocks(blocks: readonly BlockWrite[]): Promise<void> {
    const ids = new Set<string>();
    for (const block of blocks) {
      if (block.id.length === 0) throw new TypeError("Block ID cannot be empty");
      if (ids.has(block.id)) throw new Error(`Block already exists: ${block.id}`);
      ids.add(block.id);
    }
    const transaction = this.#transaction("blocks", "readwrite");
    const store = transaction.objectStore("blocks");
    blocks.forEach((block) => store.add(new Uint8Array(block.bytes), block.id));
    await transactionDone(transaction);
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    const transaction = this.#transaction("blocks", "readonly");
    const value: unknown = await requestResult<unknown>(transaction.objectStore("blocks").get(id));
    await transactionDone(transaction);
    return value === undefined ? undefined : asBytes(value);
  }

  async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    const transaction = this.#transaction("blocks", "readonly");
    const store = transaction.objectStore("blocks");
    const values = await Promise.all(ids.map((id) => requestResult<unknown>(store.get(id))));
    await transactionDone(transaction);
    return values.map((value) => (value === undefined ? undefined : asBytes(value)));
  }

  async removeBlock(id: string): Promise<void> {
    const transaction = this.#transaction("blocks", "readwrite");
    transaction.objectStore("blocks").delete(id);
    await transactionDone(transaction);
  }

  async listBlockIds(): Promise<string[]> {
    const transaction = this.#transaction("blocks", "readonly");
    const keys = await requestResult(transaction.objectStore("blocks").getAllKeys());
    await transactionDone(transaction);
    return keys.map(String).sort();
  }

  async addTable(record: TableRecord): Promise<void> {
    const transaction = this.#transaction("catalog", "readwrite");
    const store = transaction.objectStore("catalog");
    const idKey = `${TABLE_ID_PREFIX}${record.id}`;
    const nameKey = `${TABLE_NAME_PREFIX}${record.name}`;
    const [existingId, existingName] = await Promise.all([
      requestResult(store.getKey(idKey)),
      requestResult(store.getKey(nameKey)),
    ]);
    if (existingId !== undefined || existingName !== undefined) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(
        existingId !== undefined
          ? `Table already exists: ${record.id}`
          : `Table name already exists: ${record.name}`,
      );
    }
    store.add(structuredClone(record), idKey);
    store.add(record.id, nameKey);
    await transactionDone(transaction);
  }

  async getTable(id: string): Promise<TableRecord | undefined> {
    const transaction = this.#transaction("catalog", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("catalog").get(`${TABLE_ID_PREFIX}${id}`),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asTableRecord(value);
  }

  async getTableByName(name: string): Promise<TableRecord | undefined> {
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const id = (await requestResult(store.get(`${TABLE_NAME_PREFIX}${name}`))) as
      string | undefined;
    const value: unknown =
      id === undefined ? undefined : await requestResult(store.get(`${TABLE_ID_PREFIX}${id}`));
    await transactionDone(transaction);
    return value === undefined ? undefined : asTableRecord(value);
  }

  async listTables(): Promise<TableRecord[]> {
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const values: unknown[] = await requestResult(store.getAll());
    await transactionDone(transaction);
    return values
      .filter(isTableRecord)
      .map(asTableRecord)
      .sort((left, right) => left.name.localeCompare(right.name));
  }

  async addSegment(record: SegmentRecord): Promise<void> {
    const transaction = this.#transaction("segments", "readwrite");
    transaction.objectStore("segments").add(structuredClone(record), record.id);
    await transactionDone(transaction);
  }

  async getSegment(id: string): Promise<SegmentRecord | undefined> {
    const transaction = this.#transaction("segments", "readonly");
    const value: unknown = await requestResult(transaction.objectStore("segments").get(id));
    await transactionDone(transaction);
    return value === undefined ? undefined : asSegmentRecord(value);
  }

  async listSegments(tableId?: string): Promise<SegmentRecord[]> {
    const transaction = this.#transaction("segments", "readonly");
    const values: unknown[] = await requestResult(transaction.objectStore("segments").getAll());
    await transactionDone(transaction);
    return values
      .map(asSegmentRecord)
      .filter((record) => tableId === undefined || record.tableId === tableId)
      .sort((left, right) => left.id.localeCompare(right.id));
  }

  async removeSegment(id: string): Promise<void> {
    const transaction = this.#transaction("segments", "readwrite");
    transaction.objectStore("segments").delete(id);
    await transactionDone(transaction);
  }

  async reserveRowIds(tableId: string, count: number): Promise<RowIdRange> {
    validateCount(count);
    const transaction = this.#transaction("catalog", "readwrite");
    const store = transaction.objectStore("catalog");
    const key = `${ROW_ID_PREFIX}${tableId}`;
    const current = (await requestResult(store.get(key))) as bigint | undefined;
    const start = current ?? 1n;
    const endExclusive = start + BigInt(count);
    store.put(endExclusive, key);
    await transactionDone(transaction);
    return { start, endExclusive };
  }

  async getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    const tokens = [...new Set(keyTokens)];
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const tableValue: unknown = await requestResult(store.get(`${TABLE_ID_PREFIX}${tableId}`));
    const table = tableValue === undefined ? undefined : asTableRecord(tableValue);
    if (table?.uniqueKeyStorage === "chunks-v1") {
      const { existing } = await readChunkedUniqueKeys(store, tableId, tokens);
      await transactionDone(transaction);
      return [...existing].sort();
    }
    const keys = await Promise.all(
      tokens.map((token) => requestResult(store.getKey(uniqueKeyKey(tableId, token)))),
    );
    await transactionDone(transaction);
    return tokens.filter((_, index) => keys[index] !== undefined).sort();
  }

  async getCurrentManifest(): Promise<Manifest | undefined> {
    const transaction = this.#transaction(["catalog", "manifests"], "readonly");
    const version = (await requestResult(
      transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
    )) as number | undefined;
    const manifest =
      version === undefined
        ? undefined
        : ((await requestResult(transaction.objectStore("manifests").get(version))) as
            Manifest | undefined);
    await transactionDone(transaction);
    if (version !== undefined && manifest === undefined)
      throw new Error("Current manifest is missing");
    return manifest === undefined ? undefined : structuredClone(manifest);
  }

  async getManifest(version: number): Promise<Manifest | undefined> {
    const transaction = this.#transaction("manifests", "readonly");
    const value: unknown = await requestResult<unknown>(
      transaction.objectStore("manifests").get(version),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asManifest(value);
  }

  async listManifests(): Promise<Manifest[]> {
    const transaction = this.#transaction("manifests", "readonly");
    const values: unknown[] = await requestResult<unknown[]>(
      transaction.objectStore("manifests").getAll(),
    );
    await transactionDone(transaction);
    return values.map(asManifest).sort((left, right) => left.version - right.version);
  }

  async publishManifest(input: PublishManifestInput): Promise<Manifest> {
    const transaction = this.#transaction(["blocks", "catalog", "manifests"], "readwrite");
    const catalog = transaction.objectStore("catalog");
    const current = (await requestResult(catalog.get(CURRENT_MANIFEST_KEY))) as number | undefined;
    const actualVersion = current ?? null;
    if (actualVersion !== input.expectedVersion) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new WriteConflictError(input.expectedVersion, actualVersion);
    }
    const blocks = transaction.objectStore("blocks");
    for (const id of input.blockIds) {
      const key = await requestResult(blocks.getKey(id));
      if (key === undefined) {
        transaction.abort();
        await ignoreAbort(transaction);
        throw new Error(`Manifest references missing block: ${id}`);
      }
    }
    const manifest = createManifest(input);
    transaction.objectStore("manifests").add(manifest, manifest.version);
    catalog.put(manifest.version, CURRENT_MANIFEST_KEY);
    await transactionDone(transaction);
    return manifest;
  }

  async createTransaction(record: TransactionRecord): Promise<void> {
    const transaction = this.#transaction("transactions", "readwrite");
    transaction.objectStore("transactions").add(structuredClone(record), record.id);
    await transactionDone(transaction);
  }

  async getTransaction(id: string): Promise<TransactionRecord | undefined> {
    const transaction = this.#transaction("transactions", "readonly");
    const value: unknown = await requestResult<unknown>(
      transaction.objectStore("transactions").get(id),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asTransactionRecord(value);
  }

  async listTransactions(): Promise<TransactionRecord[]> {
    const transaction = this.#transaction("transactions", "readonly");
    const values: unknown[] = await requestResult<unknown[]>(
      transaction.objectStore("transactions").getAll(),
    );
    await transactionDone(transaction);
    return values
      .map(asTransactionRecord)
      .sort(
        (left, right) =>
          left.startedAt.localeCompare(right.startedAt) || left.id.localeCompare(right.id),
      );
  }

  async updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    const transaction = this.#transaction("transactions", "readwrite");
    const store = transaction.objectStore("transactions");
    const value: unknown = await requestResult<unknown>(store.get(id));
    const current = value === undefined ? undefined : asTransactionRecord(value);
    if (current?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new TransactionRecordConflictError(id, expectedRevision, current?.revision ?? null);
    }
    try {
      assertGenericTransactionUpdateAllowed(current, update);
    } catch (error) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw error;
    }
    const updated = updateTransactionRecord(current, update);
    store.put(updated, id);
    await transactionDone(transaction);
    return structuredClone(updated);
  }

  async commitTransaction(input: CommitTransactionInput): Promise<Manifest> {
    const transaction = this.#transaction(
      ["blocks", "catalog", "manifests", "transactions", "segments"],
      "readwrite",
    );
    const transactionStore = transaction.objectStore("transactions");
    const transactionValue: unknown = await requestResult<unknown>(
      transactionStore.get(input.transactionId),
    );
    const record =
      transactionValue === undefined ? undefined : asTransactionRecord(transactionValue);
    if (record?.revision !== input.expectedTransactionRevision || record.status !== "active") {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new TransactionRecordConflictError(
        input.transactionId,
        input.expectedTransactionRevision,
        record?.revision ?? null,
      );
    }

    const catalog = transaction.objectStore("catalog");
    const current = (await requestResult(catalog.get(CURRENT_MANIFEST_KEY))) as number | undefined;
    const actualVersion = current ?? null;
    if (actualVersion !== input.expectedManifestVersion) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new WriteConflictError(input.expectedManifestVersion, actualVersion);
    }

    if (record.snapshotVersion !== input.expectedManifestVersion) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error("Transaction snapshot does not match the expected manifest");
    }

    const baseManifest =
      input.expectedManifestVersion === null
        ? undefined
        : ((await requestResult(
            transaction.objectStore("manifests").get(input.expectedManifestVersion),
          )) as Manifest | undefined);
    const baseBlockIds = baseManifest?.blockIds ?? [];
    if (input.expectedManifestVersion !== null && baseManifest === undefined) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Snapshot manifest is missing: ${String(input.expectedManifestVersion)}`);
    }
    const removedBlockIds = [...new Set(input.removedBlockIds ?? [])];
    const baseBlockIdSet = new Set(baseBlockIds);
    const invalidRemovedBlock = removedBlockIds.find((id) => !baseBlockIdSet.has(id));
    if (invalidRemovedBlock !== undefined) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(
        `Cannot supersede a block outside the transaction snapshot: ${invalidRemovedBlock}`,
      );
    }
    const pendingRemovedBlock = removedBlockIds.find((id) => record.pendingBlockIds.includes(id));
    if (pendingRemovedBlock !== undefined) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Cannot supersede a pending block: ${pendingRemovedBlock}`);
    }
    const removedBlockIdSet = new Set(removedBlockIds);
    if (
      !sameBlockSet(input.blockIds, [
        ...baseBlockIds.filter((id) => !removedBlockIdSet.has(id)),
        ...record.pendingBlockIds,
      ])
    ) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error("Transaction manifest does not match its snapshot and pending blocks");
    }

    const blockStore = transaction.objectStore("blocks");
    const storedBlockKeys = await Promise.all(
      record.pendingBlockIds.map((id) => requestResult(blockStore.getKey(id))),
    );
    const missingBlockIndex = storedBlockKeys.findIndex((key) => key === undefined);
    if (missingBlockIndex >= 0) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(
        `Manifest references missing block: ${record.pendingBlockIds[missingBlockIndex] ?? ""}`,
      );
    }

    const segmentStore = transaction.objectStore("segments");
    const pendingSegmentValues: unknown[] = await Promise.all(
      record.pendingSegmentIds.map((id) => requestResult(segmentStore.get(id))),
    );
    const missingSegmentIndex = pendingSegmentValues.findIndex((value) => value === undefined);
    if (missingSegmentIndex >= 0) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(
        `Transaction references missing segment: ${record.pendingSegmentIds[missingSegmentIndex] ?? ""}`,
      );
    }
    const pendingSegments = pendingSegmentValues.map(asSegmentRecord);
    const foreignSegment = pendingSegments.find((segment) => segment.transactionId !== record.id);
    if (foreignSegment !== undefined) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Segment ${foreignSegment.id} belongs to another transaction`);
    }

    const uniqueKeyChanges = input.uniqueKeyChanges;
    let chunkState: Awaited<ReturnType<typeof readChunkedUniqueKeys>> | undefined;
    if (uniqueKeyChanges !== undefined) {
      const storedKeys =
        uniqueKeyChanges.storageMode === "chunks-v1"
          ? ((chunkState = await readChunkedUniqueKeys(
              catalog,
              uniqueKeyChanges.tableId,
              uniqueKeyChanges.keyTokens,
            )),
            uniqueKeyChanges.keyTokens.map((token) =>
              chunkState?.existing.has(token) === true ? token : undefined,
            ))
          : await Promise.all(
              uniqueKeyChanges.keyTokens.map((token) =>
                requestResult(catalog.getKey(uniqueKeyKey(uniqueKeyChanges.tableId, token))),
              ),
            );
      if (uniqueKeyChanges.requireAbsent) {
        const conflictIndex = storedKeys.findIndex((key) => key !== undefined);
        if (conflictIndex >= 0) {
          transaction.abort();
          await ignoreAbort(transaction);
          throw new UniqueKeyConflictError(
            uniqueKeyChanges.tableId,
            uniqueKeyChanges.keyTokens[conflictIndex] ?? "",
          );
        }
      }
    }

    const manifest = createManifest({
      expectedVersion: input.expectedManifestVersion,
      blockIds: input.blockIds,
      createdAt: input.committedAt,
    });
    const committed = updateTransactionRecord(record, {
      status: "committed",
      committedVersion: manifest.version,
      updatedAt: input.committedAt,
    });
    transaction.objectStore("manifests").add(manifest, manifest.version);
    catalog.put(manifest.version, CURRENT_MANIFEST_KEY);
    for (const segment of pendingSegments) {
      segmentStore.put(
        {
          ...segment,
          level: segment.level ?? 0,
          logicalOrder: segment.logicalOrder ?? manifest.version,
        },
        segment.id,
      );
    }
    if (uniqueKeyChanges !== undefined) {
      if (uniqueKeyChanges.storageMode === "chunks-v1") {
        const existing = chunkState?.existing ?? new Set<string>();
        const chunk: UniqueKeyChunk = {
          addedTokens:
            uniqueKeyChanges.remove === true
              ? []
              : uniqueKeyChanges.keyTokens.filter((token) => !existing.has(token)),
          removedTokens:
            uniqueKeyChanges.remove === true
              ? uniqueKeyChanges.keyTokens.filter((token) => existing.has(token))
              : [],
        };
        if (chunk.addedTokens.length > 0 || chunk.removedTokens.length > 0) {
          catalog.put(chunk, uniqueKeyChunkKey(uniqueKeyChanges.tableId, manifest.version));
          catalog.put(
            [...(chunkState?.versions ?? []), manifest.version],
            uniqueKeyChunkIndexKey(uniqueKeyChanges.tableId),
          );
        }
      } else {
        uniqueKeyChanges.keyTokens.forEach((token) => {
          const key = uniqueKeyKey(uniqueKeyChanges.tableId, token);
          if (uniqueKeyChanges.remove === true) catalog.delete(key);
          else catalog.put(manifest.version, key);
        });
      }
    }
    transactionStore.put(committed, committed.id);
    await transactionDone(transaction);
    return manifest;
  }

  async createLease(record: LeaseRecord): Promise<void> {
    const transaction = this.#transaction("leases", "readwrite");
    transaction.objectStore("leases").add(structuredClone(record), record.id);
    await transactionDone(transaction);
  }

  async getLease(id: string): Promise<LeaseRecord | undefined> {
    const transaction = this.#transaction("leases", "readonly");
    const value: unknown = await requestResult(transaction.objectStore("leases").get(id));
    await transactionDone(transaction);
    return value === undefined ? undefined : asLeaseRecord(value);
  }

  async listLeases(): Promise<LeaseRecord[]> {
    const transaction = this.#transaction("leases", "readonly");
    const values: unknown[] = await requestResult(transaction.objectStore("leases").getAll());
    await transactionDone(transaction);
    return values.map(asLeaseRecord).sort((left, right) => left.id.localeCompare(right.id));
  }

  async renewLease(id: string, expectedRevision: number, expiresAt: string): Promise<LeaseRecord> {
    const transaction = this.#transaction("leases", "readwrite");
    const store = transaction.objectStore("leases");
    const value: unknown = await requestResult(store.get(id));
    const record = value === undefined ? undefined : asLeaseRecord(value);
    if (record?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new LeaseConflictError(id, expectedRevision, record?.revision ?? null);
    }
    const renewed = { ...record, expiresAt, revision: record.revision + 1 };
    store.put(renewed, id);
    await transactionDone(transaction);
    return structuredClone(renewed);
  }

  async removeLease(id: string): Promise<void> {
    const transaction = this.#transaction("leases", "readwrite");
    transaction.objectStore("leases").delete(id);
    await transactionDone(transaction);
  }

  async createCompactionJob(record: CompactionJobRecord): Promise<void> {
    const normalized = normalizeCompactionJobRecord(record);
    const transaction = this.#transaction("gc", "readwrite");
    const store = transaction.objectStore("gc");
    const key = compactionJobKey(normalized.id);
    if ((await requestResult(store.getKey(key))) !== undefined) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Compaction job already exists: ${normalized.id}`);
    }
    store.add(compactionJobEnvelope(normalized), key);
    await transactionDone(transaction);
  }

  async getCompactionJob(id: string): Promise<CompactionJobRecord | undefined> {
    const transaction = this.#transaction("gc", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("gc").get(compactionJobKey(id)),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asCompactionJobEnvelope(value);
  }

  async listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]> {
    const transaction = this.#transaction("gc", "readonly");
    const values: unknown[] = await requestResult(transaction.objectStore("gc").getAll());
    await transactionDone(transaction);
    return values
      .filter(isCompactionJobEnvelope)
      .map(asCompactionJobEnvelope)
      .filter((record) => tableId === undefined || record.tableId === tableId)
      .sort(
        (left, right) =>
          left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
      );
  }

  async updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord> {
    if (update.state === "cancelled") {
      throw new TypeError("Use cancelCompactionJob to cancel a compaction job");
    }
    const transaction = this.#transaction("gc", "readwrite");
    const store = transaction.objectStore("gc");
    const key = compactionJobKey(id);
    const value: unknown = await requestResult(store.get(key));
    const current = value === undefined ? undefined : asCompactionJobEnvelope(value);
    if (current?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new CompactionJobConflictError(id, expectedRevision, current?.revision ?? null);
    }
    const updated = updateCompactionJobRecord(current, update);
    store.put(compactionJobEnvelope(updated), key);
    await transactionDone(transaction);
    return structuredClone(updated);
  }

  async cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord> {
    const transaction = this.#transaction(["gc", "transactions"], "readwrite");
    const jobStore = transaction.objectStore("gc");
    const transactionStore = transaction.objectStore("transactions");
    const key = compactionJobKey(id);
    const value: unknown = await requestResult(jobStore.get(key));
    const current = value === undefined ? undefined : asCompactionJobEnvelope(value);
    if (current?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new CompactionJobConflictError(id, expectedRevision, current?.revision ?? null);
    }
    if (isTerminalCompactionJob(current)) {
      await transactionDone(transaction);
      return structuredClone(current);
    }

    const transactionValue: unknown =
      current.transactionId === null
        ? undefined
        : await requestResult(transactionStore.get(current.transactionId));
    const linkedTransaction =
      transactionValue === undefined ? undefined : asTransactionRecord(transactionValue);
    let updated: CompactionJobRecord;
    try {
      if (linkedTransaction?.status === "committed") {
        if (linkedTransaction.committedVersion === null) {
          throw new Error(`Committed transaction has no manifest version: ${linkedTransaction.id}`);
        }
        updated = updateCompactionJobRecord(current, {
          state: "published",
          publishedVersion: linkedTransaction.committedVersion,
          updatedAt: cancelledAt,
          error: null,
        });
      } else {
        updated = updateCompactionJobRecord(current, {
          state: "cancelled",
          updatedAt: cancelledAt,
          error: null,
        });
        if (linkedTransaction?.status === "active") {
          transactionStore.put(
            updateTransactionRecord(linkedTransaction, {
              status: "aborted",
              updatedAt: cancelledAt,
              committedVersion: null,
            }),
            linkedTransaction.id,
          );
        }
      }
    } catch (error) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw error;
    }
    jobStore.put(compactionJobEnvelope(updated), key);
    await transactionDone(transaction);
    return structuredClone(updated);
  }

  async removeCompactionJob(id: string): Promise<void> {
    const transaction = this.#transaction("gc", "readwrite");
    transaction.objectStore("gc").delete(compactionJobKey(id));
    await transactionDone(transaction);
  }

  async getLogicalStorageBytes(): Promise<number> {
    const transaction = this.#transaction([...storeNames], "readonly");
    let total = 0;
    for (const storeName of storeNames) {
      total += await logicalObjectStoreBytes(transaction.objectStore(storeName));
    }
    await transactionDone(transaction);
    return total;
  }

  close(): void {
    this.#db.close();
  }

  #transaction(stores: string | string[], mode: IDBTransactionMode): IDBTransaction {
    return this.#db.transaction(stores, mode, { durability: this.#durability });
  }
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.addEventListener("success", () => resolve(request.result), { once: true });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("IndexedDB request failed")),
      {
        once: true,
      },
    );
  });
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.addEventListener("complete", () => resolve(), { once: true });
    transaction.addEventListener(
      "abort",
      () => reject(transaction.error ?? new Error("Transaction aborted")),
      {
        once: true,
      },
    );
    transaction.addEventListener(
      "error",
      () => reject(transaction.error ?? new Error("Transaction failed")),
      {
        once: true,
      },
    );
  });
}

async function ignoreAbort(transaction: IDBTransaction): Promise<void> {
  try {
    await transactionDone(transaction);
  } catch {
    // The caller reports the more useful domain error.
  }
}

function asBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return new Uint8Array(value);
  if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0));
  throw new Error("Stored block is not binary data");
}

function asManifest(value: unknown): Manifest {
  return structuredClone(value) as Manifest;
}

function asTransactionRecord(value: unknown): TransactionRecord {
  return structuredClone(value) as TransactionRecord;
}

function asTableRecord(value: unknown): TableRecord {
  return structuredClone(value) as TableRecord;
}

function isTableRecord(value: unknown): value is TableRecord {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof Reflect.get(value, "id") === "string" &&
    typeof Reflect.get(value, "name") === "string" &&
    Array.isArray(Reflect.get(value, "columns"))
  );
}

function asSegmentRecord(value: unknown): SegmentRecord {
  return structuredClone(value) as SegmentRecord;
}

function asLeaseRecord(value: unknown): LeaseRecord {
  return structuredClone(value) as LeaseRecord;
}

function compactionJobKey(id: string): string {
  return `${COMPACTION_JOB_KEY_PREFIX}${id}`;
}

function compactionJobEnvelope(record: CompactionJobRecord): CompactionJobEnvelope {
  return { kind: "compaction-job", record: structuredClone(record) };
}

function isCompactionJobEnvelope(value: unknown): value is CompactionJobEnvelope {
  return (
    typeof value === "object" &&
    value !== null &&
    Reflect.get(value, "kind") === "compaction-job" &&
    typeof Reflect.get(value, "record") === "object" &&
    Reflect.get(value, "record") !== null
  );
}

function asCompactionJobEnvelope(value: unknown): CompactionJobRecord {
  if (!isCompactionJobEnvelope(value)) throw new Error("Stored compaction job is invalid");
  return normalizeCompactionJobRecord(structuredClone(value.record));
}

function isTerminalCompactionJob(record: CompactionJobRecord): boolean {
  return record.state === "published" || record.state === "cancelled" || record.state === "aborted";
}

function assertGenericTransactionUpdateAllowed(
  record: TransactionRecord,
  update: TransactionRecordUpdate,
): void {
  if (record.status !== "active") {
    throw new TypeError(`Only active transactions can be updated; found ${record.status}`);
  }
  if (update.status === "committed") {
    throw new TypeError("Use commitTransaction to commit a transaction");
  }
  if (Reflect.has(update, "committedVersion")) {
    throw new TypeError("Only commitTransaction can set a committed transaction version");
  }
}

function validateCount(count: number): void {
  if (!Number.isSafeInteger(count) || count <= 0) {
    throw new RangeError("Row ID reservation count must be a positive whole number");
  }
}

function uniqueKeyKey(tableId: string, keyToken: string): IDBValidKey {
  return ["unique-key", tableId, keyToken];
}

function uniqueKeyChunkIndexKey(tableId: string): IDBValidKey {
  return [UNIQUE_KEY_CHUNK_INDEX, tableId];
}

function uniqueKeyChunkKey(tableId: string, version: number): IDBValidKey {
  return [UNIQUE_KEY_CHUNK, tableId, version];
}

async function readChunkedUniqueKeys(
  store: IDBObjectStore,
  tableId: string,
  requestedTokens: readonly string[],
): Promise<{ existing: Set<string>; versions: number[] }> {
  const rawVersions: unknown = await requestResult(store.get(uniqueKeyChunkIndexKey(tableId)));
  const versions = Array.isArray(rawVersions)
    ? rawVersions.filter((value): value is number => Number.isSafeInteger(value))
    : [];
  const chunks = await Promise.all(
    versions.map((version) =>
      requestResult<unknown>(store.get(uniqueKeyChunkKey(tableId, version))),
    ),
  );
  const requested = new Set(requestedTokens);
  const existing = new Set<string>();
  for (const value of chunks) {
    const chunk = asUniqueKeyChunk(value);
    for (const token of chunk.addedTokens) {
      if (requested.has(token)) existing.add(token);
    }
    for (const token of chunk.removedTokens) existing.delete(token);
  }
  return { existing, versions };
}

function asUniqueKeyChunk(value: unknown): UniqueKeyChunk {
  if (typeof value !== "object" || value === null) {
    throw new Error("Unique-key chunk is missing or invalid");
  }
  const record = value as Record<string, unknown>;
  const addedTokens = record.addedTokens;
  const removedTokens = record.removedTokens;
  if (!isStringArray(addedTokens) || !isStringArray(removedTokens)) {
    throw new Error("Unique-key chunk is invalid");
  }
  return { addedTokens, removedTokens };
}

function sameBlockSet(actual: readonly string[], expected: readonly string[]): boolean {
  const actualSet = new Set(actual);
  const expectedSet = new Set(expected);
  return (
    actualSet.size === expectedSet.size &&
    [...expectedSet].every((blockId) => actualSet.has(blockId))
  );
}

function getGlobalIndexedDb(): IDBFactory | undefined {
  return "indexedDB" in globalThis ? globalThis.indexedDB : undefined;
}

function logicalStoredBytes(value: unknown, seen = new Set<object>()): number {
  if (value === null || value === undefined) return 0;
  if (typeof value === "string") return utf8Bytes(value);
  if (typeof value === "number" || typeof value === "bigint") return 8;
  if (typeof value === "boolean") return 1;
  if (value instanceof Date) return 8;
  if (value instanceof ArrayBuffer) return value.byteLength;
  if (ArrayBuffer.isView(value)) return value.byteLength;
  if (typeof value !== "object" || seen.has(value)) return 0;
  seen.add(value);
  if (Array.isArray(value)) {
    return value.reduce<number>((bytes, entry) => bytes + logicalStoredBytes(entry, seen), 0);
  }
  return Object.entries(value).reduce(
    (bytes, [key, entry]) =>
      bytes + logicalStoredBytes(key, seen) + logicalStoredBytes(entry, seen),
    0,
  );
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((entry: unknown) => typeof entry === "string");
}

function logicalObjectStoreBytes(store: IDBObjectStore): Promise<number> {
  return new Promise((resolve, reject) => {
    let total = 0;
    const request = store.openCursor();
    request.addEventListener("success", () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(total);
        return;
      }
      total += logicalStoredBytes(cursor.key) + logicalStoredBytes(cursor.value);
      cursor.continue();
    });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("IndexedDB size cursor failed")),
      { once: true },
    );
  });
}

function utf8Bytes(value: string): number {
  if (value.length * 3 <= storageTextBuffer.byteLength) {
    return storageTextEncoder.encodeInto(value, storageTextBuffer).written;
  }
  return storageTextEncoder.encode(value).byteLength;
}
