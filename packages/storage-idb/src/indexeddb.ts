import {
  type CommitTransactionInput,
  type CompactionJobRecord,
  CompactionJobConflictError,
  type CompactionJobRecordUpdate,
  createManifest,
  type CreateGarbageCollectionJobInput,
  createGarbageCollectionJobRecord,
  storeNames,
  advanceGarbageCollectionJobRecord,
  type BlockStore,
  type BlockWrite,
  type GarbageCollectionJobRecord,
  GarbageCollectionJobConflictError,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  LeaseConflictError,
  type Manifest,
  type PublishManifestInput,
  type RowIdRange,
  type RunGarbageCollectionStepInput,
  type SegmentRecord,
  SnapshotManifestMissingError,
  type StoragePage,
  type TableRecord,
  type TempOwnerRecord,
  TempOwnerConflictError,
  type TempRunPage,
  type TransactionRecord,
  TransactionRecordConflictError,
  type TransactionRecordUpdate,
  UniqueKeyConflictError,
  normalizeCompactionJobRecord,
  normalizeGarbageCollectionJobRecord,
  normalizeSegmentRecord,
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
const GARBAGE_COLLECTION_JOB_KEY_PREFIX = "garbage-collection-job/";
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

interface GarbageCollectionJobEnvelope {
  kind: "garbage-collection-job";
  record: GarbageCollectionJobRecord;
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

  async putTempRunPage(page: TempRunPage): Promise<void> {
    validateTempRunPage(page);
    const transaction = this.#transaction("temp", "readwrite");
    transaction.objectStore("temp").put(page.bytes.slice(), tempRunPageKey(page));
    await transactionDone(transaction);
  }

  async getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    validateTempRunPageIdentity(ownerId, runId, pageIndex);
    const transaction = this.#transaction("temp", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("temp").get(["run", ownerId, runId, pageIndex]),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asBytes(value).slice();
  }

  async removeTempRun(ownerId: string, runId: string): Promise<void> {
    validateTempRunPageIdentity(ownerId, runId, 0);
    const transaction = this.#transaction("temp", "readwrite");
    const store = transaction.objectStore("temp");
    await visitObjectStoreSequentially(store, (_value, key) => {
      if (isTempRunPageKey(key, ownerId, runId)) store.delete(key);
    });
    await transactionDone(transaction);
  }

  async removeTempOwner(ownerId: string): Promise<void> {
    validateTempId(ownerId, "Temp run owner ID");
    const transaction = this.#transaction("temp", "readwrite");
    const store = transaction.objectStore("temp");
    await visitObjectStoreSequentially(store, (_value, key) => {
      if (isTempRunPageKey(key, ownerId)) store.delete(key);
    });
    store.delete(tempOwnerKey(ownerId));
    await transactionDone(transaction);
  }

  async createTempOwner(record: TempOwnerRecord): Promise<void> {
    validateTempOwnerRecord(record);
    const transaction = this.#transaction("temp", "readwrite");
    const store = transaction.objectStore("temp");
    const existing = await requestResult(store.getKey(tempOwnerKey(record.ownerId)));
    if (existing !== undefined) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Temp owner already exists: ${record.ownerId}`);
    }
    store.put(structuredClone(record), tempOwnerKey(record.ownerId));
    await transactionDone(transaction);
  }

  async getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined> {
    validateTempId(ownerId, "Temp run owner ID");
    const transaction = this.#transaction("temp", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("temp").get(tempOwnerKey(ownerId)),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asTempOwnerRecord(value);
  }

  async renewTempOwner(
    ownerId: string,
    expectedRevision: number,
    expiresAt: string,
  ): Promise<TempOwnerRecord> {
    validateTempId(ownerId, "Temp run owner ID");
    validateLeaseExpiration(expiresAt);
    const transaction = this.#transaction("temp", "readwrite");
    const store = transaction.objectStore("temp");
    const value: unknown = await requestResult(store.get(tempOwnerKey(ownerId)));
    const record = value === undefined ? undefined : asTempOwnerRecord(value);
    if (record?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new TempOwnerConflictError(ownerId, expectedRevision, record?.revision ?? null);
    }
    const renewed = { ...record, expiresAt, revision: record.revision + 1 };
    store.put(renewed, tempOwnerKey(ownerId));
    await transactionDone(transaction);
    return structuredClone(renewed);
  }

  async removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean> {
    validateTempId(ownerId, "Temp run owner ID");
    const cutoff = Date.parse(expiresAtCutoff);
    if (!Number.isFinite(cutoff)) throw new TypeError("Temp owner expiry cutoff must be valid");
    const transaction = this.#transaction("temp", "readwrite");
    const store = transaction.objectStore("temp");
    const value: unknown = await requestResult(store.get(tempOwnerKey(ownerId)));
    if (value !== undefined) {
      const record = asTempOwnerRecord(value);
      const expiresAt = Date.parse(record.expiresAt);
      if (Number.isFinite(expiresAt) && expiresAt > cutoff) {
        await transactionDone(transaction);
        return false;
      }
    }
    await visitObjectStoreSequentially(store, (_value, key) => {
      if (isTempRunPageKey(key, ownerId)) store.delete(key);
    });
    store.delete(tempOwnerKey(ownerId));
    await transactionDone(transaction);
    return true;
  }

  async listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    validatePageLimit(limit);
    const transaction = this.#transaction("temp", "readonly");
    const store = transaction.objectStore("temp");
    const [recordOwnerIds, pageOwnerIds] = await Promise.all([
      readTempOwnerRecordIds(store, afterOwnerId, limit + 1),
      readTempPageOwnerIds(store, afterOwnerId, limit + 1),
    ]);
    await transactionDone(transaction);
    const union = [...new Set([...recordOwnerIds, ...pageOwnerIds])].sort();
    const records = union.slice(0, limit);
    return {
      records,
      nextCursor: union.length > limit ? (records.at(-1) ?? null) : null,
    };
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
    const normalized = normalizeSegmentRecord(record);
    const transaction = this.#transaction("segments", "readwrite");
    transaction.objectStore("segments").add(normalized, normalized.id);
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
    const records: SegmentRecord[] = [];
    await visitObjectStoreSequentially(transaction.objectStore("segments"), (value) => {
      const record = asSegmentRecord(value);
      if (tableId === undefined || record.tableId === tableId) records.push(record);
    });
    await transactionDone(transaction);
    return records.sort((left, right) => left.id.localeCompare(right.id));
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

  async listManifestPage(afterVersion: number | null, limit: number) {
    validatePageLimit(limit);
    const transaction = this.#transaction("manifests", "readonly");
    const records = await readCursorPage(
      transaction.objectStore("manifests"),
      limit,
      asManifest,
      (key) => typeof key === "number" && (afterVersion === null || key > afterVersion),
    );
    await transactionDone(transaction);
    return {
      records,
      nextCursor: records.length === limit ? (records.at(-1)?.version ?? null) : null,
    };
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
    const transaction = this.#transaction(
      ["transactions", "manifests", "blocks", "segments"],
      "readwrite",
    );
    try {
      await assertSnapshotAvailableInTransaction(transaction, record.snapshotVersion);
      await assertPendingArtifactsAvailableInTransaction(transaction, record);
      transaction.objectStore("transactions").add(structuredClone(record), record.id);
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getTransaction(id: string): Promise<TransactionRecord | undefined> {
    const transaction = this.#transaction("transactions", "readonly");
    const value: unknown = await requestResult<unknown>(
      transaction.objectStore("transactions").get(id),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asTransactionRecord(value);
  }

  async getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>> {
    const transaction = this.#transaction("transactions", "readonly");
    const store = transaction.objectStore("transactions");
    const values = await Promise.all(ids.map((id) => requestResult<unknown>(store.get(id))));
    await transactionDone(transaction);
    return values.map((value) => (value === undefined ? undefined : asTransactionRecord(value)));
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

  async listTransactionPage(afterId: string | null, limit: number) {
    validatePageLimit(limit);
    const transaction = this.#transaction("transactions", "readonly");
    const records = await readCursorPage(
      transaction.objectStore("transactions"),
      limit,
      asTransactionRecord,
      (key) => typeof key === "string" && (afterId === null || key > afterId),
    );
    await transactionDone(transaction);
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  async updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    const transaction = this.#transaction(
      ["transactions", "manifests", "blocks", "segments"],
      "readwrite",
    );
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
      const updated = updateTransactionRecord(current, update);
      if (update.snapshotVersion !== undefined) {
        await assertSnapshotAvailableInTransaction(transaction, updated.snapshotVersion);
      }
      await assertPendingArtifactsAvailableInTransaction(
        transaction,
        updated,
        update.pendingBlockIds !== undefined,
        update.pendingSegmentIds !== undefined,
      );
      store.put(updated, id);
      await transactionDone(transaction);
      return structuredClone(updated);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
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
    validateLeaseExpiration(record.expiresAt);
    const transaction = this.#transaction(["leases", "manifests", "blocks"], "readwrite");
    try {
      await assertSnapshotAvailableInTransaction(transaction, record.manifestVersion);
      transaction.objectStore("leases").add(structuredClone(record), record.id);
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
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
    validateLeaseExpiration(expiresAt);
    const transaction = this.#transaction(["leases", "manifests", "blocks"], "readwrite");
    const store = transaction.objectStore("leases");
    const value: unknown = await requestResult(store.get(id));
    const record = value === undefined ? undefined : asLeaseRecord(value);
    if (record?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new LeaseConflictError(id, expectedRevision, record?.revision ?? null);
    }
    try {
      await assertSnapshotAvailableInTransaction(transaction, record.manifestVersion);
      const renewed = { ...record, expiresAt, revision: record.revision + 1 };
      store.put(renewed, id);
      await transactionDone(transaction);
      return structuredClone(renewed);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    const cutoff = Date.parse(expiresAtCutoff);
    if (!Number.isFinite(cutoff)) throw new TypeError("Lease expiry cutoff must be valid");
    const transaction = this.#transaction("leases", "readwrite");
    const store = transaction.objectStore("leases");
    const value: unknown = await requestResult(store.get(id));
    const record = value === undefined ? undefined : asLeaseRecord(value);
    if (record?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new LeaseConflictError(id, expectedRevision, record?.revision ?? null);
    }
    const expiresAt = Date.parse(record.expiresAt);
    if (!Number.isFinite(expiresAt) || expiresAt > cutoff) {
      await transactionDone(transaction);
      return false;
    }
    store.delete(id);
    await transactionDone(transaction);
    return true;
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

  async listCompactionJobPage(afterId: string | null, limit: number) {
    validatePageLimit(limit);
    const transaction = this.#transaction("gc", "readonly");
    const records = await readCursorPage(
      transaction.objectStore("gc"),
      limit,
      (value) => asCompactionJobEnvelope(value),
      (key) =>
        typeof key === "string" &&
        key.startsWith(COMPACTION_JOB_KEY_PREFIX) &&
        (afterId === null || key > compactionJobKey(afterId)),
    );
    await transactionDone(transaction);
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
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

  async createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord> {
    const record = createGarbageCollectionJobRecord(input);
    const transaction = this.#transaction(
      ["gc", "manifests", "segments", "transactions"],
      "readwrite",
    );
    const gcStore = transaction.objectStore("gc");
    const key = garbageCollectionJobKey(record.id);
    try {
      if ((await requestResult(gcStore.getKey(key))) !== undefined) {
        throw new Error(`Garbage collection job already exists: ${record.id}`);
      }
      await assertGarbageCollectionCandidateProvenanceInTransaction(transaction, record);
      gcStore.add(garbageCollectionJobEnvelope(record), key);
      await transactionDone(transaction);
      return structuredClone(record);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    const transaction = this.#transaction("gc", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("gc").get(garbageCollectionJobKey(id)),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asGarbageCollectionJobEnvelope(value);
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    const transaction = this.#transaction("gc", "readonly");
    const values: unknown[] = await requestResult(transaction.objectStore("gc").getAll());
    await transactionDone(transaction);
    return values
      .filter(isGarbageCollectionJobEnvelope)
      .map(asGarbageCollectionJobEnvelope)
      .sort(
        (left, right) =>
          left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
      );
  }

  async runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    validateGarbageCollectionStepInput(input);
    const transaction = this.#transaction(
      ["gc", "blocks", "segments", "catalog", "manifests", "transactions", "leases"],
      "readwrite",
    );
    const gcStore = transaction.objectStore("gc");
    const key = garbageCollectionJobKey(input.jobId);
    try {
      const value: unknown = await requestResult(gcStore.get(key));
      const current = value === undefined ? undefined : asGarbageCollectionJobEnvelope(value);
      if (current?.revision !== input.expectedRevision) {
        throw new GarbageCollectionJobConflictError(
          input.jobId,
          input.expectedRevision,
          current?.revision ?? null,
        );
      }
      if (current.state === "completed") {
        await transactionDone(transaction);
        return emptyGarbageCollectionStep(current);
      }

      const catalog = transaction.objectStore("catalog");
      const manifestStore = transaction.objectStore("manifests");
      const segmentStore = transaction.objectStore("segments");
      const blockStore = transaction.objectStore("blocks");
      const currentVersionValue: unknown = await requestResult(catalog.get(CURRENT_MANIFEST_KEY));
      if (
        currentVersionValue !== undefined &&
        (typeof currentVersionValue !== "number" ||
          !Number.isSafeInteger(currentVersionValue) ||
          currentVersionValue < 0)
      ) {
        throw new Error("Current manifest version is invalid");
      }
      const currentVersion = currentVersionValue ?? null;
      const leaseCutoff = Date.parse(current.leaseCutoff);
      await assertGarbageCollectionPinsAvailableInTransaction(
        transaction,
        currentVersion,
        leaseCutoff,
      );

      const prunedManifestVersions: number[] = [];
      const alreadyPrunedManifestVersions: number[] = [];
      const retainedManifestVersions: number[] = [];
      const missingManifestVersions: number[] = [];
      const reclaimedSegmentIds: string[] = [];
      const retainedSegmentIds: string[] = [];
      const missingSegmentIds: string[] = [];
      const reclaimedBlockIds: string[] = [];
      const retainedBlockIds: string[] = [];
      const missingBlockIds: string[] = [];
      let reclaimedBlockBytes = 0;
      let remaining = input.maxItems;
      let manifestIndex = current.cursor.manifestIndex;
      while (remaining > 0 && manifestIndex < current.candidateManifestVersions.length) {
        const version = current.candidateManifestVersions[manifestIndex];
        if (version === undefined) throw new Error("Garbage collection manifest cursor is invalid");
        const manifestValue: unknown = await requestResult(manifestStore.get(version));
        if (manifestValue === undefined) missingManifestVersions.push(version);
        else if (asManifest(manifestValue).prunedAt !== undefined) {
          alreadyPrunedManifestVersions.push(version);
        } else if (
          await isManifestVersionPinnedInTransaction(
            transaction,
            version,
            currentVersion,
            leaseCutoff,
          )
        )
          retainedManifestVersions.push(version);
        else {
          manifestStore.put({ ...asManifest(manifestValue), prunedAt: input.updatedAt }, version);
          prunedManifestVersions.push(version);
        }
        manifestIndex += 1;
        remaining -= 1;
      }

      const prunedManifestVersionSet = new Set(prunedManifestVersions);
      await assertRemainingManifestRecordsAvailable(transaction, prunedManifestVersionSet);
      let segmentIndex = current.cursor.segmentIndex;
      const segmentIdsToExamine =
        manifestIndex === current.candidateManifestVersions.length
          ? current.candidateSegmentIds.slice(segmentIndex, segmentIndex + remaining)
          : [];
      const blockCapacity = Math.max(0, remaining - segmentIdsToExamine.length);
      const blockIdsToExamine =
        manifestIndex === current.candidateManifestVersions.length &&
        segmentIndex + segmentIdsToExamine.length === current.candidateSegmentIds.length
          ? current.candidateBlockIds.slice(
              current.cursor.blockIndex,
              current.cursor.blockIndex + blockCapacity,
            )
          : [];
      const roots = await collectBoundedPhysicalRootsInTransaction(
        transaction,
        segmentIdsToExamine,
        blockIdsToExamine,
        prunedManifestVersionSet,
      );
      while (
        remaining > 0 &&
        manifestIndex === current.candidateManifestVersions.length &&
        segmentIndex < current.candidateSegmentIds.length
      ) {
        const id = current.candidateSegmentIds[segmentIndex];
        if (id === undefined) throw new Error("Garbage collection segment cursor is invalid");
        const segmentValue: unknown = await requestResult(segmentStore.get(id));
        if (segmentValue === undefined) missingSegmentIds.push(id);
        else if (roots.segmentIds.has(id)) retainedSegmentIds.push(id);
        else {
          segmentStore.delete(id);
          reclaimedSegmentIds.push(id);
        }
        segmentIndex += 1;
        remaining -= 1;
      }

      let blockIndex = current.cursor.blockIndex;
      while (
        remaining > 0 &&
        manifestIndex === current.candidateManifestVersions.length &&
        segmentIndex === current.candidateSegmentIds.length &&
        blockIndex < current.candidateBlockIds.length
      ) {
        const id = current.candidateBlockIds[blockIndex];
        if (id === undefined) throw new Error("Garbage collection block cursor is invalid");
        const blockValue: unknown = await requestResult(blockStore.get(id));
        if (blockValue === undefined) missingBlockIds.push(id);
        else if (roots.blockIds.has(id)) retainedBlockIds.push(id);
        else {
          const bytes = asBytes(blockValue);
          blockStore.delete(id);
          reclaimedBlockIds.push(id);
          reclaimedBlockBytes = safeStorageSum(reclaimedBlockBytes, bytes.byteLength);
        }
        blockIndex += 1;
        remaining -= 1;
      }

      const updated = advanceGarbageCollectionJobRecord(current, {
        examinedManifestCount:
          prunedManifestVersions.length +
          alreadyPrunedManifestVersions.length +
          retainedManifestVersions.length +
          missingManifestVersions.length,
        prunedManifestCount: prunedManifestVersions.length,
        alreadyPrunedManifestCount: alreadyPrunedManifestVersions.length,
        retainedManifestCount: retainedManifestVersions.length,
        missingManifestCount: missingManifestVersions.length,
        examinedSegmentCount:
          reclaimedSegmentIds.length + retainedSegmentIds.length + missingSegmentIds.length,
        reclaimedSegmentCount: reclaimedSegmentIds.length,
        retainedSegmentCount: retainedSegmentIds.length,
        missingSegmentCount: missingSegmentIds.length,
        examinedBlockCount:
          reclaimedBlockIds.length + retainedBlockIds.length + missingBlockIds.length,
        reclaimedBlockCount: reclaimedBlockIds.length,
        retainedBlockCount: retainedBlockIds.length,
        missingBlockCount: missingBlockIds.length,
        reclaimedBlockBytes,
        updatedAt: input.updatedAt,
      });
      gcStore.put(garbageCollectionJobEnvelope(updated), key);
      await transactionDone(transaction);
      return {
        job: structuredClone(updated),
        prunedManifestVersions,
        alreadyPrunedManifestVersions,
        retainedManifestVersions,
        missingManifestVersions,
        reclaimedSegmentIds,
        retainedSegmentIds,
        missingSegmentIds,
        reclaimedBlockIds,
        retainedBlockIds,
        missingBlockIds,
        reclaimedBlockBytes,
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async removeGarbageCollectionJob(id: string): Promise<void> {
    const transaction = this.#transaction("gc", "readwrite");
    transaction.objectStore("gc").delete(garbageCollectionJobKey(id));
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

function readCursorPage<T>(
  store: IDBObjectStore,
  limit: number,
  decode: (value: unknown) => T,
  acceptKey: (key: IDBValidKey) => boolean,
): Promise<T[]> {
  return new Promise((resolve, reject) => {
    const records: T[] = [];
    const request = store.openCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || records.length === limit) {
        resolve(records);
        return;
      }
      try {
        if (acceptKey(cursor.key)) records.push(decode(cursor.value));
        if (records.length === limit) resolve(records);
        else cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

function visitObjectStoreSequentially(
  store: IDBObjectStore,
  visit: (value: unknown, key: IDBValidKey) => unknown,
): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const request = store.openCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(false);
        return;
      }
      let result: unknown;
      try {
        result = visit(cursor.value, cursor.key);
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
        return;
      }
      Promise.resolve(result).then(
        (stop) => {
          if (stop === true) resolve(true);
          else cursor.continue();
        },
        (error: unknown) => reject(error instanceof Error ? error : new Error(String(error))),
      );
    };
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
  return normalizeSegmentRecord(value as SegmentRecord);
}

function asLeaseRecord(value: unknown): LeaseRecord {
  return structuredClone(value) as LeaseRecord;
}

async function assertSnapshotAvailableInTransaction(
  transaction: IDBTransaction,
  version: number | null,
): Promise<void> {
  if (version === null) return;
  const value: unknown = await requestResult(transaction.objectStore("manifests").get(version));
  if (value === undefined) throw new SnapshotManifestMissingError(version);
  const manifest = asManifest(value);
  if (manifest.prunedAt !== undefined) throw new SnapshotManifestMissingError(version);
  const blockStore = transaction.objectStore("blocks");
  for (const id of manifest.blockIds) {
    if ((await requestResult(blockStore.getKey(id))) === undefined) {
      throw new SnapshotManifestMissingError(version);
    }
  }
}

async function assertGarbageCollectionPinsAvailableInTransaction(
  transaction: IDBTransaction,
  currentVersion: number | null,
  leaseCutoff: number,
): Promise<void> {
  await assertSnapshotAvailableInTransaction(transaction, currentVersion);
  await visitObjectStoreSequentially(transaction.objectStore("transactions"), async (value) => {
    const record = asTransactionRecord(value);
    if (record.status === "active") {
      await assertSnapshotAvailableInTransaction(transaction, record.snapshotVersion);
    }
  });
  await visitObjectStoreSequentially(transaction.objectStore("leases"), async (value) => {
    const lease = asLeaseRecord(value);
    const expiresAt = Date.parse(lease.expiresAt);
    if (!Number.isFinite(expiresAt) || expiresAt > leaseCutoff) {
      await assertSnapshotAvailableInTransaction(transaction, lease.manifestVersion);
    }
  });
  await visitObjectStoreSequentially(transaction.objectStore("gc"), async (value) => {
    if (!isCompactionJobEnvelope(value)) return;
    const job = asCompactionJobEnvelope(value);
    if (isTerminalCompactionJob(job)) return;
    await assertSnapshotAvailableInTransaction(transaction, job.sourceManifestVersion);
    if (job.transactionId === null) return;
    const transactionValue: unknown = await requestResult(
      transaction.objectStore("transactions").get(job.transactionId),
    );
    if (transactionValue === undefined) return;
    const linkedTransaction = asTransactionRecord(transactionValue);
    if (linkedTransaction.status !== "committed") return;
    if (linkedTransaction.committedVersion === null) {
      throw new Error(`Committed transaction has no manifest version: ${linkedTransaction.id}`);
    }
    await assertSnapshotAvailableInTransaction(transaction, linkedTransaction.committedVersion);
  });
}

async function assertPendingArtifactsAvailableInTransaction(
  transaction: IDBTransaction,
  record: TransactionRecord,
  validateBlocks = true,
  validateSegments = true,
): Promise<void> {
  const [blockKeys, segmentKeys] = await Promise.all([
    validateBlocks
      ? Promise.all(
          record.pendingBlockIds.map((id) =>
            requestResult(transaction.objectStore("blocks").getKey(id)),
          ),
        )
      : Promise.resolve([]),
    validateSegments
      ? Promise.all(
          record.pendingSegmentIds.map((id) =>
            requestResult(transaction.objectStore("segments").getKey(id)),
          ),
        )
      : Promise.resolve([]),
  ]);
  const missingBlockIndex = blockKeys.findIndex((key) => key === undefined);
  if (missingBlockIndex >= 0) {
    throw new Error(
      `Transaction references missing pending block: ${record.pendingBlockIds[missingBlockIndex] ?? ""}`,
    );
  }
  const missingSegmentIndex = segmentKeys.findIndex((key) => key === undefined);
  if (missingSegmentIndex >= 0) {
    throw new Error(
      `Transaction references missing pending segment: ${record.pendingSegmentIds[missingSegmentIndex] ?? ""}`,
    );
  }
}

async function assertRemainingManifestRecordsAvailable(
  transaction: IDBTransaction,
  newlyPrunedVersions: ReadonlySet<number>,
): Promise<void> {
  await visitObjectStoreSequentially(transaction.objectStore("manifests"), async (value) => {
    const manifest = asManifest(value);
    if (manifest.prunedAt !== undefined || newlyPrunedVersions.has(manifest.version)) return;
    await assertSnapshotAvailableInTransaction(transaction, manifest.version);
  });
}

function compactionJobKey(id: string): string {
  return `${COMPACTION_JOB_KEY_PREFIX}${id}`;
}

function garbageCollectionJobKey(id: string): string {
  return `${GARBAGE_COLLECTION_JOB_KEY_PREFIX}${id}`;
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

function garbageCollectionJobEnvelope(
  record: GarbageCollectionJobRecord,
): GarbageCollectionJobEnvelope {
  return { kind: "garbage-collection-job", record: structuredClone(record) };
}

function isGarbageCollectionJobEnvelope(value: unknown): value is GarbageCollectionJobEnvelope {
  return (
    typeof value === "object" &&
    value !== null &&
    Reflect.get(value, "kind") === "garbage-collection-job" &&
    typeof Reflect.get(value, "record") === "object" &&
    Reflect.get(value, "record") !== null
  );
}

function asGarbageCollectionJobEnvelope(value: unknown): GarbageCollectionJobRecord {
  if (!isGarbageCollectionJobEnvelope(value)) {
    throw new Error("Stored garbage collection job is invalid");
  }
  return normalizeGarbageCollectionJobRecord(structuredClone(value.record));
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

async function assertGarbageCollectionCandidateProvenanceInTransaction(
  transaction: IDBTransaction,
  job: GarbageCollectionJobRecord,
): Promise<void> {
  const manifestStore = transaction.objectStore("manifests");
  for (const version of job.candidateManifestVersions) {
    if ((await requestResult(manifestStore.getKey(version))) === undefined) {
      throw new Error(`Garbage collection candidate manifest is missing: ${String(version)}`);
    }
  }
  const blockHasProvenance = async (id: string): Promise<boolean> => {
    for (const version of job.candidateManifestVersions) {
      const value: unknown = await requestResult(manifestStore.get(version));
      if (value !== undefined && asManifest(value).blockIds.includes(id)) return true;
    }
    const transactionProven = await visitObjectStoreSequentially(
      transaction.objectStore("transactions"),
      (value) => {
        const record = asTransactionRecord(value);
        return record.status === "aborted" && record.pendingBlockIds.includes(id);
      },
    );
    if (transactionProven) return true;
    return visitObjectStoreSequentially(transaction.objectStore("gc"), (value) => {
      if (!isCompactionJobEnvelope(value)) return false;
      const record = asCompactionJobEnvelope(value);
      return (
        isTerminalCompactionJob(record) &&
        (record.sourceBlockIds.includes(id) || record.outputBlockIds.includes(id))
      );
    });
  };
  for (const id of job.candidateBlockIds) {
    if (await blockHasProvenance(id)) continue;
    throw new Error(`Garbage collection block candidate has no persisted provenance: ${id}`);
  }
  for (const id of job.candidateSegmentIds) {
    let proven = await visitObjectStoreSequentially(
      transaction.objectStore("transactions"),
      (value) => {
        const record = asTransactionRecord(value);
        return record.status === "aborted" && record.pendingSegmentIds.includes(id);
      },
    );
    if (!proven) {
      proven = await visitObjectStoreSequentially(transaction.objectStore("gc"), (value) => {
        if (!isCompactionJobEnvelope(value)) return false;
        const record = asCompactionJobEnvelope(value);
        return (
          isTerminalCompactionJob(record) &&
          (record.sourceSegmentIds.includes(id) || record.outputSegmentId === id)
        );
      });
    }
    if (!proven) {
      const segmentValue: unknown = await requestResult(
        transaction.objectStore("segments").get(id),
      );
      if (segmentValue !== undefined) {
        const blockIds = segmentBlockIds(asSegmentRecord(segmentValue));
        proven = blockIds.length > 0;
        for (const blockId of blockIds) {
          if (!(await blockHasProvenance(blockId))) {
            proven = false;
            break;
          }
        }
      }
    }
    if (proven) continue;
    throw new Error(`Garbage collection segment candidate has no persisted provenance: ${id}`);
  }
}

async function isManifestVersionPinnedInTransaction(
  transaction: IDBTransaction,
  version: number,
  currentVersion: number | null,
  leaseCutoff: number,
): Promise<boolean> {
  if (currentVersion === version) return true;
  const transactionPinned = await visitObjectStoreSequentially(
    transaction.objectStore("transactions"),
    (value) => {
      const record = asTransactionRecord(value);
      return record.status === "active" && record.snapshotVersion === version;
    },
  );
  if (transactionPinned) return true;
  const leasePinned = await visitObjectStoreSequentially(
    transaction.objectStore("leases"),
    (value) => {
      const lease = asLeaseRecord(value);
      const expiresAt = Date.parse(lease.expiresAt);
      return (
        lease.manifestVersion === version &&
        (!Number.isFinite(expiresAt) || expiresAt > leaseCutoff)
      );
    },
  );
  if (leasePinned) return true;
  return visitObjectStoreSequentially(transaction.objectStore("gc"), async (value) => {
    if (!isCompactionJobEnvelope(value)) return false;
    const job = asCompactionJobEnvelope(value);
    if (isTerminalCompactionJob(job)) return false;
    if (job.sourceManifestVersion === version) return true;
    if (job.transactionId === null) return false;
    const transactionValue: unknown = await requestResult(
      transaction.objectStore("transactions").get(job.transactionId),
    );
    if (transactionValue === undefined) return false;
    const linkedTransaction = asTransactionRecord(transactionValue);
    if (linkedTransaction.status !== "committed") return false;
    if (linkedTransaction.committedVersion === null) {
      throw new Error(`Committed transaction has no manifest version: ${linkedTransaction.id}`);
    }
    return linkedTransaction.committedVersion === version;
  });
}

const MAX_GARBAGE_COLLECTION_ROOT_DEPENDENCIES = 4_096;

async function collectBoundedPhysicalRootsInTransaction(
  transaction: IDBTransaction,
  candidateSegmentIds: readonly string[],
  candidateBlockIds: readonly string[],
  newlyPrunedVersions: ReadonlySet<number>,
): Promise<{ blockIds: Set<string>; segmentIds: Set<string> }> {
  const candidateSegments = new Set(candidateSegmentIds);
  const candidateBlocks = new Set(candidateBlockIds);
  const probeBlockIds = new Set(candidateBlockIds);
  const probeSegmentIds = new Set(candidateSegmentIds);
  const relatedSegments = new Map<string, SegmentRecord>();
  const dependencyOverflow = await visitObjectStoreSequentially(
    transaction.objectStore("segments"),
    (value) => {
      const segment = asSegmentRecord(value);
      const ids = segmentBlockIds(segment);
      if (!candidateSegments.has(segment.id) && !ids.some((id) => candidateBlocks.has(id))) return;
      const newBlockIds = new Set(ids.filter((id) => !probeBlockIds.has(id)));
      if (
        relatedSegments.size >= MAX_GARBAGE_COLLECTION_ROOT_DEPENDENCIES ||
        probeBlockIds.size + newBlockIds.size > MAX_GARBAGE_COLLECTION_ROOT_DEPENDENCIES
      ) {
        return true;
      }
      relatedSegments.set(segment.id, segment);
      probeSegmentIds.add(segment.id);
      ids.forEach((id) => probeBlockIds.add(id));
      return false;
    },
  );
  if (dependencyOverflow) {
    return { blockIds: new Set(candidateBlockIds), segmentIds: new Set(candidateSegmentIds) };
  }

  const directBlockRoots = new Set<string>();
  const directSegmentRoots = new Set<string>();
  await visitObjectStoreSequentially(transaction.objectStore("manifests"), (value) => {
    const manifest = asManifest(value);
    if (manifest.prunedAt !== undefined || newlyPrunedVersions.has(manifest.version)) return;
    for (const id of manifest.blockIds) if (probeBlockIds.has(id)) directBlockRoots.add(id);
  });

  const ownerTransactionIds = new Set(
    [...relatedSegments.values()].map((segment) => segment.transactionId),
  );
  const activeOwnerTransactionIds = new Set<string>();
  await visitObjectStoreSequentially(transaction.objectStore("transactions"), (value) => {
    const record = asTransactionRecord(value);
    if (record.status !== "active") return;
    if (ownerTransactionIds.has(record.id)) activeOwnerTransactionIds.add(record.id);
    for (const id of record.pendingBlockIds) if (probeBlockIds.has(id)) directBlockRoots.add(id);
    for (const id of record.pendingSegmentIds)
      if (probeSegmentIds.has(id)) directSegmentRoots.add(id);
  });
  await visitObjectStoreSequentially(transaction.objectStore("gc"), (value) => {
    if (!isCompactionJobEnvelope(value)) return;
    const job = asCompactionJobEnvelope(value);
    if (isTerminalCompactionJob(job)) return;
    for (const id of job.sourceBlockIds) if (probeBlockIds.has(id)) directBlockRoots.add(id);
    for (const id of job.outputBlockIds) if (probeBlockIds.has(id)) directBlockRoots.add(id);
    for (const id of job.sourceSegmentIds) if (probeSegmentIds.has(id)) directSegmentRoots.add(id);
    if (job.outputSegmentId !== null && probeSegmentIds.has(job.outputSegmentId)) {
      directSegmentRoots.add(job.outputSegmentId);
    }
  });

  const rootedBlockIds = new Set<string>();
  const rootedSegmentIds = new Set<string>();
  for (const segment of relatedSegments.values()) {
    const ids = segmentBlockIds(segment);
    if (
      directSegmentRoots.has(segment.id) ||
      activeOwnerTransactionIds.has(segment.transactionId) ||
      (ids.length > 0 && ids.every((id) => directBlockRoots.has(id)))
    ) {
      if (candidateSegments.has(segment.id)) rootedSegmentIds.add(segment.id);
      for (const id of ids) if (candidateBlocks.has(id)) rootedBlockIds.add(id);
    }
  }
  for (const id of candidateBlockIds) if (directBlockRoots.has(id)) rootedBlockIds.add(id);
  for (const id of candidateSegmentIds) if (directSegmentRoots.has(id)) rootedSegmentIds.add(id);
  return { blockIds: rootedBlockIds, segmentIds: rootedSegmentIds };
}

function segmentBlockIds(segment: SegmentRecord): string[] {
  return [...new Set(Object.values(segment.columnBlockIds).flat())];
}

function validateGarbageCollectionStepInput(input: RunGarbageCollectionStepInput): void {
  if (input.jobId.length === 0) throw new TypeError("Garbage collection job ID cannot be empty");
  if (!Number.isSafeInteger(input.expectedRevision) || input.expectedRevision < 0) {
    throw new RangeError(
      "Garbage collection expected revision must be a non-negative whole number",
    );
  }
  if (!Number.isSafeInteger(input.maxItems) || input.maxItems <= 0) {
    throw new RangeError("Garbage collection item limit must be a positive whole number");
  }
  if (input.updatedAt.length === 0 || !Number.isFinite(Date.parse(input.updatedAt))) {
    throw new TypeError("Garbage collection update timestamp must be valid");
  }
}

function validatePageLimit(limit: number): void {
  if (!Number.isSafeInteger(limit) || limit <= 0) {
    throw new RangeError("Storage page limit must be a positive whole number");
  }
}

function validateLeaseExpiration(expiresAt: string): void {
  if (expiresAt.length === 0 || !Number.isFinite(Date.parse(expiresAt))) {
    throw new TypeError("Lease expiration must be valid");
  }
}

function emptyGarbageCollectionStep(job: GarbageCollectionJobRecord): GarbageCollectionStepResult {
  return {
    job: structuredClone(job),
    prunedManifestVersions: [],
    alreadyPrunedManifestVersions: [],
    retainedManifestVersions: [],
    missingManifestVersions: [],
    reclaimedSegmentIds: [],
    retainedSegmentIds: [],
    missingSegmentIds: [],
    reclaimedBlockIds: [],
    retainedBlockIds: [],
    missingBlockIds: [],
    reclaimedBlockBytes: 0,
  };
}

function safeStorageSum(left: number, right: number): number {
  const total = left + right;
  if (!Number.isSafeInteger(total)) {
    throw new RangeError("Garbage collection reclaimed block bytes exceed the safe range");
  }
  return total;
}

function abortIfActive(transaction: IDBTransaction): void {
  try {
    transaction.abort();
  } catch {
    // An already completed or aborted transaction needs no further action.
  }
}

function validateCount(count: number): void {
  if (!Number.isSafeInteger(count) || count <= 0) {
    throw new RangeError("Row ID reservation count must be a positive whole number");
  }
}

function validateTempRunPage(page: TempRunPage): void {
  validateTempRunPageIdentity(page.ownerId, page.runId, page.pageIndex);
  if (!(page.bytes instanceof Uint8Array)) throw new TypeError("Temp run page bytes are invalid");
}

function validateTempRunPageIdentity(ownerId: string, runId: string, pageIndex: number): void {
  validateTempId(ownerId, "Temp run owner ID");
  validateTempId(runId, "Temp run ID");
  if (!Number.isSafeInteger(pageIndex) || pageIndex < 0) {
    throw new RangeError("Temp run page index must be a non-negative whole number");
  }
}

function validateTempId(id: string, label: string): void {
  if (id.length === 0) throw new TypeError(`${label} cannot be empty`);
}

function tempRunPageKey(page: TempRunPage): IDBValidKey {
  return ["run", page.ownerId, page.runId, page.pageIndex];
}

function isTempRunPageKey(key: IDBValidKey, ownerId: string, runId?: string): boolean {
  return (
    Array.isArray(key) &&
    key[0] === "run" &&
    key[1] === ownerId &&
    (runId === undefined || key[2] === runId)
  );
}

function tempOwnerKey(ownerId: string): IDBValidKey {
  return ["owner", ownerId];
}

function validateTempOwnerRecord(record: TempOwnerRecord): void {
  validateTempId(record.ownerId, "Temp run owner ID");
  validateLeaseExpiration(record.expiresAt);
  if (record.revision !== 0) {
    throw new RangeError("Temp owner record must be created at revision zero");
  }
}

function asTempOwnerRecord(value: unknown): TempOwnerRecord {
  return structuredClone(value) as TempOwnerRecord;
}

// Both scans run without IDBKeyRange (absent from the injected test factory environment): owner
// keys ["owner", id] sort before page keys ["run", id, runId, index], and an empty array sorts
// after every string, so ["run", id, []] jumps past one owner's remaining pages.
function readTempOwnerRecordIds(
  store: IDBObjectStore,
  afterOwnerId: string | null,
  max: number,
): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const ownerIds: string[] = [];
    const request = store.openKeyCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || ownerIds.length >= max) {
        resolve(ownerIds);
        return;
      }
      const key = cursor.key;
      if (!Array.isArray(key) || key[0] !== "owner" || typeof key[1] !== "string") {
        resolve(ownerIds);
        return;
      }
      if (afterOwnerId === null || key[1] > afterOwnerId) ownerIds.push(key[1]);
      if (ownerIds.length >= max) resolve(ownerIds);
      else cursor.continue();
    };
  });
}

function readTempPageOwnerIds(
  store: IDBObjectStore,
  afterOwnerId: string | null,
  max: number,
): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const ownerIds: string[] = [];
    const request = store.openKeyCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || ownerIds.length >= max) {
        resolve(ownerIds);
        return;
      }
      const key = cursor.key;
      if (Array.isArray(key) && key[0] === "owner") {
        cursor.continue(["run"]);
        return;
      }
      if (!Array.isArray(key) || key[0] !== "run" || typeof key[1] !== "string") {
        cursor.continue();
        return;
      }
      if (afterOwnerId !== null && key[1] <= afterOwnerId) {
        cursor.continue(["run", afterOwnerId, []]);
        return;
      }
      ownerIds.push(key[1]);
      if (ownerIds.length >= max) {
        resolve(ownerIds);
        return;
      }
      cursor.continue(["run", key[1], []]);
    };
  });
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
