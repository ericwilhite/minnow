import {
  type BeginTransactionInput,
  type BeginTransactionResult,
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
  type CatalogProbe,
  type TriggerRecord,
  type UniqueKeyChanges,
  collectFtsCandidates,
  invalidateUncoveredFtsColumns,
  type FtsCandidates,
  type FtsColumnIndexRecord,
  type FtsPosting,
  type GarbageCollectionJobRecord,
  GarbageCollectionJobConflictError,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  LeaseConflictError,
  MANIFEST_CHECKPOINT_INTERVAL,
  type Manifest,
  type ManifestSummary,
  type StoredManifestRecord,
  applyManifestRecord,
  type PublishManifestInput,
  type QueryCatalogState,
  type RowIdRange,
  type RunGarbageCollectionStepInput,
  type SegmentRecord,
  SnapshotManifestMissingError,
  type StageTransactionArtifactsInput,
  type StoragePage,
  type TableColumnRecord,
  type TableRecord,
  TableRecordConflictError,
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
  type WriteTransactionInput,
} from "./types.js";
import {
  selectLiveRecords,
  type DatabaseSnapshot,
  type SnapshotFtsIndex,
  type SnapshotLoadProgress,
  type SnapshotTable,
} from "./snapshot.js";

const SCHEMA_VERSION = 2;
const CURRENT_MANIFEST_KEY = "manifest/current";
const CATALOG_EPOCH_KEY = "catalog/epoch";
const SEGMENT_TABLE_INDEX = "byTable";
const TABLE_ID_PREFIX = "table/id/";
const TABLE_NAME_PREFIX = "table/name/";
const ROW_ID_PREFIX = "row-id/";
const AUTO_INCREMENT_PREFIX = "auto-increment/";
const UNIQUE_KEY_CHUNK_INDEX = "unique-key-chunk-index";
const UNIQUE_KEY_CHUNK = "unique-key-chunk";
/**
 * Tail length at which a commit folds the chunk tail into the partitioned base. Appending one
 * chunk per commit keeps commits O(1), but every lookup must deserialize the whole tail; the
 * fold bounds that at this many chunk records plus point reads against the base partitions,
 * instead of a scan whose cost grows with every key ever written.
 */
const UNIQUE_KEY_TAIL_CHUNK_LIMIT = 16;
const UNIQUE_KEY_BASE_PART = "unique-key-base-part";
/**
 * Tokens per folded base partition. Small enough that a keyed point mutation deserializes
 * one partition in about a millisecond; large enough that a multi-million-key table folds into
 * hundreds of records instead of millions.
 */
const UNIQUE_KEY_PARTITION_TARGET = 16_384;
const FTS_BASE_INDEX_PREFIX = "fts-base-index/";
const FTS_BASE_PREFIX = "fts-base/";
const FTS_CHUNK_PREFIX = "fts-chunk/";
const COMPACTION_JOB_KEY_PREFIX = "compaction-job/";
const GARBAGE_COLLECTION_JOB_KEY_PREFIX = "garbage-collection-job/";
const storageTextEncoder = new TextEncoder();
const storageTextBuffer = new Uint8Array(1_024);
/** Blocks per read when copying a database out; keeps one export request list bounded. */
const SNAPSHOT_BLOCK_BATCH = 512;
/** Bytes per write transaction when loading a snapshot in. */
const SNAPSHOT_BATCH_BYTES = 8 * 1024 * 1024;

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
  /** Remembered name-to-id mappings; see getTableByName for why they need no invalidation. */
  readonly #tableIdsByName = new Map<string, string>();
  /**
   * Memoized unique-key state for the most recently written keyed table, mirroring
   * #manifestCache's validity rule: usable only while the next commit's expected manifest
   * version matches, so any commit from another instance or tab invalidates it. `present` is
   * the complete membership (folded base plus tail replay) and `chunks` are the unfolded tail
   * in commit order, so a valid cache lets a commit skip every unique-key read and lets a
   * fold rewrite the base without re-reading it. Bulk loads hit this on every batch.
   */
  #uniqueKeyCache:
    | {
        version: number;
        tableId: string;
        present: Set<string>;
        chunks: UniqueKeyChunk[];
        index: UniqueKeyChunkIndex;
      }
    | undefined;
  /**
   * The most recently resolved manifest block set, advanced in place as this instance commits.
   * Purely a memoization of immutable records: a miss (fresh instance, or another tab moved the
   * version) re-resolves from storage inside the committing transaction.
   */
  #manifestCache: { version: number; blockIds: Set<string> } | undefined;

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
      // Version 2: segments become range-readable by table. createIndex back-fills from
      // existing records, so version-1 databases upgrade in place with no data rewrite.
      const upgrade = request.transaction;
      if (upgrade !== null) {
        const segments = upgrade.objectStore("segments");
        if (!segments.indexNames.contains(SEGMENT_TABLE_INDEX)) {
          segments.createIndex(SEGMENT_TABLE_INDEX, "tableId");
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
    for (const block of blocks) {
      // Structured clone serializes a view's entire underlying buffer, so only a partial view
      // needs compacting first; a whole-buffer view clones exactly once inside add().
      const bytes =
        block.bytes.byteOffset === 0 && block.bytes.byteLength === block.bytes.buffer.byteLength
          ? block.bytes
          : block.bytes.slice();
      store.add(bytes, block.id);
    }
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

  async putTempRunPages(pages: readonly TempRunPage[]): Promise<void> {
    for (const page of pages) validateTempRunPage(page);
    if (pages.length === 0) return;
    // One transaction for the whole batch — the per-page path pays one commit each.
    const transaction = this.#transaction("temp", "readwrite");
    const store = transaction.objectStore("temp");
    for (const page of pages) store.put(page.bytes.slice(), tempRunPageKey(page));
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
    await bumpCatalogEpoch(store);
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

  async updateTable(
    id: string,
    expectedRevision: number,
    update: {
      columns?: TableColumnRecord[];
      ftsColumns?: Record<string, FtsColumnIndexRecord> | null;
      triggers?: TriggerRecord[] | null;
    },
  ): Promise<TableRecord> {
    if (update.columns !== undefined) validateTableColumns(update.columns);
    const transaction = this.#transaction("catalog", "readwrite");
    const store = transaction.objectStore("catalog");
    const idKey = `${TABLE_ID_PREFIX}${id}`;
    const value: unknown = await requestResult(store.get(idKey));
    const record = value === undefined ? undefined : (structuredClone(value) as TableRecord);
    const actualRevision = record === undefined ? null : (record.revision ?? 0);
    if (record === undefined || actualRevision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new TableRecordConflictError(id, expectedRevision, actualRevision);
    }
    const { ftsColumns: previousFts, triggers: previousTriggers, ...base } = record;
    const nextFts = update.ftsColumns === undefined ? previousFts : update.ftsColumns;
    const nextTriggers = update.triggers === undefined ? previousTriggers : update.triggers;
    const updated: TableRecord = {
      ...base,
      columns: update.columns === undefined ? record.columns : structuredClone(update.columns),
      ...(nextFts === null || nextFts === undefined
        ? {}
        : { ftsColumns: structuredClone(nextFts) }),
      ...(nextTriggers === null || nextTriggers === undefined
        ? {}
        : { triggers: structuredClone(nextTriggers) }),
      revision: expectedRevision + 1,
    };
    store.put(structuredClone(updated), idKey);
    await bumpCatalogEpoch(store);
    await transactionDone(transaction);
    return updated;
  }

  async removeTable(id: string, expectedRevision: number): Promise<void> {
    const transaction = this.#transaction(["catalog", "segments"], "readwrite");
    const catalog = transaction.objectStore("catalog");
    const value: unknown = await requestResult(catalog.get(`${TABLE_ID_PREFIX}${id}`));
    const record = value === undefined ? undefined : (structuredClone(value) as TableRecord);
    const actualRevision = record === undefined ? null : (record.revision ?? 0);
    if (record === undefined || actualRevision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new TableRecordConflictError(id, expectedRevision, actualRevision);
    }
    catalog.delete(`${TABLE_ID_PREFIX}${id}`);
    catalog.delete(`${TABLE_NAME_PREFIX}${record.name}`);
    catalog.delete(`${ROW_ID_PREFIX}${id}`);
    // Everything else this table owns is keyed by its id under one of a handful of prefixes,
    // string-keyed or array-keyed. One sequential pass collects them all: a dropped table is
    // rare and the alternative is a range read per prefix, several of which the injected test
    // factory cannot express.
    const ownedStringPrefixes = [
      `${AUTO_INCREMENT_PREFIX}${id}/`,
      `${FTS_BASE_INDEX_PREFIX}${id}/`,
      `${FTS_BASE_PREFIX}${id}/`,
      `${FTS_CHUNK_PREFIX}${id}/`,
    ];
    const ownedArrayKinds = new Set([
      UNIQUE_KEY_CHUNK_INDEX,
      UNIQUE_KEY_CHUNK,
      UNIQUE_KEY_BASE_PART,
    ]);
    const doomed: IDBValidKey[] = [];
    await visitObjectStoreSequentially(catalog, (_value, key) => {
      if (typeof key === "string") {
        if (ownedStringPrefixes.some((prefix) => key.startsWith(prefix))) doomed.push(key);
        return;
      }
      if (!Array.isArray(key)) return;
      const [kind, owner] = key as unknown[];
      if (typeof kind === "string" && ownedArrayKinds.has(kind) && owner === id) doomed.push(key);
    });
    for (const key of doomed) catalog.delete(key);
    const segments = transaction.objectStore("segments");
    const segmentKeys = await requestResult<IDBValidKey[]>(
      segments.index(SEGMENT_TABLE_INDEX).getAllKeys(id),
    );
    for (const key of segmentKeys) segments.delete(key);
    await bumpCatalogEpoch(catalog);
    await transactionDone(transaction);
    // The membership cache is keyed by manifest version, which a table drop does not move;
    // without this, a lookup after the drop would answer from the dead table's keys.
    if (this.#uniqueKeyCache?.tableId === id) this.#uniqueKeyCache = undefined;
  }

  async writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): Promise<void> {
    const transaction = this.#transaction("catalog", "readwrite");
    const store = transaction.objectStore("catalog");
    const tocKey = `${FTS_BASE_INDEX_PREFIX}${tableId}/${columnId}`;
    const previous = (await requestResult(store.get(tocKey))) as FtsBaseToc | undefined;
    const chunkPrefix = `${FTS_BASE_PREFIX}${tableId}/${columnId}/`;
    const previousCount = previous?.boundaries.length ?? 0;
    for (let ordinal = input.chunks.length; ordinal < previousCount; ordinal += 1) {
      store.delete(`${chunkPrefix}${String(ordinal).padStart(6, "0")}`);
    }
    const boundaries: Array<{ first: string; last: string }> = [];
    input.chunks.forEach((chunk, ordinal) => {
      boundaries.push({
        first: chunk[0]?.term ?? "",
        last: chunk[chunk.length - 1]?.term ?? "",
      });
      store.put(structuredClone(chunk), `${chunkPrefix}${String(ordinal).padStart(6, "0")}`);
    });
    store.put(
      { coversVersion: input.coversVersion, boundaries, totalTokens: input.totalTokens },
      tocKey,
    );
    // Commit deltas the base now covers are dead; drop their chunks and shrink the version list
    // (mirroring the unique-key chunk index — no key-range scans in this environment).
    const deltaIndexKey = ftsChunkIndexKey(tableId, columnId);
    const deltaIndex = (await requestResult(store.get(deltaIndexKey))) as
      { versions: number[] } | undefined;
    const surviving: number[] = [];
    for (const version of deltaIndex?.versions ?? []) {
      if (version <= input.coversVersion) {
        store.delete(ftsChunkKey(tableId, columnId, version));
      } else {
        surviving.push(version);
      }
    }
    store.put({ versions: surviving }, deltaIndexKey);
    await transactionDone(transaction);
  }

  async readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: ReadonlyArray<{ term: string; prefix: boolean }>,
    upToVersion: number,
  ): Promise<
    FtsCandidates & { deltaChunkCount: number; totalTokens: number; coversVersion: number }
  > {
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const [toc, deltaIndex] = (await Promise.all([
      requestResult(store.get(`${FTS_BASE_INDEX_PREFIX}${tableId}/${columnId}`)),
      requestResult(store.get(ftsChunkIndexKey(tableId, columnId))),
    ])) as [FtsBaseToc | undefined, { versions: number[] } | undefined];
    const chunkPrefix = `${FTS_BASE_PREFIX}${tableId}/${columnId}/`;
    const wantedOrdinals = (toc?.boundaries ?? []).flatMap((boundary, ordinal) =>
      terms.some((term) => {
        const upper = term.prefix ? `${term.term}￿` : term.term;
        return term.term <= boundary.last && upper >= boundary.first;
      })
        ? [ordinal]
        : [],
    );
    const coversVersion = toc?.coversVersion ?? -1;
    const wantedVersions = (deltaIndex?.versions ?? []).filter(
      (version) => version > coversVersion && version <= upToVersion,
    );
    // One readonly transaction pipelines all chunk reads concurrently instead of paying an
    // event-loop round trip per chunk.
    const [baseChunks, deltaChunks] = await Promise.all([
      Promise.all(
        wantedOrdinals.map(
          (ordinal) =>
            requestResult(
              store.get(`${chunkPrefix}${String(ordinal).padStart(6, "0")}`),
            ) as Promise<FtsPosting[] | undefined>,
        ),
      ),
      Promise.all(
        wantedVersions.map(
          (version) =>
            requestResult(store.get(ftsChunkKey(tableId, columnId, version))) as Promise<
              FtsDeltaChunk | undefined
            >,
        ),
      ),
    ]);
    await transactionDone(transaction);
    const present = deltaChunks.filter((chunk) => chunk !== undefined);
    const chunkLists = [
      ...baseChunks.filter((chunk) => chunk !== undefined),
      ...present.map((chunk) => chunk.postings),
    ];
    const totalTokens =
      (toc?.totalTokens ?? 0) + present.reduce((total, chunk) => total + chunk.totalTokens, 0);
    return {
      ...collectFtsCandidates(chunkLists, terms),
      deltaChunkCount: present.length,
      totalTokens,
      coversVersion,
    };
  }

  /**
   * Resolving a name means two reads — name to id, then id to record — and the second cannot
   * start until the first lands, so the write path pays two serialized round trips for one
   * lookup. A remembered id collapses that to one read, and needs no invalidation: the record
   * carries its own name, so a mapping that has gone stale fails the check below and falls
   * back to the full resolution. Table ids are unique per creation, so a dropped and recreated
   * table cannot be mistaken for its predecessor.
   */
  async getTableByName(name: string): Promise<TableRecord | undefined> {
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const rememberedId = this.#tableIdsByName.get(name);
    if (rememberedId !== undefined) {
      const cached: unknown = await requestResult(store.get(`${TABLE_ID_PREFIX}${rememberedId}`));
      if (cached !== undefined) {
        const record = asTableRecord(cached);
        if (record.name === name) {
          await transactionDone(transaction);
          return record;
        }
      }
      this.#tableIdsByName.delete(name);
    }
    const id = (await requestResult(store.get(`${TABLE_NAME_PREFIX}${name}`))) as
      string | undefined;
    const value: unknown =
      id === undefined ? undefined : await requestResult(store.get(`${TABLE_ID_PREFIX}${id}`));
    await transactionDone(transaction);
    if (value === undefined) return undefined;
    const record = asTableRecord(value);
    if (record.name === name) this.#tableIdsByName.set(name, record.id);
    return record;
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
    if (tableId !== undefined) {
      // Index range read: cost scales with this table's segments, not the store's total.
      const values = await requestResult<unknown[]>(
        transaction.objectStore("segments").index(SEGMENT_TABLE_INDEX).getAll(tableId),
      );
      await transactionDone(transaction);
      return values
        .map((value) => asSegmentRecord(value))
        .sort((left, right) => left.id.localeCompare(right.id));
    }
    const records: SegmentRecord[] = [];
    await visitObjectStoreSequentially(transaction.objectStore("segments"), (value) => {
      records.push(asSegmentRecord(value));
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

  async reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): Promise<RowIdRange> {
    validateAutoIncrementReservation(count, atLeast);
    const transaction = this.#transaction("catalog", "readwrite");
    const store = transaction.objectStore("catalog");
    const key = `${AUTO_INCREMENT_PREFIX}${tableId}/${columnId}`;
    const current = (await requestResult(store.get(key))) as bigint | undefined;
    const start = maxBigInt(current ?? 1n, atLeast ?? 1n);
    const endExclusive = start + BigInt(count);
    store.put(endExclusive, key);
    await transactionDone(transaction);
    return { start, endExclusive };
  }

  async getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    const tokens = [...new Set(keyTokens)];
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const versionValue: unknown = await requestResult(store.get(CURRENT_MANIFEST_KEY));
    const cache = this.#uniqueKeyCache;
    if (cache?.tableId === tableId && cache.version === versionValue) {
      await transactionDone(transaction);
      return tokens.filter((token) => cache.present.has(token)).sort();
    }
    const { existing } = await readChunkedUniqueKeys(store, tableId, tokens);
    await transactionDone(transaction);
    return [...existing].sort();
  }

  async getCurrentManifestVersion(): Promise<number | null> {
    const transaction = this.#transaction("catalog", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
    );
    await transactionDone(transaction);
    return typeof value === "number" ? value : null;
  }

  /**
   * The freshness probe runs once per query, so its latency is a floor under every read the
   * database serves. Both values are final the moment their reads land: a read-only
   * transaction has nothing left to commit, and waiting for its `complete` event only costs
   * another turn of the event loop — measured at roughly 40% of the probe in Chromium — while
   * changing nothing about what was read. The reads still race the transaction's failure
   * events, so an abort surfaces as a rejection here instead of hanging.
   */
  async getCatalogProbe(): Promise<CatalogProbe> {
    const transaction = this.#transaction("catalog", "readonly");
    const catalog = transaction.objectStore("catalog");
    const [versionValue, epochValue] = await Promise.race([
      Promise.all([
        requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
        requestResult<unknown>(catalog.get(CATALOG_EPOCH_KEY)),
      ]),
      transactionFailure(transaction),
    ]);
    return {
      manifestVersion: typeof versionValue === "number" ? versionValue : null,
      catalogEpoch: typeof epochValue === "number" ? epochValue : 0,
    };
  }

  async getQueryCatalogState(tableNames: readonly string[]): Promise<QueryCatalogState> {
    const transaction = this.#transaction(["catalog", "segments", "transactions"], "readonly");
    const catalog = transaction.objectStore("catalog");
    const [versionValue, epochValue, tableIds] = await Promise.all([
      requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
      requestResult<unknown>(catalog.get(CATALOG_EPOCH_KEY)),
      Promise.all(
        tableNames.map((name) =>
          requestResult<unknown>(catalog.get(`${TABLE_NAME_PREFIX}${name}`)),
        ),
      ),
    ]);
    const tables = await Promise.all(
      tableIds.map(async (id) => {
        if (typeof id !== "string") return undefined;
        const value: unknown = await requestResult(catalog.get(`${TABLE_ID_PREFIX}${id}`));
        return value === undefined ? undefined : asTableRecord(value);
      }),
    );
    const foundTableIds = [
      ...new Set(
        tables
          .filter((table): table is TableRecord => table !== undefined)
          .map((table) => table.id),
      ),
    ];
    // Range-read each found table's segments through the byTable index: cost scales with the
    // queried tables' segments, not the store's total segment count.
    const segmentIndex = transaction.objectStore("segments").index(SEGMENT_TABLE_INDEX);
    const segmentValues = await Promise.all(
      foundTableIds.map((tableId) => requestResult<unknown[]>(segmentIndex.getAll(tableId))),
    );
    const segments = segmentValues.flat().map((value) => asSegmentRecord(value));
    segments.sort((left, right) => left.id.localeCompare(right.id));
    const transactionIds = [...new Set(segments.map((segment) => segment.transactionId))];
    const transactionStore = transaction.objectStore("transactions");
    const transactionValues = await Promise.all(
      transactionIds.map((id) => requestResult<unknown>(transactionStore.get(id))),
    );
    await transactionDone(transaction);
    return {
      manifestVersion: typeof versionValue === "number" ? versionValue : null,
      tables,
      segments,
      transactions: transactionValues
        .filter((value) => value !== undefined)
        .map((value) => asTransactionRecord(value)),
      catalogEpoch: typeof epochValue === "number" ? epochValue : 0,
    };
  }

  async getCurrentManifest(): Promise<Manifest | undefined> {
    const transaction = this.#transaction(["catalog", "manifests"], "readonly");
    const manifestStore = transaction.objectStore("manifests");
    const version = (await requestResult(
      transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
    )) as number | undefined;
    const value: unknown =
      version === undefined ? undefined : await requestResult(manifestStore.get(version));
    const manifest =
      value === undefined
        ? undefined
        : await resolveManifestInTransaction(manifestStore, asStoredManifestRecord(value));
    await transactionDone(transaction);
    if (version !== undefined && manifest === undefined)
      throw new Error("Current manifest is missing");
    return manifest;
  }

  async getManifest(version: number): Promise<Manifest | undefined> {
    const transaction = this.#transaction("manifests", "readonly");
    const manifestStore = transaction.objectStore("manifests");
    const value: unknown = await requestResult<unknown>(manifestStore.get(version));
    const manifest =
      value === undefined
        ? undefined
        : await resolveManifestInTransaction(manifestStore, asStoredManifestRecord(value));
    await transactionDone(transaction);
    return manifest;
  }

  async listManifests(): Promise<Manifest[]> {
    const transaction = this.#transaction("manifests", "readonly");
    const values: unknown[] = await requestResult<unknown[]>(
      transaction.objectStore("manifests").getAll(),
    );
    await transactionDone(transaction);
    const records = values
      .map(asStoredManifestRecord)
      .sort((left, right) => left.version - right.version);
    // Versions are dense in ascending key order, so one running set resolves every record.
    const blockIds = new Set<string>();
    return records.map((record) => {
      applyManifestRecord(blockIds, record);
      return manifestView(record, blockIds);
    });
  }

  async listManifestPage(afterVersion: number | null, limit: number) {
    validatePageLimit(limit);
    const transaction = this.#transaction("manifests", "readonly");
    const manifestStore = transaction.objectStore("manifests");
    const stored = await readCursorPage(
      manifestStore,
      limit,
      asStoredManifestRecord,
      (key) => typeof key === "number" && (afterVersion === null || key > afterVersion),
    );
    // Resolve the first record by chain walk, then advance the same set across the page.
    const records: Manifest[] = [];
    let blockIds: Set<string> | undefined;
    let resolvedVersion = Number.NaN;
    for (const record of stored) {
      if (blockIds === undefined || record.previousVersion !== resolvedVersion) {
        blockIds = await resolveManifestBlockSetInTransaction(manifestStore, record);
      } else {
        applyManifestRecord(blockIds, record);
      }
      resolvedVersion = record.version;
      records.push(manifestView(record, blockIds));
    }
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
    // A published manifest carries its complete list, so it stores as a checkpoint and any
    // delta chain above it starts fresh.
    const manifest = createManifest(input);
    transaction.objectStore("manifests").add(manifest, manifest.version);
    catalog.put(manifest.version, CURRENT_MANIFEST_KEY);
    await bumpCatalogEpoch(catalog);
    await transactionDone(transaction);
    this.#manifestCache = { version: manifest.version, blockIds: new Set(manifest.blockIds) };
    return manifest;
  }

  async beginTransaction(input: BeginTransactionInput): Promise<BeginTransactionResult> {
    if (input.record.pendingBlockIds.length > 0 || input.record.pendingSegmentIds.length > 0) {
      throw new TypeError("A fresh transaction cannot begin with pending artifacts");
    }
    const transaction = this.#transaction(["transactions", "manifests", "catalog"], "readwrite");
    try {
      const catalog = transaction.objectStore("catalog");
      const current = (await requestResult(catalog.get(CURRENT_MANIFEST_KEY))) as
        number | undefined;
      const record: TransactionRecord = {
        ...structuredClone(input.record),
        snapshotVersion: current ?? null,
      };
      await assertSnapshotAvailableInTransaction(transaction, record.snapshotVersion);
      transaction.objectStore("transactions").add(record, record.id);
      let rowIds: RowIdRange | undefined;
      if (input.reserveRowIds !== undefined) {
        validateCount(input.reserveRowIds.count);
        const key = `${ROW_ID_PREFIX}${input.reserveRowIds.tableId}`;
        const currentRowId = (await requestResult(catalog.get(key))) as bigint | undefined;
        const start = currentRowId ?? 1n;
        const endExclusive = start + BigInt(input.reserveRowIds.count);
        catalog.put(endExclusive, key);
        rowIds = { start, endExclusive };
      }
      let autoIncrementValues: RowIdRange | undefined;
      if (input.reserveAutoIncrement !== undefined) {
        const { tableId, columnId, count, atLeast } = input.reserveAutoIncrement;
        validateAutoIncrementReservation(count, atLeast);
        const key = `${AUTO_INCREMENT_PREFIX}${tableId}/${columnId}`;
        const current = (await requestResult(catalog.get(key))) as bigint | undefined;
        const start = maxBigInt(current ?? 1n, atLeast ?? 1n);
        const endExclusive = start + BigInt(count);
        catalog.put(endExclusive, key);
        autoIncrementValues = { start, endExclusive };
      }
      await transactionDone(transaction);
      return {
        record: structuredClone(record),
        ...(rowIds === undefined ? {} : { rowIds }),
        ...(autoIncrementValues === undefined ? {} : { autoIncrementValues }),
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
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

  async stageTransactionArtifacts(
    input: StageTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    const ids = new Set<string>();
    for (const block of input.blocks) {
      if (block.id.length === 0) throw new TypeError("Block ID cannot be empty");
      if (ids.has(block.id)) throw new Error(`Block already exists: ${block.id}`);
      ids.add(block.id);
    }
    const transaction = this.#transaction(["blocks", "segments", "transactions"], "readwrite");
    try {
      const transactionStore = transaction.objectStore("transactions");
      const value: unknown = await requestResult<unknown>(
        transactionStore.get(input.transactionId),
      );
      const current = value === undefined ? undefined : asTransactionRecord(value);
      if (current?.revision !== input.expectedRevision) {
        transaction.abort();
        await ignoreAbort(transaction);
        throw new TransactionRecordConflictError(
          input.transactionId,
          input.expectedRevision,
          current?.revision ?? null,
        );
      }
      const update: TransactionRecordUpdate = {
        pendingBlockIds: [...current.pendingBlockIds, ...input.blocks.map((block) => block.id)],
        pendingSegmentIds: [
          ...current.pendingSegmentIds,
          ...input.segments.map((segment) => segment.id),
        ],
        updatedAt: input.updatedAt,
      };
      assertGenericTransactionUpdateAllowed(current, update);
      const updated = updateTransactionRecord(current, update);
      // Only previously journaled artifacts need existence probes: the ones added below commit
      // or fail atomically with the journal update itself.
      await assertPendingArtifactsAvailableInTransaction(transaction, current);
      const blockStore = transaction.objectStore("blocks");
      for (const block of input.blocks) {
        const bytes =
          block.bytes.byteOffset === 0 && block.bytes.byteLength === block.bytes.buffer.byteLength
            ? block.bytes
            : block.bytes.slice();
        blockStore.add(bytes, block.id);
      }
      const segmentStore = transaction.objectStore("segments");
      for (const segment of input.segments) {
        const normalized = normalizeSegmentRecord(segment);
        segmentStore.add(normalized, normalized.id);
      }
      transactionStore.put(updated, input.transactionId);
      await transactionDone(transaction);
      return structuredClone(updated);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary> {
    const transaction = this.#transaction(
      ["blocks", "catalog", "manifests", "transactions", "segments"],
      "readwrite",
    );
    try {
      const record = await readActiveTransactionRecord(
        transaction,
        input.transactionId,
        input.expectedTransactionRevision,
      );
      const outcome = await this.#commitInTransaction(transaction, record, input);
      await transactionDone(transaction);
      outcome.settle();
      return outcome.manifest;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async writeTransaction(input: WriteTransactionInput): Promise<ManifestSummary> {
    const blockIds = new Set<string>();
    for (const block of input.blocks) {
      if (block.id.length === 0) throw new TypeError("Block ID cannot be empty");
      if (blockIds.has(block.id)) throw new Error(`Block already exists: ${block.id}`);
      blockIds.add(block.id);
    }
    if (
      "record" in input.transaction &&
      (input.transaction.record.pendingBlockIds.length > 0 ||
        input.transaction.record.pendingSegmentIds.length > 0)
    ) {
      throw new TypeError("A fresh transaction cannot begin with pending artifacts");
    }
    const transaction = this.#transaction(
      ["blocks", "catalog", "manifests", "transactions", "segments"],
      "readwrite",
    );
    try {
      const transactionStore = transaction.objectStore("transactions");
      let base: TransactionRecord;
      if ("record" in input.transaction) {
        const id = input.transaction.record.id;
        if ((await requestResult(transactionStore.getKey(id))) !== undefined) {
          throw new Error(`Transaction already exists: ${id}`);
        }
        // The fresh record pins the version the caller prepared against, so the commit below
        // is the same compare-and-swap a begun transaction would make; a moved manifest is a
        // WriteConflictError with nothing written, record included.
        const current = (await requestResult(
          transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
        )) as number | undefined;
        const actualVersion = current ?? null;
        if (actualVersion !== input.expectedManifestVersion) {
          throw new WriteConflictError(input.expectedManifestVersion, actualVersion);
        }
        base = { ...structuredClone(input.transaction.record), snapshotVersion: actualVersion };
      } else {
        base = await readActiveTransactionRecord(
          transaction,
          input.transaction.id,
          input.transaction.expectedRevision,
        );
      }
      const staged = updateTransactionRecord(base, {
        pendingBlockIds: [...base.pendingBlockIds, ...blockIds],
        pendingSegmentIds: [
          ...base.pendingSegmentIds,
          ...input.segments.map((segment) => segment.id),
        ],
        updatedAt: input.committedAt,
      });
      // Only previously journaled artifacts need existence probes; the ones added below commit
      // or fail atomically with everything else.
      await assertPendingArtifactsAvailableInTransaction(transaction, base);
      const blockStore = transaction.objectStore("blocks");
      for (const block of input.blocks) {
        const bytes =
          block.bytes.byteOffset === 0 && block.bytes.byteLength === block.bytes.buffer.byteLength
            ? block.bytes
            : block.bytes.slice();
        blockStore.add(bytes, block.id);
      }
      const segmentStore = transaction.objectStore("segments");
      for (const segment of input.segments) {
        const normalized = normalizeSegmentRecord(segment);
        segmentStore.add(normalized, normalized.id);
      }
      const outcome = await this.#commitInTransaction(transaction, staged, input);
      await transactionDone(transaction);
      outcome.settle();
      return outcome.manifest;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  /**
   * The commit proper, inside an open readwrite transaction over every commit store, against a
   * record the caller already resolved as active at the expected revision — the journaled
   * record for `commitTransaction`, the freshly staged one for `writeTransaction`. Throws to
   * refuse (the caller aborts the storage transaction); returns the summary plus the cache
   * advances to apply once the transaction has proven durable.
   */
  async #commitInTransaction(
    transaction: IDBTransaction,
    record: TransactionRecord,
    input: Omit<CommitTransactionInput, "transactionId" | "expectedTransactionRevision">,
  ): Promise<{ manifest: ManifestSummary; settle: () => void }> {
    const transactionStore = transaction.objectStore("transactions");
    const catalog = transaction.objectStore("catalog");
    const current = (await requestResult(catalog.get(CURRENT_MANIFEST_KEY))) as number | undefined;
    const actualVersion = current ?? null;
    if (actualVersion !== input.expectedManifestVersion) {
      throw new WriteConflictError(input.expectedManifestVersion, actualVersion);
    }

    if (record.snapshotVersion !== input.expectedManifestVersion) {
      throw new Error("Transaction snapshot does not match the expected manifest");
    }

    const manifestStore = transaction.objectStore("manifests");
    const baseRecord =
      input.expectedManifestVersion === null
        ? undefined
        : await (async () => {
            const value: unknown = await requestResult(
              manifestStore.get(input.expectedManifestVersion ?? -1),
            );
            return value === undefined ? undefined : asStoredManifestRecord(value);
          })();
    if (input.expectedManifestVersion !== null && baseRecord === undefined) {
      throw new Error(`Snapshot manifest is missing: ${String(input.expectedManifestVersion)}`);
    }
    const removedBlockIds = [...new Set(input.removedBlockIds ?? [])].sort();
    const pendingRemovedBlock = removedBlockIds.find((id) => record.pendingBlockIds.includes(id));
    if (pendingRemovedBlock !== undefined) {
      throw new Error(`Cannot supersede a pending block: ${pendingRemovedBlock}`);
    }
    // The resolved base block set is needed only to validate removals and to write checkpoints;
    // an ordinary delta commit with no removals never materializes it. The instance cache makes
    // the removal/checkpoint path O(delta) when this store published the base version.
    const baseDepth =
      baseRecord === undefined || baseRecord.blockIds !== undefined
        ? 0
        : (baseRecord.deltaDepth ?? 0);
    const writesCheckpoint =
      baseRecord === undefined || baseDepth + 1 >= MANIFEST_CHECKPOINT_INTERVAL;
    let baseBlockIdSet: Set<string> | undefined;
    let cacheOwnsBaseSet = false;
    if (removedBlockIds.length > 0 || writesCheckpoint) {
      if (this.#manifestCache?.version === input.expectedManifestVersion) {
        baseBlockIdSet = this.#manifestCache.blockIds;
        cacheOwnsBaseSet = true;
      } else if (baseRecord === undefined) {
        baseBlockIdSet = new Set();
      } else {
        baseBlockIdSet = await resolveManifestBlockSetInTransaction(manifestStore, baseRecord);
      }
      const invalidRemovedBlock = removedBlockIds.find((id) => !baseBlockIdSet?.has(id));
      if (invalidRemovedBlock !== undefined) {
        throw new Error(
          `Cannot supersede a block outside the transaction snapshot: ${invalidRemovedBlock}`,
        );
      }
    }

    const blockStore = transaction.objectStore("blocks");
    const storedBlockKeys = await Promise.all(
      record.pendingBlockIds.map((id) => requestResult(blockStore.getKey(id))),
    );
    const missingBlockIndex = storedBlockKeys.findIndex((key) => key === undefined);
    if (missingBlockIndex >= 0) {
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
      throw new Error(
        `Transaction references missing segment: ${record.pendingSegmentIds[missingSegmentIndex] ?? ""}`,
      );
    }
    const pendingSegments = pendingSegmentValues.map(asSegmentRecord);
    const foreignSegment = pendingSegments.find((segment) => segment.transactionId !== record.id);
    if (foreignSegment !== undefined) {
      throw new Error(`Segment ${foreignSegment.id} belongs to another transaction`);
    }

    // The single-entry path below is the hot bulk-load path and keeps the membership cache;
    // multi-entry commits (atomic write scopes) take the simpler merged path further down and
    // drop the cache.
    const uniqueKeyEntries = input.uniqueKeyChanges ?? [];
    const uniqueKeyChanges = uniqueKeyEntries.length === 1 ? uniqueKeyEntries[0] : undefined;
    const keyCache = this.#uniqueKeyCache;
    const keyCacheValid =
      keyCache !== undefined &&
      keyCache.tableId === uniqueKeyChanges?.tableId &&
      keyCache.version === input.expectedManifestVersion;
    let keyState:
      | {
          existing: Set<string>;
          index: UniqueKeyChunkIndex;
          chunks: UniqueKeyChunk[];
          fullPresent?: Set<string>;
        }
      | undefined;
    if (uniqueKeyChanges !== undefined) {
      let conflictToken: string | undefined;
      if (keyCacheValid) {
        // Steady-state bulk load: membership answered entirely from memory, zero reads.
        const existing = new Set<string>();
        for (const token of uniqueKeyChanges.keyTokens) {
          if (keyCache.present.has(token)) existing.add(token);
        }
        keyState = {
          existing,
          index: keyCache.index,
          chunks: keyCache.chunks,
          fullPresent: keyCache.present,
        };
      } else {
        keyState = await readChunkedUniqueKeys(
          catalog,
          uniqueKeyChanges.tableId,
          uniqueKeyChanges.keyTokens,
        );
      }
      if (uniqueKeyChanges.requireAbsent) {
        conflictToken = uniqueKeyChanges.keyTokens.find((token) => keyState?.existing.has(token));
      }
      if (conflictToken !== undefined) {
        throw new UniqueKeyConflictError(uniqueKeyChanges.tableId, conflictToken);
      }
    }
    /**
     * Multi-entry key work, resolved before the manifest writes: per table, read the union
     * of touched tokens once, then replay the entries in operation order over a working set
     * so in-scope conflicts fail exactly like cross-commit conflicts. The net add/remove
     * per table applies after the manifest version is known.
     */
    const multiKeyWork: Array<{
      tableId: string;
      addedTokens: string[];
      removedTokens: string[];
    }> = [];
    if (uniqueKeyEntries.length > 1) {
      const byTable = new Map<string, UniqueKeyChanges[]>();
      for (const entry of uniqueKeyEntries) {
        const list = byTable.get(entry.tableId) ?? [];
        list.push(entry);
        byTable.set(entry.tableId, list);
      }
      for (const [tableId, entries] of byTable) {
        const unionTokens = [...new Set(entries.flatMap((entry) => entry.keyTokens))];
        const state = await readChunkedUniqueKeys(catalog, tableId, unionTokens);
        const present = state.existing;
        const original = new Set(present);
        for (const entry of entries) {
          for (const token of entry.keyTokens) {
            if (entry.remove === true) {
              present.delete(token);
              continue;
            }
            if (entry.requireAbsent && present.has(token)) {
              throw new UniqueKeyConflictError(tableId, token);
            }
            present.add(token);
          }
        }
        multiKeyWork.push({
          tableId,
          addedTokens: unionTokens.filter((token) => present.has(token) && !original.has(token)),
          removedTokens: unionTokens.filter((token) => !present.has(token) && original.has(token)),
        });
      }
    }

    const addedBlockIds = [...new Set(record.pendingBlockIds)].sort();
    const summary: ManifestSummary = {
      version: input.expectedManifestVersion === null ? 0 : input.expectedManifestVersion + 1,
      previousVersion: input.expectedManifestVersion,
      createdAt: input.committedAt,
      ...(input.changedTableIds === undefined
        ? {}
        : { changedTableIds: [...input.changedTableIds] }),
    };
    let checkpointBlockIds: Set<string> | undefined;
    let manifestRecord: StoredManifestRecord;
    if (writesCheckpoint) {
      checkpointBlockIds = new Set(baseBlockIdSet);
      for (const id of removedBlockIds) checkpointBlockIds.delete(id);
      for (const id of addedBlockIds) checkpointBlockIds.add(id);
      manifestRecord = { ...summary, blockIds: [...checkpointBlockIds].sort() };
    } else {
      manifestRecord = {
        ...summary,
        addedBlockIds,
        removedBlockIds,
        deltaDepth: baseDepth + 1,
      };
    }
    const manifest = summary;
    const committed = updateTransactionRecord(record, {
      status: "committed",
      committedVersion: manifest.version,
      updatedAt: input.committedAt,
    });
    manifestStore.add(manifestRecord, manifest.version);
    catalog.put(manifest.version, CURRENT_MANIFEST_KEY);
    await bumpCatalogEpoch(catalog);
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
    // How the unique-key cache advances once this commit proves durable: `keep` carries the
    // valid cache through with its membership delta applied; `replace` installs newly resolved
    // full knowledge; `drop` forgets a same-table cache we could not keep truthful.
    let keyCachePlan:
      | { action: "keep"; chunk: UniqueKeyChunk; index: UniqueKeyChunkIndex }
      | {
          action: "replace";
          present: Set<string>;
          chunks: UniqueKeyChunk[];
          index: UniqueKeyChunkIndex;
        }
      | { action: "drop" }
      | undefined;
    if (uniqueKeyChanges !== undefined) {
      {
        const existing = keyState?.existing ?? new Set<string>();
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
          const tableId = uniqueKeyChanges.tableId;
          const index = keyState?.index ?? { versions: [], hasBase: false };
          // Complete membership before this commit, when known: from the live cache, from a
          // bulk read that resolved every partition, or trivially empty for a base-less table
          // whose whole tail is in hand.
          const priorPresent =
            keyState?.fullPresent ??
            (index.hasBase ? undefined : replayChunks(keyState?.chunks ?? []));
          if (index.versions.length + 1 > UNIQUE_KEY_TAIL_CHUNK_LIMIT) {
            const tailChunks = [...(keyState?.chunks ?? []), chunk];
            // Fold the tail into hash-partitioned base chunks, atomically with the manifest
            // publication: hundreds of records regardless of key count, never one per key.
            //
            // A tail of point writes is worth almost nothing next to the base it would be
            // folded into, and folding it would rewrite whole partitions to record a handful
            // of tokens — a cost that grows with the table while the write does not. Such a
            // tail collapses into a single equivalent chunk instead, leaving the base alone.
            // The base is only rewritten once the tail is big enough to be worth it.
            const merged =
              index.hasBase && countTailTokens(tailChunks) < UNIQUE_KEY_PARTITION_TARGET
                ? mergeTailChunks(tailChunks)
                : undefined;
            // A token's partition is fixed by its hash and the partition count, so a tail
            // that leaves the count unchanged only disturbs the partitions its own tokens
            // land in. That path reads and rewrites those, and nothing else. Everything else
            // (no recorded size, a partition count that has to change, a base written before
            // this existed) takes the full rewrite below.
            const incremental =
              merged !== undefined
                ? undefined
                : await foldTailIntoTouchedPartitions(catalog, tableId, index, tailChunks);
            if (merged !== undefined) {
              for (const version of index.versions) {
                catalog.delete(uniqueKeyChunkKey(tableId, version));
              }
              catalog.put(merged, uniqueKeyChunkKey(tableId, manifest.version));
              const nextIndex: UniqueKeyChunkIndex = {
                versions: [manifest.version],
                hasBase: true,
                ...(index.partitions === undefined ? {} : { partitions: index.partitions }),
                ...(index.tokenCount === undefined ? {} : { tokenCount: index.tokenCount }),
              };
              catalog.put(nextIndex, uniqueKeyChunkIndexKey(tableId));
              let cachePresent = priorPresent;
              if (cachePresent !== undefined) {
                if (cachePresent === keyState?.fullPresent) cachePresent = new Set(cachePresent);
                applyChunk(cachePresent, chunk);
              }
              keyCachePlan =
                cachePresent === undefined
                  ? { action: "drop" }
                  : {
                      action: "replace",
                      present: cachePresent,
                      chunks: [merged],
                      index: nextIndex,
                    };
            } else if (incremental !== undefined) {
              for (const version of index.versions) {
                catalog.delete(uniqueKeyChunkKey(tableId, version));
              }
              const nextIndex: UniqueKeyChunkIndex = {
                versions: [],
                hasBase: true,
                partitions: incremental.partitions,
                tokenCount: incremental.tokenCount,
              };
              catalog.put(nextIndex, uniqueKeyChunkIndexKey(tableId));
              // The membership cache only survives when it was already complete; rebuilding
              // it here would re-read every partition and undo the saving.
              let cachePresent = priorPresent;
              if (cachePresent !== undefined) {
                if (cachePresent === keyState?.fullPresent) cachePresent = new Set(cachePresent);
                applyChunk(cachePresent, chunk);
              }
              keyCachePlan =
                cachePresent === undefined
                  ? { action: "drop" }
                  : { action: "replace", present: cachePresent, chunks: [], index: nextIndex };
            } else {
              let nextPresent = priorPresent;
              if (nextPresent === undefined) {
                nextPresent = await readAllV2BaseTokens(catalog, tableId, index);
                for (const tail of keyState?.chunks ?? []) applyChunk(nextPresent, tail);
              } else if (nextPresent === keyState?.fullPresent) {
                // The cache's own set must not be mutated before the commit is durable.
                nextPresent = new Set(nextPresent);
              }
              applyChunk(nextPresent, chunk);
              const partitions = Math.max(
                1,
                Math.ceil(nextPresent.size / UNIQUE_KEY_PARTITION_TARGET),
              );
              const parts: string[][] = Array.from({ length: partitions }, () => []);
              for (const token of nextPresent) {
                parts[fnv1a(token) % partitions]?.push(token);
              }
              parts.forEach((tokens, ordinal) => {
                catalog.put(tokens, uniqueKeyBasePartKey(tableId, ordinal));
              });
              for (let ordinal = partitions; ordinal < (index.partitions ?? 0); ordinal += 1) {
                catalog.delete(uniqueKeyBasePartKey(tableId, ordinal));
              }
              for (const version of index.versions) {
                catalog.delete(uniqueKeyChunkKey(tableId, version));
              }
              const nextIndex: UniqueKeyChunkIndex = {
                versions: [],
                hasBase: true,
                partitions,
                tokenCount: nextPresent.size,
              };
              catalog.put(nextIndex, uniqueKeyChunkIndexKey(tableId));
              keyCachePlan = {
                action: "replace",
                present: nextPresent,
                chunks: [],
                index: nextIndex,
              };
            }
          } else {
            catalog.put(chunk, uniqueKeyChunkKey(tableId, manifest.version));
            const nextIndex: UniqueKeyChunkIndex = {
              versions: [...index.versions, manifest.version],
              hasBase: index.hasBase,
              ...(index.partitions === undefined ? {} : { partitions: index.partitions }),
              // The base is untouched here, so its recorded size still describes it. Dropping
              // it would cost the next fold its incremental path.
              ...(index.tokenCount === undefined ? {} : { tokenCount: index.tokenCount }),
            };
            catalog.put(nextIndex, uniqueKeyChunkIndexKey(tableId));
            if (keyCacheValid) {
              keyCachePlan = { action: "keep", chunk, index: nextIndex };
            } else if (keyState?.fullPresent !== undefined) {
              const nextPresent = new Set(keyState.fullPresent);
              applyChunk(nextPresent, chunk);
              keyCachePlan = {
                action: "replace",
                present: nextPresent,
                chunks: [...keyState.chunks, chunk],
                index: nextIndex,
              };
            } else if (priorPresent !== undefined) {
              applyChunk(priorPresent, chunk);
              keyCachePlan = {
                action: "replace",
                present: priorPresent,
                chunks: [...(keyState?.chunks ?? []), chunk],
                index: nextIndex,
              };
            } else {
              keyCachePlan = { action: "drop" };
            }
          }
        }
      }
    }
    for (const work of multiKeyWork) {
      {
        if (work.addedTokens.length > 0 || work.removedTokens.length > 0) {
          const indexValue = (await requestResult(
            catalog.get(uniqueKeyChunkIndexKey(work.tableId)),
          )) as UniqueKeyChunkIndex | undefined;
          const index = indexValue ?? { versions: [], hasBase: false };
          catalog.put(
            { addedTokens: work.addedTokens, removedTokens: work.removedTokens },
            uniqueKeyChunkKey(work.tableId, manifest.version),
          );
          catalog.put(
            {
              versions: [...index.versions, manifest.version],
              hasBase: index.hasBase,
              ...(index.partitions === undefined ? {} : { partitions: index.partitions }),
              // Appending a tail chunk leaves the base alone, so its recorded size still holds.
              ...(index.tokenCount === undefined ? {} : { tokenCount: index.tokenCount }),
            },
            uniqueKeyChunkIndexKey(work.tableId),
          );
        }
      }
    }
    // Full-text deltas apply atomically with the publish; a stale writer (one that committed
    // segments to an indexed table without deltas) flips the affected columns to "invalid"
    // instead of failing the data commit — the index self-heals through a rebuild.
    const changedFtsTableIds = new Set(pendingSegments.map((segment) => segment.tableId));
    for (const tableId of changedFtsTableIds) {
      const tableValue: unknown = await requestResult(catalog.get(`${TABLE_ID_PREFIX}${tableId}`));
      if (tableValue === undefined) continue;
      const forTable = (input.ftsChanges ?? []).find((entry) => entry.tableId === tableId);
      const covered = new Set(forTable?.columns.map((column) => column.columnId) ?? []);
      const invalidated = invalidateUncoveredFtsColumns(
        structuredClone(tableValue) as TableRecord,
        covered,
      );
      if (invalidated !== undefined) {
        catalog.put(structuredClone(invalidated), `${TABLE_ID_PREFIX}${tableId}`);
      }
    }
    for (const ftsEntry of input.ftsChanges ?? []) {
      for (const column of ftsEntry.columns) {
        catalog.put(
          structuredClone({
            postings: column.postings,
            totalTokens: column.totalTokens,
          } satisfies FtsDeltaChunk),
          ftsChunkKey(ftsEntry.tableId, column.columnId, manifest.version),
        );
        const indexKey = ftsChunkIndexKey(ftsEntry.tableId, column.columnId);
        const chunkIndex = (await requestResult(catalog.get(indexKey))) as
          { versions: number[] } | undefined;
        catalog.put({ versions: [...(chunkIndex?.versions ?? []), manifest.version] }, indexKey);
      }
    }
    transactionStore.put(committed, committed.id);
    const settle = (): void => {
      if (uniqueKeyEntries.length > 1) {
        // Multi-entry commits take the merged path above; the single-table membership cache
        // cannot describe them, so it drops and rebuilds on the next bulk load.
        this.#uniqueKeyCache = undefined;
      }
      // The unique-key cache follows the same rule as the manifest cache below: advance it only
      // after the durable commit. Commits from this instance cannot silently change another
      // table's key records, so a cache for a different table just moves to the new version.
      if (uniqueKeyChanges !== undefined && keyCachePlan !== undefined) {
        if (keyCachePlan.action === "keep" && keyCacheValid) {
          // In-place delta on the live cache: no copies of a multi-million-token set per batch.
          applyChunk(keyCache.present, keyCachePlan.chunk);
          keyCache.chunks.push(keyCachePlan.chunk);
          keyCache.index = keyCachePlan.index;
          keyCache.version = manifest.version;
        } else if (keyCachePlan.action === "replace") {
          this.#uniqueKeyCache = {
            version: manifest.version,
            tableId: uniqueKeyChanges.tableId,
            present: keyCachePlan.present,
            chunks: keyCachePlan.chunks,
            index: keyCachePlan.index,
          };
        } else if (this.#uniqueKeyCache?.tableId === uniqueKeyChanges.tableId) {
          this.#uniqueKeyCache = undefined;
        } else if (this.#uniqueKeyCache !== undefined) {
          this.#uniqueKeyCache.version = manifest.version;
        }
      } else if (
        uniqueKeyChanges !== undefined &&
        this.#uniqueKeyCache?.tableId === uniqueKeyChanges.tableId
      ) {
        // A no-op key write against the cached table: nothing changed on disk, so the cache
        // just moves to the new version.
        this.#uniqueKeyCache.version = manifest.version;
      } else if (this.#uniqueKeyCache !== undefined) {
        this.#uniqueKeyCache.version = manifest.version;
      }
      // Advance the resolved-set cache only after the durable commit succeeded. A checkpoint
      // commit already built the new set; a delta commit with a resolved base applies the delta
      // in place (the cache owns that set from here on).
      if (checkpointBlockIds !== undefined) {
        this.#manifestCache = { version: manifest.version, blockIds: checkpointBlockIds };
      } else if (baseBlockIdSet !== undefined || cacheOwnsBaseSet) {
        const blockIds = baseBlockIdSet ?? new Set<string>();
        for (const id of removedBlockIds) blockIds.delete(id);
        for (const id of addedBlockIds) blockIds.add(id);
        this.#manifestCache = { version: manifest.version, blockIds };
      } else {
        // No resolved base was materialized; a later removal or checkpoint commit will resolve.
        this.#manifestCache = undefined;
      }
    };
    return { manifest, settle };
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

  async moveLease(
    id: string,
    expectedRevision: number,
    manifestVersion: number | null,
    expiresAt: string,
  ): Promise<LeaseRecord> {
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
      await assertSnapshotAvailableInTransaction(transaction, manifestVersion);
      const moved = { ...record, manifestVersion, expiresAt, revision: record.revision + 1 };
      store.put(moved, id);
      await transactionDone(transaction);
      return structuredClone(moved);
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
      const transactionStore = transaction.objectStore("transactions");
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
      const reclaimedTransactionIds: string[] = [];
      const retainedTransactionIds: string[] = [];
      const missingTransactionIds: string[] = [];
      let reclaimedBlockBytes = 0;
      let remaining = input.maxItems;
      let manifestIndex = current.cursor.manifestIndex;
      while (remaining > 0 && manifestIndex < current.candidateManifestVersions.length) {
        const version = current.candidateManifestVersions[manifestIndex];
        if (version === undefined) throw new Error("Garbage collection manifest cursor is invalid");
        const manifestValue: unknown = await requestResult(manifestStore.get(version));
        if (manifestValue === undefined) missingManifestVersions.push(version);
        else if (asStoredManifestRecord(manifestValue).prunedAt !== undefined) {
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
          // The tombstone keeps the record's full content (checkpoint list or delta) so chains
          // above it keep resolving; it only stops counting as a reachability root.
          manifestStore.put(
            { ...asStoredManifestRecord(manifestValue), prunedAt: input.updatedAt },
            version,
          );
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
      let transactionIndex = current.cursor.transactionIndex;
      if (
        remaining > 0 &&
        manifestIndex === current.candidateManifestVersions.length &&
        segmentIndex === current.candidateSegmentIds.length &&
        blockIndex === current.candidateBlockIds.length
      ) {
        const candidates = new Set(current.candidateTransactionIds.slice(transactionIndex));
        const segmentOwners = new Set<string>();
        await visitObjectStoreSequentially(segmentStore, (value) => {
          const owner = asSegmentRecord(value).transactionId;
          if (candidates.has(owner)) segmentOwners.add(owner);
        });
        const unfinishedCompactionOwners = new Set<string>();
        await visitObjectStoreSequentially(gcStore, (value) => {
          if (!isCompactionJobEnvelope(value)) return;
          const job = asCompactionJobEnvelope(value);
          if (
            !isTerminalCompactionJob(job) &&
            job.transactionId !== null &&
            candidates.has(job.transactionId)
          ) {
            unfinishedCompactionOwners.add(job.transactionId);
          }
        });
        while (remaining > 0 && transactionIndex < current.candidateTransactionIds.length) {
          const id = current.candidateTransactionIds[transactionIndex];
          if (id === undefined) {
            throw new Error("Garbage collection transaction cursor is invalid");
          }
          const transactionValue: unknown = await requestResult(transactionStore.get(id));
          const record =
            transactionValue === undefined ? undefined : asTransactionRecord(transactionValue);
          const manifestValue: unknown =
            record?.committedVersion === null || record?.committedVersion === undefined
              ? undefined
              : await requestResult(manifestStore.get(record.committedVersion));
          const manifest =
            manifestValue === undefined ? undefined : asStoredManifestRecord(manifestValue);
          if (record === undefined) missingTransactionIds.push(id);
          else if (
            unfinishedCompactionOwners.has(id) ||
            (record.status === "committed"
              ? (manifest !== undefined && manifest.prunedAt === undefined) || segmentOwners.has(id)
              : record.status === "aborted"
                ? (await anyObjectStoreKeyExists(blockStore, record.pendingBlockIds)) ||
                  (await anyObjectStoreKeyExists(segmentStore, record.pendingSegmentIds))
                : true)
          ) {
            retainedTransactionIds.push(id);
          } else {
            transactionStore.delete(id);
            reclaimedTransactionIds.push(id);
          }
          transactionIndex += 1;
          remaining -= 1;
        }
      }

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
        examinedTransactionCount:
          reclaimedTransactionIds.length +
          retainedTransactionIds.length +
          missingTransactionIds.length,
        reclaimedTransactionCount: reclaimedTransactionIds.length,
        retainedTransactionCount: retainedTransactionIds.length,
        missingTransactionCount: missingTransactionIds.length,
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
        reclaimedTransactionIds,
        retainedTransactionIds,
        missingTransactionIds,
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async removePrunedManifestRecords(): Promise<number> {
    const transaction = this.#transaction("manifests", "readwrite");
    const store = transaction.objectStore("manifests");
    try {
      const values: unknown[] = await requestResult(store.getAll());
      const records = values.map(asStoredManifestRecord);
      const earliestReadable = records
        .filter((record) => record.prunedAt === undefined)
        .sort((left, right) => left.version - right.version)[0];
      const safeBelow =
        earliestReadable === undefined
          ? Number.POSITIVE_INFINITY
          : earliestReadable.version - (earliestReadable.deltaDepth ?? 0);
      let removed = 0;
      for (const record of records) {
        if (record.prunedAt === undefined || record.version >= safeBelow) continue;
        store.delete(record.version);
        removed += 1;
      }
      await transactionDone(transaction);
      return removed;
    } catch (error) {
      transaction.abort();
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

  /**
   * Copies the current version out as a portable snapshot. See `./snapshot.ts` for what a
   * snapshot carries and what it deliberately leaves behind.
   *
   * This reads across several transactions rather than one, so a concurrent commit could move
   * the version underneath it. Hold a `backup` lease (see `../transactions/index.ts`) around
   * the call when that is possible; the offline generators that produce published datasets are
   * the only writer in their process and do not need one.
   */
  async exportSnapshot(): Promise<DatabaseSnapshot> {
    const manifest = await this.getCurrentManifest();
    if (manifest === undefined) throw new Error("There is no committed version to snapshot");
    const version = manifest.version;

    const blocks: Array<{ id: string; bytes: Uint8Array }> = [];
    for (let start = 0; start < manifest.blockIds.length; start += SNAPSHOT_BLOCK_BATCH) {
      const ids = manifest.blockIds.slice(start, start + SNAPSHOT_BLOCK_BATCH);
      const values = await this.getBlocks(ids);
      values.forEach((bytes, index) => {
        const id = ids[index] ?? "";
        if (bytes === undefined) throw new Error(`Manifest references missing block: ${id}`);
        blocks.push({ id, bytes });
      });
    }

    const { segments, transactions } = selectLiveRecords({
      liveBlockIds: new Set(manifest.blockIds),
      segments: await this.listSegments(),
      transactions: await this.listTransactions(),
      version,
    });

    const tables: SnapshotTable[] = [];
    for (const record of await this.listTables()) {
      const transaction = this.#transaction("catalog", "readonly");
      const catalog = transaction.objectStore("catalog");
      const [rowIdValue, ...autoIncrementValues] = await Promise.all([
        requestResult<unknown>(catalog.get(`${ROW_ID_PREFIX}${record.id}`)),
        ...record.columns.map((column) =>
          requestResult<unknown>(catalog.get(`${AUTO_INCREMENT_PREFIX}${record.id}/${column.id}`)),
        ),
      ]);
      const uniqueKeyTokens =
        record.uniqueKeyColumnId === undefined
          ? []
          : [...(await readAllUniqueKeyTokens(catalog, record.id))];

      const fts: SnapshotFtsIndex[] = [];
      const ftsColumns = { ...(record.ftsColumns ?? {}) };
      for (const [columnId, state] of Object.entries(ftsColumns)) {
        const toc = (await requestResult<unknown>(
          catalog.get(`${FTS_BASE_INDEX_PREFIX}${record.id}/${columnId}`),
        )) as FtsBaseToc | undefined;
        if (state.state !== "ready" || toc?.coversVersion !== version) {
          // Anything not already covering this exact version restores as a rebuild. The index
          // is a pruning accelerator that the scan re-verifies, so that costs speed, not truth.
          ftsColumns[columnId] = { ...state, state: "invalid" };
          continue;
        }
        const prefix = `${FTS_BASE_PREFIX}${record.id}/${columnId}/`;
        const chunks = (await Promise.all(
          toc.boundaries.map((_, ordinal) =>
            requestResult<unknown>(catalog.get(`${prefix}${String(ordinal).padStart(6, "0")}`)),
          ),
        )) as Array<FtsPosting[] | undefined>;
        fts.push({
          columnId,
          coversVersion: toc.coversVersion,
          totalTokens: toc.totalTokens,
          chunks: chunks.map((chunk) => chunk ?? []),
        });
      }
      await transactionDone(transaction);

      tables.push({
        record: {
          ...record,
          ...(Object.keys(ftsColumns).length === 0 ? {} : { ftsColumns }),
        },
        nextRowId: (rowIdValue as bigint | undefined) ?? 1n,
        autoIncrement: record.columns.flatMap((column, index) => {
          const next = autoIncrementValues[index] as bigint | undefined;
          return next === undefined ? [] : [{ columnId: column.id, next }];
        }),
        uniqueKeyTokens,
        fts,
      });
    }

    return { version, createdAt: new Date().toISOString(), tables, segments, transactions, blocks };
  }

  /**
   * Loads a snapshot into this store, which must be empty. Block payloads go in batched
   * transactions so a large dataset does not build one enormous write, and the current-version
   * pointer is written last: until it lands, an interrupted load leaves a store that still reads
   * as empty rather than as half a database.
   */
  async importSnapshot(
    snapshot: DatabaseSnapshot,
    options: { onProgress?: (progress: SnapshotLoadProgress) => void } = {},
  ): Promise<void> {
    const existing = await this.getCurrentManifestVersion();
    if (existing !== null) throw new Error("This store already holds a database");
    if ((await this.listTables()).length > 0) {
      throw new Error("This store already holds a catalog");
    }

    const totalBytes = snapshot.blocks.reduce((total, block) => total + block.bytes.byteLength, 0);
    let writtenBytes = 0;
    options.onProgress?.({ phase: "blocks", writtenBytes, totalBytes });

    let batch: Array<{ id: string; bytes: Uint8Array }> = [];
    let batchBytes = 0;
    const flush = async (): Promise<void> => {
      if (batch.length === 0) return;
      await this.addBlocks(batch);
      writtenBytes += batchBytes;
      options.onProgress?.({ phase: "blocks", writtenBytes, totalBytes });
      batch = [];
      batchBytes = 0;
    };
    for (const block of snapshot.blocks) {
      batch.push(block);
      batchBytes += block.bytes.byteLength;
      if (batchBytes >= SNAPSHOT_BATCH_BYTES) await flush();
    }
    await flush();

    options.onProgress?.({ phase: "catalog", writtenBytes, totalBytes });
    for (const table of snapshot.tables) await this.#importSnapshotTable(table, snapshot.version);

    const transaction = this.#transaction(["manifests", "segments", "transactions"], "readwrite");
    const manifests = transaction.objectStore("manifests");
    const segments = transaction.objectStore("segments");
    const transactions = transaction.objectStore("transactions");
    // One checkpoint carrying the complete block list, so nothing resolves through a delta
    // chain that no longer exists.
    const record: StoredManifestRecord = {
      version: snapshot.version,
      previousVersion: null,
      blockIds: snapshot.blocks.map((block) => block.id),
      createdAt: snapshot.createdAt,
      deltaDepth: 0,
    };
    manifests.put(record, snapshot.version);
    for (const segment of snapshot.segments) {
      segments.put(normalizeSegmentRecord(segment), segment.id);
    }
    for (const entry of snapshot.transactions) transactions.put(structuredClone(entry), entry.id);
    await transactionDone(transaction);

    const pointer = this.#transaction("catalog", "readwrite");
    const catalog = pointer.objectStore("catalog");
    catalog.put(snapshot.version, CURRENT_MANIFEST_KEY);
    await bumpCatalogEpoch(catalog);
    await transactionDone(pointer);
    options.onProgress?.({ phase: "done", writtenBytes, totalBytes });
  }

  async #importSnapshotTable(table: SnapshotTable, version: number): Promise<void> {
    const keyed = table.record.uniqueKeyColumnId !== undefined;
    // The snapshot carries logical membership, not a storage layout, so it always loads as the
    // current one. Writing the base pre-folded is what keeps it that way: replaying the tokens
    // as commit chunks instead would leave a tail long enough to make the first write crawl.
    const record: TableRecord = {
      ...table.record,
      ...(keyed ? { uniqueKeyLookupReady: true } : {}),
    };

    const transaction = this.#transaction("catalog", "readwrite");
    const catalog = transaction.objectStore("catalog");
    catalog.put(structuredClone(record), `${TABLE_ID_PREFIX}${record.id}`);
    catalog.put(record.id, `${TABLE_NAME_PREFIX}${record.name}`);
    catalog.put(table.nextRowId, `${ROW_ID_PREFIX}${record.id}`);
    for (const entry of table.autoIncrement) {
      catalog.put(entry.next, `${AUTO_INCREMENT_PREFIX}${record.id}/${entry.columnId}`);
    }

    if (keyed) {
      const partitions = Math.max(
        1,
        Math.ceil(table.uniqueKeyTokens.length / UNIQUE_KEY_PARTITION_TARGET),
      );
      const parts: string[][] = Array.from({ length: partitions }, () => []);
      for (const token of table.uniqueKeyTokens) parts[fnv1a(token) % partitions]?.push(token);
      parts.forEach((tokens, ordinal) => {
        catalog.put(tokens, uniqueKeyBasePartKey(record.id, ordinal));
      });
      const index: UniqueKeyChunkIndex = {
        versions: [],
        hasBase: true,
        partitions,
        tokenCount: table.uniqueKeyTokens.length,
      };
      catalog.put(index, uniqueKeyChunkIndexKey(record.id));
    }

    for (const entry of table.fts) {
      const prefix = `${FTS_BASE_PREFIX}${record.id}/${entry.columnId}/`;
      const boundaries = entry.chunks.map((chunk) => ({
        first: chunk[0]?.term ?? "",
        last: chunk[chunk.length - 1]?.term ?? "",
      }));
      entry.chunks.forEach((chunk, ordinal) => {
        catalog.put(structuredClone(chunk), `${prefix}${String(ordinal).padStart(6, "0")}`);
      });
      catalog.put(
        { coversVersion: version, boundaries, totalTokens: entry.totalTokens },
        `${FTS_BASE_INDEX_PREFIX}${record.id}/${entry.columnId}`,
      );
      catalog.put({ versions: [] }, ftsChunkIndexKey(record.id, entry.columnId));
    }
    await transactionDone(transaction);
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

/**
 * Advances the catalog epoch inside an already-open readwrite transaction on the catalog
 * store. Every catalog mutation calls this in its own transaction, so the epoch read by
 * `getCatalogProbe` is an atomic, monotonic proof of catalog identity: an unchanged epoch
 * means no table record, manifest publish, or commit has landed since the epoch was read.
 */
async function bumpCatalogEpoch(catalog: IDBObjectStore): Promise<void> {
  const value = (await requestResult(catalog.get(CATALOG_EPOCH_KEY))) as number | undefined;
  catalog.put((value ?? 0) + 1, CATALOG_EPOCH_KEY);
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

/**
 * Rejects if a transaction aborts or fails, and never resolves otherwise. Lets a caller that
 * does not wait for `complete` still surface the failure it would otherwise have missed.
 */
function transactionFailure(transaction: IDBTransaction): Promise<never> {
  return new Promise((_resolve, reject) => {
    transaction.addEventListener(
      "abort",
      () => reject(transaction.error ?? new Error("Transaction aborted")),
      { once: true },
    );
    transaction.addEventListener(
      "error",
      () => reject(transaction.error ?? new Error("Transaction failed")),
      { once: true },
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
  // Every IndexedDB get deserializes a fresh, unshared value, so the bytes return without a
  // defensive copy; wrapping an ArrayBuffer in a view is also zero-copy.
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  throw new Error("Stored block is not binary data");
}

function asStoredManifestRecord(value: unknown): StoredManifestRecord {
  return structuredClone(value) as StoredManifestRecord;
}

async function anyObjectStoreKeyExists(
  store: IDBObjectStore,
  ids: readonly string[],
): Promise<boolean> {
  for (const id of ids) {
    if ((await requestResult(store.getKey(id))) !== undefined) return true;
  }
  return false;
}

/** Builds the resolved public view of one stored record from its resolved block set. */
function manifestView(record: StoredManifestRecord, blockIds: ReadonlySet<string>): Manifest {
  return {
    version: record.version,
    previousVersion: record.previousVersion,
    blockIds: [...blockIds].sort(),
    createdAt: record.createdAt,
    ...(record.changedTableIds === undefined
      ? {}
      : { changedTableIds: [...record.changedTableIds] }),
    ...(record.prunedAt === undefined ? {} : { prunedAt: record.prunedAt }),
  };
}

/**
 * Resolves one stored record's complete block set by walking `previousVersion` links down to
 * the nearest checkpoint and replaying the deltas forward. Pruned records keep their content,
 * so the chain below any readable version always exists; a missing link is corruption.
 */
async function resolveManifestBlockSetInTransaction(
  manifestStore: IDBObjectStore,
  record: StoredManifestRecord,
): Promise<Set<string>> {
  const chain: StoredManifestRecord[] = [record];
  let cursor = record;
  while (cursor.blockIds === undefined) {
    if (cursor.previousVersion === null) {
      throw new Error(
        `Manifest delta chain has no checkpoint below version ${String(record.version)}`,
      );
    }
    const value: unknown = await requestResult(manifestStore.get(cursor.previousVersion));
    if (value === undefined) {
      throw new Error(
        `Manifest delta chain is broken at version ${String(cursor.previousVersion)}`,
      );
    }
    cursor = asStoredManifestRecord(value);
    chain.push(cursor);
  }
  const blockIds = new Set<string>();
  for (let index = chain.length - 1; index >= 0; index -= 1) {
    const link = chain[index];
    if (link !== undefined) applyManifestRecord(blockIds, link);
  }
  return blockIds;
}

async function resolveManifestInTransaction(
  manifestStore: IDBObjectStore,
  record: StoredManifestRecord,
): Promise<Manifest> {
  return manifestView(record, await resolveManifestBlockSetInTransaction(manifestStore, record));
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

/**
 * Verifies the snapshot at `version` is readable: the record exists, is not pruned, and its
 * delta chain down to a checkpoint is intact. Block existence is verified inductively — every
 * commit proves its own added blocks exist before publishing, and garbage collection only
 * deletes blocks unreachable from every non-pruned manifest — so this stays O(chain) instead of
 * probing every live block on each transaction and lease creation.
 */
/**
 * The transaction record a commit needs: present, active, and at the expected revision —
 * otherwise a `TransactionRecordConflictError`, the caller's storage transaction still open for
 * it to abort.
 */
async function readActiveTransactionRecord(
  transaction: IDBTransaction,
  id: string,
  expectedRevision: number,
): Promise<TransactionRecord> {
  const value: unknown = await requestResult<unknown>(
    transaction.objectStore("transactions").get(id),
  );
  const record = value === undefined ? undefined : asTransactionRecord(value);
  if (record?.revision !== expectedRevision || record.status !== "active") {
    throw new TransactionRecordConflictError(id, expectedRevision, record?.revision ?? null);
  }
  return record;
}

async function assertSnapshotAvailableInTransaction(
  transaction: IDBTransaction,
  version: number | null,
): Promise<void> {
  if (version === null) return;
  const manifestStore = transaction.objectStore("manifests");
  const value: unknown = await requestResult(manifestStore.get(version));
  if (value === undefined) throw new SnapshotManifestMissingError(version);
  let cursor = asStoredManifestRecord(value);
  if (cursor.prunedAt !== undefined) throw new SnapshotManifestMissingError(version);
  while (cursor.blockIds === undefined) {
    if (cursor.previousVersion === null) throw new SnapshotManifestMissingError(version);
    const linkValue: unknown = await requestResult(manifestStore.get(cursor.previousVersion));
    if (linkValue === undefined) throw new SnapshotManifestMissingError(version);
    cursor = asStoredManifestRecord(linkValue);
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

/**
 * After pruning, every remaining manifest must still be fully readable. One ascending pass
 * resolves the whole chain with a single running set (pruned records still contribute their
 * deltas), unions the blocks referenced by any remaining manifest, and verifies each referenced
 * block once instead of once per manifest.
 */
async function assertRemainingManifestRecordsAvailable(
  transaction: IDBTransaction,
  newlyPrunedVersions: ReadonlySet<number>,
): Promise<void> {
  const running = new Set<string>();
  const referenced = new Map<string, number>();
  await visitObjectStoreSequentially(transaction.objectStore("manifests"), (value) => {
    const record = asStoredManifestRecord(value);
    applyManifestRecord(running, record);
    if (record.prunedAt !== undefined || newlyPrunedVersions.has(record.version)) return;
    for (const id of running) {
      if (!referenced.has(id)) referenced.set(id, record.version);
    }
  });
  const blockStore = transaction.objectStore("blocks");
  for (const [id, version] of referenced) {
    if ((await requestResult(blockStore.getKey(id))) === undefined) {
      throw new SnapshotManifestMissingError(version);
    }
  }
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
  const candidateManifestBlockIds: Array<Set<string>> = [];
  for (const version of job.candidateManifestVersions) {
    const value: unknown = await requestResult(manifestStore.get(version));
    if (value === undefined) {
      throw new Error(`Garbage collection candidate manifest is missing: ${String(version)}`);
    }
    candidateManifestBlockIds.push(
      await resolveManifestBlockSetInTransaction(manifestStore, asStoredManifestRecord(value)),
    );
  }
  for (const id of job.candidateTransactionIds) {
    const value: unknown = await requestResult(transaction.objectStore("transactions").get(id));
    const record = value === undefined ? undefined : asTransactionRecord(value);
    if (
      record === undefined ||
      (record.status !== "aborted" &&
        (record.status !== "committed" || record.committedVersion === null))
    ) {
      throw new Error(`Garbage collection transaction candidate is not terminal: ${id}`);
    }
  }
  const blockHasProvenance = async (id: string): Promise<boolean> => {
    if (candidateManifestBlockIds.some((blockIds) => blockIds.has(id))) return true;
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
  // One ascending pass resolves every manifest with a running set (pruned records still apply
  // their deltas so the chain stays coherent); a probed block roots when it is a member at any
  // version that still counts.
  {
    const running = new Set<string>();
    const unrooted = new Set(probeBlockIds);
    await visitObjectStoreSequentially(transaction.objectStore("manifests"), (value) => {
      const record = asStoredManifestRecord(value);
      applyManifestRecord(running, record);
      if (record.prunedAt !== undefined || newlyPrunedVersions.has(record.version)) return;
      if (unrooted.size === 0) return true;
      for (const id of unrooted) {
        if (running.has(id)) {
          directBlockRoots.add(id);
          unrooted.delete(id);
        }
      }
      return false;
    });
  }

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
    reclaimedTransactionIds: [],
    retainedTransactionIds: [],
    missingTransactionIds: [],
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

interface FtsBaseToc {
  coversVersion: number;
  boundaries: Array<{ first: string; last: string }>;
  totalTokens: number;
}

interface FtsDeltaChunk {
  postings: FtsPosting[];
  totalTokens: number;
}

function ftsChunkKey(tableId: string, columnId: string, version: number): string {
  return `${FTS_CHUNK_PREFIX}${tableId}/${columnId}/${String(version)}`;
}

function ftsChunkIndexKey(tableId: string, columnId: string): string {
  return `${FTS_CHUNK_PREFIX}index/${tableId}/${columnId}`;
}

function validateAutoIncrementReservation(count: number, atLeast: bigint | undefined): void {
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new RangeError("Auto-increment reservation count must be a non-negative whole number");
  }
  if (atLeast !== undefined && atLeast < 1n) {
    throw new RangeError("Auto-increment bump target must be at least 1");
  }
}

function maxBigInt(a: bigint, b: bigint): bigint {
  return a > b ? a : b;
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

function validateTableColumns(columns: readonly TableColumnRecord[]): void {
  if (columns.length === 0) throw new TypeError("A table needs at least one column");
  const ids = new Set(columns.map(({ id }) => id));
  const names = new Set(columns.map(({ name }) => name));
  if (ids.size !== columns.length || names.size !== columns.length) {
    throw new TypeError("Table columns must have unique IDs and names");
  }
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

function uniqueKeyChunkIndexKey(tableId: string): IDBValidKey {
  return [UNIQUE_KEY_CHUNK_INDEX, tableId];
}

function uniqueKeyChunkKey(tableId: string, version: number): IDBValidKey {
  return [UNIQUE_KEY_CHUNK, tableId, version];
}

function uniqueKeyBasePartKey(tableId: string, ordinal: number): IDBValidKey {
  return [UNIQUE_KEY_BASE_PART, tableId, ordinal];
}

/**
 * Resolves which of the requested tokens exist: point probes against the folded base records,
 * then the bounded chunk tail applied in commit order (an add in a later chunk revives a token
 * a base fold removed, a removal hides a base token). The full tail chunks return with the
 * answer so a folding commit can replay them without re-reading.
 */
interface UniqueKeyChunkIndex {
  versions: number[];
  /** True once a fold has written base records; probes skip them until then. */
  hasBase: boolean;
  /** How many hash partitions the folded base is spread across. */
  partitions?: number;
  /**
   * How many tokens the folded base holds. Knowing the size without reading
   * every partition is what lets a fold rewrite only the partitions its tail touched. Absent
   * on bases written before this was recorded; the next fold rewrites them in full and fills
   * it in.
   */
  tokenCount?: number;
}

/** Accepts the legacy bare-array shape (tail only, no base) and the current object shape. */
function asUniqueKeyChunkIndex(value: unknown): UniqueKeyChunkIndex {
  const versionsOf = (candidate: unknown): number[] =>
    Array.isArray(candidate)
      ? candidate.filter((entry): entry is number => Number.isSafeInteger(entry))
      : [];
  if (Array.isArray(value)) return { versions: versionsOf(value), hasBase: false };
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    return {
      versions: versionsOf(record.versions),
      hasBase: record.hasBase === true,
      ...(Number.isSafeInteger(record.partitions) && (record.partitions as number) > 0
        ? { partitions: record.partitions as number }
        : {}),
      ...(Number.isSafeInteger(record.tokenCount) && (record.tokenCount as number) >= 0
        ? { tokenCount: record.tokenCount as number }
        : {}),
    };
  }
  return { versions: [], hasBase: false };
}

/**
 * Above this many tokens, probing the folded base per token costs more than one cursor pass
 * over it; below, point lookups win. Bulk loads sit far above, keyed point writes far below.
 */
const UNIQUE_KEY_BASE_SCAN_THRESHOLD = 2_048;

/** FNV-1a over UTF-16 code units: the stable hash that assigns a token to a base partition. */
function fnv1a(token: string): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < token.length; index += 1) {
    hash ^= token.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/** Applies one tail chunk's membership delta in place, in the order commits recorded it. */
function applyChunk(present: Set<string>, chunk: UniqueKeyChunk): void {
  for (const token of chunk.addedTokens) present.add(token);
  for (const token of chunk.removedTokens) present.delete(token);
}

/** Replays tail chunks over an empty base into a fresh membership set. */
function replayChunks(chunks: readonly UniqueKeyChunk[]): Set<string> {
  const present = new Set<string>();
  for (const chunk of chunks) applyChunk(present, chunk);
  return present;
}

function asBasePartition(value: unknown): string[] {
  if (!isStringArray(value)) throw new Error("Unique-key base partition is invalid");
  return value;
}

/** How many distinct tokens a tail carries, counting a token touched twice once. */
function countTailTokens(tailChunks: readonly UniqueKeyChunk[]): number {
  const seen = new Set<string>();
  for (const tail of tailChunks) {
    for (const token of tail.addedTokens) seen.add(token);
    for (const token of tail.removedTokens) seen.add(token);
  }
  return seen.size;
}

/**
 * Collapses a run of tail chunks into one chunk with the same effect. Replaying in commit
 * order and keeping the two sets disjoint is what makes it equivalent: a token added and then
 * removed ends up only in `removedTokens`, and the reverse ends up only in `addedTokens`, so
 * applying the result once lands exactly where applying the run in order would.
 */
function mergeTailChunks(tailChunks: readonly UniqueKeyChunk[]): UniqueKeyChunk {
  const added = new Set<string>();
  const removed = new Set<string>();
  for (const tail of tailChunks) {
    for (const token of tail.addedTokens) {
      removed.delete(token);
      added.add(token);
    }
    for (const token of tail.removedTokens) {
      added.delete(token);
      removed.add(token);
    }
  }
  return { addedTokens: [...added], removedTokens: [...removed] };
}

/**
 * Folds a tail into the folded base by touching only the partitions its tokens hash into.
 * Returns undefined when that cannot be done correctly and the caller must rewrite the whole
 * base: no folded base yet, no recorded token count, or a resulting size that needs a
 * different partition count (which moves every token, since the partition is `hash % count`).
 *
 * The touched partitions are read, replayed in commit order, and written back. Their sizes
 * before and after give the new total exactly, so the count stays true without a full read.
 */
async function foldTailIntoTouchedPartitions(
  store: IDBObjectStore,
  tableId: string,
  index: UniqueKeyChunkIndex,
  tailChunks: readonly UniqueKeyChunk[],
): Promise<{ partitions: number; tokenCount: number } | undefined> {
  const partitions = index.partitions;
  const priorCount = index.tokenCount;
  if (!index.hasBase || partitions === undefined || priorCount === undefined) return undefined;

  const touchedOrdinals = new Set<number>();
  for (const tail of tailChunks) {
    for (const token of tail.addedTokens) touchedOrdinals.add(fnv1a(token) % partitions);
    for (const token of tail.removedTokens) touchedOrdinals.add(fnv1a(token) % partitions);
  }
  if (touchedOrdinals.size === 0) return { partitions, tokenCount: priorCount };
  // No threshold on how many partitions are touched. Even a tail that reaches most of them
  // still hashes only its own tokens, where the rewrite hashes every token in the table —
  // and that hashing, not the record I/O, is what a fold spends its time on.

  const ordinals = [...touchedOrdinals];
  const stored = await Promise.all(
    ordinals.map((ordinal) =>
      requestResult<unknown>(store.get(uniqueKeyBasePartKey(tableId, ordinal))),
    ),
  );
  const buckets = new Map<number, Set<string>>();
  let sizeBefore = 0;
  for (let index = 0; index < ordinals.length; index += 1) {
    const value = stored[index];
    const tokens = new Set(value === undefined ? [] : asBasePartition(value));
    sizeBefore += tokens.size;
    buckets.set(ordinals[index] ?? 0, tokens);
  }

  // Replay in commit order, exactly as a full replay would: a token added then removed by a
  // later chunk must end up absent, and the reverse must end up present.
  for (const tail of tailChunks) {
    for (const token of tail.addedTokens) buckets.get(fnv1a(token) % partitions)?.add(token);
    for (const token of tail.removedTokens) buckets.get(fnv1a(token) % partitions)?.delete(token);
  }

  let sizeAfter = 0;
  for (const tokens of buckets.values()) sizeAfter += tokens.size;
  const tokenCount = priorCount - sizeBefore + sizeAfter;
  // A size that now wants a different partition count has to be rehashed in full.
  if (Math.max(1, Math.ceil(tokenCount / UNIQUE_KEY_PARTITION_TARGET)) !== partitions) {
    return undefined;
  }
  for (const [ordinal, tokens] of buckets) {
    store.put([...tokens], uniqueKeyBasePartKey(tableId, ordinal));
  }
  return { partitions, tokenCount };
}

async function readAllV2BaseTokens(
  store: IDBObjectStore,
  tableId: string,
  index: UniqueKeyChunkIndex,
): Promise<Set<string>> {
  if (!index.hasBase) return new Set();
  const partitions = index.partitions ?? 1;
  const parts = await Promise.all(
    Array.from({ length: partitions }, (_, ordinal) =>
      requestResult<unknown>(store.get(uniqueKeyBasePartKey(tableId, ordinal))),
    ),
  );
  const present = new Set<string>();
  for (const part of parts) {
    if (part === undefined) continue;
    for (const token of asBasePartition(part)) present.add(token);
  }
  return present;
}

/**
 * A table's complete unique-key membership: the folded base plus every unfolded tail chunk
 * replayed in commit order. Snapshots need the whole set, where the commit path only ever
 * needs the tokens it is about to write.
 */
async function readAllUniqueKeyTokens(
  store: IDBObjectStore,
  tableId: string,
): Promise<Set<string>> {
  const index = asUniqueKeyChunkIndex(
    await requestResult<unknown>(store.get(uniqueKeyChunkIndexKey(tableId))),
  );
  const present = index.hasBase
    ? await readAllV2BaseTokens(store, tableId, index)
    : new Set<string>();
  const chunks = await Promise.all(
    index.versions.map((version) =>
      requestResult<unknown>(store.get(uniqueKeyChunkKey(tableId, version))),
    ),
  );
  for (const chunk of chunks) applyChunk(present, asUniqueKeyChunk(chunk));
  return present;
}

async function readChunkedUniqueKeys(
  store: IDBObjectStore,
  tableId: string,
  requestedTokens: readonly string[],
): Promise<{
  existing: Set<string>;
  index: UniqueKeyChunkIndex;
  chunks: UniqueKeyChunk[];
  /** The complete membership (base plus tail replay), when this read resolved all of it. */
  fullPresent?: Set<string>;
}> {
  const rawIndex: unknown = await requestResult(store.get(uniqueKeyChunkIndexKey(tableId)));
  const index = asUniqueKeyChunkIndex(rawIndex);
  const bulk = requestedTokens.length >= UNIQUE_KEY_BASE_SCAN_THRESHOLD;
  const chunkReads = Promise.all(
    index.versions.map((version) =>
      requestResult<unknown>(store.get(uniqueKeyChunkKey(tableId, version))),
    ),
  );
  let baseMembership: ReadonlySet<string> | undefined;
  let fullBase: Set<string> | undefined;
  if (index.hasBase) {
    if (bulk) {
      fullBase = await readAllV2BaseTokens(store, tableId, index);
      baseMembership = fullBase;
    } else {
      // Point reads touch only the partitions the requested tokens hash to.
      const partitions = index.partitions ?? 1;
      const ordinals = [...new Set(requestedTokens.map((token) => fnv1a(token) % partitions))];
      const parts = await Promise.all(
        ordinals.map((ordinal) =>
          requestResult<unknown>(store.get(uniqueKeyBasePartKey(tableId, ordinal))),
        ),
      );
      const membership = new Set<string>();
      for (const part of parts) {
        if (part === undefined) continue;
        for (const token of asBasePartition(part)) membership.add(token);
      }
      baseMembership = membership;
    }
  }
  const chunks = (await chunkReads).map(asUniqueKeyChunk);
  const requested = new Set(requestedTokens);
  const existing = new Set<string>();
  if (baseMembership !== undefined) {
    for (const token of requestedTokens) {
      if (baseMembership.has(token)) existing.add(token);
    }
  }
  for (const chunk of chunks) {
    for (const token of chunk.addedTokens) {
      if (requested.has(token)) existing.add(token);
    }
    for (const token of chunk.removedTokens) existing.delete(token);
  }
  let fullPresent: Set<string> | undefined;
  if (fullBase !== undefined || !index.hasBase) {
    fullPresent = fullBase ?? new Set<string>();
    for (const chunk of chunks) applyChunk(fullPresent, chunk);
  }
  return {
    existing,
    index,
    chunks,
    ...(fullPresent === undefined ? {} : { fullPresent }),
  };
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
