import {
  type BeginSnapshotFrameExportInput,
  type SnapshotFrameExportSession,
  type ReadSnapshotExportFrameInput,
  type BeginSnapshotFrameImportInput,
  type SnapshotFrameImportSession,
  type RenewSnapshotFrameImportInput,
  type AppendSnapshotImportFramesInput,
  type FinishSnapshotFrameImportInput,
  type SnapshotFrame,
  type SnapshotFrameKind,
  type SnapshotFrameStreamHeader,
  type SnapshotMetadataItem,
  type BeginTransactionInput,
  type BeginTransactionResult,
  type AbortTransactionIfExpiredInput,
  type CommitTransactionInput,
  type CloseSnapshotExportInput,
  type CompactionJobRecord,
  type CompactionJobRecordUpdate,
  type CreateGarbageCollectionJobInput,
  type UpdateGarbageCollectionPlanningInput,
  type DropTableColumnInput,
  type DropTableInput,
  type BlockStore,
  BlockReadBatchTooLargeError,
  type AdoptAbortedSegmentInput,
  type CatalogProbe,
  type QueryCatalogState,
  type CatalogMutationOptions,
  type CancelSnapshotImportInput,
  type FtsCandidates,
  type FtsPostingQuery,
  type FtsPosting,
  type BeginPostingBuildInput,
  type RenewPostingBuildInput,
  type AppendPostingBuildChunkInput,
  type FinishPostingBuildInput,
  type AbortPostingBuildInput,
  type GarbageCollectionJobRecord,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  type MoveLeaseInput,
  type Manifest,
  type ManifestSummary,
  type RowIdRange,
  type RollbackTransactionArtifactsInput,
  type RenewLeaseInput,
  type RenewTempOwnerInput,
  type RenewTransactionInput,
  type RunGarbageCollectionStepInput,
  type SegmentRecord,
  type InterruptedSnapshotImport,
  type InterruptedSnapshotImportAbortResult,
  type StageTransactionArtifactsInput,
  type StoragePage,
  type StorageStats,
  type TableRecord,
  type TableRecordUpdate,
  type TempOwnerRecord,
  type TempRunPage,
  type TransactionRecord,
  type TransactionRecordUpdate,
  type WriteTransactionInput,
  type BeginUniqueKeyBuildInput,
  type RenewUniqueKeyBuildInput,
  type AppendUniqueKeyBuildChunkInput,
  type FinishUniqueKeyBuildInput,
  type AbortUniqueKeyBuildInput,
  type UniqueKeyBuildRecord,
  MAX_SNAPSHOT_SESSION_TTL_MS,
  MAX_POSTING_BUILD_TTL_MS,
  MAX_SNAPSHOT_FRAME_BATCH_BYTES,
  MAX_SNAPSHOT_FRAME_BATCH_ITEMS,
  MAX_SNAPSHOT_METADATA_BATCH_BYTES,
  SNAPSHOT_FRAME_KINDS,
  MAX_TEMP_BYTES_PER_OWNER,
  MAX_TEMP_BYTES_TOTAL,
  MAX_TEMP_PAGES_PER_OWNER,
  MAX_TEMP_PAGES_TOTAL,
  MAX_TEMP_RUNS_PER_OWNER,
  MAX_TEMP_RUNS_TOTAL,
  PostingBuildConflictError,
  StorageResourceLimitError,
  MAX_BLOCK_READ_BATCH_BYTES,
  SnapshotImportConflictError,
  StorageCorruptionError,
  activePostingStorageColumnIds,
  assertStorageBulkReadItems,
  validateStorageId,
  assertTempRunPageBatchLimits,
} from "./types.js";
import { verifyStoredBlock } from "../block-format/index.js";
import { crc32 } from "../block-format/checksum.js";
import {
  decodeSnapshotMetadataItems,
  encodeSnapshotFrameStreamFooter,
  encodeSnapshotFrameStreamHeader,
  encodeSnapshotMetadataPage,
  extendSnapshotFrameStreamChecksum,
  prepareSnapshotFrameStreamHeader,
  snapshotFrameEnvelopeParts,
  snapshotFrameStreamHeaderIdentity,
} from "./snapshot-stream.js";
import {
  RecordCore,
  validateFtsPostingChunks,
  validateId,
  validateTempRunPage,
  validateTempRunPageIdentity,
} from "./toolkit/record-core.js";

interface MemorySnapshotFrameExportState {
  session: SnapshotFrameExportSession;
  manifestVersion: number;
  metadataFrames: SnapshotFrame[];
  nextSequence: number;
  lastBlockId: string | null;
  lastFrame?: SnapshotFrame;
}

interface SnapshotObservedSummary {
  frameCount: number;
  itemCount: number;
  storedBytes: number;
}

interface MemorySnapshotFrameImportState {
  session: SnapshotFrameImportSession;
  header: SnapshotFrameStreamHeader;
  metadataFrames: SnapshotFrame[];
  observed: Record<SnapshotFrameKind, SnapshotObservedSummary>;
  checksum: number;
  itemCount: number;
  previousKindIndex: number;
  lastBlockId: string | null;
  lastBatchFrames: SnapshotFrame[];
  completedReplay: boolean;
}

/**
 * The in-process store: record semantics live in `RecordCore` (shared with the OPFS store),
 * block and temp-page bytes live in Maps here, and atomicity comes from running every mutating
 * record operation on a promise-chain queue — each queued body is synchronous, so no operation
 * ever observes another mid-mutation.
 */
export class MemoryBlockStore implements BlockStore {
  readonly #blocks = new Map<string, Uint8Array>();
  readonly #blockChecksums = new Map<string, number>();
  #physicalBlockBytes = 0;
  readonly #tempRunPages = new Map<string, Uint8Array>();
  readonly #tempUsageByOwner = new Map<
    string,
    { bytes: number; pages: number; runs: Map<string, number> }
  >();
  readonly #tempPageKeysByRun = new Map<string, Set<string>>();
  #tempBytesTotal = 0;
  #tempPagesTotal = 0;
  #tempRunsTotal = 0;
  readonly #ftsBaseBuilds = new Map<
    string,
    {
      buildId: string;
      ownerId: string;
      createdAt: string;
      expiresAt: string;
      chunks: Map<number, FtsPosting[]>;
    }
  >();
  #snapshotFrameExport: MemorySnapshotFrameExportState | undefined;
  #snapshotFrameImport: MemorySnapshotFrameImportState | undefined;
  #completedSnapshotFrameImport:
    { identity: string; version: number; header: SnapshotFrameStreamHeader } | undefined;
  readonly #core = new RecordCore({
    hasBlock: (id) => this.#blocks.has(id),
    blockByteLength: (id) => this.#blocks.get(id)?.byteLength,
    blockChecksum: (id) => this.#blockChecksums.get(id),
  });
  #commitQueue = Promise.resolve();

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    validateStorageId(id, "Block ID");
    const bytes = this.#blocks.get(id);
    return bytes === undefined ? undefined : new Uint8Array(bytes);
  }

  async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    assertStorageBulkReadItems(ids, "Block read batch");
    for (const id of ids) validateStorageId(id, "Block ID");
    let bytes = 0;
    for (const id of ids) {
      bytes += this.#blocks.get(id)?.byteLength ?? 0;
      if (!Number.isSafeInteger(bytes) || bytes > MAX_BLOCK_READ_BATCH_BYTES) {
        throw new BlockReadBatchTooLargeError(bytes);
      }
    }
    return ids.map((id) => {
      const bytes = this.#blocks.get(id);
      return bytes === undefined ? undefined : new Uint8Array(bytes);
    });
  }

  async readManifestBlock(version: number | null, id: string): Promise<Uint8Array | undefined> {
    validateStorageId(id, "Block ID");
    return this.#runAtomic(() => {
      if (!this.#core.hasManifestBlocks(version, [id])[0]) return undefined;
      const bytes = this.#blocks.get(id);
      if (bytes === undefined) {
        throw new StorageCorruptionError("memory", id, "manifest member payload is missing");
      }
      return new Uint8Array(bytes);
    });
  }

  async hasManifestBlocks(version: number | null, ids: readonly string[]): Promise<boolean[]> {
    assertStorageBulkReadItems(ids, "Manifest membership batch");
    return this.#runAtomic(() => this.#core.hasManifestBlocks(version, ids));
  }

  async putTempRunPage(page: TempRunPage): Promise<void> {
    assertTempRunPageBatchLimits([page]);
    validateTempRunPage(page);
    return this.#runAtomic(() => this.#putTempRunPages([page]));
  }

  async putTempRunPages(pages: readonly TempRunPage[]): Promise<void> {
    assertTempRunPageBatchLimits(pages);
    for (const page of pages) validateTempRunPage(page);
    return this.#runAtomic(() => this.#putTempRunPages(pages));
  }

  async getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    validateTempRunPageIdentity(ownerId, runId, pageIndex);
    return this.#tempRunPages.get(tempRunPageKey(ownerId, runId, pageIndex))?.slice();
  }

  async removeTempRun(ownerId: string, runId: string): Promise<void> {
    validateTempRunPageIdentity(ownerId, runId, 0);
    return this.#runAtomic(() => this.#removeTempRunPages(ownerId, runId));
  }

  async removeTempOwner(ownerId: string): Promise<void> {
    validateId(ownerId);
    return this.#runAtomic(() => {
      this.#removeTempOwnerPages(ownerId);
      this.#core.removeTempOwner(ownerId);
    });
  }

  async createTempOwner(record: TempOwnerRecord): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.createTempOwner(record);
    });
  }

  async getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined> {
    return this.#core.getTempOwner(ownerId);
  }

  async renewTempOwner(input: RenewTempOwnerInput): Promise<TempOwnerRecord> {
    return this.#runAtomic(() => this.#core.renewTempOwner(input));
  }

  async removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean> {
    return this.#runAtomic(() => {
      const removed = this.#core.removeTempOwnerIfExpired(ownerId, expiresAtCutoff);
      if (removed) this.#removeTempOwnerPages(ownerId);
      return removed;
    });
  }

  async listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    const pageOwnerIds: string[] = [];
    for (const key of this.#tempRunPages.keys()) {
      const separator = key.indexOf(":");
      const length = Number(key.slice(0, separator));
      pageOwnerIds.push(key.slice(separator + 1, separator + 1 + length));
    }
    return this.#core.listTempOwnerIdsPage(afterOwnerId, limit, pageOwnerIds);
  }

  async listExpiredTempOwnerPage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    return this.#core.listExpiredTempOwnerPage(expiresAtCutoff, afterCursor, limit);
  }

  #putTempRunPages(pages: readonly TempRunPage[]): void {
    const seenKeys = new Set<string>();
    const deltas = new Map<string, { bytes: number; pages: number; newRuns: Set<string> }>();
    let totalBytesDelta = 0;
    let totalPagesDelta = 0;
    let totalRunsDelta = 0;
    for (const page of pages) {
      if (this.#core.getTempOwner(page.ownerId) === undefined) {
        throw new Error(`Temp owner does not exist: ${page.ownerId}`);
      }
      const key = tempRunPageKey(page.ownerId, page.runId, page.pageIndex);
      if (seenKeys.has(key)) throw new TypeError(`Temp run page batch repeats ${key}`);
      seenKeys.add(key);
      const previousBytes = this.#tempRunPages.get(key)?.byteLength ?? 0;
      const isNewPage = !this.#tempRunPages.has(key);
      const delta = deltas.get(page.ownerId) ?? { bytes: 0, pages: 0, newRuns: new Set() };
      delta.bytes += page.bytes.byteLength - previousBytes;
      if (isNewPage) delta.pages += 1;
      const usage = this.#tempUsageByOwner.get(page.ownerId);
      if (usage?.runs.has(page.runId) !== true && !delta.newRuns.has(page.runId)) {
        delta.newRuns.add(page.runId);
      }
      deltas.set(page.ownerId, delta);
      totalBytesDelta += page.bytes.byteLength - previousBytes;
      if (isNewPage) totalPagesDelta += 1;
    }
    for (const [ownerId, delta] of deltas) {
      const usage = this.#tempUsageByOwner.get(ownerId);
      const nextBytes = (usage?.bytes ?? 0) + delta.bytes;
      const nextPages = (usage?.pages ?? 0) + delta.pages;
      const nextRuns = (usage?.runs.size ?? 0) + delta.newRuns.size;
      assertTempResourceLimit("temp owner byte", nextBytes, MAX_TEMP_BYTES_PER_OWNER);
      assertTempResourceLimit("temp page", nextPages, MAX_TEMP_PAGES_PER_OWNER);
      assertTempResourceLimit("temp run", nextRuns, MAX_TEMP_RUNS_PER_OWNER);
      totalRunsDelta += delta.newRuns.size;
    }
    assertTempResourceLimit(
      "temporary byte",
      this.#tempBytesTotal + totalBytesDelta,
      MAX_TEMP_BYTES_TOTAL,
    );
    assertTempResourceLimit(
      "temporary page total",
      this.#tempPagesTotal + totalPagesDelta,
      MAX_TEMP_PAGES_TOTAL,
    );
    assertTempResourceLimit(
      "temporary run total",
      this.#tempRunsTotal + totalRunsDelta,
      MAX_TEMP_RUNS_TOTAL,
    );
    for (const page of pages) {
      const key = tempRunPageKey(page.ownerId, page.runId, page.pageIndex);
      const previousBytes = this.#tempRunPages.get(key)?.byteLength ?? 0;
      const isNewPage = !this.#tempRunPages.has(key);
      const usage = this.#tempUsageByOwner.get(page.ownerId) ?? {
        bytes: 0,
        pages: 0,
        runs: new Map<string, number>(),
      };
      const previousRunPages = usage.runs.get(page.runId) ?? 0;
      this.#tempRunPages.set(key, page.bytes.slice());
      const runKey = tempRunPagePrefix(page.ownerId, page.runId);
      const runKeys = this.#tempPageKeysByRun.get(runKey) ?? new Set<string>();
      runKeys.add(key);
      this.#tempPageKeysByRun.set(runKey, runKeys);
      usage.bytes += page.bytes.byteLength - previousBytes;
      this.#tempBytesTotal += page.bytes.byteLength - previousBytes;
      if (isNewPage) {
        usage.pages += 1;
        this.#tempPagesTotal += 1;
        usage.runs.set(page.runId, previousRunPages + 1);
        if (previousRunPages === 0) this.#tempRunsTotal += 1;
      }
      this.#tempUsageByOwner.set(page.ownerId, usage);
    }
  }

  #removeTempRunPages(ownerId: string, runId: string): void {
    const runKey = tempRunPagePrefix(ownerId, runId);
    const keys = this.#tempPageKeysByRun.get(runKey);
    if (keys === undefined) return;
    const usage = this.#tempUsageByOwner.get(ownerId);
    let removedBytes = 0;
    for (const key of keys) {
      removedBytes += this.#tempRunPages.get(key)?.byteLength ?? 0;
      this.#tempRunPages.delete(key);
    }
    this.#tempPageKeysByRun.delete(runKey);
    this.#tempBytesTotal -= removedBytes;
    this.#tempPagesTotal -= keys.size;
    this.#tempRunsTotal -= 1;
    if (usage !== undefined) {
      usage.bytes -= removedBytes;
      usage.pages -= keys.size;
      usage.runs.delete(runId);
      if (usage.pages === 0) this.#tempUsageByOwner.delete(ownerId);
    }
  }

  #removeTempOwnerPages(ownerId: string): void {
    const usage = this.#tempUsageByOwner.get(ownerId);
    if (usage === undefined) return;
    for (const runId of [...usage.runs.keys()]) this.#removeTempRunPages(ownerId, runId);
  }

  async addTable(record: TableRecord, options: CatalogMutationOptions = {}): Promise<void> {
    this.#core.addTable(record, options);
  }

  async getTable(id: string): Promise<TableRecord | undefined> {
    return this.#core.getTable(id);
  }

  async updateTable(
    id: string,
    expectedRevision: number,
    update: TableRecordUpdate,
  ): Promise<TableRecord> {
    return this.#runAtomic(() => {
      const before = this.#core.getTable(id);
      const updated = this.#core.updateTable(id, expectedRevision, update);
      if (before !== undefined) this.#removeRetiredPostingBuilds(before, updated);
      return updated;
    });
  }

  async removeTable(
    id: string,
    expectedRevision: number,
    options: CatalogMutationOptions = {},
  ): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.removeTable(id, expectedRevision, options);
      this.#removeTablePostingBuilds(id);
    });
  }

  async dropTable(input: DropTableInput): Promise<ManifestSummary> {
    return this.#runAtomic(() => {
      const summary = this.#core.dropTable(input);
      this.#removeTablePostingBuilds(input.tableId);
      return summary;
    });
  }

  async dropTableColumn(input: DropTableColumnInput): Promise<ManifestSummary> {
    return this.#runAtomic(() => {
      const summary = this.#core.dropTableColumn(input);
      this.#ftsBaseBuilds.delete(`${input.tableId}/${input.columnId}`);
      return summary;
    });
  }

  #removeRetiredPostingBuilds(before: TableRecord, after: TableRecord): void {
    const retained = new Set([
      ...Object.keys(after.ftsColumns ?? {}),
      ...Object.values(after.secondaryIndexes ?? {}).map((index) => index.storageColumnId),
    ]);
    for (const columnId of [
      ...Object.keys(before.ftsColumns ?? {}),
      ...Object.values(before.secondaryIndexes ?? {}).map((index) => index.storageColumnId),
    ]) {
      if (!retained.has(columnId)) this.#ftsBaseBuilds.delete(`${before.id}/${columnId}`);
    }
  }

  #removeTablePostingBuilds(tableId: string): void {
    const prefix = `${tableId}/`;
    for (const key of [...this.#ftsBaseBuilds.keys()]) {
      if (key.startsWith(prefix)) this.#ftsBaseBuilds.delete(key);
    }
  }

  async writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.writeFtsBase(tableId, columnId, input);
    });
  }

  async removeFtsColumn(tableId: string, columnId: string): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.removeFtsColumn(tableId, columnId);
      this.#ftsBaseBuilds.delete(`${tableId}/${columnId}`);
    });
  }

  async beginFtsBaseBuild(input: BeginPostingBuildInput): Promise<void> {
    validatePostingBuildLifetime(input.createdAt, input.expiresAt);
    validateId(input.tableId);
    validateId(input.columnId);
    validateId(input.buildId);
    validateId(input.ownerId);
    return this.#runAtomic(() => {
      const key = `${input.tableId}/${input.columnId}`;
      const current = this.#ftsBaseBuilds.get(key);
      if (current !== undefined && Date.parse(current.expiresAt) > Date.parse(input.createdAt)) {
        if (current.buildId === input.buildId && current.ownerId === input.ownerId) return;
        throw new PostingBuildConflictError(
          current.buildId,
          current.ownerId,
          "another live build owns the column",
        );
      }
      const table = this.#core.getTable(input.tableId);
      if (table === undefined || !activePostingStorageColumnIds(table).has(input.columnId)) {
        throw new Error(`Postings index is no longer active: ${key}`);
      }
      this.#ftsBaseBuilds.set(key, {
        buildId: input.buildId,
        ownerId: input.ownerId,
        createdAt: input.createdAt,
        expiresAt: input.expiresAt,
        chunks: new Map(),
      });
    });
  }

  async renewFtsBaseBuild(input: RenewPostingBuildInput): Promise<void> {
    validatePostingBuildLifetime(input.expiresAtCutoff, input.expiresAt);
    return this.#runAtomic(() => {
      const build = this.#requirePostingBuild(input);
      build.expiresAt = input.expiresAt;
    });
  }

  async writeFtsBaseBuildChunk(input: AppendPostingBuildChunkInput): Promise<void> {
    validatePostingBuildLifetime(input.expiresAtCutoff, input.expiresAt);
    return this.#runAtomic(() => {
      const build = this.#requirePostingBuild(input);
      if (!Number.isSafeInteger(input.ordinal) || input.ordinal < 0) {
        throw new RangeError(`Full-text base chunk ordinal is invalid: ${String(input.ordinal)}`);
      }
      validateFtsPostingChunks([input.chunk], "Full-text base build");
      const replay = build.chunks.get(input.ordinal);
      if (replay !== undefined) {
        if (!samePostingChunk(replay, input.chunk)) {
          throw new PostingBuildConflictError(input.buildId, input.ownerId, "chunk replay changed");
        }
        build.expiresAt = input.expiresAt;
        return;
      }
      if (input.ordinal !== build.chunks.size) {
        throw new Error(`Full-text base chunk is out of order: ${String(input.ordinal)}`);
      }
      // Chunks are row windows, not term partitions. Terms are sorted inside each chunk, but
      // adjacent chunks can legitimately overlap or restart at an earlier term.
      build.chunks.set(input.ordinal, structuredClone(input.chunk) as FtsPosting[]);
      build.expiresAt = input.expiresAt;
    });
  }

  async finishFtsBaseBuild(input: FinishPostingBuildInput): Promise<void> {
    return this.#runAtomic(() => {
      const key = `${input.tableId}/${input.columnId}`;
      const build = this.#requirePostingBuild(input);
      if (
        !Number.isSafeInteger(input.coversVersion) ||
        input.coversVersion < -1 ||
        !Number.isSafeInteger(input.chunkCount) ||
        input.chunkCount < 0 ||
        !Number.isSafeInteger(input.totalTokens) ||
        input.totalTokens < 0
      ) {
        throw new TypeError("Full-text base build metadata is invalid");
      }
      if (input.chunkCount !== build.chunks.size) {
        throw new Error(`Full-text base build is incomplete: ${input.buildId}`);
      }
      const table = this.#core.getTable(input.tableId);
      if (table === undefined || !activePostingStorageColumnIds(table).has(input.columnId)) {
        this.#ftsBaseBuilds.delete(key);
        return;
      }
      const chunks = Array.from({ length: input.chunkCount }, (_, ordinal) => {
        const chunk = build.chunks.get(ordinal);
        if (chunk === undefined) {
          throw new Error(`Full-text base chunk is missing: ${String(ordinal)}`);
        }
        return chunk;
      });
      this.#core.writeFtsBase(input.tableId, input.columnId, {
        coversVersion: input.coversVersion,
        chunks,
        totalTokens: input.totalTokens,
      });
      this.#ftsBaseBuilds.delete(key);
    });
  }

  async abortFtsBaseBuild(input: AbortPostingBuildInput): Promise<void> {
    return this.#runAtomic(() => {
      const key = `${input.tableId}/${input.columnId}`;
      const build = this.#ftsBaseBuilds.get(key);
      if (build === undefined) return;
      if (
        build.buildId !== input.buildId ||
        (build.ownerId !== input.ownerId &&
          Date.parse(build.expiresAt) > Date.parse(input.expiresAtCutoff))
      ) {
        throw new PostingBuildConflictError(
          build.buildId,
          build.ownerId,
          "abort ownership changed",
        );
      }
      this.#ftsBaseBuilds.delete(key);
    });
  }

  #requirePostingBuild(input: {
    tableId: string;
    columnId: string;
    buildId: string;
    ownerId: string;
    expiresAtCutoff: string;
  }): {
    buildId: string;
    ownerId: string;
    createdAt: string;
    expiresAt: string;
    chunks: Map<number, FtsPosting[]>;
  } {
    const build = this.#ftsBaseBuilds.get(`${input.tableId}/${input.columnId}`);
    if (build?.buildId !== input.buildId || build.ownerId !== input.ownerId) {
      throw new PostingBuildConflictError(
        build?.buildId ?? input.buildId,
        build?.ownerId ?? input.ownerId,
        "session is missing or owned by another caller",
      );
    }
    if (Date.parse(build.expiresAt) <= Date.parse(input.expiresAtCutoff)) {
      throw new PostingBuildConflictError(build.buildId, build.ownerId, "session is expired");
    }
    return build;
  }

  async readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: readonly FtsPostingQuery[],
    upToVersion: number,
    maxRowIds?: number,
  ): Promise<
    FtsCandidates & {
      deltaChunkCount: number;
      totalTokens: number;
      coversVersion: number;
      hasBase: boolean;
    }
  > {
    return this.#core.readFtsCandidates(tableId, columnId, terms, upToVersion, maxRowIds);
  }

  async readFtsPostings(
    tableId: string,
    columnId: string,
    upToVersion: number,
    maxRowIds?: number,
    maxRetainedBytes?: number,
  ) {
    return this.#core.readFtsPostings(tableId, columnId, upToVersion, maxRowIds, maxRetainedBytes);
  }

  async getTableByName(name: string): Promise<TableRecord | undefined> {
    const id = this.#core.getTableIdByName(name);
    return id === undefined ? undefined : this.getTable(id);
  }

  async listTables(): Promise<TableRecord[]> {
    return this.#core.listTables();
  }

  async getSegment(id: string): Promise<SegmentRecord | undefined> {
    return this.#core.getSegment(id);
  }

  async listSegmentPage(afterId: string | null, limit: number) {
    return this.#core.listSegmentPage(afterId, limit);
  }

  async listTableSegmentPage(tableId: string, afterId: string | null, limit: number) {
    return this.#core.listTableSegmentPage(tableId, afterId, limit);
  }

  async removeAbortedSegment(id: string, expectedTransactionId: string): Promise<boolean> {
    return this.#runAtomic(() => this.#core.removeAbortedSegment(id, expectedTransactionId));
  }

  async adoptAbortedSegment(input: AdoptAbortedSegmentInput): Promise<TransactionRecord> {
    return this.#runAtomic(() => this.#core.adoptAbortedSegment(input));
  }

  async reserveRowIds(tableId: string, count: number): Promise<RowIdRange> {
    return this.#core.reserveRowIds(tableId, count);
  }

  async reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): Promise<RowIdRange> {
    return this.#runAtomic(() =>
      this.#core.reserveAutoIncrement(tableId, columnId, count, atLeast),
    );
  }

  async getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    assertStorageBulkReadItems(keyTokens, "Unique-key lookup batch");
    return this.#core.getExistingUniqueKeys(tableId, keyTokens);
  }

  async beginUniqueKeyBuild(input: BeginUniqueKeyBuildInput): Promise<UniqueKeyBuildRecord> {
    return this.#runAtomic(() => this.#core.beginUniqueKeyBuild(input));
  }

  async getUniqueKeyBuild(buildId: string): Promise<UniqueKeyBuildRecord | undefined> {
    return this.#core.getUniqueKeyBuild(buildId);
  }

  async renewUniqueKeyBuild(input: RenewUniqueKeyBuildInput): Promise<UniqueKeyBuildRecord> {
    return this.#runAtomic(() => this.#core.renewUniqueKeyBuild(input));
  }

  async appendUniqueKeyBuildChunk(
    input: AppendUniqueKeyBuildChunkInput,
  ): Promise<UniqueKeyBuildRecord> {
    return this.#runAtomic(() => this.#core.appendUniqueKeyBuildChunk(input));
  }

  async finishUniqueKeyBuild(input: FinishUniqueKeyBuildInput): Promise<TableRecord> {
    return this.#runAtomic(() => this.#core.finishUniqueKeyBuild(input));
  }

  async abortUniqueKeyBuild(input: AbortUniqueKeyBuildInput): Promise<boolean> {
    return this.#runAtomic(() => this.#core.abortUniqueKeyBuild(input));
  }

  async getCurrentManifestVersion(): Promise<number | null> {
    return this.#core.getCurrentManifestVersion();
  }

  async getCatalogProbe(): Promise<CatalogProbe> {
    return this.#core.getCatalogProbe();
  }

  async getQueryCatalogState(names: readonly string[]): Promise<QueryCatalogState> {
    assertStorageBulkReadItems(names, "Query catalog table batch");
    return this.#runAtomic(() => {
      const probe = this.#core.getCatalogProbe();
      const tables = names.map((name) => {
        const id = this.#core.getTableIdByName(name);
        return id === undefined ? undefined : this.#core.getTable(id);
      });
      const segments: SegmentRecord[] = [];
      for (const table of tables) {
        if (table === undefined) continue;
        let cursor: string | null = null;
        do {
          const page = this.#core.listTableSegmentPage(table.id, cursor, 1_024);
          segments.push(...page.records);
          cursor = page.nextCursor;
        } while (cursor !== null);
      }
      segments.sort((left, right) => left.id.localeCompare(right.id));
      const transactionIds = [...new Set(segments.map((segment) => segment.transactionId))];
      const transactions: TransactionRecord[] = [];
      for (let start = 0; start < transactionIds.length; start += 64) {
        for (const record of this.#core.getTransactions(transactionIds.slice(start, start + 64))) {
          if (record !== undefined) transactions.push(record);
        }
      }
      return {
        manifestVersion: probe.manifestVersion,
        tables,
        segments,
        transactions,
        catalogEpoch: probe.catalogEpoch,
      };
    });
  }

  async getCurrentManifest(): Promise<Manifest | undefined> {
    return this.#core.getCurrentManifest();
  }

  async getManifest(version: number): Promise<Manifest | undefined> {
    return this.#core.getManifest(version);
  }

  async listManifestBlockPage(input: Parameters<BlockStore["listManifestBlockPage"]>[0]) {
    return this.#core.listManifestBlockPage(input);
  }

  async listRetiredManifestBlockPage(
    input: Parameters<BlockStore["listRetiredManifestBlockPage"]>[0],
  ) {
    return this.#core.listRetiredManifestBlockPage(input);
  }

  async listManifestPage(afterVersion: number | null, limit: number) {
    return this.#core.listManifestPage(afterVersion, limit);
  }

  async beginTransaction(input: BeginTransactionInput): Promise<BeginTransactionResult> {
    return this.#runAtomic(() => this.#core.beginTransaction(input));
  }

  async createTransaction(record: TransactionRecord): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.createTransaction(record);
    });
  }

  async getTransaction(id: string): Promise<TransactionRecord | undefined> {
    return this.#core.getTransaction(id);
  }

  async getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>> {
    assertStorageBulkReadItems(ids, "Transaction read batch");
    return this.#core.getTransactions(ids);
  }

  async listTransactionPage(afterId: string | null, limit: number) {
    return this.#core.listTransactionPage(afterId, limit);
  }

  async updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    return this.#runAtomic(() => this.#core.updateTransaction(id, expectedRevision, update));
  }

  async renewTransaction(input: RenewTransactionInput): Promise<boolean> {
    return this.#runAtomic(() => this.#core.renewTransaction(input));
  }

  async abortTransactionIfExpired(
    input: AbortTransactionIfExpiredInput,
  ): Promise<TransactionRecord | undefined> {
    return this.#runAtomic(() => this.#core.abortTransactionIfExpired(input));
  }

  async stageTransactionArtifacts(
    input: StageTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    return this.#runAtomic(() => {
      const updated = this.#core.stageTransactionArtifacts(input);
      // The record half validated everything; the bytes land in this same atomic step.
      for (const block of input.blocks) this.#putBlock(block.id, block.bytes);
      return updated;
    });
  }

  async rollbackTransactionArtifacts(
    input: RollbackTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    return this.#runAtomic(() => {
      const updated = this.#core.rollbackTransactionArtifacts(input);
      for (const id of input.removeBlockIds) this.#deleteBlock(id);
      return updated;
    });
  }

  async commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary> {
    return this.#runAtomic(() => this.#core.commitTransaction(input));
  }

  async writeTransaction(input: WriteTransactionInput): Promise<ManifestSummary> {
    return this.#runAtomic(() => {
      const summary = this.#core.writeTransaction(input);
      // The record half validated everything; the bytes land in this same atomic step.
      for (const block of input.blocks) this.#putBlock(block.id, block.bytes);
      return summary;
    });
  }

  async createLease(record: LeaseRecord): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.createLease(record);
    });
  }

  async getLease(id: string): Promise<LeaseRecord | undefined> {
    return this.#core.getLease(id);
  }

  async listLeases(): Promise<LeaseRecord[]> {
    return this.#core.listLeases();
  }

  async listExpiredLeasePage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): Promise<StoragePage<LeaseRecord, string>> {
    return this.#core.listExpiredLeasePage(expiresAtCutoff, afterCursor, limit);
  }

  async renewLease(input: RenewLeaseInput): Promise<LeaseRecord> {
    return this.#runAtomic(() => this.#core.renewLease(input));
  }

  async moveLease(input: MoveLeaseInput): Promise<LeaseRecord> {
    return this.#runAtomic(() => this.#core.moveLease(input));
  }

  async removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    return this.#runAtomic(() =>
      this.#core.removeLeaseIfExpired(id, expectedRevision, expiresAtCutoff),
    );
  }

  async removeLease(input: { id: string; ownerId: string }): Promise<boolean> {
    return this.#runAtomic(() => this.#core.removeLease(input));
  }

  async createCompactionJob(record: CompactionJobRecord): Promise<void> {
    this.#core.createCompactionJob(record);
  }

  async getCompactionJob(id: string): Promise<CompactionJobRecord | undefined> {
    return this.#core.getCompactionJob(id);
  }

  async listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]> {
    return this.#core.listCompactionJobs(tableId);
  }

  async listCompactionJobPage(afterId: string | null, limit: number) {
    return this.#core.listCompactionJobPage(afterId, limit);
  }

  async updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord> {
    return this.#core.updateCompactionJob(id, expectedRevision, update);
  }

  async cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord> {
    return this.#runAtomic(() => this.#core.cancelCompactionJob(id, expectedRevision, cancelledAt));
  }

  async removeCompactionJob(id: string): Promise<boolean> {
    return this.#core.removeCompactionJob(id);
  }

  async createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord> {
    return this.#runAtomic(() => this.#core.createGarbageCollectionJob(input));
  }

  async updateGarbageCollectionPlanning(
    input: UpdateGarbageCollectionPlanningInput,
  ): Promise<GarbageCollectionJobRecord> {
    return this.#runAtomic(() => this.#core.updateGarbageCollectionPlanning(input));
  }

  async getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    return this.#core.getGarbageCollectionJob(id);
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return this.#core.listGarbageCollectionJobs();
  }

  async listGarbageCollectionJobPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<GarbageCollectionJobRecord, string>> {
    return this.#core.listGarbageCollectionJobPage(afterId, limit);
  }

  async runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    return this.#runAtomic(() => {
      const result = this.#core.runGarbageCollectionStep(input);
      result.reclaimedBlockIds.forEach((id) => this.#deleteBlock(id));
      return result;
    });
  }

  async removePrunedManifestRecords(maxItems: number): Promise<number> {
    return this.#runAtomic(() => this.#core.removePrunedManifestRecords(maxItems));
  }

  async removeGarbageCollectionJob(id: string): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.removeGarbageCollectionJob(id);
    });
  }

  async beginSnapshotFrameExport(
    input: BeginSnapshotFrameExportInput,
  ): Promise<SnapshotFrameExportSession> {
    validateSnapshotSessionLifetime(input.createdAt, input.expiresAt);
    validateId(input.ownerId);
    return this.#runAtomic(() => {
      const current = this.#snapshotFrameExport;
      if (current !== undefined) {
        if (Date.parse(current.session.expiresAt) > Date.parse(input.createdAt)) {
          throw new SnapshotImportConflictError(
            current.session.sessionId,
            current.session.ownerId,
            "another snapshot export is still live",
          );
        }
        this.#core.removeLease({
          id: current.session.sessionId,
          ownerId: current.session.ownerId,
        });
        this.#snapshotFrameExport = undefined;
      }
      const manifest = this.#core.snapshotFrameManifest();
      const metadataFrames: SnapshotFrame[] = [];
      const summaries = emptySnapshotObservedSummaries();
      const append = (item: SnapshotMetadataItem): void => {
        const payload = encodeSnapshotMetadataPage([item]);
        const kind = snapshotMetadataFrameKind(item);
        const frame: SnapshotFrame = {
          sequence: metadataFrames.length,
          kind,
          itemCount: 1,
          key: null,
          payload,
          checksum: crc32(payload),
        };
        metadataFrames.push(frame);
        const summary = summaries[kind];
        summary.frameCount += 1;
        summary.itemCount += 1;
        summary.storedBytes = safeSnapshotByteSum(summary.storedBytes, payload.byteLength);
      };
      for (const item of this.#core.snapshotFrameMetadataItems()) append(item);
      for (const item of this.#core.snapshotFramePostingItems()) append(item);
      summaries.block = {
        frameCount: manifest.liveBlockCount,
        itemCount: manifest.liveBlockCount,
        storedBytes: manifest.liveBlockBytes,
      };
      const header = prepareSnapshotFrameStreamHeader({
        formatVersion: 1,
        databaseVersion: manifest.version,
        createdAt: input.createdAt,
        kinds: summaries,
      });
      const sessionId = `snapshot-export/${crypto.randomUUID()}`;
      this.#core.createLease({
        id: sessionId,
        kind: "backup",
        manifestVersion: manifest.version,
        ownerId: input.ownerId,
        createdAt: input.createdAt,
        expiresAt: input.expiresAt,
        revision: 0,
      });
      const session = { sessionId, ownerId: input.ownerId, expiresAt: input.expiresAt, header };
      this.#snapshotFrameExport = {
        session,
        manifestVersion: manifest.version,
        metadataFrames,
        nextSequence: 0,
        lastBlockId: null,
      };
      return structuredClone(session);
    });
  }

  async readSnapshotExportFrame(
    input: ReadSnapshotExportFrameInput,
  ): Promise<SnapshotFrame | undefined> {
    validateSnapshotSessionLifetime(input.expiresAtCutoff, input.expiresAt);
    if (!Number.isSafeInteger(input.sequence) || input.sequence < 0) {
      throw new RangeError("Snapshot export sequence is invalid");
    }
    return this.#runAtomic(() => {
      const state = this.#snapshotFrameExport;
      if (state?.session.sessionId !== input.sessionId || state.session.ownerId !== input.ownerId) {
        throw new SnapshotImportConflictError(
          input.sessionId,
          input.ownerId,
          "export session is missing or owned by another caller",
        );
      }
      if (input.sequence === state.nextSequence - 1 && state.lastFrame !== undefined) {
        return copySnapshotFrame(state.lastFrame);
      }
      if (input.sequence !== state.nextSequence) {
        throw new RangeError("Snapshot export frames must be pulled in contiguous order");
      }
      const lease = this.#core.getLease(input.sessionId);
      if (lease === undefined) {
        throw new SnapshotImportConflictError(input.sessionId, input.ownerId, "lease is missing");
      }
      this.#core.renewLease({
        id: lease.id,
        expectedRevision: lease.revision,
        expiresAtCutoff: input.expiresAtCutoff,
        expiresAt: input.expiresAt,
      });
      state.session = { ...state.session, expiresAt: input.expiresAt };
      const metadata = state.metadataFrames[input.sequence];
      let frame: SnapshotFrame | undefined;
      if (metadata !== undefined) {
        frame = metadata;
      } else {
        const blockPage = this.#core.listManifestBlockPage({
          version: state.manifestVersion,
          afterBlockId: state.lastBlockId,
          limit: 1,
        });
        const record = blockPage.records[0];
        if (record === undefined) return undefined;
        const bytes = this.#blocks.get(record.blockId);
        if (bytes?.byteLength !== record.byteLength || crc32(bytes) !== record.checksum) {
          throw new StorageCorruptionError(
            "memory",
            record.blockId,
            "snapshot block disagrees with its manifest provenance",
          );
        }
        frame = {
          sequence: input.sequence,
          kind: "block",
          itemCount: 1,
          key: record.blockId,
          payload: bytes.slice(),
          checksum: record.checksum,
        };
        state.lastBlockId = record.blockId;
      }
      state.lastFrame = copySnapshotFrame(frame);
      state.nextSequence += 1;
      return copySnapshotFrame(frame);
    });
  }

  async closeSnapshotFrameExport(input: CloseSnapshotExportInput): Promise<boolean> {
    return this.#runAtomic(() => {
      const state = this.#snapshotFrameExport;
      if (state === undefined) return false;
      if (state.session.sessionId !== input.sessionId || state.session.ownerId !== input.ownerId) {
        throw new SnapshotImportConflictError(
          input.sessionId,
          input.ownerId,
          "export session belongs to another caller",
        );
      }
      this.#core.removeLease({ id: input.sessionId, ownerId: input.ownerId });
      this.#snapshotFrameExport = undefined;
      return true;
    });
  }

  async beginSnapshotFrameImport(
    input: BeginSnapshotFrameImportInput,
  ): Promise<SnapshotFrameImportSession> {
    validateSnapshotSessionLifetime(input.createdAt, input.expiresAt);
    validateId(input.ownerId);
    const header = prepareSnapshotFrameStreamHeader(input.header);
    if (snapshotFrameStreamHeaderIdentity(header) !== input.identity) {
      throw new TypeError("Snapshot import identity does not match its canonical header");
    }
    return this.#runAtomic(() => {
      const current = this.#snapshotFrameImport;
      if (current !== undefined) {
        const expired = Date.parse(current.session.expiresAt) <= Date.parse(input.createdAt);
        const sameIdentity = current.session.identity === input.identity;
        const sameHeader = sameSnapshotFrameHeader(current.header, header);
        if (expired && (!sameIdentity || !sameHeader)) {
          if (!current.completedReplay) this.#clearPhysicalBlocks();
          this.#snapshotFrameImport = undefined;
        } else {
          if (!expired && current.session.identity !== input.identity) {
            throw new SnapshotImportConflictError(
              current.session.identity,
              current.session.ownerId,
              "another snapshot import is still live",
            );
          }
          if (!expired && current.session.ownerId !== input.ownerId) {
            throw new SnapshotImportConflictError(
              current.session.identity,
              current.session.ownerId,
              "snapshot import is owned by another caller",
            );
          }
          if (!sameHeader) {
            throw new SnapshotImportConflictError(
              current.session.identity,
              current.session.ownerId,
              "snapshot import header changed",
            );
          }
          current.session = {
            ...current.session,
            ownerId: input.ownerId,
            expiresAt: input.expiresAt,
          };
          return structuredClone(current.session);
        }
      }
      const completed = this.#completedSnapshotFrameImport;
      const completedReplay =
        completed?.identity === input.identity &&
        completed.version === this.#core.getCurrentManifestVersion();
      if (completedReplay && !sameSnapshotFrameHeader(completed.header, header)) {
        throw new SnapshotImportConflictError(
          input.identity,
          input.ownerId,
          "completed snapshot header changed",
        );
      }
      if (!completedReplay && this.#core.getCurrentManifestVersion() !== null) {
        throw new Error("Snapshot store already holds a database");
      }
      if (!completedReplay && this.#blocks.size > 0) {
        throw new StorageCorruptionError(
          "memory",
          "snapshot-import",
          "empty target contains unowned payloads",
        );
      }
      const session: SnapshotFrameImportSession = {
        identity: input.identity,
        ownerId: input.ownerId,
        version: header.databaseVersion,
        createdAt: input.createdAt,
        expiresAt: input.expiresAt,
        nextSequence: 0,
        stagedBytes: 0,
      };
      this.#snapshotFrameImport = {
        session,
        header,
        metadataFrames: completedReplay ? this.#captureSnapshotMetadataFrames() : [],
        observed: emptySnapshotObservedSummaries(),
        checksum: 0,
        itemCount: 0,
        previousKindIndex: 0,
        lastBlockId: null,
        lastBatchFrames: [],
        completedReplay,
      };
      return structuredClone(session);
    });
  }

  async renewSnapshotFrameImport(
    input: RenewSnapshotFrameImportInput,
  ): Promise<SnapshotFrameImportSession> {
    validateSnapshotSessionLifetime(input.expiresAtCutoff, input.expiresAt);
    return this.#runAtomic(() => {
      const state = this.#requireLiveSnapshotFrameImport(input);
      state.session = { ...state.session, expiresAt: input.expiresAt };
      return structuredClone(state.session);
    });
  }

  async appendSnapshotImportFrames(
    input: AppendSnapshotImportFramesInput,
  ): Promise<SnapshotFrameImportSession> {
    validateSnapshotSessionLifetime(input.expiresAtCutoff, input.expiresAt);
    if (input.frames.length < 1 || input.frames.length > MAX_SNAPSHOT_FRAME_BATCH_ITEMS) {
      throw new RangeError("Snapshot import frame batch item count is out of bounds");
    }
    let batchBytes = 0;
    let metadataBatchBytes = 0;
    for (const frame of input.frames) {
      batchBytes = safeSnapshotByteSum(batchBytes, frame.payload.byteLength);
      if (batchBytes > MAX_SNAPSHOT_FRAME_BATCH_BYTES) {
        throw new RangeError("Snapshot import frame batch exceeds its byte limit");
      }
      if (frame.kind !== "block") {
        metadataBatchBytes = safeSnapshotByteSum(metadataBatchBytes, frame.payload.byteLength);
        if (metadataBatchBytes > MAX_SNAPSHOT_METADATA_BATCH_BYTES) {
          throw new RangeError("Snapshot metadata frame batch exceeds its byte limit");
        }
      }
    }
    return this.#runAtomic(() => {
      const state = this.#requireLiveSnapshotFrameImport(input);
      if (
        state.lastBatchFrames.length === input.frames.length &&
        input.frames[0]?.sequence === state.session.nextSequence - state.lastBatchFrames.length
      ) {
        if (
          !state.lastBatchFrames.every((frame, index) =>
            sameSnapshotFrame(frame, input.frames[index]),
          )
        ) {
          throw new SnapshotImportConflictError(
            input.identity,
            input.ownerId,
            "replayed snapshot frame bytes changed",
          );
        }
        state.session = { ...state.session, expiresAt: input.expiresAt };
        return structuredClone(state.session);
      }
      if (input.frames[0]?.sequence !== state.session.nextSequence) {
        throw new RangeError("Snapshot import frame batch is not the next contiguous sequence");
      }
      const observed = structuredClone(state.observed);
      let checksum = state.checksum;
      let itemCount = state.itemCount;
      let previousKindIndex = state.previousKindIndex;
      let lastBlockId = state.lastBlockId;
      let nextSequence = state.session.nextSequence;
      let stagedBytes = state.session.stagedBytes;
      const metadataFrames: SnapshotFrame[] = [];
      const blockFrames: SnapshotFrame[] = [];
      for (const frame of input.frames) {
        if (frame.sequence !== nextSequence || crc32(frame.payload) !== frame.checksum) {
          throw new Error("Snapshot import frame sequence or checksum is invalid");
        }
        const envelope = snapshotFrameEnvelopeParts(frame);
        const kindIndex = SNAPSHOT_FRAME_KINDS.indexOf(frame.kind);
        if (kindIndex < previousKindIndex) throw new Error("Snapshot frame kinds are out of order");
        previousKindIndex = kindIndex;
        if (frame.kind === "block") {
          if (frame.key === null || (lastBlockId !== null && frame.key <= lastBlockId)) {
            throw new Error("Snapshot block frames are not in canonical ID order");
          }
          verifyStoredBlock(frame.payload);
          if (state.completedReplay) {
            const current = this.#blocks.get(frame.key);
            if (current === undefined || !sameSnapshotBytes(current, frame.payload)) {
              throw new SnapshotImportConflictError(
                input.identity,
                input.ownerId,
                `completed snapshot block changed: ${frame.key}`,
              );
            }
          } else blockFrames.push(copySnapshotFrame(frame));
          lastBlockId = frame.key;
        } else {
          if (frame.itemCount !== 1) {
            throw new TypeError("Snapshot v1 metadata frames contain exactly one item");
          }
          const items = decodeSnapshotMetadataItems(frame.kind, frame.payload);
          if (items.length !== frame.itemCount) {
            throw new Error("Snapshot metadata item count disagrees with its frame");
          }
          if (state.completedReplay) {
            const expected = state.metadataFrames[frame.sequence];
            if (!sameSnapshotFrame(expected, frame)) {
              throw new SnapshotImportConflictError(
                input.identity,
                input.ownerId,
                "completed snapshot metadata changed",
              );
            }
          } else metadataFrames.push(copySnapshotFrame(frame));
        }
        checksum = extendSnapshotFrameStreamChecksum(checksum, envelope);
        itemCount = safeSnapshotByteSum(itemCount, frame.itemCount);
        stagedBytes = safeSnapshotByteSum(stagedBytes, frame.payload.byteLength);
        const summary = observed[frame.kind];
        summary.frameCount += 1;
        summary.itemCount = safeSnapshotByteSum(summary.itemCount, frame.itemCount);
        summary.storedBytes = safeSnapshotByteSum(summary.storedBytes, frame.payload.byteLength);
        const expected = state.header.kinds[frame.kind];
        if (
          summary.frameCount > expected.frameCount ||
          summary.itemCount > expected.itemCount ||
          summary.storedBytes > expected.storedBytes
        ) {
          throw new Error(`Snapshot ${frame.kind} frames exceed their header summary`);
        }
        nextSequence += 1;
      }
      for (const frame of metadataFrames) state.metadataFrames.push(frame);
      for (const frame of blockFrames) {
        if (frame.key === null) throw new Error("Snapshot block frame lost its ID");
        this.#putBlock(frame.key, frame.payload);
      }
      state.observed = observed;
      state.checksum = checksum;
      state.itemCount = itemCount;
      state.previousKindIndex = previousKindIndex;
      state.lastBlockId = lastBlockId;
      state.lastBatchFrames = input.frames.map(copySnapshotFrame);
      state.session = {
        ...state.session,
        expiresAt: input.expiresAt,
        nextSequence,
        stagedBytes,
      };
      return structuredClone(state.session);
    });
  }

  async finishSnapshotFrameImport(input: FinishSnapshotFrameImportInput): Promise<void> {
    encodeSnapshotFrameStreamFooter(input.footer);
    return this.#runAtomic(() => {
      const state = this.#requireLiveSnapshotFrameImport(input);
      const expectedFrameCount = snapshotFrameCount(state.header);
      if (
        input.footer.frameCount !== expectedFrameCount ||
        input.footer.frameCount !== state.session.nextSequence ||
        input.footer.itemCount !== state.itemCount ||
        input.footer.storedBytes !== state.session.stagedBytes ||
        input.footer.checksum !== state.checksum
      ) {
        throw new Error("Snapshot import footer does not match its staged frames");
      }
      for (const kind of SNAPSHOT_FRAME_KINDS) {
        const actual = state.observed[kind];
        const expected = state.header.kinds[kind];
        if (
          actual.frameCount !== expected.frameCount ||
          actual.itemCount !== expected.itemCount ||
          actual.storedBytes !== expected.storedBytes
        ) {
          throw new Error(`Snapshot import ${kind} generation is incomplete`);
        }
      }
      if (!state.completedReplay) {
        const items = function* (): IterableIterator<SnapshotMetadataItem> {
          for (const frame of state.metadataFrames) {
            if (frame.kind === "block") continue;
            yield* decodeSnapshotMetadataItems(frame.kind, frame.payload);
          }
        };
        const blocks = function* (
          store: MemoryBlockStore,
        ): IterableIterator<{ blockId: string; byteLength: number; checksum: number }> {
          for (const [blockId, bytes] of store.#blocks) {
            yield { blockId, byteLength: bytes.byteLength, checksum: crc32(bytes) };
          }
        };
        this.#core.loadSnapshotFrameItems(state.header, items(), blocks(this));
      }
      this.#completedSnapshotFrameImport = {
        identity: state.session.identity,
        version: state.header.databaseVersion,
        header: structuredClone(state.header),
      };
      this.#snapshotFrameImport = undefined;
    });
  }

  async cancelSnapshotFrameImport(
    input: CancelSnapshotImportInput,
  ): Promise<InterruptedSnapshotImportAbortResult> {
    return this.#runAtomic(() => {
      const state = this.#snapshotFrameImport;
      if (state?.session.identity !== input.identity || state.session.ownerId !== input.ownerId) {
        throw new SnapshotImportConflictError(
          input.identity,
          input.ownerId,
          "session is missing or owned by another caller",
        );
      }
      const result = {
        identity: input.identity,
        removedBlockCount: state.completedReplay ? 0 : state.observed.block.itemCount,
        removedBytes: state.completedReplay ? 0 : state.observed.block.storedBytes,
      };
      if (!state.completedReplay) this.#clearPhysicalBlocks();
      this.#snapshotFrameImport = undefined;
      return result;
    });
  }

  #requireLiveSnapshotFrameImport(input: {
    identity: string;
    ownerId: string;
    expiresAtCutoff: string;
  }): MemorySnapshotFrameImportState {
    const state = this.#snapshotFrameImport;
    if (state?.session.identity !== input.identity || state.session.ownerId !== input.ownerId) {
      throw new SnapshotImportConflictError(
        input.identity,
        input.ownerId,
        "session is missing or owned by another caller",
      );
    }
    if (Date.parse(state.session.expiresAt) <= Date.parse(input.expiresAtCutoff)) {
      throw new SnapshotImportConflictError(input.identity, input.ownerId, "session is expired");
    }
    return state;
  }

  #captureSnapshotMetadataFrames(): SnapshotFrame[] {
    const frames: SnapshotFrame[] = [];
    const append = (item: SnapshotMetadataItem): void => {
      const payload = encodeSnapshotMetadataPage([item]);
      frames.push({
        sequence: frames.length,
        kind: snapshotMetadataFrameKind(item),
        itemCount: 1,
        key: null,
        payload,
        checksum: crc32(payload),
      });
    };
    for (const item of this.#core.snapshotFrameMetadataItems()) append(item);
    for (const item of this.#core.snapshotFramePostingItems()) append(item);
    return frames;
  }

  async inspectInterruptedImport(): Promise<InterruptedSnapshotImport | null> {
    const framed = this.#snapshotFrameImport;
    if (framed !== undefined) {
      return {
        identity: framed.session.identity,
        version: framed.session.version,
        createdAt: framed.session.createdAt,
        stagedBlockCount: framed.observed.block.itemCount,
        stagedBytes: framed.session.stagedBytes,
      };
    }
    return null;
  }

  async abortInterruptedImport(identity: string): Promise<InterruptedSnapshotImportAbortResult> {
    return this.#runAtomic(() => {
      const framed = this.#snapshotFrameImport;
      if (framed?.session.identity === identity) {
        const result = {
          identity,
          removedBlockCount: framed.completedReplay ? 0 : framed.observed.block.itemCount,
          removedBytes: framed.completedReplay ? 0 : framed.observed.block.storedBytes,
        };
        if (!framed.completedReplay) this.#clearPhysicalBlocks();
        this.#snapshotFrameImport = undefined;
        return result;
      }
      throw new Error(`Interrupted snapshot import not found: ${identity}`);
    });
  }

  /** Block and spill bytes held; the record maps are negligible beside them. */
  async getLogicalStorageBytes(): Promise<number> {
    return this.#physicalBlockBytes + this.#tempBytesTotal;
  }

  async getStorageStats(): Promise<StorageStats> {
    return this.#runAtomic(() => {
      const current = this.#core.getCurrentManifest();
      const temporaryBytes = this.#tempBytesTotal;
      const manifestCount = countRecordPages<Manifest, number>((cursor) =>
        this.#core.listManifestPage(cursor, 1_024),
      );
      const transactionCount = countRecordPages<TransactionRecord, string>((cursor) =>
        this.#core.listTransactionPage(cursor, 1_024),
      );
      const segmentCount = countRecordPages<SegmentRecord, string>((cursor) =>
        this.#core.listSegmentPage(cursor, 1_024),
      );
      const liveBlockCount = current?.liveBlockCount ?? 0;
      const liveBlockBytes = current?.liveBlockBytes ?? 0;
      if (liveBlockCount > this.#blocks.size || liveBlockBytes > this.#physicalBlockBytes) {
        throw new StorageCorruptionError(
          "memory",
          "manifest",
          "live block summary exceeds physical payloads",
        );
      }
      return {
        backend: "memory",
        logicalBytes: this.#physicalBlockBytes + temporaryBytes,
        physicalBytes: this.#physicalBlockBytes + temporaryBytes,
        liveBlockCount,
        obsoleteBlockCount: this.#blocks.size - liveBlockCount,
        liveBlockBytes,
        obsoleteBlockBytes: this.#physicalBlockBytes - liveBlockBytes,
        temporaryBytes,
        walBytes: null,
        checkpointBytes: null,
        orphanBytes: null,
        manifestCount,
        transactionCount,
        segmentCount,
      };
    });
  }

  #putBlock(id: string, bytes: Uint8Array): void {
    const stored = new Uint8Array(bytes);
    const previous = this.#blocks.get(id);
    this.#physicalBlockBytes += stored.byteLength - (previous?.byteLength ?? 0);
    this.#blocks.set(id, stored);
    this.#blockChecksums.set(id, crc32(stored));
  }

  #deleteBlock(id: string): void {
    this.#physicalBlockBytes -= this.#blocks.get(id)?.byteLength ?? 0;
    this.#blocks.delete(id);
    this.#blockChecksums.delete(id);
  }

  #clearPhysicalBlocks(): void {
    this.#blocks.clear();
    this.#blockChecksums.clear();
    this.#physicalBlockBytes = 0;
  }

  close(): void {
    // The in-memory implementation owns no external resources.
  }

  #runAtomic<T>(operation: () => T): Promise<T> {
    let resolveResult: (value: T | PromiseLike<T>) => void;
    let rejectResult: (reason: unknown) => void;
    const result = new Promise<T>((resolve, reject) => {
      resolveResult = resolve;
      rejectResult = reject;
    });
    this.#commitQueue = this.#commitQueue.then(() => {
      try {
        if (
          this.#completedSnapshotFrameImport !== undefined &&
          this.#core.getCurrentManifestVersion() !== this.#completedSnapshotFrameImport.version
        ) {
          this.#completedSnapshotFrameImport = undefined;
        }
        resolveResult(operation());
      } catch (error) {
        rejectResult(error);
      }
    });
    return result;
  }
}

function tempRunPageKey(ownerId: string, runId: string, pageIndex: number): string {
  return `${String(ownerId.length)}:${ownerId}:${String(runId.length)}:${runId}:${String(pageIndex)}`;
}

function tempRunPagePrefix(ownerId: string, runId: string): string {
  return `${String(ownerId.length)}:${ownerId}:${String(runId.length)}:${runId}:`;
}

function countRecordPages<T, Cursor extends string | number>(
  read: (cursor: Cursor | null) => StoragePage<T, Cursor>,
): number {
  let count = 0;
  let cursor: Cursor | null = null;
  do {
    const page = read(cursor);
    count += page.records.length;
    if (!Number.isSafeInteger(count)) throw new RangeError("Storage record count overflow");
    cursor = page.nextCursor;
  } while (cursor !== null);
  return count;
}

function assertTempResourceLimit(
  resource:
    | "temp owner byte"
    | "temp page"
    | "temp run"
    | "temporary byte"
    | "temporary page total"
    | "temporary run total",
  actual: number,
  limit: number,
): void {
  if (!Number.isSafeInteger(actual) || actual < 0 || actual > limit) {
    throw new StorageResourceLimitError(resource, actual, limit);
  }
}

function validateSnapshotSessionLifetime(cutoff: string, expiresAt: string): void {
  const cutoffMs = Date.parse(cutoff);
  const expiresAtMs = Date.parse(expiresAt);
  if (!Number.isFinite(cutoffMs) || !Number.isFinite(expiresAtMs)) {
    throw new TypeError("Snapshot session timestamps are invalid");
  }
  if (expiresAtMs <= cutoffMs) {
    throw new RangeError("Snapshot session expiration must be after its cutoff");
  }
  if (expiresAtMs - cutoffMs > MAX_SNAPSHOT_SESSION_TTL_MS) {
    throw new RangeError("Snapshot session exceeds the maximum lifetime");
  }
}

function validatePostingBuildLifetime(cutoff: string, expiresAt: string): void {
  const cutoffMs = Date.parse(cutoff);
  const expiresAtMs = Date.parse(expiresAt);
  if (!Number.isFinite(cutoffMs) || !Number.isFinite(expiresAtMs)) {
    throw new TypeError("Posting build timestamps are invalid");
  }
  if (expiresAtMs <= cutoffMs || expiresAtMs - cutoffMs > MAX_POSTING_BUILD_TTL_MS) {
    throw new RangeError("Posting build expiration is outside its bounded lifetime");
  }
}

function samePostingChunk(left: readonly FtsPosting[], right: readonly FtsPosting[]): boolean {
  return (
    left.length === right.length &&
    left.every((posting, postingIndex) => {
      const other = right[postingIndex];
      return (
        other?.term === posting.term &&
        posting.rowIds.length === other.rowIds.length &&
        posting.rowIds.every((rowId, index) => rowId === other.rowIds[index]) &&
        posting.tf.length === other.tf.length &&
        posting.tf.every((frequency, index) => frequency === other.tf[index])
      );
    })
  );
}

function safeSnapshotByteSum(total: number, byteLength: number): number {
  const result = total + byteLength;
  if (!Number.isSafeInteger(result) || result < 0) {
    throw new RangeError("Snapshot payload is too large");
  }
  return result;
}

function emptySnapshotObservedSummaries(): Record<SnapshotFrameKind, SnapshotObservedSummary> {
  return Object.fromEntries(
    SNAPSHOT_FRAME_KINDS.map((kind) => [kind, { frameCount: 0, itemCount: 0, storedBytes: 0 }]),
  ) as Record<SnapshotFrameKind, SnapshotObservedSummary>;
}

function snapshotMetadataFrameKind(item: SnapshotMetadataItem): SnapshotFrameKind {
  if (item.kind === "table") return "catalog-page";
  if (item.kind === "segment") return "segment-page";
  if (item.kind === "transaction") return "transaction-page";
  if (item.kind === "unique-generation" || item.kind === "unique-chunk") return "unique-page";
  return "posting-page";
}

function copySnapshotFrame(frame: SnapshotFrame): SnapshotFrame {
  return { ...frame, payload: frame.payload.slice() };
}

function sameSnapshotFrameHeader(
  left: SnapshotFrameStreamHeader,
  right: SnapshotFrameStreamHeader,
): boolean {
  return sameSnapshotBytes(
    encodeSnapshotFrameStreamHeader(left),
    encodeSnapshotFrameStreamHeader(right),
  );
}

function sameSnapshotFrame(left: SnapshotFrame | undefined, right: SnapshotFrame | undefined) {
  if (left?.sequence === undefined || right?.sequence === undefined) return false;
  return (
    left.sequence === right.sequence &&
    left.kind === right.kind &&
    left.itemCount === right.itemCount &&
    left.key === right.key &&
    left.checksum === right.checksum &&
    sameSnapshotBytes(left.payload, right.payload)
  );
}

function snapshotFrameCount(header: SnapshotFrameStreamHeader): number {
  return SNAPSHOT_FRAME_KINDS.reduce(
    (total, kind) => safeSnapshotByteSum(total, header.kinds[kind].frameCount),
    0,
  );
}

function sameSnapshotBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.byteLength === right.byteLength && left.every((byte, index) => right[index] === byte);
}
