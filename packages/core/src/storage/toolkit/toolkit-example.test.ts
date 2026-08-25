/**
 * Proof that the public surface is sufficient to build a storage adapter: a complete,
 * persistent, log-structured `BlockStore` assembled from public entry points only — the
 * storage contract (`@minnowdb/core/storage`), the adapter toolkit
 * (`@minnowdb/core/storage/toolkit`), and the testing kit (`@minnowdb/core/testing`) — and
 * run through the same conformance suite the shipped adapters pass.
 *
 * It is also the worked example `/docs/storage/custom` describes: record semantics from
 * `RecordCore`, durability from one write-ahead log, bulk bytes in packed extents, and a
 * checkpoint that compacts the log at open. A real adapter written this way differs mainly in
 * what stands behind `SyncFileHandle` — here the in-memory OPFS shim, elsewhere OPFS itself,
 * a Node file descriptor, or anything with positioned synchronous I/O.
 */
import { describe, it } from "vitest";
import {
  MAX_TEMP_BYTES_PER_OWNER,
  MAX_TEMP_BYTES_TOTAL,
  MAX_TEMP_PAGES_PER_OWNER,
  MAX_TEMP_PAGES_TOTAL,
  MAX_TEMP_RUNS_PER_OWNER,
  MAX_TEMP_RUNS_TOTAL,
  PostingBuildConflictError,
  StorageResourceLimitError,
  assertStorageBulkReadItems,
  assertTempRunPageBatchLimits,
  validateStorageId,
} from "../index.js";
import type {
  BlockStore,
  AbortTransactionIfExpiredInput,
  AdoptAbortedSegmentInput,
  RenewTransactionInput,
  CommitTransactionInput,
  CompactionJobRecord,
  CreateGarbageCollectionJobInput,
  UpdateGarbageCollectionPlanningInput,
  DropTableColumnInput,
  DropTableInput,
  GarbageCollectionJobRecord,
  GarbageCollectionStepResult,
  LeaseRecord,
  Manifest,
  ManifestSummary,
  RowIdRange,
  RollbackTransactionArtifactsInput,
  RenewLeaseInput,
  RenewTempOwnerInput,
  RunGarbageCollectionStepInput,
  SegmentRecord,
  StageTransactionArtifactsInput,
  StoragePage,
  TableRecord,
  TempOwnerRecord,
  TempRunPage,
  TransactionRecord,
  TransactionRecordUpdate,
} from "../index.js";
import {
  ExtentPool,
  RecordCore,
  WalWriter,
  decodeSyncCheckpoint,
  encodeSyncCheckpoint,
  readFully,
  replayWalFrames,
  writeFully,
  type ExtentFiles,
  type ExtentMeta,
  type Placement,
  type RecordCoreState,
} from "./index.js";
import {
  MemoryOpfs,
  blockStoreConformanceCases,
  type BlockStoreConformanceTarget,
} from "../../testing/index.js";

type TableUpdate = Parameters<BlockStore["updateTable"]>[2];
type TransactionBegin = Parameters<BlockStore["beginTransaction"]>[0];
type CompactionUpdate = Parameters<BlockStore["updateCompactionJob"]>[2];
type FtsBaseInput = Parameters<BlockStore["writeFtsBase"]>[2];
type FtsTerms = Parameters<BlockStore["readFtsCandidates"]>[2];
type FtsBuildBegin = Parameters<BlockStore["beginFtsBaseBuild"]>[0];
type FtsBuildRenewal = Parameters<BlockStore["renewFtsBaseBuild"]>[0];
type FtsBuildAppend = Parameters<BlockStore["writeFtsBaseBuildChunk"]>[0];
type FtsBuildFinish = Parameters<BlockStore["finishFtsBaseBuild"]>[0];
type FtsBuildAbort = Parameters<BlockStore["abortFtsBaseBuild"]>[0];
type FtsBuildChunk = FtsBuildAppend["chunk"];
type UniqueBuildBegin = Parameters<BlockStore["beginUniqueKeyBuild"]>[0];
type UniqueBuildRenew = Parameters<BlockStore["renewUniqueKeyBuild"]>[0];
type UniqueBuildAppend = Parameters<BlockStore["appendUniqueKeyBuildChunk"]>[0];
type UniqueBuildFinish = Parameters<BlockStore["finishUniqueKeyBuild"]>[0];
type UniqueBuildAbort = Parameters<BlockStore["abortUniqueKeyBuild"]>[0];
interface FtsBuildState {
  buildId: string;
  ownerId: string;
  createdAt: string;
  expiresAt: string;
  chunks: Map<number, FtsBuildChunk>;
}
interface PersistedFtsBuildState extends Omit<FtsBuildState, "chunks"> {
  chunks: Array<readonly [number, FtsBuildChunk]>;
}

/** One durable mutation, exactly what replay needs to reproduce it. */
type Frame =
  | {
      op: "addTable";
      record: TableRecord;
      expectedCatalogEpoch?: number;
    }
  | { op: "updateTable"; id: string; expectedRevision: number; update: TableUpdate }
  | {
      op: "removeTable";
      id: string;
      expectedRevision: number;
      expectedCatalogEpoch?: number;
    }
  | { op: "dropTable"; input: DropTableInput }
  | { op: "dropTableColumn"; input: DropTableColumnInput }
  | { op: "removeAbortedSegment"; id: string; expectedTransactionId: string }
  | { op: "adoptAbortedSegment"; input: AdoptAbortedSegmentInput }
  | { op: "reserveRowIds"; tableId: string; count: number }
  | {
      op: "reserveAutoIncrement";
      tableId: string;
      columnId: string;
      count: number;
      atLeast?: bigint;
    }
  | { op: "createTransaction"; record: TransactionRecord }
  | { op: "beginTransaction"; input: TransactionBegin }
  | { op: "renewTransaction"; input: RenewTransactionInput }
  | { op: "abortTransactionIfExpired"; input: AbortTransactionIfExpiredInput }
  | {
      op: "updateTransaction";
      id: string;
      expectedRevision: number;
      update: TransactionRecordUpdate;
    }
  | {
      op: "stageTransactionArtifacts";
      input: Omit<StageTransactionArtifactsInput, "blocks"> & {
        blocks: Array<{ id: string; placement: Placement }>;
      };
    }
  | { op: "rollbackTransactionArtifacts"; input: RollbackTransactionArtifactsInput }
  | { op: "commitTransaction"; input: CommitTransactionInput }
  | { op: "createLease"; record: LeaseRecord }
  | { op: "renewLease"; input: RenewLeaseInput }
  | { op: "removeLeaseIfExpired"; id: string; expectedRevision: number; expiresAtCutoff: string }
  | { op: "removeLease"; input: { id: string; ownerId: string } }
  | { op: "createCompactionJob"; record: CompactionJobRecord }
  | { op: "updateCompactionJob"; id: string; expectedRevision: number; update: CompactionUpdate }
  | { op: "cancelCompactionJob"; id: string; expectedRevision: number; cancelledAt: string }
  | { op: "removeCompactionJob"; id: string }
  | { op: "createGarbageCollectionJob"; input: CreateGarbageCollectionJobInput }
  | { op: "updateGarbageCollectionPlanning"; input: UpdateGarbageCollectionPlanningInput }
  | {
      op: "garbageCollectionStep";
      effect: {
        job: GarbageCollectionJobRecord;
        prunedManifestVersions: number[];
        reclaimedSegmentIds: string[];
        reclaimedBlockIds: string[];
        reclaimedTransactionIds: string[];
        updatedAt: string;
      };
    }
  | { op: "removeGarbageCollectionJob"; id: string }
  | { op: "removePrunedManifestRecords"; maxItems: number }
  | { op: "createTempOwner"; record: TempOwnerRecord }
  | { op: "renewTempOwner"; input: RenewTempOwnerInput }
  | { op: "removeTempOwnerIfExpired"; ownerId: string; expiresAtCutoff: string }
  | { op: "removeTempOwner"; ownerId: string }
  | { op: "removeFtsColumn"; tableId: string; columnId: string }
  | { op: "writeFtsBase"; tableId: string; columnId: string; input: FtsBaseInput }
  | { op: "beginFtsBaseBuild"; input: FtsBuildBegin }
  | { op: "renewFtsBaseBuild"; input: FtsBuildRenewal }
  | { op: "writeFtsBaseBuildChunk"; input: FtsBuildAppend }
  | { op: "finishFtsBaseBuild"; input: FtsBuildFinish }
  | { op: "abortFtsBaseBuild"; input: FtsBuildAbort }
  | { op: "beginUniqueKeyBuild"; input: UniqueBuildBegin }
  | { op: "renewUniqueKeyBuild"; input: UniqueBuildRenew }
  | { op: "appendUniqueKeyBuildChunk"; input: UniqueBuildAppend }
  | { op: "finishUniqueKeyBuild"; input: UniqueBuildFinish }
  | { op: "abortUniqueKeyBuild"; input: UniqueBuildAbort };

interface Checkpoint {
  core: RecordCoreState;
  blocks: Array<readonly [string, Placement]>;
  extents: ExtentMeta;
  ftsBuilds: Array<readonly [string, PersistedFtsBuildState]>;
}

/** Minimal `ExtentFiles` (and general file access) over a directory handle. */
function filesOver(root: FileSystemDirectoryHandle): ExtentFiles {
  return {
    async openHandle(path, options) {
      let directory = root;
      for (const name of path.slice(0, -1)) {
        directory = await directory.getDirectoryHandle(name, { create: options.create });
      }
      const file = await directory.getFileHandle(path[path.length - 1] ?? "", {
        create: options.create,
      });
      return file.createSyncAccessHandle();
    },
    async deleteFile(path) {
      try {
        let directory = root;
        for (const name of path.slice(0, -1)) {
          directory = await directory.getDirectoryHandle(name, { create: false });
        }
        await directory.removeEntry(path[path.length - 1] ?? "");
        return true;
      } catch {
        return false;
      }
    },
  };
}

function sameFtsChunk(left: FtsBuildChunk, right: FtsBuildChunk): boolean {
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

class MiniLogStore implements BlockStore {
  readonly #files: ExtentFiles;
  readonly #wal: WalWriter;
  readonly #pool: ExtentPool;
  readonly #placements: Map<string, Placement>;
  readonly #core: RecordCore;
  /** Spill pages are scratch; they live and die with the instance, like the OPFS store's. */
  readonly #tempPages = new Map<string, Uint8Array>();
  readonly #tempPageKeysByRun = new Map<string, Set<string>>();
  readonly #tempUsageByOwner = new Map<
    string,
    { bytes: number; pages: number; runs: Map<string, number> }
  >();
  #tempBytesTotal = 0;
  #tempPagesTotal = 0;
  #tempRunsTotal = 0;
  readonly #ftsBuilds: Map<string, FtsBuildState>;
  #queue = Promise.resolve();

  private constructor(
    files: ExtentFiles,
    wal: WalWriter,
    pool: ExtentPool,
    placements: Map<string, Placement>,
    core: RecordCore,
    ftsBuilds: Map<string, FtsBuildState>,
  ) {
    this.#files = files;
    this.#wal = wal;
    this.#pool = pool;
    this.#placements = placements;
    this.#core = core;
    this.#ftsBuilds = ftsBuilds;
  }

  static async open(root: FileSystemDirectoryHandle): Promise<MiniLogStore> {
    const files = filesOver(root);

    // 1. The newest checkpoint, when one exists. A torn write reads as "not written".
    let checkpoint: Checkpoint | undefined;
    try {
      const handle = await files.openHandle(["checkpoint"], { create: false });
      const bytes = new Uint8Array(handle.getSize());
      readFully(handle, bytes, 0, "reading the toolkit example checkpoint");
      handle.close();
      checkpoint = decodeSyncCheckpoint(bytes) as Checkpoint | undefined;
    } catch {
      checkpoint = undefined;
    }

    const placements = new Map<string, Placement>(
      checkpoint === undefined ? [] : checkpoint.blocks,
    );
    const core = new RecordCore({
      hasBlock: (id) => placements.has(id),
      blockByteLength: (id) => placements.get(id)?.length,
    });
    if (checkpoint !== undefined) core.load(checkpoint.core);
    const pool = await ExtentPool.open(
      files,
      checkpoint === undefined ? undefined : checkpoint.extents,
    );
    const ftsBuilds = new Map<string, FtsBuildState>(
      (checkpoint === undefined ? [] : checkpoint.ftsBuilds).map(([key, build]) => [
        key,
        { ...build, chunks: new Map(build.chunks) },
      ]),
    );

    // 2. Replay every whole frame past the checkpoint; a torn tail is overwritten.
    const walHandle = await files.openHandle(["wal"], { create: true });
    const { payloads, endOffset } = replayWalFrames(walHandle);
    const wal = new WalWriter(walHandle, endOffset);
    const store = new MiniLogStore(files, wal, pool, placements, core, ftsBuilds);
    let newestExtent: Placement | undefined;
    for (const payload of payloads) {
      const frame = payload as Frame;
      store.#replay(frame);
      if (frame.op === "stageTransactionArtifacts") {
        for (const block of frame.input.blocks) {
          if (newestExtent === undefined || block.placement.extent >= newestExtent.extent) {
            newestExtent = block.placement;
          }
        }
      }
    }
    // The writer may have sealed past the checkpointed tail extent; adopt its position.
    if (newestExtent !== undefined && newestExtent.extent !== pool.tailExtentId) {
      await pool.adoptTail(newestExtent.extent, newestExtent.offset + newestExtent.length);
    }

    // 3. Compact: checkpoint, flush, and only then reset the log — never the other order,
    //    or a crash between the two would lose every frame the checkpoint did not cover.
    await store.#checkpoint();
    return store;
  }

  #replay(frame: Frame): void {
    switch (frame.op) {
      case "addTable":
        this.#core.addTable(frame.record);
        break;
      case "updateTable":
        this.#core.updateTable(frame.id, frame.expectedRevision, frame.update);
        break;
      case "removeTable":
        this.#core.removeTable(frame.id, frame.expectedRevision);
        break;
      case "dropTable":
        this.#core.dropTable(frame.input);
        break;
      case "dropTableColumn":
        this.#core.dropTableColumn(frame.input);
        break;
      case "removeAbortedSegment":
        this.#core.removeAbortedSegment(frame.id, frame.expectedTransactionId);
        break;
      case "adoptAbortedSegment":
        this.#core.adoptAbortedSegment(frame.input);
        break;
      case "reserveRowIds":
        this.#core.reserveRowIds(frame.tableId, frame.count);
        break;
      case "reserveAutoIncrement":
        this.#core.reserveAutoIncrement(frame.tableId, frame.columnId, frame.count, frame.atLeast);
        break;
      case "createTransaction":
        this.#core.createTransaction(frame.record);
        break;
      case "beginTransaction":
        this.#core.beginTransaction(frame.input);
        break;
      case "renewTransaction":
        this.#core.renewTransaction(frame.input);
        break;
      case "abortTransactionIfExpired":
        this.#core.abortTransactionIfExpired(frame.input);
        break;
      case "updateTransaction":
        this.#core.updateTransaction(frame.id, frame.expectedRevision, frame.update);
        break;
      case "stageTransactionArtifacts":
        for (const block of frame.input.blocks) {
          this.#pool.restorePlacement(block.placement);
          this.#placements.set(block.id, block.placement);
        }
        this.#core.stageTransactionArtifacts(
          {
            ...frame.input,
            blocks: frame.input.blocks.map(({ id }) => ({ id, bytes: new Uint8Array(0) })),
          },
          { blocksPrevalidated: true },
        );
        break;
      case "rollbackTransactionArtifacts":
        this.#core.rollbackTransactionArtifacts(frame.input);
        for (const id of frame.input.removeBlockIds) {
          const placement = this.#placements.get(id);
          if (placement !== undefined) this.#pool.release([placement]);
          this.#placements.delete(id);
        }
        break;
      case "commitTransaction":
        this.#core.commitTransaction(frame.input);
        break;
      case "createLease":
        this.#core.createLease(frame.record);
        break;
      case "renewLease":
        this.#core.renewLease(frame.input);
        break;
      case "removeLeaseIfExpired":
        this.#core.removeLeaseIfExpired(frame.id, frame.expectedRevision, frame.expiresAtCutoff);
        break;
      case "removeLease":
        this.#core.removeLease(frame.input);
        break;
      case "createCompactionJob":
        this.#core.createCompactionJob(frame.record);
        break;
      case "updateCompactionJob":
        this.#core.updateCompactionJob(frame.id, frame.expectedRevision, frame.update);
        break;
      case "cancelCompactionJob":
        this.#core.cancelCompactionJob(frame.id, frame.expectedRevision, frame.cancelledAt);
        break;
      case "removeCompactionJob":
        this.#core.removeCompactionJob(frame.id);
        break;
      case "createGarbageCollectionJob":
        this.#core.createGarbageCollectionJob(frame.input);
        break;
      case "updateGarbageCollectionPlanning":
        this.#core.updateGarbageCollectionPlanning(frame.input);
        break;
      case "garbageCollectionStep":
        this.#core.applyGarbageCollectionEffect(frame.effect);
        for (const id of frame.effect.reclaimedBlockIds) {
          const placement = this.#placements.get(id);
          if (placement !== undefined) {
            this.#pool.release([placement]);
            this.#placements.delete(id);
          }
        }
        break;
      case "removeGarbageCollectionJob":
        this.#core.removeGarbageCollectionJob(frame.id);
        break;
      case "removePrunedManifestRecords":
        this.#core.removePrunedManifestRecords(frame.maxItems);
        break;
      case "createTempOwner":
        this.#core.createTempOwner(frame.record);
        break;
      case "renewTempOwner":
        this.#core.renewTempOwner(frame.input);
        break;
      case "removeTempOwnerIfExpired":
        this.#core.removeTempOwnerIfExpired(frame.ownerId, frame.expiresAtCutoff);
        break;
      case "removeTempOwner":
        this.#core.removeTempOwner(frame.ownerId);
        break;
      case "writeFtsBase":
        this.#core.writeFtsBase(frame.tableId, frame.columnId, frame.input);
        break;
      case "removeFtsColumn":
        this.#core.removeFtsColumn(frame.tableId, frame.columnId);
        break;
      case "beginFtsBaseBuild":
        this.#applyBeginFtsBaseBuild(frame.input);
        break;
      case "renewFtsBaseBuild":
        this.#applyRenewFtsBaseBuild(frame.input);
        break;
      case "writeFtsBaseBuildChunk":
        this.#applyFtsBaseBuildChunk(frame.input);
        break;
      case "finishFtsBaseBuild":
        this.#applyFinishFtsBaseBuild(frame.input);
        break;
      case "abortFtsBaseBuild":
        this.#applyAbortFtsBaseBuild(frame.input);
        break;
      case "beginUniqueKeyBuild":
        this.#core.beginUniqueKeyBuild(frame.input);
        break;
      case "renewUniqueKeyBuild":
        this.#core.renewUniqueKeyBuild(frame.input);
        break;
      case "appendUniqueKeyBuildChunk":
        this.#core.appendUniqueKeyBuildChunk(frame.input);
        break;
      case "finishUniqueKeyBuild":
        this.#core.finishUniqueKeyBuild(frame.input);
        break;
      case "abortUniqueKeyBuild":
        this.#core.abortUniqueKeyBuild(frame.input);
        break;
    }
  }

  async #checkpoint(): Promise<void> {
    const payload: Checkpoint = {
      // dump()'s arrays alias live state — serialized here, in this same synchronous run.
      core: this.#core.dump(),
      blocks: [...this.#placements.entries()],
      extents: this.#pool.meta(),
      ftsBuilds: [...this.#ftsBuilds].map(([key, build]) => [
        key,
        { ...build, chunks: [...build.chunks] },
      ]),
    };
    const bytes = encodeSyncCheckpoint(payload);
    const handle = await this.#files.openHandle(["checkpoint"], { create: true });
    try {
      handle.truncate(0);
      writeFully(handle, bytes, 0, "writing the toolkit example checkpoint");
      handle.flush();
    } finally {
      handle.close();
    }
    this.#wal.reset();
  }

  /** Mutations run one at a time; the core validates then mutates, and only then logs. */
  #run<T>(operation: () => Promise<T> | T): Promise<T> {
    const result = this.#queue.then(operation);
    this.#queue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  #logged<T>(frame: Frame, mutate: () => T): T {
    const result = mutate();
    // A throw above means nothing to log; a logged frame means the mutation happened.
    this.#wal.append(frame, true);
    return result;
  }

  // ---- blocks: bytes in extents, ids to placements, both published by one frame ----

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    return this.#run(() => {
      const placement = this.#placements.get(id);
      return placement === undefined ? undefined : this.#pool.read(placement);
    });
  }

  async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    assertStorageBulkReadItems(ids, "Block read");
    const blocks: Array<Uint8Array | undefined> = [];
    for (const id of ids) blocks.push(await this.getBlock(id));
    return blocks;
  }

  async readManifestBlock(version: number | null, id: string): Promise<Uint8Array | undefined> {
    if (this.#core.hasManifestBlocks(version, [id])[0] !== true) return undefined;
    return this.getBlock(id);
  }

  async hasManifestBlocks(version: number | null, ids: readonly string[]): Promise<boolean[]> {
    return this.#core.hasManifestBlocks(version, ids);
  }

  // ---- records: every mutation is one core call plus one frame ----

  async addTable(record: TableRecord, options: Parameters<BlockStore["addTable"]>[1] = {}) {
    return this.#run(() =>
      this.#logged({ op: "addTable", record, ...options }, () => {
        this.#core.addTable(record, options);
      }),
    );
  }

  async getTable(id: string): Promise<TableRecord | undefined> {
    return this.#core.getTable(id);
  }

  async getTableByName(name: string): Promise<TableRecord | undefined> {
    const id = this.#core.getTableIdByName(name);
    return id === undefined ? undefined : this.#core.getTable(id);
  }

  async listTables(): Promise<TableRecord[]> {
    return this.#core.listTables();
  }

  async updateTable(id: string, expectedRevision: number, update: TableUpdate) {
    return this.#run(() =>
      this.#logged({ op: "updateTable", id, expectedRevision, update }, () =>
        this.#core.updateTable(id, expectedRevision, update),
      ),
    );
  }

  async removeTable(
    id: string,
    expectedRevision: number,
    options: Parameters<BlockStore["removeTable"]>[2] = {},
  ): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "removeTable", id, expectedRevision, ...options }, () => {
        this.#core.removeTable(id, expectedRevision, options);
      }),
    );
  }

  async dropTable(input: DropTableInput): Promise<ManifestSummary> {
    return this.#run(() =>
      this.#logged({ op: "dropTable", input }, () => this.#core.dropTable(input)),
    );
  }

  async dropTableColumn(input: DropTableColumnInput): Promise<ManifestSummary> {
    return this.#run(() =>
      this.#logged({ op: "dropTableColumn", input }, () => this.#core.dropTableColumn(input)),
    );
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
    return this.#run(() =>
      this.#logged({ op: "removeAbortedSegment", id, expectedTransactionId }, () =>
        this.#core.removeAbortedSegment(id, expectedTransactionId),
      ),
    );
  }

  async adoptAbortedSegment(input: AdoptAbortedSegmentInput): Promise<TransactionRecord> {
    return this.#run(() =>
      this.#logged({ op: "adoptAbortedSegment", input }, () =>
        this.#core.adoptAbortedSegment(input),
      ),
    );
  }

  async reserveRowIds(tableId: string, count: number): Promise<RowIdRange> {
    return this.#run(() =>
      this.#logged({ op: "reserveRowIds", tableId, count }, () =>
        this.#core.reserveRowIds(tableId, count),
      ),
    );
  }

  async reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): Promise<RowIdRange> {
    return this.#run(() =>
      this.#logged(
        {
          op: "reserveAutoIncrement",
          tableId,
          columnId,
          count,
          ...(atLeast === undefined ? {} : { atLeast }),
        },
        () => this.#core.reserveAutoIncrement(tableId, columnId, count, atLeast),
      ),
    );
  }

  async getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    return this.#core.getExistingUniqueKeys(tableId, keyTokens);
  }

  async beginUniqueKeyBuild(input: UniqueBuildBegin) {
    return this.#run(() =>
      this.#logged({ op: "beginUniqueKeyBuild", input }, () =>
        this.#core.beginUniqueKeyBuild(input),
      ),
    );
  }

  async getUniqueKeyBuild(buildId: string) {
    return this.#core.getUniqueKeyBuild(buildId);
  }

  async renewUniqueKeyBuild(input: UniqueBuildRenew) {
    return this.#run(() =>
      this.#logged({ op: "renewUniqueKeyBuild", input }, () =>
        this.#core.renewUniqueKeyBuild(input),
      ),
    );
  }

  async appendUniqueKeyBuildChunk(input: UniqueBuildAppend) {
    return this.#run(() =>
      this.#logged({ op: "appendUniqueKeyBuildChunk", input }, () =>
        this.#core.appendUniqueKeyBuildChunk(input),
      ),
    );
  }

  async finishUniqueKeyBuild(input: UniqueBuildFinish) {
    return this.#run(() =>
      this.#logged({ op: "finishUniqueKeyBuild", input }, () =>
        this.#core.finishUniqueKeyBuild(input),
      ),
    );
  }

  async abortUniqueKeyBuild(input: UniqueBuildAbort) {
    return this.#run(() =>
      this.#logged({ op: "abortUniqueKeyBuild", input }, () =>
        this.#core.abortUniqueKeyBuild(input),
      ),
    );
  }

  async getCurrentManifest(): Promise<Manifest | undefined> {
    return this.#core.getCurrentManifest();
  }

  async getCurrentManifestVersion(): Promise<number | null> {
    return this.#core.getCurrentManifestVersion();
  }

  async getCatalogProbe() {
    return this.#core.getCatalogProbe();
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

  async createTransaction(record: TransactionRecord): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "createTransaction", record }, () => {
        this.#core.createTransaction(record);
      }),
    );
  }

  async beginTransaction(input: TransactionBegin) {
    return this.#run(() =>
      this.#logged({ op: "beginTransaction", input }, () => this.#core.beginTransaction(input)),
    );
  }

  async renewTransaction(input: RenewTransactionInput): Promise<boolean> {
    return this.#run(() =>
      this.#logged({ op: "renewTransaction", input }, () => this.#core.renewTransaction(input)),
    );
  }

  async abortTransactionIfExpired(
    input: AbortTransactionIfExpiredInput,
  ): Promise<TransactionRecord | undefined> {
    return this.#run(() =>
      this.#logged({ op: "abortTransactionIfExpired", input }, () =>
        this.#core.abortTransactionIfExpired(input),
      ),
    );
  }

  async getTransaction(id: string): Promise<TransactionRecord | undefined> {
    return this.#core.getTransaction(id);
  }

  async getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>> {
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
    return this.#run(() =>
      this.#logged({ op: "updateTransaction", id, expectedRevision, update }, () =>
        this.#core.updateTransaction(id, expectedRevision, update),
      ),
    );
  }

  async stageTransactionArtifacts(
    input: StageTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    return this.#run(async () => {
      const ids = new Set<string>();
      for (const block of input.blocks) {
        if (ids.has(block.id) || this.#placements.has(block.id)) {
          throw new Error(`Block already exists: ${block.id}`);
        }
        ids.add(block.id);
      }
      const incomingBytes = new Map(
        input.blocks.map((block) => [block.id, block.bytes.byteLength]),
      );
      const trial = new RecordCore({
        hasBlock: (id) => this.#placements.has(id) || incomingBytes.has(id),
        blockByteLength: (id) => this.#placements.get(id)?.length ?? incomingBytes.get(id),
      });
      trial.load(this.#core.dump());
      trial.stageTransactionArtifacts(input, { blocksPrevalidated: true });
      const blocks: Array<{ id: string; placement: Placement }> = [];
      for (const block of input.blocks) {
        blocks.push({ id: block.id, placement: await this.#pool.append(block.bytes, true) });
      }
      return this.#logged({ op: "stageTransactionArtifacts", input: { ...input, blocks } }, () => {
        const updated = this.#core.stageTransactionArtifacts(
          {
            ...input,
            blocks: blocks.map(({ id }) => ({ id, bytes: new Uint8Array(0) })),
          },
          { blocksPrevalidated: true },
        );
        for (const block of blocks) this.#placements.set(block.id, block.placement);
        return updated;
      });
    });
  }

  async rollbackTransactionArtifacts(
    input: RollbackTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    return this.#run(async () => {
      const trial = new RecordCore({
        hasBlock: (id) => this.#placements.has(id),
        blockByteLength: (id) => this.#placements.get(id)?.length,
      });
      trial.load(this.#core.dump());
      trial.rollbackTransactionArtifacts(input);
      const drained: number[] = [];
      const updated = this.#logged({ op: "rollbackTransactionArtifacts", input }, () => {
        const record = this.#core.rollbackTransactionArtifacts(input);
        for (const id of input.removeBlockIds) {
          const placement = this.#placements.get(id);
          if (placement !== undefined) drained.push(...this.#pool.release([placement]));
          this.#placements.delete(id);
        }
        return record;
      });
      for (const extent of new Set(drained)) await this.#pool.deleteExtent(extent);
      return updated;
    });
  }

  async commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary> {
    return this.#run(() =>
      this.#logged({ op: "commitTransaction", input }, () => this.#core.commitTransaction(input)),
    );
  }

  async createLease(record: LeaseRecord): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "createLease", record }, () => {
        this.#core.createLease(record);
      }),
    );
  }

  async getLease(id: string): Promise<LeaseRecord | undefined> {
    return this.#core.getLease(id);
  }

  async listLeases(): Promise<LeaseRecord[]> {
    return this.#core.listLeases();
  }

  async listExpiredLeasePage(expiresAtCutoff: string, afterCursor: string | null, limit: number) {
    return this.#core.listExpiredLeasePage(expiresAtCutoff, afterCursor, limit);
  }

  async renewLease(input: RenewLeaseInput): Promise<LeaseRecord> {
    return this.#run(() =>
      this.#logged({ op: "renewLease", input }, () => this.#core.renewLease(input)),
    );
  }

  async removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    return this.#run(() =>
      this.#logged({ op: "removeLeaseIfExpired", id, expectedRevision, expiresAtCutoff }, () =>
        this.#core.removeLeaseIfExpired(id, expectedRevision, expiresAtCutoff),
      ),
    );
  }

  async removeLease(input: { id: string; ownerId: string }): Promise<boolean> {
    return this.#run(() =>
      this.#logged({ op: "removeLease", input }, () => this.#core.removeLease(input)),
    );
  }

  async createCompactionJob(record: CompactionJobRecord): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "createCompactionJob", record }, () => {
        this.#core.createCompactionJob(record);
      }),
    );
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

  async updateCompactionJob(id: string, expectedRevision: number, update: CompactionUpdate) {
    return this.#run(() =>
      this.#logged({ op: "updateCompactionJob", id, expectedRevision, update }, () =>
        this.#core.updateCompactionJob(id, expectedRevision, update),
      ),
    );
  }

  async cancelCompactionJob(id: string, expectedRevision: number, cancelledAt: string) {
    return this.#run(() =>
      this.#logged({ op: "cancelCompactionJob", id, expectedRevision, cancelledAt }, () =>
        this.#core.cancelCompactionJob(id, expectedRevision, cancelledAt),
      ),
    );
  }

  async removeCompactionJob(id: string): Promise<boolean> {
    return this.#run(() =>
      this.#logged({ op: "removeCompactionJob", id }, () => this.#core.removeCompactionJob(id)),
    );
  }

  async createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord> {
    return this.#run(() =>
      this.#logged({ op: "createGarbageCollectionJob", input }, () =>
        this.#core.createGarbageCollectionJob(input),
      ),
    );
  }

  async updateGarbageCollectionPlanning(
    input: UpdateGarbageCollectionPlanningInput,
  ): Promise<GarbageCollectionJobRecord> {
    return this.#run(() =>
      this.#logged({ op: "updateGarbageCollectionPlanning", input }, () =>
        this.#core.updateGarbageCollectionPlanning(input),
      ),
    );
  }

  async getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    return this.#core.getGarbageCollectionJob(id);
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return this.#core.listGarbageCollectionJobs();
  }

  async listGarbageCollectionJobPage(afterId: string | null, limit: number) {
    return this.#core.listGarbageCollectionJobPage(afterId, limit);
  }

  async runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    return this.#run(async () => {
      // Computed live, logged as its resolved effect: replay must not re-derive decisions
      // that depended on state (leases, versions) as it stood at this moment.
      const step = this.#core.runGarbageCollectionStep(input);
      const reclaimed: Placement[] = [];
      for (const id of step.reclaimedBlockIds) {
        const placement = this.#placements.get(id);
        if (placement !== undefined) {
          reclaimed.push(placement);
          this.#placements.delete(id);
        }
      }
      const drained = this.#pool.release(reclaimed);
      this.#wal.append(
        {
          op: "garbageCollectionStep",
          effect: {
            job: step.job,
            prunedManifestVersions: [...step.prunedManifestVersions],
            reclaimedSegmentIds: [...step.reclaimedSegmentIds],
            reclaimedBlockIds: [...step.reclaimedBlockIds],
            reclaimedTransactionIds: [...step.reclaimedTransactionIds],
            updatedAt: input.updatedAt,
          },
        } satisfies Frame,
        true,
      );
      for (const extent of drained) await this.#pool.deleteExtent(extent);
      return step;
    });
  }

  async removeGarbageCollectionJob(id: string): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "removeGarbageCollectionJob", id }, () => {
        this.#core.removeGarbageCollectionJob(id);
      }),
    );
  }

  async removePrunedManifestRecords(maxItems: number): Promise<number> {
    return this.#run(() =>
      this.#logged({ op: "removePrunedManifestRecords", maxItems }, () =>
        this.#core.removePrunedManifestRecords(maxItems),
      ),
    );
  }

  async writeFtsBase(tableId: string, columnId: string, input: FtsBaseInput): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "writeFtsBase", tableId, columnId, input }, () => {
        this.#core.writeFtsBase(tableId, columnId, input);
      }),
    );
  }

  async removeFtsColumn(tableId: string, columnId: string): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "removeFtsColumn", tableId, columnId }, () => {
        this.#core.removeFtsColumn(tableId, columnId);
      }),
    );
  }

  async beginFtsBaseBuild(input: FtsBuildBegin): Promise<void> {
    await this.#run(() => {
      const key = `${input.tableId}/${input.columnId}`;
      const current = this.#ftsBuilds.get(key);
      if (
        current?.buildId === input.buildId &&
        current.ownerId === input.ownerId &&
        current.createdAt === input.createdAt &&
        current.expiresAt === input.expiresAt
      ) {
        return;
      }
      this.#logged({ op: "beginFtsBaseBuild", input }, () => this.#applyBeginFtsBaseBuild(input));
    });
  }

  async renewFtsBaseBuild(input: FtsBuildRenewal): Promise<void> {
    await this.#run(() =>
      this.#logged({ op: "renewFtsBaseBuild", input }, () => this.#applyRenewFtsBaseBuild(input)),
    );
  }

  async writeFtsBaseBuildChunk(input: FtsBuildAppend): Promise<void> {
    await this.#run(() => {
      const build = this.#requireFtsBuild(input);
      const replay = build.chunks.get(input.ordinal);
      if (replay !== undefined) {
        if (!sameFtsChunk(replay, input.chunk)) {
          throw new PostingBuildConflictError(input.buildId, input.ownerId, "chunk replay changed");
        }
        if (build.expiresAt === input.expiresAt) return;
      }
      this.#logged({ op: "writeFtsBaseBuildChunk", input }, () =>
        this.#applyFtsBaseBuildChunk(input),
      );
    });
  }

  async finishFtsBaseBuild(input: FtsBuildFinish): Promise<void> {
    await this.#run(() =>
      this.#logged({ op: "finishFtsBaseBuild", input }, () => this.#applyFinishFtsBaseBuild(input)),
    );
  }

  async abortFtsBaseBuild(input: FtsBuildAbort): Promise<void> {
    await this.#run(() => {
      if (this.#ftsBuilds.get(`${input.tableId}/${input.columnId}`) === undefined) return;
      this.#logged({ op: "abortFtsBaseBuild", input }, () => this.#applyAbortFtsBaseBuild(input));
    });
  }

  #applyBeginFtsBaseBuild(input: FtsBuildBegin): void {
    const key = `${input.tableId}/${input.columnId}`;
    const current = this.#ftsBuilds.get(key);
    if (current !== undefined && Date.parse(current.expiresAt) > Date.parse(input.createdAt)) {
      if (current.buildId === input.buildId && current.ownerId === input.ownerId) return;
      throw new PostingBuildConflictError(
        current.buildId,
        current.ownerId,
        "another live build owns the column",
      );
    }
    this.#ftsBuilds.set(key, {
      buildId: input.buildId,
      ownerId: input.ownerId,
      createdAt: input.createdAt,
      expiresAt: input.expiresAt,
      chunks: new Map(),
    });
  }

  #applyRenewFtsBaseBuild(input: FtsBuildRenewal): void {
    this.#requireFtsBuild(input).expiresAt = input.expiresAt;
  }

  #applyFtsBaseBuildChunk(input: FtsBuildAppend): void {
    const build = this.#requireFtsBuild(input);
    const replay = build.chunks.get(input.ordinal);
    if (replay !== undefined) {
      if (!sameFtsChunk(replay, input.chunk)) {
        throw new PostingBuildConflictError(input.buildId, input.ownerId, "chunk replay changed");
      }
    } else {
      if (input.ordinal !== build.chunks.size) {
        throw new Error(`Postings chunk is out of order: ${String(input.ordinal)}`);
      }
      build.chunks.set(input.ordinal, structuredClone(input.chunk));
    }
    build.expiresAt = input.expiresAt;
  }

  #applyFinishFtsBaseBuild(input: FtsBuildFinish): void {
    const key = `${input.tableId}/${input.columnId}`;
    const build = this.#requireFtsBuild(input);
    const chunks = Array.from({ length: input.chunkCount }, (_, ordinal) => {
      const chunk = build.chunks.get(ordinal);
      if (chunk === undefined) throw new Error(`Postings chunk is missing: ${String(ordinal)}`);
      return structuredClone(chunk) as FtsBaseInput["chunks"][number];
    });
    this.#core.writeFtsBase(input.tableId, input.columnId, {
      coversVersion: input.coversVersion,
      chunks,
      totalTokens: input.totalTokens,
    });
    this.#ftsBuilds.delete(key);
  }

  #applyAbortFtsBaseBuild(input: FtsBuildAbort): void {
    const key = `${input.tableId}/${input.columnId}`;
    const build = this.#ftsBuilds.get(key);
    if (build === undefined) return;
    if (
      build.buildId !== input.buildId ||
      (build.ownerId !== input.ownerId &&
        Date.parse(build.expiresAt) > Date.parse(input.expiresAtCutoff))
    ) {
      throw new PostingBuildConflictError(build.buildId, build.ownerId, "abort ownership changed");
    }
    this.#ftsBuilds.delete(key);
  }

  #requireFtsBuild(input: FtsBuildRenewal | FtsBuildFinish): {
    buildId: string;
    ownerId: string;
    createdAt: string;
    expiresAt: string;
    chunks: Map<number, FtsBuildChunk>;
  } {
    const build = this.#ftsBuilds.get(`${input.tableId}/${input.columnId}`);
    if (
      build?.buildId !== input.buildId ||
      build.ownerId !== input.ownerId ||
      Date.parse(build.expiresAt) <= Date.parse(input.expiresAtCutoff)
    ) {
      throw new PostingBuildConflictError(
        build?.buildId ?? input.buildId,
        build?.ownerId ?? input.ownerId,
        "session is missing, expired, or owned by another caller",
      );
    }
    return build;
  }

  async readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: FtsTerms,
    upToVersion: number,
    maxRowIds?: number,
  ) {
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

  // ---- temp spill: owner records are durable records; page bytes are instance scratch ----

  async createTempOwner(record: TempOwnerRecord): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "createTempOwner", record }, () => {
        this.#core.createTempOwner(record);
      }),
    );
  }

  async getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined> {
    return this.#core.getTempOwner(ownerId);
  }

  async renewTempOwner(input: RenewTempOwnerInput): Promise<TempOwnerRecord> {
    return this.#run(() =>
      this.#logged({ op: "renewTempOwner", input }, () => this.#core.renewTempOwner(input)),
    );
  }

  async removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean> {
    return this.#run(() =>
      this.#logged({ op: "removeTempOwnerIfExpired", ownerId, expiresAtCutoff }, () => {
        const removed = this.#core.removeTempOwnerIfExpired(ownerId, expiresAtCutoff);
        if (removed) this.#dropTempPages(ownerId);
        return removed;
      }),
    );
  }

  async removeTempOwner(ownerId: string): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "removeTempOwner", ownerId }, () => {
        this.#core.removeTempOwner(ownerId);
        this.#dropTempPages(ownerId);
      }),
    );
  }

  async listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    const pageOwnerIds = [...this.#tempUsageByOwner.keys()];
    return this.#core.listTempOwnerIdsPage(afterOwnerId, limit, pageOwnerIds);
  }

  async listExpiredTempOwnerPage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    return this.#core.listExpiredTempOwnerPage(expiresAtCutoff, afterCursor, limit);
  }

  async putTempRunPage(page: TempRunPage): Promise<void> {
    assertTempRunPageBatchLimits([page]);
    validateStorageId(page.ownerId, "Temp owner ID");
    validateStorageId(page.runId, "Temp run ID");
    if (!Number.isSafeInteger(page.pageIndex) || page.pageIndex < 0) {
      throw new RangeError("Temp run page index must be a non-negative whole number");
    }
    return this.#run(() => {
      if (this.#core.getTempOwner(page.ownerId) === undefined) {
        throw new Error(`Temp owner does not exist: ${page.ownerId}`);
      }
      const key = JSON.stringify([page.ownerId, page.runId, page.pageIndex]);
      const previousBytes = this.#tempPages.get(key)?.byteLength ?? 0;
      const isNewPage = !this.#tempPages.has(key);
      const usage = this.#tempUsageByOwner.get(page.ownerId) ?? {
        bytes: 0,
        pages: 0,
        runs: new Map<string, number>(),
      };
      const previousRunPages = usage.runs.get(page.runId) ?? 0;
      const byteDelta = page.bytes.byteLength - previousBytes;
      const pageDelta = isNewPage ? 1 : 0;
      const runDelta = isNewPage && previousRunPages === 0 ? 1 : 0;
      assertTempResourceLimit("temp owner byte", usage.bytes + byteDelta, MAX_TEMP_BYTES_PER_OWNER);
      assertTempResourceLimit("temp page", usage.pages + pageDelta, MAX_TEMP_PAGES_PER_OWNER);
      assertTempResourceLimit("temp run", usage.runs.size + runDelta, MAX_TEMP_RUNS_PER_OWNER);
      assertTempResourceLimit(
        "temporary byte",
        this.#tempBytesTotal + byteDelta,
        MAX_TEMP_BYTES_TOTAL,
      );
      assertTempResourceLimit(
        "temporary page total",
        this.#tempPagesTotal + pageDelta,
        MAX_TEMP_PAGES_TOTAL,
      );
      assertTempResourceLimit(
        "temporary run total",
        this.#tempRunsTotal + runDelta,
        MAX_TEMP_RUNS_TOTAL,
      );

      this.#tempPages.set(key, page.bytes.slice());
      const runKey = JSON.stringify([page.ownerId, page.runId]);
      const runKeys = this.#tempPageKeysByRun.get(runKey) ?? new Set<string>();
      runKeys.add(key);
      this.#tempPageKeysByRun.set(runKey, runKeys);
      usage.bytes += byteDelta;
      usage.pages += pageDelta;
      if (isNewPage) usage.runs.set(page.runId, previousRunPages + 1);
      this.#tempUsageByOwner.set(page.ownerId, usage);
      this.#tempBytesTotal += byteDelta;
      this.#tempPagesTotal += pageDelta;
      this.#tempRunsTotal += runDelta;
    });
  }

  async getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    return this.#tempPages.get(JSON.stringify([ownerId, runId, pageIndex]))?.slice();
  }

  async removeTempRun(ownerId: string, runId: string): Promise<void> {
    validateStorageId(ownerId, "Temp owner ID");
    validateStorageId(runId, "Temp run ID");
    return this.#run(() => this.#dropTempRunPages(ownerId, runId));
  }

  #dropTempPages(ownerId: string): void {
    const usage = this.#tempUsageByOwner.get(ownerId);
    if (usage === undefined) return;
    for (const runId of [...usage.runs.keys()]) this.#dropTempRunPages(ownerId, runId);
  }

  #dropTempRunPages(ownerId: string, runId: string): void {
    const runKey = JSON.stringify([ownerId, runId]);
    const keys = this.#tempPageKeysByRun.get(runKey);
    if (keys === undefined) return;
    let removedBytes = 0;
    for (const key of keys) {
      removedBytes += this.#tempPages.get(key)?.byteLength ?? 0;
      this.#tempPages.delete(key);
    }
    this.#tempPageKeysByRun.delete(runKey);
    this.#tempBytesTotal -= removedBytes;
    this.#tempPagesTotal -= keys.size;
    this.#tempRunsTotal -= 1;
    const usage = this.#tempUsageByOwner.get(ownerId);
    if (usage === undefined) return;
    usage.bytes -= removedBytes;
    usage.pages -= keys.size;
    usage.runs.delete(runId);
    if (usage.pages === 0) this.#tempUsageByOwner.delete(ownerId);
  }

  close(): void {
    this.#wal.close();
    this.#pool.close();
  }
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

const roots = new Map<BlockStore, FileSystemDirectoryHandle>();
const target: BlockStoreConformanceTarget = {
  create: async () => {
    const root = new MemoryOpfs().root;
    const store = await MiniLogStore.open(root);
    roots.set(store, root);
    return store;
  },
  reopen: async (store) => {
    const root = roots.get(store);
    if (root === undefined) throw new Error("unknown store");
    store.close();
    const reopened = await MiniLogStore.open(root);
    roots.set(reopened, root);
    return reopened;
  },
};

describe("a log-structured adapter built from public exports alone", () => {
  for (const conformanceCase of blockStoreConformanceCases()) {
    it(conformanceCase.name, () => conformanceCase.run(target));
  }
});
