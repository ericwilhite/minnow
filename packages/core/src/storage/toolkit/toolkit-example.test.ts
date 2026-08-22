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
import type {
  BlockStore,
  BlockWrite,
  CommitTransactionInput,
  CompactionJobRecord,
  CreateGarbageCollectionJobInput,
  GarbageCollectionJobRecord,
  GarbageCollectionStepResult,
  LeaseRecord,
  Manifest,
  ManifestSummary,
  PublishManifestInput,
  RowIdRange,
  RunGarbageCollectionStepInput,
  SegmentRecord,
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
  replayWalFrames,
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
type CompactionUpdate = Parameters<BlockStore["updateCompactionJob"]>[2];
type FtsBaseInput = Parameters<BlockStore["writeFtsBase"]>[2];
type FtsTerms = Parameters<BlockStore["readFtsCandidates"]>[2];

/** One durable mutation, exactly what replay needs to reproduce it. */
type Frame =
  | { op: "addBlocks"; blocks: Array<{ id: string; placement: Placement }> }
  | { op: "removeBlock"; id: string }
  | { op: "addTable"; record: TableRecord }
  | { op: "updateTable"; id: string; expectedRevision: number; update: TableUpdate }
  | { op: "removeTable"; id: string; expectedRevision: number }
  | { op: "addSegment"; record: SegmentRecord }
  | { op: "removeSegment"; id: string }
  | { op: "reserveRowIds"; tableId: string; count: number }
  | {
      op: "reserveAutoIncrement";
      tableId: string;
      columnId: string;
      count: number;
      atLeast?: bigint;
    }
  | { op: "publishManifest"; input: PublishManifestInput }
  | { op: "createTransaction"; record: TransactionRecord }
  | {
      op: "updateTransaction";
      id: string;
      expectedRevision: number;
      update: TransactionRecordUpdate;
    }
  | { op: "commitTransaction"; input: CommitTransactionInput }
  | { op: "createLease"; record: LeaseRecord }
  | { op: "renewLease"; id: string; expectedRevision: number; expiresAt: string }
  | { op: "removeLeaseIfExpired"; id: string; expectedRevision: number; expiresAtCutoff: string }
  | { op: "removeLease"; id: string }
  | { op: "createCompactionJob"; record: CompactionJobRecord }
  | { op: "updateCompactionJob"; id: string; expectedRevision: number; update: CompactionUpdate }
  | { op: "cancelCompactionJob"; id: string; expectedRevision: number; cancelledAt: string }
  | { op: "removeCompactionJob"; id: string }
  | { op: "createGarbageCollectionJob"; input: CreateGarbageCollectionJobInput }
  | {
      op: "garbageCollectionStep";
      effect: {
        job: GarbageCollectionJobRecord;
        prunedManifestVersions: number[];
        reclaimedSegmentIds: string[];
        reclaimedBlockIds: string[];
        reclaimedTransactionIds?: string[];
        updatedAt: string;
      };
    }
  | { op: "removeGarbageCollectionJob"; id: string }
  | { op: "removePrunedManifestRecords" }
  | { op: "createTempOwner"; record: TempOwnerRecord }
  | { op: "renewTempOwner"; ownerId: string; expectedRevision: number; expiresAt: string }
  | { op: "removeTempOwnerIfExpired"; ownerId: string; expiresAtCutoff: string }
  | { op: "removeTempOwner"; ownerId: string }
  | { op: "writeFtsBase"; tableId: string; columnId: string; input: FtsBaseInput };

interface Checkpoint {
  core: RecordCoreState;
  blocks: Array<readonly [string, Placement]>;
  extents: ExtentMeta;
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

class MiniLogStore implements BlockStore {
  readonly #files: ExtentFiles;
  readonly #wal: WalWriter;
  readonly #pool: ExtentPool;
  readonly #placements: Map<string, Placement>;
  readonly #core: RecordCore;
  /** Spill pages are scratch; they live and die with the instance, like the OPFS store's. */
  readonly #tempPages = new Map<string, Uint8Array>();
  #queue = Promise.resolve();

  private constructor(
    files: ExtentFiles,
    wal: WalWriter,
    pool: ExtentPool,
    placements: Map<string, Placement>,
    core: RecordCore,
  ) {
    this.#files = files;
    this.#wal = wal;
    this.#pool = pool;
    this.#placements = placements;
    this.#core = core;
  }

  static async open(root: FileSystemDirectoryHandle): Promise<MiniLogStore> {
    const files = filesOver(root);

    // 1. The newest checkpoint, when one exists. A torn write reads as "not written".
    let checkpoint: Checkpoint | undefined;
    try {
      const handle = await files.openHandle(["checkpoint"], { create: false });
      const bytes = new Uint8Array(handle.getSize());
      handle.read(bytes, { at: 0 });
      handle.close();
      checkpoint = decodeSyncCheckpoint(bytes) as Checkpoint | undefined;
    } catch {
      checkpoint = undefined;
    }

    const placements = new Map<string, Placement>(checkpoint?.blocks ?? []);
    const core = new RecordCore({
      hasBlock: (id) => placements.has(id),
      blockByteLength: (id) => placements.get(id)?.length,
    });
    if (checkpoint !== undefined) core.load(checkpoint.core);
    const pool = await ExtentPool.open(files, checkpoint?.extents);

    // 2. Replay every whole frame past the checkpoint; a torn tail is overwritten.
    const walHandle = await files.openHandle(["wal"], { create: true });
    const { payloads, endOffset } = replayWalFrames(walHandle);
    const wal = new WalWriter(walHandle, endOffset);
    const store = new MiniLogStore(files, wal, pool, placements, core);
    let newestExtent: Placement | undefined;
    for (const payload of payloads) {
      const frame = payload as Frame;
      store.#replay(frame);
      if (frame.op === "addBlocks") {
        for (const block of frame.blocks) {
          pool.restorePlacement(block.placement);
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
      case "addBlocks":
        for (const block of frame.blocks) this.#placements.set(block.id, block.placement);
        break;
      case "removeBlock":
        this.#placements.delete(frame.id);
        break;
      case "addTable":
        this.#core.addTable(frame.record);
        break;
      case "updateTable":
        this.#core.updateTable(frame.id, frame.expectedRevision, frame.update);
        break;
      case "removeTable":
        this.#core.removeTable(frame.id, frame.expectedRevision);
        break;
      case "addSegment":
        this.#core.addSegment(frame.record);
        break;
      case "removeSegment":
        this.#core.removeSegment(frame.id);
        break;
      case "reserveRowIds":
        this.#core.reserveRowIds(frame.tableId, frame.count);
        break;
      case "reserveAutoIncrement":
        this.#core.reserveAutoIncrement(frame.tableId, frame.columnId, frame.count, frame.atLeast);
        break;
      case "publishManifest":
        this.#core.publishManifest(frame.input);
        break;
      case "createTransaction":
        this.#core.createTransaction(frame.record);
        break;
      case "updateTransaction":
        this.#core.updateTransaction(frame.id, frame.expectedRevision, frame.update);
        break;
      case "commitTransaction":
        this.#core.commitTransaction(frame.input);
        break;
      case "createLease":
        this.#core.createLease(frame.record);
        break;
      case "renewLease":
        this.#core.renewLease(frame.id, frame.expectedRevision, frame.expiresAt);
        break;
      case "removeLeaseIfExpired":
        this.#core.removeLeaseIfExpired(frame.id, frame.expectedRevision, frame.expiresAtCutoff);
        break;
      case "removeLease":
        this.#core.removeLease(frame.id);
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
        this.#core.removePrunedManifestRecords();
        break;
      case "createTempOwner":
        this.#core.createTempOwner(frame.record);
        break;
      case "renewTempOwner":
        this.#core.renewTempOwner(frame.ownerId, frame.expectedRevision, frame.expiresAt);
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
    }
  }

  async #checkpoint(): Promise<void> {
    const payload: Checkpoint = {
      // dump()'s arrays alias live state — serialized here, in this same synchronous run.
      core: this.#core.dump(),
      blocks: [...this.#placements.entries()],
      extents: this.#pool.meta(),
    };
    const bytes = encodeSyncCheckpoint(payload);
    const handle = await this.#files.openHandle(["checkpoint"], { create: true });
    try {
      handle.truncate(0);
      handle.write(bytes, { at: 0 });
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

  async addBlock(id: string, bytes: Uint8Array): Promise<void> {
    return this.addBlocks([{ id, bytes }]);
  }

  async addBlocks(blocks: readonly BlockWrite[]): Promise<void> {
    return this.#run(async () => {
      const ids = new Set<string>();
      for (const block of blocks) {
        if (ids.has(block.id) || this.#placements.has(block.id)) {
          throw new Error(`Block already exists: ${block.id}`);
        }
        ids.add(block.id);
      }
      const entries: Array<{ id: string; placement: Placement }> = [];
      for (const block of blocks) {
        entries.push({ id: block.id, placement: await this.#pool.append(block.bytes, true) });
      }
      this.#logged({ op: "addBlocks", blocks: entries }, () => {
        for (const entry of entries) this.#placements.set(entry.id, entry.placement);
      });
    });
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    return this.#run(() => {
      const placement = this.#placements.get(id);
      return placement === undefined ? undefined : this.#pool.read(placement);
    });
  }

  async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    const blocks: Array<Uint8Array | undefined> = [];
    for (const id of ids) blocks.push(await this.getBlock(id));
    return blocks;
  }

  async removeBlock(id: string): Promise<void> {
    return this.#run(async () => {
      const placement = this.#placements.get(id);
      if (placement === undefined) return;
      const drained = this.#pool.release([placement]);
      this.#logged({ op: "removeBlock", id }, () => this.#placements.delete(id));
      for (const extent of drained) await this.#pool.deleteExtent(extent);
    });
  }

  async listBlockIds(): Promise<string[]> {
    return [...this.#placements.keys()].sort();
  }

  // ---- records: every mutation is one core call plus one frame ----

  async addTable(record: TableRecord): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "addTable", record }, () => {
        this.#core.addTable(record);
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

  async removeTable(id: string, expectedRevision: number): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "removeTable", id, expectedRevision }, () => {
        this.#core.removeTable(id, expectedRevision);
      }),
    );
  }

  async addSegment(record: SegmentRecord): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "addSegment", record }, () => {
        this.#core.addSegment(record);
      }),
    );
  }

  async getSegment(id: string): Promise<SegmentRecord | undefined> {
    return this.#core.getSegment(id);
  }

  async listSegments(tableId?: string): Promise<SegmentRecord[]> {
    return this.#core.listSegments(tableId);
  }

  async listSegmentPage(afterId: string | null, limit: number) {
    return this.#core.listSegmentPage(afterId, limit);
  }

  async removeSegment(id: string): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "removeSegment", id }, () => {
        this.#core.removeSegment(id);
      }),
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

  async getCurrentManifest(): Promise<Manifest | undefined> {
    return this.#core.getCurrentManifest();
  }

  async getCurrentManifestVersion(): Promise<number | null> {
    return this.#core.getCurrentManifestVersion();
  }

  async getManifest(version: number): Promise<Manifest | undefined> {
    return this.#core.getManifest(version);
  }

  async listManifests(): Promise<Manifest[]> {
    return this.#core.listManifests();
  }

  async listManifestPage(afterVersion: number | null, limit: number) {
    return this.#core.listManifestPage(afterVersion, limit);
  }

  async publishManifest(input: PublishManifestInput): Promise<Manifest> {
    return this.#run(() =>
      this.#logged({ op: "publishManifest", input }, () => this.#core.publishManifest(input)),
    );
  }

  async createTransaction(record: TransactionRecord): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "createTransaction", record }, () => {
        this.#core.createTransaction(record);
      }),
    );
  }

  async getTransaction(id: string): Promise<TransactionRecord | undefined> {
    return this.#core.getTransaction(id);
  }

  async getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>> {
    return this.#core.getTransactions(ids);
  }

  async listTransactions(): Promise<TransactionRecord[]> {
    return this.#core.listTransactions();
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

  async renewLease(id: string, expectedRevision: number, expiresAt: string): Promise<LeaseRecord> {
    return this.#run(() =>
      this.#logged({ op: "renewLease", id, expectedRevision, expiresAt }, () =>
        this.#core.renewLease(id, expectedRevision, expiresAt),
      ),
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

  async removeLease(id: string): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "removeLease", id }, () => {
        this.#core.removeLease(id);
      }),
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

  async removeCompactionJob(id: string): Promise<void> {
    return this.#run(() =>
      this.#logged({ op: "removeCompactionJob", id }, () => {
        this.#core.removeCompactionJob(id);
      }),
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

  async getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    return this.#core.getGarbageCollectionJob(id);
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return this.#core.listGarbageCollectionJobs();
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

  async removePrunedManifestRecords(): Promise<number> {
    return this.#run(() =>
      this.#logged(
        { op: "removePrunedManifestRecords" },
        () => this.#core.removePrunedManifestRecords().length,
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

  async readFtsCandidates(tableId: string, columnId: string, terms: FtsTerms, upToVersion: number) {
    return this.#core.readFtsCandidates(tableId, columnId, terms, upToVersion);
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

  async renewTempOwner(
    ownerId: string,
    expectedRevision: number,
    expiresAt: string,
  ): Promise<TempOwnerRecord> {
    return this.#run(() =>
      this.#logged({ op: "renewTempOwner", ownerId, expectedRevision, expiresAt }, () =>
        this.#core.renewTempOwner(ownerId, expectedRevision, expiresAt),
      ),
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
    const pageOwnerIds: string[] = [];
    for (const key of this.#tempPages.keys()) {
      pageOwnerIds.push((JSON.parse(key) as [string, string, number])[0]);
    }
    return this.#core.listTempOwnerIdsPage(afterOwnerId, limit, pageOwnerIds);
  }

  async putTempRunPage(page: TempRunPage): Promise<void> {
    this.#tempPages.set(
      JSON.stringify([page.ownerId, page.runId, page.pageIndex]),
      page.bytes.slice(),
    );
  }

  async getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    return this.#tempPages.get(JSON.stringify([ownerId, runId, pageIndex]))?.slice();
  }

  async removeTempRun(ownerId: string, runId: string): Promise<void> {
    for (const key of this.#tempPages.keys()) {
      const [owner, run] = JSON.parse(key) as [string, string, number];
      if (owner === ownerId && run === runId) this.#tempPages.delete(key);
    }
  }

  #dropTempPages(ownerId: string): void {
    for (const key of this.#tempPages.keys()) {
      if ((JSON.parse(key) as [string, string, number])[0] === ownerId) {
        this.#tempPages.delete(key);
      }
    }
  }

  close(): void {
    this.#wal.close();
    this.#pool.close();
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
    return MiniLogStore.open(root);
  },
};

describe("a log-structured adapter built from public exports alone", () => {
  for (const conformanceCase of blockStoreConformanceCases()) {
    it(conformanceCase.name, () => conformanceCase.run(target));
  }
});
