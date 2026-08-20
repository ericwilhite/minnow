import {
  type BeginTransactionInput,
  type BeginTransactionResult,
  type BlockWrite,
  type CatalogProbe,
  type CommitTransactionInput,
  type CompactionJobRecord,
  type CompactionJobRecordUpdate,
  type CreateGarbageCollectionJobInput,
  type FtsCandidates,
  type FtsColumnIndexRecord,
  type FtsPosting,
  type GarbageCollectionJobRecord,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  type Manifest,
  type ManifestSummary,
  type PublishManifestInput,
  type QueryCatalogState,
  type RowIdRange,
  type RunGarbageCollectionStepInput,
  type SegmentRecord,
  type StageTransactionArtifactsInput,
  type StoragePage,
  type TableColumnRecord,
  type TableRecord,
  type TempOwnerRecord,
  type TempRunPage,
  type TransactionRecord,
  type TransactionRecordUpdate,
  type TriggerRecord,
  collectFtsCandidates,
} from "../types.js";
import type { DatabaseSnapshot, SnapshotLoadProgress } from "../snapshot.js";
import {
  RecordCore,
  type RecordCoreState,
  validateBeginTransactionInput,
  validateId,
  validateTempRunPage,
  validateTempRunPageIdentity,
} from "../toolkit/record-core.js";
import { OpfsTree, encodeSegment, decodeSegment } from "./files.js";
import {
  decodeChunk,
  decodeSyncCheckpoint,
  encodeChunk,
  encodeSyncCheckpoint,
} from "../toolkit/wire.js";
import { WalWriter, replayWalFrames } from "../toolkit/wal.js";
import { ExtentPool, type ExtentMeta, type Placement } from "../toolkit/extents.js";

/** Checkpoint when the WAL passes either bound; both trade replay time against pause time. */
const CHECKPOINT_WAL_BYTES = 4 * 1024 * 1024;
const CHECKPOINT_ENTRIES = 1024;
/** Decoded full-text base chunks kept in memory; least recently loaded goes first. */
const FTS_CHUNK_CACHE_SIZE = 64;

interface IdPlacement {
  id: string;
  placement: Placement;
}

interface FtsBasePointer {
  coversVersion: number;
  totalTokens: number;
  chunks: Placement[];
  chunkBounds: Array<{ first: string; last: string }>;
}

interface CheckpointState {
  lastSeq: number;
  core: RecordCoreState;
  blockIndex: Array<readonly [string, Placement]>;
  ftsBases: Array<readonly [string, FtsBasePointer]>;
  extents: ExtentMeta;
}

type WalEntryBody =
  | { op: "addBlocks"; placements: IdPlacement[] }
  | { op: "removeBlock"; id: string }
  | { op: "addTable"; record: TableRecord }
  | {
      op: "updateTable";
      id: string;
      expectedRevision: number;
      update: {
        columns?: TableColumnRecord[];
        ftsColumns?: Record<string, FtsColumnIndexRecord> | null;
        triggers?: TriggerRecord[] | null;
      };
    }
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
  | { op: "beginTransaction"; input: BeginTransactionInput }
  | { op: "createTransaction"; record: TransactionRecord }
  | {
      op: "updateTransaction";
      id: string;
      expectedRevision: number;
      update: TransactionRecordUpdate;
    }
  | {
      op: "stageTransactionArtifacts";
      transactionId: string;
      expectedRevision: number;
      blocks: IdPlacement[];
      segments: SegmentRecord[];
      updatedAt: string;
    }
  | { op: "commitTransaction"; input: CommitTransactionInput }
  | { op: "createLease"; record: LeaseRecord }
  | { op: "renewLease"; id: string; expectedRevision: number; expiresAt: string }
  | { op: "removeLeaseIfExpired"; id: string; expectedRevision: number; expiresAtCutoff: string }
  | { op: "removeLease"; id: string }
  | { op: "createCompactionJob"; record: CompactionJobRecord }
  | {
      op: "updateCompactionJob";
      id: string;
      expectedRevision: number;
      update: CompactionJobRecordUpdate;
    }
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
        updatedAt: string;
      };
    }
  | { op: "removeGarbageCollectionJob"; id: string }
  | { op: "createTempOwner"; record: TempOwnerRecord }
  | { op: "renewTempOwner"; ownerId: string; expectedRevision: number; expiresAt: string }
  | { op: "removeTempOwnerIfExpired"; ownerId: string; expiresAtCutoff: string }
  | { op: "removeTempOwner"; ownerId: string }
  | { op: "writeFtsBase"; tableId: string; columnId: string; pointer: FtsBasePointer }
  | {
      op: "importSnapshot";
      records: Omit<DatabaseSnapshot, "blocks">;
      blockPlacements: IdPlacement[];
    };

type WalEntry = WalEntryBody & { seq: number };

/**
 * The leader: the one instance holding the database's write-ahead log handle, and with it
 * every other handle — checkpoint slots, the tail extent, a small cache of sealed extents.
 *
 * Because the handles are held, every operation's storage cost is a synchronous write or read
 * measured in microseconds, and because leadership is exclusive by the browser's own file
 * lock, the leader's in-memory `RecordCore` is always provably current — there are no
 * freshness probes anywhere. An operation is: validate and mutate the core (synchronously),
 * append one checksummed WAL frame (synchronously), acknowledge. The only awaits sit before
 * that critical section (extent appends that may seal, temp-file IO), so no observer ever
 * sees state mid-mutation.
 *
 * Checkpointing is also synchronous — dump, write to the older of two fixed slots, flush,
 * reset the WAL — which is what makes a single WAL file safe: the flush precedes the reset,
 * and a crash mid-checkpoint corrupts only the slot being written while the other slot plus
 * the un-reset WAL still reconstruct everything.
 */
export class OpfsLeader {
  readonly #tree: OpfsTree;
  readonly #strict: boolean;
  readonly #core: RecordCore;
  readonly #blockIndex = new Map<string, Placement>();
  readonly #ftsBases = new Map<string, FtsBasePointer>();
  readonly #ftsChunkCache = new Map<string, FtsPosting[]>();
  #extents: ExtentPool | undefined;
  #wal: WalWriter;
  /** The pool exists from the moment recovery succeeds; before that, nothing may touch it. */
  get #pool(): ExtentPool {
    const pool = this.#extents;
    if (pool === undefined) throw new Error("The OPFS leader has not recovered yet");
    return pool;
  }
  readonly #walHandle: FileSystemSyncAccessHandle;
  readonly #slots: [FileSystemSyncAccessHandle, FileSystemSyncAccessHandle];
  /** The slot holding the newest checkpoint; the next checkpoint writes to the other. */
  #newestSlot = 0;
  #seq = 0;
  #entriesSinceCheckpoint = 0;
  #permissivePhysical = false;
  #poisoned = false;
  #closed = false;
  #chain: Promise<unknown> = Promise.resolve();
  readonly #checkpointEntries: number;
  #lastCheckpointBytes = 0;
  #checkpointScheduled = false;

  private constructor(
    tree: OpfsTree,
    strict: boolean,
    walHandle: FileSystemSyncAccessHandle,
    slots: [FileSystemSyncAccessHandle, FileSystemSyncAccessHandle],
    checkpointEntries?: number,
  ) {
    this.#checkpointEntries = checkpointEntries ?? CHECKPOINT_ENTRIES;
    this.#tree = tree;
    this.#strict = strict;
    this.#walHandle = walHandle;
    this.#slots = slots;
    this.#wal = new WalWriter(walHandle, 0);
    this.#core = new RecordCore({
      hasBlock: (id) => this.#permissivePhysical || this.#blockIndex.has(id),
      blockByteLength: (id) =>
        this.#blockIndex.get(id)?.length ?? (this.#permissivePhysical ? 0 : undefined),
    });
  }

  /** Builds a leader from held handles: newest valid checkpoint slot plus the WAL tail. */
  static async recover(
    tree: OpfsTree,
    strict: boolean,
    handles: {
      wal: FileSystemSyncAccessHandle;
      slotA: FileSystemSyncAccessHandle;
      slotB: FileSystemSyncAccessHandle;
    },
    checkpointEntries?: number,
  ): Promise<OpfsLeader> {
    const leader = new OpfsLeader(
      tree,
      strict,
      handles.wal,
      [handles.slotA, handles.slotB],
      checkpointEntries,
    );
    try {
      await leader.#loadFromDisk();
    } catch (error) {
      // A failed recovery must release every handle it acquired, or the browser's file lock
      // outlives the attempt and no connection can ever lead this database again.
      leader.crash();
      throw error;
    }
    return leader;
  }

  async #loadFromDisk(): Promise<void> {
    const states = this.#slots.map((slot) => {
      const size = slot.getSize();
      const bytes = new Uint8Array(size);
      slot.read(bytes, { at: 0 });
      return decodeSyncCheckpoint(bytes) as CheckpointState | undefined;
    });
    let checkpoint: CheckpointState | undefined;
    this.#newestSlot = 0;
    states.forEach((state, index) => {
      if (state !== undefined && (checkpoint === undefined || state.lastSeq > checkpoint.lastSeq)) {
        checkpoint = state;
        this.#newestSlot = index;
      }
    });

    this.#blockIndex.clear();
    this.#ftsBases.clear();
    this.#ftsChunkCache.clear();
    this.#extents?.close();
    this.#extents = await ExtentPool.open(this.#tree, checkpoint?.extents);
    if (checkpoint === undefined) {
      this.#core.load(EMPTY_CORE_STATE());
      this.#seq = 0;
    } else {
      this.#core.load(checkpoint.core);
      for (const [id, placement] of checkpoint.blockIndex) this.#blockIndex.set(id, placement);
      for (const [key, pointer] of checkpoint.ftsBases) this.#ftsBases.set(key, pointer);
      this.#seq = checkpoint.lastSeq;
    }

    const newest = this.#slots[this.#newestSlot];
    this.#lastCheckpointBytes = newest === undefined ? 0 : newest.getSize();
    const { payloads, endOffset } = replayWalFrames(this.#walHandle);
    let applied = 0;
    let tailExtent = -1;
    let tailEnd = 0;
    for (const payload of payloads) {
      const entry = payload as WalEntry;
      if (entry.seq <= this.#seq) continue; // Covered by the checkpoint; reset had not run yet.
      applied += 1;
      this.#applyReplayed(entry);
      for (const placement of placementsOf(entry)) {
        this.#pool.restorePlacement(placement);
        if (
          placement.extent > tailExtent ||
          (placement.extent === tailExtent && placement.offset + placement.length > tailEnd)
        ) {
          tailExtent = placement.extent;
          tailEnd = placement.offset + placement.length;
        }
      }
      this.#seq = entry.seq;
    }
    if (tailExtent > this.#pool.tailExtentId) {
      await this.#pool.adoptTail(tailExtent, tailEnd);
    }
    this.#wal = new WalWriter(this.#walHandle, endOffset);
    this.#entriesSinceCheckpoint = applied;
    this.#poisoned = false;

    // Extents fully drained before the crash but never deleted: finish the job.
    for (const id of this.#pool.release([])) await this.#pool.deleteExtent(id);
  }

  // ---------------------------------------------------------------------------------------
  // The logged-operation spine.
  // ---------------------------------------------------------------------------------------

  #run<T>(work: () => Promise<T> | T): Promise<T> {
    const result = this.#chain.then(async () => {
      if (this.#closed) throw new Error("This OPFS store connection is closed");
      if (this.#poisoned) await this.#loadFromDisk();
      return work();
    });
    this.#chain = result.catch(() => undefined);
    return result;
  }

  /**
   * Reads answer from memory without the queue — but never from a poisoned instance, whose
   * memory ran ahead of a write the disk refused. The fast path is one boolean check; the
   * poisoned path rides the queue, which reloads from the held handles first.
   */
  async #healthy(): Promise<void> {
    if (!this.#poisoned) return;
    await this.#run(() => undefined);
  }

  /**
   * The critical section: apply to the core and append the frame in one synchronous run.
   * A validation throw leaves the core untouched (validate-then-mutate bodies); a WAL write
   * failure after mutation poisons the leader, which reloads from its own handles before the
   * next operation, and the original error — quota, most importantly — escapes unwrapped.
   */
  #logged(body: WalEntryBody): unknown {
    const result = this.#applyBody(body);
    this.#appendFrame(body);
    return result;
  }

  #appendFrame(body: WalEntryBody): void {
    try {
      this.#seq += 1;
      this.#wal.append({ seq: this.#seq, ...body }, this.#strict);
      this.#entriesSinceCheckpoint += 1;
    } catch (error) {
      this.#poisoned = true;
      throw error;
    }
    // The checkpoint runs as its own queued step: the operation that trips the threshold is
    // already durable and should not absorb the pause — or a checkpoint failure. A failed
    // checkpoint (quota, most plausibly) leaves the WAL covering everything and retries on a
    // later trigger; the operations themselves stay correct throughout.
    if (this.#checkpointDue() && !this.#checkpointScheduled) {
      this.#checkpointScheduled = true;
      void this.#run(() => {
        this.#checkpointScheduled = false;
        if (this.#checkpointDue()) this.checkpointNow();
      }).catch(() => undefined);
    }
  }

  /**
   * Due when the WAL outgrows the larger of the floor and the previous checkpoint's own size —
   * so checkpointing amortizes to O(bytes written) instead of re-serializing the whole state
   * every fixed interval during a bulk load — or when the frame count alone would make a
   * replay slow.
   */
  #checkpointDue(): boolean {
    return (
      this.#wal.byteLength >= Math.max(CHECKPOINT_WAL_BYTES, this.#lastCheckpointBytes) ||
      this.#entriesSinceCheckpoint >= this.#checkpointEntries
    );
  }

  #applyBody(body: WalEntryBody): unknown {
    switch (body.op) {
      case "addBlocks": {
        for (const { id, placement } of body.placements) this.#blockIndex.set(id, placement);
        return undefined;
      }
      case "removeBlock": {
        const placement = this.#blockIndex.get(body.id);
        this.#blockIndex.delete(body.id);
        if (placement !== undefined) this.#pool.release([placement]);
        return undefined;
      }
      case "addTable":
        return this.#core.addTable(body.record);
      case "updateTable":
        return this.#core.updateTable(body.id, body.expectedRevision, body.update);
      case "removeTable": {
        this.#core.removeTable(body.id, body.expectedRevision);
        const owned = `${body.id}/`;
        for (const [key, pointer] of [...this.#ftsBases]) {
          if (!key.startsWith(owned)) continue;
          this.#ftsBases.delete(key);
          this.#dropFtsChunkCache(key);
          this.#pool.release(pointer.chunks);
        }
        return undefined;
      }
      case "addSegment":
        return this.#core.addSegment(body.record);
      case "removeSegment":
        return this.#core.removeSegment(body.id);
      case "reserveRowIds":
        return this.#core.reserveRowIds(body.tableId, body.count);
      case "reserveAutoIncrement":
        return this.#core.reserveAutoIncrement(
          body.tableId,
          body.columnId,
          body.count,
          body.atLeast,
        );
      case "publishManifest":
        return this.#core.publishManifest(body.input);
      case "beginTransaction":
        return this.#core.beginTransaction(body.input);
      case "createTransaction":
        return this.#core.createTransaction(body.record);
      case "updateTransaction":
        return this.#core.updateTransaction(body.id, body.expectedRevision, body.update);
      case "stageTransactionArtifacts": {
        for (const { id, placement } of body.blocks) this.#blockIndex.set(id, placement);
        return this.#core.stageTransactionArtifacts(
          {
            transactionId: body.transactionId,
            expectedRevision: body.expectedRevision,
            blocks: body.blocks.map(({ id }) => ({ id, bytes: EMPTY_BYTES })),
            segments: body.segments,
            updatedAt: body.updatedAt,
          },
          { blocksPrevalidated: true },
        );
      }
      case "commitTransaction":
        return this.#core.commitTransaction(body.input);
      case "createLease":
        return this.#core.createLease(body.record);
      case "renewLease":
        return this.#core.renewLease(body.id, body.expectedRevision, body.expiresAt);
      case "removeLeaseIfExpired":
        return this.#core.removeLeaseIfExpired(
          body.id,
          body.expectedRevision,
          body.expiresAtCutoff,
        );
      case "removeLease":
        return this.#core.removeLease(body.id);
      case "createCompactionJob":
        return this.#core.createCompactionJob(body.record);
      case "updateCompactionJob":
        return this.#core.updateCompactionJob(body.id, body.expectedRevision, body.update);
      case "cancelCompactionJob":
        return this.#core.cancelCompactionJob(body.id, body.expectedRevision, body.cancelledAt);
      case "removeCompactionJob":
        return this.#core.removeCompactionJob(body.id);
      case "createGarbageCollectionJob":
        return this.#core.createGarbageCollectionJob(body.input);
      case "garbageCollectionStep": {
        this.#core.applyGarbageCollectionEffect(body.effect);
        this.#releaseReclaimedBlocks(body.effect.reclaimedBlockIds);
        return undefined;
      }
      case "removeGarbageCollectionJob":
        return this.#core.removeGarbageCollectionJob(body.id);
      case "createTempOwner":
        return this.#core.createTempOwner(body.record);
      case "renewTempOwner":
        return this.#core.renewTempOwner(body.ownerId, body.expectedRevision, body.expiresAt);
      case "removeTempOwnerIfExpired":
        return this.#core.removeTempOwnerIfExpired(body.ownerId, body.expiresAtCutoff);
      case "removeTempOwner":
        return this.#core.removeTempOwner(body.ownerId);
      case "writeFtsBase": {
        const key = `${body.tableId}/${body.columnId}`;
        const previous = this.#ftsBases.get(key);
        if (previous !== undefined) {
          this.#pool.release(previous.chunks);
          this.#dropFtsChunkCache(key);
        }
        this.#ftsBases.set(key, structuredClone(body.pointer));
        this.#core.pruneFtsDeltas(body.tableId, body.columnId, body.pointer.coversVersion);
        return undefined;
      }
      case "importSnapshot": {
        if (this.#core.getCurrentManifestVersion() !== null || this.#core.listTables().length > 0) {
          throw new Error("This store already holds a database");
        }
        for (const { id, placement } of body.blockPlacements) this.#blockIndex.set(id, placement);
        this.#core.loadSnapshotRecords(
          { ...body.records, blocks: undefined },
          body.blockPlacements.map(({ id }) => id),
        );
        return undefined;
      }
    }
  }

  #applyReplayed(entry: WalEntry): void {
    this.#permissivePhysical = true;
    try {
      const { seq: _seq, ...body } = entry;
      void _seq;
      this.#applyBody(body);
    } finally {
      this.#permissivePhysical = false;
    }
  }

  #releaseReclaimedBlocks(ids: readonly string[]): void {
    const placements: Placement[] = [];
    for (const id of ids) {
      const placement = this.#blockIndex.get(id);
      if (placement !== undefined) placements.push(placement);
      this.#blockIndex.delete(id);
    }
    this.#pool.release(placements);
  }

  /** Fully synchronous: dump, write the older slot, flush it, then and only then reset. */
  checkpointNow(): void {
    const state: CheckpointState = {
      lastSeq: this.#seq,
      core: this.#core.dump(),
      blockIndex: [...this.#blockIndex.entries()],
      ftsBases: [...this.#ftsBases.entries()],
      extents: this.#pool.meta(),
    };
    const bytes = encodeSyncCheckpoint(state);
    const slotIndex = this.#newestSlot === 0 ? 1 : 0;
    const slot = this.#slots[slotIndex];
    slot.truncate(0);
    slot.write(bytes, { at: 0 });
    slot.flush();
    this.#newestSlot = slotIndex;
    this.#wal.reset();
    this.#entriesSinceCheckpoint = 0;
    this.#lastCheckpointBytes = bytes.byteLength;
  }

  async #deleteDrainedExtents(): Promise<void> {
    for (const id of this.#pool.release([])) {
      await this.#pool.deleteExtent(id);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Blocks.
  // ---------------------------------------------------------------------------------------

  async addBlock(id: string, bytes: Uint8Array): Promise<void> {
    return this.addBlocks([{ id, bytes }]);
  }

  async addBlocks(blocks: readonly BlockWrite[]): Promise<void> {
    return this.#run(async () => {
      const ids = new Set<string>();
      for (const block of blocks) {
        validateId(block.id);
        if (ids.has(block.id) || this.#blockIndex.has(block.id)) {
          throw new Error(`Block already exists: ${block.id}`);
        }
        ids.add(block.id);
      }
      const placements: IdPlacement[] = [];
      try {
        for (const block of blocks) {
          placements.push({
            id: block.id,
            placement: await this.#pool.append(block.bytes, this.#strict),
          });
        }
      } catch (error) {
        // Nothing was recorded; the written bytes are dead space. Undo the accounting.
        this.#pool.release(placements.map(({ placement }) => placement));
        throw error;
      }
      this.#logged({ op: "addBlocks", placements });
    });
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    await this.#healthy();
    const placement = this.#blockIndex.get(id);
    if (placement === undefined) return undefined;
    return this.#pool.read(placement);
  }

  async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    return Promise.all(ids.map((id) => this.getBlock(id)));
  }

  async removeBlock(id: string): Promise<void> {
    await this.#run(() => {
      if (!this.#blockIndex.has(id)) return;
      this.#logged({ op: "removeBlock", id });
    });
    await this.#deleteDrainedExtents();
  }

  async listBlockIds(): Promise<string[]> {
    await this.#healthy();
    return [...this.#blockIndex.keys()].sort();
  }

  // ---------------------------------------------------------------------------------------
  // Temp spill pages: instance-local files, never logged, never proxied.
  // ---------------------------------------------------------------------------------------

  async putTempRunPage(page: TempRunPage): Promise<void> {
    validateTempRunPage(page);
    await this.#tree.writeFile(tempPagePath(page.ownerId, page.runId, page.pageIndex), page.bytes);
  }

  async getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    validateTempRunPageIdentity(ownerId, runId, pageIndex);
    return this.#tree.readFile(tempPagePath(ownerId, runId, pageIndex));
  }

  async removeTempRun(ownerId: string, runId: string): Promise<void> {
    validateTempRunPageIdentity(ownerId, runId, 0);
    await this.#tree.deleteTree(["temp", encodeSegment(ownerId), encodeSegment(runId)]);
  }

  async removeTempOwner(ownerId: string): Promise<void> {
    validateId(ownerId);
    await this.#run(() => this.#logged({ op: "removeTempOwner", ownerId }));
    await this.#tree.deleteTree(["temp", encodeSegment(ownerId)]);
  }

  async createTempOwner(record: TempOwnerRecord): Promise<void> {
    await this.#run(() => this.#logged({ op: "createTempOwner", record }));
  }

  async getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined> {
    await this.#healthy();
    return this.#core.getTempOwner(ownerId);
  }

  async renewTempOwner(
    ownerId: string,
    expectedRevision: number,
    expiresAt: string,
  ): Promise<TempOwnerRecord> {
    return this.#run(
      () =>
        this.#logged({
          op: "renewTempOwner",
          ownerId,
          expectedRevision,
          expiresAt,
        }) as TempOwnerRecord,
    );
  }

  async removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean> {
    const removed = await this.#run(
      () => this.#logged({ op: "removeTempOwnerIfExpired", ownerId, expiresAtCutoff }) as boolean,
    );
    if (removed) await this.#tree.deleteTree(["temp", encodeSegment(ownerId)]);
    return removed;
  }

  async listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    await this.#healthy();
    const names = await this.#tree.listNames(["temp"]);
    return this.#core.listTempOwnerIdsPage(afterOwnerId, limit, names.map(decodeSegment));
  }

  // ---------------------------------------------------------------------------------------
  // Record reads: the leader's memory is always current; no probes, no refreshes.
  // ---------------------------------------------------------------------------------------

  async getTable(id: string): Promise<TableRecord | undefined> {
    await this.#healthy();
    return this.#core.getTable(id);
  }

  async getTableByName(name: string): Promise<TableRecord | undefined> {
    await this.#healthy();
    return this.#core.getTableByName(name);
  }

  async listTables(): Promise<TableRecord[]> {
    await this.#healthy();
    return this.#core.listTables();
  }

  async getSegment(id: string): Promise<SegmentRecord | undefined> {
    await this.#healthy();
    return this.#core.getSegment(id);
  }

  async listSegments(tableId?: string): Promise<SegmentRecord[]> {
    await this.#healthy();
    return this.#core.listSegments(tableId);
  }

  async getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    await this.#healthy();
    return this.#core.getExistingUniqueKeys(tableId, keyTokens);
  }

  async getCurrentManifestVersion(): Promise<number | null> {
    await this.#healthy();
    return this.#core.getCurrentManifestVersion();
  }

  async getCatalogProbe(): Promise<CatalogProbe> {
    await this.#healthy();
    return this.#core.getCatalogProbe();
  }

  async getQueryCatalogState(tableNames: readonly string[]): Promise<QueryCatalogState> {
    await this.#healthy();
    return this.#core.getQueryCatalogState(tableNames);
  }

  async getCurrentManifest(): Promise<Manifest | undefined> {
    await this.#healthy();
    return this.#core.getCurrentManifest();
  }

  async getManifest(version: number): Promise<Manifest | undefined> {
    await this.#healthy();
    return this.#core.getManifest(version);
  }

  async listManifests(): Promise<Manifest[]> {
    await this.#healthy();
    return this.#core.listManifests();
  }

  async listManifestPage(
    afterVersion: number | null,
    limit: number,
  ): Promise<StoragePage<Manifest, number>> {
    await this.#healthy();
    return this.#core.listManifestPage(afterVersion, limit);
  }

  async getTransaction(id: string): Promise<TransactionRecord | undefined> {
    await this.#healthy();
    return this.#core.getTransaction(id);
  }

  async getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>> {
    await this.#healthy();
    return this.#core.getTransactions(ids);
  }

  async listTransactions(): Promise<TransactionRecord[]> {
    await this.#healthy();
    return this.#core.listTransactions();
  }

  async listTransactionPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<TransactionRecord, string>> {
    await this.#healthy();
    return this.#core.listTransactionPage(afterId, limit);
  }

  async getLease(id: string): Promise<LeaseRecord | undefined> {
    await this.#healthy();
    return this.#core.getLease(id);
  }

  async listLeases(): Promise<LeaseRecord[]> {
    await this.#healthy();
    return this.#core.listLeases();
  }

  async getCompactionJob(id: string): Promise<CompactionJobRecord | undefined> {
    await this.#healthy();
    return this.#core.getCompactionJob(id);
  }

  async listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]> {
    await this.#healthy();
    return this.#core.listCompactionJobs(tableId);
  }

  async listCompactionJobPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<CompactionJobRecord, string>> {
    await this.#healthy();
    return this.#core.listCompactionJobPage(afterId, limit);
  }

  async getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    await this.#healthy();
    return this.#core.getGarbageCollectionJob(id);
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    await this.#healthy();
    return this.#core.listGarbageCollectionJobs();
  }

  // ---------------------------------------------------------------------------------------
  // Logged record mutations.
  // ---------------------------------------------------------------------------------------

  async addTable(record: TableRecord): Promise<void> {
    await this.#run(() => this.#logged({ op: "addTable", record }));
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
    return this.#run(
      () => this.#logged({ op: "updateTable", id, expectedRevision, update }) as TableRecord,
    );
  }

  async removeTable(id: string, expectedRevision: number): Promise<void> {
    await this.#run(() => this.#logged({ op: "removeTable", id, expectedRevision }));
    await this.#deleteDrainedExtents();
  }

  async addSegment(record: SegmentRecord): Promise<void> {
    await this.#run(() => this.#logged({ op: "addSegment", record }));
  }

  async removeSegment(id: string): Promise<void> {
    await this.#run(() => this.#logged({ op: "removeSegment", id }));
  }

  async reserveRowIds(tableId: string, count: number): Promise<RowIdRange> {
    return this.#run(() => this.#logged({ op: "reserveRowIds", tableId, count }) as RowIdRange);
  }

  async reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): Promise<RowIdRange> {
    return this.#run(
      () =>
        this.#logged({
          op: "reserveAutoIncrement",
          tableId,
          columnId,
          count,
          ...(atLeast === undefined ? {} : { atLeast }),
        }) as RowIdRange,
    );
  }

  async publishManifest(input: PublishManifestInput): Promise<Manifest> {
    // Resolve the optional timestamp before logging: replays must be deterministic.
    const resolved: PublishManifestInput = {
      ...input,
      createdAt: input.createdAt ?? new Date().toISOString(),
    };
    return this.#run(() => this.#logged({ op: "publishManifest", input: resolved }) as Manifest);
  }

  async beginTransaction(input: BeginTransactionInput): Promise<BeginTransactionResult> {
    validateBeginTransactionInput(input);
    return this.#run(
      () => this.#logged({ op: "beginTransaction", input }) as BeginTransactionResult,
    );
  }

  async createTransaction(record: TransactionRecord): Promise<void> {
    await this.#run(() => this.#logged({ op: "createTransaction", record }));
  }

  async updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    return this.#run(
      () =>
        this.#logged({
          op: "updateTransaction",
          id,
          expectedRevision,
          update,
        }) as TransactionRecord,
    );
  }

  async stageTransactionArtifacts(
    input: StageTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    return this.#run(async () => {
      const ids = new Set<string>();
      for (const block of input.blocks) {
        validateId(block.id);
        if (ids.has(block.id) || this.#blockIndex.has(block.id)) {
          throw new Error(`Block already exists: ${block.id}`);
        }
        ids.add(block.id);
      }
      const blocks: IdPlacement[] = [];
      try {
        for (const block of input.blocks) {
          blocks.push({
            id: block.id,
            placement: await this.#pool.append(block.bytes, this.#strict),
          });
        }
        return this.#logged({
          op: "stageTransactionArtifacts",
          transactionId: input.transactionId,
          expectedRevision: input.expectedRevision,
          blocks,
          segments: [...input.segments],
          updatedAt: input.updatedAt,
        }) as TransactionRecord;
      } catch (error) {
        this.#pool.release(blocks.map(({ placement }) => placement));
        throw error;
      }
    });
  }

  async commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary> {
    return this.#run(() => this.#logged({ op: "commitTransaction", input }) as ManifestSummary);
  }

  async createLease(record: LeaseRecord): Promise<void> {
    await this.#run(() => this.#logged({ op: "createLease", record }));
  }

  async renewLease(id: string, expectedRevision: number, expiresAt: string): Promise<LeaseRecord> {
    return this.#run(
      () => this.#logged({ op: "renewLease", id, expectedRevision, expiresAt }) as LeaseRecord,
    );
  }

  async removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    return this.#run(
      () =>
        this.#logged({
          op: "removeLeaseIfExpired",
          id,
          expectedRevision,
          expiresAtCutoff,
        }) as boolean,
    );
  }

  async removeLease(id: string): Promise<void> {
    await this.#run(() => this.#logged({ op: "removeLease", id }));
  }

  async createCompactionJob(record: CompactionJobRecord): Promise<void> {
    await this.#run(() => this.#logged({ op: "createCompactionJob", record }));
  }

  async updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord> {
    return this.#run(
      () =>
        this.#logged({
          op: "updateCompactionJob",
          id,
          expectedRevision,
          update,
        }) as CompactionJobRecord,
    );
  }

  async cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord> {
    return this.#run(
      () =>
        this.#logged({
          op: "cancelCompactionJob",
          id,
          expectedRevision,
          cancelledAt,
        }) as CompactionJobRecord,
    );
  }

  async removeCompactionJob(id: string): Promise<void> {
    await this.#run(() => this.#logged({ op: "removeCompactionJob", id }));
  }

  async createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord> {
    return this.#run(
      () => this.#logged({ op: "createGarbageCollectionJob", input }) as GarbageCollectionJobRecord,
    );
  }

  async runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    const result = await this.#run(() => {
      // Computed against provably-current state, logged as the resolved effect so replay
      // needs no knowledge of physical placement.
      const step = this.#core.runGarbageCollectionStep(input);
      this.#releaseReclaimedBlocks(step.reclaimedBlockIds);
      this.#appendFrame({
        op: "garbageCollectionStep",
        effect: {
          job: step.job,
          prunedManifestVersions: [...step.prunedManifestVersions],
          reclaimedSegmentIds: [...step.reclaimedSegmentIds],
          reclaimedBlockIds: [...step.reclaimedBlockIds],
          updatedAt: input.updatedAt,
        },
      });
      return step;
    });
    await this.#deleteDrainedExtents();
    return result;
  }

  async removeGarbageCollectionJob(id: string): Promise<void> {
    await this.#run(() => this.#logged({ op: "removeGarbageCollectionJob", id }));
  }

  // ---------------------------------------------------------------------------------------
  // Full-text bases: immutable chunks in extents plus a logged pointer.
  // ---------------------------------------------------------------------------------------

  async writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): Promise<void> {
    await this.#run(async () => {
      const chunks: Placement[] = [];
      try {
        for (const chunk of input.chunks) {
          chunks.push(await this.#pool.append(encodeFtsChunk(chunk), this.#strict));
        }
      } catch (error) {
        this.#pool.release(chunks);
        throw error;
      }
      const pointer: FtsBasePointer = {
        coversVersion: input.coversVersion,
        totalTokens: input.totalTokens,
        chunks,
        chunkBounds: input.chunks.map((chunk) => ({
          first: chunk[0]?.term ?? "",
          last: chunk[chunk.length - 1]?.term ?? "",
        })),
      };
      this.#logged({ op: "writeFtsBase", tableId, columnId, pointer });
    });
    await this.#deleteDrainedExtents();
  }

  async readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: ReadonlyArray<{ term: string; prefix: boolean }>,
    upToVersion: number,
  ): Promise<
    FtsCandidates & { deltaChunkCount: number; totalTokens: number; coversVersion: number }
  > {
    await this.#healthy();
    const key = `${tableId}/${columnId}`;
    const pointer = this.#ftsBases.get(key);
    const coversVersion = pointer?.coversVersion ?? -1;
    const deltas = this.#core.readFtsDeltas(tableId, columnId, coversVersion, upToVersion);
    const baseChunks =
      pointer === undefined
        ? []
        : await Promise.all(
            selectFtsChunks(pointer.chunkBounds, terms).map((ordinal) =>
              this.#loadFtsChunk(key, pointer, ordinal),
            ),
          );
    return {
      ...collectFtsCandidates([...baseChunks, ...deltas.chunkLists], terms),
      deltaChunkCount: deltas.deltaChunkCount,
      totalTokens: (pointer?.totalTokens ?? 0) + deltas.deltaTokens,
      coversVersion,
    };
  }

  async #loadFtsChunk(
    key: string,
    pointer: FtsBasePointer,
    ordinal: number,
  ): Promise<FtsPosting[]> {
    const placement = pointer.chunks[ordinal];
    if (placement === undefined) return [];
    const cacheKey = `${key}/${String(placement.extent)}/${String(placement.offset)}`;
    const cached = this.#ftsChunkCache.get(cacheKey);
    if (cached !== undefined) return cached;
    const chunk = decodeFtsChunk(await this.#pool.read(placement));
    this.#ftsChunkCache.set(cacheKey, chunk);
    if (this.#ftsChunkCache.size > FTS_CHUNK_CACHE_SIZE) {
      const [oldest] = this.#ftsChunkCache.keys();
      if (oldest !== undefined) this.#ftsChunkCache.delete(oldest);
    }
    return chunk;
  }

  #dropFtsChunkCache(keyPrefix: string): void {
    for (const key of [...this.#ftsChunkCache.keys()]) {
      if (key.startsWith(`${keyPrefix}/`)) this.#ftsChunkCache.delete(key);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Snapshots, sizing, lifecycle.
  // ---------------------------------------------------------------------------------------

  /**
   * Consistent by construction: the export runs as one queue operation, so no commit or
   * garbage collection interleaves with it. Full-text bases export as rebuilds.
   */
  async exportSnapshot(): Promise<DatabaseSnapshot> {
    return this.#run(async () => {
      const { records, blockIds } = this.#core.exportSnapshotRecords();
      const blocks: DatabaseSnapshot["blocks"] = [];
      for (const id of blockIds) {
        const placement = this.#blockIndex.get(id);
        if (placement === undefined) throw new Error(`Manifest references missing block: ${id}`);
        blocks.push({ id, bytes: await this.#pool.read(placement) });
      }
      return { ...records, blocks };
    });
  }

  async importSnapshot(
    snapshot: DatabaseSnapshot,
    options: { onProgress?: (progress: SnapshotLoadProgress) => void } = {},
  ): Promise<void> {
    const totalBytes = snapshot.blocks.reduce((total, block) => total + block.bytes.byteLength, 0);
    options.onProgress?.({ phase: "blocks", writtenBytes: 0, totalBytes });
    await this.#run(async () => {
      if (this.#core.getCurrentManifestVersion() !== null || this.#core.listTables().length > 0) {
        throw new Error("This store already holds a database");
      }
      let writtenBytes = 0;
      const blockPlacements: IdPlacement[] = [];
      try {
        for (const block of snapshot.blocks) {
          blockPlacements.push({
            id: block.id,
            placement: await this.#pool.append(block.bytes, this.#strict),
          });
          writtenBytes += block.bytes.byteLength;
          options.onProgress?.({ phase: "blocks", writtenBytes, totalBytes });
        }
      } catch (error) {
        this.#pool.release(blockPlacements.map(({ placement }) => placement));
        throw error;
      }
      this.#logged({
        op: "importSnapshot",
        // Full-text bases restore as rebuilds; the index is an accelerator, not truth.
        records: {
          version: snapshot.version,
          createdAt: snapshot.createdAt,
          tables: snapshot.tables.map((table) => ({
            ...structuredClone(table),
            fts: [],
            record: invalidateFtsColumns(structuredClone(table.record)),
          })),
          segments: structuredClone(snapshot.segments),
          transactions: structuredClone(snapshot.transactions),
        },
        blockPlacements,
      });
    });
    options.onProgress?.({ phase: "done", writtenBytes: totalBytes, totalBytes });
  }

  /** Live payload bytes plus the control files — what this database logically occupies. */
  async getLogicalStorageBytes(): Promise<number> {
    let total = this.#wal.byteLength;
    const slot = this.#slots[this.#newestSlot];
    if (slot !== undefined) total += slot.getSize();
    for (const [, bytes] of this.#pool.meta().liveBytes) total += bytes;
    for (const { size } of await this.#tree.walkFiles(["temp"])) total += size;
    return total;
  }

  /** Graceful: flush, checkpoint (so the next leader's takeover is instant), release. */
  async shutdown(): Promise<void> {
    await this.#run(() => {
      this.checkpointNow();
      this.#closed = true;
    }).catch(() => {
      // Shutting down a poisoned leader still releases its handles below.
      this.#closed = true;
    });
    this.#walHandle.close();
    for (const slot of this.#slots) slot.close();
    this.#extents?.close();
  }

  /** Test-only: what tab death does — locks release, nothing is flushed or checkpointed. */
  crash(): void {
    this.#closed = true;
    this.#walHandle.close();
    for (const slot of this.#slots) slot.close();
    this.#extents?.close();
  }
}

const EMPTY_BYTES = new Uint8Array(0);

function EMPTY_CORE_STATE(): RecordCoreState {
  return {
    currentVersion: null,
    catalogEpoch: 0,
    manifests: [],
    transactions: [],
    tables: [],
    segments: [],
    leases: [],
    compactionJobs: [],
    garbageCollectionJobs: [],
    nextRowIds: [],
    nextAutoIncrement: [],
    ftsBases: [],
    ftsDeltas: [],
    uniqueKeys: [],
    tempOwners: [],
  };
}

function tempPagePath(ownerId: string, runId: string, pageIndex: number): string[] {
  return ["temp", encodeSegment(ownerId), encodeSegment(runId), String(pageIndex)];
}

function placementsOf(entry: WalEntry): Placement[] {
  switch (entry.op) {
    case "addBlocks":
      return entry.placements.map(({ placement }) => placement);
    case "stageTransactionArtifacts":
      return entry.blocks.map(({ placement }) => placement);
    case "writeFtsBase":
      return entry.pointer.chunks;
    case "importSnapshot":
      return entry.blockPlacements.map(({ placement }) => placement);
    default:
      return [];
  }
}

function encodeFtsChunk(chunk: FtsPosting[]): Uint8Array {
  return encodeChunk(chunk);
}

function decodeFtsChunk(bytes: Uint8Array): FtsPosting[] {
  const chunk = decodeChunk(bytes) as FtsPosting[] | undefined;
  if (chunk === undefined) throw new Error("Full-text base chunk is unreadable");
  return chunk;
}

/** The ordinals of base chunks whose term range can contain any of the query's terms. */
function selectFtsChunks(
  chunkBounds: ReadonlyArray<{ first: string; last: string }>,
  terms: ReadonlyArray<{ term: string; prefix: boolean }>,
): number[] {
  const ordinals: number[] = [];
  for (const [ordinal, bounds] of chunkBounds.entries()) {
    const needed = terms.some(({ term, prefix }) => {
      if (bounds.last < term) return false;
      if (prefix) {
        return bounds.first <= `${term}￿` || bounds.first.startsWith(term);
      }
      return bounds.first <= term;
    });
    if (needed) ordinals.push(ordinal);
  }
  return ordinals;
}

function invalidateFtsColumns(record: TableRecord): TableRecord {
  if (record.ftsColumns === undefined) return record;
  const ftsColumns = Object.fromEntries(
    Object.entries(record.ftsColumns).map(([columnId, state]) => [
      columnId,
      { ...state, state: "invalid" as const },
    ]),
  );
  return { ...record, ftsColumns };
}
