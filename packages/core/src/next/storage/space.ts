/** Extent allocation and ownership over StoreSession. R8 state is a disk queue of immutable
 * before/after snapshots; the capped free-run arena never treats its unknown complement as
 * ownership. Every ownership removal/transition is serialized through one mutation barrier.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import { xxHash32 } from "../hash/xxhash32.js";
import {
  DATA_ALIGN,
  OwnershipLostError,
  StoreClosedError,
  StoreCorruptionError,
  type DataRange,
  type StoreSession,
} from "./contract.js";
import {
  MAX_BLOB_BYTES,
  SpaceFailedError,
  type WriteOptions,
  type BlobRef,
  type ByteBudget,
  type ExtentBuilder,
  type ExtentRef,
  type ExtentReader,
  type ExtentSpace,
  type ExtentWriter,
  type OpenExtentSpace,
  type OpenSpaceOptions,
  type PeekOptions,
  type PendingBlob,
  type PublishChange,
  type ReachableExtents,
  type ReadLease,
  type ReadOptions,
  type SpaceStats,
  type WriterState,
  type WrittenExtent,
} from "./extents.js";
import type { ExtentList } from "./extent-list.js";
import { BoundedFreeSpaceMap } from "./bounded-free-space.js";
import { PageCache } from "./page-cache.js";
import type { RootPin } from "./reclaim.js";
import { RootPins } from "./root-pins.js";
import { ExtentReservation, type SealPlan } from "./reservation.js";
import { MAX_SLAB } from "./slabs.js";
import { RootMemory, type RootFrame } from "./root-memory.js";
import { decodeRootSlot } from "./root-slot.js";
import { MetadataMutex } from "./metadata-mutex.js";
import { captureAudit, clearCapture, flattenCapture, type AuditCapture } from "./audit-capture.js";
import { MetadataJournal, MetadataWorkspace } from "./metadata-journal.js";
import {
  saveNativeList,
  saveExtentInput,
  reconstructState,
  reconstructNativeState,
  disposeSnapshot,
} from "./extent-snapshot.js";
import { RetirementTickets } from "./retirement-tickets.js";
import { pairPhase, type MetadataWork } from "./metadata-work.js";
import { sortExtents, mergeExtents, openPairs } from "./metadata-extents.js";
import { validatePublication, releaseWorklist, discardPrepared } from "./publication-validation.js";
const alignDown = (value: number) => value - (value % DATA_ALIGN);
export const BUILDER_BUFFER_BYTES = 256 * 1024;
class Mutex {
  #tail: Promise<void> = Promise.resolve();
  async run<T>(work: () => Promise<T>): Promise<T> {
    const previous = this.#tail;
    let done!: () => void;
    this.#tail = new Promise<void>((resolve) => {
      done = resolve;
    });
    await previous;
    try {
      return await work();
    } finally {
      done();
    }
  }
}
interface MutationGuard {
  readonly space: Space;
}
type ListLease = ReturnType<NonNullable<ReachableExtents["openList"]>>;
function listLease(
  root: Uint8Array | null,
  source: ReachableExtents,
  budget: ByteBudget,
  supplied?: ExtentList,
): ListLease | null {
  if (supplied !== undefined || root === null)
    return { list: supplied ?? [], release: () => undefined };
  return source.openList?.(root, budget) ?? null;
}
/** Format descriptors are immutable and admitted before return. Keep their credit in C,
 * instead of keeping the startup grant alive or decoding again during publication.
 */
function retainedListLease(
  root: Uint8Array | null,
  source: ReachableExtents,
  startup: ByteBudget,
  roots: RootMemory,
): ListLease | null {
  const budget = roots.leaseBudget(startup);
  let lease: ListLease | null = null;
  try {
    lease = listLease(root, source, budget);
    if (lease === null) {
      budget.close();
      return null;
    }
    budget.transfer();
    const owned = lease;
    let held = true;
    return {
      list: owned.list,
      release() {
        if (!held) return;
        held = false;
        try {
          owned.release();
        } finally {
          budget.close();
        }
      },
    };
  } catch (error) {
    try {
      lease?.release();
    } finally {
      budget.close();
    }
    throw error;
  }
}

function rootInput(
  root: Uint8Array | null,
  source: ReachableExtents,
  reader: ExtentReader,
  budget: ByteBudget,
  work?: TaskWork,
): AsyncIterable<ExtentRef> {
  return (async function* () {
    if (root === null) return;
    for await (const ref of source(root, reader, budget, work))
      yield "extent" in ref ? ref.extent : ref;
  })();
}
async function snapshotRoot(
  root: Uint8Array | null,
  source: ReachableExtents,
  reader: ExtentReader,
  budget: ByteBudget,
  dataLength: number,
  journal: MetadataJournal,
  cache: PageCache,
  work?: TaskWork,
): Promise<number> {
  const lease = listLease(root, source, budget);
  try {
    return lease === null
      ? await saveExtentInput(
          journal,
          rootInput(root, source, reader, budget, work),
          source.ordered === true,
          dataLength,
          work,
        )
      : await saveNativeList(journal, cache, lease.list, dataLength, work);
  } finally {
    lease?.release();
  }
}
export const openExtentSpace: OpenExtentSpace = async (session, options) => {
  options.signal?.throwIfAborted();
  const cache = new PageCache(session);
  cache.attach(options.registerCache(cache), true);
  let roots: RootMemory | null = null,
    currentFrame: RootFrame | null = null,
    previousFrame: RootFrame | null = null,
    nextSlot: 0 | 1 = 0,
    nextSequence = 1;
  let workspace: MetadataWorkspace | null = null,
    map: BoundedFreeSpaceMap | null = null,
    opened: Space | null = null;
  let initialCurrent: ListLease | null = null,
    initialPrevious: ListLease | null = null;
  try {
    roots = RootMemory.create(options.registerCache);
    const rootMemory = roots;
    if (session.readRootSlot === undefined)
      throw new TypeError("Extent space needs admitted native root slots");
    // S is charged but its slab buffers do not exist yet: one native root response at a time
    // borrows that allowance. Copy into C, drop the response, then request the other slot.
    const readSlot = async (slot: 0 | 1) => {
      const raw = await session.readRootSlot?.(slot),
        value = decodeRootSlot(raw ?? null, true);
      return value === null
        ? null
        : {
            sequence: value.sequence,
            frame: await rootMemory.copy(value.root, options.budget, options),
          };
    };
    const first = await readSlot(0);
    currentFrame = first?.frame ?? null;
    const second = await readSlot(1);
    if (first !== null && (second === null || first.sequence > second.sequence)) {
      previousFrame = second?.frame ?? null;
      nextSlot = 1;
      nextSequence = first.sequence + 1;
    } else if (second !== null) {
      currentFrame = second.frame;
      previousFrame = first !== null && first.sequence < second.sequence ? first.frame : null;
      if (first !== null && previousFrame === null) first.frame.release();
      nextSequence = second.sequence + 1;
    }
    cache.activateReadSlab();
    const root = currentFrame?.bytes ?? null,
      previousRoot = previousFrame?.bytes ?? null,
      dataLength = await session.dataLength();
    workspace = MetadataWorkspace.create(options.budget);
    // No cursor frames are live during the paged physical audit; its native index page uses W.
    if (options.verifyAtOpen === true) await session.audit?.(options.signal);
    const backing = session.metadata?.();
    if (backing === undefined || backing === null)
      throw new TypeError("Extent space needs fenced private metadata backing");
    await backing.reset();
    const journal = new MetadataJournal(backing, workspace);
    const reader = {
      read: (refs: readonly BlobRef[], read: ReadOptions) => cache.read(refs, read),
      peek: (ref: BlobRef, peek?: PeekOptions) => cache.peek(ref, peek),
    };
    initialCurrent = retainedListLease(root, options.reachable, options.budget, rootMemory);
    initialPrevious = retainedListLease(
      previousRoot,
      options.reachable,
      options.budget,
      rootMemory,
    );
    let current = 0,
      previous = 0;
    let initialLists: readonly [ListLease, ListLease] | null = null;
    let reconstruction: Awaited<ReturnType<typeof reconstructState>>;
    if (initialCurrent !== null && initialPrevious !== null) {
      initialLists = [initialCurrent, initialPrevious];
      reconstruction = await reconstructNativeState(
        journal,
        cache,
        initialCurrent.list,
        initialPrevious.list,
        dataLength,
        options.budget,
        options,
      );
    } else {
      initialCurrent?.release();
      initialCurrent = null;
      initialPrevious?.release();
      initialPrevious = null;
      current = await snapshotRoot(
        root,
        options.reachable,
        reader,
        options.budget,
        dataLength,
        journal,
        cache,
        options,
      );
      previous = await snapshotRoot(
        previousRoot,
        options.reachable,
        reader,
        options.budget,
        dataLength,
        journal,
        cache,
        options,
      );
      reconstruction = await reconstructState(
        journal,
        cache,
        current,
        previous,
        dataLength,
        options.budget,
        options,
      );
    }
    map = reconstruction.map;
    workspace.transfer(options.registerCache);
    map.transfer(options.registerCache);
    const space = new Space(
      session,
      options,
      cache,
      map,
      dataLength,
      workspace,
      journal,
      current,
      previous,
      roots,
      currentFrame,
      previousFrame,
      nextSlot,
      nextSequence,
      initialLists,
    );
    opened = space;
    initialCurrent = null;
    initialPrevious = null;
    // Semantic verification precedes every release/truncate. A structurally valid but incomplete
    // list must not destroy the very extent whose full walk proves it incomplete.
    if (options.verifyAtOpen === true && options.walk !== undefined) await space.audit();
    await space.initialize(reconstruction.previousOnlyBytes);
    const pending = pauseIfDue(options);
    if (pending !== undefined) await pending;
    return space;
  } catch (error) {
    if (opened !== null) await opened.close();
    else {
      try {
        initialCurrent?.release();
      } finally {
        initialPrevious?.release();
      }
      currentFrame?.release();
      previousFrame?.release();
      roots?.close();
    }
    map?.close();
    workspace?.close();
    cache.close();
    throw error;
  }
};

class Space implements ExtentSpace {
  readonly #session: StoreSession;
  readonly #reachable: ReachableExtents;
  readonly #walk: ReachableExtents | null;
  readonly #budget: ByteBudget;
  readonly #cache: PageCache;
  readonly #map: BoundedFreeSpaceMap;
  readonly #ledger: RootPins;
  readonly #metadata = new MetadataMutex();
  readonly #auditing = new Mutex();
  readonly #audits = new Set<Promise<void>>();
  readonly #tickets: RetirementTickets;
  #currentSnapshot: number;
  #previousSnapshot: number;
  #initialRetiredBytes = 0;
  #initialized = false;
  #reclaimWanted = false;
  #reclaimPending: Promise<void> | null = null;
  /** Writers not yet published or aborted. */
  readonly writers = new Set<Writer>();
  readonly #publishing = new Mutex();
  readonly #mutations = new Mutex();
  #activeMutation: MutationGuard | null = null;
  #mutationEpoch = 0;
  #activeWrites = 0;
  #closing = false;
  #closePromise: Promise<void> | null = null;
  readonly writeLock = new Mutex();
  writeSlab = new Uint8Array(MAX_SLAB);
  /** The builder whose bytes the write slab holds, from its aligned logical `start`. */
  staged: { builder: Builder; start: number; length: number } | null = null;
  readonly #background = new Set<Promise<void>>();
  #rootFrame: RootFrame | null;
  #previousFrame: RootFrame | null;
  #failed: Error | null = null;
  #closed = false;
  #releasingBytes = 0;
  /** The retired part of `#releasingBytes` (so waiting debt excludes it, review finding 5). */
  #releasingRetired = 0;
  #relocations = 0;
  /** Bytes the data space holds as far as this space knows (written or kept since open). */
  #fileLength: number;

  constructor(
    session: StoreSession,
    options: OpenSpaceOptions,
    cache: PageCache,
    map: BoundedFreeSpaceMap,
    dataLength: number,
    readonly workspace: MetadataWorkspace,
    readonly journal: MetadataJournal,
    currentSnapshot: number,
    previousSnapshot: number,
    readonly roots: RootMemory,
    currentFrame: RootFrame | null,
    previousFrame: RootFrame | null,
    private nextSlot: 0 | 1,
    private nextSequence: number,
    private initialLists: readonly [ListLease, ListLease] | null,
  ) {
    this.#session = session;
    this.#reachable = options.reachable;
    this.#walk = options.walk ?? null;
    this.#budget = options.budget;
    this.#cache = cache;
    this.#map = map;
    this.#rootFrame = currentFrame;
    this.#previousFrame = previousFrame;
    this.#fileLength = dataLength;
    this.#ledger = new RootPins(options.registerCache({ evictableBytes: 0, evict: () => 0 }), () =>
      this.#scheduleReclaim(),
    );
    this.#tickets = new RetirementTickets(journal);
    this.#currentSnapshot = currentSnapshot;
    this.#previousSnapshot = previousSnapshot;
  }

  async initialize(previousOnlyBytes: number): Promise<void> {
    this.#initialRetiredBytes = previousOnlyBytes;
    this.#initialized = true;
    if (this.initialLists === null) await this.#metadata.run(() => this.#prepareInitialTicket());
    await this.mutate((guard) => this.#releaseTail(guard));
  }

  async #prepareInitialTicket(): Promise<void> {
    await this.#tickets.prepare(
      0,
      this.#previousSnapshot,
      this.#currentSnapshot,
      this.#initialRetiredBytes,
    );
    this.#tickets.commit();
    this.#initialRetiredBytes = 0;
  }

  /** The two native lists remain immutable and rooted until this complete handoff. This runs
   * under the workspace lock, before publication's final barrier. It uses only prepaid S/W,
   * never a closed startup grant or a new admission. Generation zero cannot reclaim yet.
   */
  async #ensureInitialSnapshots(work?: TaskWork): Promise<void> {
    const lists = this.initialLists;
    if (lists === null) return;
    if (this.generation !== 0) throw new TypeError("Native initial lists outlived generation zero");
    let current = 0,
      previous = 0,
      failed = false;
    try {
      current = await saveNativeList(
        this.journal,
        this.#cache,
        lists[0].list,
        this.#fileLength,
        work,
      );
      previous = await saveNativeList(
        this.journal,
        this.#cache,
        lists[1].list,
        this.#fileLength,
        work,
      );
      if (this.#initialized) {
        await this.#tickets.prepare(0, previous, current, this.#initialRetiredBytes);
        work?.signal?.throwIfAborted();
        this.#tickets.commit();
        this.#initialRetiredBytes = 0;
      }
      this.#currentSnapshot = current;
      this.#previousSnapshot = previous;
      current = 0;
      previous = 0;
      this.initialLists = null;
      try {
        lists[0].release();
      } finally {
        lists[1].release();
      }
    } catch (error) {
      failed = true;
      if (this.#initialized) await this.#tickets.abort().catch(() => undefined);
      if (this.journal.failed) this.fail(error);
      throw error;
    } finally {
      await discardPrepared(this.journal, [current, previous], failed);
    }
  }

  get root(): Uint8Array | null {
    return this.#rootFrame?.bytes ?? null;
  }

  get previousRoot(): Uint8Array | null {
    return this.#previousFrame?.bytes ?? null;
  }

  get generation(): number {
    return this.#ledger.generation;
  }

  get failed(): Error | null {
    return this.#failed;
  }

  get map(): BoundedFreeSpaceMap {
    return this.#map;
  }

  get stats(): SpaceStats {
    let writerBytes = 0;
    for (const writer of this.writers) writerBytes += writer.bytes;
    const retired = this.#tickets.waitingBytes + this.#initialRetiredBytes - this.#releasingRetired;
    return {
      dataBytes: this.#map.end,
      freeBytes: this.#map.freeBytes,
      untrackedFreeBytes: this.#map.untrackedFreeBytes,
      writerBytes,
      waitingBytes: retired,
      releasingBytes: this.#releasingBytes,
      relocations: this.#relocations,
      retainedBytes: this.workspace.bytes + this.#map.bytes + this.roots.bytes + this.#ledger.bytes,
      pinCount: this.#ledger.pinCount,
      oldestPin: this.#ledger.oldestPin,
      cache: this.#cache.stats,
    };
  }

  // ── Reading ──────────────────────────────────────────────────────────────────────────────────

  async read(refs: readonly BlobRef[], options: ReadOptions): Promise<ReadLease> {
    this.cleanupAllowed();
    return this.#cache.read(refs, options);
  }

  peek(ref: BlobRef, options?: PeekOptions): Uint8Array | null {
    return this.#cache.peek(ref, options);
  }

  parsed(ref: BlobRef): object | null {
    return this.#cache.parsed(ref);
  }

  attach(ref: BlobRef, parsed: object, bytes: number, of: Uint8Array): boolean {
    return this.#cache.attachParsed(ref, parsed, bytes, of);
  }

  // ── Writing ──────────────────────────────────────────────────────────────────────────────────

  beginWrite(label: string, options: WriteOptions = {}): ExtentWriter {
    this.accepting();
    const writer = new Writer(this, label, options.budget ?? null);
    this.writers.add(writer);
    this.reserved();
    return writer;
  }

  /** Throws unless writes and publishes are allowed. */
  usable(): void {
    if (this.#closed) throw new StoreClosedError();
    if (this.#failed !== null) throw new SpaceFailedError(this.#failed);
  }

  accepting(): void {
    this.usable();
    this.cleanupAllowed();
  }
  cleanupAllowed(): void {
    if (this.#closing || this.#closed) throw new StoreClosedError();
  }
  /** Monotonic reservation adds ownership and only consumes tracked holes or contiguous EOF.
   * It may run during final-phase awaits: it cannot take bytes owned by a root or sealed writer.
   * Every removal/transition instead joins mutate(), including abort/free/discard/relocate/seal.
   */
  reserved(): void {
    this.#mutationEpoch++;
  }
  mutate<T>(work: (guard: MutationGuard) => Promise<T>): Promise<T> {
    return this.#mutations.run(async () => {
      const guard: MutationGuard = { space: this };
      this.#activeMutation = guard;
      this.#mutationEpoch++;
      try {
        return await work(guard);
      } finally {
        this.#mutationEpoch++;
        this.#activeMutation = null;
      }
    });
  }
  #guard(guard: MutationGuard): void {
    if (guard !== this.#activeMutation || guard.space !== this)
      throw new TypeError("Ownership transition outside its barrier");
  }

  /** A session call that writes: ownership loss fails the space. */
  /** `transfer`: the space hands `bytes` (a whole buffer it will never touch again) over. */
  async sessionWrite(offset: number, bytes: Uint8Array, transfer = false): Promise<void> {
    this.#mutationEpoch++;
    this.#activeWrites++;
    try {
      await this.#session.writeData(offset, bytes, transfer ? { transfer } : undefined);
      this.#fileLength = Math.max(this.#fileLength, offset + bytes.length);
    } catch (error) {
      if (error instanceof OwnershipLostError) this.fail(error);
      throw error;
    } finally {
      this.#activeWrites--;
      this.#mutationEpoch++;
    }
  }

  async sessionRead(offset: number, target: Uint8Array): Promise<void> {
    await this.#session.readDataInto(offset, target);
  }

  fail(error: unknown): void {
    this.#failed ??= error instanceof Error ? error : new Error(String(error));
  }

  relocated(): void {
    this.#relocations += 1;
  }

  /**
   * Writes the staged bytes. With `keepTail`, the slab keeps the partial last sector (the
   * builder is still appending); otherwise the slab is emptied and that sector goes to its builder.
   * A write error belongs to the staged builder (its next call rethrows it), not to the caller.
   */
  async flushStaged(keepTail: boolean): Promise<void> {
    const staged = this.staged;
    if (staged === null || staged.length === 0) {
      if (!keepTail) this.staged = null;
      return;
    }
    const { builder, start, length } = staged;
    const writable = keepTail ? alignDown(length) : length;
    try {
      let done = 0;
      for (const piece of builder.reservation.pieces(start, writable)) {
        await this.sessionWrite(piece.offset, this.writeSlab.subarray(done, done + piece.length));
        done += piece.length;
      }
    } catch (error) {
      builder.failure ??= error instanceof Error ? error : new Error(String(error));
      this.staged = null;
      return;
    }
    const tailStart = alignDown(length);
    if (keepTail) {
      this.writeSlab.copyWithin(0, writable, length);
      staged.start = start + writable;
      staged.length = length - writable;
    } else {
      builder.tail = this.writeSlab.slice(tailStart, length);
      this.staged = null;
    }
  }

  /**
   * Writes everything in a builder's own buffer: when it is full (its size is a multiple of
   * `DATA_ALIGN`, so every flush but the last ends on a sector boundary and nothing partial is
   * left) and at seal. To a session that `keepsWrites`, a full buffer that maps to one contiguous
   * range is transferred, never copied, and replaced; the builder never sees it again. A write
   * error belongs to the builder.
   */
  async flushOwn(builder: Builder): Promise<void> {
    const own = builder.own;
    if (own === null || own.length === 0) return;
    try {
      const pieces = builder.reservation.pieces(own.start, own.length);
      const [only] = pieces;
      if (
        this.#session.keepsWrites === true &&
        pieces.length === 1 &&
        only !== undefined &&
        own.length === own.buffer.length
      ) {
        // A full buffer that is one contiguous write, to a session that keeps what it is given:
        // hand it over whole (no copy there) and stage on in a fresh buffer of the same size; the
        // budget charge is the same. A session that writes at once copies into its file, so the
        // buffer is simply reused (a fresh one would cost an allocation for nothing).
        const full = own.buffer;
        own.buffer = new Uint8Array(full.length);
        await this.sessionWrite(only.offset, full, true);
      } else {
        let done = 0;
        for (const piece of pieces) {
          await this.sessionWrite(piece.offset, own.buffer.subarray(done, done + piece.length));
          done += piece.length;
        }
      }
    } catch (error) {
      builder.failure ??= error instanceof Error ? error : new Error(String(error));
      return;
    }
    own.start += own.length;
    own.length = 0;
  }

  /** Copies `parts` into the builder's own buffer, writing its aligned prefix as it fills. */
  async #stageOwn(
    builder: Builder,
    own: NonNullable<Builder["own"]>,
    parts: readonly Uint8Array[],
  ): Promise<void> {
    for (const part of parts) {
      let done = 0;
      while (done < part.length) {
        const take = Math.min(part.length - done, own.buffer.length - own.length);
        own.buffer.set(part.subarray(done, done + take), own.length);
        own.length += take;
        done += take;
        if (own.length === own.buffer.length) await this.flushOwn(builder);
        if (builder.failure !== null) throw builder.failure;
      }
      builder.written += part.length;
    }
  }

  /**
   * Copies `parts` into the builder's own buffer, or else into the shared slab after the
   * builder's bytes, writing as either fills.
   */
  async stage(builder: Builder, parts: readonly Uint8Array[]): Promise<void> {
    if (builder.own !== null) return this.#stageOwn(builder, builder.own, parts);
    if (this.staged !== null && this.staged.builder !== builder) await this.flushStaged(false);
    if (this.staged === null) {
      const tail = builder.tail ?? new Uint8Array(0);
      this.writeSlab.set(tail, 0);
      this.staged = { builder, start: builder.written - tail.length, length: tail.length };
      builder.tail = null;
    }
    for (const part of parts) {
      let done = 0;
      while (done < part.length) {
        const staged = this.staged as { length: number };
        const take = Math.min(part.length - done, this.writeSlab.length - staged.length);
        this.writeSlab.set(part.subarray(done, done + take), staged.length);
        staged.length += take;
        done += take;
        if (staged.length === this.writeSlab.length) await this.flushStaged(true);
        if (builder.failure !== null) throw builder.failure;
      }
      builder.written += part.length;
    }
  }

  // ── Freeing ──────────────────────────────────────────────────────────────────────────────────

  /**
   * Frees ranges no root, pin or writer can reach: drop their cached blobs, release them to the
   * platform, then make them allocatable.
   */
  free(ranges: Iterable<DataRange>): Promise<void> {
    return this.mutate((guard) => this.freeGuarded(ranges, guard));
  }
  async freeGuarded(
    ranges: Iterable<DataRange>,
    guard: MutationGuard,
    retired = false,
  ): Promise<void> {
    this.#guard(guard);
    let batch: DataRange[] = [];
    for (const range of ranges) {
      batch.push(range);
      if (batch.length === 256) {
        if (!(await this.#releaseBatch(batch, guard, retired))) return;
        batch = [];
      }
    }
    if (batch.length > 0) await this.#releaseBatch(batch, guard, retired);
  }
  async #releaseBatch(
    ranges: readonly DataRange[],
    guard: MutationGuard,
    retired: boolean,
  ): Promise<boolean> {
    this.#guard(guard);
    let bytes = 0;
    for (const range of ranges) {
      this.#cache.dropExtent(range.offset);
      bytes += range.length;
    }
    this.#releasingBytes += bytes;
    if (retired) this.#releasingRetired += bytes;
    try {
      await this.#session.releaseData(ranges);
    } catch (error) {
      this.fail(error);
      this.#releasingBytes -= bytes;
      if (retired) this.#releasingRetired -= bytes;
      return false;
    }
    this.#releasingBytes -= bytes;
    if (retired) this.#releasingRetired -= bytes;
    for (const range of ranges) this.#map.releaseKnownAllocated(range.offset, range.length);
    await this.#releaseTail(guard);
    return this.#failed === null;
  }

  async #releaseTail(guard: MutationGuard): Promise<void> {
    this.#guard(guard);
    const end = this.#map.end;
    if (end >= this.#fileLength || this.#closed) return;
    const length = this.#fileLength - end;
    this.#fileLength = end;
    try {
      await this.#session.releaseData([{ offset: end, length }]);
    } catch (error) {
      this.fail(error);
    }
  }

  /** Tracks a release in flight; a failure fails the space (its space stays allocated). */
  #background_(work: Promise<void>): void {
    const tracked = work.catch((error: unknown) => {
      this.fail(error);
    });
    this.#background.add(tracked);
    void tracked.finally(() => this.#background.delete(tracked));
  }

  /** Settles every release in flight (tests and close). */
  async settled(): Promise<void> {
    while (this.#background.size > 0) await Promise.all([...this.#background]);
  }

  // ── Publication ──────────────────────────────────────────────────────────────────────────────

  async publish(root: Uint8Array, change: PublishChange): Promise<number> {
    this.accepting();
    return this.#publishing.run(() =>
      this.#metadata.run(async () => {
        const budget = change.budget ?? this.#budget;
        // Codec/callback admission is preparation, outside the final barrier and slab scope.
        const lease = listLease(root, this.#reachable, budget, change.list);
        let frame: RootFrame | null = null;
        let raw = 0,
          claimed = 0,
          failed = false;
        try {
          frame = await this.roots.copy(root, budget, change);
          const ownedRoot = frame;
          if (lease === null)
            raw = await saveExtentInput(
              this.journal,
              rootInput(root, this.#reachable, this, budget, change),
              this.#reachable.ordered === true,
              this.#fileLength,
              change,
              undefined,
              true,
            );
          claimed = await saveExtentInput(
            this.journal,
            (function* () {
              for (const extent of change.retired) {
                if (!Number.isSafeInteger(extent.length) || extent.length <= 0)
                  throw new RangeError("publish: retirement is not an extent");
                yield {
                  offset: extent.offset,
                  length: Math.ceil(extent.length / DATA_ALIGN) * DATA_ALIGN,
                };
              }
            })(),
            false,
            Infinity,
            change,
            undefined,
            true,
          );
          await this.#ensureInitialSnapshots(change);
          return await this.#cache.withReadSlab((slab) =>
            this.mutate((guard) =>
              this.#publishNow(
                ownedRoot,
                change,
                lease?.list ?? null,
                raw,
                claimed,
                { slab },
                guard,
              ),
            ),
          );
        } catch (error) {
          failed = true;
          if (this.journal.failed) this.fail(error);
          throw error;
        } finally {
          if (frame !== null && frame !== this.#rootFrame) frame.release();
          lease?.release();
          await discardPrepared(this.journal, [raw, claimed], failed);
        }
      }),
    );
  }

  async #publishNow(
    root: RootFrame,
    change: PublishChange,
    list: ExtentList | null,
    raw: number,
    claimed: number,
    io: MetadataWork,
    guard: MutationGuard,
  ): Promise<number> {
    this.#guard(guard);
    this.usable();
    let marked = 0,
      next = 0,
      sortedClaims = 0,
      ownerRaw = 0,
      owners = 0,
      unused = 0,
      attempted = false,
      installed = false,
      failed = false;
    try {
      // Mark supplied writers before the first yielding validation step. Monotonic reservations
      // on OTHER writers are safe; these supplied writers must have no new builders or transitions.
      for (const candidate of change.writers) {
        if (!(candidate instanceof Writer) || candidate.space !== this)
          throw new TypeError("publish: a writer from another space");
        if (candidate.abortRequested || candidate.state !== "open")
          throw new TypeError(
            `publish: writer ${candidate.label} is ${candidate.state} or aborting`,
          );
        if (candidate.builders.size > 0 || candidate.busy > 0)
          throw new TypeError(`publish: writer ${candidate.label} has an unsealed builder`);
        candidate.state = "publishing";
        marked++;
      }
      next =
        list === null
          ? await pairPhase(this.journal, io, "merge", (slots) =>
              sortExtents(this.journal, raw, "exact", change, slots),
            )
          : await saveNativeList(this.journal, this.#cache, list, this.#fileLength, change, io);
      try {
        sortedClaims = await pairPhase(this.journal, io, "merge", (slots) =>
          sortExtents(this.journal, claimed, "refuse", change, slots),
        );
      } catch (error) {
        if (error instanceof StoreCorruptionError && !this.journal.failed)
          throw new RangeError(`publish: an extent is retired twice or invalid: ${error.message}`, {
            cause: error,
          });
        throw error;
      }
      ownerRaw = await saveExtentInput(
        this.journal,
        (function* () {
          for (const candidate of change.writers) {
            if (!(candidate instanceof Writer))
              throw new TypeError("publish writer changed during call");
            yield* candidate.extents;
          }
        })(),
        false,
        Infinity,
        change,
        io,
        true,
      );
      owners = await pairPhase(this.journal, io, "merge", (slots) =>
        sortExtents(this.journal, ownerRaw, "refuse", change, slots),
      );
      // Retirement authority is ONLY the two immutable root lists. No free-map complement,
      // allocator allocation test or mutable unpublished-writer observation authorizes it.
      const validated = await validatePublication(
        this.journal,
        this.#currentSnapshot,
        next,
        sortedClaims,
        owners,
        io,
        change,
      );
      unused = validated.unused;
      await this.#tickets.prepare(
        this.generation + 1,
        this.#currentSnapshot,
        next,
        validated.retiredBytes,
        sortedClaims,
      );
      change.signal?.throwIfAborted();
      // Frees/transitions remain blocked through native publish. Other reservations can only
      // consume tracked free bytes or EOF; they cannot take rooted or supplied sealed ownership.
      attempted = true;
      root.prepare(this.#session, this.nextSlot, this.nextSequence);
      await this.#session.publish(root.bytes);
      this.#tickets.commit();
      const oldPrevious = this.#previousSnapshot;
      this.#previousSnapshot = this.#currentSnapshot;
      this.#currentSnapshot = next;
      next = 0;
      this.#previousFrame?.release();
      this.#previousFrame = this.#rootFrame;
      this.#rootFrame = root;
      this.nextSlot = this.nextSlot === 0 ? 1 : 0;
      this.nextSequence++;
      const generation = this.#ledger.advance();
      for (const candidate of change.writers)
        if (candidate instanceof Writer) {
          candidate.state = "published";
          this.writers.delete(candidate);
        }
      installed = true;
      await disposeSnapshot(this.journal, oldPrevious);
      await releaseWorklist(this.journal, unused, async (ranges) => {
        await this.freeGuarded(ranges, guard);
        if (this.#failed !== null) throw new SpaceFailedError(this.#failed);
      });
      this.#scheduleReclaim();
      return generation;
    } catch (error) {
      failed = true;
      if (!attempted) await this.#tickets.abort().catch(() => undefined);
      if (attempted || this.journal.failed) this.fail(error);
      let at = 0;
      for (const candidate of change.writers) {
        if (at++ >= marked) break;
        if (candidate instanceof Writer && !installed)
          candidate.state = attempted ? "indeterminate" : "open";
      }
      throw error;
    } finally {
      await discardPrepared(this.journal, [next, sortedClaims, ownerRaw, owners, unused], failed);
    }
  }

  pin(label?: string, budget?: ByteBudget): RootPin {
    this.cleanupAllowed();
    return this.#ledger.pin(label, budget);
  }

  #scheduleReclaim(): void {
    // R8's generation-zero ticket cannot become eligible before the first publication.
    // Startup pin churn must not start pointless private I/O during the host-ready interval.
    if (this.generation === 0 || this.#tickets.waitingBytes === 0) return;
    if (this.#closing || this.#closed || this.#failed !== null) return;
    this.#reclaimWanted = true;
    if (this.#reclaimPending !== null) return;
    const run = (async () => {
      while (this.#reclaimWanted && !this.#closing && !this.#closed && this.#failed === null) {
        this.#reclaimWanted = false;
        await this.#metadata.run(() =>
          this.#cache.withReadSlab((slab) =>
            this.mutate((guard) =>
              this.#tickets.reclaim(
                this.generation,
                this.#ledger.oldestPinned,
                async (ranges) => {
                  await this.freeGuarded(ranges, guard, true);
                  if (this.#failed !== null) throw new SpaceFailedError(this.#failed);
                },
                undefined,
                { slab },
              ),
            ),
          ),
        );
      }
    })();
    this.#reclaimPending = run;
    this.#background_(run);
    void run
      .catch(() => undefined)
      .finally(() => {
        this.#reclaimPending = null;
        if (this.#reclaimWanted) this.#scheduleReclaim();
      });
  }

  // ── Audit ────────────────────────────────────────────────────────────────────────────────────

  async audit(): Promise<void> {
    this.cleanupAllowed();
    const run = this.#auditing.run(() => this.#auditLoop());
    this.#audits.add(run);
    void run.then(
      () => this.#audits.delete(run),
      () => this.#audits.delete(run),
    );
    return run;
  }
  async #auditLoop(): Promise<void> {
    for (;;) {
      const turn = pauseIfDue();
      if (turn !== undefined) await turn;
      if (this.#activeWrites > 0) {
        await this.writeLock.run(async () => undefined);
        continue;
      }
      if (this.#activeMutation !== null) {
        await this.#mutations.run(async () => undefined);
        continue;
      }
      const epoch = this.#mutationEpoch;
      try {
        await this.#auditNow(epoch);
      } catch (error) {
        if (!this.#sameEpoch(epoch)) continue;
        throw error;
      }
      if (this.#sameEpoch(epoch)) return;
      const pause = pauseIfDue();
      if (pause !== undefined) await pause;
    }
  }

  #sameEpoch(epoch: number): boolean {
    return (
      epoch === this.#mutationEpoch && this.#activeMutation === null && this.#activeWrites === 0
    );
  }

  async #auditNow(epoch: number): Promise<void> {
    await this.settled();
    // Physical audit gets W alone, before any frame phase. It never awaits a caller callback.
    await this.#metadata.run(async () => {
      await this.#session.audit?.();
      await this.#ensureInitialSnapshots();
    });
    const captured = await this.#metadata.run(() =>
      this.journal.workspace.run("spool", async () => {
        const current = this.#currentSnapshot,
          previous = this.#previousSnapshot;
        await this.journal.retain(current);
        try {
          await this.journal.retain(previous);
        } catch (error) {
          await this.journal.release(current).catch(() => undefined);
          throw error;
        }
        return {
          current,
          previous,
          root: this.#rootFrame?.retain() ?? null,
          previousRoot: this.#previousFrame?.retain() ?? null,
        };
      }),
    );
    let currentInput: AuditCapture | null = null,
      previousInput: AuditCapture | null = null;
    let currentWalkRaw = 0,
      previousWalkRaw = 0,
      currentWalk = 0,
      previousWalk = 0,
      writerRaw = 0,
      writers = 0,
      union = 0,
      failed = false;
    try {
      const source = this.#walk ?? this.#reachable;
      currentInput = await captureAudit(
        this.journal,
        this.#metadata,
        rootInput(captured.root?.bytes ?? null, source, this, this.#budget),
      );
      previousInput = await captureAudit(
        this.journal,
        this.#metadata,
        rootInput(captured.previousRoot?.bytes ?? null, source, this, this.#budget),
      );
      await this.#metadata.run(async () => {
        if (!this.#sameEpoch(epoch)) return;
        const liveWriters = this.writers;
        writerRaw = await saveExtentInput(
          this.journal,
          (function* () {
            for (const writer of liveWriters) {
              yield* writer.extents;
              for (const builder of writer.builders) yield* builder.ownedRanges();
            }
          })(),
          false,
          Infinity,
          undefined,
          undefined,
          true,
        );
        await this.#cache.withReadSlab(async (slab) => {
          const io = { slab };
          const sorting = (handle: number, duplicates: "exact" | "refuse") =>
            pairPhase(this.journal, io, "merge", (slots) =>
              sortExtents(this.journal, handle, duplicates, undefined, slots),
            );
          if (currentInput === null || previousInput === null)
            throw new Error("Audit capture missing");
          currentWalkRaw = await flattenCapture(this.journal, currentInput, io);
          previousWalkRaw = await flattenCapture(this.journal, previousInput, io);
          {
            currentWalk = await sorting(currentWalkRaw, "exact");
            previousWalk = await sorting(previousWalkRaw, "exact");
            await this.#sameSnapshot(captured.current, currentWalk, "current", io);
            await this.#sameSnapshot(captured.previous, previousWalk, "previous", io);
          }
          await this.#rootAllocated(captured.current, "current", io);
          await this.#rootAllocated(captured.previous, "previous", io);
          await this.journal.workspace.run("spool", () => this.journal.retain(captured.current));
          union = captured.current;
          const addRoot = async (handle: number) => {
            const next = await pairPhase(this.journal, io, "merge", (slots) =>
              mergeExtents(this.journal, union, handle, "union", undefined, "exact", slots),
            );
            await disposeSnapshot(this.journal, union);
            union = next;
          };
          await addRoot(captured.previous);
          await this.#tickets.forEachRoot(addRoot);
          writers = await sorting(writerRaw, "refuse");
          const accounted = await pairPhase(this.journal, io, "merge", (slots) =>
            mergeExtents(this.journal, union, writers, "union", undefined, "refuse", slots),
          );
          await disposeSnapshot(this.journal, union);
          union = accounted;
          await pairPhase(this.journal, io, "merge", async (slots) => {
            const pairs = await openPairs(this.journal, union, slots.left, true);
            let cursor = 0,
              run = 0,
              gapBytes = 0,
              tracked = 0,
              steps = 0;
            const gap = (first: number, last: number) => {
              gapBytes += last - first;
              while (run < this.#map.runCount && this.#map.runOffset(run) < last) {
                const offset = this.#map.runOffset(run),
                  length = this.#map.runLength(run++);
                if (offset < first || offset + length > last)
                  throw new Error("Extent audit: tracked free run overlaps owned space");
                tracked += length;
              }
            };
            try {
              while (await pairs.next()) {
                if (steps++ % 128 === 0) {
                  const pending = pauseIfDue();
                  if (pending !== undefined) await pending;
                }
                if (pairs.offset < cursor)
                  throw new Error("Extent audit: space is accounted twice");
                if (pairs.offset > cursor) gap(cursor, pairs.offset);
                cursor = pairs.offset + this.#map.sizeOf(pairs.length);
              }
              if (cursor > this.#map.end)
                throw new Error("Extent audit: owned space is past allocator end");
              gap(cursor, this.#map.end);
              if (run !== this.#map.runCount || gapBytes - tracked !== this.#map.untrackedFreeBytes)
                throw new Error(
                  "Extent audit: allocated but unaccounted space (a leak or incorrect untracked-free debt)",
                );
            } finally {
              await pairs.close();
            }
          });
        });
      });
    } catch (error) {
      failed = true;
      throw error;
    } finally {
      captured.root?.release();
      captured.previousRoot?.release();
      await this.#metadata.run(async () => {
        if (currentInput !== null) await clearCapture(this.journal, currentInput);
        if (previousInput !== null) await clearCapture(this.journal, previousInput);
        await discardPrepared(
          this.journal,
          [
            captured.current,
            captured.previous,
            currentWalkRaw,
            previousWalkRaw,
            currentWalk,
            previousWalk,
            writerRaw,
            writers,
            union,
          ],
          failed,
        );
      });
    }
  }

  #sameSnapshot(listed: number, walked: number, label: string, io: MetadataWork): Promise<void> {
    return pairPhase(this.journal, io, "merge", async (slots) => {
      const a = await openPairs(this.journal, listed, slots.left, true);
      let b: Awaited<ReturnType<typeof openPairs>> | null = null;
      try {
        b = await openPairs(this.journal, walked, slots.right, true);
        let hasA = await a.next(),
          hasB = await b.next(),
          steps = 0;
        while (hasA || hasB) {
          if (steps++ % 128 === 0) {
            const pending = pauseIfDue();
            if (pending !== undefined) await pending;
          }
          if (!hasA || (hasB && b.offset < a.offset))
            throw new StoreCorruptionError(
              `The ${label} root's extent list misses the extent at ${String(b.offset)} that it reaches`,
            );
          if (!hasB || a.offset < b.offset)
            throw new StoreCorruptionError(
              `The ${label} root's extent list names the extent at ${String(a.offset)}, which it does not reach`,
            );
          if (a.length !== b.length)
            throw new StoreCorruptionError(
              `The ${label} root's extent list and walk differ in length at ${String(a.offset)}`,
            );
          hasA = await a.next();
          hasB = await b.next();
        }
      } finally {
        try {
          await b?.close();
        } finally {
          await a.close();
        }
      }
    });
  }

  #rootAllocated(handle: number, label: string, io: MetadataWork): Promise<void> {
    return pairPhase(this.journal, io, "merge", async (slots) => {
      const pairs = await openPairs(this.journal, handle, slots.left, true);
      try {
        let steps = 0;
        while (await pairs.next()) {
          if (steps++ % 128 === 0) {
            const pending = pauseIfDue();
            if (pending !== undefined) await pending;
          }
          const state = this.#map.allocationState(pairs.offset, pairs.length);
          if (state === "free" || state === "outside")
            throw new Error(
              `Extent audit: the ${label} root's extent at ${String(pairs.offset)} is free`,
            );
        }
      } finally {
        await pairs.close();
      }
    });
  }

  close(): Promise<void> {
    if (this.#closePromise !== null) return this.#closePromise;
    this.#closing = true;
    return (this.#closePromise = this.#closeNow());
  }
  async #closeNow(): Promise<void> {
    await this.#publishing.run(async () => undefined);
    // Await busy operations outside the barrier: a queued seal/abort needs it to finish.
    for (const writer of this.writers) {
      try {
        await writer.settled();
      } catch (error) {
        this.fail(error);
      }
    }
    await this.writeLock.run(async () => undefined);
    await this.settled();
    await this.#mutations.run(async () => undefined);
    for (const audit of this.#audits) await audit.catch(() => undefined);
    await this.#metadata.run(async () => undefined);
    await this.#cache.settled();
    this.#closed = true;
    this.staged = null;
    this.writeSlab = new Uint8Array(0);
    this.#rootFrame?.release();
    this.#previousFrame?.release();
    this.#rootFrame = null;
    this.#previousFrame = null;
    const initial = this.initialLists;
    this.initialLists = null;
    try {
      initial?.[0].release();
    } finally {
      initial?.[1].release();
    }
    this.roots.close();
    this.#ledger.close();
    this.#cache.close();
    this.#map.close();
    this.workspace.close();
  }
}

class Writer implements ExtentWriter {
  readonly space: Space;
  readonly label: string;
  state: WriterState = "open";
  readonly builders = new Set<Builder>();
  readonly #extents: ExtentRef[] = [];
  /** Calls in flight (appends, whole seals): abort waits for them. */
  busy = 0;
  readonly #idle: Array<() => void> = [];
  #abortPromise: Promise<void> | null = null;
  get abortRequested(): boolean {
    return this.#abortPromise !== null;
  }
  async settled(): Promise<void> {
    while (this.busy > 0) await new Promise<void>((resolve) => this.#idle.push(resolve));
    await this.#abortPromise;
  }

  /** Charged for each builder's own staging buffer; null: builders share the space's slab. */
  readonly budget: ByteBudget | null;

  constructor(space: Space, label: string, budget: ByteBudget | null) {
    this.space = space;
    this.label = label;
    this.budget = budget;
  }

  get extents(): readonly ExtentRef[] {
    return this.#extents;
  }

  get bytes(): number {
    let bytes = 0;
    for (const builder of this.builders) bytes += builder.capacity;
    for (const extent of this.#extents) bytes += this.space.map.sizeOf(extent.length);
    return bytes;
  }

  usable(): void {
    if (this.state !== "open") throw new TypeError(`Writer ${this.label} is ${this.state}`);
    if (this.abortRequested) throw new TypeError(`Writer ${this.label} is aborting`);
    this.space.accepting();
  }

  async track<T>(work: () => Promise<T>): Promise<T> {
    this.busy += 1;
    try {
      return await work();
    } finally {
      this.busy -= 1;
      if (this.busy === 0) for (const resolve of this.#idle.splice(0)) resolve();
    }
  }

  open(capacity: number): ExtentBuilder {
    this.usable();
    if (!(Number.isSafeInteger(capacity) && capacity > 0))
      throw new RangeError("A builder needs a positive capacity");
    const builder = new Builder(this, new ExtentReservation(this.space.map, capacity));
    // Its own staging buffer when the budget grants one: no larger than the builder needs.
    const size = Math.min(BUILDER_BUFFER_BYTES, this.space.map.sizeOf(capacity));
    if (this.budget?.allocate(size) === true)
      builder.own = { buffer: new Uint8Array(size), start: 0, length: 0 };
    this.builders.add(builder);
    this.space.reserved();
    return builder;
  }

  sealed(builder: Builder, extent: ExtentRef | null): void {
    this.builders.delete(builder);
    if (extent !== null) this.#extents.push(extent);
  }

  async write(parts: readonly Uint8Array[]): Promise<WrittenExtent> {
    const total = parts.reduce((sum, part) => sum + part.length, 0);
    if (parts.length === 0 || total === 0) throw new RangeError("write needs at least one byte");
    const builder = this.open(total);
    await builder.append(parts);
    const written = await builder.seal();
    if (written === null) throw new Error("A non-empty builder sealed empty");
    return written;
  }

  async discard(extent: ExtentRef): Promise<void> {
    this.usable();
    await this.space.mutate(async (guard) => {
      // Accepted before close; recheck ownership, without treating close's drain as new work.
      if (this.state !== "open") throw new TypeError(`Writer ${this.label} cannot discard now`);
      this.space.usable();
      const index = this.#extents.findIndex(
        (candidate) => candidate.offset === extent.offset && candidate.length === extent.length,
      );
      if (index < 0) throw new RangeError(`Writer ${this.label} did not seal that extent`);
      this.#extents.splice(index, 1);
      await this.space.freeGuarded(
        [{ offset: extent.offset, length: this.space.map.sizeOf(extent.length) }],
        guard,
      );
    });
  }
  async abort(): Promise<void> {
    if (this.state === "aborted") return Promise.resolve();
    if (this.#abortPromise !== null) return this.#abortPromise;
    if (this.state !== "open")
      return Promise.reject(new TypeError(`Writer ${this.label} is ${this.state}`));
    this.space.cleanupAllowed();
    // Do not remove ownership or mark aborted until the queued transition owns the barrier.
    return (this.#abortPromise = this.#abortNow());
  }
  *#abortRanges(): IterableIterator<DataRange> {
    for (const builder of this.builders) {
      if (this.space.staged?.builder === builder) this.space.staged = null;
      yield* builder.abandon();
    }
    for (const extent of this.#extents)
      yield { offset: extent.offset, length: this.space.map.sizeOf(extent.length) };
  }
  async #abortNow(): Promise<void> {
    while (this.busy > 0) await new Promise<void>((resolve) => this.#idle.push(resolve));
    await this.space.writeLock.run(() =>
      this.space.mutate(async (guard) => {
        if (this.state !== "open") throw new TypeError(`Writer ${this.label} is ${this.state}`);
        this.state = "aborted";
        await this.space.freeGuarded(this.#abortRanges(), guard);
        this.builders.clear();
        this.#extents.length = 0;
        this.space.writers.delete(this);
      }),
    );
  }
}

class Builder implements ExtentBuilder {
  readonly writer: Writer;
  readonly reservation: ExtentReservation;
  /** Bytes appended (logical length). */
  #length = 0;
  /** Bytes handed to the write slab or written (trails `#length` only inside `append`). */
  written = 0;
  /** The partial last sector, kept while another builder holds the slab. */
  tail: Uint8Array | null = null;
  /** Its own staging buffer (charged to the writer's budget), from aligned logical `start`. */
  own: { buffer: Uint8Array; start: number; length: number } | null = null;
  failure: Error | null = null;
  readonly #pending: PendingBlob[] = [];
  #state: "open" | "sealed" = "open";
  /** Space a failed seal left behind (the relocation target and the old segments). */
  #leftover: DataRange[] | null = null;
  #appending = false;

  constructor(writer: Writer, reservation: ExtentReservation) {
    this.writer = writer;
    this.reservation = reservation;
  }

  get capacity(): number {
    return this.reservation.capacity;
  }

  get length(): number {
    return this.#length;
  }

  #usable(): void {
    if (this.#state !== "open") throw new TypeError("The builder is sealed");
    this.writer.usable();
    if (this.failure !== null) throw this.failure;
  }

  /** Returns the own staging buffer to the writer's budget. */
  releaseBuffer(): void {
    if (this.own === null) return;
    this.writer.budget?.free(this.own.buffer.length);
    this.own = null;
  }

  *ownedRanges(): IterableIterator<DataRange> {
    if (this.#leftover !== null) yield* this.#leftover;
    else yield* this.reservation.ownedRanges();
  }

  /** Everything this builder holds, for its writer's abort. */
  abandon(): Iterable<DataRange> {
    this.releaseBuffer();
    if (this.#leftover !== null) return this.#leftover;
    this.tail = null;
    return this.reservation.abortRanges();
  }

  extend(bytes: number): boolean {
    this.#usable();
    if (!(Number.isSafeInteger(bytes) && bytes >= 0)) throw new RangeError("Bad extend size");
    const result = this.reservation.extend(bytes);
    this.writer.space.reserved();
    return result;
  }

  async append(parts: readonly Uint8Array[]): Promise<PendingBlob[]> {
    this.#usable();
    if (this.#appending) throw new TypeError("Appends to one builder must not overlap");
    let total = 0;
    for (const part of parts) {
      if (part.length > MAX_BLOB_BYTES)
        throw new RangeError(`A blob of ${String(part.length)} bytes exceeds MAX_BLOB_BYTES`);
      total += part.length;
    }
    if (this.#length + total > this.capacity)
      throw new RangeError("The parts do not fit the builder's capacity: extend first");
    const blobs = parts.map((part) => {
      const blob = { offset: this.#length, length: part.length, checksum: xxHash32(part) };
      this.#length += part.length;
      return blob;
    });
    this.#appending = true;
    const space = this.writer.space;
    try {
      await this.writer.track(() => space.writeLock.run(() => space.stage(this, parts)));
    } catch (error) {
      this.failure ??= error instanceof Error ? error : new Error(String(error));
      throw error;
    } finally {
      this.#appending = false;
    }
    this.#pending.push(...blobs);
    return blobs;
  }

  /**
   * The whole seal is one tracked call (abort waits for it), and ownership moves in single
   * synchronous steps: the reservation is the builder's until the plan is made; then the plan's
   * ranges (`#leftover`) until the bytes are in place; then the extent is the writer's and the
   * trimmed or vacated ranges are this call's to free. A failed relocation leaves `#leftover` to
   * the writer's abort.
   */
  async seal(): Promise<WrittenExtent | null> {
    this.#usable();
    if (this.#appending) throw new TypeError("seal during an append");
    this.#state = "sealed";
    const space = this.writer.space;
    return this.writer.track(() =>
      space.writeLock.run(() =>
        space.mutate(async (guard) => {
          if (this.#length === 0) {
            if (space.staged?.builder === this) space.staged = null;
            this.releaseBuffer();
            const ranges = this.reservation.abort();
            this.writer.sealed(this, null);
            await space.freeGuarded(ranges, guard);
            return null;
          }
          let plan: SealPlan;
          try {
            if (space.staged?.builder === this) await space.flushStaged(false);
            await space.flushOwn(this);
            if (this.failure !== null) throw this.failure;
            this.releaseBuffer();
            const sealed = this.reservation.seal(this.#length);
            this.#leftover = [
              { offset: sealed.extent.offset, length: space.map.sizeOf(sealed.extent.length) },
              ...sealed.release,
            ];
            if (sealed.moves.length > 0) {
              // Relocation: the slab carries every segment's bytes to the contiguous extent.
              if (space.staged !== null) await space.flushStaged(false);
              for (const move of sealed.moves)
                for (let done = 0; done < move.length; done += MAX_SLAB) {
                  const chunk = space.writeSlab.subarray(0, Math.min(MAX_SLAB, move.length - done));
                  await space.sessionRead(move.from + done, chunk);
                  await space.sessionWrite(move.to + done, chunk);
                }
              space.relocated();
            }
            this.#leftover = null;
            this.tail = null;
            this.writer.sealed(this, sealed.extent);
            plan = sealed;
          } catch (error) {
            this.failure ??= error instanceof Error ? error : new Error(String(error));
            throw error;
          }
          await space.freeGuarded(plan.release, guard);
          if (this.writer.abortRequested)
            throw new TypeError(`Writer ${this.writer.label} was aborted during the seal`);
          const extent: ExtentRef = plan.extent;
          return { extent, blobs: this.#pending.map((blob) => ({ extent, ...blob })) };
        }),
      ),
    );
  }
}
