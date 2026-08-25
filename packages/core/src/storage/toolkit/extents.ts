import { readFully, writeFully, type SyncFileHandle } from "./sync-file.js";
import { crc32 } from "../../block-format/index.js";

/**
 * Packed, append-only extent files for bulk bytes — blocks and full-text base chunks.
 *
 * The leader appends into one tail extent through a handle it keeps open (microseconds per
 * write) and seals it at a size threshold; sealed extents are immutable. Every payload's home
 * is a placement — extent id, offset, length, and whole-payload checksum — recorded by the WAL
 * entry that publishes it. Recovery verifies each new placement before applying its entry,
 * re-derives the tail position from the consistent prefix, and overwrites any unpublished bytes
 * beyond it on the next append.
 *
 * Extents assume a single writer (the OPFS adapter's leader), so holding handles — the tail's
 * permanently, sealed ones' in a small read cache — never contends with anyone.
 */

/**
 * How the pool opens and deletes its extent files. The OPFS adapter's `OpfsTree` satisfies it
 * as-is; any substrate that can hand out `SyncFileHandle`s by path can host extents.
 */
export interface ExtentFiles {
  openHandle(path: readonly string[], options: { create: boolean }): Promise<SyncFileHandle>;
  deleteFile(path: readonly string[]): Promise<boolean>;
}

export interface ExtentPoolOptions {
  /** @internal Deterministic checksum instrumentation for performance/correctness tests. */
  _checksumForTests?: (bytes: Uint8Array) => number;
}

export interface Placement {
  extent: number;
  offset: number;
  length: number;
  /** CRC32 of every stored payload byte, independent of the payload's own format. */
  checksum: number;
}

/** Persisted in checkpoints; placements in replayed WAL entries refine `tailOffset`. */
export interface ExtentMeta {
  nextExtentId: number;
  tailExtentId: number;
  tailOffset: number;
  /** Live payload bytes per extent id; an extent at zero that is not the tail is deletable. */
  liveBytes: Array<readonly [number, number]>;
}

/** Opaque O(1) rollback point for bytes not yet published by a WAL frame. */
export class ExtentBatchMark {
  /** @internal Original counters are captured lazily only for extents the batch touches. */
  readonly _liveBytesBefore = new Map<number, number | undefined>();

  constructor(
    readonly tailExtentId: number,
    readonly nextExtentId: number,
    readonly tailOffset: number,
  ) {}
}

interface CachedReadHandle {
  handle: SyncFileHandle;
  /** Includes callers waiting to resume after a shared open, so eviction cannot race them. */
  readers: number;
  drained: Array<() => void>;
}

interface OpeningReadHandle {
  promise: Promise<SyncFileHandle>;
  /** One reservation per caller sharing this open. Transferred to the cache on success. */
  reservations: number;
}

const SEAL_BYTES = 8 * 1024 * 1024;
const READ_HANDLE_CACHE = 12;

export class ExtentPool {
  readonly #tree: ExtentFiles;
  readonly #checksum: (bytes: Uint8Array) => number;
  #tailId: number;
  #nextId: number;
  #tailOffset: number;
  #tailHandle: SyncFileHandle;
  #tailDirty = false;
  readonly #liveBytes = new Map<number, number>();
  /** Sealed-extent read handles, LRU by insertion order. The tail is never in here. */
  readonly #readHandles = new Map<number, CachedReadHandle>();
  readonly #openingReadHandles = new Map<number, OpeningReadHandle>();
  readonly #deletingExtents = new Set<number>();
  #activeBatch: ExtentBatchMark | undefined;
  #closed = false;

  private constructor(
    tree: ExtentFiles,
    meta: { tailId: number; nextId: number; tailOffset: number },
    tailHandle: SyncFileHandle,
    checksum: (bytes: Uint8Array) => number,
  ) {
    this.#tree = tree;
    this.#checksum = checksum;
    this.#tailId = meta.tailId;
    this.#nextId = meta.nextId;
    this.#tailOffset = meta.tailOffset;
    this.#tailHandle = tailHandle;
  }

  /** Opens (or creates) the tail extent. `meta` comes from the checkpoint, if any. */
  static async open(
    tree: ExtentFiles,
    meta: ExtentMeta | undefined,
    options: ExtentPoolOptions = {},
  ): Promise<ExtentPool> {
    if (meta !== undefined) assertValidExtentMeta(meta);
    const tailId = meta?.tailExtentId ?? 0;
    const nextId = meta?.nextExtentId ?? 1;
    const tailHandle = await tree.openHandle(extentPath(tailId), { create: true });
    const pool = new ExtentPool(
      tree,
      { tailId, nextId, tailOffset: meta?.tailOffset ?? 0 },
      tailHandle,
      options._checksumForTests ?? crc32,
    );
    for (const [id, bytes] of meta?.liveBytes ?? []) pool.#liveBytes.set(id, bytes);
    if (tailHandle.getSize() < pool.#tailOffset) {
      pool.close();
      throw new Error(
        `OPFS tail extent ${String(tailId)} is shorter than its checkpointed offset: ` +
          `${String(tailHandle.getSize())} < ${String(pool.#tailOffset)}`,
      );
    }
    return pool;
  }

  /** Captures the exact append/accounting state before an unpublished multi-payload batch. */
  markBatch(): ExtentBatchMark {
    if (this.#activeBatch !== undefined)
      throw new Error("An extent append batch is already active");
    const mark = new ExtentBatchMark(this.#tailId, this.#nextId, this.#tailOffset);
    this.#activeBatch = mark;
    return mark;
  }

  /** Publishes a successful batch; constant-time and allocation-free. */
  commitBatch(mark: ExtentBatchMark): void {
    if (this.#activeBatch !== mark) throw new Error("Extent batch mark is not active");
    this.#activeBatch = undefined;
  }

  /**
   * Rolls back every append and accounting mutation since `mark`. This is deliberately an
   * async slow path; successful appends retain the allocation-free hot path.
   */
  async rollbackBatch(mark: ExtentBatchMark): Promise<void> {
    if (this.#activeBatch !== mark) throw new Error("Extent batch mark is not active");
    if (mark.nextExtentId > this.#nextId) {
      throw new Error("Cannot roll an extent pool forward to a future batch mark");
    }

    this.#tailHandle.close();

    // Every id allocated after the mark is unpublished by definition. Delete it before
    // restoring the former tail so repeated refused batches cannot accumulate sealed files.
    for (let id = mark.nextExtentId; id < this.#nextId; id += 1) {
      await this.#closeCachedReadHandle(id);
      await this.#tree.deleteFile(extentPath(id));
    }
    await this.#closeCachedReadHandle(mark.tailExtentId);
    this.#tailHandle = await this.#tree.openHandle(extentPath(mark.tailExtentId), { create: true });
    this.#tailHandle.truncate(mark.tailOffset);
    this.#tailId = mark.tailExtentId;
    this.#nextId = mark.nextExtentId;
    this.#tailOffset = mark.tailOffset;
    this.#tailDirty = false;
    for (const [id, bytes] of mark._liveBytesBefore) {
      if (bytes === undefined) this.#liveBytes.delete(id);
      else this.#liveBytes.set(id, bytes);
    }
    this.#activeBatch = undefined;
  }

  async #closeCachedReadHandle(id: number): Promise<void> {
    const opening = this.#openingReadHandles.get(id);
    if (opening !== undefined) await opening.promise.catch(() => undefined);
    const cached = this.#readHandles.get(id);
    if (cached !== undefined && cached.readers > 0) {
      await new Promise<void>((resolve) => cached.drained.push(resolve));
    }
    this.#readHandles.get(id)?.handle.close();
    this.#readHandles.delete(id);
  }

  /** Removes an unpublished physical suffix left by a crash before its WAL publication. */
  truncateTailToPublishedOffset(): void {
    const size = this.#tailHandle.getSize();
    if (size < this.#tailOffset) {
      throw new Error(
        `OPFS tail extent ${String(this.#tailId)} is shorter than its published offset: ` +
          `${String(size)} < ${String(this.#tailOffset)}`,
      );
    }
    if (size > this.#tailOffset) this.#tailHandle.truncate(this.#tailOffset);
  }

  /**
   * Replay hook: a WAL entry recorded this placement after the checkpoint was taken. Advances
   * the tail position and live-byte accounting to match what the writer had done.
   */
  restorePlacement(placement: Placement): void {
    assertValidPlacement(placement);
    const liveBytes = safeExtentSum(
      this.#liveBytes.get(placement.extent) ?? 0,
      placement.length,
      "Extent live bytes",
    );
    this.#liveBytes.set(placement.extent, liveBytes);
    if (placement.extent === this.#tailId) {
      this.#tailOffset = Math.max(
        this.#tailOffset,
        safeExtentSum(placement.offset, placement.length, "Extent tail offset"),
      );
    }
    if (placement.extent >= this.#nextId) {
      // The writer sealed past the checkpointed tail; adopt its numbering.
      this.#nextId = safeExtentSuccessor(placement.extent, "Extent ID");
    }
  }

  /** Replay hook: the tail recorded by placements may belong to a newer extent than ours. */
  async adoptTail(extentId: number, offset: number): Promise<void> {
    assertValidExtentCoordinate(extentId, "Extent ID");
    assertValidExtentCoordinate(offset, "Extent tail offset");
    const nextId = safeExtentSuccessor(extentId, "Extent ID");
    if (extentId === this.#tailId) {
      this.#tailOffset = Math.max(this.#tailOffset, offset);
      return;
    }
    this.#tailHandle.close();
    // Recovery validation may have opened the newer extent through the read cache before it
    // was known to be the tail. Release that shared-read role before taking the tail lock.
    this.#readHandles.get(extentId)?.handle.close();
    this.#readHandles.delete(extentId);
    this.#tailHandle = await this.#tree.openHandle(extentPath(extentId), { create: true });
    this.#tailId = extentId;
    this.#tailOffset = offset;
    this.#nextId = Math.max(this.#nextId, nextId);
  }

  get tailExtentId(): number {
    return this.#tailId;
  }

  /** Appends synchronously; seals and rolls the tail first when it is full (the async part). */
  async append(bytes: Uint8Array, flush: boolean): Promise<Placement> {
    const initialEnd = safeExtentSum(this.#tailOffset, bytes.byteLength, "Extent tail offset");
    if (this.#tailOffset > 0 && initialEnd > SEAL_BYTES) {
      await this.#seal();
    }
    const nextOffset = safeExtentSum(this.#tailOffset, bytes.byteLength, "Extent tail offset");
    const nextLiveBytes = safeExtentSum(
      this.#liveBytes.get(this.#tailId) ?? 0,
      bytes.byteLength,
      "Extent live bytes",
    );
    const placement: Placement = {
      extent: this.#tailId,
      offset: this.#tailOffset,
      length: bytes.byteLength,
      checksum: this.#checksum(bytes),
    };
    writeFully(
      this.#tailHandle,
      bytes,
      placement.offset,
      `appending extent ${String(placement.extent)}`,
    );
    this.#tailDirty = true;
    if (flush) this.flush();
    this.#tailOffset = nextOffset;
    this.#rememberLiveBytes(this.#tailId);
    this.#liveBytes.set(this.#tailId, nextLiveBytes);
    return placement;
  }

  async #seal(): Promise<void> {
    const nextId = safeExtentSuccessor(this.#nextId, "Extent ID");
    // Relaxed durability batches flushes instead of flushing every append. Sealing is the last
    // chance to make this immutable extent durable before its held handle is released.
    this.flush();
    this.#tailHandle.close();
    // The now-sealed extent may still be read; it re-opens on demand through the cache.
    this.#tailId = this.#nextId;
    this.#nextId = nextId;
    this.#tailOffset = 0;
    this.#tailHandle = await this.#tree.openHandle(extentPath(this.#tailId), { create: true });
  }

  async read(placement: Placement): Promise<Uint8Array> {
    assertValidPlacement(placement);
    if (placement.extent === this.#tailId) return this.#readFromHandle(placement, this.#tailHandle);
    const acquired = this.#acquireReadHandle(placement.extent);
    const handle = isHandlePromise(acquired) ? await acquired : acquired;
    try {
      return this.#readFromHandle(placement, handle);
    } finally {
      this.#releaseReadHandle(placement.extent);
    }
  }

  /** Reads and verifies one complete opaque payload with exactly one checksum scan. */
  async readVerified(placement: Placement): Promise<Uint8Array> {
    const bytes = await this.read(placement);
    if (this.#checksum(bytes) !== placement.checksum) {
      throw new Error(
        `Extent placement checksum mismatch: ${String(placement.extent)}:` +
          `${String(placement.offset)}+${String(placement.length)}`,
      );
    }
    return bytes;
  }

  /** True when the complete placement currently exists; used to bound crash replay. */
  async contains(placement: Placement): Promise<boolean> {
    if (!validPlacement(placement)) return false;
    try {
      if (placement.extent === this.#tailId) {
        return placement.offset + placement.length <= this.#tailHandle.getSize();
      }
      const acquired = this.#acquireReadHandle(placement.extent);
      const handle = isHandlePromise(acquired) ? await acquired : acquired;
      try {
        return placement.offset + placement.length <= handle.getSize();
      } finally {
        this.#releaseReadHandle(placement.extent);
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === "NotFoundError") return false;
      throw error;
    }
  }

  /**
   * Returns the sealed extent with the most dead space once less than half its physical bytes
   * remain live. Relocating that extent keeps sealed-file bytes below twice live payload bytes;
   * the append tail contributes only one bounded extent beyond that ratio.
   */
  async fragmentedExtentId(): Promise<number | undefined> {
    let candidate: { id: number; wasteRatio: number } | undefined;
    for (const [id, liveBytes] of this.#liveBytes) {
      if (id === this.#tailId || liveBytes === 0) continue;
      const acquired = this.#acquireReadHandle(id);
      const handle = isHandlePromise(acquired) ? await acquired : acquired;
      let size: number;
      try {
        size = handle.getSize();
      } finally {
        this.#releaseReadHandle(id);
      }
      if (size === 0 || liveBytes * 2 >= size) continue;
      const wasteRatio = (size - liveBytes) / size;
      if (candidate === undefined || wasteRatio > candidate.wasteRatio) {
        candidate = { id, wasteRatio };
      }
    }
    return candidate?.id;
  }

  /** Actual bytes of every checkpoint-known extent, including the held tail handle. */
  async physicalByteLengths(): Promise<Map<number, number>> {
    const sizes = new Map<number, number>();
    const ids = new Set([...this.#liveBytes.keys(), this.#tailId]);
    for (const id of ids) {
      if (id === this.#tailId) {
        sizes.set(id, this.#tailHandle.getSize());
        continue;
      }
      const acquired = this.#acquireReadHandle(id);
      const handle = isHandlePromise(acquired) ? await acquired : acquired;
      try {
        sizes.set(id, handle.getSize());
      } finally {
        this.#releaseReadHandle(id);
      }
    }
    return sizes;
  }

  /** Cached hits return the handle directly: no promise allocation or extra await on reads. */
  #acquireReadHandle(extent: number): SyncFileHandle | Promise<SyncFileHandle> {
    if (this.#closed) throw new Error("This extent pool is closed");
    if (this.#deletingExtents.has(extent)) {
      throw new Error(`Extent ${String(extent)} is being deleted`);
    }
    const cached = this.#readHandles.get(extent);
    if (cached !== undefined) {
      // Refresh recency.
      this.#readHandles.delete(extent);
      this.#readHandles.set(extent, cached);
      cached.readers += 1;
      return cached.handle;
    }
    const opening = this.#openingReadHandles.get(extent);
    if (opening !== undefined) {
      opening.reservations += 1;
      return opening.promise;
    }
    const pending = { reservations: 1 } as OpeningReadHandle;
    pending.promise = this.#tree.openHandle(extentPath(extent), { create: false }).then(
      (handle) => {
        this.#openingReadHandles.delete(extent);
        // An open already in flight owns reader reservations. A deletion that starts later
        // blocks new acquisitions and awaits this promise plus those readers; rejecting the
        // earlier reservations here would let relocation invalidate a selected placement.
        if (this.#closed) {
          handle.close();
          throw new Error(`Extent ${String(extent)} became unavailable while opening`);
        }
        this.#readHandles.set(extent, {
          handle,
          readers: pending.reservations,
          drained: [],
        });
        this.#evictReadHandles();
        return handle;
      },
      (error: unknown) => {
        this.#openingReadHandles.delete(extent);
        throw error;
      },
    );
    this.#openingReadHandles.set(extent, pending);
    return pending.promise;
  }

  #releaseReadHandle(extent: number): void {
    const cached = this.#readHandles.get(extent);
    if (cached === undefined) return;
    cached.readers -= 1;
    if (cached.readers < 0) throw new Error(`Extent ${String(extent)} read lease underflow`);
    if (cached.readers === 0) {
      for (const resolve of cached.drained.splice(0)) resolve();
      this.#evictReadHandles();
    }
  }

  #evictReadHandles(): void {
    while (this.#readHandles.size > READ_HANDLE_CACHE) {
      const idle = [...this.#readHandles].find(([, cached]) => cached.readers === 0);
      if (idle === undefined) return;
      const [extent, cached] = idle;
      cached.handle.close();
      this.#readHandles.delete(extent);
    }
  }

  #readFromHandle(placement: Placement, handle: SyncFileHandle): Uint8Array {
    const end = placement.offset + placement.length;
    const size = handle.getSize();
    if (end > size) {
      throw new Error(
        `Extent placement exceeds its file: ${String(placement.extent)}:` +
          `${String(placement.offset)}+${String(placement.length)} > ${String(size)}`,
      );
    }
    const bytes = new Uint8Array(placement.length);
    readFully(
      handle,
      bytes,
      placement.offset,
      `reading extent ${String(placement.extent)} placement`,
    );
    return bytes;
  }

  /** Live-byte accounting for reclaimed payloads; reports extents now safe to delete. */
  release(placements: Iterable<Placement>): number[] {
    const decrements = new Map<number, number>();
    const ranges = new Map<number, Array<{ start: number; end: number }>>();
    for (const placement of placements) {
      assertValidPlacement(placement);
      decrements.set(placement.extent, (decrements.get(placement.extent) ?? 0) + placement.length);
      if (placement.length > 0) {
        const extentRanges = ranges.get(placement.extent) ?? [];
        extentRanges.push({ start: placement.offset, end: placement.offset + placement.length });
        ranges.set(placement.extent, extentRanges);
      }
    }
    for (const [id, extentRanges] of ranges) {
      extentRanges.sort((left, right) => left.start - right.start || left.end - right.end);
      for (let index = 1; index < extentRanges.length; index += 1) {
        const previous = extentRanges[index - 1];
        const current = extentRanges[index];
        if (previous !== undefined && current !== undefined && current.start < previous.end) {
          throw new Error(
            `Extent ${String(id)} release contains overlapping or duplicate placements: ` +
              `${String(previous.start)}..${String(previous.end)} and ` +
              `${String(current.start)}..${String(current.end)}`,
          );
        }
      }
    }
    // Validate the entire batch before mutating one counter. Underflow is an invariant
    // violation, never evidence that an extent is empty and safe to delete.
    for (const [id, decrement] of decrements) {
      const live = this.#liveBytes.get(id);
      if (live === undefined || decrement > live) {
        throw new Error(
          `Extent ${String(id)} live-byte accounting underflow: release ${String(decrement)} ` +
            `from ${live === undefined ? "an unknown extent" : String(live)}`,
        );
      }
    }
    for (const [id, decrement] of decrements) {
      this.#rememberLiveBytes(id);
      this.#liveBytes.set(id, (this.#liveBytes.get(id) ?? 0) - decrement);
    }
    const deletable: number[] = [];
    for (const [id, bytes] of this.#liveBytes) {
      if (bytes === 0 && id !== this.#tailId) deletable.push(id);
    }
    return deletable;
  }

  /** Removes a drained extent's file and forgets it. Idempotent. */
  async deleteExtent(id: number): Promise<void> {
    if (this.#activeBatch !== undefined) {
      throw new Error("Cannot delete an extent while an unpublished batch is active");
    }
    if (id === this.#tailId) return;
    this.#deletingExtents.add(id);
    try {
      const opening = this.#openingReadHandles.get(id);
      if (opening !== undefined) await opening.promise.catch(() => undefined);
      const cached = this.#readHandles.get(id);
      if (cached !== undefined && cached.readers > 0) {
        await new Promise<void>((resolve) => cached.drained.push(resolve));
      }
      this.#readHandles.get(id)?.handle.close();
      this.#readHandles.delete(id);
      await this.#tree.deleteFile(extentPath(id));
      // Preserve the zero-live entry when the physical delete is refused. The caller can then
      // retry it and account the cleanup debt instead of silently orphaning an untracked file.
      this.#liveBytes.delete(id);
    } finally {
      this.#deletingExtents.delete(id);
    }
  }

  meta(): ExtentMeta {
    return {
      nextExtentId: this.#nextId,
      tailExtentId: this.#tailId,
      tailOffset: this.#tailOffset,
      liveBytes: [...this.#liveBytes.entries()],
    };
  }

  flush(): void {
    if (!this.#tailDirty) return;
    this.#tailHandle.flush();
    this.#tailDirty = false;
  }

  close(): void {
    this.#closed = true;
    this.#tailHandle.close();
    for (const { handle } of this.#readHandles.values()) handle.close();
    this.#readHandles.clear();
  }

  #rememberLiveBytes(id: number): void {
    const batch = this.#activeBatch;
    if (batch === undefined || batch._liveBytesBefore.has(id)) return;
    batch._liveBytesBefore.set(id, this.#liveBytes.get(id));
  }
}

export function extentPath(id: number): string[] {
  return ["extents", String(id).padStart(6, "0")];
}

export function validPlacement(placement: Placement): boolean {
  return (
    Number.isSafeInteger(placement.extent) &&
    placement.extent >= 0 &&
    placement.extent < Number.MAX_SAFE_INTEGER &&
    Number.isSafeInteger(placement.offset) &&
    placement.offset >= 0 &&
    Number.isSafeInteger(placement.length) &&
    placement.length >= 0 &&
    Number.isSafeInteger(placement.checksum) &&
    placement.checksum >= 0 &&
    placement.checksum <= 0xffff_ffff &&
    placement.offset + placement.length <= Number.MAX_SAFE_INTEGER
  );
}

export function assertValidPlacement(placement: Placement): void {
  if (!validPlacement(placement)) {
    throw new TypeError(
      `Invalid extent placement: ${String(placement.extent)}:${String(placement.offset)}+` +
        `${String(placement.length)} checksum ${String(placement.checksum)}`,
    );
  }
}

export function assertValidExtentMeta(meta: ExtentMeta): void {
  if (
    !Number.isSafeInteger(meta.nextExtentId) ||
    meta.nextExtentId < 1 ||
    !Number.isSafeInteger(meta.tailExtentId) ||
    meta.tailExtentId < 0 ||
    meta.tailExtentId >= meta.nextExtentId ||
    !Number.isSafeInteger(meta.tailOffset) ||
    meta.tailOffset < 0 ||
    !Array.isArray(meta.liveBytes)
  ) {
    throw new TypeError("Invalid OPFS extent checkpoint metadata");
  }
  const ids = new Set<number>();
  for (const entry of meta.liveBytes) {
    const [id, bytes] = entry;
    if (
      !Number.isSafeInteger(id) ||
      id < 0 ||
      id >= meta.nextExtentId ||
      ids.has(id) ||
      !Number.isSafeInteger(bytes) ||
      bytes < 0
    ) {
      throw new TypeError(`Invalid OPFS extent live-byte entry: ${String(id)} => ${String(bytes)}`);
    }
    ids.add(id);
  }
}

function isHandlePromise(
  value: SyncFileHandle | Promise<SyncFileHandle>,
): value is Promise<SyncFileHandle> {
  return typeof (value as { then?: unknown }).then === "function";
}

function assertValidExtentCoordinate(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError(`${label} is out of range`);
}

function safeExtentSuccessor(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0 || value >= Number.MAX_SAFE_INTEGER) {
    throw new RangeError(`${label} cannot exceed the safe integer range`);
  }
  return value + 1;
}

function safeExtentSum(left: number, right: number, label: string): number {
  if (
    !Number.isSafeInteger(left) ||
    left < 0 ||
    !Number.isSafeInteger(right) ||
    right < 0 ||
    left > Number.MAX_SAFE_INTEGER - right
  ) {
    throw new RangeError(`${label} cannot exceed the safe integer range`);
  }
  return left + right;
}
