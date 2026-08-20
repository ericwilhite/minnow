import type { SyncFileHandle } from "./sync-file.js";

/**
 * Packed, append-only extent files for bulk bytes — blocks and full-text base chunks.
 *
 * The leader appends into one tail extent through a handle it keeps open (microseconds per
 * write) and seals it at a size threshold; sealed extents are immutable. Every payload's home
 * is a placement — extent id, offset, length — recorded by the WAL entry that publishes it,
 * so recovery re-derives the tail position from the replayed placements and any bytes beyond
 * it (a crash between an extent write and its WAL frame) are dead space, overwritten by the
 * next append.
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

export interface Placement {
  extent: number;
  offset: number;
  length: number;
}

/** Persisted in checkpoints; placements in replayed WAL entries refine `tailOffset`. */
export interface ExtentMeta {
  nextExtentId: number;
  tailExtentId: number;
  tailOffset: number;
  /** Live payload bytes per extent id; an extent at zero that is not the tail is deletable. */
  liveBytes: Array<readonly [number, number]>;
}

const SEAL_BYTES = 8 * 1024 * 1024;
const READ_HANDLE_CACHE = 12;

export class ExtentPool {
  readonly #tree: ExtentFiles;
  #tailId: number;
  #nextId: number;
  #tailOffset: number;
  #tailHandle: SyncFileHandle;
  readonly #liveBytes = new Map<number, number>();
  /** Sealed-extent read handles, LRU by insertion order. The tail is never in here. */
  readonly #readHandles = new Map<number, SyncFileHandle>();

  private constructor(
    tree: ExtentFiles,
    meta: { tailId: number; nextId: number; tailOffset: number },
    tailHandle: SyncFileHandle,
  ) {
    this.#tree = tree;
    this.#tailId = meta.tailId;
    this.#nextId = meta.nextId;
    this.#tailOffset = meta.tailOffset;
    this.#tailHandle = tailHandle;
  }

  /** Opens (or creates) the tail extent. `meta` comes from the checkpoint, if any. */
  static async open(tree: ExtentFiles, meta: ExtentMeta | undefined): Promise<ExtentPool> {
    const tailId = meta?.tailExtentId ?? 0;
    const nextId = meta?.nextExtentId ?? 1;
    const tailHandle = await tree.openHandle(extentPath(tailId), { create: true });
    const pool = new ExtentPool(
      tree,
      { tailId, nextId, tailOffset: meta?.tailOffset ?? 0 },
      tailHandle,
    );
    for (const [id, bytes] of meta?.liveBytes ?? []) pool.#liveBytes.set(id, bytes);
    return pool;
  }

  /**
   * Replay hook: a WAL entry recorded this placement after the checkpoint was taken. Advances
   * the tail position and live-byte accounting to match what the writer had done.
   */
  restorePlacement(placement: Placement): void {
    this.#liveBytes.set(
      placement.extent,
      (this.#liveBytes.get(placement.extent) ?? 0) + placement.length,
    );
    if (placement.extent === this.#tailId) {
      this.#tailOffset = Math.max(this.#tailOffset, placement.offset + placement.length);
    }
    if (placement.extent >= this.#nextId) {
      // The writer sealed past the checkpointed tail; adopt its numbering.
      this.#nextId = placement.extent + 1;
    }
  }

  /** Replay hook: the tail recorded by placements may belong to a newer extent than ours. */
  async adoptTail(extentId: number, offset: number): Promise<void> {
    if (extentId === this.#tailId) {
      this.#tailOffset = Math.max(this.#tailOffset, offset);
      return;
    }
    this.#tailHandle.close();
    this.#tailHandle = await this.#tree.openHandle(extentPath(extentId), { create: true });
    this.#tailId = extentId;
    this.#tailOffset = offset;
    this.#nextId = Math.max(this.#nextId, extentId + 1);
  }

  get tailExtentId(): number {
    return this.#tailId;
  }

  /** Appends synchronously; seals and rolls the tail first when it is full (the async part). */
  async append(bytes: Uint8Array, flush: boolean): Promise<Placement> {
    if (this.#tailOffset > 0 && this.#tailOffset + bytes.byteLength > SEAL_BYTES) {
      await this.#seal();
    }
    const placement: Placement = {
      extent: this.#tailId,
      offset: this.#tailOffset,
      length: bytes.byteLength,
    };
    this.#tailHandle.write(bytes, { at: placement.offset });
    if (flush) this.#tailHandle.flush();
    this.#tailOffset += bytes.byteLength;
    this.#liveBytes.set(this.#tailId, (this.#liveBytes.get(this.#tailId) ?? 0) + bytes.byteLength);
    return placement;
  }

  async #seal(): Promise<void> {
    this.#tailHandle.close();
    // The now-sealed extent may still be read; it re-opens on demand through the cache.
    this.#tailId = this.#nextId;
    this.#nextId += 1;
    this.#tailOffset = 0;
    this.#tailHandle = await this.#tree.openHandle(extentPath(this.#tailId), { create: true });
  }

  async read(placement: Placement): Promise<Uint8Array> {
    const bytes = new Uint8Array(placement.length);
    if (placement.extent === this.#tailId) {
      this.#tailHandle.read(bytes, { at: placement.offset });
      return bytes;
    }
    const handle = await this.#readHandle(placement.extent);
    handle.read(bytes, { at: placement.offset });
    return bytes;
  }

  async #readHandle(extent: number): Promise<SyncFileHandle> {
    const cached = this.#readHandles.get(extent);
    if (cached !== undefined) {
      // Refresh recency.
      this.#readHandles.delete(extent);
      this.#readHandles.set(extent, cached);
      return cached;
    }
    const handle = await this.#tree.openHandle(extentPath(extent), { create: false });
    this.#readHandles.set(extent, handle);
    if (this.#readHandles.size > READ_HANDLE_CACHE) {
      const [oldest] = this.#readHandles.keys();
      if (oldest !== undefined) {
        this.#readHandles.get(oldest)?.close();
        this.#readHandles.delete(oldest);
      }
    }
    return handle;
  }

  /** Live-byte accounting for reclaimed payloads; reports extents now safe to delete. */
  release(placements: Iterable<Placement>): number[] {
    for (const placement of placements) {
      const remaining = (this.#liveBytes.get(placement.extent) ?? 0) - placement.length;
      this.#liveBytes.set(placement.extent, Math.max(0, remaining));
    }
    const deletable: number[] = [];
    for (const [id, bytes] of this.#liveBytes) {
      if (bytes === 0 && id !== this.#tailId) deletable.push(id);
    }
    return deletable;
  }

  /** Removes a drained extent's file and forgets it. Idempotent. */
  async deleteExtent(id: number): Promise<void> {
    if (id === this.#tailId) return;
    this.#readHandles.get(id)?.close();
    this.#readHandles.delete(id);
    this.#liveBytes.delete(id);
    await this.#tree.deleteFile(extentPath(id));
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
    this.#tailHandle.flush();
  }

  close(): void {
    this.#tailHandle.close();
    for (const handle of this.#readHandles.values()) handle.close();
    this.#readHandles.clear();
  }
}

export function extentPath(id: number): string[] {
  return ["extents", String(id).padStart(6, "0")];
}
