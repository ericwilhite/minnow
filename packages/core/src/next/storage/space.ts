/**
 * The extent space (extents.ts): `openExtentSpace` and its writers, builders and publication,
 * over any `StoreSession`. The contract, the rules and their reasons are in extents.ts; this file
 * notes only how they are met.
 *
 * - **Allocation** is one `FreeSpaceMap` (free-space.ts) over the data space; a builder's space is
 *   an `ExtentReservation` (reservation.ts) on it.
 * - **Writes** go through one 1 MiB write slab, so a chunk appended page by page costs a few
 *   platform writes. The store writes only at `DATA_ALIGN` offsets, so the slab always starts at an
 *   aligned logical position of the builder that owns it: when it fills, its aligned prefix is
 *   written and the partial last sector stays in the slab; when another builder appends, everything
 *   staged is written (the last sector partially) and that partial sector (< 4 KiB) is kept with
 *   its builder, to start its next staging. Write amplification is at most one sector per switch.
 * - **Publication** is a promise chain (one at a time, in call order). Validation runs when a
 *   publish's turn comes, before any effect.
 * - **Reclaim**: `ReclaimLedger` (reclaim.ts) decides when retired extents are reusable; freeing
 *   drops the cache's blobs of each extent, then `releaseData`, and only when that resolves does
 *   the range enter the free map. A free tail is then released (truncated) at once: every byte the
 *   space may still write past `end` is reserved in the map, so this never cuts live data.
 */
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
  type BlobRef,
  type ByteBudget,
  type ExtentBuilder,
  type ExtentRef,
  type ExtentSpace,
  type ExtentWriter,
  type OpenExtentSpace,
  type OpenSpaceOptions,
  type PendingBlob,
  type PublishChange,
  type ReachableExtents,
  type ReadLease,
  type ReadOptions,
  type SpaceStats,
  type WriterState,
  type WrittenExtent,
} from "./extents.js";
import { FreeSpaceMap } from "./free-space.js";
import { PageCache } from "./page-cache.js";
import { ReclaimLedger, type RootPin } from "./reclaim.js";
import { ExtentReservation } from "./reservation.js";
import { MAX_SLAB } from "./slabs.js";

const alignDown = (value: number) => value - (value % DATA_ALIGN);

/** A one-at-a-time lock over a shared buffer. */
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

/** Every extent `root` reaches, by offset, with its aligned size. */
async function collect(
  root: Uint8Array | null,
  reachable: ReachableExtents,
  space: { read: ExtentSpace["read"]; peek: ExtentSpace["peek"] },
  budget: ByteBudget,
  sizeOf: (bytes: number) => number,
): Promise<Map<number, number>> {
  const extents = new Map<number, number>();
  if (root === null) return extents;
  for await (const item of reachable(root, space, budget)) {
    const extent = "extent" in item ? item.extent : item;
    if (!(extent.offset >= 0 && extent.offset % DATA_ALIGN === 0 && extent.length > 0))
      throw new StoreCorruptionError(
        `A root names an invalid extent at ${String(extent.offset)} (${String(extent.length)} bytes)`,
      );
    const size = sizeOf(extent.length);
    const known = extents.get(extent.offset);
    if (known !== undefined && known !== size)
      throw new StoreCorruptionError(`Two extents at ${String(extent.offset)} differ in size`);
    extents.set(extent.offset, size);
  }
  return extents;
}

export const openExtentSpace: OpenExtentSpace = async (session, options) => {
  options.signal?.throwIfAborted();
  const root = await session.readRoot();
  const previousRoot = await session.readPreviousRoot();
  const dataLength = await session.dataLength();
  const cache = new PageCache(session);
  cache.attach(options.registerCache(cache));
  try {
    const sizeOf = (bytes: number) => Math.ceil(bytes / DATA_ALIGN) * DATA_ALIGN;
    const reader = {
      read: (refs: readonly BlobRef[], read: ReadOptions) => cache.read(refs, read),
      peek: (ref: BlobRef) => cache.peek(ref),
    };
    const current = await collect(root, options.reachable, reader, options.budget, sizeOf);
    options.signal?.throwIfAborted();
    const previous = await collect(previousRoot, options.reachable, reader, options.budget, sizeOf);
    const previousOnly: DataRange[] = [];
    for (const [offset, length] of previous)
      if (!current.has(offset)) previousOnly.push({ offset, length });
    let map: FreeSpaceMap;
    try {
      map = FreeSpaceMap.fromAllocated(DATA_ALIGN, dataLength, [
        ...[...current].map(([offset, length]) => ({ offset, length })),
        ...previousOnly,
      ]);
    } catch (error) {
      throw new StoreCorruptionError(
        `The durable roots name inconsistent extents: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    return new Space(session, options, cache, map, root, previousRoot, previousOnly, dataLength);
  } catch (error) {
    cache.close();
    throw error;
  }
};

class Space implements ExtentSpace {
  readonly #session: StoreSession;
  readonly #reachable: ReachableExtents;
  readonly #budget: ByteBudget;
  readonly #cache: PageCache;
  readonly #map: FreeSpaceMap;
  readonly #ledger: ReclaimLedger<DataRange>;
  /** Retired extents not yet free (waiting for a publish or a pin, or being released). */
  readonly #retired = new Map<number, number>();
  /** Writers not yet published or aborted. */
  readonly writers = new Set<Writer>();
  readonly #publishing = new Mutex();
  readonly writeLock = new Mutex();
  readonly writeSlab = new Uint8Array(MAX_SLAB);
  /** The builder whose bytes the write slab holds, from its aligned logical `start`. */
  staged: { builder: Builder; start: number; length: number } | null = null;
  readonly #background = new Set<Promise<void>>();
  #root: Uint8Array | null;
  #previousRoot: Uint8Array | null;
  #failed: Error | null = null;
  #closed = false;
  #releasingBytes = 0;
  #relocations = 0;
  /** Bytes the data space holds as far as this space knows (written or kept since open). */
  #fileLength: number;

  constructor(
    session: StoreSession,
    options: OpenSpaceOptions,
    cache: PageCache,
    map: FreeSpaceMap,
    root: Uint8Array | null,
    previousRoot: Uint8Array | null,
    previousOnly: readonly DataRange[],
    dataLength: number,
  ) {
    this.#session = session;
    this.#reachable = options.reachable;
    this.#budget = options.budget;
    this.#cache = cache;
    this.#map = map;
    this.#root = root;
    this.#previousRoot = previousRoot;
    this.#fileLength = dataLength;
    this.#ledger = new ReclaimLedger<DataRange>((ranges) => {
      this.#background_(this.free(ranges));
    });
    for (const range of previousOnly) this.#retired.set(range.offset, range.length);
    this.#ledger.retireAtOpen(previousOnly);
    this.#releaseTail();
  }

  get root(): Uint8Array | null {
    return this.#root;
  }

  get previousRoot(): Uint8Array | null {
    return this.#previousRoot;
  }

  get generation(): number {
    return this.#ledger.generation;
  }

  get failed(): Error | null {
    return this.#failed;
  }

  get map(): FreeSpaceMap {
    return this.#map;
  }

  get stats(): SpaceStats {
    let writerBytes = 0;
    for (const writer of this.writers) writerBytes += writer.bytes;
    let retired = 0;
    for (const size of this.#retired.values()) retired += size;
    return {
      dataBytes: this.#map.end,
      freeBytes: this.#map.freeBytes,
      writerBytes,
      waitingBytes: retired - this.#releasingBytes,
      releasingBytes: this.#releasingBytes,
      relocations: this.#relocations,
      pinCount: this.#ledger.pinCount,
      oldestPin: this.#ledger.oldestPin,
      cache: this.#cache.stats,
    };
  }

  // ── Reading ──────────────────────────────────────────────────────────────────────────────────

  read(refs: readonly BlobRef[], options: ReadOptions): Promise<ReadLease> {
    return this.#cache.read(refs, options);
  }

  peek(ref: BlobRef): Uint8Array | null {
    return this.#cache.peek(ref);
  }

  // ── Writing ──────────────────────────────────────────────────────────────────────────────────

  beginWrite(label: string): ExtentWriter {
    this.usable();
    const writer = new Writer(this, label);
    this.writers.add(writer);
    return writer;
  }

  /** Throws unless writes and publishes are allowed. */
  usable(): void {
    if (this.#closed) throw new StoreClosedError();
    if (this.#failed !== null) throw new SpaceFailedError(this.#failed);
  }

  /** A session call that writes: ownership loss fails the space. */
  async sessionWrite(offset: number, bytes: Uint8Array): Promise<void> {
    try {
      await this.#session.writeData(offset, bytes);
    } catch (error) {
      if (error instanceof OwnershipLostError) this.fail(error);
      throw error;
    }
    this.#fileLength = Math.max(this.#fileLength, offset + bytes.length);
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

  /** Copies `parts` into the slab after `builder`'s bytes, writing as the slab fills. */
  async stage(builder: Builder, parts: readonly Uint8Array[]): Promise<void> {
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
  async free(ranges: readonly DataRange[]): Promise<void> {
    if (ranges.length === 0) return;
    let bytes = 0;
    for (const range of ranges) {
      this.#cache.dropExtent(range.offset);
      bytes += range.length;
    }
    this.#releasingBytes += bytes;
    try {
      await this.#session.releaseData(ranges);
    } catch (error) {
      // The space stays allocated (a leak until reopen), never handed out twice.
      this.fail(error);
      this.#releasingBytes -= bytes;
      return;
    }
    this.#releasingBytes -= bytes;
    for (const range of ranges) {
      this.#map.free(range.offset, range.length);
      this.#retired.delete(range.offset);
    }
    this.#releaseTail();
  }

  #releaseTail(): void {
    const end = this.#map.end;
    if (end >= this.#fileLength || this.#closed) return;
    const length = this.#fileLength - end;
    this.#fileLength = end;
    this.#background_(
      this.#session.releaseData([{ offset: end, length }]).catch((error: unknown) => {
        this.fail(error);
      }),
    );
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

  publish(root: Uint8Array, change: PublishChange): Promise<number> {
    return this.#publishing.run(() => this.#publishNow(root, change));
  }

  async #publishNow(root: Uint8Array, change: PublishChange): Promise<number> {
    this.usable();
    const writers = this.#checkWriters(change.writers);
    const retired = this.#checkRetired(change.retired);
    for (const writer of writers) writer.state = "publishing";
    try {
      await this.#session.publish(root);
    } catch (error) {
      for (const writer of writers) writer.state = "indeterminate";
      this.fail(error);
      throw error;
    }
    for (const writer of writers) {
      writer.state = "published";
      this.writers.delete(writer);
    }
    for (const range of retired) this.#retired.set(range.offset, range.length);
    this.#previousRoot = this.#root;
    this.#root = root.slice();
    return this.#ledger.advance(retired);
  }

  #checkWriters(writers: readonly ExtentWriter[]): Writer[] {
    const checked = new Set<Writer>();
    for (const candidate of writers) {
      if (!(candidate instanceof Writer) || candidate.space !== this)
        throw new TypeError("publish: a writer from another space");
      if (checked.has(candidate)) throw new TypeError(`publish: writer ${candidate.label} twice`);
      if (candidate.state !== "open")
        throw new TypeError(`publish: writer ${candidate.label} is ${candidate.state}`);
      if (candidate.builders.size > 0 || candidate.busy > 0)
        throw new TypeError(`publish: writer ${candidate.label} has an unsealed builder`);
      checked.add(candidate);
    }
    return [...checked];
  }

  #checkRetired(extents: readonly ExtentRef[]): DataRange[] {
    const held = new Set<number>();
    for (const writer of this.writers) for (const extent of writer.extents) held.add(extent.offset);
    const ranges: DataRange[] = [];
    const seen = new Set<number>();
    for (const extent of extents) {
      const size = this.#map.sizeOf(extent.length);
      const where = `the extent at ${String(extent.offset)}`;
      if (extent.offset % DATA_ALIGN !== 0 || size === 0)
        throw new RangeError(`publish: ${where} is not an extent`);
      if (seen.has(extent.offset)) throw new RangeError(`publish: ${where} is retired twice`);
      if (this.#retired.has(extent.offset))
        throw new RangeError(`publish: ${where} is already retired`);
      if (held.has(extent.offset))
        throw new RangeError(`publish: ${where} belongs to an unpublished writer`);
      if (!this.#map.isAllocated(extent.offset, size))
        throw new RangeError(`publish: ${where} is not allocated`);
      for (const writer of this.writers)
        for (const builder of writer.builders)
          for (const segment of builder.reservation.segments)
            if (
              segment.offset < extent.offset + size &&
              extent.offset < segment.offset + segment.capacity
            )
              throw new RangeError(`publish: ${where} overlaps an open builder`);
      seen.add(extent.offset);
      ranges.push({ offset: extent.offset, length: size });
    }
    return ranges;
  }

  pin(label?: string): RootPin {
    return this.#ledger.pin(label);
  }

  // ── Audit ────────────────────────────────────────────────────────────────────────────────────

  async audit(): Promise<void> {
    await this.settled();
    const fail = (message: string): never => {
      throw new Error(`Extent audit: ${message}`);
    };
    const sizeOf = (bytes: number) => this.#map.sizeOf(bytes);
    const current = await collect(this.#root, this.#reachable, this, this.#budget, sizeOf);
    const previous = await collect(this.#previousRoot, this.#reachable, this, this.#budget, sizeOf);
    const accounted: DataRange[] = [];
    for (const [offset, length] of current) {
      if (!this.#map.isAllocated(offset, length))
        fail(`the current root's extent at ${String(offset)} is free`);
      if (this.#retired.has(offset))
        fail(`the current root's extent at ${String(offset)} is retired`);
      accounted.push({ offset, length });
    }
    for (const [offset, length] of previous) {
      if (current.has(offset)) continue;
      if (!this.#map.isAllocated(offset, length))
        fail(`the previous root's extent at ${String(offset)} is free`);
      if (!this.#retired.has(offset))
        fail(`the previous root's extent at ${String(offset)} is neither reachable nor retired`);
    }
    for (const [offset, length] of this.#retired) accounted.push({ offset, length });
    for (const writer of this.writers) {
      for (const extent of writer.extents)
        accounted.push({ offset: extent.offset, length: sizeOf(extent.length) });
      for (const builder of writer.builders)
        for (const segment of builder.reservation.segments)
          accounted.push({ offset: segment.offset, length: segment.capacity });
    }
    accounted.sort((a, b) => a.offset - b.offset || a.length - b.length);
    // The accounted ranges and the free runs must tile [0, end) exactly.
    const tiles = [...accounted, ...this.#map.runs()].sort((a, b) => a.offset - b.offset);
    let cursor = 0;
    let last: DataRange | null = null;
    for (const tile of tiles) {
      if (last !== null && tile.offset === last.offset && tile.length === last.length) continue;
      if (tile.offset > cursor)
        fail(`space at ${String(cursor)} is allocated but unaccounted (a leak)`);
      if (tile.offset < cursor) fail(`space at ${String(tile.offset)} is accounted twice`);
      cursor = tile.offset + tile.length;
      last = tile;
    }
    if (cursor !== this.#map.end)
      fail(`space at ${String(cursor)} is allocated but unaccounted (a leak)`);
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    await this.settled();
    this.#closed = true;
    this.#cache.close();
  }
}

class Writer implements ExtentWriter {
  readonly space: Space;
  readonly label: string;
  state: WriterState = "open";
  readonly builders = new Set<Builder>();
  readonly #extents: ExtentRef[] = [];
  /** Calls in flight (appends, seals): abort waits for them. */
  busy = 0;
  #idle: (() => void) | null = null;

  constructor(space: Space, label: string) {
    this.space = space;
    this.label = label;
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
    this.space.usable();
  }

  async track<T>(work: () => Promise<T>): Promise<T> {
    this.busy += 1;
    try {
      return await work();
    } finally {
      this.busy -= 1;
      if (this.busy === 0) this.#idle?.();
    }
  }

  open(capacity: number): ExtentBuilder {
    this.usable();
    if (!(Number.isSafeInteger(capacity) && capacity > 0))
      throw new RangeError("A builder needs a positive capacity");
    const builder = new Builder(this, new ExtentReservation(this.space.map, capacity));
    this.builders.add(builder);
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
    const index = this.#extents.findIndex(
      (candidate) => candidate.offset === extent.offset && candidate.length === extent.length,
    );
    if (index < 0) throw new RangeError(`Writer ${this.label} did not seal that extent`);
    this.#extents.splice(index, 1);
    await this.space.free([
      { offset: extent.offset, length: this.space.map.sizeOf(extent.length) },
    ]);
  }

  async abort(): Promise<void> {
    if (this.state === "aborted") return;
    if (this.state !== "open") throw new TypeError(`Writer ${this.label} is ${this.state}`);
    this.state = "aborted";
    if (this.busy > 0)
      await new Promise<void>((resolve) => {
        this.#idle = resolve;
      });
    const ranges: DataRange[] = [];
    await this.space.writeLock.run(async () => {
      for (const builder of this.builders) {
        if (this.space.staged?.builder === builder) this.space.staged = null;
        ranges.push(...builder.abandon());
      }
    });
    this.builders.clear();
    for (const extent of this.#extents)
      ranges.push({ offset: extent.offset, length: this.space.map.sizeOf(extent.length) });
    this.#extents.length = 0;
    this.space.writers.delete(this);
    await this.space.free(ranges);
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

  /** Everything this builder holds, for its writer's abort. */
  abandon(): DataRange[] {
    if (this.#leftover !== null) return this.#leftover;
    return this.reservation.abort();
  }

  extend(bytes: number): boolean {
    this.#usable();
    if (!(Number.isSafeInteger(bytes) && bytes >= 0)) throw new RangeError("Bad extend size");
    return this.reservation.extend(bytes);
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

  async seal(): Promise<WrittenExtent | null> {
    this.#usable();
    if (this.#appending) throw new TypeError("seal during an append");
    this.#state = "sealed";
    const space = this.writer.space;
    if (this.#length === 0) {
      const ranges = await space.writeLock.run(async () => {
        if (space.staged?.builder === this) space.staged = null;
        return this.reservation.abort();
      });
      this.writer.sealed(this, null);
      await space.free(ranges);
      return null;
    }
    const plan = await this.writer
      .track(() =>
        space.writeLock.run(async () => {
          if (space.staged?.builder === this) await space.flushStaged(false);
          if (this.failure !== null) throw this.failure;
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
          return sealed;
        }),
      )
      .catch((error: unknown) => {
        this.failure ??= error instanceof Error ? error : new Error(String(error));
        throw error;
      });
    const extent: ExtentRef = plan.extent;
    this.#leftover = null;
    this.tail = null;
    this.writer.sealed(this, extent);
    await space.free(plan.release);
    return {
      extent,
      blobs: this.#pending.map((blob) => ({ extent, ...blob })),
    };
  }
}
