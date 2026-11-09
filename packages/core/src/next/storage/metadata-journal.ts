/**
 * Owner-private page chains for R8 descriptors, saved roots and reclaim worklists. Nothing here
 * is a recovery authority: a new owner resets MetadataStore and reconstructs from durable roots.
 * Callers serialize workspace use BEFORE acquiring the ownership-transition barrier. Every
 * operation then uses prepaid frames; it never asks the governor for memory or awaits a loan.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import { pauseIfDue, type TaskWork } from "../task.js";
import {
  METADATA_PAGE_BYTES,
  METADATA_SMALL_BYTES,
  StoreCorruptionError,
  type MetadataStore,
} from "./contract.js";
import {
  ReadDeniedError,
  type ByteBudget,
  type CacheMemory,
  type OpenSpaceOptions,
} from "./extents.js";

export const METADATA_WORKSPACE_BYTES = 512 * 1024;
/** Eight direct-mapped compact control pages, including buffer/view/header allowance. */
export const METADATA_CONTROL_CACHE_BYTES = 2752;
const MAGIC = 0x504d4e4d; // MNMP; private to this owner, not a supported database encoding.
const HEADER = 24;
const CHECKSUM = METADATA_PAGE_BYTES - 4;
// Keep pair worklists aligned even across page boundaries; the unused tail is checksummed.
export const METADATA_PAYLOAD_BYTES = Math.floor((CHECKSUM - HEADER) / 16) * 16;
const DESCRIPTOR = 1,
  DATA = 2,
  FREE = 3;
const DESCRIPTOR_BYTES = 48;
const EMPTY: Uint8Array[] = [];

/**
 * W inventory (all bounded before any chain exists):
 * - native-list source: two owned60KiB frames, one256KiB record, <=16KiB for128
 *   selected native index entries/views,8KiB headers,2752B fixed control cache and256B I/O:
 *   412608B. Raw list cursors borrow S, never an additional decoded E-vector.
 * - private startup merge/sort: four60KiB frames, one60KiB native metadata response,
 *   headers/cache/compact I/O:318400B. No data-index page is selected in this phase.
 * - native release: two60KiB frames, one256KiB boundary response, one60KiB boundary
 *   copy, three <=256-range vectors at80B per range, headers/cache/compact I/O:519104B.
 *   Boundary lookup is at most two index entries, covered by the header allowance.
 * - full physical audit: no cursor frames; <=4096 index entries/keys at96B per entry,
 *   headers/cache/compact I/O:404416B. The workspace mutex excludes every frame phase.
 * The remaining allowance is headroom, not capacity for an O(extents) collection. The caller
 * must not overlap source reads with the merge/reclaim phase or escape a phase's frame views.
 * Native storage contents and unreachable GC garbage are not resident managed buffers.
 */
export class MetadataWorkspace {
  readonly evictableBytes = 0;
  #budget: ByteBudget | null;
  #memory: CacheMemory | null = null;
  #frames = EMPTY;
  #frameViews: DataView[] = [];
  #io = new Uint8Array(METADATA_SMALL_BYTES);
  #controls = new Uint8Array(8 * METADATA_SMALL_BYTES);
  #controlHandles = new Float64Array(8);
  #active = false;
  #borrowed = 0;
  #readOnly = 0;
  #closed = false;
  #epoch = 0;
  get epoch(): number {
    return this.#epoch;
  }
  static create(budget: ByteBudget): MetadataWorkspace {
    if (!budget.allocate(METADATA_WORKSPACE_BYTES))
      throw new ReadDeniedError(METADATA_WORKSPACE_BYTES);
    return new MetadataWorkspace(budget);
  }
  private constructor(budget: ByteBudget) {
    this.#budget = budget;
  }
  get bytes(): number {
    return this.#closed ? 0 : METADATA_WORKSPACE_BYTES;
  }
  evict(): number {
    return 0;
  }
  transfer(register: OpenSpaceOptions["registerCache"]): void {
    if (this.#closed || this.#memory !== null)
      throw new TypeError("Workspace already transferred or closed");
    const memory = register(this);
    if (this.#budget === null) {
      memory.unregister?.();
      throw new TypeError("No workspace reservation");
    }
    const moved =
      memory.takeFrom === undefined
        ? memory.charge(METADATA_WORKSPACE_BYTES)
        : memory.takeFrom(this.#budget, METADATA_WORKSPACE_BYTES);
    if (!moved) {
      memory.unregister?.();
      throw new ReadDeniedError(METADATA_WORKSPACE_BYTES);
    }
    if (memory.takeFrom === undefined) this.#budget.free(METADATA_WORKSPACE_BYTES);
    this.#budget = null;
    this.#memory = memory;
  }
  /** The external workspace lock must already be held; this method never queues or waits. */
  async run<T>(
    phase: "source" | "merge" | "release" | "spool",
    work: () => Promise<T>,
  ): Promise<T> {
    if (this.#active || this.#closed) throw new TypeError("Workspace busy or closed");
    this.#active = true;
    this.#epoch++;
    try {
      const count = phase === "source" ? 3 : phase === "merge" ? 4 : 2;
      this.#frames = Array.from({ length: count }, () => new Uint8Array(METADATA_PAGE_BYTES));
      // Fixed header/view allowance, admitted with W before the frames exist. These views
      // belong to the phase, not a potentially escaped writer.
      this.#frameViews = this.#frames.map(viewOf);
      return await work();
    } finally {
      // Drop all references before a different phase allocates its frames: no old/new overlap.
      this.#frameViews.length = 0;
      this.#frames = EMPTY;
      this.#borrowed = 0;
      this.#readOnly = 0;
      this.#active = false;
    }
  }
  frame(index: number): Uint8Array {
    const frame = this.#frames[index];
    if (!this.#active || frame === undefined) throw new TypeError("No prepaid frame in this phase");
    return frame;
  }
  /** Owned output-frame view, valid only during the current phase. */
  frameView(index: number): DataView {
    const view = this.#frameViews[index];
    if (!this.#active || view === undefined) throw new TypeError("No prepaid view in this phase");
    return view;
  }
  claim(index: number, wholeBufferWrite = false): Uint8Array {
    const frame = this.frame(index);
    if (wholeBufferWrite && (this.#readOnly & (1 << index)) !== 0)
      throw new TypeError("A borrowed read frame cannot be used for a metadata writer");
    if (index === 0 || (this.#borrowed & (1 << index)) !== 0)
      throw new TypeError("Metadata frame is already borrowed");
    this.#borrowed |= 1 << index;
    return frame;
  }
  /** Up to six read-only metadata views can borrow the already charged read slab (S).
   * Caller holds PageCache.withReadSlab until this callback and all its cursors finish.
   * The views allocate no backing stores and can serve as in-place sort scratch; native writes
   * still require an owned whole frame.
   */
  async withReadFrames<T>(
    frames: readonly Uint8Array[],
    work: (first: number) => Promise<T>,
  ): Promise<T> {
    if (!this.#active || this.#readOnly !== 0 || frames.length > 6)
      throw new TypeError("Invalid read-frame scope");
    const first = this.#frames.length;
    for (let index = 0; index < frames.length; index++) {
      const frame = frames[index];
      if (frame === undefined) throw new TypeError("Missing read frame");
      if (
        frame.byteLength !== METADATA_PAGE_BYTES ||
        frame.buffer.byteLength !== 1024 * 1024 ||
        frame.byteOffset % 8 !== 0
      )
        throw new TypeError("Read frames must be views of the prepaid read slab");
      if (frame.buffer !== frames[0]?.buffer)
        throw new TypeError("Read frames must share one prepaid slab");
      for (let prior = 0; prior < index; prior++) {
        const before = frames[prior];
        if (
          before !== undefined &&
          before.byteOffset < frame.byteOffset + frame.length &&
          frame.byteOffset < before.byteOffset + before.length
        )
          throw new TypeError("Read frames overlap");
      }
    }
    this.#frames.push(...frames);
    const mask = ((1 << frames.length) - 1) << first;
    this.#readOnly = mask;
    let leaked: boolean;
    let value: T;
    try {
      value = await work(first);
    } finally {
      leaked = (this.#borrowed & mask) !== 0;
      this.#borrowed &= ~mask;
      this.#frames.length = first;
      this.#readOnly = 0;
    }
    if (leaked)
      throw new TypeError("Borrowed metadata cursors must close before returning the slab");
    return value;
  }

  /** Only immutable, successfully written control pages are cached. Payload pages still pass
   * through native reads/checksums; the cache has fixed capacity and no per-page directory. */
  readControl(handle: number, target: Uint8Array): boolean {
    this.assertFrame(target);
    const index = handle % 8;
    if (this.#controlHandles[index] !== handle) return false;
    target.fill(0);
    target.set(
      this.#controls.subarray(index * METADATA_SMALL_BYTES, (index + 1) * METADATA_SMALL_BYTES),
    );
    return true;
  }
  writtenControl(handle: number, frame: Uint8Array, kind: number, span: number): void {
    this.assertFrame(frame);
    const index = handle % 8;
    if (span !== METADATA_SMALL_BYTES || (kind !== DESCRIPTOR && kind !== FREE)) {
      if (this.#controlHandles[index] === handle) this.#controlHandles[index] = 0;
      return;
    }
    this.#controls.set(frame.subarray(0, METADATA_SMALL_BYTES), index * METADATA_SMALL_BYTES);
    this.#controlHandles[index] = handle;
  }

  compact(frame: Uint8Array): Uint8Array {
    this.assertFrame(frame);
    this.#io.set(frame.subarray(0, METADATA_SMALL_BYTES));
    return this.#io;
  }
  assertFrame(frame: Uint8Array): void {
    if (!this.#active || !this.#frames.includes(frame))
      throw new TypeError("Frame escaped its workspace phase");
  }
  returnFrame(frame: Uint8Array): void {
    this.assertFrame(frame);
    this.#borrowed &= ~(1 << this.#frames.indexOf(frame));
  }
  close(): void {
    if (this.#active) throw new TypeError("Cannot close an active workspace");
    if (this.#closed) return;
    this.#frames = EMPTY;
    this.#io = new Uint8Array(0);
    this.#controls = new Uint8Array(0);
    this.#controlHandles = new Float64Array(0);
    this.#budget?.free(METADATA_WORKSPACE_BYTES);
    this.#memory?.release(METADATA_WORKSPACE_BYTES);
    this.#memory?.unregister?.();
    this.#budget = null;
    this.#memory = null;
    this.#closed = true;
  }
}

const viewOf = (frame: Uint8Array) =>
  new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
function initialize(frame: Uint8Array, kind: number): void {
  frame.fill(0);
  viewOf(frame).setUint32(0, MAGIC, true);
  viewOf(frame).setUint32(4, kind, true);
}
function safe(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

export interface ChainDescriptor {
  readonly first: number;
  readonly last: number;
  readonly length: number;
  /** Intrusive queue link: the queue has scalar head/tail, never a resident descriptor array. */
  readonly next: number;
  readonly refs: number;
  /** Owner-defined scalar, e.g. a retiring generation. */
  readonly tag: number;
}

/** Handle 0 means absent. Other handles address page handle−1, with no per-page RAM index. */
export class MetadataJournal {
  #next = 1;
  #free = 0;
  #failed = false;
  constructor(
    readonly store: MetadataStore,
    readonly workspace: MetadataWorkspace,
  ) {}
  get failed(): boolean {
    return this.#failed;
  }
  get pages(): number {
    return this.#next - 1;
  }
  #live(): void {
    if (this.#failed) throw new StoreCorruptionError("Owner-private metadata journal failed");
  }
  async read(handle: number, frame: Uint8Array, expected?: number): Promise<void> {
    this.#live();
    this.workspace.assertFrame(frame);
    const pending = pauseIfDue();
    if (pending !== undefined) await pending;
    if (!safe(handle) || handle === 0 || handle >= this.#next)
      throw new RangeError("Invalid journal handle");
    try {
      if (!this.workspace.readControl(handle, frame)) await this.store.read(handle - 1, frame);
      const view = viewOf(frame),
        kind = view.getUint32(4, true),
        next = view.getFloat64(8, true),
        span = view.getUint32(20, true);
      if (
        view.getUint32(0, true) !== MAGIC ||
        (span !== METADATA_PAGE_BYTES && span !== METADATA_SMALL_BYTES) ||
        view.getUint32(span - 4, true) !== xxHash32(frame, 0, 0, span - 4) ||
        !safe(next) ||
        next >= this.#next ||
        (kind !== DESCRIPTOR && kind !== DATA && kind !== FREE) ||
        (expected !== undefined && kind !== expected) ||
        view.getUint32(16, true) > Math.min(METADATA_PAYLOAD_BYTES, span - HEADER - 4)
      )
        throw new StoreCorruptionError("Malformed owner-private metadata page");
    } catch (error) {
      this.#failed = true;
      throw error;
    }
  }
  async write(handle: number, frame: Uint8Array): Promise<void> {
    this.#live();
    this.workspace.assertFrame(frame);
    const pending = pauseIfDue();
    if (pending !== undefined) await pending;
    if (!safe(handle) || handle === 0 || handle >= this.#next)
      throw new RangeError("Invalid journal handle");
    const view = viewOf(frame);
    const used = view.getUint32(16, true);
    const span =
      HEADER + used + 4 <= METADATA_SMALL_BYTES ? METADATA_SMALL_BYTES : METADATA_PAGE_BYTES;
    view.setUint32(20, span, true);
    view.setUint32(span - 4, xxHash32(frame, 0, 0, span - 4), true);
    try {
      await this.store.write(
        handle - 1,
        span === METADATA_SMALL_BYTES ? this.workspace.compact(frame) : frame,
      );
      this.workspace.writtenControl(handle, frame, view.getUint32(4, true), span);
    } catch (error) {
      this.#failed = true;
      throw error;
    }
  }
  /** Control frame 0 is never borrowed by a data cursor or writer. */
  async allocate(): Promise<number> {
    this.#live();
    if (this.#free !== 0) {
      const handle = this.#free,
        frame = this.workspace.frame(0);
      await this.read(handle, frame);
      this.#free = viewOf(frame).getFloat64(8, true);
      return handle;
    }
    if (!Number.isSafeInteger((this.#next + 1) * METADATA_PAGE_BYTES))
      throw new RangeError("Metadata address overflow");
    return this.#next++;
  }
  async describe(handle: number): Promise<ChainDescriptor> {
    const frame = this.workspace.frame(0);
    await this.read(handle, frame, DESCRIPTOR);
    const view = viewOf(frame);
    const descriptor = {
      first: view.getFloat64(8, true),
      last: view.getFloat64(HEADER, true),
      length: view.getFloat64(HEADER + 8, true),
      next: view.getFloat64(HEADER + 16, true),
      refs: view.getFloat64(HEADER + 24, true),
      tag: view.getFloat64(HEADER + 32, true),
    };
    if (
      view.getUint32(16, true) !== DESCRIPTOR_BYTES ||
      !safe(descriptor.last) ||
      descriptor.last >= this.#next ||
      !safe(descriptor.length) ||
      !safe(descriptor.next) ||
      descriptor.next >= this.#next ||
      !safe(descriptor.refs) ||
      descriptor.refs === 0 ||
      !safe(descriptor.tag) ||
      (descriptor.length === 0
        ? descriptor.first !== 0 || descriptor.last !== 0
        : descriptor.first === 0 || descriptor.last === 0)
    )
      throw new StoreCorruptionError("Malformed owner-private chain descriptor");
    return descriptor;
  }
  async link(handle: number, next: number): Promise<void> {
    if (!safe(next) || next >= this.#next) throw new RangeError("Invalid queue link");
    await this.describe(handle);
    const frame = this.workspace.frame(0);
    viewOf(frame).setFloat64(HEADER + 16, next, true);
    await this.write(handle, frame);
  }
  async retain(handle: number): Promise<void> {
    const descriptor = await this.describe(handle),
      frame = this.workspace.frame(0);
    if (!safe(descriptor.refs + 1)) throw new RangeError("Metadata reference overflow");
    viewOf(frame).setFloat64(HEADER + 24, descriptor.refs + 1, true);
    await this.write(handle, frame);
  }
  async release(handle: number): Promise<void> {
    const descriptor = await this.describe(handle),
      frame = this.workspace.frame(0);
    if (descriptor.refs > 1) {
      viewOf(frame).setFloat64(HEADER + 24, descriptor.refs - 1, true);
      await this.write(handle, frame);
      return;
    }
    await this.discard(handle, descriptor.first, descriptor.last);
  }
  /** Trusted writer/descriptor ownership, not arbitrary caller coordinates. */
  async discard(handle: number, first: number, last: number): Promise<void> {
    const frame = this.workspace.frame(0);
    initialize(frame, FREE);
    viewOf(frame).setFloat64(8, first, true);
    // Splice the chain in O(1) I/O: no resident per-page free directory. Marking the descriptor
    // FREE also rejects a second release of a stale descriptor before its handle is reused.
    if (last !== 0) {
      await this.write(handle, frame);
      await this.read(last, frame, DATA);
      viewOf(frame).setFloat64(8, this.#free, true);
      await this.write(last, frame);
    } else {
      viewOf(frame).setFloat64(8, this.#free, true);
      await this.write(handle, frame);
    }
    this.#free = handle;
  }
  async writer(slot: number, tag = 0): Promise<ChainWriter> {
    if (slot === 0) throw new RangeError("Frame zero belongs to the journal");
    if (!safe(tag)) throw new RangeError("Invalid chain tag");
    const frame = this.workspace.claim(slot, true);
    try {
      const handle = await this.allocate();
      initialize(frame, DATA);
      return new ChainWriter(this, slot, this.workspace.epoch, handle, tag);
    } catch (error) {
      this.workspace.returnFrame(frame);
      throw error;
    }
  }
  async cursor(handle: number, slot: number, borrowed = false): Promise<ChainCursor> {
    if (slot === 0) throw new RangeError("Frame zero belongs to the journal");
    const frame = this.workspace.claim(slot);
    try {
      const descriptor = await this.describe(handle);
      if (!borrowed) await this.retain(handle);
      return new ChainCursor(this, slot, this.workspace.epoch, descriptor, handle, !borrowed);
    } catch (error) {
      this.workspace.returnFrame(frame);
      throw error;
    }
  }
}

/** One writer owns its slot until finish/abort. Caller never retains its frame after the phase. */
export class ChainWriter {
  #first = 0;
  #last = 0;
  #length = 0;
  #used = 0;
  #closed = false;
  constructor(
    readonly journal: MetadataJournal,
    readonly slot: number,
    readonly epoch: number,
    readonly handle: number,
    readonly tag: number,
  ) {}
  get frame(): Uint8Array {
    if (this.journal.workspace.epoch !== this.epoch)
      throw new TypeError("Frame escaped its workspace phase");
    return this.journal.workspace.frame(this.slot);
  }
  async append(bytes: Uint8Array, work?: TaskWork): Promise<void> {
    if (this.#closed) throw new TypeError("Chain writer is closed");
    this.journal.workspace.assertFrame(this.frame);
    let at = 0;
    while (at < bytes.length) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
      if (this.#last === 0) {
        this.#first = this.#last = await this.journal.allocate();
        const resumed = pauseIfDue(work);
        if (resumed !== undefined) await resumed;
      }
      if (this.#used === METADATA_PAYLOAD_BYTES) {
        const next = await this.journal.allocate();
        viewOf(this.frame).setFloat64(8, next, true);
        await this.#flush();
        this.#last = next;
        this.#used = 0;
        initialize(this.frame, DATA);
        const resumed = pauseIfDue(work);
        if (resumed !== undefined) await resumed;
      }
      const take = Math.min(bytes.length - at, METADATA_PAYLOAD_BYTES - this.#used);
      this.frame.set(bytes.subarray(at, at + take), HEADER + this.#used);
      this.#used += take;
      this.#length += take;
      at += take;
    }
  }
  /** Fast path for one canonical pair in an already allocated page. False asks the caller to
   * use append(), which allocates/flushes pages with shared-clock and resume checks.
   */
  tryAppendPair(offset: number, length: number): boolean {
    if (this.#closed) throw new TypeError("Chain writer is closed");
    const frame = this.frame;
    this.journal.workspace.assertFrame(frame);
    if (this.#last === 0 || this.#used + 16 > METADATA_PAYLOAD_BYTES) return false;
    const view = this.journal.workspace.frameView(this.slot);
    view.setFloat64(HEADER + this.#used, offset, true);
    view.setFloat64(HEADER + this.#used + 8, length, true);
    this.#used += 16;
    this.#length += 16;
    return true;
  }
  async #flush(): Promise<void> {
    viewOf(this.frame).setUint32(16, this.#used, true);
    await this.journal.write(this.#last, this.frame);
  }
  async finish(): Promise<number> {
    if (this.#closed) throw new TypeError("Chain writer is closed");
    this.journal.workspace.assertFrame(this.frame);
    try {
      if (this.#last !== 0) await this.#flush();
      const frame = this.journal.workspace.frame(0);
      initialize(frame, DESCRIPTOR);
      const view = viewOf(frame);
      view.setFloat64(8, this.#first, true);
      view.setUint32(16, DESCRIPTOR_BYTES, true);
      view.setFloat64(HEADER, this.#last, true);
      view.setFloat64(HEADER + 8, this.#length, true);
      view.setFloat64(HEADER + 24, 1, true);
      view.setFloat64(HEADER + 32, this.tag, true);
      await this.journal.write(this.handle, frame);
      return this.handle;
    } finally {
      this.#closed = true;
      this.journal.workspace.returnFrame(this.frame);
    }
  }

  /** Cancellation is checked by the producer; cleanup itself completes without that signal. */
  async abort(): Promise<void> {
    if (this.#closed) return;
    this.journal.workspace.assertFrame(this.frame);
    try {
      if (this.#last !== 0) await this.#flush();
      await this.journal.discard(this.handle, this.#first, this.#last);
    } finally {
      this.#closed = true;
      this.journal.workspace.returnFrame(this.frame);
    }
  }
}

/** Borrowed page slices stay valid only until the next next() call, never past the phase. */
export class ChainCursor {
  #next: number;
  #remaining: number;
  #pages = 0;
  #closed = false;
  constructor(
    readonly journal: MetadataJournal,
    readonly slot: number,
    readonly epoch: number,
    readonly descriptor: ChainDescriptor,
    readonly handle: number,
    readonly retained = true,
  ) {
    this.#next = descriptor.first;
    this.#remaining = descriptor.length;
  }
  get frame(): Uint8Array {
    if (this.journal.workspace.epoch !== this.epoch)
      throw new TypeError("Frame escaped its workspace phase");
    return this.journal.workspace.frame(this.slot);
  }
  async next(work?: TaskWork): Promise<Uint8Array | null> {
    if (this.#closed) return null;
    this.journal.workspace.assertFrame(this.frame);
    const pending = pauseIfDue(work);
    if (pending !== undefined) await pending;
    if (this.#next === 0) {
      if (this.#remaining !== 0)
        throw new StoreCorruptionError("Short owner-private metadata chain");
      await this.close();
      return null;
    }
    if (++this.#pages > Math.ceil(this.descriptor.length / METADATA_PAYLOAD_BYTES))
      throw new StoreCorruptionError("Cyclic owner-private metadata chain");
    const handle = this.#next;
    await this.journal.read(handle, this.frame, DATA);
    const resumed = pauseIfDue(work);
    if (resumed !== undefined) await resumed;
    const view = viewOf(this.frame),
      used = view.getUint32(16, true);
    this.#next = view.getFloat64(8, true);
    if (
      used === 0 ||
      used > this.#remaining ||
      (this.#next !== 0 && used !== METADATA_PAYLOAD_BYTES) ||
      (this.#remaining === used) !== (this.#next === 0) ||
      (this.#next === 0 && handle !== this.descriptor.last)
    )
      throw new StoreCorruptionError("Malformed owner-private metadata chain");
    this.#remaining -= used;
    return this.frame.subarray(HEADER, HEADER + used);
  }
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    try {
      if (this.retained) await this.journal.release(this.handle);
    } finally {
      this.journal.workspace.returnFrame(this.frame);
    }
  }
}
