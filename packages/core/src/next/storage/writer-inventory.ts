/** Sealed ownership uses the existing native inventory journal, never an extent-sized array.
 * Chunks retain seal ordinals. A zero length is a private discarded slot, never retirement input.
 * All methods ending Inside require the native metadata turn; tail append is synchronous.
 */
import { pauseIfDue, taskClock, type TaskWork } from "../task.js";
import type { ExtentRef } from "./extents.js";
import { DATA_ALIGN, METADATA_SMALL_BYTES, StoreCorruptionError } from "./contract.js";
import type { MetadataJournal } from "./metadata-journal.js";
import { clearCapture, type AuditCapture } from "./audit-capture.js";
import { ExtentPairWriter, sortExtents, validateExtentPair } from "./metadata-extents.js";
import { RETIREMENT_OWNER_BYTES, RETIREMENT_CURSOR_BYTES } from "./retirement-inventory.js";
import { disposeSnapshot } from "./extent-snapshot.js";
import { pairPhase, type MetadataWork } from "./metadata-work.js";
export const WRITER_INVENTORY_WINDOW = 64;
export const WRITER_INVENTORY_OWNER_BYTES = 8192;
/** Tail objects, cached legacy slots, a snapshot overlap, packed flush and cleanup batches. */
export const writerInventoryBytes = (capacity: number): number =>
  WRITER_INVENTORY_OWNER_BYTES + capacity * 256;
/** Complete publication overlap: preferred native owner, two immutable delta owners,
 * bounded compatibility copies, and simultaneous addition/release cursor windows. */
export const WRITER_OWNERSHIP_STEP_BYTES =
  writerInventoryBytes(WRITER_INVENTORY_WINDOW) +
  2 * (RETIREMENT_OWNER_BYTES + 128 + WRITER_INVENTORY_WINDOW * 128) +
  2 * RETIREMENT_CURSOR_BYTES;

export class WriterInventoryRequiredError extends TypeError {
  readonly code = "MINNOW_WRITER_INVENTORY_REQUIRED";
  constructor() {
    super("Use the native writer inventory cursor after sealed ownership spills");
    this.name = "WriterInventoryRequiredError";
  }
}
export type WriterExtentLocation =
  | { readonly kind: "tail"; readonly index: number; readonly extent: ExtentRef }
  | {
      readonly kind: "chunk";
      readonly handle: number;
      readonly index: number;
      readonly ordinal: number;
      readonly extent: ExtentRef;
    };
export class WriterInventory {
  readonly #chunks: AuditCapture = { head: 0, tail: 0 };
  #tail: Array<ExtentRef | undefined>;
  #legacy: ExtentRef[] = [];
  #slots = 0;
  #ordinal = 0;
  #count = 0;
  #bytes = 0;
  // A fixed-size seek hint, not a borrowed cursor or history index. Chunks retain
  // their ordinals until close (discard only tombstones a slot). Revisit the last
  // validated chunk for successive cuts; older cuts still start from the head.
  #lastChunkFirst = 1;
  #captureHandle = 0;
  #captureFirst = 1;
  #closed = false;
  #closing: Promise<void> | undefined;
  constructor(
    readonly journal: MetadataJournal,
    readonly capacity: number,
    readonly funding: { close(): void },
  ) {
    this.#tail = new Array<ExtentRef | undefined>(capacity);
  }
  /** Actual resident descriptor slots, including the borrowed compatibility cache. */
  get residentSlots(): number {
    return this.#tail.length + this.#legacy.length;
  }
  get count(): number {
    return this.#count;
  }
  get ordinal(): number {
    return this.#ordinal;
  }
  get bytes(): number {
    return this.#bytes;
  }
  get needsFlush(): boolean {
    return this.#slots === this.capacity;
  }
  /** Borrowed, bounded compatibility view. It is not an unbounded materialization API. */
  get resident(): readonly ExtentRef[] {
    if (this.#chunks.head !== 0) throw new WriterInventoryRequiredError();
    return this.#legacy;
  }
  append(extent: ExtentRef, allocatedBytes: number): void {
    if (this.#closed || this.needsFlush)
      throw new TypeError("Writer ownership window is unavailable");
    validateExtentPair(extent.offset, extent.length);
    if (!Number.isSafeInteger(this.#ordinal + 1))
      throw new RangeError("Writer seal ordinal overflow");
    const owned = Object.freeze({ offset: extent.offset, length: extent.length });
    this.#tail[this.#slots++] = owned;
    this.#legacy.push(owned);
    this.#ordinal++;
    this.#count++;
    this.#bytes += allocatedBytes;
  }
  async flushInside(work?: TaskWork): Promise<void> {
    if (this.#closed) throw new TypeError("Closed writer ownership");
    if (this.#slots === 0) return;
    const pending: AuditCapture = { head: 0, tail: 0 };
    try {
      await this.journal.workspace.run("spool", async () => {
        const first = this.#ordinal - this.#slots + 1,
          // The journal already supports compact pages. Keep this scratch stream
          // in that format rather than hashing/cloning a mostly empty large page.
          perChunk = Math.floor((METADATA_SMALL_BYTES - 28) / 16);
        for (let start = 0; start < this.#slots; start += perChunk) {
          work?.signal?.throwIfAborted();
          const count = Math.min(perChunk, this.#slots - start),
            writer = await this.journal.writer(1, first + start),
            bytes = new Uint8Array(count * 16),
            view = new DataView(bytes.buffer);
          for (let i = 0; i < count; i++) {
            const extent = this.#tail[start + i];
            view.setFloat64(i * 16, extent?.offset ?? 0, true);
            view.setFloat64(i * 16 + 8, extent?.length ?? 0, true);
          }
          let handle = 0;
          try {
            await writer.append(bytes, work);
            handle = await writer.finish();
            work?.signal?.throwIfAborted();
            if (pending.tail !== 0) await this.journal.link(pending.tail, handle);
            else pending.head = handle;
            pending.tail = handle;
            handle = 0;
          } catch (error) {
            await writer.abort().catch(() => undefined);
            if (handle !== 0) await this.journal.release(handle).catch(() => undefined);
            throw error;
          }
        }
        work?.signal?.throwIfAborted();
        if (this.#chunks.tail !== 0) await this.journal.link(this.#chunks.tail, pending.head);
        else this.#chunks.head = pending.head;
        this.#chunks.tail = pending.tail;
        this.#lastChunkFirst = first + Math.floor((this.#slots - 1) / perChunk) * perChunk;
        this.#tail.fill(undefined);
        this.#legacy.length = 0;
        this.#slots = 0;
      });
    } catch (error) {
      // The frame phase must end before the shared cleanup helper enters its own.
      await clearCapture(this.journal, pending).catch(() => undefined);
      throw error;
    }
  }
  /** No allocation or source borrow. Only the current fixed tail can be copied without IO. */
  canCopyTail(since: number): boolean {
    if (this.#closed) throw new TypeError("Closed writer ownership");
    if (!Number.isSafeInteger(since) || since < 0 || since > this.#ordinal)
      throw new RangeError("Invalid writer ownership cut");
    return since >= this.#ordinal - this.#slots;
  }
  /** Caller prepays the independent inventory owner; no writer references escape. A due
   * clock defers to the sliced journal route instead of finishing a long synchronous sort. */
  copyTail(since: number, work: TaskWork): { bytes: Uint8Array; count: number } | undefined {
    if (!this.canCopyTail(since)) return undefined;
    work.signal?.throwIfAborted();
    const clock = work.clock ?? taskClock;
    if (clock.due()) return undefined;
    const packed = new Uint8Array(this.capacity * 16),
      view = new DataView(packed.buffer),
      first = this.#ordinal - this.#slots + 1;
    let count = 0,
      steps = 0;
    for (let i = Math.max(0, since - first + 1); i < this.#slots; i++) {
      if ((steps++ & 7) === 0) {
        work.signal?.throwIfAborted();
        if (clock.due()) return undefined;
      }
      const extent = this.#tail[i];
      if (extent === undefined) continue;
      view.setFloat64(count * 16, extent.offset, true);
      view.setFloat64(count * 16 + 8, extent.length, true);
      count++;
    }
    // At most64 pairs, in the prepaid packed buffer. Physical allocation order
    // may differ from seal order; no native Array.sort or unbounded work escapes.
    for (let i = 1; i < count; i++) {
      const offset = view.getFloat64(i * 16, true),
        length = view.getFloat64(i * 16 + 8, true);
      let j = i;
      while (j > 0 && view.getFloat64((j - 1) * 16, true) > offset) {
        if ((steps++ & 63) === 0) {
          work.signal?.throwIfAborted();
          if (clock.due()) return undefined;
        }
        view.setFloat64(j * 16, view.getFloat64((j - 1) * 16, true), true);
        view.setFloat64(j * 16 + 8, view.getFloat64((j - 1) * 16 + 8, true), true);
        j--;
      }
      view.setFloat64(j * 16, offset, true);
      view.setFloat64(j * 16 + 8, length, true);
    }
    // The immutable copy includes the entire current suffix. Future increasing
    // cuts can start at the last already-validated flushed chunk, even if many
    // prior captures stayed resident and never opened the journal.
    this.#captureHandle = this.#chunks.tail;
    this.#captureFirst = this.#lastChunkFirst;
    return { bytes: packed.subarray(0, count * 16), count };
  }
  /** A copied tail bounds overlap; later appends cannot change this capture's ordinal cut. */
  async captureInside(
    io: MetadataWork,
    since = 0,
    work?: TaskWork,
  ): Promise<{ readonly handle: number; readonly count: number }> {
    if (this.#closed) throw new TypeError("Closed writer ownership");
    if (!Number.isSafeInteger(since) || since < 0 || since > this.#ordinal)
      throw new RangeError("Invalid writer ownership cut");
    const tail = this.#tail.slice(0, this.#slots),
      tailFirst = this.#ordinal - this.#slots + 1;
    let raw = 0,
      ordered = 0;
    let count = 0,
      orderedInput = true,
      previousEnd = -1;
    let captureHandle = this.#captureHandle,
      captureFirst = this.#captureFirst;
    try {
      raw = await pairPhase(this.journal, io, "merge", async (slots) => {
        const writer = await this.journal.writer(slots.output),
          output = new ExtentPairWriter(writer, false);
        const resume = this.#captureHandle !== 0 && since >= this.#captureFirst - 1;
        // The entire suffix may still be in the bounded tail. No old chunk is
        // relevant in that case, even if earlier ownership has spilled.
        const tailOnly = since >= tailFirst - 1;
        if (tailOnly) {
          captureHandle = this.#chunks.tail;
          captureFirst = this.#lastChunkFirst;
        }
        let next = tailOnly ? 0 : resume ? this.#captureHandle : this.#chunks.head,
          expected = tailOnly ? tailFirst : resume ? this.#captureFirst : 1;
        const append = (offset: number, length: number, ordinal: number): void | Promise<void> => {
          if (length === 0) return;
          validateExtentPair(offset, length);
          if (ordinal <= since) return;
          if (offset < previousEnd) orderedInput = false;
          previousEnd = offset + Math.ceil(length / DATA_ALIGN) * DATA_ALIGN;
          count++;
          return output.append(offset, length, work);
        };
        try {
          while (next !== 0) {
            const cursor = await this.journal.cursor(next, slots.left, true);
            try {
              const d = cursor.descriptor;
              if (
                d.tag !== expected ||
                d.length === 0 ||
                d.length > this.capacity * 16 ||
                d.length % 16 !== 0 ||
                d.first !== d.last
              )
                throw new StoreCorruptionError("Malformed sealed-writer inventory chunk");
              const bytes = await cursor.next(work);
              if (bytes?.length !== d.length || cursor.position.remaining !== 0)
                throw new StoreCorruptionError("Short sealed-writer inventory chunk");
              const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
              for (let i = 0; i < bytes.length / 16; i++) {
                const pending = pauseIfDue(work);
                if (pending !== undefined) await pending;
                const writing = append(
                  view.getFloat64(i * 16, true),
                  view.getFloat64(i * 16 + 8, true),
                  expected++,
                );
                if (writing !== undefined) await writing;
              }
              captureHandle = next;
              captureFirst = d.tag;
              next = d.next;
            } finally {
              await cursor.close();
            }
          }
          if (expected !== tailFirst) throw new StoreCorruptionError("Sealed-writer ordinal gap");
          for (let i = 0; i < tail.length; i++) {
            const extent = tail[i];
            if (extent !== undefined) {
              const writing = append(extent.offset, extent.length, tailFirst + i);
              if (writing !== undefined) await writing;
            }
          }
          return await writer.finish();
        } catch (error) {
          await writer.abort().catch(() => undefined);
          throw error;
        }
      });
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- append updates this flag inside the awaited metadata phase
      if (orderedInput) {
        // Normal append order is already a strict physical order. The scalar
        // fence check above also rules out duplicates/overlap before adoption.
        ordered = raw;
        raw = 0;
      } else {
        ordered = await pairPhase(this.journal, io, "merge", (slots) =>
          sortExtents(this.journal, raw, "refuse", work, slots),
        );
      }
      const handle = ordered;
      ordered = 0;
      // Commit the hint only after the complete capture and sort succeeded.
      this.#captureHandle = captureHandle;
      this.#captureFirst = captureFirst;
      return { handle, count };
    } finally {
      if (raw !== 0) await disposeSnapshot(this.journal, raw);
      if (ordered !== 0) await disposeSnapshot(this.journal, ordered);
    }
  }
  async findInside(extent: ExtentRef, work?: TaskWork): Promise<WriterExtentLocation | undefined> {
    if (this.#closed) throw new TypeError("Closed writer ownership");
    for (let i = 0; i < this.#slots; i++) {
      const owned = this.#tail[i];
      if (owned?.offset === extent.offset && owned.length === extent.length)
        return { kind: "tail", index: i, extent: owned };
    }
    return this.journal.workspace.run("spool", async () => {
      let next = this.#chunks.head,
        expected = 1;
      while (next !== 0) {
        const cursor = await this.journal.cursor(next, 1, true);
        try {
          const d = cursor.descriptor;
          if (
            d.tag !== expected ||
            d.first !== d.last ||
            d.length === 0 ||
            d.length > this.capacity * 16 ||
            d.length % 16 !== 0
          )
            throw new StoreCorruptionError("Malformed sealed-writer lookup chunk");
          const bytes = await cursor.next(work);
          if (bytes?.length !== d.length)
            throw new StoreCorruptionError("Short sealed-writer lookup chunk");
          const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
          for (let i = 0; i < bytes.length / 16; i++) {
            const pending = pauseIfDue(work);
            if (pending !== undefined) await pending;
            if (
              view.getFloat64(i * 16, true) === extent.offset &&
              view.getFloat64(i * 16 + 8, true) === extent.length
            )
              return {
                kind: "chunk",
                handle: next,
                index: i,
                ordinal: d.tag + i,
                extent: { offset: extent.offset, length: extent.length },
              };
          }
          expected += d.length / 16;
          if (expected > this.#ordinal + 1)
            throw new StoreCorruptionError("Sealed-writer lookup ordinal overflow");
          next = d.next;
        } finally {
          await cursor.close();
        }
      }
      return undefined;
    });
  }
  async clearInside(location: WriterExtentLocation, allocatedBytes: number): Promise<void> {
    if (this.#closed) throw new TypeError("Closed writer ownership");
    if (location.kind === "tail") {
      if (this.#tail[location.index] !== location.extent)
        throw new TypeError("Writer ownership changed before discard");
      this.#tail[location.index] = undefined;
      const legacy = this.#legacy.indexOf(location.extent);
      if (legacy < 0) throw new TypeError("Missing resident writer ownership");
      this.#legacy.splice(legacy, 1);
    } else {
      await this.journal.workspace.run("spool", async () => {
        const cursor = await this.journal.cursor(location.handle, 1, true);
        try {
          const bytes = await cursor.next();
          if (
            bytes === null ||
            cursor.descriptor.first !== cursor.descriptor.last ||
            cursor.descriptor.tag + location.index !== location.ordinal ||
            location.index * 16 + 16 > bytes.length
          )
            throw new StoreCorruptionError("Changed sealed-writer discard location");
          const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
          if (
            view.getFloat64(location.index * 16, true) !== location.extent.offset ||
            view.getFloat64(location.index * 16 + 8, true) !== location.extent.length
          )
            throw new StoreCorruptionError("Changed sealed-writer discard ownership");
          view.setFloat64(location.index * 16 + 8, 0, true);
          await this.journal.write(cursor.descriptor.first, cursor.frame);
        } finally {
          await cursor.close();
        }
      });
    }
    this.#count--;
    this.#bytes -= allocatedBytes;
  }
  closeInside(): Promise<void> {
    if (this.#closing !== undefined) return this.#closing;
    let resolve!: () => void, reject!: (error: unknown) => void;
    const closing = new Promise<void>((yes, no) => {
      resolve = yes;
      reject = no;
    });
    this.#closing = closing;
    this.#closed = true;
    const drain = async () => {
      try {
        await clearCapture(this.journal, this.#chunks);
      } finally {
        this.#tail = [];
        this.#legacy.length = 0;
        this.#count = this.#bytes = this.#slots = 0;
        this.#captureHandle = 0;
        this.#captureFirst = 1;
        this.#lastChunkFirst = 1;
        this.funding.close();
      }
    };
    void drain().then(resolve, reject);
    return closing;
  }
}
