/** Sealed ownership uses the existing native inventory journal, never an extent-sized array.
 * Chunks retain seal ordinals. A zero length is a private discarded slot, never retirement input.
 * All methods ending Inside require the native metadata turn; tail append is synchronous.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import type { ExtentRef } from "./extents.js";
import { StoreCorruptionError } from "./contract.js";
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
    await this.journal.workspace.run("spool", async () => {
      const first = this.#ordinal - this.#slots + 1,
        writer = await this.journal.writer(1, first),
        bytes = new Uint8Array(this.#slots * 16),
        view = new DataView(bytes.buffer);
      for (let i = 0; i < this.#slots; i++) {
        const extent = this.#tail[i];
        view.setFloat64(i * 16, extent?.offset ?? 0, true);
        view.setFloat64(i * 16 + 8, extent?.length ?? 0, true);
      }
      let handle = 0;
      try {
        await writer.append(bytes, work);
        handle = await writer.finish();
        if (this.#chunks.tail !== 0) await this.journal.link(this.#chunks.tail, handle);
        else this.#chunks.head = handle;
        this.#chunks.tail = handle;
        handle = 0;
        this.#tail.fill(undefined);
        this.#legacy.length = 0;
        this.#slots = 0;
      } catch (error) {
        await writer.abort().catch(() => undefined);
        if (handle !== 0) await this.journal.release(handle).catch(() => undefined);
        throw error;
      }
    });
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
    let count = 0;
    try {
      raw = await pairPhase(this.journal, io, "merge", async (slots) => {
        const writer = await this.journal.writer(slots.output),
          output = new ExtentPairWriter(writer, false);
        let next = this.#chunks.head,
          expected = 1;
        const append = (offset: number, length: number, ordinal: number): void | Promise<void> => {
          if (length === 0) return;
          validateExtentPair(offset, length);
          if (ordinal <= since) return;
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
      ordered = await pairPhase(this.journal, io, "merge", (slots) =>
        sortExtents(this.journal, raw, "refuse", work, slots),
      );
      const handle = ordered;
      ordered = 0;
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
        this.funding.close();
      }
    };
    void drain().then(resolve, reject);
    return closing;
  }
}
