/** Verified portable bytes live only in writer-owned, unpublished extents. No table-sized buffer. */
import { validateSnapshotValue } from "./snapshot-validate-value.js";
import type { ColumnDef } from "../access/catalog.js";
import { SnapshotBudgetError, SnapshotFormatError, type SnapshotWork } from "../format/snapshot.js";
import type {
  BlobRef,
  ByteBudget,
  ExtentBuilder,
  ExtentReader,
  ExtentWriter,
  ReadLease,
} from "../storage/extents.js";
import { pauseIfDue } from "../task.js";
import { decodeSnapshotColumnHeader, snapshotValueWidth } from "./snapshot-column.js";

export function snapshotTake(budget: ByteBudget, bytes: number): void {
  if (!budget.allocate(bytes)) throw new SnapshotBudgetError(bytes);
}
export function snapshotCheck(ok: boolean, message: string): asserts ok {
  if (!ok) throw new SnapshotFormatError(message);
}
export async function snapshotPace(work: SnapshotWork): Promise<void> {
  const pending = pauseIfDue(work);
  if (pending !== undefined) await pending;
}
/** Dynamic bookkeeping is funded before array/object construction and released as one scope. */
export class SnapshotAccount implements ByteBudget {
  used = 0;
  constructor(readonly budget: ByteBudget) {}
  allocate(bytes: number): boolean {
    if (!this.budget.allocate(bytes)) return false;
    this.used += bytes;
    return true;
  }
  take(bytes: number): void {
    snapshotTake(this, bytes);
  }
  free(bytes: number): void {
    if (bytes > this.used) throw new TypeError("Snapshot accounting underflow");
    this.used -= bytes;
    this.budget.free(bytes);
  }
  close(): void {
    this.free(this.used);
  }
}

/** One column block, not one table. Frame refs and row coordinates are explicitly charged. */
export class SnapshotStagedColumn {
  readonly account: SnapshotAccount;
  readonly offsets: Float64Array;
  /** UINT32_MAX means NULL. Fixed widths exclude the validity byte. */
  readonly lengths: Uint32Array;
  readonly builder: ExtentBuilder;
  #parts = 0;
  #bytes = 0;
  #sealed: import("../storage/extents.js").WrittenExtent | null = null;
  constructor(
    readonly column: ColumnDef,
    readonly rows: number,
    readonly expectedBytes: number,
    readonly writer: ExtentWriter,
    readonly budget: ByteBudget,
    readonly work: SnapshotWork,
  ) {
    snapshotCheck(
      rows >= 1 &&
        rows <= 256 &&
        expectedBytes >= 16 &&
        expectedBytes <= 16 + rows * (64 * 1024 * 1024 + 10),
      "Invalid staged column bounds",
    );
    this.account = new SnapshotAccount(budget);
    try {
      this.account.take(1024 + rows * 12);
      this.offsets = new Float64Array(rows);
      this.lengths = new Uint32Array(rows);
      this.builder = writer.open(expectedBytes);
    } catch (error) {
      this.account.close();
      throw error;
    }
  }
  async append(bytes: Uint8Array): Promise<void> {
    snapshotCheck(
      this.#sealed === null && this.#bytes + bytes.length <= this.expectedBytes,
      "Column exceeds declared bytes",
    );
    // Pending and sealed refs, builder's array slots and our traversal wrappers coexist.
    this.account.take(256);
    await this.builder.append([bytes]);
    this.#parts++;
    this.#bytes += bytes.length;
    await snapshotPace(this.work);
  }
  get refs(): readonly BlobRef[] {
    if (this.#sealed === null) throw new TypeError("Unsealed portable column");
    return this.#sealed.blobs;
  }
  async finish(space: ExtentReader, scratch: Uint8Array): Promise<void> {
    snapshotCheck(
      this.#bytes === this.expectedBytes && this.#parts > 0,
      "Incomplete portable column",
    );
    this.#sealed = await this.builder.seal();
    const reader = new SnapshotColumnBytes(this, space);
    try {
      await reader.fill(0, scratch.subarray(0, 16));
      snapshotCheck(
        decodeSnapshotColumnHeader(this.column.type, scratch.subarray(0, 16)) === this.rows,
        "Column header/frame rows disagree",
      );
      let at = 16;
      const width = snapshotValueWidth(this.column.type),
        view = new DataView(scratch.buffer, scratch.byteOffset, scratch.byteLength);
      for (let row = 0; row < this.rows; row++) {
        await snapshotPace(this.work);
        await reader.fill(at++, scratch.subarray(0, 1));
        const valid = scratch[0];
        snapshotCheck(valid === 0 || valid === 1, "Invalid portable validity/opcode");
        snapshotCheck(
          valid !== 0 || this.column.nullable,
          "NULL in a non-nullable portable column",
        );
        if (valid === 0) {
          this.lengths[row] = 0xffffffff;
          continue;
        }
        if (width > 0) {
          this.offsets[row] = at;
          this.lengths[row] = width;
          await reader.fill(at, scratch.subarray(0, width));
          if (this.column.type.kind === "bool")
            snapshotCheck(scratch[0] === 0 || scratch[0] === 1, "Invalid portable boolean");
          if (this.column.type.kind === "int32" && this.column.type.bits === 16)
            snapshotCheck(
              view.getInt32(0, true) >= -32768 && view.getInt32(0, true) <= 32767,
              "Portable SMALLINT out of range",
            );
          if (this.column.type.kind === "decimal") {
            snapshotCheck(
              (scratch[18] ?? 255) <= 3 && view.getUint16(16, true) <= 16383,
              "Invalid portable decimal side lanes",
            );
            if (this.column.type.scale !== null)
              snapshotCheck(
                view.getUint16(16, true) === Math.max(0, this.column.type.scale),
                "Portable decimal scale mismatch",
              );
          }
          at += width;
        } else {
          await reader.fill(at, scratch.subarray(0, 4));
          at += 4;
          const length = view.getUint32(0, true);
          snapshotCheck(length <= 64 * 1024 * 1024, "Portable value exceeds native maximum");
          this.offsets[row] = at;
          this.lengths[row] = length;
          at += length;
          await reader.fill(at, scratch.subarray(0, 5));
          at += 5;
          snapshotCheck(
            scratch[0] === 2 && view.getUint32(1, true) === 0,
            "Invalid portable dictionary reference",
          );
        }
      }
      snapshotCheck(at === this.expectedBytes, "Trailing or truncated portable column values");
    } finally {
      reader.close();
    }
    if (snapshotValueWidth(this.column.type) === 0)
      for (let row = 0; row < this.rows; row++) {
        const length = this.lengths[row] ?? 0xffffffff;
        if (length !== 0xffffffff)
          await validateSnapshotValue(this, space, this.offsets[row] ?? 0, length);
      }
  }
  async discard(): Promise<void> {
    if (this.#sealed !== null) await this.writer.discard(this.#sealed.extent);
    this.#sealed = null;
    this.account.close();
  }
  close(): void {
    this.account.close();
  }
}

/** Keeps at most one verified, non-retained staging part borrowed. */
export class SnapshotColumnBytes {
  #lease: ReadLease | null = null;
  #part = -1;
  #start = 0;
  constructor(
    readonly column: SnapshotStagedColumn,
    readonly space: ExtentReader,
  ) {}
  async #at(offset: number): Promise<Uint8Array> {
    snapshotCheck(
      Number.isSafeInteger(offset) && offset >= 0 && offset < this.column.expectedBytes,
      "Portable byte offset out of bounds",
    );
    let start = 0,
      part = 0;
    for (const ref of this.column.refs) {
      if (offset < start + ref.length) break;
      start += ref.length;
      part++;
    }
    if (this.#part !== part) {
      this.close();
      const ref = this.column.refs[part];
      snapshotCheck(ref !== undefined, "Missing staged column part");
      this.#lease = await this.space.read([ref], {
        budget: this.column.budget,
        retain: false,
        task: this.column.work,
        ...(this.column.work.signal === undefined ? {} : { signal: this.column.work.signal }),
      });
      this.#part = part;
      this.#start = start;
    }
    const bytes = this.#lease?.bytes[0];
    snapshotCheck(bytes !== undefined, "Missing staged bytes");
    return bytes.subarray(offset - this.#start);
  }
  async fill(offset: number, into: Uint8Array): Promise<void> {
    let copied = 0;
    for await (const part of this.parts(offset, into.length)) {
      into.set(part, copied);
      copied += part.length;
    }
  }
  async *parts(offset: number, length: number): AsyncGenerator<Uint8Array> {
    snapshotCheck(
      Number.isSafeInteger(length) && length >= 0 && offset + length <= this.column.expectedBytes,
      "Truncated portable value",
    );
    for (let at = offset; at < offset + length;) {
      await snapshotPace(this.column.work);
      const bytes = await this.#at(at),
        take = Math.min(bytes.length, offset + length - at);
      yield bytes.subarray(0, take);
      at += take;
    }
  }
  close(): void {
    this.#lease?.release();
    this.#lease = null;
    this.#part = -1;
  }
}
