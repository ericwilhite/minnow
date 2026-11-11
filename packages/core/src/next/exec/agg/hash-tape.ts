/** A hash tape has an on-disk directory, not a RAM array per input page. Directory
 * records use the same native spill codec (six float64 integer fields). Reversing
 * directory records once makes a constant-memory backwards traversal yield input
 * pages forwards. Payload pages themselves are never rewritten during sealing.
 */
import { SpillCorruptionError, type SpillPageRef } from "../../spill/contract.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Batch, FlatVector } from "../../vector/vector.js";
import { SpilledRun, type SpillFiles } from "../spill-pages.js";
import type { SpillIo } from "../spill-pool.js";

const F64: PhysicalType = { kind: "float64" };
const TYPES = Array.from({ length: 6 }, () => F64);
const METADATA_BYTES = 4096;
const DIRECTORY_PAGE_BYTES = 224;

/** One shared bounded encoder per aggregation, reused sequentially by all its tapes. */
export class TapeDirectory {
  #values: Float64Array | null = null;
  #columns: FlatVector[] = [];
  #charge: ByteCharge | null = null;
  #io: SpillIo | null = null;
  constructor(io: SpillIo, charge: ByteCharge) {
    this.attach(io, charge);
  }
  attach(io: SpillIo, charge: ByteCharge): void {
    if (this.#io !== null) throw new Error("Directory already attached");
    if (!charge.allocate(METADATA_BYTES)) throw new Error("Tape directory was not admitted");
    this.#charge = charge;
    this.#io = io;
    this.#values = new Float64Array(6);
    for (let i = 0; i < 6; i++)
      this.#columns.push({
        form: "flat",
        type: F64,
        length: 1,
        validity: null,
        data: { layout: "f64", values: this.#values.subarray(i, i + 1) },
      });
  }
  get io(): SpillIo {
    if (!this.#io) throw new Error("Directory outside an admitted pass");
    return this.#io;
  }
  encode(previous: SpillPageRef | null, data: SpillPageRef): Uint8Array {
    const values = this.#values;
    if (!values) throw new Error("Directory outside admission");
    values[0] = previous?.offset ?? -1;
    values[1] = previous?.length ?? 0;
    values[2] = previous?.checksum ?? 0;
    values[3] = data.offset;
    values[4] = data.length;
    values[5] = data.checksum;
    const io = this.io,
      page = io.pages.encode(
        { queryId: io.context.queryId, run: 0, partition: 0, sequence: 0 },
        this.#columns,
        1,
      );
    if (page?.length !== DIRECTORY_PAGE_BYTES)
      throw new Error("Tape directory page was not admitted");
    return page;
  }
  async read(
    run: SpilledRun,
    ref: SpillPageRef,
    maxDataPage: number,
  ): Promise<{ previous: SpillPageRef | null; data: SpillPageRef }> {
    if (ref.length !== DIRECTORY_PAGE_BYTES)
      throw new SpillCorruptionError("Invalid tape directory length");
    const io = this.io;
    io.checkAborted();
    const paced = io.pace?.();
    if (paced) await paced;
    const page = await io.readRef(run, ref);
    try {
      const decoded = await io.decode(page, TYPES);
      if (decoded.rows !== 1) throw new SpillCorruptionError("Invalid tape directory rows");
      const numbers = decoded.columns.map((column) => {
        if (column.data.layout !== "f64")
          throw new SpillCorruptionError("Invalid tape directory field");
        return column.data.values[0] ?? NaN;
      });
      const previousOffset = numbers[0] ?? NaN,
        previousLength = numbers[1] ?? NaN,
        previousChecksum = numbers[2] ?? NaN;
      const reference = (
        offset: number,
        length: number,
        checksum: number,
        max: number,
      ): SpillPageRef => {
        if (
          !Number.isSafeInteger(offset) ||
          offset < 0 ||
          !Number.isSafeInteger(length) ||
          length < 1 ||
          length > max ||
          !Number.isSafeInteger(checksum) ||
          checksum < 0 ||
          checksum > 0xffff_ffff
        )
          throw new SpillCorruptionError("Invalid tape reference");
        return { offset, length, checksum };
      };
      let previous: SpillPageRef | null = null;
      if (previousOffset === -1) {
        if (previousLength !== 0 || previousChecksum !== 0)
          throw new SpillCorruptionError("Invalid tape terminator");
      } else {
        previous = reference(
          previousOffset,
          previousLength,
          previousChecksum,
          DIRECTORY_PAGE_BYTES,
        );
        if (previous.offset === ref.offset) throw new SpillCorruptionError("Cyclic tape directory");
      }
      const data = reference(numbers[3] ?? NaN, numbers[4] ?? NaN, numbers[5] ?? NaN, maxDataPage);
      return { previous, data };
    } finally {
      io.releasePage(page);
    }
  }
  detach(): void {
    if (!this.#charge) return;
    this.#columns = [];
    this.#values = null;
    this.#io = null;
    this.#charge.free(METADATA_BYTES);
    this.#charge = null;
  }
}

export class HashTape extends SpilledRun {
  readonly #files: SpillFiles;
  readonly #data = new SpilledRun();
  #index = new SpilledRun();
  #tail: SpillPageRef | null = null;
  #sealed = false;
  #pageCount = 0;
  #largestPage = 0;
  #release: Promise<void> | null = null;
  constructor(readonly directory: TapeDirectory) {
    super();
    this.#files = directory.io.files;
    this.write = (page, rows) => this.#append(page, rows);
  }
  override get pageCount(): number {
    return this.#pageCount;
  }
  async #append(page: Uint8Array, rows: number): Promise<void> {
    if (this.#sealed) throw new Error("Append after tape seal");
    const io = this.directory.io;
    const ref = await io.files.appendRef(this.#data, page, rows);
    this.#largestPage = Math.max(this.#largestPage, page.length);
    // Payload append has settled before the shared page writer is reused for metadata.
    this.#tail = await io.files.appendRef(this.#index, this.directory.encode(this.#tail, ref), 1);
    this.rows += rows;
    this.bytes += page.length;
    this.#pageCount++;
  }
  /** Only directory records are reversed, so both memory and payload I/O stay bounded. */
  async seal(): Promise<void> {
    if (this.#sealed) return;
    const io = this.directory.io,
      next = new SpilledRun();
    let cursor = this.#tail,
      tail: SpillPageRef | null = null,
      remaining = this.#pageCount;
    while (cursor !== null) {
      // Reused scratch extents need not increase physically. The admitted tape's
      // exact page count bounds traversal and rejects cycles without a RAM ref set.
      if (remaining-- <= 0) throw new SpillCorruptionError("Cyclic tape directory");
      const item = await this.directory.read(this.#index, cursor, this.#largestPage);
      tail = await io.files.appendRef(next, this.directory.encode(tail, item.data), 1);
      cursor = item.previous;
    }
    if (remaining !== 0) throw new SpillCorruptionError("Truncated tape directory");
    await io.files.release(this.#index);
    this.#index = next;
    this.#tail = tail;
    this.#sealed = true;
  }
  async *read(
    types: readonly PhysicalType[],
  ): AsyncGenerator<Batch & { readonly columns: readonly FlatVector[] }, void, void> {
    await this.seal();
    let cursor = this.#tail,
      remaining = this.#pageCount;
    while (cursor !== null) {
      if (remaining-- <= 0) throw new SpillCorruptionError("Cyclic tape directory");
      const item = await this.directory.read(this.#index, cursor, this.#largestPage),
        io = this.directory.io;
      const page = await io.readRef(this.#data, item.data);
      try {
        const decoded = await io.decode(page, types);
        yield { length: decoded.rows, columns: decoded.columns, selection: null };
      } finally {
        io.releasePage(page);
      }
      cursor = item.previous;
    }
    if (remaining !== 0) throw new SpillCorruptionError("Truncated tape directory");
  }
  release(): Promise<void> {
    return (this.#release ??= (async () => {
      // Admission can be cancelled while the directory has no attached I/O buffers.
      // Closing unbuffered files needs their lifetime owner, not a new read grant.
      const files = this.#files;
      const results = await Promise.allSettled([
        files.release(this.#index),
        files.release(this.#data),
      ]);
      this.#tail = null;
      const errors = results
        .filter((r): r is PromiseRejectedResult => r.status === "rejected")
        .map((r) => r.reason as unknown);
      if (errors.length) throw new AggregateError(errors, "Tape close failed");
    })());
  }
  /** Proves no duplicate in-memory page-ref list grows with input. */
  get retainedPageRefs(): number {
    return this.#tail === null ? 0 : 1;
  }
}
