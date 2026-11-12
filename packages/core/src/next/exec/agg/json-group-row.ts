/** A representative group key never retains a progress loan between input rows. */
import type { Grant } from "../../memory/governor.js";
import type { SpillService } from "../../spill/contract.js";
import type { PhysicalType } from "../../types.js";
import type { Vector, FlatVector } from "../../vector/vector.js";
import { ColumnStore } from "../column-store.js";
import { JsonRecords } from "../json-records.js";
import type { Pool, SpillIo } from "../spill-pool.js";
import { operandBytes, type SortColumn } from "../sort-keys.js";

const RESIDENT_BYTES = 64 << 10;
const IO_BYTES = 512 << 10;
export class JsonGroupRow {
  readonly #resident: ColumnStore;
  #file: JsonRecords | null = null;
  #written = 0;
  #operandBytes = 0;
  #closing: Promise<void> | null = null;
  constructor(
    readonly types: readonly PhysicalType[],
    readonly order: readonly SortColumn[],
    readonly pool: Pool,
    readonly io: SpillIo,
    readonly grant: Grant,
    readonly spill: SpillService,
    readonly signal: AbortSignal,
  ) {
    this.#resident = new ColumnStore(types, pool, { chunkRows: 1, targetBytes: Infinity });
  }
  get fileBacked(): boolean {
    return this.#file !== null;
  }
  get operandBytes(): number {
    return this.#operandBytes;
  }
  get spilledBytes(): number {
    return this.#written + (this.#file?.files.written ?? 0);
  }
  vectors(): readonly FlatVector[] {
    if (this.#file) throw new Error("A spilled JSON group key must be read in a charged step");
    return this.#resident.vectors();
  }
  async capture(columns: readonly Vector[], row: number): Promise<void> {
    this.signal.throwIfAborted();
    if (this.#file) {
      this.#written += this.#file.files.written;
      await this.#file.close();
      this.#file = null;
    }
    this.#resident.reset(true);
    this.pool.settle();
    this.#operandBytes = operandBytes(this.types, this.order, columns, row, row + 1);
    if (this.#resident.appendNeed(columns, row) <= RESIDENT_BYTES) {
      // The fixed aggregate workspace funds this bounded state, never a loan.
      if (this.#resident.append(columns, null, row, 1) !== 1)
        throw new Error("JSON resident group key was not admitted");
      return;
    }
    const file = new JsonRecords(this.io, this.spill, this.types);
    this.#file = file;
    await file.write(columns, row);
    await file.seal();
  }
  async read(
    extraBytes: number,
    visit: (columns: readonly FlatVector[]) => Promise<void>,
  ): Promise<void> {
    const file = this.#file;
    if (!file) {
      await visit(this.#resident.vectors());
      return;
    }
    // Restored key, both semantic operands and all bounded read/write I/O coexist.
    // Prepay their complete step before its first loan, then retire every borrower.
    const bytes = file.retainedBytes;
    await this.pool.need(bytes + extraBytes + IO_BYTES, this.grant, this.signal);
    const release = this.pool.pinStep();
    try {
      if (!this.pool.allocate(bytes))
        throw new Error("JSON group key restoration was not admitted");
      try {
        await file.read(this.io, bytes, async (columns) => visit(columns));
      } finally {
        this.pool.free(bytes);
      }
    } finally {
      release();
    }
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#resident.close();
      if (this.#file) {
        this.#written += this.#file.files.written;
        await this.#file.close();
        this.#file = null;
      }
    })());
  }
}
