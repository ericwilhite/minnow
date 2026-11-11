/** Unadmitted groups spill whole rows in input order. Recursive passes reuse the shared
 * spill codec and rehash canonical SQL keys; no aggregate transition state is combined.
 */
import { xxHash32 } from "../../hash/xxhash32.js";
import { KeyBatch, encodeKeyBatch } from "../../keys/key-encoding.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import { BATCH_ROWS, type Batch } from "../../vector/vector.js";
import { ColumnStore } from "../column-store.js";
import { levelHash } from "../join-keys.js";
import { SpillIo } from "../spill-pool.js";
import { HashTape, type TapeDirectory } from "./hash-tape.js";

const FANOUT = 16;
export class HashPartitions {
  readonly runs: HashTape[];
  readonly #keys: KeyBatch;
  readonly #rows: Uint16Array;
  readonly #buckets: Uint8Array;
  readonly #starts: Int32Array;
  readonly #cursor: Int32Array;
  readonly #staging: ColumnStore;
  constructor(
    readonly types: readonly PhysicalType[],
    readonly keyColumns: readonly number[],
    readonly charge: ByteCharge,
    readonly io: SpillIo,
    readonly level: number,
    directory: TapeDirectory,
    runs?: readonly HashTape[],
  ) {
    this.runs = runs ? [...runs] : Array.from({ length: FANOUT }, () => new HashTape(directory));
    if (this.runs.length !== FANOUT) throw new Error("Invalid hash partition count");
    const bytes = 3 * BATCH_ROWS + 8 * (FANOUT + 1);
    if (!charge.allocate(bytes)) throw new Error("Hash partition scratch was not admitted");
    this.#rows = new Uint16Array(BATCH_ROWS);
    this.#buckets = new Uint8Array(BATCH_ROWS);
    this.#starts = new Int32Array(FANOUT + 1);
    this.#cursor = new Int32Array(FANOUT + 1);
    this.#keys = new KeyBatch(charge, 32768);
    this.#staging = new ColumnStore(types, charge, {
      chunkRows: 256,
      targetBytes: 32768,
      varlenBytes: 1024,
    });
  }
  /** All rows in this selection belong to groups absent from the resident pass. */
  async write(batch: Batch): Promise<void> {
    const keys = this.keyColumns.map((column) => {
      const vector = batch.columns[column];
      if (!vector) throw new Error("Missing partition key");
      return { vector, direction: "asc" as const, nulls: "last" as const };
    });
    const total = batch.selection?.count ?? batch.length;
    for (let active = 0; active < total;) {
      let result = encodeKeyBatch("equality", keys, batch, this.#keys, active);
      if (!result.done && result.encoded === 0) {
        await this.io.need(result.requiredBytes);
        result = encodeKeyBatch("equality", keys, batch, this.#keys, active);
        if (!result.done && result.encoded === 0)
          throw new Error("Partition key exceeded its admitted step");
      }
      const count = result.encoded;
      this.#starts.fill(0);
      for (let k = 0; k < count; k++) {
        const p = levelHash(xxHash32(this.#keys.key(k)), this.level) >>> 28;
        this.#buckets[k] = p;
        this.#starts[p + 1] = (this.#starts[p + 1] ?? 0) + 1;
      }
      for (let p = 0; p < FANOUT; p++)
        this.#starts[p + 1] = (this.#starts[p + 1] ?? 0) + (this.#starts[p] ?? 0);
      this.#cursor.set(this.#starts);
      // Stable scatter retains every group's source order across prefixes and pages.
      for (let k = 0; k < count; k++) {
        const p = this.#buckets[k] ?? 0,
          at = this.#cursor[p] ?? 0;
        this.#rows[at] = batch.selection?.rows[active + k] ?? active + k;
        this.#cursor[p] = at + 1;
      }
      for (let p = 0; p < FANOUT; p++) {
        const from = this.#starts[p] ?? 0,
          to = this.#starts[p + 1] ?? 0,
          run = this.runs[p];
        if (to === from || !run) continue;
        await this.io.stage(
          this.#staging,
          run,
          { run: this.level, partition: p },
          batch.columns,
          this.#rows,
          from,
          to - from,
        );
        await this.io.flush(this.#staging, run, { run: this.level, partition: p });
      }
      active += count;
      this.#keys.writer.reset();
      this.io.pool.settle();
    }
  }
  close(): void {
    this.#staging.close();
    this.#keys.close();
    this.charge.free(
      this.#rows.byteLength +
        this.#buckets.byteLength +
        this.#starts.byteLength +
        this.#cursor.byteLength,
    );
  }
}
