/** Read-only membership in a resident hash pass. Uses the existing table in place;
 * there is no second materialization of all keys when pressure starts.
 */
import { KeyBatch, encodeKeyBatch } from "../../keys/key-encoding.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Batch, Vector } from "../../vector/vector.js";
import type { GroupTable } from "./groups.js";

export class FrozenGroups {
  readonly #encoded: KeyBatch;
  readonly #row: Uint16Array;
  #table: GroupTable | null = null;
  #batch: Batch | null = null;
  #columns: readonly Vector[] = [];
  #closed = false;
  constructor(
    readonly charge: ByteCharge,
    readonly groups: number,
  ) {
    if (!charge.allocate(2)) throw new Error("Frozen lookup row was not admitted");
    this.#row = new Uint16Array(1);
    this.#encoded = new KeyBatch(charge, Infinity);
  }
  build(table: GroupTable, check: () => void): void {
    check();
    this.#table = table;
  }
  encode(batch: Batch, columns: readonly Vector[]): void {
    this.#batch = batch;
    this.#columns = columns;
  }
  readonly #encode = (key: Vector, row: number): Uint8Array => {
    this.#row[0] = row;
    const result = encodeKeyBatch(
      "equality",
      [{ vector: key, direction: "asc", nulls: "last" }],
      { length: key.length, selection: { rows: this.#row, count: 1 } },
      this.#encoded,
    );
    if (!result.done) throw new Error("Frozen key exceeded its admitted step");
    return this.#encoded.key(0);
  };
  find(position: number): number {
    const batch = this.#batch,
      table = this.#table;
    if (!batch || !table) throw new Error("Frozen lookup before preparation");
    const row = batch.selection?.rows[position] ?? position;
    const id = table.lookup(this.#columns, row, this.#encode);
    return id < this.groups ? id : -1;
  }
  releaseBorrow(): void {
    this.#batch = null;
    this.#columns = [];
    this.#encoded.writer.reset();
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#batch = null;
    this.#columns = [];
    this.#table = null;
    this.#encoded.close();
    this.charge.free(2);
  }
}
