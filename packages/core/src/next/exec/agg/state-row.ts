/** Exact transition fields, carried by the shared native spill codec. Float fields are
 * float64 buffers (including -0, NaN and Sxx), never JSON or finalized AVG values.
 */
import { bigintBytes, OBJECT_OVERHEAD_BYTES } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { FlatVector } from "../../vector/vector.js";
import { coefficientAt } from "../kernels/numeric.js";

const F64: PhysicalType = { kind: "float64" };
const INTEGER: PhysicalType = { kind: "decimal", precision: null, scale: null };
const METADATA = 8 * OBJECT_OVERHEAD_BYTES;
export class StateRow {
  readonly columns: FlatVector[] = [];
  #bytes = 0;
  #wanted = 0;
  #closed = false;
  constructor(
    readonly charge: ByteCharge,
    readonly measuring = false,
  ) {
    // Before snapshot code inspects/converts a coefficient: bounded BigInt/encoding scratch.
    this.#take(384 * 1024 + 4 * OBJECT_OVERHEAD_BYTES, true);
  }
  get byteLength(): number {
    return this.#wanted;
  }
  #take(bytes: number, scratch = false): void {
    this.#wanted += bytes;
    if (this.measuring && !scratch) return;
    if (!this.charge.allocate(bytes)) throw new Error("Transition snapshot was not admitted");
    this.#bytes += bytes;
  }
  number(value: number): void {
    this.#take(METADATA + 8);
    if (this.measuring) return;
    this.columns.push({
      form: "flat",
      type: F64,
      length: 1,
      validity: null,
      data: { layout: "f64", values: Float64Array.of(value) },
    });
  }
  float64(values: Float64Array, index: number): void {
    this.#take(METADATA + 8);
    if (this.measuring) return;
    const copy = new Float64Array(1);
    new Uint8Array(copy.buffer).set(
      new Uint8Array(values.buffer, values.byteOffset + 8 * index, 8),
    );
    this.columns.push({
      form: "flat",
      type: F64,
      length: 1,
      validity: null,
      data: { layout: "f64", values: copy },
    });
  }
  integer(value: bigint): void {
    this.#take(METADATA + 8 + bigintBytes(value));
    if (this.measuring) return;
    this.columns.push({
      form: "flat",
      type: INTEGER,
      length: 1,
      validity: null,
      data: {
        layout: "decimal",
        scale: 0,
        scales: null,
        specials: null,
        coefficients: { repr: "wide", values: [value] },
      },
    });
  }
  value(bytes: number, make: () => FlatVector): void {
    this.#take(METADATA + bytes);
    if (!this.measuring) this.columns.push(make());
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.columns.length = 0;
    this.charge.free(this.#bytes);
    this.#bytes = 0;
  }
}
export class StateReader {
  #column = 0;
  constructor(
    readonly columns: readonly FlatVector[],
    readonly row: number,
  ) {}
  number(): number {
    const vector = this.columns[this.#column++];
    if (vector?.data.layout !== "f64") throw new Error("Invalid float transition field");
    return vector.data.values[this.row] ?? 0;
  }
  float64(values: Float64Array, index: number): void {
    const vector = this.columns[this.#column++];
    if (vector?.data.layout !== "f64") throw new Error("Invalid float transition field");
    const source = vector.data.values;
    new Uint8Array(values.buffer, values.byteOffset + 8 * index, 8).set(
      new Uint8Array(source.buffer, source.byteOffset + 8 * this.row, 8),
    );
  }
  integer(): bigint {
    const vector = this.columns[this.#column++];
    if (vector?.data.layout !== "decimal") throw new Error("Invalid integer transition field");
    return coefficientAt(vector.data, this.row);
  }
  value(): FlatVector {
    const vector = this.columns[this.#column++];
    if (!vector) throw new Error("Missing transition field");
    return vector;
  }
  get consumed(): number {
    return this.#column;
  }
}
