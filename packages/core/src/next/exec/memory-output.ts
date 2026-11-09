/** Native, caller-owned outputs for the publication-stable MemorySegment contract. */
import type { ChunkOutput, MemoryOutputNeed } from "../access/snapshot.js";
import type { ByteBudget } from "../access/space.js";
import { OBJECT_OVERHEAD_BYTES } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import { allocateData, type VectorData } from "../vector/vector.js";

interface Prepared {
  readonly needs: readonly MemoryOutputNeed[];
  readonly out: readonly ChunkOutput[];
  readonly bytes: number;
}

export class MemoryOutputs {
  #current: Prepared | null = null;
  // Retain the old allocation's charge until the caller has installed the replacement.
  #retired = 0;
  constructor(
    readonly types: readonly PhysicalType[],
    readonly budget: ByteBudget,
  ) {}

  prepare(needs: readonly MemoryOutputNeed[]): readonly ChunkOutput[] {
    if (needs.length !== this.types.length) throw new RangeError("Memory output column count");
    if (this.#retired > 0) this.budget.free(this.#retired);
    this.#retired = 0;
    const current = this.#current;
    if (
      current !== null &&
      needs.every((need, i) => {
        const old = current.needs[i];
        return (
          old !== undefined &&
          old.rows >= need.rows &&
          old.varlenBytes >= need.varlenBytes &&
          old.columnId === need.columnId &&
          old.shape.layout === need.shape.layout &&
          old.shape.validity === need.shape.validity &&
          old.shape.integer === need.shape.integer &&
          old.shape.scales === need.shape.scales &&
          old.shape.scale === need.shape.scale &&
          old.shape.specials === need.shape.specials
        );
      })
    )
      return current.out;
    // Includes request/output descriptors, vector/data objects and every view/buffer header.
    const bytes =
      OBJECT_OVERHEAD_BYTES +
      needs.reduce((sum, need) => sum + need.bytes + 24 * OBJECT_OVERHEAD_BYTES + 8, 0);
    if (!this.budget.allocate(bytes)) throw new RangeError("Admitted memory output did not fit");
    try {
      const out = needs.map((need, i): ChunkOutput => {
        const type = this.types[i];
        if (type === undefined) throw new RangeError("Missing memory output type");
        const { shape, rows } = need;
        const integer = () =>
          shape.integer === "safe"
            ? { repr: "safe" as const, values: new Float64Array(rows) }
            : { repr: "int64" as const, values: new BigInt64Array(rows) };
        let data: VectorData;
        if (shape.layout === "int64") data = { layout: "int64", values: integer() };
        else if (shape.layout === "interval")
          data = {
            layout: "interval",
            months: new Int32Array(rows),
            days: new Int32Array(rows),
            micros: integer(),
          };
        else if (shape.layout === "decimal")
          data = {
            layout: "decimal",
            scale: shape.scale,
            scales: shape.scales ? new Uint16Array(rows) : null,
            specials: shape.specials ? new Uint8Array(rows) : null,
            coefficients:
              shape.integer === "int128"
                ? { repr: "int128", limbs: new Uint32Array(rows * 4) }
                : integer(),
          };
        else data = allocateData(type, rows, need.varlenBytes);
        return {
          flat: {
            form: "flat",
            type,
            length: 0,
            validity: shape.validity ? new Uint32Array(Math.ceil(rows / 32)) : null,
            data,
          },
          codes: new Int32Array(rows),
          varlen: null,
        };
      });
      this.#retired = current?.bytes ?? 0;
      this.#current = { needs, out, bytes };
      return out;
    } catch (error) {
      this.budget.free(bytes);
      throw error;
    }
  }

  close(): void {
    this.budget.free((this.#current?.bytes ?? 0) + this.#retired);
    this.#current = null;
    this.#retired = 0;
  }
}
