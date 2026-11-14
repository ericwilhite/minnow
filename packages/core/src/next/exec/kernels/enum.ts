/** Enum input validates labels without making a string or copying the source vector. */
import type { PhysicalType } from "../../types.js";
import type { Batch, Selection, Vector, VectorData } from "../../vector/vector.js";
type VarlenData = Extract<VectorData, { layout: "varlen" }>;
import { varlenMatchesString } from "../../vector/varlen.js";
import { ExecutionError } from "../errors.js";
import { countIn, rowsIn, slotOf, validAt, type Compiled } from "./vectors.js";

export class EnumCast implements Compiled {
  constructor(
    readonly type: Extract<PhysicalType, { kind: "enum" }>,
    readonly input: Compiled,
  ) {}
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const value = this.input.evaluate(batch, rows),
      active = rowsIn(rows),
      count = countIn(rows, batch.length);
    for (let i = 0; i < count; i++) {
      const [flat, slot] = slotOf(value, active[i] ?? 0);
      if (!validAt(flat.validity, slot)) continue;
      if (flat.data.layout !== "varlen") throw new TypeError("Enum input is not text");
      if (
        !this.type.labels.some((label) => varlenMatchesString(flat.data as VarlenData, slot, label))
      )
        throw new ExecutionError(
          `invalid input value for enum ${this.type.name}`,
          "MINNOW_INVALID_ENUM",
          "22P02",
        );
    }
    switch (value.form) {
      case "flat":
        return { ...value, type: this.type };
      case "constant":
        return { ...value, type: this.type, value: { ...value.value, type: this.type } };
      case "dictionary":
        return { ...value, type: this.type };
    }
  }
}
