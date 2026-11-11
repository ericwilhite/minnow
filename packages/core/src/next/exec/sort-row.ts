/** One borrowed row for a spill page: payload buffers stay with the input through append. */
import { allocateData } from "../vector/vector.js";
import type { FlatVector, Int64Values, Vector, VectorData } from "../vector/vector.js";

/** Caller prepays the bounded view/bitmap/NULL metadata before constructing these views. */
export const SORT_ROW_VIEW_BYTES = 1024;
export function sortRowView(vector: Vector, row: number): FlatVector {
  const valid = (bitmap: Uint32Array | null, at: number) =>
    bitmap === null || ((bitmap[at >>> 5] ?? 0) & (1 << (at & 31))) !== 0;
  const flat =
    vector.form === "flat" ? vector : vector.form === "constant" ? vector.value : vector.dictionary;
  const slot =
    vector.form === "flat" ? row : vector.form === "constant" ? 0 : (vector.codes[row] ?? 0);
  if ((vector.form === "dictionary" && !valid(vector.validity, row)) || !valid(flat.validity, slot))
    return {
      form: "flat",
      type: vector.type,
      length: 1,
      validity: new Uint32Array(1),
      data: allocateData(vector.type, 1),
    };
  const int64 = (values: Int64Values): Int64Values =>
    values.repr === "safe"
      ? { repr: "safe", values: values.values.subarray(slot, slot + 1) }
      : { repr: "int64", values: values.values.subarray(slot, slot + 1) };
  const input = flat.data;
  let data: VectorData;
  switch (input.layout) {
    case "bool":
      data = { layout: "bool", values: input.values.subarray(slot, slot + 1) };
      break;
    case "i32":
      data = { layout: "i32", values: input.values.subarray(slot, slot + 1) };
      break;
    case "f32":
      data = { layout: "f32", values: input.values.subarray(slot, slot + 1) };
      break;
    case "f64":
      data = { layout: "f64", values: input.values.subarray(slot, slot + 1) };
      break;
    case "int64":
      data = { layout: "int64", values: int64(input.values) };
      break;
    case "varlen":
      data = {
        layout: "varlen",
        bytes: input.bytes,
        offsets: input.offsets.subarray(slot, slot + 2),
      };
      break;
    case "fixed16":
      data = { layout: "fixed16", bytes: input.bytes.subarray(slot * 16, (slot + 1) * 16) };
      break;
    case "interval":
      data = {
        layout: "interval",
        months: input.months.subarray(slot, slot + 1),
        days: input.days.subarray(slot, slot + 1),
        micros: int64(input.micros),
      };
      break;
    case "decimal": {
      const coefficients = input.coefficients;
      data = {
        ...input,
        scales: input.scales?.subarray(slot, slot + 1) ?? null,
        specials: input.specials?.subarray(slot, slot + 1) ?? null,
        coefficients:
          coefficients.repr === "int128"
            ? { repr: "int128", limbs: coefficients.limbs.subarray(slot * 4, (slot + 1) * 4) }
            : coefficients.repr === "wide"
              ? { repr: "wide", values: coefficients.values.slice(slot, slot + 1) }
              : int64(coefficients),
      };
      break;
    }
  }
  return { form: "flat", type: vector.type, length: 1, validity: null, data };
}
