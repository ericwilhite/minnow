/** PostgreSQL output-function casts. Variable output uses the existing charged,
 * cancellable count/copy runner, including pressure-only producer handoff. */
import type { PhysicalType } from "../../types.js";
import { arrayElementTag } from "../../vector/array-value.js";
import { int64At } from "../../vector/int64.js";
import { bigintBytes } from "../../memory/governor.js";
import { UnsupportedPlanError } from "../errors.js";
import { valueBytes } from "../array-data.js";
import { sqlArrayText, sqlHex, sqlScalarText } from "../sql-array-text.js";
import { compileJson } from "./json.js";
import { decimalAscii, jsonAscii } from "./json-value.js";
import { coefficientAt, scaleAt, specialAt } from "./numeric.js";
import { isValidSlot, slotOf, type Compiled, type OutputArena } from "./vectors.js";
export function compileTextOutput(
  type: PhysicalType,
  input: Compiled,
  arena: OutputArena,
): Compiled {
  if (type.kind !== "text" || type.maxLength !== undefined || input.type.kind === "locator")
    throw new UnsupportedPlanError("bounded or unsupported output-function cast", {
      construct: "cast",
    });
  if (input.type.kind === "array") {
    try {
      arrayElementTag(input.type.element);
    } catch {
      throw new UnsupportedPlanError("array element output type", { construct: "cast" });
    }
  }
  return compileJson(type, "sql_text", [input], arena, function* (vectors, row, out, memory) {
    const value = vectors[0];
    if (!value || !isValidSlot(value, row)) return false;
    const [flat, slot] = slotOf(value, row),
      d = flat.data;
    if (d.layout === "varlen") {
      const bytes = valueBytes(value, row);
      if (value.type.kind === "array") yield* sqlArrayText(value.type.element, bytes, out, memory);
      else if (value.type.kind === "bytes") yield* sqlHex(bytes, out);
      else yield* out.copy(bytes);
    } else if (d.layout === "decimal") {
      const special = specialAt(d, slot);
      if (special)
        yield* jsonAscii(special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity", out);
      else {
        const coefficient = coefficientAt(d, slot),
          scratch = 128 + 8 * bigintBytes(coefficient);
        yield* memory.take(scratch);
        try {
          yield* decimalAscii(coefficient, scaleAt(d, slot), out);
        } finally {
          memory.free(scratch);
        }
      }
    } else {
      const scalar =
        d.layout === "bool"
          ? d.values[slot] !== 0
          : d.layout === "int64"
            ? int64At(d.values, slot)
            : d.layout === "fixed16"
              ? d.bytes.subarray(16 * slot, 16 * (slot + 1))
              : d.layout === "interval"
                ? {
                    months: d.months[slot] ?? 0,
                    days: d.days[slot] ?? 0,
                    micros: int64At(d.micros, slot),
                  }
                : (d.values[slot] ?? 0);
      yield* jsonAscii(sqlScalarText(scalar, value.type), out);
    }
    return true;
  });
}
