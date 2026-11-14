/** Version-2 native FTS logical rendering; never tokenize encoded vector bytes or dictionary codes. */
import { MAX_INDEXED_STRING_CHARACTERS, renderDocumentValue } from "../full-text.js";
import { formatDate, formatTimestamp, unixMillisFromTimestamp } from "../temporal.js";
import type { PhysicalType } from "../types.js";
import { formatSqlTime as ftsTimeText } from "../temporal-text.js";
export { ftsTimeText };
import { sqlArrayText, sqlUuidText } from "./sql-array-text.js";
import { Slice, runToEnd } from "./slices.js";
import { JsonOutput } from "./kernels/json-output.js";
import type { JsonMemory } from "./kernels/json.js";
import { int64At } from "../vector/int64.js";
import { decodeUtf8 } from "../vector/varlen.js";
import type { Vector } from "../vector/vector.js";
import { coefficientAt, scaleAt } from "./kernels/numeric.js";
import { flatOf, slotIndex, isValidSlot } from "./kernels/vectors.js";

/** NFKC expands a scalar by at most 18 code units (Unicode normalization conformance);
 * default lowercase by at most 3. Account input + both strings + regex slice + 4096
 * bounded token objects/strings. emitRun streams code points, never Array.from(normalized).
 * One arena shares this scratch across synchronous cells/nodes, not per input row. */
export function ftsWorkspaceBytes(inputBytes: number): number {
  return 64 * 1024 + 512 * Math.min(16384, Math.max(0, inputBytes));
}
export const FTS_WORKSPACE_BYTES = ftsWorkspaceBytes(16384);
/** Numeric scale can render many zeros from a tiny coefficient; encoded bytes alone do
 * not bound its text. Arrays of numerics keep one bounded rendered document as well. */
export function ftsDeclaredBytes(type: PhysicalType): number {
  if (type.kind === "array") return type.element.kind === "decimal" ? 16384 : 0;
  if (type.kind !== "decimal") return 0;
  return type.precision === null || type.scale === null
    ? 16384
    : Math.max(type.precision, Math.abs(type.scale)) + 3;
}

function decimalText(coefficient: bigint, scale: number): string {
  // Legacy NUMERIC stores canonical value text, not the declared output padding.
  while (scale > 0 && coefficient % 10n === 0n) {
    coefficient /= 10n;
    scale--;
  }
  const negative = coefficient < 0n;
  const digits = String(negative ? -coefficient : coefficient).padStart(scale + 1, "0");
  return (
    (negative ? "-" : "") + (scale ? digits.slice(0, -scale) + "." + digits.slice(-scale) : digits)
  );
}

/** FTS owns this bounded scratch already; casts use the same renderer with a
 * real row workspace. The document cap belongs here, never inside array_out. */
function arrayText(bytes: Uint8Array, element: PhysicalType): string {
  const limit = () => new RangeError("Full-text values cannot exceed 16384 characters");
  class DocumentOutput extends JsonOutput {
    override put(byte: number): void {
      if (this.position >= 4 * MAX_INDEXED_STRING_CHARACTERS) throw limit();
      super.put(byte);
    }
    override *copy(value: Uint8Array) {
      if (this.position + value.length > 4 * MAX_INDEXED_STRING_CHARACTERS) throw limit();
      yield* super.copy(value);
    }
  }
  const slice = new Slice(Infinity),
    memory: JsonMemory = {
      // eslint-disable-next-line require-yield -- FTS has prepaid its bounded document workspace
      *take(n) {
        if (n > FTS_WORKSPACE_BYTES) throw limit();
      },
      free: () => undefined,
    },
    guards = {
      begin(count: number) {
        if (count > 0 && 2 * count + 1 > MAX_INDEXED_STRING_CHARACTERS) throw limit();
      },
      beforeBytes(value: Uint8Array) {
        if (
          element.kind === "bytes"
            ? value.length > 8191
            : value.length > 4 * MAX_INDEXED_STRING_CHARACTERS
        )
          throw limit();
      },
    },
    counter = new DocumentOutput(null, slice);
  runToEnd(sqlArrayText(element, bytes, counter, memory, guards));
  const output = new Uint8Array(counter.position),
    writer = new DocumentOutput(output, slice);
  runToEnd(sqlArrayText(element, bytes, writer, memory, guards));
  const text = new TextDecoder("utf-8", { ignoreBOM: true }).decode(output);
  if (text.length > MAX_INDEXED_STRING_CHARACTERS) throw limit();
  return text;
}
function hexText(bytes: Uint8Array): string {
  let text = "\\x";
  for (const byte of bytes) text += byte.toString(16).padStart(2, "0");
  return text;
}

export function ftsValue(vector: Vector, row: number): string | undefined {
  if (!isValidSlot(vector, row) || vector.type.kind === "bool") return undefined;
  const data = flatOf(vector).data,
    slot = slotIndex(vector, row);
  switch (data.layout) {
    case "i32":
      return vector.type.kind === "date"
        ? formatDate(data.values[slot] ?? 0)
        : String(data.values[slot]);
    case "f64":
    case "f32":
      return String(data.values[slot]);
    case "int64": {
      const value = int64At(data.values, slot);
      if (vector.type.kind === "timestamp") {
        const date = new Date(unixMillisFromTimestamp(value));
        // Common 0.x Date values keep the exact intrinsic ISO spelling. Native-only
        // sub-ms / extended-range / infinite values retain the full native ISO value.
        return value % 1000n === 0n && Number.isFinite(date.getTime())
          ? renderDocumentValue(date)
          : formatTimestamp(value, vector.type.zoned);
      }
      if (vector.type.kind === "time") return ftsTimeText(value);
      return String(value);
    }
    case "decimal": {
      const special = data.specials?.[slot] ?? 0;
      if (special) return special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity";
      return decimalText(coefficientAt(data, slot), scaleAt(data, slot));
    }
    case "varlen": {
      if (vector.type.kind === "array")
        return arrayText(
          data.bytes.subarray(data.offsets[slot] ?? 0, data.offsets[slot + 1] ?? 0),
          vector.type.element,
        );
      if (vector.type.kind === "bytes") {
        const start = data.offsets[slot] ?? 0,
          end = data.offsets[slot + 1] ?? 0;
        if (end - start > 8191)
          throw new RangeError("Full-text values cannot exceed 16384 characters");
        return hexText(data.bytes.subarray(start, end));
      }
      const bytes = (data.offsets[slot + 1] ?? 0) - (data.offsets[slot] ?? 0);
      if (bytes > 4 * MAX_INDEXED_STRING_CHARACTERS)
        throw new RangeError("Full-text values cannot exceed 16384 characters");
      return decodeUtf8(data, slot);
    }
    case "interval":
      return `${String(data.months[slot])} mons ${String(data.days[slot])} days ${String(int64At(data.micros, slot))} usecs`;
    case "bool":
      return undefined;
    case "fixed16":
      return sqlUuidText(data.bytes.subarray(16 * slot, 16 * (slot + 1)));
  }
}
