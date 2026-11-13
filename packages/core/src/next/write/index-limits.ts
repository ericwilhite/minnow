/** Admission policy from the shipped keyToken/secondaryTupleComponent contracts, not run size.
 * Scalar text locators have 16,384 UTF-16 units. Tuple terms spend five characters per string
 * unit plus five terminators, one per boolean, sixteen per number/datetime; the whole posting
 * term is at most 65,536 characters. A composite primary's hidden scalar locator is additionally
 * subject to the scalar 16,384 limit. Imported scalar secondary paths can retain their policy.
 */
import type { IndexDef, TableDef, ColumnDef } from "../access/catalog.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import { int128At } from "../vector/int128.js";
import { sourceSlot } from "./columns.js";
import { need } from "./bytes.js";
export const INDEXED_UTF16_UNITS = 16_384;
export const INDEX_TUPLE_TERM_UNITS = 65_536;
const DOMAIN = "\u0000minnow-domain:";
export class IndexKeyLimitError extends RangeError {
  readonly code = "MINNOW_INDEX_KEY_TOO_LONG";
  constructor(
    readonly indexId: number,
    readonly units: number,
    readonly maximum: number,
  ) {
    super(
      `Index ${String(indexId)} key needs ${String(units)} units; this path permits ${String(maximum)}`,
    );
    this.name = "IndexKeyLimitError";
  }
}
export function scalarIndexLimit(index: IndexDef): boolean {
  return (
    index.keyLimit === "scalar" ||
    (index.keyLimit === undefined && index.role === "primary" && index.columnIds.length === 1)
  );
}
function termLimit(index: IndexDef) {
  return index.role === "primary" ? INDEXED_UTF16_UNITS : INDEX_TUPLE_TERM_UNITS;
}
/** No native allocations, pins or data reads: safe to use before range admission. */
export function indexMaximumKeyBytes(index: IndexDef, table: TableDef): number {
  if (!scalarIndexLimit(index))
    return Math.ceil((termLimit(index) * 3) / 5) + index.columnIds.length * 17 + 3;
  const type = need(table.columns.find((c) => c.id === index.columnIds[0])).type;
  switch (type.kind) {
    case "bool":
      return 2;
    case "int32":
    case "date":
      return 5;
    case "int64":
    case "time":
    case "timestamp":
    case "float64":
      return 9;
    case "float32":
      return 9;
    case "uuid":
    case "interval":
      return 17;
    case "text":
      return INDEXED_UTF16_UNITS * 3 + 3;
    default:
      return INDEXED_UTF16_UNITS * 3 + 32;
  }
}
function utf16(bytes: Uint8Array, start: number, end: number): number {
  let units = 0;
  for (let at = start; at < end; at++) {
    const b = bytes[at] ?? 0;
    if (b < 0x80 || b >= 0xc0) units += b >= 0xf0 ? 2 : 1;
  }
  return units;
}
/** Compare catalog text to a borrowed UTF-8 label without building a global native cache. */
function matchesText(bytes: Uint8Array, start: number, end: number, text: string): boolean {
  let index = 0;
  for (let at = start; at < end;) {
    const first = bytes[at++] ?? 0;
    let point = first;
    if (first >= 0xf0)
      point =
        ((first & 7) << 18) |
        (((bytes[at++] ?? 0) & 63) << 12) |
        (((bytes[at++] ?? 0) & 63) << 6) |
        ((bytes[at++] ?? 0) & 63);
    else if (first >= 0xe0)
      point = ((first & 15) << 12) | (((bytes[at++] ?? 0) & 63) << 6) | ((bytes[at++] ?? 0) & 63);
    else if (first >= 0xc0) point = ((first & 31) << 6) | ((bytes[at++] ?? 0) & 63);
    if (text.codePointAt(index) !== point) return false;
    index += point > 0xffff ? 2 : 1;
  }
  return index === text.length;
}
function jsonLength(text: string) {
  let n = text.length;
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    if (c === 34 || c === 92 || c === 8 || c === 9 || c === 10 || c === 12 || c === 13) n++;
    else if (c < 32) n += 5;
  }
  return n;
}
/** A domain's canonical legacy string length. No rendering of the value or UTF-8 copy. */
function stringUnits(
  column: ColumnDef,
  flat: FlatVector,
  row: number,
  exact: boolean,
): number | null {
  const type = column.type,
    data = flat.data;
  if (data.layout === "varlen") {
    const start = data.offsets[row] ?? 0,
      end = data.offsets[row + 1] ?? 0;
    if (type.kind === "enum") {
      let rank = 0;
      while (
        rank < type.labels.length &&
        !matchesText(data.bytes, start, end, need(type.labels[rank]))
      )
        rank++;
      const label = need(type.labels[rank]);
      return (
        DOMAIN.length + 5 + 8 + String(rank).length + jsonLength(type.name) + jsonLength(label)
      );
    }
    return exact && type.kind !== "bytes" ? utf16(data.bytes, start, end) : end - start;
  }
  if (data.layout === "decimal") {
    const special = data.specials?.[row] ?? 0;
    if (special !== 0) return DOMAIN.length + 8 + (special === 1 ? 3 : special === 2 ? 8 : 9);
    const values = data.coefficients;
    let digits = (
      values.repr === "wide"
        ? need(values.values[row])
        : values.repr === "int128"
          ? int128At(values.limbs, row)
          : BigInt(values.values[row] ?? 0)
    ).toString();
    const negative = digits.startsWith("-");
    if (negative) digits = digits.slice(1);
    let scale = data.scales?.[row] ?? data.scale ?? 0;
    while (scale > 0 && digits.endsWith("0") && digits !== "0") {
      digits = digits.slice(0, -1);
      scale--;
    }
    if (digits === "0") scale = 0;
    const n =
      scale <= 0 ? digits.length - scale : scale >= digits.length ? scale + 2 : digits.length + 1;
    return DOMAIN.length + 8 + n + Number(negative);
  }
  if (type.kind === "uuid") return DOMAIN.length + 5 + 36;
  // These bounded domains cannot approach the scalar limit. Their canonical legacy spelling
  // remains a string component in a composite term, rather than a number component.
  if (type.kind === "date") return DOMAIN.length + 5 + 10;
  if (type.kind === "time") {
    if (data.layout !== "int64") throw new TypeError("TIME needs an int64 vector");
    let fraction = Number(BigInt(data.values.values[row] ?? 0) % 1_000_000n),
      width = 6;
    while (width > 0 && fraction % 10 === 0) {
      fraction = Math.floor(fraction / 10);
      width--;
    }
    return DOMAIN.length + 5 + 8 + (width === 0 ? 0 : 1 + width);
  }
  if (type.kind === "interval") {
    if (data.layout !== "interval") throw new TypeError("INTERVAL needs an interval vector");
    return (
      DOMAIN.length +
      9 +
      4 +
      String(data.months[row] ?? 0).length +
      String(data.days[row] ?? 0).length +
      String(data.micros.values[row] ?? 0).length
    );
  }
  return null;
}
/** Null keys have no entries under the existing equality-index contract. A cheap UTF-8 upper
 * bound accepts ordinary short text without scanning it; only a near-limit row needs exact
 * UTF-16 counting. Both passes use the same resolved flat/constant/dictionary slots. */
export function indexLimitViolation(
  index: IndexDef,
  table: TableDef,
  columns: readonly Vector[],
  rows: Uint16Array | null,
  from: number,
  count: number,
): IndexKeyLimitError | null {
  const scalar = scalarIndexLimit(index),
    limit = scalar ? INDEXED_UTF16_UNITS : termLimit(index),
    slot: { flat: FlatVector | null; index: number } = { flat: null, index: -1 };
  // Fixed physical scalars cannot hit the legacy string ceiling. Even the longest canonical
  // interval component costs fewer than 512 term characters; usual numeric indexes need no
  // per-row work here. Wide tuples still take the exact per-attribute path below.
  if (
    (scalar || index.columnIds.length * 512 <= limit) &&
    index.columnIds.every((id) => {
      const kind = need(table.columns.find((c) => c.id === id)).type.kind;
      return kind !== "text" && kind !== "bytes" && kind !== "enum" && kind !== "decimal";
    })
  )
    return null;
  for (let k = 0; k < count; k++) {
    const row = rows === null ? from + k : (rows[from + k] ?? 0);
    let nullKey = false;
    for (const id of index.columnIds) {
      sourceSlot(need(columns[table.columns.findIndex((c) => c.id === id)]), row, slot);
      if (slot.index < 0) {
        nullKey = true;
        break;
      }
    }
    if (nullKey) continue;
    for (let pass = 0; pass < 2; pass++) {
      const exact = pass === 1;
      let total = 0,
        maximum = limit,
        exceeded = false;
      for (const id of index.columnIds) {
        const c = table.columns.findIndex((c) => c.id === id),
          column = need(table.columns[c]);
        sourceSlot(need(columns[c]), row, slot);
        const n = stringUnits(column, slot.flat, slot.index, exact);
        if (n !== null && n > INDEXED_UTF16_UNITS) {
          exceeded = true;
          total = n;
          maximum = INDEXED_UTF16_UNITS;
          break;
        }
        if (scalar) continue;
        total += n === null ? (column.type.kind === "bool" ? 1 : 16) : 5 * n + 5;
        if (total > limit) {
          exceeded = true;
          break;
        }
      }
      if (!exceeded) break;
      if (exact) return new IndexKeyLimitError(index.id, total, maximum);
    }
  }
  return null;
}
