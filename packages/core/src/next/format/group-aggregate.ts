/** MAGS1: exact, bounded group/column facts. Value pages do not change. The parent root
 * publishes this blob atomically with its sources; a BlobRef checksum is verified by storage,
 * once, before decoding. These facts never certify an unread value page's integrity.
 *
 * A reader MUST separately certify its pinned visibility and full exactly-once coverage.
 * Deletions/overlays/partial selections fall back. Immutable parse caches cannot cache that
 * query-local eligibility. Full source fields are binding facts, not additional owned edges.
 */
import { DURABLE_CODECS, DURABLE_FORMATS, requireHeader } from "../durable-format.js";
import { MAX_BLOB_BYTES, type ByteBudget } from "../storage/extents.js";
import type { ColumnChunk, BlobRef, ChunkData } from "./manifest.js";
import type { PhysicalType } from "../types.js";
import { ManifestCorruptionError, valid } from "./bytes.js";
import { parseCharge, ParseBudgetError, type Parsed } from "./manifest-codec.js";

const CODEC = DURABLE_CODECS.groupAggregate;
export const GROUP_AGGREGATE_HEADER_BYTES = CODEC.headerBytes;
export const MAX_GROUP_AGGREGATE_BYTES = CODEC.headerBytes + CODEC.payloadBytes;
/** Complete temporary encode window, including bounded sign conversion and hex text. */
export const GROUP_AGGREGATE_ENCODE_BYTES = parseCharge(MAX_GROUP_AGGREGATE_BYTES);
const HEX = "0123456789abcdef";
const text = new TextDecoder("utf-8", { fatal: true });
const TWO32 = 0x100000000;

export interface ExactGroupSum {
  readonly coefficient: bigint;
  /** The finite sum is coefficient*10^(-coefficientScale). */
  readonly coefficientScale: number;
  /** Preserve maximum input display scale, including after cancellation. */
  readonly resultScale: number;
  readonly nanCount: number;
  readonly positiveInfinityCount: number;
  readonly negativeInfinityCount: number;
}
export interface GroupAggregate {
  readonly nonNullCount: number;
  /** No SUM capability means ordinary fallback, NEVER a zero sum. */
  readonly sum: ExactGroupSum | null;
}
export interface EncodedGroupAggregate {
  readonly bytes: Uint8Array;
  /** Caller holds this until its append/IO settles, then frees it. */
  readonly charge: number;
}

export function groupAggregateClass(type: PhysicalType): number | undefined {
  if (type.kind === "decimal") return CODEC.class.decimal;
  if (type.kind === "int64") return CODEC.class.int64;
  if (type.kind === "int32") return type.bits === 16 ? CODEC.class.int16 : CODEC.class.int32;
  return undefined;
}
/** Public SQL NUMERIC types fit. Unsupported low-level typmods omit this optional
 * optimization rather than narrowing the root type or making formerly writable values fail. */
export function groupAggregateTypeSupported(type: PhysicalType): boolean {
  if (groupAggregateClass(type) === undefined) return false;
  return (
    type.kind !== "decimal" ||
    ((type.precision === null ||
      (Number.isInteger(type.precision) &&
        type.precision >= 1 &&
        type.precision <= CODEC.maximumSourcePrecision)) &&
      (type.scale === null ||
        (Number.isInteger(type.scale) &&
          type.scale >= CODEC.minimumSourceScale &&
          type.scale <= CODEC.maximumSourceScale)))
  );
}
function writeSourceType(view: DataView, column: ColumnChunk): void {
  if (column.type.kind !== "decimal") return;
  view.setUint8(
    100,
    column.type.scale === null ? CODEC.sourceScaleMode.mixed : CODEC.sourceScaleMode.fixed,
  );
  view.setUint16(102, column.type.precision ?? 0, true);
  view.setInt16(104, column.type.scale ?? 0, true);
}
function checkSourceType(view: DataView, column: ColumnChunk): void {
  valid(groupAggregateTypeSupported(column.type), "Aggregate source type is not representable");
  const type = column.type;
  valid(
    view.getUint8(100) ===
      (type.kind === "decimal" && type.scale !== null
        ? CODEC.sourceScaleMode.fixed
        : CODEC.sourceScaleMode.mixed) &&
      view.getUint16(102, true) === (type.kind === "decimal" ? (type.precision ?? 0) : 0) &&
      view.getInt16(104, true) === (type.kind === "decimal" ? (type.scale ?? 0) : 0),
    "Aggregate source typing differs",
  );
}
function safe(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}
function range(ref: ChunkData): boolean {
  return (
    safe(ref.extent.offset) &&
    safe(ref.extent.length) &&
    ref.extent.length > 0 &&
    safe(ref.extent.offset + ref.extent.length) &&
    safe(ref.offset) &&
    safe(ref.length) &&
    ref.length > 0 &&
    ref.offset <= ref.extent.length &&
    ref.length <= ref.extent.length - ref.offset
  );
}
function binding(rows: number, column: ColumnChunk): void {
  valid(Number.isInteger(rows) && rows > 0 && rows <= CODEC.maxRows, "Invalid aggregate row count");
  valid(safe(column.columnId), "Invalid aggregate column id");
  valid(range(column.data) && range(column.directory), "Invalid aggregate source range");
  valid(column.directory.length <= MAX_BLOB_BYTES, "Aggregate directory passes blob cap");
  valid(
    safe(column.directory.checksum) && column.directory.checksum <= 0xffffffff,
    "Invalid aggregate directory checksum",
  );
  valid(
    Number.isInteger(column.statistics.nullCount) &&
      column.statistics.nullCount >= 0 &&
      column.statistics.nullCount <= rows,
    "Invalid aggregate source null count",
  );
}
function state(value: GroupAggregate, rows: number, column: ColumnChunk, kind: number): void {
  valid(
    value.nonNullCount === rows - column.statistics.nullCount,
    "Aggregate count differs from its source",
  );
  const sum = value.sum;
  if (sum === null) return;
  valid(
    Number.isInteger(sum.coefficientScale) &&
      sum.coefficientScale >= CODEC.minimumCoefficientScale &&
      sum.coefficientScale <= CODEC.maximumScale,
    "Invalid aggregate coefficient scale",
  );
  valid(
    Number.isInteger(sum.resultScale) &&
      sum.resultScale >= 0 &&
      sum.resultScale <= CODEC.maximumScale &&
      sum.resultScale >= sum.coefficientScale,
    "Invalid aggregate result scale",
  );
  const countValid = (count: number): boolean =>
    Number.isInteger(count) && count >= 0 && count <= value.nonNullCount;
  valid(
    countValid(sum.nanCount) &&
      countValid(sum.positiveInfinityCount) &&
      countValid(sum.negativeInfinityCount),
    "Invalid aggregate special count",
  );
  const specials = sum.nanCount + sum.positiveInfinityCount + sum.negativeInfinityCount;
  valid(specials <= value.nonNullCount, "Too many aggregate specials");
  if (value.nonNullCount > specials && column.type.kind === "decimal" && column.type.scale !== null)
    valid(
      sum.resultScale === Math.max(0, column.type.scale),
      "Fixed-scale aggregate differs from source",
    );
  valid(
    sum.coefficient !== 0n || sum.coefficientScale === 0,
    "Zero aggregate has a coefficient scale",
  );
  if (value.nonNullCount === specials)
    valid(
      sum.coefficient === 0n && sum.coefficientScale === 0 && sum.resultScale === 0,
      "Empty finite aggregate carries a sum or scale",
    );
  if (kind !== CODEC.class.decimal) {
    valid(
      specials === 0 && sum.coefficientScale === 0 && sum.resultScale === 0,
      "Integer aggregate carries decimal metadata",
    );
    const width = kind === CODEC.class.int16 ? 16 : kind === CODEC.class.int32 ? 32 : 64;
    const minimum = -(1n << BigInt(width - 1)),
      maximum = -minimum - 1n;
    const count = BigInt(value.nonNullCount);
    valid(
      sum.coefficient >= minimum * count && sum.coefficient <= maximum * count,
      "Integer aggregate is outside its source domain",
    );
  }
}
function writeSafe(view: DataView, offset: number, value: number): void {
  view.setUint32(offset, value >>> 0, true);
  view.setUint32(offset + 4, Math.floor(value / TWO32), true);
}
function readSafe(view: DataView, offset: number): number {
  const value = view.getUint32(offset, true) + view.getUint32(offset + 4, true) * TWO32;
  valid(safe(value), "Unsafe aggregate source integer");
  return value;
}
function writeSource(view: DataView, at: number, ref: ChunkData): void {
  writeSafe(view, at, ref.extent.offset);
  writeSafe(view, at + 8, ref.extent.length);
  writeSafe(view, at + 16, ref.offset);
  if (at === 36) {
    view.setUint32(at + 24, ref.length, true);
    view.setUint32(at + 28, (ref as BlobRef).checksum, true);
  } else writeSafe(view, at + 24, ref.length);
}
function checkSource(view: DataView, at: number, ref: ChunkData): void {
  valid(
    readSafe(view, at) === ref.extent.offset &&
      readSafe(view, at + 8) === ref.extent.length &&
      readSafe(view, at + 16) === ref.offset &&
      (at === 36
        ? view.getUint32(at + 24, true) === ref.length &&
          view.getUint32(at + 28, true) === (ref as BlobRef).checksum
        : readSafe(view, at + 24) === ref.length),
    "Aggregate source identity differs",
  );
}
function digit(code: number): number {
  return code <= 57 ? code - 48 : code - 87;
}

/** Optional production: all temporary capacity is funded BEFORE bounded conversion/allocation.
 * Lack of headroom or an unrepresentable coefficient returns null, so ordinary data still writes.
 * Invalid source/count metadata is a caller error, not a silently written malformed summary. */
export function tryEncodeGroupAggregate(
  value: GroupAggregate,
  rows: number,
  column: ColumnChunk,
  budget: ByteBudget,
): EncodedGroupAggregate | null {
  const kind = groupAggregateClass(column.type);
  if (kind === undefined || !groupAggregateTypeSupported(column.type)) return null;
  const charge = GROUP_AGGREGATE_ENCODE_BYTES;
  if (!budget.allocate(charge)) return null;
  let keep = false;
  try {
    binding(rows, column);
    state(value, rows, column, kind);
    const sum = value.sum,
      specialBytes = sum !== null && kind === CODEC.class.decimal ? 12 : 0;
    let hex = "",
      negative = false,
      coefficientBytes = 0;
    if (sum !== null) {
      const maximum = CODEC.payloadBytes - specialBytes;
      if (BigInt.asIntN(maximum * 8, sum.coefficient) !== sum.coefficient) return null;
      negative = sum.coefficient < 0n;
      hex = (negative ? ~sum.coefficient : sum.coefficient).toString(16);
      coefficientBytes = Math.ceil(hex.length / 2);
      if ((hex.length & 1) === 0 && digit(hex.charCodeAt(0)) >= 8) coefficientBytes++;
    }
    const bytes = new Uint8Array(CODEC.headerBytes + coefficientBytes + specialBytes);
    const view = new DataView(bytes.buffer);
    view.setUint32(0, DURABLE_FORMATS.groupAggregate.magic, true);
    view.setUint16(4, 1, true);
    writeSafe(view, 8, column.columnId);
    view.setUint32(16, rows, true);
    view.setUint32(20, value.nonNullCount, true);
    view.setUint8(24, kind);
    view.setUint8(25, CODEC.capability.count | (sum === null ? 0 : CODEC.capability.sum));
    view.setInt16(26, sum?.coefficientScale ?? 0, true);
    view.setUint16(28, sum?.resultScale ?? 0, true);
    view.setUint16(30, coefficientBytes, true);
    view.setUint16(32, specialBytes, true);
    writeSource(view, 36, column.directory);
    writeSource(view, 68, column.data);
    writeSourceType(view, column);
    for (let i = 0; i < coefficientBytes; i++) {
      const low = hex.length - 1 - i * 2,
        high = low - 1;
      const byte =
        (low < 0 ? 0 : digit(hex.charCodeAt(low))) |
        (high < 0 ? 0 : digit(hex.charCodeAt(high)) << 4);
      bytes[CODEC.headerBytes + i] = negative ? byte ^ 255 : byte;
    }
    if (sum !== null && specialBytes !== 0) {
      const at = CODEC.headerBytes + coefficientBytes;
      view.setUint32(at, sum.nanCount, true);
      view.setUint32(at + 4, sum.positiveInfinityCount, true);
      view.setUint32(at + 8, sum.negativeInfinityCount, true);
    }
    keep = true;
    return { bytes, charge };
  } finally {
    if (!keep) budget.free(charge);
  }
}

/** Borrowed coefficient bytes remain valid only while the source lease is held. */
export interface GroupAggregateRecord {
  readonly nonNullCount: number;
  readonly sum:
    | (Omit<ExactGroupSum, "coefficient"> & {
        readonly coefficientBytes: Uint8Array;
      })
    | null;
}

/** This conversion is separately usable by a funded consumer after ALL arguments preflight.
 * No pow10/rescale is performed here. The descriptor's original parse charge funds this
 * bounded conversion and coexistence; retaining the result requires a consumer-owned charge. */
export function groupAggregateCoefficient(bytes: Uint8Array): bigint {
  valid(bytes.length >= 1 && bytes.length <= CODEC.payloadBytes, "Invalid coefficient length");
  const negative = ((bytes[bytes.length - 1] ?? 0) & 128) !== 0;
  const hex = new Uint8Array(2 + bytes.length * 2);
  hex[0] = 48;
  hex[1] = 120;
  let out = 2;
  for (let i = bytes.length - 1; i >= 0; i--) {
    const byte = (bytes[i] ?? 0) ^ (negative ? 255 : 0);
    hex[out++] = HEX.charCodeAt(byte >>> 4);
    hex[out++] = HEX.charCodeAt(byte & 15);
  }
  const magnitude = BigInt(text.decode(hex));
  return negative ? ~magnitude : magnitude;
}

/** Storage verifies the independent BlobRef checksum. The reader returns bounded validated
 * borrowed bytes, not an eager NUMERIC BigInt or rescale, BEFORE any value/dictionary load.
 * Visibility is a separate PINNED-snapshot proof and is never an immutable cache fact. */
export function decodeGroupAggregateRecord(
  bytes: Uint8Array,
  rows: number,
  column: ColumnChunk,
  budget: ByteBudget,
): Parsed<GroupAggregateRecord> {
  valid(
    bytes.length >= CODEC.headerBytes && bytes.length <= MAX_GROUP_AGGREGATE_BYTES,
    "Invalid aggregate blob length",
  );
  const charge = parseCharge(bytes.length);
  if (!budget.allocate(charge)) throw new ParseBudgetError(charge);
  try {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    requireHeader("groupAggregate", bytes, ManifestCorruptionError);
    binding(rows, column);
    const kind = groupAggregateClass(column.type);
    valid(kind !== undefined && view.getUint8(24) === kind, "Aggregate class differs from source");
    valid(
      readSafe(view, 8) === column.columnId && view.getUint32(16, true) === rows,
      "Aggregate unit differs from source",
    );
    checkSource(view, 36, column.directory);
    checkSource(view, 68, column.data);
    checkSourceType(view, column);
    const nonNullCount = view.getUint32(20, true);
    valid(
      nonNullCount === rows - column.statistics.nullCount,
      "Aggregate count differs from its source",
    );
    const capabilities = view.getUint8(25),
      coefficientBytes = view.getUint16(30, true);
    const specialBytes = view.getUint16(32, true),
      hasSum = (capabilities & CODEC.capability.sum) !== 0;
    valid(
      coefficientBytes + specialBytes <= CODEC.payloadBytes &&
        bytes.length === CODEC.headerBytes + coefficientBytes + specialBytes,
      "Aggregate payload lengths do not tile the blob",
    );
    valid(
      hasSum
        ? coefficientBytes > 0 && specialBytes === (kind === CODEC.class.decimal ? 12 : 0)
        : coefficientBytes === 0 &&
            specialBytes === 0 &&
            view.getInt16(26, true) === 0 &&
            view.getUint16(28, true) === 0,
      "Aggregate payload differs from capabilities",
    );
    let sum: GroupAggregateRecord["sum"] = null;
    if (hasSum) {
      const last = bytes[CODEC.headerBytes + coefficientBytes - 1] ?? 0;
      if (coefficientBytes > 1) {
        const previous = bytes[CODEC.headerBytes + coefficientBytes - 2] ?? 0;
        valid(
          !((last === 0 && (previous & 128) === 0) || (last === 255 && (previous & 128) !== 0)),
          "Nonminimal aggregate coefficient",
        );
      }
      const at = CODEC.headerBytes + coefficientBytes;
      const coefficientScale = view.getInt16(26, true),
        resultScale = view.getUint16(28, true);
      const nanCount = specialBytes === 0 ? 0 : view.getUint32(at, true);
      const positiveInfinityCount = specialBytes === 0 ? 0 : view.getUint32(at + 4, true);
      const negativeInfinityCount = specialBytes === 0 ? 0 : view.getUint32(at + 8, true);
      const specials = nanCount + positiveInfinityCount + negativeInfinityCount;
      valid(
        coefficientScale <= CODEC.maximumScale && resultScale >= coefficientScale,
        "Invalid aggregate scales",
      );
      valid(specials <= nonNullCount, "Too many aggregate specials");
      if (nonNullCount > specials && column.type.kind === "decimal" && column.type.scale !== null)
        valid(
          resultScale === Math.max(0, column.type.scale),
          "Fixed-scale aggregate differs from source",
        );
      const zero = coefficientBytes === 1 && last === 0;
      valid(!zero || coefficientScale === 0, "Zero aggregate has a coefficient scale");
      if (nonNullCount === specials)
        valid(
          zero && coefficientScale === 0 && resultScale === 0,
          "Empty finite aggregate carries a sum or scale",
        );
      const coefficient = bytes.subarray(CODEC.headerBytes, at);
      if (kind !== CODEC.class.decimal) {
        valid(
          coefficientScale === 0 && resultScale === 0,
          "Integer aggregate carries decimal metadata",
        );
        // <=65536 signed int64 values need at most80 signed bits; refuse longer BEFORE
        // conversion. NUMERIC coefficients are not eagerly materialized on this path.
        valid(coefficientBytes <= 10, "Integer aggregate is outside its source domain");
        const width = kind === CODEC.class.int16 ? 16 : kind === CODEC.class.int32 ? 32 : 64;
        const minimum = -(1n << BigInt(width - 1)),
          maximum = -minimum - 1n;
        const value = groupAggregateCoefficient(coefficient),
          count = BigInt(nonNullCount);
        valid(
          value >= minimum * count && value <= maximum * count,
          "Integer aggregate is outside its source domain",
        );
      }
      sum = {
        coefficientBytes: coefficient,
        coefficientScale,
        resultScale,
        nanCount,
        positiveInfinityCount,
        negativeInfinityCount,
      };
    }
    return { value: { nonNullCount, sum }, charge };
  } catch (error) {
    budget.free(charge);
    if (error instanceof ManifestCorruptionError) throw error;
    throw new ManifestCorruptionError("Group aggregate does not decode", { cause: error });
  }
}

/** Eager convenience for producer/codec tests; the executor borrows the record above. */
export function decodeGroupAggregate(
  bytes: Uint8Array,
  rows: number,
  column: ColumnChunk,
  budget: ByteBudget,
): Parsed<GroupAggregate> {
  const parsed = decodeGroupAggregateRecord(bytes, rows, column, budget);
  try {
    const record = parsed.value;
    const sum = record.sum;
    return {
      value: {
        nonNullCount: record.nonNullCount,
        sum:
          sum === null
            ? null
            : {
                coefficient: groupAggregateCoefficient(sum.coefficientBytes),
                coefficientScale: sum.coefficientScale,
                resultScale: sum.resultScale,
                nanCount: sum.nanCount,
                positiveInfinityCount: sum.positiveInfinityCount,
                negativeInfinityCount: sum.negativeInfinityCount,
              },
      },
      charge: parsed.charge,
    };
  } catch (error) {
    budget.free(parsed.charge);
    throw error;
  }
}
