import {
  assertWellFormedString,
  crc32,
  crc32Continue,
  MAX_STORED_BLOCK_BYTE_LENGTH,
} from "../block-format/index.js";
import { dateIsoString, dateMilliseconds } from "../date-value.js";
import {
  MAX_SNAPSHOT_FRAME_ITEMS,
  MAX_SNAPSHOT_METADATA_FRAME_BYTES,
  MAX_STORAGE_ID_CHARACTERS,
  MAX_ROW_ID_EXCLUSIVE_END,
  MAX_AUTO_INCREMENT_EXCLUSIVE_END,
  MAX_UNIQUE_KEY_BUILD_CHUNK_BYTES,
  MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK,
  MAX_FTS_POSTING_ROW_IDS_PER_CHUNK,
  MAX_FTS_POSTINGS_PER_CHUNK,
  MAX_FTS_POSTING_TERM_CHARACTERS,
  MAX_FTS_BASE_CHUNKS,
  MAX_FTS_TOKENS_PER_DOCUMENT,
  SNAPSHOT_FRAME_KINDS,
  type SnapshotFrame,
  type SnapshotFrameFooter,
  type SnapshotFrameKind,
  type SnapshotFrameStreamHeader,
  type SnapshotMetadataItem,
  type SnapshotCatalogItem,
  type SnapshotSegmentItem,
  type SnapshotTransactionItem,
  type SnapshotUniqueItem,
  type SnapshotPostingItem,
  normalizeSegmentRecord,
  uniqueKeyBuildChunkRetainedBytes,
  validateTableRecordBounds,
  validateStorageId,
} from "./types.js";

const MAGIC = Uint8Array.of(0x4d, 0x49, 0x4e, 0x53, 0x4e, 0x41, 0x50, 0x31); // MINSNAP1
const HEADER_PREFIX_BYTES = 20;
const FRAME_MARKER = 0x46524d31;
const FRAME_PREFIX_BYTES = 40;
const FOOTER_MARKER = 0x46545231;
const FOOTER_BYTES = 40;
const MAX_HEADER_BYTES = 64 * 1024;
const MAX_WIRE_ENTRIES = 262_144;
const MAX_WIRE_DEPTH = 128;
const MAX_UINT64_DECIMAL_BYTES = 20;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

export type SnapshotFrameStreamEntry =
  | { readonly type: "header"; readonly header: SnapshotFrameStreamHeader }
  | { readonly type: "frame"; readonly frame: SnapshotFrame }
  | { readonly type: "footer"; readonly footer: SnapshotFrameFooter };

function safeWhole(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new RangeError(`${label} must be a non-negative safe integer`);
  }
  return value as number;
}

function canonicalTimestamp(value: unknown, label: string): string {
  if (typeof value !== "string") throw new TypeError(`${label} must be a string`);
  const date = new Date(value);
  if (!Number.isFinite(dateMilliseconds(date)) || dateIsoString(date) !== value) {
    throw new TypeError(`${label} must be a canonical UTC timestamp`);
  }
  return value;
}

function sameKeys(value: object, expected: readonly string[], label: string): void {
  const keys = Object.keys(value).sort();
  const canonical = [...expected].sort();
  if (keys.length !== canonical.length || keys.some((key, index) => key !== canonical[index])) {
    throw new TypeError(`${label} has unknown or missing fields`);
  }
}

export function prepareSnapshotFrameStreamHeader(
  input: SnapshotFrameStreamHeader,
): SnapshotFrameStreamHeader {
  const runtime: unknown = input;
  if (typeof runtime !== "object" || runtime === null) {
    throw new TypeError("Snapshot stream header must be an object");
  }
  sameKeys(runtime, ["formatVersion", "databaseVersion", "createdAt", "kinds"], "Snapshot header");
  const fields = runtime as Record<string, unknown>;
  if (fields.formatVersion !== 1) throw new TypeError("Unsupported snapshot format version");
  const databaseVersion = safeWhole(fields.databaseVersion, "Snapshot database version");
  const createdAt = canonicalTimestamp(fields.createdAt, "Snapshot creation time");
  if (typeof fields.kinds !== "object" || fields.kinds === null) {
    throw new TypeError("Snapshot kind summaries must be an object");
  }
  sameKeys(fields.kinds, SNAPSHOT_FRAME_KINDS, "Snapshot kind summaries");
  const rawKinds = fields.kinds as Record<string, unknown>;
  const kinds = Object.create(null) as Record<
    SnapshotFrameKind,
    {
      frameCount: number;
      itemCount: number;
      storedBytes: number;
    }
  >;
  for (const kind of SNAPSHOT_FRAME_KINDS) {
    const summary = rawKinds[kind];
    if (typeof summary !== "object" || summary === null) {
      throw new TypeError(`Snapshot ${kind} summary must be an object`);
    }
    sameKeys(summary, ["frameCount", "itemCount", "storedBytes"], `Snapshot ${kind} summary`);
    const rawSummary = summary as Record<string, unknown>;
    const frameCount = safeWhole(rawSummary.frameCount, `Snapshot ${kind} frame count`);
    const itemCount = safeWhole(rawSummary.itemCount, `Snapshot ${kind} item count`);
    const storedBytes = safeWhole(rawSummary.storedBytes, `Snapshot ${kind} stored bytes`);
    if ((frameCount === 0) !== (itemCount === 0 && storedBytes === 0)) {
      throw new TypeError(`Snapshot ${kind} empty summary is inconsistent`);
    }
    kinds[kind] = { frameCount, itemCount, storedBytes };
  }
  return { formatVersion: 1, databaseVersion, createdAt, kinds };
}

function canonicalHeaderJson(header: SnapshotFrameStreamHeader): string {
  const prepared = prepareSnapshotFrameStreamHeader(header);
  return JSON.stringify({
    formatVersion: 1,
    databaseVersion: prepared.databaseVersion,
    createdAt: prepared.createdAt,
    kinds: Object.fromEntries(SNAPSHOT_FRAME_KINDS.map((kind) => [kind, prepared.kinds[kind]])),
  });
}

export function encodeSnapshotFrameStreamHeader(header: SnapshotFrameStreamHeader): Uint8Array {
  const bytes = textEncoder.encode(canonicalHeaderJson(header));
  if (bytes.byteLength > MAX_HEADER_BYTES) throw new RangeError("Snapshot header is too large");
  const encoded = new Uint8Array(HEADER_PREFIX_BYTES + bytes.byteLength);
  encoded.set(MAGIC, 0);
  const view = new DataView(encoded.buffer);
  view.setUint32(8, 1, true);
  view.setUint32(12, bytes.byteLength, true);
  view.setUint32(16, crc32(bytes), true);
  encoded.set(bytes, HEADER_PREFIX_BYTES);
  return encoded;
}

export function snapshotFrameStreamHeaderIdentity(header: SnapshotFrameStreamHeader): string {
  const bytes = encodeSnapshotFrameStreamHeader(header);
  return `snapshot-v1/${String(bytes.byteLength)}/${crc32(bytes).toString(16).padStart(8, "0")}`;
}

function utf8Bytes(value: string, label: string): Uint8Array {
  assertWellFormedString(value, label);
  return textEncoder.encode(value);
}

function stringFromUtf8(bytes: Uint8Array): string {
  try {
    return textDecoder.decode(bytes);
  } catch {
    throw new TypeError("Snapshot string is not valid UTF-8");
  }
}

function frameKindIndex(kind: SnapshotFrameKind): number {
  const index = SNAPSHOT_FRAME_KINDS.indexOf(kind);
  if (index < 0) throw new TypeError(`Unknown snapshot frame kind: ${kind}`);
  return index;
}

export function snapshotFrameEnvelopeParts(frame: SnapshotFrame): readonly Uint8Array[] {
  const sequence = safeWhole(frame.sequence, "Snapshot frame sequence");
  const itemCount = safeWhole(frame.itemCount, "Snapshot frame item count");
  if (itemCount < 1 || itemCount > MAX_SNAPSHOT_FRAME_ITEMS) {
    throw new RangeError("Snapshot frame item count is out of bounds");
  }
  const isBlock = frame.kind === "block";
  if (isBlock !== (frame.key !== null) || (isBlock && itemCount !== 1)) {
    throw new TypeError("Snapshot block frame identity is inconsistent");
  }
  const keyBytes =
    frame.key === null
      ? new Uint8Array()
      : utf8Bytes(validateStorageId(frame.key), "Snapshot frame key");
  const payloadLimit = isBlock ? MAX_STORED_BLOCK_BYTE_LENGTH : MAX_SNAPSHOT_METADATA_FRAME_BYTES;
  if (frame.payload.byteLength < 1 || frame.payload.byteLength > payloadLimit) {
    throw new RangeError("Snapshot frame payload length is out of bounds");
  }
  if (!Number.isInteger(frame.checksum) || frame.checksum < 0 || frame.checksum > 0xffffffff) {
    throw new RangeError("Snapshot frame checksum is invalid");
  }
  const prefix = new Uint8Array(FRAME_PREFIX_BYTES);
  const view = new DataView(prefix.buffer);
  view.setUint32(0, FRAME_MARKER, true);
  view.setUint32(4, frameKindIndex(frame.kind), true);
  view.setBigUint64(8, BigInt(sequence), true);
  view.setUint32(16, itemCount, true);
  view.setUint32(20, keyBytes.byteLength, true);
  view.setBigUint64(24, BigInt(frame.payload.byteLength), true);
  view.setUint32(32, frame.checksum, true);
  view.setUint32(36, 0, true);
  return keyBytes.byteLength === 0 ? [prefix, frame.payload] : [prefix, keyBytes, frame.payload];
}

export function extendSnapshotFrameStreamChecksum(
  checksum: number,
  parts: readonly Uint8Array[],
): number {
  let next = checksum;
  for (const part of parts) next = crc32Continue(next, part);
  return next;
}

export function encodeSnapshotFrameStreamFooter(footer: SnapshotFrameFooter): Uint8Array {
  const frameCount = safeWhole(footer.frameCount, "Snapshot footer frame count");
  const itemCount = safeWhole(footer.itemCount, "Snapshot footer item count");
  const storedBytes = safeWhole(footer.storedBytes, "Snapshot footer stored bytes");
  if (!Number.isInteger(footer.checksum) || footer.checksum < 0 || footer.checksum > 0xffffffff) {
    throw new RangeError("Snapshot footer checksum is invalid");
  }
  const bytes = new Uint8Array(FOOTER_BYTES);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, FOOTER_MARKER, true);
  view.setBigUint64(4, BigInt(frameCount), true);
  view.setBigUint64(12, BigInt(itemCount), true);
  view.setBigUint64(20, BigInt(storedBytes), true);
  view.setUint32(28, footer.checksum, true);
  return bytes;
}

class WireWriter {
  #bytes = new Uint8Array(256);
  #length = 0;

  #reserve(count: number): number {
    const next = this.#length + count;
    if (next > MAX_SNAPSHOT_METADATA_FRAME_BYTES) {
      throw new RangeError("Snapshot metadata frame exceeds its byte limit");
    }
    if (next > this.#bytes.byteLength) {
      let capacity = this.#bytes.byteLength;
      while (capacity < next) capacity = Math.min(MAX_SNAPSHOT_METADATA_FRAME_BYTES, capacity * 2);
      const grown = new Uint8Array(capacity);
      grown.set(this.#bytes.subarray(0, this.#length));
      this.#bytes = grown;
    }
    const offset = this.#length;
    this.#length = next;
    return offset;
  }

  byte(value: number): void {
    const offset = this.#reserve(1);
    this.#bytes[offset] = value;
  }

  uint32(value: number): void {
    const offset = this.#reserve(4);
    new DataView(this.#bytes.buffer).setUint32(offset, value, true);
  }

  float64(value: number): void {
    const offset = this.#reserve(8);
    new DataView(this.#bytes.buffer).setFloat64(offset, value, true);
  }

  bytes(value: Uint8Array): void {
    const offset = this.#reserve(value.byteLength);
    this.#bytes.set(value, offset);
  }

  finish(): Uint8Array {
    return this.#bytes.slice(0, this.#length);
  }

  get remaining(): number {
    return MAX_SNAPSHOT_METADATA_FRAME_BYTES - this.#length;
  }
}

function writeWireValue(
  writer: WireWriter,
  value: unknown,
  seen: WeakSet<object>,
  state: { entries: number },
  depth: number,
): void {
  if (depth > MAX_WIRE_DEPTH || ++state.entries > MAX_WIRE_ENTRIES) {
    throw new RangeError("Snapshot metadata value is too complex");
  }
  if (value === null) return writer.byte(0);
  if (value === false) return writer.byte(1);
  if (value === true) return writer.byte(2);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Snapshot metadata numbers must be finite");
    writer.byte(3);
    writer.float64(value);
    return;
  }
  if (typeof value === "string") {
    if (value.length > writer.remaining)
      throw new RangeError("Snapshot metadata string is too large");
    const bytes = utf8Bytes(value, "Snapshot metadata string");
    writer.byte(4);
    writer.uint32(bytes.byteLength);
    writer.bytes(bytes);
    return;
  }
  if (typeof value === "bigint") {
    if (value < 0n || value >= MAX_ROW_ID_EXCLUSIVE_END) {
      throw new RangeError("Snapshot metadata bigint is outside the unsigned 64-bit range");
    }
    const bytes = utf8Bytes(value.toString(), "Snapshot metadata bigint");
    writer.byte(5);
    writer.uint32(bytes.byteLength);
    writer.bytes(bytes);
    return;
  }
  if (typeof value !== "object") {
    throw new TypeError(`Unsupported snapshot metadata value: ${typeof value}`);
  }
  if (seen.has(value)) throw new TypeError("Snapshot metadata contains a cycle or alias");
  seen.add(value);
  if (value instanceof Date) {
    const milliseconds = dateMilliseconds(value);
    if (!Number.isSafeInteger(milliseconds)) {
      throw new TypeError("Snapshot metadata date is invalid");
    }
    writer.byte(8);
    writer.float64(milliseconds);
  } else if (value instanceof Uint8Array) {
    if (value.byteLength > writer.remaining - 5) {
      throw new RangeError("Snapshot metadata byte array is too large");
    }
    writer.byte(9);
    writer.uint32(value.byteLength);
    writer.bytes(value);
  } else if (Array.isArray(value)) {
    if (value.length > MAX_WIRE_ENTRIES) {
      throw new RangeError("Snapshot metadata array is too large");
    }
    writer.byte(6);
    writer.uint32(value.length);
    for (const item of value) writeWireValue(writer, item, seen, state, depth + 1);
  } else {
    const prototype: unknown = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError("Snapshot metadata must contain plain objects");
    }
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record).sort();
    if (keys.length > MAX_WIRE_ENTRIES) {
      throw new RangeError("Snapshot metadata object is too large");
    }
    writer.byte(7);
    writer.uint32(keys.length);
    for (const key of keys) {
      writeWireValue(writer, key, seen, state, depth + 1);
      writeWireValue(writer, record[key], seen, state, depth + 1);
    }
  }
}

/** Canonical substrate-independent encoding for one bounded metadata page. */
export function encodeSnapshotMetadataPage(items: readonly unknown[]): Uint8Array {
  if (items.length < 1 || items.length > MAX_SNAPSHOT_FRAME_ITEMS) {
    throw new RangeError("Snapshot metadata page item count is out of bounds");
  }
  const writer = new WireWriter();
  writeWireValue(writer, items, new WeakSet(), { entries: 0 }, 0);
  return writer.finish();
}

class WireReader {
  #offset = 0;
  #entries = 0;
  constructor(readonly bytes: Uint8Array) {}
  get done(): boolean {
    return this.#offset === this.bytes.byteLength;
  }
  get remaining(): number {
    return this.bytes.byteLength - this.#offset;
  }
  byte(): number {
    if (this.#offset >= this.bytes.byteLength) throw new Error("Snapshot metadata is truncated");
    return this.bytes[this.#offset++] ?? 0;
  }
  uint32(): number {
    if (this.#offset + 4 > this.bytes.byteLength) throw new Error("Snapshot metadata is truncated");
    const value = new DataView(this.bytes.buffer, this.bytes.byteOffset).getUint32(
      this.#offset,
      true,
    );
    this.#offset += 4;
    return value;
  }
  float64(): number {
    if (this.#offset + 8 > this.bytes.byteLength) throw new Error("Snapshot metadata is truncated");
    const value = new DataView(this.bytes.buffer, this.bytes.byteOffset).getFloat64(
      this.#offset,
      true,
    );
    this.#offset += 8;
    return value;
  }
  take(length: number): Uint8Array {
    if (length > this.bytes.byteLength - this.#offset)
      throw new Error("Snapshot metadata is truncated");
    const value = this.bytes.slice(this.#offset, this.#offset + length);
    this.#offset += length;
    return value;
  }
  entry(depth: number): void {
    if (depth > MAX_WIRE_DEPTH || ++this.#entries > MAX_WIRE_ENTRIES) {
      throw new RangeError("Snapshot metadata value is too complex");
    }
  }
}

function readWireValue(reader: WireReader, depth: number): unknown {
  reader.entry(depth);
  const tag = reader.byte();
  if (tag === 0) return null;
  if (tag === 1) return false;
  if (tag === 2) return true;
  if (tag === 3) {
    const value = reader.float64();
    if (!Number.isFinite(value)) throw new TypeError("Snapshot metadata number is not finite");
    return value;
  }
  if (tag === 4) return stringFromUtf8(reader.take(reader.uint32()));
  if (tag === 5) {
    const length = reader.uint32();
    if (length < 1 || length > MAX_UINT64_DECIMAL_BYTES || length > reader.remaining) {
      throw new RangeError("Snapshot bigint is outside the unsigned 64-bit range");
    }
    const text = stringFromUtf8(reader.take(length));
    if (!/^(?:0|[1-9][0-9]*)$/.test(text)) {
      throw new TypeError("Snapshot bigint is not canonical");
    }
    const value = BigInt(text);
    if (value >= MAX_ROW_ID_EXCLUSIVE_END) {
      throw new RangeError("Snapshot bigint is outside the unsigned 64-bit range");
    }
    return value;
  }
  if (tag === 8) {
    const milliseconds = reader.float64();
    if (!Number.isSafeInteger(milliseconds)) throw new TypeError("Snapshot date is invalid");
    const value = new Date(milliseconds);
    if (dateMilliseconds(value) !== milliseconds) throw new TypeError("Snapshot date is invalid");
    return value;
  }
  if (tag === 9) {
    const length = reader.uint32();
    if (length > MAX_SNAPSHOT_METADATA_FRAME_BYTES || length > reader.remaining) {
      throw new RangeError("Snapshot metadata byte array is too large");
    }
    return reader.take(length);
  }
  if (tag === 6) {
    const length = reader.uint32();
    if (length > MAX_WIRE_ENTRIES || length > reader.bytes.byteLength) {
      throw new RangeError("Snapshot array is too large");
    }
    return Array.from({ length }, () => readWireValue(reader, depth + 1));
  }
  if (tag === 7) {
    const length = reader.uint32();
    if (length > MAX_WIRE_ENTRIES) throw new RangeError("Snapshot object is too large");
    const value: Record<string, unknown> = {};
    let previous = "";
    for (let index = 0; index < length; index += 1) {
      const key = readWireValue(reader, depth + 1);
      if (typeof key !== "string" || (index > 0 && key <= previous)) {
        throw new TypeError("Snapshot object keys are not canonical");
      }
      previous = key;
      const item = readWireValue(reader, depth + 1);
      if (key === "__proto__") {
        Object.defineProperty(value, key, { value: item, enumerable: true, writable: true });
      } else value[key] = item;
    }
    return value;
  }
  throw new TypeError(`Unknown snapshot metadata tag: ${String(tag)}`);
}

export function decodeSnapshotMetadataPage(payload: Uint8Array): unknown[] {
  if (payload.byteLength < 1 || payload.byteLength > MAX_SNAPSHOT_METADATA_FRAME_BYTES) {
    throw new RangeError("Snapshot metadata page length is out of bounds");
  }
  const reader = new WireReader(payload);
  const value = readWireValue(reader, 0);
  if (!reader.done || !Array.isArray(value)) {
    throw new TypeError("Snapshot metadata page is not canonical");
  }
  if (value.length < 1 || value.length > MAX_SNAPSHOT_FRAME_ITEMS) {
    throw new RangeError("Snapshot metadata page item count is out of bounds");
  }
  return value;
}

function recordValue(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function exactRecord(
  value: unknown,
  fields: readonly string[],
  label: string,
): Record<string, unknown> {
  const record = recordValue(value, label);
  sameKeys(record, fields, label);
  return record;
}

function canonicalOrdinal(value: unknown, label: string): number {
  return safeWhole(value, label);
}

function canonicalCounter(value: unknown, label: string): bigint {
  if (typeof value !== "bigint" || value < 1n || value > MAX_ROW_ID_EXCLUSIVE_END) {
    throw new RangeError(`${label} must be a positive bounded bigint`);
  }
  return value;
}

function prepareCatalogItem(value: unknown): SnapshotCatalogItem {
  const item = exactRecord(
    value,
    ["kind", "record", "nextRowId", "autoIncrement"],
    "Snapshot catalog item",
  );
  if (item.kind !== "table") throw new TypeError("Snapshot catalog item kind is invalid");
  const table = item.record as SnapshotCatalogItem["record"];
  validateTableRecordBounds(table);
  const nextRowId = canonicalCounter(item.nextRowId, "Snapshot next row ID");
  if (!Array.isArray(item.autoIncrement) || item.autoIncrement.length > table.columns.length) {
    throw new RangeError("Snapshot auto-increment counters are invalid");
  }
  const autoIncrement = item.autoIncrement.map((raw) => {
    const counter = exactRecord(raw, ["columnId", "next"], "Snapshot auto-increment counter");
    const columnId = validateStorageId(counter.columnId, "Snapshot auto-increment column ID");
    const column = table.columns.find((candidate) => candidate.id === columnId);
    if (column?.defaultValue?.kind !== "autoincrement") {
      throw new TypeError("Snapshot auto-increment counter has no matching catalog column");
    }
    const next = canonicalCounter(counter.next, "Snapshot auto-increment value");
    if (next > MAX_AUTO_INCREMENT_EXCLUSIVE_END) {
      throw new RangeError("Snapshot auto-increment value exceeds its safe integer range");
    }
    return { columnId, next };
  });
  const canonicalIds = autoIncrement.map((entry) => entry.columnId);
  if (canonicalIds.some((id, index) => index > 0 && id <= (canonicalIds[index - 1] ?? ""))) {
    throw new TypeError("Snapshot auto-increment counters are not canonical");
  }
  return { kind: "table", record: table, nextRowId, autoIncrement };
}

function prepareSegmentItem(value: unknown): SnapshotSegmentItem {
  const item = exactRecord(value, ["kind", "record"], "Snapshot segment item");
  if (item.kind !== "segment") throw new TypeError("Snapshot segment item kind is invalid");
  return { kind: "segment", record: normalizeSegmentRecord(item.record as never) };
}

function prepareTransactionItem(value: unknown): SnapshotTransactionItem {
  const item = exactRecord(value, ["kind", "record"], "Snapshot transaction item");
  if (item.kind !== "transaction") {
    throw new TypeError("Snapshot transaction item kind is invalid");
  }
  const transaction = exactRecord(
    item.record,
    [
      "id",
      "ownerId",
      "expiresAt",
      "snapshotVersion",
      "pendingBlockIds",
      "pendingSegmentIds",
      "status",
      "revision",
      "startedAt",
      "updatedAt",
      "committedVersion",
    ],
    "Snapshot transaction record",
  );
  validateStorageId(transaction.id, "Snapshot transaction ID");
  validateStorageId(transaction.ownerId, "Snapshot transaction owner ID");
  canonicalTimestamp(transaction.expiresAt, "Snapshot transaction expiry");
  canonicalTimestamp(transaction.startedAt, "Snapshot transaction start time");
  canonicalTimestamp(transaction.updatedAt, "Snapshot transaction update time");
  if (
    transaction.status !== "committed" ||
    transaction.committedVersion === null ||
    !Number.isSafeInteger(transaction.committedVersion) ||
    (transaction.committedVersion as number) < 0 ||
    (transaction.snapshotVersion !== null &&
      (!Number.isSafeInteger(transaction.snapshotVersion) ||
        (transaction.snapshotVersion as number) < 0)) ||
    !Number.isSafeInteger(transaction.revision) ||
    (transaction.revision as number) < 0 ||
    !Array.isArray(transaction.pendingBlockIds) ||
    transaction.pendingBlockIds.length !== 0 ||
    !Array.isArray(transaction.pendingSegmentIds) ||
    transaction.pendingSegmentIds.length !== 0
  ) {
    throw new TypeError("Snapshot transaction must be a canonical committed record");
  }
  return { kind: "transaction", record: transaction as never };
}

function prepareUniqueItem(value: unknown): SnapshotUniqueItem {
  const record = recordValue(value, "Snapshot UNIQUE item");
  if (record.kind === "unique-generation") {
    sameKeys(
      record,
      ["kind", "tableId", "indexId", "namespaceId", "generationId", "chunkCount", "tokenCount"],
      "Snapshot UNIQUE generation",
    );
    const indexId =
      record.indexId === null
        ? null
        : validateStorageId(record.indexId, "Snapshot UNIQUE index ID");
    return {
      kind: "unique-generation",
      tableId: validateStorageId(record.tableId, "Snapshot UNIQUE table ID"),
      indexId,
      namespaceId: validateStorageId(record.namespaceId, "Snapshot UNIQUE namespace ID"),
      generationId: validateStorageId(record.generationId, "Snapshot UNIQUE generation ID"),
      chunkCount: canonicalOrdinal(record.chunkCount, "Snapshot UNIQUE chunk count"),
      tokenCount: canonicalOrdinal(record.tokenCount, "Snapshot UNIQUE token count"),
    };
  }
  sameKeys(
    record,
    ["kind", "namespaceId", "generationId", "ordinal", "keyTokens"],
    "Snapshot UNIQUE chunk",
  );
  if (record.kind !== "unique-chunk" || !Array.isArray(record.keyTokens)) {
    throw new TypeError("Snapshot UNIQUE chunk is invalid");
  }
  if (
    record.keyTokens.length < 1 ||
    record.keyTokens.length > MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK
  ) {
    throw new RangeError("Snapshot UNIQUE chunk token count is out of bounds");
  }
  const keyTokens = record.keyTokens.map((token) => {
    if (typeof token !== "string") throw new TypeError("Snapshot UNIQUE token must be a string");
    assertWellFormedString(token, "Snapshot UNIQUE token");
    return token;
  });
  if (uniqueKeyBuildChunkRetainedBytes(keyTokens) > MAX_UNIQUE_KEY_BUILD_CHUNK_BYTES) {
    throw new RangeError("Snapshot UNIQUE chunk is too large");
  }
  if (keyTokens.some((token, index) => index > 0 && token <= (keyTokens[index - 1] ?? ""))) {
    throw new TypeError("Snapshot UNIQUE tokens are not strict lexical order");
  }
  return {
    kind: "unique-chunk",
    namespaceId: validateStorageId(record.namespaceId, "Snapshot UNIQUE namespace ID"),
    generationId: validateStorageId(record.generationId, "Snapshot UNIQUE generation ID"),
    ordinal: canonicalOrdinal(record.ordinal, "Snapshot UNIQUE chunk ordinal"),
    keyTokens,
  };
}

function preparePostingItem(value: unknown): SnapshotPostingItem {
  const record = recordValue(value, "Snapshot posting item");
  if (record.kind === "posting-generation") {
    sameKeys(
      record,
      [
        "kind",
        "tableId",
        "ownerKind",
        "ownerId",
        "storageColumnId",
        "generationId",
        "coversVersion",
        "chunkCount",
        "totalTokens",
      ],
      "Snapshot posting generation",
    );
    if (record.ownerKind !== "fts-column" && record.ownerKind !== "secondary-index") {
      throw new TypeError("Snapshot posting owner kind is invalid");
    }
    const totalTokens = canonicalOrdinal(record.totalTokens, "Snapshot posting token total");
    const chunkCount = canonicalOrdinal(record.chunkCount, "Snapshot posting chunk count");
    if (chunkCount > MAX_FTS_BASE_CHUNKS) {
      throw new RangeError("Snapshot posting generation has too many chunks");
    }
    return {
      kind: "posting-generation",
      tableId: validateStorageId(record.tableId, "Snapshot posting table ID"),
      ownerKind: record.ownerKind,
      ownerId: validateStorageId(record.ownerId, "Snapshot posting owner ID"),
      storageColumnId: validateStorageId(
        record.storageColumnId,
        "Snapshot posting storage column ID",
      ),
      generationId: validateStorageId(record.generationId, "Snapshot posting generation ID"),
      coversVersion: canonicalOrdinal(record.coversVersion, "Snapshot posting covered version"),
      chunkCount,
      totalTokens,
    };
  }
  sameKeys(
    record,
    ["kind", "storageColumnId", "generationId", "ordinal", "postings"],
    "Snapshot posting chunk",
  );
  if (record.kind !== "posting-chunk" || !Array.isArray(record.postings)) {
    throw new TypeError("Snapshot posting chunk is invalid");
  }
  if (record.postings.length < 1 || record.postings.length > MAX_FTS_POSTINGS_PER_CHUNK) {
    throw new RangeError("Snapshot posting chunk count is out of bounds");
  }
  let rowIds = 0;
  let previousTerm = "";
  const postings = record.postings.map((raw, postingIndex) => {
    const posting = exactRecord(raw, ["term", "rowIds", "tf"], "Snapshot posting");
    if (
      typeof posting.term !== "string" ||
      posting.term.length > MAX_FTS_POSTING_TERM_CHARACTERS ||
      !Array.isArray(posting.rowIds) ||
      !Array.isArray(posting.tf) ||
      posting.rowIds.length === 0 ||
      posting.rowIds.length !== posting.tf.length ||
      (postingIndex > 0 && posting.term <= previousTerm)
    ) {
      throw new TypeError("Snapshot posting is not canonical");
    }
    assertWellFormedString(posting.term, "Snapshot posting term");
    previousTerm = posting.term;
    rowIds += posting.rowIds.length;
    if (rowIds > MAX_FTS_POSTING_ROW_IDS_PER_CHUNK) {
      throw new RangeError("Snapshot posting chunk has too many row IDs");
    }
    let previousRowId = 0n;
    const normalizedRowIds = posting.rowIds.map((rowId, index) => {
      if (
        typeof rowId !== "bigint" ||
        rowId < 1n ||
        rowId >= MAX_ROW_ID_EXCLUSIVE_END ||
        (index > 0 && rowId <= previousRowId)
      ) {
        throw new TypeError("Snapshot posting row IDs are not canonical");
      }
      previousRowId = rowId;
      return rowId;
    });
    const tf = posting.tf.map((count) => {
      if (
        !Number.isSafeInteger(count) ||
        (count as number) < 1 ||
        (count as number) > MAX_FTS_TOKENS_PER_DOCUMENT
      ) {
        throw new TypeError("Snapshot posting term frequency is invalid");
      }
      return count as number;
    });
    return { term: posting.term, rowIds: normalizedRowIds, tf };
  });
  return {
    kind: "posting-chunk",
    storageColumnId: validateStorageId(
      record.storageColumnId,
      "Snapshot posting storage column ID",
    ),
    generationId: validateStorageId(record.generationId, "Snapshot posting generation ID"),
    ordinal: canonicalOrdinal(record.ordinal, "Snapshot posting chunk ordinal"),
    postings,
  };
}

/** Decodes and strictly validates one canonical metadata frame before durable staging. */
export function decodeSnapshotMetadataItems(
  kind: Exclude<SnapshotFrameKind, "block">,
  payload: Uint8Array,
): SnapshotMetadataItem[] {
  return decodeSnapshotMetadataPage(payload).map((item) => {
    if (kind === "catalog-page") return prepareCatalogItem(item);
    if (kind === "segment-page") return prepareSegmentItem(item);
    if (kind === "transaction-page") return prepareTransactionItem(item);
    if (kind === "unique-page") return prepareUniqueItem(item);
    return preparePostingItem(item);
  });
}

class ChunkReader {
  #chunk: Uint8Array = new Uint8Array();
  #offset = 0;
  readonly #iterator: AsyncIterator<Uint8Array>;
  constructor(source: AsyncIterable<Uint8Array>) {
    this.#iterator = source[Symbol.asyncIterator]();
  }
  async #nextChunk(): Promise<boolean> {
    const next = await this.#iterator.next();
    if (next.done) return false;
    if (!(next.value instanceof Uint8Array) || next.value.byteLength === 0) {
      throw new RangeError("Snapshot source chunks must be non-empty Uint8Array values");
    }
    this.#chunk = next.value;
    this.#offset = 0;
    return true;
  }
  async read(length: number): Promise<Uint8Array> {
    const result = new Uint8Array(length);
    let written = 0;
    while (written < length) {
      if (this.#offset === this.#chunk.byteLength) {
        if (!(await this.#nextChunk())) throw new Error("Snapshot stream is truncated");
      }
      const count = Math.min(length - written, this.#chunk.byteLength - this.#offset);
      result.set(this.#chunk.subarray(this.#offset, this.#offset + count), written);
      this.#offset += count;
      written += count;
    }
    return result;
  }
  async done(): Promise<boolean> {
    if (this.#offset < this.#chunk.byteLength) return false;
    return !(await this.#nextChunk());
  }
  async close(): Promise<void> {
    await this.#iterator.return?.();
  }
}

/** Parses and verifies the bounded framed snapshot container without retaining prior frames. */
export async function* decodeSnapshotFrameStream(
  source: AsyncIterable<Uint8Array>,
): AsyncGenerator<SnapshotFrameStreamEntry> {
  const reader = new ChunkReader(source);
  let failure: unknown;
  try {
    const prefix = await reader.read(HEADER_PREFIX_BYTES);
    if (!MAGIC.every((byte, index) => prefix[index] === byte))
      throw new Error("Not a Minnow snapshot");
    const prefixView = new DataView(prefix.buffer);
    if (prefixView.getUint32(8, true) !== 1) throw new Error("Unsupported snapshot format version");
    const headerLength = prefixView.getUint32(12, true);
    if (headerLength < 1 || headerLength > MAX_HEADER_BYTES)
      throw new RangeError("Snapshot header is too large");
    const headerBytes = await reader.read(headerLength);
    if (crc32(headerBytes) !== prefixView.getUint32(16, true))
      throw new Error("Snapshot header checksum mismatch");
    let parsed: unknown;
    try {
      parsed = JSON.parse(textDecoder.decode(headerBytes));
    } catch {
      throw new TypeError("Snapshot header is not canonical JSON");
    }
    const header = prepareSnapshotFrameStreamHeader(parsed as SnapshotFrameStreamHeader);
    if (canonicalHeaderJson(header) !== textDecoder.decode(headerBytes)) {
      throw new TypeError("Snapshot header is not canonically encoded");
    }
    yield { type: "header", header };

    const expectedFrames = SNAPSHOT_FRAME_KINDS.reduce((total, kind) => {
      const next = total + header.kinds[kind].frameCount;
      if (!Number.isSafeInteger(next)) throw new RangeError("Snapshot frame count is too large");
      return next;
    }, 0);
    const observed = Object.fromEntries(
      SNAPSHOT_FRAME_KINDS.map((kind) => [kind, { frameCount: 0, itemCount: 0, storedBytes: 0 }]),
    ) as Record<SnapshotFrameKind, { frameCount: number; itemCount: number; storedBytes: number }>;
    let sequence = 0;
    let itemCount = 0;
    let storedBytes = 0;
    let checksum = 0;
    let previousKind = 0;
    while (sequence < expectedFrames) {
      const framePrefix = await reader.read(FRAME_PREFIX_BYTES);
      const view = new DataView(framePrefix.buffer);
      if (view.getUint32(0, true) !== FRAME_MARKER)
        throw new Error("Snapshot frame marker mismatch");
      const kindIndex = view.getUint32(4, true);
      const kind = SNAPSHOT_FRAME_KINDS[kindIndex];
      if (kind === undefined || kindIndex < previousKind)
        throw new Error("Snapshot frame order is invalid");
      previousKind = kindIndex;
      const actualSequence = Number(view.getBigUint64(8, true));
      if (!Number.isSafeInteger(actualSequence) || actualSequence !== sequence)
        throw new Error("Snapshot frame sequence is invalid");
      const frameItems = view.getUint32(16, true);
      if (frameItems < 1 || frameItems > MAX_SNAPSHOT_FRAME_ITEMS) {
        throw new RangeError("Snapshot frame item count is out of bounds");
      }
      const keyLength = view.getUint32(20, true);
      if (
        keyLength > MAX_STORAGE_ID_CHARACTERS * 3 ||
        (kind === "block" ? keyLength === 0 || frameItems !== 1 : keyLength !== 0)
      ) {
        throw new RangeError("Snapshot frame key is too large");
      }
      const payloadLength = Number(view.getBigUint64(24, true));
      if (!Number.isSafeInteger(payloadLength)) throw new RangeError("Snapshot frame is too large");
      const payloadLimit =
        kind === "block" ? MAX_STORED_BLOCK_BYTE_LENGTH : MAX_SNAPSHOT_METADATA_FRAME_BYTES;
      if (payloadLength < 1 || payloadLength > payloadLimit) {
        throw new RangeError("Snapshot frame payload length is out of bounds");
      }
      const expectedChecksum = view.getUint32(32, true);
      if (view.getUint32(36, true) !== 0)
        throw new TypeError("Snapshot frame reserved bits are set");
      const keyBytes = await reader.read(keyLength);
      const key = keyLength === 0 ? null : validateStorageId(stringFromUtf8(keyBytes));
      const payload = await reader.read(payloadLength);
      const frame: SnapshotFrame = {
        sequence,
        kind,
        itemCount: frameItems,
        key,
        payload,
        checksum: expectedChecksum,
      };
      snapshotFrameEnvelopeParts(frame);
      if (crc32(payload) !== expectedChecksum) throw new Error("Snapshot frame checksum mismatch");
      if (kind !== "block") {
        if (payload.byteLength < 5 || payload[0] !== 6) {
          throw new TypeError("Snapshot metadata frame must contain one canonical page array");
        }
        const encodedItems = new DataView(payload.buffer, payload.byteOffset + 1, 4).getUint32(
          0,
          true,
        );
        if (encodedItems !== frameItems) {
          throw new Error("Snapshot metadata frame item count does not match its payload");
        }
        if (decodeSnapshotMetadataItems(kind, payload).length !== frameItems) {
          throw new Error("Snapshot metadata frame item count is invalid");
        }
      }
      checksum = extendSnapshotFrameStreamChecksum(checksum, [framePrefix, keyBytes, payload]);
      itemCount = safeWhole(itemCount + frameItems, "Snapshot observed item count");
      storedBytes = safeWhole(storedBytes + payloadLength, "Snapshot observed stored bytes");
      const kindObserved = observed[kind];
      kindObserved.frameCount = safeWhole(
        kindObserved.frameCount + 1,
        `Snapshot ${kind} observed frame count`,
      );
      kindObserved.itemCount = safeWhole(
        kindObserved.itemCount + frameItems,
        `Snapshot ${kind} observed item count`,
      );
      kindObserved.storedBytes = safeWhole(
        kindObserved.storedBytes + payloadLength,
        `Snapshot ${kind} observed stored bytes`,
      );
      const kindExpected = header.kinds[kind];
      if (
        kindObserved.frameCount > kindExpected.frameCount ||
        kindObserved.itemCount > kindExpected.itemCount ||
        kindObserved.storedBytes > kindExpected.storedBytes
      ) {
        throw new Error(`Snapshot ${kind} frames exceed the declared summary`);
      }
      yield { type: "frame", frame };
      sequence = safeWhole(sequence + 1, "Snapshot frame sequence");
    }

    const footerBytes = await reader.read(FOOTER_BYTES);
    const footerView = new DataView(footerBytes.buffer);
    if (footerView.getUint32(0, true) !== FOOTER_MARKER)
      throw new Error("Snapshot footer marker mismatch");
    const footerFrameCount = Number(footerView.getBigUint64(4, true));
    const footerItemCount = Number(footerView.getBigUint64(12, true));
    const footerStoredBytes = Number(footerView.getBigUint64(20, true));
    if (
      !Number.isSafeInteger(footerFrameCount) ||
      !Number.isSafeInteger(footerItemCount) ||
      !Number.isSafeInteger(footerStoredBytes)
    ) {
      throw new RangeError("Snapshot footer totals are too large");
    }
    const footer: SnapshotFrameFooter = {
      frameCount: footerFrameCount,
      itemCount: footerItemCount,
      storedBytes: footerStoredBytes,
      checksum: footerView.getUint32(28, true),
    };
    if (footerBytes.subarray(32).some((byte) => byte !== 0))
      throw new TypeError("Snapshot footer reserved bits are set");
    if (
      footer.frameCount !== sequence ||
      footer.itemCount !== itemCount ||
      footer.storedBytes !== storedBytes ||
      footer.checksum !== checksum
    ) {
      throw new Error("Snapshot footer does not match its body");
    }
    for (const kind of SNAPSHOT_FRAME_KINDS) {
      const actual = observed[kind];
      const expected = header.kinds[kind];
      if (
        actual.frameCount !== expected.frameCount ||
        actual.itemCount !== expected.itemCount ||
        actual.storedBytes !== expected.storedBytes
      ) {
        throw new Error(`Snapshot ${kind} summary does not match its frames`);
      }
    }
    if (!(await reader.done())) throw new Error("Snapshot has trailing bytes");
    yield { type: "footer", footer };
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    try {
      await reader.close();
    } catch (closeError) {
      if (failure === undefined) {
        await Promise.reject(
          closeError instanceof Error ? closeError : new Error(String(closeError)),
        );
      }
    }
  }
}
