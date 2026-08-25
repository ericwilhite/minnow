import { crc32 } from "./checksum.js";
import { decodeValidatedColumn, encodeColumn } from "./column.js";
import { CompressionOutputLimitError, getCodec, getCompressionMemoryBound } from "./codecs.js";
import {
  assertBlockRowCount,
  MAX_PHYSICAL_COLUMN_BYTE_LENGTH,
  validatePhysicalColumn,
} from "./physical.js";
import type {
  BlockDescription,
  BlockMetadata,
  ColumnInput,
  Compression,
  DecodedBlock,
  DecodedPhysicalBlock,
  LogicalType,
  PhysicalColumnPayload,
  ValidatedPhysicalColumn,
} from "./types.js";

const MAGIC = Uint8Array.of(0x42, 0x52, 0x44, 0x42);
/**
 * Version 2 layout (all integers little-endian):
 *
 *    0  magic "BRDB"
 *    4  envelope checksum: crc32 over bytes [8, headerLength + metadataLength)
 *    8  format version (u16)
 *   10  header length (u16)
 *   12  logical type id (u8), 13 physical encoding id (u8), 14 codec id (u8), 15 flags (u8)
 *   16  row count (u32)
 *   20  null count (u32)
 *   24  metadata length (u32)
 *   28  encoded length (u32)
 *   32  stored length (u32)
 *   36  logical checksum: crc32 over the uncompressed encoded payload
 *   40  stored checksum: crc32 over the stored payload, before decompression
 *
 * The envelope checksum integrity-checks every header field and the metadata JSON, so header-only
 * reads (zone-map pruning, block inventories) can trust derived statistics without paying the
 * decompress-and-revalidate cost of the payload.
 */
/**
 * The first frozen block envelope version. Every persisted block carries it, and readers dispatch
 * by this value, so the number is a compatibility contract rather than an internal detail.
 *
 * Never change v2 decoding in place. A future writer gets a new number and keeps the v2 read path;
 * format-vectors.test.ts permanently freezes Minnow-owned v2 bytes.
 */
export const BLOCK_FORMAT_VERSION = 2;
const VERSION = BLOCK_FORMAT_VERSION;
export const BLOCK_HEADER_LENGTH = 44;
const HEADER_LENGTH = BLOCK_HEADER_LENGTH;
const ENVELOPE_CHECKSUM_START = 8;
const MAX_BLOCK_LENGTH = MAX_PHYSICAL_COLUMN_BYTE_LENGTH;
export const MAX_BLOCK_METADATA_BYTE_LENGTH = 1_024;
const MAX_METADATA_LENGTH = MAX_BLOCK_METADATA_BYTE_LENGTH;
/** Absolute persisted value ceiling: envelope + metadata + maximum stored payload. */
export const MAX_STORED_BLOCK_BYTE_LENGTH =
  BLOCK_HEADER_LENGTH + MAX_METADATA_LENGTH + MAX_PHYSICAL_COLUMN_BYTE_LENGTH;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

const typeIds: Record<LogicalType, number> = { boolean: 1, number: 2, string: 3, datetime: 4 };
const typesById = new Map(Object.entries(typeIds).map(([type, id]) => [id, type as LogicalType]));
// Codec 1 was a pre-freeze run-length prototype, removed because it was both slower and larger
// than raw on every measured column shape. Keep the id permanently reserved so it can never be
// confused with a different codec in diagnostic or development data.
const codecIds: Record<Compression, number> = { raw: 0, gzip: 2 };
const codecsById = new Map(
  Object.entries(codecIds).map(([codec, id]) => [id, codec as Compression]),
);

/** A codec expanded an otherwise valid physical column beyond the stored block hard limit. */
export class StoredBlockPayloadTooLargeError extends RangeError {
  constructor(readonly byteLength: number) {
    super("Stored block payload exceeds maximum byte length");
    this.name = "StoredBlockPayloadTooLargeError";
  }
}

/** Validates a codec result length without allocating the payload. */
export function assertStoredBlockPayloadByteLength(byteLength: number): void {
  if (!Number.isSafeInteger(byteLength) || byteLength < 0) {
    throw new RangeError("Invalid stored payload length");
  }
  if (byteLength > MAX_BLOCK_LENGTH) throw new StoredBlockPayloadTooLargeError(byteLength);
}

/**
 * Returns a conservative bound for the complete persisted block value: the fixed header, the
 * exact UTF-8 JSON metadata, and the selected codec's maximum stored payload.
 */
export function maximumPhysicalBlockByteLength(
  encodedByteLength: number,
  metadata: BlockMetadata,
  compression: Compression,
): number {
  assertLength(encodedByteLength, "encoded payload");
  const metadataByteLength = encodeCanonicalMetadata(metadata).byteLength;
  assertMetadataLength(metadataByteLength);
  const maximumStoredPayloadBytes = getCompressionMemoryBound(
    compression,
    encodedByteLength,
  ).maximumOutputBytes;
  const total = HEADER_LENGTH + metadataByteLength + maximumStoredPayloadBytes;
  if (!Number.isSafeInteger(total)) {
    throw new RangeError("Stored block byte bound exceeds the safe integer range");
  }
  return total;
}

export async function encodeBlock(
  input: ColumnInput,
  compression: Compression = "raw",
): Promise<Uint8Array> {
  assertBlockRowCount(input.values.length);
  const encoded = encodeColumn(input);
  return encodeValidatedPhysicalBlock(
    {
      type: input.type,
      rowCount: input.values.length,
      nullCount: encoded.nullCount,
      bytes: encoded.bytes,
      metadata: encoded.metadata,
    },
    compression,
  );
}

/** Encodes physical column bytes after validating their complete canonical v2 layout. */
export async function encodePhysicalBlock(
  input: PhysicalColumnPayload,
  compression: Compression = "raw",
): Promise<Uint8Array> {
  return encodeValidatedPhysicalBlock(validatePhysicalColumn(input), compression);
}

async function encodeValidatedPhysicalBlock(
  input: ValidatedPhysicalColumn,
  compression: Compression,
): Promise<Uint8Array> {
  // validatePhysicalColumn constructs this exact closed metadata shape. Avoid serializing,
  // parsing, and serializing it again on every write; the public sizing helper validates its
  // independently supplied metadata because it has no such provenance.
  const metadata = textEncoder.encode(JSON.stringify(input.metadata));
  // Checksum before the first possible async yield. The gzip codec also takes its input copy
  // synchronously when called, so caller mutation after encodePhysicalBlock returns cannot make
  // the checksum and compressed bytes observe different physical payloads.
  const logicalChecksum = crc32(input.bytes);
  // Avoid an async yield on raw blocks: validation and the copy into the immutable envelope
  // complete before a caller can mutate a physical input buffer. The gzip codec takes its own
  // input copy before yielding to the native stream.
  let stored: Uint8Array;
  try {
    stored =
      compression === "raw"
        ? input.bytes
        : await getCodec(compression).compress(input.bytes, MAX_BLOCK_LENGTH);
  } catch (error) {
    if (error instanceof CompressionOutputLimitError) {
      throw new StoredBlockPayloadTooLargeError(error.byteLength);
    }
    throw error;
  }
  assertMetadataLength(metadata.byteLength);
  assertLength(input.bytes.byteLength, "encoded payload");
  assertStoredBlockPayloadByteLength(stored.byteLength);

  const storedChecksum = compression === "raw" ? logicalChecksum : crc32(stored);

  const output = new Uint8Array(HEADER_LENGTH + metadata.byteLength + stored.byteLength);
  output.set(MAGIC);
  const view = new DataView(output.buffer);
  view.setUint16(8, VERSION, true);
  view.setUint16(10, HEADER_LENGTH, true);
  view.setUint8(12, typeIds[input.type]);
  view.setUint8(13, typeIds[input.type]); // the physical encoding mirrors the logical type
  view.setUint8(14, codecIds[compression]);
  view.setUint8(15, 0);
  view.setUint32(16, input.rowCount, true);
  view.setUint32(20, input.nullCount, true);
  view.setUint32(24, metadata.byteLength, true);
  view.setUint32(28, input.bytes.byteLength, true);
  view.setUint32(32, stored.byteLength, true);
  view.setUint32(36, logicalChecksum, true);
  view.setUint32(40, storedChecksum, true);
  output.set(metadata, HEADER_LENGTH);
  output.set(stored, HEADER_LENGTH + metadata.byteLength);
  view.setUint32(
    4,
    crc32(output.subarray(ENVELOPE_CHECKSUM_START, HEADER_LENGTH + metadata.byteLength)),
    true,
  );
  return output;
}

export async function decodeBlock(bytes: Uint8Array): Promise<DecodedBlock> {
  // Materialize raw values before the first async yield. Otherwise a caller could mutate the
  // input after its checksum passes but before decodeColumn reads it. The gzip codec takes an
  // input copy synchronously, so its awaited path has no equivalent gap.
  const description = verifyStoredBlock(bytes);
  const decoded =
    description.compression === "raw"
      ? validateDecodedPhysicalBlock(
          bytes.subarray(description.headerLength + description.metadataLength),
          description,
        )
      : await decodeVerifiedPhysicalBlock(bytes, description);
  return {
    description: decoded.description,
    column: decodeValidatedColumn(decoded.column),
  };
}

/**
 * Decompresses and checksum-verifies a block without materializing row values.
 *
 * For a raw block, `column.bytes` is a zero-copy view into `bytes`. Both views must be treated as
 * immutable for the lifetime of the result. Gzip results own a new decompressed allocation.
 */
export async function decodePhysicalBlock(bytes: Uint8Array): Promise<DecodedPhysicalBlock> {
  const description = verifyStoredBlock(bytes);
  if (description.compression === "raw") {
    return validateDecodedPhysicalBlock(
      bytes.subarray(description.headerLength + description.metadataLength),
      description,
    );
  }
  return decodeVerifiedPhysicalBlock(bytes, description);
}

async function decodeVerifiedPhysicalBlock(
  bytes: Uint8Array,
  description: BlockDescription,
): Promise<DecodedPhysicalBlock> {
  const stored = bytes.subarray(description.headerLength + description.metadataLength);
  const encoded = await getCodec(description.compression).decompress(
    stored,
    description.encodedLength,
  );
  // For raw blocks the stored and logical bytes are identical, and inspectBlock requires both
  // declared checksums to agree. verifyStoredBlock therefore performed the logical check too,
  // avoiding a second full scan on the default codec.
  if (description.compression !== "raw" && crc32(encoded) !== description.checksum) {
    throw new Error("Block logical checksum mismatch");
  }
  return validateDecodedPhysicalBlock(encoded, description);
}

function validateDecodedPhysicalBlock(
  encoded: Uint8Array,
  description: BlockDescription,
): DecodedPhysicalBlock {
  const column = validatePhysicalColumn({
    type: description.type,
    rowCount: description.rowCount,
    bytes: encoded,
  });
  if (column.nullCount !== description.nullCount) throw new Error("Block null count mismatch");
  if (!metadataEquals(column.metadata, description.metadata)) {
    throw new Error("Block metadata does not match physical payload");
  }
  return {
    description,
    column,
  };
}

/**
 * Verifies every stored byte without decompressing or copying the payload.
 *
 * Snapshot import and OPFS recovery use this stronger operation before accepting a block.
 * Planning and pruning should continue to use `inspectBlock`, which deliberately reads only the
 * fixed header and metadata envelope.
 */
export function verifyStoredBlock(bytes: Uint8Array): BlockDescription {
  const description = inspectBlock(bytes);
  const storedStart = bytes.byteLength - description.storedLength;
  if (crc32(bytes.subarray(storedStart)) !== description.storedChecksum) {
    throw new Error("Block stored payload checksum mismatch");
  }
  return description;
}

export function inspectBlock(bytes: Uint8Array): BlockDescription {
  // Only magic and version are common across versions. Dispatch before interpreting any later
  // offsets, so future envelopes can grow or replace the v2 header without weakening its reader.
  if (bytes.byteLength < 10) throw new Error("Truncated block header");
  if (!MAGIC.every((byte, index) => bytes[index] === byte)) throw new Error("Invalid block magic");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const formatVersion = view.getUint16(8, true);
  switch (formatVersion) {
    case 2:
      return inspectV2Block(bytes);
    default:
      throw new Error(`Unsupported block version ${String(formatVersion)}`);
  }
}

/** Permanent reader for the frozen v2 envelope. */
function inspectV2Block(bytes: Uint8Array): BlockDescription {
  if (bytes.byteLength < HEADER_LENGTH) throw new Error("Truncated block header");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const formatVersion = view.getUint16(8, true);
  if (view.getUint16(10, true) !== HEADER_LENGTH) throw new Error("Invalid block header length");
  const metadataLength = view.getUint32(24, true);
  const encodedLength = view.getUint32(28, true);
  const storedLength = view.getUint32(32, true);
  assertMetadataLength(metadataLength);
  assertLength(encodedLength, "encoded payload");
  assertLength(storedLength, "stored payload");
  if (HEADER_LENGTH + metadataLength + storedLength !== bytes.byteLength) {
    throw new Error("Block length mismatch");
  }
  // Verified before any header field is trusted: the envelope checksum covers every field after
  // itself plus the metadata JSON, making header-only reads safe against silent corruption.
  const envelope = crc32(bytes.subarray(ENVELOPE_CHECKSUM_START, HEADER_LENGTH + metadataLength));
  if (envelope !== view.getUint32(4, true)) {
    throw new Error("Block envelope checksum mismatch");
  }
  const rowCount = view.getUint32(16, true);
  assertBlockRowCount(rowCount);
  const type = typesById.get(view.getUint8(12));
  if (type === undefined) throw new Error("Unknown logical type");
  if (view.getUint8(13) !== typeIds[type]) throw new Error("Unsupported physical encoding");
  const codecId = view.getUint8(14);
  const compression = codecsById.get(codecId);
  if (compression === undefined) {
    throw new Error(
      codecId === 1 ? "Block uses reserved compression codec id 1" : "Unknown compression codec",
    );
  }
  if (view.getUint8(15) !== 0) throw new Error("Unsupported mandatory block flags");
  const checksum = view.getUint32(36, true);
  const storedChecksum = view.getUint32(40, true);
  if (compression === "raw" && checksum !== storedChecksum) {
    throw new Error("Raw block checksums do not match");
  }
  if (compression === "raw" && encodedLength !== storedLength) {
    throw new Error("Raw block lengths do not match");
  }
  const nullCount = view.getUint32(20, true);
  if (nullCount > rowCount) throw new Error("Null count exceeds row count");
  let parsedMetadata: unknown;
  let metadataJson: string;
  try {
    metadataJson = textDecoder.decode(
      bytes.subarray(HEADER_LENGTH, HEADER_LENGTH + metadataLength),
    );
    parsedMetadata = JSON.parse(metadataJson) as unknown;
  } catch {
    throw new Error("Invalid block metadata");
  }
  const metadata = parseMetadata(parsedMetadata);
  // There is one canonical metadata representation. Rejecting alternate key order, duplicate
  // keys, unknown fields, and insignificant whitespace keeps byte fixtures meaningful and avoids
  // multiple envelopes describing the same block.
  if (JSON.stringify(metadata) !== metadataJson) throw new Error("Non-canonical block metadata");
  return {
    formatVersion,
    headerLength: HEADER_LENGTH,
    metadataLength,
    type,
    compression,
    rowCount,
    nullCount,
    encodedLength,
    storedLength,
    checksum,
    storedChecksum,
    metadata,
  };
}

function assertMetadataLength(length: number): void {
  if (!Number.isSafeInteger(length) || length < 0 || length > MAX_METADATA_LENGTH) {
    throw new RangeError("Invalid metadata length");
  }
}

function assertLength(length: number, name: string): void {
  if (!Number.isSafeInteger(length) || length < 0 || length > MAX_BLOCK_LENGTH) {
    throw new RangeError(`Invalid ${name} length`);
  }
}

function encodeCanonicalMetadata(metadata: BlockMetadata): Uint8Array {
  let source: unknown;
  try {
    source = JSON.stringify(metadata);
  } catch {
    throw new TypeError("Block metadata must be canonical JSON");
  }
  if (typeof source !== "string") throw new TypeError("Block metadata must be canonical JSON");
  let canonical: BlockMetadata;
  try {
    canonical = parseMetadata(JSON.parse(source) as unknown);
  } catch {
    throw new TypeError("Block metadata must match the supported canonical shape");
  }
  if (JSON.stringify(canonical) !== source) {
    throw new TypeError("Block metadata must match the supported canonical shape");
  }
  return textEncoder.encode(source);
}

function parseMetadata(value: unknown): BlockMetadata {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Invalid block metadata shape");
  }
  const zoneMap = Reflect.get(value, "zoneMap") as unknown;
  if (zoneMap !== undefined) {
    if (
      typeof zoneMap !== "object" ||
      zoneMap === null ||
      !Number.isFinite(Reflect.get(zoneMap, "min")) ||
      !Number.isFinite(Reflect.get(zoneMap, "max"))
    ) {
      throw new Error("Invalid zone map");
    }
    const min = Reflect.get(zoneMap, "min") as number;
    const max = Reflect.get(zoneMap, "max") as number;
    if (min > max) throw new Error("Invalid zone map");
    return { zoneMap: { min, max } };
  }
  return {};
}

function metadataEquals(left: BlockMetadata, right: BlockMetadata): boolean {
  if (left.zoneMap === undefined || right.zoneMap === undefined) {
    return left.zoneMap === undefined && right.zoneMap === undefined;
  }
  return left.zoneMap.min === right.zoneMap.min && left.zoneMap.max === right.zoneMap.max;
}
