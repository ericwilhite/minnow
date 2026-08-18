import { crc32 } from "./checksum.js";
import { decodeColumn, encodeColumn } from "./column.js";
import { getCodec, getCompressionMemoryBound } from "./codecs.js";
import { MAX_PHYSICAL_COLUMN_BYTE_LENGTH, validatePhysicalColumn } from "./physical.js";
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
 * Version 1 layout (all integers little-endian):
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
 *   36  payload checksum: crc32 over the uncompressed encoded payload
 *
 * The envelope checksum authenticates every header field and the metadata JSON, so header-only
 * reads (zone-map pruning, block inventories) can trust derived statistics without paying the
 * decompress-and-revalidate cost of the payload.
 */
/**
 * The block envelope version. Every persisted block carries it, and `readBlock` refuses any
 * value it does not recognize -- so this number is a compatibility contract with every database
 * already sitting in a user's browser, not an internal detail.
 *
 * Changing it means older blocks stop being readable unless a read path for them is kept. Before
 * changing it, run `npm run fixture:format` on the *current* build to freeze what this version
 * writes; format-compatibility.test.ts then holds the new build to still reading it.
 */
export const BLOCK_FORMAT_VERSION = 1;
const VERSION = BLOCK_FORMAT_VERSION;
const HEADER_LENGTH = 40;
const ENVELOPE_CHECKSUM_START = 8;
const MAX_BLOCK_LENGTH = MAX_PHYSICAL_COLUMN_BYTE_LENGTH;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

const typeIds: Record<LogicalType, number> = { boolean: 1, number: 2, string: 3, datetime: 4 };
const typesById = new Map(Object.entries(typeIds).map(([type, id]) => [id, type as LogicalType]));
// Codec 1 was run-length encoding, removed because it was both slower and larger than raw on
// every column shape measured. The ids of the survivors do not move: renumbering would make an
// old block decode as the wrong codec instead of failing.
const codecIds: Record<Compression, number> = { raw: 0, gzip: 2 };
const codecsById = new Map(
  Object.entries(codecIds).map(([codec, id]) => [id, codec as Compression]),
);

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
  let serializedMetadata: unknown;
  try {
    serializedMetadata = JSON.stringify(metadata);
  } catch {
    throw new TypeError("Block metadata must be JSON serializable");
  }
  if (typeof serializedMetadata !== "string") {
    throw new TypeError("Block metadata must be JSON serializable");
  }
  const metadataByteLength = textEncoder.encode(serializedMetadata).byteLength;
  assertLength(metadataByteLength, "metadata");
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
  if (input.values.length > 0xffffffff) throw new RangeError("Too many rows in one block");
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

/** Encodes physical column bytes after validating their complete version-zero layout. */
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
  const metadata = textEncoder.encode(JSON.stringify(input.metadata));
  const stored = await getCodec(compression).compress(input.bytes);
  assertLength(metadata.byteLength, "metadata");
  assertLength(input.bytes.byteLength, "encoded payload");
  assertLength(stored.byteLength, "stored payload");

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
  view.setUint32(36, crc32(input.bytes), true);
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
  const decoded = await decodePhysicalBlock(bytes);
  return {
    description: decoded.description,
    column: decodeColumn(
      decoded.description.type,
      decoded.column.bytes,
      decoded.description.rowCount,
    ),
  };
}

/**
 * Decompresses and checksum-verifies a block without materializing row values.
 * The returned physical byte array is owned by the result.
 */
export async function decodePhysicalBlock(bytes: Uint8Array): Promise<DecodedPhysicalBlock> {
  const description = inspectBlock(bytes);
  const metadataLength = bytes.byteLength - HEADER_LENGTH - description.storedLength;
  const stored = bytes.subarray(HEADER_LENGTH + metadataLength);
  const encoded = await getCodec(description.compression).decompress(
    stored,
    description.encodedLength,
  );
  if (crc32(encoded) !== description.checksum) throw new Error("Block checksum mismatch");
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

export function inspectBlock(bytes: Uint8Array): BlockDescription {
  if (bytes.byteLength < HEADER_LENGTH) throw new Error("Truncated block header");
  if (!MAGIC.every((byte, index) => bytes[index] === byte)) throw new Error("Invalid block magic");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const formatVersion = view.getUint16(8, true);
  if (formatVersion !== VERSION) {
    throw new Error(`Unsupported block version ${String(formatVersion)}`);
  }
  if (view.getUint16(10, true) !== HEADER_LENGTH) throw new Error("Invalid block header length");
  const metadataLength = view.getUint32(24, true);
  const encodedLength = view.getUint32(28, true);
  const storedLength = view.getUint32(32, true);
  assertLength(metadataLength, "metadata");
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
  const type = typesById.get(view.getUint8(12));
  if (type === undefined) throw new Error("Unknown logical type");
  if (view.getUint8(13) !== typeIds[type]) throw new Error("Unsupported physical encoding");
  const codecId = view.getUint8(14);
  const compression = codecsById.get(codecId);
  if (compression === undefined) {
    throw new Error(
      codecId === 1
        ? "Block uses the removed RLE codec; rewrite it as raw or gzip with an older build"
        : "Unknown compression codec",
    );
  }
  if (view.getUint8(15) !== 0) throw new Error("Unsupported mandatory block flags");
  const rowCount = view.getUint32(16, true);
  const nullCount = view.getUint32(20, true);
  if (nullCount > rowCount) throw new Error("Null count exceeds row count");
  let parsedMetadata: unknown;
  try {
    parsedMetadata = JSON.parse(
      textDecoder.decode(bytes.subarray(HEADER_LENGTH, HEADER_LENGTH + metadataLength)),
    ) as unknown;
  } catch {
    throw new Error("Invalid block metadata");
  }
  const metadata = parseMetadata(parsedMetadata);
  return {
    formatVersion,
    type,
    compression,
    rowCount,
    nullCount,
    encodedLength,
    storedLength,
    checksum: view.getUint32(36, true),
    metadata,
  };
}

function assertLength(length: number, name: string): void {
  if (!Number.isSafeInteger(length) || length < 0 || length > MAX_BLOCK_LENGTH) {
    throw new RangeError(`Invalid ${name} length`);
  }
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
