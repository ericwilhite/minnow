/**
 * Database snapshots: one committed version of a database, copied out as a single portable
 * byte array and loaded back into any block store.
 *
 * A snapshot is a *published* view, not a backup of a store's internals. It carries the block
 * bytes the current manifest points at, the catalog needed to read them (tables, segments, and
 * the committed transactions segment visibility resolves through), the counters a later write
 * has to continue from, and the unique-key membership a later write has to conflict against.
 * It deliberately drops everything transient — leases, spill pages, garbage-collection and
 * compaction jobs, in-flight transactions — and it drops version history: the snapshot restores
 * as a single checkpoint, so a 10,000-commit database loads as one clean version.
 *
 * The container is a small JSON header followed by the block payloads laid end to end. Blocks
 * are already self-describing and doubly CRC-checked (see `../block-format/block.ts`), so the
 * container adds only a checksum over its own header and lets `inspectBlock` authenticate each
 * payload without decompressing any of them.
 */
import { crc32, gzipCodec, inspectBlock } from "../block-format/index.js";
import { validateSecondaryIndexes, validateTableColumns } from "./types.js";
import type {
  FtsPosting,
  RowIdSpan,
  SegmentRecord,
  TableRecord,
  TransactionRecord,
} from "./types.js";

/** ASCII `MNWSNAP1`, matching the block format's habit of a readable magic. */
const MAGIC = Uint8Array.from([0x4d, 0x4e, 0x57, 0x53, 0x4e, 0x41, 0x50, 0x31]);
/**
 * magic(8) + formatVersion(4) + storedHeaderLength(4) + headerChecksum(4) + headerLength(4)
 * + headerCodec(4). All little-endian, matching the block format.
 */
const PREFIX_LENGTH = 28;
const HEADER_CODEC_NONE = 0;
const HEADER_CODEC_GZIP = 1;

export const SNAPSHOT_FORMAT_VERSION = 1;

/** One indexed column's full-text base, as it stood at the snapshot's version. */
export interface SnapshotFtsIndex {
  columnId: string;
  coversVersion: number;
  totalTokens: number;
  /** Term-range partitioned, term-sorted within each chunk. */
  chunks: FtsPosting[][];
}

export interface SnapshotTable {
  record: TableRecord;
  /** The next row ID a write should reserve; below it, IDs may already be in use. */
  nextRowId: bigint;
  autoIncrement: Array<{ columnId: string; next: bigint }>;
  /** Complete unique-key membership, so a write after loading still conflicts correctly. */
  uniqueKeyTokens: string[];
  /** Complete membership for every durable UNIQUE secondary index. */
  secondaryUniqueKeys?: Array<{ indexId: string; keyTokens: string[] }>;
  fts: SnapshotFtsIndex[];
}

export interface DatabaseSnapshot {
  /** The manifest version this snapshot captured, which the restored database keeps. */
  version: number;
  createdAt: string;
  tables: SnapshotTable[];
  segments: SegmentRecord[];
  /** Committed transactions only; segment visibility resolves through `committedVersion`. */
  transactions: TransactionRecord[];
  blocks: Array<{ id: string; bytes: Uint8Array }>;
}

/** Validates catalog identities and complete uniqueness membership before a snapshot is loaded. */
export function validateSnapshotCatalog(
  tables: ReadonlyArray<{
    record: TableRecord;
    uniqueKeyTokens: readonly string[];
    secondaryUniqueKeys?: ReadonlyArray<{ indexId: string; keyTokens: readonly string[] }>;
  }>,
): void {
  const indexNames = new Set<string>();
  for (const table of tables) {
    validateTableColumns(table.record.columns);
    validateSecondaryIndexes(table.record);
    if (new Set(table.uniqueKeyTokens).size !== table.uniqueKeyTokens.length) {
      throw new Error(`Snapshot repeats a table unique key: ${table.record.name}`);
    }
    if (table.record.uniqueKeyColumnId === undefined && table.uniqueKeyTokens.length > 0) {
      throw new Error(`Snapshot has orphaned table unique keys: ${table.record.name}`);
    }
    const memberships = new Set<string>();
    for (const entry of table.secondaryUniqueKeys ?? []) {
      if (memberships.has(entry.indexId)) {
        throw new Error(`Snapshot repeats UNIQUE-index membership: ${entry.indexId}`);
      }
      memberships.add(entry.indexId);
      const index = table.record.secondaryIndexes?.[entry.indexId];
      if (index?.unique !== true || index.uniqueEnforced !== true) {
        throw new Error(`Snapshot has orphaned UNIQUE-index membership: ${entry.indexId}`);
      }
      if (new Set(entry.keyTokens).size !== entry.keyTokens.length) {
        throw new Error(`Snapshot repeats a UNIQUE-index key: ${index.name}`);
      }
    }
    for (const [indexId, index] of Object.entries(table.record.secondaryIndexes ?? {})) {
      if (indexNames.has(index.name)) throw new TypeError(`Index already exists: ${index.name}`);
      indexNames.add(index.name);
      if (index.uniqueEnforced === true && !memberships.has(indexId)) {
        throw new Error(`Snapshot is missing UNIQUE-index membership: ${index.name}`);
      }
    }
  }
}

/** Where a snapshot load has got to, for a progress bar over a multi-megabyte file. */
export interface SnapshotLoadProgress {
  phase: "blocks" | "catalog" | "done";
  writtenBytes: number;
  totalBytes: number;
}

/**
 * Where a snapshot export has got to. `reading` covers copying the version out of the store and
 * encoding it, and reports no byte counts because the encoded length is not known until it is
 * finished. `transfer` happens only when the database lives in a worker and the bytes are handed
 * back a chunk at a time; an in-page database goes straight from `reading` to `done`.
 */
export interface SnapshotExportProgress {
  phase: "reading" | "transfer" | "done";
  transferredBytes: number;
  /** Zero while reading; the encoded length once it is known. */
  totalBytes: number;
}

/** What a header-only read reports, without touching a single block payload. */
export interface SnapshotSummary {
  formatVersion: number;
  version: number;
  createdAt: string;
  tableCount: number;
  blockCount: number;
  /** Total byte length of the block payloads, excluding the header. */
  payloadBytes: number;
  byteLength: number;
}

// The wire shapes below exist because the records carry bigints, which JSON cannot represent.
// Every bigint crosses as a decimal string; full-text row IDs, which are dense and ascending,
// cross as a first value plus small deltas so a posting list stays compact.

interface WireRowIdSpan {
  rowStart: number;
  rowCount: number;
  rowIdStart: string;
}

type WireSegment = Omit<SegmentRecord, "rowIdStart" | "rowIdEndExclusive" | "rowIdSpans"> & {
  rowIdStart: string;
  rowIdEndExclusive: string;
  rowIdSpans?: WireRowIdSpan[];
};

interface WirePosting {
  term: string;
  /** First row ID as a decimal string, then ascending gaps. */
  first: string;
  deltas: number[];
  tf: number[];
}

interface WireTable {
  record: TableRecord;
  nextRowId: string;
  autoIncrement: Array<{ columnId: string; next: string }>;
  uniqueKeyTokens: string[];
  secondaryUniqueKeys?: Array<{ indexId: string; keyTokens: string[] }>;
  fts: Array<{
    columnId: string;
    coversVersion: number;
    totalTokens: number;
    chunks: WirePosting[][];
  }>;
}

interface SnapshotHeader {
  formatVersion: number;
  version: number;
  createdAt: string;
  blocks: Array<{ id: string; length: number }>;
  tables: WireTable[];
  segments: WireSegment[];
  transactions: TransactionRecord[];
}

function encodeSegment(segment: SegmentRecord): WireSegment {
  const { rowIdStart, rowIdEndExclusive, rowIdSpans, ...rest } = segment;
  return {
    ...rest,
    rowIdStart: rowIdStart.toString(),
    rowIdEndExclusive: rowIdEndExclusive.toString(),
    ...(rowIdSpans === undefined
      ? {}
      : {
          rowIdSpans: rowIdSpans.map((span) => ({
            rowStart: span.rowStart,
            rowCount: span.rowCount,
            rowIdStart: span.rowIdStart.toString(),
          })),
        }),
  };
}

function decodeSegment(segment: WireSegment): SegmentRecord {
  const { rowIdStart, rowIdEndExclusive, rowIdSpans, ...rest } = segment;
  const spans: RowIdSpan[] | undefined = rowIdSpans?.map((span) => ({
    rowStart: span.rowStart,
    rowCount: span.rowCount,
    rowIdStart: parseBigInt(span.rowIdStart, "segment row-ID span"),
  }));
  return {
    ...rest,
    rowIdStart: parseBigInt(rowIdStart, "segment row-ID start"),
    rowIdEndExclusive: parseBigInt(rowIdEndExclusive, "segment row-ID end"),
    ...(spans === undefined ? {} : { rowIdSpans: spans }),
  };
}

function encodePosting(posting: FtsPosting): WirePosting {
  const [first, ...rest] = posting.rowIds;
  if (first === undefined) return { term: posting.term, first: "0", deltas: [], tf: posting.tf };
  const deltas: number[] = [];
  let previous = first;
  for (const rowId of rest) {
    const gap = rowId - previous;
    if (gap > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new RangeError("Full-text row-ID gap is too large to snapshot");
    }
    deltas.push(Number(gap));
    previous = rowId;
  }
  return { term: posting.term, first: first.toString(), deltas, tf: posting.tf };
}

function decodePosting(posting: WirePosting): FtsPosting {
  const rowIds: bigint[] = [];
  if (posting.tf.length > 0) {
    let current = parseBigInt(posting.first, "full-text row ID");
    rowIds.push(current);
    for (const delta of posting.deltas) {
      current += BigInt(delta);
      rowIds.push(current);
    }
  }
  return { term: posting.term, rowIds, tf: posting.tf };
}

function parseBigInt(value: string, context: string): bigint {
  if (!/^-?\d+$/.test(value)) throw new Error(`Snapshot has an invalid ${context}: ${value}`);
  return BigInt(value);
}

/**
 * Serializes a snapshot. Block bytes are copied verbatim — they are already compressed and
 * checksummed, so the container never re-encodes or re-compresses them. The header is gzipped,
 * because it is JSON and its bulk is full-text posting lists and segment records, which compress
 * by roughly four to one.
 */
export async function encodeSnapshot(snapshot: DatabaseSnapshot): Promise<Uint8Array> {
  const header: SnapshotHeader = {
    formatVersion: SNAPSHOT_FORMAT_VERSION,
    version: snapshot.version,
    createdAt: snapshot.createdAt,
    blocks: snapshot.blocks.map((block) => ({ id: block.id, length: block.bytes.byteLength })),
    tables: snapshot.tables.map((table) => ({
      record: table.record,
      nextRowId: table.nextRowId.toString(),
      autoIncrement: table.autoIncrement.map((entry) => ({
        columnId: entry.columnId,
        next: entry.next.toString(),
      })),
      uniqueKeyTokens: table.uniqueKeyTokens,
      ...(table.secondaryUniqueKeys === undefined
        ? {}
        : { secondaryUniqueKeys: table.secondaryUniqueKeys }),
      fts: table.fts.map((index) => ({
        columnId: index.columnId,
        coversVersion: index.coversVersion,
        totalTokens: index.totalTokens,
        chunks: index.chunks.map((chunk) => chunk.map(encodePosting)),
      })),
    })),
    segments: snapshot.segments.map(encodeSegment),
    transactions: snapshot.transactions,
  };

  const json = new TextEncoder().encode(JSON.stringify(header));
  const stored = await gzipCodec.compress(json);
  const payloadBytes = snapshot.blocks.reduce((total, block) => total + block.bytes.byteLength, 0);
  const bytes = new Uint8Array(PREFIX_LENGTH + stored.byteLength + payloadBytes);
  bytes.set(MAGIC, 0);
  const view = new DataView(bytes.buffer);
  view.setUint32(8, SNAPSHOT_FORMAT_VERSION, true);
  view.setUint32(12, stored.byteLength, true);
  view.setUint32(16, crc32(stored), true);
  view.setUint32(20, json.byteLength, true);
  view.setUint32(24, HEADER_CODEC_GZIP, true);
  bytes.set(stored, PREFIX_LENGTH);

  let offset = PREFIX_LENGTH + stored.byteLength;
  for (const block of snapshot.blocks) {
    bytes.set(block.bytes, offset);
    offset += block.bytes.byteLength;
  }
  return bytes;
}

async function readHeader(
  bytes: Uint8Array,
): Promise<{ header: SnapshotHeader; payloadOffset: number }> {
  if (bytes.byteLength < PREFIX_LENGTH) throw new Error("Truncated snapshot header");
  if (!MAGIC.every((byte, index) => bytes[index] === byte)) {
    throw new Error("Not a Minnow snapshot");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const formatVersion = view.getUint32(8, true);
  if (formatVersion !== SNAPSHOT_FORMAT_VERSION) {
    throw new Error(`Unsupported snapshot version ${String(formatVersion)}`);
  }
  const storedLength = view.getUint32(12, true);
  const payloadOffset = PREFIX_LENGTH + storedLength;
  if (payloadOffset > bytes.byteLength) throw new Error("Truncated snapshot header");
  const stored = bytes.subarray(PREFIX_LENGTH, payloadOffset);
  // Checked before the header is decompressed, let alone parsed: a damaged header must fail
  // as a checksum mismatch rather than as whatever gzip or JSON happens to say about it.
  if (crc32(stored) !== view.getUint32(16, true)) {
    throw new Error("Snapshot header checksum mismatch");
  }
  const codec = view.getUint32(24, true);
  if (codec !== HEADER_CODEC_NONE && codec !== HEADER_CODEC_GZIP) {
    throw new Error(`Unsupported snapshot header codec ${String(codec)}`);
  }
  const json =
    codec === HEADER_CODEC_GZIP
      ? await gzipCodec.decompress(stored, view.getUint32(20, true))
      : stored;
  const header = JSON.parse(new TextDecoder().decode(json)) as SnapshotHeader;
  return { header, payloadOffset };
}

/**
 * Reads only the header. Cheap enough to run against a multi-hundred-megabyte file before
 * deciding whether to load it.
 */
export async function readSnapshotSummary(bytes: Uint8Array): Promise<SnapshotSummary> {
  const { header, payloadOffset } = await readHeader(bytes);
  return {
    formatVersion: header.formatVersion,
    version: header.version,
    createdAt: header.createdAt,
    tableCount: header.tables.length,
    blockCount: header.blocks.length,
    payloadBytes: bytes.byteLength - payloadOffset,
    byteLength: bytes.byteLength,
  };
}

/**
 * Parses a snapshot and authenticates every block. `inspectBlock` verifies each payload's magic,
 * version, declared lengths, and envelope checksum without decompressing it, so a corrupt file
 * fails here rather than mid-query.
 */
export async function decodeSnapshot(bytes: Uint8Array): Promise<DatabaseSnapshot> {
  const { header, payloadOffset } = await readHeader(bytes);
  const declared = header.blocks.reduce((total, block) => total + block.length, 0);
  if (payloadOffset + declared !== bytes.byteLength) {
    throw new Error("Snapshot length mismatch");
  }

  const blocks: Array<{ id: string; bytes: Uint8Array }> = [];
  let offset = payloadOffset;
  for (const block of header.blocks) {
    const payload = bytes.subarray(offset, offset + block.length);
    inspectBlock(payload);
    blocks.push({ id: block.id, bytes: payload });
    offset += block.length;
  }

  const present = new Set(blocks.map((block) => block.id));
  validateSnapshotCatalog(header.tables);
  const segments = header.segments.map(decodeSegment);
  for (const segment of segments) {
    for (const ids of Object.values(segment.columnBlockIds)) {
      for (const id of ids) {
        if (!present.has(id)) throw new Error(`Snapshot segment references a missing block: ${id}`);
      }
    }
  }

  return {
    version: header.version,
    createdAt: header.createdAt,
    tables: header.tables.map((table) => ({
      record: table.record,
      nextRowId: parseBigInt(table.nextRowId, "row-ID counter"),
      autoIncrement: table.autoIncrement.map((entry) => ({
        columnId: entry.columnId,
        next: parseBigInt(entry.next, "auto-increment counter"),
      })),
      uniqueKeyTokens: table.uniqueKeyTokens,
      ...(table.secondaryUniqueKeys === undefined
        ? {}
        : { secondaryUniqueKeys: table.secondaryUniqueKeys }),
      fts: table.fts.map((index) => ({
        columnId: index.columnId,
        coversVersion: index.coversVersion,
        totalTokens: index.totalTokens,
        chunks: index.chunks.map((chunk) => chunk.map(decodePosting)),
      })),
    })),
    segments,
    transactions: header.transactions,
    blocks,
  };
}

/**
 * Shared by both stores' exporters: given the manifest's live block set, decide which segments
 * and transactions the snapshot needs.
 *
 * A segment is live when every block it references survives in the manifest — compaction
 * supersedes a segment by dropping its blocks, so that test is exactly the reader's. A
 * transaction is needed when a live segment names it, and only committed transactions qualify:
 * an active one's artifacts are unpublished and would dangle. Their pending lists are cleared,
 * because after the restore the manifest is the only root anything needs.
 *
 * `committedVersion` is preserved exactly, and the restored database keeps the captured version
 * number rather than renumbering to 1. Segments sort by `logicalOrder ?? committedVersion`, so
 * flattening those versions would let an update fold ahead of the insert it patches; and change
 * detection compares committed versions against a snapshot version, which would misread any
 * transaction numbered above the manifest.
 */
export function selectLiveRecords(input: {
  liveBlockIds: ReadonlySet<string>;
  segments: readonly SegmentRecord[];
  transactions: readonly TransactionRecord[];
  version: number;
}): { segments: SegmentRecord[]; transactions: TransactionRecord[] } {
  const committed = new Map(
    input.transactions
      .filter(
        (record) =>
          record.status === "committed" &&
          record.committedVersion !== null &&
          record.committedVersion <= input.version,
      )
      .map((record) => [record.id, record]),
  );

  const segments = input.segments.filter((segment) => {
    if (!committed.has(segment.transactionId)) return false;
    return Object.values(segment.columnBlockIds)
      .flat()
      .every((id) => input.liveBlockIds.has(id));
  });

  const usedTransactionIds = new Set(segments.map((segment) => segment.transactionId));
  const transactions = [...committed.values()]
    .filter((record) => usedTransactionIds.has(record.id))
    .map((record) => ({ ...record, pendingBlockIds: [], pendingSegmentIds: [] }));

  return { segments, transactions };
}
