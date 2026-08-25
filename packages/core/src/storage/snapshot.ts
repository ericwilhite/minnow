/** Public snapshot API over the bounded framed v1 container. */
export * from "./snapshot-stream.js";

import { SNAPSHOT_FRAME_KINDS, type SnapshotFrameStreamHeader } from "./types.js";
import { decodeSnapshotFrameStream } from "./snapshot-stream.js";

export const SNAPSHOT_FORMAT_VERSION = 1;

/** Export and worker-transport chunk ceiling; direct imports accept any non-empty source chunk. */
export const MAX_SNAPSHOT_STREAM_CHUNK_BYTES = 1024 * 1024;

export interface SnapshotLoadProgress {
  phase: "blocks" | "catalog" | "done";
  writtenBytes: number;
  totalBytes: number;
}

export interface SnapshotExportProgress {
  phase: "reading" | "transfer" | "done";
  transferredBytes: number;
  /** Zero while streaming; the exact encoded length is known by the byte-array wrapper. */
  totalBytes: number;
}

/** Header summary used by restore confirmation UIs without retaining database metadata. */
export interface SnapshotSummary {
  formatVersion: number;
  version: number;
  createdAt: string;
  tableCount: number;
  blockCount: number;
  /** Stored body payload bytes, excluding frame envelopes, header, and footer. */
  payloadBytes: number;
  byteLength: number;
}

/**
 * Reads and validates the canonical header from a materialized snapshot. The decoder closes its
 * source immediately after the header; body checks run during import, where frames are consumed.
 */
export async function readSnapshotSummary(bytes: Uint8Array): Promise<SnapshotSummary> {
  if (!(bytes instanceof Uint8Array) || bytes.byteLength === 0) {
    throw new TypeError("Snapshot bytes must be a non-empty Uint8Array");
  }
  const entries = decodeSnapshotFrameStream(
    (async function* (): AsyncGenerator<Uint8Array> {
      for (let offset = 0; offset < bytes.byteLength; offset += MAX_SNAPSHOT_STREAM_CHUNK_BYTES) {
        yield bytes.subarray(offset, offset + MAX_SNAPSHOT_STREAM_CHUNK_BYTES);
      }
    })(),
  );
  const first = await entries.next();
  await entries.return(undefined);
  if (first.done || first.value.type !== "header") throw new Error("Snapshot header is missing");
  return summaryFromHeader(first.value.header, bytes.byteLength);
}

function summaryFromHeader(header: SnapshotFrameStreamHeader, byteLength: number): SnapshotSummary {
  let payloadBytes = 0;
  for (const kind of SNAPSHOT_FRAME_KINDS) {
    const next = payloadBytes + header.kinds[kind].storedBytes;
    if (!Number.isSafeInteger(next)) throw new RangeError("Snapshot payload is too large");
    payloadBytes = next;
  }
  return {
    formatVersion: header.formatVersion,
    version: header.databaseVersion,
    createdAt: header.createdAt,
    tableCount: header.kinds["catalog-page"].itemCount,
    blockCount: header.kinds.block.itemCount,
    payloadBytes,
    byteLength,
  };
}
