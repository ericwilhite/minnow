import { crc32 } from "../../block-format/index.js";
import { decodeRecordJson, encodeRecordJson } from "./wire.js";
import { readFully, writeFully, type SyncFileHandle } from "./sync-file.js";

/**
 * The write-ahead log, framed over one synchronous file handle its single writer holds open.
 *
 * Frame layout, little-endian: `u32 magic | u32 payloadLength | u32 crc32(payload) | payload`.
 * An append is one complete synchronous transfer at the tail — normally one microsecond-scale
 * write on a held handle, with retries if the platform reports a short transfer. The only
 * artifact a crash can leave is a torn final frame, which fails its checksum and reads as "not
 * written". Frames carry a global sequence number inside the
 * payload; the file is truncated to zero only after a checkpoint covering every frame has
 * been flushed, so replay is always newest-checkpoint-plus-tail.
 */

const FRAME_MAGIC = 0x4c574e4d; // "MNWL"
const FRAME_HEADER_BYTES = 12;
/**
 * A WAL frame is control data, never bulk block data.  Bounding it prevents a corrupt length
 * field (or an accidentally enormous catalog mutation) from turning recovery into an
 * unbounded allocation.  Snapshot/block payloads live in extents, so 64 MiB leaves generous
 * headroom for legitimate schema and transaction records.
 */
export const MAX_WAL_FRAME_BYTES = 64 * 1024 * 1024;

export interface ReplayedWalFrame {
  payload: unknown;
  frameEnd: number;
}

export class WalWriter {
  readonly #handle: SyncFileHandle;
  #offset: number;

  constructor(handle: SyncFileHandle, offset: number) {
    if (!Number.isSafeInteger(offset) || offset < 0) {
      throw new RangeError("WAL offset is outside the safe integer range");
    }
    this.#handle = handle;
    this.#offset = offset;
  }

  get byteLength(): number {
    return this.#offset;
  }

  /** Appends one frame; with `flush`, durable before return. Synchronous — never yields. */
  append(payload: unknown, flush: boolean): void {
    const bytes = encodeRecordJson(payload);
    if (bytes.byteLength > MAX_WAL_FRAME_BYTES) {
      throw new RangeError(
        `WAL frame payload exceeds the ${String(MAX_WAL_FRAME_BYTES)} byte limit: ` +
          String(bytes.byteLength),
      );
    }
    const frame = new Uint8Array(FRAME_HEADER_BYTES + bytes.byteLength);
    if (this.#offset > Number.MAX_SAFE_INTEGER - frame.byteLength) {
      throw new RangeError("WAL offset exceeds the safe integer range");
    }
    const view = new DataView(frame.buffer);
    view.setUint32(0, FRAME_MAGIC, true);
    view.setUint32(4, bytes.byteLength, true);
    view.setUint32(8, crc32(bytes), true);
    frame.set(bytes, FRAME_HEADER_BYTES);
    const start = this.#offset;
    try {
      writeFully(this.#handle, frame, start, "appending a WAL frame");
      if (flush) this.#handle.flush();
      this.#offset = start + frame.byteLength;
    } catch (error) {
      // A complete write followed by a failed flush is not an acknowledged frame either.
      // Remove it before unpublished extent bytes are rolled back.  If truncation itself
      // fails, recovery will fail closed when it verifies the frame's referenced payloads.
      try {
        this.#handle.truncate(start);
      } catch {
        // Preserve the original quota/I/O error.
      }
      throw error;
    }
  }

  /** Empties the log after a flushed checkpoint has covered every frame in it. */
  reset(): void {
    this.#handle.truncate(0);
    this.#handle.flush();
    this.#offset = 0;
  }

  flush(): void {
    this.#handle.flush();
  }

  close(): void {
    this.#handle.close();
  }
}

/**
 * Reads every whole, checksum-valid frame from the handle's current content, in order,
 * stopping at the first torn or foreign bytes. Returns the payloads and the byte offset where
 * appending should resume (the end of the last valid frame — a torn tail is overwritten).
 */
export function replayWalFrames(handle: SyncFileHandle): {
  payloads: unknown[];
  /** End offset of each corresponding payload frame. */
  frameEnds: number[];
  endOffset: number;
} {
  const payloads: unknown[] = [];
  const frameEnds: number[] = [];
  let endOffset = 0;
  for (const frame of iterateWalFrames(handle)) {
    payloads.push(frame.payload);
    frameEnds.push(frame.frameEnd);
    endOffset = frame.frameEnd;
  }
  return { payloads, frameEnds, endOffset };
}

/**
 * Streams checksum-valid frames with O(max-frame) memory. A torn final frame is ignored, but
 * a complete header claiming an excessive payload is corruption and fails closed.
 */
export function* iterateWalFrames(handle: SyncFileHandle): Generator<ReplayedWalFrame> {
  const size = handle.getSize();
  if (!Number.isSafeInteger(size) || size < 0) {
    throw new Error(`Invalid WAL byte length: ${String(size)}`);
  }
  const header = new Uint8Array(FRAME_HEADER_BYTES);
  const headerView = new DataView(header.buffer);
  let offset = 0;
  while (offset + FRAME_HEADER_BYTES <= size) {
    readFully(handle, header, offset, "reading a WAL frame header for recovery");
    if (headerView.getUint32(0, true) !== FRAME_MAGIC) break;
    const length = headerView.getUint32(4, true);
    const checksum = headerView.getUint32(8, true);
    if (length > MAX_WAL_FRAME_BYTES) {
      throw new Error(
        `WAL frame at offset ${String(offset)} exceeds the ${String(MAX_WAL_FRAME_BYTES)} ` +
          `byte limit: ${String(length)}`,
      );
    }
    const end = offset + FRAME_HEADER_BYTES + length;
    if (!Number.isSafeInteger(end) || end > size) break;
    const payloadBytes = new Uint8Array(length);
    readFully(
      handle,
      payloadBytes,
      offset + FRAME_HEADER_BYTES,
      "reading a WAL frame payload for recovery",
    );
    if (crc32(payloadBytes) !== checksum) break;
    yield { payload: decodeRecordJson(payloadBytes), frameEnd: end };
    offset = end;
  }
}
