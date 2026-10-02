import { crc32 } from "../../block-format/index.js";
import { decodeRecordJson, encodeRecordJson, parseRecordJsonSliced } from "./wire.js";
import { readFully, writeFully, type SyncFileHandle } from "./sync-file.js";

/**
 * The write-ahead log, framed over one synchronous file handle its single writer holds open.
 *
 * Frame layout, little-endian: `u32 magic | u32 payloadLength | u32 crc32(payload) | payload`.
 * An append is one complete synchronous transfer at the tail — normally one microsecond-scale
 * write on a held handle, with retries if the platform reports a short transfer. The only
 * artifact a crash can leave is a truncated final frame, which reads as "not written". A
 * complete frame with foreign marker or checksum-invalid bytes is corruption and fails closed.
 * Frames carry a global sequence number inside the payload; the file is truncated to zero only
 * after a checkpoint covering every frame has been flushed, so replay is always
 * newest-checkpoint-plus-tail.
 *
 * A payload larger than one frame — a commit carrying millions of keys — spans several: each
 * piece but the last goes in a continuation frame (marker `MNWC`, same layout), written a piece
 * at a time, and the ordinary frame holding the last piece completes the payload. Replay joins
 * the pieces. Continuation frames with no completing frame after them are an unacknowledged
 * tail, exactly like a truncated frame. A reader from before continuation frames refuses them as
 * foreign bytes, so a store that writes them must gate older readers with its format marker.
 */

const FRAME_MAGIC = 0x4c574e4d; // "MNWL"
const CONTINUATION_MAGIC = 0x43574e4d; // "MNWC"
const FRAME_HEADER_BYTES = 12;
/**
 * A WAL frame is control data, never bulk block data.  Bounding it prevents a corrupt length
 * field (or an accidentally enormous catalog mutation) from turning recovery into an
 * unbounded allocation.  Snapshot/block payloads live in extents, so 64 MiB leaves generous
 * headroom for legitimate schema and transaction records.
 */
export const MAX_WAL_FRAME_BYTES = 64 * 1024 * 1024;

/**
 * The payload bytes each frame of a payload that spans several carries: small enough that
 * writing, reading, or checksumming one piece is a few milliseconds of synchronous work.
 */
export const WAL_CONTINUATION_PIECE_BYTES = 4 * 1024 * 1024;

/** Payloads above this many bytes are decoded a slice at a time by `iterateWalFramesSliced`. */
const SLICED_DECODE_BYTES = 256 * 1024;

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
    this.appendEncoded(encodeRecordJson(payload), flush);
  }

  /**
   * `append` for a payload already encoded — `encodeRecordJson`'s bytes, which a caller may
   * produce a slice at a time beforehand so the append itself stays short.
   */
  appendEncoded(bytes: Uint8Array, flush: boolean): void {
    this.#appendFrame(FRAME_MAGIC, bytes, flush);
  }

  /**
   * Appends one piece of a payload that spans several frames; the ordinary frame appended next
   * (`appendEncoded`) completes it. Until it does, the pieces are an unacknowledged tail that
   * replay ignores, and `rewind` takes them back. Nothing else may append in between.
   */
  appendContinuation(piece: Uint8Array, flush: boolean): void {
    this.#appendFrame(CONTINUATION_MAGIC, piece, flush);
  }

  /**
   * `appendEncoded` for a payload of any size: the pieces before the last go in continuation
   * frames with `pause` awaited between them, so no single write holds the thread for long.
   * A failure removes every piece it wrote. Nothing else may append until it settles.
   */
  async appendEncodedSliced(
    bytes: Uint8Array,
    flush: boolean,
    pause: () => Promise<void>,
  ): Promise<void> {
    const start = this.#offset;
    const last = Math.max(0, bytes.byteLength - WAL_CONTINUATION_PIECE_BYTES);
    try {
      for (let at = 0; at < last; at += WAL_CONTINUATION_PIECE_BYTES) {
        this.appendContinuation(
          bytes.subarray(at, Math.min(last, at + WAL_CONTINUATION_PIECE_BYTES)),
          flush,
        );
        await pause();
      }
      this.appendEncoded(bytes.subarray(last), flush);
    } catch (error) {
      try {
        this.rewind(start);
      } catch {
        // Preserve the original error; recovery treats the orphaned pieces as a torn tail.
      }
      throw error;
    }
  }

  /** Takes back frames appended after `offset` — continuation pieces whose payload failed. */
  rewind(offset: number): void {
    if (!Number.isSafeInteger(offset) || offset < 0 || offset > this.#offset) {
      throw new RangeError("WAL rewind offset is outside the written log");
    }
    this.#handle.truncate(offset);
    this.#offset = offset;
  }

  #appendFrame(magic: number, bytes: Uint8Array, flush: boolean): void {
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
    view.setUint32(0, magic, true);
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
    // truncate() already changed the handle's logical file position. Keep the writer aligned
    // with it even if the durability flush is refused; a later append must start at zero rather
    // than leave a sparse, unreplayable gap after the checkpoint.
    this.#offset = 0;
    this.#handle.flush();
  }

  flush(): void {
    this.#handle.flush();
  }

  close(): void {
    this.#handle.close();
  }
}

/**
 * Reads every whole, checksum-valid frame from the handle's current content, in order. A
 * truncated final frame is ignored; complete foreign or corrupt bytes are rejected. Returns the
 * payloads and the byte offset where appending should resume (the end of the last valid frame —
 * a truncated tail is overwritten).
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
 * Streams checksum-valid frames, joining a payload that spans several. Memory is one payload.
 * A truncated final frame, or continuation pieces with no completing frame, are an
 * unacknowledged tail; complete foreign or checksum-invalid bytes are corruption and fail
 * closed.
 */
export function* iterateWalFrames(
  handle: SyncFileHandle,
  acknowledgedEndOffset = 0,
): Generator<ReplayedWalFrame> {
  for (const payload of walPayloads(handle, acknowledgedEndOffset)) {
    if (payload !== null)
      yield { payload: decodeRecordJson(payload.bytes), frameEnd: payload.frameEnd };
  }
}

/**
 * `iterateWalFrames`, with `pause` awaited between frames and a large payload decoded a slice
 * at a time, so recovering a log that holds a huge commit never holds the thread for long.
 */
export async function* iterateWalFramesSliced(
  handle: SyncFileHandle,
  acknowledgedEndOffset: number,
  pause: () => Promise<void>,
): AsyncGenerator<ReplayedWalFrame> {
  for (const piece of walPayloads(handle, acknowledgedEndOffset)) {
    await pause();
    if (piece === null) continue;
    const { bytes, frameEnd } = piece;
    const payload =
      bytes.byteLength > SLICED_DECODE_BYTES
        ? await parseRecordJsonSliced(bytes, pause)
        : decodeRecordJson(bytes);
    yield { payload, frameEnd };
  }
}

/**
 * The log's payloads as bytes, each with the end offset of the frame that completed it, and a
 * `null` after each continuation piece so a sliced caller can pause between pieces.
 */
function* walPayloads(
  handle: SyncFileHandle,
  acknowledgedEndOffset: number,
): Generator<{ bytes: Uint8Array; frameEnd: number } | null> {
  const size = handle.getSize();
  if (!Number.isSafeInteger(size) || size < 0) {
    throw new Error(`Invalid WAL byte length: ${String(size)}`);
  }
  if (
    !Number.isSafeInteger(acknowledgedEndOffset) ||
    acknowledgedEndOffset < 0 ||
    size < acknowledgedEndOffset
  ) {
    throw new Error("WAL is truncated before its acknowledged boundary");
  }
  const header = new Uint8Array(FRAME_HEADER_BYTES);
  const headerView = new DataView(header.buffer);
  let offset = 0;
  let pieces: Uint8Array[] = [];
  let piecesStart = 0;
  while (offset + FRAME_HEADER_BYTES <= size) {
    readFully(handle, header, offset, "reading a WAL frame header for recovery");
    const magic = headerView.getUint32(0, true);
    if (magic !== FRAME_MAGIC && magic !== CONTINUATION_MAGIC) {
      // A power loss can persist the file's new length without the appended bytes, leaving a
      // zero-filled tail where the in-flight frame was to go. That frame was never
      // acknowledged, so an all-zero remainder is the end of the log, exactly like a short
      // tail; anything else in its place is foreign bytes, and fails closed.
      if (offset >= acknowledgedEndOffset && isZeroFilled(handle, offset, size)) break;
      throw new Error(`WAL frame marker mismatch at offset ${String(offset)}`);
    }
    const length = headerView.getUint32(4, true);
    const checksum = headerView.getUint32(8, true);
    if (length > MAX_WAL_FRAME_BYTES) {
      throw new Error(
        `WAL frame at offset ${String(offset)} exceeds the ${String(MAX_WAL_FRAME_BYTES)} ` +
          `byte limit: ${String(length)}`,
      );
    }
    const end = offset + FRAME_HEADER_BYTES + length;
    if (!Number.isSafeInteger(end) || end > size) {
      if (offset < acknowledgedEndOffset)
        throw new Error("WAL frame is truncated before its acknowledged boundary");
      break;
    }
    const payloadBytes = new Uint8Array(length);
    readFully(
      handle,
      payloadBytes,
      offset + FRAME_HEADER_BYTES,
      "reading a WAL frame payload for recovery",
    );
    if (crc32(payloadBytes) !== checksum) {
      // The header landed but the payload page did not: the same unacknowledged tail.
      if (
        offset >= acknowledgedEndOffset &&
        payloadBytes.every((byte) => byte === 0) &&
        isZeroFilled(handle, end, size)
      )
        break;
      throw new Error(`WAL frame checksum mismatch at offset ${String(offset)}`);
    }
    if (magic === CONTINUATION_MAGIC) {
      if (pieces.length === 0) piecesStart = offset;
      pieces.push(payloadBytes);
      offset = end;
      yield null;
      continue;
    }
    const bytes = pieces.length === 0 ? payloadBytes : joinPieces([...pieces, payloadBytes]);
    pieces = [];
    yield { bytes, frameEnd: end };
    offset = end;
  }
  // Pieces with no frame to complete them were never acknowledged — unless the acknowledged
  // boundary lies beyond where they start, which no torn append can explain.
  if (pieces.length > 0 && piecesStart < acknowledgedEndOffset) {
    throw new Error("WAL continuation frames end before their acknowledged boundary");
  }
}

function joinPieces(pieces: readonly Uint8Array[]): Uint8Array {
  let total = 0;
  for (const piece of pieces) total += piece.byteLength;
  const joined = new Uint8Array(total);
  let at = 0;
  for (const piece of pieces) {
    joined.set(piece, at);
    at += piece.byteLength;
  }
  return joined;
}

/** Whether every byte from `offset` to `size` is zero, read in bounded chunks. */
function isZeroFilled(handle: SyncFileHandle, offset: number, size: number): boolean {
  const chunk = new Uint8Array(Math.min(64 * 1024, Math.max(0, size - offset)));
  for (let at = offset; at < size; at += chunk.byteLength) {
    const length = Math.min(chunk.byteLength, size - at);
    const window = length === chunk.byteLength ? chunk : chunk.subarray(0, length);
    readFully(handle, window, at, "reading a WAL tail for recovery");
    for (let index = 0; index < length; index += 1) if (window[index] !== 0) return false;
  }
  return true;
}
