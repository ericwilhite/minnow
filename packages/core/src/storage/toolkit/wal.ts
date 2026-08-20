import { crc32 } from "../../block-format/index.js";
import { decodeRecordJson, encodeRecordJson } from "./wire.js";
import type { SyncFileHandle } from "./sync-file.js";

/**
 * The write-ahead log, framed over one synchronous file handle its single writer holds open.
 *
 * Frame layout, little-endian: `u32 magic | u32 payloadLength | u32 crc32(payload) | payload`.
 * An append is a single synchronous write at the tail — measured at microseconds on a held
 * handle — and the only artifact a crash can leave is a torn final frame, which fails its
 * checksum and reads as "not written". Frames carry a global sequence number inside the
 * payload; the file is truncated to zero only after a checkpoint covering every frame has
 * been flushed, so replay is always newest-checkpoint-plus-tail.
 */

const FRAME_MAGIC = 0x4c574e4d; // "MNWL"
const FRAME_HEADER_BYTES = 12;

export class WalWriter {
  readonly #handle: SyncFileHandle;
  #offset: number;

  constructor(handle: SyncFileHandle, offset: number) {
    this.#handle = handle;
    this.#offset = offset;
  }

  get byteLength(): number {
    return this.#offset;
  }

  /** Appends one frame; with `flush`, durable before return. Synchronous — never yields. */
  append(payload: unknown, flush: boolean): void {
    const bytes = encodeRecordJson(payload);
    const frame = new Uint8Array(FRAME_HEADER_BYTES + bytes.byteLength);
    const view = new DataView(frame.buffer);
    view.setUint32(0, FRAME_MAGIC, true);
    view.setUint32(4, bytes.byteLength, true);
    view.setUint32(8, crc32(bytes), true);
    frame.set(bytes, FRAME_HEADER_BYTES);
    this.#handle.write(frame, { at: this.#offset });
    if (flush) this.#handle.flush();
    this.#offset += frame.byteLength;
  }

  /** Empties the log after a flushed checkpoint has covered every frame in it. */
  reset(): void {
    this.#handle.truncate(0);
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
  endOffset: number;
} {
  const size = handle.getSize();
  const bytes = new Uint8Array(size);
  handle.read(bytes, { at: 0 });
  const view = new DataView(bytes.buffer);
  const payloads: unknown[] = [];
  let offset = 0;
  while (offset + FRAME_HEADER_BYTES <= size) {
    if (view.getUint32(offset, true) !== FRAME_MAGIC) break;
    const length = view.getUint32(offset + 4, true);
    const checksum = view.getUint32(offset + 8, true);
    const end = offset + FRAME_HEADER_BYTES + length;
    if (end > size) break;
    const payload = bytes.subarray(offset + FRAME_HEADER_BYTES, end);
    if (crc32(payload) !== checksum) break;
    payloads.push(decodeRecordJson(payload));
    offset = end;
  }
  return { payloads, endOffset: offset };
}
