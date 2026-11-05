/**
 * The log: frame envelope, header slots, and the recovery scan. The body codec of a commit frame
 * belongs to the write-path track; its contents are fixed here as `CommitFrameBody`.
 *
 * ## Envelope (little-endian, every frame a multiple of 8 bytes)
 *   0  u32 magic "MNLF"        4  u32 salt         8  u32 lsn lo     12  u32 lsn hi
 *  16  u32 body length        20  u8 kind          21  3 bytes zero
 *  24  body, then zero padding, then the last 4 bytes: u32 chain checksum.
 * The chain checksum is xxHash32 of every byte before it, seeded with the previous frame's chain
 * checksum (the first frame after a reset is seeded with the salt). Chaining is what stops a
 * stale frame that happens to sit right after a rewritten one from validating: its seed is
 * wrong. LSNs are contiguous: each frame's is the previous one's + 1.
 *
 * ## Header slots (contract.ts LOG_HEADER_SLOT_BYTES each, two of them)
 *   0 u32 magic "MNLH"  4 u32 version  8 u32 salt  12 u32 generation lo  16 u32 generation hi
 *  20 u32 ackEnd lo  24 u32 ackEnd hi  28 u32 ackChain  32 u32 ackLsn lo  36 u32 ackLsn hi
 *  40 u32 xxHash32 of bytes 0…40 (seed 0)
 * A commit group appends its frames and, in the same flush, writes the slot not holding the
 * newest header with generation + 1 and `ackEnd` = the end of the previous acknowledged group.
 * An idle flush (≤100 ms after the last commit) writes a header acknowledging the newest group.
 *
 * ## Durability boundary (what recovery promises)
 * - Every frame before `ackEnd` of the newest valid header was acknowledged and must be present
 *   and valid; if it is not, the log is corrupt (`LogCorruptionError`), never silently shortened.
 * - Frames after `ackEnd` belong to the newest group (or groups not yet acknowledged). Recovery
 *   keeps the valid prefix and stops at the first invalid frame: the newest group has SQLite's
 *   exposure to a lying flush, every older group does not.
 * - The engine truncates the log to `validEnd` before its first append (contract.truncateLog).
 * - Frames whose salt is not the root's are a previous log incarnation and are never replayed.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import { LOG_HEADER_SLOT_BYTES } from "../storage/contract.js";

export const FRAME_MAGIC = 0x464c4e4d; // "MNLF"
export const HEADER_MAGIC = 0x484c4e4d; // "MNLH"
export const LOG_FORMAT_VERSION = 1;
export const FRAME_HEADER_BYTES = 24;
export const LOG_HEADER_BYTES = 44;
/** Largest body one frame carries; bigger commits split across frames of one group. */
export const MAX_FRAME_BODY_BYTES = 64 * 1024 * 1024;

export const FrameKind = {
  /** A commit: `CommitFrameBody`. */
  commit: 1,
  /** Continuation of a commit too large for one frame; the last part is `commit`. */
  commitPart: 2,
} as const;
export type FrameKind = (typeof FrameKind)[keyof typeof FrameKind];

export class LogCorruptionError extends Error {
  readonly code = "MINNOW_LOG_CORRUPT";
  constructor(message: string) {
    super(message);
    this.name = "LogCorruptionError";
  }
}

function setU53(view: DataView, offset: number, value: number): void {
  view.setUint32(offset, value >>> 0, true);
  view.setUint32(offset + 4, Math.floor(value / 2 ** 32), true);
}

function getU53(view: DataView, offset: number): number {
  return view.getUint32(offset, true) + view.getUint32(offset + 4, true) * 2 ** 32;
}

export function frameLength(bodyLength: number): number {
  return Math.ceil((FRAME_HEADER_BYTES + bodyLength + 4) / 8) * 8;
}

/** Encodes one frame; returns it and its chain checksum (the next frame's seed). */
export function encodeFrame(
  salt: number,
  lsn: number,
  kind: FrameKind,
  body: Uint8Array,
  previousChain: number,
): { readonly bytes: Uint8Array; readonly chain: number } {
  if (body.length > MAX_FRAME_BODY_BYTES) throw new RangeError("Log frame body is too large");
  if (!Number.isSafeInteger(lsn) || lsn < 1) throw new RangeError("Bad LSN");
  const bytes = new Uint8Array(frameLength(body.length));
  const view = new DataView(bytes.buffer);
  view.setUint32(0, FRAME_MAGIC, true);
  view.setUint32(4, salt >>> 0, true);
  setU53(view, 8, lsn);
  view.setUint32(16, body.length, true);
  bytes[20] = kind;
  bytes.set(body, FRAME_HEADER_BYTES);
  const chain = xxHash32(bytes, previousChain >>> 0, 0, bytes.length - 4);
  view.setUint32(bytes.length - 4, chain, true);
  return { bytes, chain };
}

export interface LogHeader {
  readonly salt: number;
  readonly generation: number;
  /** End offset of the acknowledged frames. */
  readonly ackEnd: number;
  /** Chain checksum of the frame ending at `ackEnd` (the salt when `ackEnd` is 0). */
  readonly ackChain: number;
  /** LSN of that frame (firstLsn − 1 when `ackEnd` is 0). */
  readonly ackLsn: number;
}

export function encodeLogHeader(header: LogHeader): Uint8Array {
  const bytes = new Uint8Array(LOG_HEADER_BYTES);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, HEADER_MAGIC, true);
  view.setUint32(4, LOG_FORMAT_VERSION, true);
  view.setUint32(8, header.salt >>> 0, true);
  setU53(view, 12, header.generation);
  setU53(view, 20, header.ackEnd);
  view.setUint32(28, header.ackChain >>> 0, true);
  setU53(view, 32, header.ackLsn);
  view.setUint32(40, xxHash32(bytes, 0, 0, 40), true);
  return bytes;
}

/** A slot's header, or null when it is empty, torn, or from another format version. */
export function decodeLogHeader(bytes: Uint8Array | null): LogHeader | null {
  if (bytes === null || bytes.length < LOG_HEADER_BYTES || bytes.length > LOG_HEADER_SLOT_BYTES)
    return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(0, true) !== HEADER_MAGIC) return null;
  if (view.getUint32(40, true) !== xxHash32(bytes, 0, 0, 40)) return null;
  if (view.getUint32(4, true) !== LOG_FORMAT_VERSION) return null;
  return {
    salt: view.getUint32(8, true),
    generation: getU53(view, 12),
    ackEnd: getU53(view, 20),
    ackChain: view.getUint32(28, true),
    ackLsn: getU53(view, 32),
  };
}

/** The newest valid header for `salt` among the two slots, and the slot the next one goes in. */
export function newestLogHeader(
  slots: readonly [Uint8Array | null, Uint8Array | null],
  salt: number,
): { readonly header: LogHeader | null; readonly nextSlot: 0 | 1 } {
  const headers = slots.map((slot) => {
    const header = decodeLogHeader(slot);
    return header !== null && header.salt === salt >>> 0 ? header : null;
  });
  const [first, second] = headers as [LogHeader | null, LogHeader | null];
  if (first === null && second === null) return { header: null, nextSlot: 0 };
  if (second === null || (first !== null && first.generation > second.generation))
    return { header: first, nextSlot: 1 };
  return { header: second, nextSlot: 0 };
}

export interface ScannedFrame {
  readonly offset: number;
  readonly lsn: number;
  readonly kind: FrameKind;
  /** A view of the scanned bytes; copy before keeping it past the scan's input. */
  readonly body: Uint8Array;
}

export interface LogScan {
  readonly frames: readonly ScannedFrame[];
  /** End of the last valid frame: truncate here before appending. */
  readonly validEnd: number;
  /** Chain checksum at `validEnd` (seed of the next frame). */
  readonly chain: number;
  /** LSN of the last valid frame (firstLsn − 1 when there is none). */
  readonly lastLsn: number;
  /** The header that bounded the scan (null: nothing acknowledged in this incarnation yet). */
  readonly header: LogHeader | null;
}

/**
 * Recovery's view of the log: the valid frames of the incarnation `salt`, starting at LSN
 * `firstLsn` (both from the durable root). Throws `LogCorruptionError` when an acknowledged frame
 * is missing or invalid.
 */
export function scanLog(
  body: Uint8Array,
  slots: readonly [Uint8Array | null, Uint8Array | null],
  salt: number,
  firstLsn: number,
): LogScan {
  const { header } = newestLogHeader(slots, salt);
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  const frames: ScannedFrame[] = [];
  let offset = 0;
  let chain = salt >>> 0;
  let lsn = firstLsn - 1;
  for (;;) {
    if (offset + FRAME_HEADER_BYTES + 4 > body.length) break;
    if (view.getUint32(offset, true) !== FRAME_MAGIC) break;
    if (view.getUint32(offset + 4, true) !== salt >>> 0) break;
    const bodyLength = view.getUint32(offset + 16, true);
    if (bodyLength > MAX_FRAME_BODY_BYTES) break;
    const length = frameLength(bodyLength);
    if (offset + length > body.length) break;
    const expected = xxHash32(body, chain, offset, offset + length - 4);
    if (view.getUint32(offset + length - 4, true) !== expected) break;
    const frameLsn = getU53(view, offset + 8);
    if (frameLsn !== lsn + 1) break;
    const kind = body[offset + 20] ?? 0;
    if (kind !== FrameKind.commit && kind !== FrameKind.commitPart) break;
    frames.push({
      offset,
      lsn: frameLsn,
      kind: kind,
      body: body.subarray(offset + FRAME_HEADER_BYTES, offset + FRAME_HEADER_BYTES + bodyLength),
    });
    chain = expected;
    lsn = frameLsn;
    offset += length;
    if (header !== null && offset === header.ackEnd && chain !== header.ackChain)
      throw new LogCorruptionError("The acknowledged log boundary does not match its frame");
  }
  if (header !== null && offset < header.ackEnd)
    throw new LogCorruptionError(
      `Acknowledged log frames are missing: valid to ${String(offset)}, acknowledged to ${String(header.ackEnd)}`,
    );
  return { frames, validEnd: offset, chain, lastLsn: lsn, header };
}

/**
 * The contents of a commit frame (codec: write-path track). One frame per commit; a commit
 * group is several frames flushed together.
 */
export interface CommitFrameBody {
  /**
   * The request that produced this commit, for exactly-once retries: the client's id (16 random
   * bytes, one per connection) and its request sequence number. Null for engine-internal commits.
   */
  readonly request: { readonly clientId: Uint8Array; readonly sequence: number } | null;
  /** The catalog version the commit was planned against; replay refuses a mismatch. */
  readonly catalogVersion: number;
  readonly tables: readonly TableChange[];
  /** Sequence (nextval) positions this commit advanced: final value per sequence. */
  readonly sequences: ReadonlyArray<{ readonly sequenceId: number; readonly value: bigint }>;
}

/**
 * Row changes to one table. An UPDATE is a delete of the old row id plus an insert under a new
 * row id. Row ids are allocated by the commit itself: `firstRowId … firstRowId + rowCount − 1`.
 */
export interface TableChange {
  readonly tableId: number;
  readonly inserts: {
    readonly firstRowId: number;
    readonly rowCount: number;
    /** One encoded column per table column, in catalog order (spill-page column encoding). */
    readonly columns: readonly Uint8Array[];
  } | null;
  /**
   * Deleted row ids, ascending, each with its primary-key bytes (key-index encoding). Keys of
   * inserted rows are derived from the inserted key columns, so they are not repeated.
   */
  readonly deletes: {
    readonly rowIds: Float64Array;
    readonly keys: readonly Uint8Array[];
  } | null;
}
