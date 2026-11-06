/**
 * The log: frame envelope, header slots, and the recovery scan. Frame bodies are
 * `CommitFrameBody` (codec: write-path track); replay rules are replay.ts.
 *
 * ## Envelope (little-endian; every frame a multiple of 8 bytes)
 *   0  u32 magic "MNLF"        4  u32 salt            8  u32 lsn lo     12  u32 lsn hi
 *  16  u32 body length        20  u8 kind (1)         21  u8 zero
 *  22  u16 part index         24  u16 part count      26  u16 zero
 *  28  body, zero padding, and in the last 4 bytes the u32 chain checksum: xxHash32 of every byte
 *      before it, seeded with the previous frame's chain checksum (the first frame of an
 *      incarnation is seeded with the salt). A stale frame behind a rewritten one never validates.
 * LSNs are contiguous. A transaction is `part count` consecutive frames with part indexes 0, 1, …;
 * its identity is the LSN of part 0 (= lsn − part index). One frame per transaction is the
 * common case; a commit bigger than `MAX_FRAME_BODY_BYTES` splits into parts.
 *
 * ## Header slots (two, `LOG_HEADER_SLOT_BYTES` each)
 *   0 u32 magic "MNLH"  4 u32 version  8 u32 salt  12 u53 generation  20 u53 ackEnd
 *  28 u32 ackChain  32 u53 ackLsn  40 u53 rootSequence  48 u32 xxHash32 of bytes 0…48 (seed 0)
 * (u53 = two u32 words, the high word ≤ 0x1F_FFFF.) A commit group appends its frames and, in the
 * same flush, writes the slot not holding the newest header with generation + 1 and `ack*` = the
 * end, chain and LSN of the previous acknowledged group. An idle flush acknowledges the newest
 * group, targeting 100 ms from the last append subject to scheduling and I/O latency (see
 * access/write-path.ts "Commit"). `rootSequence` is the sequence of the root that started
 * this incarnation (its salt), so a rolled-back root is detected.
 *
 * ## What recovery promises (`scanLog`)
 * - The acknowledged boundary must be exactly a transaction end seen by the scan, with the
 *   recorded chain and LSN; an empty boundary must be (0, salt, firstLsn − 1). Anything else, or
 *   a missing acknowledged frame, is `LogCorruptionError`, never a silently shorter log.
 * - After the boundary, the scan keeps complete transactions and stops at the first invalid
 *   frame or incomplete transaction. Recovery uses `scanLogIncremental`: one frame in memory at a
 *   time, transactions handed to a sink that stages their parts. The engine truncates to `validEnd` before its first append,
 *   so an abandoned prefix can never be completed by a later, unrelated transaction.
 * - Frames whose salt is not the root's are another incarnation and are never replayed. A valid
 *   header of another salt whose `rootSequence` is newer than the root's means the root was
 *   rolled back (a lying flush): `LogCorruptionError`.
 * - Honest flush: every acknowledged transaction survives. Lying flush: a cleanly truncated
 *   body loses at most the newest acknowledged group silently (older loss is detected); an
 *   in-order write-back loss may drop a suffix of groups, never reorder or invent one.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import { LOG_HEADER_SLOT_BYTES } from "../storage/contract.js";

export const FRAME_MAGIC = 0x464c4e4d; // "MNLF"
export const HEADER_MAGIC = 0x484c4e4d; // "MNLH"
export const LOG_FORMAT_VERSION = 1;
export const FRAME_HEADER_BYTES = 28;
export const LOG_HEADER_BYTES = 52;
/** Largest frame, envelope included; bigger transactions split into parts. */
export const MAX_FRAME_BYTES = 1024 * 1024;
/** Largest body one frame carries (`MAX_FRAME_BYTES` minus the 28-byte header and 4-byte chain). */
export const MAX_FRAME_BODY_BYTES = MAX_FRAME_BYTES - 32;
/**
 * The incremental scanner's own memory: one frame header and one whole frame alive at once.
 * Recovery admits it (memory/governor.ts) and passes the grant to `scanLogIncremental`. A sink
 * stages a multipart transaction's parts in its own memory (charged, or spilled), up to
 * `MAX_FRAME_PARTS` × `MAX_FRAME_BYTES`.
 */
export const RECOVERY_SCAN_BYTES = MAX_FRAME_BYTES + FRAME_HEADER_BYTES;
export const MAX_FRAME_PARTS = 0xffff;

export const FrameKind = { commit: 1 } as const;
export type FrameKind = (typeof FrameKind)[keyof typeof FrameKind];

export class LogCorruptionError extends Error {
  readonly code = "MINNOW_LOG_CORRUPT";
  constructor(message: string) {
    super(message);
    this.name = "LogCorruptionError";
  }
}

const SAFE_HIGH_WORD = 0x1f_ffff;

function setU53(view: DataView, offset: number, value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError("Not a u53");
  view.setUint32(offset, value >>> 0, true);
  view.setUint32(offset + 4, Math.floor(value / 2 ** 32), true);
}

/** A u53 field, or −1 when the high word makes it unsafe (a damaged or foreign field). */
function getU53(view: DataView, offset: number): number {
  const high = view.getUint32(offset + 4, true);
  if (high > SAFE_HIGH_WORD) return -1;
  return view.getUint32(offset, true) + high * 2 ** 32;
}

export function frameLength(bodyLength: number): number {
  return Math.ceil((FRAME_HEADER_BYTES + bodyLength + 4) / 8) * 8;
}

export interface FramePosition {
  readonly salt: number;
  readonly lsn: number;
  /** This frame's part index and its transaction's part count (0 and 1 for a single frame). */
  readonly partIndex: number;
  readonly partCount: number;
}

/** Encodes one frame; returns it and its chain checksum (the next frame's seed). */
export function encodeFrame(
  position: FramePosition,
  body: Uint8Array,
  previousChain: number,
): { readonly bytes: Uint8Array; readonly chain: number } {
  const { salt, lsn, partIndex, partCount } = position;
  if (body.length > MAX_FRAME_BODY_BYTES) throw new RangeError("Log frame body is too large");
  if (!Number.isSafeInteger(lsn) || lsn < 1) throw new RangeError("Bad LSN");
  if (!(partCount >= 1 && partCount <= MAX_FRAME_PARTS && partIndex >= 0 && partIndex < partCount))
    throw new RangeError("Bad transaction part");
  const bytes = new Uint8Array(frameLength(body.length));
  const view = new DataView(bytes.buffer);
  view.setUint32(0, FRAME_MAGIC, true);
  view.setUint32(4, salt >>> 0, true);
  setU53(view, 8, lsn);
  view.setUint32(16, body.length, true);
  bytes[20] = FrameKind.commit;
  view.setUint16(22, partIndex, true);
  view.setUint16(24, partCount, true);
  bytes.set(body, FRAME_HEADER_BYTES);
  const chain = xxHash32(bytes, previousChain >>> 0, 0, bytes.length - 4);
  view.setUint32(bytes.length - 4, chain, true);
  return { bytes, chain };
}

export interface LogHeader {
  readonly salt: number;
  readonly generation: number;
  /** End offset of the acknowledged transactions. */
  readonly ackEnd: number;
  /** Chain checksum at `ackEnd` (the salt when `ackEnd` is 0). */
  readonly ackChain: number;
  /** LSN of the frame ending at `ackEnd` (firstLsn − 1 when `ackEnd` is 0). */
  readonly ackLsn: number;
  /** Sequence of the root that started this incarnation. */
  readonly rootSequence: number;
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
  setU53(view, 40, header.rootSequence);
  view.setUint32(48, xxHash32(bytes, 0, 0, 48), true);
  return bytes;
}

/** A slot's header, or null when it is empty, torn, unsafe, or from another format version. */
export function decodeLogHeader(bytes: Uint8Array | null): LogHeader | null {
  if (bytes === null || bytes.length < LOG_HEADER_BYTES || bytes.length > LOG_HEADER_SLOT_BYTES)
    return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(0, true) !== HEADER_MAGIC) return null;
  if (view.getUint32(48, true) !== xxHash32(bytes, 0, 0, 48)) return null;
  if (view.getUint32(4, true) !== LOG_FORMAT_VERSION) return null;
  const header = {
    salt: view.getUint32(8, true),
    generation: getU53(view, 12),
    ackEnd: getU53(view, 20),
    ackChain: view.getUint32(28, true),
    ackLsn: getU53(view, 32),
    rootSequence: getU53(view, 40),
  };
  return header.generation < 0 || header.ackEnd < 0 || header.ackLsn < 0 || header.rootSequence < 0
    ? null
    : header;
}

/** The newest valid header for `salt` among the two slots, and the slot the next one goes in. */
export function newestLogHeader(
  slots: readonly [Uint8Array | null, Uint8Array | null],
  salt: number,
): { readonly header: LogHeader | null; readonly nextSlot: 0 | 1 } {
  const [first, second] = slots.map((slot) => {
    const header = decodeLogHeader(slot);
    return header !== null && header.salt === salt >>> 0 ? header : null;
  }) as [LogHeader | null, LogHeader | null];
  if (first !== null && (second === null || first.generation > second.generation))
    return { header: first, nextSlot: 1 };
  if (second !== null) return { header: second, nextSlot: 0 };
  return { header: null, nextSlot: 0 };
}

export interface ScannedFrame {
  readonly offset: number;
  readonly lsn: number;
  /** LSN of the transaction's first part: its identity. */
  readonly transaction: number;
  readonly partIndex: number;
  readonly partCount: number;
  /** A view of the scanned bytes; copy before keeping it past the scan's input. */
  readonly body: Uint8Array;
}

export interface LogScan extends LogScanSummary {
  /** Frames of complete transactions, in LSN order. */
  readonly frames: readonly ScannedFrame[];
}

/** The root's description of the log incarnation to recover (format/manifest.ts `log`). */
export interface LogIncarnation {
  readonly salt: number;
  readonly firstLsn: number;
  /** Sequence of the root that started this incarnation. */
  readonly rootSequence: number;
}

/** What the scan concluded (both scanners). */
export interface LogScanSummary {
  /** End of the last complete transaction: truncate here before appending. */
  readonly validEnd: number;
  /** Chain checksum at `validEnd` (seed of the next frame). */
  readonly chain: number;
  /** LSN of the last frame kept (firstLsn − 1 when there is none). */
  readonly lastLsn: number;
  /** Parts of an incomplete transaction found after `validEnd` (discarded). */
  readonly abandonedParts: number;
  /** The header that bounded the scan (null: nothing acknowledged in this incarnation yet). */
  readonly header: LogHeader | null;
}

/**
 * The scan as a state machine over consecutive frames (shared by `scanLog` and
 * `scanLogIncremental`). It keeps no frame bytes: only the chain, LSN, open part count and the
 * last transaction boundary.
 */
class LogScanState {
  readonly header: LogHeader | null;
  readonly #salt: number;
  offset = 0;
  #chain: number;
  #lsn: number;
  #openParts = 0;
  #openCount = 0;
  #boundary: { end: number; chain: number; lsn: number };
  #landed: boolean;

  constructor(slots: readonly [Uint8Array | null, Uint8Array | null], incarnation: LogIncarnation) {
    const salt = incarnation.salt >>> 0;
    this.#salt = salt;
    for (const slot of slots) {
      const other = decodeLogHeader(slot);
      if (other !== null && other.salt !== salt && other.rootSequence > incarnation.rootSequence)
        throw new LogCorruptionError("The log belongs to a newer root than the one on disk");
    }
    const { header } = newestLogHeader(slots, salt);
    this.header = header;
    const empty = incarnation.firstLsn - 1;
    if (
      header !== null &&
      header.ackEnd === 0 &&
      (header.ackChain !== salt || header.ackLsn !== empty)
    )
      throw new LogCorruptionError("An empty acknowledged boundary has the wrong chain or LSN");
    this.#chain = salt;
    this.#lsn = empty;
    this.#boundary = { end: 0, chain: salt, lsn: empty };
    this.#landed = header === null || header.ackEnd === 0;
  }

  /** The length of the frame whose first `FRAME_HEADER_BYTES` are `head`, or null to stop. */
  frameLength(head: Uint8Array): number | null {
    if (head.length < FRAME_HEADER_BYTES) return null;
    const view = new DataView(head.buffer, head.byteOffset, FRAME_HEADER_BYTES);
    if (view.getUint32(0, true) !== FRAME_MAGIC || view.getUint32(4, true) !== this.#salt)
      return null;
    const bodyLength = view.getUint32(16, true);
    return bodyLength > MAX_FRAME_BODY_BYTES ? null : frameLength(bodyLength);
  }

  /**
   * Validates the whole frame at the current offset. Returns the frame (its body a view of
   * `bytes`) and whether it completes its transaction, or null to stop.
   */
  accept(bytes: Uint8Array): { readonly frame: ScannedFrame; readonly completes: boolean } | null {
    const length = bytes.length;
    const view = new DataView(bytes.buffer, bytes.byteOffset, length);
    const expected = xxHash32(bytes, this.#chain, 0, length - 4);
    if (view.getUint32(length - 4, true) !== expected) return null;
    const frameLsn = getU53(view, 8);
    const partIndex = view.getUint16(22, true);
    const partCount = view.getUint16(24, true);
    if (frameLsn !== this.#lsn + 1 || bytes[20] !== FrameKind.commit) return null;
    if (bytes[21] !== 0 || view.getUint16(26, true) !== 0) return null;
    if (partCount < 1 || partIndex >= partCount) return null;
    const continues =
      partIndex === 0
        ? this.#openParts === 0
        : this.#openParts === partIndex && this.#openCount === partCount;
    if (!continues) return null;
    const bodyLength = view.getUint32(16, true);
    const frame: ScannedFrame = {
      offset: this.offset,
      lsn: frameLsn,
      transaction: frameLsn - partIndex,
      partIndex,
      partCount,
      body: bytes.subarray(FRAME_HEADER_BYTES, FRAME_HEADER_BYTES + bodyLength),
    };
    this.#chain = expected;
    this.#lsn = frameLsn;
    this.offset += length;
    this.#openParts = partIndex + 1;
    this.#openCount = partCount;
    const completes = partIndex === partCount - 1;
    const header = this.header;
    if (completes) {
      this.#openParts = 0;
      this.#boundary = { end: this.offset, chain: this.#chain, lsn: this.#lsn };
      if (header !== null && this.offset === header.ackEnd) {
        if (this.#chain !== header.ackChain || this.#lsn !== header.ackLsn)
          throw new LogCorruptionError("The acknowledged boundary does not match its frame");
        this.#landed = true;
      }
    }
    if (header !== null && !this.#landed && this.offset > header.ackEnd)
      throw new LogCorruptionError("The acknowledged boundary is not a transaction end");
    return { frame, completes };
  }

  finish(): LogScanSummary {
    if (!this.#landed)
      throw new LogCorruptionError(
        `Acknowledged log frames are missing: valid to ${String(this.#boundary.end)}, acknowledged to ${String(this.header?.ackEnd ?? 0)}`,
      );
    return {
      validEnd: this.#boundary.end,
      chain: this.#boundary.chain,
      lastLsn: this.#boundary.lsn,
      abandonedParts: this.#openParts,
      header: this.header,
    };
  }
}

/** Recovery's view of a log held in memory (tests, small logs). */
export function scanLog(
  body: Uint8Array,
  slots: readonly [Uint8Array | null, Uint8Array | null],
  incarnation: LogIncarnation,
): LogScan {
  const state = new LogScanState(slots, incarnation);
  const frames: ScannedFrame[] = [];
  const open: ScannedFrame[] = [];
  for (;;) {
    const length = state.frameLength(
      body.subarray(state.offset, state.offset + FRAME_HEADER_BYTES),
    );
    if (length === null || state.offset + length > body.length) break;
    const accepted = state.accept(body.subarray(state.offset, state.offset + length));
    if (accepted === null) break;
    open.push(accepted.frame);
    if (accepted.completes) frames.push(...open.splice(0));
  }
  return { ...state.finish(), frames };
}

/** Where the incremental scan reads the log body from (a `StoreSession` fits). */
export interface LogReader {
  readonly length: number;
  read(offset: number, length: number): Promise<Uint8Array>;
}

/**
 * Receives frames as the incremental scan validates them. Parts arrive before the scan knows
 * their transaction is complete: the sink stages them (a transaction larger than memory is staged
 * the way a large commit is written: into its table's memtable spill or a staging area) and
 * applies them on `commit`, or drops them on `abandon` (only the log's tail can be abandoned).
 */
export interface TransactionSink {
  /** `frame.body` is valid only during this call: copy what you keep. */
  part(frame: ScannedFrame): void | Promise<void>;
  commit(transaction: number, lastLsn: number): void | Promise<void>;
  abandon(parts: number): void | Promise<void>;
}

/** The memory recovery charges the scanner to (a governor `Grant` fits). */
export interface ScanCharge {
  allocate(bytes: number): boolean;
  free(bytes: number): void;
}

/**
 * Recovery's view of a log of any size. Memory: `RECOVERY_SCAN_BYTES` (one frame header and one
 * frame of at most `MAX_FRAME_BYTES`), allocated from `charge` before the first read; recovery
 * admits that minimum first (`RangeError` otherwise). No frame is kept after its delivery.
 *
 * Failure: if the reader, the sink or validation throws, the sink's `abandon` is called for the
 * parts delivered since the last `commit` (so it can drop staged parts), the charge is freed, and
 * the original error propagates.
 */
export async function scanLogIncremental(
  reader: LogReader,
  slots: readonly [Uint8Array | null, Uint8Array | null],
  incarnation: LogIncarnation,
  sink: TransactionSink,
  charge: ScanCharge,
): Promise<LogScanSummary> {
  if (!charge.allocate(RECOVERY_SCAN_BYTES))
    throw new RangeError("Recovery's scan memory must be admitted before the scan");
  let delivered = 0;
  try {
    const state = new LogScanState(slots, incarnation);
    for (;;) {
      if (state.offset + FRAME_HEADER_BYTES + 4 > reader.length) break;
      const length = state.frameLength(await reader.read(state.offset, FRAME_HEADER_BYTES));
      if (length === null || length > MAX_FRAME_BYTES || state.offset + length > reader.length)
        break;
      const accepted = state.accept(await reader.read(state.offset, length));
      if (accepted === null) break;
      delivered += 1;
      await sink.part(accepted.frame);
      if (accepted.completes) {
        await sink.commit(accepted.frame.transaction, accepted.frame.lsn);
        delivered = 0;
      }
    }
    const summary = state.finish();
    if (summary.abandonedParts > 0) {
      delivered = 0;
      await sink.abandon(summary.abandonedParts);
    }
    return summary;
  } catch (error) {
    if (delivered > 0)
      try {
        await sink.abandon(delivered);
      } catch {
        // The original failure is the one to report.
      }
    throw error;
  } finally {
    charge.free(RECOVERY_SCAN_BYTES);
  }
}

/**
 * The contents of a commit transaction (codec: write-path track): an ordered list of effects,
 * applied in order on commit and on replay (replay.ts).
 */
export interface CommitFrameBody {
  /** The request that produced this commit, for exactly-once (format/ledger.ts). */
  readonly request: {
    readonly clientId: Uint8Array;
    readonly sequence: number;
    /** The client's send time (same device clock); the ledger refuses expired retries. */
    readonly sentAtMs: number;
    /** Affected-row counts reported to the client, replayed for a duplicate. */
    readonly counts: Readonly<Record<string, number>>;
  } | null;
  readonly effects: readonly BodyEffect[];
}

export type BodyEffect =
  | { readonly kind: "createTable"; readonly tableId: number; readonly definition: Uint8Array }
  | {
      readonly kind: "alterTable";
      readonly tableId: number;
      readonly schemaVersion: number;
      readonly definition: Uint8Array;
    }
  | { readonly kind: "dropTable"; readonly tableId: number }
  /** Views, sequences, indexes: catalog state with no table schema change. */
  | { readonly kind: "catalog"; readonly definition: Uint8Array }
  /** A sequence block reservation: the sequence's new reserved-through value. */
  | { readonly kind: "sequence"; readonly sequenceId: number; readonly reservedThrough: bigint }
  | ({ readonly kind: "rows" } & TableChange)
  /**
   * A large transaction's rows, already published as staged in the root `rootSequence` under
   * `transaction` (replay.ts "Large transactions"). This frame is their commit point.
   */
  | {
      readonly kind: "publishedRows";
      readonly tableId: number;
      readonly schemaVersion: number;
      readonly transaction: number;
      readonly rootSequence: number;
      /** The indexes whose staged runs this commit makes visible with the rows. */
      readonly indexIds: readonly number[];
    };

/**
 * Row changes to one table under one schema version. An UPDATE is a delete of the old row id plus
 * an insert under a new row id. Row ids are allocated by the commit: `firstRowId … + rowCount − 1`.
 */
export interface TableChange {
  readonly tableId: number;
  /** The table's schema version the columns were encoded under; replay refuses a mismatch. */
  readonly schemaVersion: number;
  readonly inserts: {
    readonly firstRowId: number;
    readonly rowCount: number;
    /** One encoded column per table column, in catalog order (spill-page column encoding). */
    readonly columns: readonly Uint8Array[];
  } | null;
  /**
   * Deleted row ids, ascending, with the equality-key bytes (keys/key-encoding.ts) of every index the
   * row was in: primary key first, then each secondary index in catalog order. Keys of inserted
   * rows are derived from the inserted columns, so they are not repeated.
   */
  readonly deletes: {
    readonly rowIds: Float64Array;
    readonly keys: ReadonlyArray<readonly Uint8Array[]>;
  } | null;
}
