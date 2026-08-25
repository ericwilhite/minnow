import { crc32 } from "../../block-format/checksum.js";
import { MAX_STORED_BLOCK_BYTE_LENGTH } from "../../block-format/index.js";
import {
  MAX_SNAPSHOT_FRAME_ITEMS,
  MAX_SNAPSHOT_METADATA_FRAME_BYTES,
  SNAPSHOT_FRAME_KINDS,
  type SnapshotFrame,
  type SnapshotFrameKind,
  validateStorageId,
} from "../types.js";
import { assertValidPlacement, type Placement } from "../toolkit/extents.js";
import { readFully, writeFully, type SyncFileHandle } from "../toolkit/sync-file.js";
import { OpfsTree, encodeSegment } from "./files.js";

const LEDGER_MAGIC = 0x534c_4731; // SLG1
const LEDGER_PREFIX_BYTES = 80;
const MODE_INLINE = 0;
const MODE_PLACEMENT = 1;

export type SnapshotLedgerKind = "export" | "import" | "completed";

export interface SnapshotLedgerRecord {
  readonly sequence: number;
  readonly kind: SnapshotFrameKind;
  readonly itemCount: number;
  readonly key: string | null;
  readonly payloadLength: number;
  readonly checksum: number;
  /** Metadata payloads are inline. Block payloads live once in an extent placement. */
  readonly payload?: Uint8Array;
  readonly placement?: Placement;
}

export interface SnapshotLedgerRead {
  readonly record: SnapshotLedgerRecord;
  readonly offset: number;
  readonly nextOffset: number;
}

export function snapshotLedgerPath(kind: SnapshotLedgerKind, id: string): string[] {
  validateStorageId(id);
  return ["snapshots-v1", kind, encodeSegment(id)];
}

/**
 * Checksummed append-only control file for one snapshot session. A checkpoint stores only the
 * committed prefix length. Bytes beyond it are unpublished and truncated on recovery, so a
 * large import/export never creates a database-sized WAL/checkpoint collection.
 */
export class SnapshotFrameLedger {
  readonly #handle: SyncFileHandle;
  #length: number;
  #closed = false;

  private constructor(handle: SyncFileHandle, length: number) {
    this.#handle = handle;
    this.#length = length;
  }

  static async open(
    tree: OpfsTree,
    kind: SnapshotLedgerKind,
    id: string,
    committedLength: number,
    create: boolean,
  ): Promise<SnapshotFrameLedger> {
    if (!Number.isSafeInteger(committedLength) || committedLength < 0) {
      throw new RangeError("Snapshot ledger length is invalid");
    }
    const handle = await tree.openHandle(snapshotLedgerPath(kind, id), { create });
    const size = handle.getSize();
    if (size < committedLength) {
      handle.close();
      throw new Error(
        `Snapshot ledger is shorter than its committed prefix: ${String(size)} < ` +
          String(committedLength),
      );
    }
    // A checkpoint's committed prefix may have a valid WAL-covered suffix after it. Keep the
    // physical suffix until recovery either adopts each frame or truncates it after replay.
    return new SnapshotFrameLedger(handle, committedLength);
  }

  get byteLength(): number {
    return this.#length;
  }

  append(frame: SnapshotFrame, placement?: Placement): SnapshotLedgerRead {
    this.#assertOpen();
    const encoded = encodeLedgerRecord(frame, placement);
    const offset = this.#length;
    const nextOffset = safeLedgerSum(offset, encoded.byteLength, "Snapshot ledger length");
    writeFully(this.#handle, encoded, offset, "appending snapshot frame ledger");
    this.#length = nextOffset;
    return { record: ledgerRecordFromFrame(frame, placement), offset, nextOffset };
  }

  read(offset: number, committedLength = this.#length): SnapshotLedgerRead {
    this.#assertOpen();
    if (
      !Number.isSafeInteger(offset) ||
      offset < 0 ||
      !Number.isSafeInteger(committedLength) ||
      committedLength < offset ||
      committedLength > this.#length ||
      committedLength - offset < LEDGER_PREFIX_BYTES
    ) {
      throw new RangeError("Snapshot ledger read offset is invalid");
    }
    const prefix = new Uint8Array(LEDGER_PREFIX_BYTES);
    readFully(this.#handle, prefix, offset, "reading snapshot frame ledger prefix");
    const view = new DataView(prefix.buffer);
    if (view.getUint32(0, true) !== LEDGER_MAGIC) {
      throw new Error("Snapshot ledger record marker mismatch");
    }
    const recordLength = view.getUint32(4, true);
    if (
      recordLength < LEDGER_PREFIX_BYTES ||
      recordLength > MAX_SNAPSHOT_METADATA_FRAME_BYTES + LEDGER_PREFIX_BYTES + 3_072 ||
      recordLength > committedLength - offset
    ) {
      throw new Error("Snapshot ledger record length is invalid");
    }
    const encoded = new Uint8Array(recordLength);
    readFully(this.#handle, encoded, offset, "reading snapshot frame ledger record");
    const expectedRecordChecksum = new DataView(encoded.buffer).getUint32(76, true);
    new DataView(encoded.buffer).setUint32(76, 0, true);
    if (crc32(encoded) !== expectedRecordChecksum) {
      throw new Error("Snapshot ledger record checksum mismatch");
    }
    new DataView(encoded.buffer).setUint32(76, expectedRecordChecksum, true);
    const record = decodeLedgerRecord(encoded);
    return {
      record,
      offset,
      nextOffset: safeLedgerSum(offset, recordLength, "Snapshot ledger read offset"),
    };
  }

  *records(committedLength = this.#length): IterableIterator<SnapshotLedgerRead> {
    let offset = 0;
    while (offset < committedLength) {
      const read = this.read(offset, committedLength);
      yield read;
      offset = read.nextOffset;
    }
    if (offset !== committedLength) throw new Error("Snapshot ledger prefix ends mid-record");
  }

  flush(): void {
    this.#assertOpen();
    this.#handle.flush();
  }

  truncate(length: number): void {
    this.#assertOpen();
    if (!Number.isSafeInteger(length) || length < 0 || length > this.#length) {
      throw new RangeError("Snapshot ledger truncation is invalid");
    }
    this.#handle.truncate(length);
    this.#length = length;
  }

  /** Recovery adopts the next WAL-published prefix without copying or re-appending its bytes. */
  adoptLength(length: number): void {
    this.#assertOpen();
    if (!Number.isSafeInteger(length) || length < this.#length || length > this.#handle.getSize()) {
      throw new RangeError("Snapshot ledger recovery length is invalid");
    }
    this.#length = length;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#handle.close();
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("Snapshot ledger is closed");
  }
}

function encodeLedgerRecord(frame: SnapshotFrame, placement: Placement | undefined): Uint8Array {
  validateFrameScalars(frame);
  const isBlock = frame.kind === "block";
  if (isBlock !== (placement !== undefined)) {
    throw new TypeError("Snapshot ledger block placement is inconsistent");
  }
  if (placement !== undefined) {
    assertValidPlacement(placement);
    if (placement.length !== frame.payload.byteLength || placement.checksum !== frame.checksum) {
      throw new Error("Snapshot block placement disagrees with its frame");
    }
  }
  const keyBytes = frame.key === null ? new Uint8Array() : new TextEncoder().encode(frame.key);
  const inlineLength = placement === undefined ? frame.payload.byteLength : 0;
  const recordLength = safeLedgerSum(
    LEDGER_PREFIX_BYTES + keyBytes.byteLength,
    inlineLength,
    "Snapshot ledger record length",
  );
  const encoded = new Uint8Array(recordLength);
  const view = new DataView(encoded.buffer);
  view.setUint32(0, LEDGER_MAGIC, true);
  view.setUint32(4, recordLength, true);
  view.setBigUint64(8, BigInt(frame.sequence), true);
  view.setUint32(16, SNAPSHOT_FRAME_KINDS.indexOf(frame.kind), true);
  view.setUint32(20, frame.itemCount, true);
  view.setUint32(24, keyBytes.byteLength, true);
  view.setUint32(28, placement === undefined ? MODE_INLINE : MODE_PLACEMENT, true);
  view.setBigUint64(32, BigInt(frame.payload.byteLength), true);
  view.setUint32(40, frame.checksum, true);
  if (placement !== undefined) {
    view.setBigUint64(44, BigInt(placement.extent), true);
    view.setBigUint64(52, BigInt(placement.offset), true);
    view.setBigUint64(60, BigInt(placement.length), true);
    view.setUint32(68, placement.checksum, true);
  }
  // 72..75 are reserved zero; 76..79 is the record checksum itself.
  encoded.set(keyBytes, LEDGER_PREFIX_BYTES);
  if (placement === undefined)
    encoded.set(frame.payload, LEDGER_PREFIX_BYTES + keyBytes.byteLength);
  view.setUint32(76, 0, true);
  view.setUint32(76, crc32(encoded), true);
  return encoded;
}

function decodeLedgerRecord(encoded: Uint8Array): SnapshotLedgerRecord {
  const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
  const sequence = Number(view.getBigUint64(8, true));
  const kind = SNAPSHOT_FRAME_KINDS[view.getUint32(16, true)];
  const itemCount = view.getUint32(20, true);
  const keyLength = view.getUint32(24, true);
  const mode = view.getUint32(28, true);
  const payloadLength = Number(view.getBigUint64(32, true));
  const checksum = view.getUint32(40, true);
  if (
    kind === undefined ||
    !Number.isSafeInteger(sequence) ||
    sequence < 0 ||
    itemCount < 1 ||
    itemCount > MAX_SNAPSHOT_FRAME_ITEMS ||
    !Number.isSafeInteger(payloadLength) ||
    payloadLength < 1 ||
    (kind === "block"
      ? payloadLength > MAX_STORED_BLOCK_BYTE_LENGTH
      : payloadLength > MAX_SNAPSHOT_METADATA_FRAME_BYTES) ||
    (mode !== MODE_INLINE && mode !== MODE_PLACEMENT) ||
    encoded.subarray(72, 76).some((byte) => byte !== 0)
  ) {
    throw new TypeError("Snapshot ledger record header is invalid");
  }
  const inlineLength = mode === MODE_INLINE ? payloadLength : 0;
  if (LEDGER_PREFIX_BYTES + keyLength + inlineLength !== encoded.byteLength) {
    throw new Error("Snapshot ledger record body length is invalid");
  }
  let key: string | null = null;
  if (keyLength > 0) {
    try {
      key = validateStorageId(
        new TextDecoder("utf-8", { fatal: true }).decode(
          encoded.subarray(LEDGER_PREFIX_BYTES, LEDGER_PREFIX_BYTES + keyLength),
        ),
      );
    } catch {
      throw new TypeError("Snapshot ledger key is not canonical UTF-8");
    }
  }
  if ((kind === "block") !== (key !== null) || (kind === "block") !== (mode === MODE_PLACEMENT)) {
    throw new TypeError("Snapshot ledger frame identity is inconsistent");
  }
  if (mode === MODE_INLINE) {
    const payload = encoded.slice(LEDGER_PREFIX_BYTES + keyLength);
    if (crc32(payload) !== checksum) throw new Error("Snapshot ledger payload checksum mismatch");
    return { sequence, kind, itemCount, key, payloadLength, checksum, payload };
  }
  const placement: Placement = {
    extent: Number(view.getBigUint64(44, true)),
    offset: Number(view.getBigUint64(52, true)),
    length: Number(view.getBigUint64(60, true)),
    checksum: view.getUint32(68, true),
  };
  assertValidPlacement(placement);
  if (placement.length !== payloadLength || placement.checksum !== checksum) {
    throw new Error("Snapshot ledger placement disagrees with its frame");
  }
  return { sequence, kind, itemCount, key, payloadLength, checksum, placement };
}

function ledgerRecordFromFrame(
  frame: SnapshotFrame,
  placement: Placement | undefined,
): SnapshotLedgerRecord {
  return {
    sequence: frame.sequence,
    kind: frame.kind,
    itemCount: frame.itemCount,
    key: frame.key,
    payloadLength: frame.payload.byteLength,
    checksum: frame.checksum,
    ...(placement === undefined ? { payload: frame.payload } : { placement }),
  };
}

function validateFrameScalars(frame: SnapshotFrame): void {
  if (
    !Number.isSafeInteger(frame.sequence) ||
    frame.sequence < 0 ||
    !SNAPSHOT_FRAME_KINDS.includes(frame.kind) ||
    !Number.isSafeInteger(frame.itemCount) ||
    frame.itemCount < 1 ||
    frame.itemCount > MAX_SNAPSHOT_FRAME_ITEMS ||
    !(frame.payload instanceof Uint8Array) ||
    !Number.isSafeInteger(frame.checksum) ||
    frame.checksum < 0 ||
    frame.checksum > 0xffff_ffff
  ) {
    throw new TypeError("Snapshot frame is invalid");
  }
  if (crc32(frame.payload) !== frame.checksum) {
    throw new Error("Snapshot frame checksum mismatch");
  }
  if (frame.kind === "block") {
    if (frame.key === null || frame.itemCount !== 1) {
      throw new TypeError("Snapshot block frame identity is invalid");
    }
    validateStorageId(frame.key);
    if (frame.payload.byteLength < 1 || frame.payload.byteLength > MAX_STORED_BLOCK_BYTE_LENGTH) {
      throw new RangeError("Snapshot block frame is too large");
    }
  } else if (
    frame.key !== null ||
    frame.payload.byteLength < 1 ||
    frame.payload.byteLength > MAX_SNAPSHOT_METADATA_FRAME_BYTES
  ) {
    throw new TypeError("Snapshot metadata frame identity is invalid");
  }
}

function safeLedgerSum(left: number, right: number, label: string): number {
  if (
    !Number.isSafeInteger(left) ||
    left < 0 ||
    !Number.isSafeInteger(right) ||
    right < 0 ||
    left > Number.MAX_SAFE_INTEGER - right
  ) {
    throw new RangeError(`${label} exceeds the safe integer range`);
  }
  return left + right;
}
