import { crc32 } from "../../block-format/index.js";
import { readFully, writeFully, type SyncFileHandle } from "./sync-file.js";

const MAGIC = 0x414e4e4d;
const SLOT_BYTES = 24;
export interface WalAcknowledgement {
  readonly sequence: number;
  readonly endOffset: number;
}

/** Distinguishes a structurally incomplete witness from an unexpected platform read failure. */
export class WalAcknowledgementCorruptionError extends Error {}

/** Two initialized CRC-protected slots, independent of WAL bytes. Damage to either slot is
 * reported rather than falling back to a possibly older acknowledged state. An interrupted
 * slot update can therefore refuse recovery, but can never silently lose an acknowledged write. */
export class WalAcknowledgements {
  readonly #handle: SyncFileHandle;
  #latest: WalAcknowledgement;

  constructor(handle: SyncFileHandle, emptyDatabase: boolean) {
    this.#handle = handle;
    if (handle.getSize() === 0 && emptyDatabase) {
      const initial = this.#encode({ sequence: 0, endOffset: 0 });
      writeFully(handle, initial, 0, "initializing WAL acknowledgement");
      writeFully(handle, initial, SLOT_BYTES, "initializing WAL acknowledgement mirror");
      handle.flush();
    }
    if (handle.getSize() !== SLOT_BYTES * 2)
      throw new WalAcknowledgementCorruptionError(
        "WAL acknowledgement file is missing or truncated",
      );
    const slots: WalAcknowledgement[] = [];
    for (let index = 0; index < 2; index += 1) {
      const bytes = new Uint8Array(SLOT_BYTES);
      readFully(handle, bytes, index * SLOT_BYTES, "reading WAL acknowledgement");
      const view = new DataView(bytes.buffer);
      if (
        view.getUint32(0, true) !== MAGIC ||
        view.getUint32(4, true) !== crc32(bytes.subarray(8))
      ) {
        throw new WalAcknowledgementCorruptionError("WAL acknowledgement slot is corrupt");
      }
      const sequence = Number(view.getBigUint64(8, true));
      const endOffset = Number(view.getBigUint64(16, true));
      if (!Number.isSafeInteger(sequence) || !Number.isSafeInteger(endOffset))
        throw new WalAcknowledgementCorruptionError("Invalid WAL acknowledgement boundary");
      slots.push({ sequence, endOffset });
    }
    slots.sort((a, b) => b.sequence - a.sequence || b.endOffset - a.endOffset);
    const latest = slots[0];
    if (latest === undefined)
      throw new WalAcknowledgementCorruptionError("WAL acknowledgement has no valid slot");
    this.#latest = latest;
  }

  get latest(): WalAcknowledgement {
    return this.#latest;
  }

  publish(sequence: number, endOffset: number): void {
    const acknowledgement = { sequence, endOffset };
    const bytes = this.#encode(acknowledgement);
    writeFully(this.#handle, bytes, (sequence % 2) * SLOT_BYTES, "publishing WAL acknowledgement");
    this.#handle.flush();
    this.#latest = acknowledgement;
  }

  #encode({ sequence, endOffset }: WalAcknowledgement): Uint8Array {
    if (
      !Number.isSafeInteger(sequence) ||
      sequence < 0 ||
      !Number.isSafeInteger(endOffset) ||
      endOffset < 0
    ) {
      throw new RangeError("Invalid WAL acknowledgement boundary");
    }
    const bytes = new Uint8Array(SLOT_BYTES);
    const view = new DataView(bytes.buffer);
    view.setUint32(0, MAGIC, true);
    view.setBigUint64(8, BigInt(sequence), true);
    view.setBigUint64(16, BigInt(endOffset), true);
    view.setUint32(4, crc32(bytes.subarray(8)), true);
    return bytes;
  }
}
