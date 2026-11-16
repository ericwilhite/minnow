/**
 * Two-slot root publication, shared by the OPFS and IndexedDB stores to meet contract.ts
 * guarantee 1 on platforms whose single writes can tear.
 *
 * A publish writes the root into the slot that does not hold the current root, with a sequence
 * one higher, and flushes. `chooseRoot` reads both slots and returns the valid slot with the
 * higher sequence. A torn write damages only the slot being written, so the other slot (the
 * previous root) still wins.
 *
 * Slot layout (little-endian): magic u32 "MNRT" | sequence u32 lo | sequence u32 hi |
 * root length u32 | root bytes | xxHash32 of everything before it (seed0).
 * Low21 sequence-high bits remain u53 sequence. Bits21..23=7 select the profile8 escape;
 * bits24..31 hold manifest-format-minus2. The checksummed payload begins with min-reader u32
 * and min-writer u32, then the opaque manifest. Length includes this8-byte extension.
 * Both capabilities are required for native ownership; there is no read-only owner fallback.
 */
import {
  DURABLE_FORMATS,
  DURABLE_READER_VERSION,
  DURABLE_WRITER_VERSION,
  MANIFEST_FORMAT_VERSION,
  ROOT_SLOT_HEADER_BYTES,
  ROOT_SLOT_CAPABILITY_BYTES,
  ROOT_SLOT_PAYLOAD_OFFSET,
  ROOT_SLOT_OVERHEAD,
  ROOT_SLOT_EXTENSION_TAG,
  DurableFormatError,
  requireHeader,
} from "../durable-format.js";
import { StoreCorruptionError } from "./contract.js";
import { xxHash32 } from "../hash/xxhash32.js";

const MAGIC = DURABLE_FORMATS.rootSlot.magic; // "MNRT"
const HEADER = ROOT_SLOT_HEADER_BYTES;

export interface RootSlot {
  readonly sequence: number;
  readonly root: Uint8Array;
}

export function encodeRootSlot(sequence: number, root: Uint8Array): Uint8Array {
  if (!Number.isSafeInteger(sequence) || sequence < 1) throw new RangeError("Bad root sequence");
  const bytes = new Uint8Array(ROOT_SLOT_OVERHEAD + root.length);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint32(4, sequence >>> 0, true);
  view.setUint32(8, Math.floor(sequence / 2 ** 32), true);
  view.setUint32(12, ROOT_SLOT_CAPABILITY_BYTES + root.length, true);
  writeRootVersion(view);
  bytes.set(root, ROOT_SLOT_PAYLOAD_OFFSET);
  view.setUint32(bytes.length - 4, xxHash32(bytes, 0, 0, bytes.length - 4), true);
  return bytes;
}

/** Shared by the prepaid root frame; no allocation or dependency on the inner codec. */
export function writeRootVersion(view: DataView): void {
  view.setUint8(10, (view.getUint8(10) & 31) | (ROOT_SLOT_EXTENSION_TAG << 5));
  view.setUint8(11, MANIFEST_FORMAT_VERSION - 2);
  view.setUint32(16, DURABLE_READER_VERSION, true);
  view.setUint32(20, DURABLE_WRITER_VERSION, true);
}

/** Empty/torn slots return null; an intact unsupported identity/version refuses before fencing. */
export function decodeRootSlot(bytes: Uint8Array | null, borrow = false): RootSlot | null {
  if (bytes === null || bytes.length < HEADER + 4) return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const length = view.getUint32(12, true);
  if (HEADER + length + 4 > bytes.length) return null;
  if (view.getUint32(HEADER + length, true) !== xxHash32(bytes, 0, 0, HEADER + length)) return null;
  if (length < ROOT_SLOT_CAPABILITY_BYTES)
    throw new StoreCorruptionError("A root slot lacks its complete capability extension");
  requireHeader("rootSlot", bytes, DurableFormatError);
  if (view.getUint32(4, true) === 0 && (view.getUint32(8, true) & 0x1fffff) === 0)
    throw new StoreCorruptionError("A root slot has sequence zero");
  return {
    sequence: view.getUint32(4, true) + (view.getUint32(8, true) & 0x1fffff) * 2 ** 32,
    root: borrow
      ? bytes.subarray(ROOT_SLOT_PAYLOAD_OFFSET, HEADER + length)
      : bytes.slice(ROOT_SLOT_PAYLOAD_OFFSET, HEADER + length),
  };
}

/**
 * The current root of two slots, the previous one (the other slot, when valid and older), and which
 * slot the next publish must overwrite (the previous root's).
 */
export function chooseRoot(
  first: Uint8Array | null,
  second: Uint8Array | null,
): {
  readonly current: RootSlot | null;
  readonly previous: RootSlot | null;
  readonly nextSlot: 0 | 1;
  readonly nextSequence: number;
} {
  const a = decodeRootSlot(first);
  const b = decodeRootSlot(second);
  if (a !== null && (b === null || a.sequence > b.sequence))
    return { current: a, previous: b, nextSlot: 1, nextSequence: a.sequence + 1 };
  if (b !== null)
    return {
      current: b,
      previous: a !== null && a.sequence < b.sequence ? a : null,
      nextSlot: 0,
      nextSequence: b.sequence + 1,
    };
  return { current: null, previous: null, nextSlot: 0, nextSequence: 1 };
}
