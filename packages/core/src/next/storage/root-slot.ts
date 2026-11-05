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
 * root length u32 | root bytes | xxHash32 of everything before it (seed 0).
 */
import { xxHash32 } from "../hash/xxhash32.js";

const MAGIC = 0x54524e4d; // "MNRT"
const HEADER = 16;

export interface RootSlot {
  readonly sequence: number;
  readonly root: Uint8Array;
}

export function encodeRootSlot(sequence: number, root: Uint8Array): Uint8Array {
  if (!Number.isSafeInteger(sequence) || sequence < 1) throw new RangeError("Bad root sequence");
  const bytes = new Uint8Array(HEADER + root.length + 4);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint32(4, sequence >>> 0, true);
  view.setUint32(8, Math.floor(sequence / 2 ** 32), true);
  view.setUint32(12, root.length, true);
  bytes.set(root, HEADER);
  view.setUint32(HEADER + root.length, xxHash32(bytes, 0, 0, HEADER + root.length), true);
  return bytes;
}

/** A slot's root, or null when the slot is empty, torn or not a root slot. */
export function decodeRootSlot(bytes: Uint8Array | null): RootSlot | null {
  if (bytes === null || bytes.length < HEADER + 4) return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(0, true) !== MAGIC) return null;
  const length = view.getUint32(12, true);
  if (HEADER + length + 4 > bytes.length) return null;
  if (view.getUint32(HEADER + length, true) !== xxHash32(bytes, 0, 0, HEADER + length)) return null;
  return {
    sequence: view.getUint32(4, true) + view.getUint32(8, true) * 2 ** 32,
    root: bytes.slice(HEADER, HEADER + length),
  };
}

/** The current root of two slots, and which slot the next publish must overwrite. */
export function chooseRoot(
  first: Uint8Array | null,
  second: Uint8Array | null,
): { readonly current: RootSlot | null; readonly nextSlot: 0 | 1; readonly nextSequence: number } {
  const a = decodeRootSlot(first);
  const b = decodeRootSlot(second);
  if (a !== null && (b === null || a.sequence > b.sequence))
    return { current: a, nextSlot: 1, nextSequence: a.sequence + 1 };
  if (b !== null) return { current: b, nextSlot: 0, nextSequence: b.sequence + 1 };
  return { current: null, nextSlot: 0, nextSequence: 1 };
}
