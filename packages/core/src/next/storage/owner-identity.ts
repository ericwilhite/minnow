/** Durable, monotonic native ownership identity. No capability proof can retarget a newer lock. */
import {
  DURABLE_FORMATS,
  DURABLE_READER_VERSION,
  DURABLE_WRITER_VERSION,
  DurableFormatError,
  requireHeader,
} from "../durable-format.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { OwnershipLostError, StoreCorruptionError } from "./contract.js";

export interface OwnerIdentity {
  readonly epoch: number;
  readonly generation: number;
  readonly reader: number;
  readonly writer: number;
}
export const EMPTY_IDENTITY: OwnerIdentity = Object.freeze({
  epoch: 0,
  generation: 0,
  reader: 0,
  writer: 0,
});
export const OWNER_IDENTITY_BYTES = 40;
const OWNER_MAGIC = DURABLE_FORMATS.epochSlot.magic;
export function sameIdentity(a: OwnerIdentity, b: OwnerIdentity): boolean {
  return (
    a.epoch === b.epoch &&
    a.generation === b.generation &&
    a.reader === b.reader &&
    a.writer === b.writer
  );
}
export function requireIdentity(actual: OwnerIdentity, expected: OwnerIdentity): void {
  if (!sameIdentity(actual, expected)) throw new OwnershipLostError(expected.epoch);
}
export function nextIdentity(previous: OwnerIdentity, takeover: boolean): OwnerIdentity {
  const epoch = previous.epoch + (takeover ? 1 : 0),
    generation = previous.generation + 1;
  if (!Number.isSafeInteger(epoch) || !Number.isSafeInteger(generation) || epoch < 1)
    throw new StoreCorruptionError("The native owner identity is exhausted");
  return { epoch, generation, reader: DURABLE_READER_VERSION, writer: DURABLE_WRITER_VERSION };
}
export function requireIdentityCapabilities(identity: OwnerIdentity): void {
  if (identity.reader > DURABLE_READER_VERSION || identity.writer > DURABLE_WRITER_VERSION)
    throw new DurableFormatError(
      `Native fence requires reader ${String(identity.reader)} / writer ${String(identity.writer)}`,
    );
}
export function encodeOwnerIdentity(identity: OwnerIdentity): Uint8Array {
  const bytes = new Uint8Array(OWNER_IDENTITY_BYTES),
    view = new DataView(bytes.buffer);
  view.setUint32(0, OWNER_MAGIC, true); // MNE2; prefix CRC lets old MNEP builds refuse the magic.
  view.setUint32(4, identity.epoch >>> 0, true);
  view.setUint32(8, Math.floor(identity.epoch / 2 ** 32), true);
  view.setUint32(12, xxHash32(bytes, 0, 0, 12), true);
  view.setUint32(16, identity.generation >>> 0, true);
  view.setUint32(20, Math.floor(identity.generation / 2 ** 32), true);
  view.setUint32(24, identity.reader, true);
  view.setUint32(28, identity.writer, true);
  view.setUint32(36, xxHash32(bytes, 0, 0, 36), true);
  return bytes;
}
/** Torn slots are recoverable; intact unknown vocabulary refuses before any fencing effect. */
export function decodeOwnerIdentity(bytes: Uint8Array): OwnerIdentity | null {
  if (bytes.length < 16) return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(12, true) !== xxHash32(bytes, 0, 0, 12)) return null;
  // The magic is protected by the prefix CRC, even if an older header has only 16 bytes.
  if (view.getUint32(0, true) !== OWNER_MAGIC)
    throw new DurableFormatError("Unsupported native fence identity");
  if (bytes.length < OWNER_IDENTITY_BYTES || view.getUint32(36, true) !== xxHash32(bytes, 0, 0, 36))
    return null;

  const identity = {
    epoch: view.getUint32(4, true) + view.getUint32(8, true) * 2 ** 32,
    generation: view.getUint32(16, true) + view.getUint32(20, true) * 2 ** 32,
    reader: view.getUint32(24, true),
    writer: view.getUint32(28, true),
  };
  if (
    identity.epoch < 1 ||
    identity.generation < identity.epoch ||
    identity.reader < 1 ||
    identity.writer < 1
  )
    throw new StoreCorruptionError("Malformed native fence identity");
  requireIdentityCapabilities(identity);
  requireHeader("epochSlot", bytes, StoreCorruptionError);
  return identity;
}
/** IndexedDB has no torn-record recovery: a present invalid record is corruption. */
export function storedIdentity(value: unknown): OwnerIdentity {
  if (value === undefined) return EMPTY_IDENTITY;
  if (!(value instanceof ArrayBuffer))
    throw new StoreCorruptionError("Unsupported or malformed native fence record");
  if (value.byteLength < 16) throw new StoreCorruptionError("Malformed native fence length");
  // Recognize a checksummed future magic/floor before applying this layout's exact length.
  const identity = decodeOwnerIdentity(
    new Uint8Array(value, 0, Math.min(value.byteLength, OWNER_IDENTITY_BYTES)),
  );
  if (value.byteLength !== OWNER_IDENTITY_BYTES)
    throw new StoreCorruptionError("Malformed native fence length");
  if (identity === null) throw new StoreCorruptionError("Malformed native fence checksum");
  return identity;
}
/** Length-prefix the logical name: arbitrary names cannot alias an identity suffix. */
export function identityOwnerName(name: string, identity: OwnerIdentity): string {
  return `minnow-owner:${String(name.length)}:${name}:${String(identity.epoch)}:${String(identity.generation)}:${String(identity.reader)}:${String(identity.writer)}`;
}
export function identityGateName(name: string, identity: OwnerIdentity): string {
  return `minnow-gate:${identityOwnerName(name, identity)}`;
}

/** A held-name snapshot is only an acquisition hint. The native CAS still requires equality. */
export function identityFromNames(
  name: string,
  names: readonly string[],
): OwnerIdentity | undefined {
  const prefix = `minnow-owner:${String(name.length)}:${name}:`;
  let found: OwnerIdentity | undefined;
  for (const held of names) {
    if (!held.startsWith(prefix)) continue;
    const parts = held.slice(prefix.length).split(":").map(Number);
    if (parts.length !== 4 || parts.some((part) => !Number.isSafeInteger(part) || part < 0))
      continue;
    const [epoch, generation, reader, writer] = parts as [number, number, number, number];
    if (
      found === undefined ||
      generation > found.generation ||
      (generation === found.generation && epoch > found.epoch)
    )
      found = { epoch, generation, reader, writer };
  }
  if (found !== undefined) requireIdentityCapabilities(found);
  return found;
}
