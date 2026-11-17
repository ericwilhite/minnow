/** Durable database incarnation, independent of owner epochs and recyclable WAL salts.
 * Native creation seeds it; replacement advances it. Ordinary publications preserve it.
 */
import { DatabaseIdentityExhaustedError } from "../errors.js";
import { DATABASE_IDENTITY_CODEC } from "../durable-format.js";
import type { ByteBudget } from "../storage/extents.js";
import { ReadDeniedError } from "../storage/extents.js";
import { ByteReader, ByteWriter, ManifestCorruptionError } from "./bytes.js";

export const DATABASE_ID_BYTES = DATABASE_IDENTITY_CODEC.bytes;
export const MAX_DATABASE_ID = (1n << BigInt(DATABASE_ID_BYTES * 8)) - 1n;

export { DatabaseIdentityExhaustedError };
export function writeDatabaseIdentity(out: ByteWriter, id: bigint): void {
  if (id <= 0n || id > MAX_DATABASE_ID)
    throw new ManifestCorruptionError("Invalid database identity");
  for (
    let shift = 0n;
    shift < BigInt(DATABASE_ID_BYTES * 8);
    shift += BigInt(DATABASE_IDENTITY_CODEC.wordBits)
  )
    out.u32(Number((id >> shift) & 0xffffffffn));
}
export function readDatabaseIdentity(reader: ByteReader): bigint {
  let id = 0n;
  for (
    let shift = 0n;
    shift < BigInt(DATABASE_ID_BYTES * 8);
    shift += BigInt(DATABASE_IDENTITY_CODEC.wordBits)
  )
    id |= BigInt(reader.u32()) << shift;
  if (id === 0n) throw new ManifestCorruptionError("Zero database identity");
  return id;
}

/** Temporary random words are funded before allocation. The returned scalar belongs to the
 * caller's already-funded manifest/control graph (like the other root scalar fields).
 */
export function freshDatabaseIdentity(budget: ByteBudget): bigint {
  const charge = 80;
  if (!budget.allocate(charge)) throw new ReadDeniedError(charge);
  try {
    const words = crypto.getRandomValues(new Uint32Array(DATABASE_IDENTITY_CODEC.words));
    let id = 0n;
    for (let i = 0; i < DATABASE_IDENTITY_CODEC.words; i++)
      id |= BigInt(words[i] ?? 0) << BigInt(i * 32);
    return id || 1n;
  } finally {
    budget.free(charge);
  }
}
export function nextDatabaseIdentity(id: bigint): bigint {
  if (id <= 0n || id > MAX_DATABASE_ID)
    throw new ManifestCorruptionError("Invalid database identity");
  if (id === MAX_DATABASE_ID) throw new DatabaseIdentityExhaustedError();
  return id + 1n;
}
