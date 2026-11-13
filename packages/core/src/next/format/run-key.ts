/** Overflow-v1 descriptors: relative checksummed payload chunks in this run's extent.
 * Prefixes accelerate ordering only; equality requires the complete canonical key.
 * Bounds/context are validated before allocating decoded chunk arrays. The owner funds codecs.
 */
import { DURABLE_CODECS, RUN_CODEC, requireTag } from "../durable-format.js";
import { ManifestCorruptionError } from "./bytes.js";
import { ByteReader, ByteWriter, valid } from "./bytes.js";

export const RUN_KEY_PREFIX_BYTES = RUN_CODEC.keyPrefixBytes;
export const RUN_KEY_CHUNK_BYTES = RUN_CODEC.keyChunkBytes;
export interface KeyChunk {
  readonly offset: number;
  readonly length: number;
  readonly checksum: number;
}
export type RunKey =
  | { readonly kind: "inline"; readonly bytes: Uint8Array }
  | {
      readonly kind: "overflow";
      readonly length: number;
      readonly prefix: Uint8Array;
      readonly chunks: readonly KeyChunk[];
    };

function check(key: RunKey, payloadEnd: number): void {
  valid(Number.isSafeInteger(payloadEnd) && payloadEnd >= 0, "Invalid key payload bound");
  if (key.kind === "inline") {
    valid(key.bytes.length <= 2048, "Noncanonical inline key");
    return;
  }
  valid(Number.isSafeInteger(key.length) && key.length > 2048, "Noncanonical overflow length");
  valid(key.prefix.length === RUN_KEY_PREFIX_BYTES, "Invalid overflow prefix");
  valid(
    key.chunks.length === Math.ceil(key.length / RUN_KEY_CHUNK_BYTES),
    "Invalid key chunk count",
  );
  let total = 0,
    previousEnd = 0;
  for (const part of key.chunks) {
    valid(
      Number.isSafeInteger(part.offset) && part.offset >= previousEnd,
      "Overlapping key chunks",
    );
    valid(
      part.length === Math.min(RUN_KEY_CHUNK_BYTES, key.length - total),
      "Invalid key chunk length",
    );
    valid(part.offset <= payloadEnd - part.length, "Key chunk outside payload");
    valid(
      Number.isInteger(part.checksum) && part.checksum >= 0 && part.checksum <= 0xffffffff,
      "Invalid key chunk checksum",
    );
    previousEnd = part.offset + part.length;
    total += part.length;
  }
  valid(total === key.length, "Key chunks do not cover full length");
}

export function writeRunKey(out: ByteWriter, key: RunKey, payloadEnd: number): void {
  check(key, payloadEnd);
  out.u8(
    key.kind === "inline"
      ? DURABLE_CODECS.keyDescriptor.inline
      : DURABLE_CODECS.keyDescriptor.overflow,
  );
  if (key.kind === "inline") {
    out.bytes(key.bytes);
    return;
  }
  out.uint(key.length);
  out.bytes(key.prefix);
  out.uint(key.chunks.length);
  for (const part of key.chunks) {
    out.uint(part.offset);
    out.uint(part.length);
    out.u32(part.checksum);
  }
}

export function readRunKey(input: ByteReader, payloadEnd: number): RunKey {
  const tag = input.u8();
  requireTag(
    tag,
    Object.values(DURABLE_CODECS.keyDescriptor),
    "key descriptor tag",
    ManifestCorruptionError,
  );
  let key: RunKey;
  if (tag === DURABLE_CODECS.keyDescriptor.inline) key = { kind: "inline", bytes: input.bytes() };
  else {
    const length = input.uint(),
      prefix = input.bytes(),
      count = input.count(6);
    valid(Number.isSafeInteger(length) && length > 2048, "Noncanonical overflow length");
    valid(prefix.length === RUN_KEY_PREFIX_BYTES, "Invalid overflow prefix");
    valid(count === Math.ceil(length / RUN_KEY_CHUNK_BYTES), "Invalid key chunk count");
    const chunks: KeyChunk[] = [];
    for (let i = 0; i < count; i++)
      chunks.push({ offset: input.uint(), length: input.uint(), checksum: input.u32() });
    key = { kind: "overflow", length, prefix, chunks };
  }
  check(key, payloadEnd);
  return key;
}
