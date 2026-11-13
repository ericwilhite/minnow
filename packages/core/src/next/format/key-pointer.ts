/** Bounded key records/fences; the chunk directory and payload precede their referencing block. */
import { DURABLE_CODECS, DURABLE_FORMATS, RUN_CODEC, requireTag } from "../durable-format.js";
import { ManifestCorruptionError } from "./bytes.js";
import { ByteReader, ByteWriter, valid } from "./bytes.js";
import { type KeyChunk, RUN_KEY_PREFIX_BYTES } from "./run-key.js";
export const KEY_DIRECTORY_MAGIC = DURABLE_FORMATS.runKeyDirectory.magic;
export const KEY_DIRECTORY_VERSION = DURABLE_FORMATS.runKeyDirectory.version;
export const EXTENDED_KEY_BLOCK = RUN_CODEC.extendedKeyBlock;
export type KeyPointer =
  | { readonly kind: "inline"; readonly bytes: Uint8Array }
  | {
      readonly kind: "overflow";
      readonly length: number;
      readonly prefix: Uint8Array;
      readonly directory: KeyChunk;
    };
function check(pointer: KeyPointer, before: number): void {
  valid(Number.isSafeInteger(before) && before >= 0, "Invalid key pointer bound");
  if (pointer.kind === "inline") {
    valid(pointer.bytes.length <= 2048, "Oversized inline key pointer");
    return;
  }
  valid(
    Number.isSafeInteger(pointer.length) && pointer.length > 2048,
    "Invalid key pointer length",
  );
  valid(pointer.prefix.length === RUN_KEY_PREFIX_BYTES, "Invalid key pointer prefix");
  const ref = pointer.directory;
  valid(
    Number.isSafeInteger(ref.offset) && ref.offset >= pointer.length,
    "Key payload must precede directory",
  );
  valid(
    Number.isInteger(ref.length) && ref.length > 0 && ref.length <= 1024 * 1024,
    "Invalid key directory size",
  );
  valid(ref.offset <= before - ref.length, "Key directory must precede block");
  valid(
    Number.isInteger(ref.checksum) && ref.checksum >= 0 && ref.checksum <= 0xffffffff,
    "Invalid key directory checksum",
  );
}
export function writeKeyPointer(
  out: Pick<ByteWriter, "u8" | "uint" | "bytes" | "u32">,
  pointer: KeyPointer,
  before: number,
): void {
  check(pointer, before);
  out.u8(
    pointer.kind === "inline"
      ? DURABLE_CODECS.keyDescriptor.inline
      : DURABLE_CODECS.keyDescriptor.overflow,
  );
  if (pointer.kind === "inline") {
    out.bytes(pointer.bytes);
    return;
  }
  out.uint(pointer.length);
  out.bytes(pointer.prefix);
  out.uint(pointer.directory.offset);
  out.uint(pointer.directory.length);
  out.u32(pointer.directory.checksum);
}
export function readKeyPointer(
  input: Pick<ByteReader, "u8" | "uint" | "bytes" | "u32">,
  before: number,
): KeyPointer {
  const tag = input.u8();
  requireTag(
    tag,
    Object.values(DURABLE_CODECS.keyDescriptor),
    "key pointer tag",
    ManifestCorruptionError,
  );
  const pointer: KeyPointer =
    tag === DURABLE_CODECS.keyDescriptor.inline
      ? { kind: "inline", bytes: input.bytes() }
      : {
          kind: "overflow",
          length: input.uint(),
          prefix: input.bytes(),
          directory: { offset: input.uint(), length: input.uint(), checksum: input.u32() },
        };
  check(pointer, before);
  return pointer;
}
/** Encoded bounded pointer bytes retained by the summary owner, not complete long keys. */
export interface RunKeyStorage {
  readonly minimum: Uint8Array;
  readonly maximum: Uint8Array;
  readonly maxKeyBytes: number;
}
