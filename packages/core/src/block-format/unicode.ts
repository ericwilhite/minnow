/**
 * Returns the exact UTF-8 byte length of a well-formed JavaScript string without allocating.
 *
 * UTF-8 cannot represent isolated UTF-16 surrogate code units. `TextEncoder` silently replaces
 * them with U+FFFD, which would make persistence lossy and can collapse distinct index keys.
 * Minnow rejects those strings instead, so every accepted string has one exact UTF-8 encoding
 * and round-trips byte-for-byte through the block format.
 */
export function wellFormedUtf8ByteLength(value: string, context = "String value"): number {
  return measureWellFormedUtf8(value, context);
}

/** Rejects strings that cannot round-trip exactly through UTF-8 persistence. */
export function assertWellFormedString(value: string, context = "String value"): void {
  wellFormedUtf8ByteLength(value, context);
}

function measureWellFormedUtf8(value: string, context: string): number {
  if (typeof value !== "string") throw new TypeError(`${context} must be a string`);
  let byteLength = 0;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x7f) {
      byteLength += 1;
    } else if (code <= 0x7ff) {
      byteLength += 2;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const low = index + 1 < value.length ? value.charCodeAt(index + 1) : 0;
      if (low < 0xdc00 || low > 0xdfff) throw unpairedSurrogate(context, index);
      byteLength += 4;
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw unpairedSurrogate(context, index);
    } else {
      byteLength += 3;
    }
  }
  return byteLength;
}

function unpairedSurrogate(context: string, index: number): TypeError {
  return new TypeError(
    `${context} contains an unpaired surrogate at UTF-16 index ${String(index)}`,
  );
}
