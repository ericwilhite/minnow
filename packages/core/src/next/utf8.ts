/** UTF-8 encoding into an admitted destination, without temporary buffers or strings.
 * WHATWG Encoding: native progress is a prefix, not a promise to consume the input.
 * A short native write resumes scalar by scalar, without allocating a UTF16 suffix,
 * another byte buffer, or scratch. Callers retain their existing cooperative slices.
 */
const encoder = new TextEncoder();

export class Utf8CapacityError extends RangeError {
  readonly code = "MINNOW_UTF8_CAPACITY";
  constructor(
    readonly requiredBytes: number,
    readonly availableBytes: number,
  ) {
    super(
      `UTF-8 destination needs ${String(requiredBytes)} bytes; ${String(availableBytes)} available`,
    );
    this.name = "Utf8CapacityError";
  }
}

/** UTF-8 length without a copy. Unpaired UTF16 surrogates become U+FFFD. */
export function utf8Length(value: string): number {
  let bytes = 0;
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c < 0x80) bytes++;
    else if (c < 0x800) bytes += 2;
    else if (
      c >= 0xd800 &&
      c <= 0xdbff &&
      value.charCodeAt(i + 1) >= 0xdc00 &&
      value.charCodeAt(i + 1) <= 0xdfff
    ) {
      bytes += 4;
      i++;
    } else bytes += 3;
  }
  return bytes;
}

/** `measuredBytes`, if supplied, must be the source's exact UTF-8 length.
 * Capacity is checked before mutation. Only [0, measuredBytes) is written.
 * The native fast path needs no padded tail; a short write (including no progress)
 * is completed using scalar locals in the same destination, never by slicing source.
 */
export function encodeUtf8Into(
  value: string,
  target: Uint8Array,
  measuredBytes = utf8Length(value),
): number {
  if (target.length < measuredBytes) throw new Utf8CapacityError(measuredBytes, target.length);
  const progress = encoder.encodeInto(value, target);
  let read = progress.read,
    written = progress.written;
  while (read < value.length) {
    let point = value.charCodeAt(read++);
    if (point >= 0xd800 && point <= 0xdbff) {
      const low = value.charCodeAt(read);
      if (low >= 0xdc00 && low <= 0xdfff) {
        point = 0x10000 + ((point - 0xd800) << 10) + low - 0xdc00;
        read++;
      } else point = 0xfffd;
    } else if (point >= 0xdc00 && point <= 0xdfff) point = 0xfffd;
    const width = point < 0x80 ? 1 : point < 0x800 ? 2 : point < 0x10000 ? 3 : 4;
    if (written + width > target.length)
      throw new Utf8CapacityError(utf8Length(value), target.length);
    if (width === 1) target[written++] = point;
    else {
      target[written++] =
        width === 2
          ? 0xc0 | (point >>> 6)
          : width === 3
            ? 0xe0 | (point >>> 12)
            : 0xf0 | (point >>> 18);
      for (let shift = (width - 2) * 6; shift >= 0; shift -= 6)
        target[written++] = 0x80 | ((point >>> shift) & 0x3f);
    }
  }
  if (written !== measuredBytes)
    throw new TypeError("UTF-8 byte measurement changed while encoding");
  return written;
}
