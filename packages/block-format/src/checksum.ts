const TABLES = new Uint32Array(256 * 8);

for (let index = 0; index < 256; index += 1) {
  let value = index;
  for (let bit = 0; bit < 8; bit += 1) {
    value = (value & 1) === 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
  }
  TABLES[index] = value >>> 0;
}
// Slicing-by-8 extension tables: TABLES[k*256+b] is the CRC of byte b followed by k zero bytes,
// letting the hot loop fold eight bytes per iteration instead of one.
for (let slice = 1; slice < 8; slice += 1) {
  for (let index = 0; index < 256; index += 1) {
    const previous = TABLES[(slice - 1) * 256 + index] ?? 0;
    TABLES[slice * 256 + index] = ((previous >>> 8) ^ (TABLES[previous & 0xff] ?? 0)) >>> 0;
  }
}

/**
 * CRC-32 (IEEE) via slicing-by-8. Every decoded block is checksummed end to end, so this is a
 * per-query hot path: eight table folds per 8-byte word beat the byte-at-a-time loop several
 * times over, and the tail falls back to single-byte steps.
 */
export function crc32(bytes: Uint8Array): number {
  let checksum = 0xffffffff;
  const length = bytes.byteLength;
  const wordEnd = length - (length % 8);
  let index = 0;
  while (index < wordEnd) {
    const low =
      (checksum ^
        ((bytes[index] ?? 0) |
          ((bytes[index + 1] ?? 0) << 8) |
          ((bytes[index + 2] ?? 0) << 16) |
          ((bytes[index + 3] ?? 0) << 24))) >>>
      0;
    const high =
      ((bytes[index + 4] ?? 0) |
        ((bytes[index + 5] ?? 0) << 8) |
        ((bytes[index + 6] ?? 0) << 16) |
        ((bytes[index + 7] ?? 0) << 24)) >>>
      0;
    checksum =
      ((TABLES[1792 + (low & 0xff)] ?? 0) ^
        (TABLES[1536 + ((low >>> 8) & 0xff)] ?? 0) ^
        (TABLES[1280 + ((low >>> 16) & 0xff)] ?? 0) ^
        (TABLES[1024 + (low >>> 24)] ?? 0) ^
        (TABLES[768 + (high & 0xff)] ?? 0) ^
        (TABLES[512 + ((high >>> 8) & 0xff)] ?? 0) ^
        (TABLES[256 + ((high >>> 16) & 0xff)] ?? 0) ^
        (TABLES[high >>> 24] ?? 0)) >>>
      0;
    index += 8;
  }
  for (; index < length; index += 1) {
    checksum = (checksum >>> 8) ^ (TABLES[(checksum ^ (bytes[index] ?? 0)) & 0xff] ?? 0);
  }
  return (checksum ^ 0xffffffff) >>> 0;
}
