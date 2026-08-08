const TABLE = new Uint32Array(256);

for (let index = 0; index < TABLE.length; index += 1) {
  let value = index;
  for (let bit = 0; bit < 8; bit += 1) {
    value = (value & 1) === 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
  }
  TABLE[index] = value >>> 0;
}

export function crc32(bytes: Uint8Array): number {
  let checksum = 0xffffffff;
  for (const byte of bytes) {
    checksum = (checksum >>> 8) ^ (TABLE[(checksum ^ byte) & 0xff] ?? 0);
  }
  return (checksum ^ 0xffffffff) >>> 0;
}
