/** Read native rekey column images through the shared grammar without importing a writer. */
import type { PhysicalType } from "../types.js";
import { decodeColumnBytes } from "../vector/column-codec.js";
import { ProtocolError } from "./protocol.js";
class LiveColumnReader {
  offset = 0;
  constructor(readonly bytes: Uint8Array) {}
  u8(): number {
    if (this.offset >= this.bytes.length) throw new ProtocolError("A rekey column ends early");
    return this.bytes[this.offset++] ?? 0;
  }
  varuint(): number {
    let value = 0,
      scale = 1;
    for (let index = 0; index < 8; index++) {
      const byte = this.u8();
      value += (byte & 0x7f) * scale;
      if (byte < 0x80) {
        if (!Number.isSafeInteger(value)) break;
        return value;
      }
      scale *= 0x80;
    }
    throw new ProtocolError("A rekey integer is out of range");
  }
  raw(count: number): Uint8Array {
    if (!Number.isSafeInteger(count) || count < 0 || count > this.bytes.length - this.offset)
      throw new ProtocolError("A rekey column ends early");
    const result = this.bytes.subarray(this.offset, this.offset + count);
    this.offset += count;
    return result;
  }
  end(): void {
    if (this.offset !== this.bytes.length)
      throw new ProtocolError("A rekey column has trailing bytes");
  }
}
export function decodeLiveColumn(bytes: Uint8Array, type: PhysicalType) {
  return decodeColumnBytes(bytes, type, LiveColumnReader, ProtocolError);
}
