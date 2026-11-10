/** Count and copy JSON wire bytes in bounded work units. No number or object conversion. */
import type { Slice, SlicedJob } from "../slices.js";
import type { JsonBytesSink } from "./json-scan.js";
export class JsonOutput implements JsonBytesSink {
  position = 0;
  constructor(
    readonly bytes: Uint8Array | null,
    readonly slice: Slice,
  ) {}
  put(byte: number): void {
    if (this.bytes !== null) {
      if (this.position >= this.bytes.length) throw new RangeError("JSON output bound changed");
      this.bytes[this.position] = byte;
    }
    this.position++;
  }
  *copy(bytes: Uint8Array): SlicedJob<void> {
    if (this.bytes === null) {
      this.position += bytes.length;
      return;
    }
    for (let at = 0; at < bytes.length; at += 8192) {
      const part = bytes.subarray(at, at + 8192);
      if (this.position + part.length > this.bytes.length)
        throw new RangeError("JSON output bound changed");
      this.bytes.set(part, this.position);
      this.position += part.length;
      if (this.slice.due()) yield;
    }
  }
}
