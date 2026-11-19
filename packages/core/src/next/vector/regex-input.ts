import type { RegexInput } from "../../pattern/input.js";

/** Validated text vectors own these bytes; a caller retains the source borrow. */
export class Utf8RegexInput implements RegexInput {
  readonly length: number;
  readonly limit = Infinity;
  constructor(readonly bytes: Uint8Array) {
    this.length = bytes.length;
  }
  codePointAt(at: number): number | undefined {
    const c = this.bytes[at];
    if (c === undefined) return undefined;
    if (c < 128) return c;
    if (c < 224) return ((c & 31) << 6) | ((this.bytes[at + 1] ?? 0) & 63);
    if (c < 240)
      return (
        ((c & 15) << 12) |
        (((this.bytes[at + 1] ?? 0) & 63) << 6) |
        ((this.bytes[at + 2] ?? 0) & 63)
      );
    return (
      ((c & 7) << 18) |
      (((this.bytes[at + 1] ?? 0) & 63) << 12) |
      (((this.bytes[at + 2] ?? 0) & 63) << 6) |
      ((this.bytes[at + 3] ?? 0) & 63)
    );
  }
  advance(at: number, point: number): number {
    return at + (point < 128 ? 1 : point < 2048 ? 2 : point < 65536 ? 3 : 4);
  }
  previous(at: number): number {
    let i = at - 1;
    while (i > 0 && ((this.bytes[i] ?? 0) & 192) === 128) i--;
    return i;
  }
  newline(at: number): boolean {
    return this.bytes[at] === 10;
  }
}
