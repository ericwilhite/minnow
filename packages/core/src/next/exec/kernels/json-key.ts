/** A decoded UTF8 cursor over an ALREADY validated JSON key. Constant space, no decoded copy. */
const hex = (c: number): number => (c <= 57 ? c - 48 : c <= 70 ? c - 55 : c - 87);
export class JsonKeyCursor {
  #at = 0;
  #end = 0;
  #code = 0;
  #remaining = 0;
  constructor(readonly bytes: Uint8Array) {}
  reset(start: number, end: number): void {
    this.#at = start + 1;
    this.#end = end - 1;
    this.#remaining = 0;
  }
  next(): number | null {
    if (this.#remaining > 0) return 128 | ((this.#code >>> (6 * --this.#remaining)) & 63);
    if (this.#at >= this.#end) return null;
    let c = this.bytes[this.#at++] ?? 0;
    if (c !== 92) return c;
    c = this.bytes[this.#at++] ?? 0;
    if (c !== 117)
      return c === 98 ? 8 : c === 102 ? 12 : c === 110 ? 10 : c === 114 ? 13 : c === 116 ? 9 : c;
    let cp = 0;
    for (let k = 0; k < 4; k++) cp = cp * 16 + hex(this.bytes[this.#at++] ?? 0);
    if (cp >= 0xd800 && cp <= 0xdbff) {
      this.#at += 2;
      let low = 0;
      for (let k = 0; k < 4; k++) low = low * 16 + hex(this.bytes[this.#at++] ?? 0);
      cp = 0x10000 + (cp - 0xd800) * 1024 + low - 0xdc00;
    }
    this.#code = cp;
    if (cp < 128) return cp;
    this.#remaining = cp < 2048 ? 1 : cp < 65536 ? 2 : 3;
    return (
      (this.#remaining === 1 ? 192 : this.#remaining === 2 ? 224 : 240) |
      (cp >>> (6 * this.#remaining))
    );
  }
}
