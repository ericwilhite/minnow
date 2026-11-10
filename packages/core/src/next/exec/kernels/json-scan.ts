/** Streaming JSON validation/navigation: preserve number spellings, duplicate fields and whitespace. */
import { ExecutionError } from "../errors.js";
import type { SlicedJob, Slice } from "../slices.js";

function bad(): never {
  throw new ExecutionError("invalid input syntax for type json", "MINNOW_JSON_INPUT", "22P02");
}
function unicode(): never {
  throw new ExecutionError("unsupported Unicode escape sequence", "MINNOW_JSON_UNICODE", "22P05");
}
const white = (c: number): boolean => c === 32 || c === 9 || c === 10 || c === 13;
const digit = (c: number): boolean => c >= 48 && c <= 57;
const hex = (c: number): number =>
  c >= 48 && c <= 57 ? c - 48 : c >= 65 && c <= 70 ? c - 55 : c >= 97 && c <= 102 ? c - 87 : -1;
export interface JsonBytesSink {
  put(byte: number): void;
}
export interface JsonSpan {
  readonly start: number;
  readonly end: number;
}

/** Cursor checks the worker task clock after bounded byte work, never starts a private task. */
export class JsonScan {
  at = 0;
  #work = 0;
  constructor(
    readonly bytes: Uint8Array,
    readonly slice: Slice,
    readonly unicodeStrings = false,
  ) {}
  due(): boolean {
    return (++this.#work & 1023) === 0 && this.slice.due();
  }
  *space(): SlicedJob<void> {
    while (white(this.bytes[this.at] ?? -1)) {
      this.at++;
      if (this.due()) yield;
    }
  }
  *string(): SlicedJob<void> {
    const start = this.at;
    if (this.bytes[this.at++] !== 34) bad();
    while (this.at < this.bytes.length) {
      const c = this.bytes[this.at++] ?? -1;
      if (c === 34) {
        if (this.unicodeStrings) yield* this.stringBytes(start, this.at, DISCARD);
        return;
      }
      if (c < 32) bad();
      if (c === 92) {
        const e = this.bytes[this.at++] ?? -1;
        if (e === 117) {
          for (let k = 0; k < 4; k++) if (hex(this.bytes[this.at++] ?? -1) < 0) bad();
        } else if (![34, 92, 47, 98, 102, 110, 114, 116].includes(e)) bad();
      } else if (c >= 128) {
        let n: number, cp: number, minimum: number;
        if (c >= 194 && c <= 223) {
          n = 1;
          cp = c & 31;
          minimum = 128;
        } else if (c >= 224 && c <= 239) {
          n = 2;
          cp = c & 15;
          minimum = 2048;
        } else if (c >= 240 && c <= 244) {
          n = 3;
          cp = c & 7;
          minimum = 65536;
        } else bad();
        for (let k = 0; k < n; k++) {
          const next = this.bytes[this.at++] ?? -1;
          if ((next & 192) !== 128) bad();
          cp = cp * 64 + (next & 63);
        }
        if (cp < minimum || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) bad();
      }
      if (this.due()) yield;
    }
    bad();
  }
  *number(): SlicedJob<void> {
    if (this.bytes[this.at] === 45) this.at++;
    const first = this.bytes[this.at++] ?? -1;
    if (!digit(first)) bad();
    if (first !== 48)
      while (digit(this.bytes[this.at] ?? -1)) {
        this.at++;
        if (this.due()) yield;
      }
    if (this.bytes[this.at] === 46) {
      this.at++;
      if (!digit(this.bytes[this.at] ?? -1)) bad();
      while (digit(this.bytes[this.at] ?? -1)) {
        this.at++;
        if (this.due()) yield;
      }
    }
    const exponent = this.bytes[this.at];
    if (exponent === 101 || exponent === 69) {
      this.at++;
      if (this.bytes[this.at] === 43 || this.bytes[this.at] === 45) this.at++;
      if (!digit(this.bytes[this.at] ?? -1)) bad();
      while (digit(this.bytes[this.at] ?? -1)) {
        this.at++;
        if (this.due()) yield;
      }
    }
  }
  /** Find the maximum nesting first; the caller charges exactly this many stack bytes. */
  *depth(): SlicedJob<number> {
    let depth = 0,
      peak = 0;
    this.at = 0;
    while (this.at < this.bytes.length) {
      const c = this.bytes[this.at];
      if (c === 34) yield* this.string();
      else {
        this.at++;
        if (c === 91 || c === 123) peak = Math.max(peak, ++depth);
        else if (c === 93 || c === 125) depth--;
      }
      if (this.due()) yield;
    }
    this.at = 0;
    return peak + 1;
  }
  *validate(stack: Uint8Array): SlicedJob<void> {
    this.at = 0;
    let top = 0;
    stack[0] = 0; // root expects its one value
    for (;;) {
      if (this.due()) yield;
      yield* this.space();
      const state = stack[top],
        c = this.bytes[this.at] ?? -1;
      if (state === 9) {
        if (this.at !== this.bytes.length) bad();
        return;
      }
      if (state === 1 || state === 7) {
        // object key, with / without an allowed closing brace
        if (c === 125 && state === 1) {
          this.at++;
          top--;
          continue;
        }
        yield* this.string();
        stack[top] = 2;
        continue;
      }
      if (state === 2) {
        if (c !== 58) bad();
        this.at++;
        stack[top] = 3;
        continue;
      }
      if (state === 4 || state === 6) {
        if (c === (state === 4 ? 125 : 93)) {
          this.at++;
          top--;
          continue;
        }
        if (c !== 44) bad();
        this.at++;
        stack[top] = state === 4 ? 7 : 8;
        continue;
      }
      if (state === 5 && c === 93) {
        this.at++;
        top--;
        continue;
      }
      if (state !== 0 && state !== 3 && state !== 5 && state !== 8) bad();
      stack[top] = state === 0 ? 9 : state === 3 ? 4 : 6;
      if (c === 123 || c === 91) {
        this.at++;
        if (++top >= stack.length) bad();
        stack[top] = c === 123 ? 1 : 5;
      } else if (c === 34) yield* this.string();
      else if (c === 45 || digit(c)) yield* this.number();
      else {
        const word = c === 116 ? "true" : c === 102 ? "false" : c === 110 ? "null" : null;
        if (word === null) bad();
        for (let k = 0; k < word.length; k++)
          if (this.bytes[this.at++] !== word.charCodeAt(k)) bad();
      }
      if (this.due()) yield;
    }
  }
  /** Skip one already-validated value, retaining its original byte range. */
  *valueEnd(): SlicedJob<void> {
    const c = this.bytes[this.at];
    if (c === 34) {
      yield* this.string();
      return;
    }
    if (c === 123 || c === 91) {
      let depth = 0;
      do {
        const b = this.bytes[this.at];
        if (b === 34) yield* this.string();
        else {
          this.at++;
          if (b === 123 || b === 91) depth++;
          else if (b === 125 || b === 93) depth--;
        }
        if (this.due()) yield;
      } while (depth > 0);
    } else
      while (
        this.at < this.bytes.length &&
        !white(this.bytes[this.at] ?? -1) &&
        ![44, 93, 125].includes(this.bytes[this.at] ?? -1)
      ) {
        this.at++;
        if (this.due()) yield;
      }
  }
  *stringBytes(start: number, end: number, sink: JsonBytesSink): SlicedJob<void> {
    for (let p = start + 1; p < end - 1;) {
      let c = this.bytes[p++] ?? 0;
      if (c !== 92) sink.put(c);
      else {
        c = this.bytes[p++] ?? 0;
        if (c !== 117) {
          const v =
            c === 98 ? 8 : c === 102 ? 12 : c === 110 ? 10 : c === 114 ? 13 : c === 116 ? 9 : c;
          sink.put(v);
        } else {
          let cp = 0;
          for (let k = 0; k < 4; k++) cp = cp * 16 + hex(this.bytes[p++] ?? 0);
          if (cp === 0) unicode();
          if (cp >= 0xd800 && cp <= 0xdbff) {
            if (this.bytes[p++] !== 92 || this.bytes[p++] !== 117) bad();
            let low = 0;
            for (let k = 0; k < 4; k++) low = low * 16 + hex(this.bytes[p++] ?? 0);
            if (low < 0xdc00 || low > 0xdfff) bad();
            cp = 0x10000 + (cp - 0xd800) * 1024 + low - 0xdc00;
          } else if (cp >= 0xdc00 && cp <= 0xdfff) bad();
          if (cp < 128) sink.put(cp);
          else if (cp < 2048) {
            sink.put(192 | (cp >>> 6));
            sink.put(128 | (cp & 63));
          } else if (cp < 65536) {
            sink.put(224 | (cp >>> 12));
            sink.put(128 | ((cp >>> 6) & 63));
            sink.put(128 | (cp & 63));
          } else {
            sink.put(240 | (cp >>> 18));
            sink.put(128 | ((cp >>> 12) & 63));
            sink.put(128 | ((cp >>> 6) & 63));
            sink.put(128 | (cp & 63));
          }
        }
      }
      if (this.due()) yield;
    }
  }
  *field(key: Uint8Array | number): SlicedJob<JsonSpan | null> {
    this.at = 0;
    yield* this.space();
    const object = this.bytes[this.at] === 123;
    if (!object && this.bytes[this.at] !== 91) return null;
    if (object !== (typeof key !== "number")) return null;
    const open = this.at++;
    let wanted = typeof key === "number" ? key : -1;
    if (typeof key === "number" && key < 0) {
      let count = 0;
      yield* this.space();
      while (this.bytes[this.at] !== 93) {
        if (this.due()) yield;
        yield* this.valueEnd();
        count++;
        yield* this.space();
        if (this.bytes[this.at] !== 44) break;
        this.at++;
        yield* this.space();
      }
      wanted = count + key;
      this.at = open + 1;
    }
    let result: JsonSpan | null = null,
      index = 0;
    yield* this.space();
    while (this.bytes[this.at] !== (object ? 125 : 93)) {
      if (this.due()) yield;
      let match: boolean;
      if (object && typeof key !== "number") {
        const start = this.at;
        yield* this.string();
        const compared = { at: 0, same: true };
        yield* this.stringBytes(start, this.at, {
          put: (byte) => {
            if (key[compared.at++] !== byte) compared.same = false;
          },
        });
        match = compared.same && compared.at === key.length;
        yield* this.space();
        this.at++;
        yield* this.space();
      } else match = index === wanted;
      const start = this.at;
      yield* this.valueEnd();
      if (match) result = { start, end: this.at };
      index++;
      yield* this.space();
      if (this.bytes[this.at] !== 44) break;
      this.at++;
      yield* this.space();
    }
    return result;
  }
}

const DISCARD: JsonBytesSink = { put: () => undefined };
