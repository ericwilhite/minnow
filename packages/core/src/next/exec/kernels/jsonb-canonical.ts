/** Exact JSONB text without a JS object tree or a recursive traversal stack.
 * Key selection scans in (UTF8 length, unsigned byte) order with bounded scratch.
 * This intentionally trades wide-object CPU for an nesting-sized metadata proof. */
import { JsonKeyCursor } from "./json-key.js";
import { JsonScan, type JsonBytesSink } from "./json-scan.js";
import { jsonbNumber } from "./jsonb-number.js";
import type { JsonMemory } from "./json.js";
import type { Slice, SlicedJob } from "../slices.js";
const DISCARD: JsonBytesSink = { put: () => undefined };
const HEX = "0123456789abcdef";
export function* canonicalJsonb(
  bytes: Uint8Array,
  sink: JsonBytesSink,
  memory: JsonMemory,
  slice: Slice,
): SlicedJob<void> {
  const scan = new JsonScan(bytes, slice, true, (start, end) =>
    jsonbNumber(bytes, start, end, DISCARD, slice),
  );
  // Grow only the real nesting stack. No error-reordering lexical sizing pass,
  // and no full-document temporary allocation for a shallow, large string value.
  let validationBytes = Math.min(bytes.length + 1, 64),
    depth: number;
  yield* memory.take(validationBytes);
  try {
    depth = yield* scan.validate(new Uint8Array(validationBytes), function* (previous, required) {
      const next = Math.min(bytes.length + 1, Math.max(required, previous.length * 2));
      yield* memory.take(next);
      let grown: Uint8Array;
      try {
        grown = new Uint8Array(next);
        grown.set(previous);
      } catch (error) {
        memory.free(next);
        throw error;
      }
      memory.free(validationBytes);
      validationBytes = next;
      return grown;
    });
  } finally {
    memory.free(validationBytes);
  }
  const held = 24 * depth + 512;
  yield* memory.take(held);
  try {
    const a = new JsonKeyCursor(bytes),
      b = new JsonKeyCursor(bytes);
    const frames = new Int32Array(depth * 6);
    let top = -1;
    const compare = function* (
      start: number,
      end: number,
      otherStart: number,
      otherEnd: number,
    ): SlicedJob<number> {
      a.reset(start, end);
      b.reset(otherStart, otherEnd);
      let an = 0,
        bn = 0;
      while (a.next() !== null) {
        if ((++an & 1023) === 0 && slice.due()) yield;
      }
      while (b.next() !== null) {
        if ((++bn & 1023) === 0 && slice.due()) yield;
      }
      if (an !== bn) return an < bn ? -1 : 1;
      a.reset(start, end);
      b.reset(otherStart, otherEnd);
      for (let i = 0; i < an; i++) {
        const av = a.next() ?? 0,
          bv = b.next() ?? 0;
        if (av !== bv) return av < bv ? -1 : 1;
        if ((i & 1023) === 0 && slice.due()) yield;
      }
      return 0;
    };
    const string = function* (start: number, end: number): SlicedJob<void> {
      sink.put(34);
      yield* scan.stringBytes(start, end, {
        put: (byte) => {
          if (byte === 34 || byte === 92) {
            sink.put(92);
            sink.put(byte);
          } else if (byte < 32) {
            sink.put(92);
            const escape =
              byte === 8
                ? 98
                : byte === 9
                  ? 116
                  : byte === 10
                    ? 110
                    : byte === 12
                      ? 102
                      : byte === 13
                        ? 114
                        : 0;
            if (escape) sink.put(escape);
            else {
              sink.put(117);
              sink.put(48);
              sink.put(48);
              sink.put(HEX.charCodeAt(byte >> 4));
              sink.put(HEX.charCodeAt(byte & 15));
            }
          } else sink.put(byte);
        },
      });
      sink.put(34);
    };
    scan.at = 0;
    yield* scan.space();
    let value = scan.at;
    for (;;) {
      if (value >= 0) {
        scan.at = value;
        const c = bytes[value];
        if (c === 123 || c === 91) {
          sink.put(c);
          const base = ++top * 6;
          frames[base] = value;
          frames[base + 1] = value + 1;
          frames[base + 2] = -1;
          frames[base + 3] = -1;
          frames[base + 4] = 0;
          frames[base + 5] = c;
        } else if (c === 34) {
          yield* scan.string();
          yield* string(value, scan.at);
        } else {
          yield* scan.valueEnd();
          if (c === 45 || (c !== undefined && c >= 48 && c <= 57))
            yield* jsonbNumber(bytes, value, scan.at, sink, slice);
          else for (let at = value; at < scan.at; at++) sink.put(bytes[at] ?? 0);
        }
        value = -1;
      }
      if (top < 0) return;
      if (slice.tick()) yield;
      const base = top * 6,
        object = frames[base + 5] === 123;
      if (!object) {
        scan.at = frames[base + 1] ?? 0;
        yield* scan.space();
        if (bytes[scan.at] === 44) {
          scan.at++;
          yield* scan.space();
        }
        if (bytes[scan.at] === 93) {
          sink.put(93);
          top--;
          continue;
        }
        value = scan.at;
        yield* scan.valueEnd();
        frames[base + 1] = scan.at;
        if ((frames[base + 4] ?? 0) > 0) {
          sink.put(44);
          sink.put(32);
        }
        frames[base + 4] = (frames[base + 4] ?? 0) + 1;
        continue;
      }
      // Select the least key strictly after the preceding key; equal keys replace the
      // selected span, so the last duplicate wins even with distinct escape spellings.
      let best = -1,
        bestEnd = -1,
        bestValue = -1;
      scan.at = (frames[base] ?? 0) + 1;
      yield* scan.space();
      while (bytes[scan.at] !== 125) {
        const start = scan.at;
        yield* scan.string();
        const end = scan.at;
        let eligible = true;
        const previous = frames[base + 2] ?? -1;
        if (previous >= 0) {
          eligible = (yield* compare(start, end, previous, frames[base + 3] ?? previous)) > 0;
        }
        let selected = eligible;
        if (eligible && best >= 0) {
          selected = (yield* compare(start, end, best, bestEnd)) <= 0;
        }
        yield* scan.space();
        scan.at++;
        yield* scan.space();
        const input = scan.at;
        yield* scan.valueEnd();
        if (selected) {
          best = start;
          bestEnd = end;
          bestValue = input;
        }
        yield* scan.space();
        if (bytes[scan.at] !== 44) break;
        scan.at++;
        yield* scan.space();
        if (slice.tick()) yield;
      }
      if (best < 0) {
        sink.put(125);
        top--;
        continue;
      }
      if ((frames[base + 4] ?? 0) > 0) {
        sink.put(44);
        sink.put(32);
      }
      frames[base + 4] = (frames[base + 4] ?? 0) + 1;
      frames[base + 2] = best;
      frames[base + 3] = bestEnd;
      yield* string(best, bestEnd);
      sink.put(58);
      sink.put(32);
      value = bestValue;
    }
  } finally {
    memory.free(held);
  }
}
