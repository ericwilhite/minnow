/** Representation-native positions walk the same Unicode scalars without decoding/copying input. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, REGEX_INPUT_REACH } from "../../testing/reach.js";
import { StringRegexInput } from "../../pattern/input.js";
import { Utf8RegexInput } from "../vector/regex-input.js";

const history = fc.record({
  atom: fc.oneof(
    fc.constantFrom("a", "Ж", "中", "😀", "\ufeff", "\n", "e\u0301"),
    fc.integer({ min: 0, max: 0x10ffff }).map((n) => String.fromCodePoint(n)),
  ),
  before: fc.oneof(fc.constantFrom(0, 8191, 8192, 16383), fc.integer({ min: 0, max: 17000 })),
  marker: fc.constantFrom("", "😀", "\ud800Z", "\udc00", "\n"),
  padding: fc.integer({ min: 0, max: 31 }),
});
const examples = [
  { atom: "a", before: 0, marker: "", padding: 0 },
  { atom: "a", before: 8191, marker: "😀", padding: 3 },
  { atom: "Ж", before: 31, marker: "\ud800Z", padding: 1 },
  { atom: "中", before: 8192, marker: "\udc00", padding: 7 },
  { atom: "😀", before: 8191, marker: "\n", padding: 2 },
  { atom: "\ufeff", before: 17, marker: "", padding: 5 },
  { atom: "e\u0301", before: 31, marker: "\n", padding: 4 },
];
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
for (const seed of seedsFor("regex-native-input", [49527, 49528]))
  it(`walks borrowed UTF8 and legacy UTF16 scalar boundaries (${String(seed)})`, () => {
    const reach = createReach("regex input", REGEX_INPUT_REACH);
    fc.assert(
      fc.property(history, (data) => {
        reach.beginHistory();
        const raw = data.atom.repeat(Math.floor(data.before / data.atom.length)) + data.marker;
        const bytes = encoder.encode(raw),
          normal = decoder.decode(bytes);
        const padded = new Uint8Array(bytes.length + 2 * data.padding).fill(0xa5);
        padded.set(bytes, data.padding);
        const borrowed = padded.subarray(data.padding, data.padding + bytes.length);
        const text = new StringRegexInput(normal, 1024),
          native = new Utf8RegexInput(borrowed);
        expect(native.bytes).toBe(borrowed);
        expect(native.bytes.buffer).toBe(padded.buffer);
        expect(native.limit).toBe(Infinity);
        expect(text.limit).toBe(1024);
        let utf8 = 0,
          utf16 = 0,
          seenWidths = 0,
          newline = false;
        const offsets: Array<[number, number]> = [[0, 0]],
          widths = new Map<string, number>();
        for (const character of normal) {
          const point = character.codePointAt(0);
          if (point === undefined) throw new Error("Empty scalar");
          if (
            native.codePointAt(utf8) !== point ||
            text.codePointAt(utf16) !== point ||
            native.newline(utf8) !== (character === "\n") ||
            text.newline(utf16) !== (character === "\n")
          )
            throw new Error("Regex input Unicode scalar mismatch");
          const old8 = utf8,
            old16 = utf16;
          utf8 = native.advance(utf8, point);
          utf16 = text.advance(utf16, point);
          let width = widths.get(character);
          if (width === undefined) {
            width = encoder.encode(character).length;
            widths.set(character, width);
          }
          if (
            utf8 - old8 !== width ||
            utf16 - old16 !== character.length ||
            native.previous(utf8) !== old8 ||
            text.previous(utf16) !== old16
          )
            throw new Error("Regex input forward/reverse boundary mismatch");
          offsets.push([utf8, utf16]);
          seenWidths |= 1 << (width - 1);
          newline ||= character === "\n";
        }
        expect(utf8).toBe(native.length);
        expect(utf16).toBe(text.length);
        expect(native.codePointAt(utf8)).toBeUndefined();
        expect(text.codePointAt(utf16)).toBeUndefined();
        for (let i = offsets.length - 1; i > 0; i--) {
          const current = offsets[i],
            prior = offsets[i - 1];
          if (!current || !prior) throw new Error("Missing scalar boundary");
          if (native.previous(current[0]) !== prior[0] || text.previous(current[1]) !== prior[1])
            throw new Error("Regex input reverse traversal mismatch");
        }
        for (const [i, state] of (
          ["one byte", "two bytes", "three bytes", "four bytes"] as const
        ).entries())
          if (seenWidths & (1 << i)) reach.reach(state);
        if (newline) reach.reach("newline");
        if (!bytes.length) reach.reach("empty");
        if (data.padding > 0) reach.reach("offset view");
        if (raw !== normal) reach.reach("lone surrogate replacement");
        if (raw.includes("\ufeff")) reach.reach("BOM is data");
        if (raw.includes("\u0301")) reach.reach("combining scalar");
        if (normal.length > 8192) reach.reach("long input");
        reach.reach("forward and backward");
        reach.reach("borrowed buffer");
      }),
      { seed, numRuns: 64, examples: examples.map((data) => [data]), endOnFailure: true },
    );
    reach.assertReached();
  });
