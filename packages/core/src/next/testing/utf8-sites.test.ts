/** Every UTF8 boundary preserves WHATWG bytes under exact capacity and short native progress. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, UTF8_SITES_REACH } from "../../testing/reach.js";
import { encodeUtf8Into, Utf8CapacityError } from "../utf8.js";
import { VarlenBuilder, decodeUtf8, varlenEqual } from "../vector/varlen.js";
import { ByteWriter, ByteReader, encodeJson, decodeJson } from "../write/bytes.js";
import { ByteWindow } from "../write/window.js";
import { SMALL_COMMIT_BYTES } from "../write/commit-buffers.js";
import { Governor } from "../memory/governor.js";
import { prepareParameters } from "../exec/parameters.js";
import type { PhysicalPlan } from "../ir/ir.js";

const history = fc.record({
  atom: fc.oneof(
    fc.constantFrom("a", "Ж", "中", "😀", "\ufeff"),
    fc.integer({ min: 1, max: 0x7f }).map((n) => String.fromCodePoint(n)),
    fc.integer({ min: 0x80, max: 0x7ff }).map((n) => String.fromCodePoint(n)),
    fc.integer({ min: 0x800, max: 0xffff }).map((n) => String.fromCodePoint(n)),
    fc.integer({ min: 0x10000, max: 0x10ffff }).map((n) => String.fromCodePoint(n)),
  ),
  before: fc.oneof(
    fc.constantFrom(0, 31, 258, 512, 8191, 8192, 16383),
    fc.integer({ min: 0, max: 17000 }),
  ),
  after: fc.integer({ min: 0, max: 257 }),
  marker: fc.constantFrom("none", "pair", "high", "low", "mixed"),
  extra: fc.oneof(fc.constant(0), fc.integer({ min: 1, max: 512 })),
  mode: fc.constantFrom("native", "prefix", "zero"),
  scratch: fc.constantFrom(4, 5, 8, 16, 17, 64, 128, 1024, 32768),
});
type History = typeof history extends fc.Arbitrary<infer T> ? T : never;
const fixed = (
  atom: History["atom"],
  before: number,
  marker: History["marker"],
  extra: number,
  mode: History["mode"],
): History => ({ atom, before, marker, extra, mode, after: 17, scratch: 32768 });
const examples: Array<[History]> = [
  [{ ...fixed("Ж", 512, "none", 0, "native"), after: 0, scratch: 64 }],
  [fixed("a", 31, "pair", 257, "prefix")],
  [fixed("中", 8191, "pair", 0, "zero")],
  [fixed("😀", 8192, "high", 31, "prefix")],
  [fixed("Ж", 8191, "low", 0, "zero")],
  [fixed("\ufeff", 258, "mixed", 23, "prefix")],
  [{ ...fixed("a", 0, "none", 0, "native"), after: 0 }],
];
const sites = ["helper", "varlen", "writer", "json", "window", "parameters"] as const;
type Site = (typeof sites)[number];
const encoder = new TextEncoder(),
  decoder = new TextDecoder("utf8", { fatal: true, ignoreBOM: true });

function source(h: History): string {
  const prefix =
    h.atom.repeat(Math.floor(h.before / h.atom.length)) + "a".repeat(h.before % h.atom.length);
  const marker =
    h.marker === "pair"
      ? "😀"
      : h.marker === "high"
        ? "\ud800Z"
        : h.marker === "low"
          ? "\udc00Z"
          : h.marker === "mixed"
            ? "😀\ud800Z\udc00"
            : "";
  return prefix + marker + h.atom.repeat(h.after);
}
function sameBytes(actual: Uint8Array, expected: Uint8Array): void {
  expect(actual.length, "exact encoded extent").toBe(expected.length);
  expect(
    actual.every((b, i) => b === expected[i]),
    "byte-exact UTF8",
  ).toBe(true);
}
class Budget {
  live = 0;
  peak = 0;
  calls = 0;
  constructor(readonly cap: number) {}
  allocate(bytes: number): boolean {
    if (this.live + bytes > this.cap) return false;
    this.calls++;
    this.live += bytes;
    this.peak = Math.max(this.peak, this.live);
    return true;
  }
  free(bytes: number): void {
    this.live -= bytes;
    expect(this.live).toBeGreaterThanOrEqual(0);
  }
  create(bytes: number): Uint8Array {
    if (!this.allocate(bytes)) throw new Error("test recipe exceeded admitted bytes");
    return new Uint8Array(bytes);
  }
  release(bytes: Uint8Array): void {
    this.free(bytes.length);
  }
  reserve(bytes: number): () => void {
    if (!this.allocate(bytes)) throw new Error("test scratch exceeded admission");
    let open = true;
    return () => {
      if (open) {
        open = false;
        this.free(bytes);
      }
    };
  }
}
async function exercise(site: Site, h: History, text: string): Promise<void> {
  const expected = encoder.encode(text),
    normal = decoder.decode(expected);
  if (site === "parameters") {
    const governor = new Governor(4 << 20);
    let grant: Awaited<ReturnType<Governor["admit"]>> | undefined;
    const plan: PhysicalPlan = {
      root: { id: 1, op: "values", output: [], rows: [] },
      columns: [],
      parameters: [{ kind: "text" }],
      placeholderCount: 1,
      clockSlot: null,
      initPlans: [],
      catalogVersion: 0,
      volatile: false,
    };
    try {
      const binding = await prepareParameters(plan, [text]);
      grant = await governor.admit({
        label: "UTF8 site",
        minimumBytes: binding.bytes + h.extra,
        spillable: false,
      });
      const bound = await binding.bind(grant),
        vector = bound.slots[0];
      if (vector?.data.layout !== "varlen") throw new Error("missing text binding");
      sameBytes(vector.data.bytes, expected);
      expect(decodeUtf8(vector.data, 0), "parameter reader").toBe(normal);
    } finally {
      grant?.close();
      expect(governor.usedBytes).toBe(0);
    }
    return;
  }
  const reference = new ByteWriter();
  reference.string(text);
  const stringBytes = reference.finish();
  reference.close();
  const jsonBytes = encoder.encode(JSON.stringify({ text }));
  const initial =
    site === "varlen"
      ? Math.max(16, expected.length + h.extra)
      : site === "writer"
        ? Math.max(16, stringBytes.length + h.extra)
        : site === "json"
          ? jsonBytes.length
          : site === "window"
            ? stringBytes.length + h.extra
            : expected.length + h.extra;
  const cap =
    initial +
    (site === "varlen"
      ? 8
      : site === "json"
        ? 24 + 2 * (SMALL_COMMIT_BYTES + 16) + h.extra
        : site === "window"
          ? h.scratch + 31
          : 0);
  const budget = new Budget(cap);
  try {
    if (site === "varlen") {
      const builder = new VarlenBuilder(1, budget, initial, initial);
      try {
        expect(builder.appendString(text)).toBe("ok");
        sameBytes(builder.data().bytes, expected);
        expect(decodeUtf8(builder.data(), 0), "varlen reader").toBe(normal);
        expect(
          varlenEqual(
            builder.data(),
            0,
            { layout: "varlen", bytes: expected, offsets: Int32Array.of(0, expected.length) },
            0,
          ),
        ).toBe(true);
        expect(budget.calls).toBe(1);
      } finally {
        builder.close();
      }
    } else if (site === "writer") {
      const writer = new ByteWriter(initial, budget);
      try {
        writer.string(text);
        sameBytes(writer.bytes.subarray(0, writer.length), stringBytes);
        const reader = new ByteReader(writer.bytes.subarray(0, writer.length));
        expect(reader.string(), "ByteReader.string").toBe(normal);
        reader.end();
        expect(budget.calls).toBe(1);
      } finally {
        writer.close();
      }
    } else if (site === "json") {
      const bytes = encodeJson({ text }, budget);
      try {
        sameBytes(bytes, jsonBytes);
        expect(decodeJson(bytes), "decodeJson").toEqual({ text });
        expect(budget.calls).toBe(2); // Existing stringify reservation plus destination; no helper scratch.
      } finally {
        budget.release(bytes);
      }
    } else if (site === "window") {
      const target = budget.create(initial).fill(0xa5),
        scratch = budget.create(h.scratch),
        part = budget.create(31).fill(0xa5);
      try {
        const window = new ByteWindow(target, 0, scratch);
        window.string(text);
        expect(window.length, "window measured extent").toBe(stringBytes.length);
        sameBytes(target.subarray(0, stringBytes.length), stringBytes);
        expect(target.subarray(stringBytes.length).every((b) => b === 0xa5)).toBe(true);
        const reader = new ByteReader(target.subarray(0, stringBytes.length));
        expect(reader.string(), "window ByteReader.string").toBe(normal);
        reader.end();
        const start = Math.floor(stringBytes.length / 2);
        const partial = new ByteWindow(part, start, scratch);
        partial.string(text);
        expect(partial.length).toBe(stringBytes.length);
        sameBytes(
          part.subarray(0, Math.min(31, stringBytes.length - start)),
          stringBytes.subarray(start, start + 31),
        );
      } finally {
        budget.release(part);
        budget.release(scratch);
        budget.release(target);
      }
    } else {
      const target = budget.create(initial).fill(0xa5);
      try {
        expect(encodeUtf8Into(text, target, expected.length)).toBe(expected.length);
        sameBytes(target.subarray(0, expected.length), expected);
        expect(decoder.decode(target.subarray(0, expected.length))).toBe(normal);
        expect(target.subarray(expected.length).every((b) => b === 0xa5)).toBe(true);
      } finally {
        budget.release(target);
      }
      if (expected.length) {
        const small = budget.create(expected.length - 1).fill(0xa5);
        try {
          let error: unknown;
          try {
            encodeUtf8Into(text, small, expected.length);
          } catch (e) {
            error = e;
          }
          expect(error).toBeInstanceOf(Utf8CapacityError);
          expect(error).toMatchObject({
            code: "MINNOW_UTF8_CAPACITY",
            requiredBytes: expected.length,
            availableBytes: small.length,
          });
          expect(
            small.every((b) => b === 0xa5),
            "capacity refusal precedes mutation",
          ).toBe(true);
        } finally {
          budget.release(small);
        }
      }
    }
    expect(budget.peak).toBeLessThanOrEqual(cap);
  } finally {
    expect(budget.live, "all admitted bytes released").toBe(0);
  }
}

for (const seed of seedsFor("next-utf8-sites", [49521, 49522]))
  for (const site of sites)
    it(site + " encodes exact UTF8 with admitted buffers " + String(seed), async () => {
      const reach = createReach("UTF8 site " + site, UTF8_SITES_REACH);
      const native = new TextEncoder(),
        encode = native.encodeInto.bind(native);
      await fc.assert(
        fc.asyncProperty(history, async (h) => {
          reach.beginHistory();
          const text = source(h);
          let prefixes = 0,
            zeros = 0;
          const spy = vi
            .spyOn(TextEncoder.prototype, "encodeInto")
            .mockImplementation((value, target) => {
              if (value.length && h.mode === "zero") {
                zeros++;
                return { read: 0, written: 0 };
              }
              const scalar = value.codePointAt(0) ?? 0;
              const prefix = h.mode === "prefix" ? value.slice(0, scalar > 0xffff ? 2 : 1) : value;
              const result = encode(prefix, target);
              if (result.read < value.length) prefixes++;
              return result;
            });
          try {
            await exercise(site, h, text);
          } finally {
            spy.mockRestore();
          }
          if (!text.length) reach.reach("empty");
          if (h.atom === "a") reach.reach("one byte");
          if (h.atom === "Ж") reach.reach("two bytes");
          if (h.atom === "中" || h.atom === "\ufeff") reach.reach("three bytes");
          if (h.atom === "😀") reach.reach("four bytes");
          if (h.marker === "pair" || h.marker === "mixed" || h.atom === "😀")
            reach.reach("valid pair");
          if (h.marker === "high" || h.marker === "mixed") reach.reach("lone high");
          if (h.marker === "low" || h.marker === "mixed") reach.reach("lone low");
          if (h.atom === "\ufeff") reach.reach("BOM character");
          if (text.length > 8192) reach.reach("longer than chunk");
          if (h.before === 8191 && h.marker === "pair") reach.reach("pair across chunk");
          reach.reach(h.extra === 0 ? "exact recipe" : "generous recipe");
          if (prefixes) reach.reach("native prefix");
          if (zeros) reach.reach("native zero");
          reach.reach("zero retained");
        }),
        { seed, numRuns: 64, examples, endOnFailure: true },
      );
      reach.assertReached();
    });

it.each([0, 1, 2, 3])("refuses a window scratch smaller than one four-byte scalar (%i)", (size) => {
  const target = new Uint8Array(5),
    scratch = new Uint8Array(size);
  expect(() => new ByteWindow(target, 0, scratch).string("😀")).toThrow(Utf8CapacityError);
});
it("encodes a scalar in an exact four-byte window scratch", () => {
  const target = new Uint8Array(5);
  new ByteWindow(target, 0, new Uint8Array(4)).string("😀");
  const reader = new ByteReader(target);
  expect(reader.string()).toBe("😀");
  reader.end();
});
