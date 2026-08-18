/**
 * Properties of the block format, checked against generated inputs rather than chosen ones.
 *
 * Every other test of this layer is example-based: someone thought of a case and wrote it down.
 * That is the right way to pin a known bug and the wrong way to find an unknown one, because the
 * examples cluster where the author's attention was. The format's contract is small enough to
 * state as properties instead — a value written and read back is the same value, a zone map bounds
 * what it summarizes, a corrupted byte is detected — and a property holds for inputs nobody
 * imagined.
 *
 * fast-check shrinks a failure to a minimal reproduction and prints the seed, so a break here
 * arrives as a small concrete case rather than as a haystack.
 *
 * The generators deliberately reach for the values that break encoders: negative zero, the
 * extremes of the double range, subnormals, empty and astral-plane strings, dates at the edges of
 * the representable range, all-null columns, and empty columns.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { decodeBlock, encodeBlock } from "./block.js";
import { logicalTypes, type ColumnInput, type Compression, type LogicalType } from "./types.js";

/** How many cases each property runs. Enough to explore; small enough to stay in the fast suite. */
const RUNS = 300;

const compressions: Compression[] = ["raw", "gzip"];

/**
 * Finite doubles including the values that most often go wrong: signed zero, the largest and
 * smallest magnitudes, and a subnormal. `encodeColumn` rejects non-finite numbers, so NaN and the
 * infinities are the format's documented boundary rather than something to generate.
 */
const numberArbitrary = fc.oneof(
  { weight: 6, arbitrary: fc.double({ noNaN: true, noDefaultInfinity: true }) },
  {
    weight: 1,
    arbitrary: fc.constantFrom(
      0,
      -0,
      1,
      -1,
      Number.MAX_VALUE,
      -Number.MAX_VALUE,
      Number.MIN_VALUE,
      -Number.MIN_VALUE,
      Number.EPSILON,
      Number.MAX_SAFE_INTEGER,
      Number.MIN_SAFE_INTEGER,
      5e-324,
    ),
  },
);

/**
 * Valid Unicode text. Lone surrogates are excluded on purpose: they are not encodable as UTF-8,
 * and the format says so by decoding with `fatal: true` rather than by silently substituting.
 * The `grapheme` unit generates whole grapheme clusters, which are always well-formed.
 */
const stringArbitrary = fc.oneof(
  { weight: 6, arbitrary: fc.string({ unit: "grapheme", maxLength: 40 }) },
  // Written as escapes rather than literal characters: a raw NUL slipped into this line once
  // and made the whole file read as binary to grep.
  {
    weight: 1,
    arbitrary: fc.constantFrom(
      "",
      " ",
      "\u0000",
      "\u00a0",
      "\ufffd",
      "\u{1f469}\u200d\u{1f469}\u200d\u{1f467}\u200d\u{1f466}",
      "e\u0301",
      "\t\n\r",
    ),
  },
  { weight: 1, arbitrary: fc.string({ unit: "grapheme", minLength: 500, maxLength: 2_000 }) },
);

/** Dates across the representable range, including the epoch and both extremes. */
const dateArbitrary = fc.oneof(
  { weight: 6, arbitrary: fc.date({ noInvalidDate: true }) },
  {
    weight: 1,
    arbitrary: fc.constantFrom(
      new Date(0),
      new Date(-1),
      new Date(8.64e15),
      new Date(-8.64e15),
      new Date("2026-08-18T00:00:00.000Z"),
    ),
  },
);

function valuesFor(type: LogicalType): fc.Arbitrary<readonly unknown[]> {
  const base: fc.Arbitrary<unknown> =
    type === "boolean"
      ? fc.boolean()
      : type === "number"
        ? numberArbitrary
        : type === "string"
          ? stringArbitrary
          : dateArbitrary;
  // `null` woven in at a generated rate, so all-null and no-null columns both occur rather than
  // only the comfortable middle.
  return fc.array(fc.option(base, { nil: null, freq: 4 }), { maxLength: 64 });
}

/** A column of a generated type, with a generated compression. */
const columnArbitrary = fc.constantFrom(...logicalTypes).chain((type) =>
  fc.record({
    column: valuesFor(type).map((values) => ({ type, values }) as ColumnInput),
    compression: fc.constantFrom(...compressions),
  }),
);

/**
 * Value equality as the format must preserve it, which is stricter than `===` in one place that
 * matters: negative zero. `Object.is` separates it from positive zero, so if the format ever
 * normalized one to the other this would say so instead of passing.
 */
function sameValue(left: unknown, right: unknown): boolean {
  if (left instanceof Date && right instanceof Date) return left.getTime() === right.getTime();
  return Object.is(left, right);
}

/**
 * How a value reads in a failure message. `String(-0)` is `"0"`, so a negative-zero regression
 * would otherwise report "wrote 0, read 0" and look like a test bug rather than a real one.
 */
function show(value: unknown): string {
  if (Object.is(value, -0)) return "-0";
  if (value instanceof Date) return `Date(${value.toISOString()})`;
  if (typeof value === "string") return JSON.stringify(value);
  return String(value);
}

describe("block format properties", () => {
  it("returns exactly the values it was given", async () => {
    await fc.assert(
      fc.asyncProperty(columnArbitrary, async ({ column, compression }) => {
        const decoded = await decodeBlock(await encodeBlock(column, compression));
        expect(decoded.column.type).toBe(column.type);
        expect(decoded.column.values.length).toBe(column.values.length);
        for (let index = 0; index < column.values.length; index += 1) {
          const wrote = column.values[index];
          const read = decoded.column.values[index];
          if (!sameValue(wrote, read)) {
            throw new Error(
              `row ${String(index)} of a ${column.type} column changed: ` +
                `wrote ${show(wrote)}, read ${show(read)}`,
            );
          }
        }
      }),
      { numRuns: RUNS },
    );
  });

  it("reads the same values whichever codec stored them", async () => {
    // Compression is an encoding choice, never a semantic one. A codec that lost a value only
    // for inputs it happened to compress well would pass every fixed example above.
    await fc.assert(
      fc.asyncProperty(columnArbitrary, async ({ column }) => {
        const raw = await decodeBlock(await encodeBlock(column, "raw"));
        const gzip = await decodeBlock(await encodeBlock(column, "gzip"));
        expect(gzip.column.values.length).toBe(raw.column.values.length);
        for (let index = 0; index < raw.column.values.length; index += 1) {
          if (!sameValue(raw.column.values[index], gzip.column.values[index])) {
            throw new Error(`row ${String(index)} differs between raw and gzip`);
          }
        }
      }),
      { numRuns: RUNS },
    );
  });

  it("counts nulls and rows the way the payload does", async () => {
    await fc.assert(
      fc.asyncProperty(columnArbitrary, async ({ column, compression }) => {
        const decoded = await decodeBlock(await encodeBlock(column, compression));
        const nulls = column.values.filter((value) => value === null).length;
        // The description is what pruning and planning read without touching the payload, so it
        // has to agree with the payload rather than merely be plausible.
        expect(decoded.description.rowCount).toBe(column.values.length);
        expect(decoded.description.nullCount).toBe(nulls);
        expect(decoded.description.compression).toBe(compression);
      }),
      { numRuns: RUNS },
    );
  });

  it("summarizes with a zone map that contains every value it covers", async () => {
    // Zone maps are what let a scan skip a block unread. A map that excludes a value the block
    // actually holds does not make a query slow, it makes it wrong.
    await fc.assert(
      fc.asyncProperty(columnArbitrary, async ({ column, compression }) => {
        const decoded = await decodeBlock(await encodeBlock(column, compression));
        const zone = decoded.description.metadata.zoneMap;
        if (zone === undefined) return;
        const numeric = column.values
          .filter((value) => value !== null)
          .map((value) => (value instanceof Date ? value.getTime() : value))
          .filter((value): value is number => typeof value === "number");
        for (const value of numeric) {
          expect(value).toBeGreaterThanOrEqual(zone.min);
          expect(value).toBeLessThanOrEqual(zone.max);
        }
      }),
      { numRuns: RUNS },
    );
  });

  it("detects a single flipped byte rather than returning wrong values", async () => {
    // The checksum's whole purpose. A corrupted block must fail loudly: silently decoding it into
    // plausible-looking rows is the failure mode that reaches a user as wrong data.
    await fc.assert(
      fc.asyncProperty(
        columnArbitrary.filter(({ column }) => column.values.length > 0),
        fc.nat(),
        fc.integer({ min: 1, max: 255 }),
        async ({ column, compression }, offsetSeed, flip) => {
          const encoded = await encodeBlock(column, compression);
          const corrupted = encoded.slice();
          const offset = offsetSeed % corrupted.byteLength;
          corrupted[offset] = ((corrupted[offset] ?? 0) ^ flip) & 0xff;
          if (corrupted[offset] === encoded[offset]) return; // no change, nothing to detect

          let detected = false;
          let values: unknown[] | undefined;
          try {
            values = (await decodeBlock(corrupted)).column.values;
          } catch {
            detected = true;
          }
          if (detected) return;
          // Not every byte changes the meaning -- padding and unused bits exist. What must never
          // happen is a decode that succeeds *and* reports different values.
          expect(values?.length).toBe(column.values.length);
          for (let index = 0; index < column.values.length; index += 1) {
            if (!sameValue(column.values[index], values?.[index])) {
              throw new Error(
                `corrupting byte ${String(offset)} changed row ${String(index)} without being ` +
                  `detected: wrote ${show(column.values[index])}, read ${show(values?.[index])}`,
              );
            }
          }
        },
      ),
      { numRuns: RUNS },
    );
  });

  it("refuses values it does not claim to store", async () => {
    // The stated boundary, held as a property rather than as three examples: non-finite numbers
    // are rejected outright rather than persisted as something else.
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY),
        async (value) => {
          await expect(encodeBlock({ type: "number", values: [value] })).rejects.toThrow();
        },
      ),
      { numRuns: 3 },
    );
  });
});
