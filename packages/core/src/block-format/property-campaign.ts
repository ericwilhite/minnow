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
import { decodeBlock, encodeBlock } from "./block.js";
import { frozenVectors, fromHex } from "./frozen-vectors.js";
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

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

async function mustReject(run: () => Promise<unknown>): Promise<void> {
  try {
    await run();
  } catch (error) {
    if (!(error instanceof Error)) throw new Error("Non-error thrown", { cause: error });
    return;
  }
  throw new Error("Invalid or corrupted input was accepted");
}

export const blockFormatProperties: Record<string, (seed: number) => Promise<void>> = {
  "reads and reproduces every locked raw format vector": async () => {
    for (const { input, hex } of frozenVectors) {
      const encoded = await encodeBlock(input, "raw");
      const actualHex = Array.from(encoded, (byte) => byte.toString(16).padStart(2, "0")).join("");
      assert(actualHex === hex, `Frozen ${input.type} writer bytes changed`);
      const decoded = (await decodeBlock(fromHex(hex))).column;
      assert(
        decoded.type === input.type && decoded.values.length === input.values.length,
        "Frozen vector decoded with a different type or row count",
      );
      for (let index = 0; index < input.values.length; index++) {
        assert(
          sameValue(decoded.values[index], input.values[index]),
          "Frozen vector value changed",
        );
      }
    }
  },
  "returns exactly the values it was given": async (seed) => {
    await fc.assert(
      fc.asyncProperty(columnArbitrary, async ({ column, compression }) => {
        const decoded = await decodeBlock(await encodeBlock(column, compression));
        assert(
          Object.is(decoded.column.type, column.type),
          "Unexpected block type, length, or metadata",
        );
        assert(
          Object.is(decoded.column.values.length, column.values.length),
          "Unexpected block type, length, or metadata",
        );
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
      { numRuns: RUNS, seed },
    );
  },

  "reads the same values whichever codec stored them": async (seed) => {
    // Compression is an encoding choice, never a semantic one. A codec that lost a value only
    // for inputs it happened to compress well would pass every fixed example above.
    await fc.assert(
      fc.asyncProperty(columnArbitrary, async ({ column }) => {
        const raw = await decodeBlock(await encodeBlock(column, "raw"));
        const gzip = await decodeBlock(await encodeBlock(column, "gzip"));
        assert(
          Object.is(gzip.column.values.length, raw.column.values.length),
          "Unexpected block type, length, or metadata",
        );
        for (let index = 0; index < raw.column.values.length; index += 1) {
          if (!sameValue(raw.column.values[index], gzip.column.values[index])) {
            throw new Error(`row ${String(index)} differs between raw and gzip`);
          }
        }
      }),
      { numRuns: RUNS, seed },
    );
  },

  "counts nulls and rows the way the payload does": async (seed) => {
    await fc.assert(
      fc.asyncProperty(columnArbitrary, async ({ column, compression }) => {
        const decoded = await decodeBlock(await encodeBlock(column, compression));
        const nulls = column.values.filter((value) => value === null).length;
        // The description is what pruning and planning read without touching the payload, so it
        // has to agree with the payload rather than merely be plausible.
        assert(
          Object.is(decoded.description.rowCount, column.values.length),
          "Unexpected block type, length, or metadata",
        );
        assert(
          Object.is(decoded.description.nullCount, nulls),
          "Unexpected block type, length, or metadata",
        );
        assert(
          Object.is(decoded.description.compression, compression),
          "Unexpected block type, length, or metadata",
        );
      }),
      { numRuns: RUNS, seed },
    );
  },

  "summarizes with a zone map that contains every value it covers": async (seed) => {
    // Zone maps are what let a scan skip a block unread. A map that excludes a value the block
    // actually holds does not make a query slow, it makes it wrong.
    await fc.assert(
      fc.asyncProperty(columnArbitrary, async ({ column, compression }) => {
        const decoded = await decodeBlock(await encodeBlock(column, compression));
        const zone = decoded.description.metadata.zoneMap;
        const numeric = column.values
          .filter((value) => value !== null)
          .map((value) => (value instanceof Date ? value.getTime() : value))
          .filter((value): value is number => typeof value === "number");
        if (numeric.length === 0) {
          assert(zone === undefined, "Non-numeric or all-null column has a numeric zone map");
          return;
        }
        assert(zone !== undefined, "Numeric payload has no zone map");
        assert(zone.min === Math.min(...numeric), "Zone map minimum differs from the payload");
        assert(zone.max === Math.max(...numeric), "Zone map maximum differs from the payload");
        for (const value of numeric) {
          assert(value >= zone.min, "Zone map minimum excludes a stored value");
          assert(value <= zone.max, "Zone map maximum excludes a stored value");
        }
      }),
      { numRuns: RUNS, seed },
    );
  },

  "detects a single flipped byte rather than returning wrong values": async (seed) => {
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
          assert(
            corrupted[offset] !== encoded[offset],
            "Corruption generator did not change a byte",
          );

          // The v2 envelope covers every fixed/header metadata byte and independently checksums
          // every stored payload byte. Even semantically unused bitmap padding is canonical and
          // covered, so no changed byte is accepted.
          await mustReject(() => decodeBlock(corrupted));
        },
      ),
      { numRuns: RUNS, seed },
    );
  },

  "refuses values it does not claim to store": async () => {
    // Enumerate boundaries so a small random sample cannot accidentally omit NaN or infinity.
    for (const value of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
      await mustReject(() => encodeBlock({ type: "number", values: [value] }));
    }
    await mustReject(() => encodeBlock({ type: "datetime", values: [new Date(Number.NaN)] }));
    for (const value of ["\ud800", "\udfff", "prefix\ud800suffix"]) {
      await mustReject(() => encodeBlock({ type: "string", values: [value] }));
    }
  },
};
