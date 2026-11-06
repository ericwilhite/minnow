/**
 * `scaledFloat` pages (page/format.ts, format/manifest.ts `PageEncoding.scaledFloat`): float64
 * values that are exact decimals stored as frame-of-reference integers over 10^e, the rest
 * patched with their own 8 bytes. Law (the lead's ruling for codec A): every f64 decodes
 * bit-identically — −0, NaN payloads, ±Infinity, subnormals, and values that look decimal but are
 * not exactly representable included; a page that cannot be both smaller and exact stays plain.
 *
 * The generated property writes pages mixing exact decimals (random exponents and widths), near
 * misses one bit off, arithmetic results that only look decimal, random doubles, and every
 * special, with nulls, then compares every valid row's 64 bits after decoding. A materialized
 * page is a plain f64 view of the same lane (decode.ts), so predicates and gathers over it are
 * the plain page's: equal bits make them equal.
 */
import fc from "fast-check";
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { PageEncoding } from "../format/manifest.js";
import { ChunkReader } from "./reader.js";
import { defined, targetFor, UNLIMITED, writeGroup } from "./test-support.js";

const SEEDS = seedsFor("next-scaled-float", [0xf64, 0x1e3]);
const STATES = [
  "a scaled page",
  "a scaled page with patches",
  "a page left plain",
  "a patched NaN with a payload",
  "a patched -0",
  "a patched Infinity",
  "a patched subnormal",
  "a patched inexact decimal",
  "exponent 0",
  "exponent 3 or more",
  "width 0",
  "width 1",
  "width 2",
  "width 4",
  "a scaled page within 2^32 of the safe-integer bound",
] as const;
const reach = createReach("next-scaled-float", STATES);
afterAll(() => {
  reach.assertReached();
});

const bitsOf = (value: number): bigint => {
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, value, true);
  return view.getBigUint64(0, true);
};
const fromBits = (bits: bigint): number => {
  const view = new DataView(new ArrayBuffer(8));
  view.setBigUint64(0, bits, true);
  return view.getFloat64(0, true);
};
const isSubnormal = (value: number): boolean =>
  value !== 0 && Number.isFinite(value) && Math.abs(value) < 2.2250738585072014e-308;

/** The families a row is drawn from when it is not an exact decimal (or null). */
const NOISE = ["near", "looks", "random", "special", "special", "special"] as const;

describe.each(SEEDS)("scaledFloat pages round-trip every float64 bit for bit (seed %i)", (seed) => {
  it("decodes every valid row to the same 64 bits, and stays plain when it cannot be exact and smaller", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 3000 }),
        // The share of rows that are not exact decimals: none, within the patch limit, past it.
        fc.constantFrom(0, 0.01, 0.04, 0.3),
        fc.integer({ min: 0, max: 6 }),
        fc.constantFrom(1, 10, 300, 70_000, 2 ** 31, 2 ** 40),
        // Offsets around zero, and at the positive and negative safe-integer bounds (review
        // B2-p2i #1): a page's integers then sit within its width of 2^53.
        fc.oneof(
          fc.integer({ min: -1_000_000, max: 1_000_000 }),
          fc.constantFrom(
            Number.MAX_SAFE_INTEGER - 1,
            Number.MAX_SAFE_INTEGER - 255,
            Number.MAX_SAFE_INTEGER - 65_535,
            Number.MAX_SAFE_INTEGER - 2 ** 31,
            Number.MAX_SAFE_INTEGER - 2 ** 32 + 1,
            -Number.MAX_SAFE_INTEGER,
            -Number.MAX_SAFE_INTEGER + 300,
          ),
        ),
        fc.array(fc.bigInt({ min: 0n, max: (1n << 64n) - 1n }), { minLength: 8, maxLength: 8 }),
        async (rowCount, noise, exponent, spread, offset, randomBits) => {
          reach.beginHistory();
          const power = 10 ** exponent;
          let state = (seed ^ rowCount ^ (offset << 7)) >>> 0 || 1;
          const next = (): number => {
            state ^= state << 13;
            state ^= state >>> 17;
            state ^= state << 5;
            return state >>> 0;
          };
          const families = Array.from({ length: rowCount }, () => {
            const u = next() / 4_294_967_296;
            if (u < 0.03) return "null";
            return u < 0.03 + noise ? (NOISE[next() % NOISE.length] ?? "special") : "decimal";
          });
          const values = families.map((family, k): number | null => {
            const n = offset + (next() % spread);
            switch (family) {
              case "decimal":
                return n / power;
              case "near":
                return fromBits(bitsOf(n / power + 0.5) ^ 1n); // one mantissa bit off
              case "looks":
                return (n % 10_000) / 100 - 100; // f01's shape: decimal-looking, often inexact
              case "random":
                return fromBits(randomBits[k % randomBits.length] ?? 0n);
              case "special": {
                const specials = [
                  -0,
                  Number.POSITIVE_INFINITY,
                  Number.NEGATIVE_INFINITY,
                  Number.MIN_VALUE,
                  -2.2250738585072e-310,
                  // NaNs with payloads (quiet and signalling patterns), and the canonical one.
                  fromBits(0x7ff8_0000_dead_beefn),
                  fromBits(0xfff0_0000_0000_0001n),
                  Number.NaN,
                  Number.MAX_VALUE,
                  (Number.MAX_SAFE_INTEGER + 2) / power, // past the safe integers
                ];
                return specials[next() % specials.length] ?? 0;
              }
              default:
                return null;
            }
          });
          const { space, group } = await writeGroup([{ type: { kind: "float64" }, values }]);
          const chunk = defined(group.columns[0]);
          const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
          await reader.load(reader.directory.pages.map((_, page) => page));
          const rows = values.length;
          const target = targetFor(reader, rows, 0);
          reader.decodeRange(0, rows, target, 0);
          if (target.data.layout !== "f64") throw new Error("expected f64");
          const decoded = target.data.values;
          for (let row = 0; row < rows; row++) {
            const expected = values[row];
            const valid =
              target.validity === null ||
              (((target.validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 1;
            expect(valid, `row ${String(row)} validity`).toBe(
              expected !== null && expected !== undefined,
            );
            if (expected === null || expected === undefined) continue;
            expect(bitsOf(decoded[row] ?? 0), `row ${String(row)}: ${String(expected)}`).toBe(
              bitsOf(expected),
            );
          }
          // Reach, from what the writer chose (the stored header) and what it then had to patch.
          for (const entry of reader.directory.pages) {
            const kind = entry.encoding & 0x7f;
            const pageValues = values.slice(entry.firstRow, entry.firstRow + entry.rowCount);
            if (kind === PageEncoding.plain) reach.reach("a page left plain");
            if (kind !== PageEncoding.scaledFloat) continue;
            reach.reach("a scaled page");
            const lease = await space.read(
              [
                {
                  extent: chunk.data.extent,
                  offset: chunk.data.offset + entry.offset,
                  length: entry.length,
                  checksum: entry.checksum,
                },
              ],
              { budget: UNLIMITED },
            );
            const stored = defined(lease.bytes[0]);
            const width = stored[3] ?? 0;
            const e = new Uint32Array(stored.buffer, stored.byteOffset + 8, 1)[0] ?? 0;
            lease.release();
            reach.reach(`width ${String(width)}` as (typeof STATES)[number]);
            const base =
              new Float64Array(
                stored.buffer,
                stored.byteOffset +
                  16 +
                  (entry.nullCount > 0
                    ? Math.ceil((Math.ceil(entry.rowCount / 32) * 4) / 8) * 8
                    : 0),
                1,
              )[0] ?? 0;
            if (Math.abs(base) > Number.MAX_SAFE_INTEGER - 2 ** 32)
              reach.reach("a scaled page within 2^32 of the safe-integer bound");
            if (e === 0) reach.reach("exponent 0");
            if (e >= 3) reach.reach("exponent 3 or more");
            const present = pageValues.filter((each): each is number => each !== null);
            if (present.some((each) => Object.is(each, -0))) reach.reach("a patched -0");
            if (
              present.some(
                (each) => each === Number.POSITIVE_INFINITY || each === Number.NEGATIVE_INFINITY,
              )
            )
              reach.reach("a patched Infinity");
            if (present.some(isSubnormal)) reach.reach("a patched subnormal");
            if (present.some((each) => Number.isNaN(each) && bitsOf(each) !== bitsOf(Number.NaN)))
              reach.reach("a patched NaN with a payload");
            const inexact = present.some((each) => {
              if (!Number.isFinite(each) || Object.is(each, -0)) return false;
              const scaled = Math.round(each * 10 ** e);
              return !(Number.isSafeInteger(scaled) && Object.is(scaled / 10 ** e, each));
            });
            if (inexact) reach.reach("a patched inexact decimal");
            if (present.some((each) => !Number.isFinite(each) || Object.is(each, -0)) || inexact)
              reach.reach("a scaled page with patches");
          }
          reader.close();
        },
      ),
      { seed, numRuns: 150 },
    );
  }, 240_000);
});

describe("scaledFloat pages: pinned", () => {
  it("store W1M's f00 shape (three decimals, below 10^6) in 4 bytes a row, not 8", async () => {
    const values = Array.from({ length: 4096 }, (_, row) => ((row * 7919) % 1_000_000) / 1000);
    const { space, group } = await writeGroup([{ type: { kind: "float64" }, values }]);
    const chunk = defined(group.columns[0]);
    const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    expect(
      reader.directory.pages.every((page) => (page.encoding & 0x7f) === PageEncoding.scaledFloat),
    ).toBe(true);
    const stored = reader.directory.pages.reduce((sum, page) => sum + page.length, 0);
    expect(stored).toBeLessThan(values.length * 4.2);
    reader.close();
  });

  it("charges the decoded lane to the reader and gives it back", async () => {
    const values = Array.from({ length: 4096 }, (_, row) => row / 100);
    const { space, group } = await writeGroup([{ type: { kind: "float64" }, values }]);
    let held = 0;
    const budget = {
      allocate: (bytes: number) => ((held += bytes), true),
      free: (bytes: number) => void (held -= bytes),
    };
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget });
    const before = held;
    await reader.load(reader.directory.pages.map((_, page) => page));
    const stored = reader.directory.pages.reduce((sum, page) => sum + page.length, 0);
    expect(held - before).toBe(stored + values.length * 8);
    reader.close();
    expect(held).toBe(0);
  });
});
