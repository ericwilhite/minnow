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
import type { FlatVector } from "../vector/vector.js";
import { MemoryExtentSpace } from "./memory-extents.js";
import { ChunkReader } from "./reader.js";
import { GroupWriter } from "./writer.js";
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

const RAW_SEEDS = seedsFor("next-float-raw-bits", [0xbad5eed, 0x77]);
const RAW_STATES = [
  "a constant page of one NaN payload",
  "a constant page of a signalling NaN",
  "a constant page of -0",
  "an all-NaN page of distinct payloads",
  "a page of random 64-bit patterns",
  "a scaled page with raw patches",
  "nulls",
] as const;
const rawReach = createReach("next-float-raw-bits", RAW_STATES);
afterAll(() => {
  rawReach.assertReached();
});

const NAN_EXPONENT = 0x7ff0_0000_0000_0000n;
const MANTISSA = 0x000f_ffff_ffff_ffffn;
const isNaNBits = (bits: bigint): boolean =>
  (bits & NAN_EXPONENT) === NAN_EXPONENT && (bits & MANTISSA) !== 0n;
const isSignalling = (bits: bigint): boolean =>
  isNaNBits(bits) && (bits & 0x0008_0000_0000_0000n) === 0n;

describe.each(RAW_SEEDS)(
  "every float64 bit pattern round-trips, from raw bits (seed %i)",
  (seed) => {
    it("decodes and gathers each valid row to its own 64 bits, constant pages included", async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 1500 }),
          fc.constantFrom("constant", "nan payloads", "random", "decimal with patches"),
          fc.bigInt({ min: 0n, max: (1n << 64n) - 1n }),
          fc.boolean(),
          async (rows, mode, pick, withNulls) => {
            rawReach.beginHistory();
            let state = BigInt(seed) ^ pick ^ BigInt(rows);
            const next = (): bigint => {
              state ^= (state << 13n) & 0xffff_ffff_ffff_ffffn;
              state ^= state >> 7n;
              state ^= (state << 17n) & 0xffff_ffff_ffff_ffffn;
              return state & 0xffff_ffff_ffff_ffffn;
            };
            const values = new Float64Array(rows);
            const bits = new BigUint64Array(values.buffer);
            const view = new DataView(new ArrayBuffer(8));
            const constants = [
              0x7ff8_0000_dead_beefn, // quiet NaN with a payload
              0x7ff0_0000_0000_0001n, // signalling NaN
              0xfff4_0000_0000_00ffn, // negative signalling NaN with a payload
              0x8000_0000_0000_0000n, // −0
              pick,
            ];
            const constant = constants[Number(pick % BigInt(constants.length))] ?? pick;
            for (let row = 0; row < rows; row++) {
              if (mode === "constant") bits[row] = constant;
              else if (mode === "nan payloads") bits[row] = 0x7ff8_0000_0000_0001n + BigInt(row);
              else if (mode === "random") bits[row] = next();
              else if (row % 37 === 0) bits[row] = next();
              else {
                view.setFloat64(0, Number(next() % 1_000_000n) / 100, true);
                bits[row] = view.getBigUint64(0, true);
              }
            }
            let validity: Uint32Array | null = null;
            if (withNulls && rows > 1) {
              validity = new Uint32Array(Math.ceil(rows / 32)).fill(0xffff_ffff);
              validity[0] = (validity[0] ?? 0) & ~2; // row 1 null
              rawReach.reach("nulls");
            }
            const space = new MemoryExtentSpace();
            const type = { kind: "float64" } as const;
            const writer = new GroupWriter({
              columns: [{ columnId: 1, type }],
              writer: space.beginWrite("raw"),
            });
            const vector: FlatVector = {
              form: "flat",
              type,
              length: rows,
              validity,
              data: { layout: "f64", values },
            };
            await writer.append([vector], rows);
            const chunk = defined((await writer.finish()).columns[0]);
            const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
            await reader.load(reader.directory.pages.map((_, page) => page));
            const target = targetFor(reader, rows, 0);
            reader.decodeRange(0, rows, target, 0);
            if (target.data.layout !== "f64") throw new Error("expected f64");
            const decoded = new BigUint64Array(
              target.data.values.buffer,
              target.data.values.byteOffset,
              rows,
            );
            const valid = (row: number): boolean =>
              validity === null || (((validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 1;
            for (let row = 0; row < rows; row++)
              if (valid(row)) expect(decoded[row], `row ${String(row)}`).toBe(bits[row]);
            const chosen = Uint32Array.from({ length: Math.ceil(rows / 3) }, (_, k) => k * 3);
            const gathered = targetFor(reader, chosen.length, 0);
            reader.gather(chosen, gathered, 0);
            if (gathered.data.layout !== "f64") throw new Error("expected f64");
            const gatheredBits = new BigUint64Array(
              gathered.data.values.buffer,
              gathered.data.values.byteOffset,
              chosen.length,
            );
            chosen.forEach((row, slot) => {
              if (valid(row))
                expect(gatheredBits[slot], `gathered row ${String(row)}`).toBe(bits[row]);
            });
            for (const entry of reader.directory.pages) {
              const kind = entry.encoding & 0x7f;
              if (mode === "constant" && kind === PageEncoding.constant) {
                if (constant === 0x8000_0000_0000_0000n) rawReach.reach("a constant page of -0");
                else if (isSignalling(constant))
                  rawReach.reach("a constant page of a signalling NaN");
                else if (isNaNBits(constant)) rawReach.reach("a constant page of one NaN payload");
              }
              if (mode === "nan payloads" && entry.rowCount - entry.nullCount > 1)
                rawReach.reach("an all-NaN page of distinct payloads");
              if (mode === "random") rawReach.reach("a page of random 64-bit patterns");
              if (mode === "decimal with patches" && kind === PageEncoding.scaledFloat)
                rawReach.reach("a scaled page with raw patches");
            }
            reader.close();
          },
        ),
        { seed, numRuns: 150 },
      );
    }, 240_000);
  },
);

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
