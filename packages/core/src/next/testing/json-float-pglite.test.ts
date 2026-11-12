/** PG's float output and JSONB numeric parser are the authority, never JS's spelling. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_FLOAT_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
const bits = new DataView(new ArrayBuffer(8));
function fromBits(word: number, high: number, low: number): readonly [number, number] {
  bits.setUint32(0, word);
  const real = bits.getFloat32(0);
  bits.setUint32(0, high);
  bits.setUint32(4, low);
  return [real, bits.getFloat64(0)];
}
function realBits(value: number): number {
  bits.setFloat32(0, value);
  return bits.getUint32(0);
}
function doubleWords(value: number): readonly [number, number] {
  bits.setFloat64(0, value);
  return [bits.getUint32(0), bits.getUint32(4)];
}
interface Shape {
  family: number;
  a: number;
  b: number;
  c: number;
  offset: number;
  negative: boolean;
}
function values(s: Shape): readonly [number, number] {
  const sign = s.negative ? -1 : 1;
  switch (s.family) {
    case 1:
      return [Math.fround(sign * (2 ** 24 + s.offset)), sign * (2 ** 53 + s.offset)];
    case 2:
      return fromBits(
        (s.a & 0x807fffff) | ((1 + (s.b % 253)) << 23),
        (s.b & 0x800fffff) | ((1 + (s.a % 2045)) << 20),
        s.c & 0xfffffffe,
      );
    case 3:
      return fromBits((s.a & 0x807fffff) | 1, s.b & 0x800fffff, s.c | 1);
    case 4: {
      const real = [1e-5, 1e-4, 1e5, 1e6, 1e7][s.a % 5] ?? 1e6;
      const double = [1e-5, 1e-4, 1e14, 1e15, 1e16][s.b % 5] ?? 1e15;
      const [high, low] = doubleWords(double);
      const word = (BigInt(high) << 32n) + BigInt(low) + BigInt(s.offset);
      const [r, d] = fromBits(
        realBits(real) + s.offset,
        Number(word >> 32n),
        Number(word & 0xffffffffn),
      );
      return [sign * r, sign * d];
    }
    default:
      return fromBits(s.a, s.b, s.c);
  }
}
const sql = `SELECT to_json($1::float4)::text rj,to_jsonb($1::float4)::text rb,
 to_json($2::float8)::text dj,to_jsonb($2::float8)::text db,
 json_build_array($1::float4,$2::float8)::text a,
 jsonb_build_array($1::float4,$2::float8)::text b,
 json_array($1::float4,$2::float8)::text s,
 json_array($1::float4,$2::float8,'{}'::jsonb)::text sb,
 jsonb_build_object($1::float4,$2::float8)::text kb,
 json_build_object($1::float4,TRUE,$2::float8,FALSE)::text k`;

const anchor = (r: number, d: number, family = 0): [Shape] => {
  const [b, c] = doubleWords(d);
  return [{ family, a: realBits(r), b, c, offset: 0, negative: false }];
};
const anchors: Array<[Shape]> = [
  anchor(35675512, -24223037550190192),
  anchor(2 ** 21 + 0.25, 2 ** 50 + 0.25),
  anchor(2 ** 21 + 0.75, 2 ** 50 + 0.75),
  anchor(0, 0),
  anchor(-0, -0),
  anchor(Infinity, Infinity),
  anchor(-Infinity, -Infinity),
  anchor(NaN, NaN),
  anchor(2 ** -149, Number.MIN_VALUE),
  anchor(-(2 ** -149), -Number.MIN_VALUE),
  anchor(Math.fround(2 ** -126 - 2 ** -149), 2 ** -1022 - Number.MIN_VALUE),
  anchor(Math.fround(2 ** -126 + 2 ** -149), 2 ** -1022 + Number.MIN_VALUE),
  anchor(Math.fround(2 ** -126), 2 ** -1022),
  anchor(Math.fround(3.4028234663852886e38), Number.MAX_VALUE),
  anchor(Math.fround(1e-4), 1e-4),
  anchor(Math.fround(1e6), 1e15),
  anchor(1, 1, 1),
  anchor(0.125, 0.0625, 2),
  anchor(1, 1, 3),
  anchor(1, 1, 4),
];
it.each(seedsFor("next-json-float-bits", [0xb6ec, 0xb6ed]))(
  "keeps float4/float8 bits exact through scalar and array JSON (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON IEEE bit patterns", JSON_FLOAT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          family: fc.integer({ min: 0, max: 4 }),
          a: fc.integer({ min: 0, max: 0xffffffff }),
          b: fc.integer({ min: 0, max: 0xffffffff }),
          c: fc.integer({ min: 0, max: 0xffffffff }),
          offset: fc.integer({ min: -64, max: 64 }),
          negative: fc.boolean(),
        }),
        async (shape) => {
          reach.beginHistory();
          const [r, d] = values(shape);
          // PGlite serializes JS -0 as 0; an explicitly typed text parameter preserves that bit.
          const authority = await pg.query(sql, [
            Object.is(r, -0) ? "-0" : r,
            Object.is(d, -0) ? "-0" : d,
          ]);
          const actual = await core.execute(sql, [r, d]);
          if (!("batches" in actual)) throw new Error("Expected query");
          expect(oracleRows(actual), JSON.stringify(shape)).toEqual(authority.rows);
          reach.reach("float4 bits");
          reach.reach("float8 bits");
          reach.reach("scalar JSON and JSONB");
          reach.reach("array constructors");
          reach.reach("scalar keys");
          reach.reach("exact PostgreSQL text");
          if (shape.family === 1) {
            reach.reach("near 2^24");
            reach.reach("near 2^53");
          }
          if (shape.family === 2) reach.reach("rounding ties and neighbours");
          if (r === 2 ** 21 + 0.25 && d === 2 ** 50 + 0.25)
            reach.reach("decimal tie rounds down to even");
          if (r === 2 ** 21 + 0.75 && d === 2 ** 50 + 0.75)
            reach.reach("decimal tie rounds up to even");
          if (shape.family === 4) reach.reach("exponent transitions");
          if (r === 35675512) reach.reach("float4 midpoint exclusion");
          if (d === -24223037550190192) reach.reach("float8 midpoint exclusion");
          if (r !== 0 && Number.isFinite(r) && Math.abs(r) < 2 ** -126)
            reach.reach("float4 subnormal");
          if (d !== 0 && Number.isFinite(d) && Math.abs(d) < 2 ** -1022)
            reach.reach("float8 subnormal");
          if (Object.is(r, -0) && Object.is(d, -0)) reach.reach("negative zero");
          if (Object.is(r, 0) && Object.is(d, 0)) reach.reach("positive zero");
          if (!Number.isFinite(r) || !Number.isFinite(d)) reach.reach("special values");
        },
      ),
      { seed, numRuns: 512, examples: anchors },
    );
    reach.assertReached();
  },
  120000,
);
