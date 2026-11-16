/** PC2: forwarding structured bytes never makes an invalid value a complete snapshot. */
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  createReach,
  STRUCTURED_PARTS_REACH,
  STRUCTURED_BOUNDARIES_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import { encodeArrayValue, arrayElementTag, type ArrayElement } from "../vector/array-value.js";
import { validateStructuredParts, type ValidationAccount } from "./structured-value.js";

const utf8 = new TextEncoder();
function account(cap = 8 << 20) {
  let used = 0,
    peak = 0,
    pauses = 0;
  const value: ValidationAccount = {
    take(bytes) {
      if (used + bytes > cap) throw new Error("test funding refused");
      used += bytes;
      peak = Math.max(peak, used);
    },
    free(bytes) {
      used -= bytes;
      expect(used).toBeGreaterThanOrEqual(0);
    },
    async pace() {
      pauses++;
    },
  };
  return {
    value,
    get used() {
      return used;
    },
    get peak() {
      return peak;
    },
    get pauses() {
      return pauses;
    },
  };
}
function cases(salt: number): Array<{ element: PhysicalType; value: ArrayElement }> {
  return [
    { element: { kind: "bool" }, value: true },
    { element: { kind: "int32", bits: 16 }, value: -123 },
    { element: { kind: "int32", bits: 32 }, value: 2147483647 },
    { element: { kind: "int64" }, value: -(1n << 63n) },
    { element: { kind: "date" }, value: -2147483648 },
    { element: { kind: "time" }, value: 86400000000n },
    { element: { kind: "timestamp", zoned: false }, value: (1n << 63n) - 1n },
    { element: { kind: "timestamp", zoned: true }, value: BigInt(salt) },
    { element: { kind: "float32" }, value: NaN },
    { element: { kind: "float64" }, value: -0 },
    { element: { kind: "interval" }, value: { months: 1, days: -30, micros: -(1n << 63n) } },
    { element: { kind: "uuid" }, value: new Uint8Array(16).fill(salt) },
    {
      element: { kind: "decimal", precision: null, scale: null },
      value: { coefficient: 123450n, scale: 3 },
    },
    {
      element: { kind: "decimal", precision: null, scale: null },
      value: { coefficient: 0n, scale: 0, special: "NaN" },
    },
    { element: { kind: "text" }, value: utf8.encode("label😀" + String(salt)) },
    { element: { kind: "bytes" }, value: Uint8Array.of(0, 255, salt) },
    { element: { kind: "enum", name: "e", labels: ["label😀"] }, value: utf8.encode("label😀") },
  ];
}
async function validate(type: PhysicalType, bytes: Uint8Array, width: number, cap?: number) {
  const funded = account(cap),
    output: number[] = [];
  const lifecycle = { closed: false, started: false };
  const parts = async function* () {
    lifecycle.started = true;
    expect(funded.used).toBeGreaterThanOrEqual(131072);
    try {
      yield new Uint8Array(0);
      for (let at = 0; at < bytes.length; at += width) {
        const part = bytes.slice(at, at + width);
        yield part;
        // The producer is allowed to overwrite a borrowed part after the next stream step.
        part.fill(0xee);
      }
    } finally {
      lifecycle.closed = true;
    }
  };
  let error: unknown;
  try {
    await validateStructuredParts(type, parts, bytes.length, funded.value, (part) => {
      output.push(...part);
    });
  } catch (failure) {
    error = failure;
  }
  expect(funded.used).toBe(0);
  // An empty malformed value can be refused before the iterator is ever entered.
  if (lifecycle.started) expect(lifecycle.closed).toBe(true);
  return { error, output: Uint8Array.from(output), peak: funded.peak, pauses: funded.pauses };
}

for (const seed of seedsFor("next-structured-parts", [0xb2b1, 0xb2b2]))
  it(`validates borrowed fragments with bounded charged workspace seed ${String(seed)}`, async () => {
    const reach = createReach("structured forwarding", STRUCTURED_PARTS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 31 }),
        fc.integer({ min: 1, max: 255 }),
        async (width, salt) => {
          reach.beginHistory();
          for (const { element, value } of cases(salt)) {
            const type: PhysicalType = { kind: "array", element };
            for (const nulls of [false, true]) {
              const bytes = encodeArrayValue(element, {
                dimensions: [9],
                lowerBounds: [-3],
                elements: Array.from({ length: 9 }, (_, i) =>
                  nulls && i % 3 === 0 ? null : value,
                ),
              });
              const result = await validate(type, bytes, width);
              expect(result.error).toBeUndefined();
              expect(result.output).toEqual(bytes);
              expect(result.peak).toBeLessThanOrEqual((4 << 20) + 131072 + 512);
              reach.reach(nulls ? "array NULL bitmap" : "all array scalar kinds");
            }
          }
          for (const binary of [false, true]) {
            const bytes = utf8.encode('{"a":[1.5e2,true,null,"😀","\\uD83D\\uDE00"]}');
            const result = await validate({ kind: "json", binary }, bytes, width);
            expect(result.error).toBeUndefined();
            expect(result.output).toEqual(bytes);
            reach.reach(binary ? "JSONB" : "JSON");
            const bad = await validate({ kind: "json", binary }, utf8.encode('{"a":[1,]}'), width);
            expect(bad.error).toMatchObject({ code: "MINNOW_VALUE_CORRUPT" });
          }
          const element = { kind: "int32", bits: 32 } as const,
            type = { kind: "array", element } as const,
            original = encodeArrayValue(element, {
              dimensions: [1],
              lowerBounds: [1],
              elements: [salt],
            });
          for (const [offset, value] of [
            [0, 2],
            [1, 255],
            [2, 7],
            [3, 128],
            [4, 0],
          ] as const) {
            const bytes = original.slice();
            bytes[offset] = value;
            const result = await validate(type, bytes, width);
            expect(result.error).toMatchObject({ code: "MINNOW_VALUE_CORRUPT" });
          }
          const truncated = await validate(type, original.subarray(0, original.length - 1), width);
          expect(truncated.error).toMatchObject({ code: "MINNOW_VALUE_CORRUPT" });
          reach.reach("internal corruption");
          const refused = await validate(type, original, width, 1);
          expect((refused.error as Error).message).toBe("test funding refused");
          expect(refused.peak).toBe(0);
          reach.reach("funding before producer");
          const cancelled = account(),
            reason = new Error("validation cancelled");
          let calls = 0,
            closed = false;
          cancelled.value.pace = async () => {
            if (++calls === 2 + (salt % 5)) throw reason;
          };
          const cancellingParts = async function* () {
            try {
              yield original;
            } finally {
              closed = true;
            }
          };
          await expect(
            validateStructuredParts(
              type,
              cancellingParts,
              original.length,
              cancelled.value,
              () => undefined,
            ),
          ).rejects.toBe(reason);
          expect(closed).toBe(true);
          expect(cancelled.used).toBe(0);
          reach.reach("cancellation releases producer");
          reach.reach("borrowed parts copied before advance");
          reach.reach("charges released");
        },
      ),
      { seed, numRuns: 8 },
    );
    // A large bitmap is streamed and counted, never retained or reread. No element graph.
    const bytes = new Uint8Array(12 + 125000),
      view = new DataView(bytes.buffer);
    bytes.set([1, 3, 1, 1]);
    view.setInt32(4, 1000000, true);
    view.setInt32(8, 1, true);
    bytes.fill(255, 12);
    const result = await validate(
      { kind: "array", element: { kind: "int32", bits: 32 } },
      bytes,
      8192,
    );
    expect(result.error).toBeUndefined();
    expect(result.output).toEqual(bytes);
    expect(result.peak).toBe(131072);
    reach.reach("large bounded bitmap");
    reach.assertReached();
  }, 120_000);

/** The authorities are array-value.ts's reversible SQL scalar contract and JSONB's
 * PostgreSQL NUMERIC/Unicode contract (exec/semantic.ts). Every history exercises
 * both sides of each boundary. Malformed bytes bypass the trusted array encoder. */
function rawArray(element: PhysicalType, payload: Uint8Array, nulls: boolean): Uint8Array {
  const bytes = new Uint8Array(12 + (nulls ? 1 : 0) + payload.length),
    view = new DataView(bytes.buffer);
  bytes.set([1, arrayElementTag(element), 1, nulls ? 1 : 0]);
  view.setInt32(4, nulls ? 2 : 1, true);
  view.setInt32(8, -1, true);
  if (nulls) bytes[12] = 1;
  bytes.set(payload, nulls ? 13 : 12);
  return bytes;
}
function scalar(width: number, value: number | bigint): Uint8Array {
  const bytes = new Uint8Array(width),
    view = new DataView(bytes.buffer);
  if (width === 1) bytes[0] = Number(value);
  else if (width === 4) view.setInt32(0, Number(value), true);
  else view.setBigInt64(0, BigInt(value), true);
  return bytes;
}
function varlen(value: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(4 + value.length);
  new DataView(bytes.buffer).setUint32(0, value.length, true);
  bytes.set(value, 4);
  return bytes;
}
function numericPayload(scale: number, length: number, sign: 0 | 255): Uint8Array {
  const bytes = new Uint8Array(7 + length),
    view = new DataView(bytes.buffer);
  view.setUint16(1, scale, true);
  view.setUint32(3, length, true);
  bytes.fill(sign, 7);
  // +1 or -1, with an arbitrarily long redundant signed extension.
  bytes[7] = sign === 0 ? 1 : 255;
  return bytes;
}
for (const seed of seedsFor("next-structured-boundaries", [0xb2b9, 0xb2ba]))
  it(`checks each structured boundary and refusal seed ${String(seed)}`, async () => {
    const reach = createReach("structured rule boundaries", STRUCTURED_BOUNDARIES_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 31 }),
        fc.integer({ min: 1, max: 255 }),
        fc.boolean(),
        async (width, salt, nulls) => {
          reach.beginHistory();
          const check = async (type: PhysicalType, bytes: Uint8Array, message?: string) => {
            // Fragment every scalar/UTF-8 unit; large NUMERIC values still use bounded chunks.
            const result = await validate(type, bytes, bytes.length > 8192 ? 2048 + salt : width);
            if (message === undefined) {
              expect(result.error).toBeUndefined();
              expect(result.output).toEqual(bytes);
            } else expect(result.error).toMatchObject({ code: "MINNOW_VALUE_CORRUPT", message });
            expect(result.peak).toBeLessThanOrEqual((4 << 20) + 131072 + 512);
          };
          const json = async (source: string, message?: string, binary = true) =>
            check({ kind: "json", binary }, utf8.encode(source), message);
          // JSON's textual grammar permits escape code units that JSONB cannot store.
          for (const source of ['"\\uD800\\uDC00"', '"\\uDBFF\\uDFFF"', '"😀"']) await json(source);
          for (const [source, message] of [
            ['"\\uD800"', "Unpaired JSONB surrogate"],
            ['"\\uDBFFx"', "Unpaired JSONB surrogate"],
            ['"\\uD800\\uD800"', "Unpaired JSONB surrogate"],
            ['"\\uD800\\uE000"', "Unpaired JSONB surrogate"],
            ['"\\uD800\\n\\uDC00"', "Unpaired JSONB surrogate"],
            ['"\\uDC00"', "Invalid JSONB Unicode unit"],
            ['"\\uDFFF"', "Invalid JSONB Unicode unit"],
            ['"\\u0000"', "Invalid JSONB Unicode unit"],
          ] as const) {
            await json(source, message);
            await json(source, undefined, false);
          }
          const escape = (unit: number) => "\\u" + unit.toString(16).padStart(4, "0"),
            high = escape(0xd800 + salt),
            low = escape(0xdc00 + salt);
          await json('"' + high + low + '"');
          for (const unit of [0xd800 + salt, 0xe000 + salt]) {
            const source = '"' + high + escape(unit) + '"';
            await json(source, "Unpaired JSONB surrogate");
            await json(source, undefined, false);
          }
          await json('"' + low + '"', "Invalid JSONB Unicode unit");
          reach.reach("JSONB surrogate and NUL boundaries");
          for (const source of ["1e131071", "-9.9e131071", "0e131072"]) await json(source);
          for (const source of ["1e131072", "-9.9e131072"]) {
            await json(source, "JSONB numeric value out of range");
            await json(source, undefined, false);
          }
          const integerExponent = 131072 - String(salt).length;
          await json(String(salt) + "e" + String(integerExponent));
          await json(
            String(-salt) + "e" + String(integerExponent + salt),
            "JSONB numeric value out of range",
          );
          reach.reach("JSONB integer precision boundary");
          await json("0e1073741823");
          for (const source of [
            "0e" + String(1073741823 + salt),
            "0e-" + String(1073741823 + salt),
            "0e" + "9".repeat(salt + 10),
          ]) {
            await json(source, "JSONB numeric exponent out of range");
            await json(source, undefined, false);
          }
          reach.reach("JSONB exponent boundary");
          for (const source of ["1e-16383", "0.1e-16382", "0e-16383"]) await json(source);
          for (const source of ["1e-16384", "0.1e-16383", "0e-16384"]) {
            await json(source, "JSONB numeric value out of range");
            await json(source, undefined, false);
          }
          const fraction = "0." + "0".repeat(salt) + "1e-";
          await json(fraction + String(16382 - salt));
          await json(fraction + String(16382), "JSONB numeric value out of range");
          reach.reach("JSONB scale boundary");
          // These also keep the syntax recognizer distinct from JSONB's storage limits.
          for (const source of [
            "null",
            "true",
            "false",
            "-0",
            "0.5e+2",
            "[]",
            "{}",
            JSON.stringify({ a: [null, true, false, -0.0125, { b: '\b\f\n\r\t/\\"' }] }),
          ])
            await json(source);
          for (const [source, message] of [
            ["", "Incomplete portable JSON"],
            ["[1,]", "Unexpected JSON array end"],
            ['{"a":1,}', "Unexpected JSON object end"],
            ["truX", "Invalid JSON literal"],
            ['"\\q"', "Invalid JSON escape"],
            ['"\\uQ000"', "Invalid JSON unicode escape"],
            ['"\u0001"', "Unescaped JSON control character"],
            ["01", "JSON leading zero"],
            ["1.", "Truncated JSON token"],
            ["1e+", "Truncated JSON token"],
            ["[", "Incomplete portable JSON"],
            ['{"a" 1}', "Unexpected JSON value"],
            ["true false", "Unexpected JSON value"],
          ] as const)
            for (const binary of [false, true]) await json(source, message, binary);
          reach.reach("JSON syntax boundaries");
          for (const value of [0, 1])
            await check(
              { kind: "array", element: { kind: "bool" } },
              rawArray({ kind: "bool" }, scalar(1, value), nulls),
            );
          await check(
            { kind: "array", element: { kind: "bool" } },
            rawArray({ kind: "bool" }, scalar(1, 2 + (salt % 254)), nulls),
            "Invalid array boolean",
          );
          reach.reach("array boolean boundaries");
          const time = { kind: "time" } as const,
            small = { kind: "int32", bits: 16 } as const;
          for (const value of [0n, 86400000000n])
            await check({ kind: "array", element: time }, rawArray(time, scalar(8, value), nulls));
          for (const value of [-BigInt(salt), 86400000000n + BigInt(salt)])
            await check(
              { kind: "array", element: time },
              rawArray(time, scalar(8, value), nulls),
              "Array TIME out of range",
            );
          reach.reach("array TIME lower and upper bounds");
          for (const value of [-32768, 32767])
            await check(
              { kind: "array", element: small },
              rawArray(small, scalar(4, value), nulls),
            );
          for (const value of [-32768 - salt, 32767 + salt])
            await check(
              { kind: "array", element: small },
              rawArray(small, scalar(4, value), nulls),
              "Array SMALLINT out of range",
            );
          reach.reach("array SMALLINT lower and upper bounds");
          const text = { kind: "text", maxLength: 2 } as const,
            enumeration = { kind: "enum", name: "e", labels: ["😀x", "\ufeffx"] } as const;
          for (const value of ["😀x", "\ufeffx"]) {
            const bytes = utf8.encode(value);
            await check({ kind: "array", element: text }, rawArray(text, varlen(bytes), nulls));
            await check(
              { kind: "array", element: enumeration },
              rawArray(enumeration, varlen(bytes), nulls),
            );
          }
          await check(
            { kind: "array", element: text },
            rawArray(text, varlen(utf8.encode("😀xy")), nulls),
            "Portable text exceeds declared length",
          );
          for (const value of ["😀", "😀xy", ""])
            await check(
              { kind: "array", element: enumeration },
              rawArray(enumeration, varlen(utf8.encode(value)), nulls),
              "Unknown portable enum label",
            );
          reach.reach("text code points and exact enum labels");
          for (const malformed of [
            [0xc0, 0x80],
            [0xed, 0xa0, 0x80],
            [0xf4, 0x90, 0x80, 0x80],
            [0x80],
            [0xe2, 0x28, 0xa1],
            [0xe2, 0x82],
          ]) {
            const bytes = Uint8Array.from(malformed),
              document = Uint8Array.from([34, ...bytes, 34]);
            for (const binary of [false, true])
              await check({ kind: "json", binary }, document, "Invalid JSON UTF-8");
            await check(
              { kind: "array", element: text },
              rawArray(text, varlen(bytes), nulls),
              "Invalid structured UTF-8",
            );
          }
          reach.reach("fatal UTF-8 in JSON and array text");
          const decimal = { kind: "decimal", precision: null, scale: null } as const;
          for (const special of [1, 2, 3])
            await check(
              { kind: "array", element: decimal },
              rawArray(decimal, Uint8Array.of(special), nulls),
            );
          await check(
            { kind: "array", element: decimal },
            rawArray(decimal, Uint8Array.of(4 + (salt % 252)), nulls),
            "Invalid array NUMERIC special",
          );
          for (const scale of [0, 16383])
            await check(
              { kind: "array", element: decimal },
              rawArray(decimal, numericPayload(scale, 1, 0), nulls),
            );
          await check(
            { kind: "array", element: decimal },
            rawArray(decimal, numericPayload(16384 + salt, 1, 0), nulls),
            "Invalid array NUMERIC scale",
          );
          reach.reach("array NUMERIC scale and special boundaries");
          // 131072 integer digits fits, the next digit does not, for either sign and scale.
          const scale = salt % 3,
            power = 10n ** BigInt(131072 + scale);
          for (const sign of [-1n, 1n]) {
            const valid = encodeArrayValue(decimal, {
              dimensions: [1],
              lowerBounds: [-1],
              elements: [{ coefficient: sign * (power - BigInt(salt)), scale }],
            });
            await check({ kind: "array", element: decimal }, valid);
            // Encode at the next legal scale then corrupt it: trusted writer checks stay active.
            const invalid = encodeArrayValue(decimal, {
              dimensions: [1],
              lowerBounds: [-1],
              elements: [{ coefficient: sign * (power + BigInt(salt)), scale: scale + 1 }],
            });
            new DataView(invalid.buffer).setUint16(13, scale, true);
            await check(
              { kind: "array", element: decimal },
              invalid,
              "Array NUMERIC exceeds supported precision",
            );
          }
          reach.reach("array NUMERIC signed precision boundary");
          for (const sign of [0, 255] as const) {
            const payload = numericPayload(0, 65536 + salt, sign);
            await check({ kind: "array", element: decimal }, rawArray(decimal, payload, nulls));
            payload[payload.length - 1] = sign === 0 ? 1 : 254;
            await check(
              { kind: "array", element: decimal },
              rawArray(decimal, payload, nulls),
              "Oversized array NUMERIC coefficient",
            );
          }
          reach.reach("array NUMERIC redundant sign extension");
          reach.reach("borrowed fragments and zero remaining charge");
        },
      ),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  }, 120_000);
