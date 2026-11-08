/** Byte equality (types.ts) and DISTINCT's first-new-non-NULL-row contract. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import type { PhysicalType } from "../../types.js";
import type { FlatVector, Vector } from "../../vector/vector.js";
import { xxHash32 } from "../../hash/xxhash32.js";
import { TEXT, I32, texts, dictionary, constant } from "../kernels/test-support.js";
import { DistinctSet } from "./distinct.js";
import { ScalarByteDistinct, supportsScalarByteDistinct } from "./scalar-byte-distinct.js";

const COLLISION = ["collision-10520", "collision-192355"] as const;
const TYPES: PhysicalType[] = [
  TEXT,
  { kind: "bytes" },
  { kind: "uuid" },
  { kind: "enum", name: "e", labels: ["a", "b"] },
];
function budget(failAt = 0) {
  let calls = 0;
  return {
    used: 0,
    peak: 0,
    failed: false,
    allocate(n: number) {
      if (++calls === failAt) {
        this.failed = true;
        return false;
      }
      this.used += n;
      this.peak = Math.max(this.peak, this.used);
      return true;
    },
    free(n: number) {
      this.used -= n;
      expect(this.used).toBeGreaterThanOrEqual(0);
    },
  };
}
/** Includes nonzero backing-buffer and varlen offsets, with borrowed bytes overwritten later. */
function flat(type: PhysicalType, values: ReadonlyArray<string | null>): FlatVector {
  const source = texts(values, type);
  if (source.data.layout !== "varlen") throw new Error("builder");
  const raw = source.data;
  if (type.kind === "uuid") {
    const backing = new Uint8Array(7 + values.length * 16 + 11),
      bytes = backing.subarray(7, backing.length - 11);
    values.forEach((value, i) => {
      if (value !== null) {
        const num = Number(value.slice(1));
        new DataView(bytes.buffer, bytes.byteOffset + i * 16, 16).setUint32(0, num, true);
        bytes[i * 16 + 15] = 129;
      }
    });
    return { ...source, data: { layout: "fixed16", bytes } };
  }
  const backing = new Uint8Array(raw.bytes.length + 17),
    bytes = backing.subarray(7, backing.length - 3);
  bytes.set(raw.bytes, 3);
  return {
    ...source,
    data: { layout: "varlen", bytes, offsets: Int32Array.from(raw.offsets, (n) => n + 3) },
  };
}
function wipe(vector: Vector) {
  const flat =
    vector.form === "dictionary"
      ? vector.dictionary
      : vector.form === "constant"
        ? vector.value
        : vector;
  if (flat.data.layout !== "varlen" && flat.data.layout !== "fixed16") throw new Error("bytes");
  flat.data.bytes.fill(255);
}

it.each(seedsFor("next-scalar-byte-distinct", [0xe351, 0xe352]))(
  "matches independent byte membership across forms, collisions and reuse (seed %i)",
  (seed) => {
    const reach = createReach("scalar byte distinct", [
      "text",
      "bytes",
      "uuid",
      "enum",
      "flat",
      "dictionary",
      "constant",
      "selected",
      "empty",
      "null",
      "collision",
      "growth",
      "owned",
      "released",
    ]);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 3 }),
        fc.constantFrom("flat", "dictionary", "constant"),
        fc.boolean(),
        fc.constantFrom(127, 2048),
        fc.integer({ min: 0, max: 701 }),
        (kind, form, selected, chunk, salt) => {
          reach.beginHistory();
          const type = TYPES[kind];
          if (!type) throw new Error("type");
          const own = budget(),
            control = budget(),
            set = new ScalarByteDistinct(type, own),
            legacy = new DistinctSet(type, control, 2048),
            seen = new Set<string>();
          const out = { rows: new Uint16Array(2048), count: 0 },
            old = { rows: new Uint16Array(2048), count: 0 },
            gids = new Int32Array(2048);
          const domain = Array.from({ length: 701 }, (_, i) =>
            type.kind === "uuid"
              ? `u${String(i)}`
              : i === 0
                ? COLLISION[0]
                : i === 1
                  ? COLLISION[1]
                  : i === 2
                    ? ""
                    : `${String(i)}-α-O'Brien\u0000${"z".repeat(i % 71)}`,
          );
          const apply = (vector: Vector, raw: ReadonlyArray<string | null>, empty = false) => {
            const rows = empty
              ? { rows: new Uint16Array(0), count: 0 }
              : selected
                ? {
                    rows: Uint16Array.from(raw.map((_, i) => i).filter((i) => i % 7 !== 6)),
                    count: 0,
                  }
                : null;
            if (rows) rows.count = rows.rows.length;
            const expected: number[] = [];
            for (const row of rows === null ? raw.map((_, i) => i) : rows.rows) {
              const value = raw[row];
              if (value === null) {
                reach.reach("null");
                continue;
              }
              if (value === undefined) throw new Error("row");
              if (!seen.has(value)) {
                seen.add(value);
                expected.push(row);
              }
            }
            set.filter(vector, gids, raw.length, rows, true, out);
            legacy.filter(vector, gids, raw.length, rows, true, old);
            expect(Array.from(out.rows.subarray(0, out.count))).toEqual(expected);
            expect(Array.from(old.rows.subarray(0, old.count))).toEqual(expected);
            expect(set.count).toBe(seen.size);
            if (empty) reach.reach("empty");
          };
          try {
            // NULL and empty input must leave the set untouched even for a constant.
            apply(constant(flat(type, [null]), 5), Array<string | null>(5).fill(null));
            apply(flat(type, [domain[0] ?? ""]), [domain[0] ?? ""], true);
            // Three passes, changing dictionaries each pass and reusing their identities within it.
            for (let pass = 0; pass < 3; pass++) {
              const entries = flat(type, pass === 1 ? [...domain].reverse() : domain),
                base = dictionary(entries, []);
              if (base.form !== "dictionary") throw new Error("dictionary");
              for (let start = 0; start < 1402; start += chunk) {
                const n = Math.min(chunk, 1402 - start),
                  codes = Array.from({ length: n }, (_, j) => (start + j + salt) % 701),
                  raw = Array.from({ length: n }, (_, j) =>
                    (start + j) % 29 === 28
                      ? null
                      : ((pass === 1 ? [...domain].reverse() : domain)[codes[j] ?? 0] ?? ""),
                  );
                let vector: Vector;
                if (form === "dictionary") {
                  const built = dictionary(
                    entries,
                    raw.map((v, j) => (v === null ? null : (codes[j] ?? 0))),
                  );
                  vector = { ...built, dictionaryId: base.dictionaryId } as Vector;
                } else if (form === "constant") {
                  const v = raw[0] ?? null;
                  raw.fill(v);
                  vector = constant(flat(type, [v]), n);
                } else vector = flat(type, raw);
                apply(vector, raw);
                if (form !== "dictionary") wipe(vector);
              }
              if (form === "dictionary") wipe(entries);
            }
            // Explicit actual XXH32 collision (not a bucket collision), in generated histories.
            if (type.kind !== "uuid") {
              apply(flat(type, COLLISION), COLLISION);
              expect(seen.has(COLLISION[0]) && seen.has(COLLISION[1])).toBe(true);
              reach.reach("collision");
            }
            // Re-read after all borrowed inputs were overwritten, proving ownership of stored bytes.
            const values = domain.slice(0, Math.min(701, seen.size));
            apply(flat(type, values), values);
            reach.reach("owned");
            if (seen.size > 512) reach.reach("growth");
            reach.reach(type.kind as "text" | "bytes" | "uuid" | "enum");
            reach.reach(form);
            if (selected) reach.reach("selected");
          } finally {
            set.close();
            set.close();
            legacy.close();
            expect(own.used).toBe(0);
            expect(control.used).toBe(0);
            reach.reach("released");
          }
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [0, "flat", true, 2048, 0],
          [1, "dictionary", false, 127, 3],
          [2, "flat", false, 2048, 0],
          [3, "constant", true, 127, 7],
        ],
      },
    );
    reach.assertReached();
  },
);

it("uses full byte equality even when the complete hashes collide", () => {
  const e = new TextEncoder();
  expect(xxHash32(e.encode(COLLISION[0]))).toBe(xxHash32(e.encode(COLLISION[1])));
  expect(supportsScalarByteDistinct(I32)).toBe(false);
  const charge = budget();
  expect(() => new ScalarByteDistinct(I32, charge)).toThrow(TypeError);
  expect(charge.used).toBe(0);
});

it.each(seedsFor("next-scalar-byte-distinct-refusal", [0xe353, 0xe354]))(
  "releases precharges after constructor, growth and temporary-view denials (seed %i)",
  (seed) => {
    const reach = createReach("scalar byte distinct refusal", [
      "constructor",
      "growth",
      "complete",
      "released",
    ]);
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 100 }), (failAt) => {
        reach.beginHistory();
        const charge = budget(failAt);
        let set: ScalarByteDistinct | null = null;
        try {
          set = new ScalarByteDistinct(TEXT, charge);
          const vector = flat(
              TEXT,
              Array.from({ length: 2048 }, (_, i) => `${String(i)}-${"a".repeat(i % 257)}`),
            ),
            out = { rows: new Uint16Array(2048), count: 0 };
          set.filter(vector, new Int32Array(2048), 2048, null, true, out);
          expect(out.count).toBe(2048);
          reach.reach("complete");
        } catch (error) {
          expect(charge.failed).toBe(true);
          expect(error).toBeInstanceOf(RangeError);
          reach.reach(set === null ? "constructor" : "growth");
        } finally {
          set?.close();
          expect(charge.used).toBe(0);
          reach.reach("released");
        }
      }),
      { seed, numRuns: 32, examples: [[0], [1], [5], [10], [20], [100]] },
    );
    reach.assertReached();
  },
);

it("keeps payload and old/new overlap charged before allocation", () => {
  const charge = budget(),
    set = new ScalarByteDistinct(TEXT, charge),
    out = { rows: new Uint16Array(2048), count: 0 };
  try {
    expect(charge.used).toBeGreaterThanOrEqual(1024 * 5 + 4096);
    for (let b = 0; b < 4; b++)
      set.filter(
        flat(
          TEXT,
          Array.from({ length: 2048 }, (_, i) => `${String(b * 2048 + i)}-${"x".repeat(64)}`),
        ),
        new Int32Array(2048),
        2048,
        null,
        true,
        out,
      );
    expect(set.count).toBe(8192);
    expect(charge.used).toBeGreaterThanOrEqual(8192 * (4 + 65) + 16384 * 8);
    expect(charge.peak - charge.used).toBeGreaterThanOrEqual(128);
  } finally {
    set.close();
    expect(charge.used).toBe(0);
  }
  expect(() => set.filter(flat(TEXT, ["x"]), new Int32Array(1), 1, null, true, out)).toThrow(
    "closed",
  );
});
