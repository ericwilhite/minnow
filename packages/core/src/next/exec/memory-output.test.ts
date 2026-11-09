/** Caller-owned native memory outputs charge before growth and survive refused growth. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { MemoryOutputNeed } from "../access/snapshot.js";
import { layoutOf, type PhysicalType } from "../types.js";
import { MemoryOutputs } from "./memory-output.js";

const TYPES: readonly PhysicalType[] = [
  { kind: "bool" },
  { kind: "int32", bits: 32 },
  { kind: "float32" },
  { kind: "float64" },
  { kind: "int64" },
  { kind: "decimal", precision: null, scale: null },
  { kind: "interval" },
  { kind: "uuid" },
  { kind: "text" },
];

function need(type: PhysicalType, rows: number, payload: number): MemoryOutputNeed {
  const layout = layoutOf(type),
    variable = layout === "varlen";
  const width =
    layout === "bool"
      ? 1
      : layout === "i32" || layout === "f32"
        ? 4
        : layout === "f64" || layout === "int64"
          ? 8
          : layout === "decimal"
            ? 19
            : layout === "fixed16" || layout === "interval"
              ? 16
              : 4;
  return {
    columnId: 7,
    row: 0,
    rows,
    varlenBytes: variable ? payload : 0,
    bytes: rows * (width + 4) + Math.ceil(rows / 32) * 4 + (variable ? payload + 4 : 0),
    shape: {
      layout,
      validity: true,
      integer:
        layout === "decimal"
          ? "int128"
          : layout === "int64" || layout === "interval"
            ? "int64"
            : null,
      scales: layout === "decimal",
      scale: null,
      specials: layout === "decimal",
    },
  };
}

function arrayBytes(value: unknown): number {
  if (ArrayBuffer.isView(value)) return value.byteLength;
  if (value === null || typeof value !== "object") return 0;
  return Object.values(value).reduce<number>((sum, next: unknown) => sum + arrayBytes(next), 0);
}

it.each(seedsFor("next-native-memory-outputs", [0xc431, 0xc432]))(
  "precharges, reuses and closes every native output tier (seed %i)",
  (seed) => {
    const reach = createReach("native memory outputs", [
      ...TYPES.map((type) => layoutOf(type)),
      "reused",
      "refused-before-growth",
      "overlap",
      "released",
    ]);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: TYPES.length - 1 }),
        fc.integer({ min: 1, max: 256 }),
        fc.integer({ min: 0, max: 4096 }),
        (kind, rows, payload) => {
          reach.beginHistory();
          const type = TYPES[kind];
          if (type === undefined) throw new Error("fixture type");
          let used = 0,
            allowed = true,
            underflow = false,
            checked = false;
          const budget = {
            allocate: (bytes: number) => {
              if (!allowed) return false;
              used += bytes;
              return true;
            },
            free: (bytes: number) => {
              underflow ||= bytes > used;
              used -= bytes;
            },
          };
          const outputs = new MemoryOutputs([type], budget),
            first = need(type, rows, payload);
          try {
            const a = outputs.prepare([first]);
            expect(arrayBytes(a)).toBe(first.bytes);
            expect(
              used,
              "outputs must be charged before the caller can see them",
            ).toBeGreaterThanOrEqual(first.bytes);
            const data = a[0]?.flat.data;
            expect(data?.layout).toBe(layoutOf(type));
            if (data?.layout === "int64") expect(data.values.repr).toBe("int64");
            if (data?.layout === "interval") expect(data.micros.repr).toBe("int64");
            if (data?.layout === "decimal") {
              expect(data.coefficients.repr).toBe("int128");
              expect(data.scales?.length).toBe(rows);
              expect(data.specials?.length).toBe(rows);
            }
            reach.reach(layoutOf(type));
            const charged = used;
            expect(outputs.prepare([need(type, 1, Math.min(payload, 1))])).toBe(a);
            expect(used).toBe(charged);
            reach.reach("reused");
            const large = need(type, rows * 2, payload * 2 + 1);
            allowed = false;
            expect(() => outputs.prepare([large])).toThrow("Admitted memory output did not fit");
            expect(used).toBe(charged);
            expect(outputs.prepare([first])).toBe(a);
            reach.reach("refused-before-growth");
            allowed = true;
            const b = outputs.prepare([large]);
            expect(b).not.toBe(a);
            expect(arrayBytes(b)).toBe(large.bytes);
            expect(used).toBeGreaterThanOrEqual(charged + large.bytes);
            reach.reach("overlap");
            const withOverlap = used;
            expect(outputs.prepare([large])).toBe(b);
            expect(used).toBe(withOverlap - charged);
            checked = true;
          } finally {
            outputs.close();
            outputs.close();
            if (checked) {
              expect(used).toBe(0);
              expect(underflow).toBe(false);
              reach.reach("released");
            }
          }
        },
      ),
      { seed, numRuns: 32, examples: TYPES.map((_, i) => [i, 33, 17]) },
    );
    reach.assertReached();
  },
);
