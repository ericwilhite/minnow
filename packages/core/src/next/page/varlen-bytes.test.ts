/**
 * `varlenBytes` / `varlenBytesOf` (decode.ts): the bytes a varlen target needs, exact for every
 * page kind, and zero for pages with no varlen values (increment 6: a fixed-width page used to
 * walk its rows to compute a meaningless sum; dictionary pages now sum entry lengths in one loop
 * per code width).
 */
import { describe, expect, it } from "vitest";
import { PageEncoding } from "../format/manifest.js";
import type { PhysicalType } from "../types.js";
import { UNLIMITED } from "./memory-extents.js";
import { ChunkReader } from "./reader.js";
import { defined, targetFor, valueAt, writeGroup, type TestValue } from "./test-support.js";

const encoder = new TextEncoder();

describe("varlenBytes", () => {
  it("is exact over dictionary pages of every code width, plain pages and nulls", async () => {
    for (const distinct of [3, 300, 3000]) {
      const values: TestValue[] = Array.from({ length: 9000 }, (_, row) =>
        row % 11 === 0
          ? null
          : encoder.encode(`v${String((row * 7919) % distinct)}${"x".repeat(row % 5)}`),
      );
      const { space, group } = await writeGroup([{ type: { kind: "text" }, values }]);
      const reader = await ChunkReader.open(space, defined(group.columns[0]), {
        budget: UNLIMITED,
      });
      await reader.load(reader.directory.pages.map((_, page) => page));
      for (const [from, to] of [
        [0, 9000],
        [17, 4321],
        [5000, 5001],
      ] as const) {
        const target = targetFor(reader, to - from, reader.varlenBytes(from, to));
        reader.decodeRange(from, to, target, 0);
        let decoded = 0;
        for (let k = 0; k < to - from; k++) {
          const value = valueAt(target, k);
          if (value instanceof Uint8Array) decoded += value.length;
        }
        expect(
          reader.varlenBytes(from, to),
          `${String(distinct)} distinct, [${String(from)}, ${String(to)})`,
        ).toBe(decoded);
      }
      reader.close();
    }
  });

  it("is exact over width-zero dictionary pages with nulls (every valid row one code)", async () => {
    const values: TestValue[] = [
      ...Array.from({ length: 5000 }, (_, row) => encoder.encode(`v${String(row % 3)}`)),
      ...Array.from({ length: 9000 }, (_, row) => (row % 4 === 0 ? null : encoder.encode("v1"))),
    ];
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values }]);
    const chunk = defined(group.columns[0]);
    const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    await reader.load(reader.directory.pages.map((_, page) => page));
    // The layout under test exists: a width-zero dictionary page with nulls.
    const widthZero = reader.directory.pages.filter((entry) => {
      const at = chunk.data.extent.offset + chunk.data.offset + entry.offset;
      return (
        (entry.encoding & 0x7f) === PageEncoding.dictionary &&
        space.bytes[at + 3] === 0 &&
        entry.nullCount > 0
      );
    });
    expect(widthZero.length).toBeGreaterThan(0);
    for (const entry of widthZero) {
      const from = entry.firstRow;
      const to = entry.firstRow + entry.rowCount;
      expect(reader.varlenBytes(from, to)).toBe((entry.rowCount - entry.nullCount) * 2);
      expect(reader.varlenBytes(from + 1, to - 1)).toBe(
        values.slice(from + 1, to - 1).filter((value) => value !== null).length * 2,
      );
    }
    reader.close();
  });

  it("is zero for pages with no varlen values", async () => {
    const cases: Array<[PhysicalType, TestValue[]]> = [
      [
        { kind: "int32", bits: 32 },
        Array.from({ length: 5000 }, (_, row) => (row % 7 === 0 ? null : row % 100)),
      ],
      [{ kind: "bool" }, Array.from({ length: 5000 }, (_, row) => row % 3 === 0)],
      [
        { kind: "decimal", precision: 12, scale: 2 },
        Array.from({ length: 5000 }, (_, row) => ({ coefficient: BigInt(row % 977), special: 0 })),
      ],
    ];
    for (const [type, values] of cases) {
      const { space, group } = await writeGroup([
        { type, values, ...(type.kind === "decimal" ? { vector: { scale: 2 } } : {}) },
      ]);
      const reader = await ChunkReader.open(space, defined(group.columns[0]), {
        budget: UNLIMITED,
      });
      await reader.load(reader.directory.pages.map((_, page) => page));
      expect(reader.varlenBytes(0, values.length)).toBe(0);
      reader.close();
    }
  });
});
