import { describe, expect, it } from "vitest";
import { decodeBlock, encodeBlock, inspectBlock } from "./index.js";
import type { ColumnInput, Compression } from "./index.js";

const columns: ColumnInput[] = [
  { type: "boolean", values: [true, false, null, true] },
  { type: "number", values: [1, -2.5, null, 42] },
  { type: "string", values: ["hello", "", null, "🌍"] },
  {
    type: "datetime",
    values: [new Date("2026-01-02T03:04:05Z"), null, new Date("1970-01-01T00:00:00Z")],
  },
];

for (const compression of ["raw", "rle", "gzip"] satisfies Compression[]) {
  describe(compression, () => {
    for (const column of columns) {
      it(`round trips ${column.type}`, async () => {
        const encoded = await encodeBlock(column, compression);
        const decoded = await decodeBlock(encoded);
        expect(decoded.column).toEqual(column);
        expect(decoded.description.nullCount).toBe(
          column.values.filter((value) => value === null).length,
        );
      });
    }
  });
}

it("calculates a numeric zone map", async () => {
  const block = await encodeBlock({ type: "number", values: [3, null, -2, 9] });
  expect(inspectBlock(block).metadata.zoneMap).toEqual({ min: -2, max: 9 });
});

it("rejects corruption and untrusted lengths", async () => {
  const block = await encodeBlock({ type: "string", values: ["safe"] });
  block[block.length - 1] = (block[block.length - 1] ?? 0) ^ 0xff;
  await expect(decodeBlock(block)).rejects.toThrow("checksum");

  const invalid = await encodeBlock({ type: "number", values: [1] });
  new DataView(invalid.buffer).setUint32(28, 0xffffffff, true);
  expect(() => inspectBlock(invalid)).toThrow("length");
});

it("bounds gzip decompression by the validated declared length", async () => {
  const block = await encodeBlock({ type: "string", values: ["x".repeat(10_000)] }, "gzip");
  new DataView(block.buffer).setUint32(24, 1, true);
  await expect(decodeBlock(block)).rejects.toThrow("declared length");
});
