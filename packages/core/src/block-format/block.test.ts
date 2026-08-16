import { describe, expect, it } from "vitest";
import {
  crc32,
  decodeBlock,
  encodeBlock,
  inspectBlock,
  maximumPhysicalBlockByteLength,
} from "./index.js";
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

for (const compression of ["raw", "gzip"] satisfies Compression[]) {
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
  const view = new DataView(block.buffer);
  view.setUint32(28, 1, true);
  // Re-sign the envelope so the corrupted declared length reaches the decompression bound.
  view.setUint32(4, crc32(block.subarray(8, 40 + view.getUint32(24, true))), true);
  await expect(decodeBlock(block)).rejects.toThrow("declared length");
});

it("bounds the complete stored block including its exact metadata envelope", async () => {
  const input = { type: "number", values: [3, null, -2, 9] } as const;
  const metadata = { zoneMap: { min: -2, max: 9 } };
  const encodedByteLength = 33;
  const metadataByteLength = new TextEncoder().encode(JSON.stringify(metadata)).byteLength;

  expect(maximumPhysicalBlockByteLength(encodedByteLength, metadata, "raw")).toBe(
    40 + metadataByteLength + encodedByteLength,
  );
  expect(maximumPhysicalBlockByteLength(encodedByteLength, metadata, "gzip")).toBe(
    40 + metadataByteLength + encodedByteLength * 2 + 64,
  );

  for (const compression of ["raw", "gzip"] satisfies Compression[]) {
    const block = await encodeBlock(input, compression);
    expect(block.byteLength).toBeLessThanOrEqual(
      maximumPhysicalBlockByteLength(
        inspectBlock(block).encodedLength,
        inspectBlock(block).metadata,
        compression,
      ),
    );
  }
});

it("rejects invalid stored-block bound inputs", () => {
  expect(() => maximumPhysicalBlockByteLength(-1, {}, "raw")).toThrow(
    "Invalid encoded payload length",
  );
  expect(() => maximumPhysicalBlockByteLength(1.5, {}, "raw")).toThrow(
    "Invalid encoded payload length",
  );
  const cyclic: { self?: unknown } = {};
  cyclic.self = cyclic;
  expect(() => maximumPhysicalBlockByteLength(1, cyclic as never, "raw")).toThrow(
    "JSON serializable",
  );
});
