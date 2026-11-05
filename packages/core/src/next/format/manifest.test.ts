import { describe, expect, it } from "vitest";
import { blobAddress, type BlobRef, type ColumnChunk, type ExtentRef } from "./manifest.js";

const packed: ExtentRef = { offset: 8192, length: 3000 };
const large: ExtentRef = { offset: 16_384, length: 70_000 };
const blob = (extent: ExtentRef, offset: number, length: number): BlobRef => ({
  extent,
  offset,
  length,
  checksum: 0,
});

describe("BlobRef names its extent (ruling R15.1/6, storage's shape)", () => {
  // Two small chunks and their directories packed into one extent; one large chunk on its own.
  const chunks: Array<Pick<ColumnChunk, "data" | "directory">> = [
    { data: { extent: packed, offset: 0, length: 900 }, directory: blob(packed, 900, 100) },
    { data: { extent: packed, offset: 1000, length: 1800 }, directory: blob(packed, 2800, 200) },
    { data: { extent: large, offset: 0, length: 69_000 }, directory: blob(large, 69_000, 1000) },
  ];

  it("lets a walk list extents from the references alone, a packed extent once", () => {
    const extents = new Map<number, ExtentRef>();
    for (const chunk of chunks)
      for (const ref of [chunk.data, chunk.directory]) extents.set(ref.extent.offset, ref.extent);
    expect([...extents.values()]).toEqual([packed, large]);
  });

  it("addresses blobs within their extent and pages within the chunk's data blob", () => {
    const second = chunks[1];
    expect(second && blobAddress(second.directory)).toBe(8192 + 2800);
    // A page at offset 600 of the second chunk is data-space byte 8192 + 1000 + 600.
    expect(second && blobAddress({ ...second.data, checksum: 0 }) + 600).toBe(9792);
  });
});
