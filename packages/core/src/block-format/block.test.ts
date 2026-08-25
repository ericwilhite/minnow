import { describe, expect, it } from "vitest";
import {
  BLOCK_HEADER_LENGTH,
  BLOCK_FORMAT_VERSION,
  MAX_PHYSICAL_COLUMN_BYTE_LENGTH,
  StoredBlockPayloadTooLargeError,
  assertStoredBlockPayloadByteLength,
  crc32,
  decodeBlock,
  encodeBlock,
  inspectBlock,
  maximumPhysicalBlockByteLength,
  MAX_BLOCK_ROW_COUNT,
  verifyStoredBlock,
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

it("rejects a re-signed raw envelope whose stored and encoded lengths differ", async () => {
  const block = await encodeBlock({ type: "boolean", values: [true] }, "raw");
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
  view.setUint32(28, view.getUint32(28, true) + 1, true);
  view.setUint32(4, crc32(block.subarray(8, BLOCK_HEADER_LENGTH + view.getUint32(24, true))), true);
  expect(() => inspectBlock(block)).toThrow("Raw block lengths do not match");
});

it("materializes raw values before a caller can mutate the verified input", async () => {
  const block = await encodeBlock({ type: "boolean", values: [true, false] }, "raw");
  const description = inspectBlock(block);
  const pending = decodeBlock(block);
  const valuesOffset =
    description.headerLength + description.metadataLength + Math.ceil(description.rowCount / 8);
  block[valuesOffset] = 0b11;

  expect((await pending).column).toEqual({ type: "boolean", values: [true, false] });
});

for (const compression of ["raw", "gzip"] satisfies Compression[]) {
  it(`verifies ${compression} stored bytes without decompressing them`, async () => {
    const block = await encodeBlock({ type: "string", values: ["safe", "🌍", null] }, compression);
    const description = inspectBlock(block);
    expect(verifyStoredBlock(block)).toEqual(description);
    if (compression === "raw") expect(description.storedChecksum).toBe(description.checksum);

    const corrupted = block.slice();
    const payloadStart = corrupted.byteLength - description.storedLength;
    corrupted[payloadStart] = (corrupted[payloadStart] ?? 0) ^ 0x01;

    // Planning deliberately does not scan the full stored payload.
    expect(inspectBlock(corrupted)).toEqual(description);
    expect(() => verifyStoredBlock(corrupted)).toThrow("stored payload checksum");
    await expect(decodeBlock(corrupted)).rejects.toThrow("stored payload checksum");
  });
}

it("detects a one-bit corruption at every byte offset before decompression", async () => {
  for (const compression of ["raw", "gzip"] satisfies Compression[]) {
    const block = await encodeBlock(
      { type: "string", values: ["a", "é", "🌍", null] },
      compression,
    );
    for (let offset = 0; offset < block.byteLength; offset += 1) {
      const corrupted = block.slice();
      corrupted[offset] = (corrupted[offset] ?? 0) ^ 0x01;
      expect(
        () => verifyStoredBlock(corrupted),
        `${compression} corruption at byte ${String(offset)} was accepted`,
      ).toThrow();
    }
  }
});

it("retains the logical checksum independently of compressed-byte integrity", async () => {
  const block = await encodeBlock({ type: "string", values: ["compress me".repeat(100)] }, "gzip");
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
  view.setUint32(36, view.getUint32(36, true) ^ 0x01, true);
  resignEnvelope(block);

  // The stored gzip stream itself is intact, so the low-copy verifier has done its complete job.
  expect(() => verifyStoredBlock(block)).not.toThrow();
  await expect(decodeBlock(block)).rejects.toThrow("logical checksum");
});

it("rejects unknown ids, flags, and non-canonical metadata after envelope verification", async () => {
  const source = await encodeBlock({ type: "number", values: [1, 2] });
  const mutations: Array<{ offset: number; value: number; message: string }> = [
    { offset: 12, value: 0xff, message: "logical type" },
    { offset: 13, value: 0xff, message: "physical encoding" },
    { offset: 14, value: 0xff, message: "compression codec" },
    { offset: 15, value: 0x01, message: "flags" },
  ];
  for (const mutation of mutations) {
    const block = source.slice();
    block[mutation.offset] = mutation.value;
    resignEnvelope(block);
    expect(() => inspectBlock(block)).toThrow(mutation.message);
  }

  const reservedCodec = source.slice();
  reservedCodec[14] = 1;
  resignEnvelope(reservedCodec);
  expect(() => inspectBlock(reservedCodec)).toThrow("reserved compression codec id 1");

  const nonCanonical = source.slice();
  const metadataLength = new DataView(nonCanonical.buffer).getUint32(24, true);
  const canonical = new TextDecoder().decode(
    nonCanonical.subarray(BLOCK_HEADER_LENGTH, BLOCK_HEADER_LENGTH + metadataLength),
  );
  expect(canonical).toBe('{"zoneMap":{"min":1,"max":2}}');
  const reordered = '{"zoneMap":{"max":2,"min":1}}';
  expect(reordered.length).toBe(canonical.length);
  nonCanonical.set(new TextEncoder().encode(reordered), BLOCK_HEADER_LENGTH);
  resignEnvelope(nonCanonical);
  expect(() => inspectBlock(nonCanonical)).toThrow("Non-canonical block metadata");
});

it("refuses provisional or future versions before interpreting their layout", async () => {
  const block = await encodeBlock({ type: "boolean", values: [true] });
  expect(new DataView(block.buffer).getUint16(8, true)).toBe(BLOCK_FORMAT_VERSION);
  new DataView(block.buffer).setUint16(8, BLOCK_FORMAT_VERSION + 1, true);
  expect(() => inspectBlock(block)).toThrow(
    `Unsupported block version ${String(BLOCK_FORMAT_VERSION + 1)}`,
  );
});

it("rejects a v2 header above the decoded-row ceiling", async () => {
  const block = await encodeBlock({ type: "boolean", values: [true] });
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
  view.setUint32(16, MAX_BLOCK_ROW_COUNT, true);
  resignEnvelope(block);
  expect(inspectBlock(block).rowCount).toBe(MAX_BLOCK_ROW_COUNT);

  view.setUint32(16, MAX_BLOCK_ROW_COUNT + 1, true);
  resignEnvelope(block);
  expect(() => inspectBlock(block)).toThrow("maximum row count");
});

it("bounds gzip decompression by the validated declared length", async () => {
  const block = await encodeBlock({ type: "string", values: ["x".repeat(10_000)] }, "gzip");
  const view = new DataView(block.buffer);
  view.setUint32(28, 1, true);
  // Re-sign the envelope so the corrupted declared length reaches the decompression bound.
  view.setUint32(4, crc32(block.subarray(8, BLOCK_HEADER_LENGTH + view.getUint32(24, true))), true);
  await expect(decodeBlock(block)).rejects.toThrow("declared length");
});

it("bounds the complete stored block including its exact metadata envelope", async () => {
  const input = { type: "number", values: [3, null, -2, 9] } as const;
  const metadata = { zoneMap: { min: -2, max: 9 } };
  const encodedByteLength = 33;
  const metadataByteLength = new TextEncoder().encode(JSON.stringify(metadata)).byteLength;

  expect(maximumPhysicalBlockByteLength(encodedByteLength, metadata, "raw")).toBe(
    BLOCK_HEADER_LENGTH + metadataByteLength + encodedByteLength,
  );
  expect(maximumPhysicalBlockByteLength(encodedByteLength, metadata, "gzip")).toBe(
    BLOCK_HEADER_LENGTH + metadataByteLength + encodedByteLength * 2 + 64,
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
  expect(() => maximumPhysicalBlockByteLength(1, cyclic as never, "raw")).toThrow("canonical JSON");
  expect(() =>
    maximumPhysicalBlockByteLength(1, { zoneMap: { min: Number.NaN, max: 1 } }, "raw"),
  ).toThrow("supported canonical shape");
  expect(() => maximumPhysicalBlockByteLength(1, { unknown: true } as never, "raw")).toThrow(
    "supported canonical shape",
  );
});

it("distinguishes codec expansion beyond the stored hard limit", () => {
  expect(() => assertStoredBlockPayloadByteLength(MAX_PHYSICAL_COLUMN_BYTE_LENGTH)).not.toThrow();
  expect(() => assertStoredBlockPayloadByteLength(MAX_PHYSICAL_COLUMN_BYTE_LENGTH + 1)).toThrow(
    StoredBlockPayloadTooLargeError,
  );
  expect(() => assertStoredBlockPayloadByteLength(-1)).toThrow("Invalid stored payload length");
  expect(() => assertStoredBlockPayloadByteLength(1.5)).toThrow("Invalid stored payload length");
});

function resignEnvelope(block: Uint8Array): void {
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
  const metadataLength = view.getUint32(24, true);
  view.setUint32(4, crc32(block.subarray(8, BLOCK_HEADER_LENGTH + metadataLength)), true);
}
