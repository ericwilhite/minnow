import { describe, expect, it } from "vitest";
import { crc32 } from "../block-format/checksum.js";
import { MAX_STORED_BLOCK_BYTE_LENGTH } from "../block-format/index.js";
import {
  SNAPSHOT_FRAME_KINDS,
  type SnapshotFrame,
  type SnapshotFrameStreamHeader,
} from "./types.js";
import {
  decodeSnapshotFrameStream,
  decodeSnapshotMetadataPage,
  encodeSnapshotFrameStreamFooter,
  encodeSnapshotFrameStreamHeader,
  encodeSnapshotMetadataPage,
  extendSnapshotFrameStreamChecksum,
  snapshotFrameEnvelopeParts,
} from "./snapshot-stream.js";

function headerFor(frames: readonly SnapshotFrame[]): SnapshotFrameStreamHeader {
  const kinds = Object.fromEntries(
    SNAPSHOT_FRAME_KINDS.map((kind) => [kind, { frameCount: 0, itemCount: 0, storedBytes: 0 }]),
  ) as Record<
    (typeof SNAPSHOT_FRAME_KINDS)[number],
    { frameCount: number; itemCount: number; storedBytes: number }
  >;
  for (const frame of frames) {
    const summary = kinds[frame.kind];
    summary.frameCount += 1;
    summary.itemCount += frame.itemCount;
    summary.storedBytes += frame.payload.byteLength;
  }
  return {
    formatVersion: 1,
    databaseVersion: 7,
    createdAt: "2026-01-01T00:00:00.000Z",
    kinds,
  };
}

function containerParts(
  frames: readonly SnapshotFrame[],
  header = headerFor(frames),
): Uint8Array[] {
  const parts = [encodeSnapshotFrameStreamHeader(header)];
  let checksum = 0;
  let itemCount = 0;
  let storedBytes = 0;
  for (const frame of frames) {
    const envelope = snapshotFrameEnvelopeParts(frame);
    checksum = extendSnapshotFrameStreamChecksum(checksum, envelope);
    itemCount += frame.itemCount;
    storedBytes += frame.payload.byteLength;
    parts.push(...envelope);
  }
  parts.push(
    encodeSnapshotFrameStreamFooter({
      frameCount: frames.length,
      itemCount,
      storedBytes,
      checksum,
    }),
  );
  return parts;
}

function joinParts(parts: readonly Uint8Array[]): Uint8Array {
  const bytes = new Uint8Array(parts.reduce((total, part) => total + part.byteLength, 0));
  let offset = 0;
  for (const part of parts) {
    bytes.set(part, offset);
    offset += part.byteLength;
  }
  return bytes;
}

async function* chunks(parts: readonly Uint8Array[]): AsyncGenerator<Uint8Array> {
  for (const part of parts) {
    for (let offset = 0; offset < part.byteLength; offset += 13) {
      yield part.subarray(offset, offset + 13);
    }
  }
}

async function decode(parts: readonly Uint8Array[]) {
  const entries = [];
  for await (const entry of decodeSnapshotFrameStream(chunks(parts))) entries.push(entry);
  return entries;
}

describe("framed snapshot v1", () => {
  it("round-trips deterministic metadata and raw block frames", async () => {
    const metadata = encodeSnapshotMetadataPage([
      {
        kind: "unique-chunk",
        namespaceId: "unique-1",
        generationId: "generation-1",
        ordinal: 0,
        keyTokens: ["key-1"],
      },
    ]);
    const block = Uint8Array.of(1, 2, 3, 4);
    const frames: SnapshotFrame[] = [
      {
        sequence: 0,
        kind: "unique-page",
        itemCount: 1,
        key: null,
        payload: metadata,
        checksum: crc32(metadata),
      },
      {
        sequence: 1,
        kind: "block",
        itemCount: 1,
        key: "block-1",
        payload: block,
        checksum: crc32(block),
      },
    ];

    const entries = await decode(containerParts(frames));
    expect(entries.map((entry) => entry.type)).toEqual(["header", "frame", "frame", "footer"]);
    expect(decodeSnapshotMetadataPage(metadata)).toMatchObject([
      { kind: "unique-chunk", keyTokens: ["key-1"] },
    ]);
    expect(encodeSnapshotMetadataPage([{ z: 1, a: 2n }])).toEqual(
      encodeSnapshotMetadataPage([{ a: 2n, z: 1 }]),
    );
  });

  it("rejects per-kind totals even when the global frame count matches", async () => {
    const payload = encodeSnapshotMetadataPage([
      {
        kind: "unique-chunk",
        namespaceId: "unique-1",
        generationId: "generation-1",
        ordinal: 0,
        keyTokens: ["key-1"],
      },
    ]);
    const frame: SnapshotFrame = {
      sequence: 0,
      kind: "unique-page",
      itemCount: 1,
      key: null,
      payload,
      checksum: crc32(payload),
    };
    const original = headerFor([frame]);
    const header: SnapshotFrameStreamHeader = {
      ...original,
      kinds: {
        ...original.kinds,
        "unique-page": { ...original.kinds["unique-page"], itemCount: 2 },
      },
    };
    await expect(decode(containerParts([frame], header))).rejects.toThrow(/summary/i);
  });

  it("rejects a metadata item count mismatch before adapter staging", async () => {
    const payload = encodeSnapshotMetadataPage([
      {
        kind: "unique-chunk",
        namespaceId: "unique-1",
        generationId: "generation-1",
        ordinal: 0,
        keyTokens: ["key-1"],
      },
    ]);
    const frame: SnapshotFrame = {
      sequence: 0,
      kind: "unique-page",
      itemCount: 2,
      key: null,
      payload,
      checksum: crc32(payload),
    };
    await expect(decode(containerParts([frame]))).rejects.toThrow(/item count/i);
  });

  it("rejects hostile declared key and payload lengths before reading them", async () => {
    const payload = Uint8Array.of(1);
    const frame: SnapshotFrame = {
      sequence: 0,
      kind: "block",
      itemCount: 1,
      key: "a",
      payload,
      checksum: crc32(payload),
    };
    const parts = containerParts([frame]);
    const prefix = parts[1];
    if (prefix === undefined) throw new Error("missing frame prefix");
    const keyAttack = prefix.slice();
    new DataView(keyAttack.buffer).setUint32(20, 0xffffffff, true);
    let pulls = 0;
    const source = async function* () {
      yield parts[0] ?? new Uint8Array();
      pulls += 1;
      yield keyAttack;
      pulls += 1;
      yield new Uint8Array(1024 * 1024);
    };
    await expect(async () => {
      for await (const entry of decodeSnapshotFrameStream(source())) void entry;
    }).rejects.toThrow(/key/i);
    expect(pulls).toBe(1);

    const sizeAttack = prefix.slice();
    new DataView(sizeAttack.buffer).setBigUint64(
      24,
      BigInt(MAX_STORED_BLOCK_BYTE_LENGTH + 1),
      true,
    );
    await expect(decode([parts[0] ?? new Uint8Array(), sizeAttack])).rejects.toThrow(
      /payload length/i,
    );
  });

  it("rejects empty chunks but accepts a large upstream chunk", async () => {
    await expect(async () => {
      for await (const entry of decodeSnapshotFrameStream(
        (async function* () {
          yield new Uint8Array();
        })(),
      ))
        void entry;
    }).rejects.toThrow(/non-empty Uint8Array/i);

    const payload = new Uint8Array(1024 * 1024 + 1);
    const frame: SnapshotFrame = {
      sequence: 0,
      kind: "block",
      itemCount: 1,
      key: "large-block",
      payload,
      checksum: crc32(payload),
    };
    const container = joinParts(containerParts([frame]));
    const entries = [];
    for await (const entry of decodeSnapshotFrameStream(
      (async function* () {
        yield container;
      })(),
    )) {
      entries.push(entry.type);
    }
    expect(entries).toEqual(["header", "frame", "footer"]);
  });

  it("closes the source iterator on early cancellation", async () => {
    let closed = false;
    const source = async function* () {
      try {
        yield* chunks(containerParts([]));
      } finally {
        closed = true;
      }
    };
    const iterator = decodeSnapshotFrameStream(source())[Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toMatchObject({ value: { type: "header" } });
    await iterator.return(undefined);
    expect(closed).toBe(true);
  });

  it("rejects noncanonical metadata primitives before allocating a page", () => {
    expect(() => encodeSnapshotMetadataPage([Number.NaN])).toThrow(/finite/i);
    expect(() => encodeSnapshotMetadataPage([new Date(Number.NaN)])).toThrow(/date/i);
    expect(() => encodeSnapshotMetadataPage([-1n])).toThrow(/unsigned 64-bit/i);
    expect(() => encodeSnapshotMetadataPage([1n << 64n])).toThrow(/unsigned 64-bit/i);
    expect(() => encodeSnapshotMetadataPage(["\ud800"])).toThrow(/unpaired surrogate/i);
    const alias = { id: "same" };
    expect(() => encodeSnapshotMetadataPage([alias, alias])).toThrow(/cycle or alias/i);

    const shadowed = new Date(1234) as Date & { getTime: () => number };
    shadowed.getTime = () => Number.NaN;
    expect(decodeSnapshotMetadataPage(encodeSnapshotMetadataPage([shadowed]))).toEqual([
      new Date(1234),
    ]);
    const signedZero = decodeSnapshotMetadataPage(encodeSnapshotMetadataPage([-0]));
    expect(signedZero).toHaveLength(1);
    expect(Object.is(signedZero[0], -0)).toBe(true);

    const rawPage = (tag: number, payload: Uint8Array): Uint8Array => {
      const bytes = new Uint8Array(6 + payload.byteLength);
      const view = new DataView(bytes.buffer);
      bytes[0] = 6;
      view.setUint32(1, 1, true);
      bytes[5] = tag;
      bytes.set(payload, 6);
      return bytes;
    };
    const nonfinite = new Uint8Array(8);
    new DataView(nonfinite.buffer).setFloat64(0, Number.POSITIVE_INFINITY, true);
    expect(() => decodeSnapshotMetadataPage(rawPage(3, nonfinite))).toThrow(/finite/i);
    const fractionalDate = new Uint8Array(8);
    new DataView(fractionalDate.buffer).setFloat64(0, 1.5, true);
    expect(() => decodeSnapshotMetadataPage(rawPage(8, fractionalDate))).toThrow(/date/i);
    const oversizedBigint = new Uint8Array(4);
    new DataView(oversizedBigint.buffer).setUint32(0, 0xffffffff, true);
    expect(() => decodeSnapshotMetadataPage(rawPage(5, oversizedBigint))).toThrow(
      /unsigned 64-bit/i,
    );
  });

  it("preserves a parse failure when source cleanup also fails", async () => {
    const primary = new RangeError("primary chunk failure");
    const source: AsyncIterable<Uint8Array> = {
      [Symbol.asyncIterator]() {
        return {
          next: async () => ({ done: false as const, value: new Uint8Array() }),
          return: async () => {
            throw new Error("secondary close failure");
          },
        };
      },
    };
    await expect(async () => {
      try {
        for await (const entry of decodeSnapshotFrameStream(source)) void entry;
      } catch (error) {
        if (error instanceof RangeError) throw primary;
        throw error;
      }
    }).rejects.toBe(primary);
  });

  it("rejects truncation, trailing bytes, and frame reordering", async () => {
    const payload = encodeSnapshotMetadataPage([
      {
        kind: "unique-chunk",
        namespaceId: "unique-1",
        generationId: "generation-1",
        ordinal: 0,
        keyTokens: ["key-1"],
      },
    ]);
    const frame: SnapshotFrame = {
      sequence: 0,
      kind: "unique-page",
      itemCount: 1,
      key: null,
      payload,
      checksum: crc32(payload),
    };
    const valid = containerParts([frame]);
    await expect(decode(valid.slice(0, -1))).rejects.toThrow(/truncated/i);
    await expect(decode([...valid, Uint8Array.of(1)])).rejects.toThrow(/trailing/i);
    const badPrefix = valid[1]?.slice();
    if (badPrefix === undefined) throw new Error("missing frame prefix");
    new DataView(badPrefix.buffer).setBigUint64(8, 1n, true);
    await expect(decode([valid[0] ?? new Uint8Array(), badPrefix])).rejects.toThrow(/sequence/i);
  });
});
