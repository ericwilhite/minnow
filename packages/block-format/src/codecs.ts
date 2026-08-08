import type { Compression } from "./types.js";

export interface CompressionCodec {
  readonly id: Compression;
  compress(bytes: Uint8Array): Promise<Uint8Array>;
  decompress(bytes: Uint8Array, expectedLength: number): Promise<Uint8Array>;
}

function copy(bytes: Uint8Array): Uint8Array {
  return new Uint8Array(bytes);
}

export const rawCodec: CompressionCodec = {
  id: "raw",
  compress(bytes) {
    return Promise.resolve(copy(bytes));
  },
  decompress(bytes, expectedLength) {
    if (bytes.byteLength !== expectedLength) throw new Error("Raw payload length mismatch");
    return Promise.resolve(copy(bytes));
  },
};

export const rleCodec: CompressionCodec = {
  id: "rle",
  compress(bytes) {
    const output: number[] = [];
    for (let offset = 0; offset < bytes.length;) {
      const value = bytes[offset];
      if (value === undefined) break;
      let count = 1;
      while (count < 255 && bytes[offset + count] === value) count += 1;
      output.push(count, value);
      offset += count;
    }
    return Promise.resolve(Uint8Array.from(output));
  },
  decompress(bytes, expectedLength) {
    if (bytes.length % 2 !== 0) throw new Error("Invalid RLE payload");
    const output = new Uint8Array(expectedLength);
    let offset = 0;
    for (let index = 0; index < bytes.length; index += 2) {
      const count = bytes[index] ?? 0;
      const value = bytes[index + 1] ?? 0;
      if (count === 0 || offset + count > expectedLength) throw new Error("Invalid RLE run");
      output.fill(value, offset, offset + count);
      offset += count;
    }
    if (offset !== expectedLength) throw new Error("RLE payload length mismatch");
    return Promise.resolve(output);
  },
};

async function transform(
  bytes: Uint8Array,
  stream: CompressionStream | DecompressionStream,
  maximumOutputLength = Number.POSITIVE_INFINITY,
) {
  const write = (async () => {
    const writer = stream.writable.getWriter();
    await writer.write(new Uint8Array(bytes));
    await writer.close();
  })();
  const [, result] = await Promise.all([write, readBounded(stream.readable, maximumOutputLength)]);
  return result;
}

async function readBounded(
  readable: ReadableStream<Uint8Array>,
  maximumLength: number,
): Promise<Uint8Array> {
  const reader = readable.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > maximumLength) {
      await reader.cancel("Decompressed payload exceeds declared length");
      throw new Error("Decompressed payload exceeds declared length");
    }
    chunks.push(value);
  }
  const output = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

export const gzipCodec: CompressionCodec = {
  id: "gzip",
  async compress(bytes) {
    return transform(bytes, new CompressionStream("gzip"));
  },
  async decompress(bytes, expectedLength) {
    const output = await transform(bytes, new DecompressionStream("gzip"), expectedLength);
    if (output.byteLength !== expectedLength) throw new Error("Gzip payload length mismatch");
    return output;
  },
};

export function getCodec(id: Compression): CompressionCodec {
  switch (id) {
    case "raw":
      return rawCodec;
    case "rle":
      return rleCodec;
    case "gzip":
      return gzipCodec;
  }
}
