import type { Compression } from "./types.js";

export interface CompressionCodec {
  readonly id: Compression;
  compress(bytes: Uint8Array): Promise<Uint8Array>;
  decompress(bytes: Uint8Array, expectedLength: number): Promise<Uint8Array>;
}

export interface CompressionMemoryBound {
  /** Largest returned buffer owned by the caller. */
  readonly maximumOutputBytes: number;
  /** Additional JavaScript-owned byte storage retained while producing the result. */
  readonly scratchBytes: number;
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

function assertInputLength(inputLength: number, codec: Compression): void {
  if (!Number.isSafeInteger(inputLength) || inputLength < 0) {
    throw new RangeError(`Invalid ${codec === "rle" ? "RLE" : codec} input length`);
  }
}

function checkedLinearBound(
  inputLength: number,
  multiplier: number,
  overhead: number,
  codec: Compression,
): number {
  assertInputLength(inputLength, codec);
  if (inputLength > Math.floor((Number.MAX_SAFE_INTEGER - overhead) / multiplier)) {
    throw new RangeError(`Invalid ${codec === "rle" ? "RLE" : codec} input length`);
  }
  return inputLength * multiplier + overhead;
}

export function maximumRleCompressedLength(inputLength: number): number {
  return checkedLinearBound(inputLength, 2, 0, "rle");
}

export function rleCompressionMemoryBound(inputLength: number): CompressionMemoryBound {
  return { maximumOutputBytes: maximumRleCompressedLength(inputLength), scratchBytes: 0 };
}

export function getCompressionMemoryBound(
  compression: Compression,
  inputLength: number,
): CompressionMemoryBound {
  switch (compression) {
    case "raw":
      assertInputLength(inputLength, compression);
      return { maximumOutputBytes: inputLength, scratchBytes: 0 };
    case "rle":
      return rleCompressionMemoryBound(inputLength);
    case "gzip": {
      // Native gzip encoders stay well below two bytes per input byte. The extra envelope keeps
      // small and empty streams bounded too. `transform` retains an input copy and its output
      // chunks while joining them, so include both as scratch storage at the copy boundary.
      checkedLinearBound(inputLength, 5, 128, compression);
      const maximumOutputBytes = checkedLinearBound(inputLength, 2, 64, compression);
      return { maximumOutputBytes, scratchBytes: inputLength + maximumOutputBytes };
    }
  }
}

function rleCompressedLength(bytes: Uint8Array): number {
  let outputLength = 0;
  for (let offset = 0; offset < bytes.length;) {
    const value = bytes[offset];
    if (value === undefined) break;
    let count = 1;
    while (count < 255 && bytes[offset + count] === value) count += 1;
    outputLength += 2;
    offset += count;
  }
  return outputLength;
}

export const rleCodec: CompressionCodec = {
  id: "rle",
  compress(bytes) {
    const output = new Uint8Array(rleCompressedLength(bytes));
    let outputOffset = 0;
    for (let offset = 0; offset < bytes.length;) {
      const value = bytes[offset];
      if (value === undefined) break;
      let count = 1;
      while (count < 255 && bytes[offset + count] === value) count += 1;
      output[outputOffset] = count;
      output[outputOffset + 1] = value;
      outputOffset += 2;
      offset += count;
    }
    return Promise.resolve(output);
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
  boundErrorMessage = "Decompressed payload exceeds declared length",
) {
  const write = (async () => {
    const writer = stream.writable.getWriter();
    await writer.write(new Uint8Array(bytes));
    await writer.close();
  })();
  const [, result] = await Promise.all([
    write,
    readBounded(stream.readable, maximumOutputLength, boundErrorMessage),
  ]);
  return result;
}

async function readBounded(
  readable: ReadableStream<Uint8Array>,
  maximumLength: number,
  boundErrorMessage: string,
): Promise<Uint8Array> {
  const reader = readable.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > maximumLength) {
      await reader.cancel(boundErrorMessage);
      throw new Error(boundErrorMessage);
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
    const { maximumOutputBytes } = getCompressionMemoryBound("gzip", bytes.byteLength);
    return transform(
      bytes,
      new CompressionStream("gzip"),
      maximumOutputBytes,
      "Gzip payload exceeds compression bound",
    );
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
