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

/**
 * The raw codec returns its input view unchanged in both directions: callers treat compressed
 * and decompressed payloads as read-only (encode copies the payload into the block envelope,
 * decode consumers only read or copy out), so a full-payload defensive copy per block would be
 * pure overhead on the default compression path. The returned view may share the caller's
 * buffer at a non-zero byte offset.
 */
export const rawCodec: CompressionCodec = {
  id: "raw",
  compress(bytes) {
    return Promise.resolve(bytes);
  },
  decompress(bytes, expectedLength) {
    if (bytes.byteLength !== expectedLength) throw new Error("Raw payload length mismatch");
    return Promise.resolve(bytes);
  },
};

function assertInputLength(inputLength: number, codec: Compression): void {
  if (!Number.isSafeInteger(inputLength) || inputLength < 0) {
    throw new RangeError(`Invalid ${codec} input length`);
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
    throw new RangeError(`Invalid ${codec} input length`);
  }
  return inputLength * multiplier + overhead;
}

export function getCompressionMemoryBound(
  compression: Compression,
  inputLength: number,
): CompressionMemoryBound {
  switch (compression) {
    case "raw":
      assertInputLength(inputLength, compression);
      return { maximumOutputBytes: inputLength, scratchBytes: 0 };
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
    case "gzip":
      return gzipCodec;
  }
}
