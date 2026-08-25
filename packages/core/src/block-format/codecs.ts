import type { Compression } from "./types.js";

export interface CompressionCodec {
  readonly id: Compression;
  compress(bytes: Uint8Array, maximumOutputLength?: number): Promise<Uint8Array>;
  decompress(bytes: Uint8Array, expectedLength: number): Promise<Uint8Array>;
}

/** A caller's explicit compressed-output ceiling was reached before an output join/allocation. */
export class CompressionOutputLimitError extends RangeError {
  constructor(
    readonly byteLength: number,
    readonly maximumOutputLength: number,
  ) {
    super("Compressed payload exceeds caller output ceiling");
    this.name = "CompressionOutputLimitError";
  }
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
  async compress(bytes, maximumOutputLength) {
    if (maximumOutputLength !== undefined) {
      assertCallerOutputLength(maximumOutputLength);
      if (bytes.byteLength > maximumOutputLength) {
        throw new CompressionOutputLimitError(bytes.byteLength, maximumOutputLength);
      }
    }
    return bytes;
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
  outputLimitError?: (byteLength: number) => Error,
) {
  const write = (async () => {
    const writer = stream.writable.getWriter();
    await writer.write(new Uint8Array(bytes));
    await writer.close();
  })();
  const [writeResult, readResult] = await Promise.allSettled([
    write,
    readBounded(stream.readable, maximumOutputLength, boundErrorMessage, outputLimitError),
  ]);
  // A reader-side bound failure intentionally cancels the writer. Preserve that precise error
  // instead of racing it against the writer rejection caused by the cancellation.
  if (readResult.status === "rejected") throw readResult.reason;
  if (writeResult.status === "rejected") throw writeResult.reason;
  return readResult.value;
}

async function readBounded(
  readable: ReadableStream<Uint8Array>,
  maximumLength: number,
  boundErrorMessage: string,
  outputLimitError?: (byteLength: number) => Error,
): Promise<Uint8Array> {
  const reader = readable.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > maximumLength) {
      const error = outputLimitError?.(length) ?? new Error(boundErrorMessage);
      try {
        await reader.cancel(error);
      } catch {
        // Cancellation is cleanup. The deterministic bound error remains the operation result.
      }
      throw error;
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
  async compress(bytes, maximumOutputLength) {
    const { maximumOutputBytes } = getCompressionMemoryBound("gzip", bytes.byteLength);
    if (maximumOutputLength !== undefined) assertCallerOutputLength(maximumOutputLength);
    const outputLimit = Math.min(maximumOutputBytes, maximumOutputLength ?? maximumOutputBytes);
    const callerLimited = outputLimit < maximumOutputBytes;
    return transform(
      bytes,
      new CompressionStream("gzip"),
      outputLimit,
      callerLimited
        ? "Compressed payload exceeds caller output ceiling"
        : "Gzip payload exceeds compression bound",
      callerLimited
        ? (byteLength) => new CompressionOutputLimitError(byteLength, outputLimit)
        : undefined,
    );
  },
  async decompress(bytes, expectedLength) {
    const output = await transform(bytes, new DecompressionStream("gzip"), expectedLength);
    if (output.byteLength !== expectedLength) throw new Error("Gzip payload length mismatch");
    return output;
  },
};

function assertCallerOutputLength(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError("Invalid compression output ceiling");
  }
}

export function getCodec(id: Compression): CompressionCodec {
  switch (id) {
    case "raw":
      return rawCodec;
    case "gzip":
      return gzipCodec;
  }
}
