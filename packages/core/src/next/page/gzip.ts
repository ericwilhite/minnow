/**
 * The optional second stage: gzip (the platform's CompressionStream) over a whole encoded page,
 * kept only when the page is ≥ 16 KiB and the result is at most half its size. Reading never
 * needs it for an uncompressed page.
 *
 * Stored form: u32 raw page length, then the gzip stream. Inflating checks that the stream ends
 * at exactly that length, so a damaged stream is `PageCorruptionError`, never a short page.
 */
import { PageCorruptionError } from "./format.js";

export const GZIP_MIN_PAGE_BYTES = 16 * 1024;
/** gzip is kept only when stored ≤ raw / this. */
export const GZIP_MIN_RATIO = 2;
/** A page larger than this is never gzipped (one value that large is better left plain). */
export const GZIP_MAX_PAGE_BYTES = 16 * 1024 * 1024;

async function drain(stream: ReadableStream<Uint8Array>, limit: number): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.length;
    if (total > limit) {
      await reader.cancel();
      throw new PageCorruptionError("gzip page inflates past its recorded length");
    }
    chunks.push(value);
  }
  const out = new Uint8Array(total);
  let at = 0;
  for (const chunk of chunks) {
    out.set(chunk, at);
    at += chunk.length;
  }
  return out;
}

function pipe(
  bytes: Uint8Array,
  transform: CompressionStream | DecompressionStream,
): ReadableStream<Uint8Array> {
  const writer = transform.writable.getWriter();
  // Errors surface through the readable side; the writer's promises must not go unhandled.
  writer.write(bytes as Uint8Array<ArrayBuffer>).catch(() => undefined);
  writer.close().catch(() => undefined);
  return transform.readable;
}

/** The stored gzip form of `page`, or null when gzip does not pay (size or ratio). */
export async function gzipPage(page: Uint8Array): Promise<Uint8Array | null> {
  if (page.length < GZIP_MIN_PAGE_BYTES || page.length > GZIP_MAX_PAGE_BYTES) return null;
  const limit = Math.floor(page.length / GZIP_MIN_RATIO) - 4;
  let compressed: Uint8Array;
  try {
    compressed = await drain(pipe(page, new CompressionStream("gzip")), limit);
  } catch (error) {
    if (error instanceof PageCorruptionError) return null; // past the limit: not worth it
    throw error;
  }
  const out = new Uint8Array(4 + compressed.length);
  new DataView(out.buffer).setUint32(0, page.length, true);
  out.set(compressed, 4);
  return out;
}

/** Inflates a stored gzip page into a fresh (8-aligned) buffer of exactly its raw length. */
export async function inflatePage(stored: Uint8Array): Promise<Uint8Array> {
  if (stored.length < 4) throw new PageCorruptionError("gzip page is shorter than its header");
  const length = new DataView(stored.buffer, stored.byteOffset, 4).getUint32(0, true);
  if (length > GZIP_MAX_PAGE_BYTES)
    throw new PageCorruptionError("gzip page records an impossible length");
  let raw: Uint8Array;
  try {
    raw = await drain(pipe(stored.subarray(4), new DecompressionStream("gzip")), length);
  } catch (error) {
    if (error instanceof PageCorruptionError) throw error;
    throw new PageCorruptionError(`gzip page does not inflate: ${String(error)}`);
  }
  if (raw.length !== length)
    throw new PageCorruptionError("gzip page inflates to the wrong length");
  return raw;
}
