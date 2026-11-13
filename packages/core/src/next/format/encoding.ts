/** Unpublished rebuild formats have explicit refusal, never implicit upgrade. */
import { DURABLE_CODECS } from "../durable-format.js";
import { ManifestCorruptionError } from "./bytes.js";

export class UnsupportedEncodingError extends ManifestCorruptionError {
  override readonly code = "MINNOW_UNSUPPORTED_ENCODING";
  constructor(
    readonly format: string,
    readonly version: number,
    readonly supported: readonly number[],
  ) {
    super(
      `${format} encoding ${String(version)} is unsupported${version > Math.max(...supported) ? " (newer than this build)" : " (unpublished development format)"}`,
    );
    this.name = "UnsupportedEncodingError";
  }
}

export function requireEncoding(
  format: string,
  version: number,
  supported: readonly number[],
): void {
  if (!supported.includes(version)) throw new UnsupportedEncodingError(format, version, supported);
}

export const INLINE_RUN_ENCODING = DURABLE_CODECS.runEncoding.inline;
export const OVERFLOW_RUN_ENCODING = DURABLE_CODECS.runEncoding.overflow;
export type RunEncoding = typeof INLINE_RUN_ENCODING | typeof OVERFLOW_RUN_ENCODING;
export function readRunEncoding(version: number): RunEncoding {
  requireEncoding("run", version, [INLINE_RUN_ENCODING, OVERFLOW_RUN_ENCODING]);
  return version as RunEncoding;
}
