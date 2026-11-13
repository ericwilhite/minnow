/** Header identity checks shared by read-only native preflight and log decoding. No payload parse. */
import { requireHeader, type FormatErrorType } from "../durable-format.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { StoreCorruptionError, LOG_HEADER_SLOT_BYTES } from "./contract.js";

/** Empty/torn slots are recoverable; intact unknown formats are typed refusals. */
export function validLogHeaderEnvelope(
  bytes: Uint8Array | null,
  ErrorType: FormatErrorType = StoreCorruptionError,
): boolean {
  if (bytes === null || bytes.length < 52 || bytes.length > LOG_HEADER_SLOT_BYTES) return false;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(48, true) !== xxHash32(bytes, 0, 0, 48)) return false;
  requireHeader("logHeader", bytes, ErrorType);
  for (let at = 52; at < bytes.length; at++)
    if (bytes[at] !== 0) throw new ErrorType("Nonzero reserved log header padding");
  return true;
}
