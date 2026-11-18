/** Bounded live diagnostics use WireError's shape without materializing a lazy error stack.
 * Size cloneable data before allocation; prepaid scratch funds the bounded walk and fallback.
 */
import type { WireError } from "../owner/errors.js";
import { bigintBytes, OBJECT_OVERHEAD_BYTES, stringBytes } from "../memory/governor.js";
import { LIVE_ERROR_MAX_BYTES } from "./constants.js";

const REFUSE = new RangeError("Live error diagnostic exceeds its bounded shape");
const MAX_NODES = 32;
const MAX_FIELDS = 64;
// The owner WireError/worker-protocol contract cuts causes after eight links.
const MAX_CAUSES = 8;
const MAX_BIGINT = 1n << 4096n;
const fallback: WireError = Object.freeze({
  name: "MemoryAdmissionError",
  message: "Live error notification exceeds its bounded memory budget",
  props: Object.freeze({ code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" }),
});
interface Walk {
  readonly seen: Set<object>;
  nodes: number;
  fields: number;
}
function node(walk: Walk): void {
  if (++walk.nodes > MAX_NODES) throw REFUSE;
}
function field(walk: Walk): void {
  if (++walk.fields > MAX_FIELDS) throw REFUSE;
}
function own(value: object, key: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  if (descriptor === undefined) return undefined;
  if (!("value" in descriptor)) throw REFUSE;
  return descriptor.value as unknown;
}
function text(value: object, key: string, defaultValue: string): string {
  let current: object | null = value;
  for (let depth = 0; depth < 4 && current !== null; depth++) {
    const descriptor = Object.getOwnPropertyDescriptor(current, key);
    if (descriptor !== undefined) {
      if (!("value" in descriptor) || typeof descriptor.value !== "string") throw REFUSE;
      return descriptor.value;
    }
    current = Object.getPrototypeOf(current) as object | null;
  }
  return defaultValue;
}
function platform(error: unknown): error is DOMException {
  return typeof DOMException !== "undefined" && error instanceof DOMException;
}
function platformText(error: DOMException, key: "name" | "message"): string {
  const descriptor = Object.getOwnPropertyDescriptor(DOMException.prototype, key);
  const value: unknown = descriptor?.get?.call(error);
  if (typeof value !== "string") throw REFUSE;
  return value;
}
function nonErrorMessage(error: unknown): string {
  if (typeof error === "string") return error;
  if (
    error === null ||
    error === undefined ||
    typeof error === "number" ||
    typeof error === "boolean"
  )
    return String(error);
  // Do not invoke a consumer's toString/getter while preparing a bounded notification.
  return "Non-Error live query failure";
}
function excluded(key: string): boolean {
  return key === "name" || key === "message" || key === "stack" || key === "cause";
}
function dataBytes(value: unknown, walk: Walk, depth: number): number {
  if (depth > MAX_CAUSES) throw REFUSE;
  if (typeof value === "string") return stringBytes(value);
  if (typeof value === "bigint") {
    if (value <= -MAX_BIGINT || value >= MAX_BIGINT) throw REFUSE;
    return bigintBytes(value);
  }
  if (
    value === null ||
    value === undefined ||
    typeof value === "number" ||
    typeof value === "boolean"
  )
    return 16;
  if (typeof value !== "object") throw REFUSE;
  if (walk.seen.has(value)) return 0;
  node(walk);
  walk.seen.add(value);
  if (value instanceof ArrayBuffer) return OBJECT_OVERHEAD_BYTES + value.byteLength;
  if (ArrayBuffer.isView(value)) {
    if (!(value.buffer instanceof ArrayBuffer)) throw REFUSE;
    return OBJECT_OVERHEAD_BYTES + dataBytes(value.buffer, walk, depth + 1);
  }
  if (value instanceof Date) return OBJECT_OVERHEAD_BYTES;
  let bytes = OBJECT_OVERHEAD_BYTES;
  if (value instanceof Map) {
    Map.prototype.forEach.call(value, (item: unknown, key: unknown) => {
      field(walk);
      field(walk);
      bytes +=
        OBJECT_OVERHEAD_BYTES + dataBytes(key, walk, depth + 1) + dataBytes(item, walk, depth + 1);
    });
    return bytes;
  }
  if (value instanceof Set) {
    Set.prototype.forEach.call(value, (item: unknown) => {
      field(walk);
      bytes += OBJECT_OVERHEAD_BYTES + dataBytes(item, walk, depth + 1);
    });
    return bytes;
  }
  if (Array.isArray(value)) {
    if (value.length > MAX_FIELDS) throw REFUSE;
    bytes += 8 * value.length;
  } else {
    const prototype: unknown = Object.getPrototypeOf(value);
    if (prototype !== null && prototype !== Object.prototype) throw REFUSE;
  }
  for (const key in value)
    if (Object.hasOwn(value, key)) {
      field(walk);
      bytes +=
        OBJECT_OVERHEAD_BYTES + stringBytes(key) + dataBytes(own(value, key), walk, depth + 1);
    }
  return bytes;
}
function measure(error: unknown, walk: Walk, depth = 0): number {
  node(walk);
  if (platform(error))
    return (
      256 + stringBytes(platformText(error, "name")) + stringBytes(platformText(error, "message"))
    );
  if (!(error instanceof Error)) return 256 + stringBytes(nonErrorMessage(error));
  let bytes =
    256 + stringBytes(text(error, "name", "Error")) + stringBytes(text(error, "message", ""));
  // Causes clone their properties independently, so aliases across causes count again.
  walk.seen.clear();
  for (const key in error)
    if (Object.hasOwn(error, key) && !excluded(key)) {
      field(walk);
      bytes += OBJECT_OVERHEAD_BYTES + stringBytes(key) + dataBytes(own(error, key), walk, 0);
    }
  const cause = own(error, "cause");
  if (cause !== undefined && depth < MAX_CAUSES) bytes += measure(cause, walk, depth + 1);
  if (bytes > LIVE_ERROR_MAX_BYTES) throw REFUSE;
  return bytes;
}
function encode(error: unknown, depth = 0): WireError {
  if (platform(error))
    return {
      name: platformText(error, "name"),
      message: platformText(error, "message"),
      domException: true,
    };
  if (!(error instanceof Error)) return { name: "Error", message: nonErrorMessage(error) };
  const result: WireError = {
    name: text(error, "name", "Error"),
    message: text(error, "message", ""),
  };
  const props = Object.create(null) as Record<string, unknown>;
  let count = 0;
  for (const key in error)
    if (Object.hasOwn(error, key) && !excluded(key)) {
      props[key] = own(error, key);
      count++;
    }
  if (count > 0) result.props = structuredClone(props);
  const cause = own(error, "cause");
  if (cause !== undefined && depth < MAX_CAUSES) result.cause = encode(cause, depth + 1);
  return result;
}

/** Caller prepays LIVE_ERROR_SCRATCH_BYTES; admitted copy credit lasts until error ACK.
 * No diagnostic/property clone precedes admission. Refusals are explicit 53200 errors.
 */
export function prepareLiveError(error: unknown, admit: (bytes: number) => boolean): WireError {
  try {
    const walk: Walk = { seen: new Set(), nodes: 0, fields: 0 };
    const bytes = measure(error, walk);
    if (bytes > LIVE_ERROR_MAX_BYTES || !admit(3 * bytes + 1024)) return fallback;
    // Admission may reenter the governor. Recheck mutable diagnostics before copying them.
    walk.seen.clear();
    walk.nodes = 0;
    walk.fields = 0;
    if (measure(error, walk) > bytes) return fallback;
    return encode(error);
  } catch {
    return fallback;
  }
}
