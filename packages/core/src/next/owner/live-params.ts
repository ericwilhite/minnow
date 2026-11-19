/** Capture wire values once, admitting each owned copy before allocating it. */
import type { Param } from "./api.js";
import { bigintBytes, stringBytes } from "../memory-values.js";
import { dateMilliseconds } from "../../date-value.js";

function getter(prototype: object, name: PropertyKey): (this: unknown) => unknown {
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Capture the accessor; callers provide its receiver with call().
  const read = Object.getOwnPropertyDescriptor(prototype, name)?.get;
  if (read === undefined) throw new TypeError("Missing native parameter intrinsic");
  return read as (this: unknown) => unknown;
}
const typedPrototype = Object.getPrototypeOf(Uint8Array.prototype) as object;
const typedBuffer = getter(typedPrototype, "buffer") as (this: unknown) => ArrayBufferLike;
const typedBrand = getter(typedPrototype, Symbol.toStringTag) as (
  this: unknown,
) => string | undefined;
const bufferBytes = getter(ArrayBuffer.prototype, "byteLength") as (this: unknown) => number;
const NativeDate = Date;

export function captureLiveParams(
  params: readonly Param[],
  charge: (bytes: number) => void,
): readonly Param[] {
  const ancestors = new Set<readonly unknown[]>();
  const array = (values: readonly unknown[]): Param[] => {
    if (ancestors.has(values)) throw new TypeError("Live parameters cannot contain cycles");
    const length = values.length;
    if (!Number.isInteger(length) || length < 0 || length > 0xffffffff)
      throw new TypeError("Invalid live parameter array length");
    charge(128 + length * 8);
    const owned = new Array<Param>(length);
    ancestors.add(values);
    try {
      for (let i = 0; i < length; i++) owned[i] = copy(values[i]);
      return owned;
    } finally {
      ancestors.delete(values);
    }
  };
  const copy = (value: unknown): Param => {
    if (value === null || typeof value === "boolean" || typeof value === "number") {
      charge(16);
      return value;
    }
    if (typeof value === "string") {
      charge(stringBytes(value));
      return value;
    }
    if (typeof value === "bigint") {
      charge(bigintBytes(value));
      return value;
    }
    if (Array.isArray(value)) return array(value);
    if (ArrayBuffer.isView(value)) {
      if (typedBrand.call(value) !== "Uint8Array")
        throw new TypeError("Binary parameters must be Uint8Array values");
      // Intrinsics ignore caller-shadowed buffer/length properties. structuredClone copies
      // the full backing capacity, so reserve that capacity before making the copy.
      let bytes: number;
      try {
        bytes = bufferBytes.call(typedBuffer.call(value));
      } catch {
        throw new TypeError("Live parameters cannot use SharedArrayBuffer values");
      }
      charge(128 + bytes);
      return structuredClone(value) as Uint8Array;
    }
    if (typeof value === "object") {
      let instant: number;
      try {
        instant = dateMilliseconds(value as Date);
      } catch {
        throw new TypeError("Invalid native live parameter");
      }
      charge(128);
      return new NativeDate(instant);
    }
    throw new TypeError("Invalid native live parameter");
  };
  return array(params);
}
