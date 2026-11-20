/** Native getters read binary storage even when a row shadows a view's public properties. */
type Getter = (this: unknown) => unknown;
function getter(prototype: object, key: PropertyKey): Getter {
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Intrinsics are called with an explicit receiver.
  const read = Object.getOwnPropertyDescriptor(prototype, key)?.get;
  if (read === undefined) throw new TypeError("Missing binary intrinsic");
  return read as Getter;
}
const typed = Object.getPrototypeOf(Uint8Array.prototype) as object;
const brand = getter(typed, Symbol.toStringTag);
const typedBuffer = getter(typed, "buffer"),
  typedOffset = getter(typed, "byteOffset"),
  typedLength = getter(typed, "byteLength");
const dataBuffer = getter(DataView.prototype, "buffer"),
  dataOffset = getter(DataView.prototype, "byteOffset"),
  dataLength = getter(DataView.prototype, "byteLength");
const bufferLength = getter(ArrayBuffer.prototype, "byteLength");
function backing(value: ArrayBuffer | ArrayBufferView): ArrayBufferLike {
  if (!ArrayBuffer.isView(value)) return value;
  return (brand.call(value) === undefined ? dataBuffer : typedBuffer).call(
    value,
  ) as ArrayBufferLike;
}
export function liveBinaryCapacity(value: ArrayBuffer | ArrayBufferView): number {
  const buffer = backing(value);
  try {
    return bufferLength.call(buffer) as number;
  } catch (error) {
    throw new TypeError("Live binary values require non-shared ArrayBuffer storage", {
      cause: error,
    });
  }
}
export function liveBinaryView(value: ArrayBuffer | ArrayBufferView): Uint8Array {
  if (!ArrayBuffer.isView(value)) return new Uint8Array(value);
  const isData = brand.call(value) === undefined;
  return new Uint8Array(
    backing(value),
    (isData ? dataOffset : typedOffset).call(value) as number,
    (isData ? dataLength : typedLength).call(value) as number,
  );
}
