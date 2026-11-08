/** Independent enumeration for tests: four equally spaced classes per doubling above 32 B. */
const CLASSES = [0, 8, 16, 24];
for (let base = 32; base <= 2 ** 30; base *= 2)
  for (let part = 0; part < 4; part++) CLASSES.push(base + (base / 4) * part);
export function referenceCapacity(bytes: number): number {
  const size = CLASSES.find((size) => size >= bytes);
  if (size === undefined) throw new RangeError("Reference capacity exceeds test domain");
  return size;
}

/** Preserve every source bit, including NaN payloads, with zero-filled dedicated slack. */
export function paddedCopy<T>(value: T): T {
  if (ArrayBuffer.isView(value)) {
    const buffer = new ArrayBuffer(referenceCapacity(value.byteLength));
    new Uint8Array(buffer).set(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
    return Reflect.construct(value.constructor, [buffer]) as T;
  }
  if (Array.isArray(value)) return value.map((entry: unknown) => paddedCopy(entry)) as T;
  if (value !== null && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [key, paddedCopy(entry)]),
    ) as T;
  return value;
}
