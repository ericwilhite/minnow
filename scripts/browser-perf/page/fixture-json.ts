/** Bench fixture descriptors: preserve statistics' bigint/bytes and special number bits. */
export function fixtureReplacer(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") return { $bigint: value.toString() };
  if (value instanceof Uint8Array) return { $bytes: Array.from(value) };
  if (typeof value === "number" && (!Number.isFinite(value) || Object.is(value, -0)))
    return { $number: Object.is(value, -0) ? "-0" : String(value) };
  return value;
}
export function fixtureReviver(_key: string, value: unknown): unknown {
  if (value !== null && typeof value === "object") {
    const v = value as Record<string, unknown>;
    if (Object.keys(v).length !== 1) return value;
    if (typeof v.$bigint === "string") return BigInt(v.$bigint);
    if (Array.isArray(v.$bytes)) {
      if (
        !v.$bytes.every(
          (n: unknown) => typeof n === "number" && Number.isInteger(n) && n >= 0 && n < 256,
        )
      )
        throw new Error("Invalid fixture bytes");
      return Uint8Array.from(v.$bytes as number[]);
    }
    if (typeof v.$number === "string") {
      if (!["-0", "NaN", "Infinity", "-Infinity"].includes(v.$number))
        throw new Error("Invalid fixture number");
      return Number(v.$number);
    }
  }
  return value;
}
