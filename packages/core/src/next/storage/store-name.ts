/** The first native format fixes name encoding; names are never normalized. */
export const MAX_STORE_NAME_BYTES = 80;
export class InvalidStoreNameError extends TypeError {
  readonly code = "MINNOW_INVALID_STORE_NAME";
  constructor() {
    super("A database name must be non-empty, well-formed Unicode and at most 80 UTF-8 bytes");
    this.name = "InvalidStoreNameError";
  }
}
/** No encoding allocation or platform access before refusal. */
export function validateStoreName(name: string): void {
  if (typeof name !== "string" || name.length === 0) throw new InvalidStoreNameError();
  let bytes = 0;
  for (let at = 0; at < name.length; at++) {
    const code = name.charCodeAt(at);
    if (code >= 0xd800 && code <= 0xdbff) {
      const low = name.charCodeAt(++at);
      if (!(low >= 0xdc00 && low <= 0xdfff)) throw new InvalidStoreNameError();
      bytes += 4;
    } else {
      if (code >= 0xdc00 && code <= 0xdfff) throw new InvalidStoreNameError();
      bytes += code < 0x80 ? 1 : code < 0x800 ? 2 : 3;
    }
    if (bytes > MAX_STORE_NAME_BYTES) throw new InvalidStoreNameError();
  }
}

/** Dot-only components need escaping; encodeURIComponent otherwise fixes the native path. */
export function encodeStoreName(name: string): string {
  validateStoreName(name);
  return name === "." || name === ".." ? name.replaceAll(".", "%2E") : encodeURIComponent(name);
}
