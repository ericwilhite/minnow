/** Transient inspection provenance shared by catalog decoders and all planning boundaries. */
import { StoreCorruptionError } from "../storage/contract.js";

// Capability provenance only: retain no unknown payload bytes or extension descriptors.
const inspections = new WeakSet();
export function catalogInspection<T extends object>(value: T): T {
  inspections.add(value);
  return value;
}
export function requireWritableCatalog(value: unknown): void {
  if (typeof value === "object" && value !== null && inspections.has(value))
    throw new UnsupportedCatalogError("read-only catalog inspection cannot be rewritten");
}
/** Planning accepts fresh wrappers around decoded records and copies some operands. Check
 * the entire supplied graph before any such copy can discard inspection provenance. Keep
 * this entry check separate from the encoder's per-record check to avoid quadratic walks.
 * Typed payload bytes cannot contain records; recursion uses only a bounded call stack.
 */
export function requireWritableCatalogInput(value: unknown, depth = 0): void {
  requireWritableCatalog(value);
  if (
    depth === 0 &&
    (typeof value !== "object" ||
      value === null ||
      Array.isArray(value) ||
      ArrayBuffer.isView(value))
  )
    throw new StoreCorruptionError("Invalid catalog input record");
  if (typeof value !== "object" || value === null || ArrayBuffer.isView(value)) return;
  // The wire grammar caps nesting at32. Allow entry wrappers too, but reject cyclic/malformed
  // input without a graph-sized, uncharged visited set or an unbounded recursive walk.
  if (depth > 64) throw new StoreCorruptionError("Catalog input nesting exceeds its cap");
  if (Array.isArray(value)) {
    // Do not invoke a caller-supplied array iterator before checking its operands.
    // eslint-disable-next-line @typescript-eslint/prefer-for-of
    for (let i = 0; i < value.length; i++) requireWritableCatalogInput(value[i], depth + 1);
  } else {
    // DDL reads inherited properties too. A fresh wrapper can inherit an inspected operand
    // without copying it, even non-enumerably. Check values as the planner resolves them.
    let source: object | null = value;
    for (let hops = 0; source !== null && source !== Object.prototype; hops++) {
      if (hops > 64)
        throw new StoreCorruptionError("Catalog input prototype nesting exceeds its cap");
      for (const key of Object.getOwnPropertyNames(source))
        requireWritableCatalogInput(Reflect.get(value, key), depth + 1);
      source = Reflect.getPrototypeOf(source);
    }
  }
}
export class UnsupportedCatalogError extends Error {
  readonly code = "MINNOW_CATALOG_UNSUPPORTED";
  constructor(readonly feature: string) {
    super(`Unsupported required catalog feature: ${feature}`);
    this.name = "UnsupportedCatalogError";
  }
}
