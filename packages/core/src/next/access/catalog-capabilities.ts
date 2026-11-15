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
 * Typed payload bytes cannot contain records. Each distinct object is expanded once; temporary
 * active/completed identity sets and work are bounded by the input graph, not its path count.
 */
export function requireWritableCatalogInput(value: unknown): void {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    ArrayBuffer.isView(value)
  )
    throw new StoreCorruptionError("Invalid catalog input record");
  const complete = new WeakSet(),
    active = new WeakSet();
  visit(value, 0);
  function visit(value: unknown, depth: number): void {
    requireWritableCatalog(value);
    if (typeof value !== "object" || value === null || ArrayBuffer.isView(value)) return;
    // The wire grammar caps nesting at32. Allow entry wrappers too, but reject cyclic/malformed
    // input without an unbounded recursive walk. Shared DAG paths do not repeat subtree work.
    if (depth > 64) throw new StoreCorruptionError("Catalog input nesting exceeds its cap");
    if (active.has(value)) throw new StoreCorruptionError("Cyclic catalog input");
    if (complete.has(value)) return;
    active.add(value);
    if (Array.isArray(value)) {
      // Do not invoke a caller-supplied array iterator before checking its operands.
      // eslint-disable-next-line @typescript-eslint/prefer-for-of
      for (let i = 0; i < value.length; i++) visit(value[i], depth + 1);
    } else {
      // DDL reads inherited properties too. A fresh wrapper can inherit an inspected operand
      // without copying it, even non-enumerably. Check values as the planner resolves them.
      let source: object | null = value;
      for (let hops = 0; source !== null && source !== Object.prototype; hops++) {
        if (hops > 64)
          throw new StoreCorruptionError("Catalog input prototype nesting exceeds its cap");
        for (const key of Object.getOwnPropertyNames(source))
          visit(Reflect.get(value, key), depth + 1);
        source = Reflect.getPrototypeOf(source);
      }
    }
    active.delete(value);
    complete.add(value);
  }
}

export class UnsupportedCatalogError extends Error {
  readonly code = "MINNOW_CATALOG_UNSUPPORTED";
  constructor(readonly feature: string) {
    super(`Unsupported required catalog feature: ${feature}`);
    this.name = "UnsupportedCatalogError";
  }
}
