/** Bounded resident input to the existing inventory cursor, never retirement authority.
 * The existing 4 KiB owner allowance covers fixed controls plus at most 1 KiB packed pairs.
 * No borrowed writer objects or creating grants survive the copy. */
import type { ByteBudget } from "./extents.js";
import type { TaskWork } from "../task.js";
import type { ChainPosition } from "./metadata-journal.js";
import {
  makeRetirementInventory,
  RetirementClosedError,
  type InventoryContext,
} from "./retirement-inventory.js";
import type { RetirementInventory } from "./retirement-contract.js";
import { StoreCorruptionError } from "./contract.js";
class ResidentContext implements InventoryContext {
  // An input stream has no native retirement ticket/handle. Space publication
  // must first prepare it with the ordinary retained-reference witness.
  readonly owner = this;
  #bytes: Uint8Array | undefined;
  constructor(
    bytes: Uint8Array,
    readonly inventories: Set<RetirementInventory>,
  ) {
    this.#bytes = bytes;
  }
  /** Physical backing retained by this input (not its possibly shorter byte view). */
  get residentBytes(): number {
    return this.#bytes?.buffer.byteLength ?? 0;
  }
  read(_handle: number, position: ChainPosition | undefined, target: Uint8Array, work: TaskWork) {
    work.signal?.throwIfAborted();
    const bytes = this.#bytes;
    if (bytes === undefined) throw new RetirementClosedError();
    if (position !== undefined || bytes.length > target.length)
      throw new StoreCorruptionError("Invalid resident writer input window");
    target.set(bytes);
    return Promise.resolve({
      bytes: bytes.length,
      position: { next: 0, remaining: 0, pages: 1 },
    });
  }
  dispose(): Promise<void> {
    this.#bytes = undefined;
    return Promise.resolve();
  }
  finished(inventory: RetirementInventory): void {
    this.inventories.delete(inventory);
  }
}
/** Caller prepays RETIREMENT_OWNER_BYTES and transfers exclusive ownership of the backing. */
export function makeResidentWriterInput(
  bytes: Uint8Array,
  count: number,
  budget: ByteBudget,
  inventories: Set<RetirementInventory>,
): RetirementInventory {
  if (bytes.length !== count * 16 || bytes.buffer.byteLength > 1024)
    throw new StoreCorruptionError("Oversized resident writer input");
  return makeRetirementInventory(new ResidentContext(bytes, inventories), 0, count, budget);
}
