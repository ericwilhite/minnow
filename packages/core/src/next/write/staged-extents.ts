/** A staged part retains one charged immutable inventory, not its writer's whole resident list.
 * Extra references are publication/retirement borrows. One live owner funds its bounded
 * queue/attempt slots, including array growth overlap; cardinality stays on native scratch.
 */
import type { ByteBudget } from "../storage/extents.js";
import type { TaskWork } from "../task.js";
import { capturedWriterReferences, type CapturedWriterExtents } from "../storage/writer-extents.js";
export class StagedExtents {
  #source: CapturedWriterExtents | undefined;
  #refs = 1;
  queued = false;
  readonly empty: boolean;
  constructor(
    source: CapturedWriterExtents,
    readonly track: (pending: Promise<void>) => void,
    readonly immortal = false,
  ) {
    this.#source = source;
    this.empty = source.added.length === 0 && (source.addedInventory?.count ?? 0) === 0;
    if (immortal && !this.empty) throw new TypeError("Only the shared empty source is immortal");
  }
  retain(): void {
    if (this.immortal) return;
    if (this.#source === undefined) throw new TypeError("Closed staged extent source");
    this.#refs++;
  }
  release(): void {
    if (this.immortal) return;
    if (this.#refs === 0) return;
    if (--this.#refs === 0) this.close();
  }
  close(): void {
    if (this.immortal) return;
    this.#refs = 0;
    const source = this.#source;
    this.#source = undefined;
    if (source !== undefined) this.track(source.release());
  }
  references(budget: ByteBudget, work?: TaskWork) {
    if (this.#source === undefined) throw new TypeError("Closed staged extent source");
    return capturedWriterReferences(this.#source, budget, work);
  }
}

export const EMPTY_STAGED_EXTENTS = new StagedExtents(
  { added: [], release: async () => undefined },
  () => undefined,
  true,
);
