/** An internal checkpoint request, never a storage/memory-size error returned to SQL. */
import type { JsonMemory } from "./kernels/json.js";
import type { SlicedJob } from "./slices.js";
export class JsonWorkspaceGrowth extends Error {
  constructor(readonly required: number) {
    super("JSON row requires a new complete admission");
  }
}
/** All variable nodes of one row share this credit; mutually exclusive branches do not
 * reserve independent maximum values. Checkpoint ownership retires the row before waiting. */
export class JsonWorkspace implements JsonMemory {
  #used = 0;
  constructor(readonly capacity: number) {}
  // eslint-disable-next-line require-yield -- cannot wait while holding a producer borrow
  *take(bytes: number): SlicedJob<void> {
    this.takeNow(bytes);
  }
  takeNow(bytes: number): void {
    const required = this.#used + bytes;
    if (required > this.capacity) throw new JsonWorkspaceGrowth(required);
    this.#used = required;
  }
  free(bytes: number): void {
    if (bytes < 0 || bytes > this.#used) throw new Error("JSON workspace underflow");
    this.#used -= bytes;
  }
}
