/**
 * Write-path reservation sharing (governor.ts rules 4, 5 and 7).
 * `free` returns allocations to a grant, not to the governor. A wide batch's conservative
 * estimate must not leave its unused high-water holdings blocking merges or resident metadata.
 */
import { yieldTask } from "../exec/pipeline.js";
import type { MaybePromise } from "../access/key-index.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
import type { Grant } from "../memory/governor.js";
import type { ByteBudget } from "../storage/extents.js";

/** Return only idle, non-loan holdings, retaining the admitted working minimum. */
export function releaseIdle(grant: Grant, minimum = 0): number {
  const bytes = grant.held - Math.max(minimum, grant.allocated + grant.loaned);
  if (bytes <= 0) return 0;
  grant.release(bytes);
  return bytes;
}

/** A single resident allocation cannot fit even if every other holder releases. */
export class MetadataAllocationTooLargeError extends RangeError {
  readonly code = "MINNOW_METADATA_ALLOCATION_TOO_LARGE";
  constructor(
    readonly bytes: number,
    readonly cap: number,
  ) {
    super(`Metadata allocation ${String(bytes)} exceeds the whole cap ${String(cap)}`);
    this.name = "MetadataAllocationTooLargeError";
  }
}

/** Only the owning async operation handles pressure, after unwinding its provisional state. */
class MetadataPressure extends ParseBudgetError {
  constructor(
    bytes: number,
    readonly wanted: number,
  ) {
    super(bytes);
  }
}

/**
 * Resident metadata shares the cap with the writer. Every synchronous allocation uses this
 * budget, including directories and run readers. Under temporary pressure its async owner
 * unwinds and calls `retry`; live buffers and loans are never shed to obtain headroom.
 */
export class MetadataBudget implements ByteBudget {
  constructor(
    readonly grant: Grant,
    readonly reclaimWriter: () => number,
    readonly capBytes: number,
    readonly grow?: (wanted: number) => Promise<void>,
  ) {}

  allocate(bytes: number): boolean {
    if (bytes > this.capBytes) throw new MetadataAllocationTooLargeError(bytes, this.capBytes);
    if (this.grant.allocate(bytes)) return true;
    if (this.reclaimWriter() > 0 && this.grant.allocate(bytes)) return true;
    throw new MetadataPressure(bytes, this.grant.allocated + bytes);
  }

  /**
   * `work` must unwind provisional allocations/writers/locks on failure. Cached fast paths stay
   * synchronous. A retry waits on real tasks for the refused attempt's high-water headroom;
   * it does not repeatedly encode/abort while a reader still owns that memory. The budget takes
   * no loan. Publication owners place their retry outside their provisional writer and lock.
   */
  retry<T>(work: () => MaybePromise<T>, check: () => void): MaybePromise<T> {
    const refused = (error: unknown): MaybePromise<T> => {
      if (!(error instanceof MetadataPressure)) throw error;
      return this.#wait(error.wanted, check).then(() => {
        check();
        return this.retry(work, check);
      });
    };
    try {
      const result = work();
      return result instanceof Promise ? result.catch(refused) : result;
    } catch (error) {
      return refused(error);
    }
  }

  async #wait(wanted: number, check: () => void): Promise<void> {
    if (this.grow !== undefined) {
      check();
      await this.grow(wanted);
      check();
      return;
    }
    for (;;) {
      check();
      if (this.grant.held >= wanted || this.grant.tryGrow(wanted - this.grant.held)) return;
      this.reclaimWriter();
      await yieldTask();
    }
  }

  free(bytes: number): void {
    this.grant.free(bytes);
  }
}
