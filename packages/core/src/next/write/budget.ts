/**
 * Write-path reservation sharing (governor.ts rules 4, 5 and 7).
 * `free` returns allocations to a grant, not to the governor. A wide batch's conservative
 * estimate must not leave its unused high-water holdings blocking merges or resident metadata.
 */
import { yieldTask } from "../exec/pipeline.js";
import { checkRetries, macrotaskEpoch, RETRY_GUARD_LIMIT } from "../memory/retry-guard.js";
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
  readonly sqlState = "53200";
  constructor(
    readonly bytes: number,
    readonly cap: number,
  ) {
    super(`Metadata allocation ${String(bytes)} exceeds the whole cap ${String(cap)}`);
    this.name = "MetadataAllocationTooLargeError";
  }
}

/** Only the owning async operation handles pressure, after unwinding its provisional state. */
export class MetadataPressure extends ParseBudgetError {
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
    readonly changed?: () => void,
  ) {}

  readonly #funded: number[] = [];

  /** Metadata credit a refused attempt was funded for and has not yet settled: reclaim keeps
   * it (the writer's working minimum includes it). */
  get fundedBytes(): number {
    let most = 0;
    for (const bytes of this.#funded) most = Math.max(most, bytes);
    return most;
  }

  ordinarySource(): ByteBudget {
    return this.grant;
  }

  preflightAllocation(bytes: number, optionalBytes = 0): void {
    const wanted = this.grant.allocated + bytes;
    // Other snapshots can still own resident metadata. Only this complete working
    // minimum is an intrinsic size refusal; its overlap with them is transient.
    if (bytes > this.capBytes) throw new MetadataAllocationTooLargeError(bytes, this.capBytes);
    if (this.grant.held < wanted && !this.grant.tryGrow(wanted - this.grant.held)) {
      this.reclaimWriter();
      if (this.grant.held < wanted && !this.grant.tryGrow(wanted - this.grant.held))
        throw new MetadataPressure(bytes, wanted);
    }
    // Optional layouts may take currently free credit before construction. They
    // never reclaim, wait, or turn their spare allowance into a required minimum.
    const optional = wanted + optionalBytes;
    if (optionalBytes > 0 && optional <= this.capBytes && optional > this.grant.held)
      this.grant.tryGrow(optional - this.grant.held);
  }

  tryAllocate(bytes: number, reserve = 0): boolean {
    // Optional layouts use prepaid surplus only. They must not grow/reclaim an
    // owner merely to beat their bounded plain/disk fallback, or steal the
    // mandatory buffers that the rest of this same step has not allocated yet.
    const wanted = this.grant.allocated + bytes + reserve;
    const accepted =
      wanted <= this.capBytes && wanted <= this.grant.held && this.grant.allocate(bytes);
    if (accepted && bytes) this.changed?.();
    return accepted;
  }

  allocate(bytes: number): boolean {
    if (bytes > this.capBytes) throw new MetadataAllocationTooLargeError(bytes, this.capBytes);
    if (this.grant.allocate(bytes)) {
      if (bytes) this.changed?.();
      return true;
    }
    if (this.reclaimWriter() > 0 && this.grant.allocate(bytes)) {
      if (bytes) this.changed?.();
      return true;
    }
    throw new MetadataPressure(bytes, this.grant.allocated + bytes);
  }

  /**
   * `work` must unwind provisional allocations/writers/locks on failure. Cached fast paths stay
   * synchronous. A retry waits on real tasks for the refused attempt's high-water headroom;
   * it does not repeatedly encode/abort while a reader still owns that memory. The budget takes
   * no loan. Publication owners place their retry outside their provisional writer and lock.
   */
  retry<T>(
    work: () => MaybePromise<T>,
    check: () => void,
    grow?: (wanted: number) => Promise<void>,
    funded = 0,
    rounds = 0,
    reached = 0,
    epoch = -1,
  ): MaybePromise<T> {
    const refused = (error: unknown): MaybePromise<T> => {
      if (!(error instanceof MetadataPressure)) throw error;
      // An earlier attempt of this work was funded to `funded`, and the grant fell below it
      // without this refusal's allocations: the attempt itself moved credit out (a page read's
      // charge handed to the cache). Fund that again on top, or every retry falls short by the
      // same bytes and grows by just that much again.
      const lost = Math.max(0, funded - this.grant.held);
      const target = error.wanted + lost;
      return this.#wait(target, check, grow).then(() => {
        check();
        // Progress: the credit this work's funding has reached (a grant above every earlier
        // retry's). Only retries funded no further count toward the retry guard; asking for more
        // is not progress when nothing more arrives.
        // A retry after a macrotask (the wait ran a timer or I/O) is not a microtask spin.
        const held = this.grant.held;
        const now = macrotaskEpoch();
        const next = held > reached || now !== epoch ? 0 : rounds + 1;
        if (RETRY_GUARD_LIMIT !== null) checkRetries(`metadata ${this.grant.label}`, next);
        // The credit was grown for this attempt: reclaim keeps it until the attempt settles.
        // An attempt awaits (a lock, a writer) before it allocates; without this a queued
        // request's reclaim could take it back in between, and grow and retry would repeat.
        this.#funded.push(target);
        const settle = () => {
          const at = this.#funded.indexOf(target);
          if (at >= 0) this.#funded.splice(at, 1);
        };
        let result: MaybePromise<T>;
        try {
          result = this.retry(work, check, grow, target, next, Math.max(reached, held), now);
        } catch (error) {
          settle();
          throw error;
        }
        if (result instanceof Promise) return result.finally(settle);
        settle();
        return result;
      });
    };
    try {
      const result = work();
      return result instanceof Promise ? result.catch(refused) : result;
    } catch (error) {
      return refused(error);
    }
  }

  async #wait(
    wanted: number,
    check: () => void,
    override?: (wanted: number) => Promise<void>,
  ): Promise<void> {
    const grow = override ?? this.grow;
    if (grow !== undefined) {
      check();
      await grow(wanted);
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
    if (bytes) this.changed?.();
  }
}
