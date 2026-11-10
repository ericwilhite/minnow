/** Read leases use admitted step credit, then move to the consuming operator before it runs. */
import type { ByteBudget, ReadLease, SpaceReader } from "../access/space.js";
import { ReadDeniedError } from "../access/space.js";
import { MAX_READ_BYTES } from "../access/space.js";
import { ReadStepBoundaryError } from "../memory/governor.js";

export const READ_STEP_BYTES = MAX_READ_BYTES;
function dropLease(lease: ReadLease | null): void {
  lease?.release();
}

/** One lease's allocation can change accounts without changing its immutable byte views. */
class LeaseBudget implements ByteBudget {
  allocated = 0;
  constructor(private owner: ByteBudget) {}
  allocate(bytes: number): boolean {
    if (!this.owner.allocate(bytes)) return false;
    this.allocated += bytes;
    return true;
  }
  free(bytes: number): void {
    this.owner.free(bytes);
    this.allocated -= bytes;
  }
  move(owner: ByteBudget): boolean {
    if (!owner.allocate(this.allocated)) return false;
    this.owner.free(this.allocated);
    this.owner = owner;
    return true;
  }
}

/**
 * `step` is an isolated share of the execution's atomic minimum, not a progress loan.
 * `operator` must include its complete resident page set in that same admission. Transfer
 * allocates the destination before freeing the source, so overlap remains charged. Neither
 * account waits/grows to obtain these bytes. Growth of spillable operator state stays outside
 * this protocol and must spill rather than wait on another execution's pinned minimum.
 */
export class ReadStep {
  #pending = 0;
  #requests = 0;
  #pinned = 0;
  #tail: Promise<void> | null = null;
  constructor(
    readonly step: ByteBudget,
    readonly signal: AbortSignal,
  ) {}

  get pinnedBytes(): number {
    return this.#pinned;
  }

  get closed(): Promise<void> {
    return this.#tail ?? Promise.resolve();
  }
  get busy(): boolean {
    return this.#tail !== null;
  }

  beforeDownstream(): void {
    this.signal.throwIfAborted();
    if (this.#requests !== 0 || this.#pending !== 0 || this.#pinned !== 0)
      throw new ReadStepBoundaryError();
  }

  reader(pages: SpaceReader, nonRetaining = false): SpaceReader {
    return {
      peek: (ref, options) => pages.peek(ref, nonRetaining ? { ...options, once: true } : options),
      parsed: (ref) => pages.parsed?.(ref) ?? null,
      attach: (ref, parsed, bytes, of) => pages.attach?.(ref, parsed, bytes, of) ?? false,
      read: (refs, options): Promise<ReadLease> => {
        this.#requests++;
        const signal =
          options.signal === undefined
            ? this.signal
            : AbortSignal.any([this.signal, options.signal]);
        const previous = this.#tail;
        const work =
          previous === null
            ? this.#read(pages, refs, options, nonRetaining)
            : previous.then(() => this.#read(pages, refs, options, nonRetaining));
        const tail = work.then(
          () => undefined,
          () => undefined,
        );
        this.#tail = tail;
        void tail.then(() => {
          if (this.#tail === tail) this.#tail = null;
        });
        // Cancellation wakes the caller immediately, but the private lane remains occupied
        // until the underlying I/O drains. A late lease is returned before that lane is reused.
        return new Promise<ReadLease>((resolve, reject) => {
          const cancel = () =>
            reject(
              signal.reason instanceof Error
                ? signal.reason
                : new Error("Read cancelled", { cause: signal.reason }),
            );
          if (signal.aborted) cancel();
          else signal.addEventListener("abort", cancel, { once: true });
          const done = () => {
            this.#requests--;
            signal.removeEventListener("abort", cancel);
          };
          void work.then(
            (lease) => {
              done();
              if (signal.aborted) lease.release();
              else resolve(lease);
            },
            (error: unknown) => {
              done();
              reject(error instanceof Error ? error : new Error("Read failed", { cause: error }));
            },
          );
        });
      },
    };
  }

  async #read(
    pages: SpaceReader,
    refs: Parameters<SpaceReader["read"]>[0],
    options: Parameters<SpaceReader["read"]>[1],
    nonRetaining: boolean,
  ): Promise<ReadLease> {
    this.signal.throwIfAborted();
    options.signal?.throwIfAborted();
    const budget = new LeaseBudget({
      allocate: (bytes) => {
        if (!this.step.allocate(bytes)) return false;
        this.#pinned += bytes;
        return true;
      },
      free: (bytes) => {
        this.step.free(bytes);
        this.#pinned -= bytes;
      },
    });
    this.#pending++;
    let lease: ReadLease | null = null;
    try {
      const signal =
        options.signal === undefined ? this.signal : AbortSignal.any([this.signal, options.signal]);
      try {
        lease = await pages.read(refs, {
          ...options,
          ...(nonRetaining && options.once !== true ? { retain: false } : {}),
          budget,
          signal,
          reuseBuffers: false,
        });
      } catch (error) {
        // Optional one-pass gaps/pool rounding must not expand the admitted step. The
        // ordinary verified path charges exactly the references and uses its fixed slabs.
        if (!(error instanceof ReadDeniedError) || options.once !== true) throw error;
        lease = await pages.read(refs, {
          ...options,
          budget,
          signal,
          once: false,
          ...(nonRetaining ? { retain: false } : {}),
          reuseBuffers: false,
        });
      }
      signal.throwIfAborted();
      // The caller owns the decoded input and its resident readers. Retained leases are
      // charged there before a downstream operator can request any memory.
      if (!budget.move(options.budget)) throw new ReadStepBoundaryError();
      return lease;
    } catch (error) {
      dropLease(lease);
      throw error;
    } finally {
      this.#pending--;
    }
  }
}
