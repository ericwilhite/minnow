/**
 * The memory governor: one cap for the whole engine owner, handed out as grants.
 *
 * ## Rules
 * 1. **One cap.** `capBytes` covers the page cache, operator state, batch and I/O buffers,
 *    dictionaries, sketches, Bloom filters, result frames in flight, and buffers lent to helpers.
 *    Default: a fraction of `navigator.deviceMemory` when reported, otherwise a fixed conservative
 *    value; the single public option overrides it. `usedBytes` never exceeds the cap.
 * 2. **Admission, not refusal.** Every pipeline is admitted with its minimum before it runs
 *    (`admit`). If the minimum does not fit, admission **waits** (FIFO) while the cache is evicted
 *    and spillable grants are asked to give memory back; it never fails for size.
 * 3. **Bounded minimums.** A pipeline's minimum is fixed by its plan, never by data: the batch
 *    buffers of its operators at `batchTargetFor` bytes per batch (fewer rows for wide rows),
 *    hash-table and run headers, and spill I/O buffers. A minimum above `cap − reserve` is a
 *    planner bug (`RangeError`), not a size refusal.
 * 4. **Held vs allocated.** A grant *holds* bytes from the governor (its minimum, growth, loan)
 *    and its holder *allocates* buffers out of them (`allocate`/`free`): a suballocator, so a
 *    non-spillable operator builds buffers inside its admitted minimum without asking the
 *    governor. `allocate` takes free held bytes first and grows (spillable grants only) for the
 *    shortfall; it never waits. A resize allocates the new buffer before freeing the old one, so
 *    growth overlap is counted.
 * 5. **Progress is scheduled apart from admission.** An operator that cannot progress by spilling
 *    (one value larger than a batch, a result frame waiting for credit, a spill buffer it must fill
 *    before it can free anything) `borrow`s a **loan**. Loans are served first-come, before any
 *    admission, and may use the progress reserve (`reserveBytes`), which admissions and growth
 *    never take. What this proves, and no more: a queued admission, growth or the cache can never
 *    delay a loan; a loan waits only for earlier loans to be repaid. So progress is guaranteed if
 *    every loan holder repays without waiting on anything that itself needs memory (the operator
 *    rule: a loan covers one bounded step). A grant has at most one loan and repays it before
 *    borrowing again. Ownership of a loan is installed when it is served, atomically with its
 *    charge: closing the grant at any point returns it. A loan larger than the reserve is the one
 *    size error, `ValueTooLargeError`: about a value, never a query or table.
 * 6. **The cache yields first.** Before refusing growth or keeping anything waiting, the governor
 *    evicts unpinned encoded pages. Operators spill only after the cache is at its floor.
 * 7. **Cooperative reclaim.** While a loan or an admission waits, the governor calls each other
 *    spillable grant's `onReclaimRequest`; the operator spills at its next batch boundary and
 *    `release`s. A callback (or the cache) may release synchronously: the drain is not reentrant.
 * 8. **Lifecycle.** `close` returns everything, rejects the grant's queued loan, and makes every
 *    later call inert (`allocate`/`tryGrow` false, `borrow` rejects `GrantClosedError`). Abort
 *    listeners are removed when a request settles.
 *
 * ## Accounting unit
 * Bytes of ArrayBuffer capacity (`byteLength` allocated, not used), plus `OBJECT_OVERHEAD_BYTES`
 * per JS object kept per group, partition or run, plus `bigintBytes` per BigInt held in operator
 * state (wide decimals). JS strings are not kept in operator state (text stays UTF-8) except on the
 * collation fallback (DECISIONS.md C8), which charges `stringBytes`. Managed memory only; Gate D
 * measures browser RSS.
 */
import { BATCH_TARGET_BYTES } from "../vector/vector.js";

export const OBJECT_OVERHEAD_BYTES = 64;
/** The progress reserve: one eighth of the cap, at least 1 MiB. */
export const reserveFor = (capBytes: number): number => Math.max(1 << 20, Math.floor(capBytes / 8));

/** Bytes a BigInt occupies in V8 (header plus 64-bit digits). */
export function bigintBytes(value: bigint): number {
  const magnitude = value < 0n ? -value : value;
  return 16 + 8 * Math.max(1, Math.ceil(magnitude.toString(16).length / 16));
}

/** Bytes of a JS string kept on the collation fallback (two bytes per code unit plus header). */
export function stringBytes(value: string): number {
  return 24 + 2 * value.length;
}

/** The batch byte target for a pipeline of `operators` operators under `capBytes` (rule 3). */
export function batchTargetFor(capBytes: number, operators: number): number {
  return Math.max(
    4096,
    Math.min(BATCH_TARGET_BYTES, Math.floor(capBytes / (8 * Math.max(1, operators)))),
  );
}

export interface AdmitOptions {
  /** Diagnostic label, e.g. "query#12/pipeline#2". */
  readonly label: string;
  /** Bytes the pipeline needs before it starts (rule 3). */
  readonly minimumBytes: number;
  /** Can its operators spill when refused growth? */
  readonly spillable: boolean;
  /** Cancels a pending admission (rejects with the signal's reason; nothing is held). */
  readonly signal?: AbortSignal;
}

/**
 * A progress loan (rule 5). `repay` returns its bytes; `free` them first: if they are still
 * allocated, `repay` throws and changes nothing, so it can be retried.
 */
export interface Loan {
  readonly bytes: number;
  repay(): void;
}

export interface Grant {
  readonly label: string;
  readonly spillable: boolean;
  /** Bytes held from the governor: minimum + growth + loan. */
  readonly held: number;
  /** Bytes allocated by the holder's buffers (≤ `held`). */
  readonly allocated: number;
  /** Bytes of the outstanding loan (0 when none): they leave only through `repay`. */
  readonly loaned: number;
  /** Rule 4. Never waits. */
  allocate(bytes: number): boolean;
  free(bytes: number): void;
  /** Adds held bytes (spillable grants only). Never waits. */
  tryGrow(bytes: number): boolean;
  /**
   * Returns held bytes to the governor: only holdings beyond what is allocated and beyond the
   * outstanding loan. Loan-backed bytes leave only through `repay` (else `LoanBackedReleaseError`,
   * and nothing changes).
   */
  release(bytes: number): void;
  /** Rule 5. */
  borrow(bytes: number, signal?: AbortSignal): Promise<Loan>;
  /** Rule 7. */
  onReclaimRequest(callback: (bytes: number) => void): void;
  /** Rule 8. Idempotent. */
  close(): void;
}

/** The page cache's side of the governor (rule 6). */
export interface EvictableCache {
  readonly evictableBytes: number;
  /** Evicts at least `bytes` if possible, releasing them through its handle (possibly at once). */
  evict(bytes: number): number;
}

export interface MemoryGovernor {
  readonly capBytes: number;
  readonly reserveBytes: number;
  readonly usedBytes: number;
  readonly peakBytes: number;
  admit(options: AdmitOptions): Promise<Grant>;
  /**
   * Synchronous admission for the point-read fast path: the grant when nothing is queued and the
   * minimum fits now (after evicting cache), else null; the caller then uses `admit`. It never
   * jumps a queued admission or loan and never asks other grants to reclaim.
   */
  tryAdmit(options: Omit<AdmitOptions, "signal">): Grant | null;
  registerCache(cache: EvictableCache): {
    charge(bytes: number): boolean;
    release(bytes: number): void;
  };
}

export class ValueTooLargeError extends Error {
  readonly code = "MINNOW_VALUE_TOO_LARGE";
  constructor(bytes: number, reserve: number) {
    super(`A ${String(bytes)}-byte value exceeds the memory reserve of ${String(reserve)} bytes`);
    this.name = "ValueTooLargeError";
  }
}

export class LoanBackedReleaseError extends Error {
  readonly code = "MINNOW_LOAN_BACKED_RELEASE";
  constructor(label: string, bytes: number, releasable: number) {
    super(
      `${label} cannot release ${String(bytes)} bytes: only ${String(releasable)} are not allocated or loan-backed (repay the loan instead)`,
    );
    this.name = "LoanBackedReleaseError";
  }
}

export class GrantClosedError extends Error {
  readonly code = "MINNOW_GRANT_CLOSED";
  constructor(label: string) {
    super(`Memory grant ${label} is closed`);
    this.name = "GrantClosedError";
  }
}

function abortReason(signal: AbortSignal | undefined): Error {
  const reason: unknown = signal?.reason;
  return reason instanceof Error ? reason : new Error("Aborted", { cause: reason });
}

interface Request {
  readonly bytes: number;
  /** Null for an admission (a new grant is made). */
  readonly grant: GrantImpl | null;
  readonly label: string;
  readonly spillable: boolean;
  readonly loan: boolean;
  readonly resolve: (grant: GrantImpl) => void;
  readonly reject: (reason: unknown) => void;
  readonly detach: () => void;
}

class GrantImpl implements Grant {
  held = 0;
  allocated = 0;
  loan = 0;
  loanPending = false;
  closed = false;
  reclaim: ((bytes: number) => void) | null = null;
  constructor(
    readonly governor: Governor,
    readonly label: string,
    readonly spillable: boolean,
  ) {}

  allocate(bytes: number): boolean {
    if (this.closed) return false;
    const shortfall = this.allocated + bytes - this.held;
    if (shortfall > 0 && !this.tryGrow(shortfall)) return false;
    this.allocated += bytes;
    return true;
  }

  free(bytes: number): void {
    if (this.closed) return;
    if (bytes > this.allocated) throw new RangeError(`${this.label} freed more than it allocated`);
    this.allocated -= bytes;
  }

  tryGrow(bytes: number): boolean {
    if (this.closed || !this.spillable) return false;
    return this.governor.grow(this, bytes);
  }

  get loaned(): number {
    return this.loan;
  }

  release(bytes: number): void {
    if (this.closed) return;
    const releasable = this.held - this.allocated - this.loan;
    if (bytes > releasable) throw new LoanBackedReleaseError(this.label, bytes, releasable);
    this.held -= bytes;
    this.governor.give(bytes);
  }

  borrow(bytes: number, signal?: AbortSignal): Promise<Loan> {
    if (this.closed) return Promise.reject(new GrantClosedError(this.label));
    if (this.loan > 0 || this.loanPending)
      return Promise.reject(new RangeError(`${this.label} must repay its loan before borrowing`));
    this.loanPending = true;
    return this.governor.lend(this, bytes, signal).then(
      () => {
        // The loan was installed (and charged) when it was served; a close since then returned it.
        if (this.closed) throw new GrantClosedError(this.label);
        let repaid = false;
        return {
          bytes,
          repay: () => {
            if (repaid || this.closed) return;
            if (bytes > this.held - this.allocated)
              throw new RangeError(`${this.label} must free its loaned bytes before repaying`);
            repaid = true;
            this.loan = 0;
            this.governor.repaid(bytes);
            this.release(bytes);
          },
        };
      },
      (error: unknown) => {
        this.loanPending = false;
        throw error;
      },
    );
  }

  onReclaimRequest(callback: (bytes: number) => void): void {
    this.reclaim = callback;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.governor.forget(this);
    const held = this.held;
    this.held = 0;
    this.allocated = 0;
    if (this.loan > 0) this.governor.repaid(this.loan);
    this.loan = 0;
    this.governor.give(held);
  }
}

/** The reference governor: rules 1–8. */
export class Governor implements MemoryGovernor {
  readonly capBytes: number;
  readonly reserveBytes: number;
  usedBytes = 0;
  peakBytes = 0;
  #cache: EvictableCache | null = null;
  readonly #grants = new Set<GrantImpl>();
  readonly #loans: Request[] = [];
  readonly #admissions: Request[] = [];
  #draining = false;
  #again = false;
  /** Bytes out on loans: they count against the cap, never against the admission limit. */
  #loaned = 0;

  constructor(capBytes: number) {
    this.capBytes = capBytes;
    this.reserveBytes = reserveFor(capBytes);
  }

  #take(bytes: number): void {
    this.usedBytes += bytes;
    this.peakBytes = Math.max(this.peakBytes, this.usedBytes);
  }

  /**
   * Bytes missing for a request: loans may use the whole cap; admissions and growth must also fit
   * `cap − reserve` counting everything except loans.
   */
  #missing(bytes: number, loan: boolean): number {
    const total = this.usedBytes + bytes - this.capBytes;
    if (loan) return total;
    return Math.max(
      total,
      this.usedBytes - this.#loaned + bytes - (this.capBytes - this.reserveBytes),
    );
  }

  /** Room for a request, evicting the cache first (rule 6). */
  #room(bytes: number, loan: boolean): boolean {
    const missing = this.#missing(bytes, loan);
    if (missing <= 0) return true;
    if (this.#cache !== null && this.#cache.evictableBytes > 0) this.#cache.evict(missing);
    return this.#missing(bytes, loan) <= 0;
  }

  repaid(bytes: number): void {
    this.#loaned -= bytes;
  }

  /** Bytes out on loans (for invariant checks). */
  get loanedBytes(): number {
    return this.#loaned;
  }

  grow(grant: GrantImpl, bytes: number): boolean {
    if (this.#loans.length > 0 || this.#admissions.length > 0) return false;
    if (!this.#room(bytes, false)) return false;
    this.#take(bytes);
    grant.held += bytes;
    return true;
  }

  give(bytes: number): void {
    this.usedBytes -= bytes;
    this.#drain();
  }

  forget(grant: GrantImpl): void {
    this.#grants.delete(grant);
    for (const queue of [this.#loans, this.#admissions])
      for (const request of [...queue])
        if (request.grant === grant) {
          queue.splice(queue.indexOf(request), 1);
          request.detach();
          request.reject(new GrantClosedError(grant.label));
        }
  }

  /** Whether a release reentered the drain while it ran (then it runs again). */
  #releasedDuringDrain(): boolean {
    return this.#again;
  }

  /** Serves loans first (they may use the reserve), then admissions. Never reentrant. */
  #drain(): void {
    if (this.#draining) {
      this.#again = true;
      return;
    }
    this.#draining = true;
    try {
      do {
        this.#again = false;
        this.#serve(this.#loans, true);
        if (this.#loans.length === 0) this.#serve(this.#admissions, false);
      } while (this.#releasedDuringDrain());
    } finally {
      this.#draining = false;
    }
  }

  #serve(queue: Request[], loan: boolean): void {
    for (let request = queue[0]; request !== undefined; request = queue[0]) {
      if (!this.#room(request.bytes, loan)) {
        for (const grant of [...this.#grants])
          if (grant.spillable && grant !== request.grant) grant.reclaim?.(request.bytes);
        if (!this.#room(request.bytes, loan)) return;
      }
      // A callback may have cancelled or served the head while we asked for room.
      if (queue[0] !== request) continue;
      queue.shift();
      request.detach();
      this.#take(request.bytes);
      if (loan) this.#loaned += request.bytes;
      let grant = request.grant;
      if (grant === null) {
        grant = new GrantImpl(this, request.label, request.spillable);
        this.#grants.add(grant);
      }
      grant.held += request.bytes;
      if (request.loan) {
        grant.loan = request.bytes;
        grant.loanPending = false;
      }
      request.resolve(grant);
    }
  }

  #enqueue(
    queue: Request[],
    request: Omit<Request, "resolve" | "reject" | "detach">,
    signal?: AbortSignal,
  ): Promise<GrantImpl> {
    if (signal?.aborted === true) return Promise.reject(abortReason(signal));
    return new Promise((resolve, reject) => {
      const cancel = () => {
        const index = queue.indexOf(entry);
        if (index >= 0) queue.splice(index, 1);
        entry.detach();
        reject(abortReason(signal));
        this.#drain();
      };
      const entry: Request = {
        ...request,
        resolve,
        reject,
        detach: () => {
          signal?.removeEventListener("abort", cancel);
        },
      };
      signal?.addEventListener("abort", cancel, { once: true });
      queue.push(entry);
      this.#drain();
    });
  }

  async admit(options: AdmitOptions): Promise<Grant> {
    if (options.minimumBytes > this.capBytes - this.reserveBytes)
      throw new RangeError(`${options.label} needs a minimum above the cap: plan smaller batches`);
    return this.#enqueue(
      this.#admissions,
      {
        bytes: options.minimumBytes,
        loan: false,
        grant: null,
        label: options.label,
        spillable: options.spillable,
      },
      options.signal,
    );
  }

  tryAdmit(options: Omit<AdmitOptions, "signal">): Grant | null {
    if (options.minimumBytes > this.capBytes - this.reserveBytes)
      throw new RangeError(`${options.label} needs a minimum above the cap: plan smaller batches`);
    if (this.#draining || this.#loans.length > 0 || this.#admissions.length > 0) return null;
    if (!this.#room(options.minimumBytes, false)) return null;
    this.#take(options.minimumBytes);
    const grant = new GrantImpl(this, options.label, options.spillable);
    this.#grants.add(grant);
    grant.held += options.minimumBytes;
    return grant;
  }

  async lend(grant: GrantImpl, bytes: number, signal?: AbortSignal): Promise<void> {
    if (bytes > this.reserveBytes) throw new ValueTooLargeError(bytes, this.reserveBytes);
    await this.#enqueue(
      this.#loans,
      { bytes, loan: true, grant, label: grant.label, spillable: grant.spillable },
      signal,
    );
  }

  registerCache(cache: EvictableCache) {
    this.#cache = cache;
    return {
      charge: (bytes: number): boolean => {
        if (this.#loans.length > 0 || this.#admissions.length > 0) return false;
        if (this.#missing(bytes, false) > 0) return false;
        this.#take(bytes);
        return true;
      },
      release: (bytes: number): void => {
        this.give(bytes);
      },
    };
  }
}
