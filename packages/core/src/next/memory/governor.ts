/**
 * One owner-wide memory cap. See memory/README.md for the bounded read-step contract/proof.
 *
 * Rules:
 * 1. Held bytes include cache, leases, operator/input/result buffers and resize overlap.
 * 2. Complete executions atomically admit their whole minimum FIFO, holding nothing while
 *    waiting. `includesProgress` includes their private I/O and per-operator progress credit.
 * 3. Only a complete minimum above the whole cap is a typed admission-size refusal. An
 *    incomplete legacy operator minimum leaves the shared reserve for its isolated step loans.
 * 4. Held and allocated differ: allocate/free suballocate an existing grant. Allocation and
 *    growth never wait; spillable operators spill or continue at their admitted minimum.
 * 5. Compiled executions use private StepLoans and never wait for another execution's pinned
 *    or minimum memory. Legacy isolated loans use the reserve and must repay without asking
 *    for another memory resource. A grant has at most one outstanding legacy loan.
 * 6. Evict unused caches before requesting spill; choose the oldest unused entry across caches.
 * 7. Cooperative spill callbacks are serialized per holder. A fresh compatibility read can
 *    wait/reclaim, with cancellation. Retained inputs must use the bounded read-step contract,
 *    rather than enter a hold-and-wait cycle through makeRoom.
 * 8. Close returns every holding, rejects queued requests, and leaves later calls inert. Abort
 *    listeners are removed when requests settle. The drain is not reentrant.
 *    Cache takeFrom transfers ordinary allocated/held credit atomically; loan credit cannot transfer.
 *
 * Accounting: ArrayBuffer capacity plus OBJECT_OVERHEAD_BYTES per retained object and
 * bigintBytes per retained BigInt; collation-fallback strings use stringBytes. Managed memory
 * only; browser RSS is measured separately at Gate D.
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
   * Compatibility admission for a read: evict cache, request reclaim, then wait for temporary
   * holders with cancellation. A fresh account may admit a complete step from the whole cap.
   * Retained input must use ReadStep, rather than wait here for another pinned minimum.
   * Does not allocate buffers; false only for a closed/synthetic account.
   */
  makeRoom?(bytes: number, signal?: AbortSignal): Promise<boolean>;
  /**
   * Returns held bytes to the governor: only holdings beyond what is allocated and beyond the
   * outstanding loan. Loan-backed bytes leave only through `repay` (else `LoanBackedReleaseError`,
   * and nothing changes).
   */
  release(bytes: number): void;
  /** Rule 5. */
  borrow(bytes: number, signal?: AbortSignal): Promise<Loan>;
  /** Rule 7. */
  onReclaimRequest(callback: (bytes: number) => unknown): void;
  /** Rule 8. Idempotent. */
  close(): void;
}

/** A cache's side of the governor (rule 6). */
export interface EvictableCache {
  readonly evictableBytes: number;
  /** Evicts at least `bytes` if possible, releasing them through its handle (possibly at once). */
  evict(bytes: number): number;
  /**
   * When the entry `evict` would take next was last used (`performance.now()` milliseconds), so
   * the governor can evict across caches oldest first. Absent (or undefined): the cache is asked
   * before any cache that reports a time.
   */
  readonly oldestUnusedAt?: number | undefined;
}

export interface MemoryGovernor {
  readonly capBytes: number;
  readonly reserveBytes: number;
  readonly usedBytes: number;
  readonly peakBytes: number;
  admit(options: AdmitOptions): Promise<Grant>;
  /** One FIFO admission, then an atomic split. A zero-total group needs no shared capacity. */
  admitGroup(options: NativeGrantGroupOptions): Promise<readonly Grant[]>;
  /** Moves idle ordinary credit within this owner. No allocation or global admission occurs. */
  transferCredit(source: Grant, target: Grant, bytes: number): boolean;
  /**
   * Synchronous admission for the point-read fast path: the grant when nothing is queued and the
   * minimum fits now (after evicting cache), else null; the caller then uses `admit`. It never
   * jumps a queued admission or loan and never asks other grants to reclaim.
   */
  tryAdmit(options: Omit<AdmitOptions, "signal">): Grant | null;
  registerCache(cache: EvictableCache): {
    charge(bytes: number): boolean;
    release(bytes: number): void;
    /** Transfers existing ordinary allocated bytes from this governor's live grant; no admission. */
    takeFrom?(source: unknown, bytes: number): boolean;
    /**
     * Stops asking this cache to evict (a cache that closes; release its bytes first). Optional:
     * a cache that lives as long as its governor need not call it.
     */
    unregister?(): void;
  };
}

/** Named parts of one complete minimum. Each part remains a normal native grant. */
export interface NativeGrantGroupOptions {
  readonly label: string;
  readonly grants: ReadonlyArray<{
    readonly label: string;
    readonly minimumBytes: number;
    readonly spillable: boolean;
  }>;
  readonly signal?: AbortSignal;
  /** The total includes private progress credit; its consumers never need the shared reserve. */
  readonly includesProgress?: boolean;
}

/** A non-spillable input or a fixed minimum cannot fit the owner's normal capacity. */
export class MemoryAdmissionError extends RangeError {
  readonly code = "MINNOW_MEMORY_ADMISSION";
  constructor(
    readonly label: string,
    readonly bytes: number,
    readonly capacity: number,
  ) {
    super(
      `${label} needs a minimum above the cap: plan smaller batches (${String(bytes)} bytes, ${String(capacity)} available)`,
    );
    this.name = "MemoryAdmissionError";
  }
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

function checkSignal(signal: AbortSignal | undefined): void {
  if (signal?.aborted === true) throw abortReason(signal);
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
  readonly asked: Set<GrantImpl>;
  readonly readRoom: number | null;
  readonly pending: Set<Promise<void>>;
  readonly includesProgress: boolean;
}

class GrantImpl implements Grant {
  held = 0;
  allocated = 0;
  loan = 0;
  loanPending = false;
  closed = false;
  reclaim: ((bytes: number) => unknown) | null = null;
  reclaiming: Promise<void> | null = null;
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

  makeRoom(bytes: number, signal?: AbortSignal): Promise<boolean> {
    return this.governor.makeRoom(this, bytes, signal);
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

  onReclaimRequest(callback: (bytes: number) => unknown): void {
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
  readonly #caches: EvictableCache[] = [];
  readonly #grants = new Set<GrantImpl>();
  readonly #loans: Request[] = [];
  readonly #reads: Request[] = [];
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
   * Bytes missing for a request: loans and complete private-progress groups may use the whole
   * cap; ordinary admissions and growth must also fit `cap − reserve` excluding shared loans.
   */
  #missing(bytes: number, loan: boolean): number {
    const total = this.usedBytes + bytes - this.capBytes;
    if (loan) return total;
    return Math.max(
      total,
      this.usedBytes - this.#loaned + bytes - (this.capBytes - this.reserveBytes),
    );
  }

  /** Room for a request, evicting the caches first (rule 6). */
  #room(bytes: number, loan: boolean): boolean {
    const missing = this.#missing(bytes, loan);
    if (missing <= 0) return true;
    this.#evict(missing);
    return this.#missing(bytes, loan) <= 0;
  }

  /** Evicts up to `bytes` across the registered caches, the entry unused longest first. */
  #evict(bytes: number): void {
    const only = this.#caches.length === 1 ? this.#caches[0] : undefined;
    if (only !== undefined) {
      if (only.evictableBytes > 0) only.evict(bytes);
      return;
    }
    let remaining = bytes;
    // A cache that frees nothing when asked is not asked again in this call.
    const stuck = new Set<EvictableCache>();
    while (remaining > 0) {
      let victim: EvictableCache | null = null;
      let oldest = Number.POSITIVE_INFINITY;
      for (const cache of this.#caches) {
        if (cache.evictableBytes <= 0 || stuck.has(cache)) continue;
        const at = cache.oldestUnusedAt ?? Number.NEGATIVE_INFINITY;
        if (victim === null || at < oldest) {
          victim = cache;
          oldest = at;
        }
      }
      if (victim === null) return;
      // One entry at a time (`evict` takes at least one): the next one may be in another cache.
      const freed = victim.evict(1);
      if (freed <= 0) stuck.add(victim);
      else remaining -= freed;
    }
  }

  repaid(bytes: number): void {
    this.#loaned -= bytes;
  }

  /** Bytes out on loans (for invariant checks). */
  get loanedBytes(): number {
    return this.#loaned;
  }

  grow(grant: GrantImpl, bytes: number): boolean {
    if (this.#loans.length > 0 || this.#reads.length > 0 || this.#admissions.length > 0)
      return false;
    if (!this.#room(bytes, false)) return false;
    this.#take(bytes);
    grant.held += bytes;
    return true;
  }

  /** Reclaim is serialized per holder; two readers must never spill the same store concurrently. */
  #ask(grant: GrantImpl, bytes: number): void | Promise<void> {
    if (grant.reclaiming !== null) return grant.reclaiming;
    const work = grant.reclaim?.(bytes);
    if (!(work instanceof Promise)) return;
    const pending = work.then(
      () => {
        grant.reclaiming = null;
      },
      (error: unknown) => {
        grant.reclaiming = null;
        throw error;
      },
    );
    grant.reclaiming = pending;
    return pending;
  }

  /** Fresh-read admission: cache eviction precedes cooperative spill or a whole-cap step. */
  async makeRoom(grant: GrantImpl, bytes: number, signal?: AbortSignal): Promise<boolean> {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("nonnegative byte count required");
    const check = () => {
      checkSignal(signal);
      if (grant.closed) throw new GrantClosedError(grant.label);
    };
    check();
    if (bytes > this.capBytes) throw new MemoryAdmissionError(grant.label, bytes, this.capBytes);
    const missing = grant.allocated + bytes - grant.held;
    if (missing <= 0) return true;
    if (grant.tryGrow(missing)) return true;
    if (grant.allocated === 0) {
      if (grant.loan > 0 || grant.loanPending) throw new ReadStepBoundaryError();
      // A fresh compatibility read must not hold an unused partial minimum while
      // waiting for the complete step. Two half-cap accounts would otherwise cycle.
      if (grant.held > 0) grant.release(grant.held);
    }
    await this.#enqueue(
      this.#reads,
      {
        bytes: missing,
        loan: false,
        grant,
        label: grant.label,
        spillable: grant.spillable,
        includesProgress: false,
      },
      signal,
      bytes,
    );
    check();
    return true;
  }

  give(bytes: number): void {
    this.usedBytes -= bytes;
    this.#drain();
  }

  forget(grant: GrantImpl): void {
    this.#grants.delete(grant);
    for (const queue of [this.#loans, this.#reads, this.#admissions])
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
        if (this.#loans.length === 0) {
          this.#serve(this.#reads, false);
          if (this.#reads.length === 0) this.#serve(this.#admissions, false);
        }
      } while (this.#releasedDuringDrain());
    } finally {
      this.#draining = false;
    }
  }

  #serve(queue: Request[], loan: boolean): void {
    for (let head = queue[0]; head !== undefined; head = queue[0]) {
      const request = head;
      let wholeMinimum = loan || request.includesProgress;
      // Parallel column reads may share a grant. Re-evaluate its missing room after another
      // request was served, and after callbacks that can surrender unused holdings.
      const bytes = () =>
        request.readRoom === null || request.grant === null
          ? request.bytes
          : Math.max(0, request.grant.allocated + request.readRoom - request.grant.held);
      if (!this.#room(bytes(), wholeMinimum)) {
        for (const grant of [...this.#grants]) {
          if (
            !grant.spillable ||
            (grant === request.grant && request.readRoom === null) ||
            request.asked.has(grant)
          )
            continue;
          if (request.readRoom !== null) request.asked.add(grant);
          try {
            const work = this.#ask(grant, bytes());
            if (work instanceof Promise) {
              request.asked.add(grant);
              request.pending.add(work);
              void work.then(
                () => {
                  request.pending.delete(work);
                  this.#drain();
                },
                (error: unknown) => {
                  request.pending.delete(work);
                  this.#reject(queue, request, error);
                },
              );
              // One read asks one holder at a time; a small lease must not spill every query.
              if (request.readRoom !== null) break;
            }
          } catch (error) {
            this.#reject(queue, request, error);
          }
          if (request.readRoom !== null && this.#room(bytes(), wholeMinimum)) break;
        }
        if (
          request.readRoom !== null &&
          request.pending.size === 0 &&
          request.grant?.allocated === 0 &&
          this.#room(bytes(), true)
        )
          wholeMinimum = true;
        if (!this.#room(bytes(), wholeMinimum)) {
          // A new minimum may wait while holding nothing. A caller retaining allocated
          // input must use its admitted read step or surrender/spill that input first;
          // waiting here would create hold-and-wait on another execution's minimum.
          if (
            request.readRoom !== null &&
            request.pending.size === 0 &&
            (request.grant?.allocated ?? 0) > 0
          ) {
            this.#reject(queue, request, new ReadStepBoundaryError());
            continue;
          }
          return;
        }
      }
      // A callback may have cancelled or served the head while we asked for room.
      if (queue[0] !== request) continue;
      queue.shift();
      request.detach();
      const taken = bytes();
      this.#take(taken);
      if (loan) this.#loaned += taken;
      let grant = request.grant;
      if (grant === null) {
        grant = new GrantImpl(this, request.label, request.spillable);
        this.#grants.add(grant);
      }
      grant.held += taken;
      if (request.loan) {
        grant.loan = taken;
        grant.loanPending = false;
      }
      request.resolve(grant);
    }
  }

  #enqueue(
    queue: Request[],
    request: Omit<
      Request,
      "resolve" | "reject" | "detach" | "asked" | "readRoom" | "pending" | "includesProgress"
    > & { includesProgress?: boolean },
    signal?: AbortSignal,
    readRoom: number | null = null,
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
        includesProgress: request.includesProgress ?? false,
        asked: new Set(),
        pending: new Set(),
        readRoom,
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

  #reject(queue: Request[], request: Request, error: unknown): void {
    const at = queue.indexOf(request);
    if (at < 0) return;
    queue.splice(at, 1);
    request.detach();
    request.reject(error);
    this.#drain();
  }

  async admit(options: AdmitOptions): Promise<Grant> {
    if (options.minimumBytes > this.capBytes - this.reserveBytes)
      throw new MemoryAdmissionError(
        options.label,
        options.minimumBytes,
        this.capBytes - this.reserveBytes,
      );
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

  async admitGroup(options: NativeGrantGroupOptions): Promise<readonly Grant[]> {
    const { label, signal, includesProgress } = options;
    // Copy before waiting: the caller cannot change the named minima after admission queues.
    const parts = options.grants.map(({ label, minimumBytes, spillable }) => ({
      label,
      minimumBytes,
      spillable,
    }));
    let total = 0;
    const labels = new Set<string>();
    for (const part of parts) {
      if (!Number.isSafeInteger(part.minimumBytes) || part.minimumBytes < 0)
        throw new RangeError("A native grant minimum must be a nonnegative safe integer");
      if (labels.has(part.label)) throw new RangeError("Native grant labels must be unique");
      labels.add(part.label);
      total += part.minimumBytes;
      if (!Number.isSafeInteger(total)) throw new RangeError("Native grant total overflow");
    }
    signal?.throwIfAborted();
    if (total === 0) {
      const grants = parts.map((part) => new GrantImpl(this, part.label, part.spillable));
      for (const grant of grants) this.#grants.add(grant);
      return grants;
    }
    if (includesProgress === true && total > this.capBytes)
      throw new MemoryAdmissionError(label, total, this.capBytes);
    const admitted =
      includesProgress === true
        ? await this.#enqueue(
            this.#admissions,
            {
              bytes: total,
              loan: false,
              grant: null,
              label,
              spillable: false,
              includesProgress: true,
            },
            signal,
          )
        : await this.admit({
            label,
            minimumBytes: total,
            spillable: false,
            ...(signal === undefined ? {} : { signal }),
          });
    try {
      signal?.throwIfAborted();
      if (!(admitted instanceof GrantImpl) || admitted.governor !== this || admitted.closed)
        throw new TypeError("A grant group requires this owner's native admission");
      if (admitted.held !== total || admitted.allocated !== 0 || admitted.loaned !== 0)
        throw new TypeError("The group admission was changed before it could split");
      // No call out or suspension while ownership moves. Peak/used stay charged once.
      const grants = parts.map((part) => new GrantImpl(this, part.label, part.spillable));
      for (let index = 0; index < grants.length; index++) {
        const grant = grants[index];
        const part = parts[index];
        if (grant === undefined || part === undefined) throw new Error("Grant group length");
        admitted.held -= part.minimumBytes;
        grant.held = part.minimumBytes;
        this.#grants.add(grant);
      }
      return grants;
    } finally {
      admitted.close();
    }
  }

  transferCredit(source: Grant, target: Grant, bytes: number): boolean {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("Credit transfer bytes must be a nonnegative safe integer");
    if (
      !(source instanceof GrantImpl) ||
      !(target instanceof GrantImpl) ||
      source.governor !== this ||
      target.governor !== this ||
      source.closed ||
      target.closed ||
      (bytes > 0 && bytes > source.held - source.allocated - source.loaned)
    )
      return false;
    if (source !== target) {
      source.held -= bytes;
      target.held += bytes;
    }
    return true;
  }

  tryAdmit(options: Omit<AdmitOptions, "signal">): Grant | null {
    if (options.minimumBytes > this.capBytes - this.reserveBytes)
      throw new MemoryAdmissionError(
        options.label,
        options.minimumBytes,
        this.capBytes - this.reserveBytes,
      );
    if (
      this.#draining ||
      this.#loans.length > 0 ||
      this.#reads.length > 0 ||
      this.#admissions.length > 0
    )
      return null;
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

  /** Registers one more cache (rule 6); every registered cache is evicted before anything waits. */
  registerCache(cache: EvictableCache) {
    if (!this.#caches.includes(cache)) this.#caches.push(cache);
    let active = true;
    return {
      takeFrom: (source: unknown, bytes: number): boolean => {
        if (!Number.isSafeInteger(bytes) || bytes < 0) throw new RangeError("Bad transfer size");
        if (
          !active ||
          !(source instanceof GrantImpl) ||
          source.governor !== this ||
          source.closed ||
          source.allocated < bytes ||
          source.held - source.loan < bytes
        )
          return false;
        // No give/take/drain, eviction or callback between the two ownership changes. Existing
        // ordinary capacity changes owner; it cannot serve a queued admission in between.
        source.allocated -= bytes;
        source.held -= bytes;
        return true;
      },
      charge: (bytes: number): boolean => {
        if (this.#loans.length > 0 || this.#reads.length > 0 || this.#admissions.length > 0)
          return false;
        if (this.#missing(bytes, false) > 0) return false;
        this.#take(bytes);
        return true;
      },
      release: (bytes: number): void => {
        this.give(bytes);
      },
      unregister: (): void => {
        active = false;
        const at = this.#caches.indexOf(cache);
        if (at >= 0) this.#caches.splice(at, 1);
      },
    };
  }
}

/** Internal protocol violation: progress-backed input crossed a downstream memory request. */
export class ReadStepBoundaryError extends Error {
  readonly code = "MINNOW_READ_STEP_BOUNDARY";
  constructor() {
    super("A read-step lease crossed a downstream memory request");
    this.name = "ReadStepBoundaryError";
  }
}
