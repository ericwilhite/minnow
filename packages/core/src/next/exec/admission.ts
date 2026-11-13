/** Fixed plan buffers are reserved atomically before any breaker can grow data-dependent state. */
import {
  GrantClosedError,
  type Grant,
  type Loan,
  type MemoryGovernor,
} from "../memory/governor.js";
import type { StepLoans } from "../memory/step-loans.js";

/**
 * A share of the plan's fixed reservation plus an ordinary, independent spillable grant for
 * growth and progress loans. Unstarted pipelines keep their share: a full hash build cannot
 * consume the later probe's minimum and then wait for its admission. Each loan still belongs
 * to its own governor grant; two operators never accidentally share the one-loan slot.
 * The reservation holds bytes; only actual buffers allocate them (governor rule 4).
 */
export class ReservedGrant implements Grant {
  readonly spillable = true;
  #credit: number;
  #used = 0;
  #closed = false;
  #closing = false;
  #closeHolds = 0;
  #closeRequested = false;
  #loan: Loan | null = null;
  #loanExtra = 0;
  #reclaim: ((bytes: number) => unknown) | null = null;
  constructor(
    readonly fixed: Grant,
    readonly extra: Grant,
    minimum: number,
    readonly progress: StepLoans | null = null,
    readonly bounded = false,
    readonly onClosed?: () => void,
    readonly closeFixed = false,
  ) {
    this.#credit = minimum;
  }
  get label(): string {
    return this.extra.label;
  }
  get held(): number {
    return this.#closed ? 0 : this.#credit + this.extra.held + (this.progress?.capacity ?? 0);
  }
  get allocated(): number {
    return this.#used + this.extra.allocated + (this.progress?.budget.allocated ?? 0);
  }
  get loaned(): number {
    return this.extra.loaned + (this.#loan?.bytes ?? 0);
  }
  allocate(bytes: number): boolean {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("nonnegative byte count required");
    if (this.#closed || this.#closing) return false;
    const base = Math.min(bytes, this.#credit - this.#used);
    const loan = Math.min(
      bytes - base,
      this.#loan === null ? 0 : this.#loanExtra - (this.progress?.budget.allocated ?? 0),
    );
    if (bytes > base + loan && (this.bounded || !this.extra.allocate(bytes - base - loan)))
      return false;
    if (base > 0 && !this.fixed.allocate(base))
      throw new Error("reserved fixed buffers were not held");
    this.#used += base;
    if (loan > 0 && !this.progress?.budget.allocate(loan))
      throw new Error("admitted step loan exhausted");
    return true;
  }
  free(bytes: number): void {
    if (this.#closed) return;
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.allocated)
      throw new RangeError("free exceeds reserved grant allocation");
    const loan = Math.min(bytes, this.progress?.budget.allocated ?? 0);
    if (loan > 0) this.progress?.budget.free(loan);
    const extra = Math.min(bytes - loan, this.extra.allocated);
    if (extra > 0) this.extra.free(extra);
    const base = bytes - loan - extra;
    if (base > 0) {
      this.fixed.free(base);
      this.#used -= base;
    }
    if (this.#closing && this.allocated === 0) this.close();
  }
  tryGrow(bytes: number): boolean {
    return !this.#closed && !this.#closing && !this.bounded && this.extra.tryGrow(bytes);
  }
  async makeRoom(bytes: number, signal?: AbortSignal): Promise<boolean> {
    if (this.#closed || this.#closing) return false;
    if (this.held - this.allocated >= bytes) return true;
    // Ask for room in the independent account. A concurrent reclaim can surrender unused
    // fixed credit while this waits; extra room remains owned by this reader either way.
    return (await this.extra.makeRoom?.(bytes, signal)) ?? false;
  }
  reclaimRoom(bytes: number, signal?: AbortSignal): Promise<boolean> {
    if (this.#closed || this.#closing || this.bounded) return Promise.resolve(false);
    const needed = Math.max(0, bytes - (this.#credit - this.#used));
    return this.extra.reclaimRoom?.(needed, signal) ?? Promise.resolve(false);
  }
  release(bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("nonnegative byte count required");
    if (this.#closed) return;
    if (this.progress !== null) {
      // The execution's whole minimum is immutable until this operator closes. Growth can
      // spill/release, but another execution cannot take its next step's fixed buffers.
      const returned = Math.min(bytes, this.extra.held - this.extra.allocated - this.extra.loaned);
      if (returned > 0) this.extra.release(returned);
      return;
    }
    const base = Math.min(bytes, this.#credit - this.#used);
    if (bytes > base) this.extra.release(bytes - base);
    if (base > 0) {
      this.#credit -= base;
      this.fixed.release(base);
    }
  }
  borrow(bytes: number, signal?: AbortSignal): Promise<Loan> {
    if (this.progress !== null) {
      if (this.#closed) return Promise.reject(new GrantClosedError(this.label));
      // The existing unused minimum already covers part of this bounded step. Only its
      // shortfall needs the separately admitted progress lane.
      const additional = Math.max(0, bytes - (this.#credit - this.#used));
      return this.progress.borrow(additional, signal).then((loan) => {
        if (this.#closed || signal?.aborted) {
          loan.repay();
          signal?.throwIfAborted();
          throw new GrantClosedError(this.label);
        }
        // A local loan can be backed entirely by unused fixed credit. It still owns
        // the one-loan slot and must be visible to callers' repay guards.
        const tracked: Loan = {
          bytes,
          repay: () => {
            loan.repay();
            if (this.#loan === tracked) {
              this.#loan = null;
              this.#loanExtra = 0;
            }
          },
        };
        this.#loanExtra = additional;
        this.#loan = tracked;
        return tracked;
      });
    }
    return this.extra.borrow(bytes, signal);
  }
  onReclaimRequest(callback: (bytes: number) => unknown): void {
    this.#reclaim = callback;
    this.extra.onReclaimRequest(callback);
  }
  reclaim(bytes: number): unknown {
    if (!this.#closed) return this.#reclaim?.(bytes);
  }
  /** Retire a dedicated, quiescent phase account into the owner's idle pool.
   * Native transfers never take a live buffer or loan; the wrapper's ledger is
   * reduced before callbacks can run from a donation waking FIFO admission.
   */
  parkIdle(governor: MemoryGovernor, pool: Grant, progressGrant: Grant | null): void {
    if (
      this.#closed ||
      this.#closing ||
      !this.closeFixed ||
      this.allocated !== 0 ||
      this.loaned !== 0
    )
      throw new TypeError("Phase credit requires an open, dedicated, idle reservation");
    if (pool === this.fixed || pool === this.extra || pool === progressGrant)
      throw new TypeError("A phase must park into a separate shared idle account");
    if ((this.progress?.budget.owner ?? null) !== progressGrant)
      throw new TypeError("Wrong native progress account");
    this.#closing = true;
    const credit = this.#credit;
    this.#credit = 0;
    if (!governor.transferCredit(this.fixed, pool, credit)) {
      this.#credit = credit;
      this.#closing = false;
      throw new TypeError("Phase fixed credit is not native idle credit");
    }
    for (const native of [this.extra, progressGrant]) {
      if (native && !governor.transferCredit(native, pool, native.held))
        throw new TypeError("Phase progress/growth credit is not idle");
    }
    this.close();
  }
  /** Compiler buffers share this account with a native breaker. Keep their existing
   * charge alive until the recipe's last expression borrow ends; no new credit or loan. */
  holdClose(): () => void {
    if (this.#closed) throw new GrantClosedError(this.label);
    this.#closeHolds++;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.#closeHolds--;
      if (this.#closeHolds === 0 && this.#closeRequested) {
        this.#closeRequested = false;
        this.close();
      }
    };
  }
  close(): void {
    if (this.#closed) return;
    if (this.#closeHolds > 0) {
      this.#closeRequested = true;
      return;
    }
    if (this.bounded && this.allocated > 0) {
      // Dedicated terminal frames can retain dictionary readers after the producer ends.
      // Their source minimum remains charged until the last transfer releases its leases.
      this.#closing = true;
      return;
    }
    this.#closed = true;
    this.#reclaim = null;
    this.progress?.close();
    this.#loan = null;
    this.#loanExtra = 0;
    this.extra.close();
    if (this.#used > 0) this.fixed.free(this.#used);
    const returned = this.#credit + (this.closeFixed ? 0 : (this.progress?.capacity ?? 0));
    if (returned > 0) this.fixed.release(returned);
    if (this.closeFixed) this.fixed.close();
    this.#used = 0;
    this.#credit = 0;
    this.onClosed?.();
  }
}
