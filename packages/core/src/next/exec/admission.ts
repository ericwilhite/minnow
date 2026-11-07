/** Fixed plan buffers are reserved atomically before any breaker can grow data-dependent state. */
import type { Grant, Loan } from "../memory/governor.js";

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
  #reclaim: ((bytes: number) => void | Promise<void>) | null = null;
  constructor(
    readonly fixed: Grant,
    readonly extra: Grant,
    minimum: number,
  ) {
    this.#credit = minimum;
  }
  get label(): string {
    return this.extra.label;
  }
  get held(): number {
    return this.#credit + this.extra.held;
  }
  get allocated(): number {
    return this.#used + this.extra.allocated;
  }
  get loaned(): number {
    return this.extra.loaned;
  }
  allocate(bytes: number): boolean {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("nonnegative byte count required");
    if (this.#closed) return false;
    const base = Math.min(bytes, this.#credit - this.#used);
    if (bytes > base && !this.extra.allocate(bytes - base)) return false;
    if (base > 0 && !this.fixed.allocate(base))
      throw new Error("reserved fixed buffers were not held");
    this.#used += base;
    return true;
  }
  free(bytes: number): void {
    if (this.#closed) return;
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.allocated)
      throw new RangeError("free exceeds reserved grant allocation");
    const extra = Math.min(bytes, this.extra.allocated);
    if (extra > 0) this.extra.free(extra);
    const base = bytes - extra;
    if (base > 0) {
      this.fixed.free(base);
      this.#used -= base;
    }
  }
  tryGrow(bytes: number): boolean {
    return !this.#closed && this.extra.tryGrow(bytes);
  }
  async makeRoom(bytes: number, signal?: AbortSignal): Promise<boolean> {
    if (this.#closed) return false;
    if (this.held - this.allocated >= bytes) return true;
    // Ask for room in the independent account. A concurrent reclaim can surrender unused
    // fixed credit while this waits; extra room remains owned by this reader either way.
    return (await this.extra.makeRoom?.(bytes, signal)) ?? false;
  }
  release(bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("nonnegative byte count required");
    if (this.#closed) return;
    const base = Math.min(bytes, this.#credit - this.#used);
    if (bytes > base) this.extra.release(bytes - base);
    if (base > 0) {
      this.#credit -= base;
      this.fixed.release(base);
    }
  }
  borrow(bytes: number, signal?: AbortSignal): Promise<Loan> {
    return this.extra.borrow(bytes, signal);
  }
  onReclaimRequest(callback: (bytes: number) => void | Promise<void>): void {
    this.#reclaim = callback;
    this.extra.onReclaimRequest(callback);
  }
  reclaim(bytes: number): void | Promise<void> {
    if (!this.#closed) return this.#reclaim?.(bytes);
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.extra.close();
    if (this.#used > 0) this.fixed.free(this.#used);
    if (this.#credit > 0) this.fixed.release(this.#credit);
    this.#used = 0;
    this.#credit = 0;
  }
}
