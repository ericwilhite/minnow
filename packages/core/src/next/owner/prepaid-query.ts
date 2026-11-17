/** Query-local shares of the native root's admitted workspace. This is not another
 * governor: no admission/loan/cache ownership is delegated to the real governor.
 * A complete producer group may wait only after every previous source share retired. */
import type {
  AdmitOptions,
  EvictableCache,
  Grant,
  Loan,
  MemoryGovernor,
  NativeGrantGroupOptions,
} from "../memory/governor.js";
import { GrantClosedError, MemoryAdmissionError } from "../memory/governor.js";
import type { ByteCharge } from "../vector/varlen.js";
export const PREPAID_QUERY_BYTES = 4096;
const SHARE_BYTES = 512,
  CACHE_BYTES = 256;
function size(n: number): number {
  if (!Number.isSafeInteger(n) || n < 0) throw new RangeError("Invalid prepaid query bytes");
  return n;
}
export interface PrepaidQueryScope {
  readonly governor: Pick<MemoryGovernor, "capBytes" | "reserveBytes" | "usedBytes" | "peakBytes">;
  readonly source: Grant;
  readonly metadata: ByteCharge;
  readonly signal: AbortSignal;
  /** Called before ANY source share exists, with no source lease or producer loan.
   * Root may reclaim/wait cancellably for the COMPLETE next producer minimum. */
  prepare(bytes: number, signal: AbortSignal): Promise<void>;
  prepareMetadata(bytes: number, signal: AbortSignal): Promise<void>;
}
export class PrepaidQueryGovernor implements MemoryGovernor {
  #first: PrepaidShare | null = null;
  #held = 0;
  #caches: Set<() => void>;
  #closed = false;
  #pending: Promise<readonly Grant[]> | null = null;
  #stop: AbortController;
  #closing: Promise<void> | null = null;
  readonly #signal: AbortSignal;
  constructor(readonly scope: PrepaidQueryScope) {
    scope.signal.throwIfAborted();
    if (!scope.metadata.allocate(PREPAID_QUERY_BYTES))
      throw new RangeError("Native query metadata must be admitted");
    try {
      this.#stop = new AbortController();
      this.#caches = new Set();
      this.#signal = AbortSignal.any([scope.signal, this.#stop.signal]);
    } catch (error) {
      scope.metadata.free(PREPAID_QUERY_BYTES);
      throw error;
    }
  }
  get capBytes() {
    return this.scope.governor.capBytes;
  }
  get reserveBytes() {
    return this.scope.governor.reserveBytes;
  }
  get usedBytes() {
    return this.scope.governor.usedBytes;
  }
  get peakBytes() {
    return this.scope.governor.peakBytes;
  }
  #check(signal?: AbortSignal) {
    this.#signal.throwIfAborted();
    signal?.throwIfAborted();
    if (this.#closed) throw new GrantClosedError("native query workspace");
  }
  /** Force group admission through one complete root callback, never piecemeal admission. */
  tryAdmit(_options: Omit<AdmitOptions, "signal">): Grant | null {
    void _options;
    this.#check();
    return null;
  }
  async admit(options: AdmitOptions): Promise<Grant> {
    this.#check(options.signal);
    size(options.minimumBytes);
    const signal = options.signal ? AbortSignal.any([this.#signal, options.signal]) : this.#signal;
    const bytes = size(options.minimumBytes + SHARE_BYTES);
    if (!this.scope.metadata.allocate(bytes)) {
      if (this.#held !== 0) throw new TypeError("Native producer metadata must be prepaid");
      await this.scope.prepareMetadata(bytes, signal);
      this.#check(signal);
      if (!this.scope.metadata.allocate(bytes))
        throw new Error("Native root did not protect query metadata");
    }
    try {
      const share = new PrepaidShare(
        this,
        options.label,
        options.spillable,
        options.minimumBytes,
        false,
      );
      share.next = this.#first;
      if (this.#first) this.#first.previous = share;
      this.#first = share;
      return share;
    } catch (error) {
      this.scope.metadata.free(bytes);
      throw error;
    }
  }
  admitGroup(options: NativeGrantGroupOptions): Promise<readonly Grant[]> {
    this.#check(options.signal);
    if (this.#pending) throw new TypeError("Concurrent native query admission");
    const total = options.grants.reduce((n, p) => size(n + size(p.minimumBytes)), 0);
    if (total > this.capBytes) throw new MemoryAdmissionError(options.label, total, this.capBytes);
    // Existing idle credit can be returned without retaining a producer page/loan.
    if (options.idleCredit) {
      const idle = options.idleCredit;
      if (
        !(idle instanceof PrepaidShare) ||
        idle.owner !== this ||
        !idle.source ||
        idle.allocated ||
        idle.loaned
      )
        throw new TypeError("Invalid native query phase credit");
      idle.release(idle.held);
    }
    const signal = options.signal ? AbortSignal.any([this.#signal, options.signal]) : this.#signal;
    const work = (async () => {
      this.#check(signal);
      if (this.scope.source.held - this.scope.source.allocated < total) {
        // Source recipes or phase metadata can be retained, but a live producer cannot
        // request another working set while keeping its admitted minimum.
        if (this.#held !== 0) throw new TypeError("Native producer must retire before admission");
        await this.scope.prepare(total, signal);
        this.#check(signal);
      }
      if (!this.scope.source.allocate(total))
        throw new Error("Native root did not protect the complete query minimum");
      let remaining = total;
      const shares: PrepaidShare[] = [];
      try {
        for (const part of options.grants) {
          this.#check(signal);
          if (!this.scope.metadata.allocate(SHARE_BYTES))
            throw new RangeError("Native query share metadata must be admitted");
          let share: PrepaidShare;
          try {
            share = new PrepaidShare(this, part.label, part.spillable, part.minimumBytes, true);
          } catch (e) {
            this.scope.metadata.free(SHARE_BYTES);
            throw e;
          }
          share.next = this.#first;
          if (this.#first) this.#first.previous = share;
          this.#first = share;
          this.#held += part.minimumBytes;
          remaining -= part.minimumBytes;
          shares.push(share);
        }
        return shares;
      } catch (error) {
        for (const share of shares) share.close();
        throw error;
      } finally {
        if (remaining) this.scope.source.free(remaining);
      }
    })();
    const pending = work.finally(() => {
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  transferCredit(source: Grant, target: Grant, bytes: number): boolean {
    size(bytes);
    this.#check();
    if (
      !(source instanceof PrepaidShare) ||
      !(target instanceof PrepaidShare) ||
      source.owner !== this ||
      target.owner !== this
    )
      return false;
    return source.transfer(target, bytes);
  }
  /** Query root facts may remain caller-owned; this native scope deliberately declines
   * optional cache retention/transfers rather than creating another owner-wide cache. */
  registerCache(_cache: EvictableCache): ReturnType<MemoryGovernor["registerCache"]> {
    void _cache;
    this.#check();
    if (!this.scope.metadata.allocate(CACHE_BYTES))
      throw new RangeError("Native query cache metadata must be admitted");
    let active = true;
    const unregister = () => {
      if (active) {
        active = false;
        this.#caches.delete(unregister);
        this.scope.metadata.free(CACHE_BYTES);
      }
    };
    this.#caches.add(unregister);
    return {
      charge: () => false,
      release: () => undefined,
      takeFrom: () => false,
      unregister,
    };
  }
  grow(share: PrepaidShare, bytes: number): boolean {
    this.#check();
    size(bytes);
    if (!share.spillable) return false;
    if (!share.source) return this.scope.metadata.allocate(bytes);
    const available = this.scope.source.held - this.scope.source.allocated;
    if (available < bytes && !this.scope.source.tryGrow(bytes - available)) return false;
    if (!this.scope.source.allocate(bytes)) return false;
    this.#held += bytes;
    return true;
  }
  release(share: PrepaidShare, bytes: number): void {
    if (share.source) {
      this.scope.source.free(bytes);
      this.#held -= bytes;
    } else this.scope.metadata.free(bytes);
  }
  unlink(share: PrepaidShare): void {
    if (share.previous) share.previous.next = share.next;
    else this.#first = share.next;
    if (share.next) share.next.previous = share.previous;
    share.next = share.previous = null;
    this.scope.metadata.free(SHARE_BYTES);
  }
  /** Caller first aborts/fences the compiled runtime. Pending admissions are cancelled here;
   * accepted shares cannot outlive close even if installation raced cancellation. */
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      this.#stop.abort(new GrantClosedError("native query workspace"));
      await this.#pending?.catch(() => undefined);
      while (this.#first) this.#first.close();
      for (const unregister of this.#caches) unregister();
      this.scope.metadata.free(PREPAID_QUERY_BYTES);
      if (this.#held !== 0) throw new Error("Native query workspace retained credit");
    })());
  }
}
class PrepaidShare implements Grant {
  next: PrepaidShare | null = null;
  previous: PrepaidShare | null = null;
  #held: number;
  #used = 0;
  #closed = false;
  #loan: Loan | null = null;
  constructor(
    readonly owner: PrepaidQueryGovernor,
    readonly label: string,
    readonly spillable: boolean,
    bytes: number,
    readonly source: boolean,
  ) {
    this.#held = bytes;
  }
  get held() {
    return this.#held;
  }
  get allocated() {
    return this.#used;
  }
  get loaned() {
    return this.#loan?.bytes ?? 0;
  }
  allocate(bytes: number): boolean {
    size(bytes);
    if (this.#closed || bytes > this.#held - this.#used) return false;
    this.#used += bytes;
    return true;
  }
  free(bytes: number): void {
    size(bytes);
    if (this.#closed) return;
    if (bytes > this.#used) throw new RangeError("Native query allocation underflow");
    this.#used -= bytes;
  }
  tryGrow(bytes: number): boolean {
    size(bytes);
    if (this.#closed || !this.owner.grow(this, bytes)) return false;
    this.#held += bytes;
    return true;
  }
  async makeRoom(bytes: number, signal?: AbortSignal): Promise<boolean> {
    signal?.throwIfAborted();
    return this.#held - this.#used >= bytes || this.tryGrow(bytes - (this.#held - this.#used));
  }
  reclaimRoom(bytes: number, signal?: AbortSignal): Promise<boolean> {
    return this.makeRoom(bytes, signal);
  }
  release(bytes: number): void {
    size(bytes);
    if (this.#closed) return;
    if (bytes > this.#held - this.#used || this.#loan)
      throw new RangeError("Native query credit is still borrowed");
    this.#held -= bytes;
    this.owner.release(this, bytes);
  }
  async borrow(bytes: number, signal?: AbortSignal): Promise<Loan> {
    signal?.throwIfAborted();
    size(bytes);
    if (this.#closed) throw new GrantClosedError(this.label);
    if (this.#loan) throw new RangeError("Native query loan already active");
    if (bytes > this.#held - this.#used)
      throw new MemoryAdmissionError(this.label, bytes, this.#held - this.#used);
    let repaid = false;
    const loan: Loan = {
      bytes,
      repay: () => {
        if (repaid || this.#closed) return;
        if (this.#used) throw new RangeError("Native query loan still allocated");
        repaid = true;
        this.#loan = null;
      },
    };
    this.#loan = loan;
    return loan;
  }
  transfer(target: PrepaidShare, bytes: number): boolean {
    if (
      this.#closed ||
      target.#closed ||
      this.source !== target.source ||
      bytes > this.#held - this.#used ||
      this.#loan
    )
      return false;
    this.#held -= bytes;
    target.#held += bytes;
    return true;
  }
  onReclaimRequest(_callback: (bytes: number) => unknown): void {
    void _callback;
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.owner.release(this, this.#held);
    this.#held = this.#used = 0;
    this.#loan = null;
    this.owner.unlink(this);
  }
}
