/** Query-local shares of the native root's admitted workspace. This is not another
 * governor: no admission/loan/cache ownership is delegated to the real governor.
 * A complete producer group may wait only after every previous source share retired. */
import { abortableStep, stepSignal, type AbortableStep } from "../step-signal.js";
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
const SHARE_BYTES = 512;
/** Every cache a native query registers is declined, so one shared, inert registration serves
 * them all: it retains nothing per query and cannot fail under memory pressure. */
const DECLINED_CACHE: ReturnType<MemoryGovernor["registerCache"]> = Object.freeze({
  charge: () => false,
  release: () => undefined,
  takeFrom: () => false,
  unregister: () => undefined,
});
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
  /** What this query may size its working set for: the root's capacity beside the writer's
   * protected floor, less the root's own non-source holdings. Defaults to the cap. */
  sizingBytes?(): number;
}
export class PrepaidQueryGovernor implements MemoryGovernor {
  #first: PrepaidShare | null = null;
  #held = 0;
  #closed = false;
  #pending: Promise<readonly Grant[]> | null = null;
  #closing: Promise<void> | null = null;
  readonly #signal: AbortSignal;
  /** Follows the scope for this workspace's life; closed with it. */
  readonly #step: AbortableStep;
  constructor(readonly scope: PrepaidQueryScope) {
    scope.signal.throwIfAborted();
    if (!scope.metadata.allocate(PREPAID_QUERY_BYTES))
      throw new RangeError("Native query metadata must be admitted");
    try {
      this.#step = abortableStep("native query workspace", scope.signal);
      this.#signal = this.#step.signal;
    } catch (error) {
      scope.metadata.free(PREPAID_QUERY_BYTES);
      throw error;
    }
  }
  get capBytes() {
    return this.scope.governor.capBytes;
  }
  get sizingBytes() {
    return this.scope.sizingBytes?.() ?? this.scope.governor.capBytes;
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
    // A caller's signal adds a step that ends when this admission settles.
    const step = options.signal
      ? stepSignal("native query admission", this.#signal, options.signal)
      : null;
    const signal = step?.signal ?? this.#signal;
    const work = (async () => {
      this.#check(signal);
      // Headers belong to the SAME complete producer step. Protect them before
      // allocating its source share; late per-share allocation could refuse while
      // its own unused source credit occupied the remaining room.
      const headerBytes = size(options.grants.length * SHARE_BYTES);
      if (!this.scope.metadata.allocate(headerBytes)) {
        if (this.#held !== 0 || this.scope.source.allocated !== 0 || this.scope.source.loaned !== 0)
          throw new TypeError("Native producer metadata must be prepaid");
        if (this.scope.source.held) this.scope.source.release(this.scope.source.held);
        await this.scope.prepareMetadata(headerBytes, signal);
        this.#check(signal);
        if (!this.scope.metadata.allocate(headerBytes))
          throw new Error("Native root did not protect query headers");
      }
      let headers = headerBytes,
        remaining = 0;
      const shares: PrepaidShare[] = [];
      try {
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
        remaining = total;
        for (const part of options.grants) {
          this.#check(signal);
          const share = new PrepaidShare(this, part.label, part.spillable, part.minimumBytes, true);
          headers -= SHARE_BYTES;
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
        if (headers) this.scope.metadata.free(headers);
      }
    })();
    const pending = work.finally(() => {
      step?.close();
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
   * optional cache retention/transfers rather than creating another owner-wide cache. Declining
   * retains nothing, so registration charges nothing (a charge here could only fail as an
   * untyped error mid-statement when the root's metadata is fully used). */
  registerCache(_cache: EvictableCache): ReturnType<MemoryGovernor["registerCache"]> {
    void _cache;
    this.#check();
    return DECLINED_CACHE;
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
      this.#step.abort(new GrantClosedError("native query workspace"));
      await this.#pending?.catch(() => undefined);
      while (this.#first) this.#first.close();
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
