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
 *    hash-table and run headers, and spill I/O buffers. `batchTargetFor` shrinks the batch target
 *    so that any pipeline's minimum fits in a quarter of the cap: a minimum above the cap is a
 *    planner bug (`RangeError`), not a size refusal.
 * 4. **Grow or spill.** `tryGrow` grants or returns false, never waits. A spillable operator that is
 *    refused spills (partitions, runs) and retries smaller. Growth is charged before allocating,
 *    and a resize charges the new buffer while the old one still exists (growth overlap); the old
 *    one is released after the copy.
 * 5. **Waiting for progress.** An operator that cannot spill its way forward (one value larger
 *    than a batch, a result frame waiting for the consumer's credit) suspends with `waitFor`: the
 *    pipeline returns `blocked` (exec/pipeline.ts) until the bytes are granted. Waits are served in
 *    order from the progress reserve (`reserveBytes`), which no `tryGrow` can take, so one waiter
 *    at a time always progresses. A single value larger than the reserve cannot be held at all:
 *    `ValueTooLargeError` (the one size error, about a value, never a query or table).
 * 6. **The cache yields first.** Before refusing growth or keeping an admission waiting, the
 *    governor evicts unpinned encoded pages. Operators spill only after the cache is at its floor.
 * 7. **Cooperative reclaim.** While an admission or a wait is pending, the governor calls each
 *    spillable grant's `onReclaimRequest` with the bytes it needs; the operator spills at its next
 *    batch boundary and `release`s.
 *
 * ## Accounting unit
 * Bytes of ArrayBuffer capacity (`byteLength` of what is allocated, not what is used), plus
 * `OBJECT_OVERHEAD_BYTES` per JS object an operator keeps per group, partition or run, plus
 * `bigintBytes` per BigInt held in operator state (wide decimals). JS strings are not kept in
 * operator state (text stays UTF-8) except on the collation fallback (DECISIONS.md C8), which
 * charges `stringBytes`. The governor accounts managed memory; Gate D measures browser RSS.
 */
import { BATCH_TARGET_BYTES } from "../vector/vector.js";

export const OBJECT_OVERHEAD_BYTES = 64;
/** The progress reserve: one eighth of the cap, at least 1 MiB. */
export const reserveFor = (capBytes: number): number => Math.max(1 << 20, Math.floor(capBytes / 8));

/** Bytes a BigInt occupies in V8 (header plus 64-bit digits), for rule-3 accounting. */
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

export interface Grant {
  readonly label: string;
  /** Bytes currently held by this grant (its minimum included). */
  readonly bytes: number;
  /** Rule 4. Never waits. Only spillable grants grow past their minimum. */
  tryGrow(bytes: number): boolean;
  /** Rule 5: resolves once `bytes` are added to this grant. */
  waitFor(bytes: number, signal?: AbortSignal): Promise<void>;
  /** Returns bytes to the governor. */
  release(bytes: number): void;
  /** Rule 7. */
  onReclaimRequest(callback: (bytes: number) => void): void;
  /** Releases everything. Idempotent. */
  close(): void;
}

/** The page cache's side of the governor (rule 6). */
export interface EvictableCache {
  readonly evictableBytes: number;
  /** Evicts at least `bytes` if possible; returns the bytes freed (the cache releases them). */
  evict(bytes: number): number;
}

export interface MemoryGovernor {
  readonly capBytes: number;
  readonly reserveBytes: number;
  /** Bytes granted across all grants and the cache. Never above `capBytes`. */
  readonly usedBytes: number;
  /** Peak `usedBytes` since creation (tests assert the cap held). */
  readonly peakBytes: number;
  /** Rule 2. */
  admit(options: AdmitOptions): Promise<Grant>;
  /** The cache charges and releases its bytes through this grant-like handle. */
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

function abortReason(signal: AbortSignal | undefined): Error {
  const reason: unknown = signal?.reason;
  return reason instanceof Error ? reason : new Error("Aborted", { cause: reason });
}

interface Waiter {
  readonly bytes: number;
  readonly fromReserve: boolean;
  readonly grant: GrantImpl | null;
  readonly resolve: (grant: GrantImpl | null) => void;
  readonly reject: (reason: unknown) => void;
  readonly label: string;
  readonly spillable: boolean;
}

class GrantImpl implements Grant {
  bytes = 0;
  closed = false;
  reclaim: ((bytes: number) => void) | null = null;
  constructor(
    readonly governor: Governor,
    readonly label: string,
    readonly spillable: boolean,
  ) {}

  tryGrow(bytes: number): boolean {
    if (this.closed || !this.spillable) return false;
    return this.governor.grow(this, bytes);
  }

  waitFor(bytes: number, signal?: AbortSignal): Promise<void> {
    return this.governor.wait(this, bytes, signal);
  }

  release(bytes: number): void {
    if (bytes > this.bytes) throw new RangeError(`${this.label} released more than it holds`);
    this.bytes -= bytes;
    this.governor.give(bytes);
  }

  onReclaimRequest(callback: (bytes: number) => void): void {
    this.reclaim = callback;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.governor.forget(this);
    this.release(this.bytes);
  }
}

/** The reference governor: rules 1–7 with FIFO waiting. */
export class Governor implements MemoryGovernor {
  readonly capBytes: number;
  readonly reserveBytes: number;
  usedBytes = 0;
  peakBytes = 0;
  #cache: EvictableCache | null = null;
  readonly #grants = new Set<GrantImpl>();
  readonly #waiters: Waiter[] = [];

  constructor(capBytes: number) {
    this.capBytes = capBytes;
    this.reserveBytes = reserveFor(capBytes);
  }

  #take(bytes: number): void {
    this.usedBytes += bytes;
    this.peakBytes = Math.max(this.peakBytes, this.usedBytes);
  }

  /** Room for `bytes` under `limit`, evicting the cache first (rule 6). */
  #room(bytes: number, limit: number): boolean {
    const missing = this.usedBytes + bytes - limit;
    if (missing <= 0) return true;
    if (this.#cache !== null && this.#cache.evictableBytes > 0) this.#cache.evict(missing);
    return this.usedBytes + bytes <= limit;
  }

  grow(grant: GrantImpl, bytes: number): boolean {
    // Growth may not take the progress reserve (rule 5), nor jump the admission queue.
    if (this.#waiters.length > 0 || !this.#room(bytes, this.capBytes - this.reserveBytes))
      return false;
    this.#take(bytes);
    grant.bytes += bytes;
    return true;
  }

  give(bytes: number): void {
    this.usedBytes -= bytes;
    this.#drain();
  }

  forget(grant: GrantImpl): void {
    this.#grants.delete(grant);
  }

  #drain(): void {
    for (let waiter = this.#waiters[0]; waiter !== undefined; waiter = this.#waiters[0]) {
      const limit = waiter.fromReserve ? this.capBytes : this.capBytes - this.reserveBytes;
      if (!this.#room(waiter.bytes, limit)) {
        for (const grant of this.#grants)
          if (grant.spillable && grant !== waiter.grant) grant.reclaim?.(waiter.bytes);
        return;
      }
      this.#waiters.shift();
      this.#take(waiter.bytes);
      if (waiter.grant !== null) {
        waiter.grant.bytes += waiter.bytes;
        waiter.resolve(waiter.grant);
      } else {
        const grant = new GrantImpl(this, waiter.label, waiter.spillable);
        grant.bytes = waiter.bytes;
        this.#grants.add(grant);
        waiter.resolve(grant);
      }
    }
  }

  #enqueue(
    waiter: Omit<Waiter, "resolve" | "reject">,
    signal?: AbortSignal,
  ): Promise<GrantImpl | null> {
    if (signal?.aborted === true) return Promise.reject(abortReason(signal));
    return new Promise((resolve, reject) => {
      const entry: Waiter = { ...waiter, resolve, reject };
      const cancel = () => {
        const index = this.#waiters.indexOf(entry);
        if (index >= 0) this.#waiters.splice(index, 1);
        reject(abortReason(signal));
        this.#drain();
      };
      signal?.addEventListener("abort", cancel, { once: true });
      this.#waiters.push(entry);
      this.#drain();
    });
  }

  async admit(options: AdmitOptions): Promise<Grant> {
    if (options.minimumBytes > this.capBytes - this.reserveBytes)
      throw new RangeError(`${options.label} needs a minimum above the cap: plan smaller batches`);
    const grant = await this.#enqueue(
      {
        bytes: options.minimumBytes,
        fromReserve: false,
        grant: null,
        label: options.label,
        spillable: options.spillable,
      },
      options.signal,
    );
    if (grant === null) throw new Error("Admission resolved without a grant");
    return grant;
  }

  async wait(grant: GrantImpl, bytes: number, signal?: AbortSignal): Promise<void> {
    if (bytes > this.reserveBytes) throw new ValueTooLargeError(bytes, this.reserveBytes);
    await this.#enqueue(
      { bytes, fromReserve: true, grant, label: grant.label, spillable: grant.spillable },
      signal,
    );
  }

  registerCache(cache: EvictableCache) {
    this.#cache = cache;
    return {
      charge: (bytes: number): boolean => {
        if (this.#waiters.length > 0 || this.usedBytes + bytes > this.capBytes - this.reserveBytes)
          return false;
        this.#take(bytes);
        return true;
      },
      release: (bytes: number): void => {
        this.give(bytes);
      },
    };
  }
}
