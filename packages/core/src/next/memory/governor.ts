/**
 * One owner-wide memory cap. See memory/README.md for the bounded read-step contract/proof.
 *
 * Rules:
 * 1. Held bytes include cache, leases, operator/input/result buffers and resize overlap.
 * 2. Complete executions atomically admit their whole minimum FIFO, holding nothing while
 *    waiting. `includesProgress` includes their private I/O and per-operator progress credit.
 *    Admissions before that minimum (query preparation: retained source recipes, sizing
 *    scratch, planning probes) come from one preparation credit (`carve`, `adopt`:
 *    released bytes return to it), grown only by immediate admissions (`canAdmitNow`), or wait
 *    only while the query holds nothing. Otherwise the query releases everything and waits,
 *    holding nothing, for a credit covering its peak need, up to the usable capacity, then prepares
 *    again (exec/preparation.ts).
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
import { MemoryAdmissionError } from "../memory-values.js";

export {
  MemoryAdmissionError,
  OBJECT_OVERHEAD_BYTES,
  bigintBytes,
  stringBytes,
} from "../memory-values.js";
/** The progress reserve: one eighth of the cap, at least 1 MiB. */
export const reserveFor = (capBytes: number): number => Math.max(1 << 20, Math.floor(capBytes / 8));

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
  /** The caller holds credit it cannot release until this is served: holder growth from this
   * chain (see NativeGrantGroupOptions.holdsCredit). */
  readonly holdsCredit?: HolderChain;
}

/**
 * A registered holder chain (GOVERNOR-WAIT-POLICY point 6, checked): the only source of holder
 * growth. Holder priority is deadlock-free because holder chains never wait on each other: the
 * write engine is the one `"writer"` chain per governor (a native root, the write index range
 * cursor and an identity migration grow inside it), and an `"owner"` chain is exclusive owner
 * work (startup, reopen, snapshot import) that runs while no writing transaction is open. The
 * governor enforces both: holder growth names an open chain of this governor, and an owner
 * chain's growth while the writer chain has a writing transaction open is an invariant error
 * (`HolderChainInvariantError`), never a queued wait.
 */
export interface HolderChain {
  readonly label: string;
  readonly kind: "writer" | "owner";
  /** Writer chain only: a writing transaction is open (set by the write engine). */
  transactionOpen: boolean;
  /** Ends the chain; its later holder growth is an invariant error. Idempotent. */
  close(): void;
}

/** Opens an owner holder chain on `governor`, which must check holder chains (a `Governor`). */
export function holderChain(governor: MemoryGovernor, label: string): HolderChain {
  if (governor.openHolderChain === undefined)
    throw new TypeError(`Holder growth "${label}" needs a governor that checks holder chains`);
  return governor.openHolderChain(label, "owner");
}

/** Holder growth outside the checked holder-chain invariant (GOVERNOR-WAIT-POLICY point 6): a
 * programming error in the caller, raised before anything queues. */
export class HolderChainInvariantError extends Error {
  readonly code = "MINNOW_HOLDER_CHAIN_INVARIANT";
  readonly sqlState = "XX000";
  constructor(
    readonly label: string,
    reason: string,
  ) {
    super(`Holder growth "${label}" ${reason} (GOVERNOR-WAIT-POLICY point 6)`);
    this.name = "HolderChainInvariantError";
  }
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
  /** Breaker growth at a boundary: await reclaimable holders only; false means spill.
   * Never waits for another execution's pinned/minimum memory, and never allocates.
   */
  reclaimRoom?(bytes: number, signal?: AbortSignal): Promise<boolean>;
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
  /** Names the cache in a stall report (test builds). */
  readonly label?: string;
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
  /**
   * Optional: the capacity a query should size its working set for, when less than the cap is
   * available to it (a native body query's root beside the writer). Sizing only; admission
   * and refusal use the cap.
   */
  readonly sizingBytes?: number;
  readonly reserveBytes: number;
  readonly usedBytes: number;
  readonly peakBytes: number;
  admit(options: AdmitOptions): Promise<Grant>;
  /** One FIFO admission, then an atomic split. A zero-total group needs no shared capacity. */
  admitGroup(options: NativeGrantGroupOptions): Promise<readonly Grant[]>;
  /**
   * Registers holdings an owner keeps for its lifetime (non-spillable, never released on a
   * reclaim request: the native owner's scratch control). An admission whose need exceeds the
   * capacity less every registered permanent holding can never be served while they persist,
   * so it is refused at once with 53200, never left waiting; queued requests are re-checked when
   * registrations change. `label` names the holding in a stall report. Returns the
   * unregistration. Optional for wrapping governors.
   */
  registerPermanent?(bytes: () => number, label?: string): () => void;
  /**
   * Registers a holder chain (`HolderChain`): at most one `"writer"` chain is open per governor.
   * Optional for wrapping governors, which forward holder growth to the governor that checks it.
   */
  openHolderChain?(label: string, kind: HolderChain["kind"]): HolderChain;
  /**
   * GOVERNOR-WAIT-POLICY point 5: the most a request could ever be served while its statement
   * waits: the usable capacity (`usableCapacity(includesProgress)`: the cap, less the reserve for
   * an ordinary request, less every registered lifetime holding) less what that statement keeps
   * held while it waits (`WaitingHoldings`). A waiting group carries it as
   * `NativeGrantGroupOptions.holdings`. With `unasked`, the statement's own spillable grants
   * count nothing: their reclaim, not yet asked, may return all they hold. Optional for wrapping
   * governors that never wait (a
   * prepaid native body's).
   */
  servableNow?(own: WaitingHoldings, includesProgress?: boolean, unasked?: boolean): number;
  /** Whether `bytes` could be admitted at once, without waiting (`Governor.canAdmitNow`). */
  canAdmitNow?(bytes: number, includesProgress: boolean): boolean;
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

/** What a statement keeps held while one of its requests waits (`servableNow`). */
export interface WaitingHoldings {
  /** The statement's own grants: they stay held while it waits. */
  readonly grants: readonly Grant[];
  /** Another owner's holdings kept for the statement: its writer's, while it holds the turn. */
  readonly lent?: LentHoldings;
  /** Memory only the statement's own snapshots keep alive (`Snapshot.exclusiveBytes`): it
   * returns when they close, so it stays while the statement waits. */
  readonly pinned?: () => number;
}

/** An owner's holdings kept for the statement that holds its turn (the writer's). */
export interface LentHoldings {
  readonly grants: readonly Grant[];
  /**
   * The bytes of `grants`, beyond the owner's registered lifetime holding, that stay held
   * however long the statement waits. The rest is finite: a flush, a snapshot's close or the
   * owner's reclaim frees it. Never more than stays: while the owner runs work of its own its
   * holdings are not yet known, and it counts none.
   */
  retained(): number;
}

/** Named parts of one complete minimum. Each part remains a normal native grant. */
export interface NativeGrantGroupOptions {
  /** Shared idle native credit, usable by every earlier FIFO admission head. */
  readonly idleCredit?: Grant;
  readonly label: string;
  readonly grants: ReadonlyArray<{
    readonly label: string;
    readonly minimumBytes: number;
    readonly spillable: boolean;
  }>;
  readonly signal?: AbortSignal;
  /** The total includes private progress credit; its consumers never need the shared reserve. */
  readonly includesProgress?: boolean;
  /**
   * What the requesting statement keeps held while this group waits: its bound is
   * `servableNow(holdings)` (GOVERNOR-WAIT-POLICY point 5). A group above its bound is refused
   * with 53200 wherever it sits in its queue, never left waiting. Every queued group is checked on
   * every drain, the one that queues it included (before it waits), with its own spillable grants
   * counting nothing (their reclaim is not asked yet); at the head, after that drain's reclaim
   * requests have completed, it is checked against all its holdings. The bound grows as finite
   * holders release and shrinks when an owner's own work ends and its holdings become exactly
   * known (a writer's flush or merge: `LentHoldings.retained`); the owner wakes the queue then.
   * Groups within their bound keep their FIFO order.
   */
  readonly holdings?: () => WaitingHoldings;
  /**
   * The caller holds credit it cannot release until this is served (a native root's retained
   * base; the writer's floors inside a native step): holder growth from this registered chain.
   * It is served before every queued new admission, as soon as it fits (FIFO among holders), so a
   * holder never waits behind a request that only its own release could serve. Set explicitly;
   * never inferred. The chain is checked (`HolderChain`).
   */
  readonly holdsCredit?: HolderChain;
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
  readonly parts: NativeGrantGroupOptions["grants"] | null;
  /** `NativeGrantGroupOptions.holdings`: its bound is checked while it waits. */
  readonly holdings: (() => WaitingHoldings) | null;
}

/** A growth observer (Governor.observeGrowth) tried to change grant or governor state. */
export class GrowthObserverReentryError extends Error {
  readonly code = "MINNOW_GROWTH_OBSERVER_REENTRY";
  constructor() {
    super("A growth observer may only read: it called a grant or governor mutator");
    this.name = "GrowthObserverReentryError";
  }
}

class GrantImpl implements Grant {
  group: readonly GrantImpl[] | null = null;
  /** A grant carved from held credit returns its bytes there while that credit is open. */
  parent: GrantImpl | null = null;
  /** Called after this grant's holdings grow (Governor.observeGrowth). */
  grown: (() => void) | null = null;
  /** Holdings grew during the current operation; the observer runs once it completes. */
  pendingGrowth = false;
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
    this.governor.assertMutable();
    if (this.closed || this.governor.isIdleCredit(this)) return false;
    const shortfall = this.allocated + bytes - this.held;
    if (shortfall > 0 && !this.#grow(shortfall)) return false;
    this.allocated += bytes;
    this.governor.activity++;
    this.settleGrowth();
    return true;
  }

  /** Runs the growth observer once, after a complete operation grew this grant. */
  settleGrowth(): void {
    if (!this.pendingGrowth) return;
    this.pendingGrowth = false;
    this.governor.notifyGrowth(this);
  }

  free(bytes: number): void {
    this.governor.assertMutable();
    if (this.closed) return;
    if (bytes > this.allocated) throw new RangeError(`${this.label} freed more than it allocated`);
    this.allocated -= bytes;
    this.governor.activity++;
  }

  tryGrow(bytes: number): boolean {
    this.governor.assertMutable();
    const grew = this.#grow(bytes);
    this.settleGrowth();
    return grew;
  }
  #grow(bytes: number): boolean {
    if (this.closed || !this.spillable || this.governor.isIdleCredit(this)) return false;
    return this.governor.grow(this, bytes);
  }

  makeRoom(bytes: number, signal?: AbortSignal): Promise<boolean> {
    this.governor.assertMutable();
    return this.governor.makeRoom(this, bytes, signal);
  }

  reclaimRoom(bytes: number, signal?: AbortSignal): Promise<boolean> {
    this.governor.assertMutable();
    return this.governor.reclaimRoom(this, bytes, signal);
  }

  get loaned(): number {
    return this.loan;
  }

  release(bytes: number): void {
    this.governor.assertMutable();
    if (this.closed) return;
    const releasable = this.held - this.allocated - this.loan;
    if (bytes > releasable) throw new LoanBackedReleaseError(this.label, bytes, releasable);
    this.held -= bytes;
    this.#giveBack(bytes);
    this.parent?.settleGrowth();
  }

  /** Returned bytes go back to an open parent credit, else to the governor. */
  #giveBack(bytes: number): void {
    const parent = this.parent;
    if (parent !== null && !parent.closed) {
      parent.held += bytes;
      if (bytes > 0) parent.pendingGrowth = true;
      // Credit moved between grants: holder activity for the stall guard.
      this.governor.activity++;
    } else this.governor.give(bytes);
  }

  borrow(bytes: number, signal?: AbortSignal): Promise<Loan> {
    this.governor.assertMutable();
    if (this.closed || this.governor.isIdleCredit(this))
      return Promise.reject(new GrantClosedError(this.label));
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
            // Loan bytes came from the governor: they never join a parent credit.
            this.held -= bytes;
            this.governor.give(bytes);
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
    this.governor.assertMutable();
    if (this.closed) return;
    this.closed = true;
    // Profiles may retain closed grant handles across passes; they must not keep
    // the retired operator graph alive through its cooperative spill callback.
    this.reclaim = null;
    this.governor.forget(this);
    const held = this.held,
      loan = this.loan;
    this.held = 0;
    this.allocated = 0;
    if (loan > 0) this.governor.repaid(loan);
    this.loan = 0;
    // Loan bytes return to the governor; the rest to an open parent credit.
    if (loan > 0) this.governor.give(loan);
    this.#giveBack(held - loan);
    this.parent?.settleGrowth();
  }
}

/** The reference governor: rules 1–8. */
/** One holder in a stall report: the grants of one label, summed. */
/** One grant, reported synchronously at the moment the governor makes it (`observeGrants`). */
export interface GrantEvent {
  readonly label: string;
  /** The queue it was served from, or the synchronous fast path that granted it. */
  readonly source: "loan" | "read" | "holder" | "admission" | "tryAdmit" | "grow" | "charge";
  readonly bytes: number;
  /** Requests still queued in each class at that moment, this one excluded. */
  readonly queued: {
    readonly loans: number;
    readonly reads: number;
    readonly holders: number;
    readonly admissions: number;
  };
}

export interface StallHolder {
  readonly label: string;
  readonly held: number;
  readonly allocated: number;
  readonly spillable: boolean;
}

/** The rest of a stall report: every byte `usedBytes` counts outside the grant holders. */
export interface StallAccounts {
  /** Registered caches and the bytes each holds, evictable or not. */
  readonly caches: ReadonlyArray<{ readonly label: string; held: number; evictable: number }>;
  /** Registered lifetime holdings (`registerPermanent`). They are held inside the grants and
   * caches above (or outside the governor), so they are listed, not added to the total. */
  readonly permanent: ReadonlyArray<{ readonly label: string; readonly bytes: number }>;
  /** Used bytes no grant or cache holds. Zero unless the governor's accounting is broken. */
  readonly unattributed: number;
}

/**
 * Thrown (test builds only, `Governor.stallAfterMs`) at a waiter that saw no governor progress
 * for that long: no credit taken or returned, no request queued, served or withdrawn, and no
 * grant anywhere allocated, freed or moved. Every waiter is then waiting on something no holder
 * will release; this names the waiter and every holder instead of hanging.
 */
export class GovernorStallError extends Error {
  readonly code = "MINNOW_GOVERNOR_STALL";
  constructor(
    readonly waiter: {
      readonly label: string;
      readonly bytes: number;
      readonly includesProgress: boolean;
    },
    readonly capacity: number,
    readonly usedBytes: number,
    readonly queued: readonly string[],
    readonly holders: readonly StallHolder[],
    readonly afterMs: number,
    /** Where the waiter queued (stack at admission). */
    readonly waiterStack: string,
    /** Which rule fired: no governor progress for `afterMs`, or queued past the hard deadline
     * (`Governor.stallDeadlineWindows` windows) whatever unrelated activity continued. */
    readonly rule: "no progress" | "deadline" = "no progress",
    readonly accounts: StallAccounts = { caches: [], permanent: [], unattributed: 0 },
  ) {
    super(
      `Governor stall (${rule}): "${waiter.label}" waited ` +
        `${String(rule === "deadline" ? afterMs * Governor.stallDeadlineWindows : afterMs)} ms ` +
        `for ${String(waiter.bytes)}B (capacity ${String(capacity)}B, used ${String(usedBytes)}B) ` +
        `${rule === "deadline" ? "queued past its deadline" : "with no progress"}; holders: ` +
        holders
          .map(
            (holder) =>
              `${holder.label} ${String(holder.held)}B held/${String(holder.allocated)}B allocated`,
          )
          .join(", ") +
        `; caches: ` +
        (accounts.caches
          .map(
            (cache) =>
              `${cache.label} ${String(cache.held)}B held/${String(cache.evictable)}B evictable`,
          )
          .join(", ") || "none") +
        `; unattributed ${String(accounts.unattributed)}B; lifetime holdings: ` +
        (accounts.permanent
          .map((holding) => `${holding.label} ${String(holding.bytes)}B`)
          .join(", ") || "none") +
        `; queued at:\n${waiterStack}`,
    );
    this.name = "GovernorStallError";
  }
}

export class Governor implements MemoryGovernor {
  /**
   * Test builds only; null in production. When set, a queued request that sees no governor
   * progress for this many milliseconds is rejected with a `GovernorStallError` naming every
   * holder, so a wait nothing will ever release fails fast instead of hanging. Progress is any
   * credit taken or returned, any request queued, served or withdrawn, and any allocation, free
   * or credit move on any grant: a holder working inside memory it was granted is progress.
   * It is a no-progress window, not a proof: a holder computing without touching any grant for
   * longer than the window would also be reported.
   */
  static stallAfterMs: number | null = null;
  /** Test builds: a waiter queued for this many windows is reported whatever other activity
   * continues (review 10): unrelated grant activity cannot mask a stalled waiter indefinitely. */
  static stallDeadlineWindows = 6;
  readonly #grantObservers = new Set<(event: GrantEvent) => void>();

  /** Reports every grant synchronously, with the queues as they stand at that moment, so a
   * suite checks the serving order itself rather than promise resolution, which lags the grant
   * (GOVERNOR-WAIT-POLICY). For tests and diagnostics; returns the unsubscription. */
  observeGrants(observer: (event: GrantEvent) => void): () => void {
    this.assertMutable();
    this.#grantObservers.add(observer);
    return () => this.#grantObservers.delete(observer);
  }

  #observeGrant(label: string, source: GrantEvent["source"], bytes: number): void {
    if (this.#grantObservers.size === 0) return;
    const event: GrantEvent = {
      label,
      source,
      bytes,
      queued: {
        loans: this.#loans.length,
        reads: this.#reads.length,
        holders: this.#holders.length,
        admissions: this.#admissions.length,
      },
    };
    for (const observer of this.#grantObservers) observer(event);
  }

  readonly capBytes: number;
  readonly reserveBytes: number;
  usedBytes = 0;
  peakBytes = 0;
  /** Counts governor progress for `stallAfterMs`: credit taken or returned, requests queued,
   * served or withdrawn. */
  #progress = 0;
  /** Counts holders' activity on held grants for `stallAfterMs`: allocations, frees and credit
   * moved between grants or into a cache. A holder working inside memory it was granted is
   * progress: a waiter has stalled only if no grant anywhere changed during the window. */
  activity = 0;
  readonly #permanent = new Map<() => number, string>();
  /** Bytes each registered cache holds (charged or taken from a grant, less released). */
  readonly #cacheHeld = new Map<EvictableCache, number>();
  readonly #chains = new Set<HolderChain>();
  #writerChain: HolderChain | null = null;

  openHolderChain(label: string, kind: HolderChain["kind"]): HolderChain {
    this.assertMutable();
    if (kind === "writer" && this.#writerChain !== null)
      throw new HolderChainInvariantError(label, "opens a second writer chain on this governor");
    const chains = this.#chains;
    const chain: HolderChain = {
      label,
      kind,
      transactionOpen: false,
      close: () => {
        chains.delete(chain);
        chain.transactionOpen = false;
        if (this.#writerChain === chain) this.#writerChain = null;
      },
    };
    chains.add(chain);
    if (kind === "writer") this.#writerChain = chain;
    return chain;
  }

  /** The checked holder-chain invariant (GOVERNOR-WAIT-POLICY point 6). */
  #checkHolder(label: string, chain: HolderChain): void {
    if (!this.#chains.has(chain))
      throw new HolderChainInvariantError(label, "names no open holder chain of this governor");
    if (chain.kind !== "writer" && this.#writerChain?.transactionOpen === true)
      throw new HolderChainInvariantError(
        label,
        `grows in ${chain.kind} chain "${chain.label}" while the writer chain ` +
          `"${this.#writerChain.label}" has a writing transaction open`,
      );
  }

  servableNow(own: WaitingHoldings, includesProgress = false, unasked = false): number {
    let held = 0;
    for (const grant of new Set(own.grants))
      if (
        grant instanceof GrantImpl &&
        grant.governor === this &&
        !grant.closed &&
        !(unasked && grant.spillable)
      )
        held += grant.held;
    return (
      this.usableCapacity(includesProgress) -
      held -
      (own.lent?.retained() ?? 0) -
      (own.pinned?.() ?? 0)
    );
  }

  registerPermanent(bytes: () => number, label = "owner lifetime holdings"): () => void {
    this.assertMutable();
    this.#permanent.set(bytes, label);
    this.#drain();
    return () => {
      this.assertMutable();
      this.#permanent.delete(bytes);
      this.#drain();
    };
  }

  /** Lifetime holdings registered by owners (`registerPermanent`). */
  get permanentBytes(): number {
    let total = 0;
    for (const bytes of this.#permanent.keys()) total += bytes();
    return total;
  }

  /** The most one request may ever be granted while the registered lifetime holdings persist:
   * the cap (with `includesProgress`) or `cap - reserve`, less every registered permanent
   * holding (GOVERNOR-WAIT-POLICY). A request above it is refused with 53200 at once; one at or
   * below it is served once the holder chains ahead of it finish. */
  usableCapacity(includesProgress = false): number {
    return (
      (includesProgress ? this.capBytes : this.capBytes - this.reserveBytes) - this.permanentBytes
    );
  }

  /**
   * Every queued group above its bound (`NativeGrantGroupOptions.holdings`), wherever it sits:
   * 53200 now, never a wait. Its own spillable grants count nothing here (their reclaim is not
   * asked yet), so this refuses no more than the head check after reclaim would.
   */
  #refuseAboveBound(): boolean {
    let refused = false;
    for (const queue of [this.#holders, this.#admissions])
      for (const request of [...queue]) {
        if (request.holdings === null || request.pending.size > 0) continue;
        const most = this.servableNow(request.holdings(), request.includesProgress, true);
        if (request.bytes <= most) continue;
        this.#reject(
          queue,
          request,
          new MemoryAdmissionError(request.label, request.bytes, Math.max(0, most)),
        );
        refused = true;
      }
    return refused;
  }

  /** A queued request that can never be served while the permanent holdings persist: 53200. */
  #refuseUnservable(): void {
    for (const queue of [this.#holders, this.#admissions])
      for (const request of [...queue]) {
        const usable = this.usableCapacity(request.includesProgress);
        if (request.bytes > usable)
          this.#reject(
            queue,
            request,
            new MemoryAdmissionError(request.label, request.bytes, usable),
          );
      }
  }
  readonly #caches: EvictableCache[] = [];
  readonly #grants = new Set<GrantImpl>();
  readonly #idleCredit = new Set<GrantImpl>();
  isIdleCredit(grant: GrantImpl): boolean {
    return this.#idleCredit.has(grant);
  }
  readonly #loans: Request[] = [];
  readonly #reads: Request[] = [];
  readonly #admissions: Request[] = [];
  /** Holder growth (`holdsCredit`): served before any queued new admission. */
  readonly #holders: Request[] = [];
  #draining = false;
  #again = false;
  /** Bytes out on loans: they count against the cap, never against the admission limit. */
  #loaned = 0;

  constructor(capBytes: number) {
    this.capBytes = capBytes;
    this.reserveBytes = reserveFor(capBytes);
  }

  #take(bytes: number): void {
    this.#progress++;
    this.usedBytes += bytes;
    this.peakBytes = Math.max(this.peakBytes, this.usedBytes);
  }

  /**
   * Bytes missing for a request: loans and complete private-progress groups may use the whole
   * cap; ordinary admissions and growth must also fit `cap − reserve` excluding shared loans.
   */
  #missing(bytes: number, loan: boolean, idle = 0): number {
    const total = this.usedBytes + bytes - this.capBytes;
    if (loan) return total;
    return Math.max(
      total,
      this.usedBytes - this.#loaned - idle + bytes - (this.capBytes - this.reserveBytes),
    );
  }

  /** Room for a request, evicting the caches first (rule 6). */
  #room(bytes: number, loan: boolean, idle = 0): boolean {
    const missing = this.#missing(bytes, loan, idle);
    if (missing <= 0) return true;
    this.#evict(missing);
    return this.#missing(bytes, loan, idle) <= 0;
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
    if (
      this.#loans.length > 0 ||
      this.#reads.length > 0 ||
      this.#holders.length > 0 ||
      this.#admissions.length > 0
    )
      return false;
    if (!this.#room(bytes, false)) return false;
    this.#take(bytes);
    grant.held += bytes;
    this.#observeGrant(grant.label, "grow", bytes);
    grant.pendingGrowth = true;
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

  /** Optional breaker growth never joins admission while retaining its state. A failed
   * reclamation returns to the breaker, which must spill before making another request.
   * Native minima/progress accounts are not spillable and are never waited upon here.
   */
  async reclaimRoom(grant: GrantImpl, bytes: number, signal?: AbortSignal): Promise<boolean> {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("nonnegative byte count required");
    const check = () => {
      checkSignal(signal);
      if (grant.closed) throw new GrantClosedError(grant.label);
    };
    const grow = () => {
      check();
      const missing = grant.allocated + bytes - grant.held;
      return missing <= 0 || grant.tryGrow(missing);
    };
    if (grow()) return true;
    for (const holder of [...this.#grants]) {
      if (holder === grant || holder.closed || !holder.spillable) continue;
      const work = this.#ask(holder, grant.allocated + bytes - grant.held);
      if (work !== undefined) {
        // Reclaim owns its buffers until it drains; cancellation stops this caller's
        // wait without pretending the other holder's outstanding operation finished.
        await new Promise<void>((resolve, reject) => {
          const abort = () => reject(abortReason(signal));
          signal?.addEventListener("abort", abort, { once: true });
          void work
            .then(resolve, reject)
            .finally(() => signal?.removeEventListener("abort", abort));
          if (signal?.aborted) abort();
        });
      }
      if (grow()) return true;
    }
    return false;
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
    this.#progress++;
    this.usedBytes -= bytes;
    // A zero release is an owner's wake (`release(0)`, the writer's `notifyReclaimable`): its
    // reclaimers may return more now (a publication covered what they keep), so a queued
    // admission asks each of them once more. A read still asks one holder at a time.
    if (bytes === 0)
      for (const queue of [this.#holders, this.#admissions])
        for (const request of queue) if (request.readRoom === null) request.asked.clear();
    this.#drain();
  }

  forget(grant: GrantImpl): void {
    this.#idleCredit.delete(grant);
    this.#grants.delete(grant);
    for (const queue of [this.#loans, this.#reads, this.#holders, this.#admissions])
      for (const request of [...queue])
        if (request.grant === grant) {
          this.#progress++;
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
        if (this.#permanent.size > 0) this.#refuseUnservable();
        this.#serve(this.#loans, true);
        if (this.#loans.length === 0) {
          this.#serve(this.#reads, false);
          if (this.#reads.length === 0) {
            // Holder growth first (GOVERNOR-WAIT-POLICY): new admissions wait while any holder
            // request is queued, so none can take room a waiting holder needs.
            this.#serve(this.#holders, false);
            if (this.#holders.length === 0) this.#serve(this.#admissions, false);
          }
        }
        // After serving: only a group still waiting is checked, so none that fits now is refused.
        // A refused group may have blocked the ones behind it: serve again then.
      } while (this.#refuseAboveBound() || this.#releasedDuringDrain());
    } finally {
      this.#draining = false;
    }
  }

  /** Whether a queue served before `queue` holds a request: loans, then reads, then holder
   * growth, then new admissions (GOVERNOR-WAIT-POLICY). */
  #preempted(queue: Request[]): boolean {
    for (const higher of [this.#loans, this.#reads, this.#holders]) {
      if (higher === queue) return false;
      if (higher.length > 0) return true;
    }
    return false;
  }

  #serve(queue: Request[], loan: boolean): void {
    for (let head = queue[0]; head !== undefined; head = queue[0]) {
      const request = head;
      let wholeMinimum = loan || request.includesProgress;
      // Parallel column reads may share a grant. Re-evaluate its missing room after another
      // request was served, and after callbacks that can surrender unused holdings.
      const pooled = () => [...this.#idleCredit].reduce((sum, pool) => sum + pool.held, 0);
      const demand = () =>
        request.readRoom === null || request.grant === null
          ? request.bytes
          : Math.max(0, request.grant.allocated + request.readRoom - request.grant.held);
      const bytes = () => Math.max(0, demand() - pooled());
      // Unused private idle credit is not an ordinary allocation. It still counts
      // against the physical cap, but cannot make an earlier ordinary head wait.
      const room = (whole: boolean) => {
        const credit = pooled(),
          needed = demand();
        const fits = this.#room(Math.max(0, needed - credit), whole, Math.max(0, credit - needed));
        // Eviction is a callout: it may close/donate a pool or change a read's
        // allocation. Never serve using the deficit from before that callback.
        return fits && pooled() === credit && demand() === needed;
      };
      if (!room(wholeMinimum)) {
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
          if (request.readRoom !== null && room(wholeMinimum)) break;
        }
        if (
          request.readRoom !== null &&
          request.pending.size === 0 &&
          request.grant?.allocated === 0 &&
          room(true)
        )
          wholeMinimum = true;
        if (!room(wholeMinimum)) {
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
          // GOVERNOR-WAIT-POLICY point 5, again as holdings settle: never served while it waits.
          // Not while a reclaim it asked for is pending: its own holdings may still shrink.
          const holdings = request.pending.size === 0 ? request.holdings?.() : undefined;
          const most =
            holdings === undefined
              ? undefined
              : this.servableNow(holdings, request.includesProgress);
          if (most !== undefined && demand() > most) {
            this.#reject(
              queue,
              request,
              new MemoryAdmissionError(request.label, demand(), Math.max(0, most)),
            );
            continue;
          }
          return;
        }
      }
      // A callback may have cancelled or served the head while we asked for room.
      if (queue[0] !== request) continue;
      // Or queued a higher-priority request (reclaim and eviction callouts can enqueue): grant
      // nothing from this queue then; the drain serves the higher queue first.
      if (this.#preempted(queue)) return;
      this.#progress++;
      queue.shift();
      request.detach();
      const wanted = demand();
      const taken = bytes();
      this.#take(taken);
      if (loan) this.#loaned += wanted;
      let grant = request.grant;
      if (grant === null) {
        grant = new GrantImpl(this, request.label, request.spillable);
        this.#grants.add(grant);
      }
      grant.held += taken;
      if (request.grant !== null) grant.pendingGrowth = true;
      {
        let remaining = wanted - taken;
        for (const pool of this.#idleCredit) {
          const adopted = Math.min(remaining, pool.held);
          pool.held -= adopted;
          grant.held += adopted;
          remaining -= adopted;
          if (remaining === 0) break;
        }
        if (remaining !== 0) throw new Error("Idle credit changed during admission");
      }
      if (request.parts !== null) {
        // Native adoption and named ownership install are one synchronous drain step.
        grant.group = request.parts.map((part) => {
          const named = new GrantImpl(this, part.label, part.spillable);
          grant.held -= part.minimumBytes;
          named.held = part.minimumBytes;
          this.#grants.add(named);
          return named;
        });
      }
      if (request.loan) {
        grant.loan = wanted;
        grant.loanPending = false;
      }
      this.#observeGrant(
        request.label,
        loan
          ? "loan"
          : queue === this.#reads
            ? "read"
            : queue === this.#holders
              ? "holder"
              : "admission",
        wanted,
      );
      // Idle credit and any loan are installed: the holdings are complete.
      grant.settleGrowth();
      request.resolve(grant);
    }
  }

  #enqueue(
    queue: Request[],
    request: Omit<
      Request,
      | "resolve"
      | "reject"
      | "detach"
      | "asked"
      | "readRoom"
      | "pending"
      | "includesProgress"
      | "parts"
      | "holdings"
    > & { includesProgress?: boolean; holdings?: (() => WaitingHoldings) | null },
    signal?: AbortSignal,
    readRoom: number | null = null,
    parts: NativeGrantGroupOptions["grants"] | null = null,
  ): Promise<GrantImpl> {
    if (signal?.aborted === true) return Promise.reject(abortReason(signal));
    return new Promise((resolve, reject) => {
      const cancel = () => {
        const index = queue.indexOf(entry);
        if (index >= 0) queue.splice(index, 1);
        this.#progress++;
        entry.detach();
        reject(abortReason(signal));
        this.#drain();
      };
      const entry: Request = {
        ...request,
        includesProgress: request.includesProgress ?? false,
        holdings: request.holdings ?? null,
        asked: new Set(),
        pending: new Set(),
        readRoom,
        parts,
        resolve,
        reject,
        detach: () => {
          signal?.removeEventListener("abort", cancel);
        },
      };
      signal?.addEventListener("abort", cancel, { once: true });
      queue.push(entry);
      this.#progress++;
      this.#watchStall(queue, entry);
      this.#drain();
    });
  }

  /** Test builds: rejects `entry` once it has waited `stallAfterMs` with no progress. */
  #watchStall(queue: Request[], entry: Request): void {
    const afterMs = Governor.stallAfterMs;
    if (afterMs === null) return;
    const queuedAt = new Error().stack?.split("\n").slice(3, 16).join("\n") ?? "";
    const moved = () => this.#progress + this.activity;
    const deadline = Date.now() + afterMs * Governor.stallDeadlineWindows;
    let seen = moved(),
      since = Date.now();
    const timer = setInterval(
      () => {
        if (!queue.includes(entry)) {
          clearInterval(timer);
          return;
        }
        const now = Date.now();
        if (moved() !== seen) {
          seen = moved();
          since = now;
        }
        const rule = now - since >= afterMs ? "no progress" : now >= deadline ? "deadline" : null;
        if (rule === null) return;
        clearInterval(timer);
        const holders = new Map<string, { held: number; allocated: number; spillable: boolean }>();
        for (const grant of this.#grants) {
          const holder = holders.get(grant.label) ?? {
            held: 0,
            allocated: 0,
            spillable: grant.spillable,
          };
          holder.held += grant.held;
          holder.allocated += grant.allocated;
          holders.set(grant.label, holder);
        }
        this.#reject(
          queue,
          entry,
          new GovernorStallError(
            { label: entry.label, bytes: entry.bytes, includesProgress: entry.includesProgress },
            entry.includesProgress ? this.capBytes : this.capBytes - this.reserveBytes,
            this.usedBytes,
            [...this.#loans, ...this.#reads, ...this.#holders, ...this.#admissions].map(
              (request) => request.label,
            ),
            [...holders]
              .filter(([, holder]) => holder.held > 0)
              .map(([label, holder]) => ({ label, ...holder })),
            afterMs,
            queuedAt,
            rule,
            this.#accounts(),
          ),
        );
      },
      Math.max(10, afterMs / 4),
    );
    (timer as { unref?: () => void }).unref?.();
  }

  /** Every byte `usedBytes` counts outside the grants, for a stall report. */
  #accounts(): StallAccounts {
    let grants = 0;
    for (const grant of this.#grants) grants += grant.held;
    let unnamed = 0;
    const caches = [...this.#cacheHeld]
      .filter(([, held]) => held !== 0)
      .map(([cache, held]) => ({
        label:
          cache.label ??
          (cache.constructor === Object ? `cache ${String(++unnamed)}` : cache.constructor.name),
        held,
        evictable: cache.evictableBytes,
      }));
    return {
      caches,
      permanent: [...this.#permanent]
        .map(([bytes, label]) => ({ label, bytes: bytes() }))
        .filter((holding) => holding.bytes !== 0),
      unattributed: this.usedBytes - grants - caches.reduce((sum, cache) => sum + cache.held, 0),
    };
  }

  #reject(queue: Request[], request: Request, error: unknown): void {
    const at = queue.indexOf(request);
    if (at < 0) return;
    this.#progress++;
    queue.splice(at, 1);
    request.detach();
    request.reject(error);
    this.#drain();
  }

  async admit(options: AdmitOptions): Promise<Grant> {
    this.assertMutable();
    if (options.holdsCredit !== undefined) this.#checkHolder(options.label, options.holdsCredit);
    if (options.minimumBytes > this.usableCapacity(false))
      throw new MemoryAdmissionError(
        options.label,
        options.minimumBytes,
        this.usableCapacity(false),
      );
    return this.#enqueue(
      options.holdsCredit !== undefined ? this.#holders : this.#admissions,
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
    this.assertMutable();
    if (options.holdsCredit !== undefined) this.#checkHolder(options.label, options.holdsCredit);
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
    // The cap less every registered permanent holding: above it, this can never be served.
    const capacity = this.usableCapacity(includesProgress === true);
    if (total > capacity) throw new MemoryAdmissionError(label, total, capacity);
    const pool = options.idleCredit;
    if (pool !== undefined) {
      if (
        !(pool instanceof GrantImpl) ||
        pool.governor !== this ||
        pool.closed ||
        pool.allocated !== 0 ||
        pool.loaned !== 0 ||
        pool.loanPending
      )
        throw new TypeError("Idle credit requires this owner's live, unallocated native grant");
      this.#idleCredit.add(pool);
      this.#drain();
    }
    signal?.throwIfAborted();
    if (total === 0) {
      const grants = parts.map((part) => new GrantImpl(this, part.label, part.spillable));
      for (const grant of grants) this.#grants.add(grant);
      return grants;
    }
    const admitted = await this.#enqueue(
      options.holdsCredit !== undefined ? this.#holders : this.#admissions,
      {
        bytes: total,
        loan: false,
        grant: null,
        label,
        spillable: false,
        includesProgress: includesProgress === true,
        holdings: options.holdings ?? null,
      },
      signal,
      null,
      parts,
    );
    try {
      signal?.throwIfAborted();
      const grants = admitted.group;
      if (grants === null || admitted.held !== 0)
        throw new Error("Native group was not split atomically in the drain");
      return grants;
    } catch (error) {
      for (const grant of admitted.group ?? []) grant.close();
      throw error;
    } finally {
      admitted.close();
    }
  }

  /**
   * Whether an admission of `bytes` would be served at once: nothing is queued and it fits now,
   * after evicting caches (a private-progress group may use the reserve). Takes no credit; an
   * `admitGroup` of that size called next, synchronously, is served without waiting.
   */
  canAdmitNow(bytes: number, includesProgress: boolean): boolean {
    // Any queued request, holder growth included (GOVERNOR-WAIT-POLICY): never "at once" past it.
    if (
      this.#draining ||
      this.#loans.length > 0 ||
      this.#reads.length > 0 ||
      this.#holders.length > 0 ||
      this.#admissions.length > 0
    )
      return false;
    return this.#room(bytes, includesProgress);
  }

  /**
   * Carves a new ordinary grant of `bytes` from `source`'s idle credit, synchronously. Nothing is
   * admitted: the bytes were already held by the same owner, so no queued admission is passed.
   * The carved grant returns what it releases or closes to `source` while `source` is open (then
   * to the governor). Null when `source` cannot spare the bytes.
   */
  carve(source: Grant, label: string, spillable: boolean, bytes: number): Grant | null {
    this.assertMutable();
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("Carved bytes must be a nonnegative safe integer");
    if (
      !(source instanceof GrantImpl) ||
      source.governor !== this ||
      source.closed ||
      this.#idleCredit.has(source) ||
      bytes > source.held - source.allocated - source.loaned
    )
      return null;
    const grant = new GrantImpl(this, label, spillable);
    grant.parent = source;
    this.#grants.add(grant);
    source.held -= bytes;
    grant.held += bytes;
    this.activity++;
    return grant;
  }

  /** Whether `grant` is an open grant of this governor. Read-only: safe inside an observer. */
  isOpen(grant: Grant): boolean {
    return grant instanceof GrantImpl && grant.governor === this && !grant.closed;
  }

  /**
   * Calls `observer` after `grant`'s holdings grow (growth, a read's room, a credit transfer in),
   * or stops with null. Preparation tracks its peak with it. False for a foreign grant.
   */
  observeGrowth(grant: Grant, observer: (() => void) | null): boolean {
    this.assertMutable();
    if (!(grant instanceof GrantImpl) || grant.governor !== this) return false;
    grant.grown = observer;
    return true;
  }

  #observing = false;
  /** Runs `grant`'s growth observer, read-only: any mutator it calls throws. */
  notifyGrowth(grant: Grant): void {
    if (!(grant instanceof GrantImpl) || grant.grown === null) return;
    this.#observing = true;
    try {
      grant.grown();
    } finally {
      this.#observing = false;
    }
  }
  /** Grant and governor mutators refuse while a growth observer runs. */
  assertMutable(): void {
    if (this.#observing) throw new GrowthObserverReentryError();
  }

  /**
   * From now on `child` returns the bytes it releases to `parent` while `parent` is open, exactly
   * as if it had been carved from it. False (nothing changes) for a closed, foreign, idle or
   * already parented grant.
   */
  adopt(parent: Grant, child: Grant): boolean {
    this.assertMutable();
    if (
      !(parent instanceof GrantImpl) ||
      !(child instanceof GrantImpl) ||
      parent === child ||
      parent.governor !== this ||
      child.governor !== this ||
      parent.closed ||
      child.closed ||
      child.parent !== null ||
      this.#idleCredit.has(parent) ||
      this.#idleCredit.has(child)
    )
      return false;
    child.parent = parent;
    this.activity++;
    return true;
  }

  transferCredit(source: Grant, target: Grant, bytes: number): boolean {
    this.assertMutable();
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
      this.activity++;
      if (bytes > 0) target.pendingGrowth = true;
    }
    target.settleGrowth();
    if (
      bytes > 0 &&
      source !== target &&
      (this.#idleCredit.has(source) || this.#idleCredit.has(target))
    )
      this.#drain();
    return true;
  }

  tryAdmit(options: Omit<AdmitOptions, "signal">): Grant | null {
    this.assertMutable();
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
      this.#holders.length > 0 ||
      this.#admissions.length > 0
    )
      return null;
    if (!this.#room(options.minimumBytes, false)) return null;
    this.#take(options.minimumBytes);
    this.#observeGrant(options.label, "tryAdmit", options.minimumBytes);
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
    this.assertMutable();
    if (!this.#caches.includes(cache)) this.#caches.push(cache);
    if (!this.#cacheHeld.has(cache)) this.#cacheHeld.set(cache, 0);
    const held = (bytes: number) =>
      this.#cacheHeld.set(cache, (this.#cacheHeld.get(cache) ?? 0) + bytes);
    let active = true;
    return {
      takeFrom: (source: unknown, bytes: number): boolean => {
        this.assertMutable();
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
        held(bytes);
        this.activity++;
        return true;
      },
      charge: (bytes: number): boolean => {
        this.assertMutable();
        if (
          this.#loans.length > 0 ||
          this.#reads.length > 0 ||
          this.#holders.length > 0 ||
          this.#admissions.length > 0
        )
          return false;
        if (this.#missing(bytes, false) > 0) return false;
        this.#take(bytes);
        held(bytes);
        this.#observeGrant("cache", "charge", bytes);
        return true;
      },
      release: (bytes: number): void => {
        this.assertMutable();
        held(-bytes);
        this.give(bytes);
      },
      unregister: (): void => {
        this.assertMutable();
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
