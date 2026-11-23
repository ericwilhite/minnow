/**
 * Query preparation never waits while it holds memory (memory governor: no hold-and-wait).
 *
 * From the query's first preparation admission (retained source recipes, sizing scratch, planning
 * probes, parameter sizing) through its reservation, every admission is immediate (the governor
 * serves it at once: an immediate whole, or the governor's own group call with its label and
 * named parts) or, after a restart, served from the preparation credit, a single pool: every
 * grant the attempt obtains returns the bytes it releases to that credit while it is open, so a
 * freed scratch grant's bytes serve the next phase, and the credit grows only by immediate
 * admissions. An attempt that holds nothing at all may wait as usual: that is already a wait
 * holding nothing.
 *
 * Otherwise preparation throws PreparationWouldWait. The execution releases everything the attempt
 * holds (recipes, scratch, credit), waits holding nothing, and prepares again from scratch on a
 * fresh runtime. That wait is the admission that would have waited (its label and named parts)
 * plus one `…/preparation-credit` part; once admitted, all of it is the next attempt's credit.
 *
 * The request covers the attempt's peak simultaneous need (live grants at their peak, or live
 * grants plus the request that would have waited), at most the usable capacity; a peak need
 * above it is the typed MemoryAdmissionError (53200). It is twice the previous request when that
 * is admissible at once, else the measured peak alone: a restart never waits for more than it
 * needs. The wait carries what the statement keeps through it (its result grant, its writer's
 * holdings: `NativeGrantGroupOptions.holdings`), so a credit that could never be served beside
 * them is refused with 53200 rather than left waiting. Each credit is larger than the last and
 * covers the stopped attempt's peak, so an attempt that repeats its admissions gets past the one
 * that stopped the last. Restarts are therefore at most ceil(log2(usable / first request)) + 1
 * that doubled, plus at most one per admission of an attempt for those that took the measured
 * peak.
 *
 * Once the reservation is installed the facade passes every admission to the real governor.
 */
import {
  Governor,
  MemoryAdmissionError,
  type AdmitOptions,
  type EvictableCache,
  type Grant,
  type MemoryGovernor,
  type NativeGrantGroupOptions,
  type WaitingHoldings,
} from "../memory/governor.js";

type Part = NativeGrantGroupOptions["grants"][number];

/** A preparation admission could not be served without waiting. */
export class PreparationWouldWait extends Error {
  constructor(
    readonly label: string,
    readonly requested: number,
    /** Whether the request needed the progress reserve (more than cap - reserve). */
    readonly includesProgress: boolean,
    /** Bytes the attempt's live grants held at that moment. */
    readonly held: number,
    /** The named parts of the admission that would have waited (their sum is `requested`). */
    readonly parts: readonly Part[],
    /** The attempt's peak simultaneous need, including this request. */
    readonly needed = held + requested,
  ) {
    super(`${label}: query preparation would wait while it holds memory`);
    this.name = "PreparationWouldWait";
  }
}

export class PreparationGovernor implements MemoryGovernor {
  #credit: Grant | null = null;
  #preparing = true;
  /** Grants this attempt took; their live holdings are what it holds besides the credit. */
  readonly #taken = new Set<Grant>();
  /** The attempt's peak live holdings. */
  #peak = 0;
  /** Whether the attempt made a private-progress request (its credit may use the reserve). */
  #progress = false;
  #need = 0;
  #attempts = 1;
  constructor(
    readonly governor: Governor,
    /** The owning query's label prefix (`query#<id>`), for the credit's admission label. */
    readonly label = "query",
  ) {}

  get capBytes(): number {
    return this.governor.capBytes;
  }
  get reserveBytes(): number {
    return this.governor.reserveBytes;
  }
  servableNow(own: WaitingHoldings, includesProgress?: boolean): number {
    return this.governor.servableNow(own, includesProgress);
  }
  get usedBytes(): number {
    return this.governor.usedBytes;
  }
  get peakBytes(): number {
    return this.governor.peakBytes;
  }
  /** Whether admissions are still preparation admissions (before the reservation). */
  get preparing(): boolean {
    return this.#preparing;
  }
  /** Preparation attempts so far (1 before any restart). */
  get attempts(): number {
    return this.#attempts;
  }
  /** Live bytes this attempt's grants hold (0 once it released everything). */
  get heldBytes(): number {
    return this.#live();
  }
  /** The current credit request (0 before the first restart). */
  get needBytes(): number {
    return this.#need;
  }
  /** The held preparation credit, if any (for tests and diagnostics). */
  get credit(): Grant | null {
    return this.#credit;
  }
  /**
   * The most one request can ever be granted: the cap (less the reserve unless it includes
   * private progress), less the registered lifetime holdings (Governor.usableCapacity).
   */
  usableCapacity(includesProgress = false): number {
    return this.governor.usableCapacity(includesProgress);
  }
  registerCache(cache: EvictableCache): ReturnType<MemoryGovernor["registerCache"]> {
    return this.governor.registerCache(cache);
  }
  transferCredit(source: Grant, target: Grant, bytes: number): boolean {
    return this.governor.transferCredit(source, target, bytes);
  }

  /** Unused credit bytes. */
  #room(): number {
    const credit = this.#credit;
    return credit === null ? 0 : credit.held - credit.allocated - credit.loaned;
  }
  /** Ends this attempt's tracking of its grants (they keep their bytes; no observer remains). */
  #forget(): void {
    for (const grant of this.#taken) this.governor.observeGrowth(grant, null);
    this.#taken.clear();
  }
  /**
   * Live bytes held by this attempt's grants. A closed grant holds nothing and never grows again:
   * it leaves the set here, so each call costs the grants still open, not every grant the attempt
   * ever took (sizing passes take and close thousands). Read-only, as growth observers require.
   */
  #live(): number {
    let bytes = 0;
    for (const grant of this.#taken) {
      if (!this.governor.isOpen(grant)) {
        this.#taken.delete(grant);
        continue;
      }
      bytes += grant.held;
    }
    return bytes;
  }
  /** Grants of this attempt still tracked (open, or closed since the last count). */
  get trackedGrants(): number {
    return this.#taken.size;
  }
  /**
   * Records a grant of this attempt. With a credit (after a restart), its released bytes go back
   * to the credit from now on.
   */
  #took(grant: Grant): Grant {
    if (!this.#preparing) return grant;
    if (this.#credit !== null && grant !== this.#credit) this.governor.adopt(this.#credit, grant);
    this.#taken.add(grant);
    // A grant can grow after admission (allocation past its holdings); the peak sees that too.
    this.governor.observeGrowth(grant, () => {
      if (this.#preparing) this.#peak = Math.max(this.#peak, this.#live());
    });
    this.#peak = Math.max(this.#peak, this.#live());
    return grant;
  }
  /**
   * Adds `bytes` to the credit if the governor admits them at once (FIFO order kept: nothing
   * queued). The request keeps the label of the admission it serves.
   */
  #growCredit(credit: Grant, label: string, bytes: number): boolean {
    if (bytes <= 0) return true;
    if (bytes > this.usableCapacity(false)) return false;
    const whole = this.governor.tryAdmit({ label, minimumBytes: bytes, spillable: false });
    if (whole === null) return false;
    const moved = this.governor.transferCredit(whole, credit, bytes);
    whole.close();
    return moved;
  }
  #carve(source: Grant, label: string, spillable: boolean, bytes: number): Grant | null {
    const grant = this.governor.carve(source, label, spillable, bytes);
    return grant === null ? null : this.#took(grant);
  }
  /**
   * Holding no live grant, the attempt may wait like any execution: it first returns the idle
   * credit, so the wait holds nothing.
   */
  #holdsNothing(): boolean {
    if (this.#live() !== 0) return false;
    const idle = this.#room();
    if (idle > 0) this.#credit?.release(idle);
    return this.#credit === null || this.#credit.held === 0;
  }
  #wouldWait(label: string, total: number, progress: boolean, parts: readonly Part[]) {
    const live = this.#live();
    return new PreparationWouldWait(
      label,
      total,
      progress,
      live,
      parts,
      Math.max(this.#peak, live + total),
    );
  }

  /**
   * From the credit when it has the room, else the governor's own immediate grant (the same
   * request as without the facade), adopted by any credit; never waits.
   */
  tryAdmit(options: Omit<AdmitOptions, "signal">): Grant | null {
    if (!this.#preparing) return this.governor.tryAdmit(options);
    if (options.minimumBytes > this.usableCapacity(false)) return null;
    const room = this.#room();
    if (this.#credit !== null && room > 0 && room >= options.minimumBytes)
      return this.#carve(this.#credit, options.label, options.spillable, options.minimumBytes);
    const grant = this.governor.tryAdmit(options);
    return grant === null ? null : this.#took(grant);
  }
  admit(options: AdmitOptions): Promise<Grant> {
    if (!this.#preparing) return this.governor.admit(options);
    options.signal?.throwIfAborted();
    const grant = this.tryAdmit(options);
    if (grant !== null) return Promise.resolve(grant);
    if (this.#holdsNothing())
      return this.governor.admit(options).then((admitted) => this.#took(admitted));
    return Promise.reject(
      this.#wouldWait(options.label, options.minimumBytes, false, [
        { label: options.label, minimumBytes: options.minimumBytes, spillable: options.spillable },
      ]),
    );
  }
  admitGroup(options: NativeGrantGroupOptions): Promise<readonly Grant[]> {
    if (!this.#preparing) return this.governor.admitGroup(options);
    const total = options.grants.reduce((sum, part) => sum + part.minimumBytes, 0);
    // A zero-byte group never queues (the governor installs it at once), so it passes through.
    // That includes registering a phase idle pool with no parts.
    if (total === 0) return this.governor.admitGroup(options);
    const pool = options.idleCredit;
    if (pool !== undefined) {
      // Register the shared idle pool without parts (immediate), then serve the parts like any
      // preparation group: from the credit or an immediate admission, never a wait.
      return this.governor
        .admitGroup({ label: options.label, idleCredit: pool, grants: [] })
        .then(() => this.#serveGroup(options, total));
    }
    return this.#serveGroup(options, total);
  }
  /** Serves the parts (never an idle pool, which `admitGroup` registered). */
  #serveGroup(options: NativeGrantGroupOptions, total: number): Promise<readonly Grant[]> {
    try {
      options.signal?.throwIfAborted();
      const progress = options.includesProgress === true;
      if (progress) this.#progress = true;
      const usable = this.usableCapacity(progress);
      if (total > usable) throw new MemoryAdmissionError(options.label, total, usable);
      // All or nothing: from the credit (grown at once by an immediate whole if it is short);
      // before any restart, from one immediate whole split into the parts.
      const credit = this.#credit;
      if (credit !== null) {
        if (this.#growCredit(credit, options.label, total - this.#room()))
          return Promise.resolve(this.#split(credit, options.grants));
      } else if (total <= this.usableCapacity(false)) {
        const whole = this.governor.tryAdmit({
          label: options.label,
          minimumBytes: total,
          spillable: false,
        });
        if (whole !== null) {
          try {
            return Promise.resolve(this.#split(whole, options.grants));
          } finally {
            whole.close();
          }
        }
      }
      // Else the governor's own group, when it serves that group at once (a private-progress
      // group may use the reserve), or an ordinary wait when the attempt holds nothing.
      const group: NativeGrantGroupOptions = {
        label: options.label,
        grants: options.grants,
        includesProgress: progress,
        ...(options.signal === undefined ? {} : { signal: options.signal }),
        ...(options.holdings === undefined ? {} : { holdings: options.holdings }),
      };
      if (this.governor.canAdmitNow(total, progress) || this.#holdsNothing())
        return this.governor
          .admitGroup(group)
          .then((grants) => grants.map((grant) => this.#took(grant)));
      throw this.#wouldWait(options.label, total, progress, options.grants);
    } catch (error) {
      return Promise.reject(error instanceof Error ? error : new Error(String(error)));
    }
  }
  /** Carves every part from `source` (all or nothing). */
  #split(source: Grant, parts: NativeGrantGroupOptions["grants"]): Grant[] {
    const grants: Grant[] = [];
    for (const part of parts) {
      const grant = this.#carve(source, part.label, part.spillable, part.minimumBytes);
      if (grant === null) {
        for (const done of grants) {
          this.#taken.delete(done);
          done.close();
        }
        throw new Error("A carved preparation group did not split");
      }
      grants.push(grant);
    }
    return grants;
  }

  /**
   * After the attempt released everything it held: waits, holding nothing, for the next credit.
   * The credit is twice the previous one when that is admissible at once (at most the usable
   * capacity), else the attempt's peak need: it never waits for more than it needs. `beside` is
   * what the statement still keeps while it waits (its result grant, and its writer's holdings
   * while it holds the turn): the wait carries it (`NativeGrantGroupOptions.holdings`), so a
   * credit that could never be served beside it is refused with 53200, at once or when the
   * writer's own work ends, never left waiting.
   */
  async regrow(
    wait: PreparationWouldWait,
    signal: AbortSignal,
    beside?: () => WaitingHoldings,
  ): Promise<void> {
    if (!this.#preparing) throw new TypeError("Preparation already reserved");
    this.#credit?.close();
    this.#credit = null;
    this.#forget();
    this.#peak = 0;
    const progress = wait.includesProgress || this.#progress;
    const usable = this.usableCapacity(progress);
    if (wait.needed > usable) throw new MemoryAdmissionError(wait.label, wait.needed, usable);
    // Double the credit only when that fits now. Waiting for more than the attempt needs could
    // wait for memory only this statement's own holdings keep: wait for the need itself.
    const doubled = Math.max(wait.needed, Math.min(2 * this.#need, usable));
    this.#need = this.governor.canAdmitNow(doubled, progress) ? doubled : wait.needed;
    this.#attempts++;
    // The admission that would have waited, with its own label and parts, plus the rest of the
    // credit. All of it becomes the next attempt's credit.
    const grants = await this.governor.admitGroup({
      label: wait.label,
      includesProgress: progress,
      signal,
      ...(beside === undefined ? {} : { holdings: beside }),
      grants: [
        ...wait.parts,
        {
          label: `${this.label}/preparation-credit`,
          minimumBytes: this.#need - wait.requested,
          spillable: false,
        },
      ],
    });
    const credit = grants[wait.parts.length];
    if (signal.aborted || credit === undefined || grants.length !== wait.parts.length + 1) {
      for (const grant of grants) grant.close();
      signal.throwIfAborted();
      throw new Error("Preparation credit was not admitted");
    }
    for (const part of grants.slice(0, -1)) {
      this.governor.transferCredit(part, credit, part.held);
      part.close();
    }
    this.#credit = credit;
  }

  /**
   * After a refused attempt released everything it held, before it prepares again on another
   * snapshot (exec/run.ts `#repinAfter`): no credit, nothing taken, no peak. The need stays.
   */
  reset(): void {
    if (!this.#preparing) throw new TypeError("Preparation already reserved");
    this.#credit?.close();
    this.#credit = null;
    this.#forget();
    this.#peak = 0;
  }

  /** The reservation is installed: release the unused credit and pass admissions through. */
  finish(): void {
    this.#preparing = false;
    this.#forget();
    this.#credit?.close();
    this.#credit = null;
  }
  /** Execution end or failure: no credit outlives the query. */
  close(): void {
    this.finish();
  }
}

/** The governor a facade fronts (shared per-governor state, such as the phase idle pool). */
export function baseGovernor(governor: MemoryGovernor): MemoryGovernor {
  return governor instanceof PreparationGovernor ? governor.governor : governor;
}

/**
 * Whether a preparation may keep memory (a sizing pass's segment list) while it admits more.
 * Only under the facade: an admission that would wait while anything is held restarts the
 * attempt instead (PreparationWouldWait). A native trigger body's prepaid shares refuse any
 * admission while a share is held (owner/prepaid-query.ts), so its sources are sized whole.
 */
export function holdsAcrossAdmission(governor: MemoryGovernor): boolean {
  return governor instanceof PreparationGovernor;
}

/** The facade for a request's governor, or null when it is not the reference governor. */
export function preparationGovernor(
  governor: MemoryGovernor,
  label?: string,
): PreparationGovernor | null {
  return governor instanceof Governor ? new PreparationGovernor(governor, label) : null;
}
