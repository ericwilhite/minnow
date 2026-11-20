/**
 * Query preparation never waits while it holds memory (memory governor: no hold-and-wait).
 *
 * From the query's first preparation admission (retained source recipes, sizing scratch, planning
 * probes, parameter sizing) through its reservation, every admission is served from one held
 * preparation credit, or else only by an immediate admission. When one would wait, preparation
 * throws PreparationWouldWait; the execution releases everything the attempt holds (recipes,
 * scratch, credit), waits holding nothing for a credit of max(2 x the previous request, held +
 * requested), and restarts preparation from scratch on a fresh runtime. Requests grow
 * monotonically to the governor's capacity, so attempts are bounded; a need above the cap is the
 * typed MemoryAdmissionError (53200), never a refusal below it.
 *
 * An attempt that holds nothing at all just waits for the admission (that is already a wait
 * holding nothing; there is nothing to release or redo). Admissions the governor serves at once
 * are admitted as on the ordinary path, through the same group call.
 *
 * The credit is admitted as the very admission that would have waited: the same group label and
 * named parts, plus one `…/preparation-credit` part for the rest. The next attempt receives those
 * part grants when it asks for the same admission again; everything else it needs is carved from
 * the credit part. So what the governor sees a query wait for is still its own named minimum.
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
} from "../memory/governor.js";

type Part = NativeGrantGroupOptions["grants"][number];
const samePart = (a: Part, b: Part | undefined) =>
  b?.label === a.label && b.minimumBytes === a.minimumBytes && b.spillable === a.spillable;

/** A preparation admission could not be served without waiting. */
export class PreparationWouldWait extends Error {
  constructor(
    readonly label: string,
    readonly requested: number,
    /** Whether the request needed the progress reserve (more than cap - reserve). */
    readonly includesProgress: boolean,
    /** Bytes the attempt held at that moment (live preparation grants). */
    readonly held: number,
    /** The named parts of the admission that would have waited (their sum is `requested`). */
    readonly parts: readonly Part[],
  ) {
    super(`${label}: query preparation would wait while it holds memory`);
    this.name = "PreparationWouldWait";
  }
}

export class PreparationGovernor implements MemoryGovernor {
  #credit: Grant | null = null;
  #preparing = true;
  /** Grants this attempt took; their live holdings are the "held" part of the next request. */
  readonly #taken = new Set<Grant>();
  /** The admission the last attempt would have waited for, admitted with the credit. */
  #waited: { readonly label: string; readonly parts: readonly Part[]; grants: Grant[] } | null =
    null;
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
  registerCache(cache: EvictableCache): ReturnType<MemoryGovernor["registerCache"]> {
    return this.governor.registerCache(cache);
  }
  transferCredit(source: Grant, target: Grant, bytes: number): boolean {
    return this.governor.transferCredit(source, target, bytes);
  }

  /** From the credit, else an immediate admission; never waits. */
  tryAdmit(options: Omit<AdmitOptions, "signal">): Grant | null {
    if (!this.#preparing) return this.governor.tryAdmit(options);
    if (this.#waited?.label === options.label) {
      // The admitted wait itself, asked for again: as one grant, or (for a group asked through
      // an immediate whole first) by its group request.
      const [grant] = this.#claim(options.label, [options]) ?? [];
      return grant ?? null;
    }
    const carved =
      this.#credit === null
        ? null
        : this.governor.carve(this.#credit, options.label, options.spillable, options.minimumBytes);
    const grant =
      carved ??
      (options.minimumBytes > this.capBytes - this.reserveBytes
        ? null
        : this.governor.tryAdmit(options));
    if (grant !== null) this.#taken.add(grant);
    return grant;
  }
  /** Live bytes held by this attempt's grants. */
  #live(): number {
    let bytes = 0;
    for (const grant of this.#taken) bytes += grant.held;
    return bytes;
  }
  /**
   * Holding nothing (no live grant, credit or admitted wait), an attempt may simply wait: that is
   * already a wait holding nothing, and there is nothing to release or prepare again.
   */
  #holdsNothing(): boolean {
    return this.#credit === null && this.#waited === null && this.#live() === 0;
  }
  #took(grant: Grant): Grant {
    if (this.#preparing) this.#taken.add(grant);
    return grant;
  }
  /** The admitted wait's grants when this request is that same admission, else null. */
  #claim(label: string, parts: readonly Part[]): Grant[] | null {
    const waited = this.#waited;
    if (waited?.label !== label) return null;
    if (
      waited.parts.length !== parts.length ||
      !waited.parts.every((part, i) => samePart(part, parts[i]))
    ) {
      // A different request under the same label: keep the bytes in the credit instead.
      if (parts.length === 1 && waited.parts.length > 1) return null;
      this.#dropWaited();
      return null;
    }
    this.#waited = null;
    for (const grant of waited.grants) this.#taken.add(grant);
    return waited.grants;
  }
  /** Returns an unclaimed admitted wait to the credit (or to the governor without one). */
  #dropWaited(): void {
    const waited = this.#waited;
    if (waited === null) return;
    this.#waited = null;
    for (const grant of waited.grants) {
      if (this.#credit !== null && grant.allocated === 0 && grant.loaned === 0)
        this.governor.transferCredit(grant, this.#credit, grant.held);
      grant.close();
    }
  }
  #room(): number {
    const credit = this.#credit;
    return credit === null ? 0 : credit.held - credit.allocated - credit.loaned;
  }
  admit(options: AdmitOptions): Promise<Grant> {
    if (!this.#preparing) return this.governor.admit(options);
    options.signal?.throwIfAborted();
    const grant = this.tryAdmit(options);
    if (grant === null && this.#holdsNothing())
      return this.governor.admit(options).then((admitted) => this.#took(admitted));
    return grant === null
      ? Promise.reject(
          new PreparationWouldWait(options.label, options.minimumBytes, false, this.#live(), [
            {
              label: options.label,
              minimumBytes: options.minimumBytes,
              spillable: options.spillable,
            },
          ]),
        )
      : Promise.resolve(grant);
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
      const claimed = this.#claim(options.label, options.grants);
      if (claimed !== null) return Promise.resolve(claimed);
      const progress = options.includesProgress === true;
      const capacity = progress ? this.capBytes : this.capBytes - this.reserveBytes;
      if (total > capacity) throw new MemoryAdmissionError(options.label, total, capacity);
      // All or nothing. From the credit, each part is carved directly (so it returns there).
      if (this.#credit !== null && this.#room() >= total) {
        const credit = this.#credit;
        return Promise.resolve(this.#split(credit, options.grants, false));
      }
      // Otherwise immediately, in admitWorkingSet's order: one immediate whole split into the
      // parts, else the governor's own group when it serves that group at once (a private-
      // progress group may use the reserve). Never a queued wait.
      const whole =
        total > this.capBytes - this.reserveBytes
          ? null
          : this.governor.tryAdmit({ label: options.label, minimumBytes: total, spillable: false });
      if (whole !== null) return Promise.resolve(this.#split(whole, options.grants, true));
      if (this.governor.canAdmitNow(total, progress)) {
        // Any idle pool was registered already; the parts are an ordinary group.
        const group: NativeGrantGroupOptions = {
          label: options.label,
          grants: options.grants,
          includesProgress: progress,
          ...(options.signal === undefined ? {} : { signal: options.signal }),
        };
        return this.governor.admitGroup(group).then((grants) => {
          for (const grant of grants) this.#taken.add(grant);
          return grants;
        });
      }
      if (this.#holdsNothing()) {
        const group: NativeGrantGroupOptions = {
          label: options.label,
          grants: options.grants,
          includesProgress: progress,
          ...(options.signal === undefined ? {} : { signal: options.signal }),
        };
        return this.governor
          .admitGroup(group)
          .then((grants) => grants.map((grant) => this.#took(grant)));
      }
      throw new PreparationWouldWait(options.label, total, progress, this.#live(), options.grants);
    } catch (error) {
      return Promise.reject(error instanceof Error ? error : new Error(String(error)));
    }
  }
  /** Carves `parts` from `source` (all or nothing); a transient whole is then closed. */
  #split(source: Grant, parts: NativeGrantGroupOptions["grants"], transient: boolean): Grant[] {
    const grants: Grant[] = [];
    try {
      for (const part of parts) {
        const grant = this.governor.carve(source, part.label, part.spillable, part.minimumBytes);
        if (grant === null) throw new Error("A carved preparation group did not split");
        grants.push(grant);
      }
    } catch (error) {
      for (const done of grants) done.close();
      throw error;
    } finally {
      if (transient) source.close();
    }
    for (const grant of grants) this.#taken.add(grant);
    return grants;
  }

  /**
   * After the attempt released everything it held: waits, holding nothing, for the next credit.
   * The request is max(2 x the previous request, held + requested), capped at the capacity.
   */
  async regrow(wait: PreparationWouldWait, signal: AbortSignal): Promise<void> {
    if (!this.#preparing) throw new TypeError("Preparation already reserved");
    this.#credit?.close();
    this.#credit = null;
    this.#dropWaited();
    const capacity = this.capBytes - (wait.includesProgress ? 0 : this.reserveBytes);
    const wanted = wait.held + wait.requested;
    if (wanted > capacity) throw new MemoryAdmissionError(wait.label, wanted, capacity);
    this.#need = Math.min(capacity, Math.max(2 * this.#need, wanted));
    this.#taken.clear();
    this.#attempts++;
    // The admission that would have waited, with its own label and parts, plus the credit
    // (need >= held + requested, so the credit part covers at least what the attempt held).
    const grants = await this.governor.admitGroup({
      label: wait.label,
      includesProgress: wait.includesProgress,
      signal,
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
    this.#credit = credit;
    this.#waited = { label: wait.label, parts: wait.parts, grants: grants.slice(0, -1) };
  }

  /** The reservation is installed: release the unused credit and pass admissions through. */
  finish(): void {
    this.#preparing = false;
    this.#taken.clear();
    this.#dropWaited();
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

/** The facade for a request's governor, or null when it is not the reference governor. */
export function preparationGovernor(
  governor: MemoryGovernor,
  label?: string,
): PreparationGovernor | null {
  return governor instanceof Governor ? new PreparationGovernor(governor, label) : null;
}
