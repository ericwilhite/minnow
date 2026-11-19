/**
 * The write owner's native accounts. A step receives already admitted credit, not a wrapper
 * allocation counter: storage may transfer a real buffer out through native takeFrom.
 */
import type { Grant, MemoryGovernor } from "../memory/governor.js";
import { GrantClosedError, MemoryAdmissionError } from "../memory/governor.js";
import type { MaybePromise } from "../access/key-index.js";
import { releaseIdle } from "./budget.js";

/** Fixed bookkeeping for the one prepared native row step. */
export const WRITE_ROW_STEP_BYTES = 256;
export interface PreparedWriteCredit {
  /** Fails after consumption/owner cancellation; it never requests admission. */
  check(): void;
  /** Return only the reservation. Accepted native buffers keep their own charge. */
  release(): void;
}

export interface WriteMinimum {
  readonly rows: number;
  readonly metadata: number;
  readonly commits: number;
  readonly merge: number;
}

export class WriteCredits {
  readonly rows: Grant;
  readonly metadata: Grant;
  readonly commits: Grant;
  readonly merge: Grant;
  readonly #stop = new AbortController();
  readonly #abort: () => void;
  #closed = false;
  #mergeActive: Grant | null = null;
  #mergeFloor = 0;
  #commitFunding = 0;
  #commitMetadataFunding = 0;
  #mergeMetadataFunding = 0;
  #metadataHeadroom = 0;
  #rowMinimum: number;
  #rowFloor = 0;
  #rowStep = 0;
  #nextRowStep = 0;
  #changed: ((growth?: boolean) => void) | null = null;

  /** One write-owner controller, not a listener or account per row. */
  onNativeStepChange(changed: ((growth?: boolean) => void) | null): void {
    this.#changed = changed;
  }

  /** Potential next safe step. Its metadata headroom must be funded before publication
   * as a boundary; a provisional publication can temporarily make it exceed holdings.
   * The active merge and its remaining prepaid credit form ONE working minimum. */
  protectedBytes(
    mergeMinimum = this.minimum.merge,
    metadataHeadroom = this.#metadataHeadroom,
  ): number {
    const active = this.#mergeActive?.held ?? 0;
    return (
      Math.max(this.#rowMinimum, this.rows.allocated + this.rows.loaned, this.#rowFloor) +
      this.metadata.allocated +
      Math.max(
        this.metadata.loaned,
        metadataHeadroom,
        this.#commitMetadataFunding - this.metadata.allocated,
        this.#mergeMetadataFunding - this.metadata.allocated,
      ) +
      Math.max(this.minimum.commits, this.commits.held) +
      Math.max(
        this.minimum.merge,
        mergeMinimum,
        this.#mergeFloor,
        active + this.merge.allocated + this.merge.loaned,
      )
    );
  }

  #nativeReady(): boolean {
    return (
      this.rows.held >= Math.max(this.#rowMinimum, this.#rowFloor) &&
      this.commits.held >= this.minimum.commits &&
      this.merge.held >= Math.max(0, this.minimum.merge - (this.#mergeActive?.held ?? 0))
    );
  }

  /** Recovery-only buffers are not part of a later native producer's minimum. */
  get rowMinimum(): number {
    return this.#rowMinimum;
  }

  /** Admit the native producer's entire companion workspace together. Idle query
   * reclaim may have donated any account since the previous statement. */
  prepareNativeMinimum(
    headroom: number,
    signal?: AbortSignal,
    includesProgress = false,
  ): MaybePromise<void> {
    const metadata = this.metadata.allocated + headroom;
    if (this.#nativeReady() && this.metadata.held >= metadata) {
      return this.prepareMetadata(headroom, signal, includesProgress);
    }
    return this.#growTo(this.metadata, metadata, signal, 0, includesProgress, undefined, true);
  }

  get hasLoans(): boolean {
    return !!(
      this.rows.loaned ||
      this.metadata.loaned ||
      this.commits.loaned ||
      this.merge.loaned ||
      this.#mergeActive?.loaned
    );
  }
  get hasActiveMerge(): boolean {
    return this.#mergeActive !== null;
  }

  /** Return high-water estimates without using a live primary or merge's progress. */
  reclaimNativeStep(mergeMinimum = this.minimum.merge): void {
    this.reclaimRows();
    this.reclaimMetadata();
    releaseIdle(
      this.merge,
      Math.max(
        this.#mergeFloor,
        Math.max(0, Math.max(this.minimum.merge, mergeMinimum) - (this.#mergeActive?.held ?? 0)),
      ),
    );
  }

  #workingMinimum: () => Partial<WriteMinimum> = () => ({});

  /** Actual suspended native steps, not forecasts for a later phase. */
  bindWorkingMinimum(work: () => Partial<WriteMinimum>): void {
    this.#workingMinimum = work;
  }

  /** Includes the funded handoff before the private account is returned. */
  get hasMergeStep(): boolean {
    return this.#mergeFloor !== 0 || this.#mergeActive !== null;
  }

  /** Commit encoding and visibility installation use one atomic prepayment. */
  prepareCommit(signal?: AbortSignal): MaybePromise<void> {
    if (this.commits.held >= this.minimum.commits) {
      // The consumer has resumed. Encoding/append starts synchronously after
      // this return; its live allocations and the owner's working floor take over.
      if (this.#commitFunding !== 0) this.#commitFunding = this.#commitMetadataFunding = 0;
      return undefined;
    }
    return this.#growTo(
      this.commits,
      this.minimum.commits,
      signal,
      this.metadata.allocated + this.#metadataHeadroom,
      false,
      "commit",
    );
  }

  private constructor(
    readonly governor: MemoryGovernor,
    readonly minimum: WriteMinimum,
    readonly signal: AbortSignal | undefined,
    grants: readonly Grant[],
  ) {
    const [rows, metadata, commits, merge] = grants;
    if (!rows || !metadata || !commits || !merge)
      throw new Error("Incomplete native write admission");
    this.rows = rows;
    this.metadata = metadata;
    this.commits = commits;
    this.merge = merge;
    this.#rowMinimum = minimum.rows;
    this.#abort = () => this.cancel(signal?.reason);
    signal?.addEventListener("abort", this.#abort, { once: true });
    if (signal?.aborted) this.#abort();
  }

  static async open(
    governor: MemoryGovernor,
    minimum: WriteMinimum,
    signal?: AbortSignal,
  ): Promise<WriteCredits> {
    minimum = { ...minimum };
    const grants = await governor.admitGroup({
      label: "write owner minimum",
      grants: [
        { label: "write path", minimumBytes: minimum.rows, spillable: true },
        { label: "write path metadata", minimumBytes: minimum.metadata, spillable: true },
        { label: "write commit buffers", minimumBytes: minimum.commits, spillable: false },
        { label: "write merge credit", minimumBytes: minimum.merge, spillable: false },
      ],
      ...(signal === undefined ? {} : { signal }),
    });
    try {
      signal?.throwIfAborted();
      return new WriteCredits(governor, minimum, signal, grants);
    } catch (error) {
      for (const grant of grants) grant.close();
      throw error;
    }
  }

  #check(): void {
    this.#stop.signal.throwIfAborted();
    if (this.#closed) throw new GrantClosedError("write owner minimum");
  }

  /** Return idle estimates only; live buffers, public loans and the row working minimum stay. */
  reclaimRows(): number {
    const released = releaseIdle(this.rows, Math.max(this.#rowMinimum, this.#rowFloor));
    if (released) this.#changed?.();
    return released;
  }

  /** Retire a completed phase's row forecast; allocations, loans and an admitted
   * row step remain owned. This never asks another owner for credit. */
  retireRowsMinimum(bytes: number): number {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("Row working minimum must be a nonnegative safe integer");
    this.#rowMinimum = Math.min(this.#rowMinimum, bytes);
    return this.reclaimRows();
  }

  /** The owner calls this only between native steps. Future phase estimates are
   * revocable; live allocations, loans and a prepared row still belong to us. */
  reclaimIdleForecasts(): number {
    const working = this.#workingMinimum();
    this.#metadataHeadroom = 0;
    const released =
      releaseIdle(this.rows, Math.max(this.#rowFloor, working.rows ?? 0)) +
      releaseIdle(
        this.metadata,
        Math.max(working.metadata ?? 0, this.#commitMetadataFunding, this.#mergeMetadataFunding),
      ) +
      releaseIdle(this.commits, Math.max(working.commits ?? 0, this.#commitFunding)) +
      releaseIdle(this.merge, Math.max(this.#mergeFloor, working.merge ?? 0));
    if (released) this.#changed?.();
    return released;
  }

  /** Phase settlement may expose idle credit without freeing a live buffer. Wake
   * already queued admissions so they can ask our cooperative reclaimer again. */
  notifyReclaimable(): void {
    // A live loan can back allocations above an account's ordinary holdings.
    // Even release(0) is forbidden there: notify through a non-loan-backed
    // account without moving ownership. If every account is loan-backed, its
    // eventual repayment will itself wake the governor.
    for (const account of [this.rows, this.metadata, this.commits, this.merge]) {
      if (account.held >= account.allocated + account.loaned) {
        account.release(0);
        return;
      }
    }
  }

  /** Protect already admitted writer progress BEFORE opening the root's image lease.
   * No admission/loan here. One primary step at a time; producers have released their
   * progress before the caller funds this headroom. Reclaim/metadata/merge transfers
   * may use other idle rows, never this step's floor. Release before owner close. */
  protectRows(bytes: number): PreparedWriteCredit {
    this.#check();
    if (
      this.#rowStep !== 0 ||
      !Number.isSafeInteger(bytes) ||
      bytes < 0 ||
      !Number.isSafeInteger(this.#nextRowStep + 1) ||
      this.rows.held - this.rows.allocated - this.rows.loaned < bytes + WRITE_ROW_STEP_BYTES
    )
      throw new RangeError("Prepared row progress must already be admitted");
    if (!this.rows.allocate(WRITE_ROW_STEP_BYTES))
      throw new RangeError("Row progress metadata must be admitted");
    const id = ++this.#nextRowStep;
    this.#rowStep = id;
    this.#rowFloor = this.rows.allocated + this.rows.loaned + bytes;
    this.#changed?.();
    let released = false;
    return {
      check: () => {
        this.#check();
        if (released || this.#rowStep !== id) throw new GrantClosedError("prepared row progress");
      },
      release: () => {
        if (released) return;
        released = true;
        this.#rowStep = this.#rowFloor = 0;
        this.rows.free(WRITE_ROW_STEP_BYTES);
        this.#changed?.();
      },
    };
  }

  /** Keep the next metadata step prepaid in addition to actual native live allocations. */
  prepareMetadata(
    bytes: number,
    signal?: AbortSignal,
    includesProgress = false,
  ): MaybePromise<void> {
    const before = this.#metadataHeadroom;
    // The complete simultaneous workspace, before any of its allocations or waits.
    // Existing reader/cache allocations are temporary ownership, not this step's minimum.
    if (bytes > this.governor.capBytes)
      throw new MemoryAdmissionError("write metadata step", bytes, this.governor.capBytes);
    const wanted = this.metadata.allocated + bytes;
    if (this.metadata.held >= wanted) {
      this.#metadataHeadroom = bytes;
      if (before !== bytes) this.#changed?.();
      return undefined;
    }
    // A pending or cancelled request owns no forecast. Retain only workspace
    // already funded; install the larger requirement when admission succeeds.
    this.#metadataHeadroom = Math.min(this.#metadataHeadroom, bytes);
    if (before !== this.#metadataHeadroom) this.#changed?.();
    return this.growTo(this.metadata, wanted, signal, includesProgress);
  }

  get metadataReady(): boolean {
    return this.metadata.held >= this.metadata.allocated + this.#metadataHeadroom;
  }

  reclaimMetadata(): number {
    const released = releaseIdle(
      this.metadata,
      Math.max(
        this.metadata.allocated + this.#metadataHeadroom,
        this.#workingMinimum().metadata ?? 0,
        this.#commitMetadataFunding,
        this.#mergeMetadataFunding,
      ),
    );
    if (released) this.#changed?.();
    return released;
  }

  /** A reclaim callout must not wake its own drain when it returns no credit. */
  reclaimIdleMetadata(): number {
    this.#metadataHeadroom = 0;
    return this.reclaimMetadata();
  }

  /** Cancelled provisional work can lower its forecast without attempting fresh admission.
   * Existing allocations, loans and a newer/higher live minimum remain owned. */
  retireMetadataHeadroom(bytes: number): number {
    this.#metadataHeadroom = Math.min(this.#metadataHeadroom, bytes);
    const returned = this.reclaimMetadata();
    this.notifyReclaimable();
    return returned;
  }

  /** Fund a next step after its provisional state has unwound. Only actual live
   * bytes and the complete minimum of an already-running step survive the wait. */
  growTo(
    grant: Grant,
    wanted: number,
    signal?: AbortSignal,
    includesProgress = false,
  ): Promise<void> {
    return this.#growTo(grant, wanted, signal, 0, includesProgress);
  }

  async #growTo(
    grant: Grant,
    wanted: number,
    signal?: AbortSignal,
    metadata = 0,
    includesProgress = false,
    funding?: "commit" | "merge",
    nativeMinimum = false,
  ): Promise<void> {
    const stop =
      signal === undefined ? this.#stop.signal : AbortSignal.any([signal, this.#stop.signal]);
    const accounts = [this.rows, this.metadata, this.commits, this.merge];
    const target = accounts.indexOf(grant);
    if (target < 0) throw new Error("Growth must belong to the native write owner");
    for (;;) {
      this.#check();
      stop.throwIfAborted();
      if (
        grant.held >= wanted &&
        this.metadata.held >= metadata &&
        (!nativeMinimum || this.#nativeReady())
      ) {
        this.#rememberFundedMetadata(grant, wanted, metadata, funding);
        return;
      }
      this.#changed?.(true);
      const working = this.#workingMinimum();
      const floors = [
        Math.max(this.#rowFloor, working.rows ?? 0),
        Math.max(working.metadata ?? 0, this.#commitMetadataFunding, this.#mergeMetadataFunding),
        Math.max(working.commits ?? 0, this.#commitFunding),
        Math.max(this.#mergeFloor, working.merge ?? 0),
      ];
      const desired = accounts.map((account, i) =>
        Math.max(account.allocated + account.loaned, floors[i] ?? 0),
      );
      if (nativeMinimum) {
        desired[0] = Math.max(desired[0] ?? 0, this.#rowMinimum);
        desired[2] = Math.max(desired[2] ?? 0, this.minimum.commits);
        desired[3] = Math.max(desired[3] ?? 0, this.minimum.merge - (this.#mergeActive?.held ?? 0));
      }
      desired[target] = Math.max(desired[target] ?? 0, wanted);
      desired[1] = Math.max(desired[1] ?? 0, metadata);
      // First preserve warm WAL/merge windows when immediate credit can fund
      // this step. Before ANY wait, retry with every unused forecast available;
      // retaining a warm window must never turn into hold-and-wait.
      for (let pass = 0; pass < 2; pass++) {
        for (let i = 0; i < accounts.length; i++) {
          const targetAccount = accounts[i];
          if (!targetAccount) continue;
          for (const j of [0, 2, 3, 1]) {
            const source = accounts[j];
            if (!source || i === j) continue;
            const missing = Math.max(0, (desired[i] ?? 0) - targetAccount.held);
            if (missing === 0) break;
            let retained = desired[j] ?? 0;
            if (pass === 0 && j === 2) retained = Math.max(retained, this.minimum.commits);
            if (pass === 0 && j === 3)
              retained = Math.max(retained, this.minimum.merge - (this.#mergeActive?.held ?? 0));
            const spare = Math.max(0, source.held - retained);
            const moved = Math.min(missing, spare);
            if (moved > 0 && !this.governor.transferCredit(source, targetAccount, moved))
              throw new Error("Idle native phase credit was not transferred");
            if (moved > 0 && source === this.metadata) this.#trimMetadataHeadroom();
          }
        }
        if (
          grant.held >= wanted &&
          this.metadata.held >= metadata &&
          (!nativeMinimum || this.#nativeReady())
        ) {
          this.#rememberFundedMetadata(grant, wanted, metadata, funding);
          return;
        }
        if (
          (!nativeMinimum || this.#nativeReady()) &&
          this.metadata.held >= metadata &&
          (pass === 1 ||
            wanted - grant.held <=
              this.governor.capBytes - this.governor.reserveBytes - this.governor.usedBytes) &&
          grant.tryGrow(Math.max(0, wanted - grant.held))
        ) {
          this.#changed?.(true);
          this.#rememberFundedMetadata(grant, wanted, metadata, funding);
          return;
        }
      }
      this.#changed?.(true);
      // Dormant row, WAL, metadata and merge forecasts are all revocable. Admit
      // the next phase together instead of waiting for a deficit while holding
      // another owner's progress hostage. A live step's prepaid buffers stay owned.
      for (let i = 0; i < accounts.length; i++) {
        const account = accounts[i];
        if (account) releaseIdle(account, floors[i] ?? 0);
      }
      this.#trimMetadataHeadroom();
      const credits = await this.governor.admitGroup({
        label: `complete grow ${grant.label}`,
        ...(includesProgress ? { includesProgress: true } : {}),
        grants: accounts.map((account, i) => ({
          label: `write growth ${account.label}`,
          minimumBytes: Math.max(0, (desired[i] ?? 0) - account.held),
          spillable: false,
        })),
        signal: stop,
      });
      try {
        this.#check();
        stop.throwIfAborted();
        for (let i = 0; i < accounts.length; i++) {
          const account = accounts[i],
            credit = credits[i];
          if (!account || !credit) throw new Error("Incomplete native growth admission");
          const missing = Math.max(0, (desired[i] ?? 0) - account.held);
          if (!this.governor.transferCredit(credit, account, Math.min(credit.held, missing)))
            throw new Error("Admitted write credit was not transferred");
        }
        this.#rememberFundedMetadata(grant, wanted, metadata, funding);
      } finally {
        for (const credit of credits) credit.close();
      }
    }
  }

  #rememberFundedMetadata(
    grant: Grant,
    wanted: number,
    metadata: number,
    funding?: "commit" | "merge",
  ): void {
    const nextMetadata = Math.max(grant === this.metadata ? wanted : 0, metadata);
    const before = this.#metadataHeadroom;
    if (nextMetadata > 0)
      this.#metadataHeadroom = Math.max(
        this.#metadataHeadroom,
        nextMetadata - this.metadata.allocated,
      );
    // These are already funded next steps, not estimates for a later phase. Pin
    // the handoff before returning a promise (and before closing temporary grants
    // can reenter admission), so concurrent funding cannot trade it back and forth.
    if (funding === "commit") {
      this.#commitFunding = Math.max(this.#commitFunding, wanted);
      this.#commitMetadataFunding = Math.max(this.#commitMetadataFunding, metadata);
    } else if (funding === "merge") {
      this.#mergeFloor = Math.max(this.#mergeFloor, wanted);
      this.#mergeMetadataFunding = Math.max(this.#mergeMetadataFunding, metadata);
    }
    if (before !== this.#metadataHeadroom || funding !== undefined) this.#changed?.();
  }

  #trimMetadataHeadroom(): void {
    // A later phase cannot reserve a forecast whose credit was already donated.
    // Active metadata steps are protected by workingMinimum and cannot donate it.
    this.#metadataHeadroom = Math.min(
      this.#metadataHeadroom,
      Math.max(0, this.metadata.held - this.metadata.allocated - this.metadata.loaned),
    );
  }

  prepareMerge(bytes: number, signal?: AbortSignal): MaybePromise<void> {
    return this.#prepareMerge(bytes, signal, false);
  }

  #prepareMerge(
    bytes: number,
    signal: AbortSignal | undefined,
    complete: boolean,
    headroom = this.#metadataHeadroom,
  ): MaybePromise<void> {
    const wanted = Math.max(0, bytes - (this.#mergeActive?.held ?? 0));
    const metadata = this.metadata.allocated + headroom;
    return this.merge.held >= wanted && this.metadata.held >= metadata
      ? undefined
      : this.#growTo(
          this.merge,
          wanted,
          signal,
          metadata,
          complete,
          complete ? "merge" : undefined,
        );
  }

  async acquireMerge(label: string, bytes: number): Promise<Grant> {
    if (this.#mergeActive || this.#mergeFloor !== 0)
      throw new Error("Merge credit is already in use");
    const headroom = this.#metadataHeadroom;
    try {
      for (;;) {
        // mergeRuns consumes a ByteBudget: its entire I/O/encoder workspace is
        // prepaid here and never borrows the shared legacy reserve (rules2/3/5).
        const ready = this.#prepareMerge(bytes, undefined, true, headroom);
        if (ready instanceof Promise) await ready;
        this.#check();
        // A foreign admission can reclaim idle forecasts while preparation awaits.
        // Pin only funded credit, synchronously with its last check.
        if (this.merge.held >= bytes && this.metadata.held >= this.metadata.allocated + headroom)
          break;
      }
      this.#mergeFloor = bytes;
      return await this.#takeMerge(label, bytes);
    } finally {
      this.#mergeFloor = 0;
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- #takeMerge installs the active grant across its await.
      if (this.#mergeActive === null) this.#mergeMetadataFunding = 0;
    }
  }

  async #takeMerge(label: string, bytes: number): Promise<Grant> {
    const [grant] = await this.governor.admitGroup({
      label,
      grants: [{ label, minimumBytes: 0, spillable: false }],
      signal: this.#stop.signal,
    });
    if (!grant) throw new Error("Missing native merge target");
    try {
      this.#check();
      // Prefer idle write estimates. Exchange the same prepaid amount back to the writer,
      // then return all remaining idle estimates globally. The active merge keeps its minimum.
      const idle = Math.max(
        0,
        this.rows.held -
          Math.max(this.#rowMinimum, this.rows.allocated + this.rows.loaned, this.#rowFloor),
      );
      const returned = Math.min(bytes, idle);
      if (returned > 0) {
        if (!this.governor.transferCredit(this.rows, grant, returned))
          throw new Error("Idle write credit did not fund the merge");
        if (!this.governor.transferCredit(this.merge, this.rows, returned))
          throw new Error("Prepaid merge credit did not replace the write estimate");
      }
      if (!this.governor.transferCredit(this.merge, grant, bytes - returned))
        throw new Error("Prepaid merge credit did not fund the step");
      this.#mergeActive = grant;
      this.reclaimRows();
      this.#changed?.();
      return grant;
    } catch (error) {
      const idle = grant.held - grant.allocated - grant.loaned;
      if (idle > 0) this.governor.transferCredit(grant, this.merge, idle);
      grant.close();
      throw error;
    }
  }

  releaseMerge(grant: Grant): void {
    if (this.#mergeActive !== grant) throw new Error("Wrong native merge owner");
    if (grant.allocated !== 0 || grant.loaned !== 0)
      throw new Error("Merge buffers remain live at release");
    if (!this.governor.transferCredit(grant, this.merge, grant.held))
      throw new Error("Native merge credit was not returned");
    this.#mergeActive = null;
    this.#mergeMetadataFunding = 0;
    grant.close();
    // A completed step cannot keep an oversized future forecast while another
    // complete execution waits. The next merge prepares its full step anew.
    releaseIdle(this.merge, this.minimum.merge);
    this.#changed?.();
  }

  cancel(reason: unknown): void {
    this.#commitFunding = this.#commitMetadataFunding = this.#mergeMetadataFunding = 0;
    this.#stop.abort(reason);
  }

  /** Closing readers may inherit the existing row account, without re-admission/copying. */
  close({ keepRowsForReaders = false }: { readonly keepRowsForReaders?: boolean } = {}): void {
    if (this.#closed) return;
    if (this.#mergeActive || this.#mergeFloor !== 0 || this.#rowStep !== 0)
      throw new Error("Native write steps must settle before their accounts close");
    this.#closed = true;
    this.#changed = null;
    this.cancel(new GrantClosedError("write owner minimum"));
    this.signal?.removeEventListener("abort", this.#abort);
    for (const grant of [this.metadata, this.commits, this.merge]) grant.close();
    if (!keepRowsForReaders) this.rows.close();
  }
}
