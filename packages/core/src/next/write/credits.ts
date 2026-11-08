/**
 * The write owner's native accounts. A step receives already admitted credit, not a wrapper
 * allocation counter: storage may transfer a real buffer out through native takeFrom.
 */
import type { Grant, MemoryGovernor } from "../memory/governor.js";
import { GrantClosedError } from "../memory/governor.js";
import type { MaybePromise } from "../access/key-index.js";
import { releaseIdle } from "./budget.js";

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
  #metadataHeadroom = 0;

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
    return releaseIdle(this.rows, this.minimum.rows);
  }

  /** Keep the next metadata step prepaid in addition to actual native live allocations. */
  prepareMetadata(bytes: number, signal?: AbortSignal): MaybePromise<void> {
    this.#metadataHeadroom = bytes;
    const wanted = this.metadata.allocated + bytes;
    return this.metadata.held >= wanted ? undefined : this.growTo(this.metadata, wanted, signal);
  }

  reclaimMetadata(): number {
    return releaseIdle(this.metadata, this.metadata.allocated + this.#metadataHeadroom);
  }

  /** Fund a larger bounded step before it creates inputs or provisional publication state. */
  async growTo(grant: Grant, wanted: number, signal?: AbortSignal): Promise<void> {
    const stop =
      signal === undefined ? this.#stop.signal : AbortSignal.any([signal, this.#stop.signal]);
    for (;;) {
      this.#check();
      stop.throwIfAborted();
      const missing = wanted - grant.held;
      if (missing <= 0) return;
      const idle =
        this.rows.held - Math.max(this.minimum.rows, this.rows.allocated + this.rows.loaned);
      const moved = grant === this.rows ? 0 : Math.min(missing, Math.max(0, idle));
      if (moved > 0 && !this.governor.transferCredit(this.rows, grant, moved))
        throw new Error("Idle write credit was not transferred");
      if (grant.held >= wanted || grant.tryGrow(wanted - grant.held)) return;
      const [credit] = await this.governor.admitGroup({
        label: `grow ${grant.label}`,
        grants: [
          {
            label: "write growth credit",
            minimumBytes: Math.min(
              wanted - grant.held,
              this.governor.capBytes - this.governor.reserveBytes,
            ),
            spillable: false,
          },
        ],
        signal: stop,
      });
      if (!credit) throw new Error("Missing write growth credit");
      try {
        this.#check();
        stop.throwIfAborted();
        // Another publication may have funded the same headroom while this admission waited.
        const missing = Math.max(0, wanted - grant.held);
        if (!this.governor.transferCredit(credit, grant, Math.min(credit.held, missing)))
          throw new Error("Admitted write credit was not transferred");
      } finally {
        credit.close();
      }
    }
  }

  prepareMerge(bytes: number, signal?: AbortSignal): MaybePromise<void> {
    const wanted = Math.max(0, bytes - (this.#mergeActive?.held ?? 0));
    return this.merge.held >= wanted ? undefined : this.growTo(this.merge, wanted, signal);
  }

  async acquireMerge(label: string, bytes: number): Promise<Grant> {
    if (this.#mergeActive) throw new Error("Merge credit is already in use");
    await this.prepareMerge(bytes);
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
        this.rows.held - Math.max(this.minimum.rows, this.rows.allocated + this.rows.loaned),
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
    grant.close();
  }

  cancel(reason: unknown): void {
    this.#stop.abort(reason);
  }

  close(): void {
    if (this.#closed) return;
    if (this.#mergeActive)
      throw new Error("Native write steps must settle before their accounts close");
    this.#closed = true;
    this.cancel(new GrantClosedError("write owner minimum"));
    this.signal?.removeEventListener("abort", this.#abort);
    for (const grant of [this.rows, this.metadata, this.commits, this.merge]) grant.close();
  }
}
