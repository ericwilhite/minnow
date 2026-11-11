/** Fund synchronous hash transitions before entering them. No allocate() may grow a grant. */
import type { MaybePromise } from "../../access/key-index.js";
import type { Grant } from "../../memory/governor.js";
import type { ByteCharge } from "../../vector/varlen.js";

export class HashHeadroomError extends Error {
  constructor(
    readonly requested: number,
    readonly admitted: number,
  ) {
    super(
      `Hash transition exceeded admitted headroom (${String(requested)} > ${String(admitted)})`,
    );
    this.name = "HashHeadroomError";
  }
}

/** `minimum` includes compiler buffers already charged to the same grant. Only a
 * boundary can raise the ceiling. Reclaim can suspend with cancellation, but a
 * pinned competitor causes false: the caller freezes new groups and spills.
 */
export class HashWorkspace implements ByteCharge {
  #ceiling: number;
  #closed = false;
  constructor(
    readonly grant: Grant,
    minimum: number,
    readonly stateCapacity: () => number = () => grant.held,
  ) {
    this.#ceiling = minimum;
  }
  get admitted(): number {
    return this.#ceiling;
  }
  get used(): number {
    return this.grant.allocated;
  }
  allocate(bytes: number): boolean {
    if (this.#closed) return false;
    const next = this.grant.allocated + bytes;
    if (next > this.#ceiling || next > this.stateCapacity())
      throw new HashHeadroomError(next, Math.min(this.#ceiling, this.stateCapacity()));
    // The ceiling was funded before the transition. An implicit tryGrow here
    // would hide an understated demand estimate and reintroduce the old defect.
    if (!this.grant.allocate(bytes)) throw new HashHeadroomError(next, this.#ceiling);
    return true;
  }
  free(bytes: number): void {
    this.grant.free(bytes);
  }
  /** Peak total allocation, not a delta. No transition has started yet. */
  prepare(peak: number, signal?: AbortSignal): MaybePromise<boolean> {
    signal?.throwIfAborted();
    if (this.#closed) return false;
    if (!Number.isSafeInteger(peak) || peak < 0) throw new RangeError("Invalid hash peak");
    if (peak <= this.#ceiling && peak <= this.stateCapacity()) return true;
    const pending = this.grant.reclaimRoom?.(Math.max(0, peak - this.grant.allocated), signal);
    if (pending === undefined) return false;
    return pending.then((room) => {
      signal?.throwIfAborted();
      if (this.#closed || !room) return false;
      this.#ceiling = Math.min(peak, this.stateCapacity());
      return this.#ceiling >= peak;
    });
  }
  close(): void {
    if (this.#closed) return;
    if (this.grant.allocated !== 0) throw new Error("Hash workspace closed with live buffers");
    this.#closed = true;
  }
}
