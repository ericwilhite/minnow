/** Checkpoint intervals for the owner-ready bench. Reads are never discarded between markers.
 * Wall-only markers are synchronous. Profiling waits exclude their own wall time, but any I/O
 * arriving during one is charged to the following interval. Background writer work may overlap
 * host setup/count; these are observed intervals, not causal attribution to a component.
 */
import type { OwnerPhase, OwnerReopenPhase, ReadCounts } from "./next-storage-owner-reopen.js";
export class OwnerPhaseMeter {
  #last: ReadCounts;
  #since: number;
  constructor(
    readonly counts: ReadCounts,
    readonly phase: (phase: OwnerPhase) => void | Promise<void>,
    readonly now: () => number = () => performance.now(),
    started = now(),
  ) {
    this.#last = { ...counts };
    this.#since = started;
  }
  async finish(name: OwnerReopenPhase): Promise<OwnerPhase> {
    const end = this.now();
    const value: OwnerPhase = {
      phase: name,
      wallMs: end - this.#since,
      reads: {
        payloadBytes: this.counts.payloadBytes - this.#last.payloadBytes,
        numericBytes: this.counts.numericBytes - this.#last.numericBytes,
        reads: this.counts.reads - this.#last.reads,
      },
    };
    // Capture before callbacks can suspend or run other work; their reads belong to the next
    // interval. A wall-only marker adds no await of its own.
    this.#since = end;
    this.#last = { ...this.counts };
    const pending = this.phase(value);
    if (pending !== undefined) {
      await pending;
      this.#since = this.now();
    }
    return value;
  }
}
