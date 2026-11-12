/** A retired join's native continuation. Only files, types and scalar metadata
 * survive admission waits; no old operator, parameter binding or grant does. */
import type { HashJoinProfile } from "./join.js";
import type { RecordCheckpoint } from "./record-checkpoint.js";

export class JoinCheckpoint {
  /** Additional helper lanes beyond the ordinary join recipe: reuse its I/O lane. */
  readonly restoreLoanSlots = 0;
  /** Additional workspace: checkpoint I/O uses the ordinary admitted join pool. */
  readonly minimumBytes = 0;
  #closing: Promise<void> | null = null;
  constructor(
    readonly build: RecordCheckpoint,
    readonly probe: RecordCheckpoint,
    readonly buildComplete: boolean,
    /** RIGHT/FULL append an internal bool: already matched before the cut. */
    readonly hasMatches: boolean,
    readonly profile: Readonly<HashJoinProfile>,
    /** Maximum occurrences of one variable-width source column in either key. */
    readonly keyMultiplicity = 1,
  ) {}
  /** A read page, staging growth overlap and an encoded output page. */
  get progressBytes(): number {
    return (
      6 * this.keyMultiplicity * Math.max(this.build.maxPageBytes, this.probe.maxPageBytes) +
      64 * 1024
    );
  }
  /** Includes directory pages appended lazily by HashTape.read(). */
  get spilledBytes(): number {
    return this.profile.spilledBytes + this.build.files.written + this.probe.files.written;
  }
  close(): Promise<void> {
    return (this.#closing ??= Promise.allSettled([this.build.close(), this.probe.close()]).then(
      (results) => {
        const errors = results
          .filter((r): r is PromiseRejectedResult => r.status === "rejected")
          .map((r) => r.reason as unknown);
        if (errors.length) throw new AggregateError(errors, "Join checkpoint close failed");
      },
    ));
  }
}
