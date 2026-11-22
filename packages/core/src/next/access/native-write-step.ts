/** A native owner funding boundary, never a row/image borrow or a commit. */
export interface NativeWriteStepBoundary {
  /** The writer's holder chain: a root funded under this boundary grows inside it
   * (GOVERNOR-WAIT-POLICY point 6, checked by the governor). */
  readonly holderChain: import("../memory/governor.js").HolderChain;
  /** Native writer accounts plus non-evictable extent-storage slabs/metadata,
   * counted once after representation relief. Excludes temporary foreign cache/read pressure. */
  readonly protectedWriteBytes: number;
  readonly ordinaryCapacity: number;
  /** Matches the selected native admission mode. */
  readonly capacityBytes: number;
  /** Funding only: do not use this signal to execute SQL after admission. */
  readonly invalidated: AbortSignal;
  /** Check immediately before admission and again before allocating its workspace. */
  check(): void;
  /** Ends this witness; it does not release accepted rows or commit anything. */
  release(): void;
}

export interface NativeWriteStepOptions {
  /** Existing admitGroup mode: only a complete step with private progress qualifies. */
  readonly includesProgress?: boolean;
}

/** Internal control flow: unwind funding and retry it, never SQL or expressions. */
export class NativeWriteStepChangedError extends Error {
  readonly code = "MINNOW_NATIVE_WRITE_STEP_CHANGED";
  override readonly name = "NativeWriteStepChangedError";
  constructor() {
    super("The native write-owner footprint changed during funding");
  }
}
